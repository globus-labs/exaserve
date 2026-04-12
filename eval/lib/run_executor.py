"""Run executor: drive the full lifecycle of a materialized run.

execute_run() is the entry point called inside a PBS job. It:

  1. Loads the RunPlan from the run.yaml written by the planner.
  2. Delegates to the backend adapter for: launch -> wait_ready -> discover_targets.
  3. Spawns the replay client (eval/replay_client.py) against the discovered
     endpoints, optionally via MPI for multi-node client fanout.
  4. Writes state transitions (running -> replaying -> succeeded/failed) to
     the run bundle's state file for external monitoring.
  5. Ensures the backend is stopped in the finally block regardless of outcome.

submit_run() is a convenience wrapper that calls `qsub` on the PBS job
script rendered by the planner.
"""

from __future__ import annotations

import getpass
import json
import os
import subprocess
import sys
import tempfile
import time
from typing import Iterable

from .backends import get_backend_adapter
from .backends.base import BackendRunContext
from .run_planner import load_run_plan, resolve_run_group_dir, write_run_state


def execute_run(run_yaml_path: str, *, dry_run: bool = False) -> int:
    run_plan = load_run_plan(run_yaml_path)
    adapter = get_backend_adapter(run_plan.backend_name)
    adapter.validate(run_plan)
    ctx = BackendRunContext(run_plan=run_plan)

    if dry_run:
        write_run_state(run_plan, "dry-run")
        print(
            f"DRY RUN: would execute {run_plan.run_group_id}/{run_plan.run_id} "
            f"with backend {run_plan.backend_name}"
        )
        return 0

    launched = None
    base_urls: list[str] = []
    try:
        write_run_state(run_plan, "running", backend=run_plan.backend_name)
        launched = adapter.launch(ctx)
        adapter.wait_ready(ctx, launched)
        base_urls = adapter.discover_targets(ctx, launched)
        write_run_state(run_plan, "replaying", base_urls=base_urls)
        exit_code = _run_replay_client(run_plan, base_urls)
        if exit_code == 0:
            # Collect per-replica vLLM stats before tearing down the cluster.
            if getattr(run_plan.deployment, "collect_stats", False):
                try:
                    from .server_stats import collect_server_stats
                    collect_server_stats(run_plan.bundle.results_dir)
                except Exception as e:
                    print(f"[run_executor] WARNING: server stats collection failed: {e}", flush=True)
            replay_summary = _validate_replay_results(run_plan)
            write_run_state(
                run_plan,
                "succeeded",
                base_urls=base_urls,
                exit_code=exit_code,
                result_path=replay_summary["result_path"],
                requests_completed=replay_summary["requests_completed"],
                requests_scheduled=replay_summary["requests_scheduled"],
                errors=replay_summary["errors"],
            )
        else:
            write_run_state(run_plan, "failed", base_urls=base_urls, exit_code=exit_code)
        return exit_code
    except Exception as exc:
        payload = {"error": str(exc)}
        if base_urls:
            payload["base_urls"] = base_urls
        write_run_state(run_plan, "failed", **payload)
        raise
    finally:
        if launched is not None:
            adapter.stop(ctx, launched)


def submit_run(target: str, *, dry_run: bool = False) -> int:
    run_yaml_path = _resolve_run_yaml(target)
    run_plan = load_run_plan(run_yaml_path)
    cmd = ["qsub", run_plan.bundle.job_path]
    if dry_run:
        print(" ".join(cmd))
        return 0
    result = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if result.returncode == 0:
        print(result.stdout.strip())
    else:
        print(result.stderr.strip() or result.stdout.strip())
    return result.returncode


def _run_replay_client(run_plan, base_urls: Iterable[str]) -> int:
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = run_plan.repo_root + (
        os.pathsep + existing_pythonpath if existing_pythonpath else ""
    )
    replay_cmd = [
        sys.executable,
        "-m",
        "eval.replay_client",
        "--config",
        run_plan.runtime_manifest_path,
        "--num-runs",
        str(run_plan.client.num_runs),
        "--dest",
        run_plan.client.dest,
        "--base-urls",
        ",".join(base_urls),
    ]
    if run_plan.client.include_tp:
        replay_cmd.append("--include-tp")
    if run_plan.client.early_stop > 0:
        replay_cmd.extend(["--early-stop", str(run_plan.client.early_stop)])

    if run_plan.client.num_nodes > 1:
        hostfile = _build_hostfile(run_plan.client.num_nodes)
        try:
            command = [
                "mpiexec",
                "-n",
                str(run_plan.client.num_nodes),
                "--ppn",
                "1",
                "--cpu-bind",
                "none",
                "--hostfile",
                hostfile,
                *replay_cmd,
            ]
            return _run_command_with_tee(
                command,
                log_path=os.path.join(run_plan.bundle.logs_dir, "replay.log"),
                cwd=run_plan.repo_root,
                env=env,
            )
        finally:
            try:
                os.remove(hostfile)
            except OSError:
                pass

    return _run_command_with_tee(
        replay_cmd,
        log_path=os.path.join(run_plan.bundle.logs_dir, "replay.log"),
        cwd=run_plan.repo_root,
        env=env,
    )


def _build_hostfile(client_nodes: int) -> str:
    nodefile = os.environ.get("PBS_NODEFILE")
    if not nodefile or not os.path.isfile(nodefile):
        raise RuntimeError("PBS_NODEFILE is required for multi-node replay")
    nodes = []
    with open(nodefile, "r", encoding="utf-8") as handle:
        for line in handle:
            node = line.strip()
            if node and node not in nodes:
                nodes.append(node)
    if len(nodes) < client_nodes:
        raise RuntimeError(
            f"Requested {client_nodes} replay client nodes, but only found {len(nodes)} in PBS_NODEFILE"
        )
    fd, path = tempfile.mkstemp(prefix="aurora_eval_hosts_", text=True)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        for node in nodes[:client_nodes]:
            handle.write(node + "\n")
    return path


def _run_command_with_tee(cmd, *, log_path: str, cwd: str, env: dict[str, str]) -> int:
    os.makedirs(os.path.dirname(os.path.abspath(log_path)), exist_ok=True)
    process = subprocess.Popen(
        cmd,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    with open(log_path, "a", encoding="utf-8") as log_handle:
        assert process.stdout is not None
        for line in process.stdout:
            print(line, end="", flush=True)
            log_handle.write(line)
            log_handle.flush()
    return process.wait()


def _validate_replay_results(run_plan) -> dict[str, int | str]:
    result_path = _latest_result_path(run_plan.bundle.results_dir)
    if result_path is None:
        raise RuntimeError(
            f"Replay exited successfully but did not write any result*.json under {run_plan.bundle.results_dir}"
        )

    with open(result_path, "r", encoding="utf-8") as handle:
        raw = handle.read()
    # Tolerate trailing garbage from filesystem quirks (Lustre truncation bugs,
    # aborted prior writes that left leftover bytes past the valid JSON body).
    # Use raw_decode to parse the first valid JSON object and ignore the rest.
    try:
        payload, end_pos = json.JSONDecoder().raw_decode(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Replay result file is not valid JSON: {result_path}: {exc}") from exc
    trailing = len(raw) - end_pos
    if trailing > 0:
        # Rewrite the file in-place to drop trailing garbage so downstream tools
        # don't trip on it.  Best effort: only log on failure.
        print(
            f"[run_executor] Stripping {trailing} trailing byte(s) of garbage "
            f"from {result_path}",
            flush=True,
        )
        try:
            with open(result_path, "w", encoding="utf-8") as handle:
                handle.write(raw[:end_pos])
        except OSError as exc:
            print(f"[run_executor] Warning: could not rewrite {result_path}: {exc}", flush=True)

    # Saturation results are flat dicts with __type__=summary; replay results nest under "overall".
    overall = payload.get("overall")
    if not isinstance(overall, dict):
        if payload.get("__type__") == "summary" or payload.get("saturation_rate") is not None:
            overall = payload  # saturation result: flat dict is the summary
        else:
            raise RuntimeError(f"Replay result file is missing an 'overall' summary: {result_path}")

    requests_completed = int(overall.get("requests_completed", 0) or 0)
    requests_scheduled = int(overall.get("requests_scheduled", requests_completed) or requests_completed)
    errors = int(overall.get("errors", 0) or 0)
    successful_requests = max(requests_completed - errors, 0)
    is_saturation = overall.get("saturation_rate") is not None
    if successful_requests < 1 and not is_saturation:
        raise RuntimeError(
            "Replay completed but all requests failed: "
            f"successes={successful_requests}, errors={errors}, "
            f"completed={requests_completed}, scheduled={requests_scheduled}, "
            f"result_path={result_path}"
        )

    if errors > 0 or requests_completed < requests_scheduled:
        print(
            "Replay completed with partial request failures: "
            f"successes={successful_requests}, errors={errors}, "
            f"completed={requests_completed}, scheduled={requests_scheduled}, "
            f"result_path={result_path}",
            flush=True,
        )

    return {
        "result_path": result_path,
        "requests_completed": requests_completed,
        "requests_scheduled": requests_scheduled,
        "errors": errors,
    }


def _latest_result_path(results_dir: str) -> str | None:
    try:
        candidates = [
            name for name in os.listdir(results_dir)
            if name.startswith("result") and name.endswith(".json")
        ]
    except FileNotFoundError:
        return None
    if not candidates:
        return None
    return os.path.join(results_dir, sorted(candidates)[-1])


def _resolve_run_yaml(target: str) -> str:
    if os.path.isdir(target):
        candidate = os.path.join(target, "run.yaml")
        if os.path.isfile(candidate):
            return candidate
    if os.path.isfile(target):
        return os.path.abspath(target)
    raise FileNotFoundError(f"Could not resolve run.yaml from {target}")


# ---------------------------------------------------------------------------
# submit_all: batch-submit all pending runs for a spec
# ---------------------------------------------------------------------------

# Max jobs (running + queued) the scheduler accepts per queue.
_QUEUE_SLOT_LIMITS: dict[str, int | None] = {
    "debug": 2,           # 1 running + 1 queued
    "debug-scaling": 2,   # 1 running + 1 queued
    "prod": None,         # 1 running + unlimited queued
}
_DEFAULT_QUEUE_SLOTS = 2  # conservative fallback for unknown queues
_POLL_INTERVAL_S = 300    # 5 min between polls — minimizes qstat load on login node


def submit_all(
    spec_name: str,
    *,
    run_group: str = "latest",
    experiments_root: str | None = None,
    dry_run: bool = False,
    poll_interval: int = _POLL_INTERVAL_S,
) -> int:
    """Submit all pending runs for *spec_name*, respecting per-queue limits.

    Runs that already succeeded (status ``succeeded`` **and** a result file
    exists) are skipped.  When a queue is full the function retries every
    *poll_interval* seconds until all runs have been submitted.

    A lock file in the run group directory prevents concurrent submit_all
    processes for the same spec.  Stale locks (owner PID dead) are cleaned
    automatically with a WARNING printed.
    """
    try:
        group_dir = resolve_run_group_dir(
            spec_name,
            run_group=run_group,
            experiments_root=experiments_root,
        )
    except FileNotFoundError as exc:
        print(str(exc))
        return 1

    # --- Lock file check ---
    lock_path = os.path.join(group_dir, ".submit_all.lock")
    stale = _check_and_acquire_lock(lock_path, spec_name)
    if stale is None:
        # Lock held by a live process — abort
        return 1

    try:
        return _submit_all_locked(group_dir, spec_name, dry_run, poll_interval)
    finally:
        _release_lock(lock_path)


def _check_and_acquire_lock(lock_path: str, spec_name: str) -> bool | None:
    """Check for existing lock, acquire if free.

    Returns:
        True  — acquired, stale lock was cleaned
        False — acquired, no prior lock
        None  — lock held by live process, caller should abort
    """
    if os.path.isfile(lock_path):
        try:
            with open(lock_path, "r") as fh:
                lock_info = json.load(fh)
        except (json.JSONDecodeError, OSError):
            lock_info = {}

        owner_pid = lock_info.get("pid", 0)
        owner_host = lock_info.get("hostname", "?")
        started = lock_info.get("started", "?")
        import socket
        current_host = socket.gethostname()

        if owner_host == current_host and _pid_alive(owner_pid):
            print(
                f"WARNING: STALE_PROCESS_DETECTED — submit-all for {spec_name!r} "
                f"is already running (pid={owner_pid}, host={owner_host}, "
                f"started={started}). Refusing to start a second instance.",
                flush=True,
            )
            print(
                f"  Lock file: {lock_path}\n"
                f"  To force: kill {owner_pid} or delete the lock file.",
                flush=True,
            )
            return None

        # Stale lock — owner dead or different host
        print(
            f"WARNING: STALE_LOCK_CLEANED — previous submit-all "
            f"(pid={owner_pid}, host={owner_host}, started={started}) "
            f"is no longer running. Cleaning lock and proceeding.",
            flush=True,
        )
        os.remove(lock_path)
        _write_lock(lock_path)
        return True

    _write_lock(lock_path)
    return False


def _write_lock(lock_path: str) -> None:
    import socket
    lock_info = {
        "pid": os.getpid(),
        "hostname": socket.gethostname(),
        "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    with open(lock_path, "w") as fh:
        json.dump(lock_info, fh)


def _release_lock(lock_path: str) -> None:
    try:
        os.remove(lock_path)
    except FileNotFoundError:
        pass


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists but owned by another user


def _submit_all_locked(
    group_dir: str,
    spec_name: str,
    dry_run: bool,
    poll_interval: int,
) -> int:
    pending = _discover_pending_runs(group_dir)
    if not pending:
        print(f"No pending runs found under {group_dir}")
        return 0

    total = len(pending)
    print(f"Found {total} pending run(s) for {spec_name!r} in {os.path.basename(group_dir)!r}")

    if dry_run:
        for run_plan in pending:
            print(
                f"  [dry-run] qsub {run_plan.bundle.job_path}  "
                f"({run_plan.run_group_id}/{run_plan.run_id}, queue={run_plan.scheduler.queue})"
            )
        return 0

    remaining = list(pending)
    submitted: list[str] = []
    failed: dict[str, str] = {}

    while remaining:
        queue_counts = _count_queued_jobs()
        next_round: list = []

        for run_plan in remaining:
            queue = run_plan.scheduler.queue
            limit = _QUEUE_SLOT_LIMITS.get(queue, _DEFAULT_QUEUE_SLOTS)
            current = queue_counts.get(queue, 0)

            if limit is not None and current >= limit:
                next_round.append(run_plan)
                continue

            ok, msg = _try_qsub(run_plan)
            if ok:
                submitted.append(run_plan.run_id)
                queue_counts[queue] = current + 1
                print(
                    f"  [{len(submitted)}/{total}] Submitted "
                    f"{run_plan.run_group_id}/{run_plan.run_id}: {msg}",
                    flush=True,
                )
            else:
                # qsub rejected — likely queue full despite our count, retry
                next_round.append(run_plan)
                print(
                    f"  [{len(submitted)}/{total}] Deferred  "
                    f"{run_plan.run_group_id}/{run_plan.run_id}: {msg}",
                    flush=True,
                )

        remaining = next_round
        if remaining:
            print(
                f"  {len(remaining)} run(s) waiting for queue slots, "
                f"retrying in {poll_interval}s ...",
                flush=True,
            )
            time.sleep(poll_interval)

    print(f"\nAll {len(submitted)}/{total} run(s) submitted.", flush=True)
    if failed:
        print("Permanent failures:")
        for rid, err in failed.items():
            print(f"  {rid}: {err}")
        return 1
    return 0


def _natural_sort_key(name: str):
    """Sort key that orders '2-nodes' before '16-nodes' before '128-nodes'."""
    import re
    return [int(s) if s.isdigit() else s.lower() for s in re.split(r'(\d+)', name)]


def _discover_pending_runs(group_dir: str):
    """Return RunPlan objects for runs that are not yet successfully completed."""
    pending = []
    for entry in sorted(os.listdir(group_dir), key=_natural_sort_key):
        run_yaml = os.path.join(group_dir, entry, "run.yaml")
        if not os.path.isfile(run_yaml):
            continue
        if _is_completed(os.path.join(group_dir, entry)):
            continue
        pending.append(load_run_plan(run_yaml))
    return pending


def _is_completed(run_dir: str) -> bool:
    """A run is completed if its status is 'succeeded' and results exist."""
    state_path = os.path.join(run_dir, "state", "status.json")
    if not os.path.isfile(state_path):
        return False
    try:
        with open(state_path, "r", encoding="utf-8") as fh:
            state = json.load(fh)
    except (json.JSONDecodeError, OSError):
        return False
    if state.get("status") != "succeeded":
        return False
    results_dir = os.path.join(run_dir, "results")
    try:
        return any(
            name.startswith("result") and name.endswith(".json")
            for name in os.listdir(results_dir)
        )
    except FileNotFoundError:
        return False


def _count_queued_jobs() -> dict[str, int]:
    """Count the current user's running + queued PBS jobs per queue."""
    user = getpass.getuser()
    try:
        result = subprocess.run(
            ["qstat", "-u", user],
            capture_output=True, text=True, check=False, timeout=600,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {}
    counts: dict[str, int] = {}
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) < 6:
            continue
        # PBS table: Job_Id  Username  Queue  Jobname  SessID  NDS  TSK  Mem  Time  S  Elap
        # We need the queue (col index 2) and state (col index 9 typically).
        # Simpler: just match lines with our username and a known state letter.
        if user not in line:
            continue
        # Find the single-letter state column: R, Q, H, E, B, etc.
        state = None
        queue = None
        for i, part in enumerate(parts):
            if part in ("R", "Q", "H", "B", "E", "W", "S"):
                state = part
                # Queue is earlier in the line — in standard PBS output it's column 2
                if i >= 3:
                    queue = parts[2]
                break
        if state in ("R", "Q", "H", "B") and queue:
            counts[queue] = counts.get(queue, 0) + 1
    return counts


def _try_qsub(run_plan) -> tuple[bool, str]:
    """Attempt qsub; return (success, message)."""
    try:
        result = subprocess.run(
            ["qsub", run_plan.bundle.job_path],
            capture_output=True, text=True, check=False, timeout=600,
        )
    except subprocess.TimeoutExpired:
        return False, "qsub timed out (600s)"
    msg = (result.stdout.strip() or result.stderr.strip())
    return result.returncode == 0, msg
