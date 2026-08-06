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
from .schedulers import get_scheduler


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

        if getattr(run_plan.client, "startup_only", False):
            print("[run_executor] startup_only=True — skipping replay client", flush=True)
            write_run_state(run_plan, "succeeded", base_urls=base_urls, exit_code=0)
            return 0

        write_run_state(run_plan, "replaying", base_urls=base_urls)
        arms = list(getattr(run_plan.client, "dispatch_topologies", []) or [])
        if arms:
            # Dispatch-topology ablation: one replay pass per arm against the
            # SAME bring-up, so the arms differ only in which node(s) each
            # client rank talks to. Results land in results/<arm>/.
            exit_code = 0
            for arm in arms:
                print(f"[run_executor] dispatch topology arm: {arm}", flush=True)
                rc = _run_replay_client(
                    run_plan,
                    base_urls,
                    extra_env={
                        "EXASERVE_DIRECT_TOPOLOGY": arm,
                        "EXASERVE_RESULT_SUBDIR": arm,
                    },
                    log_name=f"replay_{arm}.log",
                )
                print(f"[run_executor] arm {arm} exited {rc}", flush=True)
                if rc != 0:
                    # Keep going: a failed arm should not cost us the others,
                    # which already paid for the bring-up.
                    exit_code = rc
        else:
            # "direct" means node-local dispatch (the default); mesh must be
            # asked for explicitly. Always pass it so the arm is on the record.
            exit_code = _run_replay_client(
                run_plan,
                base_urls,
                extra_env={
                    "EXASERVE_DIRECT_TOPOLOGY": getattr(
                        run_plan.client, "direct_dispatch", "local"
                    )
                },
            )
        if exit_code == 0:
            # Collect per-replica vLLM stats before tearing down the cluster.
            # IMP-B08: if stats were REQUESTED and collection failed, the run is
            # telemetry-incomplete and must not be labelled succeeded.
            stats_error = None
            if getattr(run_plan.deployment, "collect_stats", False):
                try:
                    from .server_stats import collect_server_stats
                    collect_server_stats(run_plan.bundle.results_dir)
                except Exception as e:
                    stats_error = str(e)
                    print(f"[run_executor] WARNING: server stats collection failed: {e}", flush=True)
            replay_summary = _validate_replay_results(run_plan)
            # IMP-B08 / PR-019: distinguish PARTIAL from SUCCEEDED. A run with
            # request errors, an incomplete dispatch, or a missing rank shard is
            # not a success — labelling it so contaminates downstream analysis.
            incomplete_reasons = list(replay_summary.get("incomplete_reasons", []))
            if stats_error:
                incomplete_reasons.append(f"required stats collection failed: {stats_error}")
            state = "succeeded" if not incomplete_reasons else "partial"
            write_run_state(
                run_plan,
                state,
                base_urls=base_urls,
                exit_code=exit_code,
                result_path=replay_summary["result_path"],
                requests_completed=replay_summary["requests_completed"],
                requests_scheduled=replay_summary["requests_scheduled"],
                errors=replay_summary["errors"],
                incomplete_reasons=incomplete_reasons or None,
            )
            if incomplete_reasons:
                print(f"[run_executor] run marked PARTIAL (not succeeded): "
                      f"{'; '.join(incomplete_reasons)}", flush=True)
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
    scheduler = get_scheduler(getattr(run_plan.scheduler, "type", "pbs"))
    if dry_run:
        print(f"[{scheduler.name}] submit {run_plan.bundle.job_path}")
        return 0
    ok, msg = scheduler.submit(run_plan.bundle.job_path)
    print(msg)
    return 0 if ok else 1


def _run_replay_client(
    run_plan,
    base_urls: Iterable[str],
    *,
    extra_env: dict[str, str] | None = None,
    log_name: str = "replay.log",
) -> int:
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = run_plan.repo_root + (
        os.pathsep + existing_pythonpath if existing_pythonpath else ""
    )
    if extra_env:
        env.update(extra_env)
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
            # Fan the load generator out to `client.num_nodes` client nodes.
            # PBS/PALS uses mpiexec --hostfile; Slurm uses srun --nodelist
            # (Cray/Slurm sites have no mpiexec).
            if getattr(run_plan.scheduler, "type", "pbs") == "slurm":
                with open(hostfile, "r", encoding="utf-8") as _hf:
                    _nodes = [ln.strip() for ln in _hf if ln.strip()]
                command = [
                    "srun",
                    f"--nodes={run_plan.client.num_nodes}",
                    "--ntasks-per-node=1",
                    "--cpu-bind=none",
                    f"--nodelist={','.join(_nodes)}",
                    *replay_cmd,
                ]
            else:
                # PALS forwards the launcher environment, but the ablation arms
                # differ ONLY by env var — pass them explicitly so a forwarding
                # change can never silently collapse the arms into one.
                env_flags: list[str] = []
                for key, value in (extra_env or {}).items():
                    env_flags.extend(["--env", f"{key}={value}"])
                command = [
                    "mpiexec",
                    "-n",
                    str(run_plan.client.num_nodes),
                    "--ppn",
                    "1",
                    "--cpu-bind",
                    "none",
                    *env_flags,
                    "--hostfile",
                    hostfile,
                    *replay_cmd,
                ]
            return _run_command_with_tee(
                command,
                log_path=os.path.join(run_plan.bundle.logs_dir, log_name),
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
        log_path=os.path.join(run_plan.bundle.logs_dir, log_name),
        cwd=run_plan.repo_root,
        env=env,
    )


def _build_hostfile(client_nodes: int) -> str:
    nodefile = os.environ.get("EXASERVE_NODEFILE") or os.environ.get("PBS_NODEFILE")
    if not nodefile or not os.path.isfile(nodefile):
        raise RuntimeError("EXASERVE_NODEFILE (or PBS_NODEFILE) is required for multi-node replay")
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
    fd, path = tempfile.mkstemp(prefix="exaserve_eval_hosts_", text=True)
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
    # Topology-ablation arms write to results/<arm>/; validate the last arm.
    search_dirs = [
        os.path.join(run_plan.bundle.results_dir, arm)
        for arm in reversed(list(getattr(run_plan.client, "dispatch_topologies", []) or []))
    ] or [run_plan.bundle.results_dir]
    result_path = next(
        (p for p in (_latest_result_path(d) for d in search_dirs) if p is not None), None
    )
    if result_path is None:
        raise RuntimeError(
            "Replay exited successfully but did not write any result*.json under "
            + ", ".join(search_dirs)
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

    # IMP-B08: report incompleteness as DATA the caller acts on, not just a
    # printed line that a "succeeded" state then contradicts.
    incomplete_reasons: list[str] = []
    if errors > 0:
        incomplete_reasons.append(f"{errors} request error(s)")
    if requests_completed < requests_scheduled:
        incomplete_reasons.append(
            f"only {requests_completed}/{requests_scheduled} requests completed")
    gather = (payload.get("meta") or {}).get("gather") if isinstance(payload, dict) else None
    if gather and not gather.get("complete", True):
        incomplete_reasons.append(
            f"incomplete gather: {gather.get('collected_ranks')}/"
            f"{gather.get('expected_ranks')} rank shards "
            f"(missing {gather.get('missing_ranks')})")
    if incomplete_reasons:
        print(
            "Replay completed with partial results: "
            f"{'; '.join(incomplete_reasons)}; result_path={result_path}",
            flush=True,
        )

    return {
        "result_path": result_path,
        "requests_completed": requests_completed,
        "requests_scheduled": requests_scheduled,
        "errors": errors,
        "incomplete_reasons": incomplete_reasons,
    }


def _result_index(name: str) -> int:
    suffix = name[len("result"):-len(".json")]
    return int(suffix) if suffix.isdigit() else -1


def _latest_result_path(results_dir: str) -> str | None:
    # PR-019: numeric ordering — lexicographic sort made result9.json beat
    # result10.json and validated a stale file as the newest result.
    try:
        candidates = [
            name for name in os.listdir(results_dir)
            if name.startswith("result") and name.endswith(".json")
            and _result_index(name) >= 0
        ]
    except FileNotFoundError:
        return None
    if not candidates:
        return None
    return os.path.join(results_dir, max(candidates, key=_result_index))


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

# Per-queue slot limits now live on the scheduler backend (scheduler.slot_limits()).
_DEFAULT_QUEUE_SLOTS = 2  # conservative fallback for unknown queues/partitions
_MAX_SUBMIT_ATTEMPTS = 20  # PR-013: bound retries; then record a permanent failure
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

    # --- Cross-host exclusive lease (PR-013) ---
    # Atomic O_CREAT|O_EXCL acquisition (no check-then-write window); a live
    # lease from ANOTHER host is respected (the old code treated any foreign
    # host as stale and cleaned it, which duplicates jobs on a shared FS).
    from exaserve.state.atomic import ExclusiveLease, LeaseHeldError

    lock_path = os.path.join(group_dir, ".submit_all.lock")
    try:
        lease = ExclusiveLease(lock_path, ttl_s=86400,
                               owner_note=f"submit-all {spec_name}").acquire()
    except LeaseHeldError as exc:
        print(
            f"WARNING: submit-all for {spec_name!r} is already running "
            f"({exc.owner.get('host')}:{exc.owner.get('pid')}, "
            f"note={exc.owner.get('note')}). Refusing to start a second instance.",
            flush=True,
        )
        return 1

    try:
        return _submit_all_locked(group_dir, spec_name, dry_run, poll_interval)
    finally:
        lease.release()


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

    scheduler = get_scheduler(getattr(pending[0].scheduler, "type", "pbs"))
    slot_limits = scheduler.slot_limits()

    if dry_run:
        for run_plan in pending:
            print(
                f"  [dry-run] {scheduler.name} submit {run_plan.bundle.job_path}  "
                f"({run_plan.run_group_id}/{run_plan.run_id}, queue={run_plan.scheduler.queue})"
            )
        return 0

    remaining = list(pending)
    submitted: list[str] = []
    failed: dict[str, str] = {}
    attempts: dict[str, int] = {}  # PR-013: per-run submission attempt counter
    user = getpass.getuser()

    while remaining:
        queue_counts = scheduler.count_queued(user)
        if queue_counts is None:
            # PR-014: scheduler unobservable — fail closed. Do not submit on
            # a blind count (that is how duplicate floods happen); wait and
            # re-observe.
            print(
                "  [submit-all] WARNING: scheduler queue counts unavailable "
                "(qstat/squeue failed); holding submissions for 30s.",
                flush=True,
            )
            time.sleep(30)
            continue
        next_round: list = []

        for run_plan in remaining:
            queue = run_plan.scheduler.queue
            limit = slot_limits.get(queue, _DEFAULT_QUEUE_SLOTS)
            current = queue_counts.get(queue, 0)

            if limit is not None and current >= limit:
                next_round.append(run_plan)
                continue

            # IMP-B09 residue: close the submit-then-persist crash window.
            # Write a SUBMITTING intent BEFORE calling the scheduler, so a
            # crash between submit() and the success write still leaves
            # evidence that this run may already own a scheduler job. A later
            # invocation must reconcile rather than blindly resubmit.
            from .run_planner import write_run_state

            try:
                write_run_state(run_plan, "submitting",
                                submit_attempt=attempts.get(run_plan.run_id, 0) + 1)
            except Exception as exc:
                # If we cannot even record intent, do NOT submit — an
                # unrecorded submission is exactly the duplicate-job hazard.
                failed[run_plan.run_id] = f"could not record submit intent: {exc}"
                print(f"  [{len(submitted)}/{total}] FAILED    "
                      f"{run_plan.run_id}: cannot persist submit intent: {exc}",
                      flush=True)
                continue

            ok, msg = scheduler.submit(run_plan.bundle.job_path)
            if ok:
                # PR-013: record the scheduler job identity durably.
                try:
                    write_run_state(run_plan, "submitted", scheduler_job_id=msg)
                except Exception as exc:  # persistence failure is not fatal here
                    print(f"  WARNING: failed to persist submitted state for "
                          f"{run_plan.run_id}: {exc}", flush=True)
                submitted.append(run_plan.run_id)
                queue_counts[queue] = current + 1
                print(
                    f"  [{len(submitted)}/{total}] Submitted "
                    f"{run_plan.run_group_id}/{run_plan.run_id}: {msg}",
                    flush=True,
                )
            else:
                # qsub rejected. Retry a bounded number of times (queue-full is
                # transient); after the cap, record a permanent failure instead
                # of retrying forever (PR-013).
                # Scheduler REJECTED the submission: no job exists, so clear
                # the intent marker back to a retryable state.
                try:
                    write_run_state(run_plan, "planned", last_submit_error=msg)
                except Exception:
                    pass
                attempts[run_plan.run_id] = attempts.get(run_plan.run_id, 0) + 1
                if attempts[run_plan.run_id] >= _MAX_SUBMIT_ATTEMPTS:
                    failed[run_plan.run_id] = msg
                    print(
                        f"  [{len(submitted)}/{total}] FAILED    "
                        f"{run_plan.run_group_id}/{run_plan.run_id} after "
                        f"{attempts[run_plan.run_id]} attempts: {msg}",
                        flush=True,
                    )
                else:
                    next_round.append(run_plan)
                    print(
                        f"  [{len(submitted)}/{total}] Deferred  "
                        f"{run_plan.run_group_id}/{run_plan.run_id} "
                        f"(attempt {attempts[run_plan.run_id]}): {msg}",
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


# PR-013: a run in any of these states already has (or had) a scheduler job;
# re-running submit-all must NOT create a second job for it. "succeeded"
# additionally requires results on disk (below).
# "submitting" means a submit call may have reached the scheduler before we
# crashed — ambiguous, so it is treated as in-flight and requires explicit
# reconciliation rather than a blind resubmit (IMP-B09).
_IN_FLIGHT_STATES = {"submitting", "submitted", "running", "replaying"}


def _is_completed(run_dir: str) -> bool:
    """True if this run must be SKIPPED by submit-all discovery: it already
    succeeded (with results) or is in flight with a recorded scheduler job."""
    state_path = os.path.join(run_dir, "state", "status.json")
    if not os.path.isfile(state_path):
        return False
    try:
        with open(state_path, "r", encoding="utf-8") as fh:
            state = json.load(fh)
    except (json.JSONDecodeError, OSError):
        return False
    status = state.get("status")
    if status in _IN_FLIGHT_STATES:
        # Idempotency: don't resubmit a run we already handed to the scheduler.
        return True
    if status == "partial":
        # IMP-B08: a partial run HAS executed. Do not silently resubmit it
        # (that would loop); it needs an explicit human decision. It is also
        # not "succeeded" — downstream analysis must see the partial state.
        return True
    if status != "succeeded":
        return False
    results_dir = os.path.join(run_dir, "results")
    try:
        return any(
            name.startswith("result") and name.endswith(".json")
            for name in os.listdir(results_dir)
        )
    except FileNotFoundError:
        return False


# Job submission and per-queue counting now live on the scheduler backend
# (exaserve eval/lib/schedulers/): scheduler.submit() and scheduler.count_queued().
