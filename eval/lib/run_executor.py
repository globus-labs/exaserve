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

import json
import os
import subprocess
import sys
import tempfile
from typing import Iterable

from .backends import get_backend_adapter
from .backends.base import BackendRunContext
from .run_planner import load_run_plan, write_run_state


def execute_run(run_yaml_path: str, *, dry_run: bool = False) -> int:
    run_plan = load_run_plan(run_yaml_path)
    adapter = get_backend_adapter(run_plan.backend_name)
    adapter.validate(run_plan)
    ctx = BackendRunContext(run_plan=run_plan)

    if dry_run:
        write_run_state(run_plan, "dry-run")
        print(f"DRY RUN: would execute {run_plan.run_id} with backend {run_plan.backend_name}")
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

    if run_plan.client.dest == "direct" and run_plan.client.num_nodes > 1:
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
        payload = json.load(handle)

    overall = payload.get("overall")
    if not isinstance(overall, dict):
        raise RuntimeError(f"Replay result file is missing an 'overall' summary: {result_path}")

    requests_completed = int(overall.get("requests_completed", 0) or 0)
    requests_scheduled = int(overall.get("requests_scheduled", requests_completed) or requests_completed)
    errors = int(overall.get("errors", 0) or 0)
    successful_requests = max(requests_completed - errors, 0)
    if successful_requests < 1:
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
