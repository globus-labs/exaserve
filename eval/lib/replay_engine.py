from __future__ import annotations

import asyncio
import glob
import json
import math
import os
import pathlib
import shutil
import signal
import statistics
import subprocess
import tempfile
import threading
import time
import uuid
from dataclasses import asdict

from eval.lib.manifest import EvalManifest, TraceGeneratorConfig, WeakScalingConfig, load_eval_manifest
from src.schemas import load_proxy_config

try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:  # pragma: no cover - optional dependency
    pass

try:
    from mpi4py import MPI as _MPI

    _MPI_AVAILABLE = True
except ImportError:  # pragma: no cover - optional dependency
    _MPI_AVAILABLE = False


TIMEOUT_S = 3600


def _init_mpi():
    if _MPI_AVAILABLE:
        comm = _MPI.COMM_WORLD
        return comm, comm.Get_rank(), comm.Get_size()
    return None, 0, 1


def _mpi_barrier(comm):
    if comm is not None and comm.Get_size() > 1:
        comm.Barrier()


def _mpi_bcast(comm, value, root=0):
    if comm is not None and comm.Get_size() > 1:
        return comm.bcast(value, root=root)
    return value


def _mpi_gather(comm, value, root=0):
    if comm is not None and comm.Get_size() > 1:
        return comm.gather(value, root=root)
    return [value]


class TraceRequest(object):
    def __init__(
        self,
        timestamp: float,
        model: str,
        prompt: str,
        input_len: int,
        output_len: int,
        tensor_parallel_size: int,
        req_id: str,
        mode: str = "chat",
    ) -> None:
        self.timestamp = timestamp
        self.model = model
        self.prompt = prompt
        self.input_len = input_len
        self.output_len = output_len
        self.tensor_parallel_size = tensor_parallel_size
        self.req_id = req_id
        self.mode = mode


def _percentile(values: list[float], fraction: float) -> float:
    if not values:
        raise ValueError("percentile requires at least one value")
    if len(values) == 1:
        return float(values[0])
    ordered = sorted(float(value) for value in values)
    rank = (len(ordered) - 1) * fraction
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return ordered[int(rank)]
    return ordered[low] * (high - rank) + ordered[high] * (rank - low)


def _trace_path(exp_config: EvalManifest) -> str:
    trace_cfg = exp_config.job_trace_config
    return str(trace_cfg.output_trace_path)


def _result_dir(exp_config: EvalManifest) -> str:
    return exp_config.pbs_result_dir


def _find_go_binary() -> str | None:
    script_dir = pathlib.Path(__file__).resolve().parent.parent
    candidates = [
        script_dir / "go_client" / "bin" / "go_dispatch",
        script_dir.parent / "eval" / "go_client" / "bin" / "go_dispatch",
    ]
    for path in candidates:
        if path.is_file() and os.access(path, os.X_OK):
            return str(path)
    return None


def _write_trace_partition(requests: list[TraceRequest], path: str) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        for request in requests:
            record = {
                "timestamp": request.timestamp,
                "model": request.model,
                "mode": request.mode,
                "prompt": request.prompt,
                "input_len": request.input_len,
                "output_len": request.output_len,
                "tensor_parallel_size": request.tensor_parallel_size,
                "req_id": request.req_id,
            }
            handle.write(json.dumps(record) + "\n")


def _spawn_go_procs(
    go_bin: str,
    base_urls: list[str],
    rank_requests: list[TraceRequest],
    generation_mode: str,
    include_tp: bool,
    concurrency: int,
    num_go_workers: int,
    rank: int,
    tmp_dir: str,
    sum_only: bool = False,
    num_go_procs: int = 1,
    warmup_rps: int = 0,
    warmup_duration_s: float = 0.0,
    stream: bool = False,
    cpuprofile_dir: str = "",
):
    # When concurrency=0 (auto-derive), the Go client derives from the ephemeral
    # port range. But with multiple Go procs sharing the same port range, each proc
    # must use a fraction to avoid port exhaustion.
    # For multi-proc, leave concurrency=0 so each Go process auto-derives
    # independently (10240 cap). This works for real inference where each proc
    # targets a subset of backends. For fast-server benchmarks (clientlab),
    # the spec should set max_active_requests explicitly.

    request_map = {request.req_id: request for request in rank_requests}
    partitions = [rank_requests[index::max(1, num_go_procs)] for index in range(max(1, num_go_procs))]
    processes = []
    result_paths = []
    for proc_index, partition in enumerate(partitions):
        trace_path = os.path.join(tmp_dir, f"rank{rank}_p{proc_index}_trace.jsonl")
        result_path = os.path.join(tmp_dir, f"rank{rank}_p{proc_index}_results.jsonl")
        result_paths.append(result_path)
        _write_trace_partition(partition, trace_path)
        cmd = [
            go_bin,
            "--base-urls",
            ",".join(base_urls),
            "--generation-mode",
            generation_mode,
            "--timeout",
            str(TIMEOUT_S),
            "--max-active-requests",
            str(concurrency),
            "--queue-capacity",
            "0",
            "--max-conns-per-host",
            str(concurrency),
            "--num-go-workers",
            str(num_go_workers),
            "--worker-id",
            f"rank{rank}_p{proc_index}",
            "--trace-file",
            trace_path,
            "--result-file",
            result_path,
        ]
        if warmup_rps > 0 and warmup_duration_s > 0:
            cmd.extend(["--warmup-rps", str(warmup_rps), "--warmup-duration", str(warmup_duration_s)])
        if cpuprofile_dir:
            prof_path = os.path.join(cpuprofile_dir, f"rank{rank}_p{proc_index}_cpu.prof")
            cmd.extend(["--cpuprofile", prof_path])
        if sum_only:
            cmd.append("--sum-only")
        if include_tp:
            cmd.append("--include-tp")
        if stream:
            cmd.append("--stream")
        process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        processes.append((proc_index, process))
    for proc_index, process in processes:
        line = process.stdout.readline().decode(errors="replace").strip()
        if line != "GO_CLI_READY":
            print(
                f"!!! [go_dispatch rank {rank} p{proc_index}] Expected GO_CLI_READY, got {line!r}",
                flush=True,
            )
    return processes, result_paths, request_map


def _read_go_results(result_path: str, request_map: dict[str, TraceRequest]):
    results = []
    last_fire_time = 0.0
    adjusted_run_t0 = None
    if not os.path.isfile(result_path):
        return results, last_fire_time, adjusted_run_t0

    with open(result_path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            if record.get("__type__") == "summary":
                last_fire_time = record.get("last_request_start_at", record.get("last_fire_time", 0.0))
                return record, last_fire_time, record.get("adjusted_run_t0")
            if record.get("__type__") == "dispatch_done":
                last_fire_time = record.get("last_request_start_at", record.get("last_fire_time", last_fire_time))
                if "adjusted_run_t0" in record:
                    adjusted_run_t0 = record["adjusted_run_t0"]
                continue
            request = request_map.get(record.get("req_id", ""))
            if request is None:
                continue
            results.append(
                (
                    request,
                    record.get("latency", 0.0),
                    record.get("success", False),
                    record.get("error", ""),
                    record.get("end_time", 0.0),
                    record.get("actual_prompt_tokens"),
                    record.get("actual_completion_tokens"),
                )
            )
    return results, last_fire_time, adjusted_run_t0


def _send_run_t0_and_wait(
    processes,
    run_t0: float,
    result_paths: list[str],
    request_map: dict[str, TraceRequest],
    interrupt_event,
    rank: int,
    sum_only: bool = False,
):
    alive = set(proc_index for proc_index, _process in processes)
    for _proc_index, process in processes:
        process.stdin.write(f"{run_t0!r}\n".encode())
        process.stdin.flush()
        process.stdin.close()

    while alive:
        for proc_index, process in processes:
            if proc_index not in alive:
                continue
            try:
                process.wait(timeout=0.5)
                alive.discard(proc_index)
            except subprocess.TimeoutExpired:
                pass
        if interrupt_event is not None and interrupt_event.is_set():
            for proc_index, process in processes:
                if proc_index in alive:
                    process.send_signal(signal.SIGTERM)
            break

    all_results = []
    max_last_fire_time = 0.0
    for proc_index, process in processes:
        stderr_text = process.stderr.read().decode(errors="replace")
        for line in stderr_text.splitlines():
            print(f"[go_dispatch rank {rank} p{proc_index}] {line}", flush=True)
        parsed, last_fire_time, _adjusted_run_t0 = _read_go_results(
            result_paths[proc_index],
            request_map,
        )
        max_last_fire_time = max(max_last_fire_time, last_fire_time)
        if sum_only and isinstance(parsed, dict):
            if not all_results:
                all_results = parsed
            else:
                for key in (
                    "requests_completed",
                    "requests_scheduled",
                    "errors",
                    "total_input_tokens",
                    "total_output_tokens",
                ):
                    all_results[key] = all_results.get(key, 0) + parsed.get(key, 0)
                all_results["p50_s"] = max(all_results.get("p50_s", 0.0), parsed.get("p50_s", 0.0))
                all_results["p99_s"] = max(all_results.get("p99_s", 0.0), parsed.get("p99_s", 0.0))
        else:
            all_results.extend(parsed)
    if sum_only and isinstance(all_results, dict):
        all_results["last_fire_time"] = max_last_fire_time
    return all_results, max_last_fire_time, run_t0


def _next_result_path(result_dir: str) -> str:
    os.makedirs(result_dir, exist_ok=True)
    candidates = glob.glob(os.path.join(result_dir, "result*.json"))
    next_index = 0
    for path in candidates:
        stem = os.path.splitext(os.path.basename(path))[0]
        suffix = stem.replace("result", "", 1)
        if suffix.isdigit():
            next_index = max(next_index, int(suffix) + 1)
    return os.path.join(result_dir, f"result{next_index}.json")


def _get_cluster_nodes() -> list[str]:
    nodefile = os.environ.get("PBS_NODEFILE")
    if not nodefile:
        raise RuntimeError("PBS_NODEFILE is required for direct mode without base_urls")
    with open(nodefile, "r", encoding="utf-8") as handle:
        nodes = []
        for line in handle:
            node = line.strip()
            if node and node not in nodes:
                nodes.append(node)
    if not nodes:
        raise RuntimeError("PBS_NODEFILE did not contain any hostnames")
    return nodes


def _port_from_manifest(exp_config: EvalManifest, override_port: int | None) -> int:
    if override_port is not None:
        return override_port
    proxy_cfg = exp_config.proxy_config
    if proxy_cfg.type != "none":
        port_file = os.path.join(
            os.path.dirname(os.path.abspath(exp_config.job_replay_client_config.config_path)),
            "proxy_out",
            "proxy_port",
        )
        if os.path.isfile(port_file):
            with open(port_file, "r", encoding="utf-8") as handle:
                try:
                    return int(handle.read().strip())
                except ValueError:
                    pass
        return proxy_cfg.port
    return 8000


def _load_trace_requests(trace_path: str) -> list[TraceRequest]:
    requests = []
    with open(trace_path, "r", encoding="utf-8") as handle:
        for line in handle:
            data = json.loads(line)
            if data.get("__type__") == "metadata":
                continue
            requests.append(
                TraceRequest(
                    timestamp=float(data["timestamp"]),
                    model=str(data["model"]),
                    prompt=str(data["prompt"]),
                    input_len=int(data.get("input_len", 0)),
                    output_len=int(data["output_len"]),
                    tensor_parallel_size=int(data.get("tensor_parallel_size", 1)),
                    req_id=uuid.uuid4().hex,
                    mode=str(data.get("mode", "chat")),
                )
            )
    return requests


def _resolve_saturation_request_shape(exp_config: EvalManifest, sat_cfg: dict) -> dict[str, int | str]:
    deployment_models = exp_config.model_deployment_config.model_configs
    trace_cfg = exp_config.job_trace_config
    return {
        "model": str(sat_cfg.get("model") or (deployment_models[0].model_id if deployment_models else "stub-model")),
        # Saturation uses a synthetic prompt, so use the configured workload input/output
        # lengths as the closest available request-shape proxy.
        "prompt_words": int(getattr(trace_cfg, "input_len", 0) or 32),
        "output_tokens": int(getattr(trace_cfg, "output_len", 0) or 16),
    }


def _build_sat_go_cmd(go_bin, base_urls, replay_cfg, sat_cfg, exp_config, mode, output_path, target_rate=None):
    """Build the Go client command for saturation or saturation-step mode."""
    sat_shape = _resolve_saturation_request_shape(exp_config, sat_cfg)
    cmd = [
        go_bin,
        "--mode", mode,
        "--base-urls", ",".join(base_urls),
        "--max-active-requests", str(replay_cfg.go_concurrency),
        "--num-go-workers", str(replay_cfg.num_go_workers),
        "--timeout", "3600",
        "--sat-model", str(sat_shape["model"]),
        "--sat-prompt-words", str(sat_shape["prompt_words"]),
        "--sat-output-tokens", str(sat_shape["output_tokens"]),
        "--sat-search-mode", str(sat_cfg.get("search_mode", "binary")),
        "--sat-initial-rate", str(sat_cfg.get("initial_rate", 100)),
        "--sat-max-rate", str(sat_cfg.get("max_rate", 0)),
        "--sat-step-duration", str(sat_cfg.get("step_duration_s", 10.0)),
        "--sat-warmup-duration", str(sat_cfg.get("warmup_duration_s", 3.0)),
        "--sat-cooldown-pause", str(sat_cfg.get("cooldown_pause_s", 2.0)),
        "--sat-tolerance", str(sat_cfg.get("tolerance", 0.05)),
        "--sat-max-error-rate", str(sat_cfg.get("max_error_rate", 0.01)),
        "--sat-plateau-ratio", str(sat_cfg.get("plateau_ratio", 0.95)),
        "--sat-output", str(output_path),
    ]
    if sat_cfg.get("verify") is False:
        cmd.append("--sat-verify=false")
    if sat_cfg.get("stream"):
        cmd.append("--sat-stream")
    max_ttft = float(sat_cfg.get("max_p99_ttft", 0.0))
    if max_ttft > 0:
        cmd.extend(["--sat-max-p99-ttft", str(max_ttft)])
    if mode == "saturation-step" and target_rate is not None:
        cmd.extend(["--sat-target-rate", str(target_rate)])
    # step-up params
    for key, flag in [("step_up_start", "--sat-step-up-start"), ("step_up_end", "--sat-step-up-end"), ("step_up_increment", "--sat-step-up-increment")]:
        val = int(sat_cfg.get(key, 0))
        if val > 0:
            cmd.extend([flag, str(val)])
    return cmd


def _run_saturation_from_manifest(go_bin, base_urls, replay_cfg, sat_cfg, exp_config, output_path, num_go_procs, go_concurrency):
    """Run saturation finder from the eval pipeline (single-proc or multi-proc)."""
    print(f"[replay_engine] Saturation mode: num_go_procs={num_go_procs}", flush=True)

    if num_go_procs <= 1:
        # Single-proc: Go handles entire search autonomously.
        cmd = _build_sat_go_cmd(go_bin, base_urls, replay_cfg, sat_cfg, exp_config, "saturation", output_path)
        print(f"[replay_engine] cmd: {' '.join(cmd)}", flush=True)

        # Stream stderr to a log file so we can see progress even on timeout.
        sat_log_path = pathlib.Path(output_path).parent / "saturation_stderr.log"
        sat_log = open(sat_log_path, "w", encoding="utf-8")
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=sat_log, universal_newlines=True)
        line = proc.stdout.readline().strip()
        if line != "GO_CLI_READY":
            proc.kill()
            proc.wait()
            sat_log.close()
            raise RuntimeError(f"saturation process failed readiness: {line!r}")

        max_steps = 30
        step_time = float(sat_cfg.get("step_duration_s", 10)) + float(sat_cfg.get("warmup_duration_s", 3)) + float(sat_cfg.get("cooldown_pause_s", 2))
        timeout_s = max(max_steps * step_time + 120.0, 300.0)
        try:
            proc.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            sat_log.close()
            raise RuntimeError(f"saturation process timed out after {timeout_s:.0f}s — check {sat_log_path}")
        sat_log.close()

        if proc.returncode != 0:
            print(f"[replay_engine] saturation stderr log: {sat_log_path}", flush=True)
            raise RuntimeError(f"saturation process exited with {proc.returncode} — check {sat_log_path}")

        if not pathlib.Path(output_path).exists():
            raise RuntimeError(f"saturation output missing: {output_path}")
        print(f"[replay_engine] Saturation output written to {output_path}", flush=True)

        # Write a minimal result file for compatibility with _validate_replay_results.
        sat_output = json.load(open(output_path))
        result_path = pathlib.Path(output_path).parent / "result0.json"
        sat_rate = sat_output.get("saturation_rate", 0)
        steps = sat_output.get("steps", [])
        # Use best step by achieved rate (not just healthy ones — all may be unhealthy
        # when the server is slow and plateau ratio is never met).
        best = max(steps, key=lambda s: s.get("achieved_rate", 0)) if steps else {}
        completed = int(best.get("completed", 0))
        failed = int(best.get("failed", 0))
        summary = {
            "__type__": "summary",
            "requests_completed": completed + failed,
            "requests_scheduled": completed + failed,
            "errors": failed,
            "p50_s": float(best.get("p50_latency_s", 0)),
            "p99_s": float(best.get("p99_latency_s", 0)),
            "total_input_tokens": 0,
            "total_output_tokens": 0,
            "saturation_rate": sat_rate,
            "saturation_mode": sat_output.get("mode", "binary"),
        }
        with open(result_path, "w") as f:
            json.dump(summary, f, indent=2)
        print(f"[replay_engine] Result summary written to {result_path}", flush=True)
    else:
        raise NotImplementedError("Multi-proc saturation in eval pipeline not yet implemented — use clientlab for multi-proc saturation")


async def replay_from_manifest(
    config_path: str,
    *,
    include_tp_override: bool | None = None,
    early_stop_override: float | None = None,
    num_runs_override: int | None = None,
    dest_override: str | None = None,
    proxy_port: int | None = None,
    base_urls_override: str | None = None,
    cpuprofile_dir: str = "",
) -> None:
    exp_config = load_eval_manifest(config_path)
    exp_config.job_replay_client_config.config_path = config_path
    replay_cfg = exp_config.job_replay_client_config

    include_tp = replay_cfg.include_tp if include_tp_override is None else include_tp_override
    early_stop = replay_cfg.early_stop if early_stop_override is None else early_stop_override
    num_runs = replay_cfg.num_runs if num_runs_override is None else num_runs_override
    dest = replay_cfg.dest if dest_override is None else dest_override
    generation_mode = replay_cfg.generation_mode
    num_go_procs = replay_cfg.num_go_procs
    num_go_workers = replay_cfg.num_go_workers
    go_concurrency = replay_cfg.go_concurrency
    warmup_rps = replay_cfg.warmup_rps
    warmup_duration_s = replay_cfg.warmup_duration_s
    sum_only = replay_cfg.sum_only

    comm, rank, mpi_size = _init_mpi()
    is_root = rank == 0
    trace_path = _trace_path(exp_config)
    port = _port_from_manifest(exp_config, proxy_port)
    if base_urls_override:
        cluster_nodes = []
        base_urls = [item.strip() for item in base_urls_override.split(",") if item.strip()]
    elif dest == "direct":
        cluster_nodes = _get_cluster_nodes()
        base_urls = [f"http://{node}:{port}" for node in cluster_nodes]
    else:
        cluster_nodes = []
        base_urls = [f"http://0.0.0.0:{port}"]

    go_bin = _find_go_binary()
    if go_bin is None:
        raise RuntimeError("go_dispatch binary not found; build eval/go_client/bin/go_dispatch first")

    # Saturation mode: skip trace loading, run saturation finder instead.
    sat_cfg = getattr(replay_cfg, "saturation", {}) or {}
    if isinstance(sat_cfg, dict) and sat_cfg.get("enabled"):
        if is_root:
            result_dir = pathlib.Path(exp_config.pbs_result_dir) if exp_config.pbs_result_dir else pathlib.Path(exp_config.pbs_working_dir) / "results"
            result_dir.mkdir(parents=True, exist_ok=True)
            sat_output_path = result_dir / "saturation_output.json"
            _run_saturation_from_manifest(
                go_bin, base_urls, replay_cfg, sat_cfg, exp_config,
                sat_output_path, num_go_procs, go_concurrency,
            )
        _mpi_barrier(comm)
        return

    requests = _load_trace_requests(trace_path)
    rank_requests = requests[rank::mpi_size]
    target_responses = int(len(requests) * early_stop) if early_stop and early_stop > 0 else None

    interrupted = False
    interrupt_event = threading.Event()

    def signal_handler(_sig, _frame):
        nonlocal interrupted
        interrupted = True
        interrupt_event.set()

    old_handler = signal.signal(signal.SIGINT, signal_handler)
    tmp_dir = tempfile.mkdtemp(prefix=f"replay_rank{rank}_")
    all_runs_results = []
    run_durations = []
    dispatch_timings = []
    t0 = time.time()

    try:
        loop = asyncio.get_running_loop()
        for run_index in range(num_runs):
            _mpi_barrier(comm)
            if interrupted:
                break

            run_warmup_rps = warmup_rps if run_index == 0 else 0
            run_warmup_duration = warmup_duration_s if run_index == 0 else 0.0
            go_processes, result_paths, request_map = await loop.run_in_executor(
                None,
                _spawn_go_procs,
                go_bin,
                base_urls,
                rank_requests,
                generation_mode,
                include_tp,
                go_concurrency,
                num_go_workers,
                rank,
                tmp_dir,
                sum_only,
                num_go_procs,
                run_warmup_rps,
                run_warmup_duration,
                replay_cfg.stream,
                cpuprofile_dir,
            )
            _mpi_barrier(comm)
            run_t0 = _mpi_bcast(comm, time.time() if is_root else None, root=0)
            local_results, last_fire_time, effective_run_t0 = await loop.run_in_executor(
                None,
                _send_run_t0_and_wait,
                go_processes,
                run_t0,
                result_paths,
                request_map,
                interrupt_event,
                rank,
                sum_only,
            )
            if is_root and last_fire_time > 0:
                trace_span = requests[-1].timestamp if requests else 0.0
                actual_dispatch_s = last_fire_time - effective_run_t0
                dispatch_timings.append(
                    {
                        "run_index": run_index,
                        "trace_span_s": trace_span,
                        "actual_dispatch_s": actual_dispatch_s,
                        "overhead_s": actual_dispatch_s - trace_span,
                    }
                )
            _mpi_barrier(comm)
            gathered = _mpi_gather(comm, local_results, root=0)
            if is_root and isinstance(local_results, dict):
                merged = {
                    "requests_completed": 0,
                    "requests_scheduled": 0,
                    "errors": 0,
                    "total_input_tokens": 0,
                    "total_output_tokens": 0,
                    "p50_s": 0.0,
                    "p99_s": 0.0,
                }
                for item in gathered:
                    for key in ("requests_completed", "requests_scheduled", "errors", "total_input_tokens", "total_output_tokens"):
                        merged[key] += item.get(key, 0)
                    merged["p50_s"] = max(merged["p50_s"], item.get("p50_s", 0.0))
                    merged["p99_s"] = max(merged["p99_s"], item.get("p99_s", 0.0))
                run_results = merged
            elif is_root:
                run_results = [item for rank_results in gathered for item in rank_results]
                if target_responses is not None and len(run_results) > target_responses:
                    run_results = run_results[:target_responses]
            else:
                run_results = []
            all_runs_results.append(run_results)
            duration_t0 = effective_run_t0 if effective_run_t0 is not None else run_t0
            if is_root:
                if isinstance(run_results, dict):
                    run_durations.append(max(time.time() - duration_t0, 0.0))
                else:
                    end_time = max((item[4] for item in run_results), default=time.time())
                    run_durations.append(max(end_time - duration_t0, 0.0))
            if run_index < num_runs - 1:
                await asyncio.sleep(10)
        if is_root:
            _save_results(
                exp_config,
                requests,
                all_runs_results[-1] if all_runs_results else [],
                all_runs_results,
                run_durations,
                dispatch_timings,
                num_runs,
                generation_mode,
                dest,
                cluster_nodes,
                mpi_size,
                num_go_procs,
                num_go_workers,
                go_concurrency,
                warmup_rps,
                warmup_duration_s,
                t0,
            )
    finally:
        signal.signal(signal.SIGINT, old_handler)
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _save_results(
    exp_config: EvalManifest,
    requests: list[TraceRequest],
    results,
    all_runs_results,
    run_durations,
    dispatch_timings,
    num_runs,
    generation_mode,
    dest,
    cluster_nodes,
    mpi_size,
    num_go_procs,
    num_go_workers,
    go_concurrency,
    warmup_rps,
    warmup_duration_s,
    t0,
) -> None:
    result_dir = _result_dir(exp_config)
    final_save_path = _next_result_path(result_dir)
    config_dict = exp_config.to_yaml_dict()
    meta = {
        "num_runs": num_runs,
        "completed_runs": len(all_runs_results),
        "generation_mode": generation_mode,
        "dest": dest,
        "cluster_nodes": cluster_nodes,
        "mpi_size": mpi_size,
        "num_go_procs": num_go_procs,
        "num_go_workers": num_go_workers,
        "go_concurrency": go_concurrency,
        "warmup_rps": warmup_rps,
        "warmup_duration_s": warmup_duration_s,
        "dispatch_timings": dispatch_timings,
    }
    duration = run_durations[-1] if run_durations else max(time.time() - t0, 1e-6)
    duration = max(duration, 1e-6)

    if isinstance(results, dict):
        completed_requests = results.get("requests_completed", 0)
        total_input_tokens = results.get("total_input_tokens", 0)
        total_output_tokens = results.get("total_output_tokens", 0)
        payload = {
            "config": config_dict,
            "meta": dict(meta, sum_only=True),
            "overall": {
                "duration_s": duration,
                "rps": completed_requests / duration,
                "processed_tps": total_input_tokens / duration,
                "generated_tps": total_output_tokens / duration,
                "tps": (total_input_tokens + total_output_tokens) / duration,
                "total_tokens": total_input_tokens + total_output_tokens,
                "total_input_tokens": total_input_tokens,
                "total_output_tokens": total_output_tokens,
                "requests_completed": completed_requests,
                "requests_scheduled": results.get("requests_scheduled", len(requests)),
                "errors": results.get("errors", 0),
                "p50_s": results.get("p50_s", 0.0),
                "p99_s": results.get("p99_s", 0.0),
            },
        }
    else:
        raw_results = []
        model_groups = {}
        total_input_tokens = 0
        total_output_tokens = 0
        usage_count = 0
        trace_count = 0
        successful_latencies = []
        for run_index, run_results in enumerate(all_runs_results):
            for item in run_results:
                request, latency, success, error_msg, _end_time, actual_prompt_tokens, actual_completion_tokens = item
                raw_results.append(
                    {
                        "run_index": run_index,
                        "model": request.model,
                        "latency": latency,
                        "success": success,
                        "error": error_msg,
                        "input_len": request.input_len,
                        "output_len": request.output_len,
                        "actual_prompt_tokens": actual_prompt_tokens,
                        "actual_completion_tokens": actual_completion_tokens,
                        "tensor_parallel_size": request.tensor_parallel_size,
                        "req_id": request.req_id,
                    }
                )
        for item in results:
            request, latency, success, error_msg, _end_time, actual_prompt_tokens, actual_completion_tokens = item
            model_groups.setdefault(request.model, []).append(item)
            if success:
                successful_latencies.append(float(latency))
                if actual_prompt_tokens is not None and actual_completion_tokens is not None:
                    total_input_tokens += int(actual_prompt_tokens)
                    total_output_tokens += int(actual_completion_tokens)
                    usage_count += 1
                else:
                    total_input_tokens += int(request.input_len)
                    total_output_tokens += int(request.output_len)
                    trace_count += 1

        per_model = {}
        for model_name, model_rows in model_groups.items():
            latencies = [float(item[1]) for item in model_rows if item[2]]
            per_model[model_name] = {
                "count": len(model_rows),
                "errors": len(model_rows) - len(latencies),
                "p50_s": (_percentile(latencies, 0.50) if latencies else None),
                "p99_s": (_percentile(latencies, 0.99) if latencies else None),
            }

        payload = {
            "config": config_dict,
            "meta": dict(
                meta,
                token_counts_from_usage_api=usage_count,
                token_counts_from_trace_spec=trace_count,
            ),
            "summary": {model_name: len(rows) for model_name, rows in model_groups.items()},
            "per_model": per_model,
            "overall": {
                "duration_s": duration,
                "rps": len(results) / duration,
                "processed_tps": total_input_tokens / duration,
                "generated_tps": total_output_tokens / duration,
                "tps": (total_input_tokens + total_output_tokens) / duration,
                "total_tokens": total_input_tokens + total_output_tokens,
                "total_input_tokens": total_input_tokens,
                "total_output_tokens": total_output_tokens,
                "requests_completed": len(results),
                "requests_scheduled": len(requests),
                "errors": sum(item["errors"] for item in per_model.values()),
                "p50_s": (_percentile(successful_latencies, 0.50) if successful_latencies else None),
                "p99_s": (_percentile(successful_latencies, 0.99) if successful_latencies else None),
            },
            "requests": raw_results,
        }
    with open(final_save_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    print(f">>> [REPLAY] Saved results to {final_save_path}", flush=True)
