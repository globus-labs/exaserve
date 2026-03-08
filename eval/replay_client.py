import argparse
import asyncio
import glob
import json
import os
import pathlib
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass


def _inject_litellm_site_packages() -> None:
    """
    Peek at --config <path> in sys.argv, extract proxy_config.python_path
    from the YAML (via regex, no yaml import yet), and prepend the litellm
    venv's site-packages to sys.path so that httpx[http2] and its h2
    dependency are importable.
    """
    config_path = None
    for i, arg in enumerate(sys.argv):
        if arg == "--config" and i + 1 < len(sys.argv):
            config_path = sys.argv[i + 1]
            break
    if not config_path:
        return
    try:
        with open(config_path) as f:
            content = f.read()
        m = re.search(r"python_path\s*:\s*['\"]?([^'\"\n]+)['\"]?", content)
        if not m:
            return
        python_bin = m.group(1).strip()
        venv_root = os.path.dirname(os.path.dirname(python_bin))
        for sp in glob.glob(os.path.join(venv_root, "lib", "python3.*", "site-packages")):
            if sp not in sys.path:
                sys.path.insert(0, sp)
                print(f"[bootstrap] Injected litellm site-packages: {sp}", flush=True)
    except Exception as exc:
        print(f"[bootstrap] Could not inject litellm site-packages: {exc}", flush=True)


_inject_litellm_site_packages()

import numpy as np
import yaml

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Optional MPI support via mpi4py.
# When the script is launched with mpiexec/mpirun (num_nodes > 1) each OS
# process is one MPI rank (one compute node). Go dispatch processes are
# spawned *within* each rank.
# Falls back transparently to single-node mode when mpi4py is absent or
# MPI_SIZE == 1.
# ---------------------------------------------------------------------------
try:
    from mpi4py import MPI as _MPI
    _MPI_AVAILABLE = True
except ImportError:
    _MPI_AVAILABLE = False


def _init_mpi():
    """Return (comm, rank, size). Falls back to (None, 0, 1) without MPI."""
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
    """Gather values from all ranks to root. Returns list on root, None elsewhere."""
    if comm is not None and comm.Get_size() > 1:
        return comm.gather(value, root=root)
    return [value]


@dataclass
class TraceRequest:
    timestamp: float
    model: str
    prompt: str
    input_len: int
    output_len: int
    tensor_parallel_size: int
    req_id: str
    mode: str = "chat"  # from trace file modes distribution, or config default

TIMEOUT_S = 3600 # 1 hour

_RESULT_PATTERN = re.compile(r"result(\d+)\.json$")


def _get_cluster_nodes() -> list:
    """Return a sorted, deduplicated list of hostnames from $PBS_NODEFILE."""
    nodefile = os.environ.get("PBS_NODEFILE")
    if not nodefile:
        raise RuntimeError(
            "PBS_NODEFILE environment variable is not set. "
            "--dest=direct requires a PBS job environment."
        )
    try:
        with open(nodefile) as f:
            nodes = sorted(set(line.strip() for line in f if line.strip()))
    except FileNotFoundError:
        raise RuntimeError(
            f"PBS_NODEFILE points to '{nodefile}' which does not exist."
        )
    if not nodes:
        raise RuntimeError(f"No nodes found in PBS_NODEFILE ('{nodefile}').")
    return nodes


def _next_result_path(result_dir: str) -> str:
    """Return path for next result file (result0.json, result1.json, ...)."""
    os.makedirs(result_dir, exist_ok=True)
    candidates = glob.glob(os.path.join(result_dir, "result*.json"))
    indices = []
    for p in candidates:
        m = _RESULT_PATTERN.search(os.path.basename(p))
        if m:
            indices.append(int(m.group(1)))
    next_idx = max(indices) + 1 if indices else 0
    return os.path.join(result_dir, f"result{next_idx}.json")



def _config_trace_path(cfg: dict) -> str:
    jtc = cfg.get('job_trace_config') or {}
    return jtc.get('output_trace_path', 'experiment_trace.jsonl')

def _config_result_dir(cfg: dict):
    return cfg.get('pbs_result_dir')


# ==============================================================================
# GO DISPATCH HELPERS
# These functions drive the Go HTTP dispatch binary (eval/go_client/go_dispatch).
# ==============================================================================

def _find_go_binary() -> str | None:
    """Return path to go_dispatch binary, or None if not found."""
    script_dir = pathlib.Path(__file__).parent
    candidates = [
        script_dir / "go_client" / "bin" / "go_dispatch",
        script_dir.parent / "eval" / "go_client" / "bin" / "go_dispatch",
    ]
    for p in candidates:
        if p.is_file() and os.access(p, os.X_OK):
            return str(p)
    return None


def _write_trace_partition(requests: list, path: str) -> None:
    """Serialise a list of TraceRequest objects to a JSONL file for go_dispatch."""
    with open(path, "w", encoding="utf-8") as f:
        for req in requests:
            record = {
                "timestamp": req.timestamp,
                "model": req.model,
                "mode": getattr(req, "mode", "chat"),
                "prompt": req.prompt,
                "input_len": req.input_len,
                "output_len": req.output_len,
                "tensor_parallel_size": req.tensor_parallel_size,
                "req_id": req.req_id,
            }
            f.write(json.dumps(record) + "\n")


def _spawn_go_procs(
    go_bin: str,
    base_urls: list,
    rank_requests: list,
    generation_mode: str,
    include_tp: bool,
    concurrency: int,
    num_go_workers: int,
    rank: int,
    tmp_dir: str,
    sum_only: bool = False,
    num_go_procs: int = 1,
    warmup_rps: int = 0,
    warmup_duration_s: float = 0,
) -> tuple[list, list, dict]:
    """
    Spawn Go processes, write trace partitions, and wait for GO_CLI_READY.

    Returns (procs, result_paths, req_map) where procs is a list of
    (p_idx, subprocess.Popen) tuples with stdin/stdout/stderr pipes.
    All processes have completed warm-up and are waiting for run_t0 on stdin.
    """
    N = max(1, num_go_procs)
    req_map = {req.req_id: req for req in rank_requests}
    partitions = [rank_requests[i::N] for i in range(N)]

    procs = []
    result_paths = []
    for p_idx in range(N):
        trace_path = os.path.join(tmp_dir, f"rank{rank}_p{p_idx}_trace.jsonl")
        result_path = os.path.join(tmp_dir, f"rank{rank}_p{p_idx}_results.jsonl")
        result_paths.append(result_path)

        _write_trace_partition(partitions[p_idx], trace_path)

        cmd = [
            go_bin,
            "--base-urls", ",".join(base_urls),
            "--generation-mode", generation_mode,
            "--timeout", str(TIMEOUT_S),
            "--concurrency", str(concurrency),
            "--num-go-workers", str(num_go_workers),
            "--worker-id", f"rank{rank}_p{p_idx}",
            "--trace-file", trace_path,
            "--result-file", result_path,
        ]
        if warmup_rps > 0 and warmup_duration_s > 0:
            cmd.extend(["--warmup-rps", str(warmup_rps),
                        "--warmup-duration", str(warmup_duration_s)])
        if sum_only:
            cmd.append("--sum-only")
        if include_tp:
            cmd.append("--include-tp")

        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        procs.append((p_idx, proc))

    print(f"[go_dispatch rank {rank}] Spawned {N} Go process(es) "
          f"(concurrency={concurrency}, num_go_workers={num_go_workers})", flush=True)

    # Wait for all Go processes to signal readiness (after warm-up)
    for p_idx, proc in procs:
        line = proc.stdout.readline().decode(errors="replace").strip()
        if line != "GO_CLI_READY":
            print(f"!!! [go_dispatch rank {rank} p{p_idx}] Expected GO_CLI_READY, got: {line!r}",
                  flush=True)

    print(f"[go_dispatch rank {rank}] All {N} Go process(es) ready", flush=True)
    return procs, result_paths, req_map


def _send_run_t0_and_wait(
    procs: list,
    run_t0: float,
    result_paths: list,
    req_map: dict,
    interrupt_event,
    rank: int,
    sum_only: bool = False,
) -> tuple:
    """
    Send run_t0 to all Go processes via stdin, wait for completion,
    and merge results.

    Returns (all_results, max_last_fire_time, run_t0).
    """
    N = len(procs)

    # Send run_t0 to each Go process
    for p_idx, proc in procs:
        proc.stdin.write(f"{run_t0!r}\n".encode())
        proc.stdin.flush()
        proc.stdin.close()

    # Poll until all subprocesses finish, forwarding interrupts
    alive = set(range(N))
    while alive:
        for p_idx, proc in procs:
            if p_idx not in alive:
                continue
            try:
                proc.wait(timeout=0.5)
                alive.discard(p_idx)
            except subprocess.TimeoutExpired:
                pass
        if interrupt_event is not None and interrupt_event.is_set():
            for p_idx, proc in procs:
                if p_idx in alive:
                    proc.send_signal(signal.SIGTERM)
            for p_idx, proc in procs:
                if p_idx in alive:
                    try:
                        proc.wait(timeout=5.0)
                    except subprocess.TimeoutExpired:
                        proc.kill()
            break

    for p_idx, proc in procs:
        stderr_out = proc.stderr.read().decode(errors="replace")
        for line in stderr_out.splitlines():
            print(f"[go_dispatch rank {rank} p{p_idx}] {line}", flush=True)
        if proc.returncode not in (0, None, -signal.SIGTERM):
            print(
                f"!!! [go_dispatch rank {rank} p{p_idx}] exited with code {proc.returncode}",
                flush=True,
            )

    # Merge results from all Go processes
    all_results = []
    max_last_fire_time = 0.0

    for result_path in result_paths:
        results, lft, _ = _read_go_results(result_path, req_map)
        if lft > max_last_fire_time:
            max_last_fire_time = lft

        if sum_only and isinstance(results, dict):
            if not all_results:
                all_results = results
            else:
                all_results["requests_completed"] = all_results.get("requests_completed", 0) + results.get("requests_completed", 0)
                all_results["requests_scheduled"] = all_results.get("requests_scheduled", 0) + results.get("requests_scheduled", 0)
                all_results["errors"] = all_results.get("errors", 0) + results.get("errors", 0)
                all_results["total_input_tokens"] = all_results.get("total_input_tokens", 0) + results.get("total_input_tokens", 0)
                all_results["total_output_tokens"] = all_results.get("total_output_tokens", 0) + results.get("total_output_tokens", 0)
                all_results["p50_s"] = max(all_results.get("p50_s", 0), results.get("p50_s", 0))
                all_results["p99_s"] = max(all_results.get("p99_s", 0), results.get("p99_s", 0))
        else:
            all_results.extend(results)

    if sum_only and isinstance(all_results, dict):
        all_results["last_fire_time"] = max_last_fire_time

    return all_results, max_last_fire_time, run_t0


def _read_go_results(result_path: str, req_map: dict) -> tuple:
    """
    Parse go_dispatch JSONL results.

    When go_dispatch was run with --sum-only, the file contains a single
    summary JSON line with __type__: "summary".  Returns:
        (summary_dict, last_fire_time, adjusted_run_t0_or_None)

    Otherwise returns the per-request 7-tuples:
        (list_of_tuples, last_fire_time, adjusted_run_t0_or_None)
    """
    results = []
    last_fire_time = 0.0
    adjusted_run_t0 = None

    if not os.path.isfile(result_path):
        print(f"!!! [go_dispatch] result file not found: {result_path}", flush=True)
        return results, last_fire_time, adjusted_run_t0

    with open(result_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"!!! [go_dispatch] malformed result line: {e}", flush=True)
                continue

            if rec.get("__type__") == "summary":
                return (
                    rec,
                    rec.get("last_fire_time", 0.0),
                    rec.get("adjusted_run_t0"),
                )

            if rec.get("__type__") == "dispatch_done":
                last_fire_time = rec.get("last_fire_time", last_fire_time)
                if "adjusted_run_t0" in rec:
                    adjusted_run_t0 = rec["adjusted_run_t0"]
                continue

            req_id = rec.get("req_id", "")
            req_obj = req_map.get(req_id)
            if req_obj is None:
                continue

            latency = rec.get("latency", 0.0)
            success = rec.get("success", False)
            error_msg = rec.get("error", "")
            end_time = rec.get("end_time", 0.0)
            apt = rec.get("actual_prompt_tokens")
            act = rec.get("actual_completion_tokens")

            results.append((req_obj, latency, success, error_msg, end_time, apt, act))

    return results, last_fire_time, adjusted_run_t0



# ==============================================================================
# MAIN REPLAY FUNCTION
# ==============================================================================

async def replay(
    config_path,
    include_tp: bool,
    early_stop: float,
    num_runs: int,
    dest: str = "proxy",
    proxy_port: int = None,    # None = auto-detect from port file or config
    base_urls_override: str = None,  # comma-separated URLs, overrides port logic
):
    # ------------------------------------------------------------------
    # 1. MPI init (no-op when running without mpiexec)
    # ------------------------------------------------------------------
    comm, rank, mpi_size = _init_mpi()
    is_root = (rank == 0)

    # ------------------------------------------------------------------
    # 2. Load Config
    # ------------------------------------------------------------------
    with open(config_path, 'r') as f:
        cfg = yaml.safe_load(f) or {}
    trace_path = _config_trace_path(cfg)
    proxy_cfg = cfg.get('proxy_config', {})
    proxy_type = proxy_cfg.get('type', 'none')

    # Port resolution priority:
    #   1. proxy_port argument (--proxy-port CLI flag)
    #   2. proxy_out/proxy_port file written by driver.py at runtime
    #      (captures the actual bound port, which may differ from config)
    #   3. proxy_config.port from the YAML config (static fallback)
    #   4. Default 8000 (no proxy)
    if proxy_port is not None:
        port = proxy_port
        if is_root:
            print(f">>> [REPLAY] Port from --proxy-port override: {port}")
    elif proxy_type != 'none':
        proxy_port_file = os.path.join(
            os.path.dirname(os.path.abspath(config_path)), "proxy_out", "proxy_port"
        )
        if os.path.isfile(proxy_port_file):
            try:
                port = int(open(proxy_port_file).read().strip())
                if is_root:
                    print(f">>> [REPLAY] Port from proxy_port file: {port}")
            except (ValueError, OSError):
                port = proxy_cfg.get('port', 4001)
        else:
            port = proxy_cfg.get('port', 4001)
    else:
        port = cfg.get('port', 8000)

    replay_cfg = cfg.get('job_replay_client_config', {})
    generation_mode = replay_cfg.get('generation_mode', 'deterministic')
    if generation_mode not in ['deterministic', 'natural']:
        if is_root:
            print(f"!!! WARNING: Invalid generation_mode '{generation_mode}', defaulting to 'deterministic'")
        generation_mode = 'deterministic'

    # Go dispatch config
    num_go_procs = replay_cfg.get('num_go_procs', 1)
    num_go_workers = replay_cfg.get('num_go_workers', 4)
    go_concurrency = replay_cfg.get('go_concurrency', 2000)
    go_sum_only = replay_cfg.get('sum_only', False)
    warmup_rps = replay_cfg.get('warmup_rps', 0)
    warmup_duration_s = replay_cfg.get('warmup_duration_s', 0)

    if is_root:
        print(
            f">>> [REPLAY] MPI size: {mpi_size} | "
            f"num_go_procs: {num_go_procs} | "
            f"num_go_workers: {num_go_workers} | "
            f"go_concurrency: {go_concurrency}"
        )
        if warmup_rps > 0 and warmup_duration_s > 0:
            print(f">>> [REPLAY] Warm-up: {warmup_rps} RPS × {warmup_duration_s}s")

    # ------------------------------------------------------------------
    # 3. Build target URL list
    # ------------------------------------------------------------------
    if base_urls_override:
        cluster_nodes = []
        base_urls = [u.strip() for u in base_urls_override.split(",")]
        if is_root:
            print(f">>> [DEST] base-urls override — {len(base_urls)} target(s): {base_urls}")
    elif dest == "direct":
        cluster_nodes = _get_cluster_nodes()
        base_urls = [f"http://{node}:{port}" for node in cluster_nodes]
        if is_root:
            print(f">>> [DEST] direct mode — {len(base_urls)} node(s): {cluster_nodes}")
    else:
        cluster_nodes = []
        base_urls = [f"http://0.0.0.0:{port}"]
        if is_root:
            print(f">>> [DEST] proxy mode — target: {base_urls[0]}")

    base_url = base_urls[0]

    # ------------------------------------------------------------------
    # 4. Load Trace (all ranks load independently; avoids large MPI transfers)
    # ------------------------------------------------------------------
    if is_root:
        print(f">>> [REPLAY] Loading trace from {trace_path}...")
    _trace_load_t0 = time.time()
    requests = []
    try:
        with open(trace_path, 'r', encoding='utf-8') as f:
            for line in f:
                data = json.loads(line)
                if data.get("__type__") == "metadata":
                    if is_root:
                        print(f">>> [REPLAY] Found trace metadata (generated {data.get('timestamp')})")
                    continue
                requests.append(TraceRequest(
                    timestamp=data['timestamp'],
                    model=data['model'],
                    prompt=data['prompt'],
                    input_len=data.get('input_len', 0),
                    output_len=data['output_len'],
                    tensor_parallel_size=data.get('tensor_parallel_size', 1),
                    req_id=uuid.uuid4().hex,
                    mode=data.get('mode', 'chat'),
                ))
    except FileNotFoundError:
        print(f"!!! ERROR [Rank {rank}]: Trace file {trace_path} not found.")
        return
    if is_root:
        _trace_load_elapsed = time.time() - _trace_load_t0
        print(f">>> [REPLAY] Loaded {len(requests)} requests in {_trace_load_elapsed:.2f}s")

    # ------------------------------------------------------------------
    # 5. Go binary detection and request partitioning
    # ------------------------------------------------------------------
    go_bin = _find_go_binary()
    if go_bin is None:
        print("!!! ERROR: Go binary (go_dispatch) not found. Build it with: "
              "cd eval/go_client && go build -o bin/go_dispatch .")
        return

    rank_requests = requests[rank::mpi_size]
    if is_root:
        print(f">>> [DISPATCH] Go binary: {go_bin}")

    # ------------------------------------------------------------------
    # 6. Early stop
    # ------------------------------------------------------------------
    target_responses = None
    if early_stop > 0:
        target_responses = int(len(requests) * early_stop)
        if is_root:
            print(
                f">>> [EARLY STOP] Will stop after {target_responses}/{len(requests)} "
                f"total responses ({early_stop*100:.1f}%)"
            )

    if is_root:
        print(f">>> [REPLAY] Target: {base_url}")
        print(f"    Requests: {len(requests)}")
        print(f"    Duration: {requests[-1].timestamp:.2f}s")
        print(f"    This rank ({rank}) handles {len(rank_requests)} / {len(requests)} requests")
        print(f">>> [REPLAY] Generation Mode: {generation_mode.upper()}")
        if generation_mode == "deterministic":
            print(f"    Using min_tokens=max_tokens={requests[0].output_len if requests else 'N/A'} "
                  "with ignore_eos=True")
        else:
            print("    Using natural generation with EOS termination.")

    # ------------------------------------------------------------------
    # 7. Signal handler (each rank handles SIGINT independently)
    # ------------------------------------------------------------------
    interrupted = False
    interrupt_event = threading.Event()

    def signal_handler(sig, frame):
        nonlocal interrupted
        interrupted = True
        interrupt_event.set()

    old_handler = signal.signal(signal.SIGINT, signal_handler)

    # ------------------------------------------------------------------
    # 8. Resource management
    # ------------------------------------------------------------------
    t0 = time.time()

    results = []
    all_runs_results = []
    run_durations = []
    tmp_dir = tempfile.mkdtemp(prefix=f"replay_rank{rank}_")

    try:
        loop = asyncio.get_running_loop()

        # ==============================================================================
        # WARM-UP PHASE (first run only)
        # Spawn Go processes with warm-up flags. They warm up and signal
        # GO_CLI_READY, then we send run_t0 and they do the main dispatch.
        # ==============================================================================
        all_runs_results = []
        run_durations = []
        dispatch_timings = []

        for run_idx in range(num_runs):
            _mpi_barrier(comm)
            if interrupted:
                if is_root:
                    print(f"\n>>> [INTERRUPTED] Skipping run {run_idx + 1}.")
                break

            if is_root:
                print("\n" + "=" * 70)
                print(f">>> [RUN {run_idx + 1}/{num_runs}] Starting main experiment replay...")
                print("=" * 70)

            # Warm-up only on the first run
            run_warmup_rps = warmup_rps if run_idx == 0 else 0
            run_warmup_dur = warmup_duration_s if run_idx == 0 else 0

            # ---- Spawn Go processes (warm-up + wait for GO_CLI_READY) ----
            go_procs, result_paths, req_map = await loop.run_in_executor(
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
                go_sum_only,
                num_go_procs,
                run_warmup_rps,
                run_warmup_dur,
            )

            # All ranks' Go processes are warmed up — synchronise before setting run_t0
            _mpi_barrier(comm)

            # ---- Synchronised start time ----
            if is_root:
                run_t0 = time.time()
            else:
                run_t0 = None
            run_t0 = _mpi_bcast(comm, run_t0, root=0)

            # ---- Per-run interrupt event ----
            run_interrupt_event = threading.Event()

            # ---- Send run_t0 to Go processes and wait for completion ----
            local_expected = len(rank_requests)
            print(
                f"[Rank {rank}] Sending run_t0 to go_dispatch for {local_expected} requests...",
                flush=True,
            )

            local_results, last_fire_time, effective_run_t0 = await loop.run_in_executor(
                None,
                _send_run_t0_and_wait,
                go_procs,
                run_t0,
                result_paths,
                req_map,
                run_interrupt_event,
                rank,
                go_sum_only,
            )

            if isinstance(local_results, dict):
                print(
                    f"\r[Rank {rank}] go_dispatch finished (summary): "
                    f"completed={local_results.get('requests_completed')}/{local_expected} "
                    f"errors={local_results.get('errors')}",
                    flush=True,
                )
            else:
                print(
                    f"\r[Rank {rank}] go_dispatch finished: "
                    f"{len(local_results)}/{local_expected} responses.",
                    flush=True,
                )

            # ---- Dispatch timing report (rank 0 only) ----
            if is_root and last_fire_time > 0:
                actual_dispatch_s = last_fire_time - effective_run_t0
                trace_span = requests[-1].timestamp if requests else 0.0
                overhead = actual_dispatch_s - trace_span
                print(
                    f">>> [DISPATCH] Trace span: {trace_span:.3f}s | "
                    f"Actual send window: {actual_dispatch_s:.3f}s | "
                    f"Overhead: {overhead:+.3f}s"
                )
                dispatch_timings.append({
                    "run_index": run_idx,
                    "trace_span_s": trace_span,
                    "actual_dispatch_s": actual_dispatch_s,
                    "overhead_s": overhead,
                })

            # ---- Barrier: wait for all ranks to finish dispatching ----
            _mpi_barrier(comm)

            # ---- Gather all results to rank 0 ----
            is_summary = isinstance(local_results, dict)
            gathered = _mpi_gather(comm, local_results, root=0)

            if is_root and is_summary:
                # Merge summary dicts from all ranks
                merged_summary = {
                    "requests_completed": 0, "requests_scheduled": 0,
                    "errors": 0, "total_input_tokens": 0, "total_output_tokens": 0,
                }
                all_latency_p50s = []
                all_latency_p99s = []
                for s in gathered:
                    merged_summary["requests_completed"] += s.get("requests_completed", 0)
                    merged_summary["requests_scheduled"] += s.get("requests_scheduled", 0)
                    merged_summary["errors"] += s.get("errors", 0)
                    merged_summary["total_input_tokens"] += s.get("total_input_tokens", 0)
                    merged_summary["total_output_tokens"] += s.get("total_output_tokens", 0)
                    if s.get("p50_s"):
                        all_latency_p50s.append(s["p50_s"])
                    if s.get("p99_s"):
                        all_latency_p99s.append(s["p99_s"])
                # For multi-rank, percentiles are approximate (max of per-rank values)
                merged_summary["p50_s"] = max(all_latency_p50s) if all_latency_p50s else 0.0
                merged_summary["p99_s"] = max(all_latency_p99s) if all_latency_p99s else 0.0
                run_results = merged_summary
            elif is_root:
                run_results = [r for rank_results in gathered for r in rank_results]
                # Early stop: trim to target if we over-collected
                if target_responses is not None and len(run_results) > target_responses:
                    run_results = run_results[:target_responses]
                    print(f"\n>>> [EARLY STOP] Trimmed to {target_responses} responses.")
            else:
                run_results = []   # non-root ranks don't process results

            all_runs_results.append(run_results)

            # Track run duration using effective_run_t0 (which accounts for
            # Go startup compensation) so that RPS = completed / duration
            # isn't diluted by go_dispatch's startup time.
            duration_t0 = effective_run_t0 if effective_run_t0 is not None else run_t0
            if is_root and run_results:
                if isinstance(run_results, dict):
                    # Summary mode: no per-request end_time, use wall clock
                    run_durations.append(max(time.time() - duration_t0, 0.0))
                else:
                    run_end_time = max((r[4] for r in run_results), default=None)
                    run_durations.append(
                        run_end_time - duration_t0 if run_end_time is not None
                        else max(time.time() - duration_t0, 0.0)
                    )
            elif is_root:
                run_durations.append(max(time.time() - duration_t0, 0.0))

            if interrupted:
                break

            if run_idx < num_runs - 1:
                if is_root:
                    print(f">>> [RUN {run_idx + 1}] Completed. Resting 10s before next run...")
                await asyncio.sleep(10)
                _mpi_barrier(comm)

        results = all_runs_results[-1] if all_runs_results else []

    except Exception as e:
        print(f"\n\n!!! [ERROR] Rank {rank} unexpected error: {e}")
        import traceback
        traceback.print_exc()
        results = []
        if not all_runs_results:
            all_runs_results = [results]

    finally:
        signal.signal(signal.SIGINT, old_handler)
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 9. Analysis & Save (rank 0 only)
    # ------------------------------------------------------------------
    if not is_root:
        return

    # Detect summary mode (Go --sum-only): results is a dict instead of list
    is_summary_mode = isinstance(results, dict)

    if is_summary_mode:
        # ------------------------------------------------------------------
        # Summary-only path: stats already computed by Go client
        # ------------------------------------------------------------------
        total_duration = run_durations[-1] if run_durations else max(time.time() - t0, 0.0)
        duration_for_rate = max(total_duration, 1e-6)

        completed_requests = results.get("requests_completed", 0)
        scheduled_requests = results.get("requests_scheduled", len(requests))
        errors = results.get("errors", 0)
        p50 = results.get("p50_s", 0.0)
        p99 = results.get("p99_s", 0.0)
        total_input_tokens = results.get("total_input_tokens", 0)
        total_output_tokens = results.get("total_output_tokens", 0)
        total_tokens = total_input_tokens + total_output_tokens

        rps = completed_requests / duration_for_rate
        tps = total_tokens / duration_for_rate
        processed_tps = total_input_tokens / duration_for_rate
        generated_tps = total_output_tokens / duration_for_rate

        print("\n" + "=" * 70)
        print(f"{'(summary)':<45} | {completed_requests:<5} | {p50:.4f}   | {p99:.4f}   | {errors}")
        print("=" * 70)
        print(
            f"System duration: {total_duration:.2f}s | RPS: {rps:.2f} | TPS: {tps:.2f} "
            f"(Processed: {processed_tps:.2f}, Generated: {generated_tps:.2f}) | "
            f"Completed: {completed_requests}/{scheduled_requests}"
        )

        config_result_dir = _config_result_dir(cfg)
        final_save_path = None
        if config_result_dir:
            final_save_path = _next_result_path(config_result_dir)

        if final_save_path:
            print(f">>> [REPLAY] Saving summary results to {final_save_path}")
            _save_t0 = time.time()
            try:
                with open(final_save_path, 'w') as f:
                    json.dump({
                        "config": cfg,
                        "meta": {
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
                            "sum_only": True,
                        },
                        "overall": {
                            "duration_s": total_duration,
                            "rps": rps,
                            "tps": tps,
                            "processed_tps": processed_tps,
                            "generated_tps": generated_tps,
                            "total_tokens": total_tokens,
                            "total_input_tokens": total_input_tokens,
                            "total_output_tokens": total_output_tokens,
                            "requests_completed": completed_requests,
                            "requests_scheduled": scheduled_requests,
                            "errors": errors,
                            "p50_s": p50,
                            "p99_s": p99,
                            "trace_span_s": dispatch_timings[-1]["trace_span_s"] if dispatch_timings else None,
                            "actual_dispatch_s": dispatch_timings[-1]["actual_dispatch_s"] if dispatch_timings else None,
                            "dispatch_overhead_s": dispatch_timings[-1]["overhead_s"] if dispatch_timings else None,
                        },
                    }, f, indent=2)
                _save_elapsed = time.time() - _save_t0
                _save_mb = os.path.getsize(final_save_path) / (1024 * 1024)
                print(f">>> [REPLAY] Save complete: {_save_mb:.1f} MB in {_save_elapsed:.2f}s")
            except Exception as e:
                print(f"!!! ERROR Saving results: {e}")

    else:
        # ------------------------------------------------------------------
        # Full per-request path (original behavior)
        # ------------------------------------------------------------------
        if interrupted:
            print(
                f"\n>>> [INTERRUPTED] Collected {len(results)} completed responses "
                f"out of {len(requests)} scheduled requests in last run."
            )

        print("\n" + "=" * 70)
        print(f"{'MODEL':<45} | {'CNT':<5} | {'P50 (s)':<8} | {'P99 (s)':<8} | {'ERR'}")
        print("-" * 70)

        model_stats = {}
        raw_results = []

        for run_idx, run_results_item in enumerate(all_runs_results):
            for r in run_results_item:
                req_obj, latency, success, error_msg, end_time, actual_prompt_tokens, actual_completion_tokens = r
                raw_results.append({
                    "run_index": run_idx,
                    "model": req_obj.model,
                    "latency": latency,
                    "success": success,
                    "error": error_msg,
                    "input_len": req_obj.input_len,
                    "output_len": req_obj.output_len,
                    "actual_prompt_tokens": actual_prompt_tokens,
                    "actual_completion_tokens": actual_completion_tokens,
                    "tensor_parallel_size": req_obj.tensor_parallel_size,
                    "req_id": req_obj.req_id
                })

        for r in results:
            req_obj, latency, success, error_msg, end_time, actual_prompt_tokens, actual_completion_tokens = r
            m_name = req_obj.model
            if m_name not in model_stats:
                model_stats[m_name] = []
            model_stats[m_name].append((req_obj, latency, success, error_msg, end_time,
                                        actual_prompt_tokens, actual_completion_tokens))

        per_model = {}
        for m_name, stats in model_stats.items():
            succ_lats = [x[1] for x in stats if x[2]]
            fails = len(stats) - len(succ_lats)
            if succ_lats:
                p50 = float(np.percentile(succ_lats, 50))
                p99 = float(np.percentile(succ_lats, 99))
                print(f"{m_name:<45} | {len(stats):<5} | {p50:.4f}   | {p99:.4f}   | {fails}")
            else:
                p50 = p99 = None
                print(f"{m_name:<45} | {len(stats):<5} | N/A        | N/A        | {fails}")
            per_model[m_name] = {
                "count": len(stats),
                "errors": fails,
                "p50_s": p50,
                "p99_s": p99,
            }

        print("=" * 70)

        total_duration = run_durations[-1] if run_durations else max(time.time() - t0, 0.0)
        duration_for_rate = max(total_duration, 1e-6)

        actual_count = 0
        trace_count = 0
        total_input_tokens = 0
        total_output_tokens = 0

        for r in results:
            if r[2]:
                req_obj = r[0]
                apt = r[5]
                act = r[6]
                if apt is not None and act is not None:
                    total_input_tokens += apt
                    total_output_tokens += act
                    actual_count += 1
                else:
                    total_input_tokens += req_obj.input_len
                    total_output_tokens += req_obj.output_len
                    trace_count += 1

        total_tokens = total_input_tokens + total_output_tokens
        completed_requests = len(results)
        scheduled_requests = len(requests)

        rps = completed_requests / duration_for_rate
        tps = total_tokens / duration_for_rate
        processed_tps = total_input_tokens / duration_for_rate
        generated_tps = total_output_tokens / duration_for_rate

        token_source = f"(Usage API: {actual_count}, Trace Spec: {trace_count})" if completed_requests > 0 else ""

        print(
            f"System duration: {total_duration:.2f}s | RPS: {rps:.2f} | TPS: {tps:.2f} "
            f"(Processed: {processed_tps:.2f}, Generated: {generated_tps:.2f}) | "
            f"Completed: {completed_requests}/{scheduled_requests}"
        )
        if token_source:
            print(f"Token counts from {token_source}")

        config_result_dir = _config_result_dir(cfg)
        final_save_path = None
        if config_result_dir:
            final_save_path = _next_result_path(config_result_dir)

        if final_save_path:
            print(f">>> [REPLAY] Saving detailed results to {final_save_path} ({len(raw_results)} records)")
            _save_t0 = time.time()
            try:
                with open(final_save_path, 'w') as f:
                    json.dump({
                        "config": cfg,
                        "meta": {
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
                            "token_counts_from_usage_api": actual_count,
                            "token_counts_from_trace_spec": trace_count,
                            "dispatch_timings": dispatch_timings,
                        },
                        "summary": {m: len(s) for m, s in model_stats.items()},
                        "per_model": per_model,
                        "overall": {
                            "duration_s": total_duration,
                            "rps": rps,
                            "tps": tps,
                            "processed_tps": processed_tps,
                            "generated_tps": generated_tps,
                            "total_tokens": total_tokens,
                            "total_input_tokens": total_input_tokens,
                            "total_output_tokens": total_output_tokens,
                            "requests_completed": completed_requests,
                            "requests_scheduled": scheduled_requests,
                            "errors": sum(v["errors"] for v in per_model.values()),
                            "p50_s": float(np.percentile(
                                [x[1] for x in results if x[2]], 50
                            )) if any(x[2] for x in results) else None,
                            "p99_s": float(np.percentile(
                                [x[1] for x in results if x[2]], 99
                            )) if any(x[2] for x in results) else None,
                            "trace_span_s": dispatch_timings[-1]["trace_span_s"] if dispatch_timings else None,
                            "actual_dispatch_s": dispatch_timings[-1]["actual_dispatch_s"] if dispatch_timings else None,
                            "dispatch_overhead_s": dispatch_timings[-1]["overhead_s"] if dispatch_timings else None,
                        },
                        "requests": raw_results
                    }, f, indent=2)
                _save_elapsed = time.time() - _save_t0
                _save_mb = os.path.getsize(final_save_path) / (1024 * 1024)
                print(f">>> [REPLAY] Save complete: {_save_mb:.1f} MB in {_save_elapsed:.2f}s")
            except Exception as e:
                print(f"!!! ERROR Saving results: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument(
        "--include-tp",
        action="store_true",
        help="Include tensor_parallel_size in every request payload.",
    )
    parser.add_argument(
        "--early-stop",
        type=float,
        default=0.0,
        help="Fraction (0-1) of responses to wait for before stopping early (0 = all).",
    )
    parser.add_argument(
        "--num-runs",
        type=int,
        default=1,
        help="Number of times to replay the main trace.",
    )
    parser.add_argument(
        "--dest", "--destination",
        dest="dest",
        choices=["proxy", "direct"],
        default="proxy",
        help=(
            "'proxy': send to localhost:<port>. "
            "'direct': round-robin over all nodes in $PBS_NODEFILE."
        ),
    )
    parser.add_argument(
        "--proxy-port",
        type=int,
        default=None,
        dest="proxy_port",
        help=(
            "Override the proxy port. By default the port is read from "
            "proxy_out/proxy_port (written by driver.py) or from the config YAML."
        ),
    )
    parser.add_argument(
        "--base-urls",
        type=str,
        default=None,
        dest="base_urls",
        help=(
            "Comma-separated base URLs (e.g. http://0.0.0.0:8000,http://0.0.0.0:8001). "
            "Overrides port-based URL construction for multi-target benchmarks."
        ),
    )
    args = parser.parse_args()

    if not (0.0 <= args.early_stop <= 1.0):
        parser.error("--early-stop must be between 0.0 and 1.0")

    asyncio.run(replay(
        args.config, args.include_tp, args.early_stop,
        args.num_runs, args.dest, args.proxy_port, args.base_urls,
    ))
