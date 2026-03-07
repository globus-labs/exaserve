import argparse
import asyncio
import gc
import glob
import itertools
import json
import multiprocessing
import os
import pathlib
import re
import resource
import shutil
import signal
import subprocess
import sys
import tempfile
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

import httpx
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
# process is one MPI rank (one compute node). Multiprocessing workers are
# spawned *within* each rank for intra-node parallelism.
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
REQ_BATCH_SIZE = 5
EXTRA_RATE = 0.2

_MAX_ERROR_PRINTS = 5
_error_print_count = 0


def _print_request_error(url: str, error_msg: str):
    """Print request error with rate limiting to avoid log flooding."""
    global _error_print_count
    _error_print_count += 1
    if _error_print_count <= _MAX_ERROR_PRINTS:
        print(f"\n!!! [REQ ERROR #{_error_print_count}] {url}\n    {error_msg}", flush=True)
    elif _error_print_count % 100 == 0:
        print(f"\n!!! [REQ ERROR] {_error_print_count} total failures so far...", flush=True)

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


async def send_request(session, base_url, req, mode_map, include_tp: bool, generation_mode: str):
    mode = getattr(req, 'mode', None)
    url = f"{base_url}/v1/chat/completions" if mode == "chat" else f"{base_url}/v1/completions"

    if generation_mode == "deterministic":
        if mode == "chat":
            payload = {
                "model": req.model,
                "messages": [{"role": "user", "content": req.prompt}],
                "max_tokens": req.output_len,
                "min_tokens": req.output_len,
                "temperature": 0.7,
                "ignore_eos": True
            }
        else:
            payload = {
                "model": req.model,
                "prompt": req.prompt,
                "max_tokens": req.output_len,
                "min_tokens": req.output_len,
                "temperature": 0.7,
                "ignore_eos": True
            }
    else:
        if mode == "chat":
            payload = {
                "model": req.model,
                "messages": [{"role": "user", "content": req.prompt}],
                "max_tokens": req.output_len,
                "temperature": 0.7,
            }
        else:
            payload = {
                "model": req.model,
                "prompt": req.prompt,
                "max_tokens": req.output_len,
                "temperature": 0.7,
            }

    if include_tp:
        payload["tensor_parallel_size"] = req.tensor_parallel_size

    start = time.time()
    success = False
    error_msg = ""
    response_data = None

    try:
        resp = await session.post(url, json=payload)
        success = (resp.status_code == 200)
        if success:
            response_data = resp.json()
        else:
            error_msg = f"HTTP {resp.status_code}: {resp.text[:300]}"
            _print_request_error(url, error_msg)
    except Exception as e:
        error_msg = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
        _print_request_error(url, error_msg)

    end_time = time.time()
    latency = end_time - start

    actual_prompt_tokens = None
    actual_completion_tokens = None
    if response_data and "usage" in response_data:
        usage = response_data["usage"]
        actual_prompt_tokens = usage.get("prompt_tokens")
        actual_completion_tokens = usage.get("completion_tokens")

    return req, latency, success, error_msg, end_time, actual_prompt_tokens, actual_completion_tokens


def _config_trace_path(cfg: dict) -> str:
    jtc = cfg.get('job_trace_config') or {}
    return jtc.get('output_trace_path', 'experiment_trace.jsonl')

def _config_result_dir(cfg: dict):
    return cfg.get('pbs_result_dir')

def _config_mode_map(cfg: dict) -> dict:
    dep = cfg.get('model_deployment_config') or {}
    model_configs = dep.get('model_configs') or []
    return {m.get('model_id', ''): m.get('mode', 'chat') for m in model_configs if m.get('model_id')}

def _config_gpu_topology(cfg: dict) -> tuple:
    dep = cfg.get('model_deployment_config') or {}
    return (dep.get('num_nodes', 1), dep.get('num_gpus_per_node', 4))


# ==============================================================================
# GO DISPATCH HELPERS
# These functions drive the Go HTTP/2 dispatch binary (eval/go_client/go_dispatch).
# If the binary is not found, replay falls back to the Python multiprocessing
# workers below for compatibility.
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


def _run_go_dispatch(
    go_bin: str,
    base_urls: list,
    run_t0: float,
    rank_requests: list,
    generation_mode: str,
    include_tp: bool,
    concurrency: int,
    interrupt_event,
    rank: int,
    tmp_dir: str,
    dispatch_workers: int = 4,
    no_save: bool = False,
) -> tuple[list, float]:
    """
    Write the rank's trace partition, invoke go_dispatch, and return
    (local_results_as_tuples, last_fire_time).

    local_results_as_tuples: list of 7-tuples matching the Python worker format:
        (TraceRequest, latency, success, error_msg, end_time,
         actual_prompt_tokens, actual_completion_tokens)
    """
    trace_path = os.path.join(tmp_dir, f"rank{rank}_trace.jsonl")
    result_path = os.path.join(tmp_dir, f"rank{rank}_results.jsonl")

    _write_trace_partition(rank_requests, trace_path)

    # Build request lookup by req_id so we can reconstruct the TraceRequest
    req_map = {req.req_id: req for req in rank_requests}

    cmd = [
        go_bin,
        "--base-urls", ",".join(base_urls),
        "--run-t0", repr(run_t0),
        "--generation-mode", generation_mode,
        "--timeout", str(TIMEOUT_S),
        "--concurrency", str(concurrency),
        "--dispatch-workers", str(dispatch_workers),
        "--trace-file", trace_path,
    ]
    if no_save:
        cmd.append("--no-save")
    else:
        cmd.extend(["--result-file", result_path])
    if include_tp:
        cmd.append("--include-tp")

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )

    # Poll until the subprocess finishes, forwarding interrupts
    while True:
        try:
            proc.wait(timeout=1.0)
            break
        except subprocess.TimeoutExpired:
            if interrupt_event is not None and interrupt_event.is_set():
                proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    proc.kill()
                break

    stderr_out = proc.stderr.read().decode(errors="replace")
    for line in stderr_out.splitlines():
        print(f"[go_dispatch rank {rank}] {line}", flush=True)

    if proc.returncode not in (0, -signal.SIGTERM):
        print(
            f"!!! [go_dispatch rank {rank}] exited with code {proc.returncode}",
            flush=True,
        )

    if no_save:
        return [], 0.0, None
    return _read_go_results(result_path, req_map)


def _read_go_results(result_path: str, req_map: dict) -> tuple[list, float]:
    """
    Parse go_dispatch JSONL results back into the 7-tuple format used by the
    Python orchestrator, plus the last_fire_time from the dispatch_done line.

    Returns: (list_of_tuples, last_fire_time, adjusted_run_t0_or_None)
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
# MULTIPROCESSING WORKER (Python fallback when Go binary is not available)
# Each worker process manages its own asyncio event loop and dispatches the
# subset of requests assigned to it on their original schedule.
# Workers are spawned by MPI ranks — MPI is NOT used inside workers.
# ==============================================================================

def _worker_entry(
    worker_id: int,
    worker_requests: list,
    base_urls: list,
    include_tp: bool,
    generation_mode: str,
    mode_map: dict,
    start_event,        # multiprocessing.Event: fires when run_t0 is set
    run_t0_val,         # multiprocessing.Value('d'): shared start timestamp
    result_queue,       # multiprocessing.Queue: for sending results back
    interrupt_event,    # multiprocessing.Event: set by main process on Ctrl-C
    safe_conn_limit: int,
    num_workers: int,
    dispatch_done_val,  # multiprocessing.Value('d'): updated to last fire time
):
    """Entry point for a worker process; runs an independent asyncio event loop."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft < hard:
            resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    except Exception:
        pass

    gc.disable()
    asyncio.run(_worker_async(
        worker_id, worker_requests, base_urls, include_tp, generation_mode,
        mode_map, start_event, run_t0_val, result_queue, interrupt_event,
        safe_conn_limit, num_workers, dispatch_done_val,
    ))


async def _worker_async(
    worker_id: int,
    worker_requests: list,
    base_urls: list,
    include_tp: bool,
    generation_mode: str,
    mode_map: dict,
    start_event,
    run_t0_val,
    result_queue,
    interrupt_event,
    safe_conn_limit: int,
    num_workers: int,
    dispatch_done_val,
):
    """Async body: waits for start signal, then dispatches requests on schedule."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, start_event.wait)
    run_t0 = run_t0_val.value

    per_worker_conn = max(10, safe_conn_limit // num_workers)
    session = httpx.AsyncClient(
        http2=True,
        limits=httpx.Limits(
            max_connections=per_worker_conn,
            max_keepalive_connections=per_worker_conn,
            keepalive_expiry=4,
        ),
        timeout=httpx.Timeout(TIMEOUT_S),
    )
    url_cycle = itertools.cycle(base_urls)

    tasks = []
    try:
        for i, req in enumerate(worker_requests):
            if interrupt_event.is_set():
                print(
                    f"\n[Worker {worker_id}] Interrupt at request {i}/{len(worker_requests)}, "
                    "stopping dispatch.",
                    flush=True,
                )
                break

            target_time = run_t0 + req.timestamp
            now = time.time()
            if target_time > now:
                await asyncio.sleep(target_time - now)

            tasks.append(asyncio.create_task(
                send_request(session, next(url_cycle), req, mode_map, include_tp, generation_mode)
            ))

        # Record when this worker fired its last request.  Use a lock-free
        # compare-and-set via the Value lock so we capture the true maximum
        # across all workers on this rank.
        last_fire = time.time()
        with dispatch_done_val.get_lock():
            if last_fire > dispatch_done_val.value:
                dispatch_done_val.value = last_fire

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if not isinstance(r, BaseException):
                result_queue.put(r)
    finally:
        await session.aclose()


# ==============================================================================
# MAIN REPLAY FUNCTION
# ==============================================================================

async def replay(
    config_path,
    include_tp: bool,
    early_stop: float,
    no_warmup: bool,
    num_runs: int,
    dest: str = "proxy",
    num_workers: int = None,   # None = resolve from config; CLI overrides config
    proxy_port: int = None,    # None = auto-detect from port file or config
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

    # Resolve num_workers: None (CLI default) → config value (num_workers_per_node) → hardcoded 4
    if num_workers is None:
        num_workers = replay_cfg.get('num_workers_per_node', 4)

    # Derive expected client-node count from total PBS nodes × per-node ratio.
    # actual_client_nodes = max(1, min(num_nodes, int(num_nodes * num_cli_per_node)))
    cfg_num_nodes = replay_cfg.get('num_nodes', mpi_size)   # total PBS nodes
    num_cli_per_node_ratio = replay_cfg.get('num_cli_per_node', 1.0)
    expected_client_nodes = max(1, min(cfg_num_nodes, round(cfg_num_nodes * num_cli_per_node_ratio)))
    if is_root:
        print(
            f">>> [REPLAY] PBS nodes: {cfg_num_nodes} | "
            f"num_cli_per_node: {num_cli_per_node_ratio:.4g} | "
            f"Expected client nodes: {expected_client_nodes} | "
            f"Actual MPI size: {mpi_size}"
        )
    if expected_client_nodes != mpi_size and is_root:
        print(
            f"!!! WARNING: Expected {expected_client_nodes} client nodes but MPI "
            f"size is {mpi_size}. Check that mpiexec was launched with the right "
            f"-n value. Proceeding with actual MPI size."
        )

    total_workers = mpi_size * num_workers

    # ------------------------------------------------------------------
    # 3. Build target URL list
    # ------------------------------------------------------------------
    if dest == "direct":
        cluster_nodes = _get_cluster_nodes()
        base_urls = [f"http://{node}:{port}" for node in cluster_nodes]
        if is_root:
            print(f">>> [DEST] direct mode — {len(base_urls)} node(s): {cluster_nodes}")
    else:
        cluster_nodes = []
        base_urls = [f"http://0.0.0.0:{port}"]
        if is_root:
            print(f">>> [DEST] proxy mode — target: {base_urls[0]}")

    url_cycle = itertools.cycle(base_urls)   # used only for warmup on this rank
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

    mode_map = _config_mode_map(cfg)

    # ------------------------------------------------------------------
    # 5. Go binary detection and request partitioning
    #
    # When the Go binary is available, a single go_dispatch subprocess per
    # MPI rank handles all of that rank's requests via goroutines.
    # Partition: requests[rank :: mpi_size]  (interleaved across ranks)
    #
    # Fallback (Go binary not found): original Python multiprocessing workers.
    # Partition: requests[global_w :: total_workers] across num_workers workers.
    # ------------------------------------------------------------------
    go_bin = _find_go_binary()
    use_go = (go_bin is not None)

    if use_go:
        rank_requests = requests[rank::mpi_size]
        if is_root:
            print(f">>> [DISPATCH] Go binary: {go_bin}")
            print(f">>> [DISPATCH] Mode: Go subprocess per MPI rank "
                  f"(concurrency=--concurrency, {num_workers} Python workers ignored)")
    else:
        rank_requests = []  # unused in Python-worker path
        if is_root:
            print(">>> [DISPATCH] Go binary not found — using Python multiprocessing workers (fallback)")

    # Python-worker path still needs the per-worker sub-partition
    if not use_go:
        rank_worker_lists = []
        for local_w in range(num_workers):
            global_w = rank * num_workers + local_w
            rank_worker_lists.append(requests[global_w::total_workers])
    else:
        rank_worker_lists = []  # unused in Go path

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

    rank_req_count = len(rank_requests) if use_go else sum(len(l) for l in rank_worker_lists)
    if is_root:
        print(f">>> [REPLAY] Target: {base_url}")
        print(f"    Requests: {len(requests)}")
        print(f"    Duration: {requests[-1].timestamp:.2f}s")
        print(f">>> [REPLAY] MPI ranks: {mpi_size} | Workers/rank: {num_workers} | "
              f"Total workers: {total_workers}")
        print(f"    This rank ({rank}) handles {rank_req_count} / {len(requests)} requests")
        print(f">>> [REPLAY] Generation Mode: {generation_mode.upper()}")
        if generation_mode == "deterministic":
            print(f"    Using min_tokens=max_tokens={requests[0].output_len if requests else 'N/A'} "
                  "with ignore_eos=True")
        else:
            print("    Using natural generation with EOS termination.")
        print(">>> [REPLAY] Disabling Garbage Collection for precision...")

    # ------------------------------------------------------------------
    # 7. Signal handler (each rank handles SIGINT independently)
    # ------------------------------------------------------------------
    interrupted = False
    interrupt_event = multiprocessing.Event()

    def signal_handler(sig, frame):
        nonlocal interrupted
        interrupted = True
        interrupt_event.set()

    old_handler = signal.signal(signal.SIGINT, signal_handler)

    # ------------------------------------------------------------------
    # 8. Resource management
    # ------------------------------------------------------------------
    gc.disable()
    t0 = time.time()

    results = []
    all_runs_results = []
    run_durations = []
    warmup_duration_s = 0.0
    session = None
    safe_conn_limit = 100
    tmp_dir = tempfile.mkdtemp(prefix=f"replay_rank{rank}_")

    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if is_root:
            print(f">>> [SYSTEM] Current open file limit: soft={soft}, hard={hard}")
        if soft < hard:
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
                soft = hard
                if is_root:
                    print(f">>> [SYSTEM] Increased soft limit to {soft}")
            except Exception as e:
                if is_root:
                    print(f">>> [SYSTEM] Failed to increase file limit: {e}")

        safe_conn_limit = max(100, soft - 512)
        if is_root:
            print(f">>> [CONFIG] Connection budget: {safe_conn_limit} total | "
                  f"{safe_conn_limit // num_workers} per worker")

        # ==============================================================================
        # WARMUP PHASE
        # All MPI ranks participate in parallel.  Total warmup load on the server =
        # warmup_count (split evenly across ranks so aggregate load is unchanged).
        # ==============================================================================
        if not no_warmup:
            num_nodes_deploy, num_gpus_per_node = _config_gpu_topology(cfg)
            total_warmup = int(num_nodes_deploy * num_gpus_per_node * REQ_BATCH_SIZE * (1 + EXTRA_RATE))
            # Each rank fires an equal share so the server sees total_warmup requests total
            per_rank_warmup = max(1, total_warmup // mpi_size)

            if is_root:
                print(f"\n>>> [WARMUP] Starting warmup phase (all {mpi_size} rank(s))...")
                print(f"    Deploy nodes: {num_nodes_deploy}, GPUs/node: {num_gpus_per_node}")
                print(f"    Total warmup: {total_warmup} | Per rank: {per_rank_warmup}")

            if requests:
                session = httpx.AsyncClient(
                    http2=True,
                    limits=httpx.Limits(
                        max_connections=safe_conn_limit,
                        max_keepalive_connections=safe_conn_limit,
                        keepalive_expiry=4,
                    ),
                    timeout=httpx.Timeout(TIMEOUT_S),
                )
                base_req = requests[0]
                warmup_tasks = []
                warmup_start = time.time()

                print(f"    [Rank {rank}] Firing {per_rank_warmup} warmup requests...", flush=True)
                for _ in range(per_rank_warmup):
                    w_req = TraceRequest(
                        timestamp=0,
                        model=base_req.model,
                        prompt=base_req.prompt,
                        input_len=base_req.input_len,
                        output_len=base_req.output_len,
                        tensor_parallel_size=base_req.tensor_parallel_size,
                        req_id=uuid.uuid4().hex,
                    )
                    warmup_tasks.append(asyncio.create_task(
                        send_request(session, next(url_cycle), w_req, mode_map, include_tp, generation_mode)
                    ))

                done, _ = await asyncio.wait(warmup_tasks, return_when=asyncio.ALL_COMPLETED)
                success_count = sum(1 for t in done if t.result()[2])
                warmup_dur = time.time() - warmup_start
                print(
                    f"    [Rank {rank}] Warmup done in {warmup_dur:.2f}s. "
                    f"Success: {success_count}/{per_rank_warmup}",
                    flush=True,
                )

                await session.aclose()
                session = None

                # All ranks must finish warmup before proceeding
                _mpi_barrier(comm)
                warmup_duration_s = warmup_dur

                if is_root:
                    print(f">>> [WARMUP] All ranks finished. Resting 10s...")
                await asyncio.sleep(10)
                _mpi_barrier(comm)
            else:
                if is_root:
                    print(">>> [WARMUP] No requests loaded. Skipping.")

        # ==============================================================================
        # MAIN EXPERIMENT LOOP (Multiple Runs)
        # run_t0 is set by rank 0 immediately after a barrier so all ranks share the
        # same wall-clock origin.  Rank 0 broadcasts it to all others.
        # ==============================================================================
        all_runs_results = []
        run_durations = []
        dispatch_timings = []   # per-run: {trace_span_s, actual_dispatch_s, overhead_s}

        for run_idx in range(num_runs):
            # Check for interrupt before starting (and still hit the barrier)
            _mpi_barrier(comm)
            if interrupted:
                if is_root:
                    print(f"\n>>> [INTERRUPTED] Skipping run {run_idx + 1}.")
                break

            if is_root:
                print("\n" + "=" * 70)
                print(f">>> [RUN {run_idx + 1}/{num_runs}] Starting main experiment replay...")
                print("=" * 70)

            # ---- Synchronised start time ----
            # rank 0 sets run_t0 right after the barrier so all ranks share the
            # same origin.  The tiny broadcast latency (~1-5 ms) is absorbed as
            # negative sleep for the first few requests, which is harmless.
            if is_root:
                run_t0 = time.time()
            else:
                run_t0 = None
            run_t0 = _mpi_bcast(comm, run_t0, root=0)

            # ---- Per-run interrupt event ----
            run_interrupt_event = multiprocessing.Event()

            # ---- Dispatch: Go subprocess or Python multiprocessing workers ----
            effective_run_t0 = run_t0
            last_fire_time = 0.0

            if use_go:
                # ------------------------------------------------------------------
                # Go path: single subprocess per rank handles all rank_requests.
                # run_t0 is broadcast above so all ranks share the same origin.
                # We run the subprocess in a thread executor so the asyncio loop
                # stays live (needed for MPI barrier and progress prints below).
                # ------------------------------------------------------------------
                local_expected = len(rank_requests)
                print(
                    f"[Rank {rank}] Launching go_dispatch for {local_expected} requests...",
                    flush=True,
                )

                # go_concurrency defaults to 2000; callers can override via config
                # (job_replay_client_config.go_concurrency) if needed.
                go_concurrency = replay_cfg.get("go_concurrency", 2000)
                # dispatch_workers: number of parallel dispatch goroutines in go_dispatch.
                # Each goroutine handles every Nth request, giving it N× longer intervals
                # and breaking the single-loop throughput ceiling (~30K req/s).
                dispatch_workers = replay_cfg.get("dispatch_workers", 4)
                go_no_save = replay_cfg.get("no_save", False)

                loop = asyncio.get_running_loop()
                local_results, last_fire_time, go_adjusted_run_t0 = await loop.run_in_executor(
                    None,
                    _run_go_dispatch,
                    go_bin,
                    base_urls,
                    run_t0,
                    rank_requests,
                    generation_mode,
                    include_tp,
                    go_concurrency,
                    run_interrupt_event,
                    rank,
                    tmp_dir,
                    dispatch_workers,
                    go_no_save,
                )
                # Use go_dispatch's adjusted run_t0 (post-startup-compensation) for
                # overhead calculation, so startup latency is not counted as dispatch
                # overhead. Fall back to the Python run_t0 for old binaries.
                effective_run_t0 = go_adjusted_run_t0 if go_adjusted_run_t0 is not None else run_t0
                print(
                    f"\r[Rank {rank}] go_dispatch finished: "
                    f"{len(local_results)}/{local_expected} responses.",
                    flush=True,
                )

            else:
                # ------------------------------------------------------------------
                # Python fallback: original multiprocessing workers.
                # ------------------------------------------------------------------
                dispatch_done_val = multiprocessing.Value('d', 0.0)
                run_start_event = multiprocessing.Value('d', run_t0)
                worker_start_event = multiprocessing.Event()
                result_queue = multiprocessing.Queue()

                processes = []
                for local_w in range(num_workers):
                    p = multiprocessing.Process(
                        target=_worker_entry,
                        args=(
                            rank * num_workers + local_w,
                            rank_worker_lists[local_w],
                            base_urls,
                            include_tp,
                            generation_mode,
                            mode_map,
                            worker_start_event,
                            run_start_event,
                            result_queue,
                            run_interrupt_event,
                            safe_conn_limit,
                            num_workers,
                            dispatch_done_val,
                        ),
                        daemon=True,
                    )
                    p.start()
                    processes.append(p)

                worker_start_event.set()

                local_expected = sum(len(l) for l in rank_worker_lists)
                local_results = []
                pending_processes = list(processes)

                while pending_processes or not result_queue.empty():
                    while True:
                        try:
                            local_results.append(result_queue.get_nowait())
                            if len(local_results) % 50 == 0:
                                print(
                                    f"\r[Rank {rank}] Received {len(local_results)}/"
                                    f"{local_expected} responses...",
                                    end="", flush=True,
                                )
                        except Exception:
                            break

                    if interrupted:
                        run_interrupt_event.set()

                    pending_processes = [p for p in pending_processes if p.is_alive()]
                    await asyncio.sleep(0.1)

                while not result_queue.empty():
                    try:
                        local_results.append(result_queue.get_nowait())
                    except Exception:
                        break

                for p in processes:
                    if p.is_alive():
                        p.terminate()
                    p.join(timeout=5)

                print(
                    f"\r[Rank {rank}] Received {len(local_results)}/{local_expected} responses.",
                    flush=True,
                )
                last_fire_time = dispatch_done_val.value
                effective_run_t0 = run_t0  # Python workers use the original run_t0

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
            gathered = _mpi_gather(comm, local_results, root=0)

            if is_root:
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
        gc.enable()
        if session and not session.closed:
            await session.close()
        # Clean up temporary JSONL files written for Go subprocess
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 9. Analysis & Save (rank 0 only)
    # ------------------------------------------------------------------
    if not is_root:
        return

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

    for run_idx, run_results in enumerate(all_runs_results):
        for r in run_results:
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
                        "warmup_duration_s": warmup_duration_s,
                        "generation_mode": generation_mode,
                        "dest": dest,
                        "cluster_nodes": cluster_nodes,
                        "mpi_size": mpi_size,
                        "num_workers_per_rank": num_workers,
                        "total_workers": total_workers,
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
        "--no-warmup",
        action="store_true",
        help="Disable the warmup phase.",
    )
    parser.add_argument(
        "--num-runs",
        type=int,
        default=1,
        help="Number of times to replay the main trace (after warmup).",
    )
    parser.add_argument(
        "--num-workers-per-node",
        type=int,
        default=None,
        dest="num_workers",
        help=(
            "Multiprocessing workers per MPI rank (per node). "
            "Defaults to job_replay_client_config.num_workers_per_node in the YAML config (or 4). "
            "CLI value overrides the config."
        ),
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
    args = parser.parse_args()

    if not (0.0 <= args.early_stop <= 1.0):
        parser.error("--early-stop must be between 0.0 and 1.0")

    asyncio.run(replay(
        args.config, args.include_tp, args.early_stop, args.no_warmup,
        args.num_runs, args.dest, args.num_workers, args.proxy_port,
    ))
