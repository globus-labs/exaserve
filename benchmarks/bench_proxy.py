"""
LiteLLM proxy benchmark.

Automatically:
  1. Launches N stub_server instances on free ports (simulating N Ray Serve backends)
  2. Generates a LiteLLM config YAML pointing to these stubs
  3. Starts LiteLLM proxy subprocess and waits for health check
  4. Fires load through LiteLLM and measures throughput, latency overhead,
     failure modes, resource usage

Parameter sweeps:
  --sweep litellm_workers : LiteLLM --num_workers [1, 2, 4, 8]
  --sweep routing          : routing strategy [least-busy, simple-shuffle, latency-based-routing]
  --sweep backends         : number of stub backend instances [1, 2, 4, 8, 16]
  --sweep rps              : target request rate for ramp/fixed tests

Usage:
  # Sweep LiteLLM workers and routing strategies
  python bench_proxy.py \\
      --litellm-python /path/to/litellm/venv/bin/python \\
      --sweep litellm_workers,routing \\
      --rps 100 --duration 30

  # Ramp test: find the breaking point
  python bench_proxy.py \\
      --litellm-python /path/to/litellm/venv/bin/python \\
      --ramp --ramp-start 10 --ramp-max 2000 --ramp-step 1.2 --ramp-window 10

  # Fixed single point
  python bench_proxy.py \\
      --litellm-python /path/to/litellm/venv/bin/python \\
      --litellm-workers 4 --routing least-busy --num-backends 4 --rps 200
"""

import atexit
import argparse
import asyncio
import gc
import itertools
import json
import math
import os
import signal
import socket
import subprocess
import sys
import time
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import yaml

try:
    import httpx
except ImportError:
    print("ERROR: httpx is required. Install via: pip install httpx[http2]", file=sys.stderr)
    sys.exit(1)

try:
    import psutil
    _PSUTIL_AVAILABLE = True
except ImportError:
    _PSUTIL_AVAILABLE = False

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

_BENCH_DIR = Path(__file__).parent
_DEFAULT_BENCH_RESULTS_DIR = Path.home() / "agpt" / "data" / "bench_results"
sys.path.insert(0, str(_BENCH_DIR))
from port_model import PortMonitor, EPHEMERAL_RANGE

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SWEEP_LITELLM_WORKERS  = [1, 2, 4, 8, 16]
SWEEP_ROUTING          = ["least-busy", "simple-shuffle", "latency-based-routing"]
SWEEP_BACKENDS         = [1, 2, 4, 8, 16]
SWEEP_RPS              = [10, 50, 100, 200, 500, 1000]

PROXY_HEALTH_TIMEOUT_S = 120
STUB_START_TIMEOUT_S   = 10
TIMEOUT_S              = 60.0   # per-request timeout when talking to LiteLLM
MODEL_ID               = "stub-model"

# p99 latency threshold for "degradation" detection in ramp tests
DEGRADE_LATENCY_S      = 2.0
# Error fraction threshold for "failure" detection
DEGRADE_ERROR_FRACTION = 0.05

REPLAY_CLIENT_ROUTING  = "least-busy"
CHECKPOINT_FILENAME    = "proxy_sweep_checkpoint.json"
_BENCH_CLIENT          = _BENCH_DIR / "bench_client.py"

_SHUTDOWN_REQUESTED: bool = False
_ACTIVE_LITELLM_MGR: Optional["LiteLLMManager"] = None
_ACTIVE_STUB_MGR: Optional["StubServerManager"] = None
_ACTIVE_CLIENT_PROC: Optional[subprocess.Popen] = None
_ACTIVE_CHECKPOINT: Optional[dict] = None
_ACTIVE_CHECKPOINT_PATH: Optional[Path] = None


def _config_key(litellm_workers: int, num_backends: int) -> str:
    return f"lw{litellm_workers}_b{num_backends}"


def _checkpoint_path(work_dir: Path) -> Path:
    return work_dir / CHECKPOINT_FILENAME


def _load_checkpoint(work_dir: Path) -> dict:
    path = _checkpoint_path(work_dir)
    if not path.exists():
        return {"meta": {}, "completed": {}}
    with open(path) as f:
        checkpoint = json.load(f)
    checkpoint.setdefault("meta", {})
    checkpoint.setdefault("completed", {})
    return checkpoint


def _save_checkpoint(work_dir: Path, checkpoint: dict):
    global _ACTIVE_CHECKPOINT, _ACTIVE_CHECKPOINT_PATH

    path = _checkpoint_path(work_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp")
    with open(tmp_path, "w") as f:
        json.dump(checkpoint, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)
    _ACTIVE_CHECKPOINT = checkpoint
    _ACTIVE_CHECKPOINT_PATH = path
    print(f"[BenchProxy] Checkpoint saved to {path}", flush=True)


def _terminate_process(proc: Optional[subprocess.Popen], label: str, timeout_s: float = 10.0):
    if proc is None or proc.poll() is not None:
        return
    print(f"[BenchProxy] Stopping {label} (pid={proc.pid})", flush=True)
    proc.terminate()
    try:
        proc.wait(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def _signal_handler(signum, _frame):
    global _SHUTDOWN_REQUESTED
    _SHUTDOWN_REQUESTED = True
    try:
        signame = signal.Signals(signum).name
    except ValueError:
        signame = str(signum)
    print(f"\n[BenchProxy] Received {signame}; shutting down after current cleanup.", flush=True)


def _cleanup_subprocesses():
    global _ACTIVE_CLIENT_PROC, _ACTIVE_LITELLM_MGR, _ACTIVE_STUB_MGR

    try:
        _terminate_process(_ACTIVE_CLIENT_PROC, "bench_client.py", timeout_s=5.0)
    except Exception as exc:
        print(f"[BenchProxy] Warning: failed to stop bench_client.py cleanly: {exc}", flush=True)
    finally:
        _ACTIVE_CLIENT_PROC = None

    try:
        if _ACTIVE_LITELLM_MGR is not None:
            _ACTIVE_LITELLM_MGR.stop()
    except Exception as exc:
        print(f"[BenchProxy] Warning: failed to stop LiteLLM cleanly: {exc}", flush=True)
    finally:
        _ACTIVE_LITELLM_MGR = None

    try:
        if _ACTIVE_STUB_MGR is not None:
            _ACTIVE_STUB_MGR.stop()
    except Exception as exc:
        print(f"[BenchProxy] Warning: failed to stop stub servers cleanly: {exc}", flush=True)
    finally:
        _ACTIVE_STUB_MGR = None

    if _ACTIVE_CHECKPOINT is not None and _ACTIVE_CHECKPOINT_PATH is not None:
        try:
            _save_checkpoint(_ACTIVE_CHECKPOINT_PATH.parent, _ACTIVE_CHECKPOINT)
        except Exception as exc:
            print(f"[BenchProxy] Warning: failed to save checkpoint during cleanup: {exc}", flush=True)


atexit.register(_cleanup_subprocesses)
signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


# ---------------------------------------------------------------------------
# Port utilities
# ---------------------------------------------------------------------------

def _find_free_port(start: int = 19000) -> int:
    """Find a free TCP port starting from `start`."""
    port = start
    while port < 65535:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                port += 1
    raise RuntimeError("No free port found")


# ---------------------------------------------------------------------------
# Stub server manager
# ---------------------------------------------------------------------------

class StubServerManager:
    """Launches and tracks multiple stub_server.py processes."""

    def __init__(self, n: int, python: str = sys.executable, latency_ms: float = 0.0,
                 response_tokens: int = 10):
        self.n = n
        self.python = python
        self.latency_ms = latency_ms
        self.response_tokens = response_tokens
        self.ports: list[int] = []
        self.procs: list[subprocess.Popen] = []
        self._stub_script = str(_BENCH_DIR / "stub_server.py")

    def start(self) -> list[int]:
        """Start N stub servers and return their ports."""
        base_port = 19100
        for i in range(self.n):
            port = _find_free_port(base_port + i * 10)
            cmd = [
                self.python, self._stub_script,
                "--port", str(port),
                "--host", "127.0.0.1",
                "--latency-ms", str(self.latency_ms),
                "--response-tokens", str(self.response_tokens),
                "--model", MODEL_ID,
                "--log-level", "error",
            ]
            proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.ports.append(port)
            self.procs.append(proc)

        # Wait for stubs to be ready
        deadline = time.time() + STUB_START_TIMEOUT_S
        for port in self.ports:
            while time.time() < deadline:
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=1):
                        break
                except OSError:
                    time.sleep(0.2)

        print(f"[Stubs] Started {self.n} stub server(s) on ports {self.ports}", flush=True)
        return self.ports

    def stop(self):
        for proc in self.procs:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
        print(f"[Stubs] Stopped {len(self.procs)} stub server(s).", flush=True)
        self.procs.clear()
        self.ports.clear()


# ---------------------------------------------------------------------------
# LiteLLM manager (reuses logic from src/proxy/litellm_proxy.py)
# ---------------------------------------------------------------------------

class LiteLLMManager:
    """Manages one LiteLLM proxy subprocess."""

    def __init__(self, python: str, port: int, num_workers: int,
                 routing_strategy: str, work_dir: Path):
        self.python = python
        self.port = port
        self.num_workers = num_workers
        self.routing_strategy = routing_strategy
        self.work_dir = work_dir
        self.proc: Optional[subprocess.Popen] = None
        self._config_path: Optional[Path] = None

    def generate_config(self, backend_ports: list[int]) -> Path:
        """Write litellm_config.yaml with stub backends."""
        model_list = [
            {
                "model_name": MODEL_ID,
                "litellm_params": {
                    "model": f"openai/{MODEL_ID}",
                    "api_base": f"http://127.0.0.1:{port}/v1",
                    "api_key": "dummy",
                },
            }
            for port in backend_ports
        ]
        config = {
            "model_list": model_list,
            "router_settings": {
                "routing_strategy": self.routing_strategy,
                "num_retries": 0,
                "timeout": TIMEOUT_S,
            },
            "general_settings": {},
        }
        self.work_dir.mkdir(parents=True, exist_ok=True)
        config_path = self.work_dir / f"litellm_config_w{self.num_workers}_{self.routing_strategy}.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        self._config_path = config_path
        return config_path

    def start(self) -> subprocess.Popen:
        litellm_bin = str(Path(self.python).parent / "litellm")
        cmd = [
            litellm_bin,
            "--config", str(self._config_path),
            "--port", str(self.port),
            "--host", "127.0.0.1",
            "--num_workers", str(self.num_workers),
        ]
        env = os.environ.copy()
        env["LITELLM_PORT"] = str(self.port)
        env["LITELLM_HOST"] = "127.0.0.1"
        env["UVICORN_PORT"] = str(self.port)
        env["UVICORN_HOST"] = "127.0.0.1"
        env["PORT"] = str(self.port)
        env["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"
        # Strip proxy env vars
        for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "DEBUG"):
            env.pop(k, None)

        log_path = self.work_dir / f"litellm_w{self.num_workers}_{self.routing_strategy}.log"
        log_f = open(log_path, "w")
        print(f"[LiteLLM] Starting: {' '.join(cmd)}", flush=True)
        print(f"[LiteLLM] Log: {log_path}", flush=True)
        self.proc = subprocess.Popen(cmd, env=env, stdout=log_f, stderr=log_f)
        self._log_f = log_f
        print(f"[LiteLLM] PID={self.proc.pid}", flush=True)
        return self.proc

    def wait_healthy(self) -> bool:
        url = f"http://127.0.0.1:{self.port}/health/liveliness"
        import urllib.request, urllib.error
        deadline = time.time() + PROXY_HEALTH_TIMEOUT_S
        while time.time() < deadline:
            if self.proc and self.proc.poll() is not None:
                print(f"[LiteLLM] Process exited (code {self.proc.returncode}) before healthy.", flush=True)
                return False
            try:
                with urllib.request.urlopen(url, timeout=3) as resp:
                    if resp.status == 200:
                        print(f"[LiteLLM] Healthy (port {self.port})", flush=True)
                        return True
            except Exception:
                pass
            time.sleep(2)
        print(f"[LiteLLM] Health check timed out ({PROXY_HEALTH_TIMEOUT_S}s)", flush=True)
        return False

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.proc = None
        if hasattr(self, "_log_f") and not self._log_f.closed:
            self._log_f.close()
        print(f"[LiteLLM] Stopped.", flush=True)

    def get_resource_usage(self) -> Optional[dict]:
        if not _PSUTIL_AVAILABLE or not self.proc:
            return None
        try:
            proc = psutil.Process(self.proc.pid)
            children = proc.children(recursive=True)
            all_procs = [proc] + children
            cpu = sum(p.cpu_percent(interval=0.1) for p in all_procs)
            mem_mb = sum(p.memory_info().rss for p in all_procs) / 1024 / 1024
            return {"cpu_percent": round(cpu, 1), "mem_mb": round(mem_mb, 1), "num_procs": len(all_procs)}
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return None


# ---------------------------------------------------------------------------
# Load generator (simple async httpx — not the full multiprocessing client)
# ---------------------------------------------------------------------------

async def _generate_load(
    base_url: str,
    target_rps: float,
    duration_s: float,
    pool_size: int = 200,
) -> list[dict]:
    """Fire requests at target_rps for duration_s. Returns list of result dicts."""
    interval = 1.0 / target_rps
    n = max(1, int(target_rps * duration_s))
    results = []

    async with httpx.AsyncClient(
        http2=True,
        limits=httpx.Limits(max_connections=pool_size, max_keepalive_connections=pool_size),
        timeout=httpx.Timeout(TIMEOUT_S),
    ) as client:
        url = f"{base_url}/v1/chat/completions"
        payload = {
            "model": MODEL_ID,
            "messages": [{"role": "user", "content": "ping"}],
            "max_tokens": 10,
            "min_tokens": 10,
            "temperature": 0.7,
            "ignore_eos": True,
        }

        tasks = []
        t0 = time.time()

        for i in range(n):
            target = t0 + i * interval
            now = time.time()
            if target > now:
                await asyncio.sleep(target - now)

            fire_time = time.time()

            async def _req(ft=fire_time, idx=i):
                status = 0
                err = ""
                try:
                    resp = await client.post(url, json=payload)
                    status = resp.status_code
                    ok = status == 200
                    if not ok:
                        err = f"HTTP {status}: {resp.text[:100]}"
                except Exception as e:
                    ok = False
                    err = f"{type(e).__name__}: {e}"
                end = time.time()
                return {"fire": ft, "end": end, "latency": end - ft, "ok": ok, "status": status, "error": err}

            tasks.append(asyncio.create_task(_req()))

        completed = await asyncio.gather(*tasks, return_exceptions=True)
        for r in completed:
            if isinstance(r, dict):
                results.append(r)

    return results


def _analyze_results(results: list[dict], t0: float, duration_s: float) -> dict:
    if not results:
        return {"count": 0, "success": 0, "errors": 0, "error_fraction": 1.0,
                "actual_rps": 0, "latency_p50_s": None, "latency_p95_s": None,
                "latency_p99_s": None}
    successes = [r for r in results if r["ok"]]
    lats = sorted(r["latency"] for r in successes)
    errors = len(results) - len(successes)

    def pct(arr, p):
        if not arr:
            return None
        return float(arr[min(int(len(arr) * p / 100), len(arr) - 1)])

    actual_rps = len(results) / max(duration_s, 1e-6)

    return {
        "count": len(results),
        "success": len(successes),
        "errors": errors,
        "error_fraction": round(errors / len(results), 4),
        "actual_rps": round(actual_rps, 2),
        "latency_p50_s": pct(lats, 50),
        "latency_p95_s": pct(lats, 95),
        "latency_p99_s": pct(lats, 99),
        "latency_max_s": lats[-1] if lats else None,
    }


# ---------------------------------------------------------------------------
# Fixed-rate sweep point
# ---------------------------------------------------------------------------

def run_fixed_point(
    proxy_port: int,
    target_rps: float,
    duration_s: float,
    litellm_manager: Optional[LiteLLMManager] = None,
    monitor_ports: bool = True,
) -> dict:
    port_monitor = None
    if monitor_ports:
        port_monitor = PortMonitor(interval_s=1.0)
        port_monitor.start()

    resource_sample = None
    if litellm_manager:
        resource_sample = litellm_manager.get_resource_usage()

    t0 = time.time()
    results = asyncio.run(_generate_load(
        base_url=f"http://127.0.0.1:{proxy_port}",
        target_rps=target_rps,
        duration_s=duration_s,
    ))
    elapsed = time.time() - t0

    if port_monitor:
        port_monitor.stop()
        snaps = port_monitor.get_samples()
        ephem = [s.ephemeral_in_use for s in snaps if s.ephemeral_in_use >= 0]
        port_info = {
            "ephemeral_peak": max(ephem) if ephem else -1,
            "ephemeral_mean": float(np.mean(ephem)) if ephem else -1,
        }
    else:
        port_info = None

    resource_after = None
    if litellm_manager:
        resource_after = litellm_manager.get_resource_usage()

    analysis = _analyze_results(results, t0, elapsed)
    analysis["target_rps"] = target_rps
    analysis["duration_s"] = round(elapsed, 2)
    analysis["port_info"] = port_info
    analysis["resource_before"] = resource_sample
    analysis["resource_after"] = resource_after
    return analysis


# ---------------------------------------------------------------------------
# Ramp test (find the breaking point)
# ---------------------------------------------------------------------------

def run_ramp_test(
    proxy_port: int,
    start_rps: float,
    max_rps: float,
    step_mult: float,
    window_s: float,
    litellm_manager: Optional[LiteLLMManager] = None,
) -> list[dict]:
    """
    Gradually increase RPS by step_mult every window_s seconds.
    Stop when p99 latency exceeds DEGRADE_LATENCY_S or error fraction exceeds
    DEGRADE_ERROR_FRACTION. Returns a list of per-step result dicts.
    """
    ramp_results = []
    rps = start_rps
    step = 0

    print(f"\n[Ramp] start={start_rps} max={max_rps} step_mult={step_mult} window={window_s}s", flush=True)

    while rps <= max_rps:
        step += 1
        print(f"[Ramp] Step {step}: RPS={rps:.0f}", flush=True)

        pt = run_fixed_point(
            proxy_port=proxy_port,
            target_rps=rps,
            duration_s=window_s,
            litellm_manager=litellm_manager,
            monitor_ports=False,
        )
        pt["ramp_step"] = step

        p99 = pt.get("latency_p99_s")
        err_frac = pt.get("error_fraction", 0)
        status = "OK"
        if err_frac >= DEGRADE_ERROR_FRACTION:
            status = "FAIL_ERRORS"
        elif p99 is not None and p99 >= DEGRADE_LATENCY_S:
            status = "FAIL_LATENCY"
        pt["ramp_status"] = status

        print(
            f"       actual_rps={pt['actual_rps']} p99={p99:.3f}s errs={err_frac*100:.1f}% [{status}]",
            flush=True,
        )
        ramp_results.append(pt)

        if status != "OK":
            print(f"[Ramp] Breaking at RPS={rps:.0f}: {status}", flush=True)
            break

        rps = round(rps * step_mult, 1)

    return ramp_results


# ---------------------------------------------------------------------------
# Replay-client max-RPS sweep
# ---------------------------------------------------------------------------

def _replay_client_notes(client_result: dict) -> str:
    if client_result.get("at_ceiling"):
        return ">= ceiling"
    if client_result.get("below_floor"):
        return "below floor"
    if client_result.get("validation_result") is None and not client_result.get("validated"):
        return "validation skipped"
    if not client_result.get("validated"):
        return "val failed, stepped back"
    return ""


def _map_replay_client_bottleneck(client_result: dict) -> str:
    if client_result.get("at_ceiling"):
        return "client"

    validation_result = client_result.get("validation_result") or {}
    bottleneck = validation_result.get("bottleneck")
    if bottleneck == "server":
        return "proxy"
    if bottleneck in {"client", "proxy", "none", "unknown"}:
        return bottleneck
    return "unknown"


def run_find_max_rps_point(proxy_port: int, point_dir: Path, args) -> dict:
    global _ACTIVE_CLIENT_PROC

    point_dir.mkdir(parents=True, exist_ok=True)
    client_work_dir = point_dir / "client_work"
    output_path = point_dir / "bench_client_find_max_rps.json"
    log_path = point_dir / "bench_client.log"

    cmd = [
        args.python,
        str(_BENCH_CLIENT),
        "--find-max-rps",
        "--base-urls", f"http://127.0.0.1:{proxy_port}",
        "--stub-port", str(proxy_port),
        "--payload", args.payload,
        "--num-go-workers", str(args.num_go_workers),
        "--num-go-procs", str(args.num_go_procs),
        "--go-concurrency", str(args.go_concurrency),
        "--rps-start", str(args.rps_start),
        "--max-rps-ceiling", str(args.max_rps_ceiling),
        "--precision", str(args.precision),
        "--probe-duration", str(args.probe_duration),
        "--duration", str(args.replay_client_duration),
        "--python", args.python,
        "--work-dir", str(client_work_dir),
        "--output", str(output_path),
    ]
    if args.skip_validation:
        cmd.append("--skip-validation")
    if args.no_port_monitor:
        cmd.append("--no-port-monitor")

    print(f"[BenchProxy] Running replay-client search: {' '.join(cmd)}", flush=True)
    print(f"[BenchProxy] Replay-client log: {log_path}", flush=True)

    with open(log_path, "w") as log_f:
        proc = subprocess.Popen(cmd, stdout=log_f, stderr=subprocess.STDOUT)
        _ACTIVE_CLIENT_PROC = proc
        try:
            while True:
                retcode = proc.poll()
                if retcode is not None:
                    break
                if _SHUTDOWN_REQUESTED:
                    _terminate_process(proc, "bench_client.py", timeout_s=5.0)
                    raise InterruptedError("Shutdown requested during replay-client point.")
                time.sleep(1)
        finally:
            _ACTIVE_CLIENT_PROC = None

    if retcode != 0:
        raise subprocess.CalledProcessError(retcode, cmd)
    if not output_path.exists():
        raise FileNotFoundError(f"bench_client output not found: {output_path}")

    with open(output_path) as f:
        output_data = json.load(f)

    results = output_data.get("results", [])
    if len(results) != 1:
        raise RuntimeError(f"Expected one bench_client result, found {len(results)} in {output_path}")

    client_result = results[0]
    return {
        "test_type": "replay_client_max_rps",
        "max_rps": client_result.get("max_rps"),
        "validated": client_result.get("validated", False),
        "at_ceiling": client_result.get("at_ceiling", False),
        "below_floor": client_result.get("below_floor", False),
        "bottleneck": _map_replay_client_bottleneck(client_result),
        "notes": _replay_client_notes(client_result),
        "client_result_path": str(output_path),
        "client_log_path": str(log_path),
        "search_history": client_result.get("search_history", []),
        "validation_result": client_result.get("validation_result"),
    }


def run_replay_client_sweep(
    litellm_python: str,
    sweep_dims: set,
    fixed_litellm_workers: int,
    fixed_num_backends: int,
    work_dir: Path,
    stub_latency_ms: float,
    args,
) -> dict:
    global _ACTIVE_LITELLM_MGR, _ACTIVE_STUB_MGR, _ACTIVE_CHECKPOINT, _ACTIVE_CHECKPOINT_PATH

    lw_list = SWEEP_LITELLM_WORKERS if "litellm_workers" in sweep_dims else [fixed_litellm_workers]
    backends_list = [b for b in SWEEP_BACKENDS if b <= 8] if "backends" in sweep_dims else [fixed_num_backends]
    total = len(lw_list) * len(backends_list)

    checkpoint = _load_checkpoint(work_dir) if args.resume else {"meta": {}, "completed": {}}
    checkpoint["meta"] = {
        **checkpoint.get("meta", {}),
        "timestamp_start": checkpoint.get("meta", {}).get("timestamp_start") or datetime.now().isoformat(),
        "mode": "replay_client_proxy_sweep",
        "litellm_python": litellm_python,
        "routing": REPLAY_CLIENT_ROUTING,
        "client_config": {
            "python": args.python,
            "go_concurrency": args.go_concurrency,
            "num_go_procs": args.num_go_procs,
            "num_go_workers": args.num_go_workers,
            "payload": args.payload,
            "rps_start": args.rps_start,
            "max_rps_ceiling": args.max_rps_ceiling,
            "precision": args.precision,
            "probe_duration": args.probe_duration,
            "replay_client_duration": args.replay_client_duration,
            "skip_validation": args.skip_validation,
        },
    }
    checkpoint.setdefault("completed", {})
    _ACTIVE_CHECKPOINT = checkpoint
    _ACTIVE_CHECKPOINT_PATH = _checkpoint_path(work_dir)

    print(f"[BenchProxy] Replay-client sweep plan: {total} point(s)", flush=True)
    print(f"  litellm_workers: {lw_list}", flush=True)
    print(f"  backends:        {backends_list}", flush=True)
    print(f"  routing:         {REPLAY_CLIENT_ROUTING}", flush=True)
    print(f"  resume:          {args.resume}", flush=True)

    point_idx = 0
    for lw in lw_list:
        for n_backends in backends_list:
            key = _config_key(lw, n_backends)
            if key in checkpoint["completed"]:
                point_idx += 1
                print(f"[BenchProxy] Point {point_idx}/{total}: skipping completed {key}", flush=True)
                continue
            if _SHUTDOWN_REQUESTED:
                print("[BenchProxy] Shutdown requested before next point; stopping sweep.", flush=True)
                break

            point_idx += 1
            print(f"\n[BenchProxy] Point {point_idx}/{total}: lw={lw} backends={n_backends}", flush=True)

            point_dir = work_dir / key
            stub_mgr = StubServerManager(
                n=n_backends,
                python=sys.executable,
                latency_ms=stub_latency_ms,
            )
            litellm_mgr = None
            result = None
            try:
                _ACTIVE_STUB_MGR = stub_mgr
                backend_ports = stub_mgr.start()
                proxy_port = _find_free_port(14000)

                litellm_mgr = LiteLLMManager(
                    python=litellm_python,
                    port=proxy_port,
                    num_workers=lw,
                    routing_strategy=REPLAY_CLIENT_ROUTING,
                    work_dir=point_dir,
                )
                _ACTIVE_LITELLM_MGR = litellm_mgr
                litellm_mgr.generate_config(backend_ports)
                litellm_mgr.start()
                healthy = litellm_mgr.wait_healthy()

                if not healthy:
                    result = {
                        "test_type": "replay_client_max_rps",
                        "litellm_workers": lw,
                        "routing": REPLAY_CLIENT_ROUTING,
                        "num_backends": n_backends,
                        "max_rps": None,
                        "validated": False,
                        "at_ceiling": False,
                        "below_floor": False,
                        "bottleneck": "unknown",
                        "notes": "litellm_failed_to_start",
                        "client_result_path": None,
                        "error": "litellm_failed_to_start",
                    }
                else:
                    result = run_find_max_rps_point(proxy_port=proxy_port, point_dir=point_dir, args=args)
                    result["litellm_workers"] = lw
                    result["routing"] = REPLAY_CLIENT_ROUTING
                    result["num_backends"] = n_backends
            except InterruptedError:
                print(f"[BenchProxy] Interrupted during {key}; leaving it incomplete for resume.", flush=True)
                result = None
            except Exception as exc:
                result = {
                    "test_type": "replay_client_max_rps",
                    "litellm_workers": lw,
                    "routing": REPLAY_CLIENT_ROUTING,
                    "num_backends": n_backends,
                    "max_rps": None,
                    "validated": False,
                    "at_ceiling": False,
                    "below_floor": False,
                    "bottleneck": "unknown",
                    "notes": str(exc),
                    "client_result_path": None,
                    "error": type(exc).__name__,
                }
            finally:
                if litellm_mgr is not None:
                    litellm_mgr.stop()
                stub_mgr.stop()
                _ACTIVE_LITELLM_MGR = None
                _ACTIVE_STUB_MGR = None

            if result is None:
                break

            checkpoint["completed"][key] = result
            _save_checkpoint(work_dir, checkpoint)

            if _SHUTDOWN_REQUESTED:
                break

            time.sleep(2)

        if _SHUTDOWN_REQUESTED:
            break

    ordered_results = []
    for lw in lw_list:
        for n_backends in backends_list:
            key = _config_key(lw, n_backends)
            if key in checkpoint["completed"]:
                ordered_results.append(checkpoint["completed"][key])

    return {
        "meta": checkpoint["meta"],
        "results": ordered_results,
        "checkpoint": checkpoint,
    }


# ---------------------------------------------------------------------------
# Full sweep runner
# ---------------------------------------------------------------------------

def run_sweep(
    litellm_python: str,
    sweep_dims: set,
    fixed_litellm_workers: int,
    fixed_routing: str,
    fixed_num_backends: int,
    fixed_rps: float,
    duration_s: float,
    work_dir: Path,
    do_ramp: bool,
    ramp_start: float,
    ramp_max: float,
    ramp_step: float,
    ramp_window: float,
    monitor_ports: bool,
    stub_latency_ms: float,
) -> list[dict]:
    global _ACTIVE_LITELLM_MGR, _ACTIVE_STUB_MGR

    lw_list      = SWEEP_LITELLM_WORKERS if "litellm_workers" in sweep_dims else [fixed_litellm_workers]
    routing_list = SWEEP_ROUTING         if "routing"         in sweep_dims else [fixed_routing]
    backends_list= SWEEP_BACKENDS        if "backends"        in sweep_dims else [fixed_num_backends]
    rps_list     = SWEEP_RPS             if "rps"             in sweep_dims else [fixed_rps]

    if do_ramp:
        rps_list = [fixed_rps]  # ramp overrides rps sweep

    total = len(lw_list) * len(routing_list) * len(backends_list) * len(rps_list)
    print(f"[BenchProxy] Sweep plan: {total} point(s)")
    print(f"  litellm_workers: {lw_list}")
    print(f"  routing:         {routing_list}")
    print(f"  backends:        {backends_list}")
    print(f"  rps:             {rps_list}")
    print(f"  ramp:            {do_ramp}\n")

    all_results = []
    point_idx = 0

    for n_backends, lw, routing in itertools.product(backends_list, lw_list, routing_list):
        if _SHUTDOWN_REQUESTED:
            print("[BenchProxy] Shutdown requested; stopping remaining fixed/ramp sweep points.", flush=True)
            break

        # --- Start stub backends ---
        stub_mgr = StubServerManager(
            n=n_backends,
            python=sys.executable,  # stub uses same env; doesn't need litellm venv
            latency_ms=stub_latency_ms,
        )
        _ACTIVE_STUB_MGR = stub_mgr
        backend_ports = stub_mgr.start()
        proxy_port = _find_free_port(14000)

        # --- Start LiteLLM proxy ---
        litellm_mgr = LiteLLMManager(
            python=litellm_python,
            port=proxy_port,
            num_workers=lw,
            routing_strategy=routing,
            work_dir=work_dir,
        )
        _ACTIVE_LITELLM_MGR = litellm_mgr
        litellm_mgr.generate_config(backend_ports)
        litellm_mgr.start()
        healthy = litellm_mgr.wait_healthy()

        if not healthy:
            print(f"[BenchProxy] LiteLLM failed to start — skipping (lw={lw}, routing={routing})", flush=True)
            litellm_mgr.stop()
            stub_mgr.stop()
            _ACTIVE_LITELLM_MGR = None
            _ACTIVE_STUB_MGR = None
            all_results.append({
                "litellm_workers": lw, "routing": routing, "num_backends": n_backends,
                "error": "litellm_failed_to_start",
            })
            continue

        # --- Run tests ---
        if do_ramp:
            point_idx += 1
            print(f"\n[BenchProxy] Ramp {point_idx}: lw={lw} routing={routing} backends={n_backends}")
            ramp = run_ramp_test(
                proxy_port=proxy_port,
                start_rps=ramp_start,
                max_rps=ramp_max,
                step_mult=ramp_step,
                window_s=ramp_window,
                litellm_manager=litellm_mgr,
            )
            all_results.append({
                "litellm_workers": lw,
                "routing": routing,
                "num_backends": n_backends,
                "test_type": "ramp",
                "ramp_steps": ramp,
                "max_sustainable_rps": _max_sustainable_rps(ramp),
            })
        else:
            for rps in rps_list:
                point_idx += 1
                print(f"\n[BenchProxy] Point {point_idx}/{total}: "
                      f"lw={lw} routing={routing} backends={n_backends} rps={rps}")
                pt = run_fixed_point(
                    proxy_port=proxy_port,
                    target_rps=rps,
                    duration_s=duration_s,
                    litellm_manager=litellm_mgr,
                    monitor_ports=monitor_ports,
                )
                pt["litellm_workers"] = lw
                pt["routing"] = routing
                pt["num_backends"] = n_backends
                pt["test_type"] = "fixed"
                all_results.append(pt)
                print(
                    f"  actual_rps={pt['actual_rps']} "
                    f"p99={pt.get('latency_p99_s'):.3f}s "
                    f"errors={pt['error_fraction']*100:.1f}%"
                )

        litellm_mgr.stop()
        stub_mgr.stop()
        _ACTIVE_LITELLM_MGR = None
        _ACTIVE_STUB_MGR = None
        time.sleep(2)  # brief pause between configurations

    return all_results


def _max_sustainable_rps(ramp_steps: list[dict]) -> Optional[float]:
    """Return the highest RPS with status OK in a ramp test."""
    ok_steps = [s for s in ramp_steps if s.get("ramp_status") == "OK"]
    if not ok_steps:
        return None
    return max(s["target_rps"] for s in ok_steps)


# ---------------------------------------------------------------------------
# Summary table
# ---------------------------------------------------------------------------

def print_summary_table(results: list[dict]):
    header = (
        f"{'lw':>4} {'routing':>20} {'backends':>8} {'rps_tgt':>8} "
        f"{'rps_act':>8} {'p99_s':>7} {'err%':>6} {'cpu%':>6} {'mem_mb':>8}"
    )
    print("\n" + "=" * len(header))
    print(header)
    print("-" * len(header))
    for r in results:
        if "error" in r and r.get("test_type") is None:
            print(f"  ERROR: {r}")
            continue
        if r.get("test_type") == "ramp":
            max_rps = r.get("max_sustainable_rps")
            print(
                f"{r['litellm_workers']:>4} {r['routing']:>20} {r['num_backends']:>8} "
                f"{'RAMP':>8} {max_rps or 'N/A':>8} {'N/A':>7} {'N/A':>6} {'N/A':>6} {'N/A':>8}"
            )
            continue
        p99 = f"{r['latency_p99_s']:.3f}" if r.get("latency_p99_s") else "N/A"
        err = f"{r['error_fraction']*100:.1f}" if "error_fraction" in r else "N/A"
        res_after = r.get("resource_after") or {}
        cpu = f"{res_after.get('cpu_percent', 'N/A')}"
        mem = f"{res_after.get('mem_mb', 'N/A')}"
        print(
            f"{r['litellm_workers']:>4} {r['routing']:>20} {r['num_backends']:>8} "
            f"{r['target_rps']:>8} {r['actual_rps']:>8} {p99:>7} {err:>6} {cpu:>6} {mem:>8}"
        )
    print("=" * len(header) + "\n")


def print_replay_client_summary_table(results: list[dict]):
    header = (
        f"{'lw':>4} {'backends':>8} {'max_rps':>10} "
        f"{'bottleneck':>10} {'validated':>10} {'notes':>24}"
    )
    print("\n" + "=" * len(header))
    print("  REPLAY-CLIENT PROXY SWEEP")
    print("=" * len(header))
    print(header)
    print("-" * len(header))
    for r in results:
        max_rps = r.get("max_rps")
        max_rps_label = f"{max_rps:.1f}" if isinstance(max_rps, (int, float)) else "N/A"
        print(
            f"{r.get('litellm_workers', '?'):>4} "
            f"{r.get('num_backends', '?'):>8} "
            f"{max_rps_label:>10} "
            f"{r.get('bottleneck', 'unknown'):>10} "
            f"{'YES' if r.get('validated') else 'NO':>10} "
            f"{r.get('notes', ''):>24}"
        )
    print("=" * len(header) + "\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark LiteLLM proxy throughput, latency, and failure modes.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--litellm-python", type=str, required=True,
        help="Path to the Python interpreter in the LiteLLM venv (e.g. /path/to/venv/bin/python).",
    )
    parser.add_argument(
        "--sweep", type=str, default="",
        help=(
            "Comma-separated dimensions to sweep: litellm_workers, routing, backends, rps. "
            "E.g. --sweep litellm_workers,routing"
        ),
    )
    parser.add_argument("--use-replay-client", action="store_true",
                        help="Use bench_client.py --find-max-rps instead of the built-in httpx load generator.")

    # Fixed-point parameters
    parser.add_argument("--litellm-workers", type=int,   default=4,            help="LiteLLM num_workers (fixed).")
    parser.add_argument("--routing",         type=str,   default="least-busy", help="Routing strategy (fixed).")
    parser.add_argument("--num-backends",    type=int,   default=4,            help="Number of stub backends (fixed).")
    parser.add_argument("--rps",             type=float, default=100.0,        help="Target RPS for fixed tests.")
    parser.add_argument("--duration",        type=float, default=30.0,         help="Duration per sweep point (s).")

    # Ramp test
    parser.add_argument("--ramp",        action="store_true", help="Run a ramp test to find LiteLLM's breaking point.")
    parser.add_argument("--ramp-start",  type=float, default=10.0,  help="Starting RPS for ramp test.")
    parser.add_argument("--ramp-max",    type=float, default=2000.0,help="Maximum RPS for ramp test.")
    parser.add_argument("--ramp-step",   type=float, default=1.2,   help="RPS multiplier per step (e.g. 1.2 = +20%%).")
    parser.add_argument("--ramp-window", type=float, default=10.0,  help="Duration of each ramp step (s).")

    # Replay-client max-RPS mode
    parser.add_argument("--go-concurrency", type=int, default=40,
                        help="Go replay_client in-flight concurrency for --use-replay-client mode.")
    parser.add_argument("--num-go-procs", type=int, default=8,
                        help="Number of Go replay_client processes for --use-replay-client mode.")
    parser.add_argument("--num-go-workers", type=int, default=2,
                        help="Number of Go dispatch workers for --use-replay-client mode.")
    parser.add_argument("--payload", type=str, default="medium",
                        choices=["small", "medium", "large", "xl"],
                        help="Payload size for --use-replay-client mode.")
    parser.add_argument("--rps-start", type=float, default=100.0,
                        help="Starting RPS for replay-client max-RPS search.")
    parser.add_argument("--max-rps-ceiling", type=float, default=500000.0,
                        help="Upper RPS ceiling for replay-client max-RPS search.")
    parser.add_argument("--precision", type=float, default=0.20,
                        help="Convergence threshold for replay-client max-RPS search.")
    parser.add_argument("--probe-duration", type=float, default=2.0,
                        help="Per-probe duration (s) for replay-client max-RPS search.")
    parser.add_argument("--replay-client-duration", type=float, default=5.0,
                        help="Full validation duration (s) when replay-client validation is enabled.")
    parser.add_argument("--skip-validation", action="store_true",
                        help="Skip replay-client full-duration validation probes.")
    parser.add_argument("--python", type=str, default=sys.executable,
                        help="Python interpreter to use for bench_client.py in replay-client mode.")
    parser.add_argument("--resume", action="store_true",
                        help=f"Resume a replay-client sweep from {CHECKPOINT_FILENAME} in --work-dir.")

    # Stub server
    parser.add_argument("--stub-latency-ms", type=float, default=0.0,
                        help="Artificial latency in stub backends (ms).")

    # Output
    parser.add_argument("--work-dir", type=str, default=None,
                        help="Directory for LiteLLM configs and logs.")
    parser.add_argument("--output",   type=str, default=None,
                        help="Output JSON file path.")
    parser.add_argument("--no-port-monitor", action="store_true",
                        help="Disable live port monitoring.")

    args = parser.parse_args()

    sweep_dims = set(d.strip() for d in args.sweep.split(",") if d.strip())

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    work_dir = Path(args.work_dir) if args.work_dir else _DEFAULT_BENCH_RESULTS_DIR / f"proxy_run_{ts}"
    work_dir.mkdir(parents=True, exist_ok=True)

    if args.use_replay_client:
        invalid_dims = sweep_dims - {"litellm_workers", "backends"}
        if args.ramp:
            parser.error("--ramp is not supported with --use-replay-client.")
        if invalid_dims:
            parser.error("--use-replay-client only supports sweep dims: litellm_workers, backends.")
        if "routing" in sweep_dims or "rps" in sweep_dims:
            parser.error("--use-replay-client does not allow routing or rps sweeps.")
        if args.num_backends not in {1, 2, 4, 8} and "backends" not in sweep_dims:
            parser.error("--use-replay-client expects --num-backends to be one of 1,2,4,8.")
        if not _BENCH_CLIENT.exists():
            parser.error(f"bench_client.py not found at {_BENCH_CLIENT}")
        if args.routing != REPLAY_CLIENT_ROUTING:
            print(
                f"[BenchProxy] Warning: forcing routing={REPLAY_CLIENT_ROUTING} in --use-replay-client mode.",
                flush=True,
            )

        replay_data = run_replay_client_sweep(
            litellm_python=args.litellm_python,
            sweep_dims=sweep_dims,
            fixed_litellm_workers=args.litellm_workers,
            fixed_num_backends=args.num_backends,
            work_dir=work_dir,
            stub_latency_ms=args.stub_latency_ms,
            args=args,
        )
        results = replay_data["results"]
        print_replay_client_summary_table(results)
        output_data = {
            "meta": {
                **replay_data["meta"],
                "timestamp_end": datetime.now().isoformat(),
                "sweep_dims": list(sweep_dims),
                "resume": args.resume,
                "checkpoint_path": str(_checkpoint_path(work_dir)),
            },
            "results": results,
        }
    else:
        results = run_sweep(
            litellm_python=args.litellm_python,
            sweep_dims=sweep_dims,
            fixed_litellm_workers=args.litellm_workers,
            fixed_routing=args.routing,
            fixed_num_backends=args.num_backends,
            fixed_rps=args.rps,
            duration_s=args.duration,
            work_dir=work_dir,
            do_ramp=args.ramp,
            ramp_start=args.ramp_start,
            ramp_max=args.ramp_max,
            ramp_step=args.ramp_step,
            ramp_window=args.ramp_window,
            monitor_ports=not args.no_port_monitor,
            stub_latency_ms=args.stub_latency_ms,
        )

        print_summary_table(results)
        output_data = {
            "meta": {
                "timestamp": ts,
                "mode": "fixed_or_ramp_proxy_sweep",
                "litellm_python": args.litellm_python,
                "sweep_dims": list(sweep_dims),
                "ramp": args.ramp,
                "duration_s": args.duration,
            },
            "results": results,
        }

    out_path = Path(args.output) if args.output else work_dir / f"proxy_sweep_{ts}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(output_data, f, indent=2)
    print(f"[BenchProxy] Results saved to {out_path}")


if __name__ == "__main__":
    main()
