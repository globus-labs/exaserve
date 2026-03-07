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
sys.path.insert(0, str(_BENCH_DIR))
from port_model import PortMonitor, EPHEMERAL_RANGE

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SWEEP_LITELLM_WORKERS  = [1, 2, 4, 8]
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
        for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
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
        if hasattr(self, "_log_f"):
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
        # --- Start stub backends ---
        stub_mgr = StubServerManager(
            n=n_backends,
            python=sys.executable,  # stub uses same env; doesn't need litellm venv
            latency_ms=stub_latency_ms,
        )
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
        litellm_mgr.generate_config(backend_ports)
        litellm_mgr.start()
        healthy = litellm_mgr.wait_healthy()

        if not healthy:
            print(f"[BenchProxy] LiteLLM failed to start — skipping (lw={lw}, routing={routing})", flush=True)
            litellm_mgr.stop()
            stub_mgr.stop()
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
    parser.add_argument("--ramp-step",   type=float, default=1.2,   help="RPS multiplier per step (e.g. 1.2 = +20%).")
    parser.add_argument("--ramp-window", type=float, default=10.0,  help="Duration of each ramp step (s).")

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
    work_dir = Path(args.work_dir) if args.work_dir else _BENCH_DIR / "results" / f"proxy_run_{ts}"
    work_dir.mkdir(parents=True, exist_ok=True)

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

    out_path = Path(args.output) if args.output else work_dir / f"proxy_sweep_{ts}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    output_data = {
        "meta": {
            "timestamp": ts,
            "litellm_python": args.litellm_python,
            "sweep_dims": list(sweep_dims),
            "ramp": args.ramp,
            "duration_s": args.duration,
        },
        "results": results,
    }
    with open(out_path, "w") as f:
        json.dump(output_data, f, indent=2)
    print(f"[BenchProxy] Results saved to {out_path}")


if __name__ == "__main__":
    main()
