"""
Client capacity benchmark — drives the real eval/replay_client.py.

For each sweep point:
  1. Generate a synthetic JSONL trace at the target RPS / duration
  2. Write a minimal YAML config pointing replay_client at the stub server
  3. Invoke eval/replay_client.py as a subprocess with --proxy-port <stub_port>
  4. Parse the result JSON it saves and extract dispatch / latency metrics
  5. Mark the point SKIP if it exceeds the wall-clock timeout

This ensures the benchmark measures the actual replay_client, not a
re-implementation that only "follows the same pattern".

Sweep dimensions supported:
  workers   -- num_go_workers  [1, 2, 4, 8, 16, 32]
  rps       -- target RPS     [10, 50, 100, 200, 500, 1000, 2000]
  payload   -- prompt/output size  [small, medium, large, xl]

Note: HTTP version and connection pool size are not sweep dimensions here
because replay_client always uses HTTP/2 and auto-sizes the pool from the
file-descriptor limit. Use bench_client's own mode (--standalone) for those.

Usage:
  # Sweep workers x RPS against stub server on port 8000
  python bench_client.py --sweep workers,rps --stub-port 8000 --duration 20

  # Single point
  python bench_client.py --workers 8 --rps 200 --payload large

  # Against null_compute Ray Serve directly
  python bench_client.py --stub-port 8000 --workers 4 --rps 100
"""

import argparse
import glob
import itertools
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import urllib.request
import urllib.error

import numpy as np
import yaml

_BENCH_DIR = Path(__file__).parent
_REPO_ROOT = _BENCH_DIR.parent
_REPLAY_CLIENT = _REPO_ROOT / "eval" / "replay_client.py"

sys.path.insert(0, str(_BENCH_DIR))
from port_model import predict as port_predict, PortMonitor, EPHEMERAL_RANGE

# ---------------------------------------------------------------------------
# Sweep definitions (same names as before for CLI compatibility)
# ---------------------------------------------------------------------------

SWEEP_WORKERS = [1, 2, 4, 8, 16, 32]
SWEEP_RPS     = [10, 100, 1000, 5000, 10000]

PAYLOAD_SIZES = {
    "small":  {"prompt_words": 20,   "output_len": 20},
    "medium": {"prompt_words": 200,  "output_len": 100},
    "large":  {"prompt_words": 2000, "output_len": 500},
    "xl":     {"prompt_words": 8000, "output_len": 1000},
}

# A dispatch overhead fraction above this means the client is falling behind.
# replay_client reports overhead_s = actual_dispatch_s - trace_span_s.
# We flag "behind" when the overrun exceeds 5 % of the trace duration.
KEEPUP_OVERHEAD_FRACTION = 0.05
# If stub server received RPS is below this fraction of target, flag as server bottleneck
SERVER_BOTTLENECK_FRACTION = 0.90
MODEL_ID = "stub-model"


# ---------------------------------------------------------------------------
# Stub server metrics helpers
# ---------------------------------------------------------------------------

def _fetch_stub_metrics(stub_port: int, timeout_s: float = 2.0) -> Optional[dict]:
    """Fetch the /metrics JSON from the stub server. Returns None on failure."""
    try:
        url = f"http://127.0.0.1:{stub_port}/metrics"
        with urllib.request.urlopen(url, timeout=timeout_s) as resp:
            return json.loads(resp.read())
    except Exception:
        return None


def _stub_server_rps(after: Optional[dict], stub_workers: int = 1) -> Optional[float]:
    """
    Estimate stub server RPS from a single post-run /metrics snapshot.

    Uses the server's own rps_last_10s sliding window (measured over the last
    10 seconds of the run) multiplied by stub_workers to account for multiple
    SO_REUSEPORT processes.

    The delta-based approach (before/after total_requests) is unreliable with
    multiple processes because each /metrics request hits a random process,
    making before/after snapshots potentially come from different processes and
    yield negative or nonsensical deltas.
    """
    if after is None:
        return None
    rps_per_process = after.get("rps_last_10s")
    if rps_per_process is None:
        return None
    total = rps_per_process * max(1, stub_workers)
    return max(0.0, total)  # clamp; never report negative


# ---------------------------------------------------------------------------
# Trace generation
# ---------------------------------------------------------------------------

def generate_trace(
    target_rps: float,
    duration_s: float,
    payload_size: str,
    trace_path: str,
    model: str = MODEL_ID,
):
    """Write a JSONL trace file readable by replay_client.py."""
    size = PAYLOAD_SIZES.get(payload_size, PAYLOAD_SIZES["medium"])
    prompt = ("benchmark " * size["prompt_words"]).strip()
    interval = 1.0 / target_rps
    n = max(1, int(target_rps * duration_s))

    t0 = time.time()
    with open(trace_path, "w") as f:
        # Optional metadata line
        f.write(json.dumps({"__type__": "metadata", "timestamp": datetime.now().isoformat()}) + "\n")
        for i in range(n):
            f.write(json.dumps({
                "timestamp": round(i * interval, 6),
                "model": model,
                "prompt": prompt,
                "input_len": size["prompt_words"],   # rough token approximation
                "output_len": size["output_len"],
                "tensor_parallel_size": 1,
                "mode": "chat",
            }) + "\n")
    elapsed = time.time() - t0
    size_mb = os.path.getsize(trace_path) / (1024 * 1024)
    print(f"  [trace] Generated {n} requests ({size_mb:.1f} MB) in {elapsed:.2f}s", flush=True)


# ---------------------------------------------------------------------------
# Config generation
# ---------------------------------------------------------------------------

def generate_config(
    stub_port: int,
    trace_path: str,
    result_dir: str,
    model: str = MODEL_ID,
    go_concurrency: int = 2000,
    num_go_workers: int = 4,
    sum_only: bool = False,
    num_go_procs: int = 1,
    num_cli_nodes: int = 1,
) -> str:
    """Write a minimal YAML config for replay_client.py and return its path."""
    cfg = {
        "port": stub_port,
        "job_trace_config": {
            "output_trace_path": trace_path,
        },
        "job_replay_client_config": {
            "generation_mode": "deterministic",
            "num_nodes": num_cli_nodes,
            "num_runs": 1,
            "dest": "proxy",
            "go_concurrency": go_concurrency,
            "num_go_workers": num_go_workers,
            "sum_only": sum_only,
            "num_go_procs": num_go_procs,
        },
        "model_deployment_config": {
            "num_nodes": 1,
            "num_gpus_per_node": 1,
            "model_configs": [{"model_id": model, "mode": "chat"}],
        },
        "proxy_config": {"type": "none"},
        "pbs_result_dir": result_dir,
    }
    config_path = os.path.join(os.path.dirname(trace_path), "config.yaml")
    with open(config_path, "w") as f:
        yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)
    return config_path


# ---------------------------------------------------------------------------
# Result parsing
# ---------------------------------------------------------------------------

def _find_latest_result(result_dir: str) -> Optional[str]:
    """Return the path of the most recently written result*.json file."""
    pattern = os.path.join(result_dir, "result*.json")
    files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
    return files[0] if files else None


def parse_result(result_dir: str) -> Optional[dict]:
    """Read the result JSON written by replay_client and return a flat metrics dict."""
    path = _find_latest_result(result_dir)
    if not path:
        return None
    try:
        with open(path) as f:
            data = json.load(f)
    except Exception as e:
        print(f"  [WARN] Could not read result file {path}: {e}", flush=True)
        return None

    overall = data.get("overall", {})
    meta = data.get("meta", {})
    dispatch_timings = meta.get("dispatch_timings", [])
    dt = dispatch_timings[-1] if dispatch_timings else {}

    trace_span    = dt.get("trace_span_s")
    actual_disp   = dt.get("actual_dispatch_s")
    overhead      = dt.get("overhead_s")         # actual_dispatch - trace_span

    return {
        "actual_rps":          overall.get("rps"),
        "latency_p50_s":       overall.get("p50_s"),
        "latency_p99_s":       overall.get("p99_s"),
        "requests_completed":  overall.get("requests_completed"),
        "requests_scheduled":  overall.get("requests_scheduled"),
        "errors":              overall.get("errors", 0),
        "duration_s":          overall.get("duration_s"),
        "trace_span_s":        trace_span,
        "actual_dispatch_s":   actual_disp,
        "dispatch_overhead_s": overhead,
    }


# ---------------------------------------------------------------------------
# Single sweep point
# ---------------------------------------------------------------------------

def run_sweep_point(
    stub_port: int,
    target_rps: float,
    num_go_workers: int,
    payload_size: str,
    duration_s: float,
    max_wall_s: float,
    python: str,
    work_dir: str,
    monitor_ports: bool,
    stub_workers: int = 1,
    model: str = MODEL_ID,
    go_concurrency: int = 2000,
    sum_only: bool = False,
    num_go_procs: int = 1,
    base_urls: str = None,
    cpuprofile_dir: str = "",
    mpi_hostfile: str = None,
    num_cli_nodes: int = 1,
) -> dict:
    """
    Run one (rps, workers, payload) combination using the real replay_client.py.
    Returns a flat metrics dict; 'timed_out' is set if the process was killed.
    """
    point_id = f"w{num_go_workers}_r{int(target_rps)}_{payload_size}"
    point_dir = os.path.join(work_dir, point_id)
    result_dir = os.path.join(point_dir, "results")
    os.makedirs(result_dir, exist_ok=True)

    trace_path  = os.path.join(point_dir, "trace.jsonl")
    config_path = generate_config(stub_port, trace_path, result_dir, model,
                                  go_concurrency=go_concurrency,
                                  num_go_workers=num_go_workers,
                                  sum_only=sum_only,
                                  num_go_procs=num_go_procs,
                                  num_cli_nodes=num_cli_nodes)
    generate_trace(target_rps, duration_s, payload_size, trace_path, model)

    if num_cli_nodes > 1 and mpi_hostfile:
        cmd = [
            "mpiexec", "-n", str(num_cli_nodes),
            "--ppn", "1", "--cpu-bind", "none",
            "--hostfile", mpi_hostfile,
            python, str(_REPLAY_CLIENT),
            "--config",              config_path,
            "--dest",                "proxy",
        ]
    else:
        cmd = [
            python, str(_REPLAY_CLIENT),
            "--config",              config_path,
            "--dest",                "proxy",
        ]
    if base_urls:
        cmd += ["--base-urls", base_urls]
    else:
        cmd += ["--proxy-port", str(stub_port)]
    if cpuprofile_dir:
        prof_dir = os.path.join(point_dir, "profiles")
        os.makedirs(prof_dir, exist_ok=True)
        cmd += ["--cpuprofile-dir", prof_dir]

    # Port prediction
    pred = port_predict(
        rps=target_rps,
        num_workers=num_go_workers,
        http_version="2",   # replay_client always uses HTTP/2
        num_client_nodes=num_cli_nodes,
    )
    if pred.status != "OK":
        print(f"  [PORT WARN] {pred.warnings[0]}", flush=True)

    # Optional live port monitor
    port_monitor = None
    if monitor_ports:
        port_monitor = PortMonitor(interval_s=1.0)
        port_monitor.start()

    # (No before-snapshot needed; we use the server's rps_last_10s sliding window
    # queried once after the run finishes, scaled by stub_workers.)

    # Run replay_client as a subprocess, killing it if max_wall_s exceeded
    timed_out = False
    log_path  = os.path.join(point_dir, "replay_client.log")
    t0 = time.time()

    with open(log_path, "w") as log_f:
        proc = subprocess.Popen(cmd, stdout=log_f, stderr=subprocess.STDOUT)
        try:
            proc.wait(timeout=max_wall_s)
        except subprocess.TimeoutExpired:
            timed_out = True
            print(
                f"\n  [TIMEOUT] Wall-clock limit {max_wall_s:.0f}s exceeded — "
                f"killing replay_client (pid={proc.pid})",
                flush=True,
            )
            proc.send_signal(signal.SIGTERM)
            time.sleep(2)
            if proc.poll() is None:
                proc.kill()
                proc.wait()

    elapsed = time.time() - t0

    # Snapshot stub server metrics after run (sliding-window RPS, scaled by worker count)
    stub_metrics_after = _fetch_stub_metrics(stub_port)
    server_rps = _stub_server_rps(stub_metrics_after, stub_workers=stub_workers)

    # Stop port monitor
    port_snap_summary = None
    if port_monitor:
        port_monitor.stop()
        snaps = port_monitor.get_samples()
        if snaps:
            ephem = [s.ephemeral_in_use for s in snaps if s.ephemeral_in_use >= 0]
            port_snap_summary = {
                "ephemeral_peak": max(ephem) if ephem else -1,
                "time_wait_peak": max(s.time_wait for s in snaps),
            }

    # Parse the result file replay_client saved (if it finished or managed to write one)
    parsed = parse_result(result_dir) if not timed_out else None

    overhead = (parsed or {}).get("dispatch_overhead_s")
    span     = (parsed or {}).get("trace_span_s")
    actual_rps = (parsed or {}).get("actual_rps")
    client_duration = (parsed or {}).get("duration_s")  # from effective_run_t0 to last response
    if overhead is not None and span and span > 0:
        dispatch_ok = (overhead / span) < KEEPUP_OVERHEAD_FRACTION
        rps_ok = (actual_rps is not None and actual_rps >= target_rps * SERVER_BOTTLENECK_FRACTION)
        # Duration sanity: use the client-measured duration (effective_run_t0 to
        # last response), NOT wall-clock elapsed (which includes trace generation,
        # Go startup, and result saving).  The client duration should not exceed
        # 2× the trace span for a healthy run.
        duration_ok = (client_duration is not None and client_duration <= span * 2.0)
        can_keep_up = dispatch_ok and rps_ok and duration_ok
    elif timed_out:
        can_keep_up = False
    else:
        can_keep_up = False

    # Determine bottleneck only when the client failed to keep up.
    # With SO_REUSEPORT, stub server /metrics hits a random process, so
    # server_rps (rps_last_10s × stub_workers) is unreliable.  Instead,
    # compare client-side actual_rps against target: if actual_rps is high
    # but duration/overhead failed, it's a client-side issue (saving, startup);
    # if actual_rps is low, the server couldn't handle the load.
    bottleneck = "unknown"
    if can_keep_up:
        bottleneck = "none"
    elif not timed_out and actual_rps is not None:
        client_fraction = actual_rps / max(target_rps, 1.0)
        if client_fraction < SERVER_BOTTLENECK_FRACTION:
            bottleneck = "server"
            print(
                f"  [BOTTLENECK] Client achieved only {actual_rps:.0f} rps "
                f"({client_fraction*100:.0f}% of target {target_rps:.0f}) — "
                f"server may be the bottleneck (scale with --stub-workers).",
                flush=True,
            )
        else:
            bottleneck = "client"
    elif not can_keep_up:
        bottleneck = "client"   # can't keep up but no data → assume client

    n_scheduled = max(1, int(target_rps * duration_s))

    return {
        # Identity
        "target_rps":           target_rps,
        "num_workers":          num_go_workers,
        "payload_size":         payload_size,
        "duration_s":           duration_s,
        "max_wall_s":           max_wall_s,
        "timed_out":            timed_out,
        # Metrics from replay_client result
        "actual_rps":           (parsed or {}).get("actual_rps"),
        "latency_p50_s":        (parsed or {}).get("latency_p50_s"),
        "latency_p99_s":        (parsed or {}).get("latency_p99_s"),
        "requests_completed":   (parsed or {}).get("requests_completed", 0),
        "requests_scheduled":   (parsed or {}).get("requests_scheduled", n_scheduled),
        "errors":               (parsed or {}).get("errors", 0),
        "trace_span_s":         span,
        "actual_dispatch_s":    (parsed or {}).get("actual_dispatch_s"),
        "dispatch_overhead_s":  overhead,
        # Derived
        "can_keep_up":          can_keep_up,
        "bottleneck":           bottleneck,
        "server_rps":           round(server_rps, 2) if server_rps is not None else None,
        "wall_elapsed_s":       round(elapsed, 2),
        # Port info
        "port_prediction": {
            "active_conns": round(pred.active_connections_per_node, 1),
            "peak_ports":   round(pred.peak_ports_per_node, 1),
            "fraction":     round(pred.fraction_used, 4),
            "status":       pred.status,
        },
        "port_monitor": port_snap_summary,
        # Path to replay_client log for debugging
        "log": log_path,
    }


# ---------------------------------------------------------------------------
# Auto max-RPS search: exponential probe + binary search + validation
# ---------------------------------------------------------------------------

def find_max_rps(
    stub_port: int,
    num_go_workers: int,
    payload_size: str,
    probe_duration_s: float,
    full_duration_s: float,
    rps_start: float,
    max_ceiling: float,
    precision: float,
    probe_wall_s: float,
    full_wall_s: float,
    python: str,
    work_dir: str,
    monitor_ports: bool,
    stub_workers: int = 1,
    model: str = MODEL_ID,
    go_concurrency: int = 2000,
    sum_only: bool = False,
    num_go_procs: int = 1,
    base_urls: str = None,
    cpuprofile: bool = False,
    skip_validation: bool = False,
    probe_cooldown: float = 0.0,
    mpi_hostfile: str = None,
    num_cli_nodes: int = 1,
) -> dict:
    """
    Find the maximum sustainable RPS for a given (workers, payload) config.

    Algorithm:
      Phase 1 — Exponential probe: double RPS each step from rps_start until failure.
      Phase 2 — Binary search: narrow [lo, hi] until (hi-lo)/lo < precision.
      Phase 3 — Validation: confirm lo at full duration; step back if it fails.

    Returns a dict with keys:
      max_rps, at_ceiling, below_floor, validated, search_history, validation_result
    """
    cfg_label = f"w{num_go_workers}_{payload_size}"
    history: list[dict] = []

    def _probe(rps: float, duration_s: float, wall_s: float, phase: str) -> dict:
        # When cpuprofile is on, profile every run (so we capture the failure)
        prof_dir = work_dir if cpuprofile else ""
        result = run_sweep_point(
            stub_port=stub_port,
            target_rps=rps,
            num_go_workers=num_go_workers,
            payload_size=payload_size,
            duration_s=duration_s,
            max_wall_s=wall_s,
            python=python,
            work_dir=work_dir,
            monitor_ports=monitor_ports,
            stub_workers=stub_workers,
            model=model,
            go_concurrency=go_concurrency,
            sum_only=sum_only,
            num_go_procs=num_go_procs,
            base_urls=base_urls,
            cpuprofile_dir=prof_dir,
            mpi_hostfile=mpi_hostfile,
            num_cli_nodes=num_cli_nodes,
        )
        entry = {
            "phase":        phase,
            "rps":          rps,
            "can_keep_up":  result["can_keep_up"],
            "timed_out":    result["timed_out"],
            "actual_rps":   result.get("actual_rps"),
            "overhead_s":   result.get("dispatch_overhead_s"),
        }
        history.append(entry)
        keep = "YES" if result["can_keep_up"] else ("SKIP" if result["timed_out"] else "NO")
        # Build a concise reason string when keep_up=NO
        reason_parts = []
        if not result["can_keep_up"] and not result["timed_out"]:
            ovhd = result.get("dispatch_overhead_s")
            sp = result.get("trace_span_s")
            ar = result.get("actual_rps")
            cd = result.get("duration_s")
            if ovhd is not None and sp and sp > 0:
                if (ovhd / sp) >= KEEPUP_OVERHEAD_FRACTION:
                    reason_parts.append(f"dispatch_ovhd={ovhd:.2f}s>{sp*KEEPUP_OVERHEAD_FRACTION:.2f}s")
                if ar is not None and ar < rps * SERVER_BOTTLENECK_FRACTION:
                    reason_parts.append(f"actual_rps={ar:.0f}<{rps*SERVER_BOTTLENECK_FRACTION:.0f}")
                if cd is not None and cd > sp * 2.0:
                    reason_parts.append(f"duration={cd:.1f}s>{sp*2.0:.1f}s")
        reason_str = f"  reason=[{', '.join(reason_parts)}]" if reason_parts else ""
        print(
            f"    [{phase:>10}] rps={rps:>8.1f}  keep_up={keep:<4}  "
            f"actual={result.get('actual_rps') or 'N/A'}  "
            f"ovhd={result.get('dispatch_overhead_s') or 'N/A'}{reason_str}",
            flush=True,
        )
        if probe_cooldown > 0:
            print(f"    [cooldown] waiting {probe_cooldown:.0f}s for TIME_WAIT drain...", flush=True)
            time.sleep(probe_cooldown)
        return result

    print(f"\n[FindMaxRPS] {cfg_label}: starting exponential probe from rps={rps_start}", flush=True)

    # ---- Phase 1: Exponential probe ----------------------------------------
    rps = rps_start
    last_good_rps = 0.0
    first_bad_rps = None
    at_ceiling = False
    below_floor = False

    while rps <= max_ceiling:
        r = _probe(rps, probe_duration_s, probe_wall_s, "probe")
        if r["can_keep_up"]:
            last_good_rps = rps
            rps = min(rps * 2, max_ceiling * 1.0001)  # cap at ceiling
        else:
            first_bad_rps = rps
            if cpuprofile:
                print(f"\n[FindMaxRPS] {cfg_label}: --cpuprofile: stopping after first failure at rps={rps:.1f}",
                      flush=True)
                return {
                    "num_workers":      num_go_workers,
                    "payload_size":     payload_size,
                    "max_rps":          last_good_rps if last_good_rps > 0 else rps,
                    "at_ceiling":       False,
                    "below_floor":      last_good_rps == 0,
                    "validated":        False,
                    "search_history":   history,
                    "validation_result": r,
                    "cpuprofile":       True,
                }
            break

    if first_bad_rps is None:
        # Passed even at ceiling — report as lower-bound result
        at_ceiling = True
        print(f"  [FindMaxRPS] {cfg_label}: still keeping up at ceiling {max_ceiling}; "
              f"reporting max_rps >= {last_good_rps}", flush=True)
        if skip_validation:
            print(f"  [FindMaxRPS] {cfg_label}: skipping validation at ceiling.", flush=True)
            vr = None
            validated = False
        else:
            vr = _probe(last_good_rps, full_duration_s, full_wall_s, "validation")
            validated = vr["can_keep_up"]
        return {
            "num_workers":      num_go_workers,
            "payload_size":     payload_size,
            "max_rps":          last_good_rps,
            "at_ceiling":       True,
            "below_floor":      False,
            "validated":        validated,
            "search_history":   history,
            "validation_result": vr,
        }

    if last_good_rps == 0.0:
        # Failed even at rps_start
        below_floor = True
        print(f"  [FindMaxRPS] {cfg_label}: cannot keep up at floor rps={rps_start}", flush=True)
        return {
            "num_workers":      num_go_workers,
            "payload_size":     payload_size,
            "max_rps":          0.0,
            "at_ceiling":       False,
            "below_floor":      True,
            "validated":        False,
            "search_history":   history,
            "validation_result": None,
        }

    print(f"\n[FindMaxRPS] {cfg_label}: bracket [{last_good_rps:.1f}, {first_bad_rps:.1f}] — "
          f"starting binary search (precision={precision*100:.0f}%)", flush=True)

    # ---- Phase 2: Binary search ---------------------------------------------
    lo, hi = last_good_rps, first_bad_rps
    last_good_rps = lo  # track last confirmed-good throughout bisection

    while hi > 0 and (hi - lo) / hi > precision:
        mid = (lo + hi) / 2.0
        r = _probe(mid, probe_duration_s, probe_wall_s, "bisect")
        if r["can_keep_up"]:
            lo = mid
            last_good_rps = mid
        else:
            hi = mid

    if skip_validation:
        print(f"\n[FindMaxRPS] {cfg_label}: converged to ~{lo:.1f} RPS — skipping validation", flush=True)
        return {
            "num_workers":       num_go_workers,
            "payload_size":      payload_size,
            "max_rps":           round(lo, 2),
            "at_ceiling":        False,
            "below_floor":       False,
            "validated":         False,
            "search_history":    history,
            "validation_result": None,
        }

    print(f"\n[FindMaxRPS] {cfg_label}: converged to ~{lo:.1f} RPS — running validation", flush=True)

    # ---- Phase 3: Validation ------------------------------------------------
    vr = _probe(lo, full_duration_s, full_wall_s, "validation")
    validated = vr["can_keep_up"]
    final_rps = lo

    if not validated:
        # Binary search converged but validation at full duration failed; step back
        # to the last confirmed-good point from probe phase (conservative)
        fallback = history[-2]["rps"] if len(history) >= 2 else rps_start
        for h in reversed(history[:-1]):
            if h["can_keep_up"]:
                fallback = h["rps"]
                break
        print(f"  [FindMaxRPS] {cfg_label}: validation FAILED — stepping back to {fallback:.1f}", flush=True)
        final_rps = fallback

    return {
        "num_workers":       num_go_workers,
        "payload_size":      payload_size,
        "max_rps":           round(final_rps, 2),
        "at_ceiling":        False,
        "below_floor":       False,
        "validated":         validated,
        "search_history":    history,
        "validation_result": vr,
    }


# ---------------------------------------------------------------------------
# Summary tables
# ---------------------------------------------------------------------------

def print_max_rps_summary_table(results: list[dict]):
    header = (
        f"{'workers':>7} {'payload':>8} {'max_rps':>9} "
        f"{'validated':>9} {'at_ceil':>7} {'notes':>20}"
    )
    print("\n" + "=" * len(header))
    print("  MAX RPS SEARCH RESULTS")
    print("=" * len(header))
    print(header)
    print("-" * len(header))
    for r in results:
        notes = ""
        if r.get("at_ceiling"):
            notes = f">= ceiling"
        elif r.get("below_floor"):
            notes = "below floor"
        elif r.get("validation_result") is None and not r.get("validated"):
            notes = "validation skipped"
        elif not r.get("validated"):
            notes = "val failed, stepped back"
        n_steps = len(r.get("search_history", []))
        print(
            f"{r['num_workers']:>7} {r['payload_size']:>8} {r['max_rps']:>9.1f} "
            f"{'YES' if r.get('validated') else 'NO':>9} "
            f"{'YES' if r.get('at_ceiling') else 'NO':>7} "
            f"{notes:>20}  ({n_steps} probes)"
        )
    print("=" * len(header) + "\n")


def print_summary_table(results: list[dict]):
    header = (
        f"{'workers':>7} {'rps_tgt':>8} {'rps_act':>8} {'srv_rps':>8} {'payload':>8} "
        f"{'ovhd_s':>7} {'lat_p99s':>9} {'errors':>6} {'status':>7} {'bottleneck':>10}"
    )
    print("\n" + "=" * len(header))
    print(header)
    print("-" * len(header))
    for r in results:
        rps_act  = f"{r['actual_rps']:.1f}"   if r.get("actual_rps")   is not None else "N/A"
        srv_rps  = f"{r['server_rps']:.1f}"   if r.get("server_rps")   is not None else "N/A"
        ovhd     = f"{r['dispatch_overhead_s']:.2f}" if r.get("dispatch_overhead_s") is not None else "N/A"
        l99      = f"{r['latency_p99_s']:.3f}" if r.get("latency_p99_s") is not None else "N/A"
        errs     = r.get("errors", "N/A")
        bottleneck = r.get("bottleneck", "unknown")
        if r.get("timed_out"):
            status = "SKIP"
        elif r.get("can_keep_up"):
            status = "YES"
        else:
            status = "NO"
        nw   = r["num_workers"]
        trps = r["target_rps"]
        ps   = r["payload_size"]
        print(
            f"{nw:>7} {trps:>8} {rps_act:>8} {srv_rps:>8} {ps:>8} "
            f"{ovhd:>7} {l99:>9} {errs:>6} {status:>7} {bottleneck:>10}"
        )
    print("=" * len(header) + "\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark replay_client.py dispatch capacity against a stub server.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Target
    parser.add_argument("--stub-port", type=int, default=8000,
                        help="Port the stub server is listening on.")
    parser.add_argument("--stub-host", type=str, default="127.0.0.1",
                        help="(Informational) stub server host. replay_client targets 0.0.0.0:<port>.")
    parser.add_argument("--model", type=str, default=MODEL_ID,
                        help="Model name used in trace requests.")

    # Auto max-RPS search
    parser.add_argument("--find-max-rps", action="store_true",
                        help=(
                            "Auto-discover max sustainable RPS per (workers, payload) using "
                            "exponential probe + binary search + validation."
                        ))
    parser.add_argument("--probe-duration", type=float, default=8.0,
                        help="Duration (s) for each probe run during max-RPS search (default 8).")
    parser.add_argument("--rps-start", type=float, default=50.0,
                        help="Starting RPS for exponential probe phase (default 50).")
    parser.add_argument("--max-rps-ceiling", type=float, default=20000.0,
                        help="Upper RPS bound; stop probing above this (default 20000).")
    parser.add_argument("--precision", type=float, default=0.02,
                        help="Binary-search convergence threshold as a fraction (default 0.02 = 2%%).")
    parser.add_argument("--probe-cooldown", type=float, default=0.0,
                        help=(
                            "Seconds to sleep between probes to let TCP TIME_WAIT drain (default 0). "
                            "Recommended 15-30s for inter-node benchmarks with limited ephemeral ports."
                        ))
    parser.add_argument("--skip-validation", action="store_true",
                        help="Skip the final full-duration validation probe in --find-max-rps mode.")

    # Sweep dimensions
    parser.add_argument(
        "--sweep", type=str, default="",
        help="Comma-separated dimensions to sweep: workers, rps, payload. E.g. --sweep workers,rps",
    )
    parser.add_argument("--sweep-workers",  type=str, default=None,
                        help="Workers to sweep, comma-sep (e.g. 1,4,8,16).")
    parser.add_argument("--sweep-rps",      type=str, default=None,
                        help="RPS values to sweep, comma-sep.")
    parser.add_argument("--sweep-payloads", type=str, default=None,
                        help="Payload sizes to sweep, comma-sep (e.g. small,medium,large,xl).")

    # Fixed parameter values
    # Legacy alias — use --num-go-workers instead
    parser.add_argument("--workers",  type=int,   default=None,     help="(Legacy alias for --num-go-workers.)")
    parser.add_argument("--rps",      type=float, default=100.0,    help="Target RPS (fixed point).")
    parser.add_argument("--payload",  type=str,   default="medium",
                        choices=list(PAYLOAD_SIZES.keys()),          help="Payload size (fixed point).")

    # Timing
    parser.add_argument("--duration",      type=float, default=20.0,
                        help="Trace duration per sweep point (s). replay_client runs for this long.")
    parser.add_argument("--point-timeout", type=float, default=0.0,
                        help=(
                            "Wall-clock deadline per sweep point (s). "
                            "If replay_client takes longer, it is killed and the point is marked SKIP. "
                            "Default 0 = auto: duration + 30s."
                        ))
    # Legacy flag (accepted but ignored — warmup is now controlled by Go client config)
    parser.add_argument("--warmup", action="store_true",
                        help="(No-op: warmup is now controlled by warmup_rps/warmup_duration_s in config.)")

    # Python interpreter
    parser.add_argument("--python", type=str, default=sys.executable,
                        help="Python interpreter to use for running replay_client.py.")
    parser.add_argument("--stub-workers", type=int, default=1,
                        help=(
                            "Number of stub server processes (SO_REUSEPORT). "
                            "Used to scale server_rps measurement in /metrics. "
                            "Must match --stub-workers passed to run_bench.sh (default 1)."
                        ))

    # Output
    parser.add_argument("--output", type=str, default=None,
                        help="Output JSON path. Defaults to benchmarks/results/client_sweep_<ts>.json")
    parser.add_argument("--work-dir", type=str, default=None,
                        help="Working directory for per-point trace/config/log files.")
    parser.add_argument("--no-port-monitor", action="store_true",
                        help="Disable live port monitoring.")

    # Legacy flags (accepted but ignored — replay_client always uses HTTP/2 and auto pool)
    parser.add_argument("--http2",  action="store_true", help="(No-op: replay_client always uses HTTP/2.)")
    parser.add_argument("--http11", action="store_true", help="(No-op: replay_client always uses HTTP/2.)")
    parser.add_argument("--pool",   type=int, default=None,
                        help="(No-op: replay_client auto-sizes pool from fd limit.)")
    # go_dispatch concurrency
    parser.add_argument("--go-concurrency", type=int, default=2000,
                        help=(
                            "Max in-flight goroutines in go_dispatch (default 2000). "
                            "Throughput ≈ go_concurrency / round_trip_latency. "
                            "Raise this to push past the default ~32K req/s ceiling "
                            "against a fast stub server."
                        ))
    parser.add_argument("--num-go-workers", type=int, default=4,
                        help=(
                            "Parallel dispatch goroutines inside go_dispatch (default 4). "
                            "Each goroutine handles every Nth request, giving it N× longer "
                            "inter-request intervals and breaking the serial dispatch ceiling."
                        ))
    parser.add_argument("--sum-only", action="store_true",
                        help="Go client writes only a summary line instead of per-request results.")
    parser.add_argument("--num-go-procs", type=int, default=1,
                        help=(
                            "Number of independent Go processes per rank (default 1). "
                            "Each gets 1/N of the requests (interleaved). "
                            "Use >1 to test single-process Go scheduler bottleneck."
                        ))
    # Multi-client-node (MPI) support
    parser.add_argument("--mpi-hostfile", type=str, default=None,
                        help="Path to MPI hostfile for multi-client-node benchmarks.")
    parser.add_argument("--num-cli-nodes", type=int, default=1,
                        help="Number of client nodes (MPI ranks). Default 1 = no MPI.")
    # Legacy sweep args kept for run_bench.sh compatibility
    parser.add_argument("--base-urls", type=str, default=None,
                        help=(
                            "Comma-separated base URLs for multi-target benchmarks "
                            "(e.g. http://0.0.0.0:8000,http://0.0.0.0:8001). "
                            "Overrides --stub-port for URL construction."
                        ))
    # Legacy sweep args kept for run_bench.sh compatibility
    parser.add_argument("--sweep-pools", type=str, default=None, help="(No-op for real replay_client.)")
    parser.add_argument("--target",      type=str, default="stub_server", help="(No-op, kept for compat.)")
    parser.add_argument("--base-url",    type=str, default=None, help="(No-op, use --base-urls instead.)")
    parser.add_argument("--cpuprofile",  action="store_true", default=False,
                        help="Enable Go CPU profiling (written during validation runs).")

    args = parser.parse_args()

    # Validate replay_client exists
    if not _REPLAY_CLIENT.exists():
        print(f"ERROR: replay_client.py not found at {_REPLAY_CLIENT}", file=sys.stderr)
        sys.exit(1)

    if args.http11:
        print("[WARN] --http11 has no effect: replay_client always uses HTTP/2.", flush=True)
    if args.pool is not None:
        print("[WARN] --pool has no effect: replay_client auto-sizes from fd limit.", flush=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    work_dir = args.work_dir or str(_BENCH_DIR / "results" / f"client_work_{ts}")
    os.makedirs(work_dir, exist_ok=True)

    # ------------------------------------------------------------------ #
    # Mode: find-max-rps                                                   #
    # ------------------------------------------------------------------ #
    if args.find_max_rps:
        if args.sweep_rps:
            print("[WARN] --sweep-rps is ignored in --find-max-rps mode.", flush=True)

        # Resolve workers and payload sweeps; RPS is what we're searching for.
        sweep_dims = set(d.strip() for d in args.sweep.split(",") if d.strip())
        workers_list = ([int(x) for x in args.sweep_workers.split(",")]  if args.sweep_workers
                        else (SWEEP_WORKERS if "workers" in sweep_dims else [args.num_go_workers]))
        payload_list = ([x.strip() for x in args.sweep_payloads.split(",")]  if args.sweep_payloads
                        else (list(PAYLOAD_SIZES.keys()) if "payload" in sweep_dims else [args.payload]))

        default_buffer = args.point_timeout if args.point_timeout > 0 else 30.0
        probe_wall_s = args.probe_duration + default_buffer
        full_wall_s  = args.duration + default_buffer

        total_configs = len(workers_list) * len(payload_list)
        print(f"[FindMaxRPS] replay_client: {_REPLAY_CLIENT}")
        print(f"[FindMaxRPS] python:        {args.python}")
        print(f"[FindMaxRPS] Configs:       {total_configs}")
        print(f"  num_go_workers:  {workers_list}")
        print(f"  num_go_procs:    {args.num_go_procs}")
        print(f"  payload:         {payload_list}")
        print(f"  rps_start:       {args.rps_start}")
        print(f"  ceiling:         {args.max_rps_ceiling}")
        print(f"  precision:       {args.precision*100:.0f}%")
        print(f"  probe_duration:  {args.probe_duration}s  full_duration: {args.duration}s")
        print(f"  go_concurrency:  {args.go_concurrency}")
        if args.probe_cooldown > 0:
            print(f"  probe_cooldown:  {args.probe_cooldown}s")
        if args.num_cli_nodes > 1:
            print(f"  num_cli_nodes:   {args.num_cli_nodes}  (MPI hostfile: {args.mpi_hostfile})")
        print(f"  stub:            http://0.0.0.0:{args.stub_port}\n")

        all_results = []
        for cfg_idx, (w, payload) in enumerate(
            itertools.product(workers_list, payload_list), start=1
        ):
            print(f"\n[FindMaxRPS] Config {cfg_idx}/{total_configs}: "
                  f"num_go_workers={w} payload={payload}", flush=True)

            result = find_max_rps(
                stub_port=args.stub_port,
                num_go_workers=w,
                payload_size=payload,
                probe_duration_s=args.probe_duration,
                full_duration_s=args.duration,
                rps_start=args.rps_start,
                max_ceiling=args.max_rps_ceiling,
                precision=args.precision,
                probe_wall_s=probe_wall_s,
                full_wall_s=full_wall_s,
                python=args.python,
                work_dir=work_dir,
                monitor_ports=not args.no_port_monitor,
                stub_workers=args.stub_workers,
                model=args.model,
                go_concurrency=args.go_concurrency,
                sum_only=args.sum_only,
                num_go_procs=args.num_go_procs,
                base_urls=args.base_urls,
                cpuprofile=args.cpuprofile,
                skip_validation=args.skip_validation,
                probe_cooldown=args.probe_cooldown,
                mpi_hostfile=args.mpi_hostfile,
                num_cli_nodes=args.num_cli_nodes,
            )
            all_results.append(result)

        print_max_rps_summary_table(all_results)

        out_path = Path(args.output) if args.output else _BENCH_DIR / "results" / f"max_rps_{ts}.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        output_data = {
            "meta": {
                "mode":           "find_max_rps",
                "timestamp":      ts,
                "replay_client":  str(_REPLAY_CLIENT),
                "python":         args.python,
                "stub_port":      args.stub_port,
                "rps_start":      args.rps_start,
                "max_rps_ceiling": args.max_rps_ceiling,
                "precision":      args.precision,
                "probe_duration_s": args.probe_duration,
                "full_duration_s":  args.duration,
                "skip_validation": args.skip_validation,
                "warmup":         args.warmup,
                "total_configs":  total_configs,
                "go_concurrency": args.go_concurrency,
            },
            "results": all_results,
        }
        with open(out_path, "w") as f:
            json.dump(output_data, f, indent=2)
        print(f"[FindMaxRPS] Results saved to {out_path}")
        return

    # ------------------------------------------------------------------ #
    # Mode: regular sweep                                                  #
    # ------------------------------------------------------------------ #
    # Resolve sweep dimensions
    sweep_dims = set(d.strip() for d in args.sweep.split(",") if d.strip())

    workers_list = ([int(x) for x in args.sweep_workers.split(",")]  if args.sweep_workers
                    else (SWEEP_WORKERS if "workers" in sweep_dims else [args.num_go_workers]))
    rps_list     = ([float(x) for x in args.sweep_rps.split(",")]    if args.sweep_rps
                    else (SWEEP_RPS if "rps" in sweep_dims else [args.rps]))
    payload_list = ([x.strip() for x in args.sweep_payloads.split(",")]  if args.sweep_payloads
                    else (list(PAYLOAD_SIZES.keys()) if "payload" in sweep_dims else [args.payload]))

    max_wall_s = args.point_timeout if args.point_timeout > 0 else args.duration + 30.0

    total_points = len(workers_list) * len(rps_list) * len(payload_list)

    print(f"[BenchClient] replay_client: {_REPLAY_CLIENT}")
    print(f"[BenchClient] python:        {args.python}")
    print(f"[BenchClient] Sweep plan:    {total_points} point(s)")
    print(f"  num_go_workers:  {workers_list}")
    print(f"  num_go_procs:    {args.num_go_procs}")
    print(f"  rps:             {rps_list}")
    print(f"  payload:         {payload_list}")
    print(f"  duration:        {args.duration}s  timeout: {max_wall_s}s")
    print(f"  go_concurrency:  {args.go_concurrency}")
    print(f"  stub:            http://0.0.0.0:{args.stub_port}\n")

    all_results = []
    for point_idx, (w, rps, payload) in enumerate(
        itertools.product(workers_list, rps_list, payload_list), start=1
    ):
        print(f"\n[BenchClient] Point {point_idx}/{total_points}: "
              f"workers={w} rps={rps} payload={payload}", flush=True)

        result = run_sweep_point(
            stub_port=args.stub_port,
            target_rps=rps,
            num_go_workers=w,
            payload_size=payload,
            duration_s=args.duration,
            max_wall_s=max_wall_s,
            python=args.python,
            work_dir=work_dir,
            monitor_ports=not args.no_port_monitor,
            stub_workers=args.stub_workers,
            model=args.model,
            go_concurrency=args.go_concurrency,
            sum_only=args.sum_only,
            num_go_procs=args.num_go_procs,
            base_urls=args.base_urls,
        )
        all_results.append(result)

        if result["timed_out"]:
            rps_act = result.get("actual_rps")
            rps_str = f" (achieved ~{rps_act:.0f} rps)" if rps_act else ""
            print(f"  SKIP — timed out after {max_wall_s:.0f}s{rps_str}", flush=True)
        else:
            ovhd = result.get("dispatch_overhead_s")
            ovhd_str = f"{ovhd:.2f}s" if ovhd is not None else "N/A"
            status = "YES" if result["can_keep_up"] else "NO"
            print(
                f"  actual_rps={result.get('actual_rps')} "
                f"dispatch_overhead={ovhd_str} "
                f"lat_p99={result.get('latency_p99_s')} "
                f"errors={result.get('errors', 0)} can_keep_up={status}",
                flush=True,
            )

    print_summary_table(all_results)

    # Save
    out_path = Path(args.output) if args.output else _BENCH_DIR / "results" / f"client_sweep_{ts}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    output_data = {
        "meta": {
            "timestamp": ts,
            "replay_client": str(_REPLAY_CLIENT),
            "python": args.python,
            "stub_port": args.stub_port,
            "duration_s": args.duration,
            "max_wall_s": max_wall_s,
            "warmup": args.warmup,
            "sweep_dims": list(sweep_dims),
            "total_points": total_points,
        },
        "results": all_results,
    }
    with open(out_path, "w") as f:
        json.dump(output_data, f, indent=2)
    print(f"[BenchClient] Results saved to {out_path}")


if __name__ == "__main__":
    main()
