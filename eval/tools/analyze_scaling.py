#!/usr/bin/env python3
"""Cross-scale analysis: given multiple run dirs (e.g. 32n/64n/256n), compute
per-scale GCS contention metrics and proxy init substep distributions.

Produces a summary table suitable for pasting into a findings doc.

Usage: analyze_scaling.py <run_dir1> <run_dir2> ...

Each run_dir is expected to contain logs/backend/<stamp>_ray_runtime/ with:
  - ray_logs/<head>/gcs_server.out (via parse_gcs_event_stats)
  - instrumentation/<host>/proxy_init_*.json (many per host)
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from statistics import mean, median

# Import the parser from the sibling module
sys.path.insert(0, str(Path(__file__).parent))
from analysis_io import read_json_object  # noqa: E402
from parse_gcs_event_stats import parse as parse_gcs, Row  # noqa: E402


def find_log_dir(run_dir: Path) -> Path | None:
    backends = list((run_dir / "logs" / "backend").glob("*_ray_runtime"))
    if not backends:
        return None
    return sorted(backends)[-1]


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    k = (len(values) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(values) - 1)
    if f == c:
        return values[f]
    return values[f] * (c - k) + values[c] * (k - f)


def analyze_gcs(log_dir: Path) -> dict:
    """Return per-method peak queueing and totals across all stats blocks."""
    gcs = next((p for p in (log_dir / "ray_logs").glob("**/gcs_server.out")), None)
    if not gcs:
        return {"gcs_log": None}
    rows = parse_gcs(gcs)
    by_method: dict[tuple[str, str], list[Row]] = {}
    for r in rows:
        by_method.setdefault((r.service, r.method), []).append(r)

    def summarize_method(group: list[Row]) -> dict:
        return {
            "blocks": len(group),
            "final_total_calls": group[-1].total,
            "peak_active": max(r.active for r in group),
            "peak_q_mean_ms": max(r.q_mean_ms for r in group),
            "peak_q_max_ms": max(r.q_max_ms for r in group),
            "peak_exec_mean_ms": max(r.exec_mean_ms for r in group),
        }

    methods = {}
    for (svc, m), group in by_method.items():
        methods[f"{svc}::{m}"] = summarize_method(group)

    return {"gcs_log": str(gcs), "methods": methods, "num_blocks_observed": len(rows)}


def analyze_proxies(log_dir: Path) -> dict:
    """Aggregate proxy_init_*.json across all collected nodes."""
    inst_dir = log_dir / "instrumentation"
    profiles = []
    for p in inst_dir.glob("*/proxy_init_*.json"):
        profiles.append(read_json_object(p))

    if not profiles:
        return {"count": 0}

    def stats_of(key: str, in_substeps: bool = False) -> dict:
        vals = []
        for p in profiles:
            v = p["substeps"].get(key) if in_substeps else p.get(key)
            if isinstance(v, (int, float)):
                vals.append(float(v))
        if not vals:
            return {}
        return {
            "n": len(vals),
            "min": round(min(vals), 4),
            "mean": round(mean(vals), 4),
            "median": round(median(vals), 4),
            "p95": round(percentile(vals, 95), 4),
            "p99": round(percentile(vals, 99), 4),
            "max": round(max(vals), 4),
        }

    return {
        "count": len(profiles),
        "unique_hosts": len({p["hostname"] for p in profiles}),
        "duration_s": stats_of("duration_s"),
        "ready_duration_s": stats_of("ready_duration_s"),
        "substep_server_tasks_and_gc": stats_of("server_tasks_and_gc", in_substeps=True),
        "substep_long_poll_client": stats_of("long_poll_client", in_substeps=True),
        "substep_create_proxies": stats_of("create_proxies", in_substeps=True),
        "substep_super_init": stats_of("super_init", in_substeps=True),
    }


def analyze_controller(log_dir: Path) -> dict:
    """Count 'unhealthy' / 'failed health check' events in controller log."""
    ctrl_logs = list(log_dir.glob("ray_logs/**/serve/controller_*.log"))
    if not ctrl_logs:
        return {"controller_log": None}
    unhealthy_proxy = 0
    unhealthy_replica = 0
    proxy_didnt_respond = 0
    replica_didnt_respond = 0
    replicas_started = 0
    for ctrl in ctrl_logs:
        text = ctrl.read_text(encoding="utf-8")
        unhealthy_proxy += text.count("failed the health check")
        unhealthy_replica += text.count("marking it unhealthy")
        proxy_didnt_respond += text.count("Didn't receive health check response for proxy")
        replica_didnt_respond += text.count("Didn't receive health check response for replica")
        replicas_started += text.count("started successfully")
    return {
        "controller_log": str(ctrl_logs[0]),
        "proxy_failed_health_check": unhealthy_proxy,
        "marking_it_unhealthy_total": unhealthy_replica,  # superset (replica + proxy)
        "proxy_didnt_receive_response": proxy_didnt_respond,
        "replica_didnt_receive_response": replica_didnt_respond,
        "replicas_started_successfully": replicas_started,
    }


def analyze_trace(log_dir: Path) -> dict:
    """Pull key phases from scaling_trace.json."""
    trace_path = log_dir / "scaling_trace.json"
    if not trace_path.exists():
        return {}
    data = json.loads(trace_path.read_text())
    phases = {}
    for p in data.get("phases", []):
        phases[p["name"]] = round(p.get("duration_s", 0), 3)
    return {"phases": phases}


def summarize(run_dir: Path) -> dict:
    log_dir = find_log_dir(run_dir)
    if log_dir is None:
        return {"run_dir": str(run_dir), "error": "no logs/backend/*_ray_runtime found"}
    return {
        "run_dir": str(run_dir),
        "log_dir": str(log_dir),
        "gcs": analyze_gcs(log_dir),
        "proxies": analyze_proxies(log_dir),
        "controller": analyze_controller(log_dir),
        "trace": analyze_trace(log_dir),
    }


def print_comparison(results: list[dict]) -> None:
    scales = [r["run_dir"].split("/")[-1] for r in results]

    def row(label: str, values: list) -> None:
        print(f"  {label:<48} " + " | ".join(f"{str(v):>14}" for v in values))

    print("\n=== Stage timing (scaling_trace phases) ===")
    phase_keys = [
        "ray_init",
        "serve.start",
        "serve.run.deploy_apps",
        "serve.run.wait_proxies",
        "stage3.total",
    ]
    for ph in phase_keys:
        vals = [r["trace"].get("phases", {}).get(ph, "-") for r in results]
        row(ph, vals)

    print("\n=== Proxy init duration (seconds, across all collected proxies) ===")
    for stat in ("mean", "p95", "p99", "max"):
        row(
            f"duration_s.{stat}",
            [r["proxies"].get("duration_s", {}).get(stat, "-") for r in results],
        )
    row(
        "proxies collected",
        [
            f"{r['proxies'].get('count', 0)}/{scales[i].replace('-nodes', '')}"
            for i, r in enumerate(results)
        ],
    )

    print("\n=== Controller health-check events ===")
    row(
        "proxy failed-health-check (× kill)",
        [r["controller"].get("proxy_failed_health_check", "-") for r in results],
    )
    row(
        "proxy 'didn't receive response'",
        [r["controller"].get("proxy_didnt_receive_response", "-") for r in results],
    )
    row(
        "replica 'didn't receive response'",
        [r["controller"].get("replica_didnt_receive_response", "-") for r in results],
    )
    row(
        "replicas started successfully",
        [r["controller"].get("replicas_started_successfully", "-") for r in results],
    )

    print("\n=== GCS event stats — peak queueing (ms) ===")
    methods_of_interest = [
        "Main service::GcsInMemoryStore.Put",
        "Main service::GcsInMemoryStore.Get",
        "Main service::NodeInfoGcsService.grpc_server.GetAllNodeAddressAndLiveness.HandleRequestImpl",
        "Main service::ActorInfoGcsService.grpc_server.GetActorInfo.HandleRequestImpl",
        "Main service::ActorInfoGcsService.grpc_server.GetNamedActorInfo.HandleRequestImpl",
        "Main service::NodeManagerService.grpc_client.GetResourceLoad.OnReplyReceived",
        "Main service::PeriodicalRunner.RunFnPeriodically",
        "Main service::GcsHealthCheckManager::MarkNodeHealthy",
        "Main service::HealthCheck",
    ]
    print(f"  {'method':<72} " + " | ".join(f"{s:>14}" for s in scales))
    for m in methods_of_interest:
        vals = []
        for r in results:
            md = r["gcs"].get("methods", {}).get(m, {})
            if md:
                vals.append(f"{md.get('peak_q_max_ms', 0):.1f}/{md.get('final_total_calls', 0)}")
            else:
                vals.append("-")
        print(f"  {m:<72} " + " | ".join(f"{v:>14}" for v in vals))
    print("  (format: peak_q_max_ms / total_calls)")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("run_dirs", nargs="+", type=Path)
    p.add_argument("--json", type=Path)
    args = p.parse_args()

    results = [summarize(rd) for rd in args.run_dirs]
    print_comparison(results)

    if args.json:
        args.json.write_text(json.dumps(results, indent=2))
        print(f"\nFull JSON written to {args.json}", file=sys.stderr)


if __name__ == "__main__":
    main()
