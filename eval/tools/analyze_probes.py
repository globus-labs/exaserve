#!/usr/bin/env python3
"""Analyze Section 6 probe data:
  - router_updates_<pid>.jsonl: update_deployment_targets calls
  - get_actor_calls_<pid>.csv: individual ray.get_actor() lookups

Usage: analyze_probes.py <run_dir1> [<run_dir2> ...]

Reports per-scale:
  - total get_actor calls, total GCS time, mean/p50/p99 per-call duration
  - total update_deployment_targets invocations, mean duration
  - per-proxy max-call-count and max-total-GCS-time
  - back-of-envelope wait_proxies prediction = total_gcs_time / n_proxies
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from statistics import mean, median


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


def find_log_dir(run_dir: Path) -> Path | None:
    backends = list((run_dir / "logs" / "backend").glob("*_ray_runtime"))
    if not backends:
        return None
    return sorted(backends)[-1]


def summarize_run(run_dir: Path) -> dict:
    log_dir = find_log_dir(run_dir)
    if not log_dir:
        return {"run_dir": str(run_dir), "error": "no log dir"}
    inst_root = log_dir / "instrumentation"
    if not inst_root.is_dir():
        return {"run_dir": str(run_dir), "error": "no instrumentation dir"}

    # Per-proxy tallies
    per_proxy_get_actor = {}  # pid -> (count, total_ms, durations)
    per_proxy_updates = {}    # pid -> list of (n_replicas, duration_s)

    for host_dir in inst_root.iterdir():
        if not host_dir.is_dir():
            continue
        for csv_path in host_dir.glob("get_actor_calls_*.csv"):
            pid = csv_path.stem.split("_")[-1]
            key = f"{host_dir.name}:{pid}"
            count = 0
            total_ms = 0.0
            durations = []
            try:
                with csv_path.open() as f:
                    for row in csv.reader(f):
                        if len(row) != 2:
                            continue
                        try:
                            dur_ms = float(row[1])
                        except ValueError:
                            continue
                        count += 1
                        total_ms += dur_ms
                        durations.append(dur_ms)
            except Exception:
                pass
            if count:
                per_proxy_get_actor[key] = (count, total_ms, durations)

        for jl_path in host_dir.glob("router_updates_*.jsonl"):
            pid = jl_path.stem.split("_")[-1]
            key = f"{host_dir.name}:{pid}"
            rows = []
            try:
                with jl_path.open() as f:
                    for line in f:
                        try:
                            rows.append(json.loads(line))
                        except Exception:
                            pass
            except Exception:
                pass
            if rows:
                per_proxy_updates[key] = rows

    # Aggregate get_actor across all proxies
    all_durations = []
    total_calls = 0
    total_ms = 0.0
    per_proxy_call_counts = []
    per_proxy_total_ms = []
    for key, (cnt, tms, durs) in per_proxy_get_actor.items():
        all_durations.extend(durs)
        total_calls += cnt
        total_ms += tms
        per_proxy_call_counts.append(cnt)
        per_proxy_total_ms.append(tms)

    all_update_n = []
    all_update_dur_s = []
    per_proxy_update_time = []
    for key, rows in per_proxy_updates.items():
        tot = 0.0
        for r in rows:
            all_update_n.append(r.get("n_replicas", 0))
            all_update_dur_s.append(r.get("duration_s", 0))
            tot += r.get("duration_s", 0)
        per_proxy_update_time.append(tot)

    return {
        "run_dir": str(run_dir),
        "scale": run_dir.name,
        "n_proxies_with_data": len(per_proxy_get_actor),
        "n_proxies_with_updates": len(per_proxy_updates),
        "get_actor": {
            "total_calls": total_calls,
            "total_seconds": round(total_ms / 1000.0, 3),
            "mean_ms": round(mean(all_durations), 3) if all_durations else 0,
            "median_ms": round(median(all_durations), 3) if all_durations else 0,
            "p95_ms": round(percentile(all_durations, 95), 3),
            "p99_ms": round(percentile(all_durations, 99), 3),
            "max_ms": round(max(all_durations), 3) if all_durations else 0,
            "per_proxy_mean_calls": round(mean(per_proxy_call_counts), 1) if per_proxy_call_counts else 0,
            "per_proxy_max_calls": max(per_proxy_call_counts) if per_proxy_call_counts else 0,
            "per_proxy_mean_total_s": round(mean(per_proxy_total_ms) / 1000, 3) if per_proxy_total_ms else 0,
            "per_proxy_max_total_s": round(max(per_proxy_total_ms) / 1000, 3) if per_proxy_total_ms else 0,
        },
        "update_deployment_targets": {
            "total_calls": len(all_update_dur_s),
            "total_seconds": round(sum(all_update_dur_s), 3),
            "mean_dur_s": round(mean(all_update_dur_s), 3) if all_update_dur_s else 0,
            "max_dur_s": round(max(all_update_dur_s), 3) if all_update_dur_s else 0,
            "mean_n_replicas": round(mean(all_update_n), 1) if all_update_n else 0,
            "max_n_replicas": max(all_update_n) if all_update_n else 0,
            "per_proxy_mean_total_s": round(mean(per_proxy_update_time), 3) if per_proxy_update_time else 0,
            "per_proxy_max_total_s": round(max(per_proxy_update_time), 3) if per_proxy_update_time else 0,
        },
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("run_dirs", nargs="+", type=Path)
    args = p.parse_args()

    summaries = [summarize_run(rd) for rd in args.run_dirs]

    # Print side-by-side table
    def row(label, key_path, fmt="{}"):
        print(f"  {label:<48} " + " | ".join(
            fmt.format(_get(s, key_path)) if _get(s, key_path) is not None else "-"
            for s in summaries
        ))

    scales = [s.get("scale", "?") for s in summaries]
    print(f"\n  {'metric':<48} " + " | ".join(f"{sc:>16}" for sc in scales))

    print("\n--- Counts ---")
    row("n proxies with get_actor data", "n_proxies_with_data", "{:>16}")
    row("n proxies with router_update data", "n_proxies_with_updates", "{:>16}")

    print("\n--- get_actor_handle (all GCS lookups) ---")
    row("total calls (cluster-wide)", "get_actor.total_calls", "{:>16}")
    row("total GCS time (s, cluster-wide)", "get_actor.total_seconds", "{:>16}")
    row("per-call mean duration (ms)", "get_actor.mean_ms", "{:>16}")
    row("per-call median (ms)", "get_actor.median_ms", "{:>16}")
    row("per-call p95 (ms)", "get_actor.p95_ms", "{:>16}")
    row("per-call p99 (ms)", "get_actor.p99_ms", "{:>16}")
    row("per-call max (ms)", "get_actor.max_ms", "{:>16}")
    row("per-proxy mean call count", "get_actor.per_proxy_mean_calls", "{:>16}")
    row("per-proxy max call count", "get_actor.per_proxy_max_calls", "{:>16}")
    row("per-proxy mean total GCS time (s)", "get_actor.per_proxy_mean_total_s", "{:>16}")
    row("per-proxy max total GCS time (s)", "get_actor.per_proxy_max_total_s", "{:>16}")

    print("\n--- update_deployment_targets ---")
    row("total broadcast calls", "update_deployment_targets.total_calls", "{:>16}")
    row("total time across all calls (s)", "update_deployment_targets.total_seconds", "{:>16}")
    row("per-call mean duration (s)", "update_deployment_targets.mean_dur_s", "{:>16}")
    row("per-call max duration (s)", "update_deployment_targets.max_dur_s", "{:>16}")
    row("mean n_replicas in broadcast", "update_deployment_targets.mean_n_replicas", "{:>16}")
    row("max n_replicas in broadcast", "update_deployment_targets.max_n_replicas", "{:>16}")
    row("per-proxy mean total time (s)", "update_deployment_targets.per_proxy_mean_total_s", "{:>16}")
    row("per-proxy max total time (s)", "update_deployment_targets.per_proxy_max_total_s", "{:>16}")


def _get(obj, path):
    for k in path.split("."):
        if not isinstance(obj, dict):
            return None
        obj = obj.get(k)
    return obj


if __name__ == "__main__":
    main()
