#!/usr/bin/env python3
"""Analyze dsm_updates_<pid>.jsonl from Section 6.2 probe.

Each line is one DeploymentStateManager.update() call with per-step
timing (s1..s7) and context (n_replicas_total, n_deployments).

Reports per-scale:
  - total number of dsm.update() calls
  - mean/p99/max duration + mean n_replicas
  - per-step mean/max contribution (which step dominates)
  - peak ticks (worst few, to see what's expensive)
"""

from __future__ import annotations

import argparse
from pathlib import Path
from statistics import mean, median

try:
    from .analysis_io import read_jsonl_objects
except ImportError:  # Direct ``python eval/tools/analyze_dsm.py`` execution.
    from analysis_io import read_jsonl_objects


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


def summarize(run_dir: Path) -> dict:
    log_dir = find_log_dir(run_dir)
    if not log_dir:
        return {"run_dir": str(run_dir), "error": "no log dir"}
    inst_root = log_dir / "instrumentation"
    dsm_files = list(inst_root.glob("*/dsm_updates_*.jsonl"))
    if not dsm_files:
        return {"run_dir": str(run_dir), "error": "no dsm_updates file"}

    all_ticks = []
    for path in dsm_files:
        all_ticks.extend(read_jsonl_objects(path))

    if not all_ticks:
        return {"run_dir": str(run_dir), "scale": run_dir.name, "n_ticks": 0}

    step_keys = [
        "s1_check_and_update_replicas",
        "s2_check_curr_status",
        "s3_drain_nodes",
        "s4_scale_replicas",
        "s5_update_status",
        "s6_schedule_and_stop",
        "s7_broadcast",
    ]
    totals = [t["subs"].get("total", 0) for t in all_ticks]
    step_vals = {k: [t["subs"].get(k, 0) for t in all_ticks] for k in step_keys}
    n_replicas_vals = [t.get("n_replicas_total", 0) for t in all_ticks]
    n_broadcasts_vals = [t.get("n_broadcasts_attempted", 0) for t in all_ticks]

    # Worst ticks by total duration
    worst = sorted(all_ticks, key=lambda t: t["subs"].get("total", 0), reverse=True)[:3]

    return {
        "run_dir": str(run_dir),
        "scale": run_dir.name,
        "n_ticks": len(all_ticks),
        "mean_n_replicas": round(mean(n_replicas_vals), 1) if n_replicas_vals else 0,
        "max_n_replicas": max(n_replicas_vals) if n_replicas_vals else 0,
        "mean_n_broadcasts": round(mean(n_broadcasts_vals), 1) if n_broadcasts_vals else 0,
        "total_mean_ms": round(1000 * mean(totals), 2),
        "total_p50_ms": round(1000 * median(totals), 2),
        "total_p99_ms": round(1000 * percentile(totals, 99), 2),
        "total_max_ms": round(1000 * max(totals), 2),
        "step_mean_ms": {k: round(1000 * mean(v), 2) for k, v in step_vals.items()},
        "step_max_ms": {k: round(1000 * max(v), 2) for k, v in step_vals.items()},
        "worst_ticks": [
            {
                "t": t["t"],
                "n_replicas": t.get("n_replicas_total"),
                "subs_ms": {k: round(v * 1000, 2) for k, v in t["subs"].items()},
            }
            for t in worst
        ],
    }


def _get(obj, path, default="-"):
    for k in path.split("."):
        if not isinstance(obj, dict):
            return default
        obj = obj.get(k)
    return obj if obj is not None else default


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("run_dirs", nargs="+", type=Path)
    args = p.parse_args()

    summaries = [summarize(rd) for rd in args.run_dirs]
    scales = [s.get("scale", "?") for s in summaries]

    def row(label, key, fmt="{:>14}"):
        vals = [_get(s, key) for s in summaries]
        print(f"  {label:<40} " + " | ".join(fmt.format(v) for v in vals))

    print(f"\n  {'metric':<40} " + " | ".join(f"{sc:>14}" for sc in scales))

    print("\n--- DSM update counts ---")
    row("n_ticks", "n_ticks")
    row("mean n_replicas", "mean_n_replicas")
    row("max n_replicas", "max_n_replicas")

    print("\n--- Total dsm.update() duration (ms) ---")
    row("mean", "total_mean_ms")
    row("p50", "total_p50_ms")
    row("p99", "total_p99_ms")
    row("MAX", "total_max_ms")

    print("\n--- Per-step MEAN time (ms) ---")
    for k in [
        "s1_check_and_update_replicas",
        "s2_check_curr_status",
        "s3_drain_nodes",
        "s4_scale_replicas",
        "s5_update_status",
        "s6_schedule_and_stop",
        "s7_broadcast",
    ]:
        row(k, f"step_mean_ms.{k}")

    print("\n--- Per-step MAX time (ms) — peak stalls ---")
    for k in [
        "s1_check_and_update_replicas",
        "s2_check_curr_status",
        "s3_drain_nodes",
        "s4_scale_replicas",
        "s5_update_status",
        "s6_schedule_and_stop",
        "s7_broadcast",
    ]:
        row(k, f"step_max_ms.{k}")

    print("\n--- Worst tick breakdown (ms) ---")
    for s in summaries:
        print(f"\n{s.get('scale', '?')}:")
        for wt in s.get("worst_ticks", []):
            print(f"  n_replicas={wt.get('n_replicas')} subs={wt.get('subs_ms')}")


if __name__ == "__main__":
    main()
