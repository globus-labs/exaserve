#!/usr/bin/env python3
"""Analyze controller_ticks_<pid>.jsonl produced by the Section 6.1 probe.

Each line is one run_control_loop_step iteration with sub-phase timings.
We compute:
  - total number of ticks
  - mean/p99/max tick duration
  - per-subphase contribution (mean + max)
  - total wall-clock time in ticks (== controller reconcile time)
  - slowest sub-phase at peak

Usage: analyze_controller_ticks.py <run_dir1> [<run_dir2> ...]
"""

from __future__ import annotations

import argparse
from pathlib import Path
from statistics import mean, median

try:
    from .analysis_io import read_jsonl_objects
except ImportError:  # Direct ``python eval/tools/analyze_controller_ticks.py`` execution.
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
    tick_files = list(inst_root.glob("*/controller_ticks_*.jsonl"))
    if not tick_files:
        return {"run_dir": str(run_dir), "error": "no controller_ticks file"}

    # Should be exactly one file (single controller)
    all_ticks = []
    for tf in tick_files:
        all_ticks.extend(read_jsonl_objects(tf))

    if not all_ticks:
        return {"run_dir": str(run_dir), "scale": run_dir.name, "n_ticks": 0}

    totals = [t["total_s"] for t in all_ticks]
    subs_dsm = [t["subs"].get("dsm_update", 0) for t in all_ticks]
    subs_asm = [t["subs"].get("asm_update", 0) for t in all_ticks]
    subs_node = [t["subs"].get("node_update", 0) for t in all_ticks]
    subs_proxy = [t["subs"].get("proxy_state_update", 0) for t in all_ticks]
    subs_cluster = [t["subs"].get("cluster_node_info_update", 0) for t in all_ticks]

    # Time between ticks (sleep interval + tick overhead)
    gaps = []
    prev_end = None
    for t in all_ticks:
        start = t["t"]
        if prev_end is not None:
            gaps.append(start - prev_end)
        prev_end = start + t["total_s"]

    # Longest ticks — useful for finding where controller stalled
    worst_ticks = sorted(all_ticks, key=lambda t: t["total_s"], reverse=True)[:5]

    return {
        "run_dir": str(run_dir),
        "scale": run_dir.name,
        "n_ticks": len(all_ticks),
        "first_tick_t": all_ticks[0]["t"],
        "last_tick_t": all_ticks[-1]["t"] + all_ticks[-1]["total_s"],
        "wall_clock_span_s": round(
            all_ticks[-1]["t"] + all_ticks[-1]["total_s"] - all_ticks[0]["t"], 2
        ),
        "total_time_in_ticks_s": round(sum(totals), 2),
        "tick_total": {
            "mean_ms": round(1000 * mean(totals), 2),
            "p50_ms": round(1000 * median(totals), 2),
            "p95_ms": round(1000 * percentile(totals, 95), 2),
            "p99_ms": round(1000 * percentile(totals, 99), 2),
            "max_ms": round(1000 * max(totals), 2),
        },
        "tick_gap": {
            "mean_ms": round(1000 * mean(gaps), 2) if gaps else 0,
            "p99_ms": round(1000 * percentile(gaps, 99), 2) if gaps else 0,
            "max_ms": round(1000 * max(gaps), 2) if gaps else 0,
        },
        "subphase_mean_ms": {
            "dsm_update": round(1000 * mean(subs_dsm), 2),
            "asm_update": round(1000 * mean(subs_asm), 2),
            "node_update": round(1000 * mean(subs_node), 2),
            "proxy_state_update": round(1000 * mean(subs_proxy), 2),
            "cluster_node_info": round(1000 * mean(subs_cluster), 2),
        },
        "subphase_max_ms": {
            "dsm_update": round(1000 * max(subs_dsm), 2),
            "asm_update": round(1000 * max(subs_asm), 2),
            "node_update": round(1000 * max(subs_node), 2),
            "proxy_state_update": round(1000 * max(subs_proxy), 2),
            "cluster_node_info": round(1000 * max(subs_cluster), 2),
        },
        "worst_ticks": [
            {
                "num": t["num_loops"],
                "t": t["t"],
                "total_s": t["total_s"],
                "subs": t["subs"],
            }
            for t in worst_ticks
        ],
    }


def _get(obj, path, default=None):
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
        vals = [_get(s, key, "-") for s in summaries]
        print(f"  {label:<40} " + " | ".join(fmt.format(v) for v in vals))

    print(f"\n  {'metric':<40} " + " | ".join(f"{sc:>14}" for sc in scales))
    print("\n--- Controller tick rate ---")
    row("n_ticks", "n_ticks")
    row("wall span (s)", "wall_clock_span_s")
    row("total time in ticks (s)", "total_time_in_ticks_s")
    row("tick total mean (ms)", "tick_total.mean_ms")
    row("tick total p50 (ms)", "tick_total.p50_ms")
    row("tick total p95 (ms)", "tick_total.p95_ms")
    row("tick total p99 (ms)", "tick_total.p99_ms")
    row("tick total max (ms)", "tick_total.max_ms")

    print("\n--- Tick gap (sleep between ticks) ---")
    row("gap mean (ms)", "tick_gap.mean_ms")
    row("gap p99 (ms)", "tick_gap.p99_ms")
    row("gap max (ms)", "tick_gap.max_ms")

    print("\n--- Sub-phase mean (ms) ---")
    row("cluster_node_info", "subphase_mean_ms.cluster_node_info")
    row("dsm_update", "subphase_mean_ms.dsm_update")
    row("asm_update", "subphase_mean_ms.asm_update")
    row("node_update (proxy nodes)", "subphase_mean_ms.node_update")
    row("proxy_state_update", "subphase_mean_ms.proxy_state_update")

    print("\n--- Sub-phase MAX (ms) — peak stall ---")
    row("cluster_node_info", "subphase_max_ms.cluster_node_info")
    row("dsm_update", "subphase_max_ms.dsm_update")
    row("asm_update", "subphase_max_ms.asm_update")
    row("node_update (proxy nodes)", "subphase_max_ms.node_update")
    row("proxy_state_update", "subphase_max_ms.proxy_state_update")


if __name__ == "__main__":
    main()
