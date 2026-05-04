#!/usr/bin/env python3
"""Compare HAProxy and direct mode weak-scaling on the same plot."""
import json
import os
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

RUNS_ROOT = "/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs"

SPECS = {
    "HAProxy (1 LB on head)": "weakscaling_haproxy_short_v3",
    "Direct (1 client/node, hash-shard)": "weakscaling_direct_short_v3",
}
RUN_GROUP = "run1"
NODE_COUNTS = [1, 2, 4, 8, 16, 32, 64, 128, 256]


def load_one(spec_name: str) -> list[tuple[int, float, float, float]]:
    """Return list of (nodes, rps, p50_ms, errors) from a spec's run group."""
    rows = []
    for n in NODE_COUNTS:
        d = Path(RUNS_ROOT) / spec_name / RUN_GROUP / f"{n}-nodes"
        results = sorted(d.glob("results/result*.json"))
        if not results:
            continue
        with open(results[-1]) as f:
            data = json.load(f)
        overall = data.get("overall", {})
        rps = float(overall.get("rps", 0))
        p50 = float(overall.get("p50_s", 0)) * 1000
        errors = int(overall.get("errors", 0))
        rows.append((n, rps, p50, errors))
    return rows


def main() -> int:
    fig, (ax_rps, ax_lat) = plt.subplots(1, 2, figsize=(14, 6))

    colors = {
        "HAProxy (1 LB on head)": "#1f77b4",
        "Direct (1 client/node, hash-shard)": "#d62728",
    }
    markers = {
        "HAProxy (1 LB on head)": "o",
        "Direct (1 client/node, hash-shard)": "s",
    }

    for label, spec in SPECS.items():
        rows = load_one(spec)
        if not rows:
            print(f"  {label}: NO DATA")
            continue
        nodes = [r[0] for r in rows]
        rps = [r[1] for r in rows]
        p50 = [r[2] for r in rows]
        print(f"  {label}: {len(rows)} points")
        for n, r, p, e in rows:
            print(f"    {n:4d} nodes  rps={r:8.1f}  p50={p:6.1f}ms  errors={e}")
        ax_rps.plot(nodes, rps, marker=markers[label], color=colors[label],
                    label=label, linewidth=2, markersize=8)
        ax_lat.plot(nodes, p50, marker=markers[label], color=colors[label],
                    label=label, linewidth=2, markersize=8)

    # Ideal linear scaling reference (based on 1-node RPS)
    base_rps = None
    for label, spec in SPECS.items():
        rows = load_one(spec)
        if rows and rows[0][0] == 1:
            base_rps = rows[0][1]
            break
    if base_rps:
        ideal_x = NODE_COUNTS
        ideal_y = [base_rps * n for n in ideal_x]
        ax_rps.plot(ideal_x, ideal_y, "--", color="gray",
                    label=f"Ideal linear (= {base_rps:.0f} RPS × N)",
                    linewidth=1.5, alpha=0.7)

    ax_rps.set_xscale("log", base=2)
    ax_rps.set_yscale("log")
    ax_rps.set_xlabel("Nodes")
    ax_rps.set_ylabel("Achieved RPS")
    ax_rps.set_title("Weak-Scaling Throughput\nLlama-3-8B, 64 in / 64 out, target 110 RPS/node")
    ax_rps.set_xticks(NODE_COUNTS)
    ax_rps.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
    ax_rps.grid(True, which="both", alpha=0.3)
    ax_rps.legend(loc="upper left", fontsize=9)

    ax_lat.set_xscale("log", base=2)
    ax_lat.set_xlabel("Nodes")
    ax_lat.set_ylabel("p50 latency (ms)")
    ax_lat.set_title("Per-Request Latency")
    ax_lat.set_xticks(NODE_COUNTS)
    ax_lat.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
    ax_lat.grid(True, which="both", alpha=0.3)
    ax_lat.legend(loc="upper left", fontsize=9)

    fig.suptitle(
        "HAProxy vs Direct Dispatch — Aurora Weak-Scaling v3 (run1)",
        fontsize=14, fontweight="bold",
    )
    fig.tight_layout()

    out = sys.argv[1] if len(sys.argv) > 1 else "/home/wenyiw/aurora_rayserver/findings/weakscaling_haproxy_vs_direct_v3.png"
    fig.savefig(out, dpi=120, bbox_inches="tight")
    print(f"\nSaved: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
