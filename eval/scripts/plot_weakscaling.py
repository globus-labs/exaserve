#!/usr/bin/env python3
"""Plot weak-scaling comparison: HAProxy vs Direct mode."""
import json
import os
import sys

RUNS_ROOT = "/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs"
RUN_GROUP = "run3"
RATE_PER_NODE = 17.5
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "findings", "weakscaling_haproxy_vs_direct.png")

def load_results(spec_name):
    results = {}
    for n in [1, 2, 4, 8, 16, 32, 64, 128]:
        f = os.path.join(RUNS_ROOT, spec_name, RUN_GROUP, f"{n}-nodes", "results", "result0.json")
        if os.path.isfile(f):
            with open(f) as fh:
                d = json.load(fh)
            o = d["overall"]
            target = n * RATE_PER_NODE
            results[n] = {
                "rps": o["rps"],
                "target": target,
                "efficiency": o["rps"] / target * 100,
                "errors": o["errors"],
                "p50": o["p50_s"],
                "p99": o["p99_s"],
                "duration": o["duration_s"],
            }
    return results

def main():
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not available, printing text summary only")
        plt = None

    haproxy = load_results("weakscaling_haproxy")
    direct = load_results("weakscaling_direct")

    # Text summary
    print("=" * 90)
    print(f"{'Mode':<10} {'Nodes':>5} {'Target':>8} {'Achieved':>10} {'Eff%':>6} {'Errors':>7} {'P50(s)':>8} {'P99(s)':>8}")
    print("-" * 90)
    for n in sorted(set(list(haproxy.keys()) + list(direct.keys()))):
        if n in haproxy:
            h = haproxy[n]
            print(f"{'HAProxy':<10} {n:>5} {h['target']:>8.0f} {h['rps']:>10.2f} {h['efficiency']:>5.1f}% {h['errors']:>7} {h['p50']:>8.3f} {h['p99']:>8.3f}")
        if n in direct:
            d = direct[n]
            print(f"{'Direct':<10} {n:>5} {d['target']:>8.0f} {d['rps']:>10.2f} {d['efficiency']:>5.1f}% {d['errors']:>7} {d['p50']:>8.3f} {d['p99']:>8.3f}")
        print()
    print("=" * 90)

    if plt is None:
        return

    # Common nodes
    h_nodes = sorted(haproxy.keys())
    d_nodes = sorted(direct.keys())

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("Weak-Scaling: HAProxy vs Direct Mode\n(Llama-3-8B, 2048in/128out, 17.5 rps/node)", fontsize=13)

    # 1. Throughput (RPS)
    ax = axes[0, 0]
    ax.plot(h_nodes, [haproxy[n]["rps"] for n in h_nodes], "o-", label="HAProxy", color="tab:blue", linewidth=2)
    ax.plot(d_nodes, [direct[n]["rps"] for n in d_nodes], "s-", label="Direct", color="tab:orange", linewidth=2)
    ideal_nodes = sorted(set(h_nodes + d_nodes))
    ax.plot(ideal_nodes, [n * RATE_PER_NODE for n in ideal_nodes], "--", label="Ideal", color="gray", alpha=0.5)
    ax.set_xlabel("Nodes")
    ax.set_ylabel("Throughput (rps)")
    ax.set_title("Achieved Throughput")
    ax.legend()
    ax.set_xscale("log", base=2)
    ax.set_yscale("log", base=2)
    ax.grid(True, alpha=0.3)

    # 2. Efficiency
    ax = axes[0, 1]
    ax.plot(h_nodes, [haproxy[n]["efficiency"] for n in h_nodes], "o-", label="HAProxy", color="tab:blue", linewidth=2)
    ax.plot(d_nodes, [direct[n]["efficiency"] for n in d_nodes], "s-", label="Direct", color="tab:orange", linewidth=2)
    ax.axhline(y=100, color="gray", linestyle="--", alpha=0.5)
    ax.axhline(y=90, color="gray", linestyle=":", alpha=0.3)
    ax.set_xlabel("Nodes")
    ax.set_ylabel("Scaling Efficiency (%)")
    ax.set_title("Scaling Efficiency")
    ax.legend()
    ax.set_xscale("log", base=2)
    ax.set_ylim(50, 105)
    ax.grid(True, alpha=0.3)

    # 3. P50 Latency
    ax = axes[1, 0]
    ax.plot(h_nodes, [haproxy[n]["p50"] for n in h_nodes], "o-", label="HAProxy p50", color="tab:blue", linewidth=2)
    ax.plot(d_nodes, [direct[n]["p50"] for n in d_nodes], "s-", label="Direct p50", color="tab:orange", linewidth=2)
    ax.set_xlabel("Nodes")
    ax.set_ylabel("Latency (s)")
    ax.set_title("P50 Latency")
    ax.legend()
    ax.set_xscale("log", base=2)
    ax.grid(True, alpha=0.3)

    # 4. P99 Latency
    ax = axes[1, 1]
    ax.plot(h_nodes, [haproxy[n]["p99"] for n in h_nodes], "o-", label="HAProxy p99", color="tab:blue", linewidth=2)
    ax.plot(d_nodes, [direct[n]["p99"] for n in d_nodes], "s-", label="Direct p99", color="tab:orange", linewidth=2)
    ax.set_xlabel("Nodes")
    ax.set_ylabel("Latency (s)")
    ax.set_title("P99 Latency")
    ax.legend()
    ax.set_xscale("log", base=2)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    plt.savefig(OUTPUT_PATH, dpi=150, bbox_inches="tight")
    print(f"\nPlot saved: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
