#!/usr/bin/env python3
"""Plot preliminary results for the 4 evaluation dimensions.

Renders one PNG per spec into the chosen --out-dir:
  - slo_curve_v1.png     goodput-vs-offered-rate, faceted by num_nodes
  - longctx_v1.png       goodput-vs-nodes for input=64 vs input=4096
  - strong_scaling_v1.png achieved RPS + p99 TTFT vs nodes (fixed total load)
  - burstgpt_v1.png      goodput bars per preset, grouped by num_nodes

Reads result*.json from the corresponding run-group dirs (default: the latest
run_group under <experiments_root>/<spec>/).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from eval.plot.goodput import (  # noqa: E402
    SLO_PRESETS, _compute_goodput, walk_run_group, _extract_node_count,
)

EXP_ROOT = Path("/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs")


def _latest_rg(spec_name: str) -> Path:
    candidates = sorted((EXP_ROOT / spec_name).glob("run*"))
    if not candidates:
        raise FileNotFoundError(f"no run groups under {EXP_ROOT / spec_name}")
    return candidates[-1]


def _load_variants(run_group: Path) -> list[tuple[str, int, dict]]:
    """Return [(variant_dir_name, num_nodes, result_dict), ...]."""
    out = []
    for child in sorted(run_group.iterdir()):
        if not child.is_dir():
            continue
        n = _extract_node_count(child.name)
        if n == 0:
            continue
        rfiles = sorted((child / "results").glob("result*.json"))
        if not rfiles:
            continue
        data = json.loads(rfiles[-1].read_text())
        out.append((child.name, n, data))
    return out


def _percentile(xs: list[float], p: float) -> float:
    if not xs:
        return float("nan")
    s = sorted(xs)
    return s[min(len(s) - 1, int(p * len(s)))]


def _ttft_stats(data: dict) -> tuple[float, float, float]:
    ts = [r["ttft_s"] for r in data.get("requests", [])
          if r.get("ttft_s") is not None and r.get("success")]
    if not ts:
        return float("nan"), float("nan"), float("nan")
    return _percentile(ts, 0.5), _percentile(ts, 0.95), _percentile(ts, 0.99)


# ---------------------------------------------------------------------------
# Plot: SLO curve (slo_curve_v1)
# ---------------------------------------------------------------------------

PRESETS_TO_SHOW = ["interactive", "mlperf_8b", "e2e_2s"]
PRESET_STYLE = {
    "interactive": {"color": "#d62728", "marker": "o"},
    "mlperf_8b":   {"color": "#1f77b4", "marker": "s"},
    "e2e_2s":      {"color": "#2ca02c", "marker": "^"},
}


def plot_slo_curve(run_group: Path, out_path: Path) -> None:
    variants = _load_variants(run_group)
    by_node: dict[int, list[tuple[float, float, dict]]] = {}
    for name, n, data in variants:
        m = re.search(r"rps(\d+)", name)
        offered = int(m.group(1)) if m else 0
        achieved = data.get("overall", {}).get("rps", 0.0)
        by_node.setdefault(n, []).append((offered, achieved, data))

    fig, axes = plt.subplots(1, len(by_node), figsize=(15, 5), sharey=False)
    if len(by_node) == 1:
        axes = [axes]

    for ax, (n_nodes, pts) in zip(axes, sorted(by_node.items())):
        pts.sort()
        xs = [p[0] * n_nodes for p in pts]   # total offered RPS
        achieved = [p[1] for p in pts]
        ax.plot(xs, achieved, color="black", linestyle="--", marker="x",
                label="achieved RPS", linewidth=1)
        for preset_name in PRESETS_TO_SHOW:
            preset = SLO_PRESETS[preset_name]
            ys = [_compute_goodput(p[2], preset)["goodput"] for p in pts]
            ax.plot(xs, ys, label=f"goodput ({preset_name})",
                    **PRESET_STYLE[preset_name], linewidth=2)
        ax.set_xlabel("Offered RPS (total)")
        ax.set_title(f"{n_nodes} node{'s' if n_nodes > 1 else ''}")
        ax.grid(True, alpha=0.3)
        ax.legend(fontsize=8, loc="best")
    axes[0].set_ylabel("RPS")
    fig.suptitle("SLO-attainment curve: goodput vs offered load (slo_curve_v1)",
                 fontsize=13)
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Plot: Long context (longctx_v1)
# ---------------------------------------------------------------------------

def plot_longctx(run_group: Path, out_path: Path) -> None:
    variants = _load_variants(run_group)
    by_input: dict[int, list[tuple[int, float, dict]]] = {}
    for name, n, data in variants:
        m = re.search(r"in(\d+)", name)
        if not m:
            continue
        in_len = int(m.group(1))
        achieved = data.get("overall", {}).get("rps", 0.0)
        by_input.setdefault(in_len, []).append((n, achieved, data))

    fig, (ax_rps, ax_ttft) = plt.subplots(1, 2, figsize=(13, 5))
    in_lens = sorted(by_input)
    colors = {64: "#1f77b4", 4096: "#d62728"}
    for in_len in in_lens:
        pts = sorted(by_input[in_len])
        xs = [p[0] for p in pts]
        ys_rps = [p[1] for p in pts]
        ys_ttft_p99 = [_ttft_stats(p[2])[2] for p in pts]
        ax_rps.plot(xs, ys_rps, marker="o", linewidth=2,
                    color=colors.get(in_len, "gray"),
                    label=f"input_len={in_len}")
        ax_ttft.plot(xs, ys_ttft_p99, marker="s", linewidth=2,
                     color=colors.get(in_len, "gray"),
                     label=f"input_len={in_len}")

    # Ideal-linear reference using the smallest scale as base.
    for in_len in in_lens:
        pts = sorted(by_input[in_len])
        base_n, base_rps, _ = pts[0]
        ax_rps.plot([p[0] for p in pts],
                    [base_rps * p[0] / base_n for p in pts],
                    linestyle=":", color=colors.get(in_len, "gray"),
                    alpha=0.5, label=f"ideal linear ({in_len})")

    ax_rps.set_xscale("log", base=2)
    ax_rps.set_yscale("log")
    ax_rps.set_xlabel("Nodes")
    ax_rps.set_ylabel("Achieved RPS (log)")
    ax_rps.set_title("Throughput vs cluster size")
    ax_rps.grid(True, alpha=0.3, which="both")
    ax_rps.legend(fontsize=8)

    ax_ttft.set_xscale("log", base=2)
    ax_ttft.set_xlabel("Nodes")
    ax_ttft.set_ylabel("p99 TTFT (s)")
    ax_ttft.set_title("p99 TTFT vs cluster size")
    ax_ttft.grid(True, alpha=0.3, which="both")
    ax_ttft.legend(fontsize=8)

    fig.suptitle("Long-context behavior (longctx_v1)", fontsize=13)
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Plot: Strong scaling (strong_scaling_v1)
# ---------------------------------------------------------------------------

def plot_strong_scaling(run_group: Path, out_path: Path,
                        target_rps: float = 500.0) -> None:
    variants = _load_variants(run_group)
    pts = []
    for _, n, data in variants:
        achieved = data.get("overall", {}).get("rps", 0.0)
        ttft_p50, ttft_p95, ttft_p99 = _ttft_stats(data)
        e2e_p99 = data.get("overall", {}).get("p99_s", float("nan"))
        pts.append((n, achieved, ttft_p50, ttft_p99, e2e_p99))
    pts.sort()
    if not pts:
        raise RuntimeError("no variants found")
    xs = [p[0] for p in pts]
    achieved = [p[1] for p in pts]

    fig, (ax_rps, ax_lat) = plt.subplots(1, 2, figsize=(13, 5))
    ax_rps.axhline(target_rps, color="black", linestyle="--",
                   alpha=0.6, label=f"target {target_rps:.0f} RPS")
    ax_rps.plot(xs, achieved, marker="o", linewidth=2,
                color="#1f77b4", label="achieved")
    ax_rps.set_xscale("log", base=2)
    ax_rps.set_xlabel("Nodes")
    ax_rps.set_ylabel("Achieved RPS")
    ax_rps.set_title(f"Throughput at fixed {target_rps:.0f}-RPS target")
    ax_rps.grid(True, alpha=0.3, which="both")
    ax_rps.legend(fontsize=8)

    ax_lat.plot(xs, [p[2] for p in pts], marker="o",
                color="#2ca02c", label="TTFT p50")
    ax_lat.plot(xs, [p[3] for p in pts], marker="s",
                color="#d62728", label="TTFT p99")
    ax_lat.plot(xs, [p[4] for p in pts], marker="^",
                color="#9467bd", label="E2E p99")
    ax_lat.set_xscale("log", base=2)
    ax_lat.set_yscale("log")
    ax_lat.set_xlabel("Nodes")
    ax_lat.set_ylabel("Latency (s, log)")
    ax_lat.set_title("Latency vs nodes (more nodes -> lower latency?)")
    ax_lat.grid(True, alpha=0.3, which="both")
    ax_lat.legend(fontsize=8)

    fig.suptitle("Strong scaling (strong_scaling_v1)", fontsize=13)
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Plot: BurstGPT (burstgpt_v1)
# ---------------------------------------------------------------------------

def plot_burstgpt(run_group: Path, out_path: Path) -> None:
    variants = _load_variants(run_group)
    variants.sort(key=lambda x: x[1])
    if not variants:
        raise RuntimeError("no burstgpt variants found")

    fig, (ax_bar, ax_ttft) = plt.subplots(1, 2, figsize=(13, 5))
    width = 0.25
    xs = list(range(len(variants)))
    for i, preset_name in enumerate(PRESETS_TO_SHOW):
        preset = SLO_PRESETS[preset_name]
        ys = [_compute_goodput(v[2], preset)["goodput"] for v in variants]
        offset = (i - 1) * width
        ax_bar.bar([x + offset for x in xs], ys, width=width,
                   color=PRESET_STYLE[preset_name]["color"],
                   label=preset_name)
    # Also overlay achieved RPS as a black diamond on each group.
    ach = [v[2].get("overall", {}).get("rps", 0.0) for v in variants]
    ax_bar.plot(xs, ach, marker="D", color="black", linestyle="",
                label="achieved RPS")
    ax_bar.set_xticks(xs)
    ax_bar.set_xticklabels([f"{v[1]}n" for v in variants])
    ax_bar.set_ylabel("RPS")
    ax_bar.set_xlabel("Cluster size")
    ax_bar.set_title("Goodput per preset (BurstGPT peak window)")
    ax_bar.grid(True, alpha=0.3, axis="y")
    ax_bar.legend(fontsize=8)

    # TTFT distribution per cluster size: p50/p95/p99 vs nodes.
    ttft_p50 = [_ttft_stats(v[2])[0] for v in variants]
    ttft_p95 = [_ttft_stats(v[2])[1] for v in variants]
    ttft_p99 = [_ttft_stats(v[2])[2] for v in variants]
    ns = [v[1] for v in variants]
    ax_ttft.plot(ns, ttft_p50, marker="o", color="#2ca02c", label="TTFT p50")
    ax_ttft.plot(ns, ttft_p95, marker="s", color="#ff7f0e", label="TTFT p95")
    ax_ttft.plot(ns, ttft_p99, marker="^", color="#d62728", label="TTFT p99")
    ax_ttft.axhline(0.5, color="#d62728", linestyle=":", alpha=0.5,
                    label="interactive SLO (0.5s)")
    ax_ttft.axhline(2.0, color="#1f77b4", linestyle=":", alpha=0.5,
                    label="mlperf_8b SLO (2s)")
    ax_ttft.set_xscale("log", base=2)
    ax_ttft.set_yscale("log")
    ax_ttft.set_xlabel("Nodes")
    ax_ttft.set_ylabel("TTFT (s, log)")
    ax_ttft.set_title("TTFT distribution under burst")
    ax_ttft.grid(True, alpha=0.3, which="both")
    ax_ttft.legend(fontsize=7)

    fig.suptitle("BurstGPT (peak window) — bursty workload (burstgpt_v1)",
                 fontsize=13)
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out-dir", default="plan/preliminary_plots")
    args = parser.parse_args(argv)

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    plans = [
        ("slo_curve_v1", plot_slo_curve),
        ("longctx_v1", plot_longctx),
        ("strong_scaling_v1", plot_strong_scaling),
        ("burstgpt_v1", plot_burstgpt),
    ]
    for spec, fn in plans:
        try:
            rg = _latest_rg(spec)
            out_path = out_dir / f"{spec}.png"
            fn(rg, out_path)
            print(f"  {spec}: {out_path}")
        except Exception as exc:
            print(f"  {spec}: FAILED ({exc})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
