#!/usr/bin/env python3
"""Fair-comparison plot for longctx v2 reruns.

Renders one PNG showing TTFT p50/p95/p99 vs num_nodes for both input lengths,
under (a) equal offered rate (4 rps/N) and (b) equal saturation fraction.
Demonstrates that when load is matched, input=4096 has STRICTLY higher TTFT
than input=64 — the inverse pattern in v1 was a load-mismatch artifact.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from eval.site_config import get_runs_root

REPO_ROOT = Path(__file__).resolve().parents[2]
EXP = get_runs_root()


def _ttft_stats(data: dict) -> tuple[float, float, float]:
    ts = sorted(
        [
            r["ttft_s"]
            for r in data.get("requests", [])
            if r.get("ttft_s") is not None and r.get("success")
        ]
    )
    if not ts:
        return float("nan"), float("nan"), float("nan")
    return ts[len(ts) // 2], ts[int(len(ts) * 0.95)], ts[int(len(ts) * 0.99)]


def _load(spec: str) -> list[tuple[int, int, float, dict]]:
    """Return [(num_nodes, input_len, achieved_rps, data), ...]."""
    out = []
    rg = EXP / spec / "run0"
    for child in sorted(rg.iterdir()):
        if not child.is_dir() or not child.name.startswith("n"):
            continue
        rfile = child / "results" / "result0.json"
        if not rfile.exists():
            continue
        data = json.loads(rfile.read_text())
        # variant: n16-in4096 -> nodes=16, input_len=4096
        parts = child.name.split("-")
        n_nodes = int(parts[0].lstrip("n"))
        in_len = int(parts[1].lstrip("in"))
        rps = data.get("overall", {}).get("rps", 0.0)
        out.append((n_nodes, in_len, rps, data))
    return out


COLORS = {64: "#1f77b4", 4096: "#d62728"}
PCT_STYLE = {"p50": (":", "o"), "p95": ("--", "s"), "p99": ("-", "^")}


def _plot_panel(ax, variants, title: str, scenario_label: str) -> None:
    by_input: dict[int, list[tuple[int, tuple[float, float, float], float]]] = {}
    for n, in_len, achieved, data in variants:
        by_input.setdefault(in_len, []).append((n, _ttft_stats(data), achieved))

    for in_len, pts in sorted(by_input.items()):
        pts.sort()
        xs = [p[0] for p in pts]
        for pct_label, (ls, marker) in PCT_STYLE.items():
            i = {"p50": 0, "p95": 1, "p99": 2}[pct_label]
            ys = [p[1][i] for p in pts]
            ax.plot(
                xs,
                ys,
                linestyle=ls,
                marker=marker,
                color=COLORS[in_len],
                alpha=0.9 if pct_label == "p99" else 0.55,
                linewidth=2 if pct_label == "p99" else 1.2,
                label=f"in={in_len}, {pct_label}" if pct_label == "p99" else None,
            )
        # also annotate the achieved RPS on the p99 line
        for n, (_p50, _p95, p99), achieved in pts:
            ax.annotate(
                f"{achieved:.0f} rps",
                (n, p99),
                xytext=(4, 4),
                textcoords="offset points",
                fontsize=7,
                color=COLORS[in_len],
            )

    ax.axhline(0.5, color="gray", linestyle=":", alpha=0.4, label="interactive SLO (0.5s)")
    ax.axhline(2.0, color="gray", linestyle="--", alpha=0.4, label="mlperf_8b SLO (2s)")
    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xlabel("Nodes")
    ax.set_ylabel("TTFT (s, log) — solid=p99, dashed=p95, dotted=p50")
    ax.set_title(f"{title}\n{scenario_label}")
    ax.grid(True, alpha=0.3, which="both")
    ax.legend(fontsize=7, loc="best")


def main() -> int:
    eqrate = _load("longctx_v2_eqrate")
    eqsat = _load("longctx_v2_eqsat")
    if not eqrate:
        print("ERROR: no eqrate variants found", file=sys.stderr)
        return 1
    if not eqsat:
        print("WARN: no eqsat variants — drawing eqrate panel only", file=sys.stderr)

    if eqsat:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 5.5))
        _plot_panel(
            ax1,
            eqrate,
            "Equal offered rate (4 rps/N)",
            "Both lengths lightly loaded -> TTFT = prefill cost only",
        )
        _plot_panel(
            ax2,
            eqsat,
            "Equal saturation (~80% per length)",
            "in=64 @72 rps/N, in=4096 @16 rps/N — see notes below",
        )
    else:
        fig, ax1 = plt.subplots(1, 1, figsize=(8, 5.5))
        _plot_panel(
            ax1,
            eqrate,
            "Equal offered rate (4 rps/N)",
            "Both lengths lightly loaded -> TTFT = prefill cost only",
        )

    fig.suptitle(
        "Long-context v2: fair TTFT comparison "
        "(v1 conflated prefill cost with queue depth from rate mismatch)",
        fontsize=12,
    )
    fig.tight_layout()
    out_path = REPO_ROOT / "plan" / "preliminary_plots" / "longctx_v2_fair.png"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=120)
    print(f"Wrote {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
