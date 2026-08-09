#!/usr/bin/env python3
"""Saturation finder plot.

Renders TTFT-vs-offered-rate and throughput-vs-offered-rate for a sat-finder
run group, with the knee point highlighted.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from eval.site_config import get_runs_root

REPO_ROOT = Path(__file__).resolve().parents[2]
EXP = get_runs_root()


def _ttft(reqs):
    ts = sorted([r["ttft_s"] for r in reqs if r.get("ttft_s") is not None and r.get("success")])
    if not ts:
        return float("nan"), float("nan"), float("nan")
    return ts[len(ts) // 2], ts[int(len(ts) * 0.95)], ts[int(len(ts) * 0.99)]


def _load(spec: str) -> list[tuple[float, float, tuple, dict]]:
    """Return [(offered, achieved, (ttft_p50, p95, p99), data), ...] sorted by offered."""
    out = []
    rg = EXP / spec / "run0"
    for child in sorted(rg.iterdir()):
        if not child.is_dir() or not child.name.startswith("rps"):
            continue
        rfile = child / "results" / "result0.json"
        if not rfile.exists():
            continue
        data = json.loads(rfile.read_text())
        offered = float(child.name[3:])
        achieved = data.get("overall", {}).get("rps", 0.0)
        ttft = _ttft(data.get("requests", []))
        out.append((offered, achieved, ttft, data))
    out.sort()
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", default="longctx_sat_4096")
    parser.add_argument("--out", default="plan/preliminary_plots/longctx_sat_4096.png")
    parser.add_argument(
        "--ttft-slo",
        type=float,
        default=2.0,
        help="SLO line on the TTFT plot (default 2s = mlperf_8b)",
    )
    args = parser.parse_args()

    rows = _load(args.spec)
    if not rows:
        print(f"No results found for {args.spec}", file=sys.stderr)
        return 1

    offered = [r[0] for r in rows]
    achieved = [r[1] for r in rows]
    p50 = [r[2][0] for r in rows]
    p95 = [r[2][1] for r in rows]
    p99 = [r[2][2] for r in rows]

    # Knee = first point where p99 exceeds SLO.
    knee_idx = next((i for i, v in enumerate(p99) if v > args.ttft_slo), None)

    fig, (ax_t, ax_r) = plt.subplots(1, 2, figsize=(13, 5))

    ax_t.plot(offered, p50, marker="o", color="#2ca02c", label="TTFT p50")
    ax_t.plot(offered, p95, marker="s", color="#ff7f0e", label="TTFT p95")
    ax_t.plot(offered, p99, marker="^", color="#d62728", linewidth=2.5, label="TTFT p99")
    ax_t.axhline(
        args.ttft_slo, color="black", linestyle="--", alpha=0.5, label=f"SLO ({args.ttft_slo}s)"
    )
    ax_t.axhline(0.5, color="gray", linestyle=":", alpha=0.4, label="interactive SLO (0.5s)")
    if knee_idx is not None:
        knee_x = offered[knee_idx]
        ax_t.axvline(knee_x, color="red", linestyle="--", alpha=0.6)
        ax_t.text(
            knee_x + 0.2,
            ax_t.get_ylim()[1] * 0.5,
            f"knee @ {knee_x:.0f} rps/N\n(p99 exceeds SLO)",
            fontsize=9,
            color="red",
        )
    ax_t.set_xlabel("Offered rps/N (input_len=4096, 1 node)")
    ax_t.set_ylabel("TTFT (s)")
    ax_t.set_title("TTFT vs offered rate")
    ax_t.grid(True, alpha=0.3)
    ax_t.legend(fontsize=8, loc="best")

    ax_r.plot(
        offered, offered, color="gray", linestyle=":", alpha=0.6, label="ideal (achieved = offered)"
    )
    ax_r.plot(offered, achieved, marker="o", color="#1f77b4", linewidth=2, label="achieved")
    for i, (off, ach) in enumerate(zip(offered, achieved)):
        ax_r.annotate(
            f"{ach / off * 100:.0f}%",
            (off, ach),
            xytext=(4, -10),
            textcoords="offset points",
            fontsize=8,
        )
    ax_r.set_xlabel("Offered rps/N")
    ax_r.set_ylabel("Achieved RPS")
    ax_r.set_title("Throughput: achieved vs offered")
    ax_r.grid(True, alpha=0.3)
    ax_r.legend(fontsize=8, loc="best")

    fig.suptitle(f"Saturation finder: {args.spec} (Llama-3-8B, 1 node, 12 replicas)", fontsize=13)
    fig.tight_layout()
    out_path = Path(args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=120)
    print(f"Wrote {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
