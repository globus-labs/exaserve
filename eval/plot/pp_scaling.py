"""405B shard-aware pipeline-parallel weak-scaling figure.

Companion to sc26_full_figures.py but for a DIFFERENT experiment: Llama-3.1-405B
served TP=8 x PP=2, one replica per 2 nodes, HAProxy round-robin across replicas,
offered load held at a fixed rate PER REPLICA (weak scaling). The scaling unit is
the REPLICA (= one PP=2 group = 2 nodes), so the x-axis is #replicas.

Reads result0.json from one explicit runN identity. Renders whatever node points
exist in that immutable run group. Style is shared with the paper figures via
plotstyle.py.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import numpy as np

import plotstyle as ps
from eval.site_config import get_runs_root

RUNS_ROOT = get_runs_root() / "sc26workshop/full/pp405b_pp2_scale"
OUT = Path(__file__).resolve().parent / "output" / "pp_scaling"
PP = 2  # nodes per replica

# HAProxy is the frontend for this sweep -> reuse its color/marker everywhere.
PROXY = "haproxy"


def _exact_result(run_group: Path, n: int):
    """Load one exact node result, rejecting corrupt or failed evidence."""
    result_path = run_group / f"n{n}" / "results" / "result0.json"
    if not result_path.is_file():
        return None
    with open(result_path, encoding="utf-8") as handle:
        data = json.load(handle)
    overall = data.get("overall", {})
    if overall.get("rps") is None or np.isnan(overall["rps"]):
        raise ValueError(f"{result_path}: missing or non-finite RPS")
    if overall.get("errors"):
        raise ValueError(f"{result_path}: result contains request errors")
    return overall


def load(run_group_id: str):
    """Return sorted list of point dicts: nodes, replicas, rps, tps, p50, p99."""
    if re.fullmatch(r"run\d+", run_group_id) is None:
        raise ValueError(f"invalid explicit run group {run_group_id!r}")
    run_group = RUNS_ROOT / run_group_id
    if not run_group.is_dir():
        raise FileNotFoundError(f"run group does not exist: {run_group}")
    pts = []
    for d in sorted(run_group.glob("n*")):
        n = int(re.search(r"/n(\d+)$", str(d)).group(1))
        o = _exact_result(run_group, n)
        if not o:
            continue
        pts.append(
            dict(
                nodes=n,
                replicas=n // PP,
                rps=o["rps"],
                tps=o.get("tps"),
                p50=o.get("p50_s"),
                p99=o.get("p99_s"),
                completed=o.get("requests_completed"),
            )
        )
    return sorted(pts, key=lambda p: p["replicas"])


def figure(pts):
    plt = ps.apply()
    if not pts:
        raise SystemExit("no valid pp405b_pp2_scale results found yet")
    reps = np.array([p["replicas"] for p in pts], float)
    rps = np.array([p["rps"] for p in pts], float)
    base_r, base_rps = reps[0], rps[0]
    per_rep = base_rps / base_r  # weak-scaling unit rate
    ideal = per_rep * reps
    eff = rps / ideal * 100.0

    fig, ax = plt.subplots(1, 2, figsize=(6.9, 2.7))

    # -- panel A: throughput vs replicas (log-log) with ideal-linear reference --
    a = ax[0]
    a.plot(
        reps,
        ideal,
        ls=":",
        lw=ps.LW,
        color="#888888",
        zorder=2,
        label=f"ideal (linear, {per_rep:.3f} rps/replica)",
    )
    ps.line(a, reps, rps, PROXY, label="405B PP=2 (measured)")
    for p, x, y in zip(pts, reps, rps):
        a.annotate(
            f"{y:.2f}\n({p['nodes']}n)",
            (x, y),
            textcoords="offset points",
            xytext=(0, 7),
            ha="center",
            va="bottom",
            fontsize=5.0,
            fontweight="bold",
            color=ps.PROXY_COLORS[PROXY],
        )
    a.set_xscale("log", base=2)
    a.set_yscale("log")
    a.set_xticks(reps)
    a.set_xticklabels([f"{int(r)}" for r in reps])
    ps.sparse_log_y(a)
    a.set_xlabel("replicas (PP=2 group, 2 nodes each)")
    a.set_ylabel("throughput (requests/s)")
    a.set_title("Aggregate throughput")
    a.set_xlim(reps.min() * 0.8, reps.max() * 1.25)
    ps.legend(a, loc="upper left")

    # -- panel B: weak-scaling efficiency vs replicas --
    b = ax[1]
    b.axhline(100, ls=":", lw=ps.LW, color="#888888", zorder=2, label="ideal (100%)")
    ps.line(b, reps, eff, PROXY, label="attained")
    for x, y in zip(reps, eff):
        b.annotate(
            f"{y:.0f}%",
            (x, y),
            textcoords="offset points",
            xytext=(0, 7),
            ha="center",
            va="bottom",
            fontsize=5.5,
            fontweight="bold",
            color=ps.PROXY_COLORS[PROXY],
        )
    b.set_xscale("log", base=2)
    b.set_xticks(reps)
    b.set_xticklabels([f"{int(r)}" for r in reps])
    b.set_xlabel("replicas (PP=2 group, 2 nodes each)")
    b.set_ylabel("weak-scaling efficiency (%)")
    b.set_title("Weak-scaling efficiency")
    b.set_ylim(0, 115)
    b.set_xlim(reps.min() * 0.8, reps.max() * 1.25)
    ps.legend(b, loc="lower left")

    span = (
        f"{int(reps.min())}–{int(reps.max())} replicas ({pts[0]['nodes']}–{pts[-1]['nodes']} nodes)"
    )
    top = ps.titles(
        fig,
        "Shard-aware pipeline-parallel weak scaling: Llama-3.1-405B (TP=8×PP=2)",
        [
            f"one replica per 2 nodes, HAProxy round-robin · {span} · fixed offered rate/replica",
            f"efficiency relative to the {int(base_r)}-replica base ({per_rep:.3f} rps/replica)",
        ],
    )
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out_pdf = OUT.with_suffix(".pdf")
    fig.tight_layout(rect=(0, 0, 1, top))
    fig.savefig(out_pdf)
    fig.savefig(OUT.with_suffix(".png"), dpi=200)
    plt.close(fig)
    return out_pdf


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-group", required=True, help="Exact runN identity")
    args = parser.parse_args(argv)
    pts = load(args.run_group)
    print("points:")
    for p in pts:
        print(
            f"  {p['nodes']:>4}n / {p['replicas']:>3} rep: rps={p['rps']:.2f} "
            f"tps={p['tps']:.0f} p50={p['p50']:.1f}s p99={p['p99']:.1f}s "
            f"completed={p['completed']}"
        )
    out = figure(pts)
    print("wrote", out)


if __name__ == "__main__":
    main()
