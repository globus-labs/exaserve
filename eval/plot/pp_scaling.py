"""405B shard-aware pipeline-parallel weak-scaling figure.

Companion to sc26_full_figures.py but for a DIFFERENT experiment: Llama-3.1-405B
served TP=8 x PP=2, one replica per 2 nodes, HAProxy round-robin across replicas,
offered load held at a fixed rate PER REPLICA (weak scaling). The scaling unit is
the REPLICA (= one PP=2 group = 2 nodes), so the x-axis is #replicas.

Reads:
  <RUNS_ROOT>/pp405b_pp2_scale/run*/n<M>/results/result0.json
picking, per node-point, the newest run with a valid 0-error result. Renders
whatever points exist (2 now, 3 once n256 lands) so it can be re-run as data
arrives. Style is shared with the paper figures via plotstyle.py.
"""
from __future__ import annotations

import glob
import json
import re
from pathlib import Path

import numpy as np

import plotstyle as ps

RUNS_ROOT = Path("/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs/"
                 "sc26workshop/full/pp405b_pp2_scale")
OUT = Path(__file__).resolve().parent / "output" / "pp_scaling"
PP = 2  # nodes per replica

# HAProxy is the frontend for this sweep -> reuse its color/marker everywhere.
PROXY = "haproxy"


def _best_result(n):
    """Newest run dir for node-count n with a valid, 0-error result0.json."""
    best = None
    for R in sorted(glob.glob(str(RUNS_ROOT / f"run*/n{n}/results/result0.json"))):
        run = int(re.search(r"/run(\d+)/", R).group(1))
        try:
            d = json.load(open(R))
        except Exception:
            continue
        o = d.get("overall", {})
        if o.get("rps") is None or np.isnan(o["rps"]):
            continue
        if o.get("errors"):  # skip runs with request errors
            continue
        if best is None or run > best[0]:
            best = (run, o)
    return best[1] if best else None


def load():
    """Return sorted list of point dicts: nodes, replicas, rps, tps, p50, p99."""
    pts = []
    for d in sorted(RUNS_ROOT.glob("run*/n*")):
        n = int(re.search(r"/n(\d+)$", str(d)).group(1))
        if any(p["nodes"] == n for p in pts):
            continue
        o = _best_result(n)
        if not o:
            continue
        pts.append(dict(nodes=n, replicas=n // PP, rps=o["rps"], tps=o.get("tps"),
                        p50=o.get("p50_s"), p99=o.get("p99_s"),
                        completed=o.get("requests_completed")))
    return sorted(pts, key=lambda p: p["replicas"])


def figure(pts):
    plt = ps.apply()
    if not pts:
        raise SystemExit("no valid pp405b_pp2_scale results found yet")
    reps = np.array([p["replicas"] for p in pts], float)
    rps = np.array([p["rps"] for p in pts], float)
    base_r, base_rps = reps[0], rps[0]
    per_rep = base_rps / base_r                       # weak-scaling unit rate
    ideal = per_rep * reps
    eff = rps / ideal * 100.0

    fig, ax = plt.subplots(1, 2, figsize=(6.9, 2.7))

    # -- panel A: throughput vs replicas (log-log) with ideal-linear reference --
    a = ax[0]
    a.plot(reps, ideal, ls=":", lw=ps.LW, color="#888888", zorder=2,
           label=f"ideal (linear, {per_rep:.3f} rps/replica)")
    ps.line(a, reps, rps, PROXY, label="405B PP=2 (measured)")
    for p, x, y in zip(pts, reps, rps):
        a.annotate(f"{y:.2f}\n({p['nodes']}n)", (x, y), textcoords="offset points",
                   xytext=(0, 7), ha="center", va="bottom", fontsize=5.0,
                   fontweight="bold", color=ps.PROXY_COLORS[PROXY])
    a.set_xscale("log", base=2); a.set_yscale("log")
    a.set_xticks(reps); a.set_xticklabels([f"{int(r)}" for r in reps])
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
        b.annotate(f"{y:.0f}%", (x, y), textcoords="offset points", xytext=(0, 7),
                   ha="center", va="bottom", fontsize=5.5, fontweight="bold",
                   color=ps.PROXY_COLORS[PROXY])
    b.set_xscale("log", base=2)
    b.set_xticks(reps); b.set_xticklabels([f"{int(r)}" for r in reps])
    b.set_xlabel("replicas (PP=2 group, 2 nodes each)")
    b.set_ylabel("weak-scaling efficiency (%)")
    b.set_title("Weak-scaling efficiency")
    b.set_ylim(0, 115)
    b.set_xlim(reps.min() * 0.8, reps.max() * 1.25)
    ps.legend(b, loc="lower left")

    span = f"{int(reps.min())}–{int(reps.max())} replicas ({pts[0]['nodes']}–{pts[-1]['nodes']} nodes)"
    top = ps.titles(
        fig,
        "Shard-aware pipeline-parallel weak scaling: Llama-3.1-405B (TP=8×PP=2)",
        [f"one replica per 2 nodes, HAProxy round-robin · {span} · fixed offered rate/replica",
         f"efficiency relative to the {int(base_r)}-replica base ({per_rep:.3f} rps/replica)"])
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out_pdf = OUT.with_suffix(".pdf")
    fig.tight_layout(rect=(0, 0, 1, top))
    fig.savefig(out_pdf)
    fig.savefig(OUT.with_suffix(".png"), dpi=200)
    plt.close(fig)
    return out_pdf


def main():
    pts = load()
    print("points:")
    for p in pts:
        print(f"  {p['nodes']:>4}n / {p['replicas']:>3} rep: rps={p['rps']:.2f} "
              f"tps={p['tps']:.0f} p50={p['p50']:.1f}s p99={p['p99']:.1f}s "
              f"completed={p['completed']}")
    out = figure(pts)
    print("wrote", out)


if __name__ == "__main__":
    main()
