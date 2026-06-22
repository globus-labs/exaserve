"""SC26 workshop FULL-SWEEP figures.

Reads the completed full sweep under
  <experiments_root>/runs/sc26workshop/full/<spec>[_nostream]/runN/nM/results/result0.json
and renders the paper's headline figures. Reuses the cell-extraction machinery
(streaming TTFT/TBT/E2E + paper-SLO attainment, cached) from sc26_preview by
retargeting its RUNS_ROOT/CACHE_DIR to the full sweep.

Key modeling note: the paper SLO (TTFT<=1s AND P99-TBT<=250ms) only applies to
STREAMING. Non-stream (E2E) cells have no per-token TBT, so they are shown by
throughput + E2E latency, never by TBT-attainment.

Figures:
  fig1_proxy_scaling   — successful throughput | success-rate | SLO attainment vs N
                         (streaming; the litellm/rayserve saturation result)
  fig2_two_mode        — SSE vs E2E: throughput + E2E latency vs N (working proxies)
  fig3_workload        — oat_8b workloads + 120b: attainment & E2E at n1 vs n64
  fig4_latency         — TTFT/TBT/E2E percentiles per proxy at n64 (where SLO is met/missed)
"""
from __future__ import annotations
import os, sys
from pathlib import Path
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")
sys.path.insert(0, str(Path(__file__).resolve().parent))

import numpy as np
import sc26_preview as P

# Retarget the extraction to the full sweep (separate cache from validation).
P.RUNS_ROOT = Path("/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs/sc26workshop/full")
P.CACHE_DIR = Path("/tmp/sc26_full_cache")
OUT = Path(__file__).resolve().parent / "output" / "sc26_full"
OUT.mkdir(parents=True, exist_ok=True)

PROXIES = ["direct", "haproxy", "envoy", "rayserve", "litellm"]
PNODES = [1, 4, 16, 64]              # full-sweep proxycmp_<p>
ALLNODES = [1, 4, 16, 64, 128, 256]  # + proxycmp_<p>_scale for n>=128
COLORS = P.COLORS


def pstem(p, n):
    """128n/256n live in proxycmp_<p>_scale; <=64 in the full-sweep spec."""
    return f"proxycmp_{p}_scale" if n >= 128 else f"proxycmp_{p}"


def nstem(p, n):
    """Non-stream (E2E): 128/256 in proxycmp_<p>_nostream_scale; <=64 in _nostream."""
    return f"proxycmp_{p}_nostream_scale" if n >= 128 else f"proxycmp_{p}_nostream"
OAT = [("oat_8b_baseline","baseline"),("oat_8b_poisson","poisson"),
       ("oat_8b_2kx2k","2k×2k"),("oat_8b_4kx4k","4k×4k"),("oat_8b_code","code"),
       ("oat_8b_chat","chat"),("oat_8b_summary","summary"),("oat_8b_burstgpt","burstgpt")]


def cell(stem, n, refresh=False):
    try:
        return P.extract_cell(stem, n, keep_arrays=False, refresh=refresh)
    except Exception as e:
        print(f"  ! extract {stem} n{n}: {e}")
        return None


def extract_run(stem, n, good):
    """Like extract_cell but from a SPECIFIC set of data run_index values — used
    to drop node-failure-killed runs (full 0%-success deaths that are infra, not
    proxy behavior) so a cell's point reflects its healthy run(s)."""
    import ijson
    from sc26_preview import CellStats
    src = P.resolve_cell(stem, n)
    if src is None:
        return None
    completed = duration = 0.0
    with open(src, "rb") as fh:
        for pr in ijson.items(fh, "per_run.item"):
            if int(pr.get("run_index", 0)) in good:
                completed += float(pr.get("requests_completed", 0) or 0)
                duration += float(pr.get("duration_s", 0) or 0)
    rps = completed / duration if duration > 0 else float("nan")
    ttft = []; tbt = []; lat = []; dec = []; nreq = nsucc = nmeet = 0
    with open(src, "rb") as fh:
        for r in ijson.items(fh, "requests.item"):
            if int(r.get("run_index", 0)) not in good:
                continue
            nreq += 1; ok = bool(r.get("success", True))
            t = r.get("ttft_s"); b = r.get("tbt_p99_s"); l = r.get("latency")
            t = float(t) if t is not None else float("nan")
            b = float(b) if b is not None else float("nan")
            l = float(l) if l is not None else float("nan")
            if ok:
                nsucc += 1; ttft.append(t); tbt.append(b); lat.append(l); dec.append(l - t)
            if ok and not np.isnan(t) and t <= P.TTFT_SLO_S and not np.isnan(b) and b <= P.TBT_P99_SLO_S:
                nmeet += 1
    arr = lambda x: np.asarray(x, dtype=np.float32)
    _p = lambda a, q: float(np.nanpercentile(a, q)) if a.size else float("nan")
    ta, ba, la, da = arr(ttft), arr(tbt), arr(lat), arr(dec)
    att = nmeet / nreq if nreq else float("nan")
    return CellStats(spec=stem, node=n, src=str(src), n_req=nreq, n_success=nsucc, rps=rps,
                     attainment=att, goodput=rps * att,
                     success_rate=(nsucc / nreq if nreq else float("nan")),
                     ttft_p50=_p(ta, 50), ttft_p99=_p(ta, 99), tbt_p50=_p(ba, 50), tbt_p99=_p(ba, 99),
                     e2e_p50=_p(la, 50), e2e_p99=_p(la, 99), decode_p50=_p(da, 50), decode_p99=_p(da, 99))


def build(refresh=False):
    """Extract every cell once (cached). Returns nested dicts."""
    S = {}  # streaming: (proxy,n) -> CellStats  (incl. 128/256 from _scale)
    NS = {}  # nostream:  (proxy,n) -> CellStats  (only <=64; no nostream scale runs)
    for p in PROXIES:
        for n in ALLNODES:
            S[(p, n)] = cell(pstem(p, n), n, refresh)
            NS[(p, n)] = cell(nstem(p, n), n, refresh)
    # envoy 256n: both clean attempts lost a node mid-job; use the healthy data
    # run (run_index=1) only, dropping the node-failure-killed run. Backstop retry
    # (proxycmp_envoy_256retry) queued — swap in if it lands clean.
    ej = extract_run("proxycmp_envoy_scale", 256, {1})
    if ej and not np.isnan(ej.rps):
        S[("envoy", 256)] = ej
    O = {}   # oat streaming/(stem,n); ONS nostream
    ONS = {}
    for stem, _ in OAT + [("oat_120b", "120b")]:
        for n in (1, 64):
            O[(stem, n)] = cell(stem, n, refresh)
            ONS[(stem, n)] = cell(f"{stem}_nostream", n, refresh)
    return S, NS, O, ONS


def _plt():
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    return plt


def fig1_proxy_scaling(S):
    """The headline: streaming proxy comparison. Successful throughput, success
    rate, and SLO attainment vs N — haproxy/envoy/direct scale, litellm/rayserve
    saturate."""
    plt = _plt()
    fig, ax = plt.subplots(3, 1, figsize=(11, 14))
    for p in PROXIES:
        xs, sr, suc, att = [], [], [], []
        for n in ALLNODES:
            st = S.get((p, n))
            if not st or np.isnan(st.rps): continue
            xs.append(n); sr.append(st.rps * st.success_rate)
            suc.append(100 * st.success_rate); att.append(st.attainment)
        if not xs: continue
        c = COLORS[p]
        ax[0].plot(xs, sr, "o-", color=c, label=p, lw=2)
        ax[1].plot(xs, suc, "o-", color=c, label=p, lw=2)
        ax[2].plot(xs, att, "o-", color=c, label=p, lw=2)
    for a in ax: a.set_xscale("log", base=2); a.set_xticks(ALLNODES); a.set_xticklabels(ALLNODES); a.grid(alpha=.3); a.legend(fontsize=9)
    ax[0].set_ylabel("successful throughput (req/s)"); ax[0].set_title("Streaming proxy comparison — successful throughput vs nodes")
    ax[1].set_ylabel("success rate (%)"); ax[1].set_title("Request success rate (litellm/rayserve saturate)"); ax[1].set_ylim(-5, 105)
    ax[2].set_ylabel("paper SLO attainment"); ax[2].set_title("SLO attainment (TTFT≤1s ∧ P99-TBT≤250ms)"); ax[2].set_xlabel("nodes")
    fig.text(0.5, 0.005,
             "envoy 256n: healthy data run only (2 attempts lost a node mid-job; envoy itself did not crash). "
             "num_runs=6 for n≤64, 3 for n∈{128,256}.", ha="center", fontsize=7, style="italic")
    fig.tight_layout(rect=(0, 0.02, 1, 1)); out = OUT / "fig1_proxy_scaling.png"; fig.savefig(out, dpi=130); plt.close(fig); return out


def fig2_two_mode(S, NS):
    """SSE vs E2E for the proxies that survive (throughput + E2E p99 latency)."""
    plt = _plt()
    fig, ax = plt.subplots(2, 1, figsize=(11, 10))
    for p in ["direct", "haproxy", "envoy"]:
        c = COLORS[p]
        for mode, d, ls, mk in [("SSE", S, "-", "o"), ("E2E", NS, "--", "s")]:
            xs, sr, e2e = [], [], []
            for n in ALLNODES:
                st = d.get((p, n))
                if not st or np.isnan(st.rps): continue
                xs.append(n); sr.append(st.rps * st.success_rate); e2e.append(st.e2e_p99)
            if not xs: continue
            ax[0].plot(xs, sr, ls, marker=mk, color=c, label=f"{p} {mode}", lw=2)
            ax[1].plot(xs, e2e, ls, marker=mk, color=c, label=f"{p} {mode}", lw=2)
    for a in ax: a.set_xscale("log", base=2); a.set_xticks(ALLNODES); a.set_xticklabels(ALLNODES); a.grid(alpha=.3); a.legend(fontsize=8, ncol=2)
    ax[0].set_ylabel("successful throughput (req/s)"); ax[0].set_title("SSE (streaming) vs E2E (non-stream): throughput")
    ax[1].set_ylabel("E2E latency p99 (s)"); ax[1].set_title("SSE vs E2E: end-to-end latency p99"); ax[1].set_xlabel("nodes")
    fig.tight_layout(); out = OUT / "fig2_two_mode.png"; fig.savefig(out, dpi=130); plt.close(fig); return out


def fig3_workload(O, ONS):
    """oat_8b workloads + 120b at n1 vs n64: SLO attainment (streaming) and E2E p99."""
    plt = _plt()
    cells = OAT + [("oat_120b", "120b")]
    labels = [l for _, l in cells]
    x = np.arange(len(cells)); w = 0.35
    fig, ax = plt.subplots(2, 1, figsize=(13, 9))
    for i, n in enumerate((1, 64)):
        att = [(O.get((s, n)).attainment if O.get((s, n)) else np.nan) for s, _ in cells]
        e2e = [(O.get((s, n)).e2e_p99 if O.get((s, n)) else np.nan) for s, _ in cells]
        ax[0].bar(x + (i - .5) * w, att, w, label=f"n{n}")
        ax[1].bar(x + (i - .5) * w, e2e, w, label=f"n{n}")
    for a in ax: a.set_xticks(x); a.set_xticklabels(labels, rotation=30, ha="right"); a.grid(alpha=.3, axis="y"); a.legend()
    ax[0].set_ylabel("SLO attainment"); ax[0].set_title("Workload sweep (streaming) — SLO attainment, n1 vs n64"); ax[0].set_ylim(0, 1.05)
    ax[1].set_ylabel("E2E p99 (s)"); ax[1].set_title("Workload sweep — E2E latency p99")
    fig.tight_layout(); out = OUT / "fig3_workload.png"; fig.savefig(out, dpi=130); plt.close(fig); return out


def fig4_latency(S):
    """TTFT/TBT/E2E percentiles per proxy at n64 (streaming) — where SLO is met/missed."""
    plt = _plt()
    metrics = [("TTFT p99", "ttft_p99", P.TTFT_SLO_S), ("TBT p99", "tbt_p99", P.TBT_P99_SLO_S), ("E2E p99", "e2e_p99", None)]
    fig, ax = plt.subplots(1, 3, figsize=(15, 5))
    for j, (title, attr, slo) in enumerate(metrics):
        vals, names, cols = [], [], []
        for p in PROXIES:
            st = S.get((p, 64))
            if not st: continue
            v = getattr(st, attr)
            vals.append(v if not np.isnan(v) else 0); names.append(p); cols.append(COLORS[p])
        ax[j].bar(names, vals, color=cols)
        if slo: ax[j].axhline(slo, ls="--", color="red", label=f"SLO {slo}s")
        ax[j].set_title(f"{title} @ n64 (streaming)"); ax[j].set_ylabel("seconds"); ax[j].grid(alpha=.3, axis="y")
        ax[j].tick_params(axis="x", rotation=30)
        if slo: ax[j].legend()
    fig.tight_layout(); out = OUT / "fig4_latency_n64.png"; fig.savefig(out, dpi=130); plt.close(fig); return out


def main():
    refresh = "--refresh" in sys.argv
    print("building cells (full sweep)...")
    S, NS, O, ONS = build(refresh=refresh)
    got = sum(1 for v in {**S, **NS, **O, **ONS}.values() if v)
    print(f"  extracted {got} cells")
    for fn, args in [(fig1_proxy_scaling, (S,)), (fig2_two_mode, (S, NS)),
                     (fig3_workload, (O, ONS)), (fig4_latency, (S,))]:
        out = fn(*args); print(f"  wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
