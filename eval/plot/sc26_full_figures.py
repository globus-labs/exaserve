"""SC26 workshop FULL-SWEEP figures.

Reads the completed full sweep under
  <experiments_root>/runs/sc26workshop/full/<spec>[_nostream]/runN/nM/results/result0.json
and renders the paper's headline figures, reusing the cached cell-extraction
machinery from sc26_preview (retargeted RUNS_ROOT/CACHE_DIR).

Consistent styling lives in eval/plot/plotstyle.py + eval/plot/matplotlibrc:
colour+marker = proxy, line style = mode (solid stream / dashed non-stream),
SAME across every figure. Value labels stack on collision (dashed box border for
dashed series). Multi-run cells get error bars (mean ± std over data runs).

Figures:
  fig1_proxy_scaling — successful throughput (stream+non-stream) | success rate |
                       TTFT attainment @ 1/2/3s | TBT attainment, vs N
  fig2_two_mode      — E2E latency p50 & p99, stream vs non-stream, vs N
  fig3_workload      — OAT 8B + 120B at n1 vs n64: SLO attainment & E2E p99
  fig4_latency       — TTFT/TBT/E2E p99 per proxy @ n64
  fig5_latency_cdf   — TTFT & P99-TBT CDFs (failures included → curves cap at the
                       success rate), per node count (n4/n64/n128)
"""
from __future__ import annotations
import os, sys
from pathlib import Path
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")
sys.path.insert(0, str(Path(__file__).resolve().parent))

import numpy as np
import sc26_preview as P
import plotstyle as ps

# Retarget the extraction to the full sweep (separate cache from validation).
P.RUNS_ROOT = Path("/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs/sc26workshop/full")
P.CACHE_DIR = Path("/tmp/sc26_full_cache")
OUT = Path(__file__).resolve().parent / "output" / "sc26_full" / "iter2"
OUT.mkdir(parents=True, exist_ok=True)

plt = ps.apply()                     # load the project matplotlibrc theme once

PROXIES = ["direct", "haproxy", "envoy", "rayserve", "litellm"]
ALLNODES = [1, 4, 16, 64, 128, 256]  # proxycmp_<p>[_scale]
CDF_NODES = [4, 64, 128]             # node counts that keep per-request arrays
COLORS = ps.PROXY_COLORS
MARKERS = ps.PROXY_MARKERS
HATCHES = {"direct": "", "haproxy": "//", "envoy": "\\\\", "rayserve": "xx", "litellm": ".."}
TTFT_THRS = P.TTFT_SLO_MULTI         # (1.0, 2.0, 3.0)

# Multi-line config subtitle (shared by the Set-1 proxy figures).
CFG_8B = ["Meta-Llama-3-8B-Instruct  ·  TP=1 (1 replica/node)  ·  64→64 tok",
          "110 req/s/node offered  ·  ALCF Aurora"]


# --- per-run helpers (error bars) -------------------------------------------

def _mean_std(vals):
    a = np.asarray([v for v in (vals or [])
                    if v is not None and not (isinstance(v, float) and np.isnan(v))], float)
    if a.size == 0:
        return float("nan"), 0.0
    return float(a.mean()), float(a.std())


_AGG = {  # fallback aggregate when per-run lists are absent (old cache / extract_run)
    "succ_rps":  lambda st: (st.rps * st.success_rate) if not np.isnan(st.rps) else float("nan"),
    "succ_rate": lambda st: st.success_rate,
    "tbt":       lambda st: st.tbt_attainment,
    "ttft1":     lambda st: st.ttft_attain_1s,
    "ttft2":     lambda st: st.ttft_attain_2s,
    "ttft3":     lambda st: st.ttft_attain_3s,
}


def _pt(st, kind):
    """(mean, std) of a per-run metric for error bars; falls back to the cell
    aggregate (std 0) when per-run data is unavailable."""
    runs = {"succ_rps": st.runs_succ_rps, "succ_rate": st.runs_succ_rate,
            "tbt": st.runs_tbt_attain, "ttft1": st.runs_ttft_attain_1s,
            "ttft2": st.runs_ttft_attain_2s, "ttft3": st.runs_ttft_attain_3s}.get(kind)
    m, s = _mean_std(runs)
    if np.isnan(m):
        return _AGG[kind](st), 0.0
    return m, s


def _collect(src, proxy, kind, scale=1.0):
    """xs, ys, yerr across ALLNODES for one proxy/metric from a cell dict."""
    xs, ys, es = [], [], []
    for n in ALLNODES:
        st = src.get((proxy, n))
        if not st or np.isnan(st.rps):
            continue
        v, e = _pt(st, kind)
        if v is None or np.isnan(v):
            continue
        xs.append(n); ys.append(v * scale); es.append(e * scale)
    return xs, ys, es


# --- cell resolution / extraction (unchanged shape) -------------------------

def pstem(p, n):
    return f"proxycmp_{p}_scale" if n >= 128 else f"proxycmp_{p}"


def nstem(p, n):
    return f"proxycmp_{p}_nostream_scale" if n >= 128 else f"proxycmp_{p}_nostream"


OAT = [("oat_8b_baseline", "baseline"), ("oat_8b_poisson", "poisson"),
       ("oat_8b_2kx2k", "2k×2k"), ("oat_8b_4kx4k", "4k×4k"), ("oat_8b_code", "code"),
       ("oat_8b_chat", "chat"), ("oat_8b_summary", "summary"), ("oat_8b_burstgpt", "burstgpt")]


def cell(stem, n, refresh=False, keep_arrays=False):
    try:
        return P.extract_cell(stem, n, keep_arrays=keep_arrays, refresh=refresh)
    except Exception as e:
        print(f"  ! extract {stem} n{n}: {e}")
        return None


def extract_run(stem, n, good):
    """Cell from a SPECIFIC set of data run_index values — drops node-failure
    runs so a point reflects its healthy run(s). (Aggregate only; per-run error
    bars fall back to std 0 for these.)"""
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
    ttft = []; tbt = []; lat = []; dec = []
    nreq = nsucc = nmeet = nttft = ntbt = 0
    with open(src, "rb") as fh:
        for r in ijson.items(fh, "requests.item"):
            if int(r.get("run_index", 0)) not in good:
                continue
            nreq += 1; ok = bool(r.get("success", True))
            t = r.get("ttft_s"); b = r.get("tbt_p99_s"); l = r.get("latency")
            l = float(l) if l is not None else float("nan")
            if t is None and b is None and not np.isnan(l):
                comp = float(r.get("actual_completion_tokens") or r.get("output_len") or 0)
                t = l; b = (l / comp) if comp > 0 else float("nan")
            else:
                t = float(t) if t is not None else float("nan")
                b = float(b) if b is not None else float("nan")
            if ok:
                nsucc += 1; ttft.append(t); tbt.append(b); lat.append(l); dec.append(l - t)
            tok = ok and not np.isnan(t) and t <= P.TTFT_SLO_S
            bok = ok and not np.isnan(b) and b <= P.TBT_P99_SLO_S
            if tok: nttft += 1
            if bok: ntbt += 1
            if tok and bok: nmeet += 1
    arr = lambda x: np.asarray(x, dtype=np.float32)
    _p = lambda a, q: float(np.nanpercentile(a, q)) if a.size else float("nan")
    ta, ba, la, da = arr(ttft), arr(tbt), arr(lat), arr(dec)
    att = nmeet / nreq if nreq else float("nan")
    return CellStats(spec=stem, node=n, src=str(src), n_req=nreq, n_success=nsucc, rps=rps,
                     attainment=att, goodput=rps * att,
                     ttft_attainment=(nttft / nreq if nreq else float("nan")),
                     tbt_attainment=(ntbt / nreq if nreq else float("nan")),
                     success_rate=(nsucc / nreq if nreq else float("nan")),
                     ttft_p50=_p(ta, 50), ttft_p99=_p(ta, 99), tbt_p50=_p(ba, 50), tbt_p99=_p(ba, 99),
                     e2e_p50=_p(la, 50), e2e_p99=_p(la, 99), decode_p50=_p(da, 50), decode_p99=_p(da, 99),
                     ttft_attain_1s=(nttft / nreq if nreq else float("nan")))


def build(refresh=False):
    S, NS = {}, {}
    for p in PROXIES:
        for n in ALLNODES:
            S[(p, n)] = cell(pstem(p, n), n, refresh, keep_arrays=(n in CDF_NODES))
            NS[(p, n)] = cell(nstem(p, n), n, refresh)
    # envoy 256n streaming collapses (0% success); keep the existing handling.
    ej = extract_run("proxycmp_envoy_scale", 256, {1})
    if ej and not np.isnan(ej.rps):
        S[("envoy", 256)] = ej
    O, ONS = {}, {}
    for stem, _ in OAT + [("oat_120b", "120b")]:
        for n in (1, 64):
            O[(stem, n)] = cell(stem, n, refresh)
            ONS[(stem, n)] = cell(f"{stem}_nostream", n, refresh)
    return S, NS, O, ONS


# --- shared axis setup ------------------------------------------------------

def _node_axis(ax):
    ax.set_xscale("log", base=2); ax.set_xticks(ALLNODES); ax.set_xticklabels(ALLNODES)


# --- figures ----------------------------------------------------------------

def fig1_proxy_scaling(S, NS):
    """Headline: per-proxy scaling vs cluster size — successful throughput
    (stream + non-stream), request success rate, TBT attainment. Stream solid /
    non-stream dashed; error bars = ±std over runs. (TTFT attainment is its own
    set of figures, fig1_ttft_{1,2,3}s.)"""
    from matplotlib.lines import Line2D
    fig, ax = plt.subplots(3, 1, figsize=(10.5, 16))
    a_tp, a_sr, a_tbt = ax
    stackers = []

    # 1) Successful throughput — both modes + ideal.
    stk = ps.AnnotationStacker(a_tp, "{:.0f}", 6.5); stackers.append(stk)
    for p in PROXIES:
        for mode, src in (("stream", S), ("nonstream", NS)):
            xs, ys, es = _collect(src, p, "succ_rps")
            if not xs:
                continue
            ps.line(a_tp, xs, ys, p, mode, yerr=es)
            stk.add_series(xs, ys, COLORS[p], dashed=ps.is_dashed(mode))
    ideal_h = None
    base = S.get(("direct", 1))
    if base and not np.isnan(base.rps):
        b = base.rps * base.success_rate
        ideal_h, = a_tp.plot(ALLNODES, [b * n for n in ALLNODES], "k--", lw=2, alpha=0.55, zorder=2)
    a_tp.set_yscale("log"); ps.plain_log_y(a_tp)
    a_tp.set_ylabel("Successful throughput (req/s)")
    a_tp.set_title("Successful throughput  (streaming + non-stream)")
    # Pairwise 2-column legend: column-major fill puts streams+ideal in the left
    # column, non-streams in the right → each row is one proxy (stream | non-stream),
    # with the orphaned "ideal" on the last left-column row.
    def _h(p, mode):
        kw = ps.proxy_kw(p, mode)
        return Line2D([0], [0], lw=ps.LW, markersize=ps.MS, markerfacecolor=kw["color"],
                      markeredgecolor="white", markeredgewidth=1.5, **kw)
    handles = [_h(p, "stream") for p in PROXIES] + [ideal_h] + [_h(p, "nonstream") for p in PROXIES]
    labels = ([ps.plabel(p, "stream") for p in PROXIES] + ["ideal linear (Direct n1×N)"]
              + [ps.plabel(p, "nonstream") for p in PROXIES])
    a_tp.legend(handles, labels, ncol=2, loc="upper left", fontsize=7.5,
                columnspacing=1.2, handlelength=2.2)

    # 2) Request success rate (streaming) — legend outside, right.
    stk = ps.AnnotationStacker(a_sr, "{:.0f}", 7); stackers.append(stk)
    for p in PROXIES:
        xs, ys, es = _collect(S, p, "succ_rate", scale=100.0)
        if not xs:
            continue
        ps.line(a_sr, xs, ys, p, "stream", label=ps.plabel(p), yerr=es)
        stk.add_series(xs, ys, COLORS[p])
    a_sr.set_ylim(-5, 115); a_sr.set_ylabel("Success rate (%)")
    a_sr.set_title("Request success rate (streaming)")
    a_sr.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), fontsize=8, borderaxespad=0.0)

    # 3) TBT attainment — both modes; legend outside, right.
    stk = ps.AnnotationStacker(a_tbt, "{:.2f}", 7); stackers.append(stk)
    for p in PROXIES:
        for mode, src in (("stream", S), ("nonstream", NS)):
            xs, ys, es = _collect(src, p, "tbt")
            if not xs:
                continue
            ps.line(a_tbt, xs, ys, p, mode, label=ps.plabel(p, mode), yerr=es)
            stk.add_series(xs, ys, COLORS[p], dashed=ps.is_dashed(mode))
    a_tbt.set_ylim(-0.05, 1.18); a_tbt.set_ylabel("TBT attainment")
    a_tbt.set_title("TBT attainment — fraction with P99 TBT ≤ 250ms")
    a_tbt.set_xlabel("Cluster size (nodes = replicas)")
    a_tbt.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), fontsize=7.5, borderaxespad=0.0)

    for a in ax:
        _node_axis(a)
    top = ps.titles(fig, "Proxy scaling: throughput, success rate, and TBT attainment vs. cluster size",
                    ["Streaming (solid) vs non-stream (dashed) · error bars = ±std over runs · non-stream TBT = coarse L÷tokens"]
                    + CFG_8B
                    + ["envoy 256n streaming = 0% (deploy collapse) · num_runs=6 for n≤64, 3 for n∈{128,256}"])
    return ps.finalize(fig, stackers, OUT / "fig1_proxy_scaling.png", rect=(0, 0, 1, top))


def fig1_ttft(S, NS):
    """TTFT attainment vs cluster size, one standalone figure per first-token
    budget (1s / 2s / 3s) — pulled out of fig1 because three thresholds in one
    panel were too noisy. Stream solid / non-stream dashed (coarse TTFT=L)."""
    outs = []
    for kind, sec in (("ttft1", 1), ("ttft2", 2), ("ttft3", 3)):
        fig, axx = plt.subplots(figsize=(10, 6))
        stk = ps.AnnotationStacker(axx, "{:.2f}", 7)
        for p in PROXIES:
            for mode, src in (("stream", S), ("nonstream", NS)):
                xs, ys, es = _collect(src, p, kind)
                if not xs:
                    continue
                ps.line(axx, xs, ys, p, mode, label=ps.plabel(p, mode), yerr=es)
                stk.add_series(xs, ys, COLORS[p], dashed=ps.is_dashed(mode))
        _node_axis(axx); axx.set_ylim(-0.05, 1.18)
        axx.set_ylabel(f"TTFT attainment (≤ {sec}s)")
        axx.set_xlabel("Cluster size (nodes = replicas)")
        axx.set_title(f"TTFT attainment — fraction of requests with TTFT ≤ {sec}s")
        axx.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), fontsize=8, borderaxespad=0.0)
        top = ps.titles(fig, f"TTFT attainment at a {sec}s first-token budget vs. cluster size",
                        ["Streaming (solid) vs non-stream (dashed) · error bars = ±std over runs",
                         "non-stream TTFT is a coarse estimate (= full E2E latency L; no per-token timing captured)"]
                        + CFG_8B)
        outs.append(ps.finalize(fig, [stk], OUT / f"fig1_ttft_{sec}s.png", rect=(0, 0, 1, top)))
    return outs


def fig2_two_mode(S, NS):
    """Streaming vs non-stream end-to-end latency (p50 and p99) vs N, for the
    proxies that survive (direct / haproxy / envoy). Throughput now lives in
    fig1; this figure is the latency comparison."""
    fig, ax = plt.subplots(2, 1, figsize=(10.5, 11))
    stackers = []
    for j, (attr, lab) in enumerate((("e2e_p50", "E2E latency p50 (s)"),
                                     ("e2e_p99", "E2E latency p99 (s)"))):
        stk = ps.AnnotationStacker(ax[j], "{:.1f}", 7); stackers.append(stk)
        for p in ["direct", "haproxy", "envoy"]:
            for mode, src in (("stream", S), ("nonstream", NS)):
                xs, ys = [], []
                for n in ALLNODES:
                    st = src.get((p, n))
                    if not st or np.isnan(st.rps):
                        continue
                    v = getattr(st, attr)
                    if np.isnan(v):
                        continue
                    xs.append(n); ys.append(v)
                if not xs:
                    continue
                ps.line(ax[j], xs, ys, p, mode, label=ps.plabel(p, mode))
                stk.add_series(xs, ys, COLORS[p], dashed=ps.is_dashed(mode))
        _node_axis(ax[j]); ax[j].set_yscale("log"); ps.plain_log_y(ax[j])
        ax[j].set_ylabel(lab); ps.legend(ax[j], ncol=2, fontsize=8)
    ax[0].set_title("End-to-end latency p50")
    ax[1].set_title("End-to-end latency p99 (tail)")
    ax[1].set_xlabel("Cluster size (nodes = replicas)")
    top = ps.titles(fig, "Streaming vs. non-streaming end-to-end latency",
                    ["Direct / HAProxy / Envoy · solid = streaming, dashed = non-stream"] + CFG_8B)
    return ps.finalize(fig, stackers, OUT / "fig2_two_mode.png", rect=(0, 0, 1, top))


def fig3_workload(O, ONS):
    """OAT 8B workloads + 120B at n1 vs n64: SLO attainment & E2E p99."""
    cells = OAT + [("oat_120b", "120b")]
    labels = [l for _, l in cells]
    x = np.arange(len(cells)); w = 0.38
    bar_colors = {1: "#9ecae1", 64: "#08519c"}; bar_hatch = {1: "", 64: "//"}
    fig, ax = plt.subplots(2, 1, figsize=(15, 10))
    for i, n in enumerate((1, 64)):
        att = [(O.get((s, n)).attainment if O.get((s, n)) else np.nan) for s, _ in cells]
        e2e = [(O.get((s, n)).e2e_p99 if O.get((s, n)) else np.nan) for s, _ in cells]
        off = (i - .5) * w
        for a, vals, fmt, dy in ((ax[0], att, "{:.2f}", 0.02), (ax[1], e2e, "{:.1f}", 0)):
            bars = a.bar(x + off, vals, w, label=f"N={n}", color=bar_colors[n],
                         hatch=bar_hatch[n], edgecolor="white", linewidth=1.0, zorder=3)
            for b, v in zip(bars, vals):
                if not np.isnan(v):
                    a.text(b.get_x() + b.get_width() / 2, v + dy, fmt.format(v), ha="center",
                           va="bottom", fontsize=7.5, fontweight="bold", color=bar_colors[n])
    for a in ax:
        a.set_xticks(x); a.set_xticklabels(labels, rotation=30, ha="right")
        a.grid(False, axis="x"); ps.legend(a)
    ax[0].set_ylim(0, 1.15); ax[0].set_ylabel("SLO attainment")
    ax[0].set_title("SLO attainment  (TTFT ≤ 1s  ∧  P99 TBT ≤ 250ms)")
    ax[1].set_ylabel("E2E p99 (s)"); ax[1].set_title("End-to-end latency (p99)")
    top = ps.titles(fig, "Workload robustness at single-node vs. 64-node scale",
                    ["8B workload sweep + 120B · streaming (SSE) · N=1 vs N=64", "ALCF Aurora"])
    fig.tight_layout(rect=(0, 0, 1, top)); out = OUT / "fig3_workload.png"
    fig.savefig(out); plt.close(fig); return out


def fig4_latency(S):
    """TTFT/TBT/E2E p99 per proxy at n64."""
    metrics = [("TTFT p99", "ttft_p99", P.TTFT_SLO_S), ("TBT p99", "tbt_p99", P.TBT_P99_SLO_S),
               ("E2E p99", "e2e_p99", None)]
    fig, ax = plt.subplots(1, 3, figsize=(16, 6))
    for j, (title, attr, slo) in enumerate(metrics):
        vals, names, cols, hats = [], [], [], []
        for p in PROXIES:
            st = S.get((p, 64))
            if not st:
                continue
            v = getattr(st, attr)
            vals.append(v if not np.isnan(v) else 0); names.append(ps.PROXY_LABEL[p])
            cols.append(COLORS[p]); hats.append(HATCHES[p])
        bars = ax[j].bar(names, vals, color=cols, edgecolor="white", linewidth=1.2, zorder=3)
        for b, h in zip(bars, hats):
            b.set_hatch(h)
        for b, v in zip(bars, vals):
            ax[j].text(b.get_x() + b.get_width() / 2, v, f"{v:.2f}", ha="center",
                       va="bottom", fontsize=8.5, fontweight="bold", color="#333333")
        if slo:
            ax[j].axhline(slo, ls="--", color="#E94F37", lw=2, label=f"SLO {slo}s")
        ax[j].grid(False, axis="x"); ax[j].set_title(title); ax[j].set_ylabel("seconds")
        ax[j].tick_params(axis="x", rotation=30)
        if slo:
            ps.legend(ax[j])
    top = ps.titles(fig, "Per-proxy latency tails at 64 nodes (TTFT / TBT / E2E, p99)",
                    ["Streaming (SSE) · N=64"] + CFG_8B)
    fig.tight_layout(rect=(0, 0, 1, top)); out = OUT / "fig4_latency_n64.png"
    fig.savefig(out); plt.close(fig); return out


def fig5_latency_cdf(S):
    """TTFT & P99-TBT CDFs across proxies, per node count (n4/n64/n128). FAILED
    requests are included as the denominator, so each curve caps at the cell's
    success rate (it never reaches 1.0) — the honest picture for saturating
    proxies (litellm/rayserve)."""
    fig, axes = plt.subplots(len(CDF_NODES), 2, figsize=(15, 5.2 * len(CDF_NODES)))
    for row, n in enumerate(CDF_NODES):
        axt, axb = axes[row]
        for p in PROXIES:
            st = S.get((p, n))
            if st is None or not st.n_req:
                continue
            try:
                ttft, tbt = P.load_arrays(pstem(p, n), n)
            except FileNotFoundError:
                continue
            c = COLORS[p]; mk = MARKERS[p]
            lbl = f"{ps.PROXY_LABEL[p]} (ok={st.success_rate:.0%})"
            for ax, arr in ((axt, ttft), (axb, tbt)):
                a = arr[np.isfinite(arr)]
                if a.size == 0:
                    continue
                # Censored CDF: denominator = ALL run≥1 requests (incl. failures),
                # so the curve tops out at (finite values / total) ≈ success rate.
                cap = a.size / st.n_req
                q = np.linspace(0.0, 100.0, 2000)
                ax.plot(np.percentile(a, q), (q / 100.0) * cap, color=c, lw=ps.LW, alpha=0.9,
                        marker=mk, markevery=200, markersize=8, markerfacecolor=c,
                        markeredgecolor="white", markeredgewidth=1.2, label=lbl, zorder=3)
        axt.axvline(P.TTFT_SLO_S, color="k", ls="--", lw=1.5, alpha=0.7)
        axt.text(P.TTFT_SLO_S, 0.04, " 1s SLO", fontsize=8, color="#333333")
        axb.axvline(P.TBT_P99_SLO_S, color="k", ls="--", lw=1.5, alpha=0.7)
        axb.text(P.TBT_P99_SLO_S, 0.04, " 250ms SLO", fontsize=8, color="#333333")
        axt.set_title(f"TTFT — {n} nodes"); axt.set_xlabel("TTFT (s)"); axt.set_ylabel("CDF (of all requests)")
        axb.set_title(f"per-request P99 TBT — {n} nodes")
        axb.set_xlabel("P99 time-between-tokens (s)"); axb.set_ylabel("CDF (of all requests)")
        for ax in (axt, axb):
            ax.set_xscale("log"); ax.set_ylim(0, 1.02); ps.legend(ax, fontsize=8)
    top = ps.titles(fig, "TTFT & per-request P99-TBT distributions across proxies and scale",
                    ["Streaming (SSE) · nodes ∈ {4, 64, 128} · failures included → curve caps at success rate",
                     "dashed = SLO (TTFT 1s / TBT 250ms)  ·  " + CFG_8B[0], CFG_8B[1]])
    fig.tight_layout(rect=(0, 0, 1, top)); out = OUT / "fig5_latency_cdf.png"
    fig.savefig(out); plt.close(fig); return out


def main():
    refresh = "--refresh" in sys.argv
    print("building cells (full sweep)...")
    S, NS, O, ONS = build(refresh=refresh)
    got = sum(1 for v in {**S, **NS, **O, **ONS}.values() if v)
    print(f"  extracted {got} cells")
    for fn, args in [(fig1_proxy_scaling, (S, NS)), (fig1_ttft, (S, NS)),
                     (fig2_two_mode, (S, NS)), (fig3_workload, (O, ONS)),
                     (fig4_latency, (S,)), (fig5_latency_cdf, (S,))]:
        out = fn(*args)
        for o in (out if isinstance(out, list) else [out]):
            print(f"  wrote {o}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
