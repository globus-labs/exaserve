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
OUT = Path(__file__).resolve().parent / "output" / "sc26_full" / "iter10"
OUT.mkdir(parents=True, exist_ok=True)

# Exact IEEEtran widths (measured: \columnwidth=252pt, \textwidth=516pt) so
# \includegraphics[width=\columnwidth]{...} does NO scaling and matplotlib point
# sizes equal on-paper points (duetosymmetry.com/code/latex-mpl-fig-tips).
PT = 1.0 / 72.27
COL_W = 252.0 * PT      # IEEEtran \columnwidth  (3.49 in)
TEXT_W = 516.0 * PT     # IEEEtran \textwidth     (7.14 in)

plt = ps.apply()                     # load the project matplotlibrc theme once

PROXIES = ["direct", "haproxy", "envoy", "rayserve", "litellm"]
ALLNODES = [1, 4, 16, 64, 128, 256]  # proxycmp_<p>[_scale]
CDF_NODES = [4, 64, 128]             # node counts that keep per-request arrays
COLORS = ps.PROXY_COLORS
MARKERS = ps.PROXY_MARKERS
HATCHES = {"direct": "", "haproxy": "//", "envoy": "\\\\", "rayserve": "xx", "litellm": ".."}
TTFT_THRS = P.TTFT_SLO_MULTI         # (1.0, 2.0, 3.0)

# Multi-line config subtitle (shared by the Set-1 proxy figures).
CFG_8B = ["Meta-Llama-3-8B-Instruct  ·  TP=1 (1 replica/node)  ·  64$\\rightarrow$64 tok",
          "110 req/s/node offered  ·  ALCF Aurora"]


# --- tokens/s secondary axis (a UNIT CONVERSION of query/s) ------------------
# Measured from usage (actual_prompt_tokens / actual_completion_tokens) across the
# sweep for the 64->64 workload: 74.7 input tok/req (64 content tokens + the
# chat-template overhead) + 64.0 output tok/req (generation always hits the
# max_tokens=64 cap) = 138.7 total tok/req. tokens/s = query/s x TOK_TOTAL is a
# DETERMINISTIC unit conversion, not an independent series -> a matplotlib
# functional secondary axis (NOT twinx). The fixed factor means the two scales
# cannot invent a correlation, so the "dual-axis" anti-pattern (which is about
# independent measures with arbitrary alignment) does not apply.
TOK_IN, TOK_OUT = 74.7, 64.0
TOK_TOTAL = TOK_IN + TOK_OUT             # 138.7 total tok/req


def _tok_secondary_axis(a, mult=TOK_TOTAL, label="Total throughput (tokens/s)"):
    """Right-hand axis expressing the SAME curves in total tokens/s. Ticks at token
    decades in 10^n (matching sparse_log_y), styled muted/recessive so it reads as
    a derived reference scale rather than a second data series. label=None draws
    ticks only (used on the interior panel to avoid crowding the column gap)."""
    from matplotlib.ticker import (LogLocator, LogFormatterMathtext, NullFormatter)
    sec = a.secondary_yaxis("right", functions=(lambda q: q * mult, lambda t: t / mult))
    sec.yaxis.set_major_locator(LogLocator(base=10.0))
    sec.yaxis.set_minor_locator(LogLocator(base=10.0, subs=tuple(np.arange(2, 10)), numticks=12))
    sec.yaxis.set_major_formatter(LogFormatterMathtext(base=10.0))
    sec.yaxis.set_minor_formatter(NullFormatter())
    if label:
        sec.set_ylabel(label, color="#555555")
    sec.tick_params(axis="y", which="both", colors="#888888")
    for lbl in sec.get_yticklabels():
        lbl.set_color("#555555")
    return sec


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

def _place(ax, x, v, proxy, mode, fmt, pos, dy=8):
    """One value label with EXPLICIT vertical placement ('up' above / 'down' below
    the data point), color-matched to the proxy (dashed box for non-stream)."""
    _place_txt(ax, x, v, fmt.format(v), COLORS[proxy], pos, dy=dy,
               dashed=(mode == "nonstream"))


def _place_txt(ax, x, y, text, color, pos="up", dy=8, dashed=False):
    """Generic boxed value label (color-matched, dashed border optional) with
    explicit up/down placement — the proxy-agnostic core of _place, also used for
    multi-line throughput+efficiency callouts on the weak-scaling figures."""
    off = dy if pos == "up" else -dy
    ax.annotate(text, (x, y), textcoords="offset points", xytext=(0, off),
                ha="center", va="bottom" if pos == "up" else "top",
                fontsize=5, fontweight="bold", color=color, zorder=6,
                bbox=dict(boxstyle="round,pad=0.15", fc="white", ec=color,
                          lw=0.5, ls="--" if dashed else "-", alpha=0.92))


def _write_caption(out_png, title, subtitle):
    """Figures no longer carry a main title/subtitle — those move to the paper
    caption. Persist the description as a sidecar '<stem>.txt' (same name as the
    figure) so it can be lifted into the paper once the plot is approved."""
    subs = [subtitle] if isinstance(subtitle, str) else list(subtitle)
    body = "\n".join([title.replace("\n", " ")] + [str(s) for s in subs])
    Path(out_png).with_suffix(".txt").write_text(body + "\n")


def fig1_proxy_scaling(S, NS, wide=True):
    """Headline 2x2 vs cluster size: successful throughput (streaming |
    non-streaming), TBT attainment, and TTFT attainment (2s budget). Stream solid /
    non-stream dashed; error bars = ±std over runs. TTFT is now merged in as the
    4th panel (was standalone fig1_ttft_2s).

    wide=True  -> full-width figure* aspect (TEXT_W x 3.3), fonts sized for
                  \\textwidth; file fig1_proxy_scaling.pdf.
    wide=False -> native single-column aspect (COL_W x 4.4), fonts sized for
                  \\columnwidth (no LaTeX down-scaling); file
                  fig1_proxy_scaling_col.pdf. Provided so the paper can carry
                  both and pick the layout that fits."""
    from matplotlib.lines import Line2D
    if wide:
        fig, axg = plt.subplots(2, 2, figsize=(TEXT_W, 3.3))
        out_name, leg_ncol, leg_frac = "fig1_proxy_scaling.png", 5, 0.34
    else:
        fig, axg = plt.subplots(2, 2, figsize=(COL_W, 4.4))
        out_name, leg_ncol, leg_frac = "fig1_proxy_scaling_col.png", 3, 0.66
    (a_tps, a_tpn), (a_tbt, a_ttft) = axg   # row0: throughput stream/non; row1: TBT / TTFT

    base = S.get(("direct", 1))
    b = base.rps * base.success_rate if (base and not np.isnan(base.rps)) else None

    def _throughput_panel(a, src, mode, title):
        top = None                                   # (n, succ_rps) of the peak curve at the largest N
        for p in PROXIES:
            xs, ys, es = _collect(src, p, "succ_rps")
            if xs:
                ps.line(a, xs, ys, p, mode, yerr=es)
                if xs[-1] == ALLNODES[-1] and (top is None or ys[-1] > top[1]):
                    top = (xs[-1], ys[-1])
        if b is not None:
            a.plot(ALLNODES, [b * n for n in ALLNODES], "k--", lw=1.0, alpha=0.55, zorder=2)
        a.set_yscale("log"); ps.sparse_log_y(a, sci=True)   # 10^n superscript decades
        a.set_ylabel("Throughput (query/s)")
        a.set_title(title)
        _tok_secondary_axis(a)                       # right axis: SAME curves in total tokens/s
        # Headline callout: peak total tokens/s at the largest cluster -- the number
        # that maps to token-throughput benchmarks (MLPerf et al.). Pinned to the
        # top-left corner (empty in both panels; the curves rise from bottom-left),
        # so it reads as a panel headline and never collides with the endpoint value
        # labels or the centred title.
        if top is not None:
            n_top, rps_top = top
            a.annotate(f"$\\approx${rps_top * TOK_TOTAL / 1e6:.1f}M tok/s @ {n_top}n",
                       (0.035, 0.94), xycoords="axes fraction",
                       ha="left", va="top", fontsize=6, fontweight="bold",
                       color="#1a1a1a", zorder=7,
                       bbox=dict(boxstyle="round,pad=0.25", fc="white", ec="#1a1a1a",
                                 lw=0.7, alpha=0.95))

    # row 0) Successful throughput — STREAMING | NON-STREAM.
    _throughput_panel(a_tps, S,  "stream",    "Successful throughput — streaming")
    _throughput_panel(a_tpn, NS, "nonstream", "Successful throughput — non-streaming")

    # row 1, left) TBT attainment — both modes.
    for p in PROXIES:
        for mode, src in (("stream", S), ("nonstream", NS)):
            xs, ys, es = _collect(src, p, "tbt")
            if xs:
                ps.line(a_tbt, xs, ys, p, mode, yerr=es)
    a_tbt.set_ylim(-0.05, 1.18); a_tbt.set_ylabel("TBT attainment")
    a_tbt.set_title("TBT attainment — P99 TBT ≤ 250ms")

    # row 1, right) TTFT attainment (2s budget) — both modes (merged from fig1_ttft).
    for p in PROXIES:
        for mode, src in (("stream", S), ("nonstream", NS)):
            xs, ys, es = _collect(src, p, "ttft2")
            if xs:
                ps.line(a_ttft, xs, ys, p, mode, yerr=es)
    a_ttft.set_ylim(-0.05, 1.18); a_ttft.set_ylabel("TTFT attainment (≤ 2s)")
    a_ttft.set_title("TTFT attainment — TTFT ≤ 2s")

    for a in (a_tps, a_tpn, a_tbt, a_ttft):
        _node_axis(a)
    a_tbt.set_xlabel("Cluster size (nodes = replicas)")
    a_ttft.set_xlabel("Cluster size (nodes = replicas)")

    # Shared legend ABOVE the panels: full names, streaming (row 1) then
    # non-streaming (row 2), aligned by proxy (column-major fill).
    def _h(p, mode):
        kw = ps.proxy_kw(p, mode)
        return Line2D([0], [0], lw=ps.LW, markersize=ps.MS, markerfacecolor=kw["color"],
                      markeredgecolor="white", markeredgewidth=1.0, **kw)
    handles, labels = [], []
    for p in PROXIES:
        handles += [_h(p, "stream"), _h(p, "nonstream")]
        labels += [f"{ps.PROXY_LABEL[p]} (stream)", f"{ps.PROXY_LABEL[p]} (non-stream)"]
    _write_caption(OUT / out_name,
                   "Proxy scaling: throughput, TBT & TTFT attainment vs. cluster size",
                   ["Streaming (solid) vs non-stream (dashed)",
                    "Right axis: total tokens/s = query/s $\\times$ 138.7 "
                    "(74.7 in + 64.0 out tok/req; a unit conversion, not independent data)"]
                   + CFG_8B)
    top = 0.99                                       # no main title; legend rides the top edge
    legh = leg_frac / fig.get_size_inches()[1]       # reserve the legend rows (more at column width)
    panel_top = top - legh
    fig.legend(handles, labels, loc="lower center", bbox_to_anchor=(0.5, panel_top + 0.004),
               ncol=leg_ncol, fontsize=6, frameon=True, columnspacing=0.9, handlelength=1.5, handletextpad=0.4)

    # --- hand-picked value labels (enabled=True bypasses the global-off default) ---
    def _val(src, p, n, kind):
        st = src.get((p, n))
        if not st or np.isnan(st.rps):
            return None
        v, _ = _pt(st, kind)
        return None if (v is None or np.isnan(v)) else v

    stk_tbt = ps.AnnotationStacker(a_tbt, "{:.2f}", enabled=True)
    stk_ttft = ps.AnnotationStacker(a_ttft, "{:.2f}", enabled=True)
    # Throughput panels: base labels = max (up), min (down), litellm (up) per x;
    # plus explicit ADDitions and placement OVeRrides (hand-tuned per request).
    THR_ADD = {"stream": {(64, "envoy"): "down", (128, "envoy"): "up", (128, "haproxy"): "down",
                          (256, "envoy"): "down", (256, "haproxy"): "up"},
               "nonstream": {}}
    THR_OVR = {"stream": {},
               "nonstream": {(256, "rayserve"): "up", (256, "haproxy"): "down"}}
    # Hug the line (default offset 8pt): litellm's up-label was floating up into
    # the envoy/haproxy numbers; rayserve's down-label was dropping onto the x-ticks.
    THR_DY = {"litellm": 4, "rayserve": 4}
    for a, src, mode in ((a_tps, S, "stream"), (a_tpn, NS, "nonstream")):
        for n in ALLNODES:
            vals = {p: _val(src, p, n, "succ_rps") for p in PROXIES}
            vals = {p: v for p, v in vals.items() if v is not None}
            if not vals:
                continue
            roles = {}                               # proxy -> "up"/"down"
            if "litellm" in vals:
                roles["litellm"] = "up"
            roles[min(vals, key=vals.get)] = "down"  # min
            roles[max(vals, key=vals.get)] = "up"    # max (wins over litellm)
            for (nn, p), pos in THR_ADD[mode].items():
                if nn == n and p in vals:
                    roles[p] = pos
            for (nn, p), pos in THR_OVR[mode].items():
                if nn == n and p in roles:
                    roles[p] = pos
            for p, pos in roles.items():
                _place(a, n, vals[p], p, mode, "{:.0f}", pos, dy=THR_DY.get(p, 8))
    # TBT attainment: hand-picked (node, proxy, mode) points
    TBT_ANN = [(1, "litellm", "stream"), (1, "haproxy", "stream"),
               (4, "rayserve", "stream"), (4, "rayserve", "nonstream"), (4, "haproxy", "stream"),
               (16, "litellm", "nonstream"), (16, "rayserve", "stream"), (16, "haproxy", "stream"),
               (64, "haproxy", "stream"), (64, "haproxy", "nonstream"), (64, "litellm", "nonstream"), (64, "direct", "stream"),
               (128, "haproxy", "nonstream"), (128, "envoy", "stream"), (128, "direct", "stream"),
               (256, "haproxy", "nonstream"), (256, "direct", "stream"), (256, "envoy", "stream")]
    for n, p, mode in TBT_ANN:
        v = _val(S if mode == "stream" else NS, p, n, "tbt")
        if v is not None:
            stk_tbt.add(n, v, COLORS[p], dashed=(mode == "nonstream"))
    # TTFT attainment: top envelope (max across all series, near 1.0) per node,
    # plus the direct-stream value — but skip any label that would sit on the
    # x-axis (the near-zero streaming points overlap the tick row).
    TTFT_FLOOR = 0.06
    for n in ALLNODES:
        pts = [(v, p, mode) for p in PROXIES
               for mode, src in (("stream", S), ("nonstream", NS))
               if (v := _val(src, p, n, "ttft2")) is not None]
        if not pts:
            continue
        vmax, pmax, mmax = max(pts)
        stk_ttft.add(n, vmax, COLORS[pmax], dashed=(mmax == "nonstream"))
        vd = _val(S, "direct", n, "ttft2")
        if vd is not None and vd >= TTFT_FLOOR:      # direct-stream, only when it clears the axis
            stk_ttft.add(n, vd, COLORS["direct"], dashed=False)
    vll1 = _val(S, "litellm", 1, "ttft2")            # LiteLLM n1 stream sits well above the axis → keep
    if vll1 is not None:
        stk_ttft.add(1, vll1, COLORS["litellm"], dashed=False)
    return ps.finalize(fig, [stk_tbt, stk_ttft], OUT / out_name,
                       rect=(0, 0, 1, panel_top))


def fig1_ttft(S, NS):
    """TTFT attainment vs cluster size, one standalone figure per first-token
    budget (1s / 2s / 3s) — pulled out of fig1 because three thresholds in one
    panel were too noisy. Stream solid / non-stream dashed (coarse TTFT=L)."""
    outs = []
    for kind, sec in (("ttft2", 2),):        # 2s budget only (matches the 2s reference SLO)
        fig, axx = plt.subplots(figsize=(COL_W, 1.68))
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
        axx.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), fontsize=6, borderaxespad=0.0)
        top = ps.titles(fig, f"TTFT attainment at a {sec}s first-token budget vs. cluster size",
                        ["Streaming (solid) vs non-stream (dashed) · error bars = $\\pm$std over runs",
                         "non-stream TTFT is a coarse estimate (= full E2E latency L; no per-token timing captured)"]
                        + CFG_8B)
        outs.append(ps.finalize(fig, [stk], OUT / f"fig1_ttft_{sec}s.png", rect=(0, 0, 1, top)))
    return outs


def fig2_two_mode(S, NS):
    """Streaming vs non-stream end-to-end latency (p50 and p99) vs N, for the
    proxies that survive (direct / haproxy / envoy). Throughput now lives in
    fig1; this figure is the latency comparison."""
    fig, ax = plt.subplots(2, 1, figsize=(COL_W, 3.36))
    F2P = ["direct", "haproxy", "envoy"]
    # hand-picked callouts (node, proxy, mode) added on top of per-x max/min, per panel:
    ADDS = [[(1, "direct", "stream"), (256, "envoy", "stream"),          # p50
             (256, "haproxy", "stream"), (256, "direct", "stream")],
            [(n, p, "stream") for n in (64, 128, 256) for p in ("direct", "haproxy")]  # p99
             + [(n, "envoy", "nonstream") for n in (64, 128)]
             + [(256, "haproxy", "nonstream")]]
    # explicit additions / placement overrides (node, proxy, mode) -> pos, per panel
    F2_ADD = [{(4, "direct", "stream"): "down", (16, "direct", "stream"): "down",
               (64, "direct", "stream"): "down", (128, "envoy", "stream"): "down",
               (128, "haproxy", "stream"): "up"},                                      # p50
              {(1, "haproxy", "stream"): "down", (4, "haproxy", "stream"): "down",
               (16, "haproxy", "stream"): "down", (64, "haproxy", "stream"): "down",
               (128, "haproxy", "stream"): "down", (256, "haproxy", "stream"): "down",
               (1, "direct", "nonstream"): "up"}]                                      # p99
    for j, (attr, lab) in enumerate((("e2e_p50", "E2E p50 (s)"),
                                     ("e2e_p99", "E2E p99 (s)"))):
        vals = {}   # (proxy, mode, node) -> value, for max/min + callouts
        for p in F2P:
            for mode, src in (("stream", S), ("nonstream", NS)):
                xs, ys = [], []
                for n in ALLNODES:
                    st = src.get((p, n))
                    if not st or np.isnan(st.rps):
                        continue
                    v = getattr(st, attr)
                    if np.isnan(v):
                        continue
                    xs.append(n); ys.append(v); vals[(p, mode, n)] = v
                if xs:
                    ps.line(ax[j], xs, ys, p, mode, label=ps.plabel(p, mode))
        _node_axis(ax[j]); ax[j].set_yscale("log")
        ps.sparse_log_y(ax[j]) if j == 1 else ps.plain_log_y(ax[j])   # p99 spans 2 decades → sparse
        ax[j].set_ylabel(lab)          # no per-panel legend (shared one, below)
        # explicit placement: per-x max (up) & min (down), callouts (up), then
        # hand-tuned additions/overrides; p99 forces every 256-node label above.
        pos_of = {}
        for n in ALLNODES:
            here = [(v, p, mode) for (p, mode, nn), v in vals.items() if nn == n]
            if not here:
                continue
            pos_of[max(here)[1:] + (n,)] = "up"
            pos_of[min(here)[1:] + (n,)] = "up"    # mins sit at the plot floor → above the point
        for (n, p, mode) in ADDS[j]:
            pos_of.setdefault((p, mode, n), "up")
        if j == 1:                                    # p99: cluster all 256n labels above ...
            for key in list(pos_of):
                if key[2] == 256:
                    pos_of[key] = "up"
        for (n, p, mode), pos in F2_ADD[j].items():   # ... explicit overrides win last
            pos_of[(p, mode, n)] = pos
        for (p, mode, n), pos in pos_of.items():
            if (p, mode, n) in vals:
                _place(ax[j], n, vals[(p, mode, n)], p, mode, "{:.1f}", pos)
    ax[0].set_title("End-to-end latency p50")
    ax[1].set_title("End-to-end latency p99 (tail)")
    ax[1].set_xlabel("Cluster size (nodes = replicas)")
    _write_caption(OUT / "fig2_two_mode.png",
                   "Streaming vs. non-streaming end-to-end latency", list(CFG_8B))
    # one shared legend riding the top edge (no main title); panels below it
    handles, labels = ax[0].get_legend_handles_labels()
    top = 0.99
    panel_top = top - 0.08
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, top),
               ncol=3, fontsize=6, frameon=True, columnspacing=1.2, handlelength=1.8)
    return ps.finalize(fig, [], OUT / "fig2_two_mode.png", rect=(0, 0, 1, panel_top))


def fig3_workload(O, ONS):
    """OAT 8B workloads + 120B at n1 vs n64: SLO attainment & E2E p99."""
    cells = OAT + [("oat_120b", "120b")]
    labels = [l for _, l in cells]
    x = np.arange(len(cells)); w = 0.38
    bar_colors = {1: "#9ecae1", 64: "#08519c"}; bar_hatch = {1: "", 64: "//"}
    fig, ax = plt.subplots(2, 1, figsize=(COL_W, 3.2))
    for i, n in enumerate((1, 64)):
        att = [(O.get((s, n)).attainment if O.get((s, n)) else np.nan) for s, _ in cells]
        e2e = [(O.get((s, n)).e2e_p99 if O.get((s, n)) else np.nan) for s, _ in cells]
        off = (i - .5) * w
        for a, vals, fmt, dy in ((ax[0], att, "{:.2f}", 0.02), (ax[1], e2e, "{:.1f}", 0)):
            bars = a.bar(x + off, vals, w, label=f"N={n}", color=bar_colors[n],
                         hatch=bar_hatch[n], edgecolor="white", linewidth=0.5, zorder=3)
            for b, v in zip(bars, vals):
                if not np.isnan(v):
                    a.text(b.get_x() + b.get_width() / 2, v + dy, fmt.format(v), ha="center",
                           va="bottom", fontsize=4.5, fontweight="bold", color=bar_colors[n])
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
    fig, ax = plt.subplots(1, 3, figsize=(TEXT_W, 2.21))
    for j, (title, attr, slo) in enumerate(metrics):
        vals, names, cols, hats = [], [], [], []
        for p in PROXIES:
            st = S.get((p, 64))
            if not st:
                continue
            v = getattr(st, attr)
            vals.append(v if not np.isnan(v) else 0); names.append(ps.PROXY_LABEL[p])
            cols.append(COLORS[p]); hats.append(HATCHES[p])
        bars = ax[j].bar(names, vals, color=cols, edgecolor="white", linewidth=0.5, zorder=3)
        for b, h in zip(bars, hats):
            b.set_hatch(h)
        for b, v in zip(bars, vals):
            ax[j].text(b.get_x() + b.get_width() / 2, v, f"{v:.2f}", ha="center",
                       va="bottom", fontsize=5, fontweight="bold", color="#333333")
        if slo:
            ax[j].axhline(slo, ls="--", color="#E94F37", lw=1.0, label=f"SLO {slo}s")
        ax[j].grid(False, axis="x"); ax[j].set_title(title); ax[j].set_ylabel("seconds")
        ax[j].tick_params(axis="x", rotation=30)
        if slo:
            ps.legend(ax[j])
    _write_caption(OUT / "fig4_latency_n64.png",
                   "Per-proxy latency tails at 64 nodes (TTFT / TBT / E2E, p99)",
                   ["Streaming (SSE) · N=64"] + CFG_8B)
    fig.tight_layout(); out = OUT / "fig4_latency_n64.png"
    fig.savefig(out); plt.close(fig); return out


def fig5_latency_cdf(S):
    """TTFT & P99-TBT CDFs across proxies, per node count (n4/n64/n128). FAILED
    requests are included as the denominator, so each curve caps at the cell's
    success rate (it never reaches 1.0) — the honest picture for saturating
    proxies (litellm/rayserve)."""
    fig, axes = plt.subplots(len(CDF_NODES), 2, figsize=(COL_W, 1.53 * len(CDF_NODES)))
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
            lbl = ps.PROXY_LABEL[p]
            for ax, arr in ((axt, ttft), (axb, tbt)):
                a = arr[np.isfinite(arr)]
                if a.size == 0:
                    continue
                # Censored CDF: denominator = ALL run≥1 requests (incl. failures),
                # so the curve tops out at (finite values / total) ≈ success rate.
                cap = a.size / st.n_req
                q = np.linspace(0.0, 100.0, 2000)
                ax.plot(np.percentile(a, q), (q / 100.0) * cap, color=c, lw=ps.LW, alpha=0.9,
                        marker=mk, markevery=200, markersize=4, markerfacecolor=c,
                        markeredgecolor="white", markeredgewidth=1.2, label=lbl, zorder=3)
        # SLO reference lines + labels, with per-row nudges so the label stays
        # visible (x is a multiplier on the log axis, y is the CDF value 0..1).
        ttx, tty = {4: (1.0, 0.12), 64: (2.5, 0.04), 128: (2.5, 0.04)}.get(n, (1.0, 0.04))
        axt.axvline(P.TTFT_SLO_S, color="k", ls="--", lw=0.8, alpha=0.7)
        axt.text(P.TTFT_SLO_S * ttx, tty, " 2s SLO", fontsize=5, color="#333333")
        btx, bty = {64: (1.0, 0.12), 128: (3.0, 0.14)}.get(n, (1.0, 0.04))
        axb.axvline(P.TBT_P99_SLO_S, color="k", ls="--", lw=0.8, alpha=0.7)
        axb.text(P.TBT_P99_SLO_S * btx, bty, " 250ms SLO", fontsize=5, color="#333333")
        axt.set_title(f"TTFT — {n}n"); axt.set_ylabel("CDF (of all requests)")
        axb.set_title(f"P99 TBT — {n}n")
        # right column reuses the left column's y-axis (drop duplicate label + ticks)
        axb.tick_params(labelleft=False)
        # x-axis labels only on the bottom row (the two share the columns above)
        if row == len(CDF_NODES) - 1:
            axt.set_xlabel("TTFT (s)")
            axb.set_xlabel("P99 time-between-tokens (s)")
        for ax in (axt, axb):
            ax.set_xscale("log"); ax.set_ylim(0, 1.02)   # no per-panel legend
    _write_caption(OUT / "fig5_latency_cdf.png",
                   "TTFT & per-request P99-TBT distributions across proxies and scale",
                   ["Streaming (SSE) · nodes $\\in$ {4, 64, 128} · failures included $\\rightarrow$ curve caps at success rate",
                    "dashed = SLO (TTFT 1s / TBT 250ms)  ·  " + CFG_8B[0], CFG_8B[1]])
    # one shared legend riding the top edge (no main title); grid below it
    from matplotlib.lines import Line2D
    handles = [Line2D([0], [0], color=COLORS[p], marker=MARKERS[p], lw=ps.LW,
                      markersize=ps.MS, markerfacecolor=COLORS[p],
                      markeredgecolor="white", markeredgewidth=0.5) for p in PROXIES]
    top = 0.99
    legh = 0.20 / fig.get_size_inches()[1]      # reserve the 1-row legend at the top
    panel_top = top - legh
    fig.legend(handles, [ps.PROXY_LABEL[p] for p in PROXIES], loc="lower center",
               bbox_to_anchor=(0.5, panel_top + 0.004), ncol=5, fontsize=6, frameon=True,
               columnspacing=1.2, handlelength=1.8)
    fig.tight_layout(rect=(0, 0, 1, panel_top)); out = OUT / "fig5_latency_cdf.png"
    fig.savefig(out); plt.close(fig); return out


# Render jobs are stashed at module scope so forked workers inherit them (and the
def fig6_sglang_backend(S):
    """Backend swap under the identical serving stack: vLLM vs SGLang weak
    scaling through the SAME HAProxy path, each offered ~0.9x its own measured
    single-node saturation (vLLM 110, SGLang 15 req/s/node). Panel (a) delivered
    successful throughput vs N (log-log, per-backend ideal slope-1 guides);
    panel (b) weak-scaling efficiency normalized to each backend's n1.
    SGLang points read directly from runs/sglang_haproxy_full/run8 (mean ± std
    over the 5 post-warmup runs); vLLM reuses the fig1 haproxy-stream cells, so
    it keeps its fig1 identity (color/marker). SGLang gets its own fixed
    identity (blue, 'P') — orange/blue + distinct markers stays legible under
    CVD. n256 omitted: the shared-HAProxy point is proxy-capped for both
    backends (fig1); the SGLang direct-mode 256n companion is a separate run."""
    import json
    from matplotlib.lines import Line2D

    SGL_NODES = [1, 4, 16, 64, 128, 256]
    SGL_ROOT = Path("/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs/sglang_haproxy_full/run8")
    SGL_COLOR, SGL_MARKER = "#386cb0", "P"

    def _sgl_point(n):
        f = SGL_ROOT / f"n{n}" / "results" / "result0.json"
        if not f.exists():
            return None
        per_run = json.loads(f.read_text())["per_run"]
        rs = [r["rps"] * (1 - r["errors"] / max(r["requests_completed"], 1))
              for r in per_run if r["run_index"] > 0]
        return (float(np.mean(rs)), float(np.std(rs))) if rs else None

    sgl = {n: p for n in SGL_NODES if (p := _sgl_point(n))}
    vll = {}
    for n in SGL_NODES:
        st = S.get(("haproxy", n)) or cell(pstem("haproxy", n), n)
        if st and not np.isnan(st.rps):
            vll[n] = _pt(st, "succ_rps")

    # SGLang through Direct-MPI dispatch (no proxy): the proxy-free companion
    # series (sglang_direct_full n1-128 + sglang_direct_n256 run4 for 256)
    # showing the backend itself scales past the HAProxy ceiling. Same
    # green/circle identity as "Direct" in fig1.
    _RUNS = Path("/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs")
    def _direct_point(n):
        stem = ("sglang_direct_n256/run4" if n == 256 else "sglang_direct_full/run0")
        f = _RUNS / stem / f"n{n}" / "results" / "result0.json"
        if not f.exists():
            return None
        per_run = json.loads(f.read_text())["per_run"]
        rs = [r["rps"] * (1 - r["errors"] / max(r["requests_completed"], 1))
              for r in per_run if r["run_index"] > 0]
        return (float(np.mean(rs)), float(np.std(rs))) if rs else None

    sgl_direct = {n: p for n in SGL_NODES if (p := _direct_point(n))}

    fig, a_tp = plt.subplots(figsize=(COL_W, 1.55))   # native single-column, compact height

    def _series(a, pts, color, marker, label, zorder=3):
        xs = sorted(pts)
        base = pts[xs[0]][0] / xs[0]
        ys = [pts[n][0] for n in xs]; es = [pts[n][1] for n in xs]
        a.errorbar(xs, ys, yerr=es, color=color, marker=marker, linestyle="-",
                   lw=ps.LW, markersize=ps.MS, markerfacecolor=color,
                   markeredgecolor="white", markeredgewidth=ps.MEW,
                   label=label, capsize=2, capthick=0.6, zorder=zorder)
        a.plot(xs, [base * n for n in xs], color=color, ls=":", lw=0.8,
               alpha=0.5, zorder=2)         # ideal slope-1 guide through this backend's n1

    _series(a_tp, vll, COLORS["haproxy"], MARKERS["haproxy"], "vLLM")
    _series(a_tp, sgl, SGL_COLOR, SGL_MARKER, "SGLang")
    if sgl_direct:
        # overlaps the haproxy-path SGLang within ~2% at 1..128 (diverges at 256);
        # keep it just under so the blue 'P' marker stays visible on top.
        _series(a_tp, sgl_direct, COLORS["direct"], MARKERS["direct"],
                "SGLang (direct)", zorder=2.8)
    a_tp.set_yscale("log"); ps.sparse_log_y(a_tp, sci=True)
    a_tp.set_ylabel("Throughput (query/s)")
    a_tp.set_xlabel("Cluster size (nodes)")

    # Efficiency is annotated ON the throughput curve now (no separate panel): each
    # labelled point carries aggregate throughput (top line) and weak-scaling
    # efficiency (per-node rate vs the backend's own single-node baseline).
    def _eff(pts):
        xs = sorted(pts); base = pts[xs[0]][0] / xs[0]
        return {n: 100.0 * (pts[n][0] / n) / base for n in xs}

    def _tp(v):
        return f"{v/1000:.1f}k" if v >= 1000 else f"{v:.0f}"

    def _label(pts, color, pos_of, dy_of=lambda n: 8):
        eff = _eff(pts)
        for n in SGL_NODES:
            if n in pts:
                _place_txt(a_tp, n, pts[n][0], f"{_tp(pts[n][0])}\n{eff[n]:.0f}%",
                           color, pos_of(n), dy=dy_of(n))
    # vLLM (top line) above everywhere; SGLang above at n1..64 but BELOW at
    # n128/n256 so it drops clear of the vLLM labels where the curves converge.
    # The n1..64 SGLang labels hug their own line (small offset) so they don't
    # rise into the vLLM line just above.
    _label(vll, COLORS["haproxy"], lambda n: "up")
    _label(sgl, SGL_COLOR, lambda n: "down" if n in (128, 256) else "up",
           dy_of=lambda n: 8 if n in (128, 256) else 3)
    if sgl_direct and 256 in sgl_direct:     # only the 256n split is distinct from sgl
        eff = _eff(sgl_direct)
        _place_txt(a_tp, 256, sgl_direct[256][0],
                   f"{_tp(sgl_direct[256][0])}\n{eff[256]:.0f}%", COLORS["direct"], "up")

    _node_axis(a_tp)

    _write_caption(OUT / "fig6_sglang_backend.png",
                   "Engine swap on the same serving stack: vLLM vs SGLang weak scaling",
                   ["Meta-Llama-3-8B-Instruct  ·  TP=1 (12 replicas/node)  ·  64$\\rightarrow$64 tok  ·  HAProxy, streaming",
                    "offered = 0.9$\\times$ own single-node saturation (vLLM 110, SGLang 15 req/s/node)  ·  ALCF Aurora",
                    "dotted = ideal (slope-1); point labels = aggregate throughput and weak-scaling efficiency (per-node vs own n1)"])
    handles = [Line2D([0], [0], color=COLORS["haproxy"], marker=MARKERS["haproxy"],
                      lw=ps.LW, markersize=ps.MS, markerfacecolor=COLORS["haproxy"],
                      markeredgecolor="white", markeredgewidth=ps.MEW),
               Line2D([0], [0], color=SGL_COLOR, marker=SGL_MARKER, lw=ps.LW,
                      markersize=ps.MS, markerfacecolor=SGL_COLOR,
                      markeredgecolor="white", markeredgewidth=ps.MEW)]
    labels = ["vLLM", "SGLang"]
    if sgl_direct:
        handles.append(Line2D([0], [0], color=COLORS["direct"], marker=MARKERS["direct"],
                              lw=ps.LW, markersize=ps.MS, markerfacecolor=COLORS["direct"],
                              markeredgecolor="white", markeredgewidth=ps.MEW))
        labels.append("SGLang (direct)")
    top = 0.99
    legh = 0.20 / fig.get_size_inches()[1]
    panel_top = top - legh
    fig.legend(handles, labels, loc="lower center",
               bbox_to_anchor=(0.5, panel_top + 0.004), ncol=len(labels), fontsize=6,
               frameon=True, columnspacing=0.9, handlelength=1.5, handletextpad=0.4)
    return ps.finalize(fig, [], OUT / "fig6_sglang_backend.png", rect=(0, 0, 1, panel_top))


def _pp405b_points(stem):
    """Newest run per node-count for a 405B PP=2 sweep variant → list of dicts
    (rep, srps=successful throughput, p50, p99, errfrac), sorted by replica count."""
    import glob
    import json
    import re
    best = {}
    for rf in glob.glob(str(P.RUNS_ROOT / stem / "run*/n*/results/result0.json")):
        n = int(re.search(r"/n(\d+)/", rf).group(1))
        run = int(re.search(r"/run(\d+)/", rf).group(1))
        try:
            o = json.load(open(rf)).get("overall", {})
        except Exception:
            continue
        rps = o.get("rps")
        if rps is None or np.isnan(rps):
            continue
        comp = o.get("requests_completed") or 0
        err = o.get("errors") or 0
        sr = (comp - err) / comp if comp else 0.0
        rec = dict(rep=n // 2, nodes=n, srps=rps * sr, p50=o.get("p50_s"), p99=o.get("p99_s"),
                   errfrac=(err / comp if comp else 0.0))
        if n not in best or run > best[n][0]:
            best[n] = (run, rec)
    return [best[n][1] for n in sorted(best)]


def fig_pp405b(_ignored=None):
    """405B (TP=8×PP=2, one replica per 2 nodes) shard-aware weak scaling: aggregate
    successful throughput and weak-scaling efficiency vs replica count, DIRECT vs
    HAProxy. Reads the pp405b_pp2_scale[_direct] sweeps directly (own x-axis)."""
    variants = [("pp405b_pp2_scale_direct", "direct", "Direct"),
                ("pp405b_pp2_scale", "haproxy", "HAProxy")]
    data = {key: _pp405b_points(stem) for stem, key, _ in variants}
    base = data["direct"][0]
    per_node = base["srps"] / base["nodes"]              # weak-scaling unit rate (per node)
    allnodes = sorted({p["nodes"] for pts in data.values() for p in pts})
    xlabel = "nodes (PP=2 → 2 nodes/replica)"
    fig, a = plt.subplots(figsize=(COL_W, 1.7))   # native single-column (one-col figure)
    a.plot(allnodes, [per_node * n for n in allnodes], ls=":", lw=1.0, color="#888888",
           zorder=2, label="ideal (linear)")
    for _, key, lab in variants:
        pts = data[key]
        ps.line(a, [p["nodes"] for p in pts], [p["srps"] for p in pts], key, "stream", label=lab)
        # each point labelled with throughput (top line) and weak-scaling efficiency (%)
        for p in pts:
            eff = p["srps"] / (per_node * p["nodes"]) * 100
            tp = f"{p['srps']/1000:.1f}k" if p["srps"] >= 1000 else f"{p['srps']:.1f}"
            _place_txt(a, p["nodes"], p["srps"], f"{tp}\n{eff:.0f}%", COLORS[key],
                       "up" if key == "direct" else "down")
    a.set_xscale("log", base=2); a.set_yscale("log"); ps.sparse_log_y(a, sci=True)
    a.set_xticks(allnodes); a.set_xticklabels([str(n) for n in allnodes])
    a.set_xlabel(xlabel)
    a.set_ylabel("Successful throughput (query/s)")
    ps.legend(a, loc="upper left")
    _write_caption(OUT / "fig7_pp405b.png",
                   "Shard-aware pipeline-parallel weak scaling: Llama-3.1-405B (TP=8 × PP=2)",
                   ["one replica per 2 nodes · 4–256 nodes (2–128 PP=2 replicas) · fixed offered rate/replica",
                    f"successful throughput; point labels = throughput and weak-scaling efficiency vs the 4-node base ({per_node:.2f} query/s/node)"])
    fig.tight_layout(); out = OUT / "fig7_pp405b.png"
    fig.savefig(out); plt.close(fig); return out


# already-built cell data they close over) via copy-on-write — no pickling of the
# large arrays; only the small integer index and the returned path(s) cross the
# process boundary.
_RENDER_JOBS = []


BUILT_CACHE = Path("/tmp/sc26_full_built.pkl")  # pickled (S, NS, O, ONS)


def _install_pdf_only():
    """Redirect every '<name>.png' savefig to '<name>.pdf' (vector PDF only, no PNG).
    Applied in the PARENT before the worker fork so forked renderers inherit it."""
    from matplotlib.figure import Figure
    if getattr(Figure.savefig, "_pdf_only", False):
        return
    _orig = Figure.savefig
    def savefig(self, fname, *a, **k):
        s = str(fname)
        if s.endswith(".png"):
            s = s[:-4] + ".pdf"
            k = {kk: vv for kk, vv in k.items() if kk not in ("format", "dpi")}
        return _orig(self, s, *a, **k)
    savefig._pdf_only = True
    Figure.savefig = savefig


def _render_job(i):
    """Render one figure in a worker process. matplotlib is NOT thread-safe, so
    figures are parallelised across PROCESSES (fork), never threads."""
    import matplotlib
    matplotlib.use("Agg")
    fn, args = _RENDER_JOBS[i]
    return fn(*args)


def main():
    global _RENDER_JOBS
    refresh = "--refresh" in sys.argv
    serial = "--serial" in sys.argv          # force sequential (debugging)
    reuse = "--reuse" in sys.argv            # skip the ~60s build; reuse last data
    if "--png" not in sys.argv:
        _install_pdf_only()                  # vector PDF only (no PNG); --png to keep PNG

    import pickle
    if reuse and not refresh and BUILT_CACHE.exists():
        print(f"reusing built data from {BUILT_CACHE} (skip build)...", flush=True)
        with open(BUILT_CACHE, "rb") as fh:
            S, NS, O, ONS = pickle.load(fh)
    else:
        print("building cells (full sweep)...", flush=True)
        S, NS, O, ONS = build(refresh=refresh)   # data load happens ONCE, in the parent
        try:
            with open(BUILT_CACHE, "wb") as fh:
                pickle.dump((S, NS, O, ONS), fh, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as e:                   # caching is best-effort
            print(f"  (warn: could not write {BUILT_CACHE}: {e})", flush=True)
    got = sum(1 for v in {**S, **NS, **O, **ONS}.values() if v)
    print(f"  extracted {got} cells", flush=True)

    _RENDER_JOBS = [(fig1_proxy_scaling, (S, NS)),          # fig1 full-width (figure*)
                    (fig1_proxy_scaling, (S, NS, False)),   # fig1 native single-column variant
                    (fig2_two_mode, (S, NS)),           # fig3 workload is now a paper table
                    (fig4_latency, (S,)), (fig5_latency_cdf, (S,)),
                    (fig6_sglang_backend, (S,)), (fig_pp405b, (S,))]
    # --only <substr>: render just the jobs whose function name contains substr.
    only = None
    if "--only" in sys.argv:
        only = sys.argv[sys.argv.index("--only") + 1]
    idxs = [i for i, (fn, _) in enumerate(_RENDER_JOBS)
            if only is None or only in fn.__name__]

    if serial or len(idxs) == 1:
        results = [_render_job(i) for i in idxs]
    else:
        import multiprocessing as mp
        nproc = min(len(idxs), max(1, (os.cpu_count() or 4) - 1))
        print(f"  rendering {len(idxs)} figures across {nproc} processes...", flush=True)
        # fork: workers inherit _RENDER_JOBS + built data (copy-on-write). Pool is
        # created AFTER the data is in place so the fork snapshot includes it.
        with mp.get_context("fork").Pool(nproc) as pool:
            results = pool.map(_render_job, idxs)

    for out in results:
        for o in (out if isinstance(out, list) else [out]):
            print(f"  wrote {o}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
