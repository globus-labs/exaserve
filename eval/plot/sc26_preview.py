#!/usr/bin/env python3
"""SC26 workshop *validation/pilot* preview plots.

Purpose: before committing to the full sweep (~1k node·h), render the handful of
figures that (a) confirm every validation cell produced sane, plottable data and
(b) preview the paper's key trends so we know the full sweep will tell the
intended story.

This is deliberately scoped to the `validation/` run groups under
  <experiments_root>/runs/sc26workshop/validation/<spec>/runN/nM/results/result0.json

Cell selection rule
-------------------
For each (spec, node-count) we take the HIGHEST-numbered `runN` group that
actually contains that node's `result0.json`, and use only `run_index >= 1`
records (run 0 is the v2-protocol warm-up and is dropped). This resolves the
messy retries automatically:
  * proxycmp_direct n64   -> run1   (only group with the cell; gather-fix re-run)
  * proxycmp_litellm n4   -> run3   (the recovered clean retry)
  * everything else       -> run0

Metrics (kept consistent with eval/plot/goodput.py)
--------------------------------------------------
  achieved throughput = sum(completed) / sum(duration)        over run>=1
  paper SLO attainment = #{TTFT<=1s AND P99-TBT<=250ms AND ok} / #requests
  goodput              = throughput * attainment              (= SLO-met req/s)

Extraction streams each result file once with ijson (yajl2_c) and writes a
compact per-cell cache (scalars json + float32 npz of the per-request arrays)
so re-plotting is instant. Pass --refresh to rebuild the cache.

Usage:
  python -m eval.plot.sc26_preview                 # extract (cached) + all plots
  python -m eval.plot.sc26_preview --refresh       # force re-extract
  python -m eval.plot.sc26_preview --only set1     # set1|set2|cdf
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from dataclasses import dataclass, asdict
from pathlib import Path

os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")

import numpy as np
import ijson

RUNS_ROOT = Path(
    "/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs/sc26workshop/validation"
)
OUT_DIR = Path(__file__).resolve().parent / "output" / "sc26_preview"
CACHE_DIR = Path("/tmp/sc26_preview_cache")

# Paper SLO (Sarathi-Serve style): TTFT <= 1s AND P99 time-between-tokens <= 250ms.
TTFT_SLO_S = 1.0
TBT_P99_SLO_S = 0.250
# Extra TTFT thresholds tracked per cell so the TTFT-attainment panel can show
# how a looser first-token budget exposes the node-count scaling trend.
TTFT_SLO_MULTI = (1.0, 2.0, 3.0)

# --- Suite definitions (mirror README "Run checklist") ----------------------

PROXIES = ["direct", "haproxy", "envoy", "rayserve", "litellm"]
PROXY_NODES = [1, 4, 16, 64]
# 256n extension exists only for these two (separate _256_val specs, prod queue).
PROXY_256 = {"direct", "haproxy"}
ALL_PROXY_NODES = [1, 4, 16, 64, 256]


def proxy_stem(proxy: str, node: int) -> str:
    """The 256n cells live in dedicated proxycmp_<proxy>_256 specs."""
    return f"proxycmp_{proxy}_256" if node == 256 else f"proxycmp_{proxy}"


def proxy_nodes(proxy: str) -> list[int]:
    return PROXY_NODES + ([256] if proxy in PROXY_256 else [])

# Set 2 OAT: (spec_stem, label, model_tag). All N in {1,64}.
OAT_CELLS = [
    ("oat_8b_baseline", "baseline\n(1k×64)", "8B"),
    ("oat_8b_poisson", "poisson", "8B"),
    ("oat_8b_2kx2k", "2k×2k", "8B"),
    ("oat_8b_4kx4k", "4k×4k", "8B"),
    ("oat_8b_code", "code", "8B"),
    ("oat_8b_chat", "chat", "8B"),
    ("oat_8b_summary", "summary", "8B"),
    ("oat_8b_burstgpt", "burstgpt", "8B"),
    ("oat_120b", "120b\n(rate 9)", "120B"),
]
OAT_NODES = [1, 64]

COLORS = {
    "direct": "#1b9e77",
    "haproxy": "#d95f02",
    "envoy": "#7570b3",
    "rayserve": "#e7298a",
    "litellm": "#66a61e",
}


# --- Cell resolution & streaming extraction ---------------------------------


def resolve_cell(spec_stem: str, node: int) -> Path | None:
    """Highest runN group that has result0.json for this (spec, node)."""
    spec_dir = RUNS_ROOT / f"{spec_stem}_val"
    if not spec_dir.is_dir():
        # Set 2 oat specs already carry the _val suffix in their stem? No: stems
        # here are bare; the dir is "<stem>_val". But guard for both.
        spec_dir = RUNS_ROOT / spec_stem
        if not spec_dir.is_dir():
            return None
    candidates = []
    for rg in spec_dir.iterdir():
        if not (rg.is_dir() and rg.name.startswith("run")):
            continue
        try:
            idx = int(rg.name[3:])
        except ValueError:
            continue
        f = rg / f"n{node}" / "results" / "result0.json"
        if f.exists():
            candidates.append((idx, f))
    if not candidates:
        return None
    return max(candidates, key=lambda t: t[0])[1]


@dataclass
class CellStats:
    spec: str
    node: int
    src: str
    n_req: int
    n_success: int
    rps: float          # achieved throughput (completed/duration), run>=1
    attainment: float   # paper SLO (TTFT≤1s ∧ P99-TBT≤250ms), failures count as miss
    ttft_attainment: float  # frac requests with TTFT ≤ 1s (separated)
    tbt_attainment: float   # frac requests with P99-TBT ≤ 250ms (separated)
    goodput: float
    success_rate: float
    ttft_p50: float
    ttft_p99: float
    tbt_p50: float
    tbt_p99: float
    e2e_p50: float
    e2e_p99: float
    decode_p50: float   # E2E - TTFT (decode duration), successful reqs
    decode_p99: float
    # TTFT attainment at 1/2/3s (aggregate over run>=1). ttft_attain_1s == ttft_attainment.
    ttft_attain_1s: float = float("nan")
    ttft_attain_2s: float = float("nan")
    ttft_attain_3s: float = float("nan")
    # Per-run series (run_index>=1) for error bars. Lists, one entry per data run.
    runs_succ_rps: list = None        # per-run successful throughput (rps × success)
    runs_succ_rate: list = None       # per-run success rate
    runs_tbt_attain: list = None      # per-run TBT attainment
    runs_ttft_attain_1s: list = None
    runs_ttft_attain_2s: list = None
    runs_ttft_attain_3s: list = None


def extract_cell(spec_stem: str, node: int, *, keep_arrays: bool,
                 refresh: bool) -> CellStats | None:
    src = resolve_cell(spec_stem, node)
    if src is None:
        return None
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tag = f"{spec_stem}__n{node}"
    sjson = CACHE_DIR / f"{tag}.json"
    snpz = CACHE_DIR / f"{tag}.npz"
    if not refresh and sjson.exists() and (not keep_arrays or snpz.exists()):
        d = json.loads(sjson.read_text())
        # Tolerate older caches written before new percentile fields existed:
        # fill any missing field with NaN (figures that need it pass refresh=True).
        return CellStats(**{k: d.get(k, float("nan"))
                            for k in CellStats.__dataclass_fields__})

    # Stream: per_run summaries (run>=1) then the requests array.
    completed = 0.0
    duration = 0.0
    per_run_cd: dict[int, list[float]] = {}   # run_index -> [completed, duration]
    with open(src, "rb") as fh:
        for pr in ijson.items(fh, "per_run.item"):
            ri = int(pr.get("run_index", 0))
            if ri >= 1:
                c = float(pr.get("requests_completed", 0) or 0)
                d = float(pr.get("duration_s", 0) or 0)
                completed += c; duration += d
                per_run_cd[ri] = [c, d]
    rps = completed / duration if duration > 0 else float("nan")

    ttft_l: list[float] = []
    tbt_l: list[float] = []
    lat_l: list[float] = []
    dec_l: list[float] = []
    n_req = 0
    n_success = 0
    n_meet = 0
    n_ttft_meet = [0, 0, 0]   # per TTFT_SLO_MULTI threshold
    n_tbt_meet = 0
    # per-run counters: run_index -> [n_req, n_succ, n_tbt_meet, n_ttft1, n_ttft2, n_ttft3]
    pr_ctr: dict[int, list[int]] = defaultdict(lambda: [0, 0, 0, 0, 0, 0])
    with open(src, "rb") as fh:
        for r in ijson.items(fh, "requests.item"):
            ri = int(r.get("run_index", 0))
            if ri < 1:
                continue
            n_req += 1
            ok = bool(r.get("success", True))
            ttft = r.get("ttft_s")
            tbt = r.get("tbt_p99_s")
            lat = r.get("latency")
            lat = float(lat) if lat is not None else float("nan")
            # Non-stream requests carry no per-token timing (ttft_s/tbt_p99_s are
            # null). Derive a COARSE estimate from the single E2E latency L and the
            # completion-token count T: average per-token τ = L/T as the TBT proxy,
            # and TTFT = L (the whole response arrives at once, so the first visible
            # token is at L). Streaming requests use their real measured values.
            if ttft is None and tbt is None and not np.isnan(lat):
                comp = r.get("actual_completion_tokens") or r.get("output_len") or 0
                comp = float(comp)
                ttft = lat
                tbt = (lat / comp) if comp > 0 else float("nan")
            else:
                ttft = float(ttft) if ttft is not None else float("nan")
                tbt = float(tbt) if tbt is not None else float("nan")
            if ok:
                n_success += 1
                ttft_l.append(ttft)
                tbt_l.append(tbt)
                lat_l.append(lat)
                dec_l.append(lat - ttft)
            ttft_meets = [ok and not np.isnan(ttft) and ttft <= thr for thr in TTFT_SLO_MULTI]
            tbt_ok = ok and not np.isnan(tbt) and tbt <= TBT_P99_SLO_S
            c = pr_ctr[ri]
            c[0] += 1
            if ok:
                c[1] += 1
            if tbt_ok:
                n_tbt_meet += 1; c[2] += 1
            for j, m in enumerate(ttft_meets):
                if m:
                    n_ttft_meet[j] += 1; c[3 + j] += 1
            if ttft_meets[0] and tbt_ok:   # paper SLO conjunction uses the 1s TTFT
                n_meet += 1

    ttft_a = np.asarray(ttft_l, dtype=np.float32)
    tbt_a = np.asarray(tbt_l, dtype=np.float32)
    lat_a = np.asarray(lat_l, dtype=np.float32)
    dec_a = np.asarray(dec_l, dtype=np.float32)
    attainment = n_meet / n_req if n_req else float("nan")
    _p = lambda a, q: float(np.nanpercentile(a, q)) if a.size else float("nan")
    _frac = lambda num: (num / n_req if n_req else float("nan"))

    # Per-run series (ordered by run_index) for error bars.
    runs = sorted(pr_ctr)
    runs_succ_rps, runs_succ_rate = [], []
    runs_tbt, runs_t1, runs_t2, runs_t3 = [], [], [], []
    for ri in runs:
        nr, ns, ntbt, nt1, nt2, nt3 = pr_ctr[ri]
        cd = per_run_cd.get(ri)
        r_rps = (cd[0] / cd[1]) if (cd and cd[1] > 0) else float("nan")
        sr = (ns / nr) if nr else float("nan")
        runs_succ_rps.append(r_rps * sr)
        runs_succ_rate.append(sr)
        runs_tbt.append(ntbt / nr if nr else float("nan"))
        runs_t1.append(nt1 / nr if nr else float("nan"))
        runs_t2.append(nt2 / nr if nr else float("nan"))
        runs_t3.append(nt3 / nr if nr else float("nan"))

    st = CellStats(
        spec=spec_stem, node=node, src=str(src),
        n_req=n_req, n_success=n_success, rps=rps,
        attainment=attainment, goodput=rps * attainment,
        ttft_attainment=_frac(n_ttft_meet[0]),
        tbt_attainment=_frac(n_tbt_meet),
        success_rate=(n_success / n_req if n_req else float("nan")),
        ttft_p50=_p(ttft_a, 50), ttft_p99=_p(ttft_a, 99),
        tbt_p50=_p(tbt_a, 50), tbt_p99=_p(tbt_a, 99),
        e2e_p50=_p(lat_a, 50), e2e_p99=_p(lat_a, 99),
        decode_p50=_p(dec_a, 50), decode_p99=_p(dec_a, 99),
        ttft_attain_1s=_frac(n_ttft_meet[0]), ttft_attain_2s=_frac(n_ttft_meet[1]),
        ttft_attain_3s=_frac(n_ttft_meet[2]),
        runs_succ_rps=runs_succ_rps, runs_succ_rate=runs_succ_rate,
        runs_tbt_attain=runs_tbt, runs_ttft_attain_1s=runs_t1,
        runs_ttft_attain_2s=runs_t2, runs_ttft_attain_3s=runs_t3,
    )
    sjson.write_text(json.dumps(asdict(st)))
    np.savez_compressed(snpz, ttft=ttft_a, tbt=tbt_a)
    return st


def load_arrays(spec_stem: str, node: int) -> tuple[np.ndarray, np.ndarray]:
    npz = CACHE_DIR / f"{spec_stem}__n{node}.npz"
    d = np.load(npz)
    return d["ttft"], d["tbt"]


# --- Plotting ---------------------------------------------------------------


def _mpl():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    return plt


def plot_set1(stats: dict[tuple[str, int], CellStats]) -> Path:
    """3-panel proxy scaling (stacked vertically): successful throughput |
    attainment | goodput, vs N. Throughput is SUCCESSFUL rps (= completed-rps ×
    success-rate) so the dropped-request proxies (rayserve/litellm) are not
    credited for requests that errored out."""
    plt = _mpl()
    fig, axes = plt.subplots(3, 1, figsize=(13, 15))
    ideal = None
    for proxy in PROXIES:
        xs, rps_ok, att, gp = [], [], [], []
        for n in proxy_nodes(proxy):
            st = stats.get((proxy, n))
            if st is None:
                continue
            xs.append(n)
            rps_ok.append(st.rps * st.success_rate)
            att.append(st.attainment)
            gp.append(st.goodput)
        if not xs:
            continue
        c = COLORS[proxy]
        axes[0].plot(xs, rps_ok, "o-", color=c, label=proxy)
        axes[1].plot(xs, att, "o-", color=c, label=proxy)
        axes[2].plot(xs, gp, "o-", color=c, label=proxy)
        if proxy == "direct":
            base = stats.get(("direct", 1))
            if base:
                ideal = [(n, base.rps * base.success_rate * n) for n in ALL_PROXY_NODES]

    if ideal:
        ix = [n for n, _ in ideal]
        iy = [v for _, v in ideal]
        axes[0].plot(ix, iy, "k--", alpha=0.5, label="ideal linear\n(direct n1×N)")

    # Overlay NON-streaming haproxy (n64, n256-corrected-client=4) on the throughput
    # panel: shows the headline — streaming collapses at 256n while non-streaming
    # scales (27k). Non-stream has no TTFT/TBT so it's throughput-panel only.
    ns_pts = []
    for n, stem in [(64, "proxycmp_haproxy_nostream"),
                    (256, "proxycmp_haproxy_nostream_c4_256")]:
        st = extract_cell(stem, n, keep_arrays=False, refresh=False)
        if st:
            ns_pts.append((n, st.rps * st.success_rate))
    if ns_pts:
        axes[0].plot([p[0] for p in ns_pts], [p[1] for p in ns_pts], "D--",
                     color=COLORS["haproxy"], alpha=0.6, markersize=9, markerfacecolor="none",
                     label="haproxy NON-stream\n(27k @256n — scales)")

    axes[0].set(title="Successful throughput vs cluster size",
                xlabel="nodes (= replicas, TP=1)", ylabel="successful requests/s")
    axes[0].set_yscale("log", base=10)
    axes[1].set(title="Paper-SLO attainment vs cluster size",
                xlabel="nodes", ylabel="attainment (TTFT≤1s ∧ P99-TBT≤250ms)")
    axes[1].set_ylim(-0.02, 1.02)
    axes[2].set(title="iso-SLO goodput vs cluster size",
                xlabel="nodes", ylabel="goodput = rps × attainment (SLO-met req/s)")
    axes[2].set_yscale("log", base=10)
    for ax in axes:
        ax.set_xscale("log", base=2)
        ax.set_xticks(ALL_PROXY_NODES)
        ax.set_xticklabels([str(n) for n in ALL_PROXY_NODES])
        ax.axvspan(64, 256, color="grey", alpha=0.06)
        ax.grid(True, which="both", alpha=0.3)
        ax.legend(fontsize=9)
    axes[0].text(128, axes[0].get_ylim()[1], " 256n: direct+haproxy only",
                 fontsize=8, va="top", ha="center", color="grey")
    fig.suptitle("Set 1 — proxy/dispatch comparison "
                 "(8B, offered rate 110 rps/node ≈ saturation stress; "
                 "256n = haproxy+direct extension)",
                 fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, 0.98))
    out = OUT_DIR / "set1_proxy_scaling.png"
    fig.savefig(out, dpi=130)
    plt.close(fig)
    return out


def plot_set2(stats: dict[tuple[str, int], CellStats]) -> Path:
    """Grouped bars: SLO attainment n1 vs n64 per workload + throughput labels."""
    plt = _mpl()
    fig, (ax, ax2) = plt.subplots(2, 1, figsize=(13, 8), sharex=True)
    labels, att1, att64, rps1, rps64 = [], [], [], [], []
    for stem, label, _ in OAT_CELLS:
        s1 = stats.get((stem, 1))
        s64 = stats.get((stem, 64))
        labels.append(label)
        att1.append(s1.attainment if s1 else np.nan)
        att64.append(s64.attainment if s64 else np.nan)
        rps1.append(s1.rps if s1 else np.nan)
        rps64.append(s64.rps if s64 else np.nan)

    x = np.arange(len(labels))
    w = 0.38
    ax.bar(x - w / 2, att1, w, label="N=1", color="#9ecae1")
    ax.bar(x + w / 2, att64, w, label="N=64", color="#08519c")
    for xi, a in zip(x - w / 2, att1):
        if not np.isnan(a):
            ax.text(xi, a + 0.02, f"{a:.2f}", ha="center", va="bottom", fontsize=7)
    for xi, a in zip(x + w / 2, att64):
        if not np.isnan(a):
            ax.text(xi, a + 0.02, f"{a:.2f}", ha="center", va="bottom", fontsize=7)
    ax.set(ylabel="paper-SLO attainment", ylim=(0, 1.15),
           title="Set 2 — OAT workload robustness: attainment N=1 vs N=64")
    ax.legend()
    ax.grid(True, axis="y", alpha=0.3)

    ax2.bar(x - w / 2, rps1, w, label="N=1", color="#a1d99b")
    ax2.bar(x + w / 2, rps64, w, label="N=64", color="#006d2c")
    ax2.set(ylabel="achieved throughput (rps)", yscale="log",
            title="achieved throughput (log) — confirms throughput scales while SLO may not")
    ax2.set_xticks(x)
    ax2.set_xticklabels(labels, fontsize=8)
    ax2.legend()
    ax2.grid(True, axis="y", which="both", alpha=0.3)
    fig.tight_layout()
    out = OUT_DIR / "set2_oat_robustness.png"
    fig.savefig(out, dpi=130)
    plt.close(fig)
    return out


def plot_cdf(stats: dict[tuple[str, int], CellStats]) -> Path:
    """TTFT & P99-TBT CDFs at N=64 across proxies — localizes the bottleneck."""
    plt = _mpl()
    fig, (axt, axb) = plt.subplots(1, 2, figsize=(14, 5.5))
    for proxy in PROXIES:
        st = stats.get((proxy, 64))
        if st is None:
            continue
        try:
            ttft, tbt = load_arrays(f"proxycmp_{proxy}", 64)
        except FileNotFoundError:
            continue
        c = COLORS[proxy]
        for ax, arr, slo in ((axt, ttft, TTFT_SLO_S), (axb, tbt, TBT_P99_SLO_S)):
            a = arr[np.isfinite(arr)]
            if a.size == 0:
                continue
            a = np.sort(a)
            y = np.arange(1, a.size + 1) / a.size
            lbl = f"{proxy} (ok={st.success_rate:.0%})"
            ax.plot(a, y, color=c, label=lbl)
    axt.axvline(TTFT_SLO_S, color="k", ls="--", alpha=0.6)
    axt.text(TTFT_SLO_S, 0.05, " 1s SLO", fontsize=8)
    axt.set(title="TTFT CDF @ N=64 (successful reqs)",
            xlabel="TTFT (s)", ylabel="CDF", xscale="log")
    axb.axvline(TBT_P99_SLO_S, color="k", ls="--", alpha=0.6)
    axb.text(TBT_P99_SLO_S, 0.05, " 250ms SLO", fontsize=8)
    axb.set(title="per-request P99 TBT CDF @ N=64 (successful reqs)",
            xlabel="P99 time-between-tokens (s)", ylabel="CDF", xscale="log")
    for ax in (axt, axb):
        ax.set_ylim(0, 1.02)
        ax.grid(True, which="both", alpha=0.3)
        ax.legend(fontsize=8)
    fig.suptitle("Set 1 diagnostic @ N=64 — where the SLO budget is spent "
                 "(front-end queueing in TTFT vs decode in TBT)", fontsize=12)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    out = OUT_DIR / "set1_latency_cdf_n64.png"
    fig.savefig(out, dpi=130)
    plt.close(fig)
    return out


def plot_disc(stats: dict[tuple[str, int], CellStats]) -> Path:
    """Discriminating test: baseline (rate 98, 64/64, stream) proxy vs direct at
    N=1 vs N=64. Two panels — median TTFT and median decode — decompose the n64
    SLO collapse into the HAProxy part (TTFT, removed by direct) and the
    server-side part (decode, proxy-invariant)."""
    plt = _mpl()
    cells = [("oat_8b_baseline", "proxy\n(HAProxy)", "#08519c"),
             ("oat_8b_baseline_direct", "direct\n(no proxy)", "#1b9e77")]
    fig, (axt, axd) = plt.subplots(1, 2, figsize=(13, 6))
    x = np.arange(2)  # n1, n64
    w = 0.38
    for i, (stem, label, c) in enumerate(cells):
        ttft = [stats.get((stem, n)).ttft_p50 if stats.get((stem, n)) else np.nan
                for n in (1, 64)]
        dec = [stats.get((stem, n)).decode_p50 if stats.get((stem, n)) else np.nan
               for n in (1, 64)]
        att = [stats.get((stem, n)).attainment if stats.get((stem, n)) else np.nan
               for n in (1, 64)]
        off = (i - 0.5) * w
        for ax, vals in ((axt, ttft), (axd, dec)):
            bars = ax.bar(x + off, vals, w, color=c, label=label)
            for b, v, a in zip(bars, vals, att):
                if not np.isnan(v):
                    ax.text(b.get_x() + b.get_width() / 2, v,
                            f"{v:.2f}\n(att {a:.2f})", ha="center", va="bottom",
                            fontsize=7.5)

    axt.axhline(TTFT_SLO_S, color="r", ls="--", alpha=0.7)
    axt.text(1.4, TTFT_SLO_S, "1s TTFT SLO", color="r", fontsize=8, va="bottom")
    axt.set(title="median TTFT — HAProxy term\n(direct removes it: 1.22→0.64s at n64)",
            ylabel="TTFT p50 (s)")
    axd.set(title="median decode (E2E−TTFT) — server-side term\n"
                  "(proxy-invariant: 2.30≈2.36s at n64)",
            ylabel="decode p50 (s)")
    for ax in (axt, axd):
        ax.set_xticks(x)
        ax.set_xticklabels(["N=1", "N=64"])
        ax.grid(True, axis="y", alpha=0.3)
        ax.legend()
    fig.suptitle("Discriminating test — baseline (8B, 98 rps/node, 64/64, stream): "
                 "the n64 SLO drop is HAProxy (TTFT) + server-side (decode), additive",
                 fontsize=12)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    out = OUT_DIR / "baseline_proxy_vs_direct_n64.png"
    fig.savefig(out, dpi=130)
    plt.close(fig)
    return out


def plot_nostream_trend(ns: dict[tuple[str, str, int], CellStats]) -> Path:
    """Streaming vs non-streaming trend across cluster size (rate 110, 64/64),
    haproxy + direct. 2×2: E2E p50, E2E p99, successful throughput, error rate.
    Shows the two bottlenecks: streaming overhead (stream≫non-stream everywhere)
    and the centralized-HAProxy throughput ceiling (haproxy non-stream still
    bends at 256n while direct scales)."""
    plt = _mpl()
    fig, axes = plt.subplots(2, 2, figsize=(15, 11))
    (a_e50, a_e99), (a_tp, a_err) = axes
    disp_color = {"haproxy": COLORS["haproxy"], "direct": COLORS["direct"]}
    mode_style = {"stream": ("o-", 1.0), "nostream": ("s--", 1.0)}
    stream_nodes = {"haproxy": [1, 4, 16, 64, 256], "direct": [1, 4, 16, 64, 256]}
    ns_nodes = {"haproxy": [64, 256], "direct": [64]}
    node_set = {"stream": stream_nodes, "nostream": ns_nodes}

    ideal = None
    for disp in ("haproxy", "direct"):
        for mode in ("stream", "nostream"):
            xs, e50, e99, tp, err = [], [], [], [], []
            for n in node_set[mode][disp]:
                st = ns.get((mode, disp, n))
                if st is None:
                    continue
                xs.append(n)
                e50.append(st.e2e_p50)
                e99.append(st.e2e_p99)
                tp.append(st.rps * st.success_rate)
                err.append(1.0 - st.success_rate)
            if not xs:
                continue
            sty, lw = mode_style[mode]
            c = disp_color[disp]
            lbl = f"{disp} {mode}"
            a_e50.plot(xs, e50, sty, color=c, lw=lw, label=lbl, markersize=7)
            a_e99.plot(xs, e99, sty, color=c, lw=lw, label=lbl, markersize=7)
            a_tp.plot(xs, tp, sty, color=c, lw=lw, label=lbl, markersize=7)
            a_err.plot(xs, [e * 100 for e in err], sty, color=c, lw=lw,
                       label=lbl, markersize=7)
            if disp == "direct" and mode == "stream":
                base = ns.get(("stream", "direct", 1))
                if base:
                    ideal = [(n, base.rps * base.success_rate * n)
                             for n in [1, 4, 16, 64, 256]]
    if ideal:
        a_tp.plot([n for n, _ in ideal], [v for _, v in ideal], "k:",
                  alpha=0.5, label="ideal linear")

    a_e50.set(title="E2E latency p50 vs N", ylabel="E2E p50 (s)", yscale="log")
    a_e99.set(title="E2E latency p99 vs N (the tail)", ylabel="E2E p99 (s)", yscale="log")
    a_tp.set(title="successful throughput vs N", ylabel="successful rps", yscale="log")
    a_err.set(title="error rate vs N", ylabel="errors (%)")
    for ax in (a_e50, a_e99, a_tp, a_err):
        ax.set_xscale("log", base=2)
        ax.set_xticks([1, 4, 16, 64, 256])
        ax.set_xticklabels(["1", "4", "16", "64", "256"])
        ax.set_xlabel("nodes (= replicas, TP=1)")
        ax.grid(True, which="both", alpha=0.3)
        ax.legend(fontsize=8)
    fig.suptitle("Streaming vs non-streaming trend (8B, rate 110, 64/64) — "
                 "stream overhead at all N + centralized-HAProxy ceiling at 256n",
                 fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    out = OUT_DIR / "stream_vs_nostream_trend.png"
    fig.savefig(out, dpi=130)
    plt.close(fig)
    return out


# --- driver -----------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--refresh", action="store_true", help="rebuild the cell cache")
    p.add_argument("--only", choices=["set1", "set2", "cdf", "disc", "nostream"],
                   default=None)
    args = p.parse_args(argv)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    stats: dict[tuple[str, int], CellStats] = {}
    ns_stats: dict[tuple[str, str, int], CellStats] = {}

    want_set1 = args.only in (None, "set1", "cdf")
    want_set2 = args.only in (None, "set2")
    want_disc = args.only in (None, "disc")
    want_nostream = args.only in (None, "nostream")

    # Set 1 cells (keep per-request arrays for the n64 CDF). 256n for direct+haproxy.
    if want_set1:
        for proxy in PROXIES:
            for n in proxy_nodes(proxy):
                st = extract_cell(proxy_stem(proxy, n), n,
                                  keep_arrays=(n == 64), refresh=args.refresh)
                if st is None:
                    print(f"  ! missing cell: {proxy} n{n}", file=sys.stderr)
                    continue
                stats[(proxy, n)] = st
                print(f"  {proxy:9s} n{n:<4d} rps={st.rps:9.1f} "
                      f"attain={st.attainment:.3f} good={st.goodput:9.1f} "
                      f"ok={st.success_rate:.0%}")

    # Discriminating test: baseline proxy vs direct at n1/n64.
    if want_disc:
        for stem in ("oat_8b_baseline", "oat_8b_baseline_direct"):
            for n in (1, 64):
                st = extract_cell(stem, n, keep_arrays=False, refresh=args.refresh)
                if st is None:
                    print(f"  ! missing cell: {stem} n{n}", file=sys.stderr)
                    continue
                stats[(stem, n)] = st
                print(f"  {stem:24s} n{n:<3d} ttft_p50={st.ttft_p50:.3f} "
                      f"decode_p50={st.decode_p50:.3f} attain={st.attainment:.3f}")

    # Streaming-vs-non-streaming trend (rate 110): stream sweep + non-stream cells.
    if want_nostream:
        plan = {
            "stream": {"haproxy": [1, 4, 16, 64, 256], "direct": [1, 4, 16, 64, 256]},
            "nostream": {"haproxy": [64, 256], "direct": [64]},
        }
        for mode, byd in plan.items():
            for disp, nodes in byd.items():
                for n in nodes:
                    if mode == "stream":
                        stem = proxy_stem(disp, n)
                    elif disp == "haproxy" and n == 256:
                        # corrected (client=4) non-stream 256n cell; the plain
                        # proxycmp_haproxy_nostream n256 is the CONFOUNDED client=256 run.
                        stem = "proxycmp_haproxy_nostream_c4_256"
                    else:
                        stem = f"proxycmp_{disp}_nostream"
                    st = extract_cell(stem, n, keep_arrays=False, refresh=args.refresh)
                    if st is None:
                        print(f"  ! missing: {mode} {disp} n{n}", file=sys.stderr)
                        continue
                    ns_stats[(mode, disp, n)] = st
                    print(f"  {mode:8s} {disp:8s} n{n:<4d} "
                          f"rps={st.rps:8.0f} ok={st.success_rate:.0%} "
                          f"e2e_p50={st.e2e_p50:7.3f} e2e_p99={st.e2e_p99:8.3f}")

    if want_set2:
        for stem, _, _ in OAT_CELLS:
            for n in OAT_NODES:
                st = extract_cell(stem, n, keep_arrays=False, refresh=args.refresh)
                if st is None:
                    print(f"  ! missing cell: {stem} n{n}", file=sys.stderr)
                    continue
                stats[(stem, n)] = st
                print(f"  {stem:18s} n{n:<3d} rps={st.rps:9.1f} "
                      f"attain={st.attainment:.3f} good={st.goodput:9.1f} "
                      f"ok={st.success_rate:.0%}")

    outs = []
    if args.only in (None, "set1"):
        outs.append(plot_set1(stats))
    if args.only in (None, "cdf"):
        outs.append(plot_cdf(stats))
    if args.only in (None, "disc"):
        outs.append(plot_disc(stats))
    if args.only in (None, "nostream"):
        outs.append(plot_nostream_trend(ns_stats))
    if args.only in (None, "set2"):
        outs.append(plot_set2(stats))
    print("\nWrote:")
    for o in outs:
        print(f"  {o}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
