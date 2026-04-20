#!/usr/bin/env python3
"""Compare HAProxy (up to 4 MPI clients), Direct-MPI, and LiteLLM dispatch modes.

Styling mirrors eval/plot/weakscaling.py and eval/plot/litellm_scaling.py:
modern seaborn palette, boxed point annotations, titled subtitle block,
log-log axes with ScalarFormatter, and GPU-worker-count tick labels.

Throughput panel shows:
- Solid line: RPS excluding errored requests (real achieved throughput)
- Dashed line: RPS including errored requests (same as overall.rps)
- TPS value text-annotated alongside each solid-line point.

Raw request-level JSON is expensive to re-parse; per-result-dir processed
summaries are cached to <results-dir>/processed.json keyed on source mtime.
"""
import os
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"
os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib-claude")

import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter

try:
    plt.style.use("seaborn-v0_8-darkgrid")
except OSError:
    try:
        plt.style.use("seaborn-darkgrid")
    except OSError:
        pass

RUNS_ROOT = "/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs"
NODE_COUNTS = [1, 2, 4, 8, 16, 32, 64, 128, 256]
TARGET_RPS_PER_NODE = 110.0
GPUS_PER_NODE = 12
PROCESSED_SCHEMA_VERSION = 2

SPECS: dict[str, list[tuple[str, str]]] = {
    "HAProxy (up to 4 MPI clients, 1 LB)": [
        ("weakscaling_haproxy_mpiclient_inst_v3", "run2"),
        ("weakscaling_haproxy_mpiclient_v3", "run2"),
    ],
    "Direct-MPI (1 client/node)": [("weakscaling_direct_short_v3", "run1")],
    "LiteLLM (8 workers on head)": [("weakscaling_litellm_short_v3", "run0")],
}

SERIES_STYLE = {
    "HAProxy (up to 4 MPI clients, 1 LB)": {"color": "#2E86AB", "marker": "o"},
    "Direct-MPI (1 client/node)":          {"color": "#A23B72", "marker": "s"},
    "LiteLLM (8 workers on head)":         {"color": "#E67E22", "marker": "D"},
}

PLOT_TITLE = "Weak-Scaling v3 — HAProxy vs Direct-MPI vs LiteLLM (ALCF Aurora)"
PLOT_SUBTITLE = (
    "Llama-3-8B, 64-tok in / 64-tok out, {gpus} GPUs per node, 4 runs mean\n"
    "Client: {rpn:g} RPS/node, 60 sec"
).format(gpus=GPUS_PER_NODE, rpn=TARGET_RPS_PER_NODE)


# ───────────────────────── processing + caching ────────────────────────

def _latest_result_file(results_dir: Path) -> Path | None:
    candidates = sorted(results_dir.glob("result*.json"))
    return candidates[-1] if candidates else None


def _compute_processed(result_path: Path) -> dict:
    """Parse a result*.json and derive error-excluded metrics."""
    with open(result_path) as f:
        data = json.load(f)
    overall = data.get("overall", {})
    requests = data.get("requests", [])

    total_reqs = len(requests)
    successful_reqs = 0
    total_tokens = 0
    successful_tokens = 0
    for r in requests:
        tok = int(r.get("actual_prompt_tokens", 0) or 0) + int(r.get("actual_completion_tokens", 0) or 0)
        total_tokens += tok
        if r.get("success"):
            successful_reqs += 1
            successful_tokens += tok

    rps = float(overall.get("rps", 0))
    tps = float(overall.get("tps", 0))
    errors = int(overall.get("errors", 0))

    # Scale overall.rps / overall.tps by the success fraction observed in the
    # request list. This keeps the "with-errors" number equal to overall.rps
    # (source of truth) while deriving a consistent error-excluded version.
    req_frac = (successful_reqs / total_reqs) if total_reqs else 1.0
    tok_frac = (successful_tokens / total_tokens) if total_tokens else 1.0
    rps_ok = rps * req_frac
    tps_ok = tps * tok_frac

    # overall.errors is unreliable (misses client-side timeouts); failures
    # derived from the request list's success flag are the source of truth.
    failures_from_list = total_reqs - successful_reqs

    return {
        "schema_version": PROCESSED_SCHEMA_VERSION,
        "source_result": result_path.name,
        "source_mtime": result_path.stat().st_mtime,
        "duration_s": float(overall.get("duration_s", 0)),
        "rps": rps,
        "tps": tps,
        "rps_ok": rps_ok,
        "tps_ok": tps_ok,
        "total_requests_in_list": total_reqs,
        "successful_requests_in_list": successful_reqs,
        "failures_in_list": failures_from_list,
        "total_tokens_in_list": total_tokens,
        "successful_tokens_in_list": successful_tokens,
        "requests_completed": int(overall.get("requests_completed", 0)),
        "errors_overall": errors,
        "p50_s": float(overall.get("p50_s", 0)),
        "p99_s": float(overall.get("p99_s", 0)),
    }


def get_processed(results_dir: Path) -> dict | None:
    """Return processed summary for the latest result*.json in results_dir.

    Caches to <results_dir>/processed.json keyed on the source file's mtime
    and schema version. Reads the cache on subsequent calls.
    """
    latest = _latest_result_file(results_dir)
    if latest is None:
        return None

    cache = results_dir / "processed.json"
    if cache.exists():
        try:
            with open(cache) as f:
                cached = json.load(f)
            if (
                cached.get("schema_version") == PROCESSED_SCHEMA_VERSION
                and cached.get("source_result") == latest.name
                and abs(cached.get("source_mtime", 0) - latest.stat().st_mtime) < 1.0
            ):
                return cached
        except (json.JSONDecodeError, OSError):
            pass

    print(f"  processing {latest} ...")
    processed = _compute_processed(latest)
    try:
        with open(cache, "w") as f:
            json.dump(processed, f, indent=2)
    except OSError as e:
        print(f"  warning: failed to write cache {cache}: {e}")
    return processed


def load_one(sources: list[tuple[str, str]]) -> list[tuple[int, dict]]:
    """Return list of (nodes, processed_dict); first source per node wins."""
    rows = []
    for n in NODE_COUNTS:
        for spec_name, run_group in sources:
            results_dir = Path(RUNS_ROOT) / spec_name / run_group / f"{n}-nodes" / "results"
            if not results_dir.exists():
                continue
            p = get_processed(results_dir)
            if p is not None:
                rows.append((n, p))
                break
    return rows


# ───────────────────────── plotting ────────────────────────────────────

def _style_axes(ax, xticks, log_y=True):
    ax.set_facecolor("#FAFAFA")
    ax.grid(True, alpha=0.35, linestyle="--", linewidth=0.8, color="#CCCCCC")
    ax.set_axisbelow(True)
    ax.set_xscale("log", base=2)
    if log_y:
        ax.set_yscale("log")
        formatter = ScalarFormatter()
        formatter.set_scientific(False)
        ax.yaxis.set_major_formatter(formatter)
        ax.yaxis.set_minor_formatter(formatter)
    ax.set_xticks(xticks)
    ax.set_xticklabels(
        [f"{n}\n({n * GPUS_PER_NODE} workers)" for n in xticks],
        fontsize=9,
    )
    ax.tick_params(axis="x", labelsize=9, colors="#333333")
    ax.tick_params(axis="y", labelsize=10, colors="#333333")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    for spine in ax.spines.values():
        spine.set_edgecolor("#DDDDDD")
        spine.set_linewidth(1.1)


def _annotate(ax, xs, ys, color, fmt="{:.0f}", xytext=(0, 12), fontsize=8):
    for x, y in zip(xs, ys):
        ax.annotate(
            fmt.format(y),
            (x, y),
            textcoords="offset points",
            xytext=xytext,
            ha="center",
            fontsize=fontsize,
            fontweight="bold",
            color=color,
            bbox=dict(
                boxstyle="round,pad=0.2",
                facecolor="white",
                edgecolor=color,
                linewidth=1.0,
                alpha=0.85,
            ),
        )


def _fmt_tps(v: float) -> str:
    if v >= 1e6:
        return f"{v/1e6:.2f}M tok/s"
    if v >= 1e3:
        return f"{v/1e3:.0f}k tok/s"
    return f"{v:.0f} tok/s"


def main() -> int:
    fig, axes = plt.subplots(3, 1, figsize=(12, 18))
    ax_rps, ax_eff, ax_lat = axes
    fig.patch.set_facecolor("white")

    for ax in axes:
        _style_axes(ax, NODE_COUNTS, log_y=True)

    ax_eff.set_yscale("linear")
    ax_eff.yaxis.set_major_formatter(ScalarFormatter())
    ax_eff.set_ylim(0, 110)

    # Ideal-scaling reference lines
    ideal_x = NODE_COUNTS
    ideal_y = [TARGET_RPS_PER_NODE * n for n in ideal_x]
    ax_rps.plot(
        ideal_x, ideal_y, linestyle=":", color="#7A7A7A", linewidth=1.8,
        alpha=0.8, label=f"Ideal ({TARGET_RPS_PER_NODE:g} RPS/node)", zorder=2,
    )
    ax_eff.axhline(
        100, color="#7A7A7A", linestyle="--", linewidth=1.8, alpha=0.8,
        label="100% efficiency", zorder=2,
    )

    for label, sources in SPECS.items():
        rows = load_one(sources)
        if not rows:
            print(f"  {label}: NO DATA")
            continue

        nodes = [n for n, _ in rows]
        rps_err = [p["rps"] for _, p in rows]                 # with errors
        rps_ok  = [p["rps_ok"] for _, p in rows]              # without errors
        tps_ok  = [p["tps_ok"] for _, p in rows]
        p50_ms  = [p["p50_s"] * 1000 for _, p in rows]
        failures = [p.get("failures_in_list", p.get("errors_overall", 0)) for _, p in rows]
        # Efficiency uses the error-excluded RPS (real sustained throughput)
        eff = [r / (TARGET_RPS_PER_NODE * n) * 100 for n, r in zip(nodes, rps_ok)]

        print(f"  {label}: {len(rows)} points")
        for n, r_err, r_ok, t_ok, p, e, ef in zip(
            nodes, rps_err, rps_ok, tps_ok, p50_ms, failures, eff
        ):
            print(f"    {n:4d} nodes  rps_ok={r_ok:8.1f}  rps_err={r_err:8.1f}  "
                  f"tps_ok={t_ok:10.1f}  eff={ef:5.1f}%  p50={p:6.1f}ms  failures={e}")

        style = SERIES_STYLE[label]
        kw_solid = dict(
            color=style["color"], marker=style["marker"],
            linewidth=2.6, markersize=9,
            markerfacecolor=style["color"], markeredgecolor="white",
            markeredgewidth=1.8, alpha=0.95, zorder=4,
        )
        kw_dashed = dict(
            color=style["color"], marker=style["marker"],
            linestyle="--", linewidth=2.0, markersize=7,
            markerfacecolor="white", markeredgecolor=style["color"],
            markeredgewidth=1.8, alpha=0.9, zorder=3,
        )

        # Throughput panel: solid (without errors) + dashed (with errors)
        ax_rps.plot(nodes, rps_ok, label=f"{label} — RPS (ok)", **kw_solid)
        ax_rps.plot(nodes, rps_err, label=f"{label} — RPS (incl. errors)", **kw_dashed)

        # Efficiency + latency panels use the single solid series
        ax_eff.plot(nodes, eff, label=label, **kw_solid)
        ax_lat.plot(nodes, p50_ms, label=label, **kw_solid)

        # Annotate solid RPS points with RPS value (above) and TPS value (below)
        _annotate(ax_rps, nodes, rps_ok, style["color"],
                  fmt="{:.0f}", xytext=(0, 12), fontsize=8)
        for x, y, t in zip(nodes, rps_ok, tps_ok):
            ax_rps.annotate(
                _fmt_tps(t),
                (x, y),
                textcoords="offset points",
                xytext=(0, -18),
                ha="center",
                fontsize=7,
                fontweight="bold",
                color=style["color"],
                bbox=dict(
                    boxstyle="round,pad=0.2",
                    facecolor="#FFF8E7",
                    edgecolor=style["color"],
                    linewidth=1.0,
                    alpha=0.9,
                ),
            )

        _annotate(ax_eff, nodes, eff, style["color"],
                  fmt="{:.0f}%", xytext=(0, 10))
        _annotate(ax_lat, nodes, p50_ms, style["color"],
                  fmt="{:.0f}", xytext=(0, 10))

    # Panel titles and axis labels
    ax_rps.set_xlabel("Number of Nodes", fontsize=11, fontweight="bold", color="#333333")
    ax_rps.set_ylabel("Requests per Second (RPS)", fontsize=11, fontweight="bold", color="#333333")
    ax_rps.set_title(
        "Throughput — RPS without errors (solid) vs RPS with errors (dashed); TPS annotated",
        fontsize=12, fontweight="bold", color="#1A1A1A",
    )

    ax_eff.set_xlabel("Number of Nodes", fontsize=11, fontweight="bold", color="#333333")
    ax_eff.set_ylabel("Scaling Efficiency (%)", fontsize=11, fontweight="bold", color="#333333")
    ax_eff.set_title(
        "Efficiency (RPS without errors ÷ ideal)",
        fontsize=12, fontweight="bold", color="#1A1A1A",
    )

    ax_lat.set_xlabel("Number of Nodes", fontsize=11, fontweight="bold", color="#333333")
    ax_lat.set_ylabel("p50 Latency (ms, log)", fontsize=11, fontweight="bold", color="#333333")
    ax_lat.set_title("Per-Request Latency (p50)", fontsize=12, fontweight="bold", color="#1A1A1A")

    for ax, loc, ncol in [(ax_rps, "upper left", 2),
                          (ax_eff, "lower left", 1),
                          (ax_lat, "upper left", 1)]:
        legend = ax.legend(
            loc=loc, fontsize=9,
            frameon=True, fancybox=True, shadow=True,
            framealpha=0.95, edgecolor="#CCCCCC", facecolor="white",
            ncol=ncol,
        )
        legend.get_frame().set_linewidth(1.2)

    fig.suptitle(PLOT_TITLE, fontsize=16, fontweight="bold", color="#1A1A1A", y=0.995)
    fig.text(
        0.5, 0.945, PLOT_SUBTITLE,
        ha="center", va="top", fontsize=11, color="#666666", style="italic",
    )

    fig.tight_layout(rect=[0, 0, 1, 0.92])

    out = sys.argv[1] if len(sys.argv) > 1 else "/home/wenyiw/aurora_rayserver/findings/weakscaling_three_dispatch_v3.png"
    fig.savefig(out, dpi=200, bbox_inches="tight", facecolor="white", edgecolor="none")
    plt.close(fig)
    print(f"\nSaved: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
