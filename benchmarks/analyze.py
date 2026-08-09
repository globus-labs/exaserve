"""
Benchmark result analyzer and visualizer.

Reads JSON output files produced by bench_client.py and bench_proxy.py and
generates plots + summary tables.

Supported plots (--plots):
  For client results (bench_client.py output):
    throughput      -- actual RPS vs num_workers, one line per payload size
    dispatch        -- dispatch delay p99 heatmap (workers x rps)
    latency         -- latency CDF / violin per payload size
    keepup          -- pass/fail matrix: can the client keep up?
    ports           -- port usage over time (if port_monitor data present)

  For proxy results (bench_proxy.py output):
    proxy_throughput-- proxy actual RPS vs LiteLLM workers, curves per routing
    proxy_latency   -- proxy p99 latency vs RPS, curves per routing strategy
    ramp            -- ramp test: p99 latency and error% vs RPS (breaking point)
    proxy_keepup    -- pass/fail matrix: litellm_workers x RPS

Usage:
  python analyze.py --input results/client_sweep.json --plots throughput,dispatch,keepup
  python analyze.py --input results/proxy_sweep.json  --plots proxy_throughput,ramp
  python analyze.py --input results/client_sweep.json --plots all
"""

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Optional

import numpy as np

try:
    import matplotlib

    matplotlib.use("Agg")  # non-interactive backend (saves files, doesn't need display)
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors
    from matplotlib.patches import Patch

    _MPL_AVAILABLE = True
except ImportError:
    _MPL_AVAILABLE = False
    print("WARNING: matplotlib not found. Table output only.", file=sys.stderr)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

DISPATCH_THRESHOLD_S = 0.1  # 100 ms


def _load(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def _out_path(input_path: str, suffix: str, out_dir: Optional[str] = None) -> str:
    stem = Path(input_path).stem
    base = Path(out_dir) if out_dir else Path(input_path).parent
    base.mkdir(parents=True, exist_ok=True)
    return str(base / f"{stem}_{suffix}.png")


def _save_fig(fig, path: str):
    fig.savefig(path, bbox_inches="tight", dpi=150)
    plt.close(fig)
    print(f"  Saved: {path}")


# ---------------------------------------------------------------------------
# ---- CLIENT PLOTS ----------------------------------------------------------
# ---------------------------------------------------------------------------


def plot_throughput(data: dict, input_path: str, out_dir: Optional[str]):
    """Actual RPS vs num_workers, one curve per payload size."""
    if not _MPL_AVAILABLE:
        return
    results = data.get("results", [])
    if not results:
        print("  [throughput] No results found.")
        return

    payload_sizes = sorted(set(r["payload_size"] for r in results))
    worker_counts = sorted(set(r["num_workers"] for r in results))

    fig, ax = plt.subplots(figsize=(8, 5))
    colors = plt.cm.tab10.colors

    for i, ps in enumerate(payload_sizes):
        pts = [(r["num_workers"], r["actual_rps"]) for r in results if r["payload_size"] == ps]
        pts.sort()
        if pts:
            ws, rpss = zip(*pts)
            ax.plot(ws, rpss, marker="o", label=ps, color=colors[i % len(colors)])

    ax.set_xlabel("Number of Workers")
    ax.set_ylabel("Actual RPS")
    ax.set_title("Client Throughput vs. Workers (by Payload Size)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.set_xticks(worker_counts)
    path = _out_path(input_path, "throughput", out_dir)
    _save_fig(fig, path)


def plot_dispatch(data: dict, input_path: str, out_dir: Optional[str]):
    """Dispatch delay p99 heatmap: workers (y) x target_rps (x)."""
    if not _MPL_AVAILABLE:
        return
    results = data.get("results", [])
    if not results:
        print("  [dispatch] No results found.")
        return

    workers = sorted(set(r["num_workers"] for r in results))
    rpss = sorted(set(r["target_rps"] for r in results))

    grid = np.full((len(workers), len(rpss)), np.nan)
    for r in results:
        wi = workers.index(r["num_workers"])
        ri = rpss.index(r["target_rps"])
        # Support both old per-request p99 (ms) and new aggregate overhead (s)
        v = r.get("dispatch_overhead_s")
        if v is None:
            v = r.get("dispatch_delay_p99_s")
        if v is not None:
            grid[wi, ri] = v * 1000  # store in ms for display

    fig, ax = plt.subplots(figsize=(max(6, len(rpss)), max(4, len(workers))))
    cmap = plt.cm.RdYlGn_r
    im = ax.imshow(
        grid,
        cmap=cmap,
        aspect="auto",
        vmin=0,
        vmax=max(
            DISPATCH_THRESHOLD_S * 1000 * 2, np.nanmax(grid) if not np.all(np.isnan(grid)) else 200
        ),
    )

    ax.set_xticks(range(len(rpss)))
    ax.set_xticklabels([str(int(r)) for r in rpss], rotation=45)
    ax.set_yticks(range(len(workers)))
    ax.set_yticklabels([str(w) for w in workers])
    ax.set_xlabel("Target RPS")
    ax.set_ylabel("Workers")
    ax.set_title(f"Dispatch Overhead (ms) — threshold={DISPATCH_THRESHOLD_S * 1000:.0f}ms")

    for wi in range(len(workers)):
        for ri in range(len(rpss)):
            v = grid[wi, ri]
            if not np.isnan(v):
                color = "white" if v > DISPATCH_THRESHOLD_S * 1000 else "black"
                ax.text(ri, wi, f"{v:.0f}", ha="center", va="center", fontsize=8, color=color)

    fig.colorbar(im, ax=ax, label="Dispatch overhead (ms)")
    path = _out_path(input_path, "dispatch_heatmap", out_dir)
    _save_fig(fig, path)


def plot_keepup(data: dict, input_path: str, out_dir: Optional[str]):
    """Pass/fail matrix: can_keep_up for (workers x rps)."""
    if not _MPL_AVAILABLE:
        return
    results = data.get("results", [])
    if not results:
        print("  [keepup] No results found.")
        return

    workers = sorted(set(r["num_workers"] for r in results))
    rpss = sorted(set(r["target_rps"] for r in results))

    # Values: 1.0 = YES, 0.0 = NO, -1.0 = SKIP (timed out)
    grid = np.full((len(workers), len(rpss)), np.nan)
    for r in results:
        wi = workers.index(r["num_workers"])
        ri = rpss.index(r["target_rps"])
        if r.get("timed_out"):
            grid[wi, ri] = -1.0
        else:
            grid[wi, ri] = 1.0 if r.get("can_keep_up") else 0.0

    fig, ax = plt.subplots(figsize=(max(6, len(rpss)), max(4, len(workers))))
    cmap = mcolors.ListedColormap(
        ["#aaaaaa", "#d73027", "#1a9850"]
    )  # grey=skip, red=fail, green=pass
    bounds = [-1.5, -0.5, 0.5, 1.5]
    norm = mcolors.BoundaryNorm(bounds, cmap.N)
    ax.imshow(grid, cmap=cmap, norm=norm, aspect="auto")

    ax.set_xticks(range(len(rpss)))
    ax.set_xticklabels([str(int(r)) for r in rpss], rotation=45)
    ax.set_yticks(range(len(workers)))
    ax.set_yticklabels([str(w) for w in workers])
    ax.set_xlabel("Target RPS")
    ax.set_ylabel("Workers")
    ax.set_title("Can Client Keep Up? (green=YES, red=NO, grey=SKIP)")

    labels = {1.0: "YES", 0.0: "NO", -1.0: "SKIP"}
    for wi in range(len(workers)):
        for ri in range(len(rpss)):
            v = grid[wi, ri]
            if not np.isnan(v):
                ax.text(
                    ri,
                    wi,
                    labels.get(v, "?"),
                    ha="center",
                    va="center",
                    fontsize=9,
                    color="white",
                    fontweight="bold",
                )

    legend_elements = [
        Patch(facecolor="#1a9850", label="Can keep up"),
        Patch(facecolor="#d73027", label="Falling behind"),
        Patch(facecolor="#aaaaaa", label="Timed out (SKIP)"),
    ]
    ax.legend(handles=legend_elements, loc="upper right")

    path = _out_path(input_path, "keepup_matrix", out_dir)
    _save_fig(fig, path)


def plot_latency_cdf(data: dict, input_path: str, out_dir: Optional[str]):
    """Latency p50/p95/p99 bar chart grouped by payload size."""
    if not _MPL_AVAILABLE:
        return
    results = data.get("results", [])
    if not results:
        print("  [latency] No results found.")
        return

    payload_sizes = sorted(set(r["payload_size"] for r in results))
    pcts = ["p50", "p95", "p99"]
    keys = ["latency_p50_s", "latency_p95_s", "latency_p99_s"]
    colors = ["#2196F3", "#FF9800", "#F44336"]

    # Average latencies per payload size
    data_by_payload = {}
    for ps in payload_sizes:
        sub = [r for r in results if r["payload_size"] == ps]
        data_by_payload[ps] = {}
        for k in keys:
            vals = [r[k] for r in sub if r.get(k) is not None]
            data_by_payload[ps][k] = float(np.mean(vals)) if vals else 0

    x = np.arange(len(payload_sizes))
    width = 0.25
    fig, ax = plt.subplots(figsize=(8, 5))
    for i, (k, label, color) in enumerate(zip(keys, pcts, colors)):
        vals = [data_by_payload[ps][k] for ps in payload_sizes]
        ax.bar(x + i * width, vals, width, label=label, color=color, alpha=0.85)

    ax.set_xlabel("Payload Size")
    ax.set_ylabel("Latency (s)")
    ax.set_title("Client-Measured Latency by Payload Size (mean across workers/rps)")
    ax.set_xticks(x + width)
    ax.set_xticklabels(payload_sizes)
    ax.legend()
    ax.grid(True, alpha=0.3, axis="y")

    path = _out_path(input_path, "latency_by_payload", out_dir)
    _save_fig(fig, path)


# ---------------------------------------------------------------------------
# ---- PROXY PLOTS -----------------------------------------------------------
# ---------------------------------------------------------------------------


def plot_proxy_throughput(data: dict, input_path: str, out_dir: Optional[str]):
    """Proxy actual RPS vs LiteLLM workers, one curve per routing strategy."""
    if not _MPL_AVAILABLE:
        return
    results = [r for r in data.get("results", []) if r.get("test_type") == "fixed"]
    if not results:
        print("  [proxy_throughput] No fixed-rate results found.")
        return

    routing_strategies = sorted(set(r["routing"] for r in results))
    colors = plt.cm.tab10.colors

    fig, ax = plt.subplots(figsize=(8, 5))
    for i, routing in enumerate(routing_strategies):
        pts = [(r["litellm_workers"], r["actual_rps"]) for r in results if r["routing"] == routing]
        pts.sort()
        if pts:
            ws, rpss = zip(*pts)
            ax.plot(ws, rpss, marker="o", label=routing, color=colors[i % len(colors)])

    ax.set_xlabel("LiteLLM Workers")
    ax.set_ylabel("Actual RPS (through proxy)")
    ax.set_title("Proxy Throughput vs. LiteLLM Workers (by routing strategy)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    path = _out_path(input_path, "proxy_throughput", out_dir)
    _save_fig(fig, path)


def plot_proxy_latency(data: dict, input_path: str, out_dir: Optional[str]):
    """Proxy p99 latency vs target_rps, one curve per routing strategy."""
    if not _MPL_AVAILABLE:
        return
    results = [r for r in data.get("results", []) if r.get("test_type") == "fixed"]
    if not results:
        print("  [proxy_latency] No fixed-rate results found.")
        return

    routing_strategies = sorted(set(r["routing"] for r in results))
    colors = plt.cm.tab10.colors

    fig, ax = plt.subplots(figsize=(8, 5))
    for i, routing in enumerate(routing_strategies):
        pts = [
            (r["target_rps"], r.get("latency_p99_s") or 0)
            for r in results
            if r["routing"] == routing
        ]
        pts.sort()
        if pts:
            rpss, lats = zip(*pts)
            ax.plot(rpss, lats, marker="o", label=routing, color=colors[i % len(colors)])

    ax.set_xlabel("Target RPS")
    ax.set_ylabel("P99 Latency (s)")
    ax.set_title("Proxy P99 Latency vs. RPS (by routing strategy)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    path = _out_path(input_path, "proxy_latency", out_dir)
    _save_fig(fig, path)


def plot_ramp(data: dict, input_path: str, out_dir: Optional[str]):
    """Ramp test: p99 latency and error% vs RPS, showing the breaking point."""
    if not _MPL_AVAILABLE:
        return
    ramp_results = [r for r in data.get("results", []) if r.get("test_type") == "ramp"]
    if not ramp_results:
        print("  [ramp] No ramp results found.")
        return

    for run in ramp_results:
        steps = run.get("ramp_steps", [])
        if not steps:
            continue
        label = (
            f"lw={run['litellm_workers']} routing={run['routing']} backends={run['num_backends']}"
        )

        rpss = [s["target_rps"] for s in steps]
        p99s = [s.get("latency_p99_s") or 0 for s in steps]
        errs = [s.get("error_fraction", 0) * 100 for s in steps]
        statuses = [s.get("ramp_status", "OK") for s in steps]

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(9, 7), sharex=True)

        ax1.plot(rpss, p99s, marker="o", color="#2196F3", label="P99 latency (s)")
        ax1.axhline(2.0, color="red", linestyle="--", alpha=0.6, label="Degradation threshold (2s)")
        for idx, (rps, status) in enumerate(zip(rpss, statuses)):
            if status != "OK":
                ax1.axvline(rps, color="red", linestyle=":", alpha=0.8)
                ax1.text(rps, max(p99s) * 0.9, f"  FAIL\n  @{rps:.0f}", color="red", fontsize=8)
                break
        ax1.set_ylabel("P99 Latency (s)")
        ax1.set_title(f"Ramp Test — {label}")
        ax1.legend()
        ax1.grid(True, alpha=0.3)

        ax2.bar(rpss, errs, color="#F44336", alpha=0.75, label="Error %")
        ax2.axhline(5.0, color="orange", linestyle="--", alpha=0.6, label="5% error threshold")
        ax2.set_xlabel("Target RPS")
        ax2.set_ylabel("Error Rate (%)")
        ax2.legend()
        ax2.grid(True, alpha=0.3, axis="y")

        out_label = f"ramp_lw{run['litellm_workers']}_{run['routing']}_be{run['num_backends']}"
        path = _out_path(input_path, out_label, out_dir)
        _save_fig(fig, path)


def plot_proxy_keepup(data: dict, input_path: str, out_dir: Optional[str]):
    """Pass/fail matrix: litellm_workers x target_rps."""
    if not _MPL_AVAILABLE:
        return
    results = [r for r in data.get("results", []) if r.get("test_type") == "fixed"]
    if not results:
        print("  [proxy_keepup] No fixed-rate results found.")
        return

    workers = sorted(set(r["litellm_workers"] for r in results))
    rpss = sorted(set(r["target_rps"] for r in results))
    grid = np.full((len(workers), len(rpss)), np.nan)

    for r in results:
        wi = workers.index(r["litellm_workers"])
        ri = rpss.index(r["target_rps"])
        ef = r.get("error_fraction", 1.0)
        p99 = r.get("latency_p99_s")
        ok = ef < 0.05 and (p99 is None or p99 < 2.0)
        grid[wi, ri] = 1.0 if ok else 0.0

    fig, ax = plt.subplots(figsize=(max(6, len(rpss)), max(4, len(workers))))
    cmap = mcolors.ListedColormap(["#d73027", "#1a9850"])
    bounds = [-0.5, 0.5, 1.5]
    norm = mcolors.BoundaryNorm(bounds, cmap.N)
    ax.imshow(grid, cmap=cmap, norm=norm, aspect="auto")

    ax.set_xticks(range(len(rpss)))
    ax.set_xticklabels([str(int(r)) for r in rpss], rotation=45)
    ax.set_yticks(range(len(workers)))
    ax.set_yticklabels([str(w) for w in workers])
    ax.set_xlabel("Target RPS")
    ax.set_ylabel("LiteLLM Workers")
    ax.set_title("Proxy: Can it handle the load? (green=YES, red=NO)")

    for wi in range(len(workers)):
        for ri in range(len(rpss)):
            v = grid[wi, ri]
            if not np.isnan(v):
                ax.text(
                    ri,
                    wi,
                    "YES" if v == 1 else "NO",
                    ha="center",
                    va="center",
                    fontsize=9,
                    color="white",
                    fontweight="bold",
                )

    legend_elements = [
        Patch(facecolor="#1a9850", label="OK (err<5%, p99<2s)"),
        Patch(facecolor="#d73027", label="Degraded"),
    ]
    ax.legend(handles=legend_elements, loc="upper right")

    path = _out_path(input_path, "proxy_keepup_matrix", out_dir)
    _save_fig(fig, path)


# ---------------------------------------------------------------------------
# Summary tables
# ---------------------------------------------------------------------------


def print_client_table(data: dict):
    results = data.get("results", [])
    if not results:
        print("No results.")
        return
    header = (
        f"{'workers':>7} {'rps_tgt':>8} {'rps_act':>8} {'payload':>8} "
        f"{'ovhd_s':>7} {'lat_p50s':>9} {'lat_p99s':>9} {'errors':>6} {'status':>7}"
    )
    print("\n" + "=" * len(header))
    print("CLIENT BENCHMARK SUMMARY")
    print("=" * len(header))
    print(header)
    print("-" * len(header))
    for r in results:
        # Support both old (dispatch_delay_p99_s) and new (dispatch_overhead_s) result formats
        ovhd = r.get("dispatch_overhead_s")
        if ovhd is None and r.get("dispatch_delay_p99_s") is not None:
            ovhd = r["dispatch_delay_p99_s"]  # fall back to per-request p99 as approximation
        ovhd_str = f"{ovhd:.2f}" if ovhd is not None else "N/A"
        l50 = f"{r['latency_p50_s']:.3f}" if r.get("latency_p50_s") is not None else "N/A"
        l99 = f"{r['latency_p99_s']:.3f}" if r.get("latency_p99_s") is not None else "N/A"
        if r.get("timed_out"):
            ok = "SKIP"
        elif r.get("can_keep_up"):
            ok = "YES"
        else:
            ok = "NO"
        nw = r.get("num_workers", "?")
        trps = r.get("target_rps", "?")
        arps = r.get("actual_rps")
        arps_str = f"{arps:.1f}" if isinstance(arps, float) else str(arps or "?")
        ps = r.get("payload_size", "?")
        errs = r.get("errors", r.get("failures", "?"))
        print(
            f"{nw:>7} {trps:>8} {arps_str:>8} {ps:>8} "
            f"{ovhd_str:>7} {l50:>9} {l99:>9} {errs:>6} {ok:>7}"
        )
    print("=" * len(header) + "\n")


def print_proxy_table(data: dict):
    results = data.get("results", [])
    if not results:
        print("No results.")
        return
    header = (
        f"{'lw':>4} {'routing':>20} {'backends':>8} {'rps_tgt':>8} "
        f"{'rps_act':>8} {'p99_s':>7} {'err%':>6} {'status':>10}"
    )
    print("\n" + "=" * len(header))
    print("PROXY BENCHMARK SUMMARY")
    print("=" * len(header))
    print(header)
    print("-" * len(header))
    for r in results:
        lw = r.get("litellm_workers", "?")
        ro = r.get("routing", "?")
        nbe = r.get("num_backends", "?")
        if r.get("test_type") == "ramp":
            max_rps = r.get("max_sustainable_rps")
            max_label = f"max={max_rps}" if max_rps is not None else "max=N/A"
            print(
                f"{lw:>4} {ro:>20} {nbe:>8} "
                f"{'RAMP':>8} {'N/A':>8} {'N/A':>7} {'N/A':>6} {max_label:>10}"
            )
        elif r.get("test_type") == "fixed":
            p99 = f"{r['latency_p99_s']:.3f}" if r.get("latency_p99_s") is not None else "N/A"
            err = f"{r['error_fraction'] * 100:.1f}" if "error_fraction" in r else "N/A"
            ef = r.get("error_fraction", 1.0)
            lat = r.get("latency_p99_s")
            status = "OK" if ef < 0.05 and (lat is None or lat < 2.0) else "DEGRADED"
            trps = r.get("target_rps", "?")
            arps = r.get("actual_rps", "?")
            print(f"{lw:>4} {ro:>20} {nbe:>8} {trps:>8} {arps:>8} {p99:>7} {err:>6} {status:>10}")
        else:
            print(f"  Unknown result type: {r}")
    print("=" * len(header) + "\n")


# ---------------------------------------------------------------------------
# ---- MAX-RPS SEARCH PLOTS --------------------------------------------------
# ---------------------------------------------------------------------------


def print_max_rps_table(data: dict):
    """Print a summary table for find_max_rps results."""
    results = data.get("results", [])
    if not results:
        print("  [max_rps] No results found.")
        return

    header = (
        f"{'workers':>7} {'payload':>8} {'max_rps':>9} "
        f"{'validated':>9} {'at_ceil':>7} {'probes':>6}  notes"
    )
    print("\n" + "=" * (len(header) + 10))
    print("  MAX RPS SEARCH RESULTS")
    print("=" * (len(header) + 10))
    print(header)
    print("-" * (len(header) + 10))
    for r in results:
        notes = ""
        if r.get("at_ceiling"):
            notes = ">= ceiling"
        elif r.get("below_floor"):
            notes = "below floor"
        elif not r.get("validated"):
            notes = "val failed, stepped back"
        n_steps = len(r.get("search_history", []))
        print(
            f"{r['num_workers']:>7} {r['payload_size']:>8} {r['max_rps']:>9.1f} "
            f"{'YES' if r.get('validated') else 'NO':>9} "
            f"{'YES' if r.get('at_ceiling') else 'NO':>7} "
            f"{n_steps:>6}  {notes}"
        )
    print("=" * (len(header) + 10) + "\n")

    meta = data.get("meta", {})
    print(
        f"  rps_start={meta.get('rps_start')}  ceiling={meta.get('max_rps_ceiling')}  "
        f"precision={meta.get('precision')}  probe_duration={meta.get('probe_duration_s')}s  "
        f"full_duration={meta.get('full_duration_s')}s\n"
    )


def plot_max_rps(data: dict, input_path: str, out_dir: Optional[str]):
    """
    Grouped bar chart: max RPS per (workers, payload).
    X-axis = number of workers, one bar group per payload size.
    """
    if not _MPL_AVAILABLE:
        return
    results = data.get("results", [])
    if not results:
        print("  [max_rps] No results found.")
        return

    payload_sizes = sorted(
        set(r["payload_size"] for r in results),
        key=lambda p: list(["small", "medium", "large", "xl"]).index(p)
        if p in ["small", "medium", "large", "xl"]
        else 999,
    )
    worker_counts = sorted(set(r["num_workers"] for r in results))

    n_payloads = len(payload_sizes)
    n_workers = len(worker_counts)
    bar_width = 0.8 / max(n_payloads, 1)
    colors = plt.cm.tab10.colors

    fig, ax = plt.subplots(figsize=(max(8, n_workers * 2), 5))

    x = np.arange(n_workers)
    for i, ps in enumerate(payload_sizes):
        max_rps_vals = []
        for w in worker_counts:
            match = [r for r in results if r["num_workers"] == w and r["payload_size"] == ps]
            max_rps_vals.append(match[0]["max_rps"] if match else 0.0)

        offset = (i - n_payloads / 2 + 0.5) * bar_width
        bars = ax.bar(
            x + offset,
            max_rps_vals,
            bar_width * 0.9,
            label=ps,
            color=colors[i % len(colors)],
            alpha=0.85,
        )

        # Annotate bars with the value
        for bar, val in zip(bars, max_rps_vals):
            if val > 0:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + max(max_rps_vals) * 0.01,
                    f"{val:.0f}",
                    ha="center",
                    va="bottom",
                    fontsize=7,
                    rotation=45,
                )

    ax.set_xlabel("Number of Workers")
    ax.set_ylabel("Max Sustainable RPS")
    ax.set_title("Max Sustainable RPS per Config (exponential probe + binary search)")
    ax.set_xticks(x)
    ax.set_xticklabels([str(w) for w in worker_counts])
    ax.legend(title="Payload")
    ax.grid(True, axis="y", alpha=0.3)
    ax.set_ylim(bottom=0)

    path = _out_path(input_path, "max_rps", out_dir)
    _save_fig(fig, path)


def plot_max_rps_search_history(data: dict, input_path: str, out_dir: Optional[str]):
    """
    One subplot per (workers, payload) config showing the search trajectory:
    RPS tried (x-axis, in order) vs whether it passed, coloured by phase.
    """
    if not _MPL_AVAILABLE:
        return
    results = data.get("results", [])
    if not results:
        return

    n = len(results)
    ncols = min(3, n)
    nrows = math.ceil(n / ncols)
    fig, axes = plt.subplots(nrows, ncols, figsize=(ncols * 5, nrows * 3.5), squeeze=False)

    phase_colors = {"probe": "#4C72B0", "bisect": "#DD8452", "validation": "#55A868"}
    marker_map = {True: "o", False: "X"}

    for idx, r in enumerate(results):
        ax = axes[idx // ncols][idx % ncols]
        history = r.get("search_history", [])
        label_cfg = f"w={r['num_workers']}  {r['payload_size']}"

        for step_i, h in enumerate(history):
            phase = h.get("phase", "probe")
            passed = h.get("can_keep_up", False)
            color = phase_colors.get(phase, "grey")
            marker = marker_map.get(passed, "s")
            ax.scatter(step_i, h["rps"], color=color, marker=marker, s=70, zorder=3)

        if history:
            rps_vals = [h["rps"] for h in history]
            ax.plot(range(len(history)), rps_vals, color="grey", linewidth=0.8, alpha=0.5)

        ax.axhline(
            y=r["max_rps"],
            color="green",
            linestyle="--",
            linewidth=1.2,
            label=f"max_rps={r['max_rps']:.0f}",
        )
        ax.set_title(label_cfg, fontsize=9)
        ax.set_xlabel("Probe step")
        ax.set_ylabel("RPS")
        ax.legend(fontsize=7)
        ax.grid(True, alpha=0.3)

    # Hide unused subplots
    for idx in range(len(results), nrows * ncols):
        axes[idx // ncols][idx % ncols].set_visible(False)

    # Phase legend
    legend_patches = [
        plt.Line2D([0], [0], marker="o", color="w", markerfacecolor=c, markersize=8, label=ph)
        for ph, c in phase_colors.items()
    ]
    fig.legend(handles=legend_patches, loc="lower right", title="Phase", fontsize=8)

    fig.suptitle("Max-RPS Search History (o=pass, X=fail)", fontsize=11)
    fig.tight_layout()
    path = _out_path(input_path, "max_rps_history", out_dir)
    _save_fig(fig, path)


# ---------------------------------------------------------------------------
# Main dispatcher
# ---------------------------------------------------------------------------

CLIENT_PLOTS = {
    "throughput": plot_throughput,
    "dispatch": plot_dispatch,
    "keepup": plot_keepup,
    "latency": plot_latency_cdf,
}

PROXY_PLOTS = {
    "proxy_throughput": plot_proxy_throughput,
    "proxy_latency": plot_proxy_latency,
    "ramp": plot_ramp,
    "proxy_keepup": plot_proxy_keepup,
}

MAX_RPS_PLOTS = {
    "max_rps": plot_max_rps,
    "max_rps_history": plot_max_rps_search_history,
}

ALL_PLOTS = {**CLIENT_PLOTS, **PROXY_PLOTS, **MAX_RPS_PLOTS}


def detect_result_type(data: dict) -> str:
    """Return 'client', 'proxy', or 'max_rps' based on result structure."""
    if data.get("meta", {}).get("mode") == "find_max_rps":
        return "max_rps"
    results = data.get("results", [])
    if not results:
        return "unknown"
    first = results[0]
    if "litellm_workers" in first:
        return "proxy"
    if "num_workers" in first:
        return "client"
    return "unknown"


def main():
    parser = argparse.ArgumentParser(
        description="Analyze and visualize benchmark results.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--input", type=str, required=True, help="Path to JSON result file.")
    parser.add_argument(
        "--plots",
        type=str,
        default="all",
        help=(
            "Comma-separated plot names to generate, or 'all'. "
            "Client: throughput, dispatch, keepup, latency. "
            "Proxy: proxy_throughput, proxy_latency, ramp, proxy_keepup."
        ),
    )
    parser.add_argument("--out-dir", type=str, default=None, help="Output directory for plots.")
    parser.add_argument("--no-plots", action="store_true", help="Skip plotting; print tables only.")
    args = parser.parse_args()

    data = _load(args.input)
    result_type = detect_result_type(data)
    print(f"[Analyze] Detected result type: {result_type}")

    # Print summary table
    if result_type == "client":
        print_client_table(data)
    elif result_type == "proxy":
        print_proxy_table(data)
    elif result_type == "max_rps":
        print_max_rps_table(data)
    else:
        print("[Analyze] Unknown result type; printing raw meta:")
        print(json.dumps(data.get("meta", {}), indent=2))

    if args.no_plots or not _MPL_AVAILABLE:
        return

    # Determine which plots to generate
    requested = set(p.strip() for p in args.plots.split(",") if p.strip())
    if "all" in requested:
        if result_type == "max_rps":
            requested = set(MAX_RPS_PLOTS.keys())
        elif result_type == "proxy":
            requested = set(PROXY_PLOTS.keys())
        else:
            requested = set(CLIENT_PLOTS.keys())

    print(f"\n[Analyze] Generating plots: {sorted(requested)}")
    out_dir = args.out_dir or str(Path(args.input).parent)

    for name in sorted(requested):
        if name not in ALL_PLOTS:
            print(f"  [WARN] Unknown plot '{name}' — skipping.")
            continue
        if result_type == "client" and name in PROXY_PLOTS:
            print(f"  [SKIP] '{name}' is a proxy plot but input is client results.")
            continue
        if result_type == "client" and name in MAX_RPS_PLOTS:
            print(f"  [SKIP] '{name}' is a max-rps plot but input is client sweep results.")
            continue
        if result_type == "proxy" and name in CLIENT_PLOTS:
            print(f"  [SKIP] '{name}' is a client plot but input is proxy results.")
            continue
        if result_type == "proxy" and name in MAX_RPS_PLOTS:
            print(f"  [SKIP] '{name}' is a max-rps plot but input is proxy results.")
            continue
        if result_type == "max_rps" and name not in MAX_RPS_PLOTS:
            print(f"  [SKIP] '{name}' is not applicable to max-rps results.")
            continue
        print(f"  Plotting: {name}")
        try:
            ALL_PLOTS[name](data, args.input, out_dir)
        except Exception as e:
            print(f"  [ERROR] Failed to generate plot '{name}': {e}")


if __name__ == "__main__":
    main()
