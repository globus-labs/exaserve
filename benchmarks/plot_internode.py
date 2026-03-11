#!/usr/bin/env python3
"""
Plotting for inter-node scaling benchmark results.

Generates:
  Plot A — Scaling curve (max RPS vs N)
  Plot B — Network bandwidth time-series per N (one subplot per node, one line per interface)
  Plot C — Packets/s time-series per N (same layout)
  Plot D — Errors & drops time-series per N (only if non-zero detected)

Usage:
    python3 plot_internode.py --input-dir /path/to/internode_YYYYMMDD_HHMMSS
"""
import os
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

# Style
try:
    plt.style.use("seaborn-v0_8-darkgrid")
except OSError:
    try:
        plt.style.use("seaborn-darkgrid")
    except OSError:
        pass

# Slingshot-11 theoretical per-NIC limits
# Aurora nodes have 8 HSN NICs (hsn0–hsn7)
BW_PER_NIC_GBS = 25.0     # GB/s per direction per NIC
PPS_PER_NIC = 30e6         # ~30M packets/s per NIC

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_summary(input_dir: Path):
    """Load internode_summary.json."""
    summary_file = input_dir / "internode_summary.json"
    if not summary_file.exists():
        return []
    with open(summary_file) as f:
        return json.load(f)


def load_netstats(ndir: Path):
    """Load all netstats_*.jsonl files in a directory.

    Returns dict: hostname -> list of records (sorted by timestamp).
    """
    by_host = defaultdict(list)
    for jsonl_file in sorted(ndir.glob("netstats_*.jsonl")):
        with open(jsonl_file) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    by_host[rec["hostname"]].append(rec)
                except (json.JSONDecodeError, KeyError):
                    continue
    # Sort each host's records by timestamp
    for host in by_host:
        by_host[host].sort(key=lambda r: r["timestamp"])
    return dict(by_host)


def compute_rates(records):
    """Compute per-interface rates from cumulative counter records.

    Returns dict: interface -> {
        "time": [...],         # seconds from first sample
        "bw_rx_gbs": [...],    # GB/s
        "bw_tx_gbs": [...],
        "pps_rx": [...],
        "pps_tx": [...],
        "errors_rx": [...],    # cumulative
        "errors_tx": [...],
        "drops_rx": [...],
        "drops_tx": [...],
    }
    """
    # Group by interface
    by_iface = defaultdict(list)
    for rec in records:
        by_iface[rec["interface"]].append(rec)

    result = {}
    for iface, recs in by_iface.items():
        if len(recs) < 2:
            continue
        t0 = recs[0]["timestamp"]
        times = []
        bw_rx, bw_tx = [], []
        pps_rx, pps_tx = [], []
        err_rx, err_tx = [], []
        drop_rx, drop_tx = [], []

        for i in range(1, len(recs)):
            dt = recs[i]["timestamp"] - recs[i - 1]["timestamp"]
            if dt <= 0:
                continue
            times.append(recs[i]["timestamp"] - t0)
            bw_rx.append((recs[i]["rx_bytes"] - recs[i - 1]["rx_bytes"]) / dt / 1e9)
            bw_tx.append((recs[i]["tx_bytes"] - recs[i - 1]["tx_bytes"]) / dt / 1e9)
            pps_rx.append((recs[i]["rx_packets"] - recs[i - 1]["rx_packets"]) / dt)
            pps_tx.append((recs[i]["tx_packets"] - recs[i - 1]["tx_packets"]) / dt)
            err_rx.append(recs[i]["rx_errors"])
            err_tx.append(recs[i]["tx_errors"])
            drop_rx.append(recs[i]["rx_drops"])
            drop_tx.append(recs[i]["tx_drops"])

        result[iface] = {
            "time": times,
            "bw_rx_gbs": bw_rx,
            "bw_tx_gbs": bw_tx,
            "pps_rx": pps_rx,
            "pps_tx": pps_tx,
            "errors_rx": err_rx,
            "errors_tx": err_tx,
            "drops_rx": drop_rx,
            "drops_tx": drop_tx,
        }
    return result


# ---------------------------------------------------------------------------
# Plot A: Scaling curve
# ---------------------------------------------------------------------------

def plot_scaling(summary, out_dir: Path):
    """Plot max RPS vs N with scaling efficiency."""
    valid = [r for r in summary if r.get("max_rps") is not None and r["max_rps"] != "null"]
    if not valid:
        print("[plot] No valid results for scaling plot.", file=sys.stderr)
        return

    ns = [r["num_nodes"] for r in valid]
    rps_vals = [float(r["max_rps"]) for r in valid]

    fig, ax1 = plt.subplots(figsize=(10, 6))

    # RPS line
    color_rps = "#2196F3"
    ax1.plot(ns, rps_vals, "o-", color=color_rps, linewidth=2, markersize=8, label="Max RPS")
    ax1.set_xlabel("Number of Nodes (N)", fontsize=13)
    ax1.set_ylabel("Max Sustainable RPS", fontsize=13, color=color_rps)
    ax1.tick_params(axis="y", labelcolor=color_rps)
    ax1.set_xticks(ns)

    # Scaling efficiency on right axis
    # Efficiency = RPS_N / ((N-1) * RPS_at_N2)
    rps_base = None
    for r in valid:
        if r["num_nodes"] == 2:
            rps_base = float(r["max_rps"])
            break
    if rps_base is None and len(valid) > 0:
        rps_base = rps_vals[0]

    if rps_base and rps_base > 0:
        ax2 = ax1.twinx()
        # For N=2: stub_nodes=1, efficiency = rps/(1*rps_base) = 1.0
        base_stub_nodes = valid[0]["num_stub_nodes"] if valid else 1
        efficiencies = []
        for r, rps in zip(valid, rps_vals):
            stub_n = r["num_stub_nodes"]
            if base_stub_nodes > 0:
                ideal_rps = rps_base * (stub_n / base_stub_nodes)
                eff = rps / ideal_rps if ideal_rps > 0 else 0
            else:
                eff = 0
            efficiencies.append(eff)

        color_eff = "#FF9800"
        ax2.plot(ns, efficiencies, "s--", color=color_eff, linewidth=1.5, markersize=6,
                 label="Scaling Efficiency")
        ax2.axhline(y=1.0, color=color_eff, linestyle=":", alpha=0.5, label="Ideal (1.0)")
        ax2.set_ylabel("Scaling Efficiency", fontsize=13, color=color_eff)
        ax2.tick_params(axis="y", labelcolor=color_eff)
        ax2.set_ylim(0, max(1.5, max(efficiencies) * 1.2) if efficiencies else 1.5)
        ax2.legend(loc="lower right", fontsize=10)

    ax1.legend(loc="upper left", fontsize=10)
    ax1.set_title("Inter-Node Replay Client Scaling", fontsize=15)
    fig.tight_layout()
    out_path = out_dir / "scaling.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"[plot] Saved: {out_path}")


# ---------------------------------------------------------------------------
# Plot B/C/D: Network time-series
# ---------------------------------------------------------------------------

def _shorten_hostname(hostname: str) -> str:
    """Shorten a long hostname for subplot titles."""
    # Take just the first component
    return hostname.split(".")[0]


def _format_value(val, unit_label):
    """Auto-scale a value to a readable string with SI prefix."""
    if val >= 1e9:
        return f"{val/1e9:.2f}G {unit_label}"
    if val >= 1e6:
        return f"{val/1e6:.2f}M {unit_label}"
    if val >= 1e3:
        return f"{val/1e3:.2f}K {unit_label}"
    return f"{val:.2f} {unit_label}"


def plot_network_timeseries(netstats_by_host, n_val, out_dir: Path,
                            metric_key_rx, metric_key_tx,
                            ylabel, title_suffix, filename,
                            hline_value=None, hline_label=None):
    """Generic network time-series plotter: one subplot per node, one line per interface.

    Y-axis auto-scales to data. If the theoretical limit (hline_value) is far above
    the actual data (>3x), it is shown as a text annotation with peak utilization %
    instead of a horizontal line that would squish the data.
    """
    hosts = sorted(netstats_by_host.keys())
    if not hosts:
        return False

    n_hosts = len(hosts)
    fig, axes = plt.subplots(n_hosts, 1, figsize=(14, 4 * n_hosts), squeeze=False, sharex=True)

    has_data = False

    for idx, host in enumerate(hosts):
        ax = axes[idx, 0]
        rates = compute_rates(netstats_by_host[host])

        # Collect all values to find data range for this subplot
        all_vals = []
        # Track each line's peak for annotation: (peak_val, peak_time, label, color)
        line_peaks = []

        for iface in sorted(rates.keys()):
            data = rates[iface]
            t = data["time"]
            rx = data[metric_key_rx]
            tx = data[metric_key_tx]
            if not t:
                continue
            has_data = True
            all_vals.extend(rx)
            all_vals.extend(tx)
            line_rx = ax.plot(t, rx, label=f"{iface} RX", linewidth=1.2)[0]
            line_tx = ax.plot(t, tx, label=f"{iface} TX", linewidth=1.2, linestyle="--")[0]
            # Record peaks
            if rx:
                peak_idx = int(np.argmax(rx))
                line_peaks.append((rx[peak_idx], t[peak_idx], f"{iface} RX", line_rx.get_color()))
            if tx:
                peak_idx = int(np.argmax(tx))
                line_peaks.append((tx[peak_idx], t[peak_idx], f"{iface} TX", line_tx.get_color()))

        # Decide how to show the theoretical limit
        peak_val = max(all_vals) if all_vals else 0
        if hline_value is not None and peak_val > 0:
            utilization_pct = (peak_val / hline_value) * 100
            if peak_val > hline_value * 0.3:
                # Data is within range of the limit — draw the line
                ax.axhline(y=hline_value, color="red", linestyle=":", alpha=0.6,
                           linewidth=1.5, label=hline_label or f"Limit ({hline_value})")
            else:
                # Data is far below the limit — annotate instead of drawing line
                # Auto-scale Y to data and show utilization as text
                y_max = peak_val * 1.3 if peak_val > 0 else 1
                ax.set_ylim(bottom=0, top=y_max)
                limit_str = _format_value(hline_value, ylabel.split("(")[-1].rstrip(")") if "(" in ylabel else "")
                ax.text(0.98, 0.95,
                        f"HW limit: {limit_str}\nPeak utilization: {utilization_pct:.2f}%",
                        transform=ax.transAxes, fontsize=9,
                        verticalalignment="top", horizontalalignment="right",
                        bbox=dict(boxstyle="round,pad=0.3", facecolor="lightyellow",
                                  edgecolor="orange", alpha=0.9))
        elif hline_value is not None and peak_val == 0:
            ax.text(0.98, 0.95,
                    f"HW limit: {hline_value} (no traffic detected)",
                    transform=ax.transAxes, fontsize=9,
                    verticalalignment="top", horizontalalignment="right",
                    bbox=dict(boxstyle="round,pad=0.3", facecolor="lightyellow",
                              edgecolor="gray", alpha=0.9))

        # Annotate peaks on lines with relatively high values
        if line_peaks and peak_val > 0:
            # Only annotate lines whose peak is >= 10% of the overall peak
            threshold = peak_val * 0.10
            for lp_val, lp_time, lp_label, lp_color in line_peaks:
                if lp_val >= threshold:
                    ax.annotate(
                        f"{lp_label}\n{_format_value(lp_val, '')}",
                        xy=(lp_time, lp_val),
                        xytext=(8, 6), textcoords="offset points",
                        fontsize=7, color=lp_color, fontweight="bold",
                        bbox=dict(boxstyle="round,pad=0.15", facecolor="white",
                                  edgecolor=lp_color, alpha=0.8),
                    )

        short_name = _shorten_hostname(host)
        role = "client" if idx == 0 else f"stub-{idx}"
        ax.set_title(f"Node {idx}: {short_name} ({role})", fontsize=11)
        ax.set_ylabel(ylabel, fontsize=10)
        ax.legend(fontsize=8, loc="upper left", ncol=2)
        ax.grid(True, alpha=0.3)

    axes[-1, 0].set_xlabel("Time (s)", fontsize=11)
    fig.suptitle(f"N={n_val} — {title_suffix}", fontsize=14, y=1.02)
    fig.tight_layout()
    out_path = out_dir / filename
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[plot] Saved: {out_path}")
    return has_data


def plot_bandwidth(netstats_by_host, n_val, out_dir):
    return plot_network_timeseries(
        netstats_by_host, n_val, out_dir,
        metric_key_rx="bw_rx_gbs",
        metric_key_tx="bw_tx_gbs",
        ylabel="Bandwidth (GB/s)",
        title_suffix="Network Bandwidth",
        filename=f"network_bandwidth_N{n_val}.png",
        hline_value=BW_PER_NIC_GBS,
        hline_label=f"Slingshot-11 limit ({BW_PER_NIC_GBS} GB/s/NIC)",
    )


def plot_pps(netstats_by_host, n_val, out_dir):
    return plot_network_timeseries(
        netstats_by_host, n_val, out_dir,
        metric_key_rx="pps_rx",
        metric_key_tx="pps_tx",
        ylabel="Packets/s",
        title_suffix="Packet Rate",
        filename=f"network_pps_N{n_val}.png",
        hline_value=PPS_PER_NIC,
        hline_label=f"Slingshot-11 limit (~{PPS_PER_NIC/1e6:.0f}M PPS/NIC)",
    )


def plot_errors_drops(netstats_by_host, n_val, out_dir):
    """Plot errors & drops only if any non-zero values exist."""
    # Check if there's anything to plot
    has_nonzero = False
    for host, records in netstats_by_host.items():
        rates = compute_rates(records)
        for iface, data in rates.items():
            for key in ("errors_rx", "errors_tx", "drops_rx", "drops_tx"):
                if any(v > 0 for v in data[key]):
                    has_nonzero = True
                    break
            if has_nonzero:
                break
        if has_nonzero:
            break

    if not has_nonzero:
        return False

    hosts = sorted(netstats_by_host.keys())
    n_hosts = len(hosts)
    fig, axes = plt.subplots(n_hosts, 1, figsize=(14, 4 * n_hosts), squeeze=False, sharex=True)

    for idx, host in enumerate(hosts):
        ax = axes[idx, 0]
        rates = compute_rates(netstats_by_host[host])

        for iface in sorted(rates.keys()):
            data = rates[iface]
            t = data["time"]
            if not t:
                continue
            for key, style in [("errors_rx", "-"), ("errors_tx", "--"),
                               ("drops_rx", "-."), ("drops_tx", ":")]:
                vals = data[key]
                if any(v > 0 for v in vals):
                    ax.plot(t, vals, label=f"{iface} {key}", linewidth=1.2, linestyle=style)

        short_name = _shorten_hostname(host)
        role = "client" if idx == 0 else f"stub-{idx}"
        ax.set_title(f"Node {idx}: {short_name} ({role})", fontsize=11)
        ax.set_ylabel("Count (cumulative)", fontsize=10)
        ax.legend(fontsize=8, loc="upper left", ncol=2)
        ax.grid(True, alpha=0.3)

    axes[-1, 0].set_xlabel("Time (s)", fontsize=11)
    fig.suptitle(f"N={n_val} — Errors & Drops", fontsize=14, y=1.02)
    fig.tight_layout()
    out_path = out_dir / f"network_errors_N{n_val}.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[plot] Saved: {out_path}")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Plot inter-node benchmark results")
    parser.add_argument("--input-dir", type=str, required=True,
                        help="Path to internode benchmark output directory")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        print(f"ERROR: Input directory does not exist: {input_dir}", file=sys.stderr)
        sys.exit(1)

    plot_dir = input_dir / "plots"
    plot_dir.mkdir(exist_ok=True)

    # Plot A: Scaling curve
    summary = load_summary(input_dir)
    if summary:
        plot_scaling(summary, plot_dir)
    else:
        print("[plot] No summary data found, skipping scaling plot.", file=sys.stderr)

    # Plot B/C/D: Per-N network time-series
    for ndir in sorted(input_dir.glob("N*")):
        if not ndir.is_dir():
            continue
        # Extract N value from directory name
        n_str = ndir.name.lstrip("N")
        try:
            n_val = int(n_str)
        except ValueError:
            continue

        netstats = load_netstats(ndir)
        if not netstats:
            print(f"[plot] No netstats data for N={n_val}, skipping.", file=sys.stderr)
            continue

        plot_bandwidth(netstats, n_val, plot_dir)
        plot_pps(netstats, n_val, plot_dir)
        plot_errors_drops(netstats, n_val, plot_dir)

    print(f"[plot] All plots saved to: {plot_dir}")


if __name__ == "__main__":
    main()
