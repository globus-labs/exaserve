#!/usr/bin/env python3
"""
Plot LiteLLM proxy scaling from a replay-client proxy sweep JSON.

The chart uses:
  - x-axis: LiteLLM worker count
  - y-axis: a selected RPS metric from the replay-client sweep
  - one line per backend count

Usage:
    python3 plot/proxy_scaling.py
    python3 plot/proxy_scaling.py --input /path/to/proxy_sweep_*.json
    python3 plot/proxy_scaling.py --output plot/proxy_scaling.png
"""

import argparse
import json
from pathlib import Path
from typing import Iterable, List, Tuple

# Edit this string directly to change the subtitle without lengthening the CLI.
DEFAULT_SUBTITLE = (
    "Each point shows the highest observed actual_rps across replay-client "
    "probes for a fixed LiteLLM worker/backend configuration. \n"
    "Local loopback."
)


def _candidate_roots():
    return [
        Path("data/bench_results"),
        Path.home() / "agpt" / "data" / "bench_results",
        Path("benchmarks/results"),
    ]


def _find_latest_proxy_sweep() -> Path:
    candidates = []
    for root in _candidate_roots():
        if not root.exists():
            continue
        candidates.extend(root.rglob("proxy_sweep_*.json"))

    candidates = [
        path for path in candidates
        if path.name != "proxy_sweep_checkpoint.json"
    ]
    if not candidates:
        searched = ", ".join(str(root) for root in _candidate_roots())
        raise FileNotFoundError(
            "No proxy sweep JSON found. Looked under: "
            f"{searched}. Pass --input explicitly if your file lives elsewhere."
        )

    return max(candidates, key=lambda path: path.stat().st_mtime)


def _extract_plot_value(row: dict, metric: str):
    if metric == "max_rps":
        value = row.get("max_rps")
        return float(value) if isinstance(value, (int, float)) else None

    values = []
    search_history = row.get("search_history")
    if isinstance(search_history, list):
        for entry in search_history:
            if not isinstance(entry, dict):
                continue
            actual_rps = entry.get("actual_rps")
            if isinstance(actual_rps, (int, float)):
                values.append(float(actual_rps))

    validation_result = row.get("validation_result")
    if isinstance(validation_result, dict):
        actual_rps = validation_result.get("actual_rps")
        if isinstance(actual_rps, (int, float)):
            values.append(float(actual_rps))

    if not values:
        return None
    return max(values)


def _metric_ylabel(metric: str) -> str:
    if metric == "max_rps":
        return "Reported Max RPS"
    return "Max Actual RPS Achieved"


def _load_results(path: Path, metric: str) -> Tuple[dict, List[dict]]:
    data = json.loads(path.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"Expected a JSON object in {path}")

    results = data.get("results")
    if not isinstance(results, list):
        raise ValueError(f"Expected 'results' list in {path}")

    rows = []
    for row in results:
        if not isinstance(row, dict):
            continue
        plot_value = _extract_plot_value(row, metric)
        lw = row.get("litellm_workers")
        backends = row.get("num_backends")
        if not isinstance(lw, int) or not isinstance(backends, int):
            continue
        if plot_value is None:
            continue
        rows.append(
            {
                "litellm_workers": lw,
                "num_backends": backends,
                "plot_rps": float(plot_value),
                "validated": bool(row.get("validated", False)),
            }
        )

    if not rows:
        raise ValueError(f"No plottable replay-client results found in {path}")

    return data, rows


def _sorted_values(rows: Iterable[dict], key: str) -> List[int]:
    return sorted({int(row[key]) for row in rows})


def _format_rps_label(value: float) -> str:
    return f"{value:,.0f}"


def plot_proxy_scaling(
    input_path: Path,
    output_path: Path,
    title: str,
    metric: str = "max_actual_rps",
    xscale: str = "linear",
    yscale: str = "linear",
    annotate: bool = True,
) -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.ticker import FuncFormatter
    except ImportError as exc:
        raise SystemExit(
            "matplotlib is required for plot/proxy_scaling.py. "
            "Install it in the active environment or run the script from an environment "
            "that already has matplotlib available."
        ) from exc

    _, rows = _load_results(input_path, metric=metric)
    worker_counts = _sorted_values(rows, "litellm_workers")
    backend_counts = _sorted_values(rows, "num_backends")
    omitted_points = []

    try:
        plt.style.use("seaborn-v0_8-whitegrid")
    except OSError:
        try:
            plt.style.use("seaborn-whitegrid")
        except OSError:
            pass

    fig, ax = plt.subplots(figsize=(9.5, 6))
    cmap = plt.get_cmap("viridis", len(backend_counts))

    for idx, backends in enumerate(backend_counts):
        series = sorted(
            (row for row in rows if row["num_backends"] == backends),
            key=lambda row: row["litellm_workers"],
        )
        if yscale == "log":
            filtered = []
            for row in series:
                if row["plot_rps"] > 0:
                    filtered.append(row)
                else:
                    omitted_points.append(row)
            series = filtered
        if not series:
            continue
        xs = [row["litellm_workers"] for row in series]
        ys = [row["plot_rps"] for row in series]
        color = cmap(idx)
        ax.plot(
            xs,
            ys,
            marker="o",
            linewidth=2.4,
            markersize=7,
            color=color,
            label=f"{backends} backends",
        )
        if annotate:
            for point_idx, row in enumerate(series):
                y_offset = 8 if (idx + point_idx) % 2 == 0 else -14
                ax.annotate(
                    _format_rps_label(row["plot_rps"]),
                    (row["litellm_workers"], row["plot_rps"]),
                    textcoords="offset points",
                    xytext=(0, y_offset),
                    ha="center",
                    va="bottom" if y_offset >= 0 else "top",
                    fontsize=8.5,
                    color=color,
                )

    fig.suptitle(title, fontsize=16, y=0.98)
    if DEFAULT_SUBTITLE:
        fig.text(
            0.5,
            0.945,
            DEFAULT_SUBTITLE,
            ha="center",
            va="top",
            fontsize=10,
            color="#555555",
        )
    ax.set_xlabel("LiteLLM Workers", fontsize=12)
    ax.set_ylabel(_metric_ylabel(metric), fontsize=12)
    ax.set_xscale(xscale)
    ax.set_yscale(yscale)
    ax.set_xticks(worker_counts)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:g}"))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:,.0f}"))
    ax.legend(title="Backend Count", frameon=True)
    ax.margins(x=0.05, y=0.08)
    if omitted_points:
        omitted_labels = ", ".join(
            f"lw={row['litellm_workers']}/b={row['num_backends']}"
            for row in omitted_points
        )
        fig.text(
            0.5,
            0.01,
            "Log-scale plot omitted non-positive plotted values: " + omitted_labels,
            ha="center",
            va="bottom",
            fontsize=8.5,
        )

    fig.tight_layout(rect=(0, 0, 1, 0.90 if DEFAULT_SUBTITLE else 0.94))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plot LiteLLM proxy scaling from a replay-client proxy sweep JSON."
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help=(
            "Path to a proxy_sweep_*.json file. If omitted, the latest result is "
            "discovered under data/bench_results, ~/agpt/data/bench_results, or benchmarks/results."
        ),
    )
    parser.add_argument(
        "--output",
        type=str,
        default="plot/proxy_scaling.png",
        help="Where to save the figure.",
    )
    parser.add_argument(
        "--title",
        type=str,
        default="LiteLLM Proxy Scaling",
        help="Plot title.",
    )
    parser.add_argument(
        "--metric",
        type=str,
        choices=["max_actual_rps", "max_rps"],
        default="max_actual_rps",
        help="Which RPS value to plot on the y-axis.",
    )
    parser.add_argument(
        "--xscale",
        type=str,
        choices=["linear", "log"],
        default="linear",
        help="Scale for the x-axis.",
    )
    parser.add_argument(
        "--yscale",
        type=str,
        choices=["linear", "log"],
        default="linear",
        help="Scale for the y-axis.",
    )
    parser.add_argument(
        "--log-log-output",
        type=str,
        default=None,
        help="Optional second output path for an additional log-log version of the same chart.",
    )
    parser.add_argument(
        "--no-annotate",
        action="store_true",
        help="Disable text annotation on each plotted point.",
    )
    args = parser.parse_args()

    input_path = Path(args.input).expanduser() if args.input else _find_latest_proxy_sweep()
    output_path = Path(args.output).expanduser()

    plot_proxy_scaling(
        input_path=input_path,
        output_path=output_path,
        title=args.title,
        metric=args.metric,
        xscale=args.xscale,
        yscale=args.yscale,
        annotate=not args.no_annotate,
    )
    print(f"[plot] Input:  {input_path}")
    print(f"[plot] Output: {output_path}")
    print(f"[plot] Metric: {args.metric}")
    print(f"[plot] Scale:  x={args.xscale}, y={args.yscale}")

    if args.log_log_output:
        log_log_output = Path(args.log_log_output).expanduser()
        plot_proxy_scaling(
            input_path=input_path,
            output_path=log_log_output,
            title=f"{args.title} (log-log)",
            metric=args.metric,
            xscale="log",
            yscale="log",
            annotate=not args.no_annotate,
        )
        print(f"[plot] LogLog: {log_log_output}")


if __name__ == "__main__":
    main()
