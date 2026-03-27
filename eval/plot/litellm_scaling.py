#!/usr/bin/env python3
"""
Plot LiteLLM proxy scaling results as a single-axis RPS chart.

This mirrors the experiment selection behavior from weakscaling.py while
focusing on three RPS-oriented lines:
1. Ideal RPS scaling from the first measured point.
2. Client RPS implied by the configured requests-per-node.
3. Measured proxy RPS from the result JSONs.
"""
import os
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"
os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")

import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import ScalarFormatter

from eval.lib.run_planner import resolve_run_group_dir
try:
    from .weakscaling import (
        EXPERIMENT_REGISTRY,
        _extract_plot_template_fields,
        _format_plot_text,
        extract_node_count,
        parse_indices,
        parse_node_select_mapping,
    )
except ImportError:  # pragma: no cover - script-mode fallback
    from weakscaling import (
        EXPERIMENT_REGISTRY,
        _extract_plot_template_fields,
        _format_plot_text,
        extract_node_count,
        parse_indices,
        parse_node_select_mapping,
    )


PLOT_TITLE_TEMPLATE = "LiteLLM Proxy + Ray Serve (EveryNode) + Dummy RayWorkers (Null-Compute)" 
PLOT_TITLE_TEMPLATE = "LiteLLM Proxy + Ray Serve (EveryNode) + vLLM Workers"
PLOT_TITLE_TEMPLATE += "\nWeak Scaling (ALCF Aurora)"
PLOT_SUBTITLE_TEMPLATE = (
    "{model_name}, TP={tensor_parallel_size}, {gpus_per_node} GPUs per node, {run_count} runs mean"
    "\nClient: {rate_per_node} RPS/node, {trace_duration_s} seconds"
    "\nProxy: {proxy_type_label}, {proxy_num_workers} workers"
)


def _latest_result_file(results_dir: Path) -> Optional[Path]:
    candidates = list(results_dir.glob("result*.json"))
    if not candidates:
        return None

    latest_file = None
    max_idx = -2
    for path in candidates:
        match = re.match(r"result(\d+)\.json", path.name)
        if match:
            file_idx = int(match.group(1))
        elif path.name == "result.json":
            file_idx = -1
        else:
            continue

        if file_idx > max_idx:
            max_idx = file_idx
            latest_file = path

    return latest_file


def _load_json(path: Path) -> dict:
    with open(path, "r") as handle:
        return json.load(handle)


def _mean(values: List[float]) -> float:
    if not values:
        return 0.0
    return float(np.mean(values))


def _result_point_from_data(data: dict, node_count: int) -> Dict[str, float]:
    cfg = data.get("config") or {}
    trace_cfg = cfg.get("job_trace_config") or {}
    overall = data.get("overall") or {}
    pbs_job_name = str(cfg.get("pbs_job_name", ""))

    rpn = float(trace_cfg.get("rpn") or 0.0)
    tps = float(overall.get("tps") or 0.0)
    trace_span_s = float(overall.get("trace_span_s") or 0.0)
    actual_dispatch_s = float(overall.get("actual_dispatch_s") or 0.0)
    dispatch_overhead_s = float(overall.get("dispatch_overhead_s") or 0.0)
    rps = float(overall.get("rps") or 0.0)

    overhead_ratio = 0.0
    if trace_span_s > 0.0:
        overhead_ratio = dispatch_overhead_s / trace_span_s

    return {
        "num_nodes": node_count,
        "rpn": rpn,
        "tps": tps,
        "client_rps": rpn * node_count,
        "rps": rps,
        "trace_span_s": trace_span_s,
        "actual_dispatch_s": actual_dispatch_s,
        "dispatch_overhead_s": dispatch_overhead_s,
        "dispatch_overhead_ratio": overhead_ratio,
        "is_null_compute": "null_compute" in pbs_job_name.lower(),
    }


def _aggregate_points(points: List[Dict[str, float]], node_count: int) -> Dict[str, float]:
    if not points:
        return {}

    rpn = points[0]["rpn"]
    tps = _mean([point["tps"] for point in points])
    trace_span_s = _mean([point["trace_span_s"] for point in points])
    actual_dispatch_s = _mean([point["actual_dispatch_s"] for point in points])
    dispatch_overhead_s = _mean([point["dispatch_overhead_s"] for point in points])
    rps = _mean([point["rps"] for point in points])

    overhead_ratio = 0.0
    if trace_span_s > 0.0:
        overhead_ratio = dispatch_overhead_s / trace_span_s

    return {
        "num_nodes": node_count,
        "rpn": rpn,
        "tps": tps,
        "client_rps": rpn * node_count,
        "rps": rps,
        "trace_span_s": trace_span_s,
        "actual_dispatch_s": actual_dispatch_s,
        "dispatch_overhead_s": dispatch_overhead_s,
        "dispatch_overhead_ratio": overhead_ratio,
        "is_null_compute": bool(points[0].get("is_null_compute", False)),
    }


def load_raw_results(
    results_folder: str,
    target_indices: List[int] = None,
    excluded_nodes: List[int] = None,
    node_select_map: Dict[int, int] = None,
) -> Tuple[List[Dict[str, float]], List[str], Dict[str, str]]:
    """
    Load LiteLLM result data while preserving weakscaling.py selection rules.

    Returns:
        results: List of dicts with node_count, client_rps, measured rps, and
                 dispatch timing information.
        missing_files: Paths that were explicitly requested but not present.
        template_fields: Plot template fields from the first loaded result.
    """
    results = []
    missing_files = []
    template_fields = None
    results_path = Path(results_folder)

    if not results_path.exists():
        raise FileNotFoundError(f"Results folder not found: {results_folder}")

    for subdir in sorted(results_path.iterdir()):
        if not subdir.is_dir():
            continue

        node_count = extract_node_count(subdir.name)
        if node_count == 0:
            continue

        if excluded_nodes and node_count in excluded_nodes:
            print(f"Skipping {node_count} nodes (excluded via argument)")
            continue

        results_dir = subdir / "results"

        if node_select_map is not None:
            selected_file = None
            if node_count in node_select_map:
                idx = node_select_map[node_count]
                selected_file = results_dir / f"result{idx}.json"
                if not selected_file.exists():
                    missing_files.append(str(selected_file))
                    print(f"Warning: {selected_file} not found, skipping {subdir.name}")
                    continue
            else:
                selected_file = _latest_result_file(results_dir)
                if not selected_file:
                    print(f"Warning: No result files found in {subdir.name}, skipping")
                    continue
                print(f"  -> Using {selected_file.name} (latest) for {subdir.name}")

            try:
                data = _load_json(selected_file)
                if template_fields is None:
                    template_fields = _extract_plot_template_fields(data)
                point = _result_point_from_data(data, node_count)
                results.append(point)
                print(
                    "Loaded "
                    f"{subdir.name}: {selected_file.name}  "
                    f"RPS={point['rps']:.2f}, ClientRPS={point['client_rps']:.2f}, "
                    f"Overhead={point['dispatch_overhead_ratio']:.2%}"
                )
            except Exception as exc:
                print(f"Error reading {selected_file}: {exc}")
            continue

        if target_indices is not None:
            selected_points = []
            first_loaded_data = None
            for idx in target_indices:
                file_path = results_dir / f"result{idx}.json"
                if not file_path.exists():
                    missing_files.append(str(file_path))
                    continue

                try:
                    data = _load_json(file_path)
                    if first_loaded_data is None:
                        first_loaded_data = data
                    selected_points.append(_result_point_from_data(data, node_count))
                except Exception as exc:
                    print(f"Error reading {file_path}: {exc}")

            if not selected_points:
                print(f"Warning: No valid results found for {subdir.name} (indices {target_indices})")
                continue

            if template_fields is None and first_loaded_data is not None:
                template_fields = _extract_plot_template_fields(
                    first_loaded_data,
                    selected_run_count=len(selected_points),
                )

            point = _aggregate_points(selected_points, node_count)
            results.append(point)
            print(
                "Loaded "
                f"{subdir.name}: {len(selected_points)} runs. "
                f"RPS={point['rps']:.2f}, ClientRPS={point['client_rps']:.2f}, "
                f"Overhead={point['dispatch_overhead_ratio']:.2%}"
            )
            continue

        latest_file = _latest_result_file(results_dir)
        if not latest_file:
            print(f"Warning: No result files found in {subdir.name}, skipping")
            continue

        print(f"  -> Using {latest_file.name} for {subdir.name}")

        try:
            data = _load_json(latest_file)
            if template_fields is None:
                template_fields = _extract_plot_template_fields(data)
            point = _result_point_from_data(data, node_count)
            results.append(point)
            print(
                f"Loaded {subdir.name}: RPS={point['rps']:.2f}, "
                f"ClientRPS={point['client_rps']:.2f}, "
                f"Overhead={point['dispatch_overhead_ratio']:.2%}"
            )
        except Exception as exc:
            print(f"Error reading {latest_file}: {exc}")

    results.sort(key=lambda item: item["num_nodes"])
    return results, missing_files, (template_fields or {})


def plot_litellm_scaling(
    results: List[Dict[str, float]],
    output_path: str = None,
    log_scale: bool = False,
    overhead_threshold: float = 0.10,
    annotate_overhead_warning: bool = True,
    template_fields: Dict[str, str] = None,
):
    if not results:
        print("No results to plot!")
        return

    num_nodes = [result["num_nodes"] for result in results]
    measured_rps = [result["rps"] for result in results]
    client_rps = [result["client_rps"] for result in results]
    measured_tps = [result["tps"] for result in results]

    first_node_count = num_nodes[0]
    first_measured_rps = measured_rps[0]
    ideal_rps = [first_measured_rps * (node_count / first_node_count) for node_count in num_nodes]

    fig, ax = plt.subplots(figsize=(12, 7))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("#FAFAFA")

    ideal_color = "#7A7A7A"
    client_color = "#E67E22"
    measured_color = "#2E86AB"
    tps_color = "#2D6A4F"
    warning_color = "#C0392B"

    ideal_line, = ax.plot(
        num_nodes,
        ideal_rps,
        linestyle="--",
        marker="s",
        color=ideal_color,
        linewidth=2.2,
        markersize=8,
        label="Ideal Serving RPS Scaling",
        alpha=0.9,
        zorder=2,
    )
    client_line, = ax.plot(
        num_nodes,
        client_rps,
        linestyle="-.",
        marker="^",
        color=client_color,
        linewidth=2.4,
        markersize=9,
        label="Client Request Generation Rate (RPS)",
        alpha=0.95,
        zorder=3,
    )

    proxy_label = (template_fields or {}).get("proxy_type_label", "LiteLLM")
    measured_line, = ax.plot(
        num_nodes,
        measured_rps,
        linestyle="-",
        marker="o",
        color=measured_color,
        linewidth=3.0,
        markersize=9,
        label=f"{proxy_label} Measured Serving RPS",
        alpha=0.95,
        zorder=4,
    )

    for node_count, rps in zip(num_nodes, ideal_rps):
        ax.annotate(
            f"{rps:.1f}",
            (node_count, rps),
            textcoords="offset points",
            xytext=(0, 26),
            ha="center",
            fontsize=8,
            fontweight="bold",
            color=ideal_color,
            bbox=dict(
                boxstyle="round,pad=0.2",
                facecolor="white",
                edgecolor=ideal_color,
                linewidth=1.0,
                alpha=0.8,
            ),
        )

    for node_count, rps in zip(num_nodes, client_rps):
        ax.annotate(
            f"{rps:.1f}",
            (node_count, rps),
            textcoords="offset points",
            xytext=(0, 28), # prevent overlaping with ideal RPS.
            ha="center",
            fontsize=8,
            fontweight="bold",
            color=client_color,
            bbox=dict(
                boxstyle="round,pad=0.2",
                facecolor="white",
                edgecolor=client_color,
                linewidth=1.0,
                alpha=0.8,
            ),
        )

    for node_count, rps in zip(num_nodes, measured_rps):
        ax.annotate(
            f"{rps:.1f}",
            (node_count, rps),
            textcoords="offset points",
            xytext=(0, 12),
            ha="center",
            fontsize=9,
            fontweight="bold",
            color=measured_color,
            bbox=dict(
                boxstyle="round,pad=0.25",
                facecolor="white",
                edgecolor=measured_color,
                linewidth=1.2,
                alpha=0.85,
            ),
        )

    if not results[0].get("is_null_compute", False):
        for node_count, tps, rps in zip(num_nodes, measured_tps, measured_rps):
            ax.annotate(
                f"{tps:.0f} TPS",
                (node_count, rps),
                textcoords="offset points",
                xytext=(0, -20),
                ha="center",
                fontsize=8,
                fontweight="bold",
                color=tps_color,
                bbox=dict(
                    boxstyle="round,pad=0.2",
                    facecolor="white",
                    edgecolor=tps_color,
                    linewidth=1.0,
                    alpha=0.8,
                ),
            )

    flagged_results = [
        result
        for result in results
        if result["dispatch_overhead_ratio"] > overhead_threshold
    ]
    if flagged_results:
        print(
            f"Heads up: dispatch overhead exceeded {overhead_threshold:.0%} "
            "for the following node counts:"
        )
        for result in flagged_results:
            print(
                f"  - {result['num_nodes']} nodes: "
                f"overhead={result['dispatch_overhead_ratio']:.2%}, "
                f"dispatch_overhead_s={result['dispatch_overhead_s']:.3f}, "
                f"trace_span_s={result['trace_span_s']:.3f}"
            )

    if annotate_overhead_warning:
        for result in flagged_results:
            ax.annotate(
                "client fails to keep up",
                (result["num_nodes"], result["client_rps"]),
                textcoords="offset points",
                xytext=(10, 12),
                ha="left",
                fontsize=9,
                fontweight="bold",
                color=warning_color,
                bbox=dict(
                    boxstyle="round,pad=0.25",
                    facecolor="white",
                    edgecolor=warning_color,
                    linewidth=1.1,
                    alpha=0.9,
                ),
            )

    ax.set_xlabel("Number of Nodes", fontsize=12, fontweight="bold", color="#333333")
    ax.set_ylabel("Requests per Second (RPS)", fontsize=12, fontweight="bold", color="#333333")
    ax.tick_params(axis="x", labelsize=11, colors="#333333")
    ax.tick_params(axis="y", labelsize=11, colors="#333333")
    ax.grid(True, alpha=0.35, linestyle="--", linewidth=0.8, color="#CCCCCC")
    ax.set_axisbelow(True)

    if log_scale:
        ax.set_xscale("log")
        ax.set_yscale("log")
        formatter = ScalarFormatter()
        formatter.set_scientific(False)
        ax.yaxis.set_major_formatter(formatter)
        ax.yaxis.set_minor_formatter(formatter)
    else:
        node_range = max(num_nodes) - min(num_nodes)
        padding = max(node_range * 0.05, 0.5)
        ax.set_xlim(min(num_nodes) - padding, max(num_nodes) + padding)

    gpu_workers_per_node = (template_fields or {}).get("tensor_parallel_size", "")
    gpu_workers_per_node_int = None
    if gpu_workers_per_node:
        try:
            gpu_workers_per_node_int = int(gpu_workers_per_node)
        except ValueError:
            gpu_workers_per_node_int = None

    ax.set_xticks(num_nodes)
    if gpu_workers_per_node_int is not None:
        ax.set_xticklabels(
            [
                f"{node_count}\n({node_count * gpu_workers_per_node_int} GPU workers)"
                for node_count in num_nodes
            ]
        )
    else:
        ax.set_xticklabels([str(node_count) for node_count in num_nodes])

    title_text = _format_plot_text(PLOT_TITLE_TEMPLATE, template_fields)
    subtitle_text = _format_plot_text(PLOT_SUBTITLE_TEMPLATE, template_fields)

    fig.suptitle(title_text, fontsize=16, fontweight="bold", color="#1A1A1A", y=0.98)
    ax.set_title(subtitle_text, fontsize=11, color="#666666", pad=10, style="italic")

    legend = ax.legend(
        [ideal_line, client_line, measured_line],
        [ideal_line.get_label(), client_line.get_label(), measured_line.get_label()],
        loc="upper left",
        fontsize=10,
        frameon=True,
        fancybox=True,
        shadow=True,
        framealpha=0.95,
        edgecolor="#CCCCCC",
        facecolor="white",
    )
    legend.get_frame().set_linewidth(1.2)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    for spine in ax.spines.values():
        spine.set_edgecolor("#DDDDDD")
        spine.set_linewidth(1.1)

    fig.tight_layout(rect=[0, 0, 1, 0.95])

    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches="tight", facecolor="white", edgecolor="none")
        plt.close(fig)
        print(f"Plot saved to {output_path}")
    else:
        plt.show()


def main():
    parser = argparse.ArgumentParser(description="Plot LiteLLM proxy scaling results")
    parser.add_argument(
        "-e",
        "--experiment",
        type=str,
        required=True,
        help=(
            "Experiment name from EXPERIMENT_REGISTRY. "
            f"Available: {', '.join(EXPERIMENT_REGISTRY.keys())}."
        ),
    )
    parser.add_argument(
        "-b",
        "--backend",
        type=str,
        default="ray",
        help="Backend name for display purposes. Default: ray.",
    )
    parser.add_argument(
        "-o",
        "--output_path",
        type=str,
        default=None,
        help="Path to save the output plot (default: litellm_scaling_log.png or litellm_scaling_linear.png)",
    )
    parser.add_argument(
        "--run-group",
        type=str,
        default="latest",
        help="Run group to read (e.g. run0). Default: latest.",
    )
    parser.add_argument(
        "--linear",
        action="store_true",
        help="Use linear scale for X and Y axes (default: log scale)",
    )
    parser.add_argument(
        "--indices",
        type=str,
        default=None,
        help=(
            "Indices of experiments to aggregate (e.g. '0-2' or '0,1,2'). "
            "Default: latest result only."
        ),
    )
    parser.add_argument(
        "--exclude-nodes",
        type=str,
        default=None,
        help="Comma-separated list of node counts to exclude (e.g. '64,128').",
    )
    parser.add_argument(
        "--node-list",
        type=str,
        default=None,
        help="Comma-separated node counts paired with --select (e.g. '1,2,4,8').",
    )
    parser.add_argument(
        "--select",
        type=str,
        default=None,
        help=(
            "Comma-separated result indices, one per entry in --node-list. "
            "Nodes not listed fall back to the latest result file."
        ),
    )
    parser.add_argument(
        "--overhead-threshold",
        type=float,
        default=0.10,
        help="Annotate points when dispatch_overhead_s / trace_span_s exceeds this threshold.",
    )
    parser.add_argument(
        "--no-overhead-annotation",
        action="store_true",
        help="Disable the on-plot 'client fails to keep up' annotation while still printing console heads-up messages.",
    )
    args = parser.parse_args()

    if bool(args.node_list) != bool(args.select):
        parser.error("--node-list and --select must be used together.")

    if args.experiment not in EXPERIMENT_REGISTRY:
        parser.error(
            f"Unknown experiment '{args.experiment}'. "
            f"Available: {', '.join(EXPERIMENT_REGISTRY.keys())}"
        )

    results_folder = resolve_run_group_dir(args.experiment, run_group=args.run_group)
    print(f"Experiment : {args.experiment}  (backend={args.backend})")
    print(f"Run group  : {args.run_group}")
    print(f"Results dir: {results_folder}")

    if not args.output_path:
        suffix = "linear" if args.linear else "log"
        args.output_path = f"litellm_scaling_{suffix}.png"

    node_select_map = None
    if args.node_list and args.select:
        node_select_map = parse_node_select_mapping(args.node_list, args.select)
        print(f"Per-node result selection: {{ {', '.join(f'{k}: result{v}.json' for k, v in node_select_map.items())} }}")

    target_indices = None
    if node_select_map is None:
        target_indices = parse_indices(args.indices)
        if target_indices:
            print(f"Aggregating results for indices: {target_indices}")

    excluded_nodes = parse_indices(args.exclude_nodes)
    if excluded_nodes:
        print(f"Excluding nodes: {excluded_nodes}")

    print(f"Loading results from: {results_folder}")
    results, missing_files, template_fields = load_raw_results(
        results_folder,
        target_indices=target_indices,
        excluded_nodes=excluded_nodes,
        node_select_map=node_select_map,
    )

    if missing_files:
        print("\n" + "!" * 50)
        print(f"WARNING: {len(missing_files)} Missing result files:")
        for missing_file in missing_files:
            print(f"  - {missing_file}")
        print("!" * 50 + "\n")

    if not results:
        print("No valid results found!")
        return

    print(f"\nFound {len(results)} node configurations")
    print("\nGenerating plot...")
    plot_litellm_scaling(
        results,
        output_path=args.output_path,
        log_scale=not args.linear,
        overhead_threshold=args.overhead_threshold,
        annotate_overhead_warning=not args.no_overhead_annotation,
        template_fields=template_fields,
    )


if __name__ == "__main__":
    main()
