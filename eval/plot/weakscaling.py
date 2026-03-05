#!/usr/bin/env python3
"""
Script to plot weak scaling results from multiple node configurations.
Creates a dual-axis line chart with TPS on the left y-axis and RPS on the right y-axis.
"""
import os
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"

import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set
import argparse

# Allow importing from the parent eval/ directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from exp_configs import EXPERIMENT_REGISTRY, DEFAULT_EXPERIMENTS_ROOT

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from matplotlib import style
from matplotlib.ticker import ScalarFormatter

# Use a modern style (try different style names for compatibility)
try:
    plt.style.use('seaborn-v0_8-darkgrid')
except OSError:
    try:
        plt.style.use('seaborn-darkgrid')
    except OSError:
        try:
            plt.style.use('seaborn')
        except OSError:
            # Fall back to default with custom styling
            pass


def extract_node_count(directory_name: str) -> int:
    """Extract the number of nodes from directory name (e.g., '1_nodes' -> 1)."""
    match = re.match(r'(\d+)_nodes', directory_name)
    if match:
        return int(match.group(1))
    return 0


def parse_indices(indices_str: str) -> Optional[List[int]]:
    """Parse indices string like '0-5' or '0,1,2' into a list of integers."""
    if not indices_str:
        return None
    indices = set()
    parts = indices_str.split(',')
    for part in parts:
        part = part.strip()
        if '-' in part:
            try:
                start, end = map(int, part.split('-'))
                indices.update(range(start, end + 1))
            except ValueError:
                print(f"Warning: Invalid range format '{part}', skipping")
        elif part:
            try:
                indices.add(int(part))
            except ValueError:
                print(f"Warning: Invalid index '{part}', skipping")
    return sorted(list(indices))


def parse_node_select_mapping(node_list_str: str, select_str: str) -> Dict[int, int]:
    """
    Parse --node-list and --select into a per-node result index mapping.

    Args:
        node_list_str: Comma-separated node counts, e.g. '1,2,4,8,16,32'
        select_str:    Comma-separated result indices (one per node), e.g. '0,1,0,0,0,0'

    Returns:
        Dict mapping node_count -> result_index
    """
    nodes = [int(x.strip()) for x in node_list_str.split(',')]
    indices = [int(x.strip()) for x in select_str.split(',')]
    if len(nodes) != len(indices):
        raise ValueError(
            f"--node-list has {len(nodes)} entries but --select has {len(indices)} entries; "
            "they must be the same length."
        )
    return dict(zip(nodes, indices))


def _latency_stats_from_data(data: dict) -> Tuple[float, float, float, float, float]:
    """Extract p50, p99 and min, max, mean latency (seconds) from loaded result data."""
    overall = data.get("overall", {})
    p50 = overall.get("p50_s", 0.0)
    p99 = overall.get("p99_s", 0.0)
    requests = data.get("requests", [])
    latencies = [r["latency"] for r in requests if isinstance(r.get("latency"), (int, float))]
    if not latencies:
        return p50, p99, 0.0, 0.0, 0.0
    return p50, p99, float(min(latencies)), float(max(latencies)), float(np.mean(latencies))


def load_results(
    results_folder: str,
    target_indices: List[int] = None,
    excluded_nodes: List[int] = None,
    node_select_map: Dict[int, int] = None,
) -> Tuple[List[Tuple], List[str]]:
    """
    Load results from all node configurations.
    
    Args:
        results_folder: Path to the experiment directory (e.g. data/experiments/<exp>).
                        Results are expected at <N_nodes>/results/result*.json.
        target_indices: Optional list of indices to aggregate (e.g. [0, 1, 2]).
                        If None, uses the latest result file in each dir.
        excluded_nodes: Optional list of node counts to exclude from the plot.
        node_select_map: Optional dict mapping node_count -> result_index.
                         When provided, picks the exact result{index}.json for each
                         listed node and falls back to the latest file for unlisted nodes.
                         Takes precedence over target_indices.
                        
    Returns:
        results: List of tuples (num_nodes, tps_mean, tps_std, rps_mean, rps_std,
                 p50, p99, lat_min, lat_max, lat_mean)
        missing_files: List of missing file paths
    """
    results = []
    missing_files = []
    results_path = Path(results_folder)
    
    if not results_path.exists():
        raise FileNotFoundError(f"Results folder not found: {results_folder}")
    
    # Iterate over subdirectories matching the pattern *_nodes
    for subdir in sorted(results_path.iterdir()):
        if not subdir.is_dir():
            continue
        
        node_count = extract_node_count(subdir.name)
        if node_count == 0:
            continue
            
        if excluded_nodes and node_count in excluded_nodes:
            print(f"Skipping {node_count} nodes (excluded via argument)")
            continue

        # ── Mode 1: per-node explicit selection (--select + --node-list) ──────
        if node_select_map is not None:
            if node_count in node_select_map:
                idx = node_select_map[node_count]
                fpath = subdir / "results" / f"result{idx}.json"
                if not fpath.exists():
                    missing_files.append(str(fpath))
                    print(f"Warning: {fpath} not found, skipping {subdir.name}")
                    continue
                try:
                    with open(fpath, 'r') as f:
                        data = json.load(f)
                    overall = data.get("overall", {})
                    tps = overall.get("tps", 0.0)
                    rps = overall.get("rps", 0.0)
                    p50, p99, lat_min, lat_max, lat_mean = _latency_stats_from_data(data)
                    results.append((node_count, tps, 0.0, rps, 0.0, p50, p99, lat_min, lat_max, lat_mean))
                    print(f"Loaded {subdir.name}: result{idx}.json  TPS={tps:.2f}, RPS={rps:.2f}")
                except Exception as e:
                    print(f"Error reading {fpath}: {e}")
            else:
                # Node not listed in --node-list: fall back to latest
                candidates = list((subdir / "results").glob("result*.json"))
                if not candidates:
                    print(f"Warning: No result files found in {subdir.name}, skipping")
                    continue
                latest_file, max_idx = None, -2
                for p in candidates:
                    m = re.match(r"result(\d+)\.json", p.name)
                    file_idx = int(m.group(1)) if m else (-1 if p.name == "result.json" else None)
                    if file_idx is not None and file_idx > max_idx:
                        max_idx, latest_file = file_idx, p
                if not latest_file:
                    continue
                print(f"  -> Using {latest_file.name} (latest) for {subdir.name}")
                try:
                    with open(latest_file, 'r') as f:
                        data = json.load(f)
                    overall = data.get("overall", {})
                    tps = overall.get("tps", 0.0)
                    rps = overall.get("rps", 0.0)
                    p50, p99, lat_min, lat_max, lat_mean = _latency_stats_from_data(data)
                    results.append((node_count, tps, 0.0, rps, 0.0, p50, p99, lat_min, lat_max, lat_mean))
                    print(f"Loaded {subdir.name}: {node_count} nodes, TPS={tps:.2f}, RPS={rps:.2f}")
                except Exception as e:
                    print(f"Error reading {latest_file}: {e}")
            continue

        # ── Mode 2: aggregate specific indices (--indices) ────────────────────
        if target_indices is not None:
            tps_list, rps_list = [], []
            p50_list, p99_list, lat_min_list, lat_max_list, lat_mean_list = [], [], [], [], []
            for idx in target_indices:
                fpath = subdir / "results" / f"result{idx}.json"
                if not fpath.exists():
                    missing_files.append(str(fpath))
                    continue
                try:
                    with open(fpath, 'r') as f:
                        data = json.load(f)
                    overall = data.get("overall", {})
                    tps_list.append(overall.get("tps", 0.0))
                    rps_list.append(overall.get("rps", 0.0))
                    p50, p99, lat_min, lat_max, lat_mean = _latency_stats_from_data(data)
                    p50_list.append(p50)
                    p99_list.append(p99)
                    lat_min_list.append(lat_min)
                    lat_max_list.append(lat_max)
                    lat_mean_list.append(lat_mean)
                except Exception as e:
                    print(f"Error reading {fpath}: {e}")
            
            if not tps_list:
                print(f"Warning: No valid results found for {subdir.name} (indices {target_indices})")
                continue
                
            tps_mean = np.mean(tps_list)
            tps_std = np.std(tps_list) if len(tps_list) > 1 else 0.0
            rps_mean = np.mean(rps_list)
            rps_std = np.std(rps_list) if len(rps_list) > 1 else 0.0
            p50_mean = np.mean(p50_list) if p50_list else 0.0
            p99_mean = np.mean(p99_list) if p99_list else 0.0
            lat_min_mean = np.mean(lat_min_list) if lat_min_list else 0.0
            lat_max_mean = np.mean(lat_max_list) if lat_max_list else 0.0
            lat_mean_mean = np.mean(lat_mean_list) if lat_mean_list else 0.0
            
            print(f"Loaded {subdir.name}: {len(tps_list)} runs. TPS={tps_mean:.2f}±{tps_std:.2f}, RPS={rps_mean:.2f}±{rps_std:.2f}")
            results.append((node_count, tps_mean, tps_std, rps_mean, rps_std, p50_mean, p99_mean, lat_min_mean, lat_max_mean, lat_mean_mean))
            continue

        # ── Mode 3: latest result only (default) ──────────────────────────────
        candidates = list((subdir / "results").glob("result*.json"))
        if not candidates:
            print(f"Warning: No result files found in {subdir.name}, skipping")
            continue
            
        latest_file, max_idx = None, -2
        for p in candidates:
            m = re.match(r"result(\d+)\.json", p.name)
            if m:
                file_idx = int(m.group(1))
            elif p.name == "result.json":
                file_idx = -1
            else:
                continue
            if file_idx > max_idx:
                max_idx, latest_file = file_idx, p
        
        if not latest_file:
            continue
             
        print(f"  -> Using {latest_file.name} for {subdir.name}")
        
        try:
            with open(latest_file, 'r') as f:
                data = json.load(f)
            overall = data.get("overall", {})
            tps = overall.get("tps", 0.0)
            rps = overall.get("rps", 0.0)
            p50, p99, lat_min, lat_max, lat_mean = _latency_stats_from_data(data)
            results.append((node_count, tps, 0.0, rps, 0.0, p50, p99, lat_min, lat_max, lat_mean))
            print(f"Loaded {subdir.name}: {node_count} nodes, TPS={tps:.2f}, RPS={rps:.2f}")
        except Exception as e:
            print(f"Error reading {latest_file}: {e}")
    
    # Sort by number of nodes
    results.sort(key=lambda x: x[0])
    return results, missing_files


def plot_weak_scaling(results: List[Tuple], output_path: str = None, log_scale: bool = False):
    """
    Plot weak scaling chart with TPS (left), RPS (right), and latency (right offset).
    Includes p99/p50 latency lines and scatter points for min, max, mean latency.
    
    Args:
        results: List of (num_nodes, tps_mean, tps_std, rps_mean, rps_std, p50, p99, lat_min, lat_max, lat_mean) tuples
        output_path: Optional path to save the plot. If None, displays interactively.
        log_scale: Whether to use log scale for x and y axes.
    """
    if not results:
        print("No results to plot!")
        return
    
    num_nodes = [r[0] for r in results]
    tps_means = [r[1] for r in results]
    tps_stds = [r[2] for r in results]
    rps_means = [r[3] for r in results]
    rps_stds = [r[4] for r in results]
    p50_vals = [r[5] for r in results]
    p99_vals = [r[6] for r in results]
    lat_mins = [r[7] for r in results]
    lat_maxs = [r[8] for r in results]
    lat_means = [r[9] for r in results]
    
    # Set up the figure with a modern style
    fig, ax1 = plt.subplots(figsize=(16, 7))
    fig.patch.set_facecolor('white')
    ax1.set_facecolor('#FAFAFA')
    
    # Modern color palette
    color1 = '#2E86AB'  # Professional blue
    color2 = '#A23B72'  # Professional purple/magenta
    color_p99 = '#E94F37'  # Red for p99
    color_p50 = '#44AF69'  # Green for p50
    
    # Plot TPS on left y-axis with enhanced styling (using errorbar)
    # capsize=5 adds caps to error bars
    line1 = ax1.errorbar(num_nodes, tps_means, yerr=tps_stds, fmt='o-', 
                        color=color1, linewidth=3, markersize=12, label='TPS', 
                        markerfacecolor=color1, markeredgecolor='white', 
                        markeredgewidth=2.5, zorder=3, alpha=0.9, capsize=5)
    
    # Add value annotations for TPS
    for i, (x, y) in enumerate(zip(num_nodes, tps_means)):
        label = f'{y:.0f}'
        # Optionally add error to label if significant
        # if tps_stds[i] > 0: label += f'±{tps_stds[i]:.0f}'
        
        ax1.annotate(label, (x, y), textcoords="offset points",
                    xytext=(0, 15), ha='center', fontsize=9, fontweight='bold',
                    color=color1, bbox=dict(boxstyle='round,pad=0.3', 
                    facecolor='white', edgecolor=color1, linewidth=1.5, alpha=0.8))
    
    ax1.set_xlabel('Number of Nodes (RayWorkers)', fontsize=13, fontweight='bold', color='#333333')
    ax1.set_ylabel('Total Throughput (Tokens per Second, TPS)', 
                   color=color1, fontsize=12, fontweight='bold')
    ax1.tick_params(axis='y', labelcolor=color1, labelsize=11)
    ax1.tick_params(axis='x', labelsize=11, colors='#333333')
    
    # Enhanced grid
    ax1.grid(True, alpha=0.4, linestyle='--', linewidth=0.8, color='#CCCCCC')
    ax1.set_axisbelow(True)
    
    # Create secondary axis for RPS
    ax2 = ax1.twinx()
    line2 = ax2.errorbar(num_nodes, rps_means, yerr=rps_stds, fmt='s-', 
                        color=color2, linewidth=3, markersize=12, label='RPS', 
                        markerfacecolor=color2, markeredgecolor='white', 
                        markeredgewidth=2.5, zorder=3, alpha=0.9, capsize=5)
    
    # Add value annotations for RPS
    for i, (x, y) in enumerate(zip(num_nodes, rps_means)):
        label = f'{y:.2f}'
        ax2.annotate(label, (x, y), textcoords="offset points",
                    xytext=(0, -25), ha='center', fontsize=9, fontweight='bold',
                    color=color2, bbox=dict(boxstyle='round,pad=0.3', 
                    facecolor='white', edgecolor=color2, linewidth=1.5, alpha=0.8))
    
    ax2.set_ylabel('Requests per Second (RPS)', color=color2, 
                   fontsize=12, fontweight='bold')
    ax2.tick_params(axis='y', labelcolor=color2, labelsize=11)
    
    # Create third axis for latency (right, offset from RPS)
    ax3 = ax1.twinx()
    ax3.spines['right'].set_position(('outward', 60))
    ax3.plot(num_nodes, p99_vals, '^-', color=color_p99, linewidth=2.5, markersize=10,
             label='p99 latency', zorder=3, alpha=0.9)
    ax3.plot(num_nodes, p50_vals, 'v-', color=color_p50, linewidth=2.5, markersize=10,
             label='p50 latency', zorder=3, alpha=0.9)
    # Scatter: min, max, mean latency
    ax3.scatter(num_nodes, lat_mins, marker='o', s=80, color='#3A7CA5', edgecolors='white',
                linewidths=2, label='min latency', zorder=4)
    ax3.scatter(num_nodes, lat_maxs, marker='s', s=80, color='#F18F01', edgecolors='white',
                linewidths=2, label='max latency', zorder=4)
    ax3.scatter(num_nodes, lat_means, marker='D', s=80, color='#5C4D7D', edgecolors='white',
                linewidths=2, label='mean latency', zorder=4)
    # Add value annotations for latency (stagger vertical offsets to reduce overlap)
    for i, (x, y) in enumerate(zip(num_nodes, p99_vals)):
        ax3.annotate(f'{y:.1f}', (x, y), textcoords="offset points", xytext=(0, 18), ha='center',
                     fontsize=9, fontweight='bold', color=color_p99,
                     bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor=color_p99, linewidth=1.5, alpha=0.8))
    for i, (x, y) in enumerate(zip(num_nodes, p50_vals)):
        ax3.annotate(f'{y:.1f}', (x, y), textcoords="offset points", xytext=(0, -22), ha='center',
                     fontsize=9, fontweight='bold', color=color_p50,
                     bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor=color_p50, linewidth=1.5, alpha=0.8))
    for i, (x, y) in enumerate(zip(num_nodes, lat_mins)):
        ax3.annotate(f'{y:.1f}', (x, y), textcoords="offset points", xytext=(8, 0), ha='left',
                     fontsize=8, fontweight='bold', color='#3A7CA5',
                     bbox=dict(boxstyle='round,pad=0.25', facecolor='white', edgecolor='#3A7CA5', linewidth=1.2, alpha=0.8))
    for i, (x, y) in enumerate(zip(num_nodes, lat_maxs)):
        ax3.annotate(f'{y:.1f}', (x, y), textcoords="offset points", xytext=(-8, 0), ha='right',
                     fontsize=8, fontweight='bold', color='#F18F01',
                     bbox=dict(boxstyle='round,pad=0.25', facecolor='white', edgecolor='#F18F01', linewidth=1.2, alpha=0.8))
    for i, (x, y) in enumerate(zip(num_nodes, lat_means)):
        ax3.annotate(f'{y:.1f}', (x, y), textcoords="offset points", xytext=(0, -8), ha='center',
                     fontsize=8, fontweight='bold', color='#5C4D7D',
                     bbox=dict(boxstyle='round,pad=0.25', facecolor='white', edgecolor='#5C4D7D', linewidth=1.2, alpha=0.8))
    ax3.set_ylabel('Latency (s)', color='#333333', fontsize=12, fontweight='bold')
    ax3.tick_params(axis='y', labelcolor='#333333', labelsize=11)
    ax3.spines['top'].set_visible(False)
    for spine in ax3.spines.values():
        spine.set_edgecolor('#DDDDDD')
        spine.set_linewidth(1.2)
    
    # Set axis scales
    if log_scale:
        ax1.set_xscale('log')
        ax1.set_yscale('log')
        ax2.set_yscale('log')
        ax3.set_yscale('log')
        
        # Use scalar formatter to show plain numbers (e.g., 100 instead of 10^2)
        for ax in [ax1, ax2, ax3]:
            formatter = ScalarFormatter()
            formatter.set_scientific(False)
            ax.yaxis.set_major_formatter(formatter)
            ax.yaxis.set_minor_formatter(formatter)
    else:
        ax1.set_xscale('linear')
        ax1.set_yscale('linear')
        ax2.set_yscale('linear')
        ax3.set_yscale('linear')

    ax1.set_xticks(num_nodes)
    ax1.set_xticklabels([f"{n}\n({n * 12} workers)" for n in num_nodes])
    
    if log_scale:
        ax1.set_xlim(min(num_nodes) * 0.8, max(num_nodes) * 1.2)
    else:
        # Add some padding for linear scale
        node_range = max(num_nodes) - min(num_nodes)
        padding = max(node_range * 0.05, 0.5)
        ax1.set_xlim(min(num_nodes) - padding, max(num_nodes) + padding)
    
    # Calculate ideal scaling line (linear scaling from first data point)
    # Ideal scaling means TPS and RPS should scale linearly with number of nodes
    if len(results) > 0:
        first_node_count = num_nodes[0]
        first_tps = tps_means[0]
        first_rps = rps_means[0]
        
        # Generate ideal scaling values (perfect linear scaling)
        ideal_tps = [first_tps * (n / first_node_count) for n in num_nodes]
        ideal_rps = [first_rps * (n / first_node_count) for n in num_nodes]
        
        # Plot ideal scaling lines (dashed); keep references for legend
        ideal_tps_line, = ax1.plot(num_nodes, ideal_tps, '--', color=color1, linewidth=2,
                                  alpha=0.6, label='Ideal TPS Scaling', zorder=2)
        ideal_rps_line, = ax2.plot(num_nodes, ideal_rps, '--', color=color2, linewidth=2,
                                  alpha=0.6, label='Ideal RPS Scaling', zorder=2)
    
    # Build legend from explicit handles/labels (errorbar() often doesn't set label on the Line2D)
    legend_handles = [line1.lines[0], line2.lines[0]]
    legend_labels = ['TPS', 'RPS']
    if len(results) > 0:
        legend_handles.extend([ideal_tps_line, ideal_rps_line])
        legend_labels.extend(['Ideal TPS Scaling', 'Ideal RPS Scaling'])
    # Add latency series (p99, p50 lines and min/max/mean scatter) from ax3
    for line in ax3.get_lines():
        legend_handles.append(line)
        legend_labels.append(line.get_label())
    for scatter in ax3.collections:
        legend_handles.append(scatter)
        legend_labels.append(scatter.get_label())
    legend = ax1.legend(legend_handles, legend_labels, loc='upper left', fontsize=10,
                        frameon=True, fancybox=True, shadow=True,
                        framealpha=0.95, edgecolor='#CCCCCC', facecolor='white')
    legend.get_frame().set_linewidth(1.5)
    
    # Remove top and right spines for cleaner look
    ax1.spines['top'].set_visible(False)
    ax2.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    
    # Make remaining spines more subtle
    for spine in ax1.spines.values():
        spine.set_edgecolor('#DDDDDD')
        spine.set_linewidth(1.2)
    for spine in ax2.spines.values():
        spine.set_edgecolor('#DDDDDD')
        spine.set_linewidth(1.2)
    
    # Enhanced title with better formatting
    title_text = 'RayServe vLLM Weak Scaling Performance (ALCF Aurora)'
    subtitle_text = 'Meta-Llama-3-8B-Instruct, chat mode, 12 GPUs per node, 3 runs avg'
    subtitle_text += '\n Note: Configuration may not be optimal. Single-node performance could be higher.'
    
    # title_text = 'RayServe Null Compute Weak Scaling Performance w/ Round-robin Clients (ALCF Aurora)'
    # # subtitle_text = '1 client per 8 nodes, 4 workers per client, 3 runs avg.'
    # subtitle_text += '\n No model staging, no tokenizer.'
    # subtitle_text += '\n Single-node performance is NOT saturated.'
    # subtitle_text += '\n Note: Preliminary results, we may not have implemented RayServe correctly.'
    fig.suptitle(title_text, fontsize=16, fontweight='bold', 
                color='#1a1a1a', y=0.98)
    ax1.set_title(subtitle_text, fontsize=11, color='#666666', pad=10, style='italic')
    
    # Adjust layout to prevent label cutoff
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    
    # Save or display
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        print(f"Plot saved to {output_path}")
    else:
        plt.show()


def main():
    """Main function to load results and generate plot."""
    parser = argparse.ArgumentParser(description='Plot weak scaling results')
    parser.add_argument("-e", "--experiment", type=str, required=True,
                       help=f"Experiment name from EXPERIMENT_REGISTRY. "
                            f"Available: {', '.join(EXPERIMENT_REGISTRY.keys())}.")
    parser.add_argument("-b", "--backend", type=str, default="ray",
                       help="Backend name used to resolve the experiment's batch_name "
                            "(e.g. 'ray', 'mpi'). Default: ray.")
    parser.add_argument("-o", "--output_path", type=str, default=None,
                       help="Path to save the output plot (default: weak_scaling_log.png or weak_scaling_linear.png)")
    parser.add_argument("--linear", action="store_true", 
                       help="Use linear scale for X and Y axes (default: log scale)")
    parser.add_argument("--indices", type=str, default=None,
                       help="Indices of experiments to aggregate (e.g. '0-5' or '0,1,2'). " 
                            "Calculates mean and error bars. Default: latest result only.")
    parser.add_argument("--exclude-nodes", type=str, default=None,
                       help="Comma-separated list of node counts to exclude (e.g., '64,128').")
    parser.add_argument("--node-list", type=str, default=None,
                       help="Comma-separated node counts paired with --select "
                            "(e.g., '1,2,4,8,16,32').")
    parser.add_argument("--select", type=str, default=None,
                       help="Comma-separated result indices, one per entry in --node-list "
                            "(e.g., '0,1,0,0,0,0'). Picks result{i}.json for each node. "
                            "Nodes not listed fall back to the latest result file.")
    args = parser.parse_args()

    if bool(args.node_list) != bool(args.select):
        parser.error("--node-list and --select must be used together.")

    # Resolve results folder from the experiment registry
    if args.experiment not in EXPERIMENT_REGISTRY:
        parser.error(
            f"Unknown experiment '{args.experiment}'. "
            f"Available: {', '.join(EXPERIMENT_REGISTRY.keys())}"
        )
    params = EXPERIMENT_REGISTRY[args.experiment]
    batch_name = params.batch_name.format(backend=args.backend)
    results_folder = os.path.join(DEFAULT_EXPERIMENTS_ROOT, batch_name)
    print(f"Experiment : {args.experiment}  (backend={args.backend})")
    print(f"Batch name : {batch_name}")

    # Determine default output filename
    if not args.output_path:
        suffix = "linear" if args.linear else "log"
        args.output_path = f"weak_scaling_{suffix}.png"

    node_select_map = None
    if args.node_list and args.select:
        node_select_map = parse_node_select_mapping(args.node_list, args.select)
        print(f"Per-node result selection: { {k: f'result{v}.json' for k, v in node_select_map.items()} }")

    target_indices = None
    if node_select_map is None:
        target_indices = parse_indices(args.indices)
        if target_indices:
            print(f"Aggregating results for indices: {target_indices}")
        
    excluded_nodes = parse_indices(args.exclude_nodes)
    if excluded_nodes:
        print(f"Excluding nodes: {excluded_nodes}")
    
    print(f"Loading results from: {results_folder}")
    results, missing_files = load_results(results_folder, target_indices, excluded_nodes, node_select_map)
    
    if missing_files:
        print("\n" + "!"*50)
        print(f"WARNING: {len(missing_files)} Missing result files:")
        for f in missing_files:
            print(f"  - {f}")
        print("!"*50 + "\n")
    
    if not results:
        print("No valid results found!")
        return
    
    print(f"\nFound {len(results)} node configurations")
    print("\nGenerating plot...")
    
    plot_weak_scaling(results, args.output_path, log_scale=not args.linear)


if __name__ == "__main__":
    main()
