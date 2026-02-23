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
import os
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set
import argparse

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

# Constant: folder path containing the experiment results
# RESULTS_FOLDER = "/home/wenyiw/agpt/data/results/weak_scaling_ray"
RESULTS_FOLDER = "/home/wenyiw/agpt/data/results/null_compute_ray"


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


def load_results(results_folder: str, target_indices: List[int] = None, excluded_nodes: List[int] = None) -> Tuple[List[Tuple], List[str]]:
    """
    Load results from all node configurations.
    
    Args:
        results_folder: Path to results
        target_indices: Optional list of indices to aggregate (e.g. [0, 1, 2]).
                        If None, uses the latest result file in each dir.
        excluded_nodes: Optional list of node counts to exclude from the plot.
                        
    Returns:
        results: List of tuples (num_nodes, tps_mean, tps_std, rps_mean, rps_std)
        missing_files: List of missing file paths (only if target_indices provided)
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
            
        tps_list = []
        rps_list = []
        
        if target_indices is not None:
            # Mode: Aggregate specific indices
            for idx in target_indices:
                fname = f"result{idx}.json"
                fpath = subdir / fname
                
                if not fpath.exists():
                    missing_files.append(str(fpath))
                    continue
                    
                try:
                    with open(fpath, 'r') as f:
                        data = json.load(f)
                    overall = data.get("overall", {})
                    tps_list.append(overall.get("tps", 0.0))
                    rps_list.append(overall.get("rps", 0.0))
                except Exception as e:
                    print(f"Error reading {fpath}: {e}")
            
            if not tps_list:
                print(f"Warning: No valid results found for {subdir.name} (indices {target_indices})")
                continue
                
            tps_mean = np.mean(tps_list)
            tps_std = np.std(tps_list) if len(tps_list) > 1 else 0.0
            rps_mean = np.mean(rps_list)
            rps_std = np.std(rps_list) if len(rps_list) > 1 else 0.0
            
            print(f"Loaded {subdir.name}: {len(tps_list)} runs. TPS={tps_mean:.2f}±{tps_std:.2f}, RPS={rps_mean:.2f}±{rps_std:.2f}")
            results.append((node_count, tps_mean, tps_std, rps_mean, rps_std))
            
        else:
            # Mode: Latest result only
            candidates = list(subdir.glob("result*.json"))
            if not candidates:
                print(f"Warning: No result files found in {subdir.name}, skipping")
                continue
                
            latest_file = None
            max_idx = -2
            
            for p in candidates:
                m = re.match(r"result(\d+)\.json", p.name)
                if m:
                    idx = int(m.group(1))
                elif p.name == "result.json":
                    idx = -1
                else:
                    continue
                    
                if idx > max_idx:
                    max_idx = idx
                    latest_file = p
            
            if not latest_file:
                 continue
                 
            print(f"  -> Using {latest_file.name} for {subdir.name}")
            
            try:
                with open(latest_file, 'r') as f:
                    data = json.load(f)
                overall = data.get("overall", {})
                tps = overall.get("tps", 0.0)
                rps = overall.get("rps", 0.0)
                
                # std is 0 for single run
                results.append((node_count, tps, 0.0, rps, 0.0))
                print(f"Loaded {subdir.name}: {node_count} nodes, TPS={tps:.2f}, RPS={rps:.2f}")
            except Exception as e:
                print(f"Error reading {latest_file}: {e}")
                continue
    
    # Sort by number of nodes
    results.sort(key=lambda x: x[0])
    return results, missing_files


def plot_weak_scaling(results: List[Tuple], output_path: str = None, log_scale: bool = False):
    """
    Plot weak scaling chart with dual y-axes.
    
    Args:
        results: List of (num_nodes, tps_mean, tps_std, rps_mean, rps_std) tuples
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
    
    # Set up the figure with a modern style
    fig, ax1 = plt.subplots(figsize=(12, 7))
    fig.patch.set_facecolor('white')
    ax1.set_facecolor('#FAFAFA')
    
    # Modern color palette
    color1 = '#2E86AB'  # Professional blue
    color2 = '#A23B72'  # Professional purple/magenta
    
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
    
    ax1.set_xlabel('Number of Nodes', fontsize=13, fontweight='bold', color='#333333')
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
    
    # Set axis scales
    if log_scale:
        ax1.set_xscale('log')
        ax1.set_yscale('log')
        ax2.set_yscale('log')
        
        # Use scalar formatter to show plain numbers (e.g., 100 instead of 10^2)
        for ax in [ax1, ax2]:
            formatter = ScalarFormatter()
            formatter.set_scientific(False)
            ax.yaxis.set_major_formatter(formatter)
            ax.yaxis.set_minor_formatter(formatter)
    else:
        ax1.set_xscale('linear')
        ax1.set_yscale('linear')
        ax2.set_yscale('linear')

    ax1.set_xticks(num_nodes)
    ax1.set_xticklabels([str(n) for n in num_nodes])
    
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
    legend = ax1.legend(legend_handles, legend_labels, loc='upper left', fontsize=11,
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
    subtitle_text = 'Meta-Llama-3-8B-Instruct, chat mode, 12 GPUs per node, 5 runs avg'
    subtitle_text += '\n Note: Preliminary results, we may not have implemented RayServe correctly.'
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
    parser.add_argument("--output_path", type=str, default=None,
                       help="Path to save the output plot (default: weak_scaling_log.png or weak_scaling_linear.png)")
    parser.add_argument("--linear", action="store_true", 
                       help="Use linear scale for X and Y axes (default: log scale)")
    parser.add_argument("--indices", type=str, default=None,
                       help="Indices of experiments to aggregate (e.g. '0-5' or '0,1,2'). " 
                            "Calculates mean and error bars. Default: latest result only.")
    parser.add_argument("--exclude-nodes", type=str, default=None,
                       help="Comma-separated list of node counts to exclude (e.g., '64,128').")
    args = parser.parse_args()
    
    # Determine default output filename
    if not args.output_path:
        suffix = "linear" if args.linear else "log"
        args.output_path = f"weak_scaling_{suffix}.png"
    
    target_indices = parse_indices(args.indices)
    if target_indices:
        print(f"Aggregating results for indices: {target_indices}")
        
    excluded_nodes = parse_indices(args.exclude_nodes)
    if excluded_nodes:
        print(f"Excluding nodes: {excluded_nodes}")
    
    print(f"Loading results from: {RESULTS_FOLDER}")
    results, missing_files = load_results(RESULTS_FOLDER, target_indices, excluded_nodes)
    
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
