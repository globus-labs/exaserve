#!/usr/bin/env python3
"""
Analyze aurora scaling trace JSON files to identify bottlenecks.

Usage:
    python eval/scripts/analyze_scaling_trace.py /tmp/aurora_scaling_trace_*.json
    python eval/scripts/analyze_scaling_trace.py trace_1node.json trace_4node.json trace_64node.json
"""

import json
import sys
from pathlib import Path


def load_trace(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def fmt_s(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds*1000:.0f}ms"
    return f"{seconds:.2f}s"


def analyze_one(trace: dict, label: str = "") -> None:
    meta = trace.get("metadata", {})
    phases = trace.get("phases", [])
    api_calls = trace.get("api_calls", [])
    replicas = trace.get("replicas", [])
    driver_phases = trace.get("driver_phases", [])

    node_count = meta.get("num_nodes", "?")
    gpu_count = meta.get("actual_gpus", meta.get("num_gpus_per_node", "?"))
    total_time = meta.get("total_time_s", meta.get("total_duration_s", "?"))

    header = f"=== {label or meta.get('hostname', 'unknown')} | {node_count} nodes | {gpu_count} GPUs | total={fmt_s(total_time) if isinstance(total_time, (int, float)) else total_time} ==="
    print(header)
    print()

    # Phase breakdown
    print("  PHASE BREAKDOWN:")
    for phase in sorted(phases, key=lambda p: p.get("wall_start", 0)):
        name = phase["name"]
        dur = phase["duration_s"]
        extras = {k: v for k, v in phase.items() if k not in ("name", "duration_s", "wall_start", "wall_end", "mono_start")}
        extra_str = ""
        if extras:
            extra_str = "  " + ", ".join(f"{k}={v}" for k, v in extras.items())
        print(f"    {name:40s} {fmt_s(dur):>10s}{extra_str}")
    print()

    # API call latency stats
    if api_calls:
        # Group by label
        call_groups: dict[str, list[float]] = {}
        for call in api_calls:
            label_key = call.get("loop_name") or call.get("label", "unknown")
            call_groups.setdefault(label_key, []).append(call["duration_s"])

        print("  API CALL LATENCY (per-call):")
        for label_key, durations in sorted(call_groups.items()):
            n = len(durations)
            total = sum(durations)
            mean = total / n
            mx = max(durations)
            mn = min(durations)
            print(f"    {label_key:40s}  count={n:4d}  total={fmt_s(total):>10s}  mean={fmt_s(mean):>8s}  min={fmt_s(mn):>8s}  max={fmt_s(mx):>8s}")
        print()

    # Node registration convergence
    poll_calls = [c for c in api_calls if c.get("loop_name") == "node_registration"]
    if poll_calls:
        print("  NODE REGISTRATION CONVERGENCE:")
        for p in poll_calls:
            it = p.get("iteration", "?")
            gpus = p.get("total_gpus", "?")
            expected = p.get("expected_gpus", "?")
            pct = p.get("pct", "?")
            nodes = p.get("alive_nodes", "?")
            dur = p.get("duration_s", 0)
            print(f"    iter={it:3}  nodes={nodes:4}  gpus={gpus}/{expected} ({pct}%)  poll_latency={fmt_s(dur)}")
        print()

    # Replica init breakdown
    if replicas:
        print(f"  REPLICA INIT BREAKDOWN ({len(replicas)} replicas):")
        init_times = [r.get("total_init_s", 0) for r in replicas]
        engine_times = [r.get("engine_create_s", 0) for r in replicas]

        print(f"    total_init:    min={fmt_s(min(init_times))}  max={fmt_s(max(init_times))}  mean={fmt_s(sum(init_times)/len(init_times))}")
        if any(engine_times):
            print(f"    engine_create: min={fmt_s(min(engine_times))}  max={fmt_s(max(engine_times))}  mean={fmt_s(sum(engine_times)/len(engine_times))}")

        # Show per-host breakdown
        hosts: dict[str, list] = {}
        for r in replicas:
            hosts.setdefault(r.get("hostname", "?"), []).append(r)
        if len(hosts) > 1:
            print(f"\n    Per-host replica init ({len(hosts)} hosts):")
            for host, host_replicas in sorted(hosts.items()):
                times = [r.get("total_init_s", 0) for r in host_replicas]
                print(f"      {host:20s}  n={len(times):3d}  mean={fmt_s(sum(times)/len(times))}  max={fmt_s(max(times))}")

        # Show wall-clock timeline: earliest start to latest end
        starts = [r.get("wall_start", 0) for r in replicas if r.get("wall_start")]
        ends = [r.get("wall_end", 0) for r in replicas if r.get("wall_end")]
        if starts and ends:
            span = max(ends) - min(starts)
            print(f"\n    Wall-clock span (first replica start → last replica done): {fmt_s(span)}")
            print(f"    Serialization overhead: {fmt_s(span - max(init_times))} "
                  f"(span - longest single init)")
        print()

    # Driver phases (from all nodes)
    if driver_phases:
        # Group by source
        sources: dict[str, list] = {}
        for dp in driver_phases:
            src = dp.get("source", "unknown")
            sources.setdefault(src, []).append(dp)

        print(f"  DRIVER PHASES ({len(driver_phases)} entries from {len(sources)} node(s)):")
        for src, src_phases in sorted(sources.items()):
            print(f"    [{src}]")
            for p in sorted(src_phases, key=lambda x: x.get("wall_end", 0)):
                print(f"      {p['name']:36s} {fmt_s(p['duration_s']):>10s}")
        print()

    print()


def compare_traces(traces: list[tuple[str, dict]]) -> None:
    """Print a comparison table across multiple runs."""
    if len(traces) < 2:
        return

    print("=" * 80)
    print("SCALING COMPARISON")
    print("=" * 80)

    # Collect key phases across all traces
    all_phase_names: set[str] = set()
    for _, trace in traces:
        for phase in trace.get("phases", []):
            all_phase_names.add(phase["name"])

    # Header
    col_width = 14
    header = f"{'phase':<40s}"
    for label, trace in traces:
        meta = trace.get("metadata", {})
        n = meta.get("num_nodes", "?")
        header += f"  {f'{n}N':>{col_width}s}"
    print(header)
    print("-" * len(header))

    # Show each phase
    for phase_name in sorted(all_phase_names):
        row = f"{phase_name:<40s}"
        for _, trace in traces:
            matching = [p for p in trace.get("phases", []) if p["name"] == phase_name]
            if matching:
                dur = matching[0]["duration_s"]
                row += f"  {fmt_s(dur):>{col_width}s}"
            else:
                row += f"  {'—':>{col_width}s}"
        print(row)

    # Total time row
    row = f"{'TOTAL':.<40s}"
    for _, trace in traces:
        total = trace.get("metadata", {}).get("total_time_s", "?")
        if isinstance(total, (int, float)):
            row += f"  {fmt_s(total):>{col_width}s}"
        else:
            row += f"  {'?':>{col_width}s}"
    print(row)
    print()


def main() -> None:
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <trace.json> [trace2.json ...]")
        sys.exit(1)

    traces: list[tuple[str, dict]] = []
    for path_str in sys.argv[1:]:
        path = Path(path_str)
        if not path.exists():
            print(f"WARNING: {path} not found, skipping")
            continue
        trace = load_trace(str(path))
        traces.append((path.stem, trace))

    for label, trace in traces:
        analyze_one(trace, label)

    if len(traces) >= 2:
        compare_traces(traces)


if __name__ == "__main__":
    main()
