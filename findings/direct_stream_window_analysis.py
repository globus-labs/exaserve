#!/usr/bin/env python3
"""Reproduce the direct-stream fixed-window analysis from checked run results."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import ijson

from eval.site_config import get_runs_root
from exaserve.state.atomic import atomic_write_json


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        raise ValueError("cannot compute a percentile from an empty sample")
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def result_files(runs_root: Path) -> dict[int, Path]:
    paper_root = runs_root / "sc26workshop" / "full"
    return {
        1: paper_root / "proxycmp_direct" / "run0" / "n1" / "results" / "result0.json",
        64: paper_root / "proxycmp_direct" / "run0" / "n64" / "results" / "result0.json",
        128: paper_root / "proxycmp_direct_scale" / "run0" / "n128" / "results" / "result0.json",
        256: paper_root / "proxycmp_direct_scale" / "run0" / "n256" / "results" / "result0.json",
    }


def analyze_result(path: Path) -> dict[int, dict[str, float | int]]:
    runs: dict[int, dict[str, list[float]]] = {}
    try:
        with path.open("rb") as handle:
            for request in ijson.items(handle, "requests.item"):
                run_index = int(request["run_index"])
                start = float(request["first_token_at"]) - float(request["ttft_s"])
                latency = float(request["latency"])
                row = runs.setdefault(
                    run_index, {"starts": [], "ends": [], "latencies": [], "tbt": []}
                )
                row["starts"].append(start)
                row["ends"].append(start + latency)
                row["latencies"].append(latency)
                tbt = request.get("tbt_p50_s")
                if tbt is not None:
                    row["tbt"].append(float(tbt))
    except (OSError, UnicodeError, ValueError, KeyError, ijson.JSONError) as exc:
        raise RuntimeError(f"cannot analyze result evidence {path}: {exc}") from exc

    if not runs:
        raise RuntimeError(f"result evidence {path} contains no request records")

    result: dict[int, dict[str, float | int]] = {}
    for run_index, row in sorted(runs.items()):
        if not row["tbt"]:
            raise RuntimeError(f"result evidence {path} run {run_index} contains no TBT samples")
        start_min, start_max = min(row["starts"]), max(row["starts"])
        end_max = max(row["ends"])
        count = len(row["latencies"])
        send_window = start_max - start_min
        makespan = end_max - start_min
        if send_window <= 0 or makespan <= 0:
            raise RuntimeError(f"result evidence {path} run {run_index} has invalid timing bounds")
        after_send = sum(1 for end in row["ends"] if end > start_max)
        result[run_index] = {
            "n": count,
            "send_window_s": round(send_window, 1),
            "makespan_s": round(makespan, 1),
            "drain_s": round(end_max - start_max, 1),
            "fraction_after_send": round(after_send / count, 4),
            "latency_p50_s": round(percentile(row["latencies"], 0.5), 2),
            "latency_p99_s": round(percentile(row["latencies"], 0.99), 2),
            "latency_max_s": round(max(row["latencies"]), 1),
            "tbt_p50_median_s": round(percentile(row["tbt"], 0.5), 4),
            "rps_measured": round(count / makespan, 1),
            "rps_send_window": round(count / send_window, 1),
            "send_window_fraction": round(send_window / makespan, 4),
        }
    return result


def analyze(runs_root: Path) -> dict[int, dict[int, dict[str, float | int]]]:
    return {nodes: analyze_result(path) for nodes, path in result_files(runs_root).items()}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--runs-root",
        type=Path,
        default=get_runs_root(),
        help="Experiment runs root (defaults to the centralized site configuration)",
    )
    parser.add_argument("--output", type=Path, help="Atomically write JSON to this path")
    args = parser.parse_args(argv)

    output: dict[str, Any] = analyze(args.runs_root)
    for nodes, runs in output.items():
        print(f"=== N={nodes} ===", file=sys.stderr)
        for run_index, values in runs.items():
            print(f" run{run_index}: {values}", file=sys.stderr)
    if args.output:
        atomic_write_json(args.output, output)
        print(f"wrote {args.output}", file=sys.stderr)
    else:
        print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
