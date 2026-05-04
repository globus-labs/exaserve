#!/usr/bin/env python3
"""Parse gcs_server.out RAY_event_stats blocks into a time-series of per-RPC
queueing + execution times.

Each block in gcs_server.out looks like:

    [2026-04-21 23:14:42,471 I 128845 128845] (gcs_server) gcs_server.cc:957: Main service Event stats:


    Global stats: 32 total (13 active)
    Event stats:
    	GcsInMemoryStore.Put - 9 total (6 active), Execution time: mean = 157.78ms, total = 1420.04ms, Queueing time: mean = 160.03ms, max = 1440.26ms, min = 0.00ms, total = 1440.31ms
    	PeriodicalRunner.RunFnPeriodically - 5 total (2 active, 1 running), Execution time: mean = 0.03ms, total = 0.15ms, Queueing time: mean = 580.63ms, max = 1441.07ms, min = 21.12ms, total = 2903.14ms
    	...
    --

Usage: parse_gcs_event_stats.py <gcs_server.out> [--csv OUTFILE]
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


BLOCK_HEADER_RE = re.compile(
    r"\[(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}).*?\] \(gcs_server\) gcs_server\.cc:\d+: "
    r"(?P<service>.+?) Event stats:"
)
GLOBAL_RE = re.compile(r"Global stats: (?P<total>\d+) total \((?P<active>\d+) active\)")
ROW_RE = re.compile(
    r"\s*(?P<method>\S+)\s+-\s+(?P<total>\d+) total\s+\((?P<active>\d+) active"
    r"(?:,\s+(?P<running>\d+) running)?\),\s+"
    r"Execution time: mean = (?P<exec_mean>[\d.]+)ms, total = (?P<exec_total>[\d.]+)ms, "
    r"Queueing time: mean = (?P<q_mean>[\d.]+)ms, max = (?P<q_max>[\d.]+)ms, "
    r"min = (?P<q_min>[-\d.]+)ms, total = (?P<q_total>[\d.]+)ms"
)


@dataclass
class Row:
    timestamp: str
    service: str
    method: str
    total: int
    active: int
    running: int
    exec_mean_ms: float
    exec_total_ms: float
    q_mean_ms: float
    q_max_ms: float
    q_min_ms: float
    q_total_ms: float


def parse(path: Path) -> list[Row]:
    rows: list[Row] = []
    with path.open() as f:
        lines = f.readlines()

    i = 0
    while i < len(lines):
        line = lines[i]
        m = BLOCK_HEADER_RE.search(line)
        if not m:
            i += 1
            continue
        ts = m.group("ts")
        service = m.group("service").strip()
        # Scan forward until we hit the "--" sentinel or another block header.
        j = i + 1
        while j < len(lines):
            l = lines[j].rstrip()
            if l == "--":
                break
            if BLOCK_HEADER_RE.search(l):
                break
            rm = ROW_RE.search(l)
            if rm:
                rows.append(
                    Row(
                        timestamp=ts,
                        service=service,
                        method=rm.group("method"),
                        total=int(rm.group("total")),
                        active=int(rm.group("active")),
                        running=int(rm.group("running") or 0),
                        exec_mean_ms=float(rm.group("exec_mean")),
                        exec_total_ms=float(rm.group("exec_total")),
                        q_mean_ms=float(rm.group("q_mean")),
                        q_max_ms=float(rm.group("q_max")),
                        q_min_ms=float(rm.group("q_min")),
                        q_total_ms=float(rm.group("q_total")),
                    )
                )
            j += 1
        i = j + 1
    return rows


def summarize(rows: list[Row]) -> None:
    """Aggregate per-method: max queueing p-mean, number of blocks observed, etc."""
    by_method: dict[tuple[str, str], list[Row]] = {}
    for r in rows:
        by_method.setdefault((r.service, r.method), []).append(r)

    # Service-method -> final cumulative counts + peak queueing time
    print(f"{'service':<24} {'method':<48} {'blocks':>7} {'final_total':>12} "
          f"{'peak_active':>12} {'peak_q_mean_ms':>15} {'peak_q_max_ms':>15}")
    for (svc, method), group in sorted(by_method.items()):
        final_total = group[-1].total
        peak_active = max(r.active for r in group)
        peak_q_mean = max(r.q_mean_ms for r in group)
        peak_q_max = max(r.q_max_ms for r in group)
        print(f"{svc:<24} {method:<48} {len(group):>7} {final_total:>12} "
              f"{peak_active:>12} {peak_q_mean:>15.2f} {peak_q_max:>15.2f}")


def write_csv(rows: list[Row], out_path: Path) -> None:
    with out_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "timestamp", "service", "method", "total", "active", "running",
            "exec_mean_ms", "exec_total_ms", "q_mean_ms", "q_max_ms", "q_min_ms", "q_total_ms",
        ])
        for r in rows:
            w.writerow([
                r.timestamp, r.service, r.method, r.total, r.active, r.running,
                r.exec_mean_ms, r.exec_total_ms, r.q_mean_ms, r.q_max_ms, r.q_min_ms, r.q_total_ms,
            ])


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("gcs_log", type=Path)
    p.add_argument("--csv", type=Path, help="Write per-block rows as CSV")
    args = p.parse_args()

    rows = parse(args.gcs_log)
    print(f"Parsed {len(rows)} rows across "
          f"{len({(r.service, r.method) for r in rows})} distinct service-methods "
          f"from {args.gcs_log}", file=sys.stderr)

    if args.csv:
        write_csv(rows, args.csv)
        print(f"CSV written to {args.csv}", file=sys.stderr)

    summarize(rows)


if __name__ == "__main__":
    main()
