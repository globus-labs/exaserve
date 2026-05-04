#!/usr/bin/env python3
"""
Parse PBS job output logs and report router worker locality.
Filters lines matching the Router Ready print format and counts
unique (pid, hostname) pairs per hostname.

Usage: python router_locality.py <pbs_output_file> [<pbs_output_file> ...]
"""

import re
import sys
from collections import defaultdict

# Matches: [Router pid=<pid>,hostname=<host>] Ready (model=<model>)
PATTERN = re.compile(
    r"\[Router pid=(\d+),hostname=([^\]]+)\] Ready \(model=([^\)]+)\)"
)


def parse_file(path: str) -> dict[str, set[int]]:
    hostname_pids: dict[str, set[int]] = defaultdict(set)
    with open(path) as f:
        for line in f:
            m = PATTERN.search(line)
            if m:
                pid, hostname = int(m.group(1)), m.group(2)
                hostname_pids[hostname].add(pid)
    return hostname_pids


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <pbs_output_file> [...]", file=sys.stderr)
        sys.exit(1)

    combined: dict[str, set[int]] = defaultdict(set)
    for path in sys.argv[1:]:
        for hostname, pids in parse_file(path).items():
            combined[hostname] |= pids

    if not combined:
        print("No Router Ready lines found.")
        sys.exit(0)

    total = sum(len(pids) for pids in combined.values())
    for hostname in sorted(combined):
        print(f"{hostname}:{len(combined[hostname])}")
    print(f"total:{total}")


if __name__ == "__main__":
    main()
