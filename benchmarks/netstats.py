#!/usr/bin/env python3
"""
Network statistics collector — polls /proc/net/dev at a configurable interval
and writes cumulative counters as JSONL (one line per interface per sample).

The output is consumed by plot_internode.py, which computes deltas and rates.

Usage:
    python3 netstats.py --output /path/to/netstats.jsonl [--interval 1.0] [--interfaces hsn0,hsn1]
"""

import argparse
import json
import os
import signal
import socket
import sys
import time

_SHUTDOWN = False


def _handle_signal(signum, frame):
    global _SHUTDOWN
    _SHUTDOWN = True


def detect_interfaces():
    """Return all interface names from /proc/net/dev."""
    interfaces = []
    with open("/proc/net/dev") as f:
        for line in f:
            parts = line.strip().split()
            if not parts or ":" not in parts[0]:
                continue
            iface = parts[0].rstrip(":")
            interfaces.append(iface)
    return interfaces


def parse_proc_net_dev(interfaces):
    """Parse /proc/net/dev and return per-interface counters."""
    results = []
    with open("/proc/net/dev") as f:
        for line in f:
            parts = line.strip().split()
            if not parts or ":" not in parts[0]:
                continue
            iface = parts[0].rstrip(":")
            if iface not in interfaces:
                continue
            results.append({
                "interface": iface,
                "rx_bytes": int(parts[1]),
                "rx_packets": int(parts[2]),
                "rx_errors": int(parts[3]),
                "rx_drops": int(parts[4]),
                "tx_bytes": int(parts[9]),
                "tx_packets": int(parts[10]),
                "tx_errors": int(parts[11]),
                "tx_drops": int(parts[12]),
            })
    return results


def main():
    parser = argparse.ArgumentParser(description="Network stats collector via /proc/net/dev")
    parser.add_argument("--interval", type=float, default=1.0,
                        help="Polling interval in seconds (default: 1.0)")
    parser.add_argument("--interfaces", type=str, default=None,
                        help="Comma-separated interface names (default: all interfaces)")
    parser.add_argument("--output", type=str, required=True,
                        help="Output JSONL file path")
    parser.add_argument("--hostname", type=str, default=None,
                        help="Hostname label (default: $(hostname))")
    args = parser.parse_args()

    hostname = args.hostname or socket.gethostname()

    if args.interfaces:
        interfaces = set(args.interfaces.split(","))
    else:
        interfaces = set(detect_interfaces())

    if not interfaces:
        print("ERROR: No interfaces found to monitor", file=sys.stderr)
        sys.exit(1)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)

    with open(args.output, "w") as f:
        while not _SHUTDOWN:
            now = time.time()
            for record in parse_proc_net_dev(interfaces):
                record["timestamp"] = now
                record["hostname"] = hostname
                f.write(json.dumps(record) + "\n")
            f.flush()
            # Sleep in small increments to catch signals promptly
            deadline = now + args.interval
            while time.time() < deadline and not _SHUTDOWN:
                time.sleep(min(0.2, deadline - time.time()))

    print(f"netstats: stopped, wrote to {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
