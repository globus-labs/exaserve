"""
Ephemeral port usage model and live monitor.

Two subcommands:

  predict  -- Estimate peak ephemeral port consumption given workload parameters.
              Warns when the predicted usage approaches the available range
              (Linux default: 32768-60999 = ~28,231 ports).

  monitor  -- Poll /proc/net/tcp{,6} (or fall back to `ss`) every N seconds and
              write a time-series JSON file.  Can run as a background process
              during benchmark runs.

Predictive model formulas
-------------------------
Ephemeral ports are consumed by TCP connections.  A connection stays in
TIME_WAIT for ~60 s after close, still occupying the port.

HTTP/1.1 (no keepalive or short keepalive):
  Each request may need a new connection.
  peak_ports = min(rps * time_wait_s, pool_size * num_workers)

HTTP/1.1 with keepalive (httpx default: keepalive_expiry=4s):
  Connections are reused while the server keeps them alive.  Port churn is
  much lower.  We model it as:
  active_connections ≈ min(rps * avg_latency_s, pool_size * num_workers)
  time_wait_churn    ≈ rps * time_wait_s  (if requests outlive the keepalive)

HTTP/2:
  One or a handful of TCP connections multiplex many streams.
  peak_ports ≈ pool_size * num_workers  (much smaller)

For multi-node scenarios, multiply by the number of client nodes.

Usage
-----
  python port_model.py predict \\
      --rps 1000 --workers 8 --pool-size 100 --http2 --nodes 4

  python port_model.py monitor \\
      --interval 1 --duration 60 --output port_usage.json
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
import threading
from dataclasses import dataclass, asdict
from typing import Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EPHEMERAL_MIN = 32768
EPHEMERAL_MAX = 60999
EPHEMERAL_RANGE = EPHEMERAL_MAX - EPHEMERAL_MIN + 1  # 28,232 on most Linux

WARN_FRACTION = 0.80  # warn above 80 %
DANGER_FRACTION = 0.95  # critical above 95 %

# TCP state codes in /proc/net/tcp (hex)
_TCP_STATES = {
    "01": "ESTABLISHED",
    "02": "SYN_SENT",
    "03": "SYN_RECV",
    "04": "FIN_WAIT1",
    "05": "FIN_WAIT2",
    "06": "TIME_WAIT",
    "07": "CLOSE",
    "08": "CLOSE_WAIT",
    "09": "LAST_ACK",
    "0A": "LISTEN",
    "0B": "CLOSING",
}


# ---------------------------------------------------------------------------
# Predictive model
# ---------------------------------------------------------------------------


@dataclass
class PortPrediction:
    rps: float
    num_workers: int
    pool_size_per_worker: int
    http_version: str  # "1.1" or "2"
    keepalive_expiry_s: float  # httpx default 4 s
    time_wait_s: float  # OS default 60 s
    avg_latency_s: float  # assumed server response latency
    num_client_nodes: int

    # --- derived ---
    active_connections_per_node: float = 0.0
    time_wait_per_node: float = 0.0
    peak_ports_per_node: float = 0.0
    total_peak_ports: float = 0.0
    fraction_used: float = 0.0
    available_range: int = EPHEMERAL_RANGE
    status: str = "OK"
    warnings: list = None

    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []
        self._compute()

    def _compute(self):
        pool = self.pool_size_per_worker * self.num_workers

        if self.http_version == "2":
            # HTTP/2 multiplexes; port count bounded by the connection pool
            self.active_connections_per_node = min(pool, self.rps * self.avg_latency_s)
            self.time_wait_per_node = pool  # upper bound; actual churn is very low
            self.peak_ports_per_node = pool
        else:
            # HTTP/1.1 with keepalive:
            # Active connections ≈ Little's law: λ * W
            self.active_connections_per_node = min(pool, self.rps * self.avg_latency_s)
            # TIME_WAIT churn: requests that arrive faster than keepalive drains them
            # Each request may close the connection after keepalive_expiry
            churn_rate = max(0.0, self.rps - pool / self.keepalive_expiry_s)
            self.time_wait_per_node = min(
                churn_rate * self.time_wait_s,
                pool * (self.time_wait_s / self.keepalive_expiry_s),
            )
            self.peak_ports_per_node = self.active_connections_per_node + self.time_wait_per_node

        self.total_peak_ports = self.peak_ports_per_node * self.num_client_nodes
        self.fraction_used = self.total_peak_ports / self.available_range

        if self.fraction_used >= DANGER_FRACTION:
            self.status = "DANGER"
            self.warnings.append(
                f"PORT EXHAUSTION LIKELY: predicted {self.total_peak_ports:.0f} ports "
                f"({self.fraction_used * 100:.1f}% of {self.available_range})"
            )
        elif self.fraction_used >= WARN_FRACTION:
            self.status = "WARN"
            self.warnings.append(
                f"Port usage high: predicted {self.total_peak_ports:.0f} ports "
                f"({self.fraction_used * 100:.1f}% of {self.available_range})"
            )
        else:
            self.status = "OK"


def predict(
    rps: float,
    num_workers: int,
    pool_size_per_worker: int = 100,
    http_version: str = "2",
    keepalive_expiry_s: float = 4.0,
    time_wait_s: float = 60.0,
    avg_latency_s: float = 1.0,
    num_client_nodes: int = 1,
) -> PortPrediction:
    """Return a PortPrediction for the given workload parameters."""
    return PortPrediction(
        rps=rps,
        num_workers=num_workers,
        pool_size_per_worker=pool_size_per_worker,
        http_version=http_version,
        keepalive_expiry_s=keepalive_expiry_s,
        time_wait_s=time_wait_s,
        avg_latency_s=avg_latency_s,
        num_client_nodes=num_client_nodes,
    )


def print_prediction(p: PortPrediction):
    """Pretty-print a PortPrediction to stdout."""
    print("\n" + "=" * 60)
    print("  Ephemeral Port Usage Prediction")
    print("=" * 60)
    print(f"  RPS:               {p.rps}")
    print(f"  Workers:           {p.num_workers}")
    print(f"  Pool / worker:     {p.pool_size_per_worker}")
    print(f"  HTTP version:      {p.http_version}")
    print(f"  Keepalive expiry:  {p.keepalive_expiry_s}s")
    print(f"  TIME_WAIT:         {p.time_wait_s}s")
    print(f"  Avg latency:       {p.avg_latency_s}s")
    print(f"  Client nodes:      {p.num_client_nodes}")
    print("-" * 60)
    print(f"  Active conns/node: {p.active_connections_per_node:.1f}")
    print(f"  TIME_WAIT/node:    {p.time_wait_per_node:.1f}")
    print(f"  Peak ports/node:   {p.peak_ports_per_node:.1f}")
    print(f"  Total peak ports:  {p.total_peak_ports:.1f}")
    print(f"  Ephemeral range:   {EPHEMERAL_MIN}-{EPHEMERAL_MAX} ({p.available_range} ports)")
    print(f"  Usage fraction:    {p.fraction_used * 100:.1f}%")
    print(f"  Status:            {p.status}")
    for w in p.warnings:
        print(f"  !! {w}")
    print("=" * 60 + "\n")


# ---------------------------------------------------------------------------
# Live monitor
# ---------------------------------------------------------------------------


@dataclass
class PortSnapshot:
    timestamp: float
    established: int
    time_wait: int
    syn_sent: int
    close_wait: int
    total_tcp: int
    ephemeral_in_use: int  # estimated from TIME_WAIT + ESTABLISHED with ephemeral src


def _read_proc_net_tcp() -> list[PortSnapshot]:
    """
    Parse /proc/net/tcp and /proc/net/tcp6.
    Returns a single PortSnapshot aggregating both IPv4 and IPv6.
    """
    counts = {state: 0 for state in _TCP_STATES.values()}
    ephemeral = 0

    for path in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("sl"):
                        continue
                    parts = line.split()
                    if len(parts) < 4:
                        continue
                    state_hex = parts[3].upper()
                    state_name = _TCP_STATES.get(state_hex, "UNKNOWN")
                    counts[state_name] = counts.get(state_name, 0) + 1

                    # Local port is the second field, format: hex_ip:hex_port
                    local = parts[1]
                    if ":" in local:
                        try:
                            local_port = int(local.split(":")[1], 16)
                            if EPHEMERAL_MIN <= local_port <= EPHEMERAL_MAX:
                                if state_name in ("ESTABLISHED", "TIME_WAIT", "SYN_SENT"):
                                    ephemeral += 1
                        except ValueError:
                            pass
        except (FileNotFoundError, PermissionError):
            pass

    return PortSnapshot(
        timestamp=time.time(),
        established=counts.get("ESTABLISHED", 0),
        time_wait=counts.get("TIME_WAIT", 0),
        syn_sent=counts.get("SYN_SENT", 0),
        close_wait=counts.get("CLOSE_WAIT", 0),
        total_tcp=sum(counts.values()),
        ephemeral_in_use=ephemeral,
    )


def _read_ss_fallback() -> Optional[PortSnapshot]:
    """Fall back to `ss -s` for basic state counts when /proc/net/tcp is unavailable."""
    try:
        out = subprocess.check_output(["ss", "-s"], text=True, timeout=5)
    except (FileNotFoundError, subprocess.TimeoutExpired, subprocess.CalledProcessError):
        return None

    established = time_wait = 0
    for line in out.splitlines():
        m = re.search(r"estab\s+(\d+)", line, re.IGNORECASE)
        if m:
            established = int(m.group(1))
        m = re.search(r"timewait\s+(\d+)", line, re.IGNORECASE)
        if m:
            time_wait = int(m.group(1))

    return PortSnapshot(
        timestamp=time.time(),
        established=established,
        time_wait=time_wait,
        syn_sent=0,
        close_wait=0,
        total_tcp=established + time_wait,
        ephemeral_in_use=-1,  # not available via ss -s
    )


def _take_snapshot() -> PortSnapshot:
    snap = _read_proc_net_tcp()
    if snap.total_tcp == 0:
        fallback = _read_ss_fallback()
        if fallback:
            return fallback
    return snap


class PortMonitor:
    """Background thread that polls port usage at a fixed interval."""

    def __init__(self, interval_s: float = 1.0, output_path: Optional[str] = None):
        self.interval_s = interval_s
        self.output_path = output_path
        self._samples: list[PortSnapshot] = []
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self):
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=self.interval_s + 2)

    def get_samples(self) -> list[PortSnapshot]:
        return list(self._samples)

    def _run(self):
        while not self._stop_event.is_set():
            snap = _take_snapshot()
            self._samples.append(snap)
            frac = snap.ephemeral_in_use / EPHEMERAL_RANGE if snap.ephemeral_in_use >= 0 else 0
            status = (
                "DANGER" if frac >= DANGER_FRACTION else ("WARN" if frac >= WARN_FRACTION else "OK")
            )
            print(
                f"[PortMonitor] ESTAB={snap.established:5d}  TW={snap.time_wait:5d}  "
                f"EPHEM={snap.ephemeral_in_use:5d}/{EPHEMERAL_RANGE}  [{status}]",
                flush=True,
            )
            self._stop_event.wait(self.interval_s)

        if self.output_path:
            self._save()

    def _save(self):
        data = [asdict(s) for s in self._samples]
        os.makedirs(os.path.dirname(os.path.abspath(self.output_path)), exist_ok=True)
        with open(self.output_path, "w") as f:
            json.dump({"samples": data, "ephemeral_range": EPHEMERAL_RANGE}, f, indent=2)
        print(f"[PortMonitor] Saved {len(data)} samples to {self.output_path}", flush=True)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _cmd_predict(args):
    http_ver = "2" if args.http2 else "1.1"
    p = predict(
        rps=args.rps,
        num_workers=args.workers,
        pool_size_per_worker=args.pool_size,
        http_version=http_ver,
        keepalive_expiry_s=args.keepalive_expiry,
        time_wait_s=args.time_wait,
        avg_latency_s=args.avg_latency,
        num_client_nodes=args.nodes,
    )
    print_prediction(p)
    if args.json:
        d = asdict(p)
        print(json.dumps(d, indent=2))
    return 0 if p.status == "OK" else 1


def _cmd_monitor(args):
    monitor = PortMonitor(interval_s=args.interval, output_path=args.output)
    print(
        f"[PortMonitor] Monitoring every {args.interval}s for {args.duration}s "
        f"-> {args.output or '(no file)'}",
        flush=True,
    )
    monitor.start()
    try:
        time.sleep(args.duration)
    except KeyboardInterrupt:
        pass
    finally:
        monitor.stop()
    return 0


def main():
    parser = argparse.ArgumentParser(description="Ephemeral port usage model and monitor.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # --- predict ---
    p = sub.add_parser("predict", help="Predict ephemeral port usage from workload parameters.")
    p.add_argument("--rps", type=float, required=True, help="Requests per second")
    p.add_argument("--workers", type=int, default=4, help="Number of client workers")
    p.add_argument(
        "--pool-size", type=int, default=100, help="HTTP connection pool size per worker"
    )
    p.add_argument("--http2", action="store_true", help="Use HTTP/2 model (much lower port usage)")
    p.add_argument(
        "--keepalive-expiry", type=float, default=4.0, help="Keepalive expiry in seconds (HTTP/1.1)"
    )
    p.add_argument(
        "--time-wait", type=float, default=60.0, help="TCP TIME_WAIT duration in seconds"
    )
    p.add_argument(
        "--avg-latency", type=float, default=1.0, help="Expected average server latency in seconds"
    )
    p.add_argument("--nodes", type=int, default=1, help="Number of client nodes")
    p.add_argument("--json", action="store_true", help="Also print JSON output")

    # --- monitor ---
    m = sub.add_parser("monitor", help="Live monitor of ephemeral port usage.")
    m.add_argument("--interval", type=float, default=1.0, help="Polling interval in seconds")
    m.add_argument(
        "--duration", type=float, default=60.0, help="Total monitoring duration in seconds"
    )
    m.add_argument("--output", type=str, default="port_usage.json", help="Output JSON file path")

    args = parser.parse_args()
    if args.cmd == "predict":
        sys.exit(_cmd_predict(args))
    elif args.cmd == "monitor":
        sys.exit(_cmd_monitor(args))


if __name__ == "__main__":
    main()
