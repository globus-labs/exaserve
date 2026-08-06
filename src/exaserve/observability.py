"""ExaServe-owned operational metrics contract (plan WP10, audit PR-032, TD-METRICS).

An operator asking "is this deployment healthy right now?" had two options:
read the driver's stdout, or scrape Ray's dashboard — which is disabled on this
stack. Neither is an ExaServe contract, and neither survives the log being
rotated or the dashboard being off.

This module is the contract: a small, bounded, dependency-free registry that
each replica exposes at ``/metrics`` in Prometheus text format, carrying the
identity every other subsystem already agrees on (deployment id, generation,
node, replica) plus the correlation id that ties a request across the proxy,
the replica, and the engine.

Deliberately not a Prometheus client dependency: the counters are few, the
format is trivial, and adding a library to the replica import path costs
startup time on every one of hundreds of replicas.
"""

from __future__ import annotations

import os
import socket
import threading
import time
from typing import Dict, Iterable, Optional, Tuple

# Cardinality is the way metrics endpoints turn into outages. Every series is
# declared here; a label set that would create an unbounded number of series
# (per-request ids, per-model-path strings) is rejected rather than accepted
# and later discovered as a memory leak.
_MAX_SERIES = 512

CORRELATION_HEADER = "x-request-id"


class _Registry:
    """Thread-safe counters/gauges with bounded cardinality."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: Dict[Tuple[str, Tuple[Tuple[str, str], ...]], float] = {}
        self._gauges: Dict[Tuple[str, Tuple[Tuple[str, str], ...]], float] = {}
        self._help: Dict[str, str] = {}
        self.dropped_series = 0

    @staticmethod
    def _key(name: str, labels: Optional[dict]) -> Tuple[str, Tuple[Tuple[str, str], ...]]:
        items = tuple(sorted((str(k), str(v)) for k, v in (labels or {}).items()))
        return name, items

    def _room(self, key) -> bool:
        if key in self._counters or key in self._gauges:
            return True
        if len(self._counters) + len(self._gauges) >= _MAX_SERIES:
            self.dropped_series += 1
            return False
        return True

    def declare(self, name: str, help_text: str) -> None:
        with self._lock:
            self._help[name] = help_text

    def inc(self, name: str, value: float = 1.0, labels: Optional[dict] = None) -> None:
        key = self._key(name, labels)
        with self._lock:
            if not self._room(key):
                return
            self._counters[key] = self._counters.get(key, 0.0) + value

    def set(self, name: str, value: float, labels: Optional[dict] = None) -> None:
        key = self._key(name, labels)
        with self._lock:
            if not self._room(key):
                return
            self._gauges[key] = float(value)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "dropped_series": self.dropped_series,
            }

    def reset(self) -> None:
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self.dropped_series = 0

    # -- rendering ---------------------------------------------------------
    def render(self, extra_labels: Optional[dict] = None) -> str:
        """Prometheus text exposition. Stable ordering so diffs are readable."""
        base = {str(k): str(v) for k, v in (extra_labels or {}).items()}
        lines: list[str] = []
        with self._lock:
            emitted: set = set()
            for kind, store in (("counter", self._counters), ("gauge", self._gauges)):
                for (name, labels) in sorted(store):
                    if name not in emitted:
                        emitted.add(name)
                        help_text = self._help.get(name, name)
                        lines.append(f"# HELP {name} {help_text}")
                        lines.append(f"# TYPE {name} {kind}")
                    merged = dict(base)
                    merged.update(dict(labels))
                    rendered = ",".join(
                        f'{k}="{_escape(v)}"' for k, v in sorted(merged.items()))
                    suffix = f"{{{rendered}}}" if rendered else ""
                    lines.append(f"{name}{suffix} {_number(store[(name, labels)])}")
            lines.append("# HELP exaserve_metrics_dropped_series_total "
                         "Series refused because the cardinality cap was reached")
            lines.append("# TYPE exaserve_metrics_dropped_series_total counter")
            lines.append(f"exaserve_metrics_dropped_series_total {self.dropped_series}")
        return "\n".join(lines) + "\n"


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _number(value: float) -> str:
    return str(int(value)) if float(value).is_integer() else repr(float(value))


REGISTRY = _Registry()

REGISTRY.declare("exaserve_requests_total", "Requests handled, by route and outcome")
REGISTRY.declare("exaserve_request_duration_seconds_total",
                 "Cumulative request service time, by route")
REGISTRY.declare("exaserve_tokens_total", "Tokens produced, by kind (prompt/completion)")
REGISTRY.declare("exaserve_replica_up", "1 while this replica is serving")
REGISTRY.declare("exaserve_replica_start_time_seconds",
                 "Unix time at which this replica became ready")


def identity_labels() -> dict:
    """The identity every ExaServe subsystem already agrees on (WP10.2).

    Deployment id is normalized the same way the receipt channel normalizes it,
    so a metric, a receipt, and a readiness snapshot can be joined on it.
    """
    from .compat.collector import deployment_scope

    return {
        "deployment_id": deployment_scope(),
        "generation": os.environ.get("EXASERVE_GENERATION", "0") or "0",
        "node": socket.gethostname(),
        "pid": str(os.getpid()),
    }


def record_request(route: str, outcome: str, duration_s: float,
                   prompt_tokens: int = 0, completion_tokens: int = 0) -> None:
    """One request's operational facts. Labels are bounded by construction."""
    REGISTRY.inc("exaserve_requests_total", 1.0,
                 {"route": route, "outcome": outcome})
    REGISTRY.inc("exaserve_request_duration_seconds_total", float(duration_s),
                 {"route": route})
    if prompt_tokens:
        REGISTRY.inc("exaserve_tokens_total", float(prompt_tokens), {"kind": "prompt"})
    if completion_tokens:
        REGISTRY.inc("exaserve_tokens_total", float(completion_tokens),
                     {"kind": "completion"})


def mark_replica_ready() -> None:
    REGISTRY.set("exaserve_replica_up", 1.0)
    REGISTRY.set("exaserve_replica_start_time_seconds", time.time())


def render_metrics() -> str:
    return REGISTRY.render(identity_labels())


def correlation_id(headers, fallback: str) -> str:
    """Propagate the caller's id when present; otherwise mint one (WP10.2).

    Accepting the caller's id is what makes a proxy log line and a replica
    metric joinable; minting one when absent is what stops a request from being
    untraceable just because the client did not supply a header.
    """
    try:
        supplied = headers.get(CORRELATION_HEADER)
    except AttributeError:
        supplied = None
    if not supplied:
        return fallback
    # A header is caller-controlled input: bound it and strip anything that
    # would break a log line or a metric label.
    cleaned = "".join(c for c in str(supplied) if c.isprintable() and c not in '"\\\n')
    return cleaned[:128] or fallback


def bounded_series() -> Tuple[int, int]:
    """(current series, cap) — for tests and the /metrics self-report."""
    snapshot = REGISTRY.snapshot()
    return len(snapshot["counters"]) + len(snapshot["gauges"]), _MAX_SERIES


def declared_metric_names() -> Iterable[str]:
    return tuple(sorted(REGISTRY._help))  # noqa: SLF001 — module-internal
