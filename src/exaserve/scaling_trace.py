"""
Lightweight instrumentation for diagnosing Ray scaling bottlenecks.

Records timestamped phases, per-call API latencies, and per-replica init
breakdowns.  Writes a single JSON trace file at the end of the run that
can be compared across different node counts to pinpoint what scales
poorly.

Usage (exaserve_serve.py / driver.py):

    from .scaling_trace import tracer

    with tracer.phase("ray.init"):
        ray.init(...)

    latency = tracer.timed_call("ray.nodes", ray.nodes)
    # latency is the return value; call metadata is recorded internally.

    tracer.save("/tmp/exaserve_scaling_trace.json")
"""

import glob
import json
import os
import socket
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, Optional


_TRACE_PARTS_DIRNAME = "scaling_trace_parts"


def tracing_enabled() -> bool:
    """Return True if scaling trace instrumentation is enabled.

    Controlled by the EXASERVE_SCALING_TRACE env var (default "1").  Set to
    "0" to fully disable all tracing I/O (file writes, file reads, JSON
    serialization).  This is important at scale where per-replica trace
    files on Lustre add significant overhead.
    """
    return os.environ.get("EXASERVE_SCALING_TRACE", "1") != "0"


def _sanitize_token(raw: str) -> str:
    token = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in raw)
    token = token.strip("._")
    return token or "default"


def trace_token() -> str:
    run_log_dir = os.environ.get("EXASERVE_RUN_LOG_DIR", "").strip()
    for candidate in (
        os.environ.get("EXASERVE_SCALING_TRACE_TOKEN", ""),
        os.path.basename(os.path.abspath(run_log_dir)) if run_log_dir else "",
        os.environ.get("PBS_JOBID", ""),
    ):
        candidate = candidate.strip()
        if candidate:
            return _sanitize_token(candidate)
    return _sanitize_token(f"{socket.gethostname()}_{os.getpid()}")


def trace_root_dir() -> str:
    run_log_dir = os.environ.get("EXASERVE_RUN_LOG_DIR", "").strip()
    if run_log_dir:
        return os.path.join(run_log_dir, _TRACE_PARTS_DIRNAME)
    return os.path.join("/tmp", f"exaserve_scaling_trace_{trace_token()}")


def default_scaling_trace_path() -> str:
    run_log_dir = os.environ.get("EXASERVE_RUN_LOG_DIR", "").strip()
    if run_log_dir:
        return os.path.join(run_log_dir, "scaling_trace.json")
    return os.path.join(trace_root_dir(), "scaling_trace.json")


def trace_part_path(kind: str, prefix: str, *, suffix: str = ".json") -> str:
    ts = time.strftime("%Y%m%d_%H%M%S")
    filename = (
        f"{prefix}_{_sanitize_token(socket.gethostname())}_{os.getpid()}_{ts}{suffix}"
    )
    return os.path.join(trace_root_dir(), kind, filename)


def list_trace_part_paths(kind: str) -> list[str]:
    pattern = os.path.join(trace_root_dir(), kind, "*.json")
    return sorted(glob.glob(pattern))


class ScalingTracer:
    """Thread-safe, singleton-style tracer for Ray scaling analysis."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._phases: list[dict] = []
        self._api_calls: list[dict] = []
        self._replicas: list[dict] = []
        self._events: list[dict] = []
        self._metadata: dict[str, Any] = {
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "trace_start": time.time(),
            "trace_token": trace_token(),
        }
        self._phase_stack: list[dict] = []
        self._call_counter = 0
        self._enabled = os.environ.get("EXASERVE_SCALING_TRACE", "1") != "0"

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    def set_metadata(self, **kwargs: Any) -> None:
        with self._lock:
            self._metadata.update(kwargs)

    # ------------------------------------------------------------------
    # Phase tracking  (coarse: "ray.init", "serve.start", "serve.run")
    # ------------------------------------------------------------------

    @contextmanager
    def phase(self, name: str, **extra: Any):
        """Context manager that records a named phase with wall-clock duration."""
        if not self._enabled:
            yield
            return
        start = time.monotonic()
        wall_start = time.time()
        entry = {
            "name": name,
            "wall_start": wall_start,
            "mono_start": start,
            **extra,
        }
        with self._lock:
            self._phase_stack.append(entry)
        try:
            yield
        finally:
            end = time.monotonic()
            entry["duration_s"] = round(end - start, 4)
            entry["wall_end"] = time.time()
            with self._lock:
                if self._phase_stack and self._phase_stack[-1] is entry:
                    self._phase_stack.pop()
                self._phases.append(entry)
            _print_trace(
                f"PHASE {name}: {entry['duration_s']:.3f}s", extra
            )

    def record_phase(self, name: str, duration_s: float, **extra: Any) -> None:
        """Manually record a phase that was timed externally."""
        if not self._enabled:
            return
        entry = {
            "name": name,
            "wall_start": time.time() - duration_s,
            "duration_s": round(duration_s, 4),
            **extra,
        }
        with self._lock:
            self._phases.append(entry)
        _print_trace(f"PHASE {name}: {duration_s:.3f}s", extra)

    # ------------------------------------------------------------------
    # Per-call API latency  (ray.nodes, ray.cluster_resources, etc.)
    # ------------------------------------------------------------------

    def timed_call(
        self,
        label: str,
        func: Callable,
        *args: Any,
        _extra: Optional[dict] = None,
        **kwargs: Any,
    ) -> Any:
        """Call *func* and record its latency.  Returns the function result."""
        if not self._enabled:
            return func(*args, **kwargs)

        start = time.monotonic()
        wall_start = time.time()
        try:
            result = func(*args, **kwargs)
        except Exception:
            elapsed = time.monotonic() - start
            self._record_api_call(label, wall_start, elapsed, error=True, extra=_extra)
            raise
        elapsed = time.monotonic() - start
        self._record_api_call(label, wall_start, elapsed, error=False, extra=_extra)
        return result

    def _record_api_call(
        self,
        label: str,
        wall_start: float,
        duration_s: float,
        *,
        error: bool = False,
        extra: Optional[dict] = None,
    ) -> None:
        with self._lock:
            idx = self._call_counter
            self._call_counter += 1
        entry = {
            "label": label,
            "call_idx": idx,
            "wall_start": wall_start,
            "duration_s": round(duration_s, 4),
            "error": error,
        }
        if extra:
            entry.update(extra)
        with self._lock:
            self._api_calls.append(entry)

    # ------------------------------------------------------------------
    # Replica-level init breakdown  (filled in by VLLMWorker.__init__)
    # ------------------------------------------------------------------

    def record_replica_init(self, replica_info: dict) -> None:
        """Record per-replica init timing breakdown."""
        if not self._enabled:
            return
        with self._lock:
            self._replicas.append(replica_info)

    # ------------------------------------------------------------------
    # Discrete events (e.g. "node joined", "proxy healthy")
    # ------------------------------------------------------------------

    def event(self, name: str, **extra: Any) -> None:
        """Record a point-in-time event."""
        if not self._enabled:
            return
        entry = {"name": name, "wall_time": time.time(), **extra}
        with self._lock:
            self._events.append(entry)

    # ------------------------------------------------------------------
    # Polling loop instrumentation
    # ------------------------------------------------------------------

    def record_poll_iteration(
        self,
        loop_name: str,
        iteration: int,
        *,
        elapsed_s: float,
        **metrics: Any,
    ) -> None:
        """Record one iteration of a polling loop (node registration, etc.)."""
        if not self._enabled:
            return
        entry = {
            "label": f"{loop_name}[{iteration}]",
            "call_idx": -1,  # sentinel: poll iteration, not a standalone call
            "wall_start": time.time(),
            "duration_s": round(elapsed_s, 4),
            "loop_name": loop_name,
            "iteration": iteration,
            **metrics,
        }
        with self._lock:
            self._api_calls.append(entry)

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        with self._lock:
            return {
                "metadata": dict(self._metadata),
                "phases": list(self._phases),
                "api_calls": list(self._api_calls),
                "replicas": list(self._replicas),
                "events": list(self._events),
            }

    def merge_driver_trace(self, driver_trace: dict) -> None:
        """Merge a driver trace (from driver.py) into this tracer."""
        with self._lock:
            driver_phases = driver_trace.get("phases", [])
            rank = driver_trace.get("rank", "?")
            hostname = driver_trace.get("hostname", "?")
            for phase in driver_phases:
                phase["source"] = f"driver.rank{rank}.{hostname}"
            self._phases.extend(driver_phases)

    def save(self, path: Optional[str] = None) -> Optional[str]:
        """Write trace JSON.  Default: $EXASERVE_RUN_LOG_DIR/scaling_trace.json, fallback /tmp."""
        if not self._enabled:
            return None
        if path is None:
            path = default_scaling_trace_path()
        self._metadata["trace_end"] = time.time()
        self._metadata["total_duration_s"] = round(
            self._metadata["trace_end"] - self._metadata["trace_start"], 4
        )
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        data = self.to_dict()
        # PR-035: atomic publish (crash/concurrent-reader safe).
        from exaserve.state.atomic import atomic_write_text
        atomic_write_text(path, json.dumps(data, indent=2, default=str))
        _print_trace(f"Trace written to {path} ({len(data['phases'])} phases, "
                     f"{len(data['api_calls'])} API calls, "
                     f"{len(data['replicas'])} replicas)")
        return path

    def save_replica_trace(self, path: Optional[str] = None) -> Optional[str]:
        """Write only replica init data — called from within VLLMWorker actors."""
        if not self._enabled:
            return None
        if path is None:
            path = trace_part_path("replica", "replica_trace")
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with self._lock:
            data = {
                "hostname": socket.gethostname(),
                "pid": os.getpid(),
                "replicas": list(self._replicas),
            }
        from exaserve.state.atomic import atomic_write_text
        atomic_write_text(path, json.dumps(data, indent=2, default=str))
        return path


# --------------------------------------------------------------------------
# Module-level singleton
# --------------------------------------------------------------------------
tracer = ScalingTracer()


def _print_trace(msg: str, extra: Any = None) -> None:
    """Print a trace line to stdout for real-time visibility."""
    parts = [f"[ScalingTrace] {msg}"]
    if extra and isinstance(extra, dict):
        detail = ", ".join(f"{k}={v}" for k, v in extra.items() if not k.startswith("_"))
        if detail:
            parts.append(f"  ({detail})")
    print("".join(parts), flush=True)


# --------------------------------------------------------------------------
# Replica stats collection via Ray named actor (replaces Lustre file I/O)
# --------------------------------------------------------------------------

_STATS_COLLECTOR_NAMESPACE = "serve"


def _stats_collector_name() -> str:
    # PR-029: deployment-scoped so a reused Ray cluster never mixes two
    # deployments' replica-init traces under one detached actor.
    for var in ("EXASERVE_DEPLOYMENT_ID", "EXASERVE_SCALING_TRACE_TOKEN",
                "EXASERVE_JOBID", "PBS_JOBID"):
        val = os.environ.get(var)
        if val:
            return f"ReplicaStatsCollector:{str(val).split('.')[0][:40]}"
    return "ReplicaStatsCollector:default"




class _ReplicaStatsCollectorImpl:
    """Collects replica init stats in-memory on the head node."""

    def __init__(self):
        self._replicas: list[dict] = []

    def report(self, replica_info: dict) -> None:
        self._replicas.append(replica_info)

    def get_all(self) -> list[dict]:
        return list(self._replicas)

    def count(self) -> int:
        return len(self._replicas)


def create_stats_collector():
    """Create the named ReplicaStatsCollector actor. Call once on head node
    after ray.init(), before serve.run() spawns replicas."""
    if not tracing_enabled():
        return None
    import ray
    actor_cls = ray.remote(_ReplicaStatsCollectorImpl)
    return actor_cls.options(
        name=_stats_collector_name(),
        namespace=_STATS_COLLECTOR_NAMESPACE,
        lifetime="detached",
        num_cpus=0,
    ).remote()


def _get_stats_collector():
    """Get the named collector actor, or None if tracing is disabled."""
    if not tracing_enabled():
        return None
    try:
        import ray
        return ray.get_actor(_stats_collector_name(), namespace=_STATS_COLLECTOR_NAMESPACE)
    except Exception:
        return None


def report_replica_stats(replica_info: dict) -> None:
    """Fire-and-forget: report replica init stats to the collector actor.
    Called from VLLMWorker.__init__ on every node."""
    collector = _get_stats_collector()
    if collector is None:
        return
    try:
        collector.report.remote(replica_info)
    except Exception:
        pass  # best effort; don't crash replica init




def collect_replica_stats() -> list[dict]:
    """Collect all replica stats from the named actor. Called once on head
    node after serve.run() completes. Returns [] if unavailable."""
    collector = _get_stats_collector()
    if collector is None:
        return []
    import ray
    try:
        stats = ray.get(collector.get_all.remote(), timeout=60)
        ray.kill(collector)
        return stats
    except Exception as exc:
        print(f"[ScalingTrace] Failed to collect replica stats: {exc}", flush=True)
        return []
