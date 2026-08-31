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
import os
import socket
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, Optional

from .state.clock import system_boot_id


_TRACE_PARTS_DIRNAME = "scaling_trace_parts"


def tracing_enabled() -> bool:
    """Return True if scaling trace instrumentation is enabled.

    Controlled by the EXASERVE_SCALING_TRACE env var (default "1").  Set to
    "0" to fully disable all tracing I/O (file writes, file reads, JSON
    serialization).  This is important at scale where per-replica trace
    files on Lustre add significant overhead.
    """
    return os.environ.get("EXASERVE_SCALING_TRACE", "1") != "0"


def merge_replica_init_evidence(
    *, actor_fields: dict[str, Any], engine_fields: Optional[dict[str, Any]] = None
) -> dict[str, Any]:
    """Merge backend diagnostics without overwriting actor-level identity/time.

    Backends historically used generic names such as ``total_init_s`` and
    ``wall_end``. Those are useful, but they describe only engine creation;
    the outer Serve actor owns the canonical replica-slot measurement. Any
    collision is therefore retained under an explicit ``engine_`` prefix.
    """
    if not isinstance(actor_fields, dict) or (
        engine_fields is not None and not isinstance(engine_fields, dict)
    ):
        raise TypeError("replica init evidence fields must be objects")
    merged: dict[str, Any] = {}
    for name, value in (engine_fields or {}).items():
        if not isinstance(name, str) or not name:
            raise ValueError("engine init evidence keys must be non-empty strings")
        destination = f"engine_{name}" if name in actor_fields else name
        if destination in actor_fields or destination in merged:
            raise ValueError(f"engine init evidence key collision at {destination!r}")
        merged[destination] = value
    merged.update(actor_fields)
    return merged


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
    filename = f"{prefix}_{_sanitize_token(socket.gethostname())}_{os.getpid()}_{ts}{suffix}"
    return os.path.join(trace_root_dir(), kind, filename)


def list_trace_part_paths(kind: str) -> list[str]:
    pattern = os.path.join(trace_root_dir(), kind, "*.json")
    return sorted(glob.glob(pattern))


class ScalingTracer:
    """Thread-safe, singleton-style tracer for Ray scaling analysis."""

    def __init__(
        self,
        *,
        wall_time: Optional[Callable[[], float]] = None,
        monotonic_time: Optional[Callable[[], float]] = None,
        boot_id: Optional[Callable[[], str]] = None,
    ) -> None:
        self._wall_time = wall_time or time.time
        self._monotonic_time = monotonic_time or time.monotonic
        self._boot_id = boot_id or system_boot_id
        self._lock = threading.Lock()
        self._phases: list[dict] = []
        self._api_calls: list[dict] = []
        self._replicas: list[dict] = []
        self._events: list[dict] = []
        self._metadata: dict[str, Any] = {
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "trace_start": self._wall_time(),
            "trace_start_monotonic": self._monotonic_time(),
            "trace_clock_boot_id": self._boot_id(),
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
            _print_trace(f"PHASE {name}: {entry['duration_s']:.3f}s", extra)

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
        self._metadata["trace_end"] = self._wall_time()
        self._metadata["trace_end_monotonic"] = self._monotonic_time()
        self._metadata["total_duration_s"] = round(
            self._metadata["trace_end_monotonic"] - self._metadata["trace_start_monotonic"],
            4,
        )
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        data = self.to_dict()
        # PR-035: atomic publish (crash/concurrent-reader safe).
        from exaserve.state.atomic import atomic_write_json

        atomic_write_json(path, data)
        _print_trace(
            f"Trace written to {path} ({len(data['phases'])} phases, "
            f"{len(data['api_calls'])} API calls, "
            f"{len(data['replicas'])} replicas)"
        )
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
        from exaserve.state.atomic import atomic_write_json

        atomic_write_json(path, data)
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
    from .telemetry import TelemetryIdentity, telemetry_actor_name

    return telemetry_actor_name("replica_init", TelemetryIdentity.from_environment())


def create_stats_collector(expected_replicas: int):
    """Create the named ReplicaStatsCollector actor. Call once on head node
    after ray.init(), before serve.run() spawns replicas."""
    if not tracing_enabled():
        return None
    import ray
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
    from .telemetry import OWNED_TELEMETRY_ACTORS, ReplicaInitStatsStore, TelemetryIdentity

    identity = TelemetryIdentity.from_environment()
    node_id = ray.get_runtime_context().get_node_id()
    actor_cls = ray.remote(ReplicaInitStatsStore)
    actor = actor_cls.options(
        name=_stats_collector_name(),
        namespace=_STATS_COLLECTOR_NAMESPACE,
        num_cpus=0,
        scheduling_strategy=NodeAffinitySchedulingStrategy(node_id=node_id, soft=False),
    ).remote(identity.to_dict(), expected_replicas)
    return OWNED_TELEMETRY_ACTORS.register("replica_init", actor)


def _get_stats_collector():
    """Get the named collector actor, or None if tracing is disabled."""
    if not tracing_enabled():
        return None
    import ray

    try:
        return ray.get_actor(_stats_collector_name(), namespace=_STATS_COLLECTOR_NAMESPACE)
    except ValueError:
        # A missing named actor is the expected optional-telemetry case. Other
        # Ray failures propagate to the caller and are recorded as drops.
        return None


def report_replica_stats(replica_info: dict) -> None:
    """Fire-and-forget: report replica init stats to the collector actor.
    Called from VLLMWorker.__init__ on every node."""
    if not tracing_enabled():
        return
    collector = _get_stats_collector()
    if collector is None:
        print("[ScalingTrace] replica-init collector unavailable; report dropped", flush=True)
        from .observability import record_telemetry_drop

        record_telemetry_drop("replica_init", "collector_unavailable")
        return
    try:
        import ray
        from .telemetry import TelemetryIdentity, replica_init_envelope

        try:
            replica_id = str(ray.get_runtime_context().get_actor_id())
        except (AttributeError, RuntimeError):
            replica_id = f"{socket.gethostname()}:{os.getpid()}"
        envelope = replica_init_envelope(
            identity=TelemetryIdentity.from_environment(),
            replica_id=replica_id,
            payload=replica_info,
        )
        ray.get(collector.report.remote(envelope), timeout=10)
    except Exception as exc:
        # Instrumentation is optional, but the loss is explicit and observable.
        from .observability import record_telemetry_drop

        record_telemetry_drop("replica_init", "push_failed")
        print(f"[ScalingTrace] replica-init report dropped: {exc}", flush=True)


def collect_replica_stats() -> list[dict]:
    """Collect all replica stats from the named actor. Called once on head
    node after serve.run() completes. Returns [] if unavailable."""
    collector = _get_stats_collector()
    if collector is None:
        return []
    import ray

    try:
        from .telemetry import TelemetryIdentity, validate_replica_init_snapshot

        snapshot = validate_replica_init_snapshot(
            ray.get(collector.snapshot.remote(), timeout=60),
            expected_identity=TelemetryIdentity.from_environment(),
        )
        if not snapshot["complete"]:
            print(
                "[ScalingTrace] replica-init telemetry incomplete: "
                f"{snapshot['received_replicas']}/"
                f"{snapshot['expected_replicas']}",
                flush=True,
            )
        return list(snapshot["replicas"].values())
    except Exception as exc:
        print(f"[ScalingTrace] Failed to collect replica stats: {exc}", flush=True)
        return []
    finally:
        try:
            ray.kill(collector, no_restart=True)
        except (RuntimeError, ValueError) as exc:
            print(f"[ScalingTrace] replica-init actor cleanup failed: {exc}", flush=True)
        else:
            from .telemetry import OWNED_TELEMETRY_ACTORS

            OWNED_TELEMETRY_ACTORS.release("replica_init")
