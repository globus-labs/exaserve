"""
Ray Serve LLM inference on Aurora (Intel XPU).

Each VLLMWorker replica handles both HTTP ingress (OpenAI-compatible API) and
vLLM inference on a single GPU tile, eliminating the separate Router layer.

For multi-model serving, each model is deployed as an independent Ray Serve
application at its own route_prefix (e.g. /llama-3-8b/v1).

Installed Ray and vLLM files remain untouched. Exact-hash, role-filtered
compatibility modules are staged with the release and activated before their
base targets can import. Aurora relies on ZE_AFFINITY_MASK for device isolation
and keeps ONEAPI_DEVICE_SELECTOR unset because Triton's SYCL probe crashes on
Aurora when Ray rewrites it to a "level_zero:..." list.
"""

import argparse
import asyncio
from collections import deque
import json
import math
import os
import socket
import threading
import time
import uuid
from typing import Optional, List, Dict, Any

from .exception_notes import add_exception_note
from .actor_runtime import build_actor_runtime_env

import random
import ray
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from ray import serve
from ray.serve.config import HTTPOptions, ProxyLocation
# vLLM is imported lazily so this module can be imported in an SGLang
# environment where vLLM is absent or its transformers pin conflicts. The
# verified DeploymentPlan selects the backend; only that engine is imported.

from . import request_validation as _rv
from .compat.collector import deployment_scope as _deployment_scope
from .model_staging import print_red, resolve_model_paths
from .plan.contracts import DeploymentPlan, ModelPlan
from .plan.runtime_binding import (
    BoundDeployment,
    BoundReplica,
    LiveNodeInventory,
    format_runtime_binding,
)
from .scaling_trace import tracer

# GCS-bootstrap hardening prepared by the site/runtime profile. RayConfig consumes
# RAY_* env at process start, so the value is only live in a process whose
# environment carries it — env present in a spawned Ray worker == RayConfig in
# that worker read it. Verified against a real remote worker (the process
# class that failed in sglang_direct_n256 run0-run3) instead of the driver,
# because a driver-only export that misses the mpiexec'd `ray start` is
# exactly the silent failure mode this guards against.
_CORE_ENV_EXPECTED = (
    "RAY_gcs_rpc_server_connect_timeout_s",
    "RAY_gcs_rpc_server_reconnect_timeout_s",
    "RAY_worker_register_timeout_seconds",
    "RAY_SERVE_MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT",
)


def _verify_core_env() -> None:
    """Probe a fresh remote Ray worker and fail fast if the GCS-hardening env
    did not propagate. This safety check has no ambient bypass."""
    missing = [key for key in _CORE_ENV_EXPECTED if not os.environ.get(key)]
    if missing:
        raise RuntimeError(
            "verified SiteProfile did not prepare required Ray core environment: "
            + ", ".join(missing)
        )
    expected = {key: os.environ[key] for key in _CORE_ENV_EXPECTED}

    @ray.remote(num_cpus=0)
    def _probe(keys):
        import os as _os

        return {k: _os.environ.get(k) for k in keys}

    seen = ray.get(_probe.remote(list(expected)), timeout=120)
    mismatched = {k: (v, seen.get(k)) for k, v in expected.items() if seen.get(k) != v}
    if mismatched:
        detail = ", ".join(f"{k}: driver={v} worker={w}" for k, (v, w) in mismatched.items())
        raise RuntimeError(
            f"GCS-hardening env not live in Ray workers ({detail}). "
            "Exports did not reach the mpiexec'd ray start; aborting before "
            "deploy rather than replaying the run0-run3 bootstrap lottery."
        )
    print(
        "[ExaServe] Core env verified in remote worker: "
        + ", ".join(f"{k}={v}" for k, v in expected.items()),
        flush=True,
    )


def get_alive_ray_gpu_nodes() -> List[Dict[str, Any]]:
    """Return alive Ray nodes that advertise GPU resources."""

    def exact_resource_count(value: Any, name: str) -> int:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or value < 0
            or not float(value).is_integer()
        ):
            raise RuntimeError(f"Ray node resource {name!r} must be a non-negative integer")
        return int(value)

    alive_nodes: List[Dict[str, Any]] = []
    nodes = ray.nodes()
    if not isinstance(nodes, list):
        raise RuntimeError("ray.nodes() returned a non-list inventory")
    for index, node in enumerate(nodes):
        if not isinstance(node, dict):
            raise RuntimeError(f"ray.nodes()[{index}] is not an object")
        alive = node.get("Alive", False)
        if not isinstance(alive, bool):
            raise RuntimeError(f"ray.nodes()[{index}].Alive is not boolean")
        if not alive:
            continue
        resources = node.get("Resources", {})
        if not isinstance(resources, dict) or any(not isinstance(key, str) for key in resources):
            raise RuntimeError(f"ray.nodes()[{index}].Resources is not a string-keyed object")
        gpu_count = exact_resource_count(resources.get("GPU", 0), "GPU")
        if gpu_count < 1:
            continue
        node_ip = node.get("NodeManagerAddress", "")
        hostname = node.get("NodeManagerHostname", "") or node_ip
        if not isinstance(node_ip, str) or not node_ip:
            raise RuntimeError(f"ray.nodes()[{index}] has no valid NodeManagerAddress")
        if not isinstance(hostname, str) or not hostname:
            raise RuntimeError(f"ray.nodes()[{index}] has no valid NodeManagerHostname")
        resource_key = f"node:{node_ip}"
        if resource_key not in resources:
            resource_key = next(
                (key for key in resources if key.startswith("node:")),
                resource_key,
            )
        alive_nodes.append(
            {
                "ip": node_ip,
                "hostname": hostname,
                "resource_key": resource_key,
                "gpu_count": gpu_count,
                "cpu_count": exact_resource_count(resources.get("CPU", 0), "CPU"),
            }
        )
    return alive_nodes


def build_node_inventory() -> list[LiveNodeInventory]:
    """Build a deterministic planner inventory from current alive Ray GPU nodes."""
    alive_nodes = sorted(get_alive_ray_gpu_nodes(), key=lambda item: item["ip"])
    return [
        LiveNodeInventory(
            ip=node["ip"],
            resource_key=node["resource_key"],
            total_gpus=node["gpu_count"],
            total_cpus=node["cpu_count"],
            hostname=node["hostname"],
        )
        for node in alive_nodes
    ]


def build_pp_placement_group_bundles(
    model_config: ModelPlan,
    planned_placement,
) -> tuple[List[Dict[str, float]], List[str]]:
    """
    Build a stage-aware placement group for one PP replica.

    Bundle 0 is a CPU-only coordinator bundle pinned to the stage-0 node.
    The remaining bundles are one-GPU worker bundles ordered by
    (pp_rank, tp_rank), which matches vLLM's Ray rank layout.
    """
    alive_gpu_nodes = get_alive_ray_gpu_nodes()
    if len(alive_gpu_nodes) < model_config.pipeline_parallel_size:
        raise RuntimeError(
            f"Need at least {model_config.pipeline_parallel_size} alive Ray GPU nodes "
            f"for PP, but found {len(alive_gpu_nodes)}: {alive_gpu_nodes}"
        )

    by_ip = {str(node["ip"]): node for node in alive_gpu_nodes}
    planned_node_ips = tuple(planned_placement.node_ips)
    missing = [ip for ip in planned_node_ips if ip not in by_ip]
    if missing:
        raise RuntimeError(f"canonical PP placement names absent Ray node IP(s): {missing}")
    stage_nodes = [by_ip[ip] for ip in planned_node_ips]
    for node in stage_nodes:
        if int(node["gpu_count"]) < model_config.tensor_parallel_size:
            raise RuntimeError(
                f"Node {node['ip']} only has {node['gpu_count']} GPUs available, "
                f"but PP stage requires tensor_parallel_size={model_config.tensor_parallel_size}"
            )

    bundles: List[Dict[str, float]] = [
        {
            "CPU": float(model_config.num_cpus_per_replica),
            str(stage_nodes[0]["resource_key"]): 0.001,
        }
    ]
    for node in stage_nodes:
        resource_key = str(node["resource_key"])
        for _ in range(model_config.tensor_parallel_size):
            bundles.append({"GPU": 1.0, resource_key: 0.001})

    return bundles, [str(node["ip"]) for node in stage_nodes]


def init_ray_cluster(
    address: str,
    namespace: str = "serve",
    include_dashboard: bool = False,
    retries: int = 60,
    retry_delay_s: float = 5.0,
) -> None:
    """Retry Ray bootstrap for slow Aurora control-plane startup."""
    if not isinstance(address, str) or not address.strip():
        raise ValueError("Ray address must be non-empty text")
    if not isinstance(namespace, str) or not namespace.strip():
        raise ValueError("Ray namespace must be non-empty text")
    if not isinstance(include_dashboard, bool):
        raise ValueError("include_dashboard must be a boolean")
    if isinstance(retries, bool) or not isinstance(retries, int) or retries <= 0:
        raise ValueError("Ray init retries must be a positive integer")
    if (
        isinstance(retry_delay_s, bool)
        or not isinstance(retry_delay_s, (int, float))
        or not math.isfinite(float(retry_delay_s))
        or retry_delay_s < 0
    ):
        raise ValueError("Ray init retry delay must be finite and nonnegative")
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            attempt_start = time.monotonic()
            ray.init(
                address=address,
                namespace=namespace,
                include_dashboard=include_dashboard,
            )
            tracer.record_phase(
                "ray.init.connect",
                time.monotonic() - attempt_start,
                attempt=attempt,
                address=address,
            )
            return
        except Exception as exc:
            last_error = exc
            tracer.event("ray.init.retry", attempt=attempt, error=str(exc))
            if attempt == retries:
                break
            print(
                f"[ExaServe] ray.init attempt {attempt}/{retries} failed for "
                f"{address}: {exc}. Retrying in {retry_delay_s:.1f}s...",
                flush=True,
            )
            time.sleep(retry_delay_s)
    raise RuntimeError(
        f"Failed to connect to Ray cluster at {address} after {retries} attempts"
    ) from last_error


def verify_ray_serve_timing_contract(plan) -> None:
    """Fail before Ray connection if plan-derived timing delivery drifted."""
    from ray.serve._private import constants

    checks = {
        "HTTP_PROXY_TIMEOUT": plan.readiness.serve_start_proxy_timeout_s,
        "PROXY_HEALTH_CHECK_TIMEOUT_S": (plan.readiness.serve_proxy_health_check_timeout_s),
        "PROXY_READY_CHECK_TIMEOUT_S": (plan.readiness.serve_proxy_ready_check_timeout_s),
    }
    mismatches = []
    for name, expected in checks.items():
        observed = getattr(constants, name, None)
        if observed is None or float(observed) != float(expected):
            mismatches.append(f"{name}: plan={expected}, runtime={observed}")
    if not getattr(constants, "_exaserve_serve_start_timeout_patch", False):
        mismatches.append("HTTP_PROXY_TIMEOUT compatibility activation has no sentinel")
    if mismatches:
        raise RuntimeError(
            "Ray Serve timing contract was not delivered before import: " + "; ".join(mismatches)
        )
    print(
        "[ExaServe] Ray Serve timing contract verified: "
        + ", ".join(f"{name}={value}" for name, value in checks.items()),
        flush=True,
    )


# ═══════════════════════════════════════════════════════════════════════════
# VLLMWorker — HTTP ingress + vLLM engine in one deployment
# ═══════════════════════════════════════════════════════════════════════════
app = FastAPI()

# PR-032: one correlation-id contract for every handler.
from .observability import (  # noqa: E402
    RequestMetricsMiddleware as _RequestMetricsMiddleware,
    correlation_id as _correlation_id,
    emit_request_link as _emit_request_link,
    record_telemetry_drop as _record_telemetry_drop,
    record_tokens as _record_tokens,
)

app.add_middleware(_RequestMetricsMiddleware)


def _typed_api_error(
    *,
    correlation: str,
    status_code: int,
    message: str,
    error_type: str,
    code: str,
    param: Optional[str],
) -> JSONResponse:
    """Build a correlated, OpenAI-shaped API error response."""
    return JSONResponse(
        {
            "error": {
                "message": message,
                "type": error_type,
                "param": param,
                "code": code,
            },
            "request_id": correlation,
        },
        status_code=status_code,
        headers={"X-Request-ID": correlation},
    )


class CollectingStatLogger:
    """Buffers vLLM scheduler and per-request stats for post-run collection.

    Conforms to vLLM v1 StatLoggerBase interface:
      __init__(vllm_config, engine_index)
      record(scheduler_stats, iteration_stats, mm_cache_stats=None, engine_idx=0)
      log()

    Passed as a class to AsyncLLMEngine.from_engine_args(stat_loggers=[CollectingStatLogger]).
    vLLM instantiates it; retrieve the instance via the class-level registry.
    """

    # Class-level registry: pid → instance (one per replica process).
    _instances = {}
    _retention_limit = 2000

    @classmethod
    def configure(cls, *, retention: int) -> None:
        if (
            isinstance(retention, bool)
            or not isinstance(retention, int)
            or not 1 <= retention <= 10_000
        ):
            raise ValueError("stats retention must be an integer in [1, 10000]")
        cls._retention_limit = retention

    def __init__(self, vllm_config=None, engine_index=0):
        retention = self._retention_limit
        self._lock = threading.Lock()
        self._retention = retention
        self._seen_requests = 0
        self._rng = random.Random(os.getpid())
        self.scheduler_snapshots = deque(maxlen=2000)
        self.finished_requests = []
        CollectingStatLogger._instances[os.getpid()] = self

    def record(self, scheduler_stats=None, iteration_stats=None, **kwargs):
        with self._lock:
            if scheduler_stats is not None:
                self.scheduler_snapshots.append(
                    {
                        "timestamp": time.time(),
                        "running": getattr(scheduler_stats, "num_running_reqs", 0),
                        "waiting": getattr(scheduler_stats, "num_waiting_reqs", 0),
                        "kv_cache_usage": getattr(scheduler_stats, "kv_cache_usage", 0.0),
                    }
                )
            if iteration_stats is not None:
                now = time.time()
                for req in getattr(iteration_stats, "finished_requests", []):
                    item = {
                        "finished_at": now,
                        "e2e_latency": getattr(req, "e2e_latency", 0.0),
                        "queued_time": getattr(req, "queued_time", 0.0),
                        "prefill_time": getattr(req, "prefill_time", 0.0),
                        "inference_time": getattr(req, "inference_time", 0.0),
                        "decode_time": getattr(req, "decode_time", 0.0),
                        "num_prompt_tokens": getattr(req, "num_prompt_tokens", 0),
                        "num_generation_tokens": getattr(req, "num_generation_tokens", 0),
                        "num_cached_tokens": getattr(req, "num_cached_tokens", 0),
                    }
                    self._seen_requests += 1
                    if len(self.finished_requests) < self._retention:
                        self.finished_requests.append(item)
                    else:
                        # Algorithm R: a uniform bounded sample across the full
                        # run, rather than an unbounded per-request history.
                        replace = self._rng.randrange(self._seen_requests)
                        if replace < self._retention:
                            self.finished_requests[replace] = item

    def log(self):
        pass

    def log_engine_initialized(self):
        pass

    def record_sleep_state(self, is_awake=0, level=0):
        pass

    @staticmethod
    def _pct(sorted_vals, q):
        if not sorted_vals:
            return None
        if len(sorted_vals) == 1:
            return sorted_vals[0]
        idx = min(len(sorted_vals) - 1, max(0, int(round(q * (len(sorted_vals) - 1)))))
        return sorted_vals[idx]

    def summary(self):
        """Per-replica server-side metrics, immune to proxy/delivery effects.

        server-TTFT = queued_time + prefill_time (arrival -> first token GENERATED).
        server-TBT  = decode_time / (gen_tokens - 1)  (true per-request decode cadence).
        These are what the decode SLO is really about; client-side TBT can be
        distorted by proxy coalescing (http-no-delay off), these cannot.
        """
        with self._lock:
            fr = list(self.finished_requests)
            snaps = list(self.scheduler_snapshots)
            total_requests = self._seen_requests
        ttft, tbt, e2e, dec, pre, que = [], [], [], [], [], []
        for r in fr:
            q = float(r.get("queued_time", 0.0) or 0.0)
            p = float(r.get("prefill_time", 0.0) or 0.0)
            d = float(r.get("decode_time", 0.0) or 0.0)
            n = int(r.get("num_generation_tokens", 0) or 0)
            ttft.append(q + p)
            que.append(q)
            pre.append(p)
            dec.append(d)
            e2e.append(float(r.get("e2e_latency", 0.0) or 0.0))
            if n > 1:
                tbt.append(d / (n - 1))
        run = [s.get("running", 0) for s in snaps]
        kv = [s.get("kv_cache_usage", 0.0) for s in snaps]

        def stats(v):
            if not v:
                return {"n": 0}
            s = sorted(v)
            return {
                "n": len(s),
                "mean": sum(s) / len(s),
                "p50": self._pct(s, 0.50),
                "p90": self._pct(s, 0.90),
                "p99": self._pct(s, 0.99),
                "max": s[-1],
            }

        return {
            "total_requests": total_requests,
            "server_ttft": stats(ttft),  # queued+prefill
            "server_tbt": stats(tbt),  # decode/(gen-1)
            "e2e": stats(e2e),
            "queued_time": stats(que),
            "prefill_time": stats(pre),
            "decode_time": stats(dec),
            "mean_batch_size": (sum(run) / len(run)) if run else 0,
            "max_batch_size": max(run) if run else 0,
            "kv_cache_peak": max(kv) if kv else 0.0,
        }

    def sample(self, cap=4000):
        """A bounded, evenly-strided per-request sample for fleet-wide pooled
        percentiles (computing exact pooled p99 needs raw values; this caps the
        wire payload at ~cap/replica while staying representative across the run)."""
        with self._lock:
            fr = list(self.finished_requests)
        if not fr:
            return []
        step = max(1, len(fr) // cap)
        out = []
        for r in fr[::step][:cap]:
            n = int(r.get("num_generation_tokens", 0) or 0)
            d = float(r.get("decode_time", 0.0) or 0.0)
            out.append(
                {
                    "finished_at": r.get("finished_at"),
                    "ttft": float(r.get("queued_time", 0.0) or 0.0)
                    + float(r.get("prefill_time", 0.0) or 0.0),
                    "tbt": (d / (n - 1)) if n > 1 else None,
                    "e2e": float(r.get("e2e_latency", 0.0) or 0.0),
                }
            )
        return out

    def to_dict(self):
        # Ship the per-replica summary + a capped sample (for pooled fleet
        # percentiles) + the scheduler trace. The full per-request array is NOT
        # shipped (unbounded at 256n); summary+sample preserve what we report.
        with self._lock:
            scheduler_snapshots = list(self.scheduler_snapshots)
        return {
            "summary": self.summary(),
            "sample": self.sample(),
            "scheduler_snapshots": scheduler_snapshots,
        }

    def live_snapshot(self) -> dict:
        with self._lock:
            return {
                "latest_scheduler": self.scheduler_snapshots[-1]
                if self.scheduler_snapshots
                else None,
                "total_finished_requests": self._seen_requests,
                "retained_finished_requests": len(self.finished_requests),
                "scheduler_snapshot_count": len(self.scheduler_snapshots),
            }

    @classmethod
    def get_instance(cls):
        return cls._instances.get(os.getpid())


# --- Serving-stats collection: replicas PUSH summaries to a named head actor ---
# Avoids serve.status() replica enumeration (no per-replica handles in this Ray
# version) and Lustre MDS load (no per-replica files). Independent of the
# EXASERVE_SCALING_TRACE gating used by the init-stats collector.
_SERVING_STATS_NS = "serve"


from .telemetry import (  # noqa: E402
    OWNED_TELEMETRY_ACTORS as _OWNED_TELEMETRY_ACTORS,
    ReplicaLifecycleStore as _ReplicaLifecycleStore,
    ServingStatsStore as _ServingStatsCollectorImpl,
    TelemetryIdentity as _TelemetryIdentity,
    lifecycle_event_envelope as _lifecycle_event_envelope,
    serving_envelope as _serving_envelope,
    telemetry_actor_name as _telemetry_actor_name,
    validate_lifecycle_snapshot as _validate_lifecycle_snapshot,
)


def create_serving_stats_collector(expected_replicas: int):
    """Create the one driver-owned, head-pinned serving telemetry actor."""
    import ray
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    identity = _TelemetryIdentity.from_environment()
    name = _telemetry_actor_name("serving", identity)
    node_id = ray.get_runtime_context().get_node_id()
    cls = ray.remote(_ServingStatsCollectorImpl)
    actor = cls.options(
        name=name,
        namespace=_SERVING_STATS_NS,
        num_cpus=0,
        scheduling_strategy=NodeAffinitySchedulingStrategy(node_id=node_id, soft=False),
    ).remote(identity.to_dict(), expected_replicas)
    return _OWNED_TELEMETRY_ACTORS.register("serving", actor)


def get_serving_stats_collector():
    import ray

    identity = _TelemetryIdentity.from_environment()
    return ray.get_actor(_telemetry_actor_name("serving", identity), namespace=_SERVING_STATS_NS)


def create_replica_lifecycle_collector(expected_components: list[str]):
    """Create the driver-owned actor that makes replica teardown observable."""
    import ray
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    identity = _TelemetryIdentity.from_environment()
    name = _telemetry_actor_name("lifecycle", identity)
    node_id = ray.get_runtime_context().get_node_id()
    cls = ray.remote(_ReplicaLifecycleStore)
    actor = cls.options(
        name=name,
        namespace=_SERVING_STATS_NS,
        num_cpus=0,
        scheduling_strategy=NodeAffinitySchedulingStrategy(node_id=node_id, soft=False),
    ).remote(identity.to_dict(), expected_components)
    return _OWNED_TELEMETRY_ACTORS.register("lifecycle", actor)


def get_replica_lifecycle_collector():
    import ray

    identity = _TelemetryIdentity.from_environment()
    return ray.get_actor(_telemetry_actor_name("lifecycle", identity), namespace=_SERVING_STATS_NS)


def require_clean_replica_shutdown(*, timeout_s: float) -> dict:
    """Fail the deployment child if Serve hid any replica destructor failure."""
    import ray

    collector = get_replica_lifecycle_collector()
    snapshot = _validate_lifecycle_snapshot(
        ray.get(collector.snapshot.remote(), timeout=max(0.1, timeout_s)),
        expected_identity=_TelemetryIdentity.from_environment(),
    )
    if not snapshot["clean"]:
        raise RuntimeError(
            "replica teardown incomplete: "
            f"missing={snapshot.get('missing_components', [])}, "
            f"active={snapshot.get('active_components', [])}, "
            f"failed={snapshot.get('failed_components', [])}"
        )
    return snapshot


def _serving_stats_push_loop(model_id: str, stop_event, period_s: float, sample_cap: int):
    """Daemon-thread loop in the replica process: every period_s, compute this
    replica's server-side summary+sample and push to the head collector. The
    last push before teardown carries near-complete data. Logs the buffered
    request count once so we can confirm the logger is actually recording.

    Scale-safe defaults are sized for the qualification target and both values
    are immutable DeploymentPlan policy rather than ambient shell knobs."""
    import ray

    if not math.isfinite(period_s) or period_s <= 0:
        _record_telemetry_drop("serving_stats", "invalid_configuration")
        raise ValueError("stats push period must be finite and positive")
    if sample_cap <= 0 or sample_cap > 10_000:
        _record_telemetry_drop("serving_stats", "invalid_configuration")
        raise ValueError("stats sample cap must be in [1, 10000]")
    try:
        node_ip = ray.util.get_node_ip_address()
    except Exception:
        node_ip = "?"
    try:
        replica_id = str(ray.get_runtime_context().get_actor_id())
    except (AttributeError, RuntimeError):
        replica_id = f"{node_ip}:{os.getpid()}"
    key = replica_id
    identity = _TelemetryIdentity.from_environment()
    collector = None
    sequence = 0
    logged_records = False
    logged_none = False
    failures = 0

    def push_once() -> None:
        nonlocal collector, failures, logged_none, logged_records, sequence
        logger = CollectingStatLogger.get_instance()
        if logger is None:
            if not logged_none:
                print(
                    f"[serving-stats] {key}: logger get_instance()=None "
                    "(stat logger not in this process)",
                    flush=True,
                )
                logged_none = True
            return
        try:
            n = logger.live_snapshot()["total_finished_requests"]
            if n and not logged_records:
                print(f"[serving-stats] {key}: recording ({n} reqs buffered)", flush=True)
                logged_records = True
            if collector is None:
                collector = get_serving_stats_collector()
            payload = _serving_envelope(
                identity=identity,
                replica_id=replica_id,
                sequence=sequence,
                model_id=model_id,
                node_ip=node_ip,
                pid=os.getpid(),
                summary=logger.summary(),
                sample=logger.sample(sample_cap),
            )
            ray.get(collector.report.remote(payload), timeout=max(1.0, min(5.0, period_s / 2.0)))
            sequence += 1
        except Exception as e:
            collector = None
            failures += 1
            _record_telemetry_drop("serving_stats", "push_failed")
            # Log at exponentially sparse intervals; the metric preserves the
            # exact count without flooding thousands of replica logs.
            if failures & (failures - 1) == 0:
                print(
                    f"[serving-stats] {key}: push failed "
                    f"({failures} drops so far): {type(e).__name__}: {e}",
                    flush=True,
                )

    while not stop_event.wait(period_s):
        push_once()
    # The explicit replica teardown hook wakes the loop immediately and sends
    # one bounded terminal snapshot before the actor process disappears.
    push_once()


@serve.deployment
class ProxyAnchor:
    """Route-less public-API anchor that makes one planned node host a proxy.

    Ray 2.53's ``ProxyLocation.EveryNode`` starts proxies only on nodes with a
    Serve replica. A PP-only stage or an otherwise idle planned rank is still
    an HAProxy backend in the canonical topology, so relying on the enum name
    alone leaves that backend permanently absent. One tiny actor is pinned to
    every planned rank; it owns no model state and exposes no HTTP route.
    """

    def check_health(self) -> None:
        return None


@serve.deployment
@serve.ingress(app)
class EngineWorker:
    """Single Ray Serve deployment: OpenAI-format HTTP ingress on one GPU tile,
    delegating inference to a pluggable EngineBackend (vLLM / SGLang / null).

    The engine is selected by the plan-derived ``engine_name`` or replaced by
    NullEngine when the plan-derived ``null_compute`` flag is set. This host owns everything engine-
    agnostic — the HTTP surface, per-tile placement wiring (via deploy_model),
    stats, and the readiness warmup — while each EngineBackend owns device
    isolation + engine creation + generation. See exaserve.engines.
    """

    def __init__(
        self,
        model_id: str,
        replica_index: int,
        local_model_path: str = None,
        null_compute: bool = False,
        tensor_parallel_size: int = 1,
        pipeline_parallel_size: int = 1,
        gpu_memory_utilization: float = 0.9,
        max_model_len: int = 4096,
        enforce_eager: bool = True,
        max_num_seqs: int = None,
        collect_stats: bool = False,
        enable_log_requests: bool = True,
        engine_name: str = "vllm",
        vendor_name: str = "xpu",
        null_compute_latency_s: float = 1.0,
        stats_retention: int = 2000,
        stats_push_period_s: float = 10.0,
        stats_sample_cap: int = 1500,
    ):
        init_start = time.time()
        pid = os.getpid()
        hostname = socket.gethostname()
        self.model_id = model_id
        self.null_compute = null_compute

        gpu_ids = []
        for g in ray.get_gpu_ids():
            try:
                gpu_ids.append(int(g))
            except (ValueError, TypeError) as exc:
                raise RuntimeError(f"Ray returned a non-integer GPU identity {g!r}") from exc
        gpu_ids.sort()
        device_id = gpu_ids[0] if gpu_ids else 0

        # Bind this live actor to exactly one precompiled logical slot before
        # the backend spawns EngineCore.  The resolved ids are inherited by the
        # engine shim, so replica and engine self-report distinct exact slots.
        from .plan.io import (
            load_allocation_binding,
            load_deployment_plan,
            rank_for_node,
            resolve_replica_index_for_live_placement,
            resolve_replica_receipt_requirement,
        )

        try:
            _receipt_plan = load_deployment_plan(os.environ["EXASERVE_PLAN_PATH"])
            _receipt_binding = load_allocation_binding(
                os.environ["EXASERVE_ALLOCATION_BINDING_PATH"]
            )
            _owner_rank = rank_for_node(_receipt_binding, hostname)
            _models = [item for item in _receipt_plan.models if item.model_id == model_id]
            if len(_models) != 1:
                raise ValueError(f"model {model_id!r} maps to {len(_models)} canonical model plans")
            if replica_index == -1:
                replica_index = resolve_replica_index_for_live_placement(
                    plan=_receipt_plan,
                    model_id=model_id,
                    owner_rank=_owner_rank,
                    device_ids=tuple(gpu_ids),
                )
            _replicas = [
                item for item in _models[0].replicas if item.replica_index == replica_index
            ]
            if len(_replicas) != 1:
                raise ValueError(
                    f"replica index {replica_index} maps to {len(_replicas)} canonical replicas"
                )
            _logical_replica = _replicas[0]
            if _logical_replica.planned_ranks[0] != _owner_rank:
                raise ValueError(
                    f"logical replica {replica_index} owner rank "
                    f"{_logical_replica.planned_ranks[0]} != live rank {_owner_rank}"
                )
            if (
                _logical_replica.tensor_parallel_size != tensor_parallel_size
                or _logical_replica.pipeline_parallel_size != pipeline_parallel_size
            ):
                raise ValueError("replica constructor topology disagrees with the canonical plan")
            expected_actor_gpus = tensor_parallel_size if pipeline_parallel_size == 1 else 0
            if len(gpu_ids) != expected_actor_gpus or len(set(gpu_ids)) != len(gpu_ids):
                raise ValueError(
                    f"logical replica {replica_index} requires {expected_actor_gpus} "
                    f"actor GPU resources, Ray assigned {gpu_ids}"
                )
            self._replica_requirement_id = resolve_replica_receipt_requirement(
                plan=_receipt_plan,
                model_id=model_id,
                replica_index=replica_index,
                role="replica",
            )
            _requirements = {
                item.receipt_requirement_id: item for item in _receipt_plan.receipt_requirements
            }
            self._replica_component_id = _requirements[self._replica_requirement_id].component_slot
            self._engine_requirement_id = None
            self._engine_component_id = None
            if not null_compute:
                self._engine_requirement_id = resolve_replica_receipt_requirement(
                    plan=_receipt_plan,
                    model_id=model_id,
                    replica_index=replica_index,
                    role="engine_core",
                )
                self._engine_component_id = _requirements[
                    self._engine_requirement_id
                ].component_slot
        except (KeyError, OSError, ValueError) as exc:
            raise RuntimeError(
                f"could not bind replica {model_id!r} on {hostname} devices "
                f"{gpu_ids} to canonical topology: {exc}"
            ) from exc
        os.environ["EXASERVE_RECEIPT_RANK"] = str(_owner_rank)
        os.environ["EXASERVE_RECEIPT_REPLICA_INDEX"] = str(replica_index)
        os.environ["EXASERVE_RECEIPT_REQUIREMENT_ID_REPLICA"] = self._replica_requirement_id
        from .compat.local_ingress import SOCKET_ENV, socket_path_for

        os.environ[SOCKET_ENV] = socket_path_for(
            os.environ["EXASERVE_DEPLOYMENT_ID"],
            int(os.environ["EXASERVE_GENERATION"]),
            owner_rank=_owner_rank,
        )
        if self._engine_requirement_id is not None and self._engine_component_id is not None:
            os.environ["EXASERVE_RECEIPT_REQUIREMENT_ID_ENGINE"] = self._engine_requirement_id
            os.environ["EXASERVE_RECEIPT_COMPONENT_ID_ENGINE"] = self._engine_component_id
            os.environ["EXASERVE_RECEIPT_MODEL_ID_ENGINE"] = model_id
            os.environ["EXASERVE_RECEIPT_DEVICE_IDS_ENGINE"] = ",".join(
                str(value) for value in sorted(gpu_ids)
            )
        else:
            os.environ.pop("EXASERVE_RECEIPT_REQUIREMENT_ID_ENGINE", None)
            os.environ.pop("EXASERVE_RECEIPT_COMPONENT_ID_ENGINE", None)
            os.environ.pop("EXASERVE_RECEIPT_MODEL_ID_ENGINE", None)
            os.environ.pop("EXASERVE_RECEIPT_DEVICE_IDS_ENGINE", None)

        # The deployment bootstrap activated only its own Ray Serve adapter.
        # A replica is a different interpreter and activates its exact role
        # only after its rank-local receipt transport is bound, and still
        # before importing an engine backend.
        from .compat.activator import CompatibilityActivator

        CompatibilityActivator().activate("replica")
        from .engines import EngineSpec, NullEngine, get_engine

        spec = EngineSpec(
            model_id=model_id,
            local_path=local_model_path or model_id,
            vendor_name=vendor_name,
            tensor_parallel_size=tensor_parallel_size,
            pipeline_parallel_size=pipeline_parallel_size,
            max_model_len=max_model_len,
            gpu_memory_utilization=gpu_memory_utilization,
            enforce_eager=enforce_eager,
            max_num_seqs=max_num_seqs,
            device_ids=gpu_ids,
            collect_stats=collect_stats,
            stats_retention=stats_retention,
            stats_push_period_s=stats_push_period_s,
            stats_sample_cap=stats_sample_cap,
            enable_log_requests=enable_log_requests,
        )
        # The host, not a backend-specific exception path, owns the public
        # context-window contract. Retain the exact canonical limit so both
        # JSON and SSE requests can be rejected before generation begins.
        self.max_model_len = spec.max_model_len

        if null_compute:
            latency = float(null_compute_latency_s)
            if not math.isfinite(latency) or latency < 0:
                raise ValueError("null_compute_latency_s must be finite and non-negative")
            self.backend = NullEngine(latency_s=latency)
            print(
                f"[EngineWorker pid={pid}] NullCompute mode on tile {device_id} "
                f"(latency={latency:.2f}s, no engine)",
                flush=True,
            )
        else:
            self.backend = get_engine(engine_name)

        self.backend.create(spec)

        total_s = time.time() - init_start
        print_red(f"[EngineWorker pid={pid}] ★ INIT TOTAL: {total_s:.2f}s ★")

        from .scaling_trace import report_replica_stats

        replica_info = {
            "pid": pid,
            "hostname": hostname,
            "model_id": model_id,
            "device_id": device_id,
            "null_compute": null_compute,
            "total_init_s": round(total_s, 4),
            "wall_start": init_start,
            "wall_end": time.time(),
        }
        init_stats = getattr(self.backend, "init_stats", None)
        if callable(init_stats):
            replica_info.update(init_stats())
        # Compat outcome rides along on the stats channel too: replica stdout
        # does not reach the driver log, so without this a receipt failure is
        # invisible and the readiness gate can only report the symptom.
        replica_info.update(self._publish_compat_receipts(null_compute))
        report_replica_stats(replica_info)
        try:
            self._lifecycle_instance_id = str(ray.get_runtime_context().get_actor_id())
        except (AttributeError, RuntimeError):
            self._lifecycle_instance_id = f"{hostname}:{pid}"
        self._report_lifecycle_started(hostname=hostname, pid=pid)

    def _lifecycle_event(self, event: str, *, detail: str = "") -> dict:
        return _lifecycle_event_envelope(
            identity=_TelemetryIdentity.from_environment(),
            component_id=self._replica_component_id,
            instance_id=self._lifecycle_instance_id,
            event=event,
            model_id=self.model_id,
            node_id=socket.gethostname(),
            pid=os.getpid(),
            detail=detail[:400],
        )

    def _report_lifecycle_started(self, *, hostname: str, pid: int) -> None:
        collector = get_replica_lifecycle_collector()
        payload = self._lifecycle_event("STARTED", detail=f"replica ready on {hostname} pid {pid}")
        accepted = ray.get(collector.report.remote(payload), timeout=10)
        if not accepted:
            raise RuntimeError(
                f"duplicate lifecycle start for {self._replica_component_id} "
                f"instance {self._lifecycle_instance_id}"
            )

    async def _report_lifecycle_terminal(self, event: str, detail: str = "") -> None:
        collector = get_replica_lifecycle_collector()
        payload = self._lifecycle_event(event, detail=detail)
        accepted = await asyncio.wait_for(collector.report.remote(payload), timeout=10.0)
        if not accepted:
            raise RuntimeError(
                f"duplicate lifecycle terminal event for {self._replica_component_id}"
            )

    def _collect_engine_self_receipts(self) -> list:
        """The receipt the spawned EngineCore wrote about itself (EN-01).

        Bounded: the engine writes during its own startup, which has already
        completed by the time this runs, so a short wait covers only clock and
        filesystem skew. Absence leaves the exact engine receipt slot missing.
        """
        from .compat import engine_shim

        directory = getattr(getattr(self, "backend", None), "_engine_receipt_dir", None)
        if (
            not directory
            or self._engine_requirement_id is None
            or self._engine_component_id is None
        ):
            return []
        try:
            return engine_shim.collect(
                directory,
                requirement_id=self._engine_requirement_id,
                component_id=self._engine_component_id,
                timeout_s=5.0,
                consume=True,
            )
        except (OSError, ValueError, TypeError) as exc:
            print(
                f"[EngineWorker pid={os.getpid()}] engine receipt collection failed: "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )
            return []

    def _publish_compat_receipts(self, null_compute: bool) -> Dict[str, Any]:
        """Attest this replica, EngineCore, and every planned vLLM worker.

        The replica self-attests its own postconditions. A real spawned engine
        self-attests through the generated startup shim and its node-local
        authenticated ingress; no owner-built substitute is accepted. Worker
        receipts go directly to the NodeSupervisor on the worker's actual
        node, so this replica never aggregates or re-authors them. Null-compute
        has no separate engine process and therefore no engine-process slots.

        A replica that cannot prove it is correctly patched must not continue
        serving.  The exact ledger still provides the independent global
        readiness proof, but local activation/delivery failures are raised at
        their causal boundary so deployment fails promptly rather than idling
        until the readiness deadline.

        Returns a small dict recorded alongside the replica's init stats.
        """

        from .compat import engine_shim
        from .compat.activator import ActivationError, CompatibilityActivator
        from .compat.producers import attest_self, deliver

        outcome: Dict[str, Any] = {
            "compat_transport": os.environ.get("EXASERVE_RECEIPT_SOCKET", "none")
        }
        try:
            activator = CompatibilityActivator()
            outcome["compat_deployment_id"] = activator.deployment_id
            outcome["compat_generation"] = activator.generation
            # Activation is idempotent; this second pass re-verifies the exact
            # installed sources and semantic sentinels immediately before the
            # replica publishes its own receipt.
            activation = activator.activate("replica")
            outcome["compat_applied"] = sorted(activation.patch_results)
            outcome["compat_not_applicable"] = list(activation.not_applicable)
            receipt = attest_self(
                requirement_id=self._replica_requirement_id,
                role="replica",
                component_id=self._replica_component_id,
                owner_scope="RANK",
                owner_rank=int(os.environ["EXASERVE_RECEIPT_RANK"]),
            )
            outcome["compat_published"] = deliver(receipt)
            # EN-01: prefer the engine's OWN receipt, written from inside the
            # spawned EngineCore by the sitecustomize shim. Missing evidence is
            # never replaced by an owner assertion.
            if not null_compute:
                try:
                    import vllm as _vllm

                    outcome["compat_engine_version"] = f"vllm {_vllm.__version__}"
                except (ImportError, AttributeError) as exc:
                    outcome["compat_engine_version"] = (
                        f"engine (version unavailable: {type(exc).__name__})"
                    )

            if null_compute:
                # Null-compute keeps generation/placement/readiness mechanics
                # but creates no separate EngineCore process. The immutable
                # plan therefore contains no engine-process receipt slot.
                outcome["compat_engine_evidence"] = "not_applicable_null_compute"
                outcome["compat_engine_published"] = False
                return outcome

            self_receipts = self._collect_engine_self_receipts()
            if self_receipts:
                published = 0
                if os.environ.get("EXASERVE_RECEIPT_SOCKET"):
                    # The engine delivered its OWN receipt over the local hop
                    # from inside its process. Forwarding a replica-built copy
                    # here would re-attest somebody else's process, which is
                    # the owner-assertion substitution the audit rejected.
                    published = len(self_receipts)
                else:
                    # A missing socket means the engine could not have used the
                    # only authoritative local hop. Diagnostic v2 files are not
                    # re-attested by the replica.
                    published = 0
                outcome["compat_engine_evidence"] = "self"
                outcome["compat_engine_published"] = published > 0
                outcome["compat_engine_receipts"] = published
                outcome["compat_engine_patches_delivered"] = (
                    os.environ.get("EXASERVE_ENGINE_SHIM_PATCHES") == "1"
                )
                if not outcome["compat_engine_published"]:
                    raise RuntimeError(
                        "engine self-receipt exists only as a diagnostic file; "
                        "the authoritative node-local delivery did not occur"
                    )
            else:
                # Audit #16: substituting an owner assertion here turned FAILED
                # engine injection into READY. No self-receipt is a causal
                # startup failure, not an invitation to keep serving unproved.
                outcome["compat_engine_evidence"] = "missing"
                outcome["compat_engine_published"] = False
                receipt_dir = getattr(getattr(self, "backend", None), "_engine_receipt_dir", "")
                diagnostic = engine_shim.latest_error(receipt_dir) if receipt_dir else None
                diagnostic_text = ""
                if diagnostic is not None:
                    diagnostic_text = (
                        f"; engine diagnostic: {diagnostic['error_type']}: {diagnostic['error']}"
                    )
                raise RuntimeError(
                    "spawned engine wrote no exact self-receipt; refusing to "
                    f"serve without engine compatibility proof{diagnostic_text}"
                )
        except ActivationError as exc:
            print(
                f"[EngineWorker pid={os.getpid()}] compatibility activation "
                f"FAILED (replica startup is fatal): {exc}",
                flush=True,
            )
            raise
        except Exception as exc:  # transport/import faults are fatal and causal
            print(
                f"[EngineWorker pid={os.getpid()}] compatibility receipt "
                f"delivery FAILED: {type(exc).__name__}: {exc}",
                flush=True,
            )
            raise RuntimeError(f"compatibility attestation failed: {exc}") from exc
        return outcome

    async def reconfigure(self, user_config):
        """Serve awaits this pre-healthy when a deployment sets user_config (see
        deploy_model). Engines that need a warmup (e.g. SGLang JIT kernels) run it
        here; others are a no-op."""
        await self.backend.warmup()
        from .observability import mark_replica_ready

        mark_replica_ready()

    async def __del__(self):
        """Stop owned work and externally attest the destructor result.

        Ray Serve logs and suppresses exceptions from this hook. The independent
        lifecycle collector is queried by the deployment driver after shutdown,
        so either a FAILED event or a missing STOPPED event remains fatal.
        """
        backend = getattr(self, "backend", None)
        try:
            if backend is not None:
                await backend.shutdown()
        except BaseException as exc:
            try:
                await self._report_lifecycle_terminal("FAILED", f"{type(exc).__name__}: {exc}")
            except BaseException as report_exc:
                add_exception_note(
                    exc, f"replica lifecycle failure report also failed: {report_exc}"
                )
            raise
        await self._report_lifecycle_terminal("STOPPED", "backend shutdown completed")

    # ---- HTTP endpoints ------------------------------------------------------

    @app.get("/health")
    async def health_check(self):
        return JSONResponse({"status": "healthy", "model": self.model_id})

    @app.get("/metrics")
    async def metrics(self):
        """PR-032/TD-METRICS: the ExaServe-owned operational surface.

        Ray's dashboard is disabled on this stack and stdout is not a contract,
        so this is how an operator asks a live replica how it is doing.
        """
        from fastapi.responses import PlainTextResponse

        from .observability import render_metrics

        return PlainTextResponse(render_metrics(), media_type="text/plain; version=0.0.4")

    @app.get("/stats")
    async def stats(self):
        pid = os.getpid()
        if self.null_compute:
            return JSONResponse({"pid": pid, "model": self.model_id, "null_compute": True})
        result = {"pid": pid, "model": self.model_id}
        result.update(self.backend.live_stats())
        return JSONResponse(result)

    def collect_stats(self) -> dict:
        """Called via ray.get(actor_handle.collect_stats.remote())."""
        data = self.backend.collect_stats()
        data.setdefault("pid", os.getpid())
        data.setdefault("model", self.model_id)
        return data

    @app.get("/v1/models")
    async def list_models(self):
        return JSONResponse(
            {
                "object": "list",
                "data": [
                    {
                        "id": self.model_id,
                        "object": "model",
                        "created": int(time.time()),
                        "owned_by": "exaserve",
                    }
                ],
            }
        )

    @app.post("/v1/chat/completions")
    async def chat_completions(self, request: Request):
        correlation = _correlation_id(request.headers, f"req-{uuid.uuid4().hex[:16]}")
        try:
            body = await request.json()
        except Exception:
            return JSONResponse(
                {"error": "malformed JSON body"},
                status_code=400,
                headers={"X-Request-ID": correlation},
            )
        try:
            _rv.require_object_body(body)  # IMP-H04: list/scalar body -> 400
            _rv.validate_model_field(body, self._served_model_names())
            _rv.validate_messages(body.get("messages"))
            _rv.validate_chat_options(body)
            sampling = self._parse_sampling(body)
            stream = _rv.strict_flag(body, "stream", False)
            add_generation_prompt = _rv.strict_flag(body, "add_generation_prompt", True)
            continue_final_message = _rv.strict_flag(body, "continue_final_message", False)
        except _rv.RequestValidationError as exc:
            return JSONResponse(
                {"error": str(exc)}, status_code=400, headers={"X-Request-ID": correlation}
            )
        messages = body.get("messages", [])

        prompt = self.backend.build_chat_prompt(
            messages,
            add_generation_prompt=add_generation_prompt,
            continue_final_message=continue_final_message,
            chat_template=body.get("chat_template"),
            chat_template_kwargs=body.get("chat_template_kwargs") or {},
        )
        preflight_error = self._context_preflight_response(
            prompt,
            sampling,
            correlation=correlation,
            prompt_param="messages",
        )
        if preflight_error is not None:
            return preflight_error

        request_id = f"chatcmpl-{uuid.uuid4().hex[:12]}"
        sampling["_request_id"] = request_id
        # PR-032: preserve a caller-supplied correlation id (linked to, not
        # conflated with, our completion id) so a request can be traced
        # gateway -> Serve -> engine.
        sampling["_correlation_id"] = correlation
        _emit_request_link(
            route="/v1/chat/completions",
            request_id=correlation,
            completion_id=request_id,
            model_id=self.model_id,
        )
        if stream:
            return StreamingResponse(
                self._chat_stream(request_id, prompt, sampling),
                media_type="text/event-stream",
                headers={"X-Request-ID": correlation},
            )
        return await self._chat_non_stream(request_id, prompt, sampling)

    @app.post("/v1/completions")
    async def completions(self, request: Request):
        correlation = _correlation_id(request.headers, f"req-{uuid.uuid4().hex[:16]}")
        try:
            body = await request.json()
        except Exception:
            return JSONResponse(
                {"error": "malformed JSON body"},
                status_code=400,
                headers={"X-Request-ID": correlation},
            )
        try:
            _rv.require_object_body(body)  # IMP-H04: list/scalar body -> 400
            _rv.validate_model_field(body, self._served_model_names())
            sampling = self._parse_sampling(body)
            stream = _rv.strict_flag(body, "stream", False)
            prompt = _rv.validate_prompt(body.get("prompt", ""))
        except _rv.RequestValidationError as exc:
            return JSONResponse(
                {"error": str(exc)}, status_code=400, headers={"X-Request-ID": correlation}
            )
        preflight_error = self._context_preflight_response(
            prompt,
            sampling,
            correlation=correlation,
            prompt_param="prompt",
        )
        if preflight_error is not None:
            return preflight_error
        request_id = f"cmpl-{uuid.uuid4().hex[:12]}"
        sampling["_request_id"] = request_id
        sampling["_correlation_id"] = correlation
        _emit_request_link(
            route="/v1/completions",
            request_id=correlation,
            completion_id=request_id,
            model_id=self.model_id,
        )
        if stream:
            return StreamingResponse(
                self._completion_stream(request_id, prompt, sampling),
                media_type="text/event-stream",
                headers={"X-Request-ID": correlation},
            )
        res = await self.backend.generate(prompt, sampling)
        _corr = sampling.get("_correlation_id", request_id)  # PR-032
        if res.error:
            return JSONResponse(
                {"error": res.error}, status_code=500, headers={"X-Request-ID": _corr}
            )
        _record_tokens(prompt_tokens=res.prompt_tokens, completion_tokens=res.completion_tokens)
        return JSONResponse(
            {
                "id": request_id,
                "object": "text_completion",
                "created": int(time.time()),
                "model": self.model_id,
                "choices": [{"index": 0, "text": res.text, "finish_reason": res.finish_reason}],
                "usage": {
                    "prompt_tokens": res.prompt_tokens,
                    "completion_tokens": res.completion_tokens,
                    "total_tokens": res.prompt_tokens + res.completion_tokens,
                    "token_count_source": res.token_count_source,
                },
            },
            headers={"X-Request-ID": _corr},  # PR-032: echo the correlation id
        )

    # ---- Internal helpers ----------------------------------------------------

    def _validate_request_context(self, prompt: str, sampling: dict, *, prompt_param: str) -> None:
        prompt_tokens = self.backend.count_prompt_tokens(prompt)
        _rv.validate_context_window(
            prompt_tokens=prompt_tokens,
            requested_completion_tokens=sampling["max_tokens"],
            max_model_len=self.max_model_len,
            prompt_param=prompt_param,
        )

    def _context_preflight_response(
        self,
        prompt: str,
        sampling: dict,
        *,
        correlation: str,
        prompt_param: str,
    ) -> Optional[JSONResponse]:
        try:
            self._validate_request_context(prompt, sampling, prompt_param=prompt_param)
        except _rv.ContextLengthExceeded as exc:
            return _typed_api_error(
                correlation=correlation,
                status_code=400,
                message=str(exc),
                error_type=exc.error_type,
                code=exc.code,
                param=exc.param,
            )
        except Exception as exc:
            print(
                f"[EngineWorker] prompt tokenization failed for request {correlation}: "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )
            return _typed_api_error(
                correlation=correlation,
                status_code=500,
                message="the model tokenizer could not validate this request",
                error_type="server_error",
                code="prompt_tokenization_failed",
                param=None,
            )
        return None

    def _served_model_names(self) -> set:
        """Identities a client may name in the `model` field for THIS
        replica: the HF id, plus its route name (PR-011)."""
        names = {self.model_id}
        try:
            from .model_paths import get_model_storage_name

            storage = get_model_storage_name(self.model_id)
            names.add(storage)
            names.add(storage.replace(".", "-"))  # route name
        except (TypeError, ValueError) as exc:
            print(
                f"[EngineWorker] model alias derivation failed for {self.model_id!r}: "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )
        return names

    @staticmethod
    def _parse_sampling(body: dict) -> dict:
        """OpenAI request body -> validated neutral sampling dict (PR-011)."""
        return _rv.parse_sampling(body)

    async def _chat_non_stream(self, request_id: str, prompt: str, sampling: dict):
        res = await self.backend.generate(prompt, sampling)
        correlation = sampling.get("_correlation_id", request_id)
        if res.error:
            return JSONResponse(
                {"error": res.error}, status_code=500, headers={"X-Request-ID": correlation}
            )
        _record_tokens(prompt_tokens=res.prompt_tokens, completion_tokens=res.completion_tokens)
        return JSONResponse(
            {
                "id": request_id,
                "object": "chat.completion",
                "created": int(time.time()),
                "model": self.model_id,
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": res.text},
                        "finish_reason": res.finish_reason,
                    }
                ],
                "usage": {
                    "prompt_tokens": res.prompt_tokens,
                    "completion_tokens": res.completion_tokens,
                    "total_tokens": res.prompt_tokens + res.completion_tokens,
                    "token_count_source": res.token_count_source,
                },
            },
            headers={"X-Request-ID": correlation},
        )

    async def _chat_stream(self, request_id: str, prompt: str, sampling: dict):
        created = int(time.time())
        usage = None
        async for chunk in self.backend.generate_stream(prompt, sampling):
            if chunk.finish_reason is not None:
                _record_tokens(
                    prompt_tokens=chunk.prompt_tokens,
                    completion_tokens=chunk.completion_tokens,
                )
                usage = {
                    "prompt_tokens": chunk.prompt_tokens,
                    "completion_tokens": chunk.completion_tokens,
                    "total_tokens": chunk.prompt_tokens + chunk.completion_tokens,
                    "token_count_source": chunk.token_count_source,
                }
            if chunk.delta:
                sse = {
                    "id": request_id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": self.model_id,
                    "choices": [
                        {"index": 0, "delta": {"content": chunk.delta}, "finish_reason": None}
                    ],
                }
                yield f"data: {json.dumps(sse)}\n\n"
            if chunk.finish_reason is not None:
                final = {
                    "id": request_id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": self.model_id,
                    "choices": [{"index": 0, "delta": {}, "finish_reason": chunk.finish_reason}],
                }
                if usage:
                    final["usage"] = usage
                yield f"data: {json.dumps(final)}\n\n"
        yield "data: [DONE]\n\n"

    async def _completion_stream(self, request_id: str, prompt: str, sampling: dict):
        created = int(time.time())
        usage = None
        async for chunk in self.backend.generate_stream(prompt, sampling):
            if chunk.finish_reason is not None:
                _record_tokens(
                    prompt_tokens=chunk.prompt_tokens,
                    completion_tokens=chunk.completion_tokens,
                )
                usage = {
                    "prompt_tokens": chunk.prompt_tokens,
                    "completion_tokens": chunk.completion_tokens,
                    "total_tokens": chunk.prompt_tokens + chunk.completion_tokens,
                    "token_count_source": chunk.token_count_source,
                }
            if chunk.delta:
                sse = {
                    "id": request_id,
                    "object": "text_completion",
                    "created": created,
                    "model": self.model_id,
                    "choices": [{"index": 0, "text": chunk.delta, "finish_reason": None}],
                }
                yield f"data: {json.dumps(sse)}\n\n"
            if chunk.finish_reason is not None:
                final = {
                    "id": request_id,
                    "object": "text_completion",
                    "created": created,
                    "model": self.model_id,
                    "choices": [{"index": 0, "text": "", "finish_reason": chunk.finish_reason}],
                }
                if usage:
                    final["usage"] = usage
                yield f"data: {json.dumps(final)}\n\n"
        yield "data: [DONE]\n\n"


# ═══════════════════════════════════════════════════════════════════════════
# Canonical deployment helpers
# ═══════════════════════════════════════════════════════════════════════════
# ---------------------------------------------------------------------------
# Deployment helpers
# ---------------------------------------------------------------------------
def deploy_model(
    model_config: ModelPlan,
    model_path_map: Dict[str, str],
    config: DeploymentPlan,
    *,
    replica_index: int,
    planned_placement: Optional[BoundReplica],
    native_replicas: int = 1,
) -> tuple:
    """
    Build a bound VLLMWorker deployment for one model.

    Ray Serve options (num_gpus, replicas, max_ongoing_requests, …) are passed
    via .options() so no factory/class-creation indirection is needed.

    Canonical managed/direct topologies build one exactly placed application
    per logical replica. Native HeadOnly builds one public Serve deployment;
    Serve schedules its replicas and each live actor binds its observed
    rank/device tuple back to one exact canonical slot before engine import.

    Returns:
        (deployment, model_id)
    """
    model_id = model_config.model_id
    local_path = model_path_map.get(model_id, model_id)
    null_compute = config.runtime.null_compute

    safe_name = model_config.route_name
    native_head_only = planned_placement is None
    if native_head_only:
        if replica_index != -1:
            raise ValueError("native Serve placement requires replica_index=-1")
        if (
            isinstance(native_replicas, bool)
            or not isinstance(native_replicas, int)
            or native_replicas < 1
        ):
            raise ValueError("native_replicas must be a positive integer")
    elif native_replicas != 1:
        raise ValueError("canonical bound placement creates exactly one replica")
    deployment_name_suffix = "-native" if native_head_only else f"-r{replica_index}"

    print(
        f"[ExaServe] Configuring EngineWorker for {model_id}\n"
        f"  Replica     : {replica_index} "
        f"(TP={model_config.tensor_parallel_size}, PP={model_config.pipeline_parallel_size})\n"
        f"  NullCompute : {null_compute}\n"
        f"  Local path  : {local_path}",
        flush=True,
    )
    if null_compute:
        latency = config.runtime.null_compute_latency_s
        print(
            f"[ExaServe] NULL-COMPUTE mode — engine replaced by sleep({latency:.2f}s)",
            flush=True,
        )

    placement_group_bundles: Optional[List[Dict[str, float]]] = None
    placement_group_strategy: Optional[str] = None
    actor_num_gpus: int
    # The compatibility gate is model-local. A deployment may legitimately
    # mix PP and TP-only models; using an allocation-wide "any PP" switch made
    # TP-only replicas claim patches that were neither needed nor applied.
    from .compat.profile import (
        MULTIPROC_WORKER_PATCH_GATE,
        PP_PATCH_GATE,
        RAY_WORKER_PATCH_GATE,
    )

    uses_ray_workers = model_config.pipeline_parallel_size > 1
    uses_multiproc_workers = (
        model_config.pipeline_parallel_size == 1 and model_config.tensor_parallel_size > 1
    )
    extra_env_vars: Dict[str, str] = {
        PP_PATCH_GATE: "1" if uses_ray_workers else "0",
        RAY_WORKER_PATCH_GATE: "1" if uses_ray_workers else "0",
        MULTIPROC_WORKER_PATCH_GATE: "1" if uses_multiproc_workers else "0",
    }

    if native_head_only and model_config.pipeline_parallel_size > 1:
        raise ValueError("native HeadOnly placement does not support pipeline parallelism")
    if model_config.pipeline_parallel_size > 1:
        assert planned_placement is not None
        placement_group_bundles, stage_node_ips = build_pp_placement_group_bundles(
            model_config, planned_placement
        )
        placement_group_strategy = "PACK"
        actor_num_gpus = 0
        print(
            f"[ExaServe] Canonical PP replica {replica_index} for {model_id}: "
            f"stage nodes={stage_node_ips}, bundles={len(placement_group_bundles)}",
            flush=True,
        )
    else:
        actor_num_gpus = model_config.tensor_parallel_size
        if native_head_only:
            print(
                f"[ExaServe] Native Ray Serve placement for {native_replicas} TP replica(s) "
                f"of {model_id}; each actor must match one canonical rank/device slot",
                flush=True,
            )
        else:
            assert planned_placement is not None
            print(
                f"[ExaServe] Canonical TP replica {replica_index} for {model_id}: "
                f"node={planned_placement.node_ips[0]}",
                flush=True,
            )

    # Engine selection is immutable DeploymentPlan content. The environment
    # carries the same value only for native dependencies and child attestation.
    engine = config.engine

    deployment_options = dict(
        name=f"EngineWorker-{safe_name}{deployment_name_suffix}",
        num_replicas=native_replicas,
        ray_actor_options={
            "num_gpus": actor_num_gpus,
            "num_cpus": model_config.num_cpus_per_replica,
            "runtime_env": build_actor_runtime_env(
                extra_env_vars,
                receipt_owner_rank=(None if native_head_only else planned_placement.owner_rank),
                dynamic_receipt_owner=native_head_only,
            ),
            **(
                {"resources": {planned_placement.node_resource_keys[0]: 0.001}}
                if planned_placement is not None and placement_group_bundles is None
                else {}
            ),
        },
        max_ongoing_requests=config.replica_max_ongoing_requests,
        health_check_period_s=config.readiness.serve_replica_health_check_period_s,
        health_check_timeout_s=config.readiness.serve_replica_health_check_timeout_s,
    )
    if placement_group_bundles is not None:
        deployment_options["placement_group_bundles"] = placement_group_bundles
        deployment_options["placement_group_strategy"] = placement_group_strategy
    # A non-None user_config makes Serve await EngineWorker.reconfigure during
    # every replica's pre-healthy phase. SGLang performs JIT warmup there;
    # other engines are a no-op. The shared hook also marks replica metrics as
    # ready at the same lifecycle point for every backend.
    deployment_options["user_config"] = {"warmup": True}

    deployment = EngineWorker.options(**deployment_options).bind(
        model_id=model_id,
        replica_index=replica_index,
        local_model_path=local_path,
        null_compute=null_compute,
        tensor_parallel_size=model_config.tensor_parallel_size,
        pipeline_parallel_size=model_config.pipeline_parallel_size,
        gpu_memory_utilization=model_config.gpu_memory_utilization,
        max_model_len=model_config.max_model_len,
        enforce_eager=model_config.enforce_eager,
        max_num_seqs=model_config.max_num_seqs,
        collect_stats=getattr(config, "collect_stats", False),
        enable_log_requests=model_config.enable_log_requests,
        engine_name=engine,
        vendor_name=config.vendor,
        null_compute_latency_s=config.runtime.null_compute_latency_s,
        stats_retention=config.runtime.stats_retention,
        stats_push_period_s=config.runtime.stats_push_period_s,
        stats_sample_cap=config.runtime.stats_sample_cap,
    )

    return deployment, model_id


def deploy_from_canonical_binding(
    config: DeploymentPlan,
    model_path_map: Dict[str, str],
    replica_plan: BoundDeployment,
) -> None:
    """Deploy every canonical model using its allocation-bound replica slots."""
    active_plans = replica_plan.models
    if not active_plans:
        raise RuntimeError("canonical DeploymentPlan contains no bound models")

    use_root_route = len(config.models) == 1
    if not use_root_route:
        print(
            f"[ExaServe] Deploying {len(active_plans)} active planned models "
            "with per-model route_prefix",
            flush=True,
        )

    for model_index, model_plan in enumerate(active_plans):
        model_config = model_plan.model
        print(
            f"\n[ExaServe] ═══ Deploying planned model {model_index + 1}/{len(active_plans)} ═══",
            flush=True,
        )

        safe_name = model_config.route_name
        n_rep = model_plan.assigned_replicas
        if config.uses_head_only_serve_proxy():
            # Preserve the paper's native Ray Serve baseline: one root-route
            # deployment with N replicas, so Ray Serve's own HeadOnly proxy
            # performs request-to-replica balancing. Each actor derives and
            # proves its canonical slot from its live rank/device assignment.
            deployment, model_id = deploy_model(
                model_config,
                model_path_map,
                config,
                replica_index=-1,
                planned_placement=None,
                native_replicas=n_rep,
            )
            with tracer.phase("serve.run", model_id=model_id, replicas=n_rep):
                serve.run(deployment, name=safe_name, route_prefix="/")
            print(
                f"[ExaServe] ✓ native HeadOnly route http://localhost:8000/v1 "
                f"(model: {model_id}, replicas={n_rep})",
                flush=True,
            )
            continue
        if n_rep == 1:
            deployment, model_id = deploy_model(
                model_config,
                model_path_map,
                config,
                replica_index=0,
                planned_placement=model_plan.replicas[0],
            )
            route_prefix = "/" if use_root_route else f"/{safe_name}"
            with tracer.phase("serve.run", model_id=model_id, replicas=1):
                serve.run(
                    deployment,
                    name=safe_name,
                    route_prefix=route_prefix,
                )
            print(
                f"[ExaServe] Service available at http://localhost:8000{route_prefix} "
                f"(model: {model_id}, replicas=1)",
                flush=True,
            )
            continue

        # Serve applies one placement template to every replica in a deployment,
        # so it cannot express the compiler's per-replica node binding.  Build
        # one single-replica application per logical slot and submit all slots in
        # one public run_many call.  HAProxy rewrites the canonical route to a
        # replica route; direct validation probes a concrete replica route.
        if not callable(getattr(serve, "run_many", None)) or not hasattr(serve, "RunTarget"):
            raise RuntimeError(
                "canonical multi-replica deployment requires the SiteProfile "
                "capability ray_serve.run_many and serve.RunTarget"
            )
        targets = []
        for replica_index, placement in enumerate(model_plan.replicas):
            deployment, model_id = deploy_model(
                model_config,
                model_path_map,
                config,
                replica_index=replica_index,
                planned_placement=placement,
            )
            targets.append(
                serve.RunTarget(
                    target=deployment,
                    name=f"{safe_name}_r{replica_index}",
                    route_prefix=f"/{safe_name}_r{replica_index}",
                )
            )
        with tracer.phase("serve.run_many", model_id=model_id, replicas=n_rep):
            try:
                serve.run_many(targets, wait_for_applications_running=True)
            except Exception as deploy_exc:
                raise RuntimeError(
                    f"canonical deployment failed for {n_rep} replicas of {model_id}: {deploy_exc}"
                ) from deploy_exc
        print(
            f"[ExaServe] ✓ {model_id}: all {n_rep} bound replica applications RUNNING",
            flush=True,
        )


def deploy_proxy_anchors(replica_plan: BoundDeployment) -> None:
    """Materialize the plan's exact per-rank Serve proxy topology.

    This uses only the public multi-application and custom-resource surfaces.
    The route-less applications are included in the canonical exact
    application predicate, so a missing/misplaced anchor cannot be hidden by
    model readiness.
    """
    if not callable(getattr(serve, "run_many", None)) or not hasattr(serve, "RunTarget"):
        raise RuntimeError(
            "canonical per-node proxy topology requires the SiteProfile "
            "capability ray_serve.run_many and serve.RunTarget"
        )
    from .control.plan_readiness import PROXY_ANCHOR_APP_PREFIX

    targets = []
    for bound_node in replica_plan.nodes:
        rank = bound_node.rank
        inventory = bound_node.inventory
        app_name = f"{PROXY_ANCHOR_APP_PREFIX}{rank}"
        anchor = ProxyAnchor.options(
            name=f"ProxyAnchor-rank-{rank}",
            num_replicas=1,
            ray_actor_options={
                "num_cpus": 0,
                "resources": {inventory.resource_key: 0.001},
                "runtime_env": build_actor_runtime_env(receipt_owner_rank=rank),
            },
        ).bind()
        targets.append(serve.RunTarget(target=anchor, name=app_name, route_prefix=None))
    if len(targets) != replica_plan.plan.num_nodes:
        raise RuntimeError(
            "proxy-anchor binding does not cover every planned rank: "
            f"{len(targets)}/{replica_plan.plan.num_nodes}"
        )
    with tracer.phase("serve.proxy_anchors", nodes=len(targets)):
        try:
            serve.run_many(targets, wait_for_applications_running=True)
        except Exception as deploy_exc:
            raise RuntimeError(
                f"canonical proxy-anchor deployment failed for {len(targets)} node(s): {deploy_exc}"
            ) from deploy_exc
    print(f"[ExaServe] ✓ {len(targets)} per-node proxy anchor(s) RUNNING", flush=True)


def main() -> None:
    """The deployment entry point (plan WP4.1).

    This was a bare ``__main__`` block, which meant the deployment could
    only ever be run by executing the module: nothing could call it, and
    every name it bound leaked into module scope (the ``for app in ...``
    loop below silently rebound the module-level FastAPI ``app``). As a
    function its locals stay local and the lifecycle is callable.

    The owning composition root executes ``exaserve.server_bootstrap`` in a
    fresh process. That bootstrap verifies and activates compatibility before
    importing this module; this function then re-verifies the plan identity.
    """
    overall_start = time.time()

    parser = argparse.ArgumentParser(description="Ray Serve LLM inference on Aurora")
    parser.add_argument("--plan", required=True, help="Verified canonical DeploymentPlan artifact")
    args = parser.parse_args()

    config_path = os.path.abspath(args.plan)
    if not os.path.isfile(config_path):
        raise SystemExit(f"[ExaServe] Plan artifact not found: {config_path}")

    # ---- Load the one verified plan; never reinterpret source YAML ----------
    from .plan.io import load_deployment_plan
    from .plan.runtime_environment import runtime_environment

    canonical_plan = load_deployment_plan(config_path)
    expected_plan_hash = os.environ.get("EXASERVE_PLAN_HASH", "")
    if expected_plan_hash != canonical_plan.deployment_plan_hash:
        raise SystemExit(
            f"[ExaServe] Plan hash mismatch: environment={expected_plan_hash!r}, "
            f"artifact={canonical_plan.deployment_plan_hash!r}"
        )
    # Overwrite every legacy runtime selector from the verified plan.  Any
    # ambient value now becomes an output compatibility variable, never an
    # un-hashed input to runtime behavior.
    os.environ.update(runtime_environment(canonical_plan))
    config = canonical_plan
    # Managed gateways and direct validation use EveryNode.  The explicit
    # native-Ray benchmark preserves Ray Serve's HeadOnly topology without
    # pretending it is a managed external gateway.
    _gateway_kind = canonical_plan.gateway.kind if canonical_plan.gateway is not None else "none"
    _ray_serve_proxy_location = (
        ProxyLocation.HeadOnly
        if canonical_plan.uses_head_only_serve_proxy()
        else ProxyLocation.EveryNode
    )
    print(
        f"[ExaServe] Loaded canonical plan "
        f"{canonical_plan.deployment_plan_hash[:12]}: {config.deployment_name}",
        flush=True,
    )
    print(
        f"[ExaServe] gateway.kind={_gateway_kind!r} -> "
        f"Ray Serve proxy_location={_ray_serve_proxy_location}",
        flush=True,
    )
    print(f"[ExaServe] Models: {len(config.models)}", flush=True)
    for cfg in config.models:
        print(
            f"  - {cfg.model_id} "
            f"(size={cfg.size_b}B, TP={cfg.tensor_parallel_size}, PP={cfg.pipeline_parallel_size})",
            flush=True,
        )

    # ---- Serve Init: ray.init + serve.start ----------------------------------
    stage1_start = time.monotonic()
    ray_address = os.environ.get("RAY_ADDRESS", "auto")
    tracer.set_metadata(
        ray_address=ray_address,
        num_nodes=config.num_nodes,
        num_gpus_per_node=config.num_gpus_per_node,
        models=[cfg.model_id for cfg in config.models],
        deployment_plan_hash=canonical_plan.deployment_plan_hash,
        generation=int(os.environ.get("EXASERVE_GENERATION", "0") or 0),
        run_semantic_hash=os.environ.get("EXASERVE_RUN_SEMANTIC_HASH", ""),
        source_snapshot_hash=os.environ.get("EXASERVE_SOURCE_SNAPSHOT_HASH", ""),
        gateway_kind=(
            canonical_plan.gateway.kind if canonical_plan.gateway is not None else None
        ),
        exposure_mode=canonical_plan.exposure.mode,
        null_compute=canonical_plan.runtime.null_compute,
        expected_model_replicas=sum(model.num_replicas for model in canonical_plan.models),
        expected_receipt_requirements=len(canonical_plan.receipt_requirements),
    )
    from .control.plan_readiness import planned_application_names

    tracer.set_metadata(
        expected_serve_applications=len(planned_application_names(canonical_plan)),
    )
    print(
        f"[ExaServe] Serve Init: Connecting to Ray cluster at {ray_address}...",
        flush=True,
    )

    verify_ray_serve_timing_contract(canonical_plan)

    with tracer.phase("ray.init"):
        init_ray_cluster(ray_address, namespace="serve", include_dashboard=False)

    # Create stats collector actor for per-replica init timing (replaces
    # per-file Lustre I/O).  Must be created after ray.init, before serve.run.
    from .scaling_trace import create_stats_collector, collect_replica_stats

    expected_stats_replicas = sum(int(model.num_replicas) for model in canonical_plan.models)
    create_stats_collector(expected_stats_replicas)
    if canonical_plan.collect_stats:
        create_serving_stats_collector(expected_stats_replicas)
    _lifecycle_components = [
        requirement.component_slot
        for requirement in canonical_plan.receipt_requirements
        if requirement.role == "replica"
    ]
    if len(_lifecycle_components) != expected_stats_replicas:
        raise RuntimeError(
            "canonical plan replica lifecycle slots do not match assigned replicas: "
            f"slots={len(_lifecycle_components)} assigned={expected_stats_replicas}"
        )
    create_replica_lifecycle_collector(_lifecycle_components)

    # IMP-B04: no receipt channel is created here. Receipts leave this node
    # over the bounded local hop to the NodeSupervisor and then the
    # authenticated §3.2 channel; the detached Ray actor that used to carry
    # them was deleted at WP13 because §3.2.1 does not accept it as an
    # authoritative readiness source.

    _verify_core_env()

    with tracer.phase("serve.start", proxy_location=str(_ray_serve_proxy_location)):
        serve.start(
            http_options=HTTPOptions(
                host="0.0.0.0",
                location=_ray_serve_proxy_location,
                # One immutable field feeds the Serve listener, rank-local
                # probes, and gateway backends.  A literal port here makes an
                # otherwise valid non-default plan fail after cluster startup.
                port=canonical_plan.exposure.serve_port,
            )
        )
    print(
        f"[ExaServe] HTTP proxy location: {_ray_serve_proxy_location}, "
        f"host=0.0.0.0, port={canonical_plan.exposure.serve_port}",
        flush=True,
    )
    tracer.record_phase("stage1.total", time.monotonic() - stage1_start)
    print_red(f"[ExaServe] ✓ Serve Init completed in {time.monotonic() - stage1_start:.2f}s")

    # ---- GPU Poll: wait for all nodes to register --------------------------
    expected_gpus = config.num_gpus_per_node * config.num_nodes
    membership_timeout_s = float(canonical_plan.readiness.initial_deadline_s)
    membership_poll_s = float(canonical_plan.readiness.validation_interval_s)
    deadline = time.monotonic() + membership_timeout_s
    total_gpus = 0
    poll_iteration = 0
    with tracer.phase("node_registration_poll", expected_gpus=expected_gpus):
        while time.monotonic() < deadline:
            iter_start = time.monotonic()

            resources = tracer.timed_call("ray.cluster_resources", ray.cluster_resources)
            total_gpus = int(resources.get("GPU", 0))

            nodes_result = tracer.timed_call("ray.nodes", ray.nodes)
            alive_nodes = sum(1 for n in nodes_result if n.get("Alive"))

            iter_elapsed = time.monotonic() - iter_start
            tracer.record_poll_iteration(
                "node_registration",
                poll_iteration,
                elapsed_s=iter_elapsed,
                alive_nodes=alive_nodes,
                total_gpus=total_gpus,
                expected_gpus=expected_gpus,
                pct=round(total_gpus / max(expected_gpus, 1) * 100, 1),
            )

            if total_gpus >= expected_gpus:
                break

            print(
                f"[ExaServe] Waiting for nodes: {alive_nodes} alive, "
                f"{total_gpus}/{expected_gpus} GPUs ({total_gpus / max(expected_gpus, 1) * 100:.0f}%)",
                flush=True,
            )
            poll_iteration += 1
            time.sleep(membership_poll_s)

    tracer.set_metadata(actual_gpus=total_gpus, alive_nodes=alive_nodes)
    if total_gpus < expected_gpus:
        # Fail closed.  The canonical lifecycle deliberately has no DEGRADED
        # state and ambient environment cannot weaken its resource predicate.
        tracer.set_metadata(resource_membership_failed=True)
        message = (
            f"Only {total_gpus}/{expected_gpus} GPUs registered after the "
            f"{membership_timeout_s:g}s deadline "
            f"({total_gpus / max(expected_gpus, 1) * 100:.0f}%)."
        )
        raise RuntimeError(f"[ExaServe] {message} Refusing to deploy with missing resources.")
    else:
        print(
            f"[ExaServe] All {total_gpus}/{expected_gpus} GPUs registered",
            flush=True,
        )

    # ---- Model Resolution: resolve staged local models ---------------------
    null_compute = canonical_plan.runtime.null_compute
    if null_compute:
        print(
            "[ExaServe] Model Resolution: NULL-COMPUTE mode — model staging skipped",
            flush=True,
        )
        model_path_map = {cfg.model_id: cfg.model_id for cfg in config.models}
    else:
        stage_start = time.time()
        print(
            f"[ExaServe] Model Resolution: Resolving staged models from {config.local_stage_path}...",
            flush=True,
        )
        model_path_map = resolve_model_paths(
            config.models,
            config.local_stage_path,
            require_complete=True,
        )
        print_red(f"[ExaServe] ✓ Model Resolution completed in {time.time() - stage_start:.2f}s")

    # ---- Model Deploy: deploy model services to Ray Serve -------------------
    stage3_start = time.monotonic()
    print("[ExaServe] Model Deploy: Deploying model services to Ray Serve...", flush=True)

    # WP4.1: the deployment lifecycle is an addressable object with typed
    # operations and a state machine, not a straight line through main(). The
    # manager owns prepare -> deploy -> validate -> drain -> stop; the
    # operations themselves are the existing, validated internals.
    from .control.deployment import DeploymentManager as _DeploymentManager

    _deploy_manager = _DeploymentManager(
        deployment_id=_deployment_scope(),
        generation=int(os.environ.get("EXASERVE_GENERATION", "0") or 0),
    )
    _deploy_manager.prepare()  # staging completed above this point

    # KI-D4/TD-PP-MULTI/KI-A5: check the config against declared capabilities
    # before deploying. A capability whose absence changes the ANSWER refuses
    # here; one that only changes performance degrades loudly and is recorded.
    from .capabilities import validate_deployment as _validate_capabilities

    _capability_report = _validate_capabilities(config)
    print(f"[Capability] {_capability_report}", flush=True)

    # Bind the compiler's exact topology to live Ray node addresses.  Resource
    # counts and placements are never recomputed after plan verification.
    with tracer.phase("bind_canonical_replica_plan"):
        from .plan.io import load_allocation_binding
        from .plan.runtime_binding import bind_runtime_deployment

        planner_nodes = build_node_inventory()
        if not planner_nodes:
            raise RuntimeError("No alive Ray GPU nodes found for canonical placement")
        allocation_binding = load_allocation_binding(os.environ["EXASERVE_ALLOCATION_BINDING_PATH"])
        replica_plan = bind_runtime_deployment(canonical_plan, allocation_binding, planner_nodes)
    print(format_runtime_binding(replica_plan), flush=True)
    with tracer.phase("deploy_from_canonical_plan"):
        if not canonical_plan.uses_head_only_serve_proxy():
            deploy_proxy_anchors(replica_plan)
        deploy_from_canonical_binding(config, model_path_map, replica_plan)

    _deploy_manager.deploy()
    tracer.record_phase("stage3.total", time.monotonic() - stage3_start)
    print_red(f"[ExaServe] ✓ Model Deploy completed in {time.monotonic() - stage3_start:.2f}s")

    # ---- Collect per-replica stats via Ray actor (no filesystem I/O) ---------
    replica_stats = collect_replica_stats()

    # IMP-B04 diagnostics: replica stdout does not reach this log, so the
    # compat outcome each replica recorded on the stats channel is summarized
    # here. Without it a receipt-routing fault is only visible as the readiness
    # gate blaming the replica role.
    _compat_pub = sum(1 for r in replica_stats if r.get("compat_published"))
    _compat_err = {r.get("compat_error") for r in replica_stats if r.get("compat_error")}
    if replica_stats:
        _transports = sorted(
            {r.get("compat_transport") for r in replica_stats if r.get("compat_transport")}
        )
        print(
            f"[Compat] replicas: {_compat_pub}/{len(replica_stats)} published a "
            f"receipt; transport={_transports}",
            flush=True,
        )
        for _err in sorted(_compat_err):
            print(f"[Compat] replica error: {_err}", flush=True)

    for rs in replica_stats:
        tracer.record_replica_init(rs)
    if replica_stats:
        has_sub = sum(1 for rs in replica_stats if "engine_sub_phases" in rs)
        print(
            f"[ExaServe] Collected {len(replica_stats)} replica init stats "
            f"(engine_create avg={sum(r.get('engine_create_s', 0) for r in replica_stats) / len(replica_stats):.1f}s, "
            f"sub-phases: {has_sub}/{len(replica_stats)})",
            flush=True,
        )

    # ---- All stages complete ------------------------------------------------
    total_time = time.time() - overall_start
    tracer.set_metadata(total_time_s=round(total_time, 4))
    trace_path = tracer.save()

    # ---- Deployment evidence (IMP-B02) --------------------------------------
    # Reaching this line used to BE readiness: it printed the marker. Then an
    # in-child predicate decided, against this node's internal Serve port, from
    # a process that can see neither the gateway nor the other ranks' sessions.
    # WP13 removed both. This process now publishes what it is the best witness
    # for -- whether its own applications reached target -- and the composition
    # root decides.
    # IMP-B02: this process is a WITNESS, not the authority. It reports
    # whether its own applications reached target; the composition root
    # verifies the advertised endpoint, the gateway, the exact receipt set
    # and the per-model canary, and commits the one READY transition.
    from .control import serve_readiness as _readiness
    from .control.deployment_ipc import deliver_snapshot as _deliver_snapshot

    _generation = int(os.environ.get("EXASERVE_GENERATION", "0") or 0)
    _plan_hash = os.environ.get("EXASERVE_PLAN_HASH", "")
    _site_hash = os.environ.get("EXASERVE_SITE_PROFILE_HASH", "")
    _binding_hash = os.environ.get("EXASERVE_ALLOCATION_BINDING_HASH", "")
    _serve_events = _readiness.ServeEventObserver(
        deployment_id=_deployment_scope(),
        generation=_generation,
        deployment_plan_hash=_plan_hash,
        site_profile_hash=_site_hash,
        allocation_binding_hash=_binding_hash,
    ).start()

    _evidence = _readiness.observe_deployment(
        deployment_id=_deployment_scope(),
        generation=_generation,
        deployment_plan_hash=_plan_hash,
        site_profile_hash=_site_hash,
        allocation_binding_hash=_binding_hash,
        publish=_deliver_snapshot,
        observer=_serve_events,
        timeout_s=float(canonical_plan.readiness.initial_deadline_s),
        poll_s=float(canonical_plan.readiness.validation_interval_s),
    )
    tracer.set_metadata(
        deployment_evidence=_evidence,
        deployment=_deploy_manager.to_dict(),
        capabilities=_capability_report,
    )
    print(
        f"[Deployment] evidence published; the composition root owns the "
        f"READY decision (applications_at_target="
        f"{_readiness.applications_at_target(_evidence)})",
        flush=True,
    )
    # WP13: the stdout marker is gone. It was unfalsifiable -- it could not be
    # revoked when a replica died 200ms later -- and every consumer that used
    # to grep for it now reads the shared DeploymentStatus record instead.
    print(
        f"[ExaServe] bring-up complete in {total_time:.2f}s; readiness is the "
        "composition root's decision",
        flush=True,
    )
    print(f"[ExaServe] Scaling trace: {trace_path}", flush=True)

    # PR-028: the driver (and PBS) deliver SIGTERM; without a handler the
    # process dies mid-request with no Serve shutdown. Convert both signals
    # into one orderly drain request. The outer RuntimeSupervisor owns the one
    # absolute TERM/KILL deadline; this child must never race it with os._exit.
    import signal as _signal

    _shutdown_requested = threading.Event()

    def _request_shutdown(signum, frame):  # noqa: ARG001
        _shutdown_requested.set()

    _signal.signal(_signal.SIGTERM, _request_shutdown)
    _signal.signal(_signal.SIGINT, _request_shutdown)

    _lifecycle_failures: list[tuple[str, Exception]] = []
    try:
        while not _shutdown_requested.wait(
            timeout=float(canonical_plan.readiness.validation_interval_s)
        ):
            _live_evidence = _readiness.sample_deployment(
                deployment_id=_deployment_scope(),
                generation=_generation,
                deployment_plan_hash=_plan_hash,
                site_profile_hash=_site_hash,
                allocation_binding_hash=_binding_hash,
                observer=_serve_events,
            )
            if not _deliver_snapshot(_live_evidence):
                raise RuntimeError("rank-zero deployment observation IPC rejected a live sample")
    except KeyboardInterrupt:
        pass
    except (RuntimeError, OSError, ValueError) as _observe_exc:
        _lifecycle_failures.append(("deployment observation channel", _observe_exc))
        print(
            f"[Deployment] live observation failed: {type(_observe_exc).__name__}: {_observe_exc}",
            flush=True,
        )
    print("[ExaServe] Shutdown requested; draining Ray Serve...", flush=True)

    _cleanup_budget = float(canonical_plan.control.watchdog_cleanup_deadline_s)
    _cleanup_deadline = time.monotonic() + _cleanup_budget

    def _cleanup_remaining() -> float:
        return max(0.0, _cleanup_deadline - time.monotonic())

    def _bounded_daemon_call(function, *, timeout_s: float, label: str):
        """Bound a blocking library teardown without creating an immortal owner."""
        outcome: list[BaseException] = []
        result: list[Any] = []

        def invoke() -> None:
            try:
                result.append(function())
            except BaseException as exc:
                outcome.append(exc)

        worker = threading.Thread(
            target=invoke,
            name=f"exaserve-cleanup-{label}",
            daemon=True,
        )
        worker.start()
        worker.join(timeout=max(0.0, timeout_s))
        if worker.is_alive():
            raise TimeoutError(f"{label} did not finish by the cleanup deadline")
        if outcome:
            raise outcome[0]
        return result[0] if result else None

    def _drain_serve(deadline_s: float) -> None:
        available = min(max(0.0, deadline_s), _cleanup_remaining())
        if available <= 0:
            raise TimeoutError("deployment cleanup deadline was exhausted before Serve drain")
        # Reserve a tail for the independent replica-lifecycle proof and local
        # observer cleanup. A stuck Ray library call remains in a daemon thread;
        # the owning deployment process can then exit and be reaped by its
        # outer process-group supervisor.
        _bounded_daemon_call(
            serve.shutdown,
            timeout_s=available * 0.75,
            label="ray-serve-shutdown",
        )
        proof_budget = min(10.0, _cleanup_remaining())
        if proof_budget <= 0:
            raise TimeoutError("no cleanup budget remained for replica lifecycle proof")
        require_clean_replica_shutdown(timeout_s=proof_budget)

    _deploy_manager._drain_fn = _drain_serve
    try:
        _deploy_manager.drain(deadline_s=_cleanup_remaining())
        print("[ExaServe] Ray Serve shut down cleanly.", flush=True)
    except Exception as exc:  # process boundary: preserve typed manager cause
        _lifecycle_failures.append(("deployment drain", exc))
        print(f"[ExaServe] Serve shutdown error: {exc}", flush=True)
    finally:
        try:
            _observer_stopped = _serve_events.stop(timeout_s=min(10.0, _cleanup_remaining()))
            if not _observer_stopped:
                _lifecycle_failures.append(
                    (
                        "Serve event observer cleanup",
                        RuntimeError("observer did not stop within cleanup deadline"),
                    )
                )
        except Exception as _observer_stop_exc:
            _lifecycle_failures.append(("Serve event observer cleanup", _observer_stop_exc))
        # KI-A6: the receipt collector that used to need reaping here no
        # longer exists; the node-local ingress dies with its NodeSupervisor.
        try:
            _deploy_manager.stop()
        except Exception as _manager_stop_exc:
            _lifecycle_failures.append(("deployment manager cleanup", _manager_stop_exc))
        try:
            from .telemetry import cleanup_owned_telemetry_actors

            _telemetry_budget = _cleanup_remaining()
            if _telemetry_budget <= 0:
                raise TimeoutError("no cleanup budget remained for telemetry actors")
            _telemetry_cleanup_errors = _bounded_daemon_call(
                cleanup_owned_telemetry_actors,
                timeout_s=_telemetry_budget,
                label="telemetry-actors",
            )
            if _telemetry_cleanup_errors:
                _lifecycle_failures.append(
                    (
                        "telemetry actor cleanup",
                        RuntimeError(str(_telemetry_cleanup_errors)),
                    )
                )
        except Exception as _telemetry_stop_exc:
            _lifecycle_failures.append(("telemetry actor cleanup", _telemetry_stop_exc))
        print(
            f"[Deployment] terminal state={_deploy_manager.state} "
            f"first_cause={_deploy_manager.first_cause}",
            flush=True,
        )
    if _lifecycle_failures:
        _first_operation, _first_failure = _lifecycle_failures[0]
        _summary = RuntimeError(f"{_first_operation} failed: {_first_failure}")
        for _secondary_operation, _secondary_failure in _lifecycle_failures[1:]:
            add_exception_note(
                _summary,
                f"secondary failure during {_secondary_operation}: "
                f"{type(_secondary_failure).__name__}: {_secondary_failure}",
            )
        raise _summary from _first_failure


if __name__ == "__main__":
    main()
