"""Aurora-specific patches for Ray Serve and vLLM.

Pinned upstream:
    ray==2.49.1 (commit c057f1e), Aurora frameworks 2025.2.0
    vllm: as bundled in Aurora frameworks 2025.2.0

Two patch sets, two delivery mechanisms:

A) ray.serve._private — overlay (PYTHONPATH shadowing)
   Located: src/patches/ray_serve_overlay/ray/serve/_private/
   Activation: launcher prepends src/patches/ray_serve_overlay to PYTHONPATH
       when AURORA_INSTRUMENTATION=1. distribute_to_nodes.sh stages a
       symlink farm at /tmp/aurora_overlay so individual files override
       the system Ray serve._private modules.
   Lifecycle: must be in place before `import ray.serve`. Monkey-patching
       at runtime is too late because Ray instantiates classes during import.

B) vLLM and ray accelerator — runtime monkey-patches
   Located: src/sitecustomize.py (1617 lines, 16 _patch_*/_install_* functions)
   Activation: sitecustomize.py is auto-imported by CPython when src/ is
       on PYTHONPATH; each function is gated by an AURORA_VLLM_*/AURORA_*
       env var. Default-on: AURORA_VLLM_PATCH_PP_LAYER_FILTER=1 (set by
       launch_cluster.sh) — required for vLLM pipeline-parallel deployments.
   Lifecycle: applied after import but before classes are instantiated.

============================================================================
Per-file inventory (overlay tree, vs ray==2.49.1 upstream)
============================================================================

proxy_state.py [10 LoC, 1% diff, 3 hunks]
    ADAPTER:  Timer/TimerBase moved to ray._common.utils (cross-version)
    FUNCTIONAL: check_health() returns the future's result (was: discarded)
    FUNCTIONAL: ProxyMetadata.name field for actor name lookup

constants.py [180 LoC, 39% diff]
    FUNCTIONAL (Aurora scale tuning):
        HTTP_PROXY_TIMEOUT 60→3600
        DEFAULT_HEALTH_CHECK_PERIOD_S 10→120
        DEFAULT_HEALTH_CHECK_TIMEOUT_S 30→600
        PROXY_HEALTH_CHECK_TIMEOUT_S 10→300
        PROXY_READY_CHECK_TIMEOUT_S 5→60
        PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD 3→100
        REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD 3→100
        RAY_SERVE_PROXY_GC_THRESHOLD 10000→700
    ADAPTER:
        get_env_float_non_zero_with_warning → get_env_float_positive
        added get_env_int_non_negative imports
    BACKPORT (newer Ray feature constants):
        BATCH_EXECUTION_TIME_BUCKETS_MS, BATCH_WAIT_TIME_BUCKETS_MS
        BATCH_UTILIZATION_BUCKETS_PERCENT, BATCH_SIZE_BUCKETS
        DEFAULT_CONSUMER_CONCURRENCY, DEFAULT_CONSTRUCTOR_RETRY_COUNT
        RAY_SERVE_USE_PACK_SCHEDULING_STRATEGY
        RAY_SERVE_THROUGHPUT_OPTIMIZED + dependent flags
        RAY_SERVE_AGGREGATE_METRICS_AT_CONTROLLER
        RAY_SERVE_FAIL_ON_RANK_ERROR
        RAY_SERVE_RECORD_AUTOSCALING_STATS_TIMEOUT_S
        RAY_SERVE_RPC_LATENCY_WARNING_THRESHOLD_MS
        SERVE_AUTOSCALING_DECISION_COUNTERS_KEY
        HEALTHY_MESSAGE
        RAY_SERVE_MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT (renamed env)
        RAY_SERVE_MAX_PER_REPLICA_RETRY_COUNT (renamed env)
        RAY_SERVE_MAX_CACHED_HANDLES (renamed env)
        RAY_SERVE_CONTROLLER_MAX_CONCURRENCY (renamed env)
    COSMETIC: typo fixes (randomed→randomized, construtor→constructor)

proxy.py [263 LoC, 21% diff, 13 hunks]
    ADAPTER:    CoreContextFilter import path (ray._common.filters)
    BACKPORT:   logs_and_metrics_route via match_route_pattern (Ray #52212)
    FUNCTIONAL: ProxyActorInterface ABC class (~140 LoC) — enables
                pluggable proxy backends (HAProxy plugin)
    FUNCTIONAL: ProxyActor inherits ProxyActorInterface
    FUNCTIONAL: __init__ rewritten to call super().__init__() first
    INSTRUMENTATION: __init__ substep timings + /tmp/aurora_inst/proxy_init_*.json
    INSTRUMENTATION: ready() timings + /tmp/aurora_inst dump
    FUNCTIONAL: serving() abstract method satisfied (no-op)
    FUNCTIONAL: check_health() typed -> bool, returns True

controller.py [258 LoC, 21% diff, 21 hunks]
    BACKPORT:   HandleMetricReport / ReplicaMetricReport plumbing
    BACKPORT:   ApplicationArgs / APIType / ReplicaRank / ExternalScalerDisabledError
    BACKPORT:   autoscaling_state_manager passed to ApplicationStateManager
    INSTRUMENTATION: RPC latency warning threshold tracking

deployment_state.py [1045 LoC, 35% diff, 22 hunks]
    FUNCTIONAL: actor_handle → actor_name refactor (avoids carrying handle
                objects through controller serialization at scale)
    BACKPORT:   HandleMetricReport / ReplicaMetricReport plumbing
    BACKPORT:   metric/handle reporting hooks

router.py [291 LoC, 28% diff, 14 hunks]
    BACKPORT:   HandleMetricReport plumbing
    FUNCTIONAL: actor_name resolution paths
    BACKPORT:   route-pattern metric tagging

common.py [195 LoC, 26% diff, 7 hunks]
    INSTRUMENTATION: _aurora_probe_* helpers (50 LoC) — buffered Lustre
                     writes for get_actor_handle profiling
    BACKPORT:   EndpointInfo.route_patterns + RoutePattern import
    FUNCTIONAL: actor_handle → actor_name in RunningReplicaInfo
    MIXED:      get_actor_handle() method — FUNCTIONAL lazy lookup
                + INSTRUMENTATION /tmp probe (separable)
    BACKPORT:   HandleMetricReport / ReplicaMetricReport / TimeStampedValue
                / TimeSeries dataclasses (90 LoC)
    BACKPORT:   RUNNING_REQUESTS_KEY, ONGOING_REQUESTS_KEY, QUEUED_REQUESTS_KEY

client.py [146 LoC, 30% diff, 9 hunks]
    BACKPORT:   APIType, ApplicationArgs, ReplicaRank
    BACKPORT:   HandleMetricReport plumbing

============================================================================
Per-function inventory (sitecustomize.py — vLLM + ray accelerator patches)
============================================================================

All functions are env-gated; the env-gate is checked inside the function
so calls at module-load are cheap when disabled.

_patch_log()                                  helper, log under AURORA_VLLM_PATCH_VERBOSE
_patch_vllm_layer_lookup()                    FUNCTIONAL  AURORA_VLLM_PATCH_PP_LAYER_FILTER  (auto-applied)
_patch_vllm_bind_kv_cache()                   FUNCTIONAL  AURORA_VLLM_PATCH_PP_LAYER_FILTER  (auto-applied)
_patch_vllm_forward_context_aliases()         FUNCTIONAL  AURORA_VLLM_PATCH_PP_LAYER_FILTER  (auto-applied)
_patch_vllm_attention_context()               FUNCTIONAL  AURORA_VLLM_PATCH_PP_LAYER_FILTER  (auto-applied)
_patch_vllm_gpu_model_runner_attn_backend()   FUNCTIONAL  AURORA_VLLM_PATCH_PP_LAYER_FILTER  (auto-applied)
_install_vllm_gpu_model_runner_import_hook()  FUNCTIONAL  AURORA_VLLM_PATCH_PP_LAYER_FILTER  (auto-applied)
_patch_vllm_ray_multigpu_bundles()            FUNCTIONAL  AURORA_VLLM_PATCH_PP_LAYER_FILTER  (auto-applied)
_patch_vllm_ray_executor_bundle_indices()     FUNCTIONAL  AURORA_VLLM_PATCH_PP_LAYER_FILTER  (auto-applied)
_patch_vllm_ray_executor_channel_type()       FUNCTIONAL  AURORA_VLLM_PATCH_PP_LAYER_FILTER  (auto-applied)
_patch_vllm_ray_executor_uncompiled_pp()      FUNCTIONAL  AURORA_VLLM_PATCH_PP_LAYER_FILTER  (auto-applied)
_patch_ray_oneapi_selector()                  FUNCTIONAL  (auto-applied; XPU device selection)
_patch_ray_accelerator_context()              FUNCTIONAL  (auto-applied; XPU context)
_patch_ray_serve_proxy_future_timeout()       FUNCTIONAL  (defined; not auto-applied)
_patch_ray_serve_proxy_startup_timeout()      FUNCTIONAL  (defined; not auto-applied)
_install_proxy_actor_profiling_hook()         INSTRUMENTATION  (defined; not auto-applied;
                                                   superseded by overlay proxy.py probes)

============================================================================
Summary by category
============================================================================
- FUNCTIONAL  (must ship for correctness/scale at >1 node, PP, etc.)
- BACKPORT    (newer Ray features pulled in early; will phase out as Aurora's
              Ray catches up)
- INSTRUMENTATION  (probes, timing, telemetry — opt-in via env vars)
- ADAPTER     (cross-version glue for symbol/import drift)
- COSMETIC    (typo fixes only)
"""

import os

__all__ = ["apply_all", "PATCH_REGISTRY", "RAY_PINNED_VERSION", "RAY_PINNED_COMMIT"]

RAY_PINNED_VERSION = "2.49.1"
RAY_PINNED_COMMIT = "c057f1ea836f3e93f110e895029caa32136fc156"


def _check_ray_version(strict: bool = False) -> None:
    """Warn (or raise, if strict) when Ray drifts from the pinned version.

    The overlay was written against ray==2.49.1 (commit c057f1e).
    Internal API changes between Ray versions can silently break the patches.
    """
    try:
        from ray import _version as _ray_version_mod
    except Exception:
        return
    actual_version = getattr(_ray_version_mod, "version", None)
    actual_commit = getattr(_ray_version_mod, "commit", None)
    if actual_version != RAY_PINNED_VERSION:
        msg = (
            f"[aurora.patches] Ray version mismatch: pinned={RAY_PINNED_VERSION} "
            f"(commit {RAY_PINNED_COMMIT[:7]}), runtime={actual_version} "
            f"(commit {(actual_commit or '?')[:7]}). Patches may break."
        )
        if strict:
            raise RuntimeError(msg)
        if os.environ.get("AURORA_PATCH_VERBOSE", "0") == "1":
            print(msg, flush=True)


def apply_all(strict: bool = False) -> None:
    """Single entry point for all Aurora patches.

    Today this is a soft validator — the actual patch application happens
    in two places that pre-date this consolidation:
        1. ray_serve_overlay/* via PYTHONPATH precedence (set up by
           scripts/launch_cluster.sh + scripts/distribute_to_nodes.sh).
        2. sitecustomize.py at the top of src/, auto-imported by CPython.

    Future work: consolidate sitecustomize.py monkey-patches into per-concern
    submodules and drive them from this function.
    """
    _check_ray_version(strict=strict)


PATCH_REGISTRY = {
    "ray_serve_overlay": {
        "type": "overlay",
        "activation": "PYTHONPATH precedence (launch_cluster.sh)",
        "env_gate": "AURORA_INSTRUMENTATION",
        "default": False,
    },
    "vllm_pp_layer_filter": {
        "type": "monkey-patch",
        "activation": "sitecustomize.py auto-import",
        "env_gate": "AURORA_VLLM_PATCH_PP_LAYER_FILTER",
        "default": True,
    },
}
