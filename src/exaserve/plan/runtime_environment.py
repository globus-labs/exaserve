"""Deterministic child-environment projection from a canonical plan.

Some native dependencies consume settings only through environment variables.
This module is the single projection boundary: callers overwrite ambient values
from hash-bearing plan semantics before any child or Ray actor is created.
"""

from __future__ import annotations

import os


def staged_pythonpath(plan, inherited: str = "") -> str:
    """Return the only supported post-staging Python search path."""
    from ..compat.generated_overlay import overlay_root_for

    overlay = overlay_root_for(plan.compatibility_profile_hash)
    excluded = {overlay, "/tmp/exaserve_src", "/tmp/exaserve_overlay"}
    existing = [entry for entry in inherited.split(os.pathsep) if entry and entry not in excluded]
    return os.pathsep.join([overlay, "/tmp/exaserve_src", *existing])


def runtime_environment(plan) -> dict[str, str]:
    policy = plan.runtime
    centralized_serve_ingress = plan.uses_head_only_serve_proxy()
    pp_enabled = any(model.pipeline_parallel_size > 1 for model in plan.models)
    multiproc_enabled = any(
        model.pipeline_parallel_size == 1 and model.tensor_parallel_size > 1
        for model in plan.models
    )
    diagnostics = bool(policy.instrumentation or plan.collect_stats)
    from ..compat.generated_overlay import ROOT_ENV, overlay_root_for
    from ..compat.profile import (
        MULTIPROC_WORKER_PATCH_GATE,
        PP_PATCH_GATE,
        RAY_WORKER_PATCH_GATE,
    )

    return {
        "EXASERVE_NULL_COMPUTE": "1" if policy.null_compute else "0",
        "EXASERVE_NULL_COMPUTE_LATENCY": str(policy.null_compute_latency_s),
        "EXASERVE_PP_SHARD_AWARE": "1" if policy.pp_shard_aware else "0",
        "EXASERVE_CLEAN_STAGE": "1" if policy.clean_stage else "0",
        "EXASERVE_SCALING_TRACE": "1" if diagnostics else "0",
        "RAY_event_stats": "1" if diagnostics else "0",
        "RAY_event_stats_print_interval_ms": "1000",
        "RAYON_NUM_THREADS": "1",
        "TOKENIZERS_PARALLELISM": "false",
        "RAY_SERVE_PROXY_HEALTH_CHECK_TIMEOUT_S": str(
            plan.readiness.serve_proxy_health_check_timeout_s
        ),
        "RAY_SERVE_PROXY_READY_CHECK_TIMEOUT_S": str(
            plan.readiness.serve_proxy_ready_check_timeout_s
        ),
        # Native HeadOnly is an explicit centralized-ingress benchmark. Ray
        # Serve otherwise prefers replicas on the proxy's node (and AZ), which
        # overloads the head node and leaves remote replicas mostly idle.
        "RAY_SERVE_PROXY_PREFER_LOCAL_NODE_ROUTING": ("0" if centralized_serve_ingress else "1"),
        "RAY_SERVE_PROXY_PREFER_LOCAL_AZ_ROUTING": ("0" if centralized_serve_ingress else "1"),
        "EXASERVE_RAY_SERVE_START_PROXY_TIMEOUT_S": str(plan.readiness.serve_start_proxy_timeout_s),
        "EXASERVE_ENGINE": plan.engine,
        "EXASERVE_VENDOR": plan.vendor,
        "EXASERVE_COMPAT_PROFILE_ID": plan.compatibility_profile_hash,
        "EXASERVE_COMPAT_MANIFEST_HASH": plan.manifest_hash,
        ROOT_ENV: overlay_root_for(plan.compatibility_profile_hash),
        "EXASERVE_COMPAT_DELIVERY": "generated-overlay",
        PP_PATCH_GATE: "1" if pp_enabled else "0",
        RAY_WORKER_PATCH_GATE: "1" if pp_enabled else "0",
        MULTIPROC_WORKER_PATCH_GATE: "1" if multiproc_enabled else "0",
        "EXASERVE_XPU_VLLM_DISABLE_RAY_COMPILED_DAG": (
            "1" if pp_enabled and plan.vendor == "xpu" else "0"
        ),
        "EXASERVE_XPU_VLLM_FORCE_RAY_CHANNEL_TYPE": (
            "auto" if pp_enabled and plan.vendor == "xpu" else ""
        ),
    }
