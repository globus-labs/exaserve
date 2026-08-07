"""One-way adapter: eval spec -> the shared compiled plan (packet P05, IMP-H01).

Eval built its own `RunPlan` from an eval-shaped `DeploymentSpec`, and core
compiled a different object from YAML. Nothing could prove the two described
the same deployment, so a serving change could alter one and not the other with
no diagnostic.

This adapter makes the eval path derive the **same** `DeploymentPlan` the core
compiler produces, so both sides agree on `deployment_plan_hash` by
construction. Per §3.2.1 it is explicitly a *one-way* migration adapter: eval
keeps its own workload/trace/client semantics, but the deployment identity is
no longer its own invention.

The legacy eval schemas remain until the WP13 cutover deletes them.
"""

from __future__ import annotations

from typing import Any, Optional

from exaserve.plan.compiler import compile_deployment_plan, compile_run_plan
from exaserve.plan.contracts import (
    DeploymentPlan,
    SchedulerPlan,
    SiteProfile,
    WorkloadPolicy,
)
from exaserve.site import default_site_profile


def deployment_raw_from_spec(deployment, *, gateway: Optional[dict] = None,
                             validation_mode: bool = False,
                             exposure: Optional[dict] = None) -> dict:
    """Render an eval `DeploymentSpec` in the compiler's input shape.

    Every field the compiler accepts is passed explicitly; anything eval knows
    that the plan does not model is left out here rather than smuggled in, so
    the compiler's unknown-key rejection stays meaningful.
    """
    raw: dict[str, Any] = {
        "num_nodes": int(deployment.num_nodes),
        "models": [
            {
                "model_id": model.model_id,
                "tensor_parallel_size": int(model.tensor_parallel_size),
                "pipeline_parallel_size": int(getattr(model, "pipeline_parallel_size", 1)),
                "max_model_len": int(model.max_model_len),
                "size": int(model.size),
                "num_replicas": getattr(model, "num_replicas", None),
                "gpu_memory_utilization": float(
                    getattr(model, "gpu_memory_utilization", 0.90)),
                "enforce_eager": bool(getattr(model, "enforce_eager", True)),
            }
            for model in deployment.models
        ],
    }
    if getattr(deployment, "num_gpus_per_node", 0):
        raw["num_gpus_per_node"] = int(deployment.num_gpus_per_node)
    if getattr(deployment, "model_storage_path", ""):
        raw["model_storage_path"] = deployment.model_storage_path
    if getattr(deployment, "local_stage_path", ""):
        raw["local_stage_path"] = deployment.local_stage_path
    if getattr(deployment, "engine", ""):
        raw["engine"] = deployment.engine
    if gateway is not None:
        raw["gateway"] = gateway
    if validation_mode:
        raw["validation_mode"] = True
    if exposure is not None:
        raw["exposure"] = exposure
    return raw


def _gateway_from_backend(backend_args: dict) -> tuple[Optional[dict], bool, Optional[dict]]:
    """Translate eval's proxy settings into gateway/exposure.

    `proxy.type: none` was eval's way of saying "hit Serve directly". That is a
    validation exposure, not a production gateway, and it now has to say so.
    """
    proxy = ((backend_args or {}).get("ray") or {}).get("proxy") or {}
    kind = str(proxy.get("type", "none")).lower()
    if kind in ("none", "direct", ""):
        return None, True, {"mode": "DIRECT_VALIDATION",
                            "serve_port": int(proxy.get("backend_port", 8000))}
    gateway = {"kind": kind, "port": int(proxy.get("port", 4001))}
    return gateway, False, {"mode": "PROXIED_INTERNAL"}


def compile_shared_deployment_plan(spec, *, deployment_id: str,
                                   site: Optional[SiteProfile] = None) -> DeploymentPlan:
    """The DeploymentPlan for an eval spec — identical to core's for one input."""
    gateway, validation_mode, exposure = _gateway_from_backend(
        getattr(spec.backend, "args", {}) or {})
    raw = deployment_raw_from_spec(spec.deployment, gateway=gateway,
                                   validation_mode=validation_mode,
                                   exposure=exposure)
    return compile_deployment_plan(raw, site=site or default_site_profile(),
                                   deployment_id=deployment_id)


def compile_shared_run_plan(spec, *, run_id: str, deployment_id: str,
                            site: Optional[SiteProfile] = None):
    """The shared RunPlan: that exact DeploymentPlan plus run semantics."""
    gateway, validation_mode, exposure = _gateway_from_backend(
        getattr(spec.backend, "args", {}) or {})
    raw = deployment_raw_from_spec(spec.deployment, gateway=gateway,
                                   validation_mode=validation_mode,
                                   exposure=exposure)
    workload = WorkloadPolicy(
        kind=getattr(spec.trace, "kind", "synthetic"),
        duration_s=float(getattr(spec.workload, "duration", 0.0)),
        input_len=int(getattr(spec.workload, "input_len", 0)),
        output_len=int(getattr(spec.workload, "output_len", 0)),
        rate_per_node=float(getattr(spec.workload, "rate_per_node", 0.0)),
        seed=int(getattr(spec.workload, "seed", 0)),
        arrival=str(getattr(spec.workload, "arrival", "fixed")),
        client_nodes=int(getattr(spec.client, "num_nodes", 1)),
        client_dest=str(getattr(spec.client, "dest", "proxy")),
    )
    scheduler = SchedulerPlan(
        type=str(getattr(spec.scheduler, "type", "pbs")),
        nodes=int(getattr(spec.scheduler, "nodes", spec.deployment.num_nodes)),
        queue=str(getattr(spec.scheduler, "queue", "")),
        account=str(getattr(spec.scheduler, "project", "")),
        walltime=str(getattr(spec.scheduler, "walltime", "")))
    return compile_run_plan(raw, site=site or default_site_profile(),
                            run_id=run_id, deployment_id=deployment_id,
                            scheduler=scheduler, workload=workload)
