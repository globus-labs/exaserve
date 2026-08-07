"""The one-way compiler: raw configuration -> canonical plan (packet P01).

Exactly one place turns YAML into a plan. Downstream never recompiles,
reloads, or reinterprets: `RuntimeSupervisor` receives typed objects and the
plan hash is the identity everything else binds to.

Two rules from §3.2.1 are enforced here rather than left to callers:

1. **No production direct exposure.** `gateway: none` used to be both legal and
   the implicit default, so a deployment with no front door was
   indistinguishable from a configured one. Now a plan either names a real
   managed gateway with `PROXIED_INTERNAL`, or explicitly declares
   `validation_mode: true` with `gateway: null` and `DIRECT_VALIDATION`. Every
   other combination fails to compile.
2. **Every accepted field is represented or rejected.** An unknown key is an
   error, never something that silently disappears between config and plan.
"""

from __future__ import annotations

import re
from typing import Any, Mapping, Optional

from .contracts import (
    SCHEMA_VERSION,
    ControlLimits,
    DeploymentPlan,
    ExposureMode,
    ExposurePlan,
    GatewayKind,
    GatewayPlan,
    ModelPlan,
    PlanError,
    ReceiptRequirement,
    RunPlan,
    SchedulerPlan,
    SiteProfile,
    WorkloadPolicy,
)

_MODEL_KEYS = {
    "model_id", "tensor_parallel_size", "pipeline_parallel_size",
    "max_model_len", "size", "gpu_memory_utilization", "enforce_eager",
    "enable_log_requests", "max_num_seqs", "num_replicas",
    "num_cpus_per_replica",
}
_DEPLOYMENT_KEYS = {
    "num_nodes", "model_configs", "models", "model_storage_path",
    "local_stage_path", "deployment_name", "replica_max_ongoing_requests",
    "num_gpus_per_node", "collect_stats", "engine", "vendor",
    "gateway", "exposure", "validation_mode", "control",
}


def _reject_unknown(raw: Mapping[str, Any], allowed: set, where: str) -> None:
    if not isinstance(raw, Mapping):
        raise PlanError(f"{where} must be a mapping, got {type(raw).__name__}")
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise PlanError(f"{where}: unknown key(s) {unknown}; accepted: {sorted(allowed)}")


def _int(raw: Mapping[str, Any], key: str, where: str,
         default: Optional[int] = None) -> int:
    value = raw.get(key, default)
    if value is None:
        raise PlanError(f"{where}.{key} is required")
    if isinstance(value, bool):
        raise PlanError(f"{where}.{key} must be an integer, got boolean")
    if isinstance(value, float) and not float(value).is_integer():
        raise PlanError(f"{where}.{key} must be an integer, got {value!r}")
    try:
        return int(value)
    except (TypeError, ValueError):
        raise PlanError(f"{where}.{key} must be an integer, got {value!r}") from None


def _bool(raw: Mapping[str, Any], key: str, where: str, default: bool) -> bool:
    value = raw.get(key, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in (
            "true", "false", "1", "0", "yes", "no", "on", "off"):
        return value.strip().lower() in ("true", "1", "yes", "on")
    raise PlanError(f"{where}.{key} must be a boolean, got {value!r}")


def _storage_name(model_id: str) -> str:
    return model_id.replace("/", "--")


def _route_name(storage_name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]", "-", storage_name).strip("-").lower()


def compile_models(raw_models: list, *, num_nodes: int,
                   gpus_per_node: int) -> tuple[ModelPlan, ...]:
    if not raw_models:
        raise PlanError("deployment must declare at least one model")
    models: list[ModelPlan] = []
    seen_route: dict[str, str] = {}
    for index, raw in enumerate(raw_models):
        where = f"models[{index}]"
        _reject_unknown(raw, _MODEL_KEYS, where)
        model_id = raw.get("model_id")
        if not model_id or not isinstance(model_id, str):
            raise PlanError(f"{where}.model_id is required and must be a string")
        storage = _storage_name(model_id)
        route = _route_name(storage)
        if route in seen_route and seen_route[route] != model_id:
            raise PlanError(
                f"model identity collision: {seen_route[route]!r} and {model_id!r} "
                f"both derive route {route!r}")
        seen_route[route] = model_id

        tp = _int(raw, "tensor_parallel_size", where, 1)
        pp = _int(raw, "pipeline_parallel_size", where, 1)
        if tp < 1 or pp < 1:
            raise PlanError(f"{where}: tp/pp must be >= 1")
        if tp > gpus_per_node:
            raise PlanError(
                f"{where}: tensor_parallel_size {tp} exceeds gpus_per_node {gpus_per_node}")
        if pp > num_nodes:
            raise PlanError(
                f"{where}: pipeline_parallel_size {pp} exceeds num_nodes {num_nodes}")
        replicas = raw.get("num_replicas")
        if replicas is not None:
            replicas = _int(raw, "num_replicas", where)
            if replicas < 1:
                raise PlanError(f"{where}.num_replicas must be >= 1")
        models.append(ModelPlan(
            model_id=model_id, storage_name=storage, route_name=route,
            tensor_parallel_size=tp, pipeline_parallel_size=pp,
            max_model_len=_int(raw, "max_model_len", where, 4096),
            size_b=_int(raw, "size", where, 8),
            gpu_memory_utilization=float(raw.get("gpu_memory_utilization", 0.90)),
            enforce_eager=_bool(raw, "enforce_eager", where, True),
            max_num_seqs=_int(raw, "max_num_seqs", where, 256),
            num_replicas=replicas))
    return tuple(models)


def compile_exposure(raw: Mapping[str, Any], *, site: SiteProfile,
                     validation_mode: bool) -> tuple[ExposurePlan, Optional[GatewayPlan]]:
    """Enforce the production/validation exposure split (§3.2.1 Q3)."""
    raw_gateway = raw.get("gateway")
    raw_exposure = raw.get("exposure") or {}
    if not isinstance(raw_exposure, Mapping):
        raise PlanError("exposure must be a mapping")

    # Legacy spelling: gateway: none / type: none meant "direct".
    if isinstance(raw_gateway, str) and raw_gateway.lower() in ("none", "direct", ""):
        raw_gateway = None
    if isinstance(raw_gateway, Mapping) and str(
            raw_gateway.get("type", raw_gateway.get("kind", ""))).lower() in (
            "none", "direct", ""):
        raw_gateway = None

    declared_mode = raw_exposure.get("mode")
    if raw_gateway is None:
        if not validation_mode:
            raise PlanError(
                "no gateway declared. Production exposure requires a managed "
                f"gateway ({sorted(k.value for k in GatewayKind)}) with mode "
                "PROXIED_INTERNAL. Direct exposure is validation-only and "
                "requires validation_mode: true with exposure.mode: "
                "DIRECT_VALIDATION.")
        mode = declared_mode or ExposureMode.DIRECT_VALIDATION.value
        if mode != ExposureMode.DIRECT_VALIDATION.value:
            raise PlanError(
                f"validation_mode with no gateway requires exposure.mode "
                f"DIRECT_VALIDATION, got {mode!r}")
        return ExposurePlan(
            mode=mode,
            serve_port=int(raw_exposure.get("serve_port", 8000))), None

    kind = str(raw_gateway.get("kind", raw_gateway.get("type", ""))).lower()
    gateway = GatewayPlan(
        kind=kind,
        port=int(raw_gateway.get("port", 4001)),
        options=tuple(sorted(
            (str(k), v) for k, v in (raw_gateway.get("options") or {}).items())))
    if not site.supports_gateway(gateway.kind):
        raise PlanError(
            f"site {site.site_id} does not support gateway {gateway.kind!r} "
            f"(supported: {sorted(site.gateway_kinds)})")
    if not validation_mode and GatewayKind(gateway.kind) not in {
            GatewayKind.HAPROXY}:
        raise PlanError(
            f"gateway {gateway.kind!r} is not a first-release production "
            "gateway; run it under validation_mode until it carries its own "
            "WP7/WP12 evidence")
    mode = declared_mode or ExposureMode.PROXIED_INTERNAL.value
    if mode != ExposureMode.PROXIED_INTERNAL.value:
        raise PlanError(
            f"a declared gateway requires exposure.mode PROXIED_INTERNAL, got {mode!r}")
    return ExposurePlan(mode=mode,
                        serve_port=int(raw_exposure.get("serve_port", 8000))), gateway


def build_receipt_requirements(*, num_nodes: int, models: tuple[ModelPlan, ...],
                               gateway: Optional[GatewayPlan]) -> tuple[ReceiptRequirement, ...]:
    """Enumerate every exactly-planned instance that must produce a receipt.

    Slots, not instances: a restart refills a slot with a new instance_id.
    """
    requirements: list[ReceiptRequirement] = [
        ReceiptRequirement(
            receipt_requirement_id="global/supervisor",
            role="supervisor", component_slot="supervisor", owner_scope="GLOBAL"),
    ]
    if gateway is not None:
        requirements.append(ReceiptRequirement(
            receipt_requirement_id=f"global/gateway/{gateway.kind}",
            role="gateway", component_slot=f"gateway/{gateway.kind}",
            owner_scope="GLOBAL"))
    for rank in range(num_nodes):
        role = "ray_head" if rank == 0 else "ray_worker"
        requirements.append(ReceiptRequirement(
            receipt_requirement_id=f"rank{rank}/{role}",
            role=role, component_slot="ray", owner_scope="RANK",
            planned_rank=rank))
        requirements.append(ReceiptRequirement(
            receipt_requirement_id=f"rank{rank}/node_supervisor",
            role="node_supervisor", component_slot="node_supervisor",
            owner_scope="RANK", planned_rank=rank))
    return tuple(requirements)


def compile_deployment_plan(raw: Mapping[str, Any], *, site: SiteProfile,
                            deployment_id: str,
                            compatibility_profile_hash: str = "",
                            manifest_hash: str = "") -> DeploymentPlan:
    """Compile one raw serving configuration into the canonical plan."""
    # A real config file carries sibling sections (ray_cluster_config,
    # proxy_config, ...). The deployment subtree is what compiles; the legacy
    # proxy_config is translated into gateway/exposure below rather than being
    # silently dropped.
    outer = raw
    if "model_deployment_config" in raw:
        raw = dict(raw["model_deployment_config"])
        legacy_proxy = outer.get("proxy_config")
        if legacy_proxy is not None and "gateway" not in raw:
            raw["gateway"] = legacy_proxy
        if "validation_mode" in outer and "validation_mode" not in raw:
            raw["validation_mode"] = outer["validation_mode"]
        if "exposure" in outer and "exposure" not in raw:
            raw["exposure"] = outer["exposure"]
    _reject_unknown(raw, _DEPLOYMENT_KEYS, "deployment")

    validation_mode = _bool(raw, "validation_mode", "deployment", False)
    num_nodes = _int(raw, "num_nodes", "deployment", 1)
    if num_nodes > site.max_nodes:
        raise PlanError(
            f"num_nodes {num_nodes} exceeds site {site.site_id} maximum {site.max_nodes}")
    gpus = _int(raw, "num_gpus_per_node", "deployment", site.gpus_per_node)

    raw_models = raw.get("models") or raw.get("model_configs") or []
    models = compile_models(list(raw_models), num_nodes=num_nodes, gpus_per_node=gpus)
    exposure, gateway = compile_exposure(raw, site=site, validation_mode=validation_mode)

    vendor = str(raw.get("vendor", site.vendors[0] if site.vendors else "xpu"))
    if site.vendors and vendor not in site.vendors:
        raise PlanError(f"vendor {vendor!r} not supported by site (has {list(site.vendors)})")
    engine = str(raw.get("engine", site.engines[0] if site.engines else "vllm"))
    if site.engines and engine not in site.engines:
        raise PlanError(f"engine {engine!r} not supported by site (has {list(site.engines)})")

    raw_control = raw.get("control") or {}
    if not isinstance(raw_control, Mapping):
        raise PlanError("control must be a mapping")
    control = ControlLimits(**{**{k: v for k, v in vars(site.control).items()
                                  if not k.startswith("_")},
                               **{str(k): v for k, v in raw_control.items()}})

    plan = DeploymentPlan(
        schema_version=SCHEMA_VERSION,
        deployment_id=deployment_id,
        site_profile_id=site.site_id,
        site_profile_hash=site.site_profile_hash or site.compute_hash(),
        compatibility_profile_hash=compatibility_profile_hash,
        manifest_hash=manifest_hash,
        num_nodes=num_nodes,
        num_gpus_per_node=gpus,
        vendor=vendor,
        engine=engine,
        model_storage_path=str(raw.get("model_storage_path", site.model_storage_path)),
        local_stage_path=str(raw.get("local_stage_path", site.local_stage_path)),
        models=models,
        exposure=exposure,
        gateway=gateway,
        receipt_requirements=build_receipt_requirements(
            num_nodes=num_nodes, models=models, gateway=gateway),
        control=control,
        validation_mode=validation_mode,
    )
    return plan.finalize()


def compile_run_plan(raw: Mapping[str, Any], *, site: SiteProfile, run_id: str,
                     deployment_id: str,
                     scheduler: Optional[SchedulerPlan] = None,
                     workload: Optional[WorkloadPolicy] = None) -> RunPlan:
    """Eval/ClientLab entry: the exact DeploymentPlan plus run semantics."""
    deployment = compile_deployment_plan(raw, site=site, deployment_id=deployment_id)
    raw_sched = raw.get("scheduler") or {}
    scheduler = scheduler or SchedulerPlan(
        type=str(raw_sched.get("type", site.scheduler_types[0]
                               if site.scheduler_types else "pbs")),
        nodes=int(raw_sched.get("nodes", deployment.num_nodes)),
        queue=str(raw_sched.get("queue", "")),
        account=str(raw_sched.get("account", "")),
        walltime=str(raw_sched.get("walltime", "")))
    if scheduler.type not in (site.scheduler_types or (scheduler.type,)):
        raise PlanError(
            f"scheduler.type {scheduler.type!r} not supported by site "
            f"{site.site_id} (has {list(site.scheduler_types)})")
    return RunPlan(
        schema_version=SCHEMA_VERSION, run_id=run_id, deployment=deployment,
        scheduler=scheduler, workload=workload or WorkloadPolicy()).finalize()
