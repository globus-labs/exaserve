"""Versioned plan schemas and the strict plan compiler (plan WP1).

Rules enforced here (with their audit findings):
- Strict coercion: ``"false"`` is False, not True (PR-006); invalid numbers
  raise with the full field path instead of degrading to auto-planning.
- Unknown keys are rejected with their path (PR-006).
- Model identity is validated across ALL derived identifiers — raw id,
  storage name, route name (PR-007).
- ``scheduler.nodes`` inherits ``deployment.num_nodes`` when omitted; an
  explicit mismatch is rejected unless a named reservation topology is
  declared (audit PR-020 / AC-PLAN-01).
- The ScaleEnvelope separates the user-approved ``qualification_target``
  from the evidence-derived ``supported_max``; only a validation-mode plan
  may exceed the latter (ADR-000 / AC-SCALE-01).
- Plans are frozen dataclasses; ``plan_hash`` is SHA-256 over the canonical
  JSON of the fully resolved plan.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any, Mapping

from exaserve.model_paths import get_model_storage_name

SCHEMA_VERSION = 1

_TRUE = {"true", "1", "yes", "on"}
_FALSE = {"false", "0", "no", "off"}


class PlanError(ValueError):
    """Configuration rejected; message carries the exact field path."""


# --------------------------- strict coercion --------------------------------

def strict_bool(value: Any, path: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in _TRUE:
            return True
        if lowered in _FALSE:
            return False
    raise PlanError(f"{path}: expected a boolean, got {value!r}")


def strict_int(value: Any, path: str, *, minimum: int | None = None,
               maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        if isinstance(value, str) and value.strip().lstrip("+-").isdigit():
            value = int(value.strip())
        else:
            raise PlanError(f"{path}: expected an integer, got {value!r}")
    if minimum is not None and value < minimum:
        raise PlanError(f"{path}: {value} < minimum {minimum}")
    if maximum is not None and value > maximum:
        raise PlanError(f"{path}: {value} > maximum {maximum}")
    return value


def strict_float(value: Any, path: str, *, minimum: float | None = None,
                 maximum: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        try:
            value = float(str(value).strip())
        except (TypeError, ValueError):
            raise PlanError(f"{path}: expected a number, got {value!r}") from None
    value = float(value)
    if minimum is not None and value < minimum:
        raise PlanError(f"{path}: {value} < minimum {minimum}")
    if maximum is not None and value > maximum:
        raise PlanError(f"{path}: {value} > maximum {maximum}")
    return value


def strict_str(value: Any, path: str, *, choices: set[str] | None = None,
               allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise PlanError(f"{path}: expected a string, got {value!r}")
    if not allow_empty and not value.strip():
        raise PlanError(f"{path}: must not be empty")
    if choices is not None and value not in choices:
        raise PlanError(f"{path}: {value!r} not in {sorted(choices)}")
    return value


def reject_unknown_keys(raw: Mapping[str, Any], known: set[str], path: str) -> None:
    unknown = set(raw) - known
    if unknown:
        raise PlanError(f"{path}: unknown key(s) {sorted(unknown)}; known: {sorted(known)}")


def _route_name(storage_name: str) -> str:
    return storage_name.replace(".", "-")


# ------------------------------ schemas -------------------------------------

GATEWAY_TYPES = {"none", "haproxy", "litellm", "envoy", "nginx", "pingora"}
PRODUCTION_GATEWAYS = {"none", "haproxy"}  # ADR-000; others benchmark-only
SCHEDULER_TYPES = {"pbs", "slurm", "psij"}
VENDORS = {"xpu", "cuda", "rocm"}
ENGINES = {"vllm", "sglang"}


@dataclass(frozen=True)
class ScaleEnvelope:
    """ADR-000 envelope: qualification target vs evidence-derived support."""

    qualification_target_nodes: int = 64
    supported_max_nodes: int = 2       # grows only with WP12 gate evidence
    validation_mode: bool = False      # a validation-tier plan may exceed it

    def check_nodes(self, nodes: int, path: str) -> None:
        if nodes > self.qualification_target_nodes:
            raise PlanError(
                f"{path}: {nodes} nodes exceeds qualification target "
                f"{self.qualification_target_nodes} (ADR-000); expanding the "
                "envelope requires user approval")
        if nodes > self.supported_max_nodes and not self.validation_mode:
            raise PlanError(
                f"{path}: {nodes} nodes exceeds evidence-backed supported "
                f"maximum {self.supported_max_nodes}; run under a "
                "validation-mode plan (WP12 gate) to qualify this tier")


@dataclass(frozen=True)
class ModelPlan:
    model_id: str
    storage_name: str
    route_name: str
    tensor_parallel_size: int
    pipeline_parallel_size: int
    max_model_len: int
    size_b: int
    gpu_memory_utilization: float
    enforce_eager: bool
    max_num_seqs: int
    num_replicas: int | None  # None = auto-plan, only when never explicitly set


@dataclass(frozen=True)
class GatewayPlan:
    type: str
    port: int
    benchmark_only: bool
    options: tuple[tuple[str, Any], ...] = ()


@dataclass(frozen=True)
class SchedulerPlan:
    type: str
    nodes: int
    reservation_topology: str | None = None  # named, typed divergence only


@dataclass(frozen=True)
class DeploymentPlan:
    schema_version: int
    source_path: str
    num_nodes: int
    num_gpus_per_node: int
    vendor: str
    engine: str
    model_storage_path: str
    local_stage_path: str
    models: tuple[ModelPlan, ...]
    gateway: GatewayPlan
    scheduler: SchedulerPlan
    envelope: ScaleEnvelope
    plan_hash: str = ""

    def to_canonical_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data.pop("plan_hash", None)
        return data


def _hash_plan(plan: DeploymentPlan) -> str:
    canonical = json.dumps(plan.to_canonical_dict(), sort_keys=True,
                           separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


# ------------------------------ compiler ------------------------------------

_MODEL_KEYS = {"model_id", "tensor_parallel_size", "pipeline_parallel_size",
               "max_model_len", "size", "gpu_memory_utilization",
               "enforce_eager", "max_num_seqs", "num_replicas"}


def _compile_model(raw: Mapping[str, Any], path: str, *, num_nodes: int,
                   gpus_per_node: int) -> ModelPlan:
    reject_unknown_keys(raw, _MODEL_KEYS, path)
    model_id = strict_str(raw.get("model_id"), f"{path}.model_id")
    tp = strict_int(raw.get("tensor_parallel_size", 1),
                    f"{path}.tensor_parallel_size", minimum=1)
    pp = strict_int(raw.get("pipeline_parallel_size", 1),
                    f"{path}.pipeline_parallel_size", minimum=1)
    if pp > num_nodes:
        raise PlanError(
            f"{path}.pipeline_parallel_size: {pp} exceeds num_nodes "
            f"{num_nodes} (staging places one PP stage per node)")
    num_replicas: int | None
    if "num_replicas" in raw and raw["num_replicas"] is not None:
        num_replicas = strict_int(raw["num_replicas"], f"{path}.num_replicas",
                                  minimum=1)
        if num_replicas * tp * pp > num_nodes * gpus_per_node:
            raise PlanError(
                f"{path}.num_replicas: {num_replicas}×tp{tp}×pp{pp} exceeds "
                f"{num_nodes}×{gpus_per_node} available GPUs")
    else:
        num_replicas = None
    storage = get_model_storage_name(model_id)
    return ModelPlan(
        model_id=model_id, storage_name=storage, route_name=_route_name(storage),
        tensor_parallel_size=tp, pipeline_parallel_size=pp,
        max_model_len=strict_int(raw.get("max_model_len", 4096),
                                 f"{path}.max_model_len", minimum=1),
        size_b=strict_int(raw.get("size", 1), f"{path}.size", minimum=1),
        gpu_memory_utilization=strict_float(
            raw.get("gpu_memory_utilization", 0.90),
            f"{path}.gpu_memory_utilization", minimum=0.05, maximum=1.0),
        enforce_eager=strict_bool(raw.get("enforce_eager", True),
                                  f"{path}.enforce_eager"),
        max_num_seqs=strict_int(raw.get("max_num_seqs", 64),
                                f"{path}.max_num_seqs", minimum=1),
        num_replicas=num_replicas,
    )


def _check_identity_collisions(models: tuple[ModelPlan, ...]) -> None:
    for attr in ("model_id", "storage_name", "route_name"):
        seen: dict[str, str] = {}
        for model in models:
            key = getattr(model, attr)
            if key in seen:
                raise PlanError(
                    f"model identity collision on {attr}: {seen[key]!r} and "
                    f"{model.model_id!r} both map to {key!r} (PR-007)")
            seen[key] = model.model_id


def compile_deployment_plan(raw: Mapping[str, Any], *, source_path: str = "<dict>",
                            envelope: ScaleEnvelope | None = None) -> DeploymentPlan:
    """Compile a legacy-shaped config mapping into an immutable plan.

    Accepts the existing config.yaml top-level shape (ray_cluster_config /
    model_deployment_config / proxy_config / optional scheduler) so current
    inputs compile once into the new contract (WP1 action 6). Never mutates
    ``raw``.
    """
    envelope = envelope or ScaleEnvelope()
    top_known = {"ray_cluster_config", "model_deployment_config", "proxy_config",
                 "scheduler", "envelope"}
    reject_unknown_keys(raw, top_known, "<top>")
    dep_raw = raw.get("model_deployment_config")
    if not isinstance(dep_raw, Mapping):
        raise PlanError("model_deployment_config: required mapping is missing")

    dep_known = {"num_nodes", "model_storage_path", "local_stage_path",
                 "deployment_name", "replica_max_ongoing_requests",
                 "num_gpus_per_node", "model_configs", "vendor", "engine"}
    reject_unknown_keys(dep_raw, dep_known, "model_deployment_config")

    num_nodes = strict_int(dep_raw.get("num_nodes"), "model_deployment_config.num_nodes",
                           minimum=1)
    envelope.check_nodes(num_nodes, "model_deployment_config.num_nodes")
    gpus = strict_int(dep_raw.get("num_gpus_per_node", 12),
                      "model_deployment_config.num_gpus_per_node", minimum=1)
    vendor = strict_str(dep_raw.get("vendor", "xpu"),
                        "model_deployment_config.vendor", choices=VENDORS)
    engine = strict_str(dep_raw.get("engine", "vllm"),
                        "model_deployment_config.engine", choices=ENGINES)

    models_raw = dep_raw.get("model_configs")
    if not isinstance(models_raw, (list, tuple)) or not models_raw:
        raise PlanError("model_deployment_config.model_configs: non-empty list required")
    models = tuple(
        _compile_model(m, f"model_deployment_config.model_configs[{i}]",
                       num_nodes=num_nodes, gpus_per_node=gpus)
        for i, m in enumerate(models_raw))
    _check_identity_collisions(models)

    proxy_raw = raw.get("proxy_config") or {"type": "none"}
    gw_type = strict_str(proxy_raw.get("type", "none"), "proxy_config.type",
                         choices=GATEWAY_TYPES)
    gateway = GatewayPlan(
        type=gw_type,
        port=strict_int(proxy_raw.get("port", 4001), "proxy_config.port",
                        minimum=1, maximum=65535),
        benchmark_only=gw_type not in PRODUCTION_GATEWAYS,
        options=tuple(sorted(
            (k, v) for k, v in proxy_raw.items() if k not in {"type", "port"})),
    )

    sched_raw = raw.get("scheduler") or {}
    sched_type = strict_str(sched_raw.get("type", "pbs"), "scheduler.type",
                            choices=SCHEDULER_TYPES)
    reservation = sched_raw.get("reservation_topology")
    if "nodes" in sched_raw and sched_raw["nodes"] is not None:
        sched_nodes = strict_int(sched_raw["nodes"], "scheduler.nodes", minimum=1)
        if sched_nodes != num_nodes and reservation is None:
            # AC-PLAN-01: explicit mismatch is rejected; divergence only via a
            # named, typed reservation topology.
            raise PlanError(
                f"scheduler.nodes: explicit {sched_nodes} != "
                f"deployment num_nodes {num_nodes}; declare "
                "scheduler.reservation_topology to diverge intentionally")
    else:
        sched_nodes = num_nodes  # omitted inherits deployment size
    # The envelope bounds the ALLOCATION as well as the deployment: an
    # oversized control-plane allocation is a measurement confound (the
    # prod-256/deploy-N fig7 incident) and a resource-policy violation.
    envelope.check_nodes(sched_nodes, "scheduler.nodes")

    plan = DeploymentPlan(
        schema_version=SCHEMA_VERSION,
        source_path=source_path,
        num_nodes=num_nodes,
        num_gpus_per_node=gpus,
        vendor=vendor,
        engine=engine,
        model_storage_path=strict_str(dep_raw.get("model_storage_path"),
                                      "model_deployment_config.model_storage_path"),
        local_stage_path=strict_str(dep_raw.get("local_stage_path", "/tmp/hf_home"),
                                    "model_deployment_config.local_stage_path"),
        models=models,
        gateway=gateway,
        scheduler=SchedulerPlan(type=sched_type, nodes=sched_nodes,
                                reservation_topology=reservation),
        envelope=envelope,
    )
    object.__setattr__(plan, "plan_hash", _hash_plan(plan))
    return plan


def from_legacy_yaml(path: str, *, envelope: ScaleEnvelope | None = None) -> DeploymentPlan:
    import yaml

    with open(path, "r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    if not isinstance(raw, Mapping):
        raise PlanError(f"{path}: top-level YAML mapping required")
    # head_ip is runtime-resolved state the legacy launcher writes into the
    # source YAML (PR-003); it is not part of user intent and is ignored here.
    return compile_deployment_plan(raw, source_path=path, envelope=envelope)
