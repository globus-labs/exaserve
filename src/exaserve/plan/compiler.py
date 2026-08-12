"""The sole one-way compiler from intent to canonical ExaServe plans.

Legacy YAML and eval objects may be translated *into* this input, but no
downstream consumer is allowed to reinterpret them.  Every accepted field is
either represented in a hash-bearing contract or rejected with its full path.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, fields, replace
from pathlib import PurePath
from typing import Any, Mapping, Optional

from .contracts import (
    SCHEMA_VERSION,
    ArtifactPolicy,
    BackendPolicy,
    ClientPolicy,
    ControlLimits,
    DeploymentPlan,
    ExposureMode,
    ExposurePlan,
    GatewayKind,
    GatewayPlan,
    ModelPlan,
    PlanError,
    ReadinessLimits,
    ReplicaPlan,
    RuntimePolicy,
    RunPlan,
    ScaleEnvelope,
    SchedulerPlan,
    SiteProfile,
    TracePolicy,
    WorkloadPolicy,
    build_receipt_requirements,
    deep_freeze,
)

_MODEL_KEYS = {
    "model_id",
    "tensor_parallel_size",
    "pipeline_parallel_size",
    "max_model_len",
    "size",
    "gpu_memory_utilization",
    "enforce_eager",
    "enable_log_requests",
    "max_num_seqs",
    "num_replicas",
    "num_cpus_per_replica",
}
_DEPLOYMENT_KEYS = {
    "num_nodes",
    "models",
    "model_storage_path",
    "local_stage_path",
    "deployment_name",
    "replica_max_ongoing_requests",
    "num_gpus_per_node",
    "node_cpus",
    "ray_port",
    "collect_stats",
    "engine",
    "vendor",
    "accelerator",
    "gateway",
    "exposure",
    "validation_mode",
    "control",
    "request_mode",
    "streaming_mode",
    "scale_envelope_id",
    "runtime",
    "readiness",
}
_GATEWAY_KEYS = {
    "kind",
    "port",
    "backend_port",
    "executable_ref",
    "worker_count",
    "options",
}
_HAPROXY_OPTION_KEYS = {
    "balance",
    "check_interval",
    "check_fall",
    "check_rise",
    "stats_port",
    "maxconn",
    "http_no_delay",
    "abortonclose",
    "nbthread",
    "stats_bind",
    "stats_admin",
}
_LITELLM_OPTION_KEYS = {
    "db_url",
    "disable_hf_tokenizer_download",
    "extra_general",
    "extra_router",
    "keepalive_timeout",
    "master_key",
    "num_retries",
    "routing_strategy",
    "timeout",
}
_EXPOSURE_KEYS = {
    "mode",
    "advertised_scheme",
    "advertised_path",
    "serve_port",
    "network_boundary",
    "auth_policy_ref",
    "request_body_limit_bytes",
}
_SCHEDULER_KEYS = {
    "type",
    "nodes",
    "queue",
    "account",
    "walltime",
    "reservation_topology",
    "launcher",
    "resources",
    "filesystem_refs",
    "policy",
    "secret_refs",
}
_CONTROL_KEYS = {item.name for item in fields(ControlLimits)} - {"evidence_backed"}
_READINESS_KEYS = {item.name for item in fields(ReadinessLimits)} - {"evidence_backed"}
_RUNTIME_KEYS = {item.name for item in fields(RuntimePolicy)}
_SECRET_NAMES = re.compile(r"(?:^|_)(?:password|secret|token|api_key|master_key)(?:$|_)")
_REQUEST_MODES = {"chat", "completion", "mixed"}
_STREAMING_MODES = {"streaming", "non_streaming"}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _mapping(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise PlanError(f"{path} must be a mapping, got {type(value).__name__}")
    return value


def _optional_mapping(value: Any, path: str) -> Mapping[str, Any]:
    """Only absence means empty; falsy malformed values remain errors."""
    return {} if value is None else _mapping(value, path)


def _reject_unknown(raw: Mapping[str, Any], allowed: set[str], path: str) -> None:
    raw = _mapping(raw, path)
    unknown = sorted(str(key) for key in raw if key not in allowed)
    if unknown:
        raise PlanError(f"{path}: unknown key(s) {unknown}; accepted: {sorted(allowed)}")


def _integer(
    value: Any,
    path: str,
    *,
    default: Optional[int] = None,
    minimum: Optional[int] = None,
    maximum: Optional[int] = None,
) -> int:
    if value is None:
        if default is None:
            raise PlanError(f"{path} is required")
        value = default
    if isinstance(value, bool) or not isinstance(value, int):
        raise PlanError(f"{path} must be an integer, got {value!r}")
    result = value
    if minimum is not None and result < minimum:
        raise PlanError(f"{path} must be >= {minimum}, got {result}")
    if maximum is not None and result > maximum:
        raise PlanError(f"{path} must be <= {maximum}, got {result}")
    return result


def _number(
    value: Any,
    path: str,
    *,
    default: Optional[float] = None,
    minimum: Optional[float] = None,
    maximum: Optional[float] = None,
) -> float:
    if value is None:
        if default is None:
            raise PlanError(f"{path} is required")
        value = default
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PlanError(f"{path} must be a number, got {value!r}")
    result = float(value)
    if not math.isfinite(result):
        raise PlanError(f"{path} must be finite, got {value!r}")
    if minimum is not None and result < minimum:
        raise PlanError(f"{path} must be >= {minimum}, got {result}")
    if maximum is not None and result > maximum:
        raise PlanError(f"{path} must be <= {maximum}, got {result}")
    return result


def _boolean(value: Any, path: str, *, default: Optional[bool] = None) -> bool:
    if value is None and default is not None:
        return default
    if isinstance(value, bool):
        return value
    raise PlanError(f"{path} must be a boolean, got {value!r}")


def _string(
    value: Any, path: str, *, default: Optional[str] = None, allow_empty: bool = False
) -> str:
    if value is None:
        if default is None:
            raise PlanError(f"{path} is required")
        value = default
    if not isinstance(value, str):
        raise PlanError(f"{path} must be a string, got {type(value).__name__}")
    result = value.strip()
    if not allow_empty and not result:
        raise PlanError(f"{path} must be a non-empty string")
    return result


def _absolute_path(value: Any, path: str, *, default: str) -> str:
    result = _string(value, path, default=default)
    if not PurePath(result).is_absolute():
        raise PlanError(f"{path} must be an absolute path, got {result!r}")
    return result


def _sha(value: str, path: str) -> str:
    if not _SHA256.fullmatch(value):
        raise PlanError(f"{path} must be a lowercase sha256 digest")
    return value


def _storage_name(model_id: str) -> str:
    return model_id.replace("/", "--")


def _route_name(storage_name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]", "-", storage_name).strip("-").lower()


@dataclass(frozen=True)
class _ModelIntent:
    model_id: str
    storage_name: str
    route_name: str
    tensor_parallel_size: int
    pipeline_parallel_size: int
    max_model_len: int
    size_b: int
    gpu_memory_utilization: float
    enforce_eager: bool
    enable_log_requests: bool
    max_num_seqs: Optional[int]
    num_cpus_per_replica: int
    requested_replicas: Optional[int]


def _compile_model_intents(
    raw_models: Any, *, num_nodes: int, gpus_per_node: int, path_base: str = "deployment.models"
) -> tuple[_ModelIntent, ...]:
    if not isinstance(raw_models, (list, tuple)) or not raw_models:
        raise PlanError("deployment.models must be a non-empty list")
    intents: list[_ModelIntent] = []
    identities: dict[str, dict[str, str]] = {"model_id": {}, "storage_name": {}, "route_name": {}}
    for index, item in enumerate(raw_models):
        path = f"{path_base}[{index}]"
        raw = _mapping(item, path)
        _reject_unknown(raw, _MODEL_KEYS, path)
        model_id = _string(raw.get("model_id"), f"{path}.model_id")
        storage_name = _storage_name(model_id)
        route_name = _route_name(storage_name)
        for family, value in (
            ("model_id", model_id),
            ("storage_name", storage_name),
            ("route_name", route_name),
        ):
            previous = identities[family].get(value)
            if previous is not None:
                raise PlanError(
                    f"{path}: model identity collision ({family}): {previous!r} and {model_id!r} "
                    f"both map to {value!r}"
                )
            identities[family][value] = model_id
        tp = _integer(
            raw.get("tensor_parallel_size"), f"{path}.tensor_parallel_size", default=1, minimum=1
        )
        pp = _integer(
            raw.get("pipeline_parallel_size"),
            f"{path}.pipeline_parallel_size",
            default=1,
            minimum=1,
        )
        if tp > gpus_per_node:
            raise PlanError(
                f"{path}.tensor_parallel_size {tp} exceeds gpus_per_node {gpus_per_node}"
            )
        if pp > num_nodes:
            raise PlanError(
                f"{path}.pipeline_parallel_size {pp} exceeds deployment nodes "
                f"{num_nodes}; one PP stage per node is required"
            )
        requested = None
        if raw.get("num_replicas") is not None:
            requested = _integer(raw["num_replicas"], f"{path}.num_replicas", minimum=1)
        max_num_seqs = None
        if raw.get("max_num_seqs") is not None:
            max_num_seqs = _integer(raw["max_num_seqs"], f"{path}.max_num_seqs", minimum=1)
        intents.append(
            _ModelIntent(
                model_id=model_id,
                storage_name=storage_name,
                route_name=route_name,
                tensor_parallel_size=tp,
                pipeline_parallel_size=pp,
                max_model_len=_integer(
                    raw.get("max_model_len"), f"{path}.max_model_len", default=4096, minimum=1
                ),
                size_b=_integer(raw.get("size"), f"{path}.size", default=8, minimum=1),
                gpu_memory_utilization=_number(
                    raw.get("gpu_memory_utilization"),
                    f"{path}.gpu_memory_utilization",
                    default=0.90,
                    minimum=0.0000001,
                    maximum=1.0,
                ),
                enforce_eager=_boolean(
                    raw.get("enforce_eager"), f"{path}.enforce_eager", default=True
                ),
                enable_log_requests=_boolean(
                    raw.get("enable_log_requests"), f"{path}.enable_log_requests", default=True
                ),
                max_num_seqs=max_num_seqs,
                num_cpus_per_replica=_integer(
                    raw.get("num_cpus_per_replica"),
                    f"{path}.num_cpus_per_replica",
                    default=4,
                    minimum=1,
                ),
                requested_replicas=requested,
            )
        )
    return tuple(intents)


def _resolve_models(
    intents: tuple[_ModelIntent, ...],
    *,
    num_nodes: int,
    gpus_per_node: int,
    cpus_per_node: int,
    envelope: ScaleEnvelope,
) -> tuple[ModelPlan, ...]:
    """Resolve exact logical slots without allocation hostnames.

    This is the canonical equivalent of the old runtime replica planner.  All
    nodes are homogeneous at this boundary, so stable rank indices are enough;
    ``AllocationBinding`` later maps them to scheduler hostnames.
    """
    remaining_devices = [list(range(gpus_per_node)) for _ in range(num_nodes)]
    remaining_cpus = [cpus_per_node for _ in range(num_nodes)]
    placements: list[list[ReplicaPlan]] = [[] for _ in intents]

    def reserve(model_index: int) -> bool:
        intent = intents[model_index]
        # A shard-aware PP model publishes one different pruned tree per
        # (replica, stage) into a stable node-local model path.  One node
        # therefore cannot host two stages of the same model: the second
        # publication would replace the first.  Keep PP replicas disjoint by
        # construction instead of relying on live Ray ordering later.
        pp_ranks_in_use = (
            {rank for replica in placements[model_index] for rank in replica.planned_ranks}
            if intent.pipeline_parallel_size > 1
            else set()
        )
        eligible_primary = [
            rank
            for rank in range(num_nodes)
            if rank not in pp_ranks_in_use
            if len(remaining_devices[rank]) >= intent.tensor_parallel_size
            and remaining_cpus[rank] >= intent.num_cpus_per_replica
        ]
        eligible_primary.sort(
            key=lambda rank: (len(remaining_devices[rank]), remaining_cpus[rank], rank)
        )
        selected: list[int] = []
        for primary in eligible_primary:
            others = [
                rank
                for rank in range(num_nodes)
                if rank != primary
                and rank not in pp_ranks_in_use
                and len(remaining_devices[rank]) >= intent.tensor_parallel_size
            ]
            others.sort(key=lambda rank: (len(remaining_devices[rank]), remaining_cpus[rank], rank))
            if len(others) >= intent.pipeline_parallel_size - 1:
                selected = [primary, *others[: intent.pipeline_parallel_size - 1]]
                break
        if not selected:
            return False
        device_groups = []
        for rank in selected:
            devices = tuple(remaining_devices[rank][: intent.tensor_parallel_size])
            del remaining_devices[rank][: intent.tensor_parallel_size]
            device_groups.append(devices)
        remaining_cpus[selected[0]] -= intent.num_cpus_per_replica
        replica_index = len(placements[model_index])
        placements[model_index].append(
            ReplicaPlan(
                replica_id=f"{intent.route_name}/replica-{replica_index}",
                replica_index=replica_index,
                planned_ranks=tuple(selected),
                planned_device_ids=tuple(device_groups),
                tensor_parallel_size=intent.tensor_parallel_size,
                pipeline_parallel_size=intent.pipeline_parallel_size,
                gpu_demand=intent.tensor_parallel_size * intent.pipeline_parallel_size,
                cpu_demand=intent.num_cpus_per_replica,
            )
        )
        return True

    # Explicit reservations are promises and therefore fail rather than
    # degrading into fewer replicas.
    for index, intent in enumerate(intents):
        if intent.requested_replicas is None:
            continue
        if intent.requested_replicas > envelope.max_replicas_per_model:
            raise PlanError(
                f"deployment.models[{index}].num_replicas exceeds envelope maximum "
                f"{envelope.max_replicas_per_model}"
            )
        for _ in range(intent.requested_replicas):
            if not reserve(index):
                raise PlanError(
                    f"deployment.models[{index}].num_replicas={intent.requested_replicas} "
                    "cannot be placed in the declared node/GPU/CPU demand"
                )

    # PP auto mode is deliberately one replica (AC-PP-01).  TP auto models
    # share remaining capacity round-robin so input order cannot starve every
    # model after the first.
    auto_pp = [
        index
        for index, item in enumerate(intents)
        if item.requested_replicas is None and item.pipeline_parallel_size > 1
    ]
    for index in auto_pp:
        if not reserve(index):
            raise PlanError(
                f"deployment.models[{index}] cannot place the required default single PP replica"
            )
    auto_tp = [
        index
        for index, item in enumerate(intents)
        if item.requested_replicas is None and item.pipeline_parallel_size == 1
    ]
    while auto_tp:
        progressed = False
        for index in auto_tp:
            if len(placements[index]) >= envelope.max_replicas_per_model:
                continue
            progressed = reserve(index) or progressed
        if not progressed:
            break
    if any(not group for group in placements):
        missing = [intents[index].model_id for index, group in enumerate(placements) if not group]
        raise PlanError(f"no feasible replica placement for required model(s) {missing}")
    total = sum(len(group) for group in placements)
    if total > envelope.max_total_replicas:
        raise PlanError(
            f"resolved replica count {total} exceeds envelope maximum {envelope.max_total_replicas}"
        )
    if len(intents) > envelope.max_models:
        raise PlanError(
            f"model count {len(intents)} exceeds envelope maximum {envelope.max_models}"
        )

    return tuple(
        ModelPlan(
            model_id=intent.model_id,
            storage_name=intent.storage_name,
            route_name=intent.route_name,
            tensor_parallel_size=intent.tensor_parallel_size,
            pipeline_parallel_size=intent.pipeline_parallel_size,
            max_model_len=intent.max_model_len,
            size_b=intent.size_b,
            gpu_memory_utilization=intent.gpu_memory_utilization,
            enforce_eager=intent.enforce_eager,
            enable_log_requests=intent.enable_log_requests,
            max_num_seqs=intent.max_num_seqs,
            num_cpus_per_replica=intent.num_cpus_per_replica,
            num_replicas=len(placements[index]),
            replicas=tuple(placements[index]),
        )
        for index, intent in enumerate(intents)
    )


def _secret_safe_options(raw: Any, path: str) -> tuple[tuple[str, Any], ...]:
    options = _optional_mapping(raw, path)
    for key, value in options.items():
        if _SECRET_NAMES.search(str(key).lower()):
            if not (
                isinstance(value, Mapping)
                and set(value) == {"secret_ref"}
                and isinstance(value.get("secret_ref"), str)
                and value["secret_ref"].strip()
            ):
                raise PlanError(
                    f"{path}.{key} is secret-bearing; plans accept only "
                    "{{secret_ref: <non-empty reference>}}, never secret values"
                )
    return deep_freeze(options, path)


def _haproxy_options(raw: Any, path: str) -> tuple[tuple[str, Any], ...]:
    """Normalize the production HAProxy capability surface, fail closed."""
    options = _optional_mapping(raw, path)
    _reject_unknown(options, _HAPROXY_OPTION_KEYS, path)
    normalized: dict[str, Any] = {}
    balance = _string(options.get("balance"), f"{path}.balance", default="leastconn")
    if balance not in {"leastconn", "roundrobin", "random"}:
        raise PlanError(f"{path}.balance is not supported: {balance!r}")
    normalized["balance"] = balance
    for key, default, minimum, maximum in (
        ("check_interval", 5000, 100, 300000),
        ("check_fall", 3, 1, 100),
        ("check_rise", 2, 1, 100),
        ("stats_port", 9999, 0, 65535),
        ("maxconn", 8000, 1, 10_000_000),
        ("nbthread", 0, 0, 4096),
    ):
        normalized[key] = _integer(
            options.get(key), f"{path}.{key}", default=default, minimum=minimum, maximum=maximum
        )
    for key, default in (("http_no_delay", True), ("abortonclose", False), ("stats_admin", False)):
        normalized[key] = _boolean(options.get(key), f"{path}.{key}", default=default)
    stats_bind = _string(options.get("stats_bind"), f"{path}.stats_bind", default="127.0.0.1")
    if stats_bind not in {"127.0.0.1", "::1", "localhost"}:
        raise PlanError(
            f"{path}.stats_bind must remain loopback in the supported security boundary"
        )
    normalized["stats_bind"] = stats_bind
    if normalized["stats_admin"]:
        raise PlanError(
            f"{path}.stats_admin is unsupported in the production gateway; "
            "the management endpoint is read-only and loopback-bound"
        )
    return deep_freeze(normalized, path)


def _litellm_options(raw: Any, path: str) -> tuple[tuple[str, Any], ...]:
    """Freeze LiteLLM's request semantics into the canonical plan.

    The renderer used to supply its own 300-second request-timeout default,
    which meant a code-only default change could alter runtime behavior without
    changing the deployment-plan hash.  Resolve and validate those defaults at
    compilation instead; secret-bearing values retain the reference-only rule.
    """
    options = _optional_mapping(raw, path)
    _reject_unknown(options, _LITELLM_OPTION_KEYS, path)
    _secret_safe_options(options, path)
    normalized = dict(options)
    strategy = _string(
        options.get("routing_strategy"),
        f"{path}.routing_strategy",
        default="least-busy",
    )
    if strategy not in {"least-busy", "simple-shuffle", "latency-based-routing"}:
        raise PlanError(f"{path}.routing_strategy is not supported: {strategy!r}")
    normalized["routing_strategy"] = strategy
    normalized["num_retries"] = _integer(
        options.get("num_retries"),
        f"{path}.num_retries",
        default=2,
        minimum=0,
        maximum=100,
    )
    normalized["timeout"] = _integer(
        options.get("timeout"),
        f"{path}.timeout",
        default=300,
        minimum=1,
        maximum=86400,
    )
    # Aurora compute nodes do not have general Internet egress.  LiteLLM's
    # Llama-family token counter otherwise attempts an on-demand Hugging Face
    # tokenizer download in request handling, which can block every worker
    # under load.  Resolve the supported LiteLLM offline fallback into the
    # immutable plan instead of depending on ambient environment or a renderer
    # default that would not affect the plan hash.
    normalized["disable_hf_tokenizer_download"] = _boolean(
        options.get("disable_hf_tokenizer_download"),
        f"{path}.disable_hf_tokenizer_download",
        default=True,
    )
    normalized["keepalive_timeout"] = _integer(
        options.get("keepalive_timeout"),
        f"{path}.keepalive_timeout",
        default=120,
        minimum=1,
        maximum=86400,
    )
    if "extra_general" in options:
        normalized["extra_general"] = _optional_mapping(
            options["extra_general"], f"{path}.extra_general"
        )
    if "extra_router" in options:
        extra_router = _optional_mapping(options["extra_router"], f"{path}.extra_router")
        shadowed = sorted({"routing_strategy", "num_retries", "timeout"}.intersection(extra_router))
        if shadowed:
            raise PlanError(
                f"{path}.extra_router cannot override canonical option(s) {shadowed}; "
                "set them at deployment.gateway.options"
            )
        normalized["extra_router"] = extra_router
    return deep_freeze(normalized, path)


def _compile_exposure(
    raw: Mapping[str, Any], *, site: SiteProfile, validation_mode: bool
) -> tuple[ExposurePlan, Optional[GatewayPlan]]:
    raw_exposure = _optional_mapping(raw.get("exposure"), "deployment.exposure")
    _reject_unknown(raw_exposure, _EXPOSURE_KEYS, "deployment.exposure")
    raw_gateway: Any = raw.get("gateway")
    if raw_gateway is not None:
        raw_gateway = _mapping(raw_gateway, "deployment.gateway")
        _reject_unknown(raw_gateway, _GATEWAY_KEYS, "deployment.gateway")
        spelling = raw_gateway.get("kind")
        if isinstance(spelling, str) and spelling.strip().lower() in ("none", "direct", ""):
            raise PlanError(
                "deployment.gateway none/direct must be represented as null with "
                "explicit DIRECT_VALIDATION exposure"
            )

    declared_mode = raw_exposure.get("mode")
    if raw_gateway is None:
        if not validation_mode:
            raise PlanError(
                "Production exposure requires HAProxy with PROXIED_INTERNAL; "
                "gateway-free exposure requires validation_mode=true"
            )
        mode = _string(
            declared_mode, "deployment.exposure.mode", default=ExposureMode.DIRECT_VALIDATION.value
        )
        if mode not in {
            ExposureMode.DIRECT_VALIDATION.value,
            ExposureMode.RAY_SERVE_HEAD_ONLY.value,
        }:
            raise PlanError(
                "deployment.exposure.mode must be DIRECT_VALIDATION or "
                "RAY_SERVE_HEAD_ONLY when gateway is null"
            )
        exposure = ExposurePlan(
            mode=mode,
            advertised_scheme=_string(
                raw_exposure.get("advertised_scheme"),
                "deployment.exposure.advertised_scheme",
                default="http",
            ),
            advertised_path=_string(
                raw_exposure.get("advertised_path"),
                "deployment.exposure.advertised_path",
                default="/v1",
            ),
            network_boundary=_string(
                raw_exposure.get("network_boundary"),
                "deployment.exposure.network_boundary",
                default=site.network_boundary,
            ),
            auth_policy_ref=(
                None
                if raw_exposure.get("auth_policy_ref") is None
                else _string(raw_exposure["auth_policy_ref"], "deployment.exposure.auth_policy_ref")
            ),
            request_body_limit_bytes=_integer(
                raw_exposure.get("request_body_limit_bytes"),
                "deployment.exposure.request_body_limit_bytes",
                default=16 << 20,
                minimum=1,
            ),
            serve_port=_integer(
                raw_exposure.get("serve_port"),
                "deployment.exposure.serve_port",
                default=8000,
                minimum=1,
                maximum=65535,
            ),
        )
        return exposure, None

    kind = _string(raw_gateway.get("kind"), "deployment.gateway.kind").lower()
    if kind not in {item.value for item in GatewayKind}:
        raise PlanError(f"deployment.gateway.kind {kind!r} is not a managed gateway")
    gateway = GatewayPlan(
        kind=kind,
        port=_integer(
            raw_gateway.get("port"),
            "deployment.gateway.port",
            default=4001,
            minimum=1,
            maximum=65535,
        ),
        backend_port=_integer(
            raw_gateway.get("backend_port"),
            "deployment.gateway.backend_port",
            default=8000,
            minimum=1,
            maximum=65535,
        ),
        executable_ref=_string(
            raw_gateway.get("executable_ref"),
            "deployment.gateway.executable_ref",
            default=f"PATH:{kind}",
        ),
        worker_count=_integer(
            raw_gateway.get("worker_count"), "deployment.gateway.worker_count", default=1, minimum=1
        ),
        options=(
            _haproxy_options(raw_gateway.get("options"), "deployment.gateway.options")
            if kind == GatewayKind.HAPROXY.value
            else (
                _litellm_options(raw_gateway.get("options"), "deployment.gateway.options")
                if kind == GatewayKind.LITELLM.value
                else _secret_safe_options(raw_gateway.get("options"), "deployment.gateway.options")
            )
        ),
    )
    if not site.supports_gateway(kind):
        raise PlanError(
            f"site {site.site_id} does not support gateway {kind!r}; "
            f"supported={sorted(site.gateway_kinds)}"
        )
    if not validation_mode and kind != GatewayKind.HAPROXY.value:
        raise PlanError(
            f"gateway {kind!r} is not a first-release production gateway; "
            "use validation_mode for qualification"
        )
    mode = _string(
        declared_mode, "deployment.exposure.mode", default=ExposureMode.PROXIED_INTERNAL.value
    )
    if mode != ExposureMode.PROXIED_INTERNAL.value:
        raise PlanError("a managed gateway requires deployment.exposure.mode=PROXIED_INTERNAL")
    exposure = ExposurePlan(
        mode=mode,
        advertised_scheme=_string(
            raw_exposure.get("advertised_scheme"),
            "deployment.exposure.advertised_scheme",
            default="http",
        ),
        advertised_path=_string(
            raw_exposure.get("advertised_path"),
            "deployment.exposure.advertised_path",
            default="/v1",
        ),
        network_boundary=_string(
            raw_exposure.get("network_boundary"),
            "deployment.exposure.network_boundary",
            default=site.network_boundary,
        ),
        auth_policy_ref=(
            None
            if raw_exposure.get("auth_policy_ref") is None
            else _string(raw_exposure["auth_policy_ref"], "deployment.exposure.auth_policy_ref")
        ),
        request_body_limit_bytes=_integer(
            raw_exposure.get("request_body_limit_bytes"),
            "deployment.exposure.request_body_limit_bytes",
            default=16 << 20,
            minimum=1,
        ),
        serve_port=gateway.backend_port,
    )
    if kind == GatewayKind.HAPROXY.value:
        if exposure.advertised_scheme != "http":
            raise PlanError("the supported HAProxy profile does not declare TLS termination")
        if exposure.auth_policy_ref is not None:
            raise PlanError("the supported HAProxy profile does not implement an auth policy")
        if exposure.network_boundary != "trusted_allocation":
            raise PlanError("the first-release HAProxy boundary is trusted_allocation only")
    return exposure, gateway


def _compatibility_identity(
    profile_hash: str, manifest_hash: str, *, vendor: str
) -> tuple[str, str]:
    if profile_hash and manifest_hash:
        return (
            _sha(profile_hash, "compatibility_profile_hash"),
            _sha(manifest_hash, "manifest_hash"),
        )
    # This import is intentionally safe before activation: profile construction
    # is metadata-only and must not import Ray/vLLM (guarded by tests).
    from ..compat.producers import manifest_hash as compute_manifest_hash
    from ..compat.profile import default_profile

    profile = default_profile(vendor)
    resolved_profile = profile_hash or profile.profile_id
    resolved_manifest = manifest_hash or compute_manifest_hash(profile)
    return (
        _sha(resolved_profile, "compatibility_profile_hash"),
        _sha(resolved_manifest, "manifest_hash"),
    )


def _generic_envelope(
    *,
    site: SiteProfile,
    vendor: str,
    engine: str,
    accelerator: str,
    gateway: Optional[GatewayPlan],
    exposure: ExposurePlan,
    request_mode: str,
    streaming_mode: str,
    validation_mode: bool,
    compatibility_profile_ref: str,
) -> ScaleEnvelope:
    return ScaleEnvelope(
        schema_version=SCHEMA_VERSION,
        envelope_id=f"{site.site_id}-unqualified-generic",
        site_id=site.site_id,
        scheduler_type=site.scheduler_types[0] if site.scheduler_types else "unknown",
        vendor=vendor,
        accelerator=accelerator,
        engine=engine,
        compatibility_profile_ref=compatibility_profile_ref,
        gateway_kind=None if gateway is None else gateway.kind,
        exposure_mode=exposure.mode,
        request_mode=request_mode,
        streaming_mode=streaming_mode,
        min_nodes=1,
        supported_max_nodes=site.max_nodes,
        qualification_target_nodes=site.max_nodes,
        qualification_target_approved=False,
        max_replicas_per_model=site.max_nodes * site.gpus_per_node,
        max_total_replicas=site.max_nodes * site.gpus_per_node,
        max_models=64,
        validation_tier="synthetic-site-profile",
        validation_mode=validation_mode,
    )


def _select_envelope(
    *,
    site: SiteProfile,
    requested_id: str,
    vendor: str,
    engine: str,
    accelerator: str,
    gateway: Optional[GatewayPlan],
    exposure: ExposurePlan,
    request_mode: str,
    streaming_mode: str,
    validation_mode: bool,
    compatibility_profile_ref: str,
    override: Optional[ScaleEnvelope],
) -> ScaleEnvelope:
    if override is not None:
        candidate = replace(override, validation_mode=validation_mode)
    else:
        wanted_gateway = None if gateway is None else gateway.kind
        matches = [
            item
            for item in site.scale_envelopes
            if (not requested_id or item.envelope_id == requested_id)
            and item.vendor == vendor
            and item.engine == engine
            and item.accelerator == accelerator
            and item.gateway_kind == wanted_gateway
            and item.exposure_mode == exposure.mode
            and item.request_mode == request_mode
            and item.streaming_mode == streaming_mode
        ]
        if len(matches) > 1:
            raise PlanError(
                "deployment matches multiple scale envelopes; specify scale_envelope_id"
            )
        if not matches:
            if requested_id:
                raise PlanError(
                    f"deployment.scale_envelope_id {requested_id!r} does not match "
                    "the deployment dimensions"
                )
            if site.scale_envelopes and not validation_mode:
                raise PlanError(
                    "production deployment does not match an evidence-backed site scale envelope; "
                    "use validation_mode for an unqualified combination "
                    f"(vendor={vendor}, accelerator={accelerator}, engine={engine}, "
                    f"gateway={wanted_gateway}, exposure={exposure.mode}, "
                    f"request={request_mode}, streaming={streaming_mode})"
                )
            candidate = _generic_envelope(
                site=site,
                vendor=vendor,
                engine=engine,
                accelerator=accelerator,
                gateway=gateway,
                exposure=exposure,
                request_mode=request_mode,
                streaming_mode=streaming_mode,
                validation_mode=validation_mode,
                compatibility_profile_ref=compatibility_profile_ref,
            )
        else:
            candidate = replace(matches[0], validation_mode=validation_mode)
    checks = {
        "site_id": site.site_id,
        "vendor": vendor,
        "accelerator": accelerator,
        "engine": engine,
        "gateway_kind": None if gateway is None else gateway.kind,
        "exposure_mode": exposure.mode,
        "request_mode": request_mode,
        "streaming_mode": streaming_mode,
    }
    for name, expected in checks.items():
        if getattr(candidate, name) != expected:
            raise PlanError(
                f"scale envelope {candidate.envelope_id!r} has {name}="
                f"{getattr(candidate, name)!r}, deployment requires {expected!r}"
            )
    if candidate.compatibility_profile_ref != compatibility_profile_ref:
        raise PlanError(
            f"scale envelope {candidate.envelope_id!r} is qualified for compatibility "
            f"profile {candidate.compatibility_profile_ref!r}, deployment resolves "
            f"{compatibility_profile_ref!r}"
        )
    return candidate


def _normalize_input(
    raw: Mapping[str, Any],
) -> dict[str, Any]:
    """Accept only the canonical deployment input schema.

    Migration aliases used to reinterpret nested ``model_deployment_config``,
    ``proxy_config`` and mutable Ray config. Keeping that adapter after cutover
    left two public configuration languages with different defaults.
    """
    return dict(_mapping(raw, "deployment"))


def compile_deployment_plan(
    raw: Mapping[str, Any],
    *,
    site: Optional[SiteProfile] = None,
    deployment_id: str = "deployment",
    compatibility_profile_hash: str = "",
    manifest_hash: str = "",
    envelope: Optional[ScaleEnvelope] = None,
) -> DeploymentPlan:
    """Compile one immutable serving/resource contract.

    Source paths and timestamps belong in ``RunProvenance`` and are not accepted
    at this semantic boundary.
    """
    if site is None:
        from ..site import default_site_profile

        site = default_site_profile()
    deployment = _normalize_input(raw)
    _reject_unknown(deployment, _DEPLOYMENT_KEYS, "deployment")

    validation_mode = _boolean(
        deployment.get("validation_mode"), "deployment.validation_mode", default=False
    )
    num_nodes = _integer(deployment.get("num_nodes"), "deployment.num_nodes", default=1, minimum=1)
    if num_nodes > site.max_nodes:
        raise PlanError(
            f"deployment.num_nodes {num_nodes} exceeds site {site.site_id} maximum {site.max_nodes}"
        )
    gpus = _integer(
        deployment.get("num_gpus_per_node"),
        "deployment.num_gpus_per_node",
        default=site.gpus_per_node,
        minimum=1,
        maximum=site.gpus_per_node,
    )
    node_cpus = _integer(
        deployment.get("node_cpus"),
        "deployment.node_cpus",
        default=site.cpus_per_node,
        minimum=1,
        maximum=site.cpus_per_node,
    )
    ray_port = _integer(
        deployment.get("ray_port"),
        "deployment.ray_port",
        default=6379,
        minimum=1,
        maximum=65535,
    )
    vendor = _string(
        deployment.get("vendor"),
        "deployment.vendor",
        default=site.vendors[0] if site.vendors else "xpu",
    )
    engine = _string(
        deployment.get("engine"),
        "deployment.engine",
        default=site.engines[0] if site.engines else "vllm",
    )
    if site.vendors and vendor not in site.vendors:
        raise PlanError(f"deployment.vendor {vendor!r} unsupported by site {site.site_id}")
    if site.engines and engine not in site.engines:
        raise PlanError(f"deployment.engine {engine!r} unsupported by site {site.site_id}")
    accelerator = _string(
        deployment.get("accelerator"),
        "deployment.accelerator",
        default=(site.accelerator_inventory[0] if site.accelerator_inventory else vendor),
    )
    request_mode = _string(
        deployment.get("request_mode"), "deployment.request_mode", default="completion"
    )
    streaming_mode = _string(
        deployment.get("streaming_mode"), "deployment.streaming_mode", default="non_streaming"
    )
    if request_mode not in _REQUEST_MODES:
        raise PlanError(
            f"deployment.request_mode must be one of {sorted(_REQUEST_MODES)}, got {request_mode!r}"
        )
    if streaming_mode not in _STREAMING_MODES:
        raise PlanError(
            f"deployment.streaming_mode must be one of {sorted(_STREAMING_MODES)}, "
            f"got {streaming_mode!r}"
        )
    exposure, gateway = _compile_exposure(deployment, site=site, validation_mode=validation_mode)
    compat_hash, resolved_manifest_hash = _compatibility_identity(
        compatibility_profile_hash, manifest_hash, vendor=vendor
    )
    selected_envelope = _select_envelope(
        site=site,
        requested_id=_string(
            deployment.get("scale_envelope_id"),
            "deployment.scale_envelope_id",
            default="",
            allow_empty=True,
        ),
        vendor=vendor,
        engine=engine,
        accelerator=accelerator,
        gateway=gateway,
        exposure=exposure,
        request_mode=request_mode,
        streaming_mode=streaming_mode,
        validation_mode=validation_mode,
        compatibility_profile_ref=compat_hash,
        override=envelope,
    )
    selected_envelope.check_nodes(num_nodes)

    raw_models = deployment.get("models")
    model_path = "deployment.models"
    intents = _compile_model_intents(
        raw_models, num_nodes=num_nodes, gpus_per_node=gpus, path_base=model_path
    )
    models = _resolve_models(
        intents,
        num_nodes=num_nodes,
        gpus_per_node=gpus,
        cpus_per_node=node_cpus,
        envelope=selected_envelope,
    )

    raw_runtime = _optional_mapping(deployment.get("runtime"), "deployment.runtime")
    _reject_unknown(raw_runtime, _RUNTIME_KEYS, "deployment.runtime")
    multi_replica = any(model.num_replicas > 1 for model in models)
    multi_replica_pp = any(
        model.pipeline_parallel_size > 1 and model.num_replicas > 1 for model in models
    )
    if multi_replica and "ray_serve.run_many" not in site.launcher_capabilities:
        raise PlanError(
            "canonical multi-replica placement requires the SiteProfile "
            "capability 'ray_serve.run_many'"
        )
    requested_shard_aware = _boolean(
        raw_runtime.get("pp_shard_aware"),
        "deployment.runtime.pp_shard_aware",
        default=multi_replica_pp,
    )
    if requested_shard_aware != multi_replica_pp:
        raise PlanError(
            "deployment.runtime.pp_shard_aware must be enabled exactly for "
            "multi-replica pipeline-parallel models"
        )
    runtime = RuntimePolicy(
        null_compute=_boolean(
            raw_runtime.get("null_compute"), "deployment.runtime.null_compute", default=False
        ),
        null_compute_latency_s=_number(
            raw_runtime.get("null_compute_latency_s"),
            "deployment.runtime.null_compute_latency_s",
            default=1.0,
            minimum=0.0,
        ),
        instrumentation=_boolean(
            raw_runtime.get("instrumentation"), "deployment.runtime.instrumentation", default=False
        ),
        pp_shard_aware=requested_shard_aware,
        clean_stage=_boolean(
            raw_runtime.get("clean_stage"), "deployment.runtime.clean_stage", default=False
        ),
        stats_retention=_integer(
            raw_runtime.get("stats_retention"),
            "deployment.runtime.stats_retention",
            default=2000,
            minimum=1,
        ),
        stats_push_period_s=_number(
            raw_runtime.get("stats_push_period_s"),
            "deployment.runtime.stats_push_period_s",
            default=10.0,
            minimum=0.0000001,
        ),
        stats_sample_cap=_integer(
            raw_runtime.get("stats_sample_cap"),
            "deployment.runtime.stats_sample_cap",
            default=1500,
            minimum=1,
        ),
    )

    raw_control = _optional_mapping(deployment.get("control"), "deployment.control")
    _reject_unknown(raw_control, _CONTROL_KEYS, "deployment.control")
    base_control = {item.name: getattr(site.control, item.name) for item in fields(ControlLimits)}
    for key, value in raw_control.items():
        if key.startswith("max_"):
            base_control[key] = _integer(value, f"deployment.control.{key}", minimum=1)
        else:
            base_control[key] = _number(value, f"deployment.control.{key}", minimum=0.0000001)
    control = ControlLimits(**base_control)

    raw_readiness = _optional_mapping(deployment.get("readiness"), "deployment.readiness")
    _reject_unknown(raw_readiness, _READINESS_KEYS, "deployment.readiness")
    base_readiness = {
        item.name: getattr(site.readiness, item.name) for item in fields(ReadinessLimits)
    }
    for key, value in raw_readiness.items():
        if key == "allow_excess_resources":
            base_readiness[key] = _boolean(value, f"deployment.readiness.{key}")
        else:
            base_readiness[key] = _number(value, f"deployment.readiness.{key}", minimum=0.0000001)
    # LiteLLM imports and initializes every worker before binding.  The small
    # probe took 51s (one worker) and 59s (eight workers); after removing the
    # erroneous node-by-replica Cartesian configuration, the real 12-entry,
    # eight-worker paper topology reached READY 27.5s after endpoint setup.
    # The generic 30s edge is therefore unsafe while a two-minute boundary is
    # finite and retains measured headroom.  Its live ingress can also be
    # intentionally saturated by the paper workload.  A queued request remains
    # valid until LiteLLM's own resolved request timeout, so post-READY recovery
    # must cover that bound plus one complete externally advertised canary.
    if gateway is not None and gateway.kind == GatewayKind.LITELLM.value:
        litellm_request_timeout_s = float(dict(gateway.options)["timeout"])
        base_readiness["gateway_start_deadline_s"] = max(
            120.0, float(base_readiness["gateway_start_deadline_s"])
        )
        base_readiness["recovery_deadline_s"] = max(
            litellm_request_timeout_s + float(base_readiness["canary_timeout_s"]),
            float(base_readiness["recovery_deadline_s"]),
        )
    readiness = ReadinessLimits(**base_readiness)

    plan = DeploymentPlan(
        schema_version=SCHEMA_VERSION,
        deployment_id=_string(deployment_id, "deployment_id"),
        site_profile_id=site.site_id,
        site_profile_hash=site.site_profile_hash or site.compute_hash(),
        compatibility_profile_hash=compat_hash,
        manifest_hash=resolved_manifest_hash,
        scale_envelope=selected_envelope,
        num_nodes=num_nodes,
        num_gpus_per_node=gpus,
        node_cpus=node_cpus,
        ray_port=ray_port,
        vendor=vendor,
        engine=engine,
        model_storage_path=_absolute_path(
            deployment.get("model_storage_path"),
            "deployment.model_storage_path",
            default=site.model_storage_path,
        ),
        local_stage_path=_absolute_path(
            deployment.get("local_stage_path"),
            "deployment.local_stage_path",
            default=site.local_stage_path,
        ),
        deployment_name=_string(
            deployment.get("deployment_name"),
            "deployment.deployment_name",
            default="exaserve_serve",
        ),
        replica_max_ongoing_requests=_integer(
            deployment.get("replica_max_ongoing_requests"),
            "deployment.replica_max_ongoing_requests",
            default=32,
            minimum=1,
        ),
        collect_stats=_boolean(
            deployment.get("collect_stats"), "deployment.collect_stats", default=False
        ),
        models=models,
        exposure=exposure,
        gateway=gateway,
        receipt_requirements=build_receipt_requirements(
            num_nodes=num_nodes,
            models=models,
            gateway=gateway,
            include_engine_processes=not runtime.null_compute,
        ),
        control=control,
        readiness=readiness,
        runtime=runtime,
        validation_mode=validation_mode,
    )
    finalized = plan.finalize()
    # A deployment hash excludes scheduler intent, but accepting an invalid
    # sibling scheduler block would still be silent interpretation drift.
    return finalized


def _compile_scheduler(
    raw: Mapping[str, Any], *, site: SiteProfile, deployment: DeploymentPlan
) -> SchedulerPlan:
    raw = _optional_mapping(raw, "scheduler")
    _reject_unknown(raw, _SCHEDULER_KEYS, "scheduler")
    scheduler_type = _string(
        raw.get("type"),
        "scheduler.type",
        default=site.scheduler_types[0] if site.scheduler_types else "pbs",
    )
    if site.scheduler_types and scheduler_type not in site.scheduler_types:
        raise PlanError(f"scheduler.type {scheduler_type!r} unsupported by site {site.site_id}")
    nodes = _integer(
        raw.get("nodes"),
        "scheduler.nodes",
        default=deployment.num_nodes,
        minimum=1,
        maximum=site.max_nodes,
    )
    topology = raw.get("reservation_topology")
    if topology is not None:
        topology = _string(topology, "scheduler.reservation_topology")
        if topology not in ("logical_subset", "oversized_control"):
            raise PlanError(
                "scheduler.reservation_topology must be logical_subset or oversized_control"
            )
    resources = _optional_mapping(raw.get("resources"), "scheduler.resources")
    if nodes != deployment.num_nodes:
        declared_deployment_nodes = resources.get("deployment_nodes")
        if (
            topology is None
            or _integer(
                declared_deployment_nodes, "scheduler.resources.deployment_nodes", minimum=1
            )
            != deployment.num_nodes
        ):
            raise PlanError(
                f"scheduler.nodes {nodes} != deployment.num_nodes "
                f"{deployment.num_nodes}; divergence requires a typed "
                "reservation_topology and matching resources.deployment_nodes"
            )
    if nodes > deployment.scale_envelope.qualification_target_nodes:
        raise PlanError(
            f"scheduler.nodes {nodes} exceeds scale-envelope qualification target "
            f"{deployment.scale_envelope.qualification_target_nodes}"
        )
    if "filesystem_refs" in raw and "filesystems" in raw:
        raise PlanError("scheduler cannot declare filesystem_refs and filesystems")
    filesystem_refs = raw.get("filesystem_refs", raw.get("filesystems", ()))
    if not isinstance(filesystem_refs, (list, tuple)):
        raise PlanError("scheduler.filesystem_refs must be a list")
    secret_refs = () if raw.get("secret_refs") is None else raw["secret_refs"]
    if not isinstance(secret_refs, (list, tuple)) or any(
        not isinstance(item, str) or not item for item in secret_refs
    ):
        raise PlanError("scheduler.secret_refs must be a list of non-empty references")
    return SchedulerPlan(
        type=scheduler_type,
        nodes=nodes,
        queue=_string(raw.get("queue"), "scheduler.queue", default="", allow_empty=True),
        account=_string(
            raw.get("account", raw.get("project")),
            "scheduler.account",
            default="",
            allow_empty=True,
        ),
        walltime=_string(raw.get("walltime"), "scheduler.walltime", default="", allow_empty=True),
        reservation_topology=topology,
        launcher=_string(raw.get("launcher"), "scheduler.launcher", default="mpi"),
        resources=deep_freeze(resources, "scheduler.resources"),
        filesystem_refs=tuple(filesystem_refs),
        policy=deep_freeze(
            _optional_mapping(raw.get("policy"), "scheduler.policy"), "scheduler.policy"
        ),
        secret_refs=tuple(secret_refs),
    )


def compile_run_plan(
    raw: Mapping[str, Any],
    *,
    site: Optional[SiteProfile] = None,
    run_id: str,
    deployment_id: str,
    scheduler: Optional[SchedulerPlan] = None,
    workload: Optional[WorkloadPolicy] = None,
    trace: Optional[TracePolicy] = None,
    client: Optional[ClientPolicy] = None,
    backend: Optional[BackendPolicy] = None,
    artifacts: Optional[ArtifactPolicy] = None,
) -> RunPlan:
    """Compile the eval/ClientLab wrapper around the exact deployment object."""
    if site is None:
        from ..site import default_site_profile

        site = default_site_profile()
    workload_supplied = workload is not None
    client_supplied = client is not None
    workload = workload or WorkloadPolicy()
    client = client or ClientPolicy()
    active_modes = {name for name, weight in workload.modes if weight > 0}
    request_mode = "mixed" if len(active_modes) > 1 else next(iter(active_modes))
    streaming_mode = "streaming" if client.streaming else "non_streaming"
    deployment_raw = dict(raw)
    for name, derived in (
        ("request_mode", request_mode),
        ("streaming_mode", streaming_mode),
    ):
        declared = deployment_raw.get(name)
        if declared is not None and declared != derived:
            raise PlanError(
                f"deployment.{name} {declared!r} disagrees with the typed run policy {derived!r}"
            )
        deployment_raw[name] = derived
    deployment = compile_deployment_plan(deployment_raw, site=site, deployment_id=deployment_id)
    if not client_supplied:
        destination = (
            workload.client_dest
            if workload_supplied
            else (
                "proxy"
                if deployment.exposure.mode == ExposureMode.PROXIED_INTERNAL.value
                else "direct"
            )
        )
        nodes = (
            workload.client_nodes
            if workload_supplied
            else (deployment.num_nodes if destination == "direct" else 1)
        )
        client = replace(client, destination=destination, nodes=nodes)
    if not workload_supplied:
        workload = replace(
            workload,
            client_dest=client.destination,
            client_nodes=client.nodes,
        )
    scheduler = scheduler or _compile_scheduler({}, site=site, deployment=deployment)
    if scheduler.type not in (site.scheduler_types or (scheduler.type,)):
        raise PlanError(f"scheduler.type {scheduler.type!r} unsupported by site {site.site_id}")
    if scheduler.type != deployment.scale_envelope.scheduler_type:
        raise PlanError(
            f"scheduler.type {scheduler.type!r} disagrees with scale envelope "
            f"{deployment.scale_envelope.envelope_id!r} scheduler "
            f"{deployment.scale_envelope.scheduler_type!r}"
        )
    if scheduler.nodes != deployment.num_nodes and scheduler.reservation_topology is None:
        raise PlanError("scheduler/deployment node mismatch requires reservation_topology")
    return RunPlan(
        schema_version=SCHEMA_VERSION,
        run_id=_string(run_id, "run_id"),
        deployment=deployment,
        scheduler=scheduler,
        workload=workload,
        trace=trace or TracePolicy(),
        client=client,
        backend=backend or BackendPolicy(),
        artifacts=artifacts or ArtifactPolicy(),
    ).finalize()


__all__ = [
    "_DEPLOYMENT_KEYS",
    "build_receipt_requirements",
    "compile_deployment_plan",
    "compile_run_plan",
]
