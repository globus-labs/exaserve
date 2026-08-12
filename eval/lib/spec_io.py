"""Spec I/O: loading, validation, and normalization of experiment spec YAML files.

This module is the single entry point for turning a spec YAML into an
ExperimentSpec dataclass. It performs three phases:

  1. **Parsing** — YAML dict -> typed dataclass fields, with site_config
     defaults filling in any omitted paths (model storage, prompts, etc.).
  2. **Validation** — structural checks (required fields, value ranges,
     supported enum values) that catch user errors early.
  3. **Normalization** — derived-field sync (e.g., client.num_nodes defaults
     to deployment.num_nodes when not explicitly set).

Design note: site_config is accessed lazily via get_site_config() rather
than module-level constants so that tests and non-Aurora environments can
override it without import-time side effects.
"""

from __future__ import annotations

import os
import math
from dataclasses import replace
from typing import Any

from eval.site_config import get_site_config

from .models import (
    BackendSpec,
    ClientSpec,
    DerivedField,
    DeploymentSpec,
    ExperimentSpec,
    MatrixAxis,
    MatrixSpec,
    ModelSpec,
    SaturationSpec,
    SchedulerSpec,
    TraceSpec,
    WorkloadSpec,
)
from .saturation import parse_saturation_spec, validate_saturation_spec
from .utils import dotted_get, dump_yaml_file, load_yaml_file, resolve_path


SUPPORTED_TRACE_KINDS = {"weak_scaling", "azure_trace", "dataset_replay"}
SUPPORTED_BACKENDS = {"ray", "mock"}


def _mapping(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError(f"{path} must be a mapping with string keys")
    return dict(value)


def _shape(
    data: dict[str, Any], path: str, allowed: set[str], required: set[str] = frozenset()
) -> None:
    unknown = sorted(set(data) - allowed)
    missing = sorted(required - set(data))
    if unknown or missing:
        raise ValueError(f"{path} shape mismatch: unknown={unknown}, missing={missing}")


def _string(value: Any, path: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{path} must be a string")
    return value


def _integer(value: Any, path: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{path} must be an integer")
    return value


def _number(value: Any, path: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
    ):
        raise ValueError(f"{path} must be a finite number")
    return float(value)


def _boolean(value: Any, path: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{path} must be a boolean")
    return value


def _list(value: Any, path: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{path} must be a list")
    return list(value)


def _optional_path(value: Any, path: str) -> str | None:
    if value is None or value == "":
        return None
    return _string(value, path)


def _ray_backend_args(value: Any) -> dict[str, Any]:
    raw = _mapping(value, "backend.args.ray")
    _shape(raw, "backend.args.ray", {"launch", "proxy"})
    result: dict[str, Any] = {}
    if "launch" in raw:
        launch = _mapping(raw["launch"], "backend.args.ray.launch")
        _shape(
            launch,
            "backend.args.ray.launch",
            {
                "env_script",
                "ray_node_cpus",
                "ray_head_port",
                "null_compute",
                "null_compute_latency_s",
                "instrumentation",
                "clean_stage",
            },
        )
        validators = {
            "env_script": _string,
            "ray_node_cpus": _integer,
            "ray_head_port": _integer,
            "null_compute": _boolean,
            "null_compute_latency_s": _number,
            "instrumentation": _boolean,
            "clean_stage": _boolean,
        }
        result["launch"] = {
            key: validators[key](item, f"backend.args.ray.launch.{key}")
            for key, item in launch.items()
        }
    if "proxy" in raw:
        proxy = _mapping(raw["proxy"], "backend.args.ray.proxy")
        _shape(
            proxy,
            "backend.args.ray.proxy",
            {
                "type",
                "port",
                "backend_port",
                "executable_ref",
                "python_path",
                "num_workers",
                "options",
            },
        )
        proxy_result: dict[str, Any] = {}
        text_fields = {"type", "executable_ref", "python_path"}
        integer_fields = {"port", "backend_port", "num_workers"}
        for key, item in proxy.items():
            path = f"backend.args.ray.proxy.{key}"
            if key in text_fields:
                proxy_result[key] = _string(item, path)
            elif key in integer_fields:
                proxy_result[key] = _integer(item, path)
            else:
                proxy_result[key] = _mapping(item, path)
        result["proxy"] = proxy_result
    return result


def _model_from_dict(data: Any, index: int) -> ModelSpec:
    path = f"deployment.models[{index}]"
    raw = _mapping(data, path)
    _shape(
        raw,
        path,
        {
            "model_id",
            "tensor_parallel_size",
            "max_model_len",
            "size",
            "pipeline_parallel_size",
            "num_replicas",
            "num_cpus_per_replica",
            "gpu_memory_utilization",
            "enforce_eager",
            "enable_log_requests",
            "max_num_seqs",
        },
        {"model_id"},
    )
    num_replicas = raw.get("num_replicas")
    max_num_seqs = raw.get("max_num_seqs")
    return ModelSpec(
        model_id=_string(raw["model_id"], f"{path}.model_id"),
        tensor_parallel_size=_integer(
            raw.get("tensor_parallel_size", 1), f"{path}.tensor_parallel_size"
        ),
        max_model_len=_integer(raw.get("max_model_len", 4096), f"{path}.max_model_len"),
        size=_integer(raw.get("size", 8), f"{path}.size"),
        pipeline_parallel_size=_integer(
            raw.get("pipeline_parallel_size", 1), f"{path}.pipeline_parallel_size"
        ),
        num_replicas=(
            None if num_replicas is None else _integer(num_replicas, f"{path}.num_replicas")
        ),
        num_cpus_per_replica=_integer(
            raw.get("num_cpus_per_replica", 4), f"{path}.num_cpus_per_replica"
        ),
        gpu_memory_utilization=_number(
            raw.get("gpu_memory_utilization", 0.90), f"{path}.gpu_memory_utilization"
        ),
        enforce_eager=_boolean(raw.get("enforce_eager", True), f"{path}.enforce_eager"),
        enable_log_requests=_boolean(
            raw.get("enable_log_requests", True), f"{path}.enable_log_requests"
        ),
        max_num_seqs=(
            None if max_num_seqs is None else _integer(max_num_seqs, f"{path}.max_num_seqs")
        ),
    )


def _saturation_from_dict(value: Any) -> SaturationSpec:
    return parse_saturation_spec(value, path="client.saturation")


def _matrix_from_dict(data: dict[str, Any]) -> MatrixSpec:
    data = _mapping(data, "matrix")
    _shape(data, "matrix", {"axes", "name_template", "derived", "combine"})
    axes = []
    for index, raw_axis_value in enumerate(_list(data.get("axes", []), "matrix.axes")):
        raw_axis = _mapping(raw_axis_value, f"matrix.axes[{index}]")
        _shape(
            raw_axis,
            f"matrix.axes[{index}]",
            {"name", "values", "targets", "label_template"},
            {"name", "values", "targets"},
        )
        axes.append(
            MatrixAxis(
                name=_string(raw_axis["name"], f"matrix.axes[{index}].name"),
                values=_list(raw_axis["values"], f"matrix.axes[{index}].values"),
                targets=[
                    _string(item, f"matrix.axes[{index}].targets[{item_index}]")
                    for item_index, item in enumerate(
                        _list(raw_axis["targets"], f"matrix.axes[{index}].targets")
                    )
                ],
                label_template=_string(
                    raw_axis.get("label_template", "{value}"),
                    f"matrix.axes[{index}].label_template",
                ),
            )
        )
    derived = []
    for index, raw_value in enumerate(_list(data.get("derived", []), "matrix.derived")):
        raw_derived = _mapping(raw_value, f"matrix.derived[{index}]")
        _shape(raw_derived, f"matrix.derived[{index}]", {"path", "expr"}, {"path", "expr"})
        derived.append(
            DerivedField(
                path=_string(raw_derived["path"], f"matrix.derived[{index}].path"),
                expr=_string(raw_derived["expr"], f"matrix.derived[{index}].expr"),
            )
        )
    combine = _string(data.get("combine", "cartesian"), "matrix.combine")
    if combine not in ("cartesian", "zip"):
        raise ValueError(f"matrix.combine must be 'cartesian' or 'zip', got {combine!r}")
    return MatrixSpec(
        axes=axes,
        name_template=_string(data.get("name_template", ""), "matrix.name_template"),
        derived=derived,
        combine=combine,
    )


def load_experiment_spec(path: str) -> ExperimentSpec:
    raw = load_yaml_file(path)
    _shape(
        raw,
        "spec",
        {
            "name",
            "matrix",
            "trace",
            "workload",
            "deployment",
            "client",
            "backend",
            "scheduler",
        },
        {"name", "trace", "workload", "deployment"},
    )
    base_dir = os.path.dirname(os.path.abspath(path))
    matrix = _matrix_from_dict(raw.get("matrix", {}))

    trace_raw = _mapping(raw.get("trace", {}), "trace")
    workload_raw = _mapping(raw.get("workload", {}), "workload")
    deployment_raw = _mapping(raw.get("deployment", {}), "deployment")
    client_raw = _mapping(raw.get("client", {}), "client")
    backend_raw = _mapping(raw.get("backend", {}), "backend")
    scheduler_raw = _mapping(raw.get("scheduler", {}), "scheduler")
    _shape(
        trace_raw,
        "trace",
        {
            "kind",
            "input_prompt_path",
            "input_trace_path",
            "tokenizer_builder",
        },
        {"kind"},
    )
    _shape(
        workload_raw,
        "workload",
        {
            "duration",
            "input_len",
            "output_len",
            "rate_per_node",
            "speedup",
            "sampling_strategy",
            "generation_mode",
            "arrival",
            "seed",
            "modes",
        },
        {"duration"},
    )
    _shape(
        deployment_raw,
        "deployment",
        {
            "num_nodes",
            "models",
            "model_storage_path",
            "local_stage_path",
            "replica_max_ongoing_requests",
            "num_gpus_per_node",
            "collect_stats",
            "control",
            "readiness",
            "validation_mode",
            "engine",
        },
        {"models"},
    )
    _shape(
        client_raw,
        "client",
        {
            "num_runs",
            "include_tp",
            "early_stop",
            "dest",
            "num_nodes",
            "num_go_procs",
            "num_go_workers",
            "go_concurrency",
            "warmup_rps",
            "warmup_duration_s",
            "sum_only",
            "stream",
            "startup_only",
            "direct_dispatch",
            "direct_pair_shift",
            "request_timeout_s",
            "drain_wait_timeout_s",
            "shard_timeout_s",
            "direct_target_ready_timeout_s",
            "direct_target_probe_timeout_s",
            "direct_target_interval_s",
            "direct_target_max_workers",
            "dispatch_topologies",
            "saturation",
        },
    )
    _shape(backend_raw, "backend", {"default", "args"})
    _shape(
        scheduler_raw,
        "scheduler",
        {
            "type",
            "nodes",
            "queue",
            "walltime",
            "project",
            "filesystems",
            "keep_output",
            "mail_user",
            "mail_events",
        },
    )
    prompt_raw = _optional_path(trace_raw.get("input_prompt_path"), "trace.input_prompt_path")
    trace_input_raw = _optional_path(trace_raw.get("input_trace_path"), "trace.input_trace_path")
    trace = TraceSpec(
        kind=_string(trace_raw["kind"], "trace.kind"),
        input_prompt_path=resolve_path(
            prompt_raw or get_site_config().input_prompt_path,
            base_dir=base_dir,
        )
        or "",
        input_trace_path=resolve_path(
            trace_input_raw or get_site_config().input_trace_path,
            base_dir=base_dir,
        )
        or "",
        tokenizer_builder=_string(
            trace_raw.get("tokenizer_builder", ""), "trace.tokenizer_builder"
        ),
    )
    modes_raw = _mapping(workload_raw.get("modes", {"chat": 1, "completion": 0}), "workload.modes")
    workload = WorkloadSpec(
        duration=_number(workload_raw["duration"], "workload.duration"),
        input_len=_integer(workload_raw.get("input_len", 2048), "workload.input_len"),
        output_len=_integer(workload_raw.get("output_len", 512), "workload.output_len"),
        rate_per_node=_number(workload_raw.get("rate_per_node", 80.0), "workload.rate_per_node"),
        speedup=_number(workload_raw.get("speedup", 1.0), "workload.speedup"),
        sampling_strategy=_string(
            workload_raw.get("sampling_strategy", "peak"), "workload.sampling_strategy"
        ),
        generation_mode=_string(
            workload_raw.get("generation_mode", "deterministic"), "workload.generation_mode"
        ),
        arrival=_string(workload_raw.get("arrival", "fixed"), "workload.arrival"),
        seed=_integer(workload_raw.get("seed", 42), "workload.seed"),
        modes={key: _integer(value, f"workload.modes.{key}") for key, value in modes_raw.items()},
    )
    scheduler_nodes = _integer(scheduler_raw.get("nodes", 1), "scheduler.nodes")
    models_raw = _list(deployment_raw.get("models", []), "deployment.models")
    model_storage_raw = _optional_path(
        deployment_raw.get("model_storage_path"), "deployment.model_storage_path"
    )
    local_stage_raw = _optional_path(
        deployment_raw.get("local_stage_path"), "deployment.local_stage_path"
    )
    deployment = DeploymentSpec(
        num_nodes=_integer(
            deployment_raw.get("num_nodes", scheduler_nodes), "deployment.num_nodes"
        ),
        models=[_model_from_dict(item, index) for index, item in enumerate(models_raw)],
        model_storage_path=resolve_path(
            model_storage_raw or get_site_config().model_storage_path,
            base_dir=base_dir,
        )
        or get_site_config().model_storage_path,
        local_stage_path=resolve_path(
            local_stage_raw or get_site_config().local_stage_path,
            base_dir=base_dir,
        )
        or get_site_config().local_stage_path,
        replica_max_ongoing_requests=_integer(
            deployment_raw.get("replica_max_ongoing_requests", 64),
            "deployment.replica_max_ongoing_requests",
        ),
        num_gpus_per_node=_integer(
            deployment_raw.get("num_gpus_per_node", get_site_config().num_gpus_per_node),
            "deployment.num_gpus_per_node",
        ),
        collect_stats=_boolean(
            deployment_raw.get("collect_stats", False), "deployment.collect_stats"
        ),
        control=_mapping(deployment_raw.get("control", {}), "deployment.control"),
        readiness=_mapping(deployment_raw.get("readiness", {}), "deployment.readiness"),
        validation_mode=_boolean(
            deployment_raw.get("validation_mode", False), "deployment.validation_mode"
        ),
        engine=_string(deployment_raw.get("engine", "vllm"), "deployment.engine").lower(),
    )
    dispatch_raw = _list(client_raw.get("dispatch_topologies", []), "client.dispatch_topologies")
    client = ClientSpec(
        num_runs=_integer(client_raw.get("num_runs", 1), "client.num_runs"),
        include_tp=_boolean(client_raw.get("include_tp", False), "client.include_tp"),
        early_stop=_number(client_raw.get("early_stop", 0.0), "client.early_stop"),
        dest=_string(client_raw.get("dest", "proxy"), "client.dest"),
        num_nodes=_integer(client_raw.get("num_nodes", deployment.num_nodes), "client.num_nodes"),
        num_go_procs=_integer(client_raw.get("num_go_procs", 1), "client.num_go_procs"),
        num_go_workers=_integer(client_raw.get("num_go_workers", 4), "client.num_go_workers"),
        go_concurrency=_integer(client_raw.get("go_concurrency", 0), "client.go_concurrency"),
        warmup_rps=_integer(client_raw.get("warmup_rps", 0), "client.warmup_rps"),
        warmup_duration_s=_number(
            client_raw.get("warmup_duration_s", 0.0), "client.warmup_duration_s"
        ),
        sum_only=_boolean(client_raw.get("sum_only", False), "client.sum_only"),
        stream=_boolean(client_raw.get("stream", False), "client.stream"),
        startup_only=_boolean(client_raw.get("startup_only", False), "client.startup_only"),
        direct_dispatch=_string(
            client_raw.get("direct_dispatch", "local"), "client.direct_dispatch"
        ),
        direct_pair_shift=_integer(
            client_raw.get("direct_pair_shift", 1), "client.direct_pair_shift"
        ),
        request_timeout_s=_number(
            client_raw.get("request_timeout_s", 3600.0), "client.request_timeout_s"
        ),
        drain_wait_timeout_s=_number(
            client_raw.get("drain_wait_timeout_s", 3780.0), "client.drain_wait_timeout_s"
        ),
        shard_timeout_s=_number(client_raw.get("shard_timeout_s", 600.0), "client.shard_timeout_s"),
        direct_target_ready_timeout_s=_number(
            client_raw.get("direct_target_ready_timeout_s", 300.0),
            "client.direct_target_ready_timeout_s",
        ),
        direct_target_probe_timeout_s=_number(
            client_raw.get("direct_target_probe_timeout_s", 2.0),
            "client.direct_target_probe_timeout_s",
        ),
        direct_target_interval_s=_number(
            client_raw.get("direct_target_interval_s", 5.0),
            "client.direct_target_interval_s",
        ),
        direct_target_max_workers=_integer(
            client_raw.get("direct_target_max_workers", 64),
            "client.direct_target_max_workers",
        ),
        dispatch_topologies=[
            _string(item, f"client.dispatch_topologies[{index}]")
            for index, item in enumerate(dispatch_raw)
        ],
        saturation=_saturation_from_dict(client_raw.get("saturation", {})),
    )
    backend_args_raw = _mapping(backend_raw.get("args", {}), "backend.args")
    _shape(backend_args_raw, "backend.args", SUPPORTED_BACKENDS)
    parsed_backend_args = {
        key: (_ray_backend_args(value) if key == "ray" else _mapping(value, f"backend.args.{key}"))
        for key, value in backend_args_raw.items()
    }
    if parsed_backend_args.get("mock"):
        raise ValueError("backend.args.mock does not accept options")
    backend = BackendSpec(
        default=_string(backend_raw.get("default", "ray"), "backend.default"),
        args=parsed_backend_args,
    )
    scheduler = SchedulerSpec(
        type=_string(scheduler_raw.get("type", "pbs"), "scheduler.type"),
        nodes=_integer(scheduler_raw.get("nodes", deployment.num_nodes), "scheduler.nodes"),
        queue=_string(scheduler_raw.get("queue", ""), "scheduler.queue"),
        walltime=_string(scheduler_raw.get("walltime", ""), "scheduler.walltime"),
        project=_string(scheduler_raw.get("project", "AuroraGPT"), "scheduler.project"),
        filesystems=_string(
            scheduler_raw.get("filesystems", "home:flare"), "scheduler.filesystems"
        ),
        keep_output=_string(scheduler_raw.get("keep_output", "doe"), "scheduler.keep_output"),
        mail_user=_string(
            scheduler_raw.get("mail_user", get_site_config().pbs_mail_user), "scheduler.mail_user"
        ),
        mail_events=_string(scheduler_raw.get("mail_events", "bae"), "scheduler.mail_events"),
    )
    spec = ExperimentSpec(
        name=_string(raw["name"], "spec.name"),
        matrix=matrix,
        trace=trace,
        workload=workload,
        deployment=deployment,
        client=client,
        backend=backend,
        scheduler=scheduler,
        spec_path=os.path.abspath(path),
    )
    validate_experiment_spec(spec)
    return normalize_experiment_spec(spec)


# KI-C5: a bounded default for proxy runs (see normalize()).
DEFAULT_PROXY_CLIENT_NODES = 4


def normalize_experiment_spec(spec: ExperimentSpec) -> ExperimentSpec:
    normalized = replace(spec)
    normalized.deployment = replace(
        spec.deployment,
        control=dict(spec.deployment.control),
        readiness=dict(spec.deployment.readiness),
    )
    normalized.client = replace(spec.client)
    normalized.scheduler = replace(spec.scheduler)
    normalized.trace = replace(spec.trace)
    normalized.workload = replace(spec.workload)
    normalized.backend = replace(
        spec.backend, args={key: dict(value) for key, value in spec.backend.args.items()}
    )
    if normalized.client.num_nodes < 1:
        # KI-C5: this default is only right for dest=direct, where one client
        # rank per node is exactly what saturates the fleet. For dest=proxy it
        # points EVERY rank at a single front proxy -- at 256 nodes that is 256
        # client ranks on one process, which was misread as a proxy throughput
        # regression until the harness was examined. Proxy runs get a bounded
        # default and say so; set client.num_nodes explicitly to override.
        if normalized.client.dest == "proxy":
            normalized.client.num_nodes = min(
                normalized.deployment.num_nodes, DEFAULT_PROXY_CLIENT_NODES
            )
            if normalized.deployment.num_nodes > DEFAULT_PROXY_CLIENT_NODES:
                print(
                    f"[spec] client.num_nodes defaulted to "
                    f"{normalized.client.num_nodes} for dest=proxy "
                    f"(deployment.num_nodes={normalized.deployment.num_nodes}); "
                    "one rank per node would aim the whole fleet at a single "
                    "proxy. Set client.num_nodes explicitly to override.",
                    flush=True,
                )
        else:
            normalized.client.num_nodes = normalized.deployment.num_nodes
    if normalized.scheduler.nodes < 1:
        normalized.scheduler.nodes = normalized.deployment.num_nodes
    if not normalized.trace.input_prompt_path:
        normalized.trace.input_prompt_path = get_site_config().input_prompt_path
    if normalized.trace.kind == "azure_trace" and not normalized.trace.input_trace_path:
        normalized.trace.input_trace_path = get_site_config().input_trace_path
    return normalized


def validate_experiment_spec(spec: ExperimentSpec) -> None:
    if not spec.name:
        raise ValueError("spec.name is required")
    if spec.trace.kind not in SUPPORTED_TRACE_KINDS:
        raise ValueError(
            f"Unsupported trace.kind '{spec.trace.kind}'. Expected one of {sorted(SUPPORTED_TRACE_KINDS)}"
        )
    if spec.workload.duration <= 0:
        raise ValueError("workload.duration must be > 0")
    if spec.workload.input_len < 1 or spec.workload.output_len < 1:
        raise ValueError("workload.input_len and workload.output_len must be >= 1")
    if spec.workload.rate_per_node < 0:
        raise ValueError("workload.rate_per_node must be >= 0")
    if spec.workload.speedup <= 0:
        raise ValueError("workload.speedup must be > 0")
    if (
        not spec.workload.modes
        or not set(spec.workload.modes) <= {"chat", "completion"}
        or any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in spec.workload.modes.values()
        )
        or sum(spec.workload.modes.values()) < 1
    ):
        raise ValueError(
            "workload.modes must contain only chat/completion non-negative integer weights "
            "and one positive weight"
        )
    if spec.deployment.num_nodes < 1:
        raise ValueError("deployment.num_nodes must be >= 1")
    if spec.scheduler.nodes < 1:
        raise ValueError("scheduler.nodes must be >= 1")
    # PR-020: enum + bound checks that were previously missing (a bad value
    # here surfaced only much later, or silently).
    if spec.scheduler.type not in {"pbs", "slurm", "psij"}:
        raise ValueError(f"scheduler.type must be pbs/slurm/psij, got {spec.scheduler.type!r}")
    engine = getattr(spec.deployment, "engine", "vllm")
    if engine not in {"vllm", "sglang"}:
        raise ValueError(f"deployment.engine must be vllm/sglang, got {engine!r}")
    arrival = getattr(spec.workload, "arrival", "fixed")
    if arrival not in {"fixed", "poisson"}:
        raise ValueError(f"workload.arrival must be fixed/poisson, got {arrival!r}")
    if getattr(spec.client, "go_concurrency", 0) < 0:
        raise ValueError("client.go_concurrency must be >= 0 (zero selects auto-sizing)")
    if getattr(spec.client, "num_go_procs", 1) < 1:
        raise ValueError("client.num_go_procs must be >= 1")
    if spec.client.num_runs < 1 or spec.client.num_go_workers < 1:
        raise ValueError("client.num_runs and client.num_go_workers must be >= 1")
    if spec.client.warmup_rps < 0 or spec.client.warmup_duration_s < 0:
        raise ValueError("client warmup values must be non-negative")
    if spec.client.direct_pair_shift < 1 or spec.client.direct_target_max_workers < 1:
        raise ValueError("client direct_pair_shift/max_workers must be positive")
    for name in (
        "request_timeout_s",
        "drain_wait_timeout_s",
        "shard_timeout_s",
        "direct_target_ready_timeout_s",
        "direct_target_probe_timeout_s",
        "direct_target_interval_s",
    ):
        if getattr(spec.client, name) <= 0:
            raise ValueError(f"client.{name} must be positive")
    if spec.client.dest not in {"proxy", "direct"}:
        raise ValueError("client.dest must be 'proxy' or 'direct'")
    if spec.client.early_stop < 0.0 or spec.client.early_stop > 1.0:
        raise ValueError("client.early_stop must be between 0.0 and 1.0")
    saturation = spec.client.saturation
    validate_saturation_spec(saturation, path="client.saturation")
    matrix_controlled_fields = {target for axis in spec.matrix.axes for target in axis.targets} | {
        derived.path for derived in spec.matrix.derived
    }
    if saturation.enabled:
        if len(spec.deployment.models) != 1:
            raise ValueError("client.saturation requires exactly one deployed model")
        if spec.client.num_go_procs != 1:
            raise ValueError(
                "eval client.saturation currently supports exactly one Go process; "
                "use ClientLab for multi-process saturation"
            )
        if ("client.num_nodes" not in matrix_controlled_fields and spec.client.num_nodes != 1) or (
            "client.num_runs" not in matrix_controlled_fields and spec.client.num_runs != 1
        ):
            raise ValueError(
                "eval client.saturation requires num_nodes=1 and num_runs=1; "
                "use ClientLab for distributed/repeated saturation"
            )
        if spec.client.dest == "direct" and spec.deployment.num_nodes != 1:
            raise ValueError("eval direct saturation supports exactly one deployment node")
    if spec.client.direct_dispatch not in {"local", "mesh", "paired"}:
        raise ValueError(
            f"client.direct_dispatch must be local/mesh/paired, got {spec.client.direct_dispatch!r}"
        )
    if spec.client.dispatch_topologies:
        if len(set(spec.client.dispatch_topologies)) != len(spec.client.dispatch_topologies):
            raise ValueError("client.dispatch_topologies must not contain duplicate arms")
        unknown = set(spec.client.dispatch_topologies) - {"mesh", "local", "paired"}
        if unknown:
            raise ValueError(
                f"client.dispatch_topologies: unknown arm(s) {sorted(unknown)}; "
                "allowed: mesh, local, paired"
            )
        if spec.client.dest != "direct":
            raise ValueError("client.dispatch_topologies requires client.dest='direct'")
    # local/paired pin each rank to one node, so a rank per node is required or
    # the un-targeted nodes sit idle and the offered rate per node is wrong.
    pinned = spec.client.dispatch_topologies or [spec.client.direct_dispatch]
    if spec.client.dest == "proxy" and spec.client.direct_dispatch != "local":
        raise ValueError("client.direct_dispatch must be local when client.dest='proxy'")
    if (
        spec.client.dest == "direct"
        and any(arm in {"local", "paired"} for arm in pinned)
        and spec.client.num_nodes != spec.deployment.num_nodes
    ):
        raise ValueError(
            f"client.dest=direct with direct_dispatch={pinned} needs one client rank "
            f"per node (client.num_nodes={spec.client.num_nodes}, "
            f"deployment.num_nodes={spec.deployment.num_nodes})"
        )
    if not spec.deployment.models:
        raise ValueError("deployment.models must contain at least one model")
    for model in spec.deployment.models:
        if model.tensor_parallel_size < 1:
            raise ValueError(f"{model.model_id}: tensor_parallel_size must be >= 1")
        if model.pipeline_parallel_size < 1:
            raise ValueError(f"{model.model_id}: pipeline_parallel_size must be >= 1")
        if model.max_model_len < 1:
            raise ValueError(f"{model.model_id}: max_model_len must be >= 1")
        if model.size < 1:
            raise ValueError(f"{model.model_id}: size must be >= 1")
        if model.num_replicas is not None and model.num_replicas < 1:
            raise ValueError(f"{model.model_id}: num_replicas must be >= 1")
        if not 0 < model.gpu_memory_utilization <= 1:
            raise ValueError(f"{model.model_id}: gpu_memory_utilization must be in (0, 1]")
    if spec.backend.default not in SUPPORTED_BACKENDS:
        raise ValueError(
            f"Unsupported backend.default '{spec.backend.default}'. Expected one of {sorted(SUPPORTED_BACKENDS)}"
        )
    for axis in spec.matrix.axes:
        if not axis.name:
            raise ValueError("matrix axis name is required")
        if not axis.values:
            raise ValueError(f"matrix axis '{axis.name}' must have at least one value")
        if not axis.targets:
            raise ValueError(f"matrix axis '{axis.name}' must target at least one field")
    axis_names = {axis.name for axis in spec.matrix.axes}
    if len(axis_names) != len(spec.matrix.axes):
        raise ValueError("matrix axis names must be unique")
    for axis in spec.matrix.axes:
        for target in axis.targets:
            try:
                dotted_get(spec, target)
            except KeyError as exc:
                raise ValueError(
                    f"matrix axis {axis.name!r} targets unknown field {target!r}"
                ) from exc
    sample_axis_values = {axis.name: axis.values[0] for axis in spec.matrix.axes}
    for derived in spec.matrix.derived:
        if not derived.path:
            raise ValueError("matrix derived field must specify a path")
        if not derived.expr:
            raise ValueError(f"matrix derived field '{derived.path}' must specify an expr")
        try:
            dotted_get(spec, derived.path)
        except KeyError as exc:
            raise ValueError(f"matrix derived field targets unknown path {derived.path!r}") from exc
        from .matrix import _eval_derived

        try:
            _eval_derived(derived.expr, sample_axis_values)
        except Exception as exc:
            raise ValueError(
                f"matrix derived field {derived.path!r} has invalid expression: {exc}"
            ) from exc


def save_run_snapshot(path: str, spec: ExperimentSpec) -> None:
    dump_yaml_file(path, spec)
