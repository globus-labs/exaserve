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
from dataclasses import replace
from typing import Any

from site_config import get_site_config

from .models import (
    BackendSpec,
    ClientSpec,
    DeploymentSpec,
    ExperimentSpec,
    MatrixAxis,
    MatrixSpec,
    ModelSpec,
    SchedulerSpec,
    TraceSpec,
    WorkloadSpec,
)
from .utils import dump_yaml_file, load_yaml_file, resolve_path


SUPPORTED_TRACE_KINDS = {"weak_scaling", "azure_trace"}
SUPPORTED_BACKENDS = {"ray", "mock"}




def _matrix_from_dict(data: dict[str, Any]) -> MatrixSpec:
    axes = []
    for raw_axis in data.get("axes", []):
        axes.append(
            MatrixAxis(
                name=str(raw_axis["name"]),
                values=list(raw_axis.get("values", [])),
                targets=[str(item) for item in raw_axis.get("targets", [])],
                label_template=str(raw_axis.get("label_template", "{value}")),
            )
        )
    return MatrixSpec(
        axes=axes,
        name_template=str(data.get("name_template", "")),
    )


def load_experiment_spec(path: str) -> ExperimentSpec:
    raw = load_yaml_file(path)
    base_dir = os.path.dirname(os.path.abspath(path))
    matrix = _matrix_from_dict(raw.get("matrix", {}))

    trace_raw = raw.get("trace", {})
    workload_raw = raw.get("workload", {})
    deployment_raw = raw.get("deployment", {})
    client_raw = raw.get("client", {})
    backend_raw = raw.get("backend", {})
    scheduler_raw = raw.get("scheduler", {})

    trace = TraceSpec(
        kind=str(trace_raw["kind"]),
        input_prompt_path=resolve_path(
            trace_raw.get("input_prompt_path") or get_site_config().input_prompt_path,
            base_dir=base_dir,
        )
        or "",
        input_trace_path=resolve_path(
            trace_raw.get("input_trace_path") or get_site_config().input_trace_path,
            base_dir=base_dir,
        )
        or "",
    )
    workload = WorkloadSpec(
        duration=float(workload_raw["duration"]),
        input_len=int(workload_raw.get("input_len", 2048)),
        output_len=int(workload_raw.get("output_len", 512)),
        rate_per_node=float(workload_raw.get("rate_per_node", 80.0)),
        speedup=float(workload_raw.get("speedup", 1.0)),
        sampling_strategy=str(workload_raw.get("sampling_strategy", "peak")),
        generation_mode=str(workload_raw.get("generation_mode", "deterministic")),
        seed=int(workload_raw.get("seed", 42)),
        modes={
            str(key): int(value)
            for key, value in dict(workload_raw.get("modes", {"chat": 1, "completion": 0})).items()
        },
    )
    deployment = DeploymentSpec(
        num_nodes=int(deployment_raw.get("num_nodes", scheduler_raw.get("nodes", 1))),
        models=[ModelSpec.from_dict(item) for item in deployment_raw.get("models", [])],
        model_storage_path=resolve_path(
            deployment_raw.get("model_storage_path") or get_site_config().model_storage_path,
            base_dir=base_dir,
        )
        or get_site_config().model_storage_path,
        local_stage_path=resolve_path(
            deployment_raw.get("local_stage_path") or get_site_config().local_stage_path,
            base_dir=base_dir,
        )
        or get_site_config().local_stage_path,
        worker_max_ongoing=int(deployment_raw.get("worker_max_ongoing", 64)),
        num_gpus_per_node=int(
            deployment_raw.get("num_gpus_per_node", get_site_config().num_gpus_per_node)
        ),
    )
    client = ClientSpec(
        num_runs=int(client_raw.get("num_runs", 1)),
        include_tp=bool(client_raw.get("include_tp", False)),
        early_stop=float(client_raw.get("early_stop", 0.0)),
        dest=str(client_raw.get("dest", "proxy")),
        num_nodes=int(client_raw.get("num_nodes", deployment.num_nodes)),
        num_go_procs=int(client_raw.get("num_go_procs", 1)),
        num_go_workers=int(client_raw.get("num_go_workers", 4)),
        go_concurrency=int(client_raw.get("go_concurrency", 2000)),
        warmup_rps=int(client_raw.get("warmup_rps", 0)),
        warmup_duration_s=float(client_raw.get("warmup_duration_s", 0.0)),
        sum_only=bool(client_raw.get("sum_only", False)),
    )
    backend = BackendSpec(
        default=str(backend_raw.get("default", "ray")),
        args={str(key): dict(value or {}) for key, value in dict(backend_raw.get("args", {})).items()},
    )
    scheduler = SchedulerSpec(
        type=str(scheduler_raw.get("type", "pbs")),
        nodes=int(scheduler_raw.get("nodes", deployment.num_nodes)),
        queue=str(scheduler_raw.get("queue", "")),
        walltime=str(scheduler_raw.get("walltime", "")),
        project=str(scheduler_raw.get("project", "AuroraGPT")),
        filesystems=str(scheduler_raw.get("filesystems", "home:flare")),
        keep_output=str(scheduler_raw.get("keep_output", "doe")),
        mail_user=str(scheduler_raw.get("mail_user", get_site_config().pbs_mail_user)),
        mail_events=str(scheduler_raw.get("mail_events", "bae")),
    )
    spec = ExperimentSpec(
        name=str(raw["name"]),
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


def normalize_experiment_spec(spec: ExperimentSpec) -> ExperimentSpec:
    normalized = replace(spec)
    normalized.deployment = replace(spec.deployment)
    normalized.client = replace(spec.client)
    normalized.scheduler = replace(spec.scheduler)
    normalized.trace = replace(spec.trace)
    normalized.workload = replace(spec.workload)
    normalized.backend = replace(spec.backend, args={key: dict(value) for key, value in spec.backend.args.items()})
    if normalized.client.num_nodes < 1:
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
    if spec.deployment.num_nodes < 1:
        raise ValueError("deployment.num_nodes must be >= 1")
    if spec.scheduler.nodes < 1:
        raise ValueError("scheduler.nodes must be >= 1")
    if spec.client.dest not in {"proxy", "direct"}:
        raise ValueError("client.dest must be 'proxy' or 'direct'")
    if spec.client.early_stop < 0.0 or spec.client.early_stop > 1.0:
        raise ValueError("client.early_stop must be between 0.0 and 1.0")
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


def save_run_snapshot(path: str, spec: ExperimentSpec) -> None:
    dump_yaml_file(path, spec)
