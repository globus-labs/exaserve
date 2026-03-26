"""Data models for the eval control plane.

All configuration flows through plain dataclasses defined here, with no
behavior — just data shapes. This keeps the data layer decoupled from I/O,
validation, and execution logic in sibling modules.

Hierarchy (from spec YAML to execution):

  ExperimentSpec            <- parsed from a spec YAML (via spec_io.py)
    ├── MatrixSpec          <- defines the combinatorial sweep (expand via matrix.py)
    ├── TraceSpec           <- what kind of trace to generate
    ├── WorkloadSpec        <- request shape: duration, token lengths, rate
    ├── DeploymentSpec      <- cluster layout: nodes, models, storage paths
    │     └── ModelSpec[]   <- per-model TP/PP/replica config
    ├── ClientSpec          <- replay client tuning: Go procs, concurrency
    ├── BackendSpec         <- which backend adapter to use + per-backend args
    └── SchedulerSpec       <- PBS queue/walltime/project settings

  VariantSpec               <- one concrete point in the matrix sweep

  TraceArtifact             <- content-addressed trace file on disk
  RunBundle                 <- directory layout for a materialized run
  RunPlan                   <- full snapshot written to run.yaml (self-contained)
  ReplayRequest             <- single request row loaded by replay_client.py
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class MatrixAxis:
    name: str
    values: list[Any]
    targets: list[str]
    label_template: str = "{value}"


@dataclass
class MatrixSpec:
    axes: list[MatrixAxis] = field(default_factory=list)
    name_template: str = ""


@dataclass
class ModelSpec:
    model_id: str
    tensor_parallel_size: int
    max_model_len: int
    size: int
    pipeline_parallel_size: int = 1
    num_replicas: int | None = None
    num_cpus_per_replica: int = 4
    gpu_memory_utilization: float = 0.90
    enforce_eager: bool = True
    enable_log_requests: bool = True

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ModelSpec":
        return cls(
            model_id=str(data["model_id"]),
            tensor_parallel_size=int(data.get("tensor_parallel_size", 1)),
            max_model_len=int(data.get("max_model_len", 4096)),
            size=int(data.get("size", 8)),
            pipeline_parallel_size=int(data.get("pipeline_parallel_size", 1)),
            num_replicas=(
                None if data.get("num_replicas") is None else int(data["num_replicas"])
            ),
            num_cpus_per_replica=int(data.get("num_cpus_per_replica", 4)),
            gpu_memory_utilization=float(data.get("gpu_memory_utilization", 0.90)),
            enforce_eager=bool(data.get("enforce_eager", True)),
            enable_log_requests=bool(data.get("enable_log_requests", True)),
        )


@dataclass
class TraceSpec:
    kind: str
    input_prompt_path: str = ""
    input_trace_path: str = ""


@dataclass
class WorkloadSpec:
    duration: float
    input_len: int = 2048
    output_len: int = 512
    rate_per_node: float = 80.0
    speedup: float = 1.0
    sampling_strategy: str = "peak"
    generation_mode: str = "deterministic"
    seed: int = 42
    modes: dict[str, int] = field(default_factory=lambda: {"chat": 1, "completion": 0})


@dataclass
class DeploymentSpec:
    num_nodes: int
    models: list[ModelSpec]
    model_storage_path: str = ""
    local_stage_path: str = ""
    worker_max_ongoing: int = 64
    num_gpus_per_node: int = 0


@dataclass
class ClientSpec:
    num_runs: int = 1
    include_tp: bool = False
    early_stop: float = 0.0
    dest: str = "proxy"
    num_nodes: int = 1
    num_go_procs: int = 1
    num_go_workers: int = 4
    go_concurrency: int = 2000
    warmup_rps: int = 0
    warmup_duration_s: float = 0.0
    sum_only: bool = False


@dataclass
class BackendSpec:
    default: str = "ray"
    args: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass
class SchedulerSpec:
    type: str = "pbs"
    nodes: int = 1
    queue: str = ""
    walltime: str = ""
    project: str = "AuroraGPT"
    filesystems: str = "home:flare"
    keep_output: str = "doe"
    mail_user: str = ""
    mail_events: str = "bae"


@dataclass
class ExperimentSpec:
    name: str
    matrix: MatrixSpec
    trace: TraceSpec
    workload: WorkloadSpec
    deployment: DeploymentSpec
    client: ClientSpec
    backend: BackendSpec
    scheduler: SchedulerSpec
    spec_path: str = ""


@dataclass
class VariantSpec:
    spec: ExperimentSpec
    variant_name: str
    axis_values: dict[str, Any] = field(default_factory=dict)


@dataclass
class TraceArtifact:
    trace_id: str
    trace_path: str
    metadata_path: str
    spec_hash: str
    store_dir: str


@dataclass
class RunBundle:
    root_dir: str
    run_yaml_path: str
    job_path: str
    logs_dir: str
    pbs_stdout_dir: str
    pbs_stderr_dir: str
    results_dir: str
    state_dir: str
    state_path: str
    meta_dir: str
    spec_snapshot_path: str
    runtime_dir: str


# TODO: Consider slimming RunPlan to store only spec_snapshot_path + variant
# overrides instead of duplicating full DeploymentSpec/ClientSpec/etc. This would
# reduce run.yaml size and avoid data staleness, at the cost of requiring the
# spec snapshot to be resolved at load time.
@dataclass
class RunPlan:
    run_id: str
    created_at: str
    repo_root: str
    bundle: RunBundle
    spec_name: str
    variant_name: str
    axis_values: dict[str, Any]
    backend_name: str
    backend_args: dict[str, Any]
    trace_artifact: TraceArtifact
    runtime_manifest_path: str
    deployment: DeploymentSpec
    client: ClientSpec
    scheduler: SchedulerSpec
    workload: WorkloadSpec
    trace: TraceSpec
    spec_path: str = ""


@dataclass
class ReplayRequest:
    timestamp: float
    model: str
    prompt: str
    input_len: int
    output_len: int
    tensor_parallel_size: int
    req_id: str
    mode: str = "chat"
