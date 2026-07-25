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
class DerivedField:
    path: str
    expr: str


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
    derived: list[DerivedField] = field(default_factory=list)
    combine: str = "cartesian"  # "cartesian" (default) or "zip"


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
    max_num_seqs: int | None = None

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
            max_num_seqs=(
                None if data.get("max_num_seqs") is None else int(data["max_num_seqs"])
            ),
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
    arrival: str = "fixed"  # "fixed" (constant interval) or "poisson" (exponential)
    seed: int = 42
    modes: dict[str, int] = field(default_factory=lambda: {"chat": 1, "completion": 0})


@dataclass
class DeploymentSpec:
    num_nodes: int
    models: list[ModelSpec]
    model_storage_path: str = ""
    local_stage_path: str = ""
    replica_max_ongoing_requests: int = 64
    num_gpus_per_node: int = 0
    collect_stats: bool = False
    # Inference engine backend: "vllm" (default) or "sglang". Drop-in — the whole
    # pipeline (staging, Ray Serve, HAProxy, replay client, metrics) is identical;
    # only the per-replica engine differs. "sglang" routes the serving stack to a
    # venv that adds SGLang on the frameworks env and deploys SGLangWorker replicas.
    engine: str = "vllm"


@dataclass
class SaturationSpec:
    enabled: bool = False
    search_mode: str = "binary"
    initial_rate: int = 100
    max_rate: int = 0
    step_duration_s: float = 10.0
    warmup_duration_s: float = 3.0
    cooldown_pause_s: float = 2.0
    tolerance: float = 0.05
    max_error_rate: float = 0.01
    plateau_ratio: float = 0.95
    max_p99_ttft: float = 0.0
    verify: bool = True
    step_up_start: int = 0
    step_up_end: int = 0
    step_up_increment: int = 0
    stream: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SaturationSpec":
        if not data:
            return cls()
        return cls(
            enabled=bool(data.get("enabled", False)),
            search_mode=str(data.get("search_mode", "binary")),
            initial_rate=int(data.get("initial_rate", 100)),
            max_rate=int(data.get("max_rate", 0)),
            step_duration_s=float(data.get("step_duration_s", 10.0)),
            warmup_duration_s=float(data.get("warmup_duration_s", 3.0)),
            cooldown_pause_s=float(data.get("cooldown_pause_s", 2.0)),
            tolerance=float(data.get("tolerance", 0.05)),
            max_error_rate=float(data.get("max_error_rate", 0.01)),
            plateau_ratio=float(data.get("plateau_ratio", 0.95)),
            max_p99_ttft=float(data.get("max_p99_ttft", 0.0)),
            verify=bool(data.get("verify", True)),
            step_up_start=int(data.get("step_up_start", 0)),
            step_up_end=int(data.get("step_up_end", 0)),
            step_up_increment=int(data.get("step_up_increment", 0)),
            stream=bool(data.get("stream", False)),
        )


@dataclass
class ClientSpec:
    num_runs: int = 1
    include_tp: bool = False
    early_stop: float = 0.0
    dest: str = "proxy"
    num_nodes: int = 1
    num_go_procs: int = 1
    num_go_workers: int = 4
    go_concurrency: int = 0  # 0 = auto-derive from ephemeral port range in Go client
    warmup_rps: int = 0
    warmup_duration_s: float = 0.0
    sum_only: bool = False
    stream: bool = False
    startup_only: bool = False  # If True, exit after CLUSTER FULLY READY (skip replay)
    # dest=direct dispatch-topology ablation. Empty = the historical behaviour
    # (every client rank sprays across every node = "mesh"). When set, the
    # replay client is run once per entry against the SAME bring-up, and each
    # arm's results land in results/<topology>/. Arms:
    #   mesh   — rank hits all N nodes (hash-routed); ~1-1/N of traffic remote
    #   local  — rank hits only its own node (loopback, one peer)
    #   paired — rank hits exactly one *remote* node (permutation; one peer)
    # local vs paired separates "cross-node delivery" from "per-node peer fan-out".
    dispatch_topologies: list[str] = field(default_factory=list)
    saturation: SaturationSpec = field(default_factory=SaturationSpec)


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
    group_root_dir: str
    root_dir: str
    run_yaml_path: str
    job_path: str
    logs_dir: str
    pbs_stdout_dir: str
    pbs_stderr_dir: str
    results_dir: str
    state_dir: str
    state_path: str
    runtime_dir: str


@dataclass
class RunPlan:
    run_id: str
    run_group_id: str
    created_at: str
    repo_root: str
    snapshot_root: str
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
