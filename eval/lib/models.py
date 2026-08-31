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
  RunMaterialization        <- paths/provenance around canonical RunPlan artifact
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


@dataclass
class TraceSpec:
    kind: str
    input_prompt_path: str = ""
    input_trace_path: str = ""
    # Optional "module:function" dotted path returning {model_id: tokenizer}.
    # Empty selects the default Hugging Face loader. Crosses the trace
    # materialization process boundary via the pickled spec (hermetic tests
    # inject eval.testing:empty_tokenizer_map here).
    tokenizer_builder: str = ""


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
    # Optional canonical compiler overrides.  Eval owns only the YAML frontend;
    # the shared deployment compiler remains the schema and value authority.
    control: dict[str, Any] = field(default_factory=dict)
    readiness: dict[str, Any] = field(default_factory=dict)
    # Explicit qualification escape from the evidence-backed production
    # ceiling. This remains hash-bearing in the canonical DeploymentPlan and
    # cannot expand the candidate qualification target.
    validation_mode: bool = False
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
    startup_only: bool = False  # If True, exit after canonical READY (skip replay)
    # How a dest=direct rank picks its target node(s):
    #   local  — its OWN node only. This is what "direct" means: no routing
    #            layer, no cross-node hop. THE DEFAULT.
    #   mesh   — hash-route across all N nodes. This was the accidental
    #            historical behaviour (see doc/ and the direct_topology spec);
    #            it makes every node stream to N remote peers and is a
    #            *different experiment*, kept only for studying that effect.
    #   paired — exactly one remote node (permutation): one peer, still remote.
    direct_dispatch: str = "local"
    direct_pair_shift: int = 1
    request_timeout_s: float = 3600.0
    drain_wait_timeout_s: float = 3780.0
    shard_timeout_s: float = 600.0
    direct_target_ready_timeout_s: float = 300.0
    direct_target_probe_timeout_s: float = 2.0
    direct_target_interval_s: float = 5.0
    direct_target_max_workers: int = 64
    # Ablation: replay once per entry against the SAME bring-up, each arm's
    # results landing in results/<arm>/. Overrides direct_dispatch per arm.
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
class RunMaterialization:
    """Execution-location metadata around the shared semantic RunPlan.

    This is deliberately not another plan.  ``semantic_plan_path`` is the sole
    hash-bearing run contract; this object names materialized files, logs, and
    location references that do not alter deployment identity.  Semantic
    properties below are projections of the loaded canonical RunPlan and are
    intentionally omitted from ``run.yaml``.
    """

    run_id: str
    run_group_id: str
    created_at: str
    repo_root: str
    snapshot_root: str
    bundle: RunBundle
    spec_name: str
    variant_name: str
    axis_values: dict[str, Any]
    trace_artifact: TraceArtifact
    runtime_manifest_path: str
    semantic_plan_path: str
    deployment_plan_path: str
    site_profile_path: str
    run_semantic_hash: str
    deployment_plan_hash: str
    source_snapshot_hash: str
    deployment_id_scheme: str
    input_prompt_path: str = ""
    input_trace_path: str = ""
    spec_path: str = ""
    semantic_plan: Any = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.semantic_plan is None:
            raise ValueError("RunMaterialization requires its canonical RunPlan")
        if self.semantic_plan.run_semantic_hash != self.run_semantic_hash:
            raise ValueError("materialization hash disagrees with canonical RunPlan")

    @staticmethod
    def _thaw(value: Any) -> Any:
        if isinstance(value, tuple):
            if all(
                isinstance(item, tuple) and len(item) == 2 and isinstance(item[0], str)
                for item in value
            ):
                return {item[0]: RunMaterialization._thaw(item[1]) for item in value}
            return [RunMaterialization._thaw(item) for item in value]
        return value

    @property
    def backend_name(self) -> str:
        return self.semantic_plan.backend.name

    @property
    def backend_args(self) -> dict[str, Any]:
        return self._thaw(self.semantic_plan.backend.options)

    @property
    def deployment(self) -> DeploymentSpec:
        plan = self.semantic_plan.deployment
        return DeploymentSpec(
            num_nodes=plan.num_nodes,
            models=[
                ModelSpec(
                    model_id=model.model_id,
                    tensor_parallel_size=model.tensor_parallel_size,
                    max_model_len=model.max_model_len,
                    size=model.size_b,
                    pipeline_parallel_size=model.pipeline_parallel_size,
                    num_replicas=model.num_replicas,
                    num_cpus_per_replica=model.num_cpus_per_replica,
                    gpu_memory_utilization=model.gpu_memory_utilization,
                    enforce_eager=model.enforce_eager,
                    enable_log_requests=model.enable_log_requests,
                    max_num_seqs=model.max_num_seqs,
                )
                for model in plan.models
            ],
            model_storage_path=plan.model_storage_path,
            local_stage_path=plan.local_stage_path,
            replica_max_ongoing_requests=plan.replica_max_ongoing_requests,
            num_gpus_per_node=plan.num_gpus_per_node,
            collect_stats=plan.collect_stats,
            control={
                name: getattr(plan.control, name)
                for name in plan.control.__dataclass_fields__
                if name != "evidence_backed"
            },
            readiness={
                name: getattr(plan.readiness, name)
                for name in plan.readiness.__dataclass_fields__
                if name != "evidence_backed"
            },
            validation_mode=plan.validation_mode,
            engine=plan.engine,
        )

    @property
    def client(self) -> ClientSpec:
        policy = self.semantic_plan.client
        saturation = SaturationSpec(
            **{
                name: getattr(policy.saturation, name)
                for name in SaturationSpec.__dataclass_fields__
            }
        )
        return ClientSpec(
            num_runs=policy.num_runs,
            include_tp=policy.include_tp,
            early_stop=policy.early_stop,
            dest=policy.destination,
            num_nodes=policy.nodes,
            num_go_procs=policy.processes,
            num_go_workers=policy.workers,
            go_concurrency=policy.concurrency,
            warmup_rps=policy.warmup_rps,
            warmup_duration_s=policy.warmup_duration_s,
            sum_only=policy.sum_only,
            stream=policy.streaming,
            startup_only=policy.startup_only,
            direct_dispatch=policy.dispatch_topology,
            direct_pair_shift=policy.direct_pair_shift,
            request_timeout_s=policy.request_timeout_s,
            drain_wait_timeout_s=policy.drain_wait_timeout_s,
            shard_timeout_s=policy.shard_timeout_s,
            direct_target_ready_timeout_s=policy.direct_target_ready_timeout_s,
            direct_target_probe_timeout_s=policy.direct_target_probe_timeout_s,
            direct_target_interval_s=policy.direct_target_interval_s,
            direct_target_max_workers=policy.direct_target_max_workers,
            dispatch_topologies=list(policy.dispatch_topologies),
            saturation=saturation,
        )

    @property
    def scheduler(self) -> SchedulerSpec:
        policy = self._thaw(self.semantic_plan.scheduler.policy)
        return SchedulerSpec(
            type=self.semantic_plan.scheduler.type,
            nodes=self.semantic_plan.scheduler.nodes,
            queue=self.semantic_plan.scheduler.queue,
            walltime=self.semantic_plan.scheduler.walltime,
            project=self.semantic_plan.scheduler.account,
            filesystems=":".join(self.semantic_plan.scheduler.filesystem_refs),
            keep_output=str(policy.get("keep_output", "")),
            mail_user=str(policy.get("mail_user", "")),
            mail_events=str(policy.get("mail_events", "")),
        )

    @property
    def workload(self) -> WorkloadSpec:
        policy = self.semantic_plan.workload
        return WorkloadSpec(
            duration=policy.duration_s,
            input_len=policy.input_len,
            output_len=policy.output_len,
            rate_per_node=policy.rate_per_node,
            speedup=policy.speedup,
            sampling_strategy=policy.sampling_strategy,
            generation_mode=policy.generation_mode,
            arrival=policy.arrival,
            seed=policy.seed,
            modes=dict(policy.modes),
        )

    @property
    def trace(self) -> TraceSpec:
        policy = self.semantic_plan.trace
        return TraceSpec(
            kind=policy.kind,
            input_prompt_path=self.input_prompt_path,
            input_trace_path=self.input_trace_path,
            tokenizer_builder=(
                "" if policy.tokenizer_builder_ref == "default" else policy.tokenizer_builder_ref
            ),
        )

    def to_materialization_dict(self) -> dict[str, Any]:
        """Return only location/provenance fields for the mutable wrapper."""
        return {
            "run_id": self.run_id,
            "run_group_id": self.run_group_id,
            "created_at": self.created_at,
            "repo_root": self.repo_root,
            "snapshot_root": self.snapshot_root,
            "bundle": self.bundle,
            "spec_name": self.spec_name,
            "variant_name": self.variant_name,
            "axis_values": self.axis_values,
            "trace_artifact": self.trace_artifact,
            "runtime_manifest_path": self.runtime_manifest_path,
            "semantic_plan_path": self.semantic_plan_path,
            "deployment_plan_path": self.deployment_plan_path,
            "site_profile_path": self.site_profile_path,
            "run_semantic_hash": self.run_semantic_hash,
            "deployment_plan_hash": self.deployment_plan_hash,
            "source_snapshot_hash": self.source_snapshot_hash,
            "deployment_id_scheme": self.deployment_id_scheme,
            "input_prompt_path": self.input_prompt_path,
            "input_trace_path": self.input_trace_path,
            "spec_path": self.spec_path,
        }


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
