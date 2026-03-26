"""Run planner: materialize experiment specs into executable run bundles.

This is the core materialization layer. Given a spec path, it:

  1. Loads and validates the spec (spec_io).
  2. Expands the matrix into concrete variants (matrix.py).
  3. For each variant:
     a. Generates / reuses a content-addressed trace artifact (trace_store).
     b. Creates a run bundle directory tree (logs, results, state, meta, runtime).
     c. Asks the backend adapter to build a runtime manifest and validate.
     d. Renders a PBS job script that will invoke `eval.cli run execute`.
     e. Writes the self-contained run.yaml (RunPlan) that captures everything
        needed to execute the run without re-reading the original spec.

The run.yaml is the contract between materialize-time and execute-time:
the executor only needs the run.yaml path to reconstruct the full RunPlan
and drive the backend lifecycle.
"""

from __future__ import annotations

import os
from dataclasses import replace
from typing import Any

from src.site_config import get_site_config

from .backends import get_backend_adapter
from .matrix import expand_matrix
from .models import (
    ClientSpec,
    DeploymentSpec,
    ModelSpec,
    RunBundle,
    RunPlan,
    SchedulerSpec,
    TraceArtifact,
    TraceSpec,
    VariantSpec,
    WorkloadSpec,
)
from .schedulers.pbs import default_queue_and_walltime, render_pbs_job
from .spec_io import load_experiment_spec
from .trace_store import materialize_trace_artifact
from .utils import (
    dump_json_file,
    dump_yaml_file,
    ensure_dir,
    load_yaml_file,
    slugify,
    utc_timestamp,
)


def runs_root(root: str | None = None) -> str:
    base_root = root or os.path.join(get_site_config().experiments_root, "runs")
    return ensure_dir(base_root)


def materialize_run_bundles(
    spec_path: str,
    *,
    backend_name: str | None = None,
    experiments_root: str | None = None,
    trace_root: str | None = None,
) -> list[RunPlan]:
    spec = load_experiment_spec(spec_path)
    variants = expand_matrix(spec)
    run_plans = []
    for variant in variants:
        run_plan = _materialize_variant(
            variant,
            backend_name=backend_name or spec.backend.default,
            experiments_root=experiments_root,
            trace_root=trace_root,
        )
        run_plans.append(run_plan)
    return run_plans


def materialize_traces(
    spec_path: str,
    *,
    trace_root: str | None = None,
) -> list[TraceArtifact]:
    spec = load_experiment_spec(spec_path)
    artifacts = []
    for variant in expand_matrix(spec):
        artifacts.append(materialize_trace_artifact(variant, store_root=trace_root))
    return artifacts


def load_run_plan(path: str) -> RunPlan:
    data = load_yaml_file(path)
    bundle_raw = data["bundle"]
    trace_raw = data["trace_artifact"]
    deployment_raw = data["deployment"]
    client_raw = data["client"]
    scheduler_raw = data["scheduler"]
    workload_raw = data["workload"]
    trace_spec_raw = data["trace"]

    bundle = RunBundle(
        root_dir=str(bundle_raw["root_dir"]),
        run_yaml_path=str(bundle_raw["run_yaml_path"]),
        job_path=str(bundle_raw["job_path"]),
        logs_dir=str(bundle_raw["logs_dir"]),
        pbs_stdout_dir=str(bundle_raw["pbs_stdout_dir"]),
        pbs_stderr_dir=str(bundle_raw["pbs_stderr_dir"]),
        results_dir=str(bundle_raw["results_dir"]),
        state_dir=str(bundle_raw["state_dir"]),
        state_path=str(bundle_raw["state_path"]),
        meta_dir=str(bundle_raw["meta_dir"]),
        spec_snapshot_path=str(bundle_raw["spec_snapshot_path"]),
        runtime_dir=str(bundle_raw["runtime_dir"]),
    )
    trace_artifact = TraceArtifact(
        trace_id=str(trace_raw["trace_id"]),
        trace_path=str(trace_raw["trace_path"]),
        metadata_path=str(trace_raw["metadata_path"]),
        spec_hash=str(trace_raw["spec_hash"]),
        store_dir=str(trace_raw["store_dir"]),
    )
    deployment = DeploymentSpec(
        num_nodes=int(deployment_raw["num_nodes"]),
        models=[
            ModelSpec.from_dict(model_raw) for model_raw in deployment_raw.get("models", [])
        ],
        model_storage_path=str(deployment_raw["model_storage_path"]),
        local_stage_path=str(deployment_raw["local_stage_path"]),
        worker_max_ongoing=int(deployment_raw["worker_max_ongoing"]),
        num_gpus_per_node=int(deployment_raw["num_gpus_per_node"]),
    )
    client = ClientSpec(
        num_runs=int(client_raw["num_runs"]),
        include_tp=bool(client_raw["include_tp"]),
        early_stop=float(client_raw["early_stop"]),
        dest=str(client_raw["dest"]),
        num_nodes=int(client_raw["num_nodes"]),
        num_go_procs=int(client_raw["num_go_procs"]),
        num_go_workers=int(client_raw["num_go_workers"]),
        go_concurrency=int(client_raw["go_concurrency"]),
        warmup_rps=int(client_raw["warmup_rps"]),
        warmup_duration_s=float(client_raw["warmup_duration_s"]),
        sum_only=bool(client_raw["sum_only"]),
    )
    scheduler = SchedulerSpec(
        type=str(scheduler_raw["type"]),
        nodes=int(scheduler_raw["nodes"]),
        queue=str(scheduler_raw["queue"]),
        walltime=str(scheduler_raw["walltime"]),
        project=str(scheduler_raw["project"]),
        filesystems=str(scheduler_raw["filesystems"]),
        keep_output=str(scheduler_raw["keep_output"]),
        mail_user=str(scheduler_raw["mail_user"]),
        mail_events=str(scheduler_raw["mail_events"]),
    )
    workload = WorkloadSpec(
        duration=float(workload_raw["duration"]),
        input_len=int(workload_raw["input_len"]),
        output_len=int(workload_raw["output_len"]),
        rate_per_node=float(workload_raw["rate_per_node"]),
        speedup=float(workload_raw["speedup"]),
        sampling_strategy=str(workload_raw["sampling_strategy"]),
        generation_mode=str(workload_raw["generation_mode"]),
        seed=int(workload_raw["seed"]),
        modes={str(key): int(value) for key, value in workload_raw["modes"].items()},
    )
    trace_spec = TraceSpec(
        kind=str(trace_spec_raw["kind"]),
        input_prompt_path=str(trace_spec_raw.get("input_prompt_path", "")),
        input_trace_path=str(trace_spec_raw.get("input_trace_path", "")),
    )
    return RunPlan(
        run_id=str(data["run_id"]),
        created_at=str(data["created_at"]),
        repo_root=str(data["repo_root"]),
        bundle=bundle,
        spec_name=str(data["spec_name"]),
        variant_name=str(data["variant_name"]),
        axis_values={str(key): value for key, value in data.get("axis_values", {}).items()},
        backend_name=str(data["backend_name"]),
        backend_args=dict(data.get("backend_args", {})),
        trace_artifact=trace_artifact,
        runtime_manifest_path=str(data["runtime_manifest_path"]),
        deployment=deployment,
        client=client,
        scheduler=scheduler,
        workload=workload,
        trace=trace_spec,
        spec_path=str(data.get("spec_path", "")),
    )


def write_run_state(run_plan: RunPlan, status: str, **extra: Any) -> None:
    payload = {
        "run_id": run_plan.run_id,
        "status": status,
        "updated_at": utc_timestamp(),
    }
    payload.update(extra)
    dump_json_file(run_plan.bundle.state_path, payload)


def _materialize_variant(
    variant: VariantSpec,
    *,
    backend_name: str,
    experiments_root: str | None,
    trace_root: str | None,
) -> RunPlan:
    spec = variant.spec
    artifact = materialize_trace_artifact(variant, store_root=trace_root)

    run_id = f"{utc_timestamp()}_{slugify(variant.variant_name)}"
    root_dir = os.path.join(runs_root(experiments_root), spec.name, run_id)
    bundle = RunBundle(
        root_dir=ensure_dir(root_dir),
        run_yaml_path=os.path.join(root_dir, "run.yaml"),
        job_path=os.path.join(root_dir, "job", "job.pbs"),
        logs_dir=ensure_dir(os.path.join(root_dir, "logs")),
        pbs_stdout_dir=ensure_dir(os.path.join(root_dir, "logs", "pbs", "stdout")),
        pbs_stderr_dir=ensure_dir(os.path.join(root_dir, "logs", "pbs", "stderr")),
        results_dir=ensure_dir(os.path.join(root_dir, "results")),
        state_dir=ensure_dir(os.path.join(root_dir, "state")),
        state_path=os.path.join(root_dir, "state", "status.json"),
        meta_dir=ensure_dir(os.path.join(root_dir, "meta")),
        spec_snapshot_path=os.path.join(root_dir, "meta", "spec_snapshot.yaml"),
        runtime_dir=ensure_dir(os.path.join(root_dir, "runtime")),
    )
    runtime_manifest_path = os.path.join(bundle.runtime_dir, f"{backend_name}_runtime.yaml")
    scheduler = _resolve_scheduler(spec.scheduler)
    backend_args = dict(spec.backend.args.get(backend_name, {}))
    run_plan = RunPlan(
        run_id=run_id,
        created_at=utc_timestamp(),
        repo_root=os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")),
        bundle=bundle,
        spec_name=spec.name,
        variant_name=variant.variant_name,
        axis_values=dict(variant.axis_values),
        backend_name=backend_name,
        backend_args=backend_args,
        trace_artifact=artifact,
        runtime_manifest_path=runtime_manifest_path,
        deployment=replace(spec.deployment),
        client=replace(spec.client),
        scheduler=scheduler,
        workload=replace(spec.workload),
        trace=replace(spec.trace),
        spec_path=spec.spec_path,
    )

    adapter = get_backend_adapter(backend_name)
    adapter.validate(run_plan)
    adapter.build_runtime_manifest(run_plan)
    runtime_env = adapter.runtime_env(run_plan)

    ensure_dir(os.path.dirname(bundle.job_path))
    job_text = render_pbs_job(
        job_name=f"{spec.name}_{variant.variant_name}",
        num_nodes=scheduler.nodes,
        queue=scheduler.queue,
        walltime=scheduler.walltime,
        project=scheduler.project,
        filesystems=scheduler.filesystems,
        keep_output=scheduler.keep_output,
        stdout_dir=bundle.pbs_stdout_dir,
        stderr_dir=bundle.pbs_stderr_dir,
        mail_user=scheduler.mail_user,
        mail_events=scheduler.mail_events,
        code_root=run_plan.repo_root,
        env_script=runtime_env.env_script,
        run_yaml_path=bundle.run_yaml_path,
    )
    with open(bundle.job_path, "w", encoding="utf-8") as handle:
        handle.write(job_text)

    dump_yaml_file(bundle.spec_snapshot_path, spec)
    dump_yaml_file(bundle.run_yaml_path, run_plan)
    write_run_state(run_plan, "materialized")
    return run_plan


def _resolve_scheduler(spec: SchedulerSpec) -> SchedulerSpec:
    queue = spec.queue
    walltime = spec.walltime
    if not queue or not walltime:
        default_queue, default_walltime = default_queue_and_walltime(spec.nodes)
        if not queue:
            queue = default_queue
        if not walltime:
            walltime = default_walltime
    return replace(spec, queue=queue, walltime=walltime)


