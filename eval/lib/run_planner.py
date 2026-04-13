"""Run planner: materialize experiment specs into executable run bundles.

This is the core materialization layer. Given a spec path, it:

  1. Loads and validates the spec (spec_io).
  2. Expands the matrix into concrete variants (matrix.py).
  3. Creates a run group directory under runs/<spec>/runN with shared metadata.
  4. Creates or reuses a commit snapshot of the repo under site_config.snapshot_dir.
  5. For each variant:
     a. Generates / reuses a content-addressed trace artifact (trace_store).
     b. Creates a run bundle directory tree (job, logs, results, state, runtime).
     c. Asks the backend adapter to build a runtime manifest and validate.
     d. Renders a PBS job script that will invoke `eval.cli run execute`.
     e. Writes the self-contained run.yaml (RunPlan).
"""

from __future__ import annotations

import multiprocessing
import os
import re
import shutil
import subprocess
import sys
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import replace
from typing import Any

_MP_CONTEXT = multiprocessing.get_context("forkserver")

from site_config import get_site_config

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
from .utils import dump_json_file, dump_yaml_file, ensure_dir, load_yaml_file, slugify, utc_timestamp


_DEFAULT_MAX_WORKERS = 8
_RUN_GROUP_RE = re.compile(r"^run(\d+)$")


def runs_root(root: str | None = None) -> str:
    base_root = root or os.path.join(get_site_config().experiments_root, "runs")
    return ensure_dir(base_root)


def spec_runs_dir(spec_name: str, *, experiments_root: str | None = None) -> str:
    return os.path.join(runs_root(experiments_root), spec_name)


def list_run_group_ids(spec_name: str, *, experiments_root: str | None = None) -> list[str]:
    spec_dir = spec_runs_dir(spec_name, experiments_root=experiments_root)
    if not os.path.isdir(spec_dir):
        return []

    group_ids: list[tuple[int, str]] = []
    for entry in os.listdir(spec_dir):
        match = _RUN_GROUP_RE.fullmatch(entry)
        if match is None:
            continue
        group_ids.append((int(match.group(1)), entry))
    group_ids.sort()
    return [entry for _, entry in group_ids]


def resolve_run_group_dir(
    spec_name: str,
    *,
    run_group: str = "latest",
    experiments_root: str | None = None,
) -> str:
    spec_dir = spec_runs_dir(spec_name, experiments_root=experiments_root)
    if not os.path.isdir(spec_dir):
        raise FileNotFoundError(f"No runs directory found for spec {spec_name!r}: {spec_dir}")

    group_ids = list_run_group_ids(spec_name, experiments_root=experiments_root)
    if not group_ids:
        raise FileNotFoundError(
            f"No run groups found for spec {spec_name!r} under {spec_dir}. Expected runs/<spec>/runN/."
        )

    selected = group_ids[-1] if run_group == "latest" else run_group
    if selected not in group_ids:
        raise FileNotFoundError(
            f"Run group {run_group!r} not found for spec {spec_name!r}. Available: {', '.join(group_ids)}"
        )
    return os.path.join(spec_dir, selected)


def _next_run_group_id(spec_name: str, *, experiments_root: str | None = None) -> str:
    group_ids = list_run_group_ids(spec_name, experiments_root=experiments_root)
    if not group_ids:
        return "run0"
    latest = max(int(_RUN_GROUP_RE.fullmatch(group_id).group(1)) for group_id in group_ids)  # type: ignore[union-attr]
    return f"run{latest + 1}"


def _progress(msg: str) -> None:
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()


def _project_root_fallback() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def _resolve_repo_root(repo_root: str | None = None) -> str:
    fallback = os.path.abspath(repo_root or _project_root_fallback())
    result = subprocess.run(
        ["git", "-C", fallback, "rev-parse", "--show-toplevel"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode == 0 and result.stdout.strip():
        return os.path.abspath(result.stdout.strip())
    return fallback


def _git_output(repo_root: str, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", repo_root, *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"Git command failed in {repo_root}: git {' '.join(args)}\n{stderr}")
    return result.stdout.strip()


def _detect_repo_state(repo_root: str) -> tuple[str, list[str]]:
    commit_sha = _git_output(repo_root, "rev-parse", "HEAD")
    status_output = _git_output(repo_root, "status", "--porcelain=v1")
    dirty_files = []
    for line in status_output.splitlines():
        if not line:
            continue
        if len(line) >= 3 and line[2] == " ":
            dirty_files.append(line[3:])
        else:
            dirty_files.append(line[2:].lstrip())
    return commit_sha, dirty_files


def _warn_dirty_repo(repo_root: str, dirty_files: list[str]) -> None:
    _progress(
        f"WARNING: repo has {len(dirty_files)} uncommitted file(s); "
        f"snapshot will use committed HEAD only: {repo_root}"
    )
    for path in dirty_files:
        _progress(f"  dirty: {path}")


def _build_go_client(snapshot_root: str) -> None:
    go_client_dir = os.path.join(snapshot_root, "eval", "go_client")
    if not os.path.isdir(go_client_dir):
        return
    result = subprocess.run(
        ["make", "build"],
        cwd=go_client_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to build go_client in snapshot:\n{result.stderr.strip()}"
        )
    _progress("  go_client built successfully")


def _ensure_repo_snapshot(repo_root: str, commit_sha: str) -> str:
    snapshot_parent = ensure_dir(get_site_config().snapshot_dir)
    snapshot_root = os.path.join(snapshot_parent, commit_sha)
    metadata_path = os.path.join(snapshot_root, "snapshot_meta.json")
    if os.path.isfile(metadata_path):
        return snapshot_root

    temp_root = tempfile.mkdtemp(prefix=f"{commit_sha[:12]}_", dir=snapshot_parent)
    archive_process = subprocess.Popen(
        ["git", "-C", repo_root, "archive", "--format=tar", "HEAD"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=False,
    )
    try:
        assert archive_process.stdout is not None
        extract = subprocess.run(
            ["tar", "-xf", "-", "-C", temp_root],
            stdin=archive_process.stdout,
            check=False,
            capture_output=True,
            text=False,
        )
    finally:
        if archive_process.stdout is not None:
            archive_process.stdout.close()
    _, archive_stderr = archive_process.communicate()
    if archive_process.returncode != 0:
        shutil.rmtree(temp_root, ignore_errors=True)
        raise RuntimeError(
            f"Failed to export git snapshot for {commit_sha}: "
            f"{archive_stderr.decode('utf-8', errors='replace').strip()}"
        )
    if extract.returncode != 0:
        shutil.rmtree(temp_root, ignore_errors=True)
        raise RuntimeError(
            f"Failed to extract git snapshot for {commit_sha}: "
            f"{extract.stderr.decode('utf-8', errors='replace').strip()}"
        )

    _build_go_client(temp_root)

    dump_json_file(
        os.path.join(temp_root, "snapshot_meta.json"),
        {
            "commit_sha": commit_sha,
            "created_at": utc_timestamp(),
            "source_repo_root": repo_root,
        },
    )

    try:
        os.replace(temp_root, snapshot_root)
    except FileExistsError:
        shutil.rmtree(temp_root, ignore_errors=True)
    return snapshot_root


def _create_run_group(
    spec,
    *,
    spec_path: str,
    experiments_root: str | None,
    created_at: str,
    commit_sha: str,
    snapshot_root: str,
    dirty_files: list[str],
) -> tuple[str, str, str]:
    spec_dir = ensure_dir(spec_runs_dir(spec.name, experiments_root=experiments_root))
    run_group_id = _next_run_group_id(spec.name, experiments_root=experiments_root)
    group_root_dir = ensure_dir(os.path.join(spec_dir, run_group_id))
    meta_dir = ensure_dir(os.path.join(group_root_dir, "meta"))
    group_spec_path = os.path.join(meta_dir, "spec.yaml")
    shutil.copyfile(spec_path, group_spec_path)
    dump_json_file(
        os.path.join(meta_dir, "run_group.json"),
        {
            "run_group_id": run_group_id,
            "created_at": created_at,
            "spec_name": spec.name,
            "spec_source_path": os.path.abspath(spec_path),
            "git_commit": commit_sha,
            "git_dirty": bool(dirty_files),
            "dirty_files": dirty_files,
            "snapshot_path": snapshot_root,
        },
    )
    return run_group_id, group_root_dir, group_spec_path


def materialize_run_bundles(
    spec_path: str,
    *,
    backend_name: str | None = None,
    experiments_root: str | None = None,
    trace_root: str | None = None,
    max_workers: int | None = None,
    repo_root: str | None = None,
    force_trace: bool = False,
) -> list[RunPlan]:
    spec = load_experiment_spec(spec_path)
    variants = expand_matrix(spec)
    total = len(variants)
    resolved_backend = backend_name or spec.backend.default
    repo_root_live = _resolve_repo_root(repo_root)
    commit_sha, dirty_files = _detect_repo_state(repo_root_live)
    if dirty_files:
        _warn_dirty_repo(repo_root_live, dirty_files)
    snapshot_root = _ensure_repo_snapshot(repo_root_live, commit_sha)
    created_at = utc_timestamp()
    run_group_id, group_root_dir, group_spec_path = _create_run_group(
        spec,
        spec_path=spec_path,
        experiments_root=experiments_root,
        created_at=created_at,
        commit_sha=commit_sha,
        snapshot_root=snapshot_root,
        dirty_files=dirty_files,
    )
    _progress(
        f"Materializing {total} variant(s) for {spec.name!r} in {run_group_id!r} "
        f"(backend={resolved_backend})"
    )

    if total <= 1:
        run_plans = []
        for variant in variants:
            _progress(f"  [1/{total}] {variant.variant_name} ...")
            rp = _materialize_variant(
                variant,
                backend_name=resolved_backend,
                group_root_dir=group_root_dir,
                run_group_id=run_group_id,
                group_created_at=created_at,
                group_spec_path=group_spec_path,
                snapshot_root=snapshot_root,
                trace_root=trace_root,
                force_trace=force_trace,
            )
            _progress(f"  [1/{total}] {variant.variant_name} done")
            run_plans.append(rp)
        return run_plans

    workers = min(max_workers or _DEFAULT_MAX_WORKERS, total)
    _progress(f"  using {workers} workers")
    results: list[RunPlan | None] = [None] * total
    with ProcessPoolExecutor(max_workers=workers, mp_context=_MP_CONTEXT) as pool:
        future_to_idx = {
            pool.submit(
                _materialize_variant,
                variant,
                backend_name=resolved_backend,
                group_root_dir=group_root_dir,
                run_group_id=run_group_id,
                group_created_at=created_at,
                group_spec_path=group_spec_path,
                snapshot_root=snapshot_root,
                trace_root=trace_root,
                force_trace=force_trace,
            ): idx
            for idx, variant in enumerate(variants)
        }
        done_count = 0
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            done_count += 1
            name = variants[idx].variant_name
            results[idx] = future.result()
            _progress(f"  [{done_count}/{total}] {name} done")
    return results  # type: ignore[return-value]


def materialize_traces(
    spec_path: str,
    *,
    trace_root: str | None = None,
    max_workers: int | None = None,
    force: bool = False,
) -> list[TraceArtifact]:
    spec = load_experiment_spec(spec_path)
    variants = expand_matrix(spec)
    total = len(variants)
    _progress(f"Materializing {total} trace(s) for {spec.name!r}")

    if total <= 1:
        artifacts = []
        for variant in variants:
            _progress(f"  [1/{total}] {variant.variant_name} ...")
            art = materialize_trace_artifact(variant, store_root=trace_root, force=force)
            _progress(f"  [1/{total}] {variant.variant_name} done")
            artifacts.append(art)
        return artifacts

    workers = min(max_workers or _DEFAULT_MAX_WORKERS, total)
    _progress(f"  using {workers} workers")
    results: list[TraceArtifact | None] = [None] * total
    with ProcessPoolExecutor(max_workers=workers, mp_context=_MP_CONTEXT) as pool:
        future_to_idx = {
            pool.submit(materialize_trace_artifact, variant, store_root=trace_root, force=force): idx
            for idx, variant in enumerate(variants)
        }
        done_count = 0
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            done_count += 1
            name = variants[idx].variant_name
            results[idx] = future.result()
            _progress(f"  [{done_count}/{total}] {name} done")
    return results  # type: ignore[return-value]


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
        group_root_dir=str(bundle_raw["group_root_dir"]),
        root_dir=str(bundle_raw["root_dir"]),
        run_yaml_path=str(bundle_raw["run_yaml_path"]),
        job_path=str(bundle_raw["job_path"]),
        logs_dir=str(bundle_raw["logs_dir"]),
        pbs_stdout_dir=str(bundle_raw["pbs_stdout_dir"]),
        pbs_stderr_dir=str(bundle_raw["pbs_stderr_dir"]),
        results_dir=str(bundle_raw["results_dir"]),
        state_dir=str(bundle_raw["state_dir"]),
        state_path=str(bundle_raw["state_path"]),
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
        models=[ModelSpec.from_dict(model_raw) for model_raw in deployment_raw.get("models", [])],
        model_storage_path=str(deployment_raw["model_storage_path"]),
        local_stage_path=str(deployment_raw["local_stage_path"]),
        replica_max_ongoing_requests=int(deployment_raw["replica_max_ongoing_requests"]),
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
        startup_only=bool(client_raw.get("startup_only", False)),
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
        run_group_id=str(data["run_group_id"]),
        created_at=str(data["created_at"]),
        repo_root=str(data["repo_root"]),
        snapshot_root=str(data["snapshot_root"]),
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
        "run_group_id": run_plan.run_group_id,
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
    group_root_dir: str,
    run_group_id: str,
    group_created_at: str,
    group_spec_path: str,
    snapshot_root: str,
    trace_root: str | None,
    force_trace: bool = False,
) -> RunPlan:
    spec = variant.spec
    artifact = materialize_trace_artifact(variant, store_root=trace_root, force=force_trace)
    run_id = slugify(variant.variant_name)
    root_dir = os.path.join(group_root_dir, run_id)
    bundle = RunBundle(
        group_root_dir=group_root_dir,
        root_dir=ensure_dir(root_dir),
        run_yaml_path=os.path.join(root_dir, "run.yaml"),
        job_path=os.path.join(root_dir, "job", "job.pbs"),
        logs_dir=ensure_dir(os.path.join(root_dir, "logs")),
        pbs_stdout_dir=ensure_dir(os.path.join(root_dir, "logs", "pbs", "stdout")),
        pbs_stderr_dir=ensure_dir(os.path.join(root_dir, "logs", "pbs", "stderr")),
        results_dir=ensure_dir(os.path.join(root_dir, "results")),
        state_dir=ensure_dir(os.path.join(root_dir, "state")),
        state_path=os.path.join(root_dir, "state", "status.json"),
        runtime_dir=ensure_dir(os.path.join(root_dir, "runtime")),
    )
    runtime_manifest_path = os.path.join(bundle.runtime_dir, f"{backend_name}_runtime.yaml")
    scheduler = _resolve_scheduler(spec.scheduler)
    backend_args = dict(spec.backend.args.get(backend_name, {}))
    run_plan = RunPlan(
        run_id=run_id,
        run_group_id=run_group_id,
        created_at=group_created_at,
        repo_root=snapshot_root,
        snapshot_root=snapshot_root,
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
        spec_path=group_spec_path,
    )

    adapter = get_backend_adapter(backend_name)
    adapter.validate(run_plan)
    adapter.build_runtime_manifest(run_plan)
    runtime_env = adapter.runtime_env(run_plan)

    ensure_dir(os.path.dirname(bundle.job_path))
    job_text = render_pbs_job(
        job_name=f"{spec.name}_{run_group_id}_{variant.variant_name}",
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
