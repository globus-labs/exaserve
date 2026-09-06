"""Run executor: drive the full lifecycle of a materialized run.

execute_run() is the entry point called inside a PBS job. It:

  1. Loads materialization metadata and verifies the canonical RunPlan.
  2. Delegates to the backend adapter for: launch -> wait_ready -> discover_targets.
  3. Spawns the replay client (eval/replay_client.py) against the discovered
     endpoints, optionally via MPI for multi-node client fanout.
  4. Writes state transitions (running -> replaying -> succeeded/failed) to
     the run bundle's state file for external monitoring.
  5. Ensures the backend is stopped in the finally block regardless of outcome.

submit_run() is a convenience wrapper that calls `qsub` on the PBS job
script rendered by the planner.
"""

from __future__ import annotations

import getpass
import hashlib
import math
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Callable, Iterable

from exaserve.exception_notes import add_exception_note
from exaserve.schedulers import SubmissionRejected, get_scheduler

from .backends import get_backend_adapter
from .backends.base import (
    BackendRunContext,
    process_group_exists,
    terminate_process_tree,
)
from .run_planner import load_run_plan, resolve_run_group_dir, write_run_state


def execute_run(run_yaml_path: str, *, dry_run: bool = False) -> int:
    run_plan = load_run_plan(run_yaml_path)
    adapter = get_backend_adapter(run_plan.backend_name)
    adapter.validate(run_plan)
    ctx = BackendRunContext(run_plan=run_plan)

    if dry_run:
        write_run_state(run_plan, "dry-run")
        print(
            f"DRY RUN: would execute {run_plan.run_group_id}/{run_plan.run_id} "
            f"with backend {run_plan.backend_name}"
        )
        return 0

    # A scheduler normally starts one executor, but retries, operator error,
    # and duplicate scheduler delivery can race.  Result0 is deliberately a
    # single-generation artifact, so claim the complete launch/replay/publish
    # lifecycle before creating any backend process.  The heartbeat makes the
    # lease usable for multi-hour runs without permitting a second executor to
    # overwrite the first one's evidence.
    from exaserve.state.atomic import ExclusiveLease, LeaseHeartbeat, LeaseHeldError

    lease_path = run_plan.bundle.state_path + ".execute"
    try:
        lease = ExclusiveLease(
            lease_path,
            ttl_s=300,
            owner_note=f"execute {run_plan.run_group_id}/{run_plan.run_id}",
        ).acquire()
    except LeaseHeldError as exc:
        raise RuntimeError(
            f"another executor owns {run_plan.run_group_id}/{run_plan.run_id}: {exc}"
        ) from exc
    try:
        with LeaseHeartbeat(lease, interval_s=30) as heartbeat:
            return _execute_run_locked(run_plan, adapter, ctx, heartbeat)
    finally:
        active_error = sys.exc_info()[1]
        try:
            lease.release()
        except BaseException as release_exc:
            if active_error is None:
                raise
            add_exception_note(
                active_error, f"run execution lease cleanup also failed: {release_exc}"
            )


def _execute_run_locked(run_plan, adapter, ctx, heartbeat) -> int:
    """Execute one run while its cross-host fencing lease is live."""
    launched = None
    base_urls: list[str] = []
    try:
        heartbeat.ensure_held()
        write_run_state(run_plan, "running", backend=run_plan.backend_name)
        launched = adapter.launch(ctx)
        adapter.wait_ready(ctx, launched)
        heartbeat.ensure_held()
        base_urls = adapter.discover_targets(ctx, launched)

        if getattr(run_plan.client, "startup_only", False):
            print("[run_executor] startup_only=True — skipping replay client", flush=True)
            heartbeat.ensure_held()
            evidence_entries = _capture_deployment_evidence(run_plan, launched)
            measurement_entries = _capture_startup_measurement(
                run_plan,
                launched,
                ready_evidence_path=evidence_entries["deployment_ready_evidence"],
            )
            heartbeat.ensure_held()
            adapter.stop(ctx, launched)
            terminal_entries = _capture_startup_terminal_evidence(run_plan, launched)
            launched = None
            heartbeat.ensure_held()
            result_entries = {
                **evidence_entries,
                **measurement_entries,
                **terminal_entries,
            }
            expected_ids = (
                "deployment_ready_evidence",
                "compatibility_receipts",
                "run_provenance",
                "startup_scaling_trace",
                "startup_metrics",
                "deployment_shutdown_report",
                "deployment_terminal_status",
            )
            manifest = _publish_result_manifest(
                run_plan, entries=result_entries, expected_ids=expected_ids, reasons=()
            )
            if not manifest.complete:
                write_run_state(
                    run_plan,
                    "partial",
                    base_urls=base_urls,
                    exit_code=3,
                    incomplete_reasons=list(manifest.incomplete_reasons),
                    result_manifest_hash=manifest.manifest_hash,
                )
                print(
                    "[run_executor] startup-only run marked PARTIAL (not succeeded): "
                    + "; ".join(manifest.incomplete_reasons),
                    flush=True,
                )
                return 3
            write_run_state(
                run_plan,
                "succeeded",
                base_urls=base_urls,
                exit_code=0,
                result_manifest_hash=manifest.manifest_hash,
            )
            return 0

        write_run_state(run_plan, "replaying", base_urls=base_urls)
        arms = list(getattr(run_plan.client, "dispatch_topologies", []) or [])
        if arms:
            # Dispatch-topology ablation: one replay pass per arm against the
            # SAME bring-up, so the arms differ only in which node(s) each
            # client rank talks to. Results land in results/<arm>/.
            exit_code = 0
            for arm in arms:
                print(f"[run_executor] dispatch topology arm: {arm}", flush=True)
                rc = _run_replay_client(
                    run_plan,
                    base_urls,
                    topology_arm=arm,
                    log_name=f"replay_{arm}.log",
                    backend_process=launched.monitor.process,
                    runtime_capsule_manifest_path=os.path.join(
                        launched.monitor.status_dir, "source_staging_manifest.json"
                    ),
                    runtime_generation=launched.monitor.expected_generation,
                )
                print(f"[run_executor] arm {arm} exited {rc}", flush=True)
                if rc != 0:
                    # Keep going: a failed arm should not cost us the others,
                    # which already paid for the bring-up.
                    if exit_code == 0:
                        exit_code = rc
        else:
            # "direct" means node-local dispatch (the default); mesh must be
            # asked for explicitly. Always pass it so the arm is on the record.
            exit_code = _run_replay_client(
                run_plan,
                base_urls,
                backend_process=launched.monitor.process,
                runtime_capsule_manifest_path=os.path.join(
                    launched.monitor.status_dir, "source_staging_manifest.json"
                ),
                runtime_generation=launched.monitor.expected_generation,
            )
        heartbeat.ensure_held()
        if exit_code == 0:
            # Collect per-replica vLLM stats before tearing down the cluster.
            # IMP-B08: if stats were REQUESTED and collection failed, the run is
            # telemetry-incomplete and must not be labelled succeeded.
            stats_error = None
            if getattr(run_plan.deployment, "collect_stats", False):
                try:
                    from .server_stats import (
                        collect_server_stats,
                        ray_address_from_status,
                    )
                    from exaserve.status_api import read_deployment_status
                    from exaserve.telemetry import TelemetryIdentity

                    expected_replicas = sum(
                        int(model.num_replicas or 0) for model in run_plan.deployment.models
                    )
                    status_dir = launched.monitor.status_dir
                    if not isinstance(status_dir, str) or not status_dir:
                        raise RuntimeError("launched backend has no typed deployment status path")
                    deployment_status = read_deployment_status(status_dir)
                    if deployment_status is None:
                        raise RuntimeError("deployment status unavailable for stats identity")
                    stats_identity = TelemetryIdentity(
                        deployment_id=deployment_status.deployment_id,
                        generation=deployment_status.generation,
                        deployment_plan_hash=(deployment_status.deployment_plan_hash),
                        allocation_binding_hash=(deployment_status.allocation_binding_hash),
                    )
                    import ray as stats_ray_client

                    owns_stats_connection = not stats_ray_client.is_initialized()
                    try:
                        stats = collect_server_stats(
                            run_plan.bundle.results_dir,
                            identity=stats_identity,
                            expected_replicas=expected_replicas,
                            ray_address=ray_address_from_status(
                                status_dir,
                                status=deployment_status,
                                plan=run_plan.semantic_plan.deployment,
                            ),
                        )
                    finally:
                        # collect_server_stats may initialize a driver in this
                        # executor.  Close only that connection before the
                        # serving owner drains GCS; otherwise Ray's background
                        # client terminates the executor before manifest commit.
                        if owns_stats_connection and stats_ray_client.is_initialized():
                            stats_ray_client.shutdown()
                    if stats.get("error"):
                        raise RuntimeError(str(stats["error"]))
                    replica_count = stats.get("replica_count")
                    if type(replica_count) is not int or replica_count != expected_replicas:
                        raise RuntimeError(
                            f"server stats cover {stats.get('replica_count', 0)}/"
                            f"{expected_replicas} expected replicas"
                        )
                except Exception as e:
                    stats_error = str(e)
                    print(
                        f"[run_executor] WARNING: server stats collection failed: {e}", flush=True
                    )
            replay_summary = _validate_replay_results(run_plan)
            # IMP-B08 / PR-019: distinguish PARTIAL from SUCCEEDED. A run with
            # request errors, an incomplete dispatch, or a missing rank shard is
            # not a success — labelling it so contaminates downstream analysis.
            incomplete_reasons = list(replay_summary.get("incomplete_reasons", []))
            if stats_error:
                incomplete_reasons.append(f"required stats collection failed: {stats_error}")
            evidence_entries = _capture_deployment_evidence(run_plan, launched)
            result_entries = {
                **replay_summary["result_entries"],
                **evidence_entries,
            }
            expected_ids = tuple(replay_summary["expected_ids"]) + (
                "deployment_ready_evidence",
                "compatibility_receipts",
                "run_provenance",
            )
            if getattr(run_plan.deployment, "collect_stats", False):
                expected_ids += ("server_stats", "replica_stats")
                for logical_id, filename in (
                    ("server_stats", "server_stats.json"),
                    ("replica_stats", "replica_stats_all.json"),
                ):
                    path = os.path.join(run_plan.bundle.results_dir, filename)
                    if os.path.isfile(path):
                        result_entries[logical_id] = path
                    else:
                        incomplete_reasons.append(f"required result {logical_id} is missing")
            # Hashing result/evidence files and collecting telemetry can outlive
            # several renewals.  A stale executor may clean up what it owns, but
            # it must never publish over the successor that took its lease.
            heartbeat.ensure_held()
            # Teardown is part of run completeness. Do not publish even an
            # apparently complete ResultManifest until owned deployment
            # cleanup has succeeded.
            adapter.stop(ctx, launched)
            launched = None
            heartbeat.ensure_held()
            manifest = _publish_result_manifest(
                run_plan,
                entries=result_entries,
                expected_ids=expected_ids,
                reasons=incomplete_reasons,
            )
            incomplete_reasons = list(manifest.incomplete_reasons)
            state = "succeeded" if manifest.complete else "partial"
            write_run_state(
                run_plan,
                state,
                base_urls=base_urls,
                exit_code=exit_code,
                result_path=replay_summary["result_path"],
                requests_completed=replay_summary["requests_completed"],
                requests_scheduled=replay_summary["requests_scheduled"],
                errors=replay_summary["errors"],
                incomplete_reasons=incomplete_reasons or None,
                result_manifest_hash=manifest.manifest_hash,
            )
            if incomplete_reasons:
                print(
                    f"[run_executor] run marked PARTIAL (not succeeded): "
                    f"{'; '.join(incomplete_reasons)}",
                    flush=True,
                )
                return 3
        else:
            adapter.stop(ctx, launched)
            launched = None
            heartbeat.ensure_held()
            write_run_state(run_plan, "failed", base_urls=base_urls, exit_code=exit_code)
        return exit_code
    except (KeyboardInterrupt, SystemExit) as exc:
        # Operator/scheduler cancellation is a terminal result, not an
        # unrecorded BaseException that leaves durable state at RUNNING.
        stop_error = None
        if launched is not None:
            try:
                adapter.stop(ctx, launched)
            except BaseException as cleanup_exc:
                stop_error = cleanup_exc
            launched = None
        try:
            heartbeat.ensure_held()
            payload = {"error": f"{type(exc).__name__}: {str(exc) or 'execution interrupted'}"}
            if stop_error is not None:
                payload["cleanup_error"] = str(stop_error)
            write_run_state(run_plan, "cancelled", **payload)
        except BaseException as publication_exc:
            add_exception_note(
                exc,
                f"cancelled RunStatus publication also failed: {publication_exc}",
            )
        if stop_error is not None:
            add_exception_note(exc, f"backend cleanup also failed: {stop_error}")
        raise
    except Exception as exc:
        stop_error = None
        if launched is not None:
            try:
                adapter.stop(ctx, launched)
            except Exception as cleanup_exc:
                stop_error = cleanup_exc
            launched = None
        try:
            heartbeat.ensure_held()
        except BaseException as fence_exc:
            raise RuntimeError(
                f"run failed ({exc}); execution lease was lost, so terminal status "
                "was not published"
            ) from fence_exc
        payload = {"error": str(exc)}
        if stop_error is not None:
            payload["cleanup_error"] = str(stop_error)
        if base_urls:
            payload["base_urls"] = base_urls
        try:
            write_run_state(run_plan, "failed", **payload)
        except Exception as status_exc:
            raise RuntimeError(
                f"run failed ({exc}); terminal status publication also failed: {status_exc}"
            ) from exc
        if stop_error is not None:
            raise RuntimeError(
                f"run failed ({exc}); backend cleanup also failed: {stop_error}"
            ) from exc
        raise
    finally:
        if launched is not None:
            # This path is for BaseException (for example KeyboardInterrupt),
            # because ordinary failures are handled above. Cleanup evidence is
            # attached to the first cause; it must never replace that cause.
            active = sys.exc_info()[1]
            try:
                adapter.stop(ctx, launched)
            except Exception as cleanup_exc:
                if active is None:
                    raise
                add_exception_note(active, f"backend cleanup also failed: {cleanup_exc}")
                print(
                    f"[run_executor] cleanup after {type(active).__name__} "
                    f"also failed: {cleanup_exc}",
                    file=sys.stderr,
                    flush=True,
                )


def submit_run(target: str, *, dry_run: bool = False) -> int:
    run_yaml_path = _resolve_run_yaml(target)
    run_plan = load_run_plan(run_yaml_path)
    scheduler = get_scheduler(getattr(run_plan.scheduler, "type", "pbs"))
    if dry_run:
        print(f"[{scheduler.name}] submit {run_plan.bundle.job_path}")
        return 0
    kind, detail = _submit_run_once(run_plan, scheduler, submit_attempt=1)
    if kind in {"submitted", "reconciled", "attached"}:
        print(detail)
        return 0
    prefix = "AMBIGUOUS: " if kind == "ambiguous" else ""
    print(prefix + detail)
    return 1


def _scheduler_run_identity(run_plan, record=None) -> str:
    data = (record.data if record is not None else {}) or {}
    identity = data.get("scheduler_run_identity", "")
    if not isinstance(identity, str):
        raise RuntimeError("RunStatus scheduler identity must be text")
    from .run_planner import scheduler_run_identity

    expected = scheduler_run_identity(
        run_semantic_hash=run_plan.run_semantic_hash,
        spec_name=run_plan.spec_name,
        run_group_id=run_plan.run_group_id,
        run_id=run_plan.run_id,
    )
    if identity and identity != expected:
        raise RuntimeError(
            f"RunStatus scheduler identity {identity!r} disagrees with RunPlan {expected!r}"
        )
    return identity or expected


def _reconcile_one_submission(run_plan, scheduler, identity: str) -> str:
    """Attach one exact scheduler job or keep the submit intent ambiguous."""
    from .run_planner import write_run_state

    try:
        matches = scheduler.find_by_run_identity(identity, user=getpass.getuser())
    except Exception as exc:
        raise RuntimeError(f"exact scheduler reconciliation unavailable: {exc}") from exc
    if len(matches) == 0:
        raise RuntimeError(
            f"no exact scheduler job named {identity!r}; absence is not proof "
            "that the submit request was never accepted"
        )
    if len(matches) != 1:
        raise RuntimeError(
            f"{len(matches)} jobs share exact identity {identity!r}; operator "
            "cancellation is required"
        )
    job_id = matches[0].job_id
    write_run_state(
        run_plan,
        "submitted",
        scheduler_job_id=job_id,
        scheduler_run_identity=identity,
        reconciled=True,
    )
    return job_id


def _submit_run_once(run_plan, scheduler, *, submit_attempt: int) -> tuple[str, str]:
    """Submit at most once under a per-run cross-process lease.

    The durable ``submitting`` phase precedes the external call. Only a typed
    definite rejection clears it. Timeouts, zero-exit parse failures, and
    status-publication failures retain the intent and require exact scheduler
    reconciliation before another submit is possible.
    """
    from exaserve.state.atomic import ExclusiveLease, LeaseHeartbeat, LeaseHeldError
    from exaserve.state.status import StatusStore
    from .run_planner import write_run_state

    lease_path = run_plan.bundle.state_path + ".submit"
    try:
        lease = ExclusiveLease(
            lease_path, ttl_s=3600, owner_note=f"submit {run_plan.run_group_id}/{run_plan.run_id}"
        ).acquire()
    except LeaseHeldError as exc:
        return "ambiguous", f"another submitter holds the run lease: {exc}"
    heartbeat = LeaseHeartbeat(lease, interval_s=30.0)
    heartbeat_started = False
    try:
        heartbeat.start()
        heartbeat_started = True
        record = StatusStore.run(run_plan.bundle.state_path).load()
        if record is None:
            return "ambiguous", "RunStatus is missing; refusing an unrecorded submission"
        identity = _scheduler_run_identity(run_plan, record)
        if record.state in {"SUBMITTED", "RUNNING"}:
            job_id = (record.data or {}).get("scheduler_job_id", "")
            if not isinstance(job_id, str):
                raise RuntimeError("RunStatus scheduler job ID must be text")
            if not job_id:
                return "ambiguous", f"{record.state} RunStatus lacks scheduler_job_id"
            return "attached", job_id
        if record.state != "PLANNED":
            return (
                "terminal",
                f"run is {record.state}; create an explicit new materialization/generation "
                "instead of reusing its scheduler identity",
            )
        if (record.data or {}).get("phase") == "submitting":
            try:
                return "reconciled", _reconcile_one_submission(run_plan, scheduler, identity)
            except Exception as exc:
                return "ambiguous", str(exc)

        try:
            write_run_state(run_plan, "submitting", submit_attempt=submit_attempt)
        except Exception as exc:
            return "ambiguous", f"cannot persist submit intent: {exc}"
        try:
            heartbeat.ensure_held()
            submission = scheduler.submit(Path(run_plan.bundle.job_path))
            heartbeat.ensure_held()
        except SubmissionRejected as exc:
            try:
                write_run_state(run_plan, "planned", last_submit_error=str(exc))
            except Exception as status_exc:
                return (
                    "ambiguous",
                    f"scheduler rejected submission ({exc}); intent reset failed: {status_exc}",
                )
            return "rejected", str(exc)
        except Exception as exc:
            try:
                job_id = _reconcile_one_submission(run_plan, scheduler, identity)
            except Exception as reconciliation_exc:
                return (
                    "ambiguous",
                    f"submit ownership unknown ({exc}); {reconciliation_exc}",
                )
            return "reconciled", job_id
        try:
            write_run_state(
                run_plan,
                "submitted",
                scheduler_job_id=submission.job_id,
                scheduler_run_identity=identity,
                reconciled=False,
            )
        except Exception as exc:
            return (
                "ambiguous",
                f"scheduler accepted {submission.job_id!r}, but status publication failed: {exc}",
            )
        return "submitted", submission.job_id
    finally:
        active_error = sys.exc_info()[1]
        heartbeat_error = None
        if heartbeat_started:
            try:
                heartbeat.stop(active_error)
            except BaseException as stop_exc:
                heartbeat_error = stop_exc
                if active_error is not None:
                    add_exception_note(
                        active_error, f"run submission heartbeat cleanup also failed: {stop_exc}"
                    )
        try:
            lease.release()
        except BaseException as release_exc:
            if active_error is None and heartbeat_error is None:
                raise
            target_error = active_error or heartbeat_error
            assert target_error is not None
            add_exception_note(
                target_error, f"run submission lease cleanup also failed: {release_exc}"
            )
        if active_error is None and heartbeat_error is not None:
            raise heartbeat_error


@lru_cache(maxsize=16)
def _validated_replay_capsule_environment(
    manifest_path: str,
    deployment_id: str,
    generation: int,
    deployment_plan_hash: str,
    site_profile_hash: str,
    compatibility_profile_hash: str,
    compatibility_manifest_hash: str,
    vendor: str,
    engine: str,
    null_compute: bool,
    expected_ranks: int,
) -> tuple[tuple[str, str], ...]:
    """Read the head-published capsule receipt once and bind its local paths."""
    from exaserve.source_staging import (
        runtime_paths_from_result,
        validate_source_staging_result,
    )
    from exaserve.state.atomic import strict_json_load_path
    from exaserve.compat.profile import default_profile
    from exaserve.vllm_modelinfo_seed import expected_source_seed_evidence

    compatibility = default_profile(vendor)
    seed_evidence = expected_source_seed_evidence(
        compatibility,
        install_required=engine == "vllm" and not null_compute,
    )

    result = validate_source_staging_result(
        strict_json_load_path(manifest_path),
        expected_deployment_id=deployment_id,
        expected_generation=generation,
        expected_plan_hash=deployment_plan_hash,
        expected_site_profile_hash=site_profile_hash,
        expected_compatibility_profile_id=compatibility_profile_hash,
        expected_compatibility_manifest_hash=compatibility_manifest_hash,
        require_seed_evidence=True,
        expected_seed_evidence=seed_evidence,
    )
    if len(result["rank_receipts"]) != expected_ranks:
        raise RuntimeError("source capsule receipt rank count disagrees with the replay allocation")
    if result["local_eval_manifest"] is None or result["local_run_plan"] is None:
        raise RuntimeError("source capsule omitted required local evaluation artifacts")
    paths = runtime_paths_from_result(result)
    environment = {
        "EXASERVE_LOCAL_RUNTIME_ROOT": str(paths.root),
        "EXASERVE_LOCAL_STATE_ROOT": str(paths.state_root),
        "EXASERVE_LOCAL_GO_DISPATCH": str(paths.go_dispatch_path),
        "EXASERVE_LOCAL_EVAL_MANIFEST": str(paths.eval_manifest_path),
        "EXASERVE_LOCAL_RUN_PLAN_PATH": str(paths.run_plan_path),
        "EXASERVE_LOCAL_PLAN_PATH": str(paths.plan_path),
        "EXASERVE_LOCAL_SITE_PROFILE_PATH": str(paths.site_profile_path),
        "EXASERVE_LOCAL_BINDING_PATH": str(paths.binding_path),
        "EXASERVE_QUALIFIED_PYTHON": result["qualified_python"],
        "EXASERVE_QUALIFIED_PYTHON_SHA256": result["qualified_python_sha256"],
        "EXASERVE_QUALIFIED_PYTHON_SITE_PROFILE_HASH": site_profile_hash,
        "EXASERVE_COMPAT_SOURCES_NODE_PROFILE": result["compatibility_profile_id"],
        "EXASERVE_COMPAT_SOURCES_NODE_MANIFEST": result["compatibility_manifest_hash"],
    }
    return tuple(sorted(environment.items()))


def _closed_replay_worker_environment(run_plan, capsule_environment: dict[str, str]) -> dict:
    from exaserve.plan.runtime_environment import (
        SHARED_ROOTS_ENV,
        RuntimePaths,
        closed_runtime_environment,
    )

    from exaserve.plan.io import load_site_profile

    plan = run_plan.semantic_plan.deployment
    profile = load_site_profile(
        os.path.join(capsule_environment["EXASERVE_LOCAL_RUNTIME_ROOT"], "run", "site.profile.json")
    )
    if (
        profile.site_id != plan.site_profile_id
        or profile.site_profile_hash != plan.site_profile_hash
    ):
        raise RuntimeError("replay capsule SiteProfile does not match DeploymentPlan")
    paths = RuntimePaths.from_roots(
        capsule_environment["EXASERVE_LOCAL_RUNTIME_ROOT"],
        capsule_environment["EXASERVE_LOCAL_STATE_ROOT"],
        policy=profile,
        require_runtime=True,
    )
    paths.verify_capsule(policy=profile)
    paths.prepare_state(policy=profile)
    base = os.environ.copy()
    base.update(capsule_environment)
    selected = closed_runtime_environment(plan, paths=paths, base_environment=base, policy=profile)
    selected["PYTHONSAFEPATH"] = "1"
    # Replay consumes only local paths. The general runtime guard's declaration
    # of shared roots is intentionally not inherited by this closed client.
    selected.pop(SHARED_ROOTS_ENV, None)
    exact_names = {
        "PATH",
        "LD_LIBRARY_PATH",
        "LANG",
        "LC_ALL",
        "TZ",
        "HOME",
        "TMPDIR",
        "TMP",
        "TEMP",
        "XDG_CACHE_HOME",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "IPYTHONDIR",
        "JUPYTER_CONFIG_DIR",
        "NUMBA_CACHE_DIR",
        "TORCH_EXTENSIONS_DIR",
        "MPLCONFIGDIR",
        "HF_HOME",
        "HF_HUB_CACHE",
        "HUGGINGFACE_HUB_CACHE",
        "TRANSFORMERS_CACHE",
        "TORCH_HOME",
        "TRITON_CACHE_DIR",
        "VLLM_CACHE_ROOT",
        "RAY_TMPDIR",
        "PYTHONPATH",
        "PYTHONNOUSERSITE",
        "PYTHONSAFEPATH",
        "PYTHONDONTWRITEBYTECODE",
        "PYTHONPYCACHEPREFIX",
        "PMIX_MCA_mca_base_param_files",
        "PMIX_MCA_mca_base_component_path",
        "EXASERVE_QUALIFIED_PYTHON",
        "EXASERVE_QUALIFIED_PYTHON_SHA256",
        "EXASERVE_QUALIFIED_PYTHON_SITE_PROFILE_HASH",
        "EXASERVE_PLAN_PATH",
        "EXASERVE_SITE_PROFILE_PATH",
        "EXASERVE_ALLOCATION_BINDING_PATH",
        "EXASERVE_RUN_LOG_DIR",
        "EXASERVE_COMPAT_PROFILE_ID",
        "EXASERVE_COMPAT_OVERLAY_ROOT",
        "EXASERVE_COMPAT_SOURCES_NODE_PROFILE",
        "EXASERVE_COMPAT_SOURCES_NODE_MANIFEST",
    }
    filtered = {
        name: value
        for name, value in selected.items()
        if name in exact_names
        or name.startswith("EXASERVE_LOCAL_")
        or name.startswith(("FI_", "MPICH_"))
    }
    shared_values = {
        name: value
        for name, value in filtered.items()
        if any(root in value for root in ("/home/", "/lus/flare/"))
    }
    if shared_values:
        raise RuntimeError(
            f"closed replay worker environment retains shared paths: {sorted(shared_values)}"
        )
    return filtered


def _run_replay_client(
    run_plan,
    base_urls: Iterable[str],
    *,
    topology_arm: str | None = None,
    log_name: str = "replay.log",
    backend_process: subprocess.Popen | None = None,
    runtime_capsule_manifest_path: str | None = None,
    runtime_generation: int | None = None,
) -> int:
    env = os.environ.copy()
    env["PYTHONNOUSERSITE"] = "1"
    env["PYTHONSAFEPATH"] = "1"
    replay_python = sys.executable
    replay_cwd = run_plan.repo_root
    runtime_root = env.get("EXASERVE_LOCAL_RUNTIME_ROOT")
    if run_plan.client.num_nodes > 1:
        required_capsule_env = {
            "EXASERVE_LOCAL_RUNTIME_ROOT",
            "EXASERVE_LOCAL_GO_DISPATCH",
            "EXASERVE_LOCAL_STATE_ROOT",
            "EXASERVE_LOCAL_EVAL_MANIFEST",
            "EXASERVE_LOCAL_RUN_PLAN_PATH",
            "EXASERVE_LOCAL_PLAN_PATH",
            "EXASERVE_QUALIFIED_PYTHON",
            "EXASERVE_QUALIFIED_PYTHON_SHA256",
            "EXASERVE_QUALIFIED_PYTHON_SITE_PROFILE_HASH",
            "EXASERVE_COMPAT_SOURCES_NODE_PROFILE",
            "EXASERVE_COMPAT_SOURCES_NODE_MANIFEST",
        }
        if runtime_capsule_manifest_path is not None and runtime_generation is not None:
            capsule_env = dict(
                _validated_replay_capsule_environment(
                    os.path.abspath(runtime_capsule_manifest_path),
                    run_plan.semantic_plan.deployment.deployment_id,
                    runtime_generation,
                    run_plan.deployment_plan_hash,
                    run_plan.semantic_plan.deployment.site_profile_hash,
                    run_plan.semantic_plan.deployment.compatibility_profile_hash,
                    run_plan.semantic_plan.deployment.manifest_hash,
                    run_plan.semantic_plan.deployment.vendor,
                    run_plan.semantic_plan.deployment.engine,
                    run_plan.semantic_plan.deployment.runtime.null_compute,
                    run_plan.scheduler.nodes,
                )
            )
            env = _closed_replay_worker_environment(run_plan, capsule_env)
            runtime_root = env.get("EXASERVE_LOCAL_RUNTIME_ROOT")
        elif not required_capsule_env <= set(env):
            if runtime_capsule_manifest_path is None or runtime_generation is None:
                missing = sorted(required_capsule_env - set(env))
                raise RuntimeError(
                    "multi-rank replay has no validated runtime capsule receipt; "
                    f"missing environment {missing}"
                )
        else:
            env = _closed_replay_worker_environment(
                run_plan, {name: env[name] for name in required_capsule_env}
            )
            runtime_root = env.get("EXASERVE_LOCAL_RUNTIME_ROOT")
        if not runtime_root or not os.path.isabs(runtime_root):
            raise RuntimeError("multi-rank replay requires EXASERVE_LOCAL_RUNTIME_ROOT")
        runtime_root = os.path.realpath(runtime_root)
        python_root = os.path.realpath(os.path.join(runtime_root, "python"))
        go_binary = env.get("EXASERVE_LOCAL_GO_DISPATCH")
        state_root = env.get("EXASERVE_LOCAL_STATE_ROOT")
        qualified_python = env.get("EXASERVE_QUALIFIED_PYTHON")
        for name, path in (
            ("runtime Python tree", python_root),
            ("local Go replay binary", go_binary),
        ):
            if not path or not os.path.isabs(path):
                raise RuntimeError(f"multi-rank replay requires an absolute {name}")
            resolved = os.path.realpath(path)
            if os.path.commonpath((runtime_root, resolved)) != runtime_root:
                raise RuntimeError(f"{name} escapes EXASERVE_LOCAL_RUNTIME_ROOT")
        if not os.path.isdir(python_root):
            raise RuntimeError("local runtime Python tree is missing")
        go_binary = os.path.realpath(go_binary)
        if not os.path.isfile(go_binary) or not os.access(go_binary, os.X_OK):
            raise RuntimeError("local Go replay binary is missing or not executable")
        if not state_root or not os.path.isabs(state_root) or not os.path.isdir(state_root):
            raise RuntimeError("multi-rank replay requires an existing EXASERVE_LOCAL_STATE_ROOT")
        if (
            not qualified_python
            or not os.path.isabs(qualified_python)
            or not os.path.isfile(qualified_python)
            or not os.access(qualified_python, os.X_OK)
        ):
            raise RuntimeError("multi-rank replay requires an executable EXASERVE_QUALIFIED_PYTHON")
        replay_python = os.path.realpath(qualified_python)
        replay_cwd = python_root
        env["PYTHONPATH"] = python_root
        resolved_state = os.path.realpath(state_root)
        for name in ("HOME", "TMPDIR", "XDG_CACHE_HOME", "HF_HOME"):
            value = env.get(name)
            if (
                not value
                or not os.path.isabs(value)
                or os.path.commonpath((resolved_state, os.path.realpath(value))) != resolved_state
            ):
                raise RuntimeError(f"multi-rank replay {name} is not under local state root")
    else:
        # A staged runtime is preferred for one rank as well, but preserve the
        # head-only compatibility path for local diagnostics/materializations.
        if runtime_root and os.path.isdir(os.path.join(runtime_root, "python")):
            replay_cwd = os.path.realpath(os.path.join(runtime_root, "python"))
            env["PYTHONPATH"] = replay_cwd
            qualified_python = env.get("EXASERVE_QUALIFIED_PYTHON")
            if qualified_python:
                replay_python = os.path.realpath(qualified_python)
        else:
            env["PYTHONPATH"] = run_plan.repo_root
    timeout_s = _replay_process_timeout_s(run_plan)
    replay_cmd = [
        replay_python,
        "-m",
        "eval.replay_client",
        "--config",
        (
            env["EXASERVE_LOCAL_EVAL_MANIFEST"]
            if run_plan.client.num_nodes > 1
            else run_plan.runtime_manifest_path
        ),
        "--base-urls",
        ",".join(base_urls),
    ]
    if topology_arm:
        replay_cmd.extend(["--dispatch-topology", topology_arm, "--result-subdir", topology_arm])

    def backend_exit_reason() -> str | None:
        if backend_process is None:
            return None
        return_code = backend_process.poll()
        if return_code is None:
            return None
        return f"deployment backend exited with code {return_code}"

    if run_plan.client.num_nodes > 1:
        hostfile = _build_hostfile(run_plan.client.num_nodes)
        try:
            _validate_replay_hostfile_binding(
                hostfile,
                binding_path=env.get("EXASERVE_ALLOCATION_BINDING_PATH", ""),
                run_plan=run_plan,
            )
            # Fan the load generator out to `client.num_nodes` client nodes.
            # PBS/PALS uses mpiexec --hostfile; Slurm uses srun --nodelist
            # (Cray/Slurm sites have no mpiexec).
            if getattr(run_plan.scheduler, "type", "pbs") == "slurm":
                from exaserve.state.atomic import regular_file_reader

                with regular_file_reader(hostfile) as _hf:
                    _nodes = [ln.strip() for ln in _hf if ln.strip()]
                command = [
                    "srun",
                    f"--nodes={run_plan.client.num_nodes}",
                    "--ntasks-per-node=1",
                    "--cpu-bind=none",
                    f"--export=NONE,{','.join(sorted(env))}",
                    f"--chdir={replay_cwd}",
                    f"--nodelist={','.join(_nodes)}",
                    *replay_cmd,
                ]
            else:
                command = [
                    "mpiexec",
                    "--genvnone",
                    "--envnone",
                    "--shared",
                    "--envlist",
                    ",".join(sorted(env)),
                    "-n",
                    str(run_plan.client.num_nodes),
                    "--ppn",
                    "1",
                    "--cpu-bind",
                    "none",
                    "--hostfile",
                    hostfile,
                    "--wdir",
                    replay_cwd,
                    *replay_cmd,
                ]
            result = _run_command_with_tee(
                command,
                log_path=os.path.join(run_plan.bundle.logs_dir, log_name),
                cwd=replay_cwd,
                env={**os.environ, **env},
                timeout_s=timeout_s,
                abort_check=backend_exit_reason,
            )
        except BaseException as exc:
            try:
                os.remove(hostfile)
            except FileNotFoundError:
                pass
            except OSError as cleanup_exc:
                add_exception_note(
                    exc,
                    f"temporary replay hostfile cleanup also failed: {cleanup_exc}",
                )
            raise
        try:
            os.remove(hostfile)
        except FileNotFoundError:
            pass
        except OSError as exc:
            raise RuntimeError(
                f"could not remove temporary replay hostfile {hostfile}: {exc}"
            ) from exc
        return result

    return _run_command_with_tee(
        replay_cmd,
        log_path=os.path.join(run_plan.bundle.logs_dir, log_name),
        cwd=replay_cwd,
        env=env,
        timeout_s=timeout_s,
        abort_check=backend_exit_reason,
    )


def _build_hostfile(client_nodes: int) -> str:
    nodefile = os.environ.get("EXASERVE_NODEFILE") or os.environ.get("PBS_NODEFILE")
    if not nodefile or not os.path.isfile(nodefile):
        raise RuntimeError("EXASERVE_NODEFILE (or PBS_NODEFILE) is required for multi-node replay")
    nodes = []
    from exaserve.state.atomic import regular_file_reader

    with regular_file_reader(nodefile) as handle:
        for line in handle:
            node = line.strip()
            if node and node not in nodes:
                nodes.append(node)
    if len(nodes) < client_nodes:
        raise RuntimeError(
            f"Requested {client_nodes} replay client nodes, but only found {len(nodes)} in PBS_NODEFILE"
        )
    fd, path = tempfile.mkstemp(prefix="exaserve_eval_hosts_", text=True, dir="/tmp")
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        for node in nodes[:client_nodes]:
            handle.write(node + "\n")
    return path


def _validate_replay_hostfile_binding(hostfile: str, *, binding_path: str, run_plan) -> None:
    """Bind PALS/Slurm rank order to the canonical allocation rank order."""
    if not binding_path or not os.path.isabs(binding_path):
        raise RuntimeError("multi-node replay requires a local AllocationBinding path")
    from exaserve.plan.contracts import same_node
    from exaserve.plan.io import load_allocation_binding
    from exaserve.state.atomic import regular_file_reader

    binding = load_allocation_binding(binding_path)
    if (
        binding.deployment_plan_hash != run_plan.deployment_plan_hash
        or binding.site_profile_hash != run_plan.semantic_plan.deployment.site_profile_hash
    ):
        raise RuntimeError("replay AllocationBinding belongs to another deployment")
    with regular_file_reader(hostfile) as handle:
        nodes = [line.strip() for line in handle if line.strip()]
    expected_count = run_plan.client.num_nodes
    expected = [binding.node_for(rank) for rank in range(expected_count)]
    if (
        len(nodes) != expected_count
        or any(not node for node in expected)
        or any(not same_node(observed, planned) for observed, planned in zip(nodes, expected))
    ):
        raise RuntimeError(
            "replay hostfile rank order disagrees with canonical AllocationBinding: "
            f"observed={nodes}, expected={expected}"
        )


def _replay_process_timeout_s(run_plan) -> float:
    """Return a finite outer deadline for one replay/MPI process group.

    The Go client has a per-request deadline, but a launcher, barrier, or pipe
    can still wedge outside that deadline.  Budget one request drain and one
    shard assembly per repeat, plus dispatch, warm-up, and inter-run cooldown.
    Every term is bound into the canonical RunPlan; inherited environment
    variables cannot change experiment completion semantics.
    """
    runs = max(1, int(run_plan.client.num_runs))
    dispatch_s = max(0.0, float(run_plan.workload.duration))
    warmup_s = max(0.0, float(run_plan.client.warmup_duration_s))
    request_timeout_s = float(run_plan.client.request_timeout_s)
    # The canonical field retains its v2 name for manifest compatibility; it
    # now bounds supervised MPI result transfer/reduction, not file shards.
    shard_timeout_s = float(run_plan.client.shard_timeout_s)
    for name, value in (
        ("client.request_timeout_s", request_timeout_s),
        ("client.shard_timeout_s", shard_timeout_s),
    ):
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"{name} must be finite and positive")
    cooldown_s = 75.0 * max(0, runs - 1)
    return max(
        300.0,
        runs * (dispatch_s + request_timeout_s + shard_timeout_s) + warmup_s + cooldown_s + 120.0,
    )


def _run_command_with_tee(
    cmd,
    *,
    log_path: str,
    cwd: str,
    env: dict[str, str],
    timeout_s: float,
    abort_check: Callable[[], str | None] | None = None,
) -> int:
    if not math.isfinite(timeout_s) or timeout_s <= 0:
        raise ValueError("process timeout must be finite and positive")
    os.makedirs(os.path.dirname(os.path.abspath(log_path)), exist_ok=True)
    process = subprocess.Popen(
        cmd,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        start_new_session=True,
    )
    reader_error: list[BaseException] = []

    def drain() -> None:
        try:
            with open(log_path, "a", encoding="utf-8") as log_handle:
                assert process.stdout is not None
                for line in process.stdout:
                    print(line, end="", flush=True)
                    log_handle.write(line)
                    log_handle.flush()
        except BaseException as exc:  # propagate thread failures to the owner
            reader_error.append(exc)

    reader = threading.Thread(target=drain, name="exaserve-replay-log", daemon=True)
    reader.start()
    boundary_error: BaseException | None = None
    return_code: int | None = None
    deadline = time.monotonic() + timeout_s
    try:
        while True:
            if abort_check is not None:
                reason = abort_check()
                if reason is not None:
                    boundary_error = RuntimeError(
                        f"replay aborted because {reason}; diagnostics: {log_path}"
                    )
                    break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                boundary_error = TimeoutError(
                    f"replay process exceeded its {timeout_s:.1f}s deadline; "
                    f"diagnostics: {log_path}"
                )
                break
            try:
                return_code = process.wait(timeout=min(0.5, remaining))
                break
            except subprocess.TimeoutExpired:
                continue
    except BaseException as exc:
        boundary_error = exc

    # A launcher exiting is not sufficient proof that its process tree exited:
    # an MPI child (or any descendant inheriting stdout) can survive the leader,
    # keep the log reader blocked forever, and leak into the rest of the job.
    # Check the immutable session id after the leader is reaped and always close
    # that owned boundary before inspecting the reader.
    escaped_after_clean_exit = boundary_error is None and process_group_exists(process.pid)
    cleanup_error: BaseException | None = None
    cleanup_deadline = time.monotonic() + 20.0
    if process.poll() is None or process_group_exists(process.pid):
        try:
            terminate_process_tree(
                process,
                process_group=process.pid,
                deadline=cleanup_deadline,
            )
        except BaseException as exc:
            cleanup_error = exc

    reader.join(timeout=max(0.0, cleanup_deadline - time.monotonic()))
    if reader.is_alive():
        cleanup_error = cleanup_error or RuntimeError(
            "replay diagnostic reader did not stop by the shared cleanup deadline"
        )
    elif process.stdout is not None and not process.stdout.closed:
        process.stdout.close()
    if cleanup_error is not None:
        if boundary_error is not None:
            add_exception_note(
                boundary_error, f"replay process cleanup also failed: {cleanup_error}"
            )
        else:
            raise RuntimeError(f"replay process cleanup failed: {cleanup_error}") from cleanup_error
    if reader_error:
        reader_failure = RuntimeError(f"replay diagnostic reader failed: {reader_error[0]}")
        if boundary_error is not None:
            add_exception_note(boundary_error, str(reader_failure))
        else:
            raise reader_failure from reader_error[0]
    if boundary_error is not None:
        raise boundary_error
    if escaped_after_clean_exit:
        raise RuntimeError(
            f"replay launcher exited but left descendants in process group {process.pid}; "
            "the escaped processes were terminated"
        )
    if return_code is None:
        raise RuntimeError("replay process ended without a return code")
    return int(return_code)


def _validate_replay_results(run_plan) -> dict:
    """Validate every declared replay arm; no trailing/partial JSON is repaired."""
    from exaserve.state.atomic import strict_json_load_path

    search_dirs = [
        os.path.join(run_plan.bundle.results_dir, arm)
        for arm in list(getattr(run_plan.client, "dispatch_topologies", []) or [])
    ] or [run_plan.bundle.results_dir]
    arm_names = list(getattr(run_plan.client, "dispatch_topologies", []) or [])
    if not arm_names:
        arm_names = ["default"]
    entries: dict[str, str] = {}
    expected_ids = tuple(f"replay/{arm}" for arm in arm_names)
    incomplete_reasons: list[str] = []
    totals = {"requests_completed": 0, "requests_scheduled": 0, "errors": 0}
    for arm, directory in zip(arm_names, search_dirs):
        try:
            result_path = _exact_result_path(directory)
        except ValueError as exc:
            incomplete_reasons.append(f"replay arm {arm}: {exc}")
            continue
        if result_path is None:
            incomplete_reasons.append(f"replay arm {arm}: result file missing")
            continue
        try:
            payload = strict_json_load_path(result_path)
        except (OSError, ValueError) as exc:
            incomplete_reasons.append(f"replay arm {arm}: invalid complete JSON: {exc}")
            continue
        overall = payload.get("overall")
        if not isinstance(overall, dict):
            if payload.get("__type__") == "summary" or payload.get("saturation_rate") is not None:
                overall = payload
            else:
                incomplete_reasons.append(f"replay arm {arm}: missing overall summary")
                continue
        counts = {}
        for name, default in (
            ("requests_completed", 0),
            ("requests_scheduled", overall.get("requests_completed", 0)),
            ("errors", 0),
        ):
            value = overall.get(name, default)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                incomplete_reasons.append(f"replay arm {arm}: {name} must be a nonnegative integer")
                counts[name] = 0
            else:
                counts[name] = value
        completed = counts["requests_completed"]
        scheduled = counts["requests_scheduled"]
        errors = counts["errors"]
        if errors > completed or completed > scheduled:
            incomplete_reasons.append(
                f"replay arm {arm}: inconsistent request counts "
                f"scheduled={scheduled}, completed={completed}, errors={errors}"
            )
        if completed - errors < 1:
            incomplete_reasons.append(f"replay arm {arm}: all requests failed")
        if errors:
            incomplete_reasons.append(f"replay arm {arm}: {errors} request error(s)")
        if completed < scheduled:
            incomplete_reasons.append(f"replay arm {arm}: only {completed}/{scheduled} completed")
        meta = payload.get("meta")
        expected_ranks = int(run_plan.client.num_nodes)
        expected_runs = int(run_plan.client.num_runs)
        gathers = meta.get("gather_by_run") if isinstance(meta, dict) else None
        if (
            not isinstance(gathers, list)
            or len(gathers) != expected_runs
            or any(not _valid_gather_evidence(item, expected_ranks) for item in gathers)
        ):
            incomplete_reasons.append(
                f"replay arm {arm}: missing, incomplete, or invalid typed "
                f"gather evidence for {expected_runs} run(s) x {expected_ranks} client rank(s)"
            )
        if not isinstance(meta, dict) or meta.get("completed_runs") != expected_runs:
            incomplete_reasons.append(
                f"replay arm {arm}: completed_runs does not equal declared num_runs={expected_runs}"
            )
        per_run = payload.get("per_run")
        if not isinstance(per_run, list) or len(per_run) != expected_runs:
            incomplete_reasons.append(
                f"replay arm {arm}: per_run does not cover declared num_runs={expected_runs}"
            )
        else:
            for run_index, summary in enumerate(per_run):
                reason = _per_run_incomplete_reason(summary, run_index)
                if reason:
                    incomplete_reasons.append(f"replay arm {arm}: {reason}")
        entries[f"replay/{arm}"] = result_path
        totals["requests_completed"] += completed
        totals["requests_scheduled"] += scheduled
        totals["errors"] += errors
    if incomplete_reasons:
        print(
            f"Replay completed with partial results: {'; '.join(incomplete_reasons)}",
            flush=True,
        )

    return {
        "result_path": next(iter(entries.values()), ""),
        **totals,
        "incomplete_reasons": incomplete_reasons,
        "result_entries": entries,
        "expected_ids": expected_ids,
    }


def _valid_gather_evidence(gather, expected_ranks: int) -> bool:
    """Exact client-rank completeness; legacy absent/count-only data is invalid."""
    if not isinstance(gather, dict) or set(gather) != {
        "schema_version",
        "expected_ranks",
        "collected_ranks",
        "missing_ranks",
        "complete",
        "shards",
    }:
        return False
    expected = list(range(expected_ranks))
    if (
        type(gather.get("schema_version")) is not int
        or gather.get("schema_version") != 1
        or gather.get("expected_ranks") != expected_ranks
        or gather.get("collected_ranks") != expected
        or gather.get("missing_ranks") != []
        or gather.get("complete") is not True
    ):
        return False
    shards = gather.get("shards")
    if not isinstance(shards, list) or len(shards) != expected_ranks:
        return False
    seen = []
    for shard in shards:
        if not isinstance(shard, dict) or set(shard) != {
            "rank",
            "size_bytes",
            "sha256",
            "transport",
        }:
            return False
        rank = shard.get("rank")
        size = shard.get("size_bytes")
        digest = shard.get("sha256")
        if (
            isinstance(rank, bool)
            or not isinstance(rank, int)
            or isinstance(size, bool)
            or not isinstance(size, int)
            or size < 0
            or not isinstance(digest, str)
            or len(digest) != 64
            or any(char not in "0123456789abcdef" for char in digest)
            or shard.get("transport") not in {"in_memory", "mpi_chunked", "mpi_reduce"}
        ):
            return False
        seen.append(rank)
    return seen == expected


def _per_run_incomplete_reason(summary, run_index: int) -> str:
    if not isinstance(summary, dict) or summary.get("run_index") != run_index:
        return f"per_run[{run_index}] has invalid identity"
    counts = []
    for name in ("requests_completed", "requests_scheduled", "errors"):
        value = summary.get(name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return f"per_run[{run_index}].{name} must be a nonnegative integer"
        counts.append(value)
    completed, scheduled, errors = counts
    if errors > completed or completed > scheduled:
        return f"per_run[{run_index}] has inconsistent request counts"
    if errors or completed < scheduled:
        return (
            f"per_run[{run_index}] is incomplete: scheduled={scheduled}, "
            f"completed={completed}, errors={errors}"
        )
    return ""


def _finite_nonnegative(value, label: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) < 0
    ):
        raise RuntimeError(f"startup measurement {label} must be finite and non-negative")
    return float(value)


def _capture_startup_measurement(run_plan, launched, *, ready_evidence_path: str) -> dict[str, str]:
    """Seal and summarize the canonical deployment trace for startup-only runs."""
    from exaserve.state.atomic import (
        atomic_create_or_verify_bytes,
        atomic_create_or_verify_json,
        regular_file_reader,
        strict_json_load_path,
        strict_json_loads,
    )

    status_dir = launched.monitor.status_dir
    if not isinstance(status_dir, str) or not status_dir:
        raise RuntimeError("startup measurement has no deployment status directory")
    trace_source = os.path.join(status_dir, "scaling_trace.json")
    trace = strict_json_load_path(trace_source)
    if not isinstance(trace, dict):
        raise RuntimeError("startup scaling trace must be an object")
    metadata = trace.get("metadata")
    phases = trace.get("phases")
    api_calls = trace.get("api_calls")
    events = trace.get("events")
    replicas = trace.get("replicas")
    if not all(isinstance(value, list) for value in (phases, api_calls, events, replicas)):
        raise RuntimeError("startup scaling trace collections are malformed")
    if not isinstance(metadata, dict):
        raise RuntimeError("startup scaling trace metadata is malformed")

    plan = run_plan.semantic_plan.deployment
    from exaserve.control.plan_readiness import planned_application_names

    expected_replicas = sum(model.num_replicas for model in plan.models)
    expected_applications = len(planned_application_names(plan))
    application_layout = (
        "node_grouped_null"
        if any(plan.node_grouped_null_application_groups(model) for model in plan.models)
        else ("native_head_only" if plan.uses_head_only_serve_proxy() else "per_replica")
    )
    expected_metadata = {
        "deployment_plan_hash": run_plan.deployment_plan_hash,
        "generation": launched.monitor.expected_generation,
        "run_semantic_hash": run_plan.run_semantic_hash,
        "source_snapshot_hash": run_plan.source_snapshot_hash,
        "num_nodes": plan.num_nodes,
        "expected_model_replicas": expected_replicas,
        "expected_serve_applications": expected_applications,
        "expected_receipt_requirements": len(plan.receipt_requirements),
        "serve_application_layout": application_layout,
    }
    for name, expected in expected_metadata.items():
        if metadata.get(name) != expected:
            raise RuntimeError(
                f"startup scaling trace {name}={metadata.get(name)!r} != {expected!r}"
            )
    expected_slots = {
        replica.replica_id: (model.model_id, replica.replica_index)
        for model in plan.models
        for replica in model.replicas
    }
    if len(expected_slots) != expected_replicas:
        raise RuntimeError("canonical plan contains duplicate startup replica slots")
    observed_slots = set()
    null_compute = metadata.get("null_compute")
    if type(null_compute) is not bool or null_compute is not plan.runtime.null_compute:
        raise RuntimeError("startup scaling trace null_compute flag is malformed")
    for index, replica in enumerate(replicas):
        if not isinstance(replica, dict):
            raise RuntimeError(f"startup scaling trace replica[{index}] is malformed")
        slot = replica.get("component_slot")
        expected_identity = expected_slots.get(slot)
        if expected_identity is None:
            raise RuntimeError(
                f"startup scaling trace replica[{index}] names unplanned slot {slot!r}"
            )
        if slot in observed_slots:
            raise RuntimeError(f"startup scaling trace duplicates replica slot {slot!r}")
        if (replica.get("model_id"), replica.get("replica_index")) != expected_identity:
            raise RuntimeError(
                f"startup scaling trace replica slot {slot!r} has the wrong model/index"
            )
        if replica.get("null_compute") is not null_compute:
            raise RuntimeError(f"startup scaling trace replica slot {slot!r} has inconsistent mode")
        total_init_s = _finite_nonnegative(
            replica.get("total_init_s"), f"replica[{index}].total_init_s"
        )
        _finite_nonnegative(replica.get("wall_start"), f"replica[{index}].wall_start")
        _finite_nonnegative(replica.get("wall_end"), f"replica[{index}].wall_end")
        monotonic_start = _finite_nonnegative(
            replica.get("monotonic_start"), f"replica[{index}].monotonic_start"
        )
        monotonic_end = _finite_nonnegative(
            replica.get("monotonic_end"), f"replica[{index}].monotonic_end"
        )
        if (
            monotonic_end < monotonic_start
            or abs(total_init_s - (monotonic_end - monotonic_start)) > 0.01
        ):
            raise RuntimeError(
                f"startup scaling trace replica slot {slot!r} timing is inconsistent"
            )
        observed_slots.add(slot)
    if observed_slots != set(expected_slots):
        missing_slots = sorted(set(expected_slots) - observed_slots)
        raise RuntimeError(
            "startup scaling trace lacks exact replica coverage: "
            f"observed={len(observed_slots)}/{expected_replicas}, missing={missing_slots[:8]}"
        )

    phase_rows = []
    phase_names = set()
    for index, phase in enumerate(phases):
        if not isinstance(phase, dict) or not isinstance(phase.get("name"), str):
            raise RuntimeError(f"startup scaling trace phase[{index}] is malformed")
        duration = _finite_nonnegative(phase.get("duration_s"), f"phase[{index}].duration_s")
        phase_names.add(phase["name"])
        phase_rows.append({"name": phase["name"], "duration_s": duration})
    required_phases = {"ray.init", "serve.start", "deploy_from_canonical_plan", "stage3.total"}
    missing = sorted(required_phases - phase_names)
    if missing:
        raise RuntimeError(f"startup scaling trace is missing required phases: {missing}")

    api_summary: dict[str, dict[str, float | int]] = {}
    for index, call in enumerate(api_calls):
        if not isinstance(call, dict) or not isinstance(call.get("label"), str):
            raise RuntimeError(f"startup scaling trace api_calls[{index}] is malformed")
        duration = _finite_nonnegative(call.get("duration_s"), f"api_calls[{index}].duration_s")
        item = api_summary.setdefault(
            call["label"], {"count": 0, "total_duration_s": 0.0, "max_duration_s": 0.0}
        )
        item["count"] = int(item["count"]) + 1
        item["total_duration_s"] = float(item["total_duration_s"]) + duration
        item["max_duration_s"] = max(float(item["max_duration_s"]), duration)

    if not isinstance(ready_evidence_path, str) or not ready_evidence_path:
        raise RuntimeError("startup measurement has no sealed READY evidence path")
    with regular_file_reader(ready_evidence_path, binary=True) as handle:
        ready_evidence_bytes = handle.read()
    ready_evidence = strict_json_loads(ready_evidence_bytes.decode("utf-8"))
    readiness_snapshot = (
        ready_evidence.get("readiness_snapshot", {}) if isinstance(ready_evidence, dict) else {}
    )
    if (
        not isinstance(ready_evidence, dict)
        or not isinstance(readiness_snapshot, dict)
        or ready_evidence.get("schema_version") != 2
        or ready_evidence.get("state") != "READY"
        or ready_evidence.get("generation") != launched.monitor.expected_generation
        or ready_evidence.get("deployment_plan_hash") != run_plan.deployment_plan_hash
        or ready_evidence.get("run_semantic_hash") != run_plan.run_semantic_hash
        or readiness_snapshot.get("ready") is not True
    ):
        raise RuntimeError("startup measurement lacks the exact sealed READY generation")
    ready_evidence_revision = ready_evidence.get("revision")
    ready_transition_revision = ready_evidence.get("state_revision")
    if (
        isinstance(ready_evidence_revision, bool)
        or not isinstance(ready_evidence_revision, int)
        or isinstance(ready_transition_revision, bool)
        or not isinstance(ready_transition_revision, int)
        or ready_transition_revision < 0
        or ready_transition_revision > ready_evidence_revision
    ):
        raise RuntimeError("startup READY evidence revision identity is malformed")
    trace_start_monotonic = _finite_nonnegative(
        metadata.get("trace_start_monotonic"), "metadata.trace_start_monotonic"
    )
    trace_end_monotonic = _finite_nonnegative(
        metadata.get("trace_end_monotonic"), "metadata.trace_end_monotonic"
    )
    trace_boot_id = metadata.get("trace_clock_boot_id")
    ready_boot_id = ready_evidence.get("state_clock_boot_id")
    if (
        not isinstance(trace_boot_id, str)
        or not trace_boot_id
        or len(trace_boot_id) > 128
        or trace_boot_id != ready_boot_id
    ):
        raise RuntimeError("startup trace and READY transition do not share one boot clock")
    ready_transition_monotonic = _finite_nonnegative(
        ready_evidence.get("state_changed_monotonic"),
        "ready_evidence.state_changed_monotonic",
    )
    ready_after_trace_start_s = _finite_nonnegative(
        ready_transition_monotonic - trace_start_monotonic,
        "ready_after_trace_start_s",
    )
    trace_total_duration_s = _finite_nonnegative(
        metadata.get("total_duration_s"), "metadata.total_duration_s"
    )
    observed_trace_duration_s = _finite_nonnegative(
        trace_end_monotonic - trace_start_monotonic,
        "observed_trace_duration_s",
    )
    if abs(trace_total_duration_s - observed_trace_duration_s) > 0.01:
        raise RuntimeError("startup scaling trace total duration disagrees with monotonic anchors")

    trace_dest = os.path.join(run_plan.bundle.results_dir, "startup_scaling_trace.json")
    with regular_file_reader(trace_source, binary=True) as handle:
        atomic_create_or_verify_bytes(trace_dest, handle.read())
    summary_dest = os.path.join(run_plan.bundle.results_dir, "startup_metrics.json")
    atomic_create_or_verify_json(
        summary_dest,
        {
            "schema_version": 2,
            **expected_metadata,
            "gateway_kind": metadata.get("gateway_kind"),
            "exposure_mode": metadata.get("exposure_mode"),
            "null_compute": null_compute,
            "deployment_ready_evidence_sha256": hashlib.sha256(ready_evidence_bytes).hexdigest(),
            "ready_evidence_revision": ready_evidence_revision,
            "ready_evidence_updated_at": _finite_nonnegative(
                ready_evidence.get("updated_at"), "ready_evidence.updated_at"
            ),
            "ready_transition_revision": ready_transition_revision,
            "ready_transition_at": _finite_nonnegative(
                ready_evidence.get("state_changed_at"), "ready_evidence.state_changed_at"
            ),
            "ready_transition_monotonic": ready_transition_monotonic,
            "clock_boot_id": ready_boot_id,
            "ready_after_trace_start_s": ready_after_trace_start_s,
            "trace_total_duration_s": trace_total_duration_s,
            "phase_timings": phase_rows,
            "api_call_summary": api_summary,
            "event_count": len(events),
            "replica_measurement_count": len(replicas),
            "timing_semantics": {
                "clock": "same-boot monotonic timestamps; wall timestamps are diagnostic only",
                "trace_total_duration_s": "deployment child trace start through canonical app deployment",
                "ready_after_trace_start_s": "deployment child trace start through the immutable external READY transition",
                "phase_timings": "current canonical phase names; not legacy deploy_apps/wait_proxies",
            },
        },
    )
    return {
        "startup_scaling_trace": trace_dest,
        "startup_metrics": summary_dest,
    }


def _capture_startup_terminal_evidence(run_plan, launched) -> dict[str, str]:
    """Seal the already-validated shutdown report and terminal status."""
    from dataclasses import asdict

    from exaserve.state.atomic import (
        atomic_create_or_verify_bytes,
        atomic_create_or_verify_json,
        regular_file_reader,
        strict_json_load_path,
    )
    from exaserve.status_api import read_deployment_status

    status_dir = launched.monitor.status_dir
    report_source = os.path.join(status_dir, "shutdown_report.json")
    report = strict_json_load_path(report_source)
    if (
        not isinstance(report, dict)
        or report.get("clean") is not True
        or report.get("deadline_exhausted") is not False
        or report.get("errors") not in (None, [])
        or report.get("observed_terminal_state") != "STOPPED"
        or report.get("deployment_plan_hash") != run_plan.deployment_plan_hash
        or report.get("generation") != launched.monitor.expected_generation
    ):
        raise RuntimeError(f"startup shutdown report is not clean/exact: {report}")
    terminal = read_deployment_status(status_dir)
    if (
        terminal is None
        or terminal.state != "STOPPED"
        or terminal.generation != launched.monitor.expected_generation
        or terminal.deployment_plan_hash != run_plan.deployment_plan_hash
    ):
        raise RuntimeError("startup terminal status is missing or has the wrong identity")

    report_dest = os.path.join(run_plan.bundle.results_dir, "deployment_shutdown_report.json")
    with regular_file_reader(report_source, binary=True) as handle:
        atomic_create_or_verify_bytes(report_dest, handle.read())
    status_dest = os.path.join(run_plan.bundle.results_dir, "deployment_terminal_status.json")
    atomic_create_or_verify_json(status_dest, asdict(terminal))
    return {
        "deployment_shutdown_report": report_dest,
        "deployment_terminal_status": status_dest,
    }


def _capture_deployment_evidence(run_plan, launched) -> dict[str, str]:
    """Freeze and verify a recovered READY revision, receipts, and provenance.

    Continuous readiness deliberately revokes READY while a transient failure
    is inside its resolved recovery window.  A saturated replay can end during
    that VALIDATING interval even though the deployment recovers immediately
    once load drains.  Result publication must wait for that same bounded
    policy outcome instead of racing one status read; terminal state or expiry
    still fails closed.
    """
    from exaserve.evidence import capture_ready_evidence
    from exaserve.status_api import read_deployment_status

    status_dir = launched.monitor.status_dir
    if not isinstance(status_dir, str) or not status_dir:
        raise RuntimeError("launched backend has no typed deployment status path")
    readiness = run_plan.semantic_plan.deployment.readiness
    recovery_s = float(readiness.recovery_deadline_s)
    poll_s = min(1.0, float(readiness.validation_interval_s))
    deadline = time.monotonic() + recovery_s
    last_state = "UNAVAILABLE"
    last_detail = "deployment status unavailable"
    while True:
        status = read_deployment_status(status_dir)
        if status is not None:
            last_state = status.state
            last_detail = status.detail or status.reason_code or "no status detail"
            if status.ready:
                return capture_ready_evidence(
                    status_dir=status_dir,
                    destination_dir=run_plan.bundle.results_dir,
                    expected_generation=status.generation,
                    expected_plan_hash=run_plan.deployment_plan_hash,
                    expected_run_semantic_hash=run_plan.run_semantic_hash,
                )
            if status.terminal:
                raise RuntimeError(
                    "deployment became terminal before result commit: "
                    f"state={last_state}, detail={last_detail}"
                )
        process = launched.monitor.process
        if process is not None and process.poll() is not None:
            raise RuntimeError(
                "deployment exited before READY evidence commit: "
                f"exit={process.returncode}, last_state={last_state}, detail={last_detail}"
            )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError(
                "deployment did not recover canonical READY before result commit within "
                f"{recovery_s:g}s: state={last_state}, detail={last_detail}"
            )
        time.sleep(min(poll_s, remaining))


def _publish_result_manifest(run_plan, *, entries: dict[str, str], expected_ids, reasons):
    from exaserve.state.results import ResultEntry, ResultManifest, write_result_manifest

    materialized = []
    manifest_reasons = list(reasons)
    results_root = os.path.abspath(run_plan.bundle.results_dir)
    for logical_id, path in sorted(entries.items()):
        try:
            materialized.append(ResultEntry.from_file(logical_id, path, root=results_root))
        except (OSError, ValueError) as exc:
            manifest_reasons.append(f"{logical_id}: cannot hash result: {exc}")
    observed_ids = {entry.logical_id for entry in materialized}
    for missing in sorted(set(expected_ids) - observed_ids):
        message = f"required result {missing} is missing"
        if message not in manifest_reasons:
            manifest_reasons.append(message)
    manifest = ResultManifest(
        schema_version=2,
        run_id=run_plan.run_id,
        run_semantic_hash=run_plan.run_semantic_hash,
        deployment_plan_hash=run_plan.deployment_plan_hash,
        expected_ids=tuple(sorted(set(expected_ids))),
        entries=tuple(sorted(materialized, key=lambda item: item.logical_id)),
        incomplete_reasons=tuple(sorted(set(manifest_reasons))),
        generated_at=datetime.now(timezone.utc).isoformat(),
        complete=not manifest_reasons and observed_ids == set(expected_ids),
    ).finalize()
    write_result_manifest(
        os.path.join(run_plan.bundle.results_dir, "result_manifest.json"), manifest
    )
    return manifest


def _result_manifest_complete(path: str) -> bool:
    from exaserve.state.results import complete_result_manifest

    return complete_result_manifest(path)


def _exact_result_path(results_dir: str) -> str | None:
    """Return the one result owned by this immutable run bundle.

    A retry that would create ``result1.json`` needs a new materialization and
    semantic identity.  Picking the newest file would let stale/partial output
    silently replace the result bound to this run.
    """
    try:
        candidates = sorted(
            name for name in os.listdir(results_dir) if re.fullmatch(r"result\d+\.json", name)
        )
    except FileNotFoundError:
        return None
    if not candidates:
        return None
    if candidates != ["result0.json"]:
        raise ValueError(
            "result identity is ambiguous; expected only result0.json, "
            f"observed {candidates}. Materialize a new run rather than selecting newest."
        )
    return os.path.join(results_dir, "result0.json")


def _resolve_run_yaml(target: str) -> str:
    if os.path.isdir(target):
        candidate = os.path.join(target, "run.yaml")
        if os.path.isfile(candidate):
            return candidate
    if os.path.isfile(target):
        return os.path.abspath(target)
    raise FileNotFoundError(f"Could not resolve run.yaml from {target}")


# ---------------------------------------------------------------------------
# submit_all: batch-submit all pending runs for a spec
# ---------------------------------------------------------------------------

# Backends may report a qualified site limit.  In its absence submit-all uses
# one active job per queue as a local anti-flood throttle; this is deliberately
# not represented as an ALCF policy or quota.
_DEFAULT_QUEUE_SLOTS = 1
_MAX_SUBMIT_ATTEMPTS = 20  # PR-013: bound retries; then record a permanent failure
_MAX_OBSERVATION_FAILURES = 5
_MAX_QUEUE_WAIT_S = 12 * 3600
_POLL_INTERVAL_S = 300  # 5 min between polls — minimizes qstat load on login node


def submit_all(
    spec_name: str,
    *,
    run_group: str,
    experiments_root: str | None = None,
    dry_run: bool = False,
    poll_interval: int = _POLL_INTERVAL_S,
) -> int:
    """Submit all pending runs for *spec_name*, respecting per-queue limits.

    Runs that already succeeded (status ``succeeded`` **and** a result file
    exists) are skipped.  When a queue is full the function retries every
    *poll_interval* seconds until all runs have been submitted.

    A lock file in the run group directory prevents concurrent submit_all
    processes for the same spec.  Stale locks (owner PID dead) are cleaned
    automatically with a WARNING printed.
    """
    try:
        group_dir = resolve_run_group_dir(
            spec_name,
            run_group=run_group,
            experiments_root=experiments_root,
        )
    except FileNotFoundError as exc:
        print(str(exc))
        return 1

    # --- Cross-host exclusive lease (PR-013) ---
    # Atomic O_CREAT|O_EXCL acquisition (no check-then-write window); a live
    # lease from ANOTHER host is respected (the old code treated any foreign
    # host as stale and cleaned it, which duplicates jobs on a shared FS).
    from exaserve.state.atomic import ExclusiveLease, LeaseHeartbeat, LeaseHeldError

    lock_path = os.path.join(group_dir, ".submit_all.lock")
    try:
        lease = ExclusiveLease(
            lock_path, ttl_s=86400, owner_note=f"submit-all {spec_name}"
        ).acquire()
    except LeaseHeldError as exc:
        print(
            f"WARNING: submit-all for {spec_name!r} is already running "
            f"({exc.owner.get('host')}:{exc.owner.get('pid')}, "
            f"note={exc.owner.get('note')}). Refusing to start a second instance.",
            flush=True,
        )
        return 1

    heartbeat = LeaseHeartbeat(lease, interval_s=60.0)
    heartbeat_started = False
    try:
        heartbeat.start()
        heartbeat_started = True
        return _submit_all_locked(group_dir, spec_name, dry_run, poll_interval, heartbeat)
    finally:
        active_error = sys.exc_info()[1]
        heartbeat_error = None
        if heartbeat_started:
            try:
                heartbeat.stop(active_error)
            except BaseException as stop_exc:
                heartbeat_error = stop_exc
                if active_error is not None:
                    add_exception_note(
                        active_error, f"submit-all heartbeat cleanup also failed: {stop_exc}"
                    )
        try:
            lease.release()
        except BaseException as release_exc:
            if active_error is None and heartbeat_error is None:
                raise
            target_error = active_error or heartbeat_error
            assert target_error is not None
            add_exception_note(target_error, f"submit-all lease cleanup also failed: {release_exc}")
        if active_error is None and heartbeat_error is not None:
            raise heartbeat_error


def _submit_all_locked(
    group_dir: str,
    spec_name: str,
    dry_run: bool,
    poll_interval: int,
    heartbeat,
) -> int:
    heartbeat.ensure_held()
    scheduler_types = sorted(
        {getattr(run_plan.scheduler, "type", "pbs") for run_plan in _all_run_plans(group_dir)}
    )
    if len(scheduler_types) > 1:
        print(
            "Mixed scheduler types in one submit-all group are unsupported by "
            f"the PBS release control plane: {scheduler_types}. "
            "No scheduler was contacted and no jobs were submitted.",
            flush=True,
        )
        return 1
    reconciliation_errors = _reconcile_ambiguous_runs(group_dir)
    if reconciliation_errors:
        for error in reconciliation_errors:
            print(f"AMBIGUOUS: {error}", flush=True)
        print("No new jobs were submitted; resolve ambiguous scheduler ownership first.")
        return 1
    pending = _discover_pending_runs(group_dir)
    if not pending:
        print(f"No pending runs found under {group_dir}")
        return 0

    total = len(pending)
    print(f"Found {total} pending run(s) for {spec_name!r} in {os.path.basename(group_dir)!r}")

    scheduler = get_scheduler(getattr(pending[0].scheduler, "type", "pbs"))
    slot_limits = scheduler.slot_limits()

    if dry_run:
        for run_plan in pending:
            print(
                f"  [dry-run] {scheduler.name} submit {run_plan.bundle.job_path}  "
                f"({run_plan.run_group_id}/{run_plan.run_id}, queue={run_plan.scheduler.queue})"
            )
        return 0

    remaining = list(pending)
    submitted: list[str] = []
    failed: dict[str, str] = {}
    attempts: dict[str, int] = {}  # PR-013: per-run submission attempt counter
    user = getpass.getuser()
    observation_failures = 0
    wait_started = time.monotonic()

    while remaining:
        heartbeat.ensure_held()
        queue_counts = scheduler.count_queued(user)
        if queue_counts is None:
            # PR-014: scheduler unobservable — fail closed. Do not submit on
            # a blind count (that is how duplicate floods happen); wait and
            # re-observe.
            print(
                "  [submit-all] WARNING: scheduler queue counts unavailable "
                "(qstat/squeue failed); holding submissions for 30s.",
                flush=True,
            )
            observation_failures += 1
            if observation_failures >= _MAX_OBSERVATION_FAILURES:
                print(
                    f"scheduler remained unobservable for "
                    f"{observation_failures} attempts; aborting safely",
                    flush=True,
                )
                return 1
            time.sleep(min(30, max(1, poll_interval)))
            continue
        observation_failures = 0
        next_round: list = []

        for run_plan in remaining:
            queue = run_plan.scheduler.queue
            limit = slot_limits.get(queue, _DEFAULT_QUEUE_SLOTS)
            current = queue_counts.get(queue, 0)

            if limit is not None and current >= limit:
                next_round.append(run_plan)
                continue

            attempt = attempts.get(run_plan.run_id, 0) + 1
            heartbeat.ensure_held()
            kind, msg = _submit_run_once(run_plan, scheduler, submit_attempt=attempt)
            if kind in {"submitted", "reconciled", "attached"}:
                submitted.append(run_plan.run_id)
                queue_counts[queue] = current + 1
                print(
                    f"  [{len(submitted)}/{total}] Submitted "
                    f"{run_plan.run_group_id}/{run_plan.run_id}: {msg} ({kind})",
                    flush=True,
                )
            elif kind == "rejected":
                # Only a typed definite rejection may clear the durable intent
                # and enter the bounded retry path.
                attempts[run_plan.run_id] = attempt
                if attempt >= _MAX_SUBMIT_ATTEMPTS:
                    failed[run_plan.run_id] = msg
                    from .run_planner import write_run_state

                    try:
                        write_run_state(
                            run_plan,
                            "invalid",
                            last_submit_error=msg,
                            submit_attempts=attempt,
                        )
                    except Exception as exc:
                        failed[run_plan.run_id] += f"; terminal status publication failed: {exc}"
                    print(
                        f"  [{len(submitted)}/{total}] FAILED    "
                        f"{run_plan.run_group_id}/{run_plan.run_id} after "
                        f"{attempt} attempts: {msg}",
                        flush=True,
                    )
                else:
                    next_round.append(run_plan)
                    print(
                        f"  [{len(submitted)}/{total}] Deferred  "
                        f"{run_plan.run_group_id}/{run_plan.run_id} "
                        f"(definite rejection, attempt {attempt}): {msg}",
                        flush=True,
                    )
            else:
                failed[run_plan.run_id] = msg
                print(f"  AMBIGUOUS  {run_plan.run_id}: {msg}", flush=True)

        remaining = next_round
        if remaining:
            if time.monotonic() - wait_started >= _MAX_QUEUE_WAIT_S:
                print(
                    f"queue capacity did not become available within "
                    f"{_MAX_QUEUE_WAIT_S}s; aborting without altering pending runs",
                    flush=True,
                )
                return 1
            print(
                f"  {len(remaining)} run(s) waiting for queue slots, "
                f"retrying in {poll_interval}s ...",
                flush=True,
            )
            time.sleep(poll_interval)

    print(f"\nAll {len(submitted)}/{total} run(s) submitted.", flush=True)
    if failed:
        print("Permanent failures:")
        for rid, err in failed.items():
            print(f"  {rid}: {err}")
        return 1
    return 0


def _natural_sort_key(name: str):
    """Sort key that orders '2-nodes' before '16-nodes' before '128-nodes'."""
    import re

    return [int(s) if s.isdigit() else s.lower() for s in re.split(r"(\d+)", name)]


def _discover_pending_runs(group_dir: str):
    """Return run materializations that are not successfully completed."""
    pending = []
    for entry in sorted(os.listdir(group_dir), key=_natural_sort_key):
        run_yaml = os.path.join(group_dir, entry, "run.yaml")
        if not os.path.isfile(run_yaml):
            continue
        if _is_completed(os.path.join(group_dir, entry)):
            continue
        pending.append(load_run_plan(run_yaml))
    return pending


def _all_run_plans(group_dir: str):
    plans = []
    for entry in sorted(os.listdir(group_dir), key=_natural_sort_key):
        run_yaml = os.path.join(group_dir, entry, "run.yaml")
        if os.path.isfile(run_yaml):
            plans.append(load_run_plan(run_yaml))
    return plans


def _reconcile_ambiguous_runs(group_dir: str) -> list[str]:
    """Attach exact scheduler identities left by submit-then-persist crashes."""
    from exaserve.state.status import StatusStore

    errors: list[str] = []
    for run_plan in _all_run_plans(group_dir):
        store = StatusStore.run(run_plan.bundle.state_path)
        record = store.load()
        if record is None or record.state != "PLANNED":
            continue
        if (record.data or {}).get("phase") != "submitting":
            continue
        scheduler = get_scheduler(getattr(run_plan.scheduler, "type", "pbs"))
        attempt = (record.data or {}).get("submit_attempt", 1)
        if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
            attempt = 1
        kind, detail = _submit_run_once(run_plan, scheduler, submit_attempt=attempt)
        if kind not in {"reconciled", "attached"}:
            errors.append(f"{run_plan.run_id}: {detail}")
    return errors


# PR-013: a run in any of these states already has (or had) a scheduler job;
# re-running submit-all must NOT create a second job for it. "succeeded"
# additionally requires results on disk (below).
# "submitting" means a submit call may have reached the scheduler before we
# crashed — ambiguous, so it is treated as in-flight and requires explicit
# reconciliation rather than a blind resubmit (IMP-B09).
_IN_FLIGHT_STATES = {"SUBMITTED", "RUNNING"}


def _is_completed(run_dir: str) -> bool:
    """True if this run must be SKIPPED by submit-all discovery: it already
    succeeded (with results) or is in flight with a recorded scheduler job."""
    state_path = os.path.join(run_dir, "state", "status.json")
    if not os.path.isfile(state_path):
        return False
    from exaserve.state.atomic import strict_json_load_path

    try:
        state = strict_json_load_path(state_path)
    except (OSError, ValueError) as exc:
        raise RuntimeError(
            f"refusing submit discovery with unreadable run state {state_path}: {exc}"
        ) from exc
    if not isinstance(state, dict):
        raise RuntimeError(f"refusing submit discovery with non-object run state {state_path}")
    status = state.get("state")
    phase = (state.get("data") or {}).get("phase")
    if status == "PLANNED" and phase == "submitting":
        return True
    if status in _IN_FLIGHT_STATES:
        # Idempotency: don't resubmit a run we already handed to the scheduler.
        return True
    if status in {"PARTIAL", "FAILED", "CANCELLED", "INVALID"}:
        # IMP-B08: a partial run HAS executed. Do not silently resubmit it
        # (that would loop); it needs an explicit human decision. It is also
        # not "succeeded" — downstream analysis must see the partial state.
        return True
    if status != "SUCCEEDED":
        return False
    manifest = os.path.join(run_dir, "results", "result_manifest.json")
    return _result_manifest_complete(manifest)


# Job submission and per-queue counting now live on the scheduler backend
# (exaserve eval/lib/schedulers/): scheduler.submit() and scheduler.count_queued().
