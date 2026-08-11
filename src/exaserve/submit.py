"""Idempotent scheduler submission and canonical endpoint discovery.

``exaserve-serve-submit`` compiles and persists the immutable DeploymentPlan
and SiteProfile before scheduler submission.  ``exaserve-serve-url`` resolves
the exact submission record and returns only the endpoint published by the
canonical READY status; scheduler RUNNING, a log line, or an adjacent port file
is never treated as serving readiness.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .config import get_site_defaults
from .exception_notes import add_exception_note
from .schedulers import (
    JobSpec,
    SchedulerState,
    SubmissionRejected,
    default_queue_and_walltime,
    get_scheduler,
)
from .state.atomic import (
    ExclusiveLease,
    LeaseHeartbeat,
    atomic_create_json,
    atomic_create_text,
    atomic_write_json,
    atomic_write_text,
)


_SUBMISSION_SCHEMA_VERSION = 2
_ACTIVE_OR_UNCERTAIN = {
    SchedulerState.PENDING,
    SchedulerState.RUNNING,
    SchedulerState.HELD,
    SchedulerState.UNKNOWN,
}

# ``bootstrap_script`` is JobSpec's sole deliberate site-admin shell seam.
_DEFAULT_ENV_SETUP = """if [ -f "$HOME/script/env_aurora" ]; then
    source "$HOME/script/env_aurora"
else
    module load frameworks
fi"""


def _private_directory(path: Path) -> Path:
    from .state.atomic import ensure_owned_directory

    return Path(ensure_owned_directory(path))


def _registry_root() -> Path:
    configured = os.environ.get("EXASERVE_SUBMISSION_REGISTRY", "").strip()
    root = (
        Path(configured).expanduser()
        if configured
        else Path.home() / ".cache" / "exaserve" / "submissions"
    )
    return _private_directory(root)


def _registry_path(job_id: str) -> Path:
    if not isinstance(job_id, str) or not job_id.strip():
        raise ValueError("scheduler job id must be a non-empty string")
    digest = hashlib.sha256(job_id.strip().encode()).hexdigest()
    return _registry_root() / f"{digest}.json"


def _load_object(path: Path) -> Optional[dict]:
    from .state.atomic import strict_json_load_path

    try:
        payload = strict_json_load_path(path)
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"submission metadata {path} is unreadable: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"submission metadata {path} must be a JSON object")
    return payload


def _validate_submission_record(payload: dict, *, expected_job_id: str = "") -> dict:
    required = {
        "schema_version",
        "phase",
        "intent_id",
        "run_identity",
        "scheduler",
        "job_id",
        "deployment_id",
        "generation",
        "deployment_plan_hash",
        "run_dir",
        "plan_path",
        "site_profile_path",
        "job_script_path",
        "submitted_at",
    }
    if set(payload) != required:
        raise RuntimeError(
            "submission metadata shape mismatch: "
            f"missing={sorted(required - set(payload))}, "
            f"unknown={sorted(set(payload) - required)}"
        )
    if (
        type(payload["schema_version"]) is not int
        or payload["schema_version"] != _SUBMISSION_SCHEMA_VERSION
    ):
        raise RuntimeError("unsupported submission metadata schema")
    if payload["phase"] not in {"PREPARED", "SUBMITTING", "REJECTED", "SUBMITTED"}:
        raise RuntimeError("submission metadata phase is invalid")
    for field in (
        "intent_id",
        "run_identity",
        "scheduler",
        "deployment_id",
        "deployment_plan_hash",
        "run_dir",
        "plan_path",
        "site_profile_path",
        "job_script_path",
    ):
        if not isinstance(payload[field], str) or not payload[field]:
            raise RuntimeError(f"submission metadata {field} must be non-empty")
    if not re.fullmatch(r"[0-9a-f]{64}", payload["intent_id"]):
        raise RuntimeError("submission intent id must be a lowercase SHA-256 digest")
    if not re.fullmatch(r"[0-9a-f]{64}", payload["deployment_plan_hash"]):
        raise RuntimeError("submission plan hash must be a lowercase SHA-256 digest")
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,15}", payload["run_identity"]):
        raise RuntimeError("submission run identity is not scheduler-safe")
    for field in ("run_dir", "plan_path", "site_profile_path", "job_script_path"):
        if not os.path.isabs(payload[field]):
            raise RuntimeError(f"submission metadata {field} must be absolute")
    if payload["phase"] == "SUBMITTED" and (
        not isinstance(payload["job_id"], str) or not payload["job_id"]
    ):
        raise RuntimeError("submitted metadata lacks a scheduler job id")
    if payload["phase"] != "SUBMITTED" and payload["job_id"] != "":
        raise RuntimeError("non-submitted metadata must not invent a scheduler job id")
    if payload["phase"] == "SUBMITTED" and (
        not isinstance(payload["submitted_at"], str) or not payload["submitted_at"]
    ):
        raise RuntimeError("submitted metadata lacks a submission timestamp")
    if payload["phase"] == "PREPARED" and payload["submitted_at"] != "":
        raise RuntimeError("prepared metadata must not invent a submission timestamp")
    if payload["phase"] in {"SUBMITTING", "REJECTED"} and (
        not isinstance(payload["submitted_at"], str) or not payload["submitted_at"]
    ):
        raise RuntimeError("attempted submission metadata lacks an attempt timestamp")
    if expected_job_id and payload["job_id"] != expected_job_id:
        raise RuntimeError(
            f"submission registry belongs to job {payload['job_id']!r}, not {expected_job_id!r}"
        )
    if (
        isinstance(payload["generation"], bool)
        or not isinstance(payload["generation"], int)
        or payload["generation"] < 0
    ):
        raise RuntimeError("submission generation must be a non-negative integer")
    return payload


def _publish_submission_record(payload: dict, *, intent_path: Path) -> None:
    validated = _validate_submission_record(payload)
    atomic_write_json(intent_path, validated)
    if validated["phase"] == "SUBMITTED":
        registry_path = _registry_path(validated["job_id"])
        try:
            atomic_create_json(registry_path, validated)
        except FileExistsError:
            existing = _load_object(registry_path)
            if existing != validated:
                raise RuntimeError(
                    f"immutable submission registry {registry_path} already has different content"
                )


def _submission_intent_id(
    *,
    deployment_plan_hash: str,
    scheduler: str,
    queue: str,
    walltime: str,
    account: str,
) -> str:
    semantic_input = {
        "deployment_plan_hash": deployment_plan_hash,
        "scheduler": scheduler,
        "queue": queue,
        "walltime": walltime,
        "account": account,
    }
    return hashlib.sha256(
        json.dumps(semantic_input, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _write_job_id(job_id_file: Optional[str | os.PathLike], cfg_path: Path, job_id: str) -> None:
    job_id_path = Path(job_id_file) if job_id_file else cfg_path.with_suffix(".jobid")
    job_id_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(job_id_path, job_id + "\n")


def _new_prepared_submission(
    *,
    cfg_path: Path,
    log_dir: Path,
    scheduler,
    defaults,
    plan,
    site_profile,
    queue: str,
    walltime: str,
    project_account: str,
    job_name: Optional[str],
    intent_id: str,
) -> tuple[dict, str]:
    """Persist canonical artifacts and render one exact scheduler job."""
    label = re.sub(r"[^A-Za-z0-9]", "", job_name or "es")[:2] or "es"
    attempt = secrets.token_hex(8)
    run_identity = f"{label}-{attempt[:12]}"  # <= 15, including prefix separator
    deployment_id = f"serve-{attempt}"
    generation = time.time_ns()

    from .plan.io import write_deployment_plan, write_site_profile

    # The plan was compiled before scheduler defaults were selected so queue
    # policy is derived from canonical topology.  Bind this attempt's identity
    # without recompiling or creating a competing interpretation of the YAML.
    from dataclasses import replace

    plan = replace(plan, deployment_id=deployment_id).finalize()
    artifact_dir = _private_directory(log_dir / "submissions" / deployment_id)
    run_dir = log_dir / "runs" / f"{deployment_id}-g{generation}"
    plan_path = artifact_dir / "deployment.plan.json"
    site_profile_path = artifact_dir / "site.profile.json"
    write_deployment_plan(str(plan_path), plan)
    write_site_profile(str(site_profile_path), site_profile)

    environment = {
        "EXASERVE_DEPLOYMENT_ID": deployment_id,
        "EXASERVE_GENERATION": str(generation),
        "EXASERVE_RUN_LOG_DIR": str(run_dir),
        "EXASERVE_SITE_PROFILE_PATH": str(site_profile_path),
    }
    job_spec = JobSpec(
        command_argv=(sys.executable, "-u", "-m", "exaserve.launcher", str(plan_path)),
        cwd=cfg_path.parent,
        environment=environment,
        environment_unset=tuple(site_profile.environment_unset),
        bootstrap_script=_DEFAULT_ENV_SETUP,
        num_nodes=plan.num_nodes,
        walltime=walltime,
        account=project_account,
        job_name=run_identity,
        stdout_dir=log_dir,
        stderr_dir=log_dir,
        queue=queue,
        gpus_per_node=plan.num_gpus_per_node,
        filesystems=defaults.filesystems,
        keep_flag=defaults.keep_flag,
        run_identity=run_identity,
    )
    ext = {"slurm": "sbatch", "psij": "psij.sh"}.get(scheduler.name, "pbs")
    job_script_path = artifact_dir / f"job.{ext}"
    atomic_create_text(job_script_path, scheduler.render_job(job_spec))
    prepared = {
        "schema_version": _SUBMISSION_SCHEMA_VERSION,
        "phase": "PREPARED",
        "intent_id": intent_id,
        "run_identity": run_identity,
        "scheduler": scheduler.name,
        "job_id": "",
        "deployment_id": deployment_id,
        "generation": generation,
        "deployment_plan_hash": plan.deployment_plan_hash,
        "run_dir": str(run_dir),
        "plan_path": str(plan_path),
        "site_profile_path": str(site_profile_path),
        "job_script_path": str(job_script_path),
        "submitted_at": "",
    }
    return prepared, str(job_script_path)


def submit_serve(
    config_path: str | os.PathLike,
    *,
    job_id_file: Optional[str | os.PathLike] = None,
    queue: Optional[str] = None,
    walltime: Optional[str] = None,
    project_account: Optional[str] = None,
    job_name: Optional[str] = None,
    log_dir: Optional[str | os.PathLike] = None,
    dry_run: bool = False,
    force_new: bool = False,
) -> str:
    """Compile, persist, and submit at most one job for one intent.

    A PREPARED record is durable before calling the scheduler. If submission
    succeeds but the local state write fails, a retry reconciles the exact
    scheduler-visible identity instead of submitting again. Repeated completed
    calls attach to the recorded job unless ``force_new`` is explicit.
    """
    cfg_path = Path(config_path).resolve()
    if not cfg_path.is_file():
        raise FileNotFoundError(f"deployment config not found: {cfg_path}")
    defaults = get_site_defaults()
    from .plan.compiler import compile_deployment_plan
    from .site import default_site_profile
    from .yaml_support import load_yaml_mapping_text

    from .state.atomic import regular_file_reader

    with regular_file_reader(cfg_path, binary=True) as handle:
        config_bytes = handle.read()
    raw = load_yaml_mapping_text(config_bytes, source=cfg_path)
    site_profile = default_site_profile()
    topology_plan = compile_deployment_plan(raw, site=site_profile, deployment_id="pending")
    topology_queue, topology_walltime = default_queue_and_walltime(topology_plan.num_nodes)
    resolved_queue = queue or defaults.queue or topology_queue
    resolved_walltime = walltime or defaults.walltime or topology_walltime
    resolved_account = project_account or defaults.project_account
    scheduler = get_scheduler()
    if scheduler.name not in site_profile.scheduler_types:
        raise ValueError(
            f"scheduler {scheduler.name!r} is not qualified by SiteProfile "
            f"{site_profile.site_id!r}; supported schedulers: "
            f"{', '.join(site_profile.scheduler_types) or 'none'}"
        )
    from .site import require_execution_qualification

    require_execution_qualification(topology_plan, site_profile)
    resolved_log_dir = (Path(log_dir) if log_dir else cfg_path.parent / "pbs_logs").resolve()
    resolved_log_dir.mkdir(parents=True, exist_ok=True)
    intent_id = _submission_intent_id(
        deployment_plan_hash=topology_plan.deployment_plan_hash,
        scheduler=scheduler.name,
        queue=resolved_queue,
        walltime=resolved_walltime,
        account=resolved_account,
    )
    intent_dir = _private_directory(resolved_log_dir / "submission_intents")
    intent_path = intent_dir / f"{intent_id}.json"
    lease = ExclusiveLease(
        intent_dir / f"{intent_id}.lock",
        ttl_s=600.0,
        owner_note="serve submission",
    )
    lease.acquire()
    heartbeat = LeaseHeartbeat(lease, interval_s=30.0)
    heartbeat_started = False
    try:
        heartbeat.start()
        heartbeat_started = True
        existing = None if dry_run else _load_object(intent_path)
        prepared: Optional[dict] = None
        job_script_path = ""
        if existing is not None:
            existing = _validate_submission_record(existing)
            if existing["intent_id"] != intent_id or existing["scheduler"] != scheduler.name:
                raise RuntimeError("submission intent identity changed")
            if existing["phase"] == "SUBMITTED" and not force_new:
                _publish_submission_record(existing, intent_path=intent_path)
                _write_job_id(job_id_file, cfg_path, existing["job_id"])
                return str(existing["job_id"])
            if existing["phase"] == "SUBMITTING":
                matches = scheduler.find_by_run_identity(existing["run_identity"])
                if len(matches) > 1:
                    raise RuntimeError(
                        f"ambiguous scheduler recovery for {existing['run_identity']!r}: "
                        f"{[item.job_id for item in matches]}"
                    )
                if len(matches) == 1:
                    recovered = dict(existing)
                    recovered.update(
                        phase="SUBMITTED",
                        job_id=matches[0].job_id,
                        submitted_at=datetime.now(timezone.utc).isoformat(),
                    )
                    _publish_submission_record(recovered, intent_path=intent_path)
                    _write_job_id(job_id_file, cfg_path, recovered["job_id"])
                    return str(recovered["job_id"])
                if force_new:
                    raise RuntimeError(
                        "cannot force a new generation while an earlier submit intent "
                        "has no reconciled scheduler result"
                    )
                raise RuntimeError(
                    f"submission ownership for {existing['run_identity']!r} is ambiguous: "
                    "no exact scheduler record is currently visible, but absence is not "
                    "proof that the prior request was never accepted"
                )
            if existing["phase"] in {"PREPARED", "REJECTED"}:
                prepared = dict(existing)
                prepared.update(phase="PREPARED", job_id="", submitted_at="")
                job_script_path = str(existing["job_script_path"])

        if prepared is None:
            prepared, job_script_path = _new_prepared_submission(
                cfg_path=cfg_path,
                log_dir=resolved_log_dir,
                scheduler=scheduler,
                defaults=defaults,
                plan=topology_plan,
                site_profile=site_profile,
                queue=resolved_queue,
                walltime=resolved_walltime,
                project_account=resolved_account,
                job_name=job_name,
                intent_id=intent_id,
            )
        if dry_run:
            sys.stderr.write(
                f"[exaserve-serve-submit] dry-run ({scheduler.name}); "
                f"would submit {job_script_path}\n"
            )
            sys.stderr.write(
                "[exaserve-serve-submit] canonical endpoint will be published "
                f"under {prepared['run_dir']} only after READY\n"
            )
            return ""

        _publish_submission_record(prepared, intent_path=intent_path)
        submitting = dict(prepared)
        submitting.update(
            phase="SUBMITTING",
            job_id="",
            submitted_at=datetime.now(timezone.utc).isoformat(),
        )
        _publish_submission_record(submitting, intent_path=intent_path)
        try:
            heartbeat.ensure_held()
            submission = scheduler.submit(Path(job_script_path))
            heartbeat.ensure_held()
        except SubmissionRejected:
            rejected = dict(submitting)
            rejected["phase"] = "REJECTED"
            _publish_submission_record(rejected, intent_path=intent_path)
            raise
        submitted = dict(prepared)
        submitted.update(
            phase="SUBMITTED",
            job_id=submission.job_id,
            submitted_at=datetime.now(timezone.utc).isoformat(),
        )
        _publish_submission_record(submitted, intent_path=intent_path)
        _write_job_id(job_id_file, cfg_path, submission.job_id)
        return submission.job_id
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
                        active_error, f"submission heartbeat cleanup also failed: {stop_exc}"
                    )
        try:
            lease.release()
        except BaseException as release_exc:
            if active_error is None and heartbeat_error is None:
                raise
            target_error = active_error or heartbeat_error
            assert target_error is not None
            add_exception_note(target_error, f"submission lease cleanup also failed: {release_exc}")
        if active_error is None and heartbeat_error is not None:
            raise heartbeat_error


def serve_url(
    job_id: str,
    config_path: Optional[str | os.PathLike] = None,
    *,
    wait: bool = False,
    poll_interval_s: float = 5.0,
    timeout_s: float = 1800.0,
    port: Optional[int] = None,
) -> str:
    """Return only the canonical endpoint of this exact READY generation.

    ``config_path`` and ``port`` remain accepted for API compatibility but
    cannot override deployment status. Guessing from config, scheduler host,
    or a newest ``proxy_port`` file can select the wrong generation.
    """
    del config_path, port
    if poll_interval_s <= 0 or timeout_s <= 0:
        raise ValueError("poll interval and timeout must be positive")
    record = _load_object(_registry_path(job_id))
    if record is None:
        raise RuntimeError(
            f"no canonical submission registry exists for job {job_id!r}; "
            "an endpoint cannot be inferred from scheduler state"
        )
    record = _validate_submission_record(record, expected_job_id=job_id)
    if record["phase"] != "SUBMITTED":
        raise RuntimeError(f"job {job_id!r} has no completed submission record")

    scheduler = get_scheduler(record["scheduler"])
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            from .status_api import DeploymentNotReady, require_ready_endpoint

            return require_ready_endpoint(
                record["run_dir"],
                expected_generation=record["generation"],
                expected_plan_hash=record["deployment_plan_hash"],
            )
        except DeploymentNotReady as exc:
            not_ready = str(exc)
        observation = scheduler.observe(job_id)
        if observation.state not in _ACTIVE_OR_UNCERTAIN:
            raise RuntimeError(
                f"job {job_id} became {observation.state.value} before READY: {not_ready}"
            )
        if not wait:
            raise RuntimeError(f"job {job_id} is not READY: {not_ready}. Pass --wait to poll.")
        if time.monotonic() > deadline:
            raise TimeoutError(f"job {job_id} did not publish READY within {timeout_s}s")
        time.sleep(poll_interval_s)


def _serve_submit_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="exaserve-serve-submit")
    parser.add_argument("config", help="Deployment config YAML")
    parser.add_argument("--job-id-file", default=None)
    parser.add_argument("--queue", default=None)
    parser.add_argument("--walltime", default=None)
    parser.add_argument("--project-account", default=None)
    parser.add_argument("--job-name", default=None, help="Readable two-character job-name prefix")
    parser.add_argument("--log-dir", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--new-generation",
        action="store_true",
        help="Submit a new generation instead of attaching to the prior intent",
    )
    parser.add_argument("--wait", action="store_true", help="Wait for canonical READY")
    parser.add_argument("--poll-interval", type=float, default=5.0)
    parser.add_argument("--timeout", type=float, default=1800.0)
    return parser


def serve_submit_main() -> int:
    args = _serve_submit_argparser().parse_args()
    job_id = submit_serve(
        args.config,
        job_id_file=args.job_id_file,
        queue=args.queue,
        walltime=args.walltime,
        project_account=args.project_account,
        job_name=args.job_name,
        log_dir=args.log_dir,
        dry_run=args.dry_run,
        force_new=args.new_generation,
    )
    if job_id:
        print(job_id)
        if args.wait:
            print(
                serve_url(
                    job_id,
                    wait=True,
                    poll_interval_s=args.poll_interval,
                    timeout_s=args.timeout,
                )
            )
    return 0


def _serve_url_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="exaserve-serve-url")
    parser.add_argument("job_id_or_file", help="scheduler job id or a file containing one")
    parser.add_argument(
        "--config", default=None, help="Deprecated; status owns deployment identity"
    )
    parser.add_argument("--port", type=int, default=None, help="Deprecated; status owns endpoint")
    parser.add_argument("--wait", action="store_true", help="Poll until canonical READY")
    parser.add_argument("--poll-interval", type=float, default=5.0)
    parser.add_argument("--timeout", type=float, default=1800.0)
    return parser


def _resolve_job_id(token: str) -> str:
    candidate = Path(token)
    if candidate.is_file():
        from .state.atomic import regular_file_reader

        with regular_file_reader(candidate) as handle:
            lines = handle.read().splitlines()
        if not lines or not lines[0].strip():
            raise ValueError(f"job-id file {candidate} is empty")
        return lines[0].strip()
    if not token.strip():
        raise ValueError("scheduler job id must be non-empty")
    return token.strip()


def serve_url_main() -> int:
    args = _serve_url_argparser().parse_args()
    job_id = _resolve_job_id(args.job_id_or_file)
    url = serve_url(
        job_id,
        config_path=args.config,
        wait=args.wait,
        poll_interval_s=args.poll_interval,
        timeout_s=args.timeout,
        port=args.port,
    )
    print(url)
    return 0
