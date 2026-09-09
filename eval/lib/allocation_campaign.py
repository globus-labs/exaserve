"""Finite, head-owned PBS acquisition for immutable exact-size eval children.

This controller never changes a child's RunPlan or implements deployment
lifecycle. The child's scheduler request is UNUSED for acquisition; its frozen
``eval.cli run execute`` owns launch, replay, evidence, and shutdown. Acquisition
and subset provenance are separate, immutable campaign sidecars.
"""

from __future__ import annotations

import hashlib
import math
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

from exaserve.plan.contracts import SchedulerPlan, canonical_hash, canonical_node_id
from exaserve.exception_notes import add_exception_note
from exaserve.schedulers import JobSpec, SubmissionRejected, get_scheduler
from exaserve.state.atomic import (
    ExclusiveLease,
    LeaseHeartbeat,
    LeaseHeldError,
    atomic_create_json,
    atomic_create_text,
    atomic_write_json,
    ensure_owned_directory,
    regular_file_reader,
    strict_json_load_path,
    strict_json_loads,
)

from .run_planner import (
    _detect_repo_state,
    _ensure_repo_snapshot,
    _source_snapshot_hash,
    load_run_plan,
)
from .utils import dataclass_to_dict


_SCHEMA = 1
_CLEANUP_S = 900
_FORCE_S = 30
_FIELDS = {
    "schema_version",
    "root",
    "scheduler",
    "controller",
    "children",
    "child_timeout_s",
    "cleanup_reserve_s",
    "qualification_mode",
    "qualification",
    "created_at",
    "campaign_hash",
}
_CHILD_FIELDS = {
    "run_yaml",
    "snapshot_root",
    "source_snapshot_hash",
    "run_semantic_hash",
    "deployment_plan_hash",
    "deployment_id",
    "logical_nodes",
    "null_compute",
    "inputs",
}
_CONTROLLER_FIELDS = {
    "snapshot_root",
    "source_snapshot_hash",
    "commit",
    "module_sha256",
    "excluded_dirty_files",
}


class _OwnedAllocationPreflightError(RuntimeError):
    """Native identity is positively bound; preflight failure is terminal."""


def _sha(path: str | Path) -> str:
    digest = hashlib.sha256()
    with regular_file_reader(path, binary=True) as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def _seconds(value: str) -> int:
    if not isinstance(value, str) or not re.fullmatch(r"\d+:[0-5]\d:[0-5]\d", value):
        raise ValueError("walltime must use HH:MM:SS")
    hours, minutes, seconds = map(int, value.split(":"))
    return 3600 * hours + 60 * minutes + seconds


def _positive(value, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        raise ValueError(f"{name} must be a positive finite number")
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a positive finite number")
    return float(value)


def _validate_contract(campaign: dict) -> None:
    """Apply identical semantic checks at materialization and every reload."""
    if type(campaign["schema_version"]) is not int or campaign["schema_version"] != _SCHEMA:
        raise ValueError("campaign schema version mismatch")
    mode = campaign["qualification_mode"]
    if (
        type(mode) is not bool
        or type(campaign["cleanup_reserve_s"]) is not int
        or campaign["cleanup_reserve_s"] != _CLEANUP_S
    ):
        raise ValueError("campaign mode/cleanup reserve mismatch")
    request_raw = campaign["scheduler"]
    if not isinstance(request_raw, dict) or set(request_raw) != set(
        SchedulerPlan.__dataclass_fields__
    ):
        raise ValueError("campaign SchedulerPlan shape mismatch")
    request = SchedulerPlan(**request_raw)
    expected = SchedulerPlan(
        type="pbs",
        nodes=request.nodes,
        account="AuroraGPT",
        queue=request.queue,
        walltime=request.walltime,
        filesystem_refs=("home", "flare"),
        launcher="mpi",
    )
    if request != expected:
        raise ValueError("campaign scheduler contains unsupported acquisition policy")
    controller = campaign["controller"]
    if not isinstance(controller, dict) or set(controller) != _CONTROLLER_FIELDS:
        raise ValueError("controller identity shape mismatch")
    for field in ("source_snapshot_hash", "module_sha256"):
        if not isinstance(controller[field], str) or not re.fullmatch(
            r"[0-9a-f]{64}", controller[field]
        ):
            raise ValueError("controller digest is malformed")
    if not isinstance(controller["commit"], str) or not re.fullmatch(
        r"[0-9a-f]{40}", controller["commit"]
    ):
        raise ValueError("controller commit is malformed")
    if not isinstance(controller["excluded_dirty_files"], list) or any(
        not isinstance(p, str) for p in controller["excluded_dirty_files"]
    ):
        raise ValueError("excluded dirty paths must be a string list")
    for value in (campaign["root"], controller["snapshot_root"]):
        if not isinstance(value, str) or os.path.abspath(value) != value:
            raise ValueError("campaign paths must be absolute normalized paths")
    children = campaign["children"]
    if not isinstance(children, list) or not children:
        raise ValueError("campaign requires a nonempty ordered list of child references")
    for child in children:
        if not isinstance(child, dict) or set(child) != _CHILD_FIELDS:
            raise ValueError("campaign child reference shape mismatch")
        if type(child["logical_nodes"]) is not int or type(child["null_compute"]) is not bool:
            raise ValueError("campaign child node count/null mode requires exact types")
        for field in ("run_yaml", "snapshot_root"):
            if not isinstance(child[field], str) or os.path.abspath(child[field]) != child[field]:
                raise ValueError("child locations must be normalized absolute paths")
        for field in ("source_snapshot_hash", "run_semantic_hash", "deployment_plan_hash"):
            if not isinstance(child[field], str) or not re.fullmatch(r"[0-9a-f]{64}", child[field]):
                raise ValueError("child digest is malformed")
        if not isinstance(child["inputs"], dict) or len(child["inputs"]) != 8:
            raise ValueError("child must bind the complete eight-file input inventory")
    for field in ("run_yaml", "deployment_id"):
        identities = {
            str(Path(child[field]).resolve()) if field == "run_yaml" else child[field]
            for child in children
        }
        if len(identities) != len(children):
            raise ValueError("campaign children must have distinct paths/deployment identities")
    if len({child["source_snapshot_hash"] for child in children}) != 1:
        raise ValueError("campaign children must share one frozen source identity")
    logical = [child["logical_nodes"] for child in children]
    if mode:
        if (request.nodes, request.queue, logical) != (4, "capacity", [2, 2]) or not all(
            child["null_compute"] for child in children
        ):
            raise ValueError(
                "qualification requires physical4/capacity and two logical2 null children"
            )
        if campaign["qualification"] is not None:
            raise ValueError("qualification mode cannot consume another proof")
    elif (request.nodes, request.queue, logical) != (256, "prod", [32, 64, 128]) or any(
        child["null_compute"] for child in children
    ):
        raise ValueError("production requires prod256 and real logical32/64/128 children")
    timeout = _positive(campaign["child_timeout_s"], "child_timeout_s")
    walltime = _seconds(request.walltime)
    if walltime > (3600 if mode else 21600) or walltime < (
        timeout + _CLEANUP_S + 2 * _FORCE_S
    ) * len(children):
        raise ValueError("walltime cannot cover all child deadlines plus reserved cleanup")


def _child_reference(path: str) -> dict:
    plan = load_run_plan(str(Path(path).resolve()))
    deployment = plan.semantic_plan.deployment
    if plan.scheduler.type != "pbs" or plan.scheduler.nodes != deployment.num_nodes:
        raise ValueError("campaign children require exact-size canonical PBS plans")
    if plan.client.startup_only or not deployment.runtime.clean_stage:
        raise ValueError("campaign children require full replay and clean_stage=true")
    if plan.client.num_runs != 2 or plan.client.stream:
        raise ValueError("campaign requires two complete non-streaming replay runs")
    if deployment.control.watchdog_cleanup_deadline_s > _CLEANUP_S:
        raise ValueError("child watchdog exceeds campaign's reserved graceful-cleanup budget")
    paths = [
        plan.bundle.run_yaml_path,
        plan.bundle.job_path,
        plan.semantic_plan_path,
        plan.deployment_plan_path,
        plan.site_profile_path,
        plan.runtime_manifest_path,
        plan.trace_artifact.trace_path,
        plan.trace_artifact.metadata_path,
    ]
    return {
        "run_yaml": plan.bundle.run_yaml_path,
        "snapshot_root": plan.snapshot_root,
        "source_snapshot_hash": plan.source_snapshot_hash,
        "run_semantic_hash": plan.run_semantic_hash,
        "deployment_plan_hash": plan.deployment_plan_hash,
        "deployment_id": deployment.deployment_id,
        "logical_nodes": deployment.num_nodes,
        "null_compute": deployment.runtime.null_compute,
        "inputs": {p: _sha(p) for p in paths},
    }


def _validate_child_inputs(child: dict):
    if not isinstance(child, dict) or set(child) != _CHILD_FIELDS:
        raise ValueError("campaign child reference shape mismatch")
    if canonical_hash(_child_reference(child["run_yaml"])) != canonical_hash(child):
        raise ValueError("campaign child input/source identity changed")
    return load_run_plan(child["run_yaml"])


def _require_fresh(plan) -> None:
    from exaserve.state.status import StatusStore

    record = StatusStore.run(plan.bundle.state_path).load()
    if (
        record is None
        or record.state != "PLANNED"
        or record.data.get("phase") != "materialized"
        or record.revision != 0
    ):
        raise RuntimeError("campaign child is not a fresh, unsubmitted materialization")


def _controller_snapshot(repo_root: str | None, allow_dirty: bool) -> dict:
    root = Path(repo_root or Path(__file__).resolve().parents[2]).resolve()
    excluded = []
    if (root / "snapshot_meta.json").is_file():
        snapshot = str(root)
        commit = strict_json_load_path(root / "snapshot_meta.json")["commit_sha"]
    else:
        commit, excluded = _detect_repo_state(str(root))
        if excluded and not allow_dirty:
            raise ValueError("controller repository is dirty; commit or explicitly allow_dirty")
        snapshot = _ensure_repo_snapshot(str(root), commit)
    module = Path(snapshot) / "eval/lib/allocation_campaign.py"
    if not module.is_file() or _sha(module) != _sha(__file__):
        raise ValueError("controller must first be committed; selected snapshot differs from code")
    return {
        "snapshot_root": snapshot,
        "source_snapshot_hash": _source_snapshot_hash(snapshot),
        "commit": commit,
        "module_sha256": _sha(module),
        "excluded_dirty_files": excluded,
    }


def _validate_qualification(reference: dict, controller: dict, children: list[dict]) -> dict:
    if not isinstance(reference, dict) or set(reference) != {"path", "sha256"}:
        raise ValueError("production requires a hash-bound qualification report")
    if _sha(reference["path"]) != reference["sha256"]:
        raise ValueError("qualification report bytes changed")
    report = strict_json_load_path(reference["path"])
    required = {
        "schema_version",
        "campaign_hash",
        "controller_source_snapshot_hash",
        "child_source_snapshot_hash",
        "acquisition",
        "mpi_proof",
        "children",
        "sentinel",
        "process_audit",
        "verdict",
        "completed_at",
    }
    if not isinstance(report, dict) or set(report) != required:
        raise ValueError("qualification report shape mismatch")
    if (
        type(report["schema_version"]) is not int
        or report["schema_version"] != _SCHEMA
        or report["verdict"] != "PASS"
        or report["controller_source_snapshot_hash"] != controller["source_snapshot_hash"]
        or {c["source_snapshot_hash"] for c in children} != {report["child_source_snapshot_hash"]}
        or report["sentinel"].get("survived_cleanup") is not True
        or report["sentinel"].get("reaped") is not True
        or report["process_audit"].get("clean") is not True
        or report["mpi_proof"].get("passed") is not True
        or len(report["children"]) != 2
        or [c.get("outcome") for c in report["children"]] != ["SUCCEEDED", "CANCELLED_AFTER_READY"]
    ):
        raise ValueError("qualification did not pass the exact controller/child source boundary")
    proof_root = Path(reference["path"]).resolve().parent
    if Path(reference["path"]).resolve() != proof_root / "qualification.json":
        raise ValueError("qualification report must be the canonical proof-campaign artifact")
    proof = _load_campaign(str(proof_root / "campaign.json"))
    if not proof["qualification_mode"] or proof["campaign_hash"] != report["campaign_hash"]:
        raise ValueError("qualification report belongs to a different/non-proof campaign")
    state = _state(proof)
    if state["phase"] != "SUCCEEDED" or state["job_id"] != report["acquisition"]["job_id"]:
        raise ValueError("qualification campaign did not publish canonical success")
    _require_pbs_zero_exit(state["job_id"], _job_spec(proof).job_name)
    if proof["controller"]["source_snapshot_hash"] != controller["source_snapshot_hash"]:
        raise ValueError("qualification controller differs from production controller")
    for filename, expected in (
        ("acquisition.json", report["acquisition"]),
        ("mpi_proof.json", report["mpi_proof"]),
    ):
        if strict_json_load_path(proof_root / filename) != expected:
            raise ValueError("qualification acquisition/MPI sidecar differs from the report")
    physical = report["acquisition"]["physical_nodes"]
    if (
        len(physical) != 4
        or canonical_hash(physical) != report["acquisition"]["physical_nodes_hash"]
    ):
        raise ValueError("qualification physical allocation evidence is inconsistent")
    _validate_sentinel_evidence(
        report["sentinel"],
        proof_root / "sentinel.log",
        campaign_hash=proof["campaign_hash"],
        nodes=physical,
        receipts=report["children"],
    )
    # Re-run the exact acceptance boundary and authenticate its complete evidence,
    # not just a mutable PASS field or a nonempty list of asserted booleans.
    for index, receipt in enumerate(report["children"]):
        if strict_json_load_path(proof_root / f"child-{index}/receipt.json") != receipt:
            raise ValueError("qualification child receipt differs from its immutable sidecar")
        if receipt["child"] != proof["children"][index] or receipt["job_id"] != state["job_id"]:
            raise ValueError("qualification child receipt belongs to another campaign")
        subset = _select_subset(physical, 2, report["acquisition"]["head"])
        if receipt["subset"] != subset or receipt["subset_hash"] != canonical_hash(subset):
            raise ValueError("qualification child subset differs from the acquisition")
        plan = _validate_child_inputs(receipt["child"])
        accepted = _accept_child(
            plan,
            subset,
            state["job_id"],
            proof_root / "child-1" if index else Path(plan.bundle.results_dir),
            cancelled=index == 1,
        )
        if any(receipt.get(key) != value for key, value in accepted.items()):
            raise ValueError("qualification canonical acceptance no longer matches its receipt")
        for path, digest in receipt["evidence"].items():
            if _sha(path) != digest:
                raise ValueError("qualification evidence changed")
        audit = strict_json_load_path(proof_root / f"audit-{index}/process_audit.json")
        _require_clean_audit(audit, physical)
        if audit["deployment_ids"] != [
            child["deployment_id"] for child in proof["children"]
        ] or audit["runtime_paths"] != _audit_paths(
            proof["children"], report["children"][: index + 1]
        ):
            raise ValueError("qualification process audit omitted an owned generation")
    final_audit = strict_json_load_path(proof_root / "sentinel-reaped/process_audit.json")
    _require_clean_audit(final_audit, physical)
    if (
        report["process_audit"] != final_audit
        or final_audit.get("sentinel_token") != report["sentinel"]["token"]
    ):
        raise ValueError("qualification lacks the final exact sentinel-reaping audit")
    if final_audit["sentinel_token"] != "--exaserve-subset-sentinel=" + proof[
        "campaign_hash"
    ] or final_audit["runtime_paths"] != _audit_paths(proof["children"], report["children"]):
        raise ValueError("qualification sentinel/generation audit identity mismatch")
    return report


def _require_pbs_zero_exit(job_id: str, job_name: str) -> dict:
    """Require explicit native success; an absent exit field is not zero."""
    records = get_scheduler("pbs")._query("-x", "-f", job_id)
    if len(records) != 1:
        raise ValueError("qualification requires one exact native PBS completion record")
    record = records[0]
    if (
        record.get("job_id") != job_id
        or record.get("Job_Name") != job_name
        or record.get("job_state") != "F"
        or record.get("Exit_status") != "0"
    ):
        raise ValueError("qualification PBS job lacks explicit native exit status zero")
    return record


def materialize_campaign(
    children: list[str],
    *,
    output_dir: str,
    physical_nodes: int,
    queue: str,
    walltime: str,
    child_timeout_s: float,
    qualification_path: str | None = None,
    qualification_mode: bool = False,
    repo_root: str | None = None,
    allow_dirty: bool = False,
) -> str:
    """Create immutable acquisition intent. No child is submitted or modified."""
    if type(qualification_mode) is not bool or type(allow_dirty) is not bool:
        raise ValueError("mode flags must be boolean")
    if not isinstance(children, list) or not children or len(children) != len(set(children)):
        raise ValueError("children must be a nonempty list of distinct run.yaml paths")
    timeout = _positive(child_timeout_s, "child_timeout_s")
    scheduler = SchedulerPlan(
        type="pbs",
        nodes=physical_nodes,
        queue=queue,
        account="AuroraGPT",
        walltime=walltime,
        filesystem_refs=("home", "flare"),
        launcher="mpi",
    )
    refs = [_child_reference(path) for path in children]
    for child in refs:
        _require_fresh(load_run_plan(child["run_yaml"]))
    if len({c["source_snapshot_hash"] for c in refs}) != 1:
        raise ValueError("campaign children must share one frozen source identity")
    logical = [c["logical_nodes"] for c in refs]
    if qualification_mode:
        if (physical_nodes, queue, logical) != (4, "capacity", [2, 2]):
            raise ValueError("qualification requires physical4/capacity and two logical2 children")
        if not all(c["null_compute"] for c in refs) or _seconds(walltime) > 3600:
            raise ValueError("qualification requires null children and at most one hour")
        if qualification_path is not None:
            raise ValueError("qualification mode does not consume another qualification")
    elif (physical_nodes, queue, logical) != (256, "prod", [32, 64, 128]):
        raise ValueError("current production campaign is exactly prod256 with logical32/64/128")
    if _seconds(walltime) > (3600 if qualification_mode else 21600):
        raise ValueError("campaign exceeds its approved finite walltime budget")
    if (timeout + _CLEANUP_S + 2 * _FORCE_S) * len(refs) > _seconds(walltime):
        raise ValueError("walltime cannot cover all child deadlines plus reserved cleanup")
    controller = _controller_snapshot(repo_root, allow_dirty)
    qualification = None
    if not qualification_mode:
        if not qualification_path:
            raise ValueError("production materialization requires a passed qualification report")
        qualification = {
            "path": str(Path(qualification_path).resolve()),
            "sha256": _sha(qualification_path),
        }
        _validate_qualification(qualification, controller, refs)
    root = Path(output_dir).resolve()
    if root.exists():
        raise FileExistsError("campaign output must be a fresh directory")
    root = Path(ensure_owned_directory(root))
    payload = {
        "schema_version": _SCHEMA,
        "root": str(root),
        "scheduler": dataclass_to_dict(scheduler),
        "controller": controller,
        "children": refs,
        "child_timeout_s": timeout,
        "cleanup_reserve_s": _CLEANUP_S,
        "qualification_mode": qualification_mode,
        "qualification": qualification,
        "created_at": _now(),
    }
    _validate_contract(payload)
    payload["campaign_hash"] = canonical_hash(payload)
    path = root / "campaign.json"
    atomic_create_json(path, payload)
    job = _job_spec(payload)
    atomic_create_text(root / "job.pbs", get_scheduler("pbs").render_job(job))
    atomic_create_json(
        root / "campaign.state.json",
        {
            "campaign_hash": payload["campaign_hash"],
            "phase": "PREPARED",
            "job_id": "",
        },
    )
    return str(path)


def _job_spec(campaign: dict) -> JobSpec:
    request = SchedulerPlan(**campaign["scheduler"])
    root = Path(campaign["root"])
    controller = Path(campaign["controller"]["snapshot_root"])
    identity = "ac-" + campaign["campaign_hash"][:12]
    # Site-required PBS spool locations; child evidence stays in its bundle.
    spool = Path.home() / "aurora_rayserver/tmp"
    return JobSpec(
        job_name=identity,
        num_nodes=request.nodes,
        walltime=request.walltime,
        queue=request.queue,
        account=request.account,
        filesystems="home:flare",
        stdout_dir=spool,
        stderr_dir=spool,
        cwd=controller,
        source_env_script=Path.home() / "script/env_aurora",
        pythonpath=(controller, controller / "src"),
        run_identity=identity,
        environment_unset=("ONEAPI_DEVICE_SELECTOR",),
        command_argv=(
            "python3",
            "-m",
            "eval.cli",
            "allocation",
            "execute",
            str(root / "campaign.json"),
        ),
    )


def _load_campaign(path: str) -> dict:
    value = strict_json_load_path(path)
    if not isinstance(value, dict) or set(value) != _FIELDS or value["schema_version"] != _SCHEMA:
        raise ValueError("campaign artifact shape/version mismatch")
    _validate_contract(value)
    payload = {key: item for key, item in value.items() if key != "campaign_hash"}
    if canonical_hash(payload) != value["campaign_hash"]:
        raise ValueError("campaign hash mismatch")
    if Path(path).resolve() != Path(value["root"]) / "campaign.json":
        raise ValueError("campaign artifact location mismatch")
    _positive(value["child_timeout_s"], "child_timeout_s")
    if value["cleanup_reserve_s"] != _CLEANUP_S or type(value["qualification_mode"]) is not bool:
        raise ValueError("campaign cleanup/mode contract mismatch")
    controller = value["controller"]
    if _source_snapshot_hash(controller["snapshot_root"]) != controller["source_snapshot_hash"]:
        raise ValueError("controller snapshot identity changed")
    if (
        _sha(Path(controller["snapshot_root"]) / "eval/lib/allocation_campaign.py")
        != controller["module_sha256"]
    ):
        raise ValueError("controller module identity changed")
    for child in value["children"]:
        _validate_child_inputs(child)
    if not value["qualification_mode"]:
        _validate_qualification(value["qualification"], controller, value["children"])
    expected_script = get_scheduler("pbs").render_job(_job_spec(value))
    with regular_file_reader(Path(value["root"]) / "job.pbs") as stream:
        if stream.read() != expected_script:
            raise ValueError("campaign job body differs from canonical scheduler rendering")
    return value


def _state(campaign: dict) -> dict:
    result = strict_json_load_path(Path(campaign["root"]) / "campaign.state.json")
    if result.get("campaign_hash") != campaign["campaign_hash"]:
        raise ValueError("campaign state identity mismatch")
    return result


def _write_state(campaign: dict, **updates) -> None:
    state = _state(campaign)
    state.update(updates, updated_at=_now())
    atomic_write_json(Path(campaign["root"]) / "campaign.state.json", state)


def submit_campaign(path: str) -> str:
    """Submit once; uncertain acceptance remains fenced until exact reconciliation."""
    campaign = _load_campaign(path)
    root = Path(campaign["root"])
    scheduler = get_scheduler("pbs")
    identity = _job_spec(campaign).run_identity
    with ExclusiveLease(root / "submit.lease", ttl_s=3600) as lease:
        with LeaseHeartbeat(lease, interval_s=30) as heartbeat:
            state = _state(campaign)
            if state["phase"] in {"SUBMITTED", "RUNNING", "SUCCEEDED", "FAILED"}:
                if not state["job_id"]:
                    raise RuntimeError("recorded campaign lacks its native scheduler identity")
                return state["job_id"]
            if state["phase"] == "SUBMITTING":
                matches = scheduler.find_by_run_identity(identity)
                if len(matches) != 1:
                    raise RuntimeError(
                        "ambiguous campaign submission; no unique exact scheduler match"
                    )
                job_id = matches[0].job_id
            elif state["phase"] in {"PREPARED", "REJECTED"}:
                for child in campaign["children"]:
                    _require_fresh(_validate_child_inputs(child))
                _write_state(campaign, phase="SUBMITTING", submitted_at=_now())
                try:
                    heartbeat.ensure_held()
                    job_id = scheduler.submit(root / "job.pbs").job_id
                    heartbeat.ensure_held()
                except SubmissionRejected as exc:
                    _write_state(campaign, phase="REJECTED", error=str(exc))
                    raise
                except BaseException:
                    # Do not reset intent or retry a potentially accepted qsub.
                    raise
            else:
                raise RuntimeError("campaign is not eligible for submission")
            _write_state(campaign, phase="SUBMITTED", job_id=job_id)
            return job_id


def _read_nodes(path: str) -> list[str]:
    with regular_file_reader(path) as stream:
        nodes = [line.strip() for line in stream if line.strip()]
    normalized = [canonical_node_id(node) for node in nodes]
    if not nodes or len(normalized) != len(set(normalized)) or not all(normalized):
        raise ValueError("allocation nodefile is empty or contains duplicate aliases")
    return nodes


def _select_subset(nodes: list[str], logical_nodes: int, head: str) -> list[str]:
    if not isinstance(nodes, list) or any(
        not isinstance(node, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+", node) for node in nodes
    ):
        raise ValueError("physical nodes must be a nonempty list of hostnames")
    normalized = list(map(canonical_node_id, nodes))
    if not normalized or not all(normalized) or len(normalized) != len(set(normalized)):
        raise ValueError("physical nodes contain duplicate canonical aliases")
    if type(logical_nodes) is not int or not 0 < logical_nodes < len(nodes):
        raise ValueError("logical subset must be smaller than its physical allocation")
    if canonical_node_id(nodes[0]) != canonical_node_id(head):
        raise ValueError("controller must execute on the original PBS allocation head")
    return nodes[:logical_nodes]


def _child_environment(child: dict, nodefile: str, native_job_id: str) -> dict[str, str]:
    if os.environ.get("PBS_JOBID") != native_job_id:
        raise ValueError("native PBS_JOBID changed")
    env = {key: value for key, value in os.environ.items() if not key.startswith("EXASERVE_")}
    for name in (
        "VIRTUAL_ENV",
        "PYTHONHOME",
        "PYTHONPYCACHEPREFIX",
        "CONDA_PREFIX",
        "CONDA_DEFAULT_ENV",
    ):
        env.pop(name, None)
    env.update(
        {
            "PBS_NODEFILE": nodefile,
            "EXASERVE_NODEFILE": nodefile,
            "EXASERVE_SCHEDULER": "pbs",
            "EXASERVE_JOBID": native_job_id,
            "PYTHONPATH": child["snapshot_root"] + os.pathsep + child["snapshot_root"] + "/src",
            "PYTHONNOUSERSITE": "1",
            "PYTHONSAFEPATH": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
        }
    )
    if "ONEAPI_DEVICE_SELECTOR" in env or env.get("SLURM_JOB_ID"):
        raise ValueError("conflicting Aurora accelerator/scheduler environment")
    return env


def _bind_native_submission(campaign: dict, job_id: str, stop_requested) -> None:
    """Scheduler delivery can precede the qsub caller's status publication."""
    deadline = time.monotonic() + 660
    while True:
        state = _state(campaign)
        if state["phase"] == "SUBMITTED" and state["job_id"] == job_id:
            return
        if state["phase"] != "SUBMITTING" or state["job_id"]:
            raise RuntimeError("campaign native delivery disagrees with submission state")
        if stop_requested[0] or time.monotonic() >= deadline:
            raise RuntimeError("bounded native submission reconciliation was cancelled or expired")
        try:
            lease = ExclusiveLease(Path(campaign["root"]) / "submit.lease", ttl_s=3600).acquire()
        except LeaseHeldError:
            time.sleep(0.5)
            continue
        try:
            state = _state(campaign)
            if state["phase"] == "SUBMITTING" and not state["job_id"]:
                # The caller already verified this running native record's exact
                # scheduler-visible identity; it is positive acceptance evidence.
                _write_state(campaign, phase="SUBMITTED", job_id=job_id)
            elif state["phase"] != "SUBMITTED" or state["job_id"] != job_id:
                raise RuntimeError("submission state changed during exact reconciliation")
            return
        finally:
            lease.release()


def _validate_allocation(campaign: dict, stop_requested=None) -> tuple[dict, list[str], float]:
    stop_requested = stop_requested if stop_requested is not None else [False]
    job_id = os.environ.get("PBS_JOBID", "")
    nodefile = os.environ.get("PBS_NODEFILE", "")
    state = _state(campaign)
    if not job_id or not nodefile or state["phase"] not in {"SUBMITTED", "SUBMITTING"}:
        raise RuntimeError("execution requires this campaign's newly submitted native PBS job")
    if os.environ.get("AURORA_SUBJOB") or os.environ.get("SLURM_JOB_ID"):
        raise RuntimeError("campaign must begin in its original PBS batch allocation")
    nodes = _read_nodes(nodefile)
    request = SchedulerPlan(**campaign["scheduler"])
    _select_subset(nodes, 1, socket.gethostname())
    records = get_scheduler("pbs")._query("-f", job_id)
    if len(records) != 1:
        raise RuntimeError("cannot verify one native PBS allocation record")
    record = records[0]
    if (
        record.get("job_id") != job_id
        or record.get("job_state") != "R"
        or record.get("Job_Name") != _job_spec(campaign).job_name
    ):
        raise RuntimeError("live PBS acquisition identity/request does not match campaign")
    _bind_native_submission(campaign, job_id, stop_requested)
    if len(nodes) != request.nodes:
        raise _OwnedAllocationPreflightError(
            "physical allocation count differs from acquisition request"
        )
    if (
        record.get("Resource_List.walltime") != request.walltime
        or record.get("Account_Name") != request.account
    ):
        raise _OwnedAllocationPreflightError(
            "bound native PBS resource request differs from campaign"
        )
    actual_queue = record.get("queue", "")
    allowed_queues = (
        {"capacity"} if request.queue == "capacity" else {"prod", "small", "medium", "large"}
    )
    if actual_queue not in allowed_queues:
        raise _OwnedAllocationPreflightError(
            "live PBS job is outside the approved acquisition/routing queues"
        )
    allocated = list(
        dict.fromkeys(
            part.split("/", 1)[0] for part in record.get("exec_host", "").split("+") if part
        )
    )
    if (
        sorted(map(canonical_node_id, allocated)) != sorted(map(canonical_node_id, nodes))
        or not allocated
        or canonical_node_id(allocated[0]) != canonical_node_id(nodes[0])
        or record.get("Resource_List.nodect") != str(request.nodes)
    ):
        raise _OwnedAllocationPreflightError(
            "native PBS physical inventory differs from the nodefile/request"
        )
    # Refresh remaining walltime after any submit-publication wait.
    refreshed = get_scheduler("pbs")._query("-f", job_id)
    if len(refreshed) != 1 or refreshed[0].get("job_state") != "R":
        raise _OwnedAllocationPreflightError(
            "PBS allocation stopped while submission was reconciled"
        )
    record = refreshed[0]
    # prod is a routing queue: retain the actual execution queue separately.
    used = _seconds(record.get("resources_used.walltime", "00:00:00"))
    remaining = _seconds(request.walltime) - used
    acquisition = {
        "job_id": job_id,
        "physical_nodes": nodes,
        "physical_nodes_hash": canonical_hash(nodes),
        "original_nodefile": nodefile,
        "original_nodefile_sha256": _sha(nodefile),
        "requested_scheduler": campaign["scheduler"],
        "actual_queue": record.get("queue", ""),
        "remaining_walltime_s": remaining,
        "head": socket.gethostname(),
        "observed_at": _now(),
        "child_scheduler_requests_used_for_acquisition": False,
    }
    return acquisition, nodes, time.monotonic() + remaining


def _mpi_command(nodes: int, plan, argv: list[str]) -> list[str]:
    from exaserve.model_bcast import bootstrap_application_environment, mpi_launch_prefix
    from exaserve.plan.io import load_site_profile

    profile = load_site_profile(plan.site_profile_path)
    return [
        *mpi_launch_prefix(
            nodes,
            application_cwd="/tmp",
            application_environment=bootstrap_application_environment(profile=profile),
        ),
        *argv,
    ]


def _mpi_proof(plan, environment: dict, subset: list[str], root: Path) -> dict:
    from exaserve.control.finite_process import run_finite

    command = _mpi_command(len(subset), plan, ["/bin/hostname"])
    placed = run_finite(command, timeout_s=120, env=environment)
    observed = [line.strip() for line in placed.stdout.splitlines() if line.strip()]
    over = run_finite(
        _mpi_command(len(subset) + 1, plan, ["/bin/hostname"]),
        timeout_s=120,
        env=environment,
    )
    evidence = {
        "command": command,
        "stdout": placed.stdout,
        "stderr": placed.stderr,
        "returncode": placed.returncode,
        "overlaunch_returncode": over.returncode,
        "overlaunch_stdout": over.stdout,
        "overlaunch_stderr": over.stderr,
        "passed": placed.returncode == 0
        and sorted(map(canonical_node_id, observed)) == sorted(map(canonical_node_id, subset))
        and over.returncode != 0
        and not any(
            re.fullmatch(r"[A-Za-z0-9_.-]+", line.strip())
            for line in over.stdout.splitlines()
            if line.strip()
        )
        and "Cannot place all ranks" in (over.stdout + over.stderr),
    }
    atomic_create_json(root / "mpi_proof.json", evidence)
    if not evidence["passed"]:
        raise RuntimeError("logical-subset MPI placement/overlaunch proof failed")
    return evidence


def _owned_stop(process, *, grace_s: float) -> None:
    """Let frozen execute_run catch KeyboardInterrupt and stop its own backend."""
    from .backends.base import process_group_exists, terminate_process_tree

    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGINT)
        except ProcessLookupError:
            process.poll()
        try:
            process.wait(timeout=grace_s)
        except subprocess.TimeoutExpired:
            terminate_process_tree(process, process_group=process.pid, deadline_s=30)
            raise RuntimeError("canonical executor exceeded graceful cleanup; campaign must stop")
    if process_group_exists(process.pid):
        terminate_process_tree(process, process_group=process.pid, deadline_s=30)
        raise RuntimeError("executor left an owned process group after shutdown")


def _accept_child(
    plan, subset: list[str], job_id: str, ready_dir: Path, *, cancelled: bool
) -> dict:
    from .backends.ray import RayBackendAdapter
    from .paper_acceptance import require_accepted_paper_run, load_authenticated_result_json
    from .run_executor import _validate_replay_results
    from exaserve.plan.io import load_allocation_binding, load_run_provenance
    from exaserve.source_staging import validate_source_staging_result
    from exaserve.compat.profile import default_profile
    from exaserve.vllm_modelinfo_seed import expected_source_seed_evidence
    from exaserve.state.status import StatusStore
    from exaserve.state.receipts import load_receipt_manifest
    from exaserve.compat.receipt_v2 import ExactReceiptLedger, receipt_from_dict
    from exaserve.status_api import _validate_ready_payload

    status_dir = Path(plan.bundle.logs_dir) / "backend/deployment"
    binding = load_allocation_binding(status_dir / "allocation_binding.json")
    deployment = plan.semantic_plan.deployment
    if (
        binding.scheduler_allocation_id != job_id
        or binding.deployment_plan_hash != plan.deployment_plan_hash
        or [canonical_node_id(node) for _, node in binding.rank_to_node]
        != list(map(canonical_node_id, subset))
    ):
        raise RuntimeError("runtime allocation binding escaped the exact logical subset")
    ready = strict_json_load_path(ready_dir / "deployment_ready_evidence.json")
    _validate_ready_payload(
        ready["readiness_snapshot"],
        model_map=ready["model_map"],
        capability_map=ready["capability_map"],
        num_nodes=len(subset),
        exposure_mode=deployment.exposure.mode,
        plan=deployment,
    )
    observed = ready.get("readiness_snapshot", {}).get("nodes", [])
    if (
        ready.get("state") != "READY"
        or ready.get("generation") != binding.generation
        or ready.get("allocation_binding_hash") != binding.allocation_binding_hash
        or len(observed) != len(subset)
        or not all(node.get("alive") is True for node in observed)
        or sorted(canonical_node_id(node.get("node_name", "")) for node in observed)
        != sorted(map(canonical_node_id, subset))
    ):
        raise RuntimeError("READY network membership is not the exact logical subset")
    receipts = load_receipt_manifest(ready_dir / "compatibility_receipts.json")
    if receipts.manifest_hash != ready["readiness_snapshot"]["receipt_manifest_hash"]:
        raise RuntimeError("READY receipt manifest identity mismatch")
    ledger = ExactReceiptLedger(deployment, binding)
    for payload in receipts.receipts:
        receipt = receipt_from_dict(payload)
        ok, reason = ledger.accept(
            receipt,
            from_global_authority=receipt.owner_rank is None,
            session_rank=receipt.owner_rank,
            session_node=binding.node_for(receipt.owner_rank)
            if receipt.owner_rank is not None
            else None,
        )
        if not ok:
            raise RuntimeError(f"READY compatibility receipt rejected: {reason}")
    if not ledger.satisfied()[0]:
        raise RuntimeError("READY compatibility receipt set is not exact and complete")
    provenance = load_run_provenance(ready_dir / "run_provenance.json")
    if (
        provenance.source_snapshot_hash != plan.source_snapshot_hash
        or provenance.run_semantic_hash != plan.run_semantic_hash
        or provenance.allocation_binding_hash != binding.allocation_binding_hash
    ):
        raise RuntimeError("child source/semantic/allocation provenance mismatch")
    source_path = status_dir / "source_staging_manifest.json"
    validate_source_staging_result(
        strict_json_load_path(source_path),
        expected_deployment_id=deployment.deployment_id,
        expected_generation=binding.generation,
        expected_plan_hash=plan.deployment_plan_hash,
        expected_site_profile_hash=deployment.site_profile_hash,
        expected_binding_hash=binding.allocation_binding_hash,
        expected_compatibility_profile_id=deployment.compatibility_profile_hash,
        expected_compatibility_manifest_hash=deployment.manifest_hash,
        expected_rank_to_node=binding.rank_to_node,
        expected_run_dir=status_dir,
        require_seed_evidence=True,
        require_current_state_layout=True,
        expected_seed_evidence=expected_source_seed_evidence(
            default_profile(deployment.vendor),
            install_required=deployment.engine == "vllm" and not deployment.runtime.null_compute,
        ),
    )
    RayBackendAdapter._validate_shutdown_evidence(
        SimpleNamespace(run_plan=plan),
        SimpleNamespace(
            monitor=SimpleNamespace(
                status_dir=str(status_dir), expected_generation=binding.generation
            )
        ),
        expected_terminal_state="STOPPED",
    )
    record = StatusStore.run(plan.bundle.state_path).load()
    manifest_hash = None
    if cancelled:
        _require_clean_cancelled_status(plan, record)
    else:
        replay = _validate_replay_results(plan)
        if replay["incomplete_reasons"] or replay.get("errors"):
            raise RuntimeError("child replay is incomplete or contains request errors")
        accepted = require_accepted_paper_run(
            plan.bundle.root_dir,
            required_result_ids=(
                *replay["expected_ids"],
                "deployment_ready_evidence",
                "compatibility_receipts",
                "run_provenance",
            ),
        )
        trace_metadata = strict_json_load_path(plan.trace_artifact.metadata_path)
        requests = trace_metadata.get("row_count")
        if type(requests) is not int or requests <= 0:
            raise RuntimeError("hash-bound trace metadata lacks a positive request count")
        for logical_id in replay["expected_ids"]:
            entries = [
                entry for entry in accepted.manifest.entries if entry.logical_id == logical_id
            ]
            if len(entries) != 1:
                raise RuntimeError("replay has no unique authenticated result entry")
            document = load_authenticated_result_json(plan.bundle.root_dir, entries[0])
            records = document.get("per_run")
            if not isinstance(records, list) or len(records) != 2:
                raise RuntimeError("paper protocol requires exactly two complete replays")
            for index, record in enumerate(records):
                if (
                    type(record.get("run_index")) is not int
                    or record["run_index"] != index
                    or any(
                        type(record.get(field)) is not int or record[field] != requests
                        for field in ("requests_scheduled", "requests_completed")
                    )
                    or type(record.get("errors")) is not int
                    or record["errors"] != 0
                ):
                    raise RuntimeError(
                        "replay does not cover the exact hash-bound trace request count"
                    )
            overall = document.get("overall")
            mirrored = (
                "requests_completed",
                "requests_scheduled",
                "errors",
                "duration_s",
                "rps",
                "p50_s",
                "p99_s",
            )
            if not isinstance(overall, dict) or any(
                field not in overall
                or field not in records[-1]
                or canonical_hash(overall[field]) != canonical_hash(records[-1][field])
                for field in mirrored
            ):
                raise RuntimeError("overall result does not mirror the reported final replay")
        manifest_hash = accepted.manifest.manifest_hash
    paths = [
        source_path,
        status_dir / "allocation_binding.json",
        status_dir / "shutdown_report.json",
        status_dir / "deployment_status.json",
        Path(plan.bundle.state_path),
        ready_dir / "deployment_ready_evidence.json",
        ready_dir / "compatibility_receipts.json",
        ready_dir / "run_provenance.json",
    ]
    if not cancelled:
        paths.append(Path(plan.bundle.results_dir) / "result_manifest.json")
    return {
        "outcome": "CANCELLED_AFTER_READY" if cancelled else "SUCCEEDED",
        "generation": binding.generation,
        "allocation_binding_hash": binding.allocation_binding_hash,
        "result_manifest_hash": manifest_hash,
        "evidence": {str(path): _sha(path) for path in paths},
    }


def _require_child_status_identity(plan, record) -> None:
    expected = {
        "run_id": plan.run_id,
        "run_group_id": plan.run_group_id,
        "run_semantic_hash": plan.run_semantic_hash,
        "deployment_plan_hash": plan.deployment_plan_hash,
        "source_snapshot_hash": plan.source_snapshot_hash,
    }
    if (
        record is None
        or record.record_id != f"{plan.run_group_id}/{plan.run_id}"
        or record.provenance != expected
    ):
        raise RuntimeError("cancellation checkpoint RunStatus identity mismatch")


def _require_clean_cancelled_status(plan, record) -> None:
    _require_child_status_identity(plan, record)
    if record.state != "CANCELLED" or "cleanup_error" in record.data:
        raise RuntimeError("qualification cancellation lacks clean canonical CANCELLED RunStatus")


def _can_cancel_after_ready(plan, status) -> bool:
    """Wait for the frozen executor to acknowledge READY before injecting SIGINT.

    Its readiness observer polls independently of this controller. A READY
    publication alone can precede its observed-readiness teardown contract;
    the typed replay phase proves that wait_ready has actually returned.
    """
    if status is None or not status.ready:
        return False
    from exaserve.state.status import StatusStore

    record = StatusStore.run(plan.bundle.state_path).load()
    if record is None:
        return False
    _require_child_status_identity(plan, record)
    return record.state == "RUNNING" and record.data.get("phase") == "replaying"


def _run_child(
    campaign,
    child,
    index,
    subset,
    nodefile,
    job_id,
    stop_requested,
    deadline,
    parent_heartbeat=None,
):
    from exaserve.evidence import capture_ready_evidence
    from exaserve.status_api import read_deployment_status

    root = Path(campaign["root"])
    receipt_dir = Path(ensure_owned_directory(root / f"child-{index}"))
    plan = _validate_child_inputs(child)
    environment = _child_environment(child, nodefile, job_id)
    cancel_test = campaign["qualification_mode"] and index == 1
    child_started = time.monotonic()
    watchdog = float(plan.semantic_plan.deployment.control.watchdog_cleanup_deadline_s)
    cleanup_grace = watchdog + min(30.0, max(5.0, watchdog * 0.1))
    budget = min(campaign["child_timeout_s"], deadline - child_started - cleanup_grace - _FORCE_S)
    if budget < campaign["child_timeout_s"]:
        raise RuntimeError("remaining parent walltime cannot cover the next child's budget")
    command = [sys.executable, "-u", "-m", "eval.cli", "run", "execute", child["run_yaml"]]
    receipt = {
        "child": child,
        "job_id": job_id,
        "subset": subset,
        "subset_hash": canonical_hash(subset),
        "subset_nodefile_sha256": _sha(nodefile),
        "command": command,
        "controller_source_snapshot_hash": campaign["controller"]["source_snapshot_hash"],
        "started_at": _now(),
        "environment": {
            key: environment[key]
            for key in (
                "PBS_JOBID",
                "PBS_NODEFILE",
                "EXASERVE_NODEFILE",
                "PYTHONPATH",
                "PYTHONNOUSERSITE",
                "PYTHONSAFEPATH",
                "PYTHONDONTWRITEBYTECODE",
            )
        },
    }
    atomic_create_json(receipt_dir / "invocation.json", receipt)
    cancelled = False
    with ExclusiveLease(plan.bundle.state_path + ".submit", ttl_s=300) as lease:
        with LeaseHeartbeat(lease, interval_s=30) as heartbeat:
            _require_fresh(plan)
            with open(receipt_dir / "executor.log", "x", encoding="utf-8") as log:
                process = subprocess.Popen(
                    command,
                    cwd=child["snapshot_root"],
                    env=environment,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                    text=True,
                )
                try:
                    while process.poll() is None:
                        if parent_heartbeat is not None:
                            parent_heartbeat.ensure_held()
                        heartbeat.ensure_held()
                        if stop_requested[0] or time.monotonic() - child_started > budget:
                            raise RuntimeError("parent cancelled or child exceeded its deadline")
                        status_dir = str(Path(plan.bundle.logs_dir) / "backend/deployment")
                        status = read_deployment_status(status_dir)
                        if status is not None and status.terminal and status.state == "FAILED":
                            raise RuntimeError(
                                f"child deployment failed early: {status.reason_code}: {status.detail}"
                            )
                        if cancel_test and _can_cancel_after_ready(plan, status):
                            capture_ready_evidence(
                                status_dir=status_dir,
                                destination_dir=str(receipt_dir),
                                expected_generation=status.generation,
                                expected_plan_hash=plan.deployment_plan_hash,
                                expected_run_semantic_hash=plan.run_semantic_hash,
                            )
                            cancelled = True
                            _owned_stop(
                                process,
                                grace_s=max(
                                    0, min(cleanup_grace, deadline - time.monotonic() - _FORCE_S)
                                ),
                            )
                            break
                        time.sleep(0.5)
                except BaseException as exc:
                    try:
                        _owned_stop(
                            process,
                            grace_s=max(
                                0, min(cleanup_grace, deadline - time.monotonic() - _FORCE_S)
                            ),
                        )
                    except BaseException as cleanup_exc:
                        add_exception_note(
                            exc, f"canonical executor cleanup also failed: {cleanup_exc}"
                        )
                    raise
                _owned_stop(process, grace_s=0)
                if (cancel_test and (not cancelled or process.returncode == 0)) or (
                    not cancelled and process.returncode != 0
                ):
                    raise RuntimeError(
                        f"child exit {process.returncode} did not meet its scenario contract"
                    )
            heartbeat.ensure_held()
            if parent_heartbeat is not None:
                parent_heartbeat.ensure_held()
            accepted = _accept_child(
                plan,
                subset,
                job_id,
                receipt_dir if cancelled else Path(plan.bundle.results_dir),
                cancelled=cancelled,
            )
            _validate_child_inputs(child)
            if parent_heartbeat is not None:
                parent_heartbeat.ensure_held()
            receipt.update(
                accepted, completed_at=_now(), elapsed_s=time.monotonic() - child_started
            )
            atomic_create_json(receipt_dir / "receipt.json", receipt)
            return receipt


def _audit_paths(children: list[dict], receipts: list[dict]) -> list[str]:
    paths = [child["run_yaml"] for child in children]
    for child in children:
        paths.extend(path for path in child["inputs"] if path.endswith("/deployment.plan.json"))
    for receipt in receipts:
        source_path = next(
            path for path in receipt["evidence"] if path.endswith("/source_staging_manifest.json")
        )
        source = strict_json_load_path(source_path)
        paths.extend([source["local_runtime_root"], source["local_state_root"]])
    return sorted(set(paths))


def _require_clean_audit(audit: dict, nodes: list[str]) -> None:
    fields = {
        "clean",
        "records",
        "stderr",
        "returncode",
        "sentinel_token",
        "deployment_ids",
        "runtime_paths",
    }
    if not isinstance(audit, dict) or set(audit) != fields:
        raise ValueError("process audit shape mismatch")
    records = audit["records"]
    if (
        audit["clean"] is not True
        or type(audit["returncode"]) is not int
        or audit["returncode"] != 0
        or not isinstance(records, list)
        or len(records) != len(nodes)
        or any(
            not isinstance(record, dict)
            or set(record) != {"host", "remaining_pids"}
            or record["remaining_pids"] != []
            for record in records
        )
        or sorted(canonical_node_id(record["host"]) for record in records)
        != sorted(map(canonical_node_id, nodes))
        or not audit["deployment_ids"]
        or not audit["runtime_paths"]
    ):
        raise RuntimeError("generation-owned processes remain or the physical audit is incomplete")


def _process_audit(
    plan,
    environment,
    nodes: list[str],
    deployment_ids: list[str],
    root: Path,
    *,
    runtime_paths: list[str],
    sentinel_token: str | None = None,
) -> dict:
    """Only node-local /proc is inspected; diagnostic ranks read no shared files."""
    import json
    import base64
    from exaserve.control.finite_process import run_finite

    code = (
        "import os,json,socket,sys,base64; targets=json.loads(base64.b64decode(sys.argv[1])); found=[]\n"
        "for entry in os.listdir('/proc'):\n"
        " if not entry.isdigit(): continue\n"
        " try:\n"
        "  p='/proc/'+entry\n"
        "  if os.stat(p).st_uid != os.getuid(): continue\n"
        "  with open(p+'/environ','rb') as f: env=f.read().split(b'\\0')\n"
        "  with open(p+'/cmdline','rb') as f: args=f.read().split(b'\\0')\n"
        "  owned=any(('EXASERVE_DEPLOYMENT_ID='+i).encode() in env for i in targets['ids'])\n"
        "  owned=owned or any(path.encode() in arg for path in targets['paths'] for arg in args)\n"
        "  owned=owned or (targets['sentinel'] is not None and targets['sentinel'].encode() in args)\n"
        "  if owned: found.append(int(entry))\n"
        " except (FileNotFoundError,ProcessLookupError): pass\n"
        "print(json.dumps({'host':socket.gethostname(),'remaining_pids':found}),flush=True)"
    )
    targets = base64.b64encode(
        json.dumps(
            {"ids": deployment_ids, "paths": runtime_paths, "sentinel": sentinel_token}
        ).encode()
    ).decode()
    result = run_finite(
        _mpi_command(len(nodes), plan, [sys.executable, "-I", "-S", "-c", code, targets]),
        timeout_s=120,
        env=environment,
    )
    records = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    clean = (
        result.returncode == 0
        and len(records) == len(nodes)
        and all(r["remaining_pids"] == [] for r in records)
        and sorted(canonical_node_id(r["host"]) for r in records)
        == sorted(map(canonical_node_id, nodes))
    )
    evidence = {
        "clean": clean,
        "records": records,
        "stderr": result.stderr,
        "returncode": result.returncode,
        "sentinel_token": sentinel_token,
        "deployment_ids": deployment_ids,
        "runtime_paths": runtime_paths,
    }
    atomic_create_json(root / "process_audit.json", evidence)
    _require_clean_audit(evidence, nodes)
    return evidence


def _campaign_guard(stop_requested, heartbeat, deadline: float) -> None:
    """Cancellation and fencing remain authoritative through final publication."""
    heartbeat.ensure_held()
    if stop_requested[0]:
        raise RuntimeError("parent cancellation requested")
    if time.monotonic() >= deadline:
        raise RuntimeError("parent allocation deadline expired")


def _timestamp_s(value: str) -> float:
    from datetime import datetime

    if not isinstance(value, str):
        raise ValueError("campaign evidence timestamp must be text")
    timestamp = datetime.fromisoformat(value)
    if timestamp.tzinfo is None:
        raise ValueError("campaign evidence timestamp must include its timezone")
    return timestamp.timestamp()


def _validate_sentinel_heartbeat(record, expected_host, token, expected_identity=None) -> None:
    if (
        not isinstance(record, dict)
        or set(record) != {"time", "host", "pid", "token", "sequence"}
        or not isinstance(record["host"], str)
        or canonical_node_id(record["host"]) != canonical_node_id(expected_host)
        or type(record["pid"]) is not int
        or record["pid"] <= 0
        or record["token"] != token
        or type(record["sequence"]) is not int
        or record["sequence"] < 0
    ):
        raise RuntimeError("excluded-node sentinel heartbeat identity is invalid")
    _positive(record["time"], "sentinel heartbeat time")
    if expected_identity is not None and any(
        record[field] != expected_identity[field] for field in ("host", "pid", "token")
    ):
        raise RuntimeError("excluded-node sentinel host/PID/token changed")


def _sentinel_witness(
    process,
    log_path,
    *,
    expected_host,
    token,
    stop_requested,
    heartbeat,
    deadline,
    expected_identity=None,
    after_time=None,
    timeout_s=30.0,
) -> dict:
    """The head attests a fresh remote heartbeat against an exact log prefix."""
    witness_deadline = min(deadline, time.monotonic() + _positive(timeout_s, "sentinel timeout"))
    while True:
        _campaign_guard(stop_requested, heartbeat, deadline)
        if process.poll() is not None:
            raise RuntimeError("excluded-node sentinel exited before its survival witness")
        with regular_file_reader(log_path, binary=True) as stream:
            transcript = stream.read()
        # MPI may emit diagnostics, and the final write can be incomplete. Only
        # complete structured heartbeat lines are candidates; diagnostics remain
        # bound by the witness's transcript prefix and final transcript digest.
        boundary = transcript.rfind(b"\n") + 1
        complete = transcript[:boundary]
        lines = complete.splitlines(keepends=True)
        for line in reversed(lines):
            if not line.lstrip().startswith(b"{"):
                boundary -= len(line)
                continue
            record = strict_json_loads(line.decode("utf-8"))
            _validate_sentinel_heartbeat(record, expected_host, token, expected_identity)
            observed = time.time()
            if (
                -1 <= observed - record["time"] <= 5
                and (after_time is None or record["time"] >= after_time)
                and (
                    expected_identity is None or record["sequence"] > expected_identity["sequence"]
                )
            ):
                _campaign_guard(stop_requested, heartbeat, deadline)
                return {
                    "heartbeat": record,
                    "observed_at_s": observed,
                    "transcript_bytes": boundary,
                    "transcript_sha256": hashlib.sha256(transcript[:boundary]).hexdigest(),
                }
            break
        if time.monotonic() >= witness_deadline:
            raise RuntimeError("excluded-node sentinel did not provide a fresh bounded heartbeat")
        time.sleep(min(0.25, max(0, witness_deadline - time.monotonic())))


def _validate_sentinel_evidence(
    evidence, log_path, *, campaign_hash: str, nodes: list[str], receipts: list[dict]
) -> None:
    """Reconstruct startup and each cleanup witness from the sealed transcript."""
    fields = {
        "survived_cleanup",
        "reaped",
        "token",
        "host",
        "pid",
        "log_sha256",
        "startup",
        "cleanup_checks",
    }
    if (
        not isinstance(evidence, dict)
        or set(evidence) != fields
        or evidence["survived_cleanup"] is not True
        or evidence["reaped"] is not True
        or not isinstance(evidence["cleanup_checks"], list)
        or len(evidence["cleanup_checks"]) != len(receipts)
        or not receipts
    ):
        raise ValueError("qualification sentinel requires startup and every cleanup witness")
    token = "--exaserve-subset-sentinel=" + campaign_hash
    if evidence["token"] != token:
        raise ValueError("qualification sentinel token differs from the campaign")
    with regular_file_reader(log_path, binary=True) as stream:
        transcript = stream.read()
    if hashlib.sha256(transcript).hexdigest() != evidence["log_sha256"]:
        raise ValueError("qualification sentinel transcript changed")
    witnesses = [evidence["startup"], *evidence["cleanup_checks"]]
    identity = None
    previous = None
    for index, witness in enumerate(witnesses):
        if not isinstance(witness, dict) or set(witness) != {
            "heartbeat",
            "observed_at_s",
            "transcript_bytes",
            "transcript_sha256",
        }:
            raise ValueError("qualification sentinel witness shape mismatch")
        size = witness["transcript_bytes"]
        if type(size) is not int or not 0 < size <= len(transcript):
            raise ValueError("qualification sentinel transcript prefix is invalid")
        prefix = transcript[:size]
        if (
            not prefix.endswith(b"\n")
            or hashlib.sha256(prefix).hexdigest() != witness["transcript_sha256"]
            or strict_json_loads(prefix.splitlines()[-1].decode("utf-8")) != witness["heartbeat"]
        ):
            raise ValueError("qualification sentinel witness is not bound to its transcript")
        record = witness["heartbeat"]
        _validate_sentinel_heartbeat(record, nodes[-1], token, identity)
        observed = _positive(witness["observed_at_s"], "sentinel observation time")
        if not -1 <= observed - record["time"] <= 5:
            raise ValueError("qualification sentinel witness was stale or from the future")
        if index == 0:
            identity = record
            if observed > _timestamp_s(receipts[0]["started_at"]):
                raise ValueError("qualification sentinel was not witnessed before child startup")
            if evidence["host"] != record["host"] or evidence["pid"] != record["pid"]:
                raise ValueError("qualification sentinel summary identity differs from startup")
        else:
            completed = _timestamp_s(receipts[index - 1]["completed_at"])
            if (
                record["time"] < completed
                or observed < completed
                or observed <= previous["observed_at_s"]
                or record["sequence"] <= previous["heartbeat"]["sequence"]
                or size <= previous["transcript_bytes"]
            ):
                raise ValueError("qualification sentinel lacks a new post-cleanup heartbeat")
            if index < len(receipts) and observed > _timestamp_s(receipts[index]["started_at"]):
                raise ValueError("qualification sentinel cleanup witness followed the next child")
        previous = witness


def execute_campaign(path: str) -> int:
    """Run one finite sequence inside its real PBS parent allocation."""
    campaign = _load_campaign(path)
    if (
        Path(__file__).resolve()
        != (
            Path(campaign["controller"]["snapshot_root"]) / "eval/lib/allocation_campaign.py"
        ).resolve()
    ):
        raise RuntimeError("campaign execution must import its sealed controller snapshot")
    root = Path(campaign["root"])
    stop_requested = [False]
    previous = {}
    for sig in (signal.SIGTERM, signal.SIGINT):
        previous[sig] = signal.signal(sig, lambda _sig, _frame: stop_requested.__setitem__(0, True))
    sentinel = None
    sentinel_log = None
    owns_execution = False
    try:
        with ExclusiveLease(root / "execute.lease", ttl_s=300) as lease:
            with LeaseHeartbeat(lease, interval_s=30) as heartbeat:
                try:
                    acquisition, nodes, deadline = _validate_allocation(campaign, stop_requested)
                except _OwnedAllocationPreflightError:
                    owns_execution = True
                    raise
                owns_execution = True
                _campaign_guard(stop_requested, heartbeat, deadline)
                atomic_create_json(root / "acquisition.json", acquisition)
                _write_state(campaign, phase="RUNNING")
                plan = _validate_child_inputs(campaign["children"][0])
                reports = []
                mpi_proof = None
                sentinel_evidence = {"survived_cleanup": False, "reaped": False}
                # The nodefiles consumed by head-side PALS are verified tmpfs,
                # never shared manifests exposed to ranks.
                from exaserve.plan.io import load_site_profile
                from exaserve.source_staging import qualify_runtime_staging_base

                local_base = qualify_runtime_staging_base(
                    Path("/tmp"), load_site_profile(plan.site_profile_path)
                )
                with tempfile.TemporaryDirectory(
                    prefix="allocation-campaign-", dir=local_base
                ) as temporary:
                    physical_file = Path(temporary) / "physical.nodes"
                    atomic_create_text(physical_file, "\n".join(nodes) + "\n")
                    physical_env = _child_environment(
                        campaign["children"][0], str(physical_file), acquisition["job_id"]
                    )
                    if campaign["qualification_mode"]:
                        spare_file = Path(temporary) / "excluded.nodes"
                        atomic_create_text(spare_file, nodes[-1] + "\n")
                        spare_env = _child_environment(
                            campaign["children"][0], str(spare_file), acquisition["job_id"]
                        )
                        code = (
                            "import time,socket,os,sys,json\nsequence=0\nwhile True:\n"
                            " print(json.dumps({'time':time.time(),'host':socket.gethostname(),"
                            "'pid':os.getpid(),'token':sys.argv[1],'sequence':sequence}),flush=True)\n"
                            " sequence+=1;time.sleep(1)"
                        )
                        sentinel_log = open(root / "sentinel.log", "x", encoding="utf-8")
                        sentinel_token = "--exaserve-subset-sentinel=" + campaign["campaign_hash"]
                        sentinel_evidence["token"] = sentinel_token
                        sentinel = subprocess.Popen(
                            _mpi_command(
                                1,
                                plan,
                                [sys.executable, "-I", "-S", "-u", "-c", code, sentinel_token],
                            ),
                            env=spare_env,
                            cwd="/tmp",
                            stdout=sentinel_log,
                            stderr=subprocess.STDOUT,
                            start_new_session=True,
                            text=True,
                        )
                        startup = _sentinel_witness(
                            sentinel,
                            root / "sentinel.log",
                            expected_host=nodes[-1],
                            token=sentinel_token,
                            stop_requested=stop_requested,
                            heartbeat=heartbeat,
                            deadline=deadline,
                            timeout_s=120,
                        )
                        sentinel_evidence.update(
                            startup=startup,
                            cleanup_checks=[],
                            host=startup["heartbeat"]["host"],
                            pid=startup["heartbeat"]["pid"],
                        )
                    for index, child in enumerate(campaign["children"]):
                        _campaign_guard(stop_requested, heartbeat, deadline)
                        subset = _select_subset(nodes, child["logical_nodes"], socket.gethostname())
                        subset_file = Path(temporary) / f"subset-{index}.nodes"
                        atomic_create_text(subset_file, "\n".join(subset) + "\n")
                        if campaign["qualification_mode"] and index == 0:
                            mpi_proof = _mpi_proof(
                                plan,
                                _child_environment(child, str(subset_file), acquisition["job_id"]),
                                subset,
                                root,
                            )
                            _campaign_guard(stop_requested, heartbeat, deadline)
                        receipt = _run_child(
                            campaign,
                            child,
                            index,
                            subset,
                            str(subset_file),
                            acquisition["job_id"],
                            stop_requested,
                            deadline,
                            heartbeat,
                        )
                        _campaign_guard(stop_requested, heartbeat, deadline)
                        reports.append(receipt)
                        audit_root = Path(ensure_owned_directory(root / f"audit-{index}"))
                        audit = _process_audit(
                            plan,
                            physical_env,
                            nodes,
                            [c["deployment_id"] for c in campaign["children"]],
                            audit_root,
                            runtime_paths=_audit_paths(campaign["children"], reports),
                        )
                        _campaign_guard(stop_requested, heartbeat, deadline)
                        if sentinel is not None:
                            check = _sentinel_witness(
                                sentinel,
                                root / "sentinel.log",
                                expected_host=nodes[-1],
                                token=sentinel_token,
                                stop_requested=stop_requested,
                                heartbeat=heartbeat,
                                deadline=deadline,
                                expected_identity=sentinel_evidence["startup"]["heartbeat"],
                                after_time=_timestamp_s(receipt["completed_at"]),
                            )
                            sentinel_evidence["cleanup_checks"].append(check)
                    if sentinel is not None:
                        sentinel_log.flush()
                        _campaign_guard(stop_requested, heartbeat, deadline)
                        sentinel_evidence["survived_cleanup"] = True
                        _owned_stop(sentinel, grace_s=20)
                        _campaign_guard(stop_requested, heartbeat, deadline)
                        sentinel_evidence["reaped"] = True
                        sentinel_evidence["log_sha256"] = _sha(root / "sentinel.log")
                        sentinel = None
                        _validate_sentinel_evidence(
                            sentinel_evidence,
                            root / "sentinel.log",
                            campaign_hash=campaign["campaign_hash"],
                            nodes=nodes,
                            receipts=reports,
                        )
                        audit = _process_audit(
                            plan,
                            physical_env,
                            nodes,
                            [c["deployment_id"] for c in campaign["children"]],
                            Path(ensure_owned_directory(root / "sentinel-reaped")),
                            runtime_paths=_audit_paths(campaign["children"], reports),
                            sentinel_token=sentinel_token,
                        )
                        _campaign_guard(stop_requested, heartbeat, deadline)
                    _campaign_guard(stop_requested, heartbeat, deadline)
                    report = {
                        "schema_version": _SCHEMA,
                        "campaign_hash": campaign["campaign_hash"],
                        "controller_source_snapshot_hash": campaign["controller"][
                            "source_snapshot_hash"
                        ],
                        "child_source_snapshot_hash": campaign["children"][0][
                            "source_snapshot_hash"
                        ],
                        "acquisition": acquisition,
                        "mpi_proof": mpi_proof,
                        "children": reports,
                        "sentinel": sentinel_evidence,
                        "process_audit": audit,
                        "verdict": "PASS",
                        "completed_at": _now(),
                    }
                    _campaign_guard(stop_requested, heartbeat, deadline)
                    atomic_create_json(
                        root
                        / (
                            "qualification.json"
                            if campaign["qualification_mode"]
                            else "completion.json"
                        ),
                        report,
                    )
                    _campaign_guard(stop_requested, heartbeat, deadline)
                    _write_state(campaign, phase="SUCCEEDED")
                    _campaign_guard(stop_requested, heartbeat, deadline)
                    return 0
    except BaseException as exc:
        if owns_execution:
            try:
                _write_state(campaign, phase="FAILED", error=f"{type(exc).__name__}: {exc}")
            except BaseException as publication_exc:
                add_exception_note(
                    exc, f"parent failure publication also failed: {publication_exc}"
                )
        raise
    finally:
        active_error = sys.exc_info()[1]
        cleanup_failures = []
        if sentinel is not None:
            try:
                _owned_stop(sentinel, grace_s=20)
            except BaseException as exc:
                cleanup_failures.append(exc)
        if sentinel_log is not None:
            try:
                sentinel_log.close()
            except BaseException as exc:
                cleanup_failures.append(exc)
        for sig, handler in previous.items():
            try:
                signal.signal(sig, handler)
            except BaseException as exc:
                cleanup_failures.append(exc)
        if cleanup_failures:
            if active_error is not None:
                for exc in cleanup_failures:
                    add_exception_note(active_error, f"parent final cleanup also failed: {exc}")
            else:
                raise RuntimeError(f"parent final cleanup failed: {cleanup_failures}")
