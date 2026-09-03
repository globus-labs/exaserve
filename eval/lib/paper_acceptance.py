"""Fail-closed binding from a paper result back to its immutable RunPlan."""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from exaserve.plan.io import run_provenance_from_dict
from exaserve.state.atomic import regular_file_reader, strict_json_loads
from exaserve.state.results import ResultEntry, ResultManifest, load_result_manifest
from exaserve.state.status import StatusRecord, StatusStore

from .run_planner import load_run_plan


@dataclass(frozen=True)
class AcceptedPaperRun:
    run_plan: object
    manifest: ResultManifest
    status: StatusRecord
    run_provenance: object


def load_authenticated_result_json(cell_dir: str | Path, entry: ResultEntry):
    """Read, authenticate, and decode one manifest entry from one descriptor."""
    if type(entry) is not ResultEntry:
        raise TypeError("authenticated paper result requires a ResultEntry")
    cell = Path(cell_dir)
    root = os.path.abspath(cell / "results")
    candidate = os.path.abspath(os.path.join(root, entry.path))
    try:
        if os.path.commonpath((root, candidate)) != root:
            raise RuntimeError(f"paper result entry escapes its bundle: {entry.path!r}")
        with regular_file_reader(candidate, binary=True) as handle:
            payload = handle.read()
            size = os.fstat(handle.fileno()).st_size
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"paper result entry is unavailable: {candidate}") from exc
    digest = hashlib.sha256(payload).hexdigest()
    if size != entry.size_bytes or digest != entry.sha256:
        raise RuntimeError(f"paper result entry content changed: {candidate}")
    try:
        return strict_json_loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise RuntimeError(f"paper result entry is not strict UTF-8 JSON: {candidate}") from exc


def require_accepted_paper_run(
    cell_dir: str | Path, *, required_result_ids: Iterable[str]
) -> AcceptedPaperRun:
    """Verify success, manifest bytes, and every canonical identity edge."""
    cell = Path(cell_dir)
    run_plan = load_run_plan(str(cell / "run.yaml"))
    if Path(run_plan.bundle.root_dir) != cell:
        raise RuntimeError(f"paper cell path disagrees with RunPlan bundle: {cell}")
    manifest = load_result_manifest(str(cell / "results" / "result_manifest.json"))
    required = set(required_result_ids)
    if not manifest.complete or not required.issubset(manifest.expected_ids):
        raise RuntimeError(f"paper result manifest is incomplete: {cell}")
    if (
        manifest.run_id != run_plan.run_id
        or manifest.run_semantic_hash != run_plan.run_semantic_hash
        or manifest.deployment_plan_hash != run_plan.deployment_plan_hash
    ):
        raise RuntimeError(f"paper ResultManifest identity disagrees with RunPlan: {cell}")
    status = StatusStore.run(run_plan.bundle.state_path).load()
    expected_status_provenance = {
        "run_id": run_plan.run_id,
        "run_group_id": run_plan.run_group_id,
        "run_semantic_hash": run_plan.run_semantic_hash,
        "deployment_plan_hash": run_plan.deployment_plan_hash,
        "source_snapshot_hash": run_plan.source_snapshot_hash,
    }
    if (
        status is None
        or status.state != "SUCCEEDED"
        or status.record_id != f"{run_plan.run_group_id}/{run_plan.run_id}"
        or status.provenance != expected_status_provenance
        or status.data.get("result_manifest_hash") != manifest.manifest_hash
    ):
        raise RuntimeError(f"paper RunStatus identity/completion is invalid: {cell}")
    provenance_entries = [
        entry for entry in manifest.entries if entry.logical_id == "run_provenance"
    ]
    if len(provenance_entries) != 1:
        raise RuntimeError(f"paper result has no unique run provenance: {cell}")
    provenance = run_provenance_from_dict(
        load_authenticated_result_json(cell, provenance_entries[0])
    )
    deployment = run_plan.semantic_plan.deployment
    if (
        provenance.run_id != run_plan.run_id
        or provenance.deployment_id != deployment.deployment_id
        or provenance.deployment_plan_hash != run_plan.deployment_plan_hash
        or provenance.run_semantic_hash != run_plan.run_semantic_hash
        or provenance.source_snapshot_hash != run_plan.source_snapshot_hash
    ):
        raise RuntimeError(f"paper RunProvenance identity disagrees with RunPlan: {cell}")
    return AcceptedPaperRun(
        run_plan=run_plan,
        manifest=manifest,
        status=status,
        run_provenance=provenance,
    )
