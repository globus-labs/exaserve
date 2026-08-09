"""Shared eval/ClientLab capture of one canonical READY evidence bundle."""

from __future__ import annotations

import os
from dataclasses import asdict


def capture_ready_evidence(
    *,
    status_dir: str,
    destination_dir: str,
    expected_generation: int,
    expected_plan_hash: str,
    expected_run_semantic_hash: str,
) -> dict[str, str]:
    from .plan.io import load_run_provenance
    from .state.atomic import (
        atomic_create_or_verify_bytes,
        atomic_create_or_verify_json,
        regular_file_reader,
    )
    from .state.receipts import load_receipt_manifest
    from .status_api import read_deployment_status

    status = read_deployment_status(status_dir)
    if (
        status is None
        or not status.ready
        or status.generation != expected_generation
        or status.deployment_plan_hash != expected_plan_hash
    ):
        raise RuntimeError(
            "deployment lost the exact canonical READY generation before result commit"
        )
    from .state.atomic import ensure_owned_directory

    ensure_owned_directory(destination_dir)
    status_path = os.path.join(destination_dir, "deployment_ready_evidence.json")
    atomic_create_or_verify_json(status_path, asdict(status))

    snapshot = status.readiness_snapshot
    receipt_source = snapshot.get("receipt_manifest_path")
    receipt_hash = snapshot.get("receipt_manifest_hash")
    if not isinstance(receipt_source, str) or not receipt_source:
        raise RuntimeError("READY status names no compatibility receipt manifest")
    if not isinstance(receipt_hash, str):
        raise RuntimeError("READY status receipt manifest hash is malformed")
    receipt_dest = os.path.join(destination_dir, "compatibility_receipts.json")
    with regular_file_reader(receipt_source, binary=True) as handle:
        receipt_bytes = handle.read()
    atomic_create_or_verify_bytes(receipt_dest, receipt_bytes)
    receipt_manifest = load_receipt_manifest(receipt_dest)
    if receipt_manifest.manifest_hash != receipt_hash or receipt_manifest.receipt_hashes != tuple(
        sorted(status.receipt_hashes)
    ):
        raise RuntimeError("READY receipt manifest disagrees with deployment status")
    provenance_source = os.path.join(status_dir, "run_provenance.json")
    provenance_dest = os.path.join(destination_dir, "run_provenance.json")
    with regular_file_reader(provenance_source, binary=True) as handle:
        provenance_bytes = handle.read()
    atomic_create_or_verify_bytes(provenance_dest, provenance_bytes)
    provenance = load_run_provenance(provenance_dest)
    if (
        provenance.run_provenance_hash != status.run_provenance_hash
        or provenance.run_semantic_hash != expected_run_semantic_hash
        or provenance.deployment_plan_hash != expected_plan_hash
    ):
        raise RuntimeError("run provenance disagrees with READY status/RunPlan")
    return {
        "deployment_ready_evidence": status_path,
        "compatibility_receipts": receipt_dest,
        "run_provenance": provenance_dest,
    }
