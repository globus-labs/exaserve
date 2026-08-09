"""Persisted state rejects coercion and ambiguous JSON encodings."""

from __future__ import annotations

import pytest

from exaserve.state.atomic import atomic_write_json, strict_json_loads
from exaserve.state.gateway import classify_gateway_evidence
from exaserve.state.process_ownership import (
    ProcessOwnershipError,
    ProcessOwnershipReceipt,
)
from exaserve.state.receipts import ReceiptManifest, ReceiptManifestError


def test_strict_state_json_rejects_duplicate_keys_and_nonfinite_numbers(tmp_path):
    with pytest.raises(ValueError, match="duplicate JSON key"):
        strict_json_loads('{"state":"READY","state":"FAILED"}')
    with pytest.raises(ValueError, match="non-finite JSON number"):
        strict_json_loads('{"updated_at":NaN}')
    with pytest.raises(ValueError, match="Out of range float values"):
        atomic_write_json(tmp_path / "invalid.json", {"updated_at": float("inf")})
    assert not (tmp_path / "invalid.json").exists()


def test_receipt_manifest_rejects_scalar_sequences_and_boolean_versions():
    base = dict(
        schema_version=1,
        deployment_id="deployment",
        generation=0,
        deployment_plan_hash="a" * 64,
        allocation_binding_hash="b" * 64,
        receipt_hashes=(),
        receipts=(),
    )
    with pytest.raises(ReceiptManifestError, match="unsupported"):
        ReceiptManifest(**{**base, "schema_version": True})
    with pytest.raises(ReceiptManifestError, match="receipt_hashes"):
        ReceiptManifest(**{**base, "receipt_hashes": "a" * 64})
    with pytest.raises(ReceiptManifestError, match="receipts must contain"):
        ReceiptManifest(**{**base, "receipts": {}})


def test_process_ownership_receipt_rejects_scalar_temp_paths():
    with pytest.raises(ProcessOwnershipError, match="temp paths must be a sequence"):
        ProcessOwnershipReceipt(
            schema_version=1,
            uid=1,
            hostname="node",
            deployment_id="deployment",
            generation=0,
            rank=0,
            component_id="ray",
            pid=1,
            pgid=1,
            process_start_ticks=1,
            argv_hash="a" * 64,
            temp_paths="/tmp/path",  # type: ignore[arg-type]
            created_at=1.0,
        )


def test_gateway_evidence_does_not_coerce_health_or_capture_types():
    base = dict(
        deployment_id="deployment",
        generation=0,
        deployment_plan_hash="a" * 64,
        allocation_binding_hash="b" * 64,
        gateway_kind="haproxy",
        process_state="RUNNING",
        returncode=None,
        health_ok=True,
        detail="healthy",
    )
    with pytest.raises(ValueError, match="health_ok must be a boolean"):
        classify_gateway_evidence(**{**base, "health_ok": 1})
    with pytest.raises(ValueError, match="total_bytes must be nonnegative"):
        classify_gateway_evidence(**base, capture={"total_bytes": "1"})
