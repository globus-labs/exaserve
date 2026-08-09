"""Negative release-gate tests for the canonical hardening ledger."""

from copy import deepcopy
from io import StringIO
from pathlib import Path
import importlib.util

import pytest


_VALIDATOR_PATH = (
    Path(__file__).resolve().parents[1] / "scripts" / "hardening" / "validate_findings.py"
)
_SPEC = importlib.util.spec_from_file_location("validate_findings", _VALIDATOR_PATH)
assert _SPEC is not None and _SPEC.loader is not None
validator = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(validator)


def _record(**changes):
    record = {
        "id": "TEST-001",
        "source": "test",
        "severity": "high",
        "invariant": "the gate fails closed",
        "primary_work_package": "WP0",
        "affected_regions": ["scripts/hardening/validate_findings.py"],
        "acceptance_tests": ["AC-TST-01"],
        "status": "FIXED",
        "decision": "Implemented and proven by the validator contract tests.",
        "evidence": ["tests/test_findings_validator.py"],
        "fallback": "none — the exact schema is mandatory",
        "residual_risk": "none — malformed records fail validation",
        "support_impact": "release accounting is fail-closed",
        "owner": "codex",
        "approval": None,
        "revisit_condition": "the canonical record schema changes",
    }
    record.update(changes)
    return record


def _problems(record, tmp_path=None):
    root = Path(__file__).resolve().parents[1] if tmp_path is None else tmp_path
    return validator.validate([record], repo_root=root)


def test_exact_fixed_record_is_valid():
    assert _problems(_record()) == []


def test_empty_ledger_never_passes():
    assert validator.validate([]) == ["findings must contain at least one record"]


def test_yaml_duplicate_keys_are_rejected_before_schema_validation():
    import yaml

    with pytest.raises(yaml.constructor.ConstructorError, match="duplicate key 'status'"):
        validator.load_yaml_unique(
            StringIO("findings:\n  - id: X\n    status: OPEN\n    status: FIXED\n")
        )


def test_missing_and_unknown_record_fields_fail():
    record = _record(invented="not canonical")
    del record["decision"]
    problems = _problems(record)
    assert any("missing exact-schema" in p and "decision" in p for p in problems)
    assert any("unknown exact-schema" in p and "invented" in p for p in problems)


def test_list_fields_must_not_be_scalar_prose():
    problems = _problems(_record(affected_regions="src/exaserve/launcher.py"))
    assert any("affected_regions must be a YAML list" in p for p in problems)


def test_unknown_acceptance_id_fails():
    problems = _problems(_record(acceptance_tests=["AC-CUTOVER-01"]))
    assert any("unknown ID 'AC-CUTOVER-01'" in p for p in problems)


def test_fixed_requires_existing_durable_evidence(tmp_path):
    problems = _problems(_record(evidence=["tests/does_not_exist.py"]), tmp_path=tmp_path)
    assert any("local reference does not exist" in p for p in problems)


def _external_archive_document(record):
    return {
        "meta": {
            "candidate": "candidate",
            "wheel_sha256": "a" * 64,
            "scope_state": "TECHNICAL_PASS_SCOPE_PENDING",
            "review_manifest": "artifacts/hardening/candidate-candidate-review.json",
            "review_manifest_sha256": "b" * 64,
        },
        "findings": [record],
    }


def test_clean_checkout_accepts_hash_bound_external_evidence(tmp_path):
    record = _record(
        affected_regions=["implementation.py"],
        evidence=["artifacts/hardening/candidate/result.json"],
    )
    records, problems = validator.validate_document(
        _external_archive_document(record), repo_root=tmp_path
    )
    assert records == [record]
    assert problems == []


def test_clean_checkout_rejects_unbound_external_evidence(tmp_path):
    record = _record(
        affected_regions=["implementation.py"],
        evidence=["artifacts/hardening/candidate/result.json"],
    )
    document = _external_archive_document(record)
    del document["meta"]["review_manifest_sha256"]
    _, problems = validator.validate_document(document, repo_root=tmp_path)
    assert any("review_manifest_sha256" in problem for problem in problems)
    assert any("local reference does not exist" in problem for problem in problems)


def test_partial_external_archive_remains_fail_closed(tmp_path):
    (tmp_path / "artifacts" / "hardening").mkdir(parents=True)
    record = _record(
        affected_regions=["implementation.py"],
        evidence=["artifacts/hardening/candidate/result.json"],
    )
    _, problems = validator.validate_document(
        _external_archive_document(record), repo_root=tmp_path
    )
    assert any("local reference does not exist" in problem for problem in problems)


def test_fixed_rejects_prose_as_evidence():
    problems = _problems(_record(evidence=["the tests passed on my machine"]))
    assert any("prose, not a stable" in p for p in problems)


def test_in_progress_may_name_missing_proof_explicitly():
    record = _record(
        status="IN_PROGRESS",
        decision="Missing production-path failure injection evidence.",
        acceptance_tests=[],
        evidence=[],
    )
    assert _problems(record) == []


def test_in_progress_cannot_hide_empty_proof():
    record = _record(
        status="IN_PROGRESS",
        decision="Work is fine.",
        acceptance_tests=[],
        evidence=[],
    )
    assert any("must explicitly name the missing proof" in p for p in _problems(record))


def test_approval_is_exact_and_only_for_accepted_limit():
    record = _record(status="ACCEPTED_LIMIT", approval={"approver_id": "codex"})
    problems = _problems(record)
    assert any("approval missing field" in p for p in problems)
    assert any("cannot be self-issued" in p for p in problems)

    fixed = deepcopy(_record())
    fixed["approval"] = {
        "approver_id": "product-owner",
        "approved_at": "2026-08-07T00:00:00Z",
        "evidence_ref": "doc/hardening/decisions/ADR-000-production-envelope.md",
        "scope": "candidate scope",
    }
    assert any("approval must be null" in p for p in _problems(fixed))
