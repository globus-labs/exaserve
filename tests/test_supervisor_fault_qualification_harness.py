"""Fail-closed tests for the declared AC-SUP-01 hardware harness."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]
HARNESS_PATH = ROOT / "scripts" / "hardening" / "run_supervisor_fault_qualification.py"
FAILED_EXPERIMENT_PATH = ROOT / "artifacts/hardening/final35-supervisor-experiment-plan.json"
FAILED_RESULT_PATH = (
    ROOT / "artifacts/hardening/final35-supervisor-faults-2n-20260809-a1/qualification/result.json"
)
DECLARATION_FAILURE_EXPERIMENT_PATH = (
    ROOT / "artifacts/hardening/final36-supervisor-experiment-plan.json"
)
DECLARATION_FAILURE_RESULT_PATH = (
    ROOT
    / "artifacts/hardening/final36-supervisor-watchdog-2n-20260809-a1/qualification/result.json"
)
IDENTITY_FAILURE_EXPERIMENT_PATH = (
    ROOT / "artifacts/hardening/final37-supervisor-experiment-plan.json"
)
IDENTITY_FAILURE_RESULT_PATH = (
    ROOT
    / "artifacts/hardening/final37-supervisor-watchdog-2n-20260809-a1/qualification/result.json"
)
EXPERIMENT_PATH = ROOT / "artifacts/hardening/final38-supervisor-experiment-plan.json"
SPEC = importlib.util.spec_from_file_location("supervisor_fault_qualification", HARNESS_PATH)
assert SPEC is not None and SPEC.loader is not None
harness = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(harness)


def _requires_evidence(*paths: Path):
    return pytest.mark.skipif(
        any(not path.is_file() for path in paths),
        reason="hardware campaign evidence is retained outside clean source checkouts",
    )


@_requires_evidence(FAILED_EXPERIMENT_PATH, FAILED_RESULT_PATH)
def test_failed_supervisor_campaign_remains_immutable_negative_evidence():
    import json

    gate_id = "FQ-FINAL35-SUPERVISOR-FAULTS-2N-20260809"
    _, gate, paths = harness._load_gate(FAILED_EXPERIMENT_PATH, gate_id)
    result = json.loads(FAILED_RESULT_PATH.read_text(encoding="utf-8"))
    assert paths["output"] == FAILED_RESULT_PATH.parent
    assert gate["attempt"] == gate["attempt_limit"] == 1
    assert result["gate_id"] == gate_id
    assert result["passed"] is False
    assert "owner left exact-generation processes for fallback" in result["error"]


@_requires_evidence(DECLARATION_FAILURE_EXPERIMENT_PATH, DECLARATION_FAILURE_RESULT_PATH)
def test_mismatched_declaration_remains_immutable_prelaunch_failure():
    import json

    gate_id = "FQ-FINAL36-SUPERVISOR-WATCHDOG-2N-20260809"
    _, gate, paths = harness._load_gate(DECLARATION_FAILURE_EXPERIMENT_PATH, gate_id)
    result = json.loads(DECLARATION_FAILURE_RESULT_PATH.read_text(encoding="utf-8"))
    assert paths["output"] == DECLARATION_FAILURE_RESULT_PATH.parent
    assert gate["attempt"] == gate["attempt_limit"] == 1
    assert result["passed"] is False
    assert result["scenarios"] == []
    assert "compiled plan does not match" in result["error"]


@_requires_evidence(IDENTITY_FAILURE_EXPERIMENT_PATH, IDENTITY_FAILURE_RESULT_PATH)
def test_truncated_deployment_identity_remains_immutable_startup_failure():
    import json

    gate_id = "FQ-FINAL37-SUPERVISOR-WATCHDOG-2N-20260809"
    _, gate, paths = harness._load_gate(IDENTITY_FAILURE_EXPERIMENT_PATH, gate_id)
    result = json.loads(IDENTITY_FAILURE_RESULT_PATH.read_text(encoding="utf-8"))
    stdout = (paths["output"] / "head-ray-child-death/stdout.log").read_text(encoding="utf-8")
    assert result["passed"] is False
    assert result["scenarios"] == []
    assert "terminal before READY" in result["error"]
    assert "deployment_id does not match the plan" in stdout


@_requires_evidence(EXPERIMENT_PATH)
def test_final38_supervisor_gate_is_immutable_cleanup_evidence_but_not_strict_cause_evidence():
    import json

    gate_id = "FQ-FINAL38-SUPERVISOR-WATCHDOG-2N-20260809"
    document, gate, paths = harness._load_gate(EXPERIMENT_PATH, gate_id)
    result = json.loads((paths["output"] / "result.json").read_text(encoding="utf-8"))
    assert document["schema_version"] == 1
    assert gate["attempt"] == gate["attempt_limit"] == 1
    assert gate["logical_nodes"] == gate["physical_allocation_nodes"] == 2
    assert gate["scenarios"] == ["head-ray-child-death", "worker-supervisor-death"]
    assert result["passed"] is True
    assert all(
        item["terminal_detail"].endswith("rank_launcher exited unexpectedly")
        for item in result["scenarios"]
    )
    for item in result["scenarios"]:
        cleanup = json.loads(
            (paths["output"] / item["scenario"] / "exact_generation_cleanup.json").read_text(
                encoding="utf-8"
            )
        )
        assert all(not report["matched"] for report in cleanup["reports"])


@pytest.mark.parametrize(
    ("scenario", "requirement_id", "role", "component", "rank"),
    (
        ("head-ray-child-death", "rank0/ray_head", "ray_head", "ray", 0),
        (
            "worker-supervisor-death",
            "rank1/node_supervisor",
            "node_supervisor",
            "node_supervisor",
            1,
        ),
    ),
)
def test_fault_target_requires_one_exact_self_attested_rank_owner(
    scenario, requirement_id, role, component, rank
):
    binding = SimpleNamespace(rank_to_node=((0, "node0"), (1, "node1")))
    receipt = {
        "receipt_requirement_id": requirement_id,
        "role": role,
        "component_id": component,
        "owner_scope": "RANK",
        "owner_rank": rank,
        "attestation_type": "SELF",
        "node_id": f"node{rank}",
        "pid": 100 + rank,
        "receipt_hash": "a" * 64,
    }
    target = harness._owned_target({"receipts": [receipt]}, binding, scenario)
    assert target["rank"] == rank
    assert target["pid"] == 100 + rank
    assert target["receipt_requirement_id"] == requirement_id

    receipt["attestation_type"] = "SUPERVISOR"
    with pytest.raises(RuntimeError, match="SELF-attested target"):
        harness._owned_target({"receipts": [receipt]}, binding, scenario)


def test_fault_target_rejects_duplicates_and_wrong_nodes():
    binding = SimpleNamespace(rank_to_node=((0, "node0"), (1, "node1")))
    receipt = {
        "receipt_requirement_id": "rank0/ray_head",
        "role": "ray_head",
        "component_id": "ray",
        "owner_scope": "RANK",
        "owner_rank": 0,
        "attestation_type": "SELF",
        "node_id": "node0",
        "pid": 100,
        "receipt_hash": "a" * 64,
    }
    with pytest.raises(RuntimeError, match="one exact"):
        harness._owned_target(
            {"receipts": [receipt, dict(receipt)]}, binding, "head-ray-child-death"
        )
    receipt["node_id"] = "node1"
    with pytest.raises(RuntimeError, match="one exact"):
        harness._owned_target({"receipts": [receipt]}, binding, "head-ray-child-death")
