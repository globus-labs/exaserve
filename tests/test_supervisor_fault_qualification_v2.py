"""Fail-closed first-cause checks for the strict AC-SUP-01 harness."""

import importlib.util
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
HARNESS_PATH = ROOT / "scripts/hardening/run_supervisor_fault_qualification_v2.py"
EXPERIMENT_PATH = ROOT / "artifacts/hardening/final39-supervisor-experiment-plan.json"
RESULT_PATH = (
    ROOT
    / "artifacts/hardening/final39-supervisor-watchdog-v2-2n-20260809-a1/qualification/result.json"
)
SPEC = importlib.util.spec_from_file_location("supervisor_fault_qualification_v2", HARNESS_PATH)
assert SPEC is not None and SPEC.loader is not None
harness = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(harness)


def _requires_evidence(*paths: Path):
    return pytest.mark.skipif(
        any(not path.is_file() for path in paths),
        reason="hardware campaign evidence is retained outside clean source checkouts",
    )


def _result(scenario: str, detail: str) -> dict:
    return {
        "terminal_state": "FAILED",
        "terminal_reason_code": "UNEXPECTED_EXIT",
        "terminal_detail": detail,
        "scenario": scenario,
    }


@_requires_evidence(EXPERIMENT_PATH, RESULT_PATH)
def test_final39_v2_gate_is_immutable_negative_adjudicator_evidence():
    import json

    gate_id = "FQ-FINAL39-SUPERVISOR-WATCHDOG-V2-2N-20260809"
    document, gate, paths = harness._load_gate(EXPERIMENT_PATH, gate_id)
    result = json.loads(RESULT_PATH.read_text(encoding="utf-8"))
    assert document["schema_version"] == 2
    assert gate["attempt"] == gate["attempt_limit"] == 1
    assert gate["logical_nodes"] == gate["physical_allocation_nodes"] == 2
    assert gate["scenarios"] == ["head-ray-child-death", "worker-supervisor-death"]
    assert paths["output"] == RESULT_PATH.parent
    assert result["passed"] is False
    assert "did not preserve UNEXPECTED_EXIT" in result["error"]
    status = json.loads(
        (paths["output"] / "head-ray-child-death/deployment/deployment_status.json").read_text(
            encoding="utf-8"
        )
    )
    assert status["reason_code"] == "FIRST_CAUSE"
    assert "rank 0 component ray: exit=137" in status["detail"]


def test_strict_head_ray_cause_accepts_exact_rank_component_and_exit():
    harness._require_typed_first_cause(
        "head-ray-child-death",
        _result(
            "head-ray-child-death",
            "rank_launcher: UNEXPECTED_EXIT (exit=143) rank 0 component ray: "
            "exit=137; rank launcher exit=143",
        ),
    )


def test_strict_worker_supervisor_cause_accepts_exact_rank_disconnect():
    harness._require_typed_first_cause(
        "worker-supervisor-death",
        _result(
            "worker-supervisor-death",
            "rank_launcher: UNEXPECTED_EXIT (exit=143) authenticated rank control "
            "session disappeared without GOODBYE for rank(s) [1]; rank launcher exit=143",
        ),
    )


@pytest.mark.parametrize(
    ("scenario", "detail"),
    (
        ("head-ray-child-death", "rank_launcher exited unexpectedly"),
        (
            "head-ray-child-death",
            "rank launcher exited without typed rank evidence within 1s; exit=143",
        ),
        (
            "worker-supervisor-death",
            "authenticated rank control session disappeared without GOODBYE for rank(s) [0]; "
            "rank launcher exit=143",
        ),
    ),
)
def test_strict_cause_rejects_generic_or_wrong_rank_evidence(scenario, detail):
    with pytest.raises(RuntimeError):
        harness._require_typed_first_cause(scenario, _result(scenario, detail))


def test_strict_cause_rejects_wrong_terminal_contract():
    result = _result(
        "head-ray-child-death",
        "rank 0 component ray: exit=137; rank launcher exit=143",
    )
    result["terminal_reason_code"] = "INVALID_RESULT"
    with pytest.raises(RuntimeError, match="UNEXPECTED_EXIT"):
        harness._require_typed_first_cause("head-ray-child-death", result)
