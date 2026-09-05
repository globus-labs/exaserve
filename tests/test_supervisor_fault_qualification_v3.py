"""Canonical FIRST_CAUSE and cleanup checks for AC-SUP-01 hardware."""

import importlib.util
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
HARNESS_PATH = ROOT / "scripts/hardening/run_supervisor_fault_qualification_v3.py"
EXPERIMENT_PATH = ROOT / "artifacts/hardening/final40-supervisor-experiment-plan.json"
CURRENT_EXPERIMENT_PATH = ROOT / "artifacts/hardening/final41-supervisor-experiment-plan.json"
CURRENT_RESULT_PATH = (
    ROOT
    / "artifacts/hardening/final41-supervisor-watchdog-v3-2n-20260809-a1/qualification/result.json"
)
NEXT_EXPERIMENT_PATH = ROOT / "artifacts/hardening/final42-supervisor-q2-experiment-plan.json"
RESULT_PATH = (
    ROOT
    / "artifacts/hardening/final40-supervisor-watchdog-v3-2n-20260809-a1/qualification/result.json"
)
SPEC = importlib.util.spec_from_file_location("supervisor_fault_qualification_v3", HARNESS_PATH)
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
        "terminal_reason_code": "FIRST_CAUSE",
        "terminal_detail": detail,
        "scenario": scenario,
    }


@_requires_evidence(EXPERIMENT_PATH, RESULT_PATH)
def test_final40_canonical_gate_is_immutable_negative_first_disconnect_evidence(
    tmp_path, monkeypatch
):
    import json

    document = json.loads(EXPERIMENT_PATH.read_text(encoding="utf-8"))
    gate = document["gates"][0]
    output = ROOT / gate["output_path"]
    result = json.loads(RESULT_PATH.read_text(encoding="utf-8"))
    assert document["schema_version"] == 3
    assert gate["attempt"] == gate["attempt_limit"] == 1
    assert gate["logical_nodes"] == gate["physical_allocation_nodes"] == 2
    assert gate["scenarios"] == ["head-ray-child-death", "worker-supervisor-death"]
    assert output == RESULT_PATH.parent
    assert result["passed"] is False
    assert "missing=['rank(s) [1]']" in result["error"]

    runtime_result = json.loads((output / "runtime/result.json").read_text(encoding="utf-8"))
    assert runtime_result["passed"] is True
    worker = runtime_result["scenarios"][1]
    assert worker["scenario"] == "worker-supervisor-death"
    assert "rank(s) [0, 1]" in worker["terminal_detail"]

    monkeypatch.chdir(tmp_path)
    derived = harness._derived_runtime_plan(document, gate, output)
    assert derived["schema_version"] == 1
    assert derived["gates"][0]["output_path"].endswith("/qualification/runtime")
    assert derived["gates"][0]["expected_observations"] == list(
        harness.fault_runtime._EXPECTED_OBSERVATIONS
    )


@_requires_evidence(CURRENT_EXPERIMENT_PATH, CURRENT_RESULT_PATH)
def test_final41_canonical_replacement_gate_is_exact_and_passed():
    import json

    document = json.loads(CURRENT_EXPERIMENT_PATH.read_text(encoding="utf-8"))
    gate = document["gates"][0]
    result = json.loads(CURRENT_RESULT_PATH.read_text(encoding="utf-8"))
    assert document["schema_version"] == 3
    assert gate["attempt"] == gate["attempt_limit"] == 1
    assert gate["logical_nodes"] == gate["physical_allocation_nodes"] == 2
    assert gate["scenarios"] == ["head-ray-child-death", "worker-supervisor-death"]
    assert ROOT / gate["output_path"] == CURRENT_RESULT_PATH.parent
    assert result["schema_version"] == 3
    assert result["passed"] is True
    assert result["declared_gate"] == gate
    assert result["experiment_plan_sha256"] == (
        "8b7b5454b25c49cef7d37028f8256eb84b541d00931830add117a632a4159422"
    )
    assert result["harness_sha256"] == (
        "b37c63e6d60337e74717cb37d2e8aa6d8e6763299cd5d1c4f692a26180f89e1c"
    )
    assert result["runtime_result_sha256"] == (
        "4fbb135c3a00c3e723ffdf78326fc0f4c269659b2189d9404cf21cb0b490475c"
    )
    assert [item["scenario"] for item in result["scenarios"]] == gate["scenarios"]
    for scenario in result["scenarios"]:
        assert scenario["passed"] is True
        assert scenario["terminal_state"] == "FAILED"
        assert scenario["terminal_reason_code"] == "FIRST_CAUSE"
        cleanup = result["exact_generation_cleanup"][scenario["scenario"]]
        assert len(cleanup["reports"]) == 2
        assert all(report["matched"] == [] for report in cleanup["reports"])
        assert all(report["signals"] == [] for report in cleanup["reports"])
        assert all(report["survivors"] == [] for report in cleanup["reports"])

    harness._require_typed_first_cause("head-ray-child-death", result["scenarios"][0])
    harness._require_typed_first_cause("worker-supervisor-death", result["scenarios"][1])


@_requires_evidence(NEXT_EXPERIMENT_PATH)
def test_final42_q2_declaration_binds_the_current_canonical_harness():
    import json

    gate_id = "FQ-FINAL42-SUPERVISOR-WATCHDOG-V3Q2-2N-20260809"
    with pytest.raises(RuntimeError, match="lifecycle support changed"):
        harness._load_gate(NEXT_EXPERIMENT_PATH, gate_id)
    document = json.loads(NEXT_EXPERIMENT_PATH.read_text(encoding="utf-8"))
    gate = next(item for item in document["gates"] if item["gate_id"] == gate_id)
    assert document["schema_version"] == 3
    assert gate["attempt"] == gate["attempt_limit"] == 1
    assert gate["logical_nodes"] == gate["physical_allocation_nodes"] == 2
    assert gate["scenarios"] == ["head-ray-child-death", "worker-supervisor-death"]
    assert ROOT / gate["output_path"] == (
        ROOT / "artifacts/hardening/final42-supervisor-watchdog-v3q2-2n-20260809-a1/qualification"
    )


def test_canonical_head_ray_cause_uses_public_first_cause_and_exact_internal_reason():
    harness._require_typed_first_cause(
        "head-ray-child-death",
        _result(
            "head-ray-child-death",
            "rank_launcher: UNEXPECTED_EXIT (exit=143) rank 0 component ray: "
            "exit=137; rank launcher exit=143",
        ),
    )


def test_canonical_worker_supervisor_cause_preserves_exact_rank_disconnect():
    harness._require_typed_first_cause(
        "worker-supervisor-death",
        _result(
            "worker-supervisor-death",
            "rank_launcher: UNEXPECTED_EXIT (exit=143) authenticated rank control "
            "session disappeared without GOODBYE for rank(s) [1]; rank launcher exit=143",
        ),
    )


def test_internal_reason_is_not_mistaken_for_the_public_status_reason():
    result = _result(
        "head-ray-child-death",
        "rank_launcher: UNEXPECTED_EXIT (exit=143) rank 0 component ray: "
        "exit=137; rank launcher exit=143",
    )
    result["terminal_reason_code"] = "UNEXPECTED_EXIT"
    with pytest.raises(RuntimeError, match="FIRST_CAUSE"):
        harness._require_typed_first_cause("head-ray-child-death", result)


@pytest.mark.parametrize(
    ("scenario", "detail"),
    (
        ("head-ray-child-death", "rank_launcher exited unexpectedly"),
        (
            "worker-supervisor-death",
            "rank_launcher: UNEXPECTED_EXIT (exit=143) authenticated rank control session "
            "disappeared without GOODBYE for rank(s) [0]; rank launcher exit=143",
        ),
    ),
)
def test_canonical_cause_rejects_generic_or_wrong_rank_evidence(scenario, detail):
    with pytest.raises(RuntimeError):
        harness._require_typed_first_cause(scenario, _result(scenario, detail))


def test_cleanup_requires_zero_matches_signals_and_survivors(tmp_path):
    from scripts.hardening import run_final_null_qualification as lifecycle

    path = tmp_path / "runtime" / "head-ray-child-death"
    path.mkdir(parents=True)
    cleanup = {
        "schema_version": 1,
        "reports": [
            {"hostname": "n0", "matched": [], "signals": [], "survivors": []},
            {"hostname": "n1", "matched": [], "signals": [], "survivors": []},
        ],
    }
    lifecycle._atomic_json(path / "exact_generation_cleanup.json", cleanup)
    assert harness._require_zero_fallback(tmp_path, "head-ray-child-death") == cleanup
    cleanup["reports"][1]["signals"] = [{"pid": 10, "signal": "TERM"}]
    lifecycle._atomic_json(path / "exact_generation_cleanup.json", cleanup)
    with pytest.raises(RuntimeError, match="signals"):
        harness._require_zero_fallback(tmp_path, "head-ray-child-death")
