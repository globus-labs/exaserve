"""Fail-closed tests for generic immutable-candidate evidence adjudication."""

from copy import deepcopy
import importlib.util
import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
ADJUDICATOR_PATH = ROOT / "scripts/hardening/adjudicate.py"
REVIEW_PATH = ROOT / "artifacts/hardening/final42-candidate-review.json"
SPEC = importlib.util.spec_from_file_location("hardening_adjudicator", ADJUDICATOR_PATH)
assert SPEC is not None and SPEC.loader is not None
adjudicator = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(adjudicator)

requires_review = pytest.mark.skipif(
    not REVIEW_PATH.is_file(),
    reason="final42 hardware evidence is retained outside clean source checkouts",
)


def _review():
    if not REVIEW_PATH.is_file():
        pytest.skip("final42 hardware evidence is retained outside clean source checkouts")
    return json.loads(REVIEW_PATH.read_text(encoding="utf-8"))


def _shape_review():
    """Minimal policy-shaped review; no retained hardware evidence required."""
    return {
        "schema_version": 1,
        "candidate_label": "candidate",
        "adjudicated_on": "2026-08-09",
        "scope_state": "TECHNICAL_PASS_SCOPE_PENDING",
        "candidate": {field: "placeholder" for field in adjudicator.CANDIDATE_FIELDS},
        "packaged_gate": {},
        "campaigns": {"lifecycle": {}, "proxy": {}, "supervisor": {}},
        "dispositions": {
            "in_progress": sorted(adjudicator.EXPECTED_IN_PROGRESS),
            "external_blocker": sorted(adjudicator.EXPECTED_EXTERNAL),
            "out_of_production_scope": sorted(adjudicator.EXPECTED_OUT_OF_SCOPE),
        },
    }


def _lifecycle_inputs(index: int = 0):
    review = _review()
    campaign = review["campaigns"]["lifecycle"]
    plan_path = ROOT / campaign["plan_path"]
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    return review, campaign, plan_path.resolve(), plan["harness"], plan["gates"][index]


def _proxy_inputs(index: int = 0):
    review = _review()
    campaign = review["campaigns"]["proxy"]
    plan_path = ROOT / campaign["plan_path"]
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    return review, campaign, plan_path.resolve(), plan, plan["gates"][index]


@requires_review
def test_candidate_verifier_accepts_exact_final42_package_and_campaigns():
    review = adjudicator.verify_candidate_review(REVIEW_PATH)
    assert review["candidate_label"] == "final42"


def test_candidate_verifier_rejects_manifest_outside_repository(tmp_path):
    outside = tmp_path / "review.json"
    outside.write_text("{}\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="inside the repository"):
        adjudicator.verify_candidate_review(outside)


def test_candidate_verifier_rejects_bootstrap_bytes_that_differ_from_wheel(monkeypatch):
    review = _review()
    target = (ROOT / review["candidate"]["bootstrap_path"] / "exaserve" / "__init__.py").resolve()
    original = Path.read_bytes

    def read_mutated(path):
        if path.resolve() == target:
            return b"tampered bootstrap\n"
        return original(path)

    monkeypatch.setattr(Path, "read_bytes", read_mutated)
    with pytest.raises(RuntimeError, match="differs from the wheel"):
        adjudicator._verify_candidate(review)


@pytest.mark.parametrize(
    ("field", "bad_value"),
    (
        ("scope_state", "PRODUCTION_PASS"),
        ("candidate_label", ""),
    ),
)
def test_review_shape_rejects_identity_or_scope_drift(field, bad_value):
    review = _shape_review()
    review[field] = bad_value
    with pytest.raises(RuntimeError, match="identity or scope"):
        adjudicator._verify_review_shape(review)


def test_review_shape_rejects_disposition_drift():
    review = _shape_review()
    review["dispositions"]["in_progress"].remove("PR-033")
    with pytest.raises(RuntimeError, match="drifted from release policy"):
        adjudicator._verify_review_shape(review)


def test_review_shape_accepts_only_the_optional_scale_campaign():
    review = _shape_review()
    review["campaigns"]["scale"] = {}
    adjudicator._verify_review_shape(review)

    review["campaigns"]["invented"] = {}
    with pytest.raises(RuntimeError, match="only the optional scale"):
        adjudicator._verify_review_shape(review)


def test_lifecycle_result_rejects_embedded_declaration_drift():
    review, campaign, plan_path, harness, row = _lifecycle_inputs()
    declaration = deepcopy(campaign["results"][0])
    result_path = ROOT / declaration["path"]
    original = adjudicator._load_json

    def load_mutated(path):
        value = original(path)
        if path == result_path.resolve():
            value["declared_gate"]["attempt_limit"] = 2
        return value

    adjudicator._load_json = load_mutated
    try:
        with pytest.raises(RuntimeError, match="exact declared gate"):
            adjudicator._verify_lifecycle_result(
                review, plan_path, campaign["plan_sha256"], harness, row, declaration
            )
    finally:
        adjudicator._load_json = original


def test_lifecycle_result_rejects_output_and_scenario_drift():
    review, campaign, plan_path, harness, row = _lifecycle_inputs()
    declaration = deepcopy(campaign["results"][0])
    declaration["path"] = "artifacts/hardening/wrong/result.json"
    with pytest.raises((FileNotFoundError, RuntimeError), match="wrong|declared gate output"):
        adjudicator._verify_lifecycle_result(
            review, plan_path, campaign["plan_sha256"], harness, row, declaration
        )

    declaration = deepcopy(campaign["results"][0])
    result_path = ROOT / declaration["path"]
    original = adjudicator._load_json

    def load_mutated(path):
        value = original(path)
        if path == result_path.resolve():
            value["scenarios"].pop()
        return value

    adjudicator._load_json = load_mutated
    try:
        with pytest.raises(RuntimeError, match="scenario evidence"):
            adjudicator._verify_lifecycle_result(
                review, plan_path, campaign["plan_sha256"], harness, row, declaration
            )
    finally:
        adjudicator._load_json = original


def test_lifecycle_result_rejects_cleanup_survivor():
    review, campaign, plan_path, harness, row = _lifecycle_inputs()
    declaration = campaign["results"][0]
    cleanup_path = ROOT / declaration["cleanup"]["normal-drain"]["path"]
    original = adjudicator._load_json

    def load_mutated(path):
        value = original(path)
        if path == cleanup_path.resolve():
            value["reports"][0]["survivors"] = [{"pid": 1}]
        return value

    adjudicator._load_json = load_mutated
    try:
        with pytest.raises(RuntimeError, match="cleanup is incomplete"):
            adjudicator._verify_lifecycle_result(
                review, plan_path, campaign["plan_sha256"], harness, row, declaration
            )
    finally:
        adjudicator._load_json = original


def test_proxy_result_rejects_request_identity_drift():
    review, campaign, plan_path, plan, row = _proxy_inputs()
    declaration = campaign["results"][0]
    result_path = ROOT / declaration["path"]
    original = adjudicator._load_json

    def load_mutated(path):
        value = original(path)
        if path == result_path.resolve():
            requests = value["execution"]["client_workload"]["requests"]
            requests[1]["request_id"] = requests[0]["request_id"]
        return value

    adjudicator._load_json = load_mutated
    try:
        with pytest.raises(RuntimeError, match="request identities"):
            adjudicator._verify_proxy_result(
                review, plan_path.resolve(), campaign["plan_sha256"], plan, row, declaration
            )
    finally:
        adjudicator._load_json = original


def test_proxy_diagnostics_reject_process_and_metric_drift():
    review, campaign, _, _, _ = _proxy_inputs()
    result = json.loads((ROOT / campaign["results"][0]["path"]).read_text(encoding="utf-8"))
    diagnostics = deepcopy(result["execution"]["gateway_diagnostics"])
    diagnostics["samples"][0]["pid"] += 1
    with pytest.raises(RuntimeError, match="process identity changed"):
        adjudicator._verify_proxy_diagnostics(diagnostics, "proxy-result")

    diagnostics = deepcopy(result["execution"]["gateway_diagnostics"])
    diagnostics["peak_connections"] += 1
    with pytest.raises(RuntimeError, match="summaries do not derive"):
        adjudicator._verify_proxy_diagnostics(diagnostics, "proxy-result")

    diagnostics = deepcopy(result["execution"]["gateway_diagnostics"])
    diagnostics["active_opens_delta"] += 1
    with pytest.raises(RuntimeError, match="deltas do not derive"):
        adjudicator._verify_proxy_diagnostics(diagnostics, "proxy-result")


def test_supervisor_shutdown_rejects_false_cleanliness():
    review = _review()
    result_path = ROOT / review["campaigns"]["supervisor"]["result_path"]
    result = json.loads(result_path.read_text(encoding="utf-8"))
    scenario = deepcopy(result["scenarios"][0])
    scenario["shutdown_report"]["deadline_exhausted"] = True
    with pytest.raises(RuntimeError, match="shutdown evidence is incomplete"):
        adjudicator._verify_shutdown(scenario, str(result_path), failed=True)


def test_supervisor_cleanup_rejects_survivor():
    review = _review()
    result_path = ROOT / review["campaigns"]["supervisor"]["result_path"]
    result = json.loads(result_path.read_text(encoding="utf-8"))
    scenario = result["scenarios"][0]
    mutated = deepcopy(result)
    mutated["exact_generation_cleanup"][scenario["scenario"]]["reports"][0]["survivors"] = [
        {"pid": 1}
    ]
    with pytest.raises(RuntimeError, match="cleanup is incomplete"):
        adjudicator._verify_supervisor_cleanup(mutated, scenario, str(result_path))
