"""AC-PROXY-01 classification and causal-capture contract."""

import pytest

from exaserve.state.gateway import classify_gateway_evidence


BASE = {
    "deployment_id": "deployment",
    "generation": 1,
    "deployment_plan_hash": "a" * 64,
    "allocation_binding_hash": "b" * 64,
    "gateway_kind": "haproxy",
}


@pytest.mark.parametrize(
    ("process_state", "returncode", "health_ok", "classification"),
    (
        ("READY", None, True, "healthy"),
        ("RUNNING", None, False, "degraded"),
        ("FAILED", 17, False, "process_dead"),
        ("FAILED", -15, False, "process_dead"),
    ),
)
def test_gateway_classifications_are_distinct_and_typed(
    process_state, returncode, health_ok, classification
):
    evidence = classify_gateway_evidence(
        **BASE,
        process_state=process_state,
        returncode=returncode,
        health_ok=health_ok,
        detail="exact observation",
        capture={
            "tail": "last gateway output",
            "total_bytes": 30,
            "dropped_bytes": 0,
            "truncated": False,
        },
    )
    assert evidence.classification == classification
    assert evidence.health_ok is (classification == "healthy")
    assert evidence.log_tail == "last gateway output"
    if returncode is not None and returncode < 0:
        assert evidence.signal == 15 and evidence.exit_code is None
    elif returncode is not None:
        assert evidence.exit_code == returncode and evidence.signal is None
    else:
        assert evidence.exit_code is evidence.signal is None


def test_gateway_classifier_rejects_coercible_health_values():
    with pytest.raises(ValueError, match="must be a boolean"):
        classify_gateway_evidence(
            **BASE,
            process_state="RUNNING",
            returncode=None,
            health_ok=1,
            detail="coercible",
        )
