"""Regressions for defects found by the implementation audit (IMP-*).

Each test reproduces a concrete defect the audit demonstrated, so the fixed
behavior is locked in. Hermetic: temp dirs + loopback only.
"""

from __future__ import annotations

import json
import time

import pytest

from exaserve.model_staging import check_model_exists
from exaserve.plan import PlanError, compile_deployment_plan
from exaserve.request_validation import (
    RequestValidationError,
    require_object_body,
    strict_flag,
    validate_model_field,
)
from exaserve.state.atomic import ExclusiveLease
from exaserve.state.status import (
    DeploymentState,
    IllegalTransition,
    StatusConflict,
    StatusStore,
)


# ---------------- IMP-B05: model completion marker ---------------------------

def test_completion_marker_is_verified_not_trusted(tmp_path):
    m = tmp_path / "model"
    m.mkdir()
    (m / "config.json").write_text("{}")
    (m / "model.safetensors").write_text("weights")
    assert check_model_exists(m) is True          # marker written
    (m / "model.safetensors").unlink()            # weights vanish afterwards
    assert check_model_exists(m) is False         # must NOT still say complete


def test_empty_or_corrupt_marker_is_not_complete(tmp_path):
    from exaserve.model_staging import COMPLETION_MARKER

    m = tmp_path / "model"
    m.mkdir()
    (m / COMPLETION_MARKER).write_text("{}")      # degenerate marker
    assert check_model_exists(m) is False
    (m / COMPLETION_MARKER).write_text("{not json")
    assert check_model_exists(m) is False


def test_tokenizer_only_marker_does_not_certify_full_model(tmp_path):
    from exaserve.model_staging import COMPLETION_MARKER, _write_completion_marker

    m = tmp_path / "model"
    m.mkdir()
    (m / "tokenizer.json").write_text("{}")
    _write_completion_marker(m, tokenizer_only=True)
    assert json.loads((m / COMPLETION_MARKER).read_text())["kind"] == "tokenizer_only"
    assert check_model_exists(m) is False


# ---------------- IMP-B07: lease fencing + status CAS ------------------------

def test_stale_holder_cannot_delete_successors_lease(tmp_path):
    path = tmp_path / "run.lease"
    old = ExclusiveLease(path, ttl_s=0.01).acquire()
    time.sleep(0.05)                              # old lease expires
    new = ExclusiveLease(path, ttl_s=60).acquire()  # legitimate takeover
    old.release()                                 # stale holder releases
    assert path.exists(), "stale holder deleted the successor's live lease"
    assert new.holds_lease() is True
    new.release()
    assert not path.exists()


def test_lease_renew_only_while_held(tmp_path):
    path = tmp_path / "run.lease"
    lease = ExclusiveLease(path, ttl_s=0.01).acquire()
    time.sleep(0.05)
    thief = ExclusiveLease(path, ttl_s=60).acquire()
    assert lease.renew() is False                 # we lost it; must not extend
    assert thief.renew() is True
    thief.release()


def test_status_cannot_initialize_in_ready(tmp_path):
    store = StatusStore.deployment(tmp_path / "s.json")
    with pytest.raises(IllegalTransition):
        store.initialize("d", DeploymentState.READY)
    store.initialize("d", DeploymentState.PLANNED)  # only PLANNED is legal


def test_status_revision_cas_rejects_aba_stale_writer(tmp_path):
    store = StatusStore.deployment(tmp_path / "s.json")
    store.initialize("d", DeploymentState.PLANNED)
    for a, b in ((DeploymentState.PLANNED, DeploymentState.STAGING),
                 (DeploymentState.STAGING, DeploymentState.CLUSTER_STARTING),
                 (DeploymentState.CLUSTER_STARTING, DeploymentState.DEPLOYING),
                 (DeploymentState.DEPLOYING, DeploymentState.VALIDATING),
                 (DeploymentState.VALIDATING, DeploymentState.READY)):
        store.transition(a, b, reason_code="step")
    stale_revision = store.load().revision            # writer's view of READY
    # The record cycles READY -> VALIDATING -> READY behind that writer's back.
    store.transition(DeploymentState.READY, DeploymentState.VALIDATING, reason_code="loss")
    store.transition(DeploymentState.VALIDATING, DeploymentState.READY, reason_code="recovered")
    with pytest.raises(StatusConflict, match="ABA|revision"):
        store.transition(DeploymentState.READY, DeploymentState.DRAINING,
                         reason_code="stale", expected_revision=stale_revision)


# ---------------- IMP-H01: plan identity + immutability ----------------------

def _raw():
    return {
        "model_deployment_config": {
            "num_nodes": 2, "model_storage_path": "/m",
            "model_configs": [{"model_id": "a/b", "max_model_len": 64, "size": 1}],
        },
        "proxy_config": {"type": "none"},
    }


def test_plan_hash_is_path_independent(tmp_path):
    a = compile_deployment_plan(_raw(), source_path="/a/config.yaml")
    b = compile_deployment_plan(_raw(), source_path="/b/config.yaml")
    assert a.plan_hash == b.plan_hash, "source path must not change plan identity"


def test_plan_options_are_deeply_immutable():
    raw = _raw()
    raw["proxy_config"] = {"type": "none", "opts": {"k": "v"}}
    plan = compile_deployment_plan(raw)
    before = plan.plan_hash
    raw["proxy_config"]["opts"]["k"] = "MUTATED"   # mutate the caller's dict
    assert plan.plan_hash == before
    assert "MUTATED" not in repr(plan.gateway.options), "plan content mutated post-compile"


def test_plan_rejects_non_finite_numbers():
    raw = _raw()
    raw["model_deployment_config"]["model_configs"][0]["gpu_memory_utilization"] = "nan"
    with pytest.raises(PlanError, match="finite"):
        compile_deployment_plan(raw)


def test_falsy_reservation_topology_cannot_bypass_node_agreement():
    raw = _raw()
    raw["scheduler"] = {"type": "pbs", "nodes": 1, "reservation_topology": False}
    with pytest.raises(PlanError):
        compile_deployment_plan(raw)


# ---------------- IMP-H04: request body typing -------------------------------

@pytest.mark.parametrize("body", [[1, 2], "string", 42, None])
def test_non_object_bodies_raise_typed_error(body):
    with pytest.raises(RequestValidationError):
        require_object_body(body)
    with pytest.raises(RequestValidationError):
        validate_model_field(body, {"m"})


def test_non_string_model_raises_typed_error():
    with pytest.raises(RequestValidationError, match="model must be a string"):
        validate_model_field({"model": ["x"]}, {"m"})


def test_string_false_is_not_truthy_for_protocol_flags():
    assert strict_flag({"stream": "false"}, "stream", True) is False
    assert strict_flag({"stream": "true"}, "stream", False) is True
    assert strict_flag({}, "stream", False) is False
    with pytest.raises(RequestValidationError):
        strict_flag({"stream": 7}, "stream", False)
