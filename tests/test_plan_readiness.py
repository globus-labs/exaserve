"""P03 / IMP-B02: readiness is a predicate over the PLAN, not the survivors."""

from __future__ import annotations

import json

import pytest

from exaserve.compat.receipt_v2 import ExactReceiptLedger
from exaserve.control.plan_readiness import DeploymentPhase, PlanReadiness
from exaserve.plan.compiler import compile_deployment_plan
from exaserve.plan.contracts import SiteProfile, build_allocation_binding


def _site():
    return SiteProfile(
        schema_version=2, site_id="s", max_nodes=64, gpus_per_node=12,
        cpus_per_node=64, scheduler_types=("pbs",), gateway_kinds=("haproxy",),
        vendors=("xpu",), engines=("vllm",), model_storage_path="/m",
        local_stage_path="/t").finalize()


def _plan(nodes=2, gateway=True, replicas=2):
    raw = {"num_nodes": nodes,
           "models": [{"model_id": "a/b", "tensor_parallel_size": 1,
                       "max_model_len": 4096, "size": 8,
                       "num_replicas": replicas}]}
    if gateway:
        raw["gateway"] = {"kind": "haproxy", "port": 4001}
    else:
        raw["validation_mode"] = True
        raw["exposure"] = {"mode": "DIRECT_VALIDATION"}
    return compile_deployment_plan(raw, site=_site(), deployment_id="d")


class _Receipts:
    """Stand-in ledger so these tests exercise readiness, not receipt parsing."""

    def __init__(self, ok=True, missing=()):
        self._ok, self._missing = ok, list(missing)

    def satisfied(self):
        return self._ok, {"planned": 6, "accepted": 6 if self._ok else 5,
                          "missing": self._missing, "unexpected": []}


def _readiness(plan=None, receipts=None, sessions=None):
    plan = plan or _plan()
    binding = build_allocation_binding(plan=plan, generation=1,
                                       scheduler_allocation_id="j",
                                       nodes=[f"n{i}" for i in range(plan.num_nodes)])
    return PlanReadiness(plan=plan, binding=binding,
                         receipts=receipts or _Receipts(), sessions=sessions,
                         log=lambda *_: None)


def _make_ready(r, plan=None):
    plan = plan or r.plan
    r.set_advertised_endpoint("http://gateway:4001")
    r.set_gateway(alive=True, healthy=True)
    for model in plan.models:
        r.set_route(model.route_name, True)
        r.set_canary(model.model_id, True)
        r.set_replicas(model.model_id, 2, 2)
    return r


# -- the expectation comes from the plan -------------------------------------

def test_a_planned_node_that_never_appears_blocks_readiness():
    """The old gate derived expected nodes from survivors, so this passed."""
    class _Sessions:
        generation_state = "REGISTERING"
        terminal_reason = None

        def readiness_revoked_ranks(self):
            return (1,)                     # rank 1 never established

    r = _make_ready(_readiness(sessions=_Sessions()))
    verdict = r.evaluate()
    assert not verdict.ready
    assert any("ranks not established" in b for b in verdict.blockers)


def test_a_missing_replica_blocks_against_the_planned_target():
    r = _make_ready(_readiness())
    r.set_replicas("a/b", 1, 2)
    verdict = r.evaluate()
    assert not verdict.ready
    assert any("1/2 replicas" in b for b in verdict.blockers)


def test_missing_receipt_slots_block_by_name():
    r = _make_ready(_readiness(receipts=_Receipts(False, ["rank1/ray_worker"])))
    verdict = r.evaluate()
    assert not verdict.ready
    assert any("rank1/ray_worker" in b for b in verdict.blockers)


def test_a_complete_plan_reaches_ready():
    verdict = _make_ready(_readiness()).evaluate()
    assert verdict.ready, verdict.blockers


# -- the advertised endpoint -------------------------------------------------

def test_production_readiness_requires_a_live_healthy_gateway():
    r = _make_ready(_readiness())
    r.set_gateway(alive=False, healthy=False)
    assert any("gateway process is not running" in b for b in r.evaluate().blockers)
    r.set_gateway(alive=True, healthy=False)
    assert any("gateway health check failed" in b for b in r.evaluate().blockers)


def test_readiness_needs_an_advertised_endpoint_at_all():
    r = _make_ready(_readiness())
    r.set_advertised_endpoint("")
    assert any("no advertised endpoint" in b for b in r.evaluate().blockers)


def test_a_canary_is_required_for_every_planned_model():
    r = _make_ready(_readiness())
    r.set_canary("a/b", False)
    verdict = r.evaluate()
    assert not verdict.ready
    assert any("no completion via the advertised endpoint" in b
               for b in verdict.blockers)


def test_validation_direct_mode_needs_no_gateway_process():
    plan = _plan(gateway=False)
    r = _readiness(plan=plan)
    r.set_advertised_endpoint("http://serve:8000")
    for model in plan.models:
        r.set_route(model.route_name, True)
        r.set_canary(model.model_id, True)
        r.set_replicas(model.model_id, 2, 2)
    verdict = r.evaluate()
    assert verdict.ready, verdict.blockers


# -- transitions -------------------------------------------------------------

def test_ready_is_persisted_atomically_and_revocable(tmp_path):
    r = _make_ready(_readiness())
    r.enter_validating()
    verdict = r.commit_ready(str(tmp_path))
    assert verdict.ready and r.phase == DeploymentPhase.READY.value
    data = json.loads((tmp_path / "readiness.json").read_text())
    assert data["ready"] is True and data["deployment_plan_hash"]
    assert not list(tmp_path.glob("*.tmp.*"))

    r.revoke("gateway health check failed")
    assert r.phase == DeploymentPhase.VALIDATING.value
    data = json.loads((tmp_path / "readiness.json").read_text())
    assert data["ready"] is False, "a revoked READY must not stay readable as READY"


def test_a_dead_gateway_process_goes_straight_to_failed(tmp_path):
    r = _make_ready(_readiness())
    r.commit_ready(str(tmp_path))
    phase = r.revoke("gateway exited", gateway_dead=True)
    assert phase == DeploymentPhase.FAILED.value
    assert r.first_failure == "gateway exited"


def test_revalidation_can_return_to_ready_in_the_same_generation(tmp_path):
    r = _make_ready(_readiness())
    r.commit_ready(str(tmp_path))
    r.revoke("transient route failure")
    assert r.phase == DeploymentPhase.VALIDATING.value
    verdict = r.commit_ready(str(tmp_path))
    assert verdict.ready and r.phase == DeploymentPhase.READY.value


def test_failed_durable_publication_is_fatal(tmp_path, monkeypatch):
    """A READY nobody can read is not READY."""
    r = _make_ready(_readiness())
    monkeypatch.setattr(r, "_persist", lambda *a, **k: "")
    with pytest.raises(RuntimeError, match="durably persist"):
        r.commit_ready(str(tmp_path))
    assert r.phase == DeploymentPhase.FAILED.value


def test_a_terminal_generation_blocks_readiness():
    class _Sessions:
        generation_state = "TERMINAL"
        terminal_reason = "rank 1 lost"

        def readiness_revoked_ranks(self):
            return ()

    r = _make_ready(_readiness(sessions=_Sessions()))
    assert any("generation terminal" in b for b in r.evaluate().blockers)


def test_the_verdict_carries_plan_identity_not_the_string_plan():
    """readiness.json used to contain plan_hash: "plan"."""
    verdict = _make_ready(_readiness()).evaluate()
    assert len(verdict.deployment_plan_hash) == 64
    assert len(verdict.allocation_binding_hash) == 64
