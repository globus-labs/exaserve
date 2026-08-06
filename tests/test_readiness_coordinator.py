"""WP5 / AC-RDY-01+02 (audit IMP-B02): typed, revocable, receipt-gated READY."""

from __future__ import annotations

import pytest

from exaserve.compat.profile import CompatibilityProfile, PatchSpec
from exaserve.compat.receipt import ReceiptStore, build_receipt
from exaserve.control.contracts import SCHEMA_VERSION, ComponentObservation
from exaserve.control.readiness import ReadinessCoordinator, ReadinessPlan

DEP, GEN, PH = "dep-1", 3, "plan-hash"


def _plan(**over):
    base = dict(
        deployment_id=DEP, generation=GEN, plan_hash=PH,
        expected_nodes=("node0", "node1"),
        expected_components=("ray-head", "gateway"),
        expected_replicas={"m": 2},
        expected_routes=("/m",),
    )
    base.update(over)
    return ReadinessPlan(**base)


def _obs(component, node, state="READY", *, rank=0, model=None, replica=None,
         generation=GEN, instance="i0", seq=1):
    return ComponentObservation(
        schema_version=SCHEMA_VERSION, deployment_id=DEP, plan_hash=PH,
        generation=generation, component_id=component, instance_id=instance,
        sequence=seq, owner_scope="RANK", owner_rank=rank, role=component,
        node_id=node, state=state, observed_at=0.0, model_id=model,
        replica_id=replica)


def _make_ready(coord):
    coord.observe(_obs("ray-head", "node0"))
    coord.observe(_obs("gateway", "node0"))
    coord.observe(_obs("replica-a", "node0", model="m", replica="r0"))
    coord.observe(_obs("replica-b", "node1", model="m", replica="r1", rank=1))
    coord.set_route_health("/m", True)
    coord.set_canary("/m", True)


def test_full_predicate_satisfied_is_ready():
    coord = ReadinessCoordinator(_plan())
    _make_ready(coord)
    snap = coord.evaluate()
    assert snap.ready is True, snap.blockers
    assert snap.blockers == ()


@pytest.mark.parametrize("withhold", [
    "node", "component", "replica", "route", "canary",
])
def test_each_conjunct_alone_blocks_readiness(withhold):
    """AC-RDY-01: withhold exactly ONE observation; READY must not hold and
    the blocker must be named."""
    coord = ReadinessCoordinator(_plan())
    coord.observe(_obs("ray-head", "node0"))
    coord.observe(_obs("gateway", "node0"))
    coord.observe(_obs("replica-a", "node0", model="m", replica="r0"))
    if withhold != "replica":
        coord.observe(_obs("replica-b", "node1", model="m", replica="r1", rank=1))
    elif withhold == "replica":
        coord.observe(_obs("filler", "node1", rank=1))  # node present, replica absent
    if withhold != "route":
        coord.set_route_health("/m", True)
    if withhold != "canary":
        coord.set_canary("/m", True)
    if withhold == "node":
        coord2 = ReadinessCoordinator(_plan(expected_nodes=("node0", "node1", "node2")))
        _make_ready(coord2)
        snap = coord2.evaluate()
        assert not snap.ready and any("nodes not reporting" in b for b in snap.blockers)
        return
    if withhold == "component":
        coord3 = ReadinessCoordinator(
            _plan(expected_components=("ray-head", "gateway", "missing-thing")))
        _make_ready(coord3)
        snap = coord3.evaluate()
        assert not snap.ready and any("missing-thing" in b for b in snap.blockers)
        return
    snap = coord.evaluate()
    assert snap.ready is False
    assert snap.blockers, f"{withhold} withheld but no blocker reported"


def test_ready_is_revocable_after_component_loss():
    """The legacy path latched READY permanently; this must not."""
    coord = ReadinessCoordinator(_plan())
    _make_ready(coord)
    assert coord.is_ready() is True
    coord.observe(_obs("gateway", "node0", state="FAILED", seq=2))
    snap = coord.evaluate()
    assert snap.ready is False
    assert any("gateway" in b for b in snap.blockers)


def test_replica_loss_revokes_ready():
    coord = ReadinessCoordinator(_plan())
    _make_ready(coord)
    assert coord.is_ready()
    coord.observe(_obs("replica-b", "node1", state="FAILED", model="m",
                       replica="r1", rank=1, seq=2))
    snap = coord.evaluate()
    assert not snap.ready and any("1/2 replicas" in b for b in snap.blockers)


def test_stale_generation_observation_is_rejected():
    coord = ReadinessCoordinator(_plan())
    assert coord.observe(_obs("ray-head", "node0", generation=GEN - 1)) is False
    assert not coord.is_ready()


def test_lease_expiry_revokes_ready():
    """A component that stops reporting loses readiness (plan WP5.10)."""
    fake = {"t": 0.0}
    coord = ReadinessCoordinator(_plan(), lease_timeout_s=10.0,
                                 clock=lambda: fake["t"])
    _make_ready(coord)
    assert coord.is_ready() is True
    fake["t"] = 60.0                      # no fresh observations
    snap = coord.evaluate()
    assert snap.ready is False
    assert any("lease stale" in b for b in snap.blockers)


def test_missing_compatibility_receipt_blocks_ready():
    """AC-COMP-01: READY is receipt-gated."""
    profile = CompatibilityProfile(
        schema_version=1, name="p", python="3.12.12", ray="2.53.0",
        vllm="0.15.0", vendor="xpu",
        patches=(PatchSpec("SC-01", "t", "upstream-fix", ("replica",),
                           "sitecustomize", "cap"),),
        required_roles=("supervisor", "replica"))
    object.__setattr__(profile, "profile_id", profile.compute_id())
    store = ReceiptStore(profile, DEP, GEN)
    coord = ReadinessCoordinator(_plan(), receipts=store)
    _make_ready(coord)
    snap = coord.evaluate()
    assert snap.ready is False
    assert any("receipt" in b for b in snap.blockers)

    store.add(build_receipt(profile=profile, role="supervisor", deployment_id=DEP,
                            generation=GEN, patch_results={}))
    store.add(build_receipt(profile=profile, role="replica", deployment_id=DEP,
                            generation=GEN, patch_results={"SC-01": True}))
    assert coord.evaluate().ready is True


def test_stdout_text_cannot_make_a_deployment_ready():
    """IMP-B02: the coordinator has no log input at all. Feeding it the legacy
    marker text as a component id must not satisfy anything."""
    coord = ReadinessCoordinator(_plan())
    coord.observe(_obs("CLUSTER FULLY READY", "node0"))
    coord.observe(_obs("ALL SERVICES READY", "node1", rank=1))
    snap = coord.evaluate()
    assert snap.ready is False
    assert len(snap.blockers) >= 3   # components, replicas, route, canary
