"""P03 / IMP-B02: readiness is a predicate over the PLAN, not the survivors."""

from __future__ import annotations

import pytest

from exaserve.control.readiness import DeploymentPhase, ReadinessCoordinator
from exaserve.control.plan_readiness import (
    planned_application_names,
    planned_proxy_anchor_names,
)
from exaserve.plan.compiler import compile_deployment_plan
from exaserve.plan.contracts import SiteProfile, build_allocation_binding


def _site():
    return SiteProfile(
        schema_version=3,
        site_id="s",
        max_nodes=64,
        gpus_per_node=12,
        cpus_per_node=64,
        scheduler_types=("pbs",),
        gateway_kinds=("haproxy",),
        vendors=("xpu",),
        engines=("vllm",),
        model_storage_path="/m",
        local_stage_path="/t",
        launcher_capabilities=("ray_serve.run_many",),
    ).finalize()


def _plan(nodes=2, gateway=True, replicas=2, head_only=False, null_compute=False):
    raw = {
        "num_nodes": nodes,
        "models": [
            {
                "model_id": "a/b",
                "tensor_parallel_size": 1,
                "max_model_len": 4096,
                "size": 8,
                "num_replicas": replicas,
            }
        ],
    }
    if null_compute:
        raw["validation_mode"] = True
        raw["runtime"] = {"null_compute": True}
    if head_only:
        raw["validation_mode"] = True
        raw["exposure"] = {"mode": "RAY_SERVE_HEAD_ONLY"}
    elif gateway:
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
        return self._ok, {
            "planned": 6,
            "accepted": 6 if self._ok else 5,
            "missing": self._missing,
            "unexpected": [],
        }


def _readiness(plan=None, receipts=None, sessions=None):
    plan = plan or _plan()
    binding = build_allocation_binding(
        plan=plan,
        generation=1,
        scheduler_allocation_id="j",
        nodes=[f"n{i}" for i in range(plan.num_nodes)],
    )
    return ReadinessCoordinator(
        plan=plan,
        binding=binding,
        receipts=receipts or _Receipts(),
        sessions=sessions,
        log=lambda *_: None,
    )


def _make_ready(r, plan=None, *, advertised_endpoint=True):
    plan = plan or r.plan
    nodes = [
        {
            "node_id": f"ray-{rank}",
            "node_name": node,
            "node_address": f"10.0.0.{rank + 1}",
            "alive": True,
            "cpu": float(plan.node_cpus),
            "gpu": float(plan.num_gpus_per_node),
        }
        for rank, node in r.binding.rank_to_node
    ]
    proxy_nodes = nodes[:1] if plan.uses_head_only_serve_proxy() else nodes
    r.set_cluster(
        nodes=nodes,
        proxies=[{"node_id": node["node_id"], "status": "HEALTHY"} for node in proxy_nodes],
    )
    for rank in r.binding.ranks():
        r.set_rank_component(rank, True)
    r.set_owned_component("rank_launcher", True)
    r.set_owned_component("deployment", True)
    r.set_applications(planned_application_names(plan))
    if advertised_endpoint:
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
            return (1,)  # rank 1 never established

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


def test_excess_replicas_do_not_satisfy_an_exact_plan():
    r = _make_ready(_readiness())
    r.set_replicas("a/b", 3, 2)
    verdict = r.evaluate()
    assert not verdict.ready
    assert any("3/2 replicas" in blocker for blocker in verdict.blockers)
    assert "model/a/b/replicas" in verdict.unhealthy_identities


def test_observed_serve_target_must_equal_the_planned_target():
    r = _make_ready(_readiness())
    r.set_replicas("a/b", 2, 3)
    verdict = r.evaluate()
    assert not verdict.ready
    assert any("observed target 3, planned 2" in blocker for blocker in verdict.blockers)
    assert verdict.model_map["a/b"]["observed_target"] == 3


def test_readiness_model_projection_matches_the_public_status_contract():
    from exaserve.status_api import _validate_model_map

    verdict = _make_ready(_readiness()).evaluate()
    assert _validate_model_map(verdict.model_map) is verdict.model_map


def test_missing_receipt_slots_block_by_name():
    r = _make_ready(_readiness(receipts=_Receipts(False, ["rank1/ray_worker"])))
    verdict = r.evaluate()
    assert not verdict.ready
    assert any("rank1/ray_worker" in b for b in verdict.blockers)


def test_a_complete_plan_reaches_ready():
    verdict = _make_ready(_readiness()).evaluate()
    assert verdict.ready, verdict.blockers


def test_every_planned_rank_has_an_exact_proxy_anchor_application():
    plan = _plan(nodes=2)
    anchors = planned_proxy_anchor_names(plan)
    assert anchors == {
        "_exaserve_proxy_anchor_r0",
        "_exaserve_proxy_anchor_r1",
    }
    assert anchors < planned_application_names(plan)


def test_null_haproxy_groups_exact_replica_slots_by_planned_rank():
    plan = _plan(nodes=2, replicas=24, null_compute=True)
    model = plan.models[0]
    assert plan.node_grouped_null_application_groups(model) == (
        (0, tuple(range(12))),
        (1, tuple(range(12, 24))),
    )
    assert planned_application_names(plan) == {
        "_exaserve_proxy_anchor_r0",
        "_exaserve_proxy_anchor_r1",
        f"{model.route_name}_g0",
        f"{model.route_name}_g1",
    }


def test_uneven_null_replica_groups_keep_exact_per_replica_routes():
    plan = _plan(nodes=2, replicas=13, null_compute=True)
    model = plan.models[0]
    assert plan.node_grouped_null_application_groups(model) == ()
    applications = planned_application_names(plan)
    assert f"{model.route_name}_r0" in applications
    assert f"{model.route_name}_r12" in applications
    assert not any(name.startswith(f"{model.route_name}_g") for name in applications)


def test_head_only_requires_only_the_head_proxy_and_no_anchor_applications():
    plan = _plan(nodes=2, head_only=True)
    assert planned_proxy_anchor_names(plan) == frozenset()
    readiness = _make_ready(_readiness(plan), plan)
    verdict = readiness.evaluate()
    assert verdict.ready, verdict.blockers
    assert verdict.proxies == ({"node_id": "ray-0", "status": "HEALTHY"},)


def test_a_missing_proxy_anchor_application_blocks_readiness():
    r = _make_ready(_readiness())
    applications = set(planned_application_names(r.plan))
    applications.remove("_exaserve_proxy_anchor_r1")
    r.set_applications(applications)
    verdict = r.evaluate()
    assert not verdict.ready
    assert any("_exaserve_proxy_anchor_r1" in blocker for blocker in verdict.blockers)


def test_unplanned_serve_application_blocks_readiness():
    r = _make_ready(_readiness())
    r.set_applications((*planned_application_names(r.plan), "stale-deployment"))
    verdict = r.evaluate()
    assert not verdict.ready
    assert any("serve applications unplanned" in blocker for blocker in verdict.blockers)


# -- the advertised endpoint -------------------------------------------------


def test_production_readiness_requires_a_live_healthy_gateway():
    r = _make_ready(_readiness())
    r.set_gateway(alive=False, healthy=False)
    assert any("gateway process is not running" in b for b in r.evaluate().blockers)
    r.set_gateway(alive=True, healthy=False)
    assert any("gateway health check failed" in b for b in r.evaluate().blockers)


def test_readiness_needs_an_advertised_endpoint_at_all():
    r = _make_ready(_readiness(), advertised_endpoint=False)
    assert any("no advertised endpoint" in b for b in r.evaluate().blockers)


def test_advertised_endpoint_observation_rejects_empty_or_nontext_values():
    r = _readiness()
    for invalid in ("", None, False, 0):
        with pytest.raises(ValueError, match="advertised endpoint"):
            r.set_advertised_endpoint(invalid)


def test_a_canary_is_required_for_every_planned_model():
    r = _make_ready(_readiness())
    r.set_canary("a/b", False)
    verdict = r.evaluate()
    assert not verdict.ready
    assert any("no completion via the advertised endpoint" in b for b in verdict.blockers)


def test_validation_direct_mode_needs_no_gateway_process():
    plan = _plan(gateway=False)
    r = _make_ready(_readiness(plan=plan), plan)
    r.set_advertised_endpoint("http://serve:8000")
    verdict = r.evaluate()
    assert verdict.ready, verdict.blockers


# -- transitions -------------------------------------------------------------


def test_ready_is_revocable_and_persistence_is_not_duplicated(tmp_path):
    r = _make_ready(_readiness())
    r.enter_validating()
    verdict = r.commit_ready()
    assert verdict.ready and r.phase == DeploymentPhase.READY.value
    assert not list(tmp_path.iterdir()), (
        "the coordinator must not create a second readiness artifact"
    )

    r.revoke("gateway health check failed")
    assert r.phase == DeploymentPhase.VALIDATING.value


def test_a_dead_gateway_process_goes_straight_to_failed(tmp_path):
    r = _make_ready(_readiness())
    r.commit_ready()
    phase = r.revoke("gateway exited", gateway_dead=True)
    assert phase == DeploymentPhase.FAILED.value
    assert r.first_failure == "gateway exited"


def test_revalidation_can_return_to_ready_in_the_same_generation(tmp_path):
    r = _make_ready(_readiness())
    r.commit_ready()
    r.revoke("transient route failure")
    assert r.phase == DeploymentPhase.VALIDATING.value
    verdict = r.commit_ready()
    assert verdict.ready and r.phase == DeploymentPhase.READY.value


def test_a_terminal_generation_blocks_readiness():
    class _Sessions:
        generation_state = "TERMINAL"
        terminal_reason = "rank 1 lost"

        def readiness_revoked_ranks(self):
            return ()

    r = _make_ready(_readiness(sessions=_Sessions()))
    assert any("generation terminal" in b for b in r.evaluate().blockers)


def test_the_verdict_carries_exact_plan_identity_and_blocker_sets():
    verdict = _make_ready(_readiness()).evaluate()
    assert len(verdict.deployment_plan_hash) == 64
    assert len(verdict.allocation_binding_hash) == 64
    assert verdict.missing_identities == ()
    assert verdict.unhealthy_identities == ()
