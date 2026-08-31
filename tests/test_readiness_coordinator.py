"""WP5 / AC-RDY-01+02: the canonical, revocable readiness authority."""

from __future__ import annotations

import time
from types import SimpleNamespace

import pytest

from exaserve.control.readiness import ReadinessCoordinator


class _Receipts:
    def satisfied(self):
        return True, {"planned": 0, "accepted": 0, "missing": [], "unexpected": []}

    def accepted_receipts(self):
        return ()


def _synthetic(size: int):
    models = tuple(
        SimpleNamespace(model_id=f"model-{i}", route_name=f"route-{i}", num_replicas=1)
        for i in range(size)
    )
    plan = SimpleNamespace(
        deployment_id="dep",
        deployment_plan_hash="a" * 64,
        num_nodes=size,
        node_cpus=8,
        num_gpus_per_node=1,
        readiness=SimpleNamespace(allow_excess_resources=True),
        models=models,
        gateway=None,
        is_production_exposure=lambda: False,
        uses_head_only_serve_proxy=lambda: False,
        node_grouped_null_application_groups=lambda _model: (),
    )
    binding = SimpleNamespace(
        generation=3,
        allocation_binding_hash="b" * 64,
        rank_to_node=tuple((i, f"node-{i}.example") for i in range(size)),
        ranks=lambda: tuple(range(size)),
    )
    coordinator = ReadinessCoordinator(
        plan=plan, binding=binding, receipts=_Receipts(), log=lambda *_: None
    )
    nodes = tuple(
        {
            "node_id": f"ray-{i}",
            "node_name": f"node-{i}",
            "node_address": f"10.0.{i // 250}.{i % 250 + 1}",
            "alive": True,
            "cpu": 8.0,
            "gpu": 1.0,
        }
        for i in range(size)
    )
    coordinator.set_cluster(
        nodes=nodes,
        proxies=tuple({"node_id": f"ray-{i}", "status": "HEALTHY"} for i in range(size)),
    )
    coordinator.set_advertised_endpoint("http://head:8000")
    coordinator.set_owned_component("rank_launcher", True)
    coordinator.set_owned_component("deployment", True)
    coordinator.set_applications(
        [model.route_name for model in models]
        + [f"_exaserve_proxy_anchor_r{i}" for i in range(size)]
    )
    for i in range(size):
        coordinator.set_rank_component(i, True)
        coordinator.set_replicas(f"model-{i}", 1, 1)
        coordinator.set_route(f"route-{i}", True)
        coordinator.set_canary(f"model-{i}", True)
    return coordinator


def test_high_cardinality_reconciliation_is_linear_and_exact():
    """The old rank-by-node scan was O(K^2); 2,048 identities exposed it."""
    coordinator = _synthetic(2048)
    started = time.perf_counter()
    verdict = coordinator.evaluate()
    elapsed = time.perf_counter() - started
    assert verdict.ready, verdict.blockers[:3]
    assert len(verdict.nodes) == 2048
    # A deliberately generous hermetic bound catches quadratic regressions
    # without making normal CI timing-sensitive.
    assert elapsed < 2.0


def test_one_missing_high_cardinality_identity_is_named_exactly():
    coordinator = _synthetic(512)
    nodes = list(coordinator._nodes)
    nodes.pop(317)
    coordinator.set_cluster(nodes=nodes, proxies=coordinator._proxies)
    verdict = coordinator.evaluate()
    assert not verdict.ready
    assert "ray-node/rank/317" in verdict.missing_identities


def test_ready_revokes_when_one_required_component_is_lost():
    coordinator = _synthetic(8)
    coordinator.enter_validating()
    assert coordinator.commit_ready().ready
    coordinator.set_rank_component(5, False)
    verdict = coordinator.evaluate()
    assert not verdict.ready
    assert "component/rank/5/ray" in verdict.unhealthy_identities
    coordinator.revoke("rank 5 observation expired")
    assert coordinator.phase == "VALIDATING"


def test_stdout_has_no_ingestion_surface():
    coordinator = _synthetic(1)
    assert not hasattr(coordinator, "observe_log")
    assert not hasattr(coordinator, "feed_stdout")


@pytest.mark.parametrize(
    "mutation",
    [
        lambda c: c.set_gateway(alive="false", healthy=True),
        lambda c: c.set_route("route-0", "false"),
        lambda c: c.set_canary("model-0", 1),
        lambda c: c.set_replicas("model-0", True, 1),
        lambda c: c.set_rank_component(0, "healthy"),
        lambda c: c.set_owned_component("deployment", 1),
    ],
)
def test_readiness_never_coerces_truthy_evidence(mutation):
    coordinator = _synthetic(1)
    with pytest.raises(ValueError):
        mutation(coordinator)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda c: c.set_route("unplanned", True),
        lambda c: c.set_canary("unplanned", True),
        lambda c: c.set_replicas("unplanned", 1, 1),
        lambda c: c.set_rank_component(7, True),
        lambda c: c.set_owned_component("unplanned", True),
    ],
)
def test_unplanned_incremental_evidence_cannot_grow_readiness_state(mutation):
    coordinator = _synthetic(1)
    with pytest.raises(ValueError, match="not planned"):
        mutation(coordinator)


def test_cluster_and_application_snapshots_are_strict_and_bounded():
    coordinator = _synthetic(1)
    with pytest.raises(ValueError, match="duplicate"):
        coordinator.set_applications(["route-0", "route-0"])
    with pytest.raises(ValueError, match="invalid fields"):
        coordinator.set_cluster(nodes=[{"node_id": "partial"}], proxies=[])
    with pytest.raises(ValueError, match="boolean"):
        coordinator.set_cluster(
            nodes=[
                {
                    "node_id": "ray-0",
                    "node_name": "node-0",
                    "node_address": "10.0.0.1",
                    "alive": "true",
                    "cpu": 8.0,
                    "gpu": 1.0,
                }
            ],
            proxies=[],
        )
