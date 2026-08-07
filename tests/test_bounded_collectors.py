"""KI-A4 / KI-A6 / PR-029: no fleet-wide polling, no unbounded detached state."""

from __future__ import annotations

import pytest

from exaserve.compat import collector


def test_the_ray_receipt_actor_is_gone(tmp_path):
    """WP13 deleted it: §3.2.1 does not accept it as a readiness source.

    A "fallback" to an unauthenticated transport is not a safety net; it is
    the violation with a longer name.
    """
    for name in ("create_receipt_collector", "drain_receipts",
                 "shutdown_collector", "_ReceiptCollectorImpl", "collector_name"):
        assert not hasattr(collector, name), f"{name} survived the WP13 cutover"

    from importlib import resources

    source = (resources.files("exaserve") / "server.py").read_text()
    assert "create_receipt_collector" not in source
    assert "shutdown_collector" not in source


def test_the_bound_moved_to_the_node_local_ingress(tmp_path):
    """The cap and its drop count follow the receipts to their real transport."""
    from exaserve.compat.local_ingress import LocalReceiptIngress, deliver_receipt

    path = str(tmp_path / "d" / "receipts.sock")
    ingress = LocalReceiptIngress(path, max_queued=4)
    assert ingress.start()
    try:
        results = [deliver_receipt({"i": i}, path=path) for i in range(9)]
    finally:
        ingress.stop()
    assert sum(results) == 4
    assert ingress.dropped == 5          # truncation is counted, never silent


def test_publishing_without_the_hop_is_a_named_failure(monkeypatch):
    monkeypatch.delenv("EXASERVE_RECEIPT_SOCKET", raising=False)

    class _R:
        role = "replica"

    assert collector.publish_receipt(_R()) is False

def test_proxy_fanout_is_off_when_the_gate_is_on():
    """KI-A4: the wait_proxies cliff was an O(N) RPC fan-out from the head."""
    from importlib import resources

    source = (resources.files("exaserve") / "server.py").read_text()
    assert "_wait_for_proxies_fanout" in source
    assert 'EXASERVE_PROXY_FANOUT_WAIT' in source
    # The fan-out must be reachable ONLY through the guarded call.
    assert source.count("h.serving.remote(") == 1, (
        "the per-proxy fan-out appears outside the guarded legacy helper")


def test_the_gate_covers_what_the_fanout_used_to(monkeypatch):
    """Skipping the fan-out is only safe because the predicate is stronger."""
    from exaserve.control.readiness import ReadinessCoordinator
    from exaserve.control.serve_readiness import build_plan

    plan = build_plan(deployment_id="d", generation=1, plan_hash="h",
                      node_ids=["n0", "n1"], expected_replicas={"app": 2},
                      routes=["app"])
    # Every node's proxy is a required component of the predicate...
    assert "proxy@n0" in plan.expected_components
    assert "proxy@n1" in plan.expected_components
    # ...and a route must additionally answer a real completion.
    coord = ReadinessCoordinator(plan)
    assert any("canary" in b for b in coord.blockers())
