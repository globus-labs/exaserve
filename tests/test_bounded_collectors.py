"""KI-A4 / KI-A6 / PR-029: no fleet-wide polling, no unbounded detached state."""

from __future__ import annotations

import pytest

from exaserve.compat import collector


def test_the_receipt_store_is_bounded_and_reports_truncation():
    """A detached actor that grows with the fleet outlives its deployment."""
    impl = collector._ReceiptCollectorImpl()
    for i in range(collector._MAX_RECEIPTS + 25):
        impl.report({"role": "replica", "pid": i})
    assert impl.count() == collector._MAX_RECEIPTS
    assert impl.dropped() == 25, "truncation must be visible, not silent"


def test_the_collector_has_a_shutdown_path():
    """Detached actors survive their creator; somebody has to reap them."""
    assert hasattr(collector, "shutdown_collector")
    # Without a live Ray cluster it reports "nothing to do" rather than raising.
    assert collector.shutdown_collector() is False


def test_the_deploy_path_reaps_the_collector():
    from importlib import resources

    source = (resources.files("exaserve") / "server.py").read_text()
    assert "shutdown_collector" in source, "nothing reaps the receipt collector"


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
