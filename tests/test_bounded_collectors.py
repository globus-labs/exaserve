"""KI-A4 / KI-A6 / PR-029: no fleet-wide polling, no unbounded detached state."""

from __future__ import annotations


from exaserve.compat import collector


def test_the_ray_receipt_actor_is_gone(tmp_path):
    """WP13 deleted it: §3.2.1 does not accept it as a readiness source.

    A "fallback" to an unauthenticated transport is not a safety net; it is
    the violation with a longer name.
    """
    for name in (
        "create_receipt_collector",
        "drain_receipts",
        "shutdown_collector",
        "_ReceiptCollectorImpl",
        "collector_name",
    ):
        assert not hasattr(collector, name), f"{name} survived the WP13 cutover"

    from importlib import resources

    source = (resources.files("exaserve") / "server.py").read_text()
    assert "create_receipt_collector" not in source
    assert "shutdown_collector" not in source


def test_the_bound_moved_to_the_node_local_ingress(tmp_path):
    """The cap and its drop count follow the receipts to their real transport."""
    from exaserve.compat.local_ingress import (
        LocalReceiptIngress,
        deliver_receipt,
        socket_path_for,
    )

    path = socket_path_for("bounded-collector-test", 0, root=str(tmp_path))
    ingress = LocalReceiptIngress(path, max_queued=4)
    assert ingress.start()
    try:
        results = [deliver_receipt({"i": i}, path=path) for i in range(9)]
    finally:
        ingress.stop()
    assert sum(results) == 4
    assert ingress.dropped == 5  # truncation is counted, never silent


def test_publishing_without_the_hop_is_a_named_failure(monkeypatch):
    monkeypatch.delenv("EXASERVE_RECEIPT_SOCKET", raising=False)

    class _R:
        role = "replica"

    assert collector.publish_receipt(_R()) is False


def test_proxy_fanout_legacy_path_is_deleted():
    """KI-A4: the O(N) actor-RPC fan-out is not a hidden comparison switch."""
    from importlib import resources

    source = (resources.files("exaserve") / "server.py").read_text()
    assert "_wait_for_proxies_fanout" not in source
    assert "EXASERVE_PROXY_FANOUT_WAIT" not in source
    assert "h.serving.remote(" not in source


def test_the_gate_covers_what_the_fanout_used_to():
    """Skipping the fan-out is safe only because the sole gate is stronger."""
    import inspect

    from exaserve.control.readiness import ReadinessCoordinator

    source = inspect.getsource(ReadinessCoordinator.evaluate)
    assert "serve proxy" in source
    assert "canary" in source
    assert "_proxies" in source
    assert "_canary_ok" in source
