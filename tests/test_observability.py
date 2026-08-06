"""PR-032 / TD-METRICS: one ExaServe-owned operational contract."""

from __future__ import annotations

import pytest

from exaserve import observability as obs


@pytest.fixture(autouse=True)
def _clean_registry():
    obs.REGISTRY.reset()
    yield
    obs.REGISTRY.reset()


def test_metrics_render_in_prometheus_text_format():
    obs.record_request("/v1/completions", "ok", 0.25,
                       prompt_tokens=7, completion_tokens=8)
    text = obs.render_metrics()
    assert "# TYPE exaserve_requests_total counter" in text
    assert 'route="/v1/completions"' in text and 'outcome="ok"' in text
    assert "exaserve_tokens_total" in text
    # Every series carries the shared identity so it can be joined with
    # receipts and readiness snapshots.
    assert "deployment_id=" in text and "node=" in text and "generation=" in text


def test_identity_matches_the_id_other_subsystems_use(monkeypatch):
    """A metric must be joinable with a receipt and a readiness snapshot."""
    monkeypatch.setenv("EXASERVE_DEPLOYMENT_ID",
                       "8736431.aurora-pbs-0001.hostmgmt.example")
    from exaserve.compat.collector import deployment_scope

    assert obs.identity_labels()["deployment_id"] == deployment_scope() == "8736431"


def test_cardinality_is_capped_and_the_drop_is_reported():
    """An unbounded label set is how a metrics endpoint becomes an outage."""
    for i in range(obs._MAX_SERIES + 50):
        obs.REGISTRY.inc("exaserve_requests_total", 1.0, {"route": f"/r{i}"})
    current, cap = obs.bounded_series()
    assert current <= cap
    assert obs.REGISTRY.snapshot()["dropped_series"] > 0
    assert "exaserve_metrics_dropped_series_total" in obs.render_metrics()


def test_counters_accumulate_and_gauges_replace():
    obs.record_request("/v1/completions", "ok", 1.0)
    obs.record_request("/v1/completions", "ok", 2.0)
    snapshot = obs.REGISTRY.snapshot()
    total = snapshot["counters"][("exaserve_requests_total",
                                  (("outcome", "ok"), ("route", "/v1/completions")))]
    assert total == 2
    obs.mark_replica_ready()
    obs.mark_replica_ready()
    assert snapshot is not None
    assert obs.REGISTRY.snapshot()["gauges"][("exaserve_replica_up", ())] == 1.0


def test_a_supplied_correlation_id_is_propagated():
    class _Headers(dict):
        pass

    assert obs.correlation_id(_Headers({"x-request-id": "abc-123"}), "fallback") == "abc-123"


def test_a_missing_correlation_id_is_minted_not_dropped():
    assert obs.correlation_id({}, "cmpl-xyz") == "cmpl-xyz"
    assert obs.correlation_id(None, "cmpl-xyz") == "cmpl-xyz"


def test_a_hostile_correlation_id_cannot_break_a_log_line_or_label():
    """The header is caller-controlled input."""
    hostile = 'evil"\ninjected exaserve_requests_total{x="1"} 999'
    cleaned = obs.correlation_id({"x-request-id": hostile}, "fallback")
    assert "\n" not in cleaned and '"' not in cleaned
    assert len(cleaned) <= 128


def test_an_oversized_correlation_id_is_bounded():
    cleaned = obs.correlation_id({"x-request-id": "a" * 5000}, "fallback")
    assert len(cleaned) == 128


def test_label_values_are_escaped_in_the_rendered_output():
    obs.REGISTRY.inc("exaserve_requests_total", 1.0, {"route": 'a"b'})
    assert '\\"' in obs.render_metrics()


def test_the_replica_exposes_metrics():
    """The endpoint must exist on the deployment, not just in the module."""
    pytest.importorskip("ray", exc_type=ImportError)
    from importlib import resources

    source = (resources.files("exaserve") / "server.py").read_text()
    assert '@app.get("/metrics")' in source
    assert "render_metrics" in source


def test_handlers_use_the_correlation_contract():
    from importlib import resources

    source = (resources.files("exaserve") / "server.py").read_text()
    assert source.count("_correlation_id(request.headers") == 2
    assert 'request.headers.get("x-request-id")' not in source, (
        "a handler still reads the header directly, bypassing sanitization")
