"""PR-032 / TD-METRICS: one ExaServe-owned operational contract."""

from __future__ import annotations

import asyncio
import json

import pytest

from exaserve import observability as obs


@pytest.fixture(autouse=True)
def _clean_registry():
    obs.REGISTRY.reset()
    yield
    obs.REGISTRY.reset()


def test_metrics_render_in_prometheus_text_format():
    obs.record_request("/v1/completions", "ok", 0.25, prompt_tokens=7, completion_tokens=8)
    text = obs.render_metrics()
    assert "# TYPE exaserve_requests_total counter" in text
    assert 'route="/v1/completions"' in text and 'outcome="ok"' in text
    assert "exaserve_tokens_total" in text
    # Every series carries the shared identity so it can be joined with
    # receipts and readiness snapshots.
    assert "deployment_id=" in text and "node=" in text and "generation=" in text


def test_identity_matches_the_id_other_subsystems_use(monkeypatch):
    """A metric must be joinable with a receipt and a readiness snapshot."""
    exact = "8736431.aurora-pbs-0001.hostmgmt.example"
    monkeypatch.setenv("EXASERVE_DEPLOYMENT_ID", exact)
    from exaserve.compat.collector import deployment_scope

    assert obs.identity_labels()["deployment_id"] == deployment_scope() == exact


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
    total = snapshot["counters"][
        ("exaserve_requests_total", (("outcome", "ok"), ("route", "/v1/completions")))
    ]
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


def test_structured_request_link_preserves_distinct_ids(capsys, monkeypatch):
    monkeypatch.setenv("EXASERVE_DEPLOYMENT_ID", "deployment-a")
    monkeypatch.setenv("EXASERVE_GENERATION", "7")
    obs.emit_request_link(
        route="/v1/completions",
        request_id="transport-1",
        completion_id="cmpl-2",
        model_id="model-a",
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["event"] == "request_link"
    assert payload["request_id"] == "transport-1"
    assert payload["completion_id"] == "cmpl-2"
    assert payload["request_id"] != payload["completion_id"]


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


def _request_scope(path="/v1/completions"):
    return {"type": "http", "path": path}


def test_request_middleware_records_after_stream_completion_once():
    sent = []

    async def streaming_app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200})
        await send({"type": "http.response.body", "body": b"one", "more_body": True})
        assert not obs.REGISTRY.snapshot()["counters"]
        await send({"type": "http.response.body", "body": b"two", "more_body": False})

    async def send(message):
        sent.append(message)

    asyncio.run(obs.RequestMetricsMiddleware(streaming_app)(_request_scope(), None, send))
    counters = obs.REGISTRY.snapshot()["counters"]
    key = (
        "exaserve_requests_total",
        (("outcome", "ok"), ("route", "/v1/completions")),
    )
    assert counters[key] == 1
    assert len(sent) == 3


def test_request_middleware_records_exceptions_and_ignores_unbounded_paths():
    async def failing_app(scope, receive, send):
        raise RuntimeError("boom")

    async def send(message):
        raise AssertionError("failing app must not send")

    middleware = obs.RequestMetricsMiddleware(failing_app)
    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(middleware(_request_scope("/v1/chat/completions"), None, send))
    key = (
        "exaserve_requests_total",
        (("outcome", "exception"), ("route", "/v1/chat/completions")),
    )
    assert obs.REGISTRY.snapshot()["counters"][key] == 1

    obs.REGISTRY.reset()
    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(middleware(_request_scope("/caller/controlled/path"), None, send))
    assert not obs.REGISTRY.snapshot()["counters"]


def test_telemetry_drop_labels_are_finite():
    obs.record_telemetry_drop("serving_stats", "push_failed")
    with pytest.raises(ValueError, match="unknown telemetry channel"):
        obs.record_telemetry_drop("caller-controlled", "push_failed")
    with pytest.raises(ValueError, match="unknown telemetry drop reason"):
        obs.record_telemetry_drop("serving_stats", "caller-controlled")


def test_handlers_use_the_correlation_contract():
    from importlib import resources

    source = (resources.files("exaserve") / "server.py").read_text()
    assert source.count("_correlation_id(request.headers") == 2
    assert source.count('headers={"X-Request-ID": correlation}') >= 6
    assert "_correlation_id(request.headers, request_id)" not in source, (
        "transport correlation IDs must be distinct from completion IDs"
    )
    assert 'request.headers.get("x-request-id")' not in source, (
        "a handler still reads the header directly, bypassing sanitization"
    )
