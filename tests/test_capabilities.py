"""KI-D4 / TD-PP-MULTI / TD-CHATTPL / KI-A5: nothing degrades silently."""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

from exaserve import capabilities as caps


def test_every_capability_states_what_enables_it_and_what_happens_without_it():
    for capability in caps.CAPABILITIES:
        assert capability.enabled_by, f"{capability.name} has no enablement"
        assert capability.on_unavailable in ("refuse", "degrade")
        if capability.on_unavailable == "refuse":
            assert capability.summary


def test_an_answer_changing_fallback_refuses_rather_than_substituting(monkeypatch):
    """A plain-text prompt is not the format the model was tuned on."""
    monkeypatch.delenv("EXASERVE_ALLOW_CHAT_TEMPLATE_FALLBACK", raising=False)
    with pytest.raises(caps.CapabilityUnavailable) as excinfo:
        caps.degrade_or_refuse("chat_template_fallback", "model m")
    message = str(excinfo.value)
    assert "not comparable" in message or "NOT the" in message
    assert "chat_template" in message


def test_an_inherited_environment_cannot_enable_answer_changing_fallback(monkeypatch):
    monkeypatch.setenv("EXASERVE_ALLOW_CHAT_TEMPLATE_FALLBACK", "1")
    with pytest.raises(caps.CapabilityUnavailable):
        caps.degrade_or_refuse("chat_template_fallback", "model m")


def test_a_performance_only_capability_degrades_loudly(monkeypatch):
    """KI-A5: thread guards are ambient shell defaults that can vanish."""
    monkeypatch.delenv("RAYON_NUM_THREADS", raising=False)
    monkeypatch.delenv("TOKENIZERS_PARALLELISM", raising=False)
    said = []
    assert caps.degrade_or_refuse("thread_oversubscription_guard", log=said.append) is False
    assert said and "DEGRADED" in said[0]
    assert "RAYON_NUM_THREADS" in said[0]


def test_thread_guards_present_means_available(monkeypatch):
    monkeypatch.setenv("RAYON_NUM_THREADS", "1")
    monkeypatch.setenv("TOKENIZERS_PARALLELISM", "false")
    assert caps.available("thread_oversubscription_guard") is True


def test_an_undeclared_capability_is_an_error():
    with pytest.raises(KeyError):
        caps.get("not_a_capability")


def test_the_engine_gates_the_chat_template_fallback():
    from importlib import resources

    for backend in ("vllm.py", "sglang.py"):
        source = (resources.files("exaserve") / "engines" / backend).read_text()
        assert "degrade_or_refuse" in source, f"{backend} still substitutes silently"


def test_sglang_refuses_an_implicit_answer_changing_prompt_fallback(monkeypatch):
    from exaserve.engines.sglang import SGLangEngine

    class MissingTemplateTokenizer:
        def apply_chat_template(self, *args, **kwargs):
            raise ValueError("tokenizer chat_template is not set")

    monkeypatch.setenv("EXASERVE_ALLOW_CHAT_TEMPLATE_FALLBACK", "1")
    engine = SGLangEngine()
    engine.model_id = "model-a"
    engine.tokenizer = MissingTemplateTokenizer()
    with pytest.raises(caps.CapabilityUnavailable, match="chat_template_fallback"):
        engine.build_chat_prompt([{"role": "user", "content": "hi"}])


def test_sglang_forwards_explicit_chat_template_and_kwargs():
    from exaserve.engines.sglang import SGLangEngine

    class RecordingTokenizer:
        def __init__(self):
            self.kwargs = None

        def apply_chat_template(self, messages, **kwargs):
            self.kwargs = kwargs
            return "rendered"

    engine = SGLangEngine()
    engine.tokenizer = RecordingTokenizer()
    rendered = engine.build_chat_prompt(
        [{"role": "user", "content": "hi"}],
        chat_template="{{ messages }}",
        chat_template_kwargs={"tools": ["calculator"]},
    )
    assert rendered == "rendered"
    assert engine.tokenizer.kwargs["chat_template"] == "{{ messages }}"
    assert engine.tokenizer.kwargs["tools"] == ["calculator"]


def test_sglang_does_not_disguise_unrelated_tokenizer_failures():
    from exaserve.engines.sglang import SGLangEngine

    class BrokenTokenizer:
        def apply_chat_template(self, *args, **kwargs):
            raise RuntimeError("tokenizer corrupted")

    engine = SGLangEngine()
    engine.tokenizer = BrokenTokenizer()
    with pytest.raises(RuntimeError, match="corrupted"):
        engine.build_chat_prompt([{"role": "user", "content": "hi"}])


def test_the_deploy_path_validates_capabilities():
    from importlib import resources

    source = (resources.files("exaserve") / "server.py").read_text()
    assert "validate_deployment as _validate_capabilities" in source
    assert "capabilities=_capability_report" in source, (
        "the capability report is not recorded with the run"
    )


def test_fake_streaming_cannot_enter_a_real_streaming_comparison():
    """KI-B3: litellm's TBT is degenerate; mixing it in compares two things."""
    with pytest.raises(caps.CapabilityUnavailable) as excinfo:
        caps.require_streaming_comparison("litellm", streaming=True)
    assert "degenerate" in str(excinfo.value)
    # A real streaming proxy is fine, and a non-streaming run is unaffected.
    caps.require_streaming_comparison("haproxy", streaming=True)
    caps.require_streaming_comparison("litellm", streaming=False)


@pytest.mark.parametrize(
    ("proxy", "streaming", "expected"),
    [
        ("haproxy", True, caps.TIMING_INCREMENTAL_SSE),
        ("envoy", True, caps.TIMING_INCREMENTAL_SSE),
        ("litellm", True, caps.TIMING_BUFFERED_RESPONSE),
        ("litellm", False, caps.TIMING_COARSE_FULL_RESPONSE),
    ],
)
def test_timing_semantics_are_explicit(proxy, streaming, expected):
    assert caps.classify_timing_semantics(proxy, streaming) == expected


def test_litellm_streaming_deployment_does_not_claim_real_token_timing(monkeypatch):
    monkeypatch.setenv("RAYON_NUM_THREADS", "1")
    monkeypatch.setenv("TOKENIZERS_PARALLELISM", "false")
    plan = SimpleNamespace(
        gateway=SimpleNamespace(kind="litellm"),
        exposure=SimpleNamespace(mode="PROXIED_INTERNAL"),
        scale_envelope=SimpleNamespace(streaming_mode="streaming"),
    )
    report = caps.validate_deployment(plan)
    assert report["real_streaming_metrics"] is False
    assert caps.deployment_timing_semantics(plan) == caps.TIMING_BUFFERED_RESPONSE


def test_haproxy_incremental_streaming_claim_is_plan_specific(monkeypatch):
    monkeypatch.setenv("RAYON_NUM_THREADS", "1")
    monkeypatch.setenv("TOKENIZERS_PARALLELISM", "false")
    plan = SimpleNamespace(
        gateway=SimpleNamespace(kind="haproxy"),
        exposure=SimpleNamespace(mode="PROXIED_INTERNAL"),
        scale_envelope=SimpleNamespace(streaming_mode="streaming"),
    )
    assert caps.validate_deployment(plan)["real_streaming_metrics"] is True
    assert caps.available("real_streaming_metrics") is False


def test_aurora_xpu_isolation_never_introduces_oneapi_selector(monkeypatch):
    from exaserve.vendors.xpu import XPUVendor

    monkeypatch.setenv("ONEAPI_DEVICE_SELECTOR", "ambient-invalid-selector")
    XPUVendor().isolate_devices([3], engine_name="sglang")
    assert "ONEAPI_DEVICE_SELECTOR" not in os.environ
    assert os.environ["ZE_AFFINITY_MASK"] == "3"
