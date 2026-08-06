"""KI-D4 / TD-PP-MULTI / TD-CHATTPL / KI-A5: nothing degrades silently."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from exaserve import capabilities as caps


@dataclass
class _Model:
    model_id: str = "m"
    pipeline_parallel_size: int = 1
    num_replicas: int | None = None


@dataclass
class _Config:
    model_configs: list


def test_every_capability_states_what_enables_it_and_what_happens_without_it():
    for capability in caps.CAPABILITIES:
        assert capability.enabled_by, f"{capability.name} has no enablement"
        assert capability.on_unavailable in ("refuse", "degrade")
        if capability.on_unavailable == "refuse":
            assert capability.summary


def test_multi_replica_pp_is_refused_without_the_shard_aware_path(monkeypatch):
    """pp>1 with replicas>1 works through shard-aware placement and nowhere else."""
    monkeypatch.delenv("EXASERVE_PP_SHARD_AWARE", raising=False)
    config = _Config([_Model(pipeline_parallel_size=2, num_replicas=4)])
    with pytest.raises(caps.CapabilityUnavailable) as excinfo:
        caps.validate_deployment(config, log=lambda *_: None)
    assert "pp_multi_replica" in str(excinfo.value)
    assert "EXASERVE_PP_SHARD_AWARE" in str(excinfo.value)


def test_multi_replica_pp_is_allowed_when_the_capability_is_enabled(monkeypatch):
    monkeypatch.setenv("EXASERVE_PP_SHARD_AWARE", "1")
    config = _Config([_Model(pipeline_parallel_size=2, num_replicas=4)])
    report = caps.validate_deployment(config, log=lambda *_: None)
    assert report["pp_multi_replica"] is True


def test_single_replica_pp_needs_no_capability(monkeypatch):
    monkeypatch.delenv("EXASERVE_PP_SHARD_AWARE", raising=False)
    config = _Config([_Model(pipeline_parallel_size=4, num_replicas=1)])
    caps.validate_deployment(config, log=lambda *_: None)   # must not raise


def test_an_answer_changing_fallback_refuses_rather_than_substituting(monkeypatch):
    """A plain-text prompt is not the format the model was tuned on."""
    monkeypatch.delenv("EXASERVE_ALLOW_CHAT_TEMPLATE_FALLBACK", raising=False)
    with pytest.raises(caps.CapabilityUnavailable) as excinfo:
        caps.degrade_or_refuse("chat_template_fallback", "model m")
    message = str(excinfo.value)
    assert "not comparable" in message or "NOT the" in message
    assert "EXASERVE_ALLOW_CHAT_TEMPLATE_FALLBACK" in message


def test_a_performance_only_capability_degrades_loudly(monkeypatch):
    """KI-A5: thread guards are ambient shell defaults that can vanish."""
    monkeypatch.delenv("RAYON_NUM_THREADS", raising=False)
    monkeypatch.delenv("TOKENIZERS_PARALLELISM", raising=False)
    said = []
    assert caps.degrade_or_refuse("thread_oversubscription_guard",
                                  log=said.append) is False
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

    source = (resources.files("exaserve") / "engines" / "vllm.py").read_text()
    assert "degrade_or_refuse" in source, "the fallback still substitutes silently"


def test_the_deploy_path_validates_capabilities():
    from importlib import resources

    source = (resources.files("exaserve") / "server.py").read_text()
    assert "validate_deployment as _validate_capabilities" in source
    assert "capabilities=_capability_report" in source, (
        "the capability report is not recorded with the run")
