"""Engine startup owns security-sensitive kwargs and finite resources."""

from __future__ import annotations

import asyncio
import sys
from types import ModuleType, SimpleNamespace

import pytest

from exaserve.engines.base import (
    EngineCaps,
    EngineSpec,
    GenDelta,
    GenResult,
    NullEngine,
    merge_engine_kwargs,
)
from exaserve.engines.sglang import SGLangEngine
from exaserve.engines.vllm import VLLMEngine


def test_extra_engine_kwargs_cannot_override_owned_fields():
    with pytest.raises(ValueError, match="master_port, trust_remote_code"):
        merge_engine_kwargs(
            {"model": "/model", "trust_remote_code": False},
            {"master_port": 1234, "trust_remote_code": True},
            protected=("master_port", "trust_remote_code"),
        )


def test_engine_spec_rejects_coercion_and_snapshots_mutable_inputs():
    device_ids = [2]
    extras = {"quantization": {"mode": "none"}}
    spec = EngineSpec(
        model_id="m",
        local_path="/m",
        device_ids=device_ids,
        extra_engine_kwargs=extras,
    )
    device_ids[0] = 7
    extras["quantization"]["mode"] = "mutated"
    assert spec.device_ids == [2]
    assert spec.extra_engine_kwargs["quantization"]["mode"] == "none"

    with pytest.raises(ValueError, match="model_id"):
        EngineSpec(model_id=1, local_path="/m")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="device_ids"):
        EngineSpec(model_id="m", local_path="/m", device_ids="2")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="tensor_parallel_size"):
        EngineSpec(model_id="m", local_path="/m", device_ids=[1], tensor_parallel_size=2)


def test_null_engine_labels_approximate_token_accounting():
    engine = NullEngine(latency_s=0)
    engine.create(EngineSpec(model_id="m", local_path="/m"))

    result = asyncio.run(engine.generate("one two three", {"max_tokens": 2}))

    assert result.prompt_tokens == 3
    assert result.completion_tokens == 2
    assert result.token_count_source == "whitespace_approximation"


def test_vllm_empty_stream_is_not_rendered_as_a_success(monkeypatch):
    engine = VLLMEngine()

    class EmptyEngine:
        async def generate(self, *_args, **_kwargs):
            if False:
                yield None

    engine.engine = EmptyEngine()
    monkeypatch.setattr(engine, "_params", lambda _sampling: object())

    async def consume():
        return [item async for item in engine.generate_stream("prompt", {})]

    with pytest.raises(RuntimeError, match="ended without an output"):
        asyncio.run(consume())


def test_sglang_empty_stream_is_not_rendered_as_a_success(monkeypatch):
    engine = SGLangEngine()
    aborted = []

    class EmptyEngine:
        async def async_generate(self, **_kwargs):
            async def empty():
                if False:
                    yield None

            return empty()

    engine.engine = EmptyEngine()
    monkeypatch.setattr(engine, "_abort_engine_request", lambda rid: aborted.append(rid) or True)

    async def consume():
        return [item async for item in engine.generate_stream("prompt", {})]

    with pytest.raises(RuntimeError, match="ended without an output"):
        asyncio.run(consume())
    assert len(aborted) == 1


def test_engine_results_and_capabilities_reject_coercible_contract_values():
    with pytest.raises(ValueError, match="prompt_tokens"):
        GenResult(prompt_tokens="3")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="completion_tokens"):
        GenDelta(completion_tokens=True)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="streaming"):
        EngineCaps(streaming=1)  # type: ignore[arg-type]


def test_sglang_output_metadata_rejects_coercible_token_counts():
    with pytest.raises(RuntimeError, match="prompt_tokens"):
        SGLangEngine._token_count({"prompt_tokens": "3"}, "prompt_tokens")


def test_sglang_uses_a_lease_disables_remote_code_and_restores_main(monkeypatch):
    created: dict = {}

    class FakeLease:
        port = 31234
        released = False

        def release(self):
            self.released = True

    lease = FakeLease()

    class FakeVendor:
        name = "xpu"

        def isolate_devices(self, _ids, _engine):
            return None

        def sglang_default_attention(self):
            return None

        def torch_device(self):
            return "xpu"

    class FakeTokenizer:
        @staticmethod
        def from_pretrained(path, **kwargs):
            created["tokenizer"] = (path, kwargs)
            return object()

    class FakeSGLangEngine:
        def __init__(self, **kwargs):
            created["engine"] = kwargs

    import exaserve.state.ports
    import exaserve.vendors

    monkeypatch.setattr(exaserve.vendors, "get_vendor", lambda _name: FakeVendor())
    monkeypatch.setattr(exaserve.state.ports, "reserve_port", lambda *_a, **_k: lease)
    monkeypatch.setitem(sys.modules, "sglang", SimpleNamespace(Engine=FakeSGLangEngine))
    monkeypatch.setitem(
        sys.modules,
        "transformers",
        SimpleNamespace(AutoTokenizer=FakeTokenizer),
    )
    main = sys.modules["__main__"]
    old_spec = getattr(main, "__spec__", None)
    had_file = hasattr(main, "__file__")
    old_file = getattr(main, "__file__", None)

    backend = SGLangEngine()
    backend.create(EngineSpec(model_id="m", local_path="/m", device_ids=[2]))

    assert created["tokenizer"][1]["trust_remote_code"] is False
    assert created["engine"]["trust_remote_code"] is False
    assert created["engine"]["nccl_port"] == lease.port
    assert lease.released
    assert getattr(main, "__spec__", None) is old_spec
    assert hasattr(main, "__file__") is had_file
    if had_file:
        assert main.__file__ == old_file


def test_sglang_releases_the_port_when_engine_creation_fails(monkeypatch):
    class FakeLease:
        port = 31235
        released = False

        def release(self):
            self.released = True

    lease = FakeLease()

    class FakeVendor:
        name = "xpu"

        def isolate_devices(self, _ids, _engine):
            return None

        def sglang_default_attention(self):
            return None

        def torch_device(self):
            return "xpu"

    class BrokenEngine:
        def __init__(self, **_kwargs):
            raise RuntimeError("constructor failed")

    import exaserve.state.ports
    import exaserve.vendors

    monkeypatch.setattr(exaserve.vendors, "get_vendor", lambda _name: FakeVendor())
    monkeypatch.setattr(exaserve.state.ports, "reserve_port", lambda *_a, **_k: lease)
    monkeypatch.setitem(sys.modules, "sglang", SimpleNamespace(Engine=BrokenEngine))
    monkeypatch.setitem(
        sys.modules,
        "transformers",
        SimpleNamespace(AutoTokenizer=SimpleNamespace(from_pretrained=lambda *_a, **_k: object())),
    )

    with pytest.raises(RuntimeError, match="constructor failed"):
        SGLangEngine().create(EngineSpec(model_id="m", local_path="/m", device_ids=[2]))

    assert lease.released


@pytest.mark.parametrize(
    ("constructor_fails", "stats_startup_fails", "shim_fails"),
    [(False, False, False), (True, False, False), (False, True, False), (False, False, True)],
)
def test_vllm_default_creation_is_import_light_and_releases_its_lease(
    monkeypatch, constructor_fails, stats_startup_fails, shim_fails
):
    created: dict = {}

    class FakeLease:
        port = 32345
        released = False

        def release(self):
            self.released = True

    lease = FakeLease()

    class FakeVendor:
        name = "xpu"

        def isolate_devices(self, _ids, _engine):
            return None

    class FakeArgs:
        def __init__(self, **kwargs):
            created["args"] = kwargs

    class FakeAsyncEngine:
        @staticmethod
        def from_engine_args(args, **kwargs):
            created["engine"] = (args, kwargs)
            if constructor_fails:
                raise RuntimeError("vLLM constructor failed")
            return FakeCreatedEngine()

    class FakeCreatedEngine:
        def shutdown(self):
            created["shutdown"] = True

    modules = {
        "vllm": ModuleType("vllm"),
        "vllm.engine": ModuleType("vllm.engine"),
        "vllm.engine.arg_utils": ModuleType("vllm.engine.arg_utils"),
        "vllm.engine.async_llm_engine": ModuleType("vllm.engine.async_llm_engine"),
    }
    modules["vllm.engine.arg_utils"].AsyncEngineArgs = FakeArgs
    modules["vllm.engine.async_llm_engine"].AsyncLLMEngine = FakeAsyncEngine
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)

    # If default vLLM startup reaches back into the Serve host, this sentinel
    # lacks every requested attribute and the test fails at that import.
    server_module = ModuleType("exaserve.server")
    if stats_startup_fails:
        server_module.CollectingStatLogger = SimpleNamespace(
            configure=lambda **_kwargs: None,
            get_instance=lambda: None,
        )
    monkeypatch.setitem(sys.modules, "exaserve.server", server_module)

    import exaserve.compat.engine_shim as shim
    import exaserve.state.ports
    import exaserve.vendors

    monkeypatch.setattr(exaserve.vendors, "get_vendor", lambda _name: FakeVendor())
    monkeypatch.setattr(exaserve.state.ports, "reserve_port", lambda *_a, **_k: lease)
    monkeypatch.setattr(shim, "environment_snapshot", lambda: {})

    def prepare_environment(*_args, **_kwargs):
        if shim_fails:
            raise RuntimeError("staged compatibility bootstrap unavailable")

    monkeypatch.setattr(shim, "prepare_environment", prepare_environment)
    monkeypatch.setattr(shim, "restore_environment", lambda _snapshot: None)

    backend = VLLMEngine()
    if constructor_fails:
        with pytest.raises(RuntimeError, match="vLLM constructor failed"):
            backend.create(EngineSpec(model_id="m", local_path="/m", device_ids=[2]))
    elif shim_fails:
        with pytest.raises(RuntimeError, match="compatibility bootstrap"):
            backend.create(EngineSpec(model_id="m", local_path="/m", device_ids=[2]))
    elif stats_startup_fails:
        with pytest.raises(RuntimeError, match="did not instantiate CollectingStatLogger"):
            backend.create(
                EngineSpec(
                    model_id="m",
                    local_path="/m",
                    device_ids=[2],
                    collect_stats=True,
                )
            )
        assert created["shutdown"] is True
    else:
        backend.create(EngineSpec(model_id="m", local_path="/m", device_ids=[2]))

    assert created["args"]["master_port"] == lease.port
    assert created["args"]["trust_remote_code"] is False
    assert lease.released
