"""PR-017 acceptance: trace identity covers every content-affecting input."""

from __future__ import annotations

import json
import importlib
import sys
from types import SimpleNamespace

from eval.lib.spec_io import load_experiment_spec
from eval.lib.trace_generators import _request_mode
from eval.lib.trace_store import _trace_identity
from eval.lib.utils import stable_hash


def _spec_yaml(prompt_path, arrival: str) -> str:
    return (
        f"""
name: ident_spec
trace:
  kind: weak_scaling
  input_prompt_path: {prompt_path}
  tokenizer_builder: "eval.testing:whitespace_tokenizer_map"
workload:
  duration: 1.0
  input_len: 8
  output_len: 4
  rate_per_node: 2.0
  arrival: {arrival}
deployment:
  num_nodes: 1
  models:
    - model_id: test/model
      tensor_parallel_size: 1
      pipeline_parallel_size: 1
      max_model_len: 64
      size: 1
client:
  num_runs: 1
  dest: direct
  num_go_procs: 1
  num_go_workers: 1
  go_concurrency: 4
backend:
  default: mock
  args:
    mock: {{}}
scheduler:
  type: pbs
""".strip()
        + "\n"
    )


def _write(tmp_path, name: str, arrival: str, prompt_content: dict) -> str:
    prompt_path = tmp_path / f"prompts_{name}.json"
    prompt_path.write_text(json.dumps(prompt_content))
    spec_path = tmp_path / f"{name}.yaml"
    spec_path.write_text(_spec_yaml(prompt_path, arrival))
    return str(spec_path)


_PROMPTS = {"prompts": [{"text": "hello world one two three"}]}


def test_arrival_mode_changes_trace_identity(tmp_path):
    fixed = load_experiment_spec(_write(tmp_path, "a", "fixed", _PROMPTS))
    poisson = load_experiment_spec(_write(tmp_path, "b", "poisson", _PROMPTS))
    # identical except arrival: the ids must differ (old code collided).
    id_fixed = _trace_identity(fixed)
    id_poisson = _trace_identity(poisson)
    id_fixed["trace"] = id_poisson["trace"] = {}  # ignore per-file paths/digests
    assert stable_hash(id_fixed, length=64) != stable_hash(id_poisson, length=64)
    assert id_fixed["workload"]["arrival"] == "fixed"


def test_prompt_content_edit_changes_trace_identity(tmp_path):
    spec_path = _write(tmp_path, "c", "fixed", _PROMPTS)
    before = stable_hash(_trace_identity(load_experiment_spec(spec_path)), length=64)
    # In-place edit at the SAME path: identity must change (content digest).
    prompt_path = tmp_path / "prompts_c.json"
    prompt_path.write_text(json.dumps({"prompts": [{"text": "edited corpus"}]}))
    after = stable_hash(_trace_identity(load_experiment_spec(spec_path)), length=64)
    assert before != after


def test_equal_prompt_content_at_different_paths_has_one_content_identity(tmp_path):
    first = load_experiment_spec(_write(tmp_path, "first", "fixed", _PROMPTS))
    second = load_experiment_spec(_write(tmp_path, "second", "fixed", _PROMPTS))

    assert stable_hash(_trace_identity(first), length=64) == stable_hash(
        _trace_identity(second), length=64
    )


def test_custom_tokenizer_builder_source_edit_changes_trace_identity(tmp_path, monkeypatch):
    package = tmp_path / "identity_builder"
    package.mkdir()
    (package / "__init__.py").write_text("from .helper import build\n")
    helper = package / "helper.py"
    helper.write_text("def build(spec):\n    return {}\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    spec = load_experiment_spec(_write(tmp_path, "builder", "fixed", _PROMPTS))
    spec.trace.tokenizer_builder = "identity_builder:build"
    before = stable_hash(_trace_identity(spec), length=64)

    helper.write_text("def build(spec):\n    return {'changed': True}\n")
    importlib.invalidate_caches()
    after = stable_hash(_trace_identity(spec), length=64)
    assert before != after
    sys.modules.pop("identity_builder", None)


def test_default_tokenizer_file_edit_changes_identity_without_hashing_weights(tmp_path):
    prompt_path = tmp_path / "prompts.json"
    prompt_path.write_text(json.dumps(_PROMPTS))
    spec_path = tmp_path / "default.yaml"
    spec_path.write_text(
        _spec_yaml(prompt_path, "fixed").replace(
            '  tokenizer_builder: "eval.testing:whitespace_tokenizer_map"\n', ""
        )
    )
    model_dir = tmp_path / "models" / "test--model"
    model_dir.mkdir(parents=True)
    tokenizer = model_dir / "tokenizer.json"
    tokenizer.write_text('{"version": 1}')
    (model_dir / "model.safetensors").write_bytes(b"weight-v1")

    spec = load_experiment_spec(str(spec_path))
    spec.deployment.model_storage_path = str(tmp_path / "models")
    before = stable_hash(_trace_identity(spec), length=64)
    (model_dir / "model.safetensors").write_bytes(b"weight-v2")
    assert stable_hash(_trace_identity(spec), length=64) == before
    tokenizer.write_text('{"version": 2}')
    assert stable_hash(_trace_identity(spec), length=64) != before


def test_request_protocol_selection_honors_single_and_mixed_typed_modes():
    completion = SimpleNamespace(
        workload=SimpleNamespace(seed=17, modes={"chat": 0, "completion": 1})
    )
    assert {_request_mode(completion, index) for index in range(20)} == {"completion"}

    mixed = SimpleNamespace(workload=SimpleNamespace(seed=17, modes={"chat": 1, "completion": 1}))
    first = [_request_mode(mixed, index) for index in range(100)]
    second = [_request_mode(mixed, index) for index in range(100)]
    assert first == second
    assert set(first) == {"chat", "completion"}
