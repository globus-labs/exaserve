"""PR-017 acceptance: trace identity covers every content-affecting input."""

from __future__ import annotations

import json

from eval.lib.spec_io import load_experiment_spec
from eval.lib.trace_store import _trace_identity
from eval.lib.utils import stable_hash


def _spec_yaml(prompt_path, arrival: str) -> str:
    return f"""
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
""".strip() + "\n"


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
    assert stable_hash(id_fixed, length=16) != stable_hash(id_poisson, length=16)
    assert id_fixed["workload"]["arrival"] == "fixed"


def test_prompt_content_edit_changes_trace_identity(tmp_path):
    spec_path = _write(tmp_path, "c", "fixed", _PROMPTS)
    before = stable_hash(_trace_identity(load_experiment_spec(spec_path)), length=16)
    # In-place edit at the SAME path: identity must change (content digest).
    prompt_path = tmp_path / "prompts_c.json"
    prompt_path.write_text(json.dumps({"prompts": [{"text": "edited corpus"}]}))
    after = stable_hash(_trace_identity(load_experiment_spec(spec_path)), length=16)
    assert before != after
