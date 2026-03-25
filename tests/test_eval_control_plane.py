import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from eval.lib.backends import get_backend_adapter
from eval.lib.matrix import expand_matrix
from eval.lib.run_executor import execute_run
from eval.lib.run_planner import load_run_plan, materialize_run_bundles, materialize_traces
from eval.lib.spec_io import load_experiment_spec


def _write_prompt_dataset(path: Path) -> None:
    path.write_text(
        json.dumps(
            [
                {
                    "conversations": [
                        {"from": "human", "value": "Explain Aurora scheduling."},
                        {"from": "gpt", "value": "Sure."},
                    ]
                }
            ]
        ),
        encoding="utf-8",
    )


def _write_spec(path: Path, prompt_path: Path, *, proxy_type: str = "none") -> None:
    path.write_text(
        f"""
name: test_spec
matrix:
  name_template: "{{num_nodes}}_nodes"
  axes:
    - name: num_nodes
      values: [1, 2]
      targets: [deployment.num_nodes, client.num_nodes, scheduler.nodes]
trace:
  kind: weak_scaling
  input_prompt_path: {prompt_path}
workload:
  duration: 1.0
  input_len: 8
  output_len: 4
  rate_per_node: 2.0
deployment:
  models:
    - model_id: test/model
      tensor_parallel_size: 1
      pipeline_parallel_size: 1
      max_model_len: 64
      size: 1
client:
  num_runs: 1
  dest: {"proxy" if proxy_type != "none" else "direct"}
  num_go_procs: 1
  num_go_workers: 1
  go_concurrency: 4
backend:
  default: mock
  args:
    mock: {{}}
    ray:
      proxy:
        type: {proxy_type}
scheduler:
  type: pbs
""".strip()
        + "\n",
        encoding="utf-8",
    )


@pytest.fixture()
def temp_spec(tmp_path, monkeypatch):
    prompt_path = tmp_path / "prompts.json"
    spec_path = tmp_path / "spec.yaml"
    _write_prompt_dataset(prompt_path)
    _write_spec(spec_path, prompt_path)
    monkeypatch.setattr("eval.lib.trace_store._build_tokenizer_map", lambda _spec: {})
    return spec_path


def test_spec_load_and_matrix_expand(temp_spec):
    spec = load_experiment_spec(str(temp_spec))
    variants = expand_matrix(spec)
    assert spec.name == "test_spec"
    assert [variant.variant_name for variant in variants] == ["1_nodes", "2_nodes"]
    assert [variant.spec.deployment.num_nodes for variant in variants] == [1, 2]
    assert [variant.spec.client.num_nodes for variant in variants] == [1, 2]


def test_trace_artifacts_are_reused(temp_spec, tmp_path):
    artifacts_one = materialize_traces(str(temp_spec), trace_root=str(tmp_path / "traces"))
    artifacts_two = materialize_traces(str(temp_spec), trace_root=str(tmp_path / "traces"))
    assert [artifact.trace_id for artifact in artifacts_one] == [
        artifact.trace_id for artifact in artifacts_two
    ]
    assert Path(artifacts_one[0].trace_path).is_file()
    with open(artifacts_one[0].metadata_path, "r", encoding="utf-8") as handle:
        metadata = json.load(handle)
    assert metadata["row_count"] > 0


def test_run_bundle_materialization_and_mock_execute(temp_spec, tmp_path):
    plans = materialize_run_bundles(
        str(temp_spec),
        backend_name="mock",
        experiments_root=str(tmp_path / "runs"),
        trace_root=str(tmp_path / "traces"),
    )
    assert len(plans) == 2
    run_plan = load_run_plan(plans[0].bundle.run_yaml_path)
    assert Path(run_plan.bundle.job_path).is_file()
    assert Path(run_plan.runtime_manifest_path).is_file()
    assert Path(run_plan.bundle.spec_snapshot_path).is_file()
    assert run_plan.scheduler.queue == "debug"

    exit_code = execute_run(run_plan.bundle.run_yaml_path, dry_run=True)
    assert exit_code == 0
    state = json.loads(Path(run_plan.bundle.state_path).read_text(encoding="utf-8"))
    assert state["status"] == "dry-run"


def test_ray_adapter_selects_litellm_env(temp_spec, tmp_path):
    prompt_path = tmp_path / "prompts.json"
    spec_path = tmp_path / "ray_spec.yaml"
    _write_prompt_dataset(prompt_path)
    _write_spec(spec_path, prompt_path, proxy_type="litellm")

    plans = materialize_run_bundles(
        str(spec_path),
        backend_name="ray",
        experiments_root=str(tmp_path / "runs"),
        trace_root=str(tmp_path / "traces"),
    )
    adapter = get_backend_adapter("ray")
    runtime_env = adapter.runtime_env(plans[0])
    assert runtime_env.env_script.endswith("env_litellm")


def test_cli_validate_and_submit_dry_run(temp_spec, tmp_path):
    run_plans = materialize_run_bundles(
        str(temp_spec),
        backend_name="mock",
        experiments_root=str(tmp_path / "runs"),
        trace_root=str(tmp_path / "traces"),
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = os.getcwd() + os.pathsep + env.get("PYTHONPATH", "")

    validate = subprocess.run(
        [sys.executable, "-m", "eval.cli", "spec", "validate", str(temp_spec)],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    assert validate.returncode == 0
    assert "VALID test_spec" in validate.stdout

    submit = subprocess.run(
        [
            sys.executable,
            "-m",
            "eval.cli",
            "run",
            "submit",
            run_plans[0].bundle.run_yaml_path,
            "--dry-run",
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    assert submit.returncode == 0
    assert "qsub" in submit.stdout


def test_eval_runtime_has_no_legacy_import_hacks():
    result = subprocess.run(
        [
            "rg",
            "-n",
            "from exp_configs import \\*|sys\\.path\\.insert\\(",
            "eval/cli.py",
            "eval/replay_client.py",
            "eval/lib",
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 1, result.stdout
