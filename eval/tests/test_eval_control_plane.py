import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import eval.site_config as site_config
from eval.lib.backends import get_backend_adapter
from eval.lib.matrix import expand_matrix
from eval.lib.run_executor import execute_run
from eval.lib.run_planner import (
    load_run_plan,
    materialize_run_bundles,
    materialize_traces,
    resolve_run_group_dir,
)
from eval.lib.spec_io import load_experiment_spec


@pytest.fixture(autouse=True)
def reset_site_config_cache():
    site_config.clear_site_config_cache()
    yield
    site_config.clear_site_config_cache()


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
  replica_max_ongoing_requests: 7
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


def _init_git_repo(path: Path, *, tracked_contents: str = "committed\n") -> Path:
    path.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init"], cwd=path, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    subprocess.run(
        ["git", "config", "user.email", "test@example.com"],
        cwd=path,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test User"],
        cwd=path,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    (path / "tracked.txt").write_text(tracked_contents, encoding="utf-8")
    subprocess.run(["git", "add", "tracked.txt"], cwd=path, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    subprocess.run(
        ["git", "commit", "-m", "initial"],
        cwd=path,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return path


def _write_plot_result(path: Path, *, pbs_job_name: str = "weak_scaling_run0_1_nodes") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "config": {
                    "pbs_job_name": pbs_job_name,
                    "job_trace_config": {
                        "rpn": 2,
                        "duration": 1,
                        "input_len": 8,
                        "output_len": 4,
                    },
                    "model_deployment_config": {
                        "num_gpus_per_node": 12,
                        "model_configs": [
                            {
                                "model_id": "test/model",
                                "tensor_parallel_size": 1,
                            }
                        ],
                    },
                    "job_replay_client_config": {
                        "num_go_procs": 1,
                        "num_go_workers": 1,
                        "go_concurrency": 4,
                        "warmup_rps": 0,
                        "warmup_duration_s": 0.0,
                    },
                    "proxy_config": {
                        "type": "litellm",
                        "num_workers": 2,
                    },
                },
                "meta": {
                    "completed_runs": 1,
                },
                "overall": {
                    "tps": 10.0,
                    "rps": 2.0,
                    "errors": 0,
                    "p50_s": 0.1,
                    "p99_s": 0.2,
                    "trace_span_s": 1.0,
                    "actual_dispatch_s": 1.0,
                    "dispatch_overhead_s": 0.0,
                },
                "requests": [
                    {"latency": 0.1},
                    {"latency": 0.2},
                ],
            }
        ),
        encoding="utf-8",
    )


@pytest.fixture()
def temp_spec(tmp_path, monkeypatch):
    prompt_path = tmp_path / "prompts.json"
    spec_path = tmp_path / "spec.yaml"
    _write_prompt_dataset(prompt_path)
    _write_spec(spec_path, prompt_path)
    monkeypatch.setattr("eval.lib.trace_generators.build_tokenizer_map", lambda _spec: {})
    return spec_path


def test_spec_load_and_matrix_expand(temp_spec):
    spec = load_experiment_spec(str(temp_spec))
    variants = expand_matrix(spec)
    assert spec.name == "test_spec"
    assert spec.deployment.replica_max_ongoing_requests == 7
    assert [variant.variant_name for variant in variants] == ["1_nodes", "2_nodes"]
    assert [variant.spec.deployment.num_nodes for variant in variants] == [1, 2]
    assert [variant.spec.client.num_nodes for variant in variants] == [1, 2]


def test_matrix_derived_fields(tmp_path, monkeypatch):
    prompt_path = tmp_path / "prompts.json"
    _write_prompt_dataset(prompt_path)
    spec_path = tmp_path / "derived_spec.yaml"
    spec_path.write_text(
        f"""
name: test_derived
matrix:
  name_template: "{{num_nodes}}_nodes"
  axes:
    - name: num_nodes
      values: [1, 4, 16]
      targets: [deployment.num_nodes, client.num_nodes, scheduler.nodes]
  derived:
    - path: client.go_concurrency
      expr: "num_nodes * 100"
    - path: client.num_go_procs
      expr: "min(num_nodes * 4, 32)"
trace:
  kind: weak_scaling
  input_prompt_path: {prompt_path}
workload:
  duration: 1.0
  input_len: 8
  output_len: 4
  rate_per_node: 2.0
deployment:
  replica_max_ongoing_requests: 7
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
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("eval.lib.trace_generators.build_tokenizer_map", lambda _spec: {})
    spec = load_experiment_spec(str(spec_path))
    variants = expand_matrix(spec)
    assert len(variants) == 3
    assert [v.spec.client.go_concurrency for v in variants] == [100, 400, 1600]
    assert [v.spec.client.num_go_procs for v in variants] == [4, 16, 32]
    assert [v.spec.deployment.num_nodes for v in variants] == [1, 4, 16]


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


def test_run_bundle_materialization_and_mock_execute(temp_spec, tmp_path, monkeypatch):
    repo_root = _init_git_repo(tmp_path / "repo")
    monkeypatch.setenv("AURORA_SNAPSHOT_DIR", str(tmp_path / "snapshots"))

    plans = materialize_run_bundles(
        str(temp_spec),
        backend_name="mock",
        experiments_root=str(tmp_path / "runs"),
        trace_root=str(tmp_path / "traces"),
        repo_root=str(repo_root),
    )
    assert len(plans) == 2
    run_plan = load_run_plan(plans[0].bundle.run_yaml_path)
    assert run_plan.run_group_id == "run0"
    assert Path(run_plan.bundle.group_root_dir).name == "run0"
    assert Path(run_plan.bundle.root_dir).name == "1-nodes"
    assert Path(run_plan.bundle.job_path).is_file()
    assert Path(run_plan.runtime_manifest_path).is_file()
    assert Path(run_plan.bundle.group_root_dir, "meta", "spec.yaml").is_file()
    assert Path(run_plan.bundle.group_root_dir, "meta", "run_group.json").is_file()
    assert run_plan.scheduler.queue == "debug"
    assert run_plan.repo_root == run_plan.snapshot_root
    assert Path(run_plan.snapshot_root).is_dir()
    assert Path(run_plan.spec_path).samefile(Path(run_plan.bundle.group_root_dir) / "meta" / "spec.yaml")

    exit_code = execute_run(run_plan.bundle.run_yaml_path, dry_run=True)
    assert exit_code == 0
    state = json.loads(Path(run_plan.bundle.state_path).read_text(encoding="utf-8"))
    assert state["status"] == "dry-run"
    assert state["run_group_id"] == "run0"


def test_materialize_twice_creates_incrementing_run_groups_and_reuses_snapshot(
    temp_spec, tmp_path, monkeypatch
):
    repo_root = _init_git_repo(tmp_path / "repo")
    monkeypatch.setenv("AURORA_SNAPSHOT_DIR", str(tmp_path / "snapshots"))

    first = materialize_run_bundles(
        str(temp_spec),
        backend_name="mock",
        experiments_root=str(tmp_path / "runs"),
        trace_root=str(tmp_path / "traces"),
        repo_root=str(repo_root),
    )
    second = materialize_run_bundles(
        str(temp_spec),
        backend_name="mock",
        experiments_root=str(tmp_path / "runs"),
        trace_root=str(tmp_path / "traces"),
        repo_root=str(repo_root),
    )

    assert {plan.run_group_id for plan in first} == {"run0"}
    assert {plan.run_group_id for plan in second} == {"run1"}
    assert {plan.snapshot_root for plan in first} == {plan.snapshot_root for plan in second}
    latest = resolve_run_group_dir("test_spec", experiments_root=str(tmp_path / "runs"))
    assert Path(latest).name == "run1"


def test_dirty_repo_warning_and_snapshot_excludes_uncommitted_content(
    temp_spec, tmp_path, monkeypatch, capsys
):
    repo_root = _init_git_repo(tmp_path / "repo", tracked_contents="committed\n")
    monkeypatch.setenv("AURORA_SNAPSHOT_DIR", str(tmp_path / "snapshots"))
    (repo_root / "tracked.txt").write_text("dirty tracked\n", encoding="utf-8")
    (repo_root / "untracked.txt").write_text("untracked\n", encoding="utf-8")

    plans = materialize_run_bundles(
        str(temp_spec),
        backend_name="mock",
        experiments_root=str(tmp_path / "runs"),
        trace_root=str(tmp_path / "traces"),
        repo_root=str(repo_root),
        max_workers=1,
    )
    captured = capsys.readouterr()
    assert "WARNING: repo has" in captured.err
    assert "tracked.txt" in captured.err
    assert "untracked.txt" in captured.err

    snapshot_root = Path(plans[0].snapshot_root)
    assert (snapshot_root / "tracked.txt").read_text(encoding="utf-8") == "committed\n"
    assert not (snapshot_root / "untracked.txt").exists()
    run_group_meta = json.loads(
        (Path(plans[0].bundle.group_root_dir) / "meta" / "run_group.json").read_text(encoding="utf-8")
    )
    assert run_group_meta["git_dirty"] is True
    assert sorted(run_group_meta["dirty_files"]) == ["tracked.txt", "untracked.txt"]


def test_ray_adapter_always_uses_aurora_env(temp_spec, tmp_path, monkeypatch):
    """Backend always uses env_aurora; litellm runs as a separate subprocess."""
    prompt_path = tmp_path / "prompts.json"
    spec_path = tmp_path / "ray_spec.yaml"
    repo_root = _init_git_repo(tmp_path / "repo")
    _write_prompt_dataset(prompt_path)
    _write_spec(spec_path, prompt_path, proxy_type="litellm")
    monkeypatch.setenv("AURORA_SNAPSHOT_DIR", str(tmp_path / "snapshots"))

    plans = materialize_run_bundles(
        str(spec_path),
        backend_name="ray",
        experiments_root=str(tmp_path / "runs"),
        trace_root=str(tmp_path / "traces"),
        repo_root=str(repo_root),
    )
    adapter = get_backend_adapter("ray")
    runtime_env = adapter.runtime_env(plans[0])
    assert runtime_env.env_script.endswith("env_aurora")


def test_cli_validate_and_submit_all_latest_dry_run(temp_spec, tmp_path, monkeypatch):
    repo_root = _init_git_repo(tmp_path / "repo")
    monkeypatch.setenv("AURORA_SNAPSHOT_DIR", str(tmp_path / "snapshots"))
    run_kwargs = dict(
        backend_name="mock",
        experiments_root=str(tmp_path / "runs"),
        trace_root=str(tmp_path / "traces"),
        repo_root=str(repo_root),
    )
    materialize_run_bundles(str(temp_spec), **run_kwargs)
    materialize_run_bundles(str(temp_spec), **run_kwargs)

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

    submit_all = subprocess.run(
        [
            sys.executable,
            "-m",
            "eval.cli",
            "run",
            "submit-all",
            "test_spec",
            "--run-group",
            "latest",
            "--experiments-root",
            str(tmp_path / "runs"),
            "--dry-run",
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    assert submit_all.returncode == 0
    assert "run1" in submit_all.stdout
    assert "run0" not in submit_all.stdout


def test_plot_scripts_resolve_latest_run_group(tmp_path, monkeypatch):
    experiments_root = tmp_path / "experiments"
    result_path = (
        experiments_root
        / "runs"
        / "weak_scaling_tests"
        / "run0"
        / "1-nodes"
        / "results"
        / "result0.json"
    )
    _write_plot_result(result_path)

    env = os.environ.copy()
    env["PYTHONPATH"] = os.getcwd() + os.pathsep + env.get("PYTHONPATH", "")
    env["AURORA_EXPERIMENTS_ROOT"] = str(experiments_root)
    weak_plot = tmp_path / "weak.png"
    litellm_plot = tmp_path / "litellm.png"

    weak = subprocess.run(
        [
            sys.executable,
            "-m",
            "eval.plot.weakscaling",
            "-e",
            "weak_scaling_tests",
            "--run-group",
            "latest",
            "--linear",
            "-o",
            str(weak_plot),
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    assert weak.returncode == 0, weak.stderr
    assert weak_plot.is_file()

    litellm = subprocess.run(
        [
            sys.executable,
            "-m",
            "eval.plot.litellm_scaling",
            "-e",
            "weak_scaling_tests",
            "--run-group",
            "latest",
            "--linear",
            "-o",
            str(litellm_plot),
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    assert litellm.returncode == 0, litellm.stderr
    assert litellm_plot.is_file()


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
