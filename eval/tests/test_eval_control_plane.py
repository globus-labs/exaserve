import copy
import io
import json
import os
import re
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest
import yaml

import eval.site_config as site_config
from eval.lib.backends import get_backend_adapter
from eval.lib.matrix import expand_matrix
from eval.lib.models import VariantSpec
from eval.lib.run_executor import execute_run
from eval.lib.run_planner import (
    _build_go_client,
    _extract_git_archive,
    load_run_plan,
    materialize_run_bundles,
    materialize_traces,
    resolve_run_group_dir,
)
from eval.lib.spec_io import load_experiment_spec
from eval.lib.trace_store import materialize_trace_artifact


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
  tokenizer_builder: "eval.testing:whitespace_tokenizer_map"
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
    subprocess.run(
        ["git", "init"], cwd=path, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
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
    subprocess.run(
        ["git", "add", "tracked.txt"],
        cwd=path,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    subprocess.run(
        ["git", "commit", "-m", "initial"],
        cwd=path,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return path


def test_snapshot_go_client_build_is_static_and_reproducible(tmp_path, monkeypatch):
    go_client = tmp_path / "eval" / "go_client"
    go_client.mkdir(parents=True)
    fake_go = str(tmp_path / "bin" / "go")
    calls = []

    def fake_run_finite(argv, *, timeout_s, cwd=None, env=None, **kwargs):
        del timeout_s, kwargs
        calls.append((list(argv), cwd, None if env is None else dict(env)))
        if argv[1:] == ["env", "GOVERSION"]:
            return subprocess.CompletedProcess(argv, 0, "go1.25.3\n", "")
        binary = Path(cwd) / "bin" / "go_dispatch"
        binary.parent.mkdir()
        binary.write_bytes(b"deterministic-static-client")
        binary.chmod(0o755)
        return subprocess.CompletedProcess(argv, 0, "", "")

    import exaserve.control.finite_process as finite_process
    import eval.lib.run_planner as run_planner

    monkeypatch.setattr(run_planner.shutil, "which", lambda name: fake_go if name == "go" else None)
    monkeypatch.setattr(finite_process, "run_finite", fake_run_finite)

    identity = _build_go_client(str(tmp_path))

    assert identity["go_version"] == "go1.25.3"
    assert re.fullmatch(r"[0-9a-f]{64}", identity["go_dispatch_sha256"])
    argv, cwd, env = calls[1]
    assert argv == [
        fake_go,
        "build",
        "-trimpath",
        "-buildvcs=false",
        "-ldflags=-buildid=",
        "-o",
        os.path.join("bin", "go_dispatch"),
        ".",
    ]
    assert cwd == str(go_client)
    assert env is not None and env["CGO_ENABLED"] == "0"


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
                    "gather_by_run": [
                        {
                            "schema_version": 1,
                            "expected_ranks": 1,
                            "collected_ranks": [0],
                            "missing_ranks": [],
                            "complete": True,
                            "shards": [
                                {
                                    "rank": 0,
                                    "size_bytes": 1,
                                    "sha256": "1" * 64,
                                    "transport": "in_memory",
                                }
                            ],
                        }
                    ],
                },
                "per_run": [{"run_index": 0}],
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
  tokenizer_builder: "eval.testing:whitespace_tokenizer_map"
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


def test_trace_cache_is_variant_independent_and_force_does_not_replace_it(temp_spec, tmp_path):
    spec = load_experiment_spec(str(temp_spec))
    first = materialize_trace_artifact(
        VariantSpec(spec=spec, variant_name="presentation-a"),
        store_root=str(tmp_path / "traces"),
    )
    before_trace = Path(first.trace_path).read_bytes()
    before_metadata = Path(first.metadata_path).read_bytes()

    second = materialize_trace_artifact(
        VariantSpec(spec=spec, variant_name="presentation-b"),
        store_root=str(tmp_path / "traces"),
    )
    verified = materialize_trace_artifact(
        VariantSpec(spec=spec, variant_name="presentation-c"),
        store_root=str(tmp_path / "traces"),
        force=True,
    )

    assert first.trace_id == second.trace_id == verified.trace_id
    assert Path(first.trace_path).read_bytes() == before_trace
    assert Path(first.metadata_path).read_bytes() == before_metadata
    assert "variant_name" not in json.loads(before_metadata)


def test_parallel_trace_materialization_has_a_finite_owner_deadline(temp_spec, tmp_path):
    with pytest.raises(TimeoutError, match="unfinished variants"):
        materialize_traces(
            str(temp_spec),
            trace_root=str(tmp_path / "traces"),
            timeout_s=1e-9,
            force=True,
        )


def test_snapshot_archive_extraction_rejects_path_escape(tmp_path):
    archive_path = tmp_path / "snapshot.tar"
    with tarfile.open(archive_path, "w") as archive:
        member = tarfile.TarInfo("../escape")
        payload = b"not part of the snapshot"
        member.size = len(payload)
        archive.addfile(member, io.BytesIO(payload))

    with pytest.raises(RuntimeError, match="unsafe entry"):
        _extract_git_archive(str(archive_path), str(tmp_path / "destination"))
    assert not (tmp_path / "escape").exists()


def test_run_bundle_materialization_and_mock_execute(temp_spec, tmp_path, monkeypatch):
    repo_root = _init_git_repo(tmp_path / "repo")
    monkeypatch.setenv("EXASERVE_SNAPSHOT_DIR", str(tmp_path / "snapshots"))

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
    assert run_plan.scheduler.queue == "capacity"
    assert run_plan.repo_root == run_plan.snapshot_root
    assert Path(run_plan.snapshot_root).is_dir()
    assert Path(run_plan.spec_path).samefile(
        Path(run_plan.bundle.group_root_dir) / "meta" / "spec.yaml"
    )

    exit_code = execute_run(run_plan.bundle.run_yaml_path, dry_run=True)
    assert exit_code == 0
    state = json.loads(Path(run_plan.bundle.state_path).read_text(encoding="utf-8"))
    assert state["state"] == "PLANNED"
    assert state["data"]["phase"] == "dry-run"
    assert state["provenance"]["run_group_id"] == "run0"


def test_execute_run_refuses_a_concurrent_executor(temp_spec, tmp_path, monkeypatch):
    from exaserve.state.atomic import ExclusiveLease

    repo_root = _init_git_repo(tmp_path / "repo")
    monkeypatch.setenv("EXASERVE_SNAPSHOT_DIR", str(tmp_path / "snapshots"))
    plan = materialize_run_bundles(
        str(temp_spec),
        backend_name="mock",
        experiments_root=str(tmp_path / "runs"),
        trace_root=str(tmp_path / "traces"),
        repo_root=str(repo_root),
    )[0]

    with ExclusiveLease(plan.bundle.state_path + ".execute", ttl_s=60):
        with pytest.raises(RuntimeError, match="another executor owns"):
            execute_run(plan.bundle.run_yaml_path)

    state = json.loads(Path(plan.bundle.state_path).read_text(encoding="utf-8"))
    assert state["state"] == "PLANNED"
    assert state["data"]["phase"] == "materialized"


def test_startup_only_cannot_succeed_with_an_incomplete_result_manifest(monkeypatch):
    from types import SimpleNamespace

    from eval.lib.run_executor import _execute_run_locked

    transitions = []

    class Adapter:
        def launch(self, _ctx):
            return SimpleNamespace()

        def wait_ready(self, _ctx, _launched):
            return None

        def discover_targets(self, _ctx, _launched):
            return ["http://target"]

        def stop(self, _ctx, _launched):
            return None

    class Heartbeat:
        def ensure_held(self):
            return None

    run_plan = SimpleNamespace(
        backend_name="mock",
        client=SimpleNamespace(startup_only=True),
    )
    incomplete = SimpleNamespace(
        complete=False,
        incomplete_reasons=("compatibility receipt disappeared before hashing",),
        manifest_hash="a" * 64,
    )
    monkeypatch.setattr(
        "eval.lib.run_executor.write_run_state",
        lambda _plan, state, **data: transitions.append((state, data)),
    )
    monkeypatch.setattr(
        "eval.lib.run_executor._capture_deployment_evidence",
        lambda _plan, _launched: {"compatibility_receipts": "/missing"},
    )
    monkeypatch.setattr(
        "eval.lib.run_executor._publish_result_manifest",
        lambda *_args, **_kwargs: incomplete,
    )

    assert _execute_run_locked(run_plan, Adapter(), object(), Heartbeat()) == 3
    assert [state for state, _ in transitions] == ["running", "partial"]
    assert transitions[-1][1]["exit_code"] == 3


def test_startup_only_never_publishes_a_result_manifest_before_cleanup(monkeypatch):
    from types import SimpleNamespace

    from eval.lib.run_executor import _execute_run_locked

    published = []
    transitions = []

    class Adapter:
        def launch(self, _ctx):
            return SimpleNamespace()

        def wait_ready(self, _ctx, _launched):
            return None

        def discover_targets(self, _ctx, _launched):
            return ["http://target"]

        def stop(self, _ctx, _launched):
            raise RuntimeError("cleanup failed")

    class Heartbeat:
        def ensure_held(self):
            return None

    run_plan = SimpleNamespace(
        backend_name="mock",
        client=SimpleNamespace(startup_only=True),
    )
    monkeypatch.setattr(
        "eval.lib.run_executor.write_run_state",
        lambda _plan, state, **data: transitions.append((state, data)),
    )
    monkeypatch.setattr(
        "eval.lib.run_executor._capture_deployment_evidence",
        lambda _plan, _launched: {"compatibility_receipts": "/captured"},
    )
    monkeypatch.setattr(
        "eval.lib.run_executor._publish_result_manifest",
        lambda *_args, **_kwargs: published.append(True),
    )

    with pytest.raises(RuntimeError, match="cleanup failed"):
        _execute_run_locked(run_plan, Adapter(), object(), Heartbeat())

    assert published == []
    assert [state for state, _ in transitions] == ["running", "failed"]


def test_keyboard_interrupt_publishes_cancelled_after_cleanup(monkeypatch):
    from types import SimpleNamespace

    from eval.lib.run_executor import _execute_run_locked

    transitions = []
    stopped = []

    class Adapter:
        def launch(self, _ctx):
            return SimpleNamespace()

        def wait_ready(self, _ctx, _launched):
            raise KeyboardInterrupt()

        def stop(self, _ctx, launched):
            stopped.append(launched)

    class Heartbeat:
        def ensure_held(self):
            return None

    run_plan = SimpleNamespace(backend_name="mock")
    monkeypatch.setattr(
        "eval.lib.run_executor.write_run_state",
        lambda _plan, state, **data: transitions.append((state, data)),
    )

    with pytest.raises(KeyboardInterrupt):
        _execute_run_locked(run_plan, Adapter(), object(), Heartbeat())

    assert len(stopped) == 1
    assert [state for state, _ in transitions] == ["running", "cancelled"]
    assert transitions[-1][1]["error"].startswith("KeyboardInterrupt:")


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda payload, outside: payload["bundle"].__setitem__("results_dir", str(outside)),
            "bundle.results_dir",
        ),
        (
            lambda payload, outside: payload.__setitem__(
                "runtime_manifest_path", str(outside / "mock_runtime.yaml")
            ),
            "runtime_manifest_path",
        ),
        (
            lambda payload, outside: payload["trace_artifact"].__setitem__(
                "trace_path", str(outside / "trace.jsonl")
            ),
            "trace_artifact.trace_path",
        ),
    ],
)
def test_run_bundle_rejects_hash_exempt_path_substitution(
    temp_spec, tmp_path, monkeypatch, mutate, message
):
    repo_root = _init_git_repo(tmp_path / "repo")
    monkeypatch.setenv("EXASERVE_SNAPSHOT_DIR", str(tmp_path / "snapshots"))
    plan = materialize_run_bundles(
        str(temp_spec),
        backend_name="mock",
        experiments_root=str(tmp_path / "runs"),
        trace_root=str(tmp_path / "traces"),
        repo_root=str(repo_root),
    )[0]
    run_yaml = Path(plan.bundle.run_yaml_path)
    payload = yaml.safe_load(run_yaml.read_text(encoding="utf-8"))
    outside = tmp_path / "outside"
    outside.mkdir()
    mutate(payload, outside)
    run_yaml.write_text(yaml.safe_dump(payload), encoding="utf-8")
    with pytest.raises(ValueError, match=message):
        load_run_plan(str(run_yaml))


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda payload: payload.__setitem__("run_id", 123), "run_id must be"),
        (
            lambda payload: payload.__setitem__("run_id", "another-run"),
            "run_id disagrees",
        ),
        (
            lambda payload: payload.__setitem__("axis_values", {1: "value"}),
            "axis_values must be",
        ),
    ],
)
def test_run_bundle_rejects_coerced_or_unbound_identity_fields(
    temp_spec, tmp_path, monkeypatch, mutate, message
):
    repo_root = _init_git_repo(tmp_path / "repo")
    monkeypatch.setenv("EXASERVE_SNAPSHOT_DIR", str(tmp_path / "snapshots"))
    plan = materialize_run_bundles(
        str(temp_spec),
        backend_name="mock",
        experiments_root=str(tmp_path / "runs"),
        trace_root=str(tmp_path / "traces"),
        repo_root=str(repo_root),
    )[0]
    run_yaml = Path(plan.bundle.run_yaml_path)
    payload = yaml.safe_load(run_yaml.read_text(encoding="utf-8"))
    mutate(payload)
    run_yaml.write_text(yaml.safe_dump(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        load_run_plan(str(run_yaml))


def test_run_bundle_rejects_unmodelled_nested_fields_and_missing_spec_snapshot(
    temp_spec, tmp_path, monkeypatch
):
    repo_root = _init_git_repo(tmp_path / "repo")
    monkeypatch.setenv("EXASERVE_SNAPSHOT_DIR", str(tmp_path / "snapshots"))
    plan = materialize_run_bundles(
        str(temp_spec),
        backend_name="mock",
        experiments_root=str(tmp_path / "runs"),
        trace_root=str(tmp_path / "traces"),
        repo_root=str(repo_root),
    )[0]
    run_yaml = Path(plan.bundle.run_yaml_path)
    original = yaml.safe_load(run_yaml.read_text(encoding="utf-8"))
    mutations = (
        lambda payload: payload["bundle"].__setitem__("future_path", "/tmp/elsewhere"),
        lambda payload: payload["trace_artifact"].__setitem__("future_hash", "0" * 64),
        lambda payload: payload.pop("spec_path"),
    )
    for mutate in mutations:
        payload = copy.deepcopy(original)
        mutate(payload)
        run_yaml.write_text(yaml.safe_dump(payload), encoding="utf-8")
        with pytest.raises(ValueError, match="shape mismatch|missing"):
            load_run_plan(str(run_yaml))


def test_run_bundle_rejects_trace_metadata_with_a_different_identity(
    temp_spec, tmp_path, monkeypatch
):
    repo_root = _init_git_repo(tmp_path / "repo")
    monkeypatch.setenv("EXASERVE_SNAPSHOT_DIR", str(tmp_path / "snapshots"))
    plan = materialize_run_bundles(
        str(temp_spec),
        backend_name="mock",
        experiments_root=str(tmp_path / "runs"),
        trace_root=str(tmp_path / "traces"),
        repo_root=str(repo_root),
    )[0]
    metadata_path = Path(plan.trace_artifact.metadata_path)
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["trace_identity"]["version"] = 999
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    with pytest.raises(ValueError, match="identity does not derive"):
        load_run_plan(plan.bundle.run_yaml_path)


def test_materialize_twice_creates_incrementing_run_groups_and_reuses_snapshot(
    temp_spec, tmp_path, monkeypatch
):
    repo_root = _init_git_repo(tmp_path / "repo")
    monkeypatch.setenv("EXASERVE_SNAPSHOT_DIR", str(tmp_path / "snapshots"))

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
    first_state = json.loads(Path(first[0].bundle.state_path).read_text(encoding="utf-8"))
    second_state = json.loads(Path(second[0].bundle.state_path).read_text(encoding="utf-8"))
    assert (
        first_state["data"]["scheduler_run_identity"]
        != second_state["data"]["scheduler_run_identity"]
    )
    selected = resolve_run_group_dir(
        "test_spec", run_group="run1", experiments_root=str(tmp_path / "runs")
    )
    assert Path(selected).name == "run1"
    with pytest.raises(ValueError, match="explicit runN"):
        resolve_run_group_dir(
            "test_spec", run_group="latest", experiments_root=str(tmp_path / "runs")
        )


def test_materialization_rejects_variant_names_with_colliding_run_ids(
    temp_spec, tmp_path, monkeypatch
):
    from types import SimpleNamespace

    monkeypatch.setattr(
        "eval.lib.run_planner.expand_matrix",
        lambda _spec: [
            SimpleNamespace(variant_name="model/a"),
            SimpleNamespace(variant_name="model-a"),
        ],
    )

    with pytest.raises(ValueError, match="collide after filesystem-safe normalization"):
        materialize_run_bundles(
            str(temp_spec),
            backend_name="mock",
            experiments_root=str(tmp_path / "runs"),
            trace_root=str(tmp_path / "traces"),
            repo_root=str(tmp_path / "repo"),
        )


def test_run_bundle_loader_rejects_a_symlink_artifact(temp_spec, tmp_path, monkeypatch):
    repo_root = _init_git_repo(tmp_path / "repo")
    monkeypatch.setenv("EXASERVE_SNAPSHOT_DIR", str(tmp_path / "snapshots"))
    plan = materialize_run_bundles(
        str(temp_spec),
        backend_name="mock",
        experiments_root=str(tmp_path / "runs"),
        trace_root=str(tmp_path / "traces"),
        repo_root=str(repo_root),
    )[0]
    alias = tmp_path / "run.yaml"
    alias.symlink_to(plan.bundle.run_yaml_path)

    with pytest.raises(OSError):
        load_run_plan(str(alias))


def test_reused_snapshot_fails_closed_after_artifact_tampering(temp_spec, tmp_path, monkeypatch):
    repo_root = _init_git_repo(tmp_path / "repo")
    monkeypatch.setenv("EXASERVE_SNAPSHOT_DIR", str(tmp_path / "snapshots"))
    kwargs = dict(
        backend_name="mock",
        experiments_root=str(tmp_path / "runs"),
        trace_root=str(tmp_path / "traces"),
        repo_root=str(repo_root),
    )
    first = materialize_run_bundles(str(temp_spec), **kwargs)
    snapshot_file = Path(first[0].snapshot_root) / "tracked.txt"
    snapshot_file.write_text("tampered\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="artifact_manifest_hash mismatch"):
        materialize_run_bundles(str(temp_spec), **kwargs)


def test_dirty_repo_warning_and_snapshot_excludes_uncommitted_content(
    temp_spec, tmp_path, monkeypatch, capsys
):
    repo_root = _init_git_repo(tmp_path / "repo", tracked_contents="committed\n")
    monkeypatch.setenv("EXASERVE_SNAPSHOT_DIR", str(tmp_path / "snapshots"))
    (repo_root / "tracked.txt").write_text("dirty tracked\n", encoding="utf-8")
    (repo_root / "untracked.txt").write_text("untracked\n", encoding="utf-8")

    # PR-034: a dirty tree now REQUIRES explicit acknowledgement, else it
    # raises instead of silently snapshotting stale HEAD.
    with pytest.raises(RuntimeError, match="uncommitted"):
        materialize_run_bundles(
            str(temp_spec),
            backend_name="mock",
            experiments_root=str(tmp_path / "runs"),
            trace_root=str(tmp_path / "traces"),
            repo_root=str(repo_root),
            max_workers=1,
        )

    plans = materialize_run_bundles(
        str(temp_spec),
        backend_name="mock",
        experiments_root=str(tmp_path / "runs"),
        trace_root=str(tmp_path / "traces"),
        repo_root=str(repo_root),
        max_workers=1,
        allow_dirty=True,
    )
    captured = capsys.readouterr()
    assert "WARNING: repo has" in captured.err
    assert "tracked.txt" in captured.err
    assert "untracked.txt" in captured.err

    snapshot_root = Path(plans[0].snapshot_root)
    assert (snapshot_root / "tracked.txt").read_text(encoding="utf-8") == "committed\n"
    assert not (snapshot_root / "untracked.txt").exists()
    run_group_meta = json.loads(
        (Path(plans[0].bundle.group_root_dir) / "meta" / "run_group.json").read_text(
            encoding="utf-8"
        )
    )
    assert run_group_meta["git_dirty"] is True
    assert sorted(run_group_meta["dirty_files"]) == ["tracked.txt", "untracked.txt"]


def test_ray_adapter_always_uses_exaserve_env(temp_spec, tmp_path, monkeypatch):
    """Backend always uses env_aurora; litellm runs as a separate subprocess."""
    prompt_path = tmp_path / "prompts.json"
    spec_path = tmp_path / "ray_spec.yaml"
    repo_root = _init_git_repo(tmp_path / "repo")
    _write_prompt_dataset(prompt_path)
    _write_spec(spec_path, prompt_path, proxy_type="litellm")
    monkeypatch.setenv("EXASERVE_SNAPSHOT_DIR", str(tmp_path / "snapshots"))

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


def test_cli_validate_and_submit_all_exact_run_group_dry_run(temp_spec, tmp_path, monkeypatch):
    repo_root = _init_git_repo(tmp_path / "repo")
    monkeypatch.setenv("EXASERVE_SNAPSHOT_DIR", str(tmp_path / "snapshots"))
    run_kwargs = dict(
        backend_name="mock",
        experiments_root=str(tmp_path / "runs"),
        trace_root=str(tmp_path / "traces"),
        repo_root=str(repo_root),
    )
    materialize_run_bundles(str(temp_spec), **run_kwargs)
    materialize_run_bundles(str(temp_spec), **run_kwargs)

    env = os.environ.copy()
    # Same child-process source-layout contract as the root conftest.
    source_root = Path(__file__).resolve().parents[2]
    env["PYTHONPATH"] = os.pathsep.join(
        [str(source_root), str(source_root / "src")]
        + ([env["PYTHONPATH"]] if env.get("PYTHONPATH") else [])
    )

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
            "run1",
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


def test_plot_scripts_resolve_an_exact_run_group_and_result(tmp_path, monkeypatch):
    experiments_root = tmp_path / "experiments"
    result_path = (
        experiments_root
        / "runs"
        / "legacy"
        / "weak_scaling_tests"
        / "run0"
        / "1-nodes"
        / "results"
        / "result0.json"
    )
    _write_plot_result(result_path)

    env = os.environ.copy()
    # Child processes must receive the same source-layout contract as the
    # root conftest: repo root (eval package) plus src/ (exaserve package).
    repo_root = Path(__file__).resolve().parents[2]
    env["PYTHONPATH"] = os.pathsep.join(
        [str(repo_root), str(repo_root / "src")]
        + ([env["PYTHONPATH"]] if env.get("PYTHONPATH") else [])
    )
    env["EXASERVE_EXPERIMENTS_ROOT"] = str(experiments_root)
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
            "run0",
            "--indices",
            "0",
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
            "run0",
            "--indices",
            "0",
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
    # AC-TST-01: in-process scan, cwd-independent, no undeclared host binary
    # (previously shelled out to ripgrep, which is absent on some hosts).
    pattern = re.compile(r"from exp_configs import \*|sys\.path\.insert\(")
    repo_root = Path(__file__).resolve().parents[2]
    targets = [
        repo_root / "eval" / "cli.py",
        repo_root / "eval" / "replay_client.py",
    ]
    targets.extend(sorted((repo_root / "eval" / "lib").rglob("*.py")))
    offenders = []
    for path in targets:
        if not path.is_file():
            continue
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if pattern.search(line):
                offenders.append(f"{path.relative_to(repo_root)}:{lineno}: {line.strip()}")
    assert not offenders, "\n".join(offenders)


def test_matrix_derived_rejects_unsafe_expressions():
    # PR-016: spec expressions are configuration, not code. The classic
    # sandbox escapes and any attribute traversal outside math.* must raise.
    from eval.lib.matrix import _eval_derived

    assert _eval_derived("min(nodes * 4, 100)", {"nodes": 8}) == 32
    assert _eval_derived("math.ceil(nodes / 3)", {"nodes": 8}) == 3
    assert _eval_derived("4 if nodes > 4 else 1", {"nodes": 8}) == 4
    assert _eval_derived("1 < nodes <= 8 and 'debug' or 'prod'", {"nodes": 8}) == "debug"

    for evil in (
        "().__class__.__mro__[1].__subclasses__()",
        "__import__('os').system('true')",
        "min.__globals__",
        "math.__loader__",
        "[x for x in (1,)]",
        "(lambda: 1)()",
        "nodes.__class__",
        "open('/etc/passwd')",
    ):
        with pytest.raises(ValueError):
            _eval_derived(evil, {"nodes": 8})

    class HostileNumber:
        def __mul__(self, _other):
            raise AssertionError("configuration evaluator invoked user code")

    with pytest.raises(ValueError, match="unsupported value type"):
        _eval_derived("nodes * 2", {"nodes": HostileNumber()})
    with pytest.raises(ValueError, match="exponent"):
        _eval_derived("2 ** 1000", {"nodes": 8})


def test_matrix_derived_implementation_does_not_compile_or_eval_python():
    import inspect
    from eval.lib import matrix

    source = inspect.getsource(matrix)
    assert "compile(" not in source
    assert "eval(" not in source


def test_spec_validation_rejects_bad_enums_and_bounds(tmp_path, monkeypatch):
    # PR-020: enum + bound checks in validate_experiment_spec.
    from eval.lib.spec_io import validate_experiment_spec, load_experiment_spec

    prompt = tmp_path / "p.json"
    _write_prompt_dataset(prompt)
    spec_path = tmp_path / "s.yaml"
    _write_spec(spec_path, prompt)
    spec = load_experiment_spec(str(spec_path))
    validate_experiment_spec(spec)  # baseline valid

    import dataclasses

    bad_sched = dataclasses.replace(
        spec, scheduler=dataclasses.replace(spec.scheduler, type="cobalt")
    )
    with pytest.raises(ValueError, match="scheduler.type"):
        validate_experiment_spec(bad_sched)

    bad_engine = dataclasses.replace(
        spec, deployment=dataclasses.replace(spec.deployment, engine="tensorrt")
    )
    with pytest.raises(ValueError, match="engine"):
        validate_experiment_spec(bad_engine)

    bad_arrival = dataclasses.replace(
        spec, workload=dataclasses.replace(spec.workload, arrival="uniform")
    )
    with pytest.raises(ValueError, match="arrival"):
        validate_experiment_spec(bad_arrival)

    # Zero is the documented auto-sizing sentinel; negative values are invalid.
    validate_experiment_spec(
        dataclasses.replace(spec, client=dataclasses.replace(spec.client, go_concurrency=0))
    )
    bad_conc = dataclasses.replace(spec, client=dataclasses.replace(spec.client, go_concurrency=-1))
    with pytest.raises(ValueError, match="go_concurrency"):
        validate_experiment_spec(bad_conc)
