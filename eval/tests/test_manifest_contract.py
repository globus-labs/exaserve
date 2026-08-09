from __future__ import annotations

import asyncio
import hashlib
import json

import pytest
import yaml

from eval.lib.manifest import (
    EvalManifest,
    ReplayClientConfig,
    TraceGeneratorConfig,
    load_eval_manifest,
)
from eval.lib.replay_engine import replay_from_manifest
from exaserve.plan.compiler import compile_deployment_plan
from exaserve.plan.contracts import (
    SCHEMA_VERSION,
    ClientPolicy,
    RunPlan,
    SchedulerPlan,
    TracePolicy,
    WorkloadPolicy,
)
from exaserve.plan.io import write_deployment_plan, write_run_plan


def _manifest(tmp_path):
    trace_path = tmp_path / "trace.jsonl"
    trace_path.write_text('{"timestamp": 0}\n', encoding="utf-8")
    trace_hash = hashlib.sha256(trace_path.read_bytes()).hexdigest()
    plan = compile_deployment_plan(
        {
            "num_nodes": 1,
            "num_gpus_per_node": 12,
            "validation_mode": True,
            "exposure": {"mode": "DIRECT_VALIDATION"},
            "gateway": None,
            "request_mode": "chat",
            "models": [
                {
                    "model_id": "org/model",
                    "num_replicas": 1,
                    "max_model_len": 64,
                    "size": 1,
                }
            ],
        },
        deployment_id="manifest-test",
    )
    plan_path = tmp_path / "deployment.plan.json"
    write_deployment_plan(str(plan_path), plan)
    run_plan = RunPlan(
        schema_version=SCHEMA_VERSION,
        run_id="manifest-test/run",
        deployment=plan,
        scheduler=SchedulerPlan(type="pbs", nodes=1, queue="debug", walltime="00:10:00"),
        workload=WorkloadPolicy(
            kind="synthetic",
            duration_s=1,
            output_len=4,
            seed=1,
            modes=(("chat", 1),),
            client_nodes=1,
            client_dest="direct",
        ),
        trace=TracePolicy(kind="synthetic", trace_content_hash=trace_hash),
        client=ClientPolicy(destination="direct", nodes=1, workers=4),
    ).finalize()
    run_plan_path = tmp_path / "run.plan.json"
    write_run_plan(str(run_plan_path), run_plan)
    manifest = EvalManifest(
        pbs_result_dir=str(tmp_path / "results"),
        pbs_stdout_dir=str(tmp_path / "stdout"),
        pbs_stderr_dir=str(tmp_path / "stderr"),
        pbs_num_nodes=1,
        pbs_walltime="00:10:00",
        pbs_queue_name="debug",
        pbs_job_name="test",
        pbs_working_dir=str(tmp_path),
        job_trace_config=TraceGeneratorConfig(
            "", "", 1, "peak", 1, 4, str(trace_path), modes={"chat": 1}
        ),
        job_replay_client_config=ReplayClientConfig(
            config_path=str(tmp_path / "runtime.yaml"), dest="direct"
        ),
        job_seed=1,
        deployment_plan_path=str(plan_path),
        deployment_plan_hash=plan.deployment_plan_hash,
        run_plan_path=str(run_plan_path),
        run_semantic_hash=run_plan.run_semantic_hash,
        trace_content_hash=trace_hash,
    )
    return manifest, run_plan


def test_eval_manifest_round_trips_one_exact_deployment_plan(tmp_path):
    manifest, run_plan = _manifest(tmp_path)
    path = tmp_path / "runtime.yaml"
    manifest.save_yaml(str(path))
    loaded = load_eval_manifest(str(path))
    assert loaded.deployment_plan.deployment_plan_hash == run_plan.deployment.deployment_plan_hash
    assert loaded.run_plan.run_semantic_hash == run_plan.run_semantic_hash
    text = path.read_text(encoding="utf-8")
    assert "model_deployment_config" not in text and "proxy_config" not in text
    assert loaded.job_replay_client_config.num_go_procs == 1
    assert loaded.job_replay_client_config.num_go_workers == 4


def test_eval_manifest_rejects_duplicate_yaml_keys(tmp_path):
    manifest, _run_plan = _manifest(tmp_path)
    path = tmp_path / "runtime.yaml"
    manifest.save_yaml(str(path))
    path.write_text(path.read_text(encoding="utf-8") + "schema_version: 2\n", encoding="utf-8")
    with pytest.raises(Exception, match="duplicate key 'schema_version'"):
        load_eval_manifest(str(path))


def test_eval_manifest_refuses_plan_tampering(tmp_path):
    manifest, _plan = _manifest(tmp_path)
    payload = json.loads(open(manifest.deployment_plan_path, encoding="utf-8").read())
    payload["deployment_name"] = "tampered"
    open(manifest.deployment_plan_path, "w", encoding="utf-8").write(json.dumps(payload))
    with pytest.raises(ValueError, match="hash|artifact"):
        manifest.save_yaml(str(tmp_path / "runtime.yaml"))


def test_hash_valid_manifest_cannot_override_canonical_run_or_trace(tmp_path):
    manifest, _run_plan = _manifest(tmp_path)
    path = tmp_path / "runtime.yaml"
    manifest.save_yaml(str(path))
    _rewrite_manifest(
        path,
        lambda payload: payload["job_replay_client_config"].__setitem__("num_go_workers", 8),
    )
    with pytest.raises(ValueError, match="replay policy disagrees.*num_go_workers"):
        load_eval_manifest(str(path))

    second_path = tmp_path / "runtime-second.yaml"
    manifest.save_yaml(str(second_path))
    (tmp_path / "trace.jsonl").write_text('{"timestamp": 1}\n', encoding="utf-8")
    with pytest.raises(ValueError, match="trace artifact content hash mismatch"):
        load_eval_manifest(str(second_path))


def test_eval_manifest_is_create_once_and_loader_rejects_symlinks(tmp_path):
    manifest, _run_plan = _manifest(tmp_path)
    path = tmp_path / "runtime.yaml"
    manifest.save_yaml(str(path))
    with pytest.raises(FileExistsError):
        manifest.save_yaml(str(path))

    alias = tmp_path / "runtime-alias.yaml"
    alias.symlink_to(path)
    with pytest.raises(OSError):
        load_eval_manifest(str(alias))


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"dest_override": "proxy"}, "override dest"),
        ({"base_urls_override": "http://node0:9999"}, "canonical port"),
    ],
)
def test_replay_rejects_runtime_drift_before_mpi_init(tmp_path, monkeypatch, kwargs, message):
    manifest, _run_plan = _manifest(tmp_path)
    path = tmp_path / "runtime.yaml"
    manifest.save_yaml(str(path))

    def forbidden_mpi():
        raise AssertionError("MPI initialized before immutable replay validation")

    monkeypatch.setattr("eval.lib.replay_engine._init_mpi", forbidden_mpi)
    with pytest.raises(ValueError, match=message):
        asyncio.run(replay_from_manifest(str(path), **kwargs))


def _rewrite_manifest(path, change):
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    change(payload)
    canonical_payload = {
        key: value
        for key, value in payload.items()
        if key not in {"schema_version", "eval_manifest_hash"}
    }
    canonical = json.dumps(
        canonical_payload, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    payload["eval_manifest_hash"] = hashlib.sha256(canonical.encode()).hexdigest()
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("num_go_procs", 0, "positive integer"),
        ("num_runs", 0, "positive integer"),
        ("go_concurrency", -1, "non-negative integer"),
        ("early_stop", 1.1, "between 0.0 and 1.0"),
        ("dest", "somewhere", "proxy.*direct"),
        ("direct_dispatch", "ambient", "local, mesh, or paired"),
        ("request_timeout_s", 0, "positive"),
        ("direct_target_max_workers", 0, "positive integer"),
        ("dispatch_topologies", ["local", "local"], "duplicate-free"),
    ],
)
def test_hash_valid_manifest_still_rejects_invalid_replay_semantics(
    tmp_path, field, value, message
):
    manifest, _plan = _manifest(tmp_path)
    path = tmp_path / "runtime.yaml"
    manifest.save_yaml(str(path))
    _rewrite_manifest(
        path,
        lambda payload: payload["job_replay_client_config"].__setitem__(field, value),
    )
    with pytest.raises(ValueError, match=message):
        load_eval_manifest(str(path))


def test_hash_valid_manifest_rejects_multi_process_saturation(tmp_path):
    manifest, _plan = _manifest(tmp_path)
    path = tmp_path / "runtime.yaml"
    manifest.save_yaml(str(path))

    def make_invalid(payload):
        replay = payload["job_replay_client_config"]
        replay["num_go_procs"] = 2
        replay["saturation"]["enabled"] = True

    _rewrite_manifest(path, make_invalid)
    with pytest.raises(ValueError, match="ClientLab"):
        load_eval_manifest(str(path))


def test_hash_valid_manifest_rejects_distributed_or_repeated_saturation(tmp_path):
    manifest, _plan = _manifest(tmp_path)
    path = tmp_path / "runtime.yaml"
    manifest.save_yaml(str(path))

    def make_invalid(payload):
        replay = payload["job_replay_client_config"]
        replay["num_runs"] = 2
        replay["saturation"]["enabled"] = True

    _rewrite_manifest(path, make_invalid)
    with pytest.raises(ValueError, match="num_nodes=1 and num_runs=1"):
        load_eval_manifest(str(path))


def test_hash_valid_manifest_rejects_proxy_topology_arms(tmp_path):
    manifest, _plan = _manifest(tmp_path)
    path = tmp_path / "runtime.yaml"
    manifest.save_yaml(str(path))

    def make_invalid(payload):
        replay = payload["job_replay_client_config"]
        replay["dest"] = "proxy"
        replay["dispatch_topologies"] = ["mesh"]

    _rewrite_manifest(path, make_invalid)
    with pytest.raises(ValueError, match="requires replay.dest='direct'"):
        load_eval_manifest(str(path))


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("duration", 0, "positive"),
        ("speedup", -1, "positive"),
        ("output_len", 0, "integer"),
        ("output_trace_path", "", "non-empty"),
        ("modes", {"chat": 0}, "one positive"),
    ],
)
def test_hash_valid_manifest_still_rejects_invalid_trace_semantics(tmp_path, field, value, message):
    manifest, _plan = _manifest(tmp_path)
    path = tmp_path / "runtime.yaml"
    manifest.save_yaml(str(path))

    def make_invalid(payload):
        payload["job_trace_config"][field] = value

    _rewrite_manifest(path, make_invalid)
    with pytest.raises(ValueError, match=message):
        load_eval_manifest(str(path))


def test_hash_valid_manifest_rejects_unknown_saturation_fields(tmp_path):
    manifest, _plan = _manifest(tmp_path)
    path = tmp_path / "runtime.yaml"
    manifest.save_yaml(str(path))
    _rewrite_manifest(
        path,
        lambda payload: payload["job_replay_client_config"]["saturation"].__setitem__(
            "typo_rate", 10
        ),
    )
    with pytest.raises(ValueError, match="unknown fields.*typo_rate"):
        load_eval_manifest(str(path))


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (
            lambda payload: payload.__setitem__("deployment_plan_path", "relative.json"),
            "deployment_plan_path must be absolute",
        ),
        (
            lambda payload: payload.__setitem__("run_plan_path", "relative.json"),
            "run_plan_path must be absolute",
        ),
        (
            lambda payload: payload.__setitem__("pbs_result_dir", "results"),
            "pbs_result_dir must be absolute",
        ),
        (
            lambda payload: payload["job_trace_config"].__setitem__(
                "output_trace_path", "trace.jsonl"
            ),
            "output_trace_path must be absolute",
        ),
        (
            lambda payload: payload["job_replay_client_config"].__setitem__(
                "config_path", "runtime.yaml"
            ),
            "config_path must be absolute",
        ),
    ],
)
def test_hash_valid_manifest_rejects_relative_runtime_paths(tmp_path, change, message):
    manifest, _plan = _manifest(tmp_path)
    path = tmp_path / "runtime.yaml"
    manifest.save_yaml(str(path))
    _rewrite_manifest(path, change)
    with pytest.raises(ValueError, match=message):
        load_eval_manifest(str(path))
