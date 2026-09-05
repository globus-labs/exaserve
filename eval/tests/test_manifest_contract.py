from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from eval.lib.manifest import (
    EvalManifest,
    ReplayClientConfig,
    TraceGeneratorConfig,
    load_eval_manifest,
)
from eval.lib.replay_engine import _verify_local_replay_filesystem, replay_from_manifest
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


def test_capsule_manifest_validation_uses_only_local_plan_copies(tmp_path, monkeypatch):
    manifest, _run_plan = _manifest(tmp_path)
    path = tmp_path / "runtime.yaml"
    manifest.save_yaml(str(path))
    local = tmp_path / "capsule" / "run"
    local.mkdir(parents=True)
    local_deployment = local / "deployment.plan.json"
    local_run = local / "run.plan.json"
    local_deployment.write_bytes(Path(manifest.deployment_plan_path).read_bytes())
    local_run.write_bytes(Path(manifest.run_plan_path).read_bytes())

    import eval.lib.manifest as manifest_module

    real_load_deployment = manifest_module.load_deployment_plan
    real_load_run = manifest_module.load_run_plan
    opened = []
    monkeypatch.setattr(
        manifest_module,
        "load_deployment_plan",
        lambda selected: opened.append(os.fspath(selected)) or real_load_deployment(selected),
    )
    monkeypatch.setattr(
        manifest_module,
        "load_run_plan",
        lambda selected: opened.append(os.fspath(selected)) or real_load_run(selected),
    )
    loaded = load_eval_manifest(
        str(path),
        verify_trace_artifact=False,
        deployment_plan_path_override=str(local_deployment),
        run_plan_path_override=str(local_run),
    )
    assert opened == [str(local_run), str(local_deployment)]
    assert loaded.run_plan.run_semantic_hash == manifest.run_semantic_hash
    assert loaded.run_plan_path == manifest.run_plan_path


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
def test_replay_initializes_mpi_before_root_validates_runtime_drift(
    tmp_path, monkeypatch, kwargs, message
):
    manifest, _run_plan = _manifest(tmp_path)
    path = tmp_path / "runtime.yaml"
    manifest.save_yaml(str(path))

    initialized = False

    def record_mpi():
        nonlocal initialized
        initialized = True
        return None, 0, 1

    monkeypatch.setattr("eval.lib.replay_engine._init_mpi", record_mpi)
    with pytest.raises(ValueError, match=message):
        asyncio.run(replay_from_manifest(str(path), **kwargs))
    assert initialized


def test_replay_rejects_mpi_world_that_disagrees_with_canonical_client_nodes(tmp_path, monkeypatch):
    manifest, _run_plan = _manifest(tmp_path)
    path = tmp_path / "runtime.yaml"
    manifest.save_yaml(str(path))

    class WrongWorld:
        def Get_size(self):
            return 2

        def bcast(self, value, *, root):
            assert root == 0
            return value

    monkeypatch.setattr("eval.lib.replay_engine._init_mpi", lambda: (WrongWorld(), 0, 2))
    with pytest.raises(RuntimeError, match="world size 2.*client.num_nodes 1"):
        asyncio.run(
            replay_from_manifest(
                str(path),
                base_urls_override="http://node0:8000",
            )
        )


def test_replay_rank_zero_cannot_read_trace_from_a_non_head_node(tmp_path, monkeypatch):
    for name, value in {
        "EXASERVE_LOCAL_RUNTIME_ROOT": str(tmp_path / "runtime"),
        "EXASERVE_LOCAL_STATE_ROOT": str(tmp_path / "state"),
        "EXASERVE_LOCAL_PLAN_PATH": str(tmp_path / "runtime/run/deployment.plan.json"),
        "EXASERVE_SITE_PROFILE_PATH": str(tmp_path / "runtime/run/site.profile.json"),
        "EXASERVE_ALLOCATION_BINDING_PATH": str(tmp_path / "runtime/run/allocation_binding.json"),
    }.items():
        monkeypatch.setenv(name, value)
    plan = SimpleNamespace(
        site_profile_id="site",
        site_profile_hash="a" * 64,
        deployment_plan_hash="b" * 64,
    )
    profile = SimpleNamespace(site_id="site", site_profile_hash="a" * 64)
    binding = SimpleNamespace(
        deployment_plan_hash="b" * 64,
        site_profile_hash="a" * 64,
        node_for=lambda rank: "allocation-head" if rank == 0 else "worker",
    )
    monkeypatch.setattr("exaserve.plan.io.load_deployment_plan", lambda _path: plan)
    monkeypatch.setattr("exaserve.plan.io.load_site_profile", lambda _path: profile)
    monkeypatch.setattr("exaserve.plan.io.load_allocation_binding", lambda _path: binding)
    monkeypatch.setattr("socket.gethostname", lambda: "wrong-worker")
    with pytest.raises(RuntimeError, match="rank 0.*AllocationBinding node"):
        _verify_local_replay_filesystem(rank=0)


def test_non_root_replay_never_opens_shared_manifest_trace_or_result(tmp_path, monkeypatch):
    runtime = tmp_path / "runtime"
    (runtime / "python").mkdir(parents=True)
    (runtime / "bin").mkdir()
    go_binary = runtime / "bin" / "go_dispatch"
    go_binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    go_binary.chmod(0o700)
    state = tmp_path / "state"
    state.mkdir()
    monkeypatch.setenv("EXASERVE_LOCAL_RUNTIME_ROOT", str(runtime))
    monkeypatch.setenv("EXASERVE_LOCAL_GO_DISPATCH", str(go_binary))
    monkeypatch.setenv("EXASERVE_LOCAL_STATE_ROOT", str(state))
    monkeypatch.setenv("EXASERVE_QUALIFIED_PYTHON", __import__("sys").executable)

    replay = ReplayClientConfig(
        config_path="/shared/runtime.yaml",
        dest="proxy",
        num_nodes=2,
    )
    context = {
        "schema_version": 1,
        "replay": replay,
        "topology": "local",
        "base_urls": ["http://head:8000"],
        "cluster_nodes": [],
        "saturation": dict(replay.saturation),
    }

    class Immediate:
        def test(self):
            return True, None

    class WorkerComm:
        def __init__(self):
            self.broadcasts = [
                {"context": context, "error": None},
                {"ok": True, "error": None, "total": 0, "last_timestamp": 0.0},
                1.0,
            ]

        def Get_size(self):
            return 2

        def Get_rank(self):
            return 1

        def bcast(self, value, *, root):
            assert value is None and root == 0
            return self.broadcasts.pop(0)

        def scatter(self, value, *, root):
            assert value is None and root == 0
            return None

        def Barrier(self):
            return None

        def isend(self, _value, *, dest, tag):
            assert dest == 0 and tag > 0
            return Immediate()

    comm = WorkerComm()
    monkeypatch.setattr("eval.lib.replay_engine._init_mpi", lambda: (comm, 1, 2))
    monkeypatch.setattr(
        "eval.lib.replay_engine._verify_local_replay_filesystem", lambda **_kwargs: None
    )
    monkeypatch.setattr(
        "eval.lib.replay_engine.load_eval_manifest",
        lambda *_a, **_k: (_ for _ in ()).throw(
            AssertionError("non-root loaded the shared manifest")
        ),
    )
    monkeypatch.setattr("eval.lib.replay_engine._spawn_go_procs", lambda *_a: ([], [], {}))
    monkeypatch.setattr("eval.lib.replay_engine._send_run_t0_and_wait", lambda *_a: ([], 0.0, 1.0))
    monkeypatch.setattr(
        "eval.lib.replay_engine._reduce_dispatch_end_via_mpi",
        lambda *_a, **_k: None,
    )

    import builtins

    real_open = builtins.open
    real_os_open = os.open
    real_stat = os.stat
    real_lstat = os.lstat
    real_listdir = os.listdir
    real_scandir = os.scandir
    real_access = os.access
    shared_ops = []

    def shared_path(path):
        if not isinstance(path, (str, os.PathLike)):
            return False
        value = os.fspath(path)
        return value in {"/shared", "/home", "/lus/flare"} or value.startswith(
            ("/shared/", "/home/", "/lus/flare/")
        )

    def guard(operation, function):
        def guarded(path, *args, **kwargs):
            if shared_path(path):
                shared_ops.append((operation, os.fspath(path)))
                raise AssertionError(f"non-root {operation} touched shared path {path}")
            return function(path, *args, **kwargs)

        return guarded

    def guarded_open(path, *args, **kwargs):
        if shared_path(path):
            shared_ops.append(("open", os.fspath(path)))
            raise AssertionError(f"non-root opened shared path {path}")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", guarded_open)
    monkeypatch.setattr(os, "open", guard("os.open", real_os_open))
    monkeypatch.setattr(os, "stat", guard("stat", real_stat))
    monkeypatch.setattr(os, "lstat", guard("lstat", real_lstat))
    monkeypatch.setattr(os, "listdir", guard("listdir", real_listdir))
    monkeypatch.setattr(os, "scandir", guard("scandir", real_scandir))
    monkeypatch.setattr(os, "access", guard("access", real_access))
    asyncio.run(replay_from_manifest("/shared/runtime.yaml", base_urls_override="ignored"))
    assert shared_ops == []
    assert comm.broadcasts == []


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
        ("generation_mode", "/home/user/mode", "deterministic or natural"),
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
