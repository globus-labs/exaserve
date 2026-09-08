from __future__ import annotations

import hashlib
import json
import os
import struct
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from eval.lib.backends.base import BackendProcessHandle, terminate_process_tree
from eval.lib.run_executor import (
    _replay_process_timeout_s,
    _run_command_with_tee,
    _run_replay_client,
    _validate_replay_hostfile_binding,
    _validate_replay_results,
)
from eval.lib.replay_engine import (
    _LAST_GATHER_META,
    _MPI_FRAME_BYTES,
    _MPI_HEADER_BYTES,
    _RESULT_CHUNK_BYTES,
    TraceRequest,
    _apply_direct_topology,
    _decode_gather_payload,
    _decode_mpi_message,
    _distribute_trace_requests,
    _encode_gather_payload,
    _encode_mpi_message,
    _gather_raw_results_via_mpi,
    _init_mpi,
    _load_trace_requests,
    _merge_go_process_summaries,
    _mpi_irecv_message,
    _mpi_isend_message,
    _next_result_path,
    _port_from_manifest,
    _read_go_results,
    _reduce_summary_via_mpi,
    _reduce_dispatch_end_via_mpi,
    _send_run_t0_and_wait,
    _stop_replay_process,
    _validate_base_urls,
    _wait_request,
)


def _run_plan(
    *,
    runs: int = 2,
    duration: float = 5.0,
    warmup: float = 3.0,
    request_timeout: float = 10.0,
    shard_timeout: float = 5.0,
):
    return SimpleNamespace(
        client=SimpleNamespace(
            num_runs=runs,
            warmup_duration_s=warmup,
            request_timeout_s=request_timeout,
            shard_timeout_s=shard_timeout,
        ),
        workload=SimpleNamespace(duration=duration),
    )


def _multi_node_replay_plan(tmp_path):
    return SimpleNamespace(
        client=SimpleNamespace(
            num_nodes=2,
            num_runs=1,
            warmup_duration_s=0.0,
            request_timeout_s=1.0,
            shard_timeout_s=1.0,
        ),
        workload=SimpleNamespace(duration=0.0),
        scheduler=SimpleNamespace(type="pbs", nodes=2),
        bundle=SimpleNamespace(logs_dir=str(tmp_path)),
        repo_root=str(tmp_path),
        runtime_manifest_path=str(tmp_path / "runtime.json"),
        deployment_plan_hash="1" * 64,
        semantic_plan=SimpleNamespace(
            deployment=SimpleNamespace(
                deployment_id="deployment",
                site_profile_hash="2" * 64,
                compatibility_profile_hash="3" * 64,
                manifest_hash="4" * 64,
                vendor="xpu",
                engine="vllm",
                runtime=SimpleNamespace(null_compute=True),
            )
        ),
    )


def _set_local_replay_env(tmp_path, monkeypatch):
    runtime = tmp_path / "runtime"
    (runtime / "python").mkdir(parents=True)
    (runtime / "bin").mkdir()
    go_binary = runtime / "bin" / "go_dispatch"
    go_binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    go_binary.chmod(0o700)
    run_root = runtime / "run"
    run_root.mkdir()
    eval_manifest = run_root / "eval_manifest.yaml"
    run_plan = run_root / "run.plan.json"
    deployment_plan = run_root / "deployment.plan.json"
    for path in (eval_manifest, run_plan, deployment_plan):
        path.write_text("{}\n", encoding="utf-8")
    state = tmp_path / "state"
    state.mkdir()
    monkeypatch.setenv("EXASERVE_LOCAL_RUNTIME_ROOT", str(runtime))
    monkeypatch.setenv("EXASERVE_LOCAL_GO_DISPATCH", str(go_binary))
    monkeypatch.setenv("EXASERVE_LOCAL_STATE_ROOT", str(state))
    monkeypatch.setenv("EXASERVE_LOCAL_EVAL_MANIFEST", str(eval_manifest))
    monkeypatch.setenv("EXASERVE_LOCAL_RUN_PLAN_PATH", str(run_plan))
    monkeypatch.setenv("EXASERVE_LOCAL_PLAN_PATH", str(deployment_plan))
    monkeypatch.setenv("EXASERVE_QUALIFIED_PYTHON", sys.executable)
    monkeypatch.setenv("EXASERVE_QUALIFIED_PYTHON_SHA256", "1" * 64)
    monkeypatch.setenv("EXASERVE_QUALIFIED_PYTHON_SITE_PROFILE_HASH", "2" * 64)
    monkeypatch.setenv("EXASERVE_COMPAT_SOURCES_NODE_PROFILE", "3" * 64)
    monkeypatch.setenv("EXASERVE_COMPAT_SOURCES_NODE_MANIFEST", "4" * 64)
    monkeypatch.setattr(
        "eval.lib.run_executor._closed_replay_worker_environment",
        lambda _plan, environment: dict(
            environment,
            PYTHONPATH=str(runtime / "python"),
            PYTHONNOUSERSITE="1",
            HOME=str(state / "home"),
            TMPDIR=str(state / "tmp"),
            XDG_CACHE_HOME=str(state / "cache"),
            HF_HOME=str(state / "cache" / "huggingface"),
        ),
    )
    monkeypatch.setattr(
        "eval.lib.run_executor._validate_replay_hostfile_binding",
        lambda *_a, **_k: None,
    )


def test_replay_hostfile_rank_order_is_bound_to_allocation(monkeypatch, tmp_path):
    hostfile = tmp_path / "hosts"
    hostfile.write_text("worker\nhead\n", encoding="utf-8")
    binding = SimpleNamespace(
        deployment_plan_hash="1" * 64,
        site_profile_hash="2" * 64,
        node_for=lambda rank: ("head", "worker")[rank],
    )
    monkeypatch.setattr("exaserve.plan.io.load_allocation_binding", lambda _path: binding)
    with pytest.raises(RuntimeError, match="rank order disagrees"):
        _validate_replay_hostfile_binding(
            str(hostfile),
            binding_path=str(tmp_path / "allocation_binding.json"),
            run_plan=_multi_node_replay_plan(tmp_path),
        )


def test_replay_hostfile_cleanup_failure_is_fatal_after_success(tmp_path, monkeypatch):
    _set_local_replay_env(tmp_path, monkeypatch)
    hostfile = tmp_path / "hosts"
    hostfile.write_text("node1\nnode2\n", encoding="utf-8")
    monkeypatch.setattr("eval.lib.run_executor._build_hostfile", lambda _count: str(hostfile))
    monkeypatch.setattr("eval.lib.run_executor._run_command_with_tee", lambda *_a, **_k: 0)
    monkeypatch.setattr(
        "eval.lib.run_executor.os.remove",
        lambda _path: (_ for _ in ()).throw(OSError("cleanup denied")),
    )

    with pytest.raises(RuntimeError, match="temporary replay hostfile"):
        _run_replay_client(_multi_node_replay_plan(tmp_path), ["http://service"])


def test_replay_hostfile_cleanup_preserves_the_primary_failure(tmp_path, monkeypatch):
    _set_local_replay_env(tmp_path, monkeypatch)
    hostfile = tmp_path / "hosts"
    hostfile.write_text("node1\nnode2\n", encoding="utf-8")
    monkeypatch.setattr("eval.lib.run_executor._build_hostfile", lambda _count: str(hostfile))

    def fail_run(*_args, **_kwargs):
        raise ValueError("replay failed")

    monkeypatch.setattr("eval.lib.run_executor._run_command_with_tee", fail_run)
    monkeypatch.setattr(
        "eval.lib.run_executor.os.remove",
        lambda _path: (_ for _ in ()).throw(OSError("cleanup denied")),
    )

    with pytest.raises(ValueError, match="replay failed") as caught:
        _run_replay_client(_multi_node_replay_plan(tmp_path), ["http://service"])
    assert any("hostfile cleanup also failed" in note for note in caught.value.__notes__)


def test_multi_rank_replay_recovers_local_capsule_from_head_receipt(tmp_path, monkeypatch):
    runtime = tmp_path / "runtime"
    (runtime / "python").mkdir(parents=True)
    (runtime / "bin").mkdir()
    (runtime / "run").mkdir()
    go_binary = runtime / "bin" / "go_dispatch"
    go_binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    go_binary.chmod(0o700)
    state = tmp_path / "state"
    state.mkdir()
    for name in (
        "EXASERVE_LOCAL_RUNTIME_ROOT",
        "EXASERVE_LOCAL_GO_DISPATCH",
        "EXASERVE_LOCAL_STATE_ROOT",
        "EXASERVE_LOCAL_EVAL_MANIFEST",
        "EXASERVE_LOCAL_RUN_PLAN_PATH",
        "EXASERVE_LOCAL_PLAN_PATH",
        "EXASERVE_QUALIFIED_PYTHON",
        "EXASERVE_QUALIFIED_PYTHON_SHA256",
        "EXASERVE_QUALIFIED_PYTHON_SITE_PROFILE_HASH",
        "EXASERVE_COMPAT_SOURCES_NODE_PROFILE",
        "EXASERVE_COMPAT_SOURCES_NODE_MANIFEST",
    ):
        monkeypatch.delenv(name, raising=False)
    capsule_env = {
        "EXASERVE_LOCAL_RUNTIME_ROOT": str(runtime),
        "EXASERVE_LOCAL_GO_DISPATCH": str(go_binary),
        "EXASERVE_LOCAL_STATE_ROOT": str(state),
        "EXASERVE_LOCAL_EVAL_MANIFEST": str(runtime / "run" / "eval_manifest.yaml"),
        "EXASERVE_LOCAL_RUN_PLAN_PATH": str(runtime / "run" / "run.plan.json"),
        "EXASERVE_LOCAL_PLAN_PATH": str(runtime / "run" / "deployment.plan.json"),
        "EXASERVE_QUALIFIED_PYTHON": sys.executable,
        "EXASERVE_QUALIFIED_PYTHON_SHA256": "1" * 64,
        "EXASERVE_QUALIFIED_PYTHON_SITE_PROFILE_HASH": "2" * 64,
        "EXASERVE_COMPAT_SOURCES_NODE_PROFILE": "3" * 64,
        "EXASERVE_COMPAT_SOURCES_NODE_MANIFEST": "4" * 64,
    }
    calls = []
    monkeypatch.setattr(
        "eval.lib.run_executor._validated_replay_capsule_environment",
        lambda *args: calls.append(args) or tuple(capsule_env.items()),
    )
    monkeypatch.setattr(
        "eval.lib.run_executor._closed_replay_worker_environment",
        lambda _plan, environment: dict(
            environment,
            PYTHONPATH=str(runtime / "python"),
            PYTHONNOUSERSITE="1",
            HOME=str(state / "home"),
            TMPDIR=str(state / "tmp"),
            XDG_CACHE_HOME=str(state / "cache"),
            HF_HOME=str(state / "cache" / "huggingface"),
        ),
    )
    hostfile = tmp_path / "hosts"
    hostfile.write_text("node1\nnode2\n", encoding="utf-8")
    monkeypatch.setattr("eval.lib.run_executor._build_hostfile", lambda _count: str(hostfile))
    monkeypatch.setattr(
        "eval.lib.run_executor._validate_replay_hostfile_binding",
        lambda *_a, **_k: None,
    )
    launched = {}

    def capture(command, **kwargs):
        launched.update(command=command, **kwargs)
        return 0

    monkeypatch.setattr("eval.lib.run_executor._run_command_with_tee", capture)
    plan = _multi_node_replay_plan(tmp_path)
    assert (
        _run_replay_client(
            plan,
            ["http://service"],
            runtime_capsule_manifest_path="/shared/source_staging_manifest.json",
            runtime_generation=7,
        )
        == 0
    )
    assert len(calls) == 1
    assert launched["cwd"] == str(runtime / "python")
    assert launched["env"]["PYTHONPATH"] == str(runtime / "python")
    assert launched["env"]["PYTHONNOUSERSITE"] == "1"
    assert os.path.realpath(sys.executable) in launched["command"]
    config_index = launched["command"].index("--config") + 1
    assert launched["command"][config_index] == capsule_env["EXASERVE_LOCAL_EVAL_MANIFEST"]
    assert plan.runtime_manifest_path not in launched["command"]
    assert "--genvnone" in launched["command"]
    assert "--envnone" in launched["command"]
    assert "--shared" in launched["command"]
    assert launched["command"][launched["command"].index("--wdir") + 1] == str(runtime / "python")
    names = launched["command"][launched["command"].index("--envlist") + 1].split(",")
    assert set(capsule_env) | {"PYTHONPATH", "PYTHONNOUSERSITE"} <= set(names)
    shared_values = {
        name: launched["env"][name]
        for name in names
        if launched["env"][name].startswith(("/home/", "/lus/flare/"))
    }
    assert not shared_values, shared_values


def test_multi_rank_slurm_replay_exports_only_the_closed_environment(tmp_path, monkeypatch):
    _set_local_replay_env(tmp_path, monkeypatch)
    hostfile = tmp_path / "hosts"
    hostfile.write_text("node1\nnode2\n", encoding="utf-8")
    monkeypatch.setattr("eval.lib.run_executor._build_hostfile", lambda _count: str(hostfile))
    launched = {}
    monkeypatch.setattr(
        "eval.lib.run_executor._run_command_with_tee",
        lambda command, **kwargs: launched.update(command=command, **kwargs) or 0,
    )
    plan = _multi_node_replay_plan(tmp_path)
    plan.scheduler.type = "slurm"
    assert _run_replay_client(plan, ["http://service"]) == 0
    export = next(item for item in launched["command"] if item.startswith("--export="))
    assert export.startswith("--export=NONE,")
    assert "ALL" not in export.split("=", 1)[1].split(",")


def test_replay_outer_deadline_comes_only_from_the_plan(monkeypatch):
    monkeypatch.setenv("EXASERVE_REPLAY_TIMEOUT_S", "99999")
    monkeypatch.setenv("EXASERVE_REPLAY_SHARD_TIMEOUT_S", "99999")
    monkeypatch.setenv("EXASERVE_REPLAY_PROCESS_TIMEOUT_S", "12.5")
    assert _replay_process_timeout_s(_run_plan()) == 300.0
    assert _replay_process_timeout_s(_run_plan(request_timeout=1000.0)) == 2218.0


def test_replay_process_group_is_killed_at_deadline(tmp_path):
    log_path = tmp_path / "replay.log"
    with pytest.raises(TimeoutError, match="exceeded"):
        _run_command_with_tee(
            [sys.executable, "-c", "import time; print('started', flush=True); time.sleep(60)"],
            log_path=str(log_path),
            cwd=str(tmp_path),
            env=os.environ.copy(),
            timeout_s=0.1,
        )
    assert "started" in log_path.read_text(encoding="utf-8")


def test_replay_process_group_is_killed_when_backend_exits(tmp_path):
    backend = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(0.2)"],
        start_new_session=True,
    )
    try:
        with pytest.raises(RuntimeError, match="deployment backend exited with code 0"):
            _run_command_with_tee(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                log_path=str(tmp_path / "backend-exit.log"),
                cwd=str(tmp_path),
                env=os.environ.copy(),
                timeout_s=30.0,
                abort_check=(
                    lambda: (
                        None
                        if backend.poll() is None
                        else f"deployment backend exited with code {backend.returncode}"
                    )
                ),
            )
    finally:
        if backend.poll() is None:
            backend.terminate()
        backend.wait(timeout=5)


def test_replay_clean_launcher_exit_cannot_leave_descendants(tmp_path):
    child_path = tmp_path / "escaped.pid"
    program = (
        "import subprocess,sys; "
        "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); "
        "open(sys.argv[1],'w').write(str(p.pid))"
    )
    with pytest.raises(RuntimeError, match="left descendants"):
        _run_command_with_tee(
            [sys.executable, "-c", program, str(child_path)],
            log_path=str(tmp_path / "escaped.log"),
            cwd=str(tmp_path),
            env=os.environ.copy(),
            timeout_s=5.0,
        )
    child_pid = int(child_path.read_text(encoding="utf-8"))
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and Path(f"/proc/{child_pid}").exists():
        time.sleep(0.02)
    if Path(f"/proc/{child_pid}/stat").exists():
        assert Path(f"/proc/{child_pid}/stat").read_text().split()[2] == "Z"


def test_backend_log_reader_failure_is_not_silent(tmp_path):
    blocked_parent = tmp_path / "not-a-directory"
    blocked_parent.write_text("file", encoding="utf-8")
    process = subprocess.Popen(
        [sys.executable, "-c", "import time; print('line', flush=True); time.sleep(2)"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    handle = BackendProcessHandle(
        process=process,
        log_path=str(blocked_parent / "service.log"),
        status_dir=str(tmp_path / "status"),
    ).start()
    try:
        with pytest.raises(RuntimeError, match="diagnostic reader failed"):
            handle.wait_for_ready(1.0)
    finally:
        if process.poll() is None:
            process.terminate()
        process.wait(timeout=5)
    with pytest.raises(RuntimeError, match="diagnostic reader failed"):
        handle.close()


def test_backend_cleanup_reaps_descendant_after_launcher_exits(tmp_path):
    pid_path = tmp_path / "backend-child.pid"
    program = (
        "import subprocess,sys; "
        "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); "
        "open(sys.argv[1],'w').write(str(p.pid))"
    )
    process = subprocess.Popen(
        [sys.executable, "-c", program, str(pid_path)], start_new_session=True
    )
    process_group = process.pid
    process.wait(timeout=5)
    child_pid = int(pid_path.read_text(encoding="utf-8"))
    terminate_process_tree(process, process_group=process_group, deadline_s=2.0)
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and Path(f"/proc/{child_pid}").exists():
        time.sleep(0.02)
    if Path(f"/proc/{child_pid}/stat").exists():
        assert Path(f"/proc/{child_pid}/stat").read_text().split()[2] == "Z"


def test_backend_cleanup_reaps_an_exited_group_leader_during_grace():
    process = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        start_new_session=True,
    )
    started = time.monotonic()

    terminate_process_tree(
        process,
        process_group=process.pid,
        deadline_s=1.0,
        graceful_s=0.8,
    )

    assert process.returncode is not None
    assert time.monotonic() - started < 0.7


def _captured_process(program: str):
    stdout_capture = tempfile.TemporaryFile(mode="w+b")
    stderr_capture = tempfile.TemporaryFile(mode="w+b")
    process = subprocess.Popen(
        [sys.executable, "-c", program],
        stdin=subprocess.PIPE,
        stdout=stdout_capture,
        stderr=stderr_capture,
        start_new_session=True,
    )
    return {
        "index": 0,
        "process": process,
        "stdout": stdout_capture,
        "stderr": stderr_capture,
    }


def test_go_dispatch_capture_cannot_deadlock_on_more_than_a_pipe_buffer(tmp_path, capsys):
    result_path = tmp_path / "result.jsonl"
    result_path.write_text(
        '{"__type__":"summary","requests_completed":0,"requests_scheduled":0,'
        '"errors":0,"p50_s":0.0,"p99_s":0.0,"total_input_tokens":0,'
        '"latency_quantile_method":"mergeable_histogram_estimate_2pct_through_7200s",'
        '"latency_histogram":{"bucket_upper_bounds_s":[0.1,1.0,-1.0],'
        '"counts":[0,0,0],"count":0,"sum_s":0.0},'
        '"total_output_tokens":0,"last_fire_time":0.0,"last_request_start_at":0.0,'
        '"last_body_done_at":0.0,"adjusted_run_t0":1.0}\n',
        encoding="utf-8",
    )
    entry = _captured_process(
        "import sys; sys.stdin.readline(); sys.stderr.write('x' * 131072); sys.stderr.flush()"
    )
    parsed, _last_fire, _run_t0 = _send_run_t0_and_wait(
        [entry], 1.0, [str(result_path)], {}, None, 0, True
    )
    assert parsed["requests_completed"] == 0
    assert len(capsys.readouterr().out) >= 131072
    assert entry["process"].poll() == 0
    assert entry["stdout"].closed
    assert entry["stderr"].closed


def _go_summary_row() -> dict:
    return {
        "__type__": "summary",
        "requests_completed": 0,
        "requests_scheduled": 0,
        "errors": 0,
        "p50_s": 0.0,
        "p99_s": 0.0,
        "latency_quantile_method": "mergeable_histogram_estimate_2pct_through_7200s",
        "latency_histogram": {
            "bucket_upper_bounds_s": [0.1, 1.0, -1.0],
            "counts": [0, 0, 0],
            "count": 0,
            "sum_s": 0.0,
        },
        "total_input_tokens": 0,
        "total_output_tokens": 0,
        "last_fire_time": 0.0,
        "last_request_start_at": 0.0,
        "last_body_done_at": 0.0,
        "adjusted_run_t0": 1.0,
    }


def test_go_result_summary_is_the_only_terminal_row(tmp_path):
    result_path = tmp_path / "result.jsonl"
    summary = _go_summary_row()
    result_path.write_text(
        json.dumps(summary) + "\n" + json.dumps(summary) + "\n", encoding="utf-8"
    )

    with pytest.raises(ValueError, match="after terminal summary"):
        _read_go_results(str(result_path), {})


def test_go_result_rejects_trailing_data_after_dispatch_done(tmp_path):
    result_path = tmp_path / "result.jsonl"
    dispatch = {
        "__type__": "dispatch_done",
        "last_fire_time": 0.0,
        "last_request_start_at": 0.0,
        "last_body_done_at": 0.0,
        "adjusted_run_t0": 1.0,
    }
    result_path.write_text(
        json.dumps(dispatch) + "\n" + json.dumps({"unexpected": True}) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="after terminal dispatch_done"):
        _read_go_results(str(result_path), {})


def test_go_result_requires_a_terminal_row(tmp_path):
    result_path = tmp_path / "result.jsonl"
    result_path.write_text("\n", encoding="utf-8")

    with pytest.raises(ValueError, match="missing its terminal row"):
        _read_go_results(str(result_path), {})


def test_go_dispatch_deadline_stops_and_reaps_process_group(tmp_path):
    result_path = tmp_path / "result.jsonl"
    entry = _captured_process("import sys,time; sys.stdin.readline(); time.sleep(60)")
    with pytest.raises(RuntimeError, match="exceeded its"):
        _send_run_t0_and_wait(
            [entry], 1.0, [str(result_path)], {}, None, 2, drain_wait_timeout_s=0.05
        )
    assert entry["process"].poll() is not None
    assert entry["stdout"].closed
    assert entry["stderr"].closed


def test_go_dispatch_rejects_invalid_drain_deadline():
    for value in (True, 0, -1, float("nan"), float("inf")):
        with pytest.raises(ValueError, match="drain deadline"):
            _send_run_t0_and_wait([], 1.0, [], {}, None, 0, drain_wait_timeout_s=value)


def test_go_dispatch_cleanup_uses_one_deadline_for_every_worker(tmp_path, monkeypatch):
    result_paths = []
    entries = []
    for index in range(2):
        result_path = tmp_path / f"result-{index}.jsonl"
        result_path.write_text(
            json.dumps(
                {
                    "__type__": "dispatch_done",
                    "last_fire_time": 0.0,
                    "last_request_start_at": 0.0,
                    "last_body_done_at": 0.0,
                    "adjusted_run_t0": 1.0,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        result_paths.append(str(result_path))
        entry = _captured_process("import sys; sys.stdin.readline()")
        entry["index"] = index
        entries.append(entry)

    deadlines = []
    real_stop = _stop_replay_process

    def recording_stop(process, *, label, grace_s=5.0, deadline=None):
        deadlines.append(deadline)
        return real_stop(process, label=label, grace_s=grace_s, deadline=deadline)

    monkeypatch.setattr("eval.lib.replay_engine._stop_replay_process", recording_stop)
    _send_run_t0_and_wait(entries, 1.0, result_paths, {}, None, 0)

    assert len(deadlines) >= 4
    assert deadlines[0] is not None
    assert len(set(deadlines)) == 1


def test_replay_stop_reaps_descendant_after_leader_exits(tmp_path):
    pid_path = tmp_path / "child.pid"
    program = (
        "import subprocess,sys; "
        "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); "
        "open(sys.argv[1],'w').write(str(p.pid))"
    )
    process = subprocess.Popen(
        [sys.executable, "-c", program, str(pid_path)], start_new_session=True
    )
    process.wait(timeout=5)
    child_pid = int(pid_path.read_text(encoding="utf-8"))
    _stop_replay_process(process, label="test replay process", grace_s=0.05)
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and Path(f"/proc/{child_pid}").exists():
        time.sleep(0.02)
    if Path(f"/proc/{child_pid}/stat").exists():
        assert Path(f"/proc/{child_pid}/stat").read_text().split()[2] == "Z"


def test_result_mpi_payload_round_trip_preserves_typed_records():
    request = TraceRequest(1.5, "model", "prompt", 2, 3, 1, "request-1", "chat")
    record = (request, 0.5, True, "", 2.0, 2, 3, 0.1, 1.6, 0.01, 0.02, 0.03, 3)

    decoded = _decode_gather_payload(_encode_gather_payload([record]))

    assert len(decoded) == 1
    assert decoded[0][0].req_id == "request-1"
    assert decoded[0][1:] == record[1:]


def test_result_mpi_payload_decoder_rejects_pickle_and_unknown_shapes():
    import pickle

    with pytest.raises((UnicodeError, ValueError)):
        _decode_gather_payload(pickle.dumps({"attacker": "controlled"}))
    with pytest.raises(ValueError, match="invalid shape"):
        _decode_gather_payload(b'{"schema_version":1,"kind":"summary","summary":{},"extra":true}')


class _MessageRequest:
    """Model real MPI typed-buffer completion, including its capacity bound."""

    def __init__(self, *, buffer=None, frame=None, never=False, polls=0, count=None):
        self.buffer = buffer
        self.frame = frame
        self.never = never
        self.polls = polls
        self.count = count
        self.cancelled = False

    def Test(self, status=None):
        if self.never:
            return False
        if self.polls:
            self.polls -= 1
            return False
        if self.frame is not None:
            assert self.buffer is not None
            if len(self.frame) > len(self.buffer):
                raise RuntimeError("MPI_ERR_TRUNCATE: frame exceeds posted receive capacity")
            self.buffer[: len(self.frame)] = self.frame
            assert status is not None
            status.count = len(self.frame) if self.count is None else self.count
        return True

    def Cancel(self):
        self.cancelled = True


class _ImmediateBufferRequest:
    def Test(self):
        return True


class _NeverBufferRequest:
    def Test(self):
        return False


class _SendComm:
    def __init__(self):
        self.messages = []
        self.frames = []

    def Isend(self, buffer, *, dest, tag):
        from mpi4py import MPI

        assert (dest, tag) == (0, 27181)
        assert buffer[1] is MPI.BYTE
        self.frames.append(bytes(buffer[0]))
        self.messages.append(_decode_mpi_message(bytes(buffer[0])))
        return _MessageRequest()


class _ReceiveComm:
    def __init__(self, messages):
        self.messages = {rank: list(items) for rank, items in messages.items()}
        self.requests = []

    def Irecv(self, buffer, *, source, tag):
        from mpi4py import MPI

        assert tag == 27181
        assert buffer[1] is MPI.BYTE and len(buffer[0]) == _MPI_FRAME_BYTES
        if not self.messages[source]:
            request = _MessageRequest(never=True)
        else:
            message = self.messages[source].pop(0)
            frame = message if isinstance(message, bytes) else _encode_mpi_message(message)
            request = _MessageRequest(buffer=buffer[0], frame=frame)
        self.requests.append(request)
        return request


class _SummaryComm:
    def __init__(self, remote_summary, *, reduction_offset=0, reduction_never=False):
        self.remote_summary = remote_summary
        self.reduction_offset = reduction_offset
        self.reduction_never = reduction_never

    def Ireduce(self, send, receive, *, op, root):
        from mpi4py import MPI

        assert root == 0 and op == MPI.SUM
        assert send[1] is MPI.UINT64_T and receive[1] is MPI.UINT64_T
        for index, value in enumerate(send[0]):
            receive[0][index] = (value * 2 + self.reduction_offset) & ((1 << 64) - 1)
        return _NeverBufferRequest() if self.reduction_never else _ImmediateBufferRequest()

    def Irecv(self, buffer, *, source, tag):
        from mpi4py import MPI

        assert source == 1 and tag == 27182
        assert buffer[1] is MPI.BYTE and len(buffer[0]) == _MPI_FRAME_BYTES
        if self.remote_summary is None:
            return _MessageRequest(never=True)
        return _MessageRequest(
            buffer=buffer[0],
            frame=_encode_mpi_message(
                ("summary", 0, source, _encode_gather_payload(self.remote_summary))
            ),
        )


class _SummarySenderComm:
    def __init__(self):
        self.messages = []

    def Ireduce(self, send, receive, *, op, root):
        from mpi4py import MPI

        assert send[1] is MPI.UINT64_T and receive is None
        assert op == MPI.SUM and root == 0
        return _ImmediateBufferRequest()

    def Isend(self, buffer, *, dest, tag):
        from mpi4py import MPI

        assert dest == 0 and tag == 27182
        assert buffer[1] is MPI.BYTE
        self.messages.append(_decode_mpi_message(bytes(buffer[0])))
        return _MessageRequest()


class _SummaryMessageComm(_SummaryComm):
    def __init__(self, remote_summary, message):
        super().__init__(remote_summary)
        self.message = message

    def Irecv(self, buffer, *, source, tag):
        from mpi4py import MPI

        assert source == 1 and tag == 27182
        assert buffer[1] is MPI.BYTE
        frame = (
            self.message if isinstance(self.message, bytes) else _encode_mpi_message(self.message)
        )
        return _MessageRequest(buffer=buffer[0], frame=frame)


class _MaxComm:
    def Ireduce(self, send, receive, *, op, root):
        from mpi4py import MPI

        assert op == MPI.MAX and root == 0
        assert send[1] is MPI.DOUBLE
        if receive is not None:
            assert receive[1] is MPI.DOUBLE
            receive[0][0] = 9.0
        return _ImmediateBufferRequest()


def _raw_record(request_id="request-1"):
    request = TraceRequest(1.5, "model", "prompt", 2, 3, 1, request_id, "chat")
    return (request, 0.5, True, "", 2.0, 2, 3, 0.1, 1.6, 0.01, 0.02, 0.03, 3)


def test_multi_rank_mpi_transfer_emits_complete_evidence_without_files(tmp_path, monkeypatch):
    _install_fake_mpi(monkeypatch)
    remote_comm = _SendComm()
    remote_record = _raw_record("remote")
    assert (
        _gather_raw_results_via_mpi(
            remote_comm,
            [remote_record],
            run_index=0,
            rank=1,
            mpi_size=2,
            is_root=False,
            timeout_s=0.1,
        )
        is None
    )
    forbidden = tmp_path / "must-not-exist"
    gathered = _gather_raw_results_via_mpi(
        _ReceiveComm({1: remote_comm.messages}),
        [_raw_record("root")],
        run_index=0,
        rank=0,
        mpi_size=2,
        is_root=True,
        timeout_s=0.1,
    )

    assert [[row[0].req_id for row in rows] for rows in gathered] == [["root"], ["remote"]]
    assert not forbidden.exists()
    assert _LAST_GATHER_META["complete"] is True
    assert {item["transport"] for item in _LAST_GATHER_META["shards"]} == {"mpi_chunked"}
    from eval.lib.run_executor import _valid_gather_evidence

    assert _valid_gather_evidence(_LAST_GATHER_META, 2)


def test_multi_rank_mpi_transfer_timeout_fails_closed(monkeypatch):
    _install_fake_mpi(monkeypatch)
    with pytest.raises(RuntimeError, match=r"timed out.*missing.*\[1\]"):
        _gather_raw_results_via_mpi(
            _ReceiveComm({1: []}),
            [_raw_record("root")],
            run_index=0,
            rank=0,
            mpi_size=2,
            is_root=True,
            timeout_s=0.01,
        )
    assert _LAST_GATHER_META["complete"] is False
    assert _LAST_GATHER_META["missing_ranks"] == [1]


def _install_fake_mpi(monkeypatch):
    class Status:
        count = 0

        def Get_count(self, datatype):
            assert datatype is mpi.BYTE
            return self.count

    mpi = SimpleNamespace(
        SUM=object(), MAX=object(), UINT64_T=object(), DOUBLE=object(), BYTE=object(), Status=Status
    )
    monkeypatch.setitem(sys.modules, "mpi4py", SimpleNamespace(MPI=mpi))
    monkeypatch.setattr("eval.lib.replay_engine._CANCELLED_MPI_MESSAGES", [])
    monkeypatch.setattr("eval.lib.replay_engine._FAILED_MPI_COLLECTIVE", None)
    return mpi


@pytest.mark.parametrize("kind", ["data", "summary"])
@pytest.mark.parametrize("payload_bytes", [32769, _RESULT_CHUNK_BYTES])
def test_mpi_message_round_trip_crosses_default_object_receive_limit(
    monkeypatch, kind, payload_bytes
):
    _install_fake_mpi(monkeypatch)
    message = (kind, 4, 1, b"x" * payload_bytes)
    frame = _encode_mpi_message(message)
    receive = _mpi_irecv_message(_ReceiveComm({1: [frame]}), source=1, tag=27181)

    assert len(frame) > payload_bytes
    assert receive.test() == (True, message)
    assert receive.test() == (True, message)


def _frame_with_header(header, payload=b"", *, canonical=True):
    encoded = json.dumps(
        header,
        sort_keys=canonical,
        separators=(",", ":") if canonical else None,
    ).encode()
    return struct.pack("!I", len(encoded)) + encoded + payload


@pytest.mark.parametrize(
    "frame",
    [
        b"",
        b"abc",
        struct.pack("!I", 0),
        struct.pack("!I", _MPI_HEADER_BYTES + 1) + b"x" * (_MPI_HEADER_BYTES + 1),
        struct.pack("!I", 100) + b"{}",
        b"x" * (_MPI_FRAME_BYTES + 1),
        _frame_with_header([]),
        _frame_with_header(
            {
                "schema_version": 2,
                "kind": "data",
                "run_index": 0,
                "sequence": 0,
                "payload_bytes": 1,
            },
            b"x",
        ),
        _frame_with_header(
            {
                "schema_version": True,
                "kind": "data",
                "run_index": 0,
                "sequence": 0,
                "payload_bytes": 1,
            },
            b"x",
        ),
        _frame_with_header(
            {
                "schema_version": 1,
                "kind": "data",
                "run_index": False,
                "sequence": 0,
                "payload_bytes": 1,
            },
            b"x",
        ),
        _frame_with_header(
            {
                "schema_version": 1,
                "kind": "data",
                "run_index": 0,
                "sequence": -1,
                "payload_bytes": 1,
            },
            b"x",
        ),
        _frame_with_header(
            {
                "schema_version": 1,
                "kind": "data",
                "run_index": 0,
                "sequence": 0,
                "payload_bytes": 2,
            },
            b"x",
        ),
        _frame_with_header(
            {"schema_version": 1, "kind": "unknown", "run_index": 0, "payload_bytes": 1}, b"x"
        ),
        _frame_with_header(
            {
                "schema_version": 1,
                "kind": "data",
                "run_index": 0,
                "sequence": 0,
                "payload_bytes": 1,
                "extra": 1,
            },
            b"x",
        ),
        _frame_with_header(
            {
                "schema_version": 1,
                "kind": "data",
                "run_index": 0,
                "sequence": 0,
                "payload_bytes": 1,
            },
            b"x",
            canonical=False,
        ),
        _frame_with_header(
            {
                "schema_version": 1,
                "kind": "end",
                "run_index": 0,
                "chunks": 0,
                "records": 0,
                "evidence": {},
                "payload_bytes": 1,
            },
            b"x",
        ),
    ],
)
def test_mpi_message_rejects_malformed_truncated_or_oversized_frames(frame):
    with pytest.raises(ValueError, match="MPI message|MPI terminal"):
        _decode_mpi_message(frame)


def test_mpi_message_rejects_duplicate_header_keys():
    header = b'{"kind":"data","payload_bytes":1,"run_index":0,"schema_version":1,"sequence":0,"sequence":1}'
    with pytest.raises(ValueError):
        _decode_mpi_message(struct.pack("!I", len(header)) + header + b"x")


def test_mpi_message_sender_rejects_payload_or_header_overflow_before_posting(monkeypatch):
    _install_fake_mpi(monkeypatch)
    sender = _SendComm()
    with pytest.raises(ValueError, match="payload exceeds"):
        _mpi_isend_message(
            sender, ("data", 0, 0, b"x" * (_RESULT_CHUNK_BYTES + 1)), dest=0, tag=27181
        )
    with pytest.raises(ValueError, match="header exceeds"):
        _mpi_isend_message(
            sender, ("end", 0, 0, 0, {"padding": "x" * _MPI_HEADER_BYTES}), dest=0, tag=27181
        )
    assert sender.frames == []


@pytest.mark.parametrize("count", [-1, _MPI_FRAME_BYTES + 1])
def test_mpi_message_receive_rejects_invalid_status_count(monkeypatch, count):
    _install_fake_mpi(monkeypatch)
    comm = _ReceiveComm({1: [("data", 0, 0, b"x")]})
    request = _mpi_irecv_message(comm, source=1, tag=27181)
    comm.requests[0].count = count
    with pytest.raises(RuntimeError, match="receive count exceeds"):
        request.test()


def test_mpi_message_timeout_cancels_without_releasing_live_buffers(monkeypatch):
    import eval.lib.replay_engine as replay

    _install_fake_mpi(monkeypatch)
    comm = _ReceiveComm({1: []})
    request = _mpi_irecv_message(comm, source=1, tag=27181)
    with pytest.raises(RuntimeError, match="absolute MPI deadline"):
        _wait_request(request, deadline=time.monotonic() - 1, label="test receive")
    assert comm.requests[0].cancelled
    assert replay._CANCELLED_MPI_MESSAGES == [request]
    assert len(request.buffer) == _MPI_FRAME_BYTES


def test_mpi_message_send_keeps_its_buffer_until_cancel_completes(monkeypatch):
    import eval.lib.replay_engine as replay

    _install_fake_mpi(monkeypatch)

    class Sender:
        def Isend(self, buffer, *, dest, tag):
            self.buffer = buffer[0]
            return _MessageRequest(never=True)

    sender = Sender()
    request = _mpi_isend_message(sender, ("data", 0, 0, b"x" * 40000), dest=0, tag=27181)
    assert request.buffer is sender.buffer
    assert request.test() == (False, None)
    request.cancel()
    assert replay._CANCELLED_MPI_MESSAGES == [request]
    request.request.never = False
    request.cancel()
    assert replay._CANCELLED_MPI_MESSAGES == []


def test_mpi_message_send_test_exception_retains_active_buffer(monkeypatch):
    import eval.lib.replay_engine as replay

    _install_fake_mpi(monkeypatch)

    class FailedTestRequest(_MessageRequest):
        def Test(self, status=None):
            raise RuntimeError("injected MPI Test failure")

    class Sender:
        def Isend(self, buffer, *, dest, tag):
            self.buffer = buffer[0]
            self.request = FailedTestRequest()
            return self.request

    sender = Sender()
    request = _mpi_isend_message(sender, ("data", 0, 0, b"x" * 40000), dest=0, tag=27181)
    with pytest.raises(RuntimeError, match="injected MPI Test failure"):
        _wait_request(request, deadline=time.monotonic() + 1, label="test send")
    assert sender.request.cancelled
    assert replay._CANCELLED_MPI_MESSAGES == [request]
    assert request.buffer is sender.buffer


def test_multi_rank_mpi_frame_failure_cancels_other_pending_receives(monkeypatch):
    _install_fake_mpi(monkeypatch)
    comm = _ReceiveComm({1: [b"invalid"], 2: []})
    with pytest.raises(ValueError, match="MPI message header"):
        _gather_raw_results_via_mpi(
            comm, [], run_index=0, rank=0, mpi_size=3, is_root=True, timeout_s=1
        )
    assert comm.requests[1].cancelled
    assert not _LAST_GATHER_META.get("complete", False)


def test_multi_rank_mpi_receive_setup_failure_cancels_already_posted_receives(monkeypatch):
    _install_fake_mpi(monkeypatch)

    class FailedReceiveComm(_ReceiveComm):
        def Irecv(self, buffer, *, source, tag):
            if source == 2:
                raise RuntimeError("injected post failure")
            return super().Irecv(buffer, source=source, tag=tag)

    comm = FailedReceiveComm({1: []})
    with pytest.raises(RuntimeError, match="injected post failure"):
        _gather_raw_results_via_mpi(
            comm, [], run_index=0, rank=0, mpi_size=3, is_root=True, timeout_s=1
        )
    assert comm.requests[0].cancelled
    assert not _LAST_GATHER_META.get("complete", False)


def test_multi_rank_mpi_large_chunks_and_empty_shards(monkeypatch):
    _install_fake_mpi(monkeypatch)
    sender = _SendComm()
    records = []
    for index in range(3):
        row = _raw_record(f"large-{index}")
        request = TraceRequest(1.5, "model", "x" * (700 << 10), 2, 3, 1, f"large-{index}", "chat")
        records.append((request, *row[1:]))
    _gather_raw_results_via_mpi(
        sender, records, run_index=0, rank=1, mpi_size=3, is_root=False, timeout_s=1
    )
    empty_sender = _SendComm()
    _gather_raw_results_via_mpi(
        empty_sender, [], run_index=0, rank=2, mpi_size=3, is_root=False, timeout_s=1
    )
    assert [message[0] for message in sender.messages] == ["data", "data", "data", "end"]
    assert all(32768 < len(frame) <= _MPI_FRAME_BYTES for frame in sender.frames[:-1])
    assert [message[0] for message in empty_sender.messages] == ["end"]

    gathered = _gather_raw_results_via_mpi(
        _ReceiveComm({1: sender.frames, 2: empty_sender.frames}),
        [_raw_record("root")],
        run_index=0,
        rank=0,
        mpi_size=3,
        is_root=True,
        timeout_s=1,
    )
    assert [_encode_gather_payload(shard) for shard in gathered] == [
        _encode_gather_payload([_raw_record("root")]),
        _encode_gather_payload(records),
        _encode_gather_payload([]),
    ]
    assert _LAST_GATHER_META["complete"] is True
    assert _LAST_GATHER_META["shards"][1]["size_bytes"] > 2 << 20
    assert _LAST_GATHER_META["shards"][2]["sha256"] == hashlib.sha256(b"").hexdigest()


def test_multi_rank_summary_large_evidence_progresses_alongside_reduction(monkeypatch):
    _install_fake_mpi(monkeypatch)
    summary = _go_summary_row()
    summary.update(requests_completed=1, requests_scheduled=1, errors=1)
    summary["error_samples"] = {"synthetic": "x" * 40000}
    assert len(_encode_gather_payload(summary)) > 32768

    class ConcurrentSummaryComm(_SummaryComm):
        def Ireduce(self, send, receive, *, op, root):
            super().Ireduce(send, receive, op=op, root=root)
            owner = self

            class Reduction:
                def Test(self):
                    return owner.message.polls == 0

            return Reduction()

        def Irecv(self, buffer, *, source, tag):
            self.message = super().Irecv(buffer, source=source, tag=tag)
            self.message.polls = 2
            return self.message

    reduced = _reduce_summary_via_mpi(
        ConcurrentSummaryComm(summary),
        summary,
        run_index=0,
        rank=0,
        mpi_size=2,
        is_root=True,
        timeout_s=1,
    )
    assert reduced["requests_completed"] == 2
    assert reduced["errors"] == 2
    assert _LAST_GATHER_META["shards"][1]["size_bytes"] > 32768
    assert _LAST_GATHER_META["complete"] is True


def test_multi_rank_summary_uses_bounded_mpi_reductions(monkeypatch):
    _install_fake_mpi(monkeypatch)
    summary = _go_summary_row()
    summary.update(
        requests_completed=2,
        requests_scheduled=2,
        total_input_tokens=3,
        total_output_tokens=4,
        p50_s=0.2,
        p99_s=0.4,
        latency_histogram={
            "bucket_upper_bounds_s": [0.1, 1.0, -1.0],
            "counts": [0, 2, 0],
            "count": 2,
            "sum_s": 0.6,
        },
    )
    reduced = _reduce_summary_via_mpi(
        _SummaryComm(summary),
        summary,
        run_index=0,
        rank=0,
        mpi_size=2,
        is_root=True,
        timeout_s=0.1,
    )
    assert reduced["requests_completed"] == 4
    assert reduced["total_output_tokens"] == 8
    assert reduced["p99_s"] == pytest.approx(1.0)
    assert _LAST_GATHER_META["complete"] is True
    assert {item["transport"] for item in _LAST_GATHER_META["shards"]} == {"mpi_summary_p2p"}
    from eval.lib.run_executor import _valid_gather_evidence

    assert _valid_gather_evidence(_LAST_GATHER_META, 2)
    legacy = {
        **_LAST_GATHER_META,
        "shards": [{**item, "transport": "mpi_reduce"} for item in _LAST_GATHER_META["shards"]],
    }
    assert _valid_gather_evidence(legacy, 2)


def test_multi_rank_summary_sender_uses_bounded_canonical_message(monkeypatch):
    _install_fake_mpi(monkeypatch)
    summary = _go_summary_row()
    sender = _SummarySenderComm()

    assert (
        _reduce_summary_via_mpi(
            sender,
            summary,
            run_index=3,
            rank=1,
            mpi_size=2,
            is_root=False,
            timeout_s=0.1,
        )
        is None
    )
    assert sender.messages == [("summary", 3, 1, _encode_gather_payload(summary))]


def test_multi_rank_summary_timeout_fails_closed(monkeypatch):
    _install_fake_mpi(monkeypatch)
    summary = _go_summary_row()
    with pytest.raises(RuntimeError, match=r"pending operations.*uint64-reduction"):
        _reduce_summary_via_mpi(
            _SummaryComm(summary, reduction_never=True),
            summary,
            run_index=0,
            rank=0,
            mpi_size=2,
            is_root=True,
            timeout_s=0.01,
        )
    assert _LAST_GATHER_META["complete"] is False


@pytest.mark.parametrize("failure", ["receive_post", "receive_test", "collective_test", "timeout"])
def test_pending_summary_collective_retains_buffers_and_rejects_retry(monkeypatch, failure):
    import eval.lib.replay_engine as replay

    _install_fake_mpi(monkeypatch)
    summary = _go_summary_row()

    class Collective:
        cancelled = False

        def Test(self):
            if failure == "collective_test":
                raise RuntimeError("injected collective test failure")
            return False

        def Cancel(self):
            self.cancelled = True

    class Comm(_SummaryComm):
        posts = 0

        def Ireduce(self, send, receive, *, op, root):
            self.posts += 1
            self.send = send[0]
            self.receive = receive[0]
            self.collective = Collective()
            return self.collective

        def Irecv(self, buffer, *, source, tag):
            if failure == "receive_post":
                raise RuntimeError("injected receive post failure")
            if failure == "receive_test":

                class ReceiveFailure(_MessageRequest):
                    def Test(self, status=None):
                        raise RuntimeError("injected receive test failure")

                return ReceiveFailure()
            return super().Irecv(buffer, source=source, tag=tag)

    comm = Comm(summary)
    with pytest.raises(RuntimeError, match="deadline|injected"):
        _reduce_summary_via_mpi(
            comm, summary, run_index=0, rank=0, mpi_size=2, is_root=True, timeout_s=0.01
        )
    held = replay._FAILED_MPI_COLLECTIVE
    assert held is not None
    assert held[0] is comm.collective
    assert held[1] is comm.send
    assert held[2] is comm.receive
    assert not comm.collective.cancelled
    assert not _LAST_GATHER_META.get("complete", False)
    with pytest.raises(RuntimeError, match="unfinished collective failed"):
        _reduce_summary_via_mpi(
            comm, summary, run_index=1, rank=0, mpi_size=2, is_root=True, timeout_s=0.01
        )
    assert comm.posts == 1
    assert replay._FAILED_MPI_COLLECTIVE is held


def test_multi_rank_summary_missing_rank_fails_closed(monkeypatch):
    _install_fake_mpi(monkeypatch)
    summary = _go_summary_row()
    with pytest.raises(RuntimeError, match=r"summary-rank-1"):
        _reduce_summary_via_mpi(
            _SummaryComm(None),
            summary,
            run_index=0,
            rank=0,
            mpi_size=2,
            is_root=True,
            timeout_s=0.01,
        )
    assert _LAST_GATHER_META["missing_ranks"] == [1]


def test_multi_rank_summary_rejects_reduction_disagreement(monkeypatch):
    _install_fake_mpi(monkeypatch)
    summary = _go_summary_row()
    with pytest.raises(RuntimeError, match="disagrees with exact rank evidence"):
        _reduce_summary_via_mpi(
            _SummaryComm(summary, reduction_offset=1),
            summary,
            run_index=0,
            rank=0,
            mpi_size=2,
            is_root=True,
            timeout_s=0.1,
        )


@pytest.mark.parametrize(
    "message",
    [
        ("data", 0, 1, b"{}"),
        ("summary", 0, 7, b"{}"),
    ],
)
def test_multi_rank_summary_rejects_malformed_or_oversized_evidence(monkeypatch, message):
    _install_fake_mpi(monkeypatch)
    summary = _go_summary_row()
    with pytest.raises(RuntimeError, match="invalid MPI summary message"):
        _reduce_summary_via_mpi(
            _SummaryMessageComm(summary, message),
            summary,
            run_index=0,
            rank=0,
            mpi_size=2,
            is_root=True,
            timeout_s=0.1,
        )


def test_multi_rank_summary_rejects_noncanonical_evidence(monkeypatch):
    _install_fake_mpi(monkeypatch)
    summary = _go_summary_row()
    encoded = json.dumps(
        {"schema_version": 1, "kind": "summary", "summary": summary}, indent=2
    ).encode()
    with pytest.raises(RuntimeError, match="non-canonical"):
        _reduce_summary_via_mpi(
            _SummaryMessageComm(summary, ("summary", 0, 1, encoded)),
            summary,
            run_index=0,
            rank=0,
            mpi_size=2,
            is_root=True,
            timeout_s=0.1,
        )


def test_multi_rank_summary_rejects_uint64_aggregate_overflow(monkeypatch):
    _install_fake_mpi(monkeypatch)
    summary = _go_summary_row()
    summary["total_input_tokens"] = (1 << 64) - 1
    with pytest.raises(RuntimeError, match="aggregate exceeds UINT64_MAX"):
        _reduce_summary_via_mpi(
            _SummaryComm(summary),
            summary,
            run_index=0,
            rank=0,
            mpi_size=2,
            is_root=True,
            timeout_s=0.1,
        )


def test_same_rank_go_process_summaries_merge_histograms_not_quantile_maxima():
    left = _go_summary_row()
    left.update(
        requests_completed=1,
        requests_scheduled=1,
        p50_s=0.1,
        p99_s=0.1,
        latency_histogram={
            "bucket_upper_bounds_s": [0.1, 1.0, -1.0],
            "counts": [1, 0, 0],
            "count": 1,
            "sum_s": 0.1,
        },
    )
    right = _go_summary_row()
    right.update(
        requests_completed=1,
        requests_scheduled=1,
        p50_s=0.9,
        p99_s=0.9,
        latency_histogram={
            "bucket_upper_bounds_s": [0.1, 1.0, -1.0],
            "counts": [0, 1, 0],
            "count": 1,
            "sum_s": 0.9,
        },
    )
    merged = _merge_go_process_summaries([left, right])
    assert merged["p50_s"] == pytest.approx(0.1)
    assert merged["p99_s"] == pytest.approx(1.0)
    assert merged["latency_histogram"]["count"] == 2
    assert merged["latency_quantile_method"] == "mergeable_histogram_estimate_2pct_through_7200s"


def test_dispatch_end_uses_the_latest_rank_not_rank_zero(monkeypatch):
    _install_fake_mpi(monkeypatch)
    assert (
        _reduce_dispatch_end_via_mpi(_MaxComm(), 2.0, mpi_size=2, is_root=True, timeout_s=0.1)
        == 9.0
    )


def test_dispatch_end_non_root_uses_no_receive_buffer(monkeypatch):
    _install_fake_mpi(monkeypatch)
    assert (
        _reduce_dispatch_end_via_mpi(_MaxComm(), 2.0, mpi_size=2, is_root=False, timeout_s=0.1)
        is None
    )


@pytest.mark.parametrize("is_root", [False, True])
@pytest.mark.parametrize("test_raises", [False, True])
def test_pending_dispatch_collective_retains_buffers_and_rejects_retry(
    monkeypatch, is_root, test_raises
):
    import eval.lib.replay_engine as replay

    _install_fake_mpi(monkeypatch)

    class Collective:
        cancelled = False

        def Test(self):
            if test_raises:
                raise RuntimeError("injected collective test failure")
            return False

        def Cancel(self):
            self.cancelled = True

    class Comm:
        posts = 0

        def Ireduce(self, send, receive, *, op, root):
            self.posts += 1
            self.send = send[0]
            self.receive = receive[0] if receive is not None else None
            self.collective = Collective()
            return self.collective

    comm = Comm()
    with pytest.raises(RuntimeError, match="deadline|injected"):
        _reduce_dispatch_end_via_mpi(comm, 2.0, mpi_size=2, is_root=is_root, timeout_s=0.01)
    held = replay._FAILED_MPI_COLLECTIVE
    assert held is not None
    assert held[0] is comm.collective
    assert held[1] is comm.send
    assert held[2] is comm.receive
    assert not comm.collective.cancelled
    with pytest.raises(RuntimeError, match="unfinished collective failed"):
        _reduce_dispatch_end_via_mpi(comm, 2.0, mpi_size=2, is_root=is_root, timeout_s=0.01)
    assert comm.posts == 1
    assert replay._FAILED_MPI_COLLECTIVE is held


def test_completed_collective_is_not_retained_when_result_validation_fails(monkeypatch):
    import eval.lib.replay_engine as replay

    _install_fake_mpi(monkeypatch)

    class InvalidResultComm(_MaxComm):
        def Ireduce(self, send, receive, *, op, root):
            request = super().Ireduce(send, receive, op=op, root=root)
            receive[0][0] = float("nan")
            return request

    with pytest.raises(ValueError, match="global dispatch last_fire_time"):
        _reduce_dispatch_end_via_mpi(
            InvalidResultComm(), 2.0, mpi_size=2, is_root=True, timeout_s=0.1
        )
    assert replay._FAILED_MPI_COLLECTIVE is None


def test_dispatch_end_rejects_unbounded_deadline(monkeypatch):
    _install_fake_mpi(monkeypatch)
    with pytest.raises(ValueError, match="finite and positive"):
        _reduce_dispatch_end_via_mpi(
            _MaxComm(), 2.0, mpi_size=2, is_root=True, timeout_s=float("inf")
        )


def test_mpi_import_failure_cannot_create_independent_roots(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def reject_mpi(name, *args, **kwargs):
        if name == "mpi4py":
            raise ImportError("missing")
        return real_import(name, *args, **kwargs)

    monkeypatch.setenv("PMI_SIZE", "2")
    monkeypatch.setattr(builtins, "__import__", reject_mpi)
    with pytest.raises(RuntimeError, match="requires mpi4py"):
        _init_mpi()


def test_mpi_import_failure_rejects_launcher_rank_without_size_hint(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def reject_mpi(name, *args, **kwargs):
        if name == "mpi4py":
            raise ImportError("missing")
        return real_import(name, *args, **kwargs)

    monkeypatch.delenv("PMI_SIZE", raising=False)
    monkeypatch.setenv("PALS_RANKID", "0")
    monkeypatch.setattr(builtins, "__import__", reject_mpi)
    with pytest.raises(RuntimeError, match="requires mpi4py"):
        _init_mpi()


def test_mpi_version_mismatch_fails_before_runtime_import(monkeypatch):
    import builtins
    from importlib import metadata

    real_import = builtins.__import__
    real_version = metadata.version
    imported_runtime = False

    def version(name):
        return "9.9.9" if name == "mpi4py" else real_version(name)

    def observe_import(name, *args, **kwargs):
        nonlocal imported_runtime
        if name == "mpi4py":
            imported_runtime = True
            raise AssertionError("MPI runtime imported before version proof")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(metadata, "version", version)
    monkeypatch.setattr(builtins, "__import__", observe_import)
    with pytest.raises(RuntimeError, match="does not match the compatibility profile"):
        _init_mpi()
    assert imported_runtime is False


class _RootScatterComm:
    def __init__(self):
        self.rank_parts = []
        self.status = None

    def scatter(self, values, *, root):
        assert root == 0 and isinstance(values, list) and len(values) == 2
        if values[0] is not None:
            self.rank_parts.append(values)
        return values[0]

    def bcast(self, value, *, root):
        assert root == 0
        self.status = value
        return value

    def Get_size(self):
        return 2


class _WorkerScatterComm:
    def __init__(self, parts, status):
        self.parts = list(parts)
        self.status = status

    def scatter(self, value, *, root):
        assert value is None and root == 0
        return self.parts.pop(0)

    def bcast(self, value, *, root):
        assert value is None and root == 0
        return self.status

    def Get_size(self):
        return 2


def test_root_reads_and_hashes_trace_once_then_scatters_partitions(tmp_path, monkeypatch):
    trace = tmp_path / "trace.jsonl"
    trace.write_text(
        '{"__type__":"metadata"}\n'
        '{"timestamp":0.0,"model":"m","prompt":"a","input_len":1,"output_len":1}\n'
        '{"timestamp":1.0,"model":"m","prompt":"b","input_len":1,"output_len":1}\n',
        encoding="utf-8",
    )
    trace_hash = hashlib.sha256(trace.read_bytes()).hexdigest()
    from exaserve.state import atomic

    real_reader = atomic.regular_file_reader
    opens = []

    def recording_reader(path, *args, **kwargs):
        opens.append(os.fspath(path))
        return real_reader(path, *args, **kwargs)

    monkeypatch.setattr(atomic, "regular_file_reader", recording_reader)
    comm = _RootScatterComm()
    requests, total, span = _distribute_trace_requests(
        comm,
        rank=0,
        mpi_size=2,
        trace_path=str(trace),
        expected_hash=trace_hash,
    )
    assert opens == [str(trace)]
    assert [item.req_id for item in requests] == [f"{0:032x}"]
    assert total == 2 and span == 1.0
    assert [[item.req_id for item in part] for part in comm.rank_parts[0]] == [
        [f"{0:032x}"],
        [f"{1:032x}"],
    ]


def test_non_root_trace_distribution_performs_zero_file_opens(monkeypatch):
    request = TraceRequest(0.0, "m", "p", 1, 1, 1, f"{0:032x}")
    status = {"ok": True, "error": None, "total": 1, "last_timestamp": 0.0}
    comm = _WorkerScatterComm([[request], None], status)

    def forbidden_reader(*_args, **_kwargs):
        raise AssertionError("non-root attempted a file open")

    monkeypatch.setattr("exaserve.state.atomic.regular_file_reader", forbidden_reader)
    requests, total, span = _distribute_trace_requests(
        comm,
        rank=1,
        mpi_size=2,
        trace_path=None,
        expected_hash=None,
    )
    assert requests == [request]
    assert total == 1 and span == 0.0


def test_trace_request_loading_rejects_coercion_and_duplicate_json(tmp_path):
    trace = tmp_path / "bad.jsonl"
    trace.write_text(
        '{"timestamp":"0","model":"m","prompt":"p","output_len":1}\n',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="timestamp"):
        _load_trace_requests(str(trace))

    trace.write_text(
        '{"timestamp":0,"timestamp":1,"model":"m","prompt":"p","output_len":1}\n',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="duplicate JSON key"):
        _load_trace_requests(str(trace))


def test_runtime_port_comes_only_from_the_canonical_plan(tmp_path):
    port_file = tmp_path / "proxy_out" / "proxy_port"
    port_file.parent.mkdir()
    port_file.write_text("not-a-port", encoding="utf-8")
    manifest = SimpleNamespace(
        deployment_plan=SimpleNamespace(
            gateway=SimpleNamespace(port=4321),
            exposure=SimpleNamespace(serve_port=8000),
        )
    )
    assert _port_from_manifest(manifest) == 4321


def test_local_direct_dispatch_keeps_all_routes_on_the_current_host(monkeypatch):
    monkeypatch.setattr("eval.lib.replay_engine._local_addresses", lambda: {"node-a", "10.0.0.1"})
    urls = [
        "http://node-a:8000/model_r0",
        "http://node-a:8000/model_r1",
        "http://node-b:8000/model_r2",
        "http://node-b:8000/model_r3",
    ]
    assert _apply_direct_topology(urls, 0, 2, topology="local", pair_shift=1) == urls[:2]
    assert _apply_direct_topology(urls, 0, 2, topology="paired", pair_shift=1) == urls[2:]


def test_runtime_base_urls_must_match_canonical_target_count_and_port():
    manifest = SimpleNamespace(
        job_replay_client_config=SimpleNamespace(dest="direct"),
        deployment_plan=SimpleNamespace(
            gateway=None,
            exposure=SimpleNamespace(serve_port=8000),
            num_nodes=2,
            models=[SimpleNamespace(num_replicas=2)],
        ),
    )
    urls = ["http://node-a:8000/model_r0", "http://node-b:8000/model_r1"]
    assert _validate_base_urls(manifest, urls) == urls
    with pytest.raises(ValueError, match="expected 2"):
        _validate_base_urls(manifest, urls[:1])
    with pytest.raises(ValueError, match="canonical port"):
        _validate_base_urls(manifest, [urls[0], "http://node-b:9000/model_r1"])
    with pytest.raises(ValueError, match="credentials"):
        _validate_base_urls(
            manifest, ["http://user@node-a:8000/model_r0", "http://node-b:8000/model_r1"]
        )


def _gather_evidence(ranks: int) -> dict:
    return {
        "schema_version": 1,
        "expected_ranks": ranks,
        "collected_ranks": list(range(ranks)),
        "missing_ranks": [],
        "complete": True,
        "shards": [
            {
                "rank": rank,
                "size_bytes": 1,
                "sha256": f"{rank + 1:064x}",
                "transport": "mpi_chunked" if ranks > 1 else "in_memory",
            }
            for rank in range(ranks)
        ],
    }


def test_replay_result_requires_complete_evidence_for_every_repeat(tmp_path):
    results = tmp_path / "results"
    results.mkdir()
    payload = {
        "overall": {"requests_completed": 1, "requests_scheduled": 1, "errors": 0},
        "meta": {
            "completed_runs": 2,
            "gather_by_run": [_gather_evidence(2), _gather_evidence(2)],
        },
        "per_run": [
            {"run_index": 0, "requests_completed": 1, "requests_scheduled": 1, "errors": 0},
            {"run_index": 1, "requests_completed": 1, "requests_scheduled": 1, "errors": 0},
        ],
    }
    (results / "result0.json").write_text(json.dumps(payload), encoding="utf-8")
    plan = SimpleNamespace(
        bundle=SimpleNamespace(results_dir=str(results)),
        client=SimpleNamespace(dispatch_topologies=[], num_nodes=2, num_runs=2),
    )
    assert _validate_replay_results(plan)["incomplete_reasons"] == []

    payload["meta"]["gather_by_run"] = payload["meta"]["gather_by_run"][1:]
    (results / "result0.json").write_text(json.dumps(payload), encoding="utf-8")
    reasons = _validate_replay_results(plan)["incomplete_reasons"]
    assert any("2 run(s) x 2 client rank(s)" in reason for reason in reasons)


def test_replay_result_refuses_to_select_a_newest_generation(tmp_path):
    results = tmp_path / "results"
    results.mkdir()
    payload = {
        "overall": {"requests_completed": 1, "requests_scheduled": 1, "errors": 0},
        "meta": {"completed_runs": 1, "gather_by_run": [_gather_evidence(1)]},
        "per_run": [
            {"run_index": 0, "requests_completed": 1, "requests_scheduled": 1, "errors": 0}
        ],
    }
    for index in (0, 1):
        (results / f"result{index}.json").write_text(json.dumps(payload), encoding="utf-8")
    plan = SimpleNamespace(
        bundle=SimpleNamespace(results_dir=str(results)),
        client=SimpleNamespace(dispatch_topologies=[], num_nodes=1, num_runs=1),
    )

    reasons = _validate_replay_results(plan)["incomplete_reasons"]
    assert any("result identity is ambiguous" in reason for reason in reasons)


def test_replay_result_identity_requires_new_materialization(tmp_path):
    results = tmp_path / "results"
    assert _next_result_path(str(results)) == str(results / "result0.json")
    (results / "result0.json").write_text("{}", encoding="utf-8")

    with pytest.raises(FileExistsError, match="materialize a new run"):
        _next_result_path(str(results))
