from __future__ import annotations

import hashlib
import json
import os
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
    _validate_replay_results,
)
from eval.lib.replay_engine import (
    _LAST_GATHER_META,
    TraceRequest,
    _apply_direct_topology,
    _decode_gather_payload,
    _encode_gather_payload,
    _gather_results_via_shards,
    _load_trace_requests,
    _load_trace_shard_manifest,
    _next_result_path,
    _port_from_manifest,
    _read_go_results,
    _send_run_t0_and_wait,
    _stage_trace_shards,
    _stop_replay_process,
    _validate_base_urls,
)
from exaserve.state.atomic import atomic_create_bytes


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
        scheduler=SimpleNamespace(type="pbs"),
        bundle=SimpleNamespace(logs_dir=str(tmp_path)),
        repo_root=str(tmp_path),
        runtime_manifest_path=str(tmp_path / "runtime.json"),
    )


def test_replay_hostfile_cleanup_failure_is_fatal_after_success(tmp_path, monkeypatch):
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


def test_result_shard_json_round_trip_preserves_typed_records():
    request = TraceRequest(1.5, "model", "prompt", 2, 3, 1, "request-1", "chat")
    record = (request, 0.5, True, "", 2.0, 2, 3, 0.1, 1.6, 0.01, 0.02, 0.03, 3)

    decoded = _decode_gather_payload(_encode_gather_payload([record]))

    assert len(decoded) == 1
    assert decoded[0][0].req_id == "request-1"
    assert decoded[0][1:] == record[1:]


def test_result_shard_decoder_rejects_pickle_and_unknown_shapes():
    import pickle

    with pytest.raises((UnicodeError, ValueError)):
        _decode_gather_payload(pickle.dumps({"attacker": "controlled"}))
    with pytest.raises(ValueError, match="invalid shape"):
        _decode_gather_payload(b'{"schema_version":1,"kind":"summary","summary":{},"extra":true}')


def test_multi_rank_gather_emits_schema_accepted_complete_evidence(tmp_path):
    shard_dir = tmp_path / "_shards" / ("a" * 32)
    shard_dir.mkdir(parents=True)
    summary = _go_summary_row()
    atomic_create_bytes(shard_dir / "run0_rank1.json", _encode_gather_payload(summary))

    gathered = _gather_results_via_shards(
        object(),
        summary,
        run_index=0,
        shard_dir=str(shard_dir),
        rank=0,
        mpi_size=2,
        is_root=True,
        timeout_s=0.1,
    )

    assert gathered == [summary, summary]
    assert "invalid_shards" not in _LAST_GATHER_META
    assert _LAST_GATHER_META["complete"] is True
    from eval.lib.run_executor import _valid_gather_evidence

    assert _valid_gather_evidence(_LAST_GATHER_META, 2)


def test_gather_attempt_directory_does_not_consume_a_stale_prior_attempt(tmp_path):
    summary = _go_summary_row()
    stale_dir = tmp_path / "_shards" / ("a" * 32)
    stale_dir.mkdir(parents=True)
    atomic_create_bytes(stale_dir / "run0_rank1.json", _encode_gather_payload(summary))
    current_dir = tmp_path / "_shards" / ("b" * 32)

    gathered = _gather_results_via_shards(
        object(),
        summary,
        run_index=0,
        shard_dir=str(current_dir),
        rank=0,
        mpi_size=2,
        is_root=True,
        timeout_s=0.01,
    )

    assert gathered == [summary]
    assert _LAST_GATHER_META["complete"] is False
    assert _LAST_GATHER_META["missing_ranks"] == [1]


def test_trace_staging_failure_is_not_a_whole_trace_fallback(tmp_path):
    with pytest.raises(FileNotFoundError):
        _stage_trace_shards(str(tmp_path / "missing.jsonl"), 2, "0" * 64)


def test_trace_shards_are_identity_bound_and_corruption_fails_closed(tmp_path):
    trace = tmp_path / "trace.jsonl"
    trace.write_text(
        '{"__type__":"metadata"}\n'
        '{"timestamp":0.0,"model":"m","prompt":"a","input_len":1,"output_len":1}\n'
        '{"timestamp":1.0,"model":"m","prompt":"b","input_len":1,"output_len":1}\n',
        encoding="utf-8",
    )
    trace_hash = hashlib.sha256(trace.read_bytes()).hexdigest()
    shard_dir = _stage_trace_shards(str(trace), 2, trace_hash)
    marker = _load_trace_shard_manifest(shard_dir, trace_hash=trace_hash, mpi_size=2)
    assert marker["total"] == 2
    rank0 = Path(shard_dir) / "rank0.jsonl"
    rank0.write_text("{}\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        _load_trace_requests(str(rank0), expected_hash=marker["shards"][0]["sha256"])


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
                "transport": "shared_file" if ranks > 1 else "in_memory",
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
