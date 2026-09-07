from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from eval.lib.replay_failure import (
    DETAIL,
    REASON_CODE,
    capture_replay_process_failure,
    validate_replay_process_failure,
)
from eval.lib.run_executor import _execute_run_locked
from eval.lib.run_planner import _validate_run_state_payload, write_run_state
from exaserve.state.status import StatusStore


def _plan(tmp_path: Path, *, arms=()):
    root = tmp_path / "bundle"
    logs = root / "logs"
    logs.mkdir(parents=True)
    (root / "state").mkdir()
    return SimpleNamespace(
        backend_name="mock",
        client=SimpleNamespace(startup_only=False, dispatch_topologies=list(arms)),
        bundle=SimpleNamespace(
            root_dir=str(root),
            logs_dir=str(logs),
            state_path=str(root / "state" / "run_status.json"),
        ),
        run_id="n4",
        run_group_id="run9",
        run_semantic_hash="1" * 64,
        deployment_plan_hash="2" * 64,
        source_snapshot_hash="3" * 64,
    )


def test_replay_failure_is_bounded_and_does_not_copy_secret_log_content(tmp_path):
    plan = _plan(tmp_path)
    secret = "hf_token=do-not-persist-in-status"
    log = Path(plan.bundle.logs_dir) / "replay.log"
    log.write_text(f"request failed; {secret}\n", encoding="utf-8")

    failure = capture_replay_process_failure(
        plan, exit_code=17, topology_arm=None, log_name="replay.log"
    )

    assert set(failure) == {
        "schema_version",
        "phase",
        "command_id",
        "exit_code",
        "diagnostic",
    }
    assert failure["command_id"] == "mpi_replay_client"
    assert failure["diagnostic"] == {
        "path": "logs/replay.log",
        "size_bytes": log.stat().st_size,
        "sha256": hashlib.sha256(log.read_bytes()).hexdigest(),
    }
    assert secret not in json.dumps(failure)
    validate_replay_process_failure(plan, failure, status_exit_code=17)


@pytest.mark.parametrize(
    ("field", "value", "match"),
    [
        ("exit_code", 18, "exit_code disagrees"),
        ("diagnostic.path", "logs/replay_mesh.log", "content identity mismatch"),
        ("diagnostic.sha256", "0" * 64, "content identity mismatch"),
    ],
)
def test_replay_failure_validation_binds_status_path_and_hash(tmp_path, field, value, match):
    plan = _plan(tmp_path)
    (Path(plan.bundle.logs_dir) / "replay.log").write_text("failed\n", encoding="utf-8")
    failure = capture_replay_process_failure(
        plan, exit_code=17, topology_arm=None, log_name="replay.log"
    )
    tampered = copy.deepcopy(failure)
    if field == "exit_code":
        tampered[field] = value
    else:
        section, key = field.split(".")
        tampered[section][key] = value

    with pytest.raises(ValueError, match=match):
        validate_replay_process_failure(plan, tampered, status_exit_code=17)


def test_replay_failure_missing_or_symlink_log_fails_closed(tmp_path):
    plan = _plan(tmp_path)
    with pytest.raises(ValueError, match="regular, non-symlink"):
        capture_replay_process_failure(plan, exit_code=1, topology_arm=None, log_name="replay.log")

    target = tmp_path / "outside.log"
    target.write_text("failure\n", encoding="utf-8")
    (Path(plan.bundle.logs_dir) / "replay.log").symlink_to(target)
    with pytest.raises(ValueError, match="regular, non-symlink"):
        capture_replay_process_failure(plan, exit_code=1, topology_arm=None, log_name="replay.log")


@pytest.mark.parametrize(
    ("failed_cleanup_attempts", "expected_cleanup_error"),
    [
        (1, None),
        (
            2,
            "attempt 1: RuntimeError: cleanup failed 1; attempt 2: RuntimeError: cleanup failed 2",
        ),
    ],
)
def test_replay_failure_resolves_cleanup_before_preserving_first_cause(
    tmp_path, monkeypatch, failed_cleanup_attempts, expected_cleanup_error
):
    plan = _plan(tmp_path)
    log = Path(plan.bundle.logs_dir) / "replay.log"
    log.write_text("mpi replay failed\n", encoding="utf-8")
    events = []
    transitions = []

    launched = SimpleNamespace(
        monitor=SimpleNamespace(
            process=object(), status_dir=str(tmp_path / "status"), expected_generation=1
        )
    )
    stop_attempts = 0

    class Adapter:
        def launch(self, _ctx):
            return launched

        def wait_ready(self, _ctx, _launched):
            return None

        def discover_targets(self, _ctx, _launched):
            return ["http://target"]

        def stop(self, _ctx, _launched):
            nonlocal stop_attempts
            stop_attempts += 1
            events.append("cleanup")
            if stop_attempts <= failed_cleanup_attempts:
                raise RuntimeError(f"cleanup failed {stop_attempts}")

    class Heartbeat:
        def ensure_held(self):
            return None

    monkeypatch.setattr(
        "eval.lib.run_executor._run_replay_client",
        lambda *_args, **_kwargs: events.append("replay_exit") or 17,
    )
    real_capture = capture_replay_process_failure

    def capture(*args, **kwargs):
        events.append("capture_first_cause")
        return real_capture(*args, **kwargs)

    monkeypatch.setattr("eval.lib.replay_failure.capture_replay_process_failure", capture)

    def record_state(_plan, state, *, reason_code=None, detail=None, **data):
        transitions.append((state, reason_code, detail, data))
        if state == "failed":
            events.append("publish_terminal")

    monkeypatch.setattr("eval.lib.run_executor.write_run_state", record_state)

    assert _execute_run_locked(plan, Adapter(), object(), Heartbeat()) == 17
    assert events == [
        "replay_exit",
        "capture_first_cause",
        "cleanup",
        "cleanup",
        "publish_terminal",
    ]
    state, reason_code, detail, data = transitions[-1]
    assert state == "failed"
    assert reason_code == REASON_CODE
    assert detail == DETAIL
    assert data["exit_code"] == data["failure"]["exit_code"] == 17
    if expected_cleanup_error is None:
        assert "cleanup_error" not in data
    else:
        assert data["cleanup_error"] == expected_cleanup_error


def test_replay_failure_reason_and_controlled_detail_are_durable(tmp_path):
    plan = _plan(tmp_path)
    (Path(plan.bundle.logs_dir) / "replay.log").write_text("failed\n", encoding="utf-8")
    failure = capture_replay_process_failure(
        plan, exit_code=17, topology_arm=None, log_name="replay.log"
    )
    write_run_state(plan, "materialized")
    write_run_state(plan, "running", backend="mock")
    write_run_state(plan, "replaying", base_urls=["http://target"])
    write_run_state(
        plan,
        "failed",
        reason_code=REASON_CODE,
        detail=DETAIL,
        base_urls=["http://target"],
        exit_code=17,
        failure=failure,
    )

    status = StatusStore.run(plan.bundle.state_path).load()
    assert status is not None
    assert status.state == "FAILED"
    assert status.reason_code == REASON_CODE
    assert status.detail == DETAIL
    assert status.data["failure"] == failure


def test_replay_reason_override_is_narrow_and_legacy_failure_remains_valid(tmp_path):
    plan = _plan(tmp_path)
    _validate_run_state_payload(plan, "failed", {"error": "legacy failure"})

    with pytest.raises(ValueError, match="reason_code override"):
        _validate_run_state_payload(
            plan,
            "failed",
            {"exit_code": 1},
            reason_code=REASON_CODE,
            detail=DETAIL,
        )
    with pytest.raises(ValueError, match="reason_code override"):
        _validate_run_state_payload(
            plan,
            "cancelled",
            {"error": "cancelled"},
            reason_code=REASON_CODE,
            detail=DETAIL,
        )
