from __future__ import annotations

import json
import os
import threading
import time
from types import SimpleNamespace

import pytest

from exaserve.control.process_handshake import (
    HANDSHAKE_SCHEMA_VERSION,
    prepare_ready_handshake,
    wait_ready_handshake,
)


class _Process:
    def __init__(self, pid: int = 1234):
        self.pid = pid
        self.returncode = None

    def poll(self):
        return self.returncode


def test_nonce_and_pid_bound_handshake_is_consumed(tmp_path):
    path, token = prepare_ready_handshake(tmp_path, "worker")
    process = _Process()

    def publish():
        time.sleep(0.02)
        temporary = path + ".tmp"
        with open(temporary, "w", encoding="utf-8") as handle:
            json.dump(
                {
                    "schema_version": HANDSHAKE_SCHEMA_VERSION,
                    "pid": process.pid,
                    "token": token,
                },
                handle,
            )
        os.replace(temporary, path)

    thread = threading.Thread(target=publish)
    thread.start()
    payload = wait_ready_handshake(process, path=path, token=token, timeout_s=1.0)
    thread.join()
    assert payload["pid"] == process.pid
    assert not os.path.exists(path)


@pytest.mark.parametrize(
    "override",
    [
        {"pid": 9999},
        {"pid": True},
        {"token": "stale"},
        {"schema_version": True},
        {"schema_version": 99},
    ],
)
def test_handshake_rejects_wrong_identity(tmp_path, override):
    path, token = prepare_ready_handshake(tmp_path, "worker")
    process = _Process()
    payload = {"schema_version": 1, "pid": process.pid, "token": token} | override
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)
    with pytest.raises(RuntimeError, match="identity mismatch"):
        wait_ready_handshake(process, path=path, token=token, timeout_s=0.2)


def test_handshake_reports_child_exit_instead_of_hanging(tmp_path):
    path, token = prepare_ready_handshake(tmp_path, "worker")
    process = _Process()
    process.returncode = 17
    with pytest.raises(RuntimeError, match="exited 17"):
        wait_ready_handshake(process, path=path, token=token, timeout_s=0.2)


def test_handshake_requires_a_real_started_process(tmp_path):
    path, token = prepare_ready_handshake(tmp_path, "worker")
    with pytest.raises(ValueError, match="PID"):
        wait_ready_handshake(
            SimpleNamespace(pid=None, poll=lambda: None),
            path=path,
            token=token,
        )


def test_handshake_rejects_boolean_pid_alias(tmp_path):
    path, token = prepare_ready_handshake(tmp_path, "worker")
    with pytest.raises(ValueError, match="PID"):
        wait_ready_handshake(
            SimpleNamespace(pid=True, poll=lambda: None),
            path=path,
            token=token,
        )
