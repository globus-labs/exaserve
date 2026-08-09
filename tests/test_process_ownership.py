"""WP4: stale cleanup is receipt-bound and cannot pattern-kill strangers."""

from __future__ import annotations

import os
import subprocess
import sys
import threading

import pytest

from exaserve.state.process_ownership import (
    ProcessOwnershipError,
    ProcessOwnershipRegistry,
    cleanup_stale_owned_processes,
    generation_runtime_root,
)


def _sleeping_child():
    return subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(300)"], start_new_session=True
    )


def test_ray_runtime_path_is_compact_enough_for_linux_unix_sockets(monkeypatch):
    monkeypatch.delenv("EXASERVE_RUNTIME_OWNERSHIP_ROOT", raising=False)
    root = generation_runtime_root("deployment-name-that-may-be-eighty-characters-long", 10**20, 63)
    # Ray 2.49 appends a timestamp/pid session name and its longest critical
    # socket suffix. Leave margin below Linux's 107-byte sockaddr_un limit.
    representative = os.path.join(
        root,
        "ray",
        "session_2026-08-08_08-39-29_259275_9999999",
        "sockets",
        "plasma_store",
    )
    assert len(representative.encode()) < 107


def test_compact_runtime_root_remains_receipt_owned(tmp_path, monkeypatch):
    monkeypatch.setenv("EXASERVE_PROCESS_OWNERSHIP_ROOT", str(tmp_path / "receipts"))
    monkeypatch.setenv("EXASERVE_RUNTIME_OWNERSHIP_ROOT", str(tmp_path / "runtime"))
    child = _sleeping_child()
    runtime = generation_runtime_root("d", 1, 0)
    os.makedirs(runtime)
    registry = ProcessOwnershipRegistry(deployment_id="d", generation=1, rank=0)
    receipt = registry.record(
        "ray",
        pid=child.pid,
        pgid=os.getpgid(child.pid),
        argv=child.args,
        temp_paths=(runtime,),
    )
    os.killpg(os.getpgid(child.pid), 15)
    child.wait(timeout=5)
    registry.release("ray")
    assert not os.path.exists(receipt)
    assert not os.path.exists(runtime)


def test_stale_cleanup_reaps_only_an_exact_owned_process_group(tmp_path, monkeypatch):
    monkeypatch.setenv("EXASERVE_PROCESS_OWNERSHIP_ROOT", str(tmp_path / "owned"))
    owned = _sleeping_child()
    stranger = _sleeping_child()
    runtime = tmp_path / "owned" / "runtime" / "old"
    runtime.mkdir(parents=True)
    registry = ProcessOwnershipRegistry(deployment_id="current", generation=1, rank=0)
    registry.record(
        "ray",
        pid=owned.pid,
        pgid=os.getpgid(owned.pid),
        argv=owned.args,
        temp_paths=(str(runtime),),
    )
    reaper = threading.Thread(target=owned.wait, daemon=True)
    reaper.start()
    try:
        assert (
            cleanup_stale_owned_processes(deployment_id="current", generation=2, deadline_s=5) == 1
        )
        reaper.join(2)
        assert owned.poll() is not None
        assert stranger.poll() is None
        assert not runtime.exists()
    finally:
        if stranger.poll() is None:
            os.killpg(os.getpgid(stranger.pid), 15)
            stranger.wait(timeout=5)
        if owned.poll() is None:
            os.killpg(os.getpgid(owned.pid), 9)
            owned.wait(timeout=5)


def test_stale_cleanup_never_reaps_another_live_deployment(tmp_path, monkeypatch):
    monkeypatch.setenv("EXASERVE_PROCESS_OWNERSHIP_ROOT", str(tmp_path / "owned"))
    other = _sleeping_child()
    registry = ProcessOwnershipRegistry(deployment_id="other", generation=1, rank=0)
    receipt = registry.record(
        "ray",
        pid=other.pid,
        pgid=os.getpgid(other.pid),
        argv=other.args,
    )
    try:
        assert (
            cleanup_stale_owned_processes(deployment_id="current", generation=2, deadline_s=1) == 0
        )
        assert other.poll() is None
        assert os.path.exists(receipt)
    finally:
        os.killpg(os.getpgid(other.pid), 15)
        other.wait(timeout=5)
        registry.release("ray")


@pytest.mark.parametrize("existing_generation", [2, 3])
def test_stale_cleanup_refuses_equal_or_newer_live_generation(
    tmp_path, monkeypatch, existing_generation
):
    monkeypatch.setenv("EXASERVE_PROCESS_OWNERSHIP_ROOT", str(tmp_path / "owned"))
    existing = _sleeping_child()
    registry = ProcessOwnershipRegistry(
        deployment_id="current", generation=existing_generation, rank=0
    )
    receipt = registry.record(
        "ray",
        pid=existing.pid,
        pgid=os.getpgid(existing.pid),
        argv=existing.args,
    )
    try:
        with pytest.raises(ProcessOwnershipError, match="live (same|newer) generation"):
            cleanup_stale_owned_processes(deployment_id="current", generation=2, deadline_s=1)
        assert existing.poll() is None
        assert os.path.exists(receipt)
    finally:
        os.killpg(os.getpgid(existing.pid), 15)
        existing.wait(timeout=5)
        registry.release("ray")


def test_stale_cleanup_garbage_collects_a_dead_current_generation(tmp_path, monkeypatch):
    monkeypatch.setenv("EXASERVE_PROCESS_OWNERSHIP_ROOT", str(tmp_path / "owned"))
    dead = _sleeping_child()
    runtime = tmp_path / "owned" / "runtime" / "dead-current"
    runtime.mkdir(parents=True)
    registry = ProcessOwnershipRegistry(deployment_id="current", generation=2, rank=0)
    receipt = registry.record(
        "ray",
        pid=dead.pid,
        pgid=os.getpgid(dead.pid),
        argv=dead.args,
        temp_paths=(str(runtime),),
    )
    os.killpg(os.getpgid(dead.pid), 15)
    dead.wait(timeout=5)
    assert cleanup_stale_owned_processes(deployment_id="current", generation=2, deadline_s=1) == 1
    assert not os.path.exists(receipt)
    assert not runtime.exists()


def test_normal_release_removes_receipt_and_owned_runtime(tmp_path, monkeypatch):
    monkeypatch.setenv("EXASERVE_PROCESS_OWNERSHIP_ROOT", str(tmp_path / "owned"))
    child = _sleeping_child()
    runtime = tmp_path / "owned" / "runtime" / "current"
    runtime.mkdir(parents=True)
    registry = ProcessOwnershipRegistry(deployment_id="d", generation=1, rank=0)
    receipt = registry.record(
        "ray",
        pid=child.pid,
        pgid=os.getpgid(child.pid),
        argv=child.args,
        temp_paths=(str(runtime),),
    )
    os.killpg(os.getpgid(child.pid), 15)
    child.wait(timeout=5)
    registry.release("ray")
    assert not os.path.exists(receipt)
    assert not runtime.exists()


def test_release_cleanup_failure_remains_retryable(tmp_path, monkeypatch):
    from exaserve.state import process_ownership as ownership

    monkeypatch.setenv("EXASERVE_PROCESS_OWNERSHIP_ROOT", str(tmp_path / "owned"))
    child = _sleeping_child()
    runtime = tmp_path / "owned" / "runtime" / "current"
    runtime.mkdir(parents=True)
    registry = ProcessOwnershipRegistry(deployment_id="d", generation=1, rank=0)
    receipt = registry.record(
        "ray",
        pid=child.pid,
        pgid=os.getpgid(child.pid),
        argv=child.args,
        temp_paths=(str(runtime),),
    )
    os.killpg(os.getpgid(child.pid), 15)
    child.wait(timeout=5)
    real_cleanup = ownership._remove_owned_paths

    def fail_cleanup(_receipt):
        raise ProcessOwnershipError("injected")

    monkeypatch.setattr(ownership, "_remove_owned_paths", fail_cleanup)
    with pytest.raises(ProcessOwnershipError, match="injected"):
        registry.release("ray")
    assert os.path.exists(receipt)
    monkeypatch.setattr(ownership, "_remove_owned_paths", real_cleanup)
    registry.release("ray")
    assert not os.path.exists(receipt)
