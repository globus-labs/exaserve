"""WP4: stale cleanup is receipt-bound and cannot pattern-kill strangers."""

from __future__ import annotations

import os
from pathlib import Path
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
    test_root = f"/tmp/xt-{os.getpid()}"
    monkeypatch.setenv("EXASERVE_RUNTIME_OWNERSHIP_ROOT", test_root)
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
    Path(root).rmdir()
    Path(test_root).rmdir()


def test_ownership_roots_reject_intermediate_symlinks_without_touching_target(
    tmp_path, monkeypatch
):
    shared_target = tmp_path / "simulated-shared"
    shared_target.mkdir()
    sentinel = shared_target / "sentinel"
    sentinel.write_text("unchanged", encoding="utf-8")

    receipt_link = tmp_path / "owned-link"
    receipt_link.symlink_to(shared_target, target_is_directory=True)
    monkeypatch.setenv("EXASERVE_PROCESS_OWNERSHIP_ROOT", str(receipt_link))
    with pytest.raises(ProcessOwnershipError, match="unsafe component"):
        ProcessOwnershipRegistry(deployment_id="d", generation=1, rank=0)

    runtime_link = tmp_path / "runtime-link"
    runtime_link.symlink_to(shared_target, target_is_directory=True)
    monkeypatch.setenv("EXASERVE_RUNTIME_OWNERSHIP_ROOT", str(runtime_link))
    with pytest.raises(ProcessOwnershipError, match="unsafe component"):
        generation_runtime_root("d", 1, 0)
    assert sentinel.read_text(encoding="utf-8") == "unchanged"
    assert sorted(path.name for path in shared_target.iterdir()) == ["sentinel"]


def test_ownership_root_rejects_declared_shared_path_before_creation(tmp_path, monkeypatch):
    candidate = f"/home/{os.getuid()}-must-not-create-exaserve-ownership"
    monkeypatch.setenv("EXASERVE_PROCESS_OWNERSHIP_ROOT", candidate)
    with pytest.raises(ProcessOwnershipError, match="not a private local path"):
        ProcessOwnershipRegistry(deployment_id="d", generation=1, rank=0)
    assert not os.path.lexists(candidate)


def test_release_never_follows_intermediate_owned_path_symlink(tmp_path, monkeypatch):
    root = tmp_path / "owned"
    monkeypatch.setenv("EXASERVE_PROCESS_OWNERSHIP_ROOT", str(root))
    registry = ProcessOwnershipRegistry(deployment_id="d", generation=1, rank=0)
    runtime_parent = root / "runtime"
    runtime = runtime_parent / "generation"
    runtime.mkdir(parents=True)
    child = _sleeping_child()
    receipt = registry.record(
        "ray",
        pid=child.pid,
        pgid=os.getpgid(child.pid),
        argv=child.args,
        temp_paths=(str(runtime),),
    )
    os.killpg(os.getpgid(child.pid), 15)
    child.wait(timeout=5)

    runtime_parent.rename(root / "runtime-original")
    simulated_shared = tmp_path / "simulated-shared-cleanup"
    redirected = simulated_shared / "generation"
    redirected.mkdir(parents=True)
    sentinel = redirected / "sentinel"
    sentinel.write_text("unchanged", encoding="utf-8")
    runtime_parent.symlink_to(simulated_shared, target_is_directory=True)

    with pytest.raises(ProcessOwnershipError, match="unsafe component"):
        registry.release("ray")
    assert Path(receipt).is_file()
    assert sentinel.read_text(encoding="utf-8") == "unchanged"


def test_compact_runtime_root_remains_receipt_owned(tmp_path, monkeypatch):
    monkeypatch.setenv("EXASERVE_PROCESS_OWNERSHIP_ROOT", str(tmp_path / "receipts"))
    monkeypatch.setenv("EXASERVE_RUNTIME_OWNERSHIP_ROOT", str(tmp_path / "runtime"))
    child = _sleeping_child()
    runtime = generation_runtime_root("d", 1, 0)
    os.makedirs(runtime, exist_ok=True)
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


def test_stale_cleanup_removes_the_exact_generation_local_state(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from exaserve.plan import runtime_environment

    monkeypatch.setenv("EXASERVE_PROCESS_OWNERSHIP_ROOT", str(tmp_path / "owned"))

    def local_state(plan, generation, rank=0):
        del rank
        return str(tmp_path / "state" / plan.deployment_id / f"g{generation}")

    monkeypatch.setattr(runtime_environment, "default_local_state_root", local_state)
    owned = _sleeping_child()
    state = local_state(SimpleNamespace(deployment_id="current"), 1)
    os.makedirs(state)
    registry = ProcessOwnershipRegistry(deployment_id="current", generation=1, rank=0)
    registry.record(
        "ray",
        pid=owned.pid,
        pgid=os.getpgid(owned.pid),
        argv=owned.args,
        temp_paths=(state,),
    )
    os.killpg(os.getpgid(owned.pid), 15)
    owned.wait(timeout=5)
    assert cleanup_stale_owned_processes(deployment_id="current", generation=2, deadline_s=2) == 1
    assert not os.path.exists(state)


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


def test_stale_cleanup_preserves_republished_current_generation_state(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from exaserve.plan import runtime_environment

    monkeypatch.setenv("EXASERVE_PROCESS_OWNERSHIP_ROOT", str(tmp_path / "owned"))

    def local_state(plan, generation, rank=0):
        del rank
        return str(tmp_path / "state" / plan.deployment_id / f"g{generation}")

    monkeypatch.setattr(runtime_environment, "default_local_state_root", local_state)
    dead = _sleeping_child()
    state = local_state(SimpleNamespace(deployment_id="current"), 2)
    seed = Path(state) / "cache" / "vllm" / "modelinfos" / "seed.json"
    seed.parent.mkdir(parents=True)
    seed.write_text("reviewed", encoding="utf-8")
    registry = ProcessOwnershipRegistry(deployment_id="current", generation=2, rank=0)
    receipt = registry.record(
        "ray",
        pid=dead.pid,
        pgid=os.getpgid(dead.pid),
        argv=dead.args,
        temp_paths=(state,),
    )
    os.killpg(os.getpgid(dead.pid), 15)
    dead.wait(timeout=5)

    assert cleanup_stale_owned_processes(deployment_id="current", generation=2, deadline_s=1) == 1
    assert not os.path.exists(receipt)
    assert seed.read_text(encoding="utf-8") == "reviewed"


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
