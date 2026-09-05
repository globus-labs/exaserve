"""The node-local watchdog survives and cleans after NodeSupervisor death."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from exaserve.control.process_guardian import guardian_argv
from exaserve.state.process_ownership import (
    ProcessOwnershipError,
    ProcessOwnershipRegistry,
    cleanup_owned_component_processes,
    process_start_ticks,
)


def _wait_for_path(path: Path, timeout_s: float = 5.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(0.02)
    raise AssertionError(f"{path} was not created within {timeout_s:g}s")


def _wait_process_gone(pid: int, timeout_s: float = 5.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.02)
    raise AssertionError(f"pid {pid} survived {timeout_s:g}s")


def _child_command(pid_path: Path, *, exit_code: int | None = None) -> list[str]:
    if exit_code is None:
        body = (
            "import os,time,pathlib; "
            f"pathlib.Path({str(pid_path)!r}).write_text(str(os.getpid())); "
            "time.sleep(300)"
        )
    else:
        body = f"raise SystemExit({exit_code})"
    return [sys.executable, "-c", body]


def _start_guardian(
    tmp_path: Path,
    *,
    owner_pid: int,
    owner_ticks: int,
    child_argv: list[str],
) -> tuple[subprocess.Popen, ProcessOwnershipRegistry, str, Path]:
    runtime = tmp_path / "runtime" / "generation"
    runtime.mkdir(parents=True)
    argv = guardian_argv(
        child_argv,
        owner_pid=owner_pid,
        owner_start_ticks=owner_ticks,
        deployment_id="deployment",
        generation=7,
        rank=1,
        cleanup_deadline_s=2.0,
        receipt_wait_s=2.0,
    )
    environment = dict(os.environ)
    environment["EXASERVE_PROCESS_OWNERSHIP_ROOT"] = str(tmp_path / "owned")
    environment["EXASERVE_RUNTIME_OWNERSHIP_ROOT"] = str(tmp_path / "runtime")
    source_root = str(Path(__file__).resolve().parents[1] / "src")
    inherited_pythonpath = environment.get("PYTHONPATH", "")
    environment["PYTHONPATH"] = os.pathsep.join(
        item for item in (source_root, inherited_pythonpath) if item
    )
    guardian = subprocess.Popen(argv, env=environment, start_new_session=True)
    registry = ProcessOwnershipRegistry(deployment_id="deployment", generation=7, rank=1)
    receipt = registry.record(
        "ray",
        pid=guardian.pid,
        pgid=os.getpgid(guardian.pid),
        argv=argv,
        temp_paths=(str(runtime),),
    )
    return guardian, registry, receipt, runtime


def test_guardian_owner_loss_reaps_child_and_releases_exact_receipt(tmp_path, monkeypatch):
    monkeypatch.setenv("EXASERVE_PROCESS_OWNERSHIP_ROOT", str(tmp_path / "owned"))
    monkeypatch.setenv("EXASERVE_RUNTIME_OWNERSHIP_ROOT", str(tmp_path / "runtime"))
    owner = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(300)"], start_new_session=True
    )
    child_pid_path = tmp_path / "child.json"
    guardian = None
    child_pid = None
    try:
        guardian, _, receipt, runtime = _start_guardian(
            tmp_path,
            owner_pid=owner.pid,
            owner_ticks=process_start_ticks(owner.pid),
            child_argv=_child_command(child_pid_path),
        )
        _wait_for_path(child_pid_path)
        child_pid = int(child_pid_path.read_text())

        os.killpg(os.getpgid(owner.pid), signal.SIGKILL)
        owner.wait(timeout=5)
        assert guardian.wait(timeout=5) == 1
        _wait_process_gone(child_pid)
        assert not os.path.exists(receipt)
        assert not runtime.exists()
    finally:
        if owner.poll() is None:
            os.killpg(os.getpgid(owner.pid), signal.SIGKILL)
            owner.wait(timeout=5)
        if guardian is not None and guardian.poll() is None:
            os.killpg(os.getpgid(guardian.pid), signal.SIGKILL)
            guardian.wait(timeout=5)
        if child_pid is not None:
            try:
                os.kill(child_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_guardian_sigkill_leaves_exact_child_receipt_for_rank_cleanup(tmp_path, monkeypatch):
    monkeypatch.setenv("EXASERVE_PROCESS_OWNERSHIP_ROOT", str(tmp_path / "owned"))
    monkeypatch.setenv("EXASERVE_RUNTIME_OWNERSHIP_ROOT", str(tmp_path / "runtime"))
    child_pid_path = tmp_path / "child.json"
    guardian, registry, guardian_receipt, runtime = _start_guardian(
        tmp_path,
        owner_pid=os.getpid(),
        owner_ticks=process_start_ticks(os.getpid()),
        child_argv=_child_command(child_pid_path),
    )
    child_pid = None
    try:
        _wait_for_path(child_pid_path)
        child_pid = int(child_pid_path.read_text())
        receipt_dir = tmp_path / "owned" / "receipts"
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and len(list(receipt_dir.glob("*.json"))) < 2:
            time.sleep(0.02)
        assert len(list(receipt_dir.glob("*.json"))) == 2
        with pytest.raises(ProcessOwnershipError, match="guardian is still live"):
            cleanup_owned_component_processes(
                deployment_id="deployment",
                generation=7,
                rank=1,
                component_id="ray_child",
                deadline_s=1,
            )

        os.killpg(os.getpgid(guardian.pid), signal.SIGKILL)
        guardian.wait(timeout=5)
        os.kill(child_pid, 0)  # separate group survived; exact receipt must recover it
        assert (
            cleanup_owned_component_processes(
                deployment_id="deployment",
                generation=7,
                rank=1,
                component_id="ray_child",
                deadline_s=5,
            )
            == 1
        )
        _wait_process_gone(child_pid)
        registry.release("ray")
        assert not os.path.exists(guardian_receipt)
        assert not runtime.exists()
        assert list(receipt_dir.glob("*.json")) == []
    finally:
        if guardian.poll() is None:
            os.killpg(os.getpgid(guardian.pid), signal.SIGKILL)
            guardian.wait(timeout=5)
        if child_pid is not None:
            try:
                os.kill(child_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_guardian_normal_stop_leaves_artifacts_for_parent_diagnostics(tmp_path, monkeypatch):
    monkeypatch.setenv("EXASERVE_PROCESS_OWNERSHIP_ROOT", str(tmp_path / "owned"))
    monkeypatch.setenv("EXASERVE_RUNTIME_OWNERSHIP_ROOT", str(tmp_path / "runtime"))
    child_pid_path = tmp_path / "child.json"
    guardian, registry, receipt, runtime = _start_guardian(
        tmp_path,
        owner_pid=os.getpid(),
        owner_ticks=process_start_ticks(os.getpid()),
        child_argv=_child_command(child_pid_path),
    )
    _wait_for_path(child_pid_path)
    child_pid = int(child_pid_path.read_text())
    os.killpg(os.getpgid(guardian.pid), signal.SIGTERM)
    assert guardian.wait(timeout=5) == 0
    _wait_process_gone(child_pid)
    assert os.path.exists(receipt)
    assert runtime.exists()
    registry.release("ray")
    assert not os.path.exists(receipt)
    assert not runtime.exists()


def test_guardian_propagates_unexpected_child_exit(tmp_path, monkeypatch):
    monkeypatch.setenv("EXASERVE_PROCESS_OWNERSHIP_ROOT", str(tmp_path / "owned"))
    monkeypatch.setenv("EXASERVE_RUNTIME_OWNERSHIP_ROOT", str(tmp_path / "runtime"))
    guardian, registry, receipt, _ = _start_guardian(
        tmp_path,
        owner_pid=os.getpid(),
        owner_ticks=process_start_ticks(os.getpid()),
        child_argv=_child_command(tmp_path / "unused", exit_code=7),
    )
    assert guardian.wait(timeout=5) == 7
    assert os.path.exists(receipt)
    registry.release("ray")


def test_guardian_command_is_an_argument_vector_not_serialized_shell(tmp_path):
    argv = guardian_argv(
        [sys.executable, "-c", "print('a value with spaces')"],
        owner_pid=os.getpid(),
        owner_start_ticks=process_start_ticks(os.getpid()),
        deployment_id="d",
        generation=1,
        rank=0,
        cleanup_deadline_s=1,
    )
    child_index = argv.index("--child")
    assert argv[child_index + 1 :] == [
        sys.executable,
        "-c",
        "print('a value with spaces')",
    ]
    assert json.dumps(argv)
