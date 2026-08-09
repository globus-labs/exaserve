"""Finite native work owns and reaps its complete process group."""

from __future__ import annotations

import sys
import threading
import time

import pytest

from exaserve.control.finite_process import (
    FiniteProcessCancelled,
    FiniteProcessTimeout,
    run_finite,
)


def _alive_non_zombie(pid: int) -> bool:
    try:
        state = open(f"/proc/{pid}/stat", encoding="utf-8").read().split()[2]
    except FileNotFoundError:
        return False
    return state != "Z"


def test_timeout_reaps_a_grandchild_that_closed_capture_pipes(tmp_path):
    pid_file = tmp_path / "grandchild.pid"
    program = (
        "import os,subprocess,sys,time; "
        "p=subprocess.Popen([sys.executable,'-c',"
        "'import os,time; os.close(1); os.close(2); time.sleep(60)']); "
        "open(sys.argv[1],'w').write(str(p.pid)); time.sleep(60)"
    )
    with pytest.raises(FiniteProcessTimeout):
        run_finite(
            [sys.executable, "-c", program, str(pid_file)], timeout_s=0.3, termination_grace_s=0.1
        )
    pid = int(pid_file.read_text())
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and _alive_non_zombie(pid):
        time.sleep(0.02)
    assert not _alive_non_zombie(pid)


def test_run_finite_never_invokes_a_shell(tmp_path):
    sentinel = tmp_path / "nope"
    payload = f"$(touch {sentinel})"
    result = run_finite(
        [sys.executable, "-c", "import sys; print(sys.argv[1])", payload], timeout_s=2, check=True
    )
    assert result.stdout.strip() == payload
    assert not sentinel.exists()


def test_cancellation_promptly_reaps_the_complete_process_group(tmp_path):
    pid_file = tmp_path / "grandchild.pid"
    program = (
        "import subprocess,sys,time; "
        "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); "
        "open(sys.argv[1],'w').write(str(p.pid)); time.sleep(60)"
    )
    cancellation = threading.Event()
    timer = threading.Timer(0.2, cancellation.set)
    timer.start()
    started = time.monotonic()
    try:
        with pytest.raises(FiniteProcessCancelled):
            run_finite(
                [sys.executable, "-c", program, str(pid_file)],
                timeout_s=30,
                termination_grace_s=0.5,
                cancel_requested=cancellation.is_set,
            )
    finally:
        timer.join()
    assert time.monotonic() - started < 2
    pid = int(pid_file.read_text())
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and _alive_non_zombie(pid):
        time.sleep(0.02)
    assert not _alive_non_zombie(pid)


@pytest.mark.parametrize("deadline", [True, float("nan"), float("inf"), 0.0, -1.0])
def test_run_finite_rejects_unbounded_or_invalid_deadlines(deadline):
    with pytest.raises(ValueError, match="deadline|timeout_s"):
        run_finite([sys.executable, "-c", "pass"], timeout_s=deadline)
