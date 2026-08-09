"""Bounded execution for finite native and scheduler-adapter commands.

Finite work is still a real process boundary, but it must never leave an MPI,
compiler, or verifier descendant running after timeout or failure.  This helper
owns one fresh process group and returns only after that group is gone.
"""

from __future__ import annotations

import os
import math
import signal
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence

from ..exception_notes import add_exception_note


class FiniteProcessError(RuntimeError):
    """A finite process could not satisfy its bounded ownership contract."""


class FiniteProcessTimeout(FiniteProcessError):
    def __init__(self, argv: Sequence[str], timeout_s: float, stdout: str, stderr: str) -> None:
        self.argv = tuple(argv)
        self.timeout_s = timeout_s
        self.stdout = stdout
        self.stderr = stderr
        super().__init__(f"finite process timed out after {timeout_s:.3f}s: {argv[0]}")


class FiniteProcessCancelled(FiniteProcessError):
    """A caller-requested cancellation reaped the complete process group."""

    def __init__(self, argv: Sequence[str], stdout: str, stderr: str) -> None:
        self.argv = tuple(argv)
        self.stdout = stdout
        self.stderr = stderr
        super().__init__(f"finite process cancelled: {argv[0]}")


def _group_exists(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _stop_group(pgid: int, *, deadline: float) -> None:
    if not _group_exists(pgid):
        return
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        return
    term_deadline = time.monotonic() + max(0.0, deadline - time.monotonic()) / 2.0
    while time.monotonic() < term_deadline:
        if not _group_exists(pgid):
            return
        time.sleep(min(0.02, max(0.0, term_deadline - time.monotonic())))
    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        return
    while time.monotonic() < deadline and _group_exists(pgid):
        time.sleep(min(0.02, max(0.0, deadline - time.monotonic())))
    if _group_exists(pgid):
        raise FiniteProcessError(f"process group {pgid} survived SIGKILL")


def _signal_group(pgid: int, sig: signal.Signals) -> None:
    try:
        os.killpg(pgid, sig)
    except ProcessLookupError:
        return


def _terminate_and_communicate(
    process: subprocess.Popen[str], *, grace_s: float
) -> tuple[str, str]:
    """Boundedly stop the group while continuing to drain and reap its leader."""
    deadline = time.monotonic() + max(0.0, grace_s)
    _signal_group(process.pid, signal.SIGTERM)
    try:
        stdout, stderr = process.communicate(timeout=max(0.0, deadline - time.monotonic()) / 2.0)
    except subprocess.TimeoutExpired:
        _signal_group(process.pid, signal.SIGKILL)
        try:
            stdout, stderr = process.communicate(timeout=max(0.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired as exc:
            raise FiniteProcessError(
                f"process {process.pid} did not become reapable after SIGKILL"
            ) from exc
    # communicate() reaped the leader. Any remaining process with this pgid is
    # now an independently lingering descendant and must be removed explicitly.
    if _group_exists(process.pid):
        _stop_group(process.pid, deadline=deadline)
    return stdout or "", stderr or ""


def run_finite(
    argv: Sequence[str],
    *,
    timeout_s: float,
    cwd: str | os.PathLike[str] | None = None,
    env: Mapping[str, str] | None = None,
    check: bool = False,
    termination_grace_s: float = 5.0,
    pass_fds: Sequence[int] = (),
    cancel_requested: Callable[[], bool] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run an argument vector in a fresh process group with bounded cleanup.

    ``cancel_requested`` is polled while the process is live.  It gives signal
    handlers and owning supervisors a prompt cancellation surface even when a
    native verifier or driver call would otherwise block until its full
    deadline.  Cancellation is typed and returns only after the process group
    has been reaped.
    """
    if not argv or any(not isinstance(item, str) or "\x00" in item for item in argv):
        raise ValueError("finite process argv must contain non-NUL strings")
    for name, value in (
        ("timeout_s", timeout_s),
        ("termination_grace_s", termination_grace_s),
    ):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{name} must be numeric")
        if not math.isfinite(float(value)) or value <= 0:
            raise ValueError("finite process deadlines must be finite and bounded")
    if any(isinstance(fd, bool) or not isinstance(fd, int) or fd < 0 for fd in pass_fds):
        raise ValueError("pass_fds must contain non-negative file descriptors")
    if cancel_requested is not None and not callable(cancel_requested):
        raise ValueError("cancel_requested must be callable or null")
    process = subprocess.Popen(  # noqa: S603 - validated argument vector
        list(argv),
        cwd=cwd,
        env=dict(env) if env is not None else None,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
        pass_fds=tuple(pass_fds),
    )
    deadline = time.monotonic() + float(timeout_s)
    try:
        while True:
            if cancel_requested is not None:
                cancelled = cancel_requested()
                if not isinstance(cancelled, bool):
                    raise ValueError("cancel_requested must return a bool")
                if cancelled:
                    stdout, stderr = _terminate_and_communicate(
                        process, grace_s=termination_grace_s
                    )
                    raise FiniteProcessCancelled(argv, stdout, stderr)
            remaining = max(0.0, deadline - time.monotonic())
            try:
                stdout, stderr = process.communicate(timeout=min(0.1, remaining))
                break
            except subprocess.TimeoutExpired as exc:
                if time.monotonic() < deadline:
                    continue
                try:
                    stdout, stderr = _terminate_and_communicate(
                        process, grace_s=termination_grace_s
                    )
                except Exception as cleanup_exc:
                    add_exception_note(exc, f"finite process cleanup failed: {cleanup_exc}")
                    raise FiniteProcessError(
                        f"timed-out finite process could not be reaped: {argv[0]}"
                    ) from cleanup_exc
                raise FiniteProcessTimeout(argv, timeout_s, stdout, stderr) from exc
    except BaseException as exc:
        if process.poll() is None or _group_exists(process.pid):
            try:
                _terminate_and_communicate(process, grace_s=termination_grace_s)
            except Exception as cleanup_exc:
                add_exception_note(exc, f"finite process cleanup failed: {cleanup_exc}")
        raise

    # communicate() cannot return while descendants still hold the captured
    # pipes, but a descendant may explicitly close them.  Such a daemon is a
    # contract violation for finite work and is reaped here.
    if _group_exists(process.pid):
        _stop_group(
            process.pid,
            deadline=time.monotonic() + max(0.0, termination_grace_s),
        )
        raise FiniteProcessError(f"finite process left descendants after exit: {argv[0]}")

    completed = subprocess.CompletedProcess(list(argv), int(process.returncode), stdout, stderr)
    if check and completed.returncode:
        raise subprocess.CalledProcessError(
            completed.returncode, completed.args, output=completed.stdout, stderr=completed.stderr
        )
    return completed


__all__ = [
    "FiniteProcessCancelled",
    "FiniteProcessError",
    "FiniteProcessTimeout",
    "run_finite",
]
