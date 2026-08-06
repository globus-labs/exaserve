"""Runtime supervisor and managed components (plan WP4, audit IMP-B01/B03).

``ManagedComponent`` wraps one OS-boundary child with typed lifecycle:
start / observe / wait / stop, an explicit owner scope, its own process group,
and bounded termination. ``RuntimeSupervisor`` owns a registry of them, decides
terminal state, and preserves the FIRST causal failure.

Invariants this closes (audit IMP-B03):
- every declared long-lived component is polled, including the Ray head;
- an UNEXPECTED exit is fatal even when the exit code is 0;
- children run in their own process group so cleanup reaches descendants;
- signal handling is installed before any child starts, not after READY;
- the first cause survives later cleanup errors.
"""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import time
from dataclasses import dataclass, field
from typing import Callable, Optional, Sequence

from .contracts import ComponentState, OwnerScope


class SupervisorError(RuntimeError):
    pass


def _group_has_members(pgid: Optional[int]) -> bool:
    """True if any process still belongs to this group (IMP-B03)."""
    if pgid is None:
        return False
    try:
        os.killpg(pgid, 0)
        return True
    except (ProcessLookupError, OSError):
        return False


@dataclass
class FirstCause:
    component_id: str
    reason_code: str
    detail: str
    exit_code: Optional[int] = None
    at: float = field(default_factory=time.time)

    def __str__(self) -> str:
        return (f"{self.component_id}: {self.reason_code} "
                f"(exit={self.exit_code}) {self.detail}")


@dataclass
class ManagedComponent:
    """One supervised OS-boundary child (plan §3.1)."""

    component_id: str
    argv: Sequence[str]
    owner_scope: str = OwnerScope.GLOBAL.value
    owner_rank: Optional[int] = None
    long_lived: bool = True          # unexpected exit is fatal
    env: Optional[dict] = None
    cwd: Optional[str] = None
    stdout: Optional[object] = None  # file object; NEVER parsed for lifecycle
    # A finite component additionally validates its result before succeeding.
    result_check: Optional[Callable[[], tuple[bool, str]]] = None

    process: Optional[subprocess.Popen] = None
    state: str = ComponentState.NEW.value
    _stopping: bool = False
    _pgid: Optional[int] = None   # captured at start; getpgid fails post-reap

    # -- lifecycle ---------------------------------------------------------
    def start(self) -> dict:
        self.state = ComponentState.STARTING.value
        # IMP-B03: own process group so termination reaches descendants
        # (Ray/Serve/engine children previously escaped cleanup).
        self.process = subprocess.Popen(
            list(self.argv), env=self.env, cwd=self.cwd,
            stdout=self.stdout, stderr=subprocess.STDOUT if self.stdout else None,
            start_new_session=True,
        )
        self.state = ComponentState.RUNNING.value
        try:
            self._pgid = os.getpgid(self.process.pid)
        except OSError:
            self._pgid = self.process.pid  # start_new_session => pid == pgid
        return {"pid": self.process.pid, "pgid": self._pgid}

    def observe(self) -> tuple[str, Optional[int]]:
        """Return (state, exit_code). Never blocks."""
        if self.process is None:
            return self.state, None
        code = self.process.poll()
        if code is None:
            return self.state, None
        if self._stopping:
            self.state = ComponentState.STOPPED.value
        else:
            # IMP-B03: an unexpected exit is FAILED regardless of exit code —
            # a long-lived service that returns 0 has still disappeared.
            self.state = (ComponentState.STOPPED.value
                          if not self.long_lived and code == 0
                          else ComponentState.FAILED.value)
        return self.state, code

    def stop(self, reason: str = "shutdown", deadline_s: float = 30.0) -> None:
        """Idempotent, bounded: TERM the group, then KILL the group.

        IMP-B03: the group is reaped even when the DIRECT child has already
        exited — a launcher that backgrounds work leaves descendants alive in
        its group, and returning early here let them survive as orphans.
        """
        self._stopping = True
        if self.process is None:
            self.state = ComponentState.STOPPED.value
            return
        pgid = self._pgid
        child_alive = self.process.poll() is None
        if child_alive:
            self.state = ComponentState.STOPPING.value

        def _signal_group(sig) -> bool:
            """Signal the group; True if anything was there to signal."""
            if pgid is None:
                return False
            try:
                os.killpg(pgid, sig)
                return True
            except ProcessLookupError:
                return False
            except OSError:
                return False

        if not _signal_group(signal.SIGTERM) and child_alive:
            with contextlib.suppress(OSError, ProcessLookupError):
                self.process.terminate()
        if child_alive:
            try:
                self.process.wait(timeout=deadline_s)
            except subprocess.TimeoutExpired:
                pass
        # Give the group a grace window, then force-kill any survivors.
        deadline = time.monotonic() + min(deadline_s, 10.0)
        while time.monotonic() < deadline:
            if not _group_has_members(pgid):
                break
            time.sleep(0.1)
        if _group_has_members(pgid):
            _signal_group(signal.SIGKILL)
            grace = time.monotonic() + 10.0
            while time.monotonic() < grace and _group_has_members(pgid):
                time.sleep(0.1)
        with contextlib.suppress(subprocess.TimeoutExpired):
            self.process.wait(timeout=5)
        self.state = ComponentState.STOPPED.value


class RuntimeSupervisor:
    """Allocation-head coordinator: owns components, terminal state, cleanup."""

    def __init__(self, *, poll_interval_s: float = 2.0) -> None:
        self.components: dict[str, ManagedComponent] = {}
        self.poll_interval_s = poll_interval_s
        self.first_cause: Optional[FirstCause] = None
        self._shutdown_requested = False
        self._signals_installed = False

    # -- registry ----------------------------------------------------------
    def register(self, component: ManagedComponent) -> ManagedComponent:
        if component.component_id in self.components:
            raise SupervisorError(f"duplicate component {component.component_id}")
        self.components[component.component_id] = component
        return component

    def install_signal_handlers(self) -> None:
        """IMP-B03: install BEFORE starting children so a partial startup
        still drains instead of dying abruptly."""
        if self._signals_installed:
            return

        def _handler(signum, _frame):
            self.request_shutdown(f"signal {signum}")

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                signal.signal(sig, _handler)
            except ValueError:
                pass  # not main thread (tests)
        self._signals_installed = True

    def request_shutdown(self, reason: str) -> None:
        self._shutdown_requested = True
        self.record_cause("supervisor", "SHUTDOWN_REQUESTED", reason)

    def record_cause(self, component_id: str, reason_code: str, detail: str,
                     exit_code: Optional[int] = None) -> None:
        """First cause wins; later cleanup errors never overwrite it."""
        if self.first_cause is None:
            self.first_cause = FirstCause(component_id, reason_code, detail, exit_code)

    # -- supervision loop --------------------------------------------------
    def start_all(self) -> None:
        self.install_signal_handlers()
        for comp in self.components.values():
            comp.start()

    def poll_once(self) -> Optional[FirstCause]:
        """Poll EVERY registered component once. Returns a cause if fatal."""
        for comp in self.components.values():
            state, code = comp.observe()
            if state == ComponentState.FAILED.value:
                self.record_cause(
                    comp.component_id, "UNEXPECTED_EXIT",
                    f"{comp.component_id} exited unexpectedly", code)
                return self.first_cause
            if (state == ComponentState.STOPPED.value and not comp.long_lived
                    and comp.result_check is not None):
                ok, why = comp.result_check()
                if not ok:
                    self.record_cause(comp.component_id, "INVALID_RESULT", why, code)
                    return self.first_cause
        return None

    def supervise(self, *, until: Optional[Callable[[], bool]] = None,
                  timeout_s: Optional[float] = None) -> Optional[FirstCause]:
        """Run until a component fails, `until()` is true, or timeout."""
        deadline = None if timeout_s is None else time.monotonic() + timeout_s
        while not self._shutdown_requested:
            cause = self.poll_once()
            if cause is not None:
                return cause
            if until is not None and until():
                return None
            if deadline is not None and time.monotonic() > deadline:
                self.record_cause("supervisor", "DEADLINE", f"exceeded {timeout_s}s")
                return self.first_cause
            time.sleep(self.poll_interval_s)
        return self.first_cause

    def shutdown(self, *, drain_s: float = 30.0) -> None:
        """Stop every component in reverse registration order, bounded."""
        for comp in reversed(list(self.components.values())):
            try:
                comp.stop(deadline_s=drain_s)
            except Exception as exc:  # cleanup error must not hide first cause
                self.record_cause(comp.component_id, "CLEANUP_ERROR", str(exc))

    def exit_code(self) -> int:
        """0 only when nothing failed (plan WP4 exit gate)."""
        if self.first_cause is None:
            return 0
        if self.first_cause.reason_code == "SHUTDOWN_REQUESTED":
            return 143
        return self.first_cause.exit_code or 1
