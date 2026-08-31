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
import math
import os
import signal
import subprocess
import threading
import time
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Callable, Mapping, Optional, Sequence

from ..exception_notes import add_exception_note
from .contracts import ComponentState, OwnerScope


class SupervisorError(RuntimeError):
    pass


class BoundedOutputCapture:
    """Continuously drain a child pipe while retaining only a bounded tail.

    This is diagnostic evidence, never a lifecycle or readiness input.  The
    drain thread prevents a noisy child from blocking on a full pipe while the
    fixed-size tail prevents unbounded controller memory growth.
    """

    def __init__(self, *, max_bytes: int = 64 << 10) -> None:
        if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 1:
            raise ValueError("capture max_bytes must be a positive integer")
        self.max_bytes = max_bytes
        self._tail = bytearray()
        self._total_bytes = 0
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._error: Optional[BaseException] = None

    def start(self, stream) -> None:
        if self._thread is not None:
            raise RuntimeError("output capture has already been started")

        def _drain() -> None:
            try:
                while True:
                    chunk = stream.read(8192)
                    if not chunk:
                        return
                    if isinstance(chunk, str):
                        chunk = chunk.encode("utf-8", errors="replace")
                    with self._lock:
                        self._total_bytes += len(chunk)
                        self._tail.extend(chunk)
                        overflow = len(self._tail) - self.max_bytes
                        if overflow > 0:
                            del self._tail[:overflow]
            except BaseException as exc:
                with self._lock:
                    self._error = exc

        self._thread = threading.Thread(target=_drain, name="exaserve-output-capture", daemon=True)
        self._thread.start()

    def join(self, timeout_s: float = 0.2) -> bool:
        if (
            isinstance(timeout_s, bool)
            or not isinstance(timeout_s, (int, float))
            or not math.isfinite(float(timeout_s))
            or timeout_s < 0
        ):
            raise ValueError("output capture join timeout must be finite and non-negative")
        if self._thread is not None:
            self._thread.join(max(0.0, timeout_s))
            return not self._thread.is_alive()
        return True

    def snapshot(self) -> dict:
        with self._lock:
            tail = bytes(self._tail)
            total = self._total_bytes
            error = self._error
        return {
            "tail": tail.decode("utf-8", errors="replace"),
            "retained_bytes": len(tail),
            "total_bytes": total,
            "dropped_bytes": max(0, total - len(tail)),
            "truncated": total > len(tail),
            "error": None if error is None else f"{type(error).__name__}: {error}",
        }

    def failure(self) -> Optional[str]:
        with self._lock:
            error = self._error
        return None if error is None else f"{type(error).__name__}: {error}"


def _group_has_members(pgid: Optional[int]) -> bool:
    """True if any process still belongs to this group (IMP-B03)."""
    if pgid is None:
        return False
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


@dataclass
class FirstCause:
    component_id: str
    reason_code: str
    detail: str
    exit_code: Optional[int] = None
    at: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        for name in ("component_id", "reason_code", "detail"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ValueError(f"failure cause {name} must be non-empty text")
        if self.exit_code is not None and (
            isinstance(self.exit_code, bool) or not isinstance(self.exit_code, int)
        ):
            raise ValueError("failure cause exit_code must be null or an integer")
        if (
            isinstance(self.at, bool)
            or not isinstance(self.at, (int, float))
            or not math.isfinite(float(self.at))
            or self.at < 0
        ):
            raise ValueError("failure cause timestamp must be finite and non-negative")

    def __str__(self) -> str:
        return f"{self.component_id}: {self.reason_code} (exit={self.exit_code}) {self.detail}"


@dataclass
class ManagedComponent:
    """One supervised OS-boundary child (plan §3.1)."""

    component_id: str
    argv: Sequence[str]
    owner_scope: str = OwnerScope.GLOBAL.value
    owner_rank: Optional[int] = None
    long_lived: bool = True  # unexpected exit is fatal
    env: Optional[dict] = None
    cwd: Optional[str] = None
    stdout: Optional[object] = None  # file object; NEVER parsed for lifecycle
    # A finite component additionally validates its result before succeeding.
    result_check: Optional[Callable[[], tuple[bool, str]]] = None
    on_starting: Optional[Callable[[], None]] = None
    on_started: Optional[Callable[[dict], None]] = None
    on_stopped: Optional[Callable[[], None]] = None
    on_unexpected_exit: Optional[Callable[[Optional[int]], str]] = None
    pass_fds: tuple[int, ...] = ()
    output_capture: Optional[BoundedOutputCapture] = None

    process: Optional[subprocess.Popen] = None
    state: str = ComponentState.NEW.value
    _stopping: bool = False
    _pgid: Optional[int] = None  # captured at start; getpgid fails post-reap

    def __post_init__(self) -> None:
        self._validate_contract(snapshot=True)

    def _validate_contract(self, *, snapshot: bool) -> None:
        if not isinstance(self.component_id, str) or not self.component_id:
            raise ValueError("managed component_id must be non-empty text")
        if isinstance(self.argv, (str, bytes)):
            raise ValueError("managed component argv must be an argument vector")
        argv = tuple(self.argv)
        if not argv or any(
            not isinstance(item, str) or not item or "\x00" in item for item in argv
        ):
            raise ValueError("managed component argv must contain non-empty string arguments")
        if snapshot:
            self.argv = argv
        if self.owner_scope not in {OwnerScope.GLOBAL.value, OwnerScope.RANK.value}:
            raise ValueError("managed component owner_scope must be GLOBAL or RANK")
        if self.owner_scope == OwnerScope.GLOBAL.value:
            if self.owner_rank is not None:
                raise ValueError("GLOBAL managed component owner_rank must be null")
        elif (
            isinstance(self.owner_rank, bool)
            or not isinstance(self.owner_rank, int)
            or self.owner_rank < 0
        ):
            raise ValueError("RANK managed component owner_rank must be non-negative")
        if not isinstance(self.long_lived, bool):
            raise ValueError("managed component long_lived must be boolean")
        if self.env is not None:
            if not isinstance(self.env, Mapping) or any(
                not isinstance(key, str)
                or not key
                or not isinstance(value, str)
                or "\x00" in key
                or "\x00" in value
                for key, value in self.env.items()
            ):
                raise ValueError("managed component env must be a string mapping without NUL")
            if snapshot:
                self.env = MappingProxyType(dict(self.env))  # type: ignore[assignment]
        if self.cwd is not None:
            try:
                cwd = os.fspath(self.cwd)
            except TypeError as exc:
                raise ValueError("managed component cwd must be a path") from exc
            if not isinstance(cwd, str) or not cwd or "\x00" in cwd:
                raise ValueError("managed component cwd must be a non-empty path")
            if snapshot:
                self.cwd = cwd
        for name in (
            "result_check",
            "on_starting",
            "on_started",
            "on_stopped",
            "on_unexpected_exit",
        ):
            value = getattr(self, name)
            if value is not None and not callable(value):
                raise ValueError(f"managed component {name} must be null or callable")
        if not isinstance(self.pass_fds, (tuple, list)) or any(
            isinstance(fd, bool) or not isinstance(fd, int) or fd < 0 for fd in self.pass_fds
        ):
            raise ValueError("managed component pass_fds must contain non-negative integers")
        if len(self.pass_fds) != len(set(self.pass_fds)):
            raise ValueError("managed component pass_fds must be unique")
        if snapshot:
            self.pass_fds = tuple(self.pass_fds)
        if self.output_capture is not None and not isinstance(
            self.output_capture, BoundedOutputCapture
        ):
            raise ValueError("managed component output_capture has an invalid type")

    # -- lifecycle ---------------------------------------------------------
    def start(self, *, rollback_deadline: Optional[float] = None) -> dict:
        # NodeSupervisor legitimately changes owner_scope/rank after initial
        # construction. Revalidate every execution-bearing field at the exact
        # Popen boundary so later caller mutation cannot bypass the contract.
        self._validate_contract(snapshot=True)
        if self.process is not None:
            raise SupervisorError(f"component {self.component_id!r} has already been started")
        if rollback_deadline is not None and (
            isinstance(rollback_deadline, bool)
            or not isinstance(rollback_deadline, (int, float))
            or not math.isfinite(float(rollback_deadline))
            or rollback_deadline < 0
        ):
            raise ValueError("component rollback deadline must be finite and nonnegative")
        self.state = ComponentState.STARTING.value
        if self.output_capture is not None and self.stdout is not None:
            self.state = ComponentState.FAILED.value
            raise ValueError("stdout and output_capture are mutually exclusive")
        if self.on_starting is not None:
            try:
                # This is the last parent-only boundary before the child can
                # execute. Launch-coupled protocol clocks arm here so a fast
                # child cannot outrun a post-Popen callback.
                self.on_starting()
            except BaseException:
                self.state = ComponentState.FAILED.value
                raise
        # IMP-B03: own process group so termination reaches descendants
        # (Ray/Serve/engine children previously escaped cleanup).
        try:
            self.process = subprocess.Popen(
                list(self.argv),
                env=self.env,
                cwd=self.cwd,
                stdout=(subprocess.PIPE if self.output_capture is not None else self.stdout),
                stderr=(
                    subprocess.STDOUT
                    if self.stdout is not None or self.output_capture is not None
                    else None
                ),
                start_new_session=True,
                pass_fds=tuple(self.pass_fds),
            )
        except (OSError, ValueError):
            self.state = ComponentState.FAILED.value
            raise
        try:
            self._pgid = os.getpgid(self.process.pid)
        except OSError:
            self._pgid = self.process.pid  # start_new_session => pid == pgid
        try:
            if self.output_capture is not None:
                assert self.process.stdout is not None
                self.output_capture.start(self.process.stdout)
            identity = {"pid": self.process.pid, "pgid": self._pgid}
            if self.on_started is not None:
                self.on_started(identity)
        except BaseException as exc:
            try:
                self.stop(
                    "post-spawn initialization failed",
                    deadline=(
                        float(rollback_deadline)
                        if rollback_deadline is not None
                        else time.monotonic() + 30.0
                    ),
                )
            except BaseException as cleanup_exc:
                add_exception_note(exc, f"component rollback also failed: {cleanup_exc}")
            self.state = ComponentState.FAILED.value
            raise
        self.state = ComponentState.RUNNING.value
        return identity

    def wait(self, deadline: float) -> tuple[str, Optional[int]]:
        """Wait only until an absolute monotonic deadline."""
        if self.process is None:
            raise SupervisorError(f"component {self.component_id!r} has not been started")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(list(self.argv), 0)
        try:
            self.process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            raise
        return self.observe()

    def observe(self) -> tuple[str, Optional[int]]:
        """Return (state, exit_code). Never blocks."""
        if self.process is None:
            return self.state, None
        if self.output_capture is not None and self.output_capture.failure() is not None:
            self.state = ComponentState.FAILED.value
            return self.state, self.process.poll()
        code = self.process.poll()
        if code is None:
            return self.state, None
        if not self._stopping and _group_has_members(self._pgid):
            # Reaping the direct launcher is not success while a descendant in
            # its owned session remains.  Mark it fatal immediately so callers
            # cannot consume a finite result while escaped work is still live.
            self.state = ComponentState.FAILED.value
            return self.state, code
        if self.output_capture is not None:
            self.output_capture.join()
        if self._stopping:
            self.state = ComponentState.STOPPED.value
        else:
            # IMP-B03: an unexpected exit is FAILED regardless of exit code —
            # a long-lived service that returns 0 has still disappeared.
            self.state = (
                ComponentState.STOPPED.value
                if not self.long_lived and code == 0
                else ComponentState.FAILED.value
            )
        return self.state, code

    def stop(
        self,
        reason: str = "shutdown",
        deadline_s: float = 30.0,
        *,
        deadline: Optional[float] = None,
    ) -> None:
        """Idempotent, bounded: TERM the group, then KILL the group.

        IMP-B03: the group is reaped even when the DIRECT child has already
        exited — a launcher that backgrounds work leaves descendants alive in
        its group, and returning early here let them survive as orphans.
        """
        self._stopping = True
        for name, value in (("deadline_s", deadline_s), ("deadline", deadline)):
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"component cleanup {name} must be numeric")
            if not math.isfinite(float(value)):
                raise ValueError(f"component cleanup {name} must be finite")
        absolute_deadline = (
            deadline if deadline is not None else time.monotonic() + max(0.0, deadline_s)
        )
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
            except PermissionError:
                return True

        if not _signal_group(signal.SIGTERM) and child_alive:
            with contextlib.suppress(OSError, ProcessLookupError):
                self.process.terminate()
        if child_alive:
            try:
                self.process.wait(timeout=max(0.0, min(5.0, absolute_deadline - time.monotonic())))
            except subprocess.TimeoutExpired:
                pass
        # Give TERM at most half the remaining budget; KILL and reap consume
        # the other half. Every component shares the supervisor's ONE absolute
        # cleanup deadline rather than each receiving a fresh full timeout.
        remaining = max(0.0, absolute_deadline - time.monotonic())
        term_deadline = time.monotonic() + remaining / 2
        while time.monotonic() < term_deadline:
            if not _group_has_members(pgid):
                break
            time.sleep(min(0.1, max(0.0, term_deadline - time.monotonic())))
        if _group_has_members(pgid):
            _signal_group(signal.SIGKILL)
        while time.monotonic() < absolute_deadline and _group_has_members(pgid):
            with contextlib.suppress(subprocess.TimeoutExpired):
                self.process.wait(timeout=min(0.1, max(0.0, absolute_deadline - time.monotonic())))
            if _group_has_members(pgid):
                time.sleep(min(0.05, max(0.0, absolute_deadline - time.monotonic())))
        with contextlib.suppress(subprocess.TimeoutExpired):
            self.process.wait(timeout=max(0.0, absolute_deadline - time.monotonic()))
        if _group_has_members(pgid):
            self.state = ComponentState.FAILED.value
            raise SupervisorError(
                f"component {self.component_id!r} retained process-group "
                f"members after cleanup deadline ({reason})"
            )
        self.state = ComponentState.STOPPED.value
        if self.output_capture is not None:
            if not self.output_capture.join(
                timeout_s=max(0.0, min(1.0, absolute_deadline - time.monotonic()))
            ):
                self.state = ComponentState.FAILED.value
                raise SupervisorError(
                    f"component {self.component_id!r} output drain did not stop by cleanup deadline"
                )
            capture_failure = self.output_capture.failure()
            if capture_failure is not None:
                self.state = ComponentState.FAILED.value
                raise SupervisorError(
                    f"component {self.component_id!r} output drain failed: {capture_failure}"
                )
        if self.on_stopped is not None:
            self.on_stopped()


class RuntimeSupervisor:
    """Allocation-head coordinator: owns components, terminal state, cleanup."""

    def __init__(self, *, poll_interval_s: float = 2.0) -> None:
        if (
            isinstance(poll_interval_s, bool)
            or not isinstance(poll_interval_s, (int, float))
            or not math.isfinite(float(poll_interval_s))
            or poll_interval_s <= 0
        ):
            raise ValueError("supervisor poll interval must be finite and positive")
        self.components: dict[str, ManagedComponent] = {}
        self.poll_interval_s = poll_interval_s
        self.first_cause: Optional[FirstCause] = None
        self.secondary_causes: list[FirstCause] = []
        self._shutdown_requested = False
        self._signals_installed = False

    # -- registry ----------------------------------------------------------
    def register(self, component: ManagedComponent) -> ManagedComponent:
        if not isinstance(component, ManagedComponent):
            raise SupervisorError("supervisor can register only ManagedComponent values")
        component._validate_contract(snapshot=True)
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
            except ValueError as exc:
                raise SupervisorError(
                    "signal handlers must be installed from the main thread before launch"
                ) from exc
        self._signals_installed = True

    def request_shutdown(self, reason: str) -> None:
        self._shutdown_requested = True
        self.record_cause("supervisor", "SHUTDOWN_REQUESTED", reason)

    def record_cause(
        self, component_id: str, reason_code: str, detail: str, exit_code: Optional[int] = None
    ) -> None:
        """First cause wins; later cleanup errors never overwrite it."""
        cause = FirstCause(component_id, reason_code, detail, exit_code)
        if self.first_cause is None:
            self.first_cause = cause
        else:
            self.secondary_causes.append(cause)

    # -- supervision loop --------------------------------------------------
    def start_all(self, *, rollback_s: float = 30.0) -> None:
        if (
            isinstance(rollback_s, bool)
            or not isinstance(rollback_s, (int, float))
            or not math.isfinite(float(rollback_s))
            or rollback_s < 0
        ):
            raise ValueError("startup rollback budget must be finite and nonnegative")
        self.install_signal_handlers()
        started: list[ManagedComponent] = []
        cleanup_deadline: Optional[float] = None
        try:
            for comp in self.components.values():
                # The child and all siblings in this startup transaction share
                # this exact rollback deadline if post-spawn initialization
                # fails.  ManagedComponent must not silently mint an additional
                # thirty-second cleanup window before the supervisor sees it.
                cleanup_deadline = time.monotonic() + float(rollback_s)
                started.append(comp)
                comp.start(rollback_deadline=cleanup_deadline)
        except Exception as exc:  # noqa: BLE001 - process-boundary transaction
            failed = next(
                (
                    item
                    for item in self.components.values()
                    if item.state == ComponentState.FAILED.value
                ),
                None,
            )
            self.record_cause(
                failed.component_id if failed is not None else "startup", "START_FAILED", str(exc)
            )
            if cleanup_deadline is None:
                cleanup_deadline = time.monotonic() + float(rollback_s)
            for item in reversed(started):
                try:
                    item.stop("startup rollback", deadline=cleanup_deadline)
                except Exception as cleanup_exc:  # noqa: BLE001 - visit all siblings
                    self.record_cause(
                        item.component_id,
                        "START_ROLLBACK_FAILED",
                        f"{type(cleanup_exc).__name__}: {cleanup_exc}",
                    )
            raise SupervisorError(f"component startup transaction failed: {exc}") from exc

    def poll_once(self) -> Optional[FirstCause]:
        """Poll EVERY registered component once. Returns a cause if fatal."""
        for comp in self.components.values():
            state, code = comp.observe()
            if state == ComponentState.FAILED.value:
                detail = f"{comp.component_id} exited unexpectedly"
                if comp.on_unexpected_exit is not None:
                    try:
                        captured = comp.on_unexpected_exit(code)
                        if captured:
                            detail = captured
                    except Exception as exc:  # noqa: BLE001 - evidence boundary
                        detail += f"; failure-evidence capture failed: {exc}"
                self.record_cause(comp.component_id, "UNEXPECTED_EXIT", detail, code)
                return self.first_cause
            if (
                state == ComponentState.STOPPED.value
                and not comp.long_lived
                and comp.result_check is not None
            ):
                ok, why = comp.result_check()
                if not ok:
                    self.record_cause(comp.component_id, "INVALID_RESULT", why, code)
                    return self.first_cause
        return None

    def supervise(
        self, *, until: Optional[Callable[[], bool]] = None, timeout_s: Optional[float] = None
    ) -> Optional[FirstCause]:
        """Run until a component fails, `until()` is true, or timeout."""
        if timeout_s is not None and (
            isinstance(timeout_s, bool)
            or not isinstance(timeout_s, (int, float))
            or not math.isfinite(float(timeout_s))
            or timeout_s <= 0
        ):
            raise ValueError("supervision timeout must be finite and positive")
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

    def shutdown(self, *, drain_s: float = 30.0, deadline: Optional[float] = None) -> bool:
        """Reverse cleanup under one absolute deadline; verify every group."""
        for name, value in (("drain_s", drain_s), ("deadline", deadline)):
            if value is None:
                continue
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
            ):
                raise ValueError(f"supervisor cleanup {name} must be finite numeric")
        deadline = deadline if deadline is not None else time.monotonic() + max(0.0, drain_s)
        clean = True
        for comp in reversed(list(self.components.values())):
            try:
                comp.stop(deadline=deadline)
            except Exception as exc:  # noqa: BLE001 - cleanup must visit siblings
                clean = False
                self.record_cause(comp.component_id, "CLEANUP_ERROR", str(exc))
        return clean and all(
            comp.state == ComponentState.STOPPED.value for comp in self.components.values()
        )

    def exit_code(self) -> int:
        """0 only when nothing failed (plan WP4 exit gate)."""
        if self.first_cause is None:
            return 0
        if self.first_cause.reason_code == "SHUTDOWN_REQUESTED":
            return 143
        native = self.first_cause.exit_code
        if native is None or native == 0:
            return 1
        if native < 0:
            return 128 + abs(native)
        return native
