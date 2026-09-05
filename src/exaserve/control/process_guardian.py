"""Independent node-local guardian for one long-lived child process group.

The rank ``NodeSupervisor`` is deliberately killable: scheduler launchers and
hardware faults can terminate it without running Python ``finally`` blocks.
Consequently it cannot itself be the watchdog promised by the control-plane
contract.  This small child survives in its own session, owns the Ray process
group that it creates, and binds its lifetime to the exact parent PID *and*
Linux start ticks.  If that owner disappears, the guardian terminates only its
own child group under the resolved deadline and releases only its own verified
generation receipt.

No stdout or process-name matching participates in lifecycle decisions.
"""

from __future__ import annotations

import argparse
import math
import os
import signal
import sys
import time
from typing import Optional, Sequence

from ..state.process_ownership import (
    ProcessOwnershipError,
    load_process_ownership,
    process_ownership_path,
    process_start_ticks,
    release_current_process_ownership,
    same_process_identity,
)
from .supervisor import ManagedComponent, SupervisorError


class ProcessGuardianError(RuntimeError):
    pass


def guardian_argv(
    child_argv: Sequence[str],
    *,
    owner_pid: int,
    owner_start_ticks: int,
    deployment_id: str,
    generation: int,
    rank: int,
    cleanup_deadline_s: float,
    receipt_wait_s: float = 30.0,
) -> list[str]:
    """Build the immutable argument vector for a generation-bound guardian."""
    if isinstance(child_argv, (str, bytes)):
        raise ValueError("guardian child argv must be an argument vector")
    child = tuple(child_argv)
    if not child or any(not isinstance(item, str) or not item or "\x00" in item for item in child):
        raise ValueError("guardian child argv must contain non-empty strings")
    for name, value in (("owner_pid", owner_pid), ("owner_start_ticks", owner_start_ticks)):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"guardian {name} must be a positive integer")
    if not isinstance(deployment_id, str) or not deployment_id:
        raise ValueError("guardian deployment_id must be non-empty text")
    for name, value in (("generation", generation), ("rank", rank)):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"guardian {name} must be a non-negative integer")
    for name, value in (
        ("cleanup_deadline_s", cleanup_deadline_s),
        ("receipt_wait_s", receipt_wait_s),
    ):
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or value <= 0
        ):
            raise ValueError(f"guardian {name} must be finite and positive")
    from ..plan.runtime_environment import (
        LOCAL_RUNTIME_ROOT_ENV,
        QUALIFIED_PYTHON_ENV,
        require_non_shared_path,
    )

    configured_python = os.environ.get(QUALIFIED_PYTHON_ENV, "")
    if os.environ.get(LOCAL_RUNTIME_ROOT_ENV) and not configured_python:
        raise ValueError("managed guardian requires EXASERVE_QUALIFIED_PYTHON")
    python = configured_python or sys.executable
    if os.environ.get(QUALIFIED_PYTHON_ENV):
        python = require_non_shared_path(python, name="qualified guardian Python")
    return [
        python,
        "-m",
        "exaserve.control.process_guardian",
        "--owner-pid",
        str(owner_pid),
        "--owner-start-ticks",
        str(owner_start_ticks),
        "--deployment-id",
        deployment_id,
        "--generation",
        str(generation),
        "--rank",
        str(rank),
        "--cleanup-deadline-s",
        f"{float(cleanup_deadline_s):.17g}",
        "--receipt-wait-s",
        f"{float(receipt_wait_s):.17g}",
        "--child",
        *child,
    ]


def _await_own_receipt(
    *,
    owner_pid: int,
    owner_start_ticks: int,
    deployment_id: str,
    generation: int,
    rank: int,
    timeout_s: float,
) -> str:
    """Wait until the parent durably records this guardian before child spawn."""
    pid = os.getpid()
    start_ticks = process_start_ticks(pid)
    path = process_ownership_path(pid, start_ticks)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if not same_process_identity(owner_pid, owner_start_ticks):
            raise ProcessGuardianError("exact NodeSupervisor owner disappeared before arming")
        try:
            receipt = load_process_ownership(path)
        except FileNotFoundError:
            time.sleep(min(0.02, max(0.0, deadline - time.monotonic())))
            continue
        except ProcessOwnershipError as exc:
            raise ProcessGuardianError(f"guardian ownership receipt is invalid: {exc}") from exc
        if (
            receipt.pid != pid
            or receipt.pgid != os.getpgrp()
            or receipt.process_start_ticks != start_ticks
            or receipt.deployment_id != deployment_id
            or receipt.generation != generation
            or receipt.rank != rank
            or receipt.component_id != "ray"
        ):
            raise ProcessGuardianError("guardian ownership receipt has the wrong identity")
        return path
    raise ProcessGuardianError(
        f"guardian ownership receipt was not published within {timeout_s:g}s"
    )


def _await_child_receipt(
    *,
    guardian_pid: int,
    guardian_start_ticks: int,
    deployment_id: str,
    generation: int,
    rank: int,
    timeout_s: float,
    child_argv: Sequence[str],
) -> None:
    """Fence child exec until its separate process-group receipt is durable."""

    pid = os.getpid()
    start_ticks = process_start_ticks(pid)
    path = process_ownership_path(pid, start_ticks)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if not same_process_identity(guardian_pid, guardian_start_ticks):
            raise ProcessGuardianError("guardian disappeared before child ownership was armed")
        try:
            receipt = load_process_ownership(path)
        except FileNotFoundError:
            time.sleep(min(0.02, max(0.0, deadline - time.monotonic())))
            continue
        except ProcessOwnershipError as exc:
            raise ProcessGuardianError(f"child ownership receipt is invalid: {exc}") from exc
        from ..plan.contracts import canonical_hash

        if (
            receipt.pid != pid
            or receipt.pgid != os.getpgrp()
            or receipt.process_start_ticks != start_ticks
            or receipt.deployment_id != deployment_id
            or receipt.generation != generation
            or receipt.rank != rank
            or receipt.component_id != "ray_child"
            or receipt.argv_hash != canonical_hash(tuple(child_argv))
        ):
            raise ProcessGuardianError("child ownership receipt has the wrong identity")
        return
    raise ProcessGuardianError(f"child ownership receipt was not published within {timeout_s:g}s")


def run_guarded_child_bootstrap(
    child_argv: Sequence[str],
    *,
    guardian_pid: int,
    guardian_start_ticks: int,
    deployment_id: str,
    generation: int,
    rank: int,
    receipt_wait_s: float,
) -> int:
    """Wait for exact ownership and replace this process with guarded Ray."""

    if isinstance(child_argv, (str, bytes)) or not child_argv:
        raise ProcessGuardianError("guarded child bootstrap requires an argument vector")
    _await_child_receipt(
        guardian_pid=guardian_pid,
        guardian_start_ticks=guardian_start_ticks,
        deployment_id=deployment_id,
        generation=generation,
        rank=rank,
        timeout_s=receipt_wait_s,
        child_argv=child_argv,
    )
    try:
        os.execvpe(child_argv[0], list(child_argv), os.environ)
    except OSError as exc:
        raise ProcessGuardianError(f"guarded child exec failed: {exc}") from exc


def _normalized_exit_code(code: Optional[int]) -> int:
    if code is None or code == 0:
        return 0 if code == 0 else 1
    return 128 + abs(code) if code < 0 else min(255, code)


def run_guardian(
    child_argv: Sequence[str],
    *,
    owner_pid: int,
    owner_start_ticks: int,
    deployment_id: str,
    generation: int,
    rank: int,
    cleanup_deadline_s: float,
    receipt_wait_s: float,
) -> int:
    """Run one guarded child and return its scheduler-safe terminal status."""
    receipt_path = _await_own_receipt(
        owner_pid=owner_pid,
        owner_start_ticks=owner_start_ticks,
        deployment_id=deployment_id,
        generation=generation,
        rank=rank,
        timeout_s=receipt_wait_s,
    )
    stop_signal: Optional[int] = None

    def request_stop(signum, _frame) -> None:
        nonlocal stop_signal
        if stop_signal is None:
            stop_signal = int(signum)

    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, request_stop)

    guardian_pid = os.getpid()
    guardian_start_ticks = process_start_ticks(guardian_pid)
    bootstrap_argv = [
        sys.executable,
        "-m",
        "exaserve.control.process_guardian",
        "--guarded-child-bootstrap",
        "--guardian-pid",
        str(guardian_pid),
        "--guardian-start-ticks",
        str(guardian_start_ticks),
        "--deployment-id",
        deployment_id,
        "--generation",
        str(generation),
        "--rank",
        str(rank),
        "--receipt-wait-s",
        f"{float(receipt_wait_s):.17g}",
        "--child",
        *child_argv,
    ]
    child = ManagedComponent(component_id="guarded_ray", argv=tuple(bootstrap_argv))
    child_registry = None
    try:
        identity = child.start(rollback_deadline=time.monotonic() + cleanup_deadline_s)
        from ..state.process_ownership import ProcessOwnershipRegistry

        child_registry = ProcessOwnershipRegistry(
            deployment_id=deployment_id,
            generation=generation,
            rank=rank,
        )
        child_registry.record(
            "ray_child",
            pid=identity["pid"],
            pgid=identity["pgid"],
            # The receipt describes the executable this gated bootstrap is
            # committed to exec, not the short-lived Python wait wrapper.
            argv=child_argv,
        )
    except (OSError, ProcessOwnershipError, SupervisorError, ValueError) as exc:
        if child.process is not None:
            try:
                child.stop(
                    "child ownership publication failed",
                    deadline=time.monotonic() + cleanup_deadline_s,
                )
            except SupervisorError as cleanup_exc:
                raise ProcessGuardianError(
                    f"guarded Ray ownership failed ({exc}); cleanup also failed: {cleanup_exc}"
                ) from exc
        raise ProcessGuardianError(f"guarded Ray process failed to start: {exc}") from exc

    owner_lost = False
    child_code: Optional[int] = None
    try:
        while True:
            _, child_code = child.observe()
            if child_code is not None:
                break
            if stop_signal is not None:
                break
            if not same_process_identity(owner_pid, owner_start_ticks):
                owner_lost = True
                break
            time.sleep(0.05)
    finally:
        cleanup_deadline = time.monotonic() + cleanup_deadline_s
        child_clean = False
        try:
            child.stop(
                "NodeSupervisor owner lost" if owner_lost else "guardian shutdown",
                deadline=cleanup_deadline,
            )
            child_clean = True
        except SupervisorError as exc:
            raise ProcessGuardianError(f"guarded Ray cleanup failed: {exc}") from exc
        finally:
            if child_registry is not None and child_clean:
                child_registry.release("ray_child")

    if owner_lost:
        release_current_process_ownership(
            receipt_path,
            deployment_id=deployment_id,
            generation=generation,
            rank=rank,
            component_id="ray",
        )
        return 1
    if stop_signal is not None:
        return 0
    return _normalized_exit_code(child_code)


def main(argv: Optional[Sequence[str]] = None) -> int:
    arguments = list(argv) if argv is not None else sys.argv[1:]
    if "--guarded-child-bootstrap" in arguments:
        parser = argparse.ArgumentParser(prog="exaserve-guarded-child-bootstrap")
        parser.add_argument("--guarded-child-bootstrap", action="store_true")
        parser.add_argument("--guardian-pid", required=True, type=int)
        parser.add_argument("--guardian-start-ticks", required=True, type=int)
        parser.add_argument("--deployment-id", required=True)
        parser.add_argument("--generation", required=True, type=int)
        parser.add_argument("--rank", required=True, type=int)
        parser.add_argument("--receipt-wait-s", required=True, type=float)
        parser.add_argument("--child", nargs=argparse.REMAINDER, required=True)
        args = parser.parse_args(arguments)
        try:
            return run_guarded_child_bootstrap(
                args.child,
                guardian_pid=args.guardian_pid,
                guardian_start_ticks=args.guardian_start_ticks,
                deployment_id=args.deployment_id,
                generation=args.generation,
                rank=args.rank,
                receipt_wait_s=args.receipt_wait_s,
            )
        except (ProcessGuardianError, ProcessOwnershipError, ValueError) as exc:
            print(f"[ProcessGuardian] {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
            return 1
    parser = argparse.ArgumentParser(prog="exaserve-process-guardian")
    parser.add_argument("--owner-pid", required=True, type=int)
    parser.add_argument("--owner-start-ticks", required=True, type=int)
    parser.add_argument("--deployment-id", required=True)
    parser.add_argument("--generation", required=True, type=int)
    parser.add_argument("--rank", required=True, type=int)
    parser.add_argument("--cleanup-deadline-s", required=True, type=float)
    parser.add_argument("--receipt-wait-s", required=True, type=float)
    parser.add_argument("--child", nargs=argparse.REMAINDER, required=True)
    args = parser.parse_args(arguments)
    try:
        return run_guardian(
            args.child,
            owner_pid=args.owner_pid,
            owner_start_ticks=args.owner_start_ticks,
            deployment_id=args.deployment_id,
            generation=args.generation,
            rank=args.rank,
            cleanup_deadline_s=args.cleanup_deadline_s,
            receipt_wait_s=args.receipt_wait_s,
        )
    except (ProcessGuardianError, ProcessOwnershipError, ValueError) as exc:
        print(f"[ProcessGuardian] {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
