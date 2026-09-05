"""Node-local process fault/cleanup helper for hardware qualification only.

The caller delivers this module in the immutable runtime capsule and invokes it
with a closed environment.  It never accepts a shared run path.  Live fault
targets are fenced with their kernel environment and a pidfd; fallback cleanup
can signal only process groups carrying this generation's ownership receipt.
"""

from __future__ import annotations

import argparse
import ctypes
import json
import os
from pathlib import Path
import platform
import signal
import socket
import time


def _process_environment(pid: int) -> dict[str, str]:
    raw = (Path("/proc") / str(pid) / "environ").read_bytes()
    if len(raw) > 1 << 20:
        raise RuntimeError(f"process {pid} environment exceeds 1 MiB")
    result: dict[str, str] = {}
    for entry in (item for item in raw.split(b"\0") if item):
        key, separator, value = entry.partition(b"=")
        if not separator:
            continue
        name = key.decode("utf-8", errors="strict")
        if name in result:
            raise RuntimeError(f"process {pid} has duplicate environment key {name!r}")
        result[name] = value.decode("utf-8", errors="strict")
    return result


def _process_identity(
    pid: int,
    *,
    deployment_id: str,
    generation: int,
    plan_hash: str,
    owner_rank: int | None = None,
    requirement_id: str | None = None,
    role: str | None = None,
) -> dict:
    from .process_ownership import process_start_ticks

    if pid <= 1 or pid == os.getpid():
        raise RuntimeError("qualification target PID is unsafe")
    process = Path("/proc") / str(pid)
    status = (process / "status").read_text(encoding="utf-8")
    uid_line = next(line for line in status.splitlines() if line.startswith("Uid:"))
    if int(uid_line.split()[1]) != os.getuid():
        raise RuntimeError("qualification target belongs to another uid")
    environment = _process_environment(pid)
    expected = {
        "EXASERVE_DEPLOYMENT_ID": deployment_id,
        "EXASERVE_GENERATION": str(generation),
        "EXASERVE_PLAN_HASH": plan_hash,
    }
    if owner_rank is not None:
        expected["EXASERVE_RECEIPT_RANK"] = str(owner_rank)
    if any(environment.get(name) != value for name, value in expected.items()):
        raise RuntimeError("qualification target does not carry the exact generation identity")
    if role == "ray_worker":
        if (
            environment.get("EXASERVE_RECEIPT_ROLE") != role
            or environment.get("EXASERVE_RECEIPT_SLOT") != requirement_id
        ):
            raise RuntimeError("qualification Ray target does not carry the exact receipt slot")
    elif role == "replica":
        if (
            environment.get("EXASERVE_COMPAT_ROLE") != "replica"
            or environment.get("EXASERVE_RECEIPT_REQUIREMENT_ID_REPLICA") != requirement_id
        ):
            raise RuntimeError(
                "qualification replica target does not carry the exact receipt requirement"
            )
    elif role is not None:
        raise RuntimeError(f"unsupported qualification process role {role!r}")
    command = [
        item.decode("utf-8", errors="replace")
        for item in (process / "cmdline").read_bytes().split(b"\0")
        if item
    ]
    return {
        "pid": pid,
        "process_start_ticks": process_start_ticks(pid),
        "pgid": os.getpgid(pid),
        "argv": command,
    }


def signal_exact_process(
    *,
    pid: int,
    signal_name: str,
    deployment_id: str,
    generation: int,
    plan_hash: str,
    owner_rank: int,
    requirement_id: str,
    role: str,
) -> dict:
    if signal_name not in {"TERM", "KILL"}:
        raise ValueError("qualification signal must be TERM or KILL")
    pidfd = _open_pidfd(pid)
    try:
        identity = _process_identity(
            pid,
            deployment_id=deployment_id,
            generation=generation,
            plan_hash=plan_hash,
            owner_rank=owner_rank,
            requirement_id=requirement_id,
            role=role,
        )
        _send_pidfd_signal(pidfd, getattr(signal, f"SIG{signal_name}"))
    finally:
        os.close(pidfd)
    return {
        "schema_version": 1,
        "hostname": socket.gethostname(),
        "deployment_id": deployment_id,
        "generation": generation,
        "deployment_plan_hash": plan_hash,
        "owner_rank": owner_rank,
        "requirement_id": requirement_id,
        "role": role,
        "process": identity,
        "signal": signal_name,
    }


def _send_pidfd_signal(pidfd: int, signum: int) -> None:
    native = getattr(signal, "pidfd_send_signal", None)
    if native is not None:
        native(pidfd, signum)
        return
    libc = ctypes.CDLL(None, use_errno=True)
    function = getattr(libc, "pidfd_send_signal", None)
    if function is not None:
        function.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint]
        function.restype = ctypes.c_int
        result = function(pidfd, signum, None, 0)
    else:
        _require_pidfd_syscall_architecture()
        result = libc.syscall(424, pidfd, signum, None, 0)
    if result != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))


def _open_pidfd(pid: int) -> int:
    native = getattr(os, "pidfd_open", None)
    if native is not None:
        return int(native(pid))
    _require_pidfd_syscall_architecture()
    libc = ctypes.CDLL(None, use_errno=True)
    descriptor = int(libc.syscall(434, pid, 0))
    if descriptor < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    return descriptor


def _require_pidfd_syscall_architecture() -> None:
    if platform.machine().lower() not in {"x86_64", "amd64", "aarch64", "arm64"}:
        raise RuntimeError("pidfd syscall numbers are not qualified on this architecture")


def _matching_environment(pid: int, deployment_id: str, generation: int, plan_hash: str) -> bool:
    try:
        environment = _process_environment(pid)
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return False
    return (
        environment.get("EXASERVE_DEPLOYMENT_ID") == deployment_id
        and environment.get("EXASERVE_GENERATION") == str(generation)
        and environment.get("EXASERVE_PLAN_HASH") == plan_hash
    )


def cleanup_exact_generation(
    *, deployment_id: str, generation: int, plan_hash: str, timeout_s: float = 8.0
) -> dict:
    from .process_ownership import (
        _existing_receipt_directory,
        _group_alive,
        _remove_owned_paths,
        _same_process,
        load_process_ownership,
    )

    def same_exact_process(receipt) -> bool:
        return _same_process(receipt) and _matching_environment(
            receipt.pid,
            deployment_id,
            generation,
            plan_hash,
        )

    receipt_dir = _existing_receipt_directory()
    receipts = []
    if receipt_dir is not None:
        for path in sorted(receipt_dir.glob("*.json")):
            receipt = load_process_ownership(str(path))
            if (
                receipt.uid == os.getuid()
                and receipt.hostname == socket.gethostname()
                and receipt.deployment_id == deployment_id
                and receipt.generation == generation
                and same_exact_process(receipt)
            ):
                receipts.append((path, receipt))
    matched = [
        {
            "pid": receipt.pid,
            "pgid": receipt.pgid,
            "process_start_ticks": receipt.process_start_ticks,
            "rank": receipt.rank,
            "component_id": receipt.component_id,
            "receipt_hash": receipt.receipt_hash,
        }
        for _path, receipt in receipts
    ]
    signals = []
    deadline = time.monotonic() + timeout_s
    for _path, receipt in reversed(receipts):
        if same_exact_process(receipt):
            try:
                os.killpg(receipt.pgid, signal.SIGTERM)
                signals.append({"pgid": receipt.pgid, "signal": "TERM"})
            except ProcessLookupError:
                pass
    term_deadline = min(deadline, time.monotonic() + timeout_s / 2)
    while time.monotonic() < term_deadline and any(
        _same_process(receipt) for _path, receipt in receipts
    ):
        time.sleep(0.05)
    for _path, receipt in reversed(receipts):
        if same_exact_process(receipt):
            try:
                os.killpg(receipt.pgid, signal.SIGKILL)
                signals.append({"pgid": receipt.pgid, "signal": "KILL"})
            except ProcessLookupError:
                pass
    while time.monotonic() < deadline and any(
        _group_alive(receipt.pgid) for _path, receipt in receipts
    ):
        time.sleep(0.05)
    receipt_survivors = [receipt.pid for _path, receipt in receipts if _group_alive(receipt.pgid)]
    if receipt_survivors:
        raise RuntimeError(f"owned qualification processes survived cleanup: {receipt_survivors}")
    for path, receipt in receipts:
        _remove_owned_paths(receipt)
        try:
            path.unlink()
        except FileNotFoundError:
            pass
    survivors = [
        int(path.name)
        for path in Path("/proc").glob("[0-9]*")
        if int(path.name) != os.getpid()
        and _matching_environment(int(path.name), deployment_id, generation, plan_hash)
    ]
    if survivors:
        raise RuntimeError(f"unowned exact-generation processes survived cleanup: {survivors}")
    return {
        "schema_version": 1,
        "hostname": socket.gethostname(),
        "deployment_id": deployment_id,
        "generation": generation,
        "deployment_plan_hash": plan_hash,
        "matched": matched,
        "signals": signals,
        "survivors": [],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="exaserve-qualification-process")
    subparsers = parser.add_subparsers(dest="operation", required=True)
    signal_parser = subparsers.add_parser("signal")
    signal_parser.add_argument("--pid", type=int, required=True)
    signal_parser.add_argument("--signal", choices=("TERM", "KILL"), required=True)
    signal_parser.add_argument("--owner-rank", type=int, required=True)
    signal_parser.add_argument("--requirement-id", required=True)
    signal_parser.add_argument("--role", choices=("ray_worker", "replica"), required=True)
    cleanup_parser = subparsers.add_parser("cleanup")
    cleanup_parser.add_argument("--timeout", type=float, default=8.0)
    for child in (signal_parser, cleanup_parser):
        child.add_argument("--deployment-id", required=True)
        child.add_argument("--generation", type=int, required=True)
        child.add_argument("--plan-hash", required=True)
    args = parser.parse_args(argv)
    common = {
        "deployment_id": args.deployment_id,
        "generation": args.generation,
        "plan_hash": args.plan_hash,
    }
    if args.operation == "signal":
        report = signal_exact_process(
            pid=args.pid,
            signal_name=args.signal,
            owner_rank=args.owner_rank,
            requirement_id=args.requirement_id,
            role=args.role,
            **common,
        )
    else:
        report = cleanup_exact_generation(timeout_s=args.timeout, **common)
    print(json.dumps(report, sort_keys=True, separators=(",", ":"), allow_nan=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
