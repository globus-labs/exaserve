"""Node-local process-group ownership receipts and stale-generation cleanup.

Cleanup may signal only a process group that a prior ExaServe NodeSupervisor
recorded immediately after creating it.  PID reuse is fenced with Linux process
start ticks; arbitrary name matching and ``pkill -f`` are intentionally absent.
"""

from __future__ import annotations

import contextlib
import json
import math
import os
import re
import shutil
import signal
import socket
import time
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Iterable

from ..plan.contracts import canonical_hash
from .atomic import atomic_create_json, strict_json_load_path

SCHEMA_VERSION = 1
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class ProcessOwnershipError(RuntimeError):
    pass


def ownership_root() -> str:
    override = os.environ.get("EXASERVE_PROCESS_OWNERSHIP_ROOT", "").strip()
    return os.path.abspath(override or f"/tmp/exaserve-owned-{os.getuid()}")


def runtime_ownership_root() -> str:
    """Short node-local root for runtime trees that contain Unix sockets.

    Ray appends ``ray/session_<timestamp>_<pid>/sockets/<name>`` to
    ``RAY_TMPDIR``.  Nesting that suffix below the descriptive receipt root can
    exceed Linux's 107-byte AF_UNIX pathname limit before Ray starts.  The
    durable receipt retains the complete deployment/generation/rank identity;
    the ephemeral path therefore uses a collision-resistant compact token.
    """
    override = os.environ.get("EXASERVE_RUNTIME_OWNERSHIP_ROOT", "").strip()
    return os.path.abspath(override or f"/tmp/xr-{os.getuid()}")


def generation_runtime_root(deployment_id: str, generation: int, rank: int) -> str:
    token = canonical_hash(
        {
            "deployment_id": deployment_id,
            "generation": generation,
            "rank": rank,
        }
    )[:20]
    return os.path.join(runtime_ownership_root(), token)


def _is_owned_temp_path(path: str) -> bool:
    absolute = os.path.abspath(path)
    for root in (ownership_root(), runtime_ownership_root()):
        if os.path.commonpath((root, absolute)) == root and absolute != root:
            return True
    return False


def _process_start_ticks(pid: int) -> int:
    try:
        raw = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        # comm is parenthesized and may contain spaces. Fields after the final
        # `) ` begin at field 3; starttime is field 22, hence index 19.
        tail = raw.rsplit(") ", 1)[1].split()
        return int(tail[19])
    except (OSError, IndexError, ValueError) as exc:
        raise ProcessOwnershipError(f"cannot read start identity for pid {pid}: {exc}") from exc


def process_start_ticks(pid: int) -> int:
    """Return the Linux start-time identity used to fence PID reuse.

    This is public because the node-local watchdog must bind itself to the
    exact NodeSupervisor generation that created it.  A numeric parent PID by
    itself is not an ownership identity: it may be reused after a hard crash.
    """
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        raise ProcessOwnershipError("process identity pid must be a positive integer")
    return _process_start_ticks(pid)


def same_process_identity(pid: int, start_ticks: int) -> bool:
    """Whether *pid* still names the exact process generation supplied."""
    if (
        isinstance(pid, bool)
        or not isinstance(pid, int)
        or pid <= 0
        or isinstance(start_ticks, bool)
        or not isinstance(start_ticks, int)
        or start_ticks <= 0
    ):
        raise ProcessOwnershipError("process identity requires positive pid/start ticks")
    try:
        return _process_start_ticks(pid) == start_ticks
    except ProcessOwnershipError as exc:
        if isinstance(exc.__cause__, (FileNotFoundError, ProcessLookupError)):
            return False
        raise


def process_ownership_path(pid: int, start_ticks: int) -> str:
    """Deterministic receipt path for one exact process generation."""
    if (
        isinstance(pid, bool)
        or not isinstance(pid, int)
        or pid <= 0
        or isinstance(start_ticks, bool)
        or not isinstance(start_ticks, int)
        or start_ticks <= 0
    ):
        raise ProcessOwnershipError("ownership path requires positive pid/start ticks")
    return os.path.join(ownership_root(), "receipts", f"{os.getuid()}-{pid}-{start_ticks}.json")


@dataclass(frozen=True)
class ProcessOwnershipReceipt:
    schema_version: int
    uid: int
    hostname: str
    deployment_id: str
    generation: int
    rank: int
    component_id: str
    pid: int
    pgid: int
    process_start_ticks: int
    argv_hash: str
    temp_paths: tuple[str, ...]
    created_at: float
    receipt_hash: str = ""

    def __post_init__(self) -> None:
        if isinstance(self.schema_version, bool) or self.schema_version != SCHEMA_VERSION:
            raise ProcessOwnershipError("unsupported process ownership schema")
        for name in ("uid", "generation", "rank", "pid", "pgid", "process_start_ticks"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ProcessOwnershipError(f"process ownership {name} is invalid")
        if self.pid <= 0 or self.pgid <= 0 or self.process_start_ticks <= 0:
            raise ProcessOwnershipError("process ownership PID identity is invalid")
        for name in ("hostname", "deployment_id", "component_id"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ProcessOwnershipError(f"process ownership {name} is required")
        if not isinstance(self.argv_hash, str) or not _SHA256.fullmatch(self.argv_hash):
            raise ProcessOwnershipError("process ownership argv_hash is invalid")
        if (
            not isinstance(self.created_at, (int, float))
            or isinstance(self.created_at, bool)
            or not math.isfinite(float(self.created_at))
            or self.created_at <= 0
        ):
            raise ProcessOwnershipError("process ownership created_at is invalid")
        if not isinstance(self.temp_paths, (tuple, list)):
            raise ProcessOwnershipError("owned temp paths must be a sequence")
        object.__setattr__(self, "temp_paths", tuple(self.temp_paths))
        for path in self.temp_paths:
            if not isinstance(path, str) or not path:
                raise ProcessOwnershipError("owned temp path is invalid")
            if not _is_owned_temp_path(path):
                raise ProcessOwnershipError(f"owned temp path escapes ownership root: {path}")
        if not isinstance(self.receipt_hash, str) or (
            self.receipt_hash and not _SHA256.fullmatch(self.receipt_hash)
        ):
            raise ProcessOwnershipError("process ownership receipt_hash is invalid")

    def canonical(self) -> dict:
        payload = asdict(self)
        payload.pop("receipt_hash", None)
        return payload

    def compute_hash(self) -> str:
        return canonical_hash(self.canonical())

    def finalize(self) -> "ProcessOwnershipReceipt":
        return replace(self, receipt_hash=self.compute_hash())


class ProcessOwnershipRegistry:
    def __init__(self, *, deployment_id: str, generation: int, rank: int) -> None:
        self.deployment_id = deployment_id
        self.generation = generation
        self.rank = rank
        self.directory = os.path.join(ownership_root(), "receipts")
        os.makedirs(self.directory, mode=0o700, exist_ok=True)
        os.chmod(self.directory, 0o700)
        self._owned: dict[str, tuple[str, str]] = {}

    def record(
        self,
        component_id: str,
        *,
        pid: int,
        pgid: int,
        argv: Iterable[str],
        temp_paths: Iterable[str] = (),
    ) -> str:
        if isinstance(argv, (str, bytes)):
            raise ProcessOwnershipError("owned process argv must be a sequence of strings")
        argv_items = tuple(argv)
        if not argv_items or any(not isinstance(item, str) or not item for item in argv_items):
            raise ProcessOwnershipError("owned process argv must contain non-empty strings")
        if isinstance(temp_paths, (str, bytes)):
            raise ProcessOwnershipError("owned temp paths must be a sequence of strings")
        temp_path_items = tuple(temp_paths)
        if any(not isinstance(item, str) or not item for item in temp_path_items):
            raise ProcessOwnershipError("owned temp paths must contain non-empty strings")
        receipt = ProcessOwnershipReceipt(
            schema_version=SCHEMA_VERSION,
            uid=os.getuid(),
            hostname=socket.gethostname(),
            deployment_id=self.deployment_id,
            generation=self.generation,
            rank=self.rank,
            component_id=component_id,
            pid=pid,
            pgid=pgid,
            process_start_ticks=_process_start_ticks(pid),
            argv_hash=canonical_hash(argv_items),
            temp_paths=tuple(os.path.abspath(item) for item in temp_path_items),
            created_at=time.time(),
        ).finalize()
        path = process_ownership_path(receipt.pid, receipt.process_start_ticks)
        try:
            atomic_create_json(path, asdict(receipt))
        except FileExistsError as exc:
            raise ProcessOwnershipError(f"ownership receipt already exists: {path}") from exc
        self._owned[component_id] = (path, receipt.receipt_hash)
        return path

    def release(self, component_id: str) -> None:
        owned = self._owned.get(component_id)
        if owned is None:
            return
        path, expected_hash = owned
        try:
            receipt = load_process_ownership(path)
        except FileNotFoundError:
            self._owned.pop(component_id, None)
            return
        if receipt.receipt_hash != expected_hash:
            raise ProcessOwnershipError(f"ownership receipt changed before release: {path}")
        _remove_owned_paths(receipt)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(path)
        self._owned.pop(component_id, None)


def load_process_ownership(path: str) -> ProcessOwnershipReceipt:
    try:
        raw = strict_json_load_path(path)
    except FileNotFoundError:
        # Absence is an ordinary idempotency state for release callers.  Keep
        # it distinguishable from a present-but-malformed security receipt.
        raise
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        raise ProcessOwnershipError(f"invalid ownership receipt: {exc}") from exc
    expected = set(ProcessOwnershipReceipt.__dataclass_fields__)
    if not isinstance(raw, dict) or set(raw) != expected:
        raise ProcessOwnershipError("process ownership receipt shape mismatch")
    try:
        receipt = ProcessOwnershipReceipt(**raw)
    except (TypeError, ValueError, KeyError) as exc:
        raise ProcessOwnershipError(f"invalid ownership receipt: {exc}") from exc
    if receipt.receipt_hash != receipt.compute_hash():
        raise ProcessOwnershipError("process ownership receipt hash mismatch")
    return receipt


def release_current_process_ownership(
    path: str,
    *,
    deployment_id: str,
    generation: int,
    rank: int,
    component_id: str,
) -> None:
    """Release the caller's own exact receipt and its owned temporary paths.

    The independent node-local watchdog uses this only after its owner has
    disappeared and every guarded child has been terminated.  Requiring the
    live caller to match the receipt's PID, process-group, start ticks, uid,
    host, and generation prevents a helper from deleting another process's
    state merely because it can name a receipt file.
    """
    receipt = load_process_ownership(path)
    expected_path = process_ownership_path(receipt.pid, receipt.process_start_ticks)
    if os.path.abspath(path) != os.path.abspath(expected_path):
        raise ProcessOwnershipError("ownership receipt path does not match its identity")
    expected = (deployment_id, generation, rank, component_id)
    observed = (
        receipt.deployment_id,
        receipt.generation,
        receipt.rank,
        receipt.component_id,
    )
    if observed != expected:
        raise ProcessOwnershipError(
            f"ownership receipt generation/component mismatch: {observed!r} != {expected!r}"
        )
    if receipt.uid != os.getuid() or receipt.hostname != socket.gethostname():
        raise ProcessOwnershipError("ownership receipt does not belong to this node/user")
    if (
        receipt.pid != os.getpid()
        or receipt.pgid != os.getpgrp()
        or receipt.process_start_ticks != _process_start_ticks(os.getpid())
    ):
        raise ProcessOwnershipError("caller is not the exact process named by the receipt")
    _remove_owned_paths(receipt)
    try:
        os.unlink(path)
    except FileNotFoundError:
        # Another local owner cannot pass the exact identity checks above.
        # Missing after validation therefore means an idempotent retry raced
        # the same process's previous successful unlink.
        pass


def _same_process(receipt: ProcessOwnershipReceipt) -> bool:
    try:
        os.kill(receipt.pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError as exc:
        raise ProcessOwnershipError(
            f"cannot verify ownership of live pid {receipt.pid}: {exc}"
        ) from exc
    try:
        if _process_start_ticks(receipt.pid) != receipt.process_start_ticks:
            return False
        if os.getpgid(receipt.pid) != receipt.pgid:
            return False
        status_text = Path(f"/proc/{receipt.pid}/status").read_text(encoding="utf-8")
        uid_line = next(line for line in status_text.splitlines() if line.startswith("Uid:"))
        return int(uid_line.split()[1]) == receipt.uid == os.getuid()
    except (FileNotFoundError, ProcessLookupError):
        return False
    except (OSError, StopIteration, ValueError, ProcessOwnershipError) as exc:
        raise ProcessOwnershipError(
            f"cannot verify process identity for pid {receipt.pid}: {exc}"
        ) from exc


def _group_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _remove_owned_paths(receipt: ProcessOwnershipReceipt) -> None:
    failures = []
    for path in receipt.temp_paths:
        absolute = os.path.abspath(path)
        if not _is_owned_temp_path(absolute):
            failures.append(f"unsafe path {absolute}")
            continue
        try:
            if os.path.islink(absolute) or os.path.isfile(absolute):
                os.unlink(absolute)
            elif os.path.isdir(absolute):
                shutil.rmtree(absolute)
        except OSError as exc:
            failures.append(f"{absolute}: {exc}")
    if failures:
        raise ProcessOwnershipError("owned runtime cleanup failed: " + "; ".join(failures))


def cleanup_stale_owned_processes(
    *, deployment_id: str, generation: int, deadline_s: float = 30.0
) -> int:
    """Reap superseded generations of this deployment on this node.

    A receipt for another *live* deployment is not stale merely because the
    current process can see it under the same Unix account.  Signalling it
    would let one concurrent deployment kill another.  Likewise, an equal or
    newer live generation of this deployment is a conflict, never a cleanup
    target: an old/repeated launch must not kill the current launch.  Dead
    receipts from any generation/deployment are safe to garbage-collect; live
    groups are signalled only for a strictly older generation of this exact
    deployment.
    """
    if not isinstance(deadline_s, (int, float)) or isinstance(deadline_s, bool):
        raise ProcessOwnershipError("stale cleanup deadline must be numeric")
    if not math.isfinite(float(deadline_s)) or not deadline_s > 0:
        raise ProcessOwnershipError("stale cleanup deadline must be finite and positive")
    directory = os.path.join(ownership_root(), "receipts")
    if not os.path.isdir(directory):
        return 0
    deadline = time.monotonic() + max(0.0, deadline_s)
    cleaned = 0
    for path in sorted(Path(directory).glob("*.json")):
        receipt = load_process_ownership(str(path))
        if receipt.uid != os.getuid() or receipt.hostname != socket.gethostname():
            continue
        live = _same_process(receipt)
        same_deployment = receipt.deployment_id == deployment_id
        if live:
            if not same_deployment:
                continue
            if receipt.generation >= generation:
                relationship = "same" if receipt.generation == generation else "newer"
                raise ProcessOwnershipError(
                    f"live {relationship} generation {receipt.generation} already owns "
                    f"process group {receipt.pgid} for deployment {deployment_id!r}"
                )
            try:
                os.killpg(receipt.pgid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            term_deadline = min(deadline, time.monotonic() + 5.0)
            while time.monotonic() < term_deadline and _group_alive(receipt.pgid):
                time.sleep(0.05)
            if _group_alive(receipt.pgid):
                try:
                    os.killpg(receipt.pgid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            while time.monotonic() < deadline and _group_alive(receipt.pgid):
                time.sleep(0.05)
            if _group_alive(receipt.pgid):
                raise ProcessOwnershipError(
                    f"owned stale process group {receipt.pgid} survived cleanup"
                )
        _remove_owned_paths(receipt)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(path)
        cleaned += 1
    return cleaned
