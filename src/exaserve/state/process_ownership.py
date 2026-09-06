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
import stat
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


def _runtime_site_profile():
    site_path = os.environ.get("EXASERVE_SITE_PROFILE_PATH", "").strip()
    if not site_path:
        runtime_root = os.environ.get("EXASERVE_LOCAL_RUNTIME_ROOT", "").strip()
        if runtime_root:
            site_path = os.path.join(runtime_root, "run", "site.profile.json")
    if not site_path:
        return None
    from ..plan.io import load_site_profile
    from ..site import require_complete_filesystem_policy

    profile = load_site_profile(site_path)
    require_complete_filesystem_policy(profile)
    return profile


def _approved_local_root(candidate: Path, profile) -> Path:
    if profile is not None:
        roots = [
            Path(key.removeprefix("local_root:"))
            for key, _value in profile.filesystem_semantics
            if key.startswith("local_root:")
        ]
        matches = [root for root in roots if candidate == root or root in candidate.parents]
        if not matches:
            raise ProcessOwnershipError(
                f"ownership path is outside SiteProfile local roots: {candidate}"
            )
        return max(matches, key=lambda item: len(item.parts))
    # Standalone unit/tool callers have no runtime SiteProfile. Keep their
    # scratch boundary narrow; managed ranks always take the profile branch.
    tmp = Path("/tmp")
    if candidate == tmp or tmp in candidate.parents:
        return tmp
    parent = candidate.parent
    while not parent.exists() and parent != parent.parent:
        parent = parent.parent
    return parent


def _lexically_shared(candidate: Path, profile) -> bool:
    from ..plan.runtime_environment import shared_roots

    return any(candidate == root or root in candidate.parents for root in shared_roots(profile))


def _open_private_directory_chain(candidate: Path, *, local_root: Path, create: bool) -> None:
    """Open/create without following any component or crossing the local mount."""

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(candidate.anchor, flags)
    current = Path(candidate.anchor)
    local_device = None
    try:
        for part in candidate.parts[1:]:
            current /= part
            try:
                child = os.open(part, flags, dir_fd=descriptor)
            except FileNotFoundError:
                if not create or local_device is None:
                    raise
                os.mkdir(part, 0o700, dir_fd=descriptor)
                child = os.open(part, flags, dir_fd=descriptor)
            except OSError as exc:
                raise ProcessOwnershipError(
                    f"ownership directory contains an unsafe component {current}: {exc}"
                ) from exc
            metadata = os.fstat(child)
            if not stat.S_ISDIR(metadata.st_mode):
                os.close(child)
                raise ProcessOwnershipError(
                    f"ownership directory component is not a directory: {current}"
                )
            if current == local_root:
                local_device = metadata.st_dev
            elif local_device is not None:
                if metadata.st_dev != local_device:
                    os.close(child)
                    raise ProcessOwnershipError(
                        f"ownership directory crosses a filesystem boundary: {current}"
                    )
                if metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) & 0o022:
                    os.close(child)
                    raise ProcessOwnershipError(
                        f"ownership directory is not private to this uid: {current}"
                    )
            os.close(descriptor)
            descriptor = child
        if local_device is None:
            raise ProcessOwnershipError(
                f"ownership directory is not beneath its approved local root: {candidate}"
            )
    finally:
        os.close(descriptor)


def _prepare_private_local_directory(path: str | os.PathLike, *, create: bool) -> str:
    candidate = Path(os.path.abspath(os.fspath(path)))
    if (
        not candidate.is_absolute()
        or candidate == Path(candidate.anchor)
        or ".." in candidate.parts
        or "\x00" in str(candidate)
    ):
        raise ProcessOwnershipError(f"ownership directory path is unsafe: {candidate}")
    profile = _runtime_site_profile()
    local_root = _approved_local_root(candidate, profile)
    if (
        candidate == local_root
        or _lexically_shared(candidate, profile)
        or _lexically_shared(local_root, profile)
    ):
        raise ProcessOwnershipError(f"ownership directory is not a private local path: {candidate}")
    if profile is not None:
        # Verify the already-existing declared root before creating a single
        # child. Otherwise a wrongly mounted /tmp could receive one mkdir
        # before the post-creation identity check rejected it.
        from ..plan.runtime_environment import RuntimePathError, validate_declared_filesystem

        try:
            _open_private_directory_chain(local_root, local_root=local_root, create=False)
            if (
                validate_declared_filesystem(
                    local_root,
                    policy=profile,
                    root_kind="local_root",
                )
                is None
            ):
                raise RuntimePathError("ownership local root has no filesystem identity")
        except RuntimePathError as exc:
            raise ProcessOwnershipError(str(exc)) from exc
    _open_private_directory_chain(candidate, local_root=local_root, create=create)
    if profile is not None:
        from ..plan.runtime_environment import (
            require_contained_local_path,
            validate_declared_filesystem,
        )

        try:
            if (
                validate_declared_filesystem(
                    candidate,
                    policy=profile,
                    root_kind="local_root",
                )
                is None
            ):
                raise RuntimePathError("ownership directory has no filesystem identity")
            require_contained_local_path(
                candidate,
                local_root,
                policy=profile,
                name="ownership directory",
                require_exists=True,
            )
        except RuntimePathError as exc:
            raise ProcessOwnershipError(str(exc)) from exc
    return str(candidate)


def prepare_ownership_root(*, create: bool = True) -> str:
    return _prepare_private_local_directory(ownership_root(), create=create)


def prepare_runtime_ownership_root(*, create: bool = True) -> str:
    return _prepare_private_local_directory(runtime_ownership_root(), create=create)


def generation_runtime_root(deployment_id: str, generation: int, rank: int) -> str:
    token = canonical_hash(
        {
            "deployment_id": deployment_id,
            "generation": generation,
            "rank": rank,
        }
    )[:20]
    root = prepare_runtime_ownership_root()
    candidate = os.path.join(root, token)
    return _prepare_private_local_directory(candidate, create=True)


def _is_owned_temp_path(
    path: str,
    *,
    deployment_id: str | None = None,
    generation: int | None = None,
) -> bool:
    absolute = os.path.abspath(path)
    for root in (ownership_root(), runtime_ownership_root()):
        if os.path.commonpath((root, absolute)) == root and absolute != root:
            return True
    if deployment_id is not None and generation is not None:
        from types import SimpleNamespace

        from ..plan.runtime_environment import default_local_state_root

        expected = default_local_state_root(
            SimpleNamespace(deployment_id=deployment_id), generation
        )
        # State ownership is intentionally exact, not "anything under /tmp".
        # The receipt's deployment/generation fields cryptographically bind
        # this one directory and stale cleanup re-derives the same path.
        if absolute == os.path.abspath(expected):
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
            if not _is_owned_temp_path(
                path,
                deployment_id=self.deployment_id,
                generation=self.generation,
            ):
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
        root = prepare_ownership_root()
        self.directory = _prepare_private_local_directory(
            os.path.join(root, "receipts"), create=True
        )
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


def _validate_receipt_file(path: str) -> str:
    root = prepare_ownership_root(create=False)
    directory = _prepare_private_local_directory(os.path.join(root, "receipts"), create=False)
    candidate = os.path.abspath(path)
    if os.path.dirname(candidate) != directory:
        raise ProcessOwnershipError("ownership receipt escapes the private receipt directory")
    try:
        metadata = os.lstat(candidate)
    except FileNotFoundError:
        raise
    except OSError as exc:
        raise ProcessOwnershipError(f"ownership receipt cannot be inspected safely: {exc}") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or _mount_identity(candidate) != _mount_identity(directory)
    ):
        raise ProcessOwnershipError("ownership receipt is not a same-filesystem owned regular file")
    return candidate


def load_process_ownership(path: str) -> ProcessOwnershipReceipt:
    try:
        raw = strict_json_load_path(_validate_receipt_file(path))
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


def _mount_identity(path: str | os.PathLike) -> tuple[str, str, str]:
    """Return the kernel mount ID/point/type covering one existing path.

    Overlayfs may report different ``st_dev`` values for a regular file and
    its parent directory even though both belong to the same mount. Linux
    mountinfo is the authoritative namespace identity for containment.
    """

    try:
        resolved = Path(path).resolve(strict=True)
        lines = Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise ProcessOwnershipError(f"cannot inspect ownership mount identity: {exc}") from exc
    matches: list[tuple[int, tuple[str, str, str]]] = []
    for line in lines:
        try:
            left, right = line.split(" - ", 1)
            left_fields = left.split()
            right_fields = right.split()
            mount_text = left_fields[4]
            for encoded, decoded in (
                ("\\040", " "),
                ("\\011", "\t"),
                ("\\012", "\n"),
                ("\\134", "\\"),
            ):
                mount_text = mount_text.replace(encoded, decoded)
            mount_point = Path(mount_text)
            if resolved != mount_point and mount_point not in resolved.parents:
                continue
            identity = (left_fields[0], str(mount_point), right_fields[0])
            matches.append((len(mount_point.parts), identity))
        except (IndexError, ValueError):
            continue
    if not matches:
        raise ProcessOwnershipError(f"no mountinfo entry covers ownership path: {resolved}")
    return max(matches, key=lambda item: item[0])[1]


def _remove_owned_paths(
    receipt: ProcessOwnershipReceipt,
    *,
    preserve_paths: tuple[str, ...] = (),
) -> None:
    preserved = {os.path.abspath(path) for path in preserve_paths}
    failures = []
    for path in receipt.temp_paths:
        absolute = os.path.abspath(path)
        if absolute in preserved:
            continue
        if not _is_owned_temp_path(
            absolute,
            deployment_id=receipt.deployment_id,
            generation=receipt.generation,
        ):
            failures.append(f"unsafe path {absolute}")
            continue
        try:
            parent = _prepare_private_local_directory(os.path.dirname(absolute), create=False)
            metadata = os.lstat(absolute)
            if stat.S_ISLNK(metadata.st_mode):
                raise ProcessOwnershipError(f"owned path is a symlink: {absolute}")
            if stat.S_ISREG(metadata.st_mode):
                if metadata.st_uid != os.getuid() or _mount_identity(absolute) != _mount_identity(
                    parent
                ):
                    raise ProcessOwnershipError(f"owned file is not local/private: {absolute}")
                os.unlink(absolute)
            elif stat.S_ISDIR(metadata.st_mode):
                _prepare_private_local_directory(absolute, create=False)
                shutil.rmtree(absolute)
            else:
                raise ProcessOwnershipError(f"owned path has an unsupported type: {absolute}")
        except FileNotFoundError:
            continue
        except ProcessOwnershipError as exc:
            failures.append(str(exc))
        except OSError as exc:
            failures.append(f"{absolute}: {exc}")
    if failures:
        raise ProcessOwnershipError("owned runtime cleanup failed: " + "; ".join(failures))


def _terminate_owned_group(receipt: ProcessOwnershipReceipt, *, deadline: float) -> None:
    if not _group_alive(receipt.pgid):
        return
    try:
        os.killpg(receipt.pgid, signal.SIGTERM)
    except ProcessLookupError:
        return
    term_deadline = min(deadline, time.monotonic() + 5.0)
    while time.monotonic() < term_deadline and _group_alive(receipt.pgid):
        time.sleep(0.05)
    if _group_alive(receipt.pgid):
        try:
            os.killpg(receipt.pgid, signal.SIGKILL)
        except ProcessLookupError:
            return
    while time.monotonic() < deadline and _group_alive(receipt.pgid):
        time.sleep(0.05)
    if _group_alive(receipt.pgid):
        raise ProcessOwnershipError(f"owned process group {receipt.pgid} survived cleanup")


def _existing_receipt_directory() -> Path | None:
    try:
        root = prepare_ownership_root(create=False)
        directory = _prepare_private_local_directory(os.path.join(root, "receipts"), create=False)
    except FileNotFoundError:
        return None
    return Path(directory)


def cleanup_owned_component_processes(
    *,
    deployment_id: str,
    generation: int,
    rank: int,
    component_id: str,
    deadline_s: float,
) -> int:
    """Reap exact current-generation child groups after their guardian dies."""

    if not all(isinstance(value, str) and value for value in (deployment_id, component_id)):
        raise ProcessOwnershipError("owned component cleanup identity is invalid")
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in (generation, rank)
    ):
        raise ProcessOwnershipError("owned component cleanup generation/rank is invalid")
    if (
        isinstance(deadline_s, bool)
        or not isinstance(deadline_s, (int, float))
        or not math.isfinite(float(deadline_s))
        or deadline_s <= 0
    ):
        raise ProcessOwnershipError("owned component cleanup deadline must be finite and positive")
    directory = _existing_receipt_directory()
    if directory is None:
        return 0
    deadline = time.monotonic() + float(deadline_s)
    cleaned = 0
    receipts = [
        (path, load_process_ownership(str(path))) for path in sorted(directory.glob("*.json"))
    ]
    if component_id == "ray_child" and any(
        receipt.uid == os.getuid()
        and receipt.hostname == socket.gethostname()
        and receipt.deployment_id == deployment_id
        and receipt.generation == generation
        and receipt.rank == rank
        and receipt.component_id == "ray"
        and _same_process(receipt)
        for _path, receipt in receipts
    ):
        raise ProcessOwnershipError(
            "refusing child fallback cleanup while its exact guardian is still live"
        )
    for path, receipt in receipts:
        if (
            receipt.uid != os.getuid()
            or receipt.hostname != socket.gethostname()
            or receipt.deployment_id != deployment_id
            or receipt.generation != generation
            or receipt.rank != rank
            or receipt.component_id != component_id
        ):
            continue
        _terminate_owned_group(receipt, deadline=deadline)
        _remove_owned_paths(receipt)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(path)
        cleaned += 1
    return cleaned


def cleanup_stale_owned_processes(
    *, deployment_id: str, generation: int, deadline_s: float = 30.0
) -> int:
    """Reap superseded generations of this deployment on this node.

    A receipt for another *live* deployment is not stale merely because the
    current process can see it under the same Unix account.  Signalling it
    would let one concurrent deployment kill another.  Likewise, an equal or
    newer live generation of this deployment is a conflict, never a cleanup
    target: an old/repeated launch must not kill the current launch. Dead
    receipts from any generation/deployment are safe to garbage-collect,
    except that a dead receipt from the current generation may name the exact
    state root that source staging has already republished for this retry. That
    root is preserved while the dead receipt and its other owned paths are
    removed. Live groups are signalled only for a strictly older generation of
    this exact deployment.
    """
    if not isinstance(deadline_s, (int, float)) or isinstance(deadline_s, bool):
        raise ProcessOwnershipError("stale cleanup deadline must be numeric")
    if not math.isfinite(float(deadline_s)) or not deadline_s > 0:
        raise ProcessOwnershipError("stale cleanup deadline must be finite and positive")
    directory = _existing_receipt_directory()
    if directory is None:
        return 0
    deadline = time.monotonic() + max(0.0, deadline_s)
    cleaned = 0
    for path in sorted(directory.glob("*.json")):
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
            _terminate_owned_group(receipt, deadline=deadline)
        preserve_paths: tuple[str, ...] = ()
        if same_deployment and receipt.generation == generation:
            from types import SimpleNamespace

            from ..plan.runtime_environment import default_local_state_root

            preserve_paths = (
                default_local_state_root(
                    SimpleNamespace(deployment_id=deployment_id),
                    generation,
                ),
            )
        _remove_owned_paths(receipt, preserve_paths=preserve_paths)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(path)
        cleaned += 1
    return cleaned
