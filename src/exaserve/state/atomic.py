"""Atomic filesystem primitives (plan WP2.1/WP2.2).

Invariant (WP2 exit gate): interruption at any write boundary leaves either
the previous valid state or the complete new state — never a
successful-looking partial artifact.

Design notes:
- Writes go to a same-directory temp file (mkstemp), are flushed and fsynced,
  then published with ``os.replace`` (atomic on POSIX within one filesystem).
  The containing directory is fsynced afterwards. A directory-fsync error is
  propagated because the rename may be visible without being known durable;
  callers reconcile the exact installed value before retrying.
- ``ExclusiveLease`` is the shared-filesystem run lock (PR-013 class): the
  lease file is created with O_CREAT|O_EXCL (atomic on POSIX and on Lustre),
  carries owner identity, and is stealable only when expired by TTL —
  never merely because the owner hostname differs (the audit's
  foreign-host-equals-stale defect).
"""

from __future__ import annotations

import contextlib
import errno
import json
import math
import os
import secrets as _secrets
import socket
import stat
import tempfile
import threading
import time
from typing import Any

from ..exception_notes import add_exception_note


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number {value!r} is not permitted")


def strict_json_load(handle: Any) -> Any:
    """Decode persisted state without duplicate keys or NaN/Infinity values."""
    return json.load(
        handle,
        object_pairs_hook=_unique_json_object,
        parse_constant=_reject_json_constant,
    )


def strict_json_loads(text: str) -> Any:
    """String form of :func:`strict_json_load`."""
    return json.loads(
        text,
        object_pairs_hook=_unique_json_object,
        parse_constant=_reject_json_constant,
    )


@contextlib.contextmanager
def regular_file_reader(path: str | os.PathLike, *, binary: bool = False, encoding: str = "utf-8"):
    """Open the final path component without following a symlink.

    Checking with ``lstat`` and then calling ``open`` has a replacement window
    in which a shared-filesystem peer can swap in a symlink.  This helper binds
    validation and reading to the same descriptor.
    """
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(os.fspath(path), flags)
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError(f"artifact is not a regular, non-symlink file: {path}")
        if binary:
            handle = os.fdopen(fd, "rb")
        else:
            handle = os.fdopen(fd, "r", encoding=encoding)
        with handle:
            fd = -1
            yield handle
    finally:
        if fd >= 0:
            os.close(fd)


def strict_json_load_path(path: str | os.PathLike) -> Any:
    """Strictly decode JSON from one descriptor-verified regular file."""
    with regular_file_reader(path) as handle:
        return strict_json_load(handle)


def ensure_owned_directory(
    path: str | os.PathLike, *, mode: int = 0o700, parents: bool = True
) -> str:
    """Create/verify one user-owned directory without accepting a final symlink.

    Parent directories are ordinary operator-selected filesystem context, but
    the final state directory is an ExaServe ownership boundary.  It must bind
    to a real directory owned by this uid and must not be writable by another
    uid/group.  Opening with ``O_NOFOLLOW|O_DIRECTORY`` closes the usual
    ``is_dir``/``chmod`` symlink race.
    """

    if type(mode) is not int or not 0 <= mode <= 0o777:
        raise ValueError("directory mode must be an integer permission mask")
    path_str = os.path.abspath(os.fspath(path))
    if parents:
        os.makedirs(path_str, mode=mode, exist_ok=True)
    else:
        try:
            os.mkdir(path_str, mode=mode)
        except FileExistsError:
            pass
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path_str, flags)
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISDIR(metadata.st_mode):
            raise NotADirectoryError(path_str)
        if metadata.st_uid != os.getuid():
            raise PermissionError(f"directory is not owned by the current uid: {path_str}")
        if stat.S_IMODE(metadata.st_mode) & 0o022:
            raise PermissionError(f"directory is writable by another uid/group: {path_str}")
    finally:
        os.close(fd)
    return path_str


def fsync_directory(dirpath: str | os.PathLike) -> None:
    """Make a completed rename durable or report unknown durability.

    The new file may already be visible when this raises. Callers must treat
    that as an ambiguous publication and reconcile exact content before retry.
    """
    fd = os.open(os.fspath(dirpath), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_bytes(path: str | os.PathLike, data: bytes, *, fsync: bool = True) -> None:
    path = os.fspath(path)
    dirpath = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp = tempfile.mkstemp(prefix=f".{os.path.basename(path)}.", suffix=".tmp", dir=dirpath)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            if fsync:
                handle.flush()
                os.fsync(handle.fileno())
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise
    if fsync:
        fsync_directory(dirpath)


def atomic_write_text(path: str | os.PathLike, text: str, *, fsync: bool = True) -> None:
    atomic_write_bytes(path, text.encode("utf-8"), fsync=fsync)


def atomic_create_bytes(path: str | os.PathLike, data: bytes, *, fsync: bool = True) -> None:
    """Publish a complete file exactly once without a replacement window.

    ``O_EXCL`` on the destination would prevent replacement but exposes a
    partially written destination after a crash.  Instead, write and fsync a
    same-directory temporary file, then atomically link it into the final name.
    The link fails with :class:`FileExistsError` when any directory entry
    already owns that name; callers may then reconcile exact existing content.
    """
    path = os.fspath(path)
    dirpath = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp = tempfile.mkstemp(prefix=f".{os.path.basename(path)}.", suffix=".tmp", dir=dirpath)
    published = False
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            if fsync:
                handle.flush()
                os.fsync(handle.fileno())
        os.link(tmp, path, follow_symlinks=False)
        published = True
    finally:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
    if published and fsync:
        fsync_directory(dirpath)


def atomic_create_text(path: str | os.PathLike, text: str, *, fsync: bool = True) -> None:
    atomic_create_bytes(path, text.encode("utf-8"), fsync=fsync)


def atomic_create_json(
    path: str | os.PathLike, data: Any, *, fsync: bool = True, **dump_kwargs: Any
) -> None:
    dump_kwargs.setdefault("indent", 2)
    dump_kwargs.setdefault("sort_keys", True)
    dump_kwargs.setdefault("allow_nan", False)
    atomic_create_text(path, json.dumps(data, **dump_kwargs) + "\n", fsync=fsync)


def atomic_create_yaml(
    path: str | os.PathLike, data: Any, *, fsync: bool = True, **dump_kwargs: Any
) -> None:
    """Create one immutable YAML artifact using the same publish contract."""
    import yaml

    dump_kwargs.setdefault("sort_keys", False)
    atomic_create_text(path, yaml.safe_dump(data, **dump_kwargs), fsync=fsync)


def atomic_create_or_verify_bytes(
    path: str | os.PathLike, data: bytes, *, fsync: bool = True
) -> bool:
    """Create an immutable artifact, or accept an exact existing byte copy.

    Returns ``True`` when this call published the name and ``False`` when an
    earlier publisher already installed the exact bytes. A conflicting file,
    symlink, or non-regular object is never replaced.
    """
    try:
        atomic_create_bytes(path, data, fsync=fsync)
        return True
    except FileExistsError:
        try:
            with regular_file_reader(path, binary=True) as handle:
                observed = handle.read()
        except (OSError, ValueError) as exc:
            raise FileExistsError(
                errno.EEXIST,
                f"immutable artifact exists but is not a verifiable regular file: {exc}",
                os.fspath(path),
            ) from exc
        if observed != data:
            raise FileExistsError(
                errno.EEXIST,
                "immutable artifact already exists with different content",
                os.fspath(path),
            )
        return False


def atomic_create_or_verify_text(path: str | os.PathLike, text: str, *, fsync: bool = True) -> bool:
    return atomic_create_or_verify_bytes(path, text.encode("utf-8"), fsync=fsync)


def atomic_create_or_verify_json(
    path: str | os.PathLike, data: Any, *, fsync: bool = True, **dump_kwargs: Any
) -> bool:
    dump_kwargs.setdefault("indent", 2)
    dump_kwargs.setdefault("sort_keys", True)
    dump_kwargs.setdefault("allow_nan", False)
    return atomic_create_or_verify_text(path, json.dumps(data, **dump_kwargs) + "\n", fsync=fsync)


def atomic_create_or_verify_yaml(
    path: str | os.PathLike, data: Any, *, fsync: bool = True, **dump_kwargs: Any
) -> bool:
    import yaml

    dump_kwargs.setdefault("sort_keys", False)
    return atomic_create_or_verify_text(path, yaml.safe_dump(data, **dump_kwargs), fsync=fsync)


@contextlib.contextmanager
def atomic_text_writer(path: str | os.PathLike, *, fsync: bool = True):
    """Stream a potentially large text artifact, then publish it atomically."""
    path = os.fspath(path)
    dirpath = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp = tempfile.mkstemp(prefix=f".{os.path.basename(path)}.", suffix=".tmp", dir=dirpath)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            yield handle
            if fsync:
                handle.flush()
                os.fsync(handle.fileno())
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise
    if fsync:
        fsync_directory(dirpath)


def atomic_write_json(
    path: str | os.PathLike, data: Any, *, fsync: bool = True, **dump_kwargs: Any
) -> None:
    dump_kwargs.setdefault("indent", 2)
    dump_kwargs.setdefault("sort_keys", True)
    dump_kwargs.setdefault("allow_nan", False)
    atomic_write_text(path, json.dumps(data, **dump_kwargs) + "\n", fsync=fsync)


def atomic_write_yaml(
    path: str | os.PathLike, data: Any, *, fsync: bool = True, **dump_kwargs: Any
) -> None:
    import yaml

    dump_kwargs.setdefault("sort_keys", False)
    atomic_write_text(path, yaml.safe_dump(data, **dump_kwargs), fsync=fsync)


class LeaseHeldError(RuntimeError):
    """The lease is validly held by another owner."""

    def __init__(self, path: str, owner: dict[str, Any]):
        self.owner = owner
        super().__init__(f"lease {path} held by {owner}")


class LeaseReleaseError(RuntimeError):
    """A lease could not be released and remains retryable by its owner."""


class ExclusiveLease:
    """Cross-host exclusive lease on a shared filesystem.

    Acquisition is O_CREAT|O_EXCL — atomic, no check-then-write window.
    A held lease may be taken over only when its TTL has expired; a live
    lease from another host is respected (plan WP2.2; audit PR-013).
    Same-host takeover is additionally allowed when the owning PID is dead.
    """

    def __init__(
        self, path: str | os.PathLike, *, ttl_s: float = 3600.0, owner_note: str = ""
    ) -> None:
        self.path = os.fspath(path)
        if not isinstance(self.path, str) or not self.path:
            raise ValueError("lease path must be nonempty text")
        if (
            isinstance(ttl_s, bool)
            or not isinstance(ttl_s, (int, float))
            or not math.isfinite(float(ttl_s))
            or ttl_s <= 0
        ):
            raise ValueError("lease TTL must be finite and positive")
        if not isinstance(owner_note, str):
            raise ValueError("lease owner note must be text")
        self.ttl_s = float(ttl_s)
        self.owner_note = owner_note
        self._held = False
        # IMP-B07: a unique fencing token per acquisition. release() and
        # renew() only act when the on-disk lease still carries OUR token, so
        # a stale holder can never delete or extend a successor's lease.
        self._token: str | None = None

    # -- helpers -----------------------------------------------------------
    def _identity(self) -> dict[str, Any]:
        return {
            "host": socket.gethostname(),
            "pid": os.getpid(),
            "acquired_at": time.time(),
            "ttl_s": self.ttl_s,
            "note": self.owner_note,
            "token": self._token,
        }

    def _current_token(self) -> str | None:
        owner = self.read_owner()
        return owner.get("token") if owner else None

    def holds_lease(self) -> bool:
        """True iff the on-disk lease is still ours (fencing check)."""
        return bool(self._held and self._token and self._current_token() == self._token)

    def read_owner(self) -> dict[str, Any] | None:
        def unreadable_owner() -> dict[str, Any]:
            acquired_at = time.time()
            try:
                metadata = os.lstat(self.path)
                if stat.S_ISREG(metadata.st_mode) and metadata.st_uid == os.getuid():
                    acquired_at -= max(0.0, time.time() - metadata.st_mtime)
            except OSError:
                pass
            return {
                "host": "?",
                "pid": -1,
                "acquired_at": acquired_at,
                "ttl_s": self.ttl_s,
                "note": "unreadable",
            }

        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            fd = os.open(self.path, flags)
        except FileNotFoundError:
            return None
        except OSError:
            return unreadable_owner()
        try:
            metadata = os.fstat(fd)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid():
                os.close(fd)
                return unreadable_owner()
            with os.fdopen(fd, "r", encoding="utf-8") as handle:
                owner = strict_json_load(handle)
        except (json.JSONDecodeError, ValueError, OSError, UnicodeError):
            with contextlib.suppress(OSError):
                os.close(fd)
            return unreadable_owner()
        if not isinstance(owner, dict):
            return unreadable_owner()
        return owner

    def _expired(self, owner: dict[str, Any]) -> bool:
        ttl = owner.get("ttl_s", self.ttl_s)
        acquired_at = owner.get("acquired_at", 0)
        if (
            isinstance(ttl, bool)
            or not isinstance(ttl, (int, float))
            or isinstance(acquired_at, bool)
            or not isinstance(acquired_at, (int, float))
        ):
            return False
        if not math.isfinite(float(ttl)) or ttl <= 0 or not math.isfinite(float(acquired_at)):
            return False
        age = time.time() - acquired_at
        if age > ttl:
            return True
        if (
            owner.get("host") == socket.gethostname()
            and isinstance(owner.get("pid"), int)
            and not isinstance(owner.get("pid"), bool)
            and owner["pid"] > 0
        ):
            try:
                os.kill(owner["pid"], 0)
            except ProcessLookupError:
                return True
            except OSError:
                return False
        return False

    def _try_arbitration(self) -> int | None:
        """Acquire the per-lease mutation lock, or fail closed.

        Takeover, renewal, and release all mutate the same directory entry.
        They therefore have to participate in one arbitration protocol; a
        token check followed by an uncoordinated replace/unlink is not a CAS
        and can otherwise overwrite or remove a successor's lease.
        """
        arbitration = self.path + ".takeover.lock"
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        for attempt in range(2):
            try:
                return os.open(arbitration, flags, 0o600)
            except FileExistsError:
                if attempt:
                    return None
                try:
                    metadata = os.lstat(arbitration)
                    if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid():
                        return None
                    if time.time() - metadata.st_mtime > 60:
                        os.unlink(arbitration)
                        continue
                except OSError:
                    continue
                return None
        return None

    def _release_arbitration(self, fd: int) -> None:
        owned = os.fstat(fd)
        os.close(fd)
        arbitration = self.path + ".takeover.lock"
        try:
            current = os.lstat(arbitration)
        except FileNotFoundError:
            return
        if (current.st_dev, current.st_ino) == (owned.st_dev, owned.st_ino):
            os.unlink(arbitration)

    # -- lifecycle ----------------------------------------------------------
    def acquire(self) -> "ExclusiveLease":
        if self._held:  # idempotent only while the fencing token is current
            if self.holds_lease():
                return self
            self._held = False
            self._token = None
        while True:
            self._token = _secrets.token_hex(16)  # fresh fencing token
            try:
                flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
                if hasattr(os, "O_NOFOLLOW"):
                    flags |= os.O_NOFOLLOW
                fd = os.open(self.path, flags, 0o600)
            except FileExistsError:
                owner = self.read_owner()
                if owner is None:
                    continue  # vanished between EXCL failure and read: retry
                if not self._expired(owner):
                    raise LeaseHeldError(self.path, owner) from None
                # IMP-B07: expired-lease takeover must elect EXACTLY ONE
                # winner. `os.replace` alone does not — two stealers can both
                # rename over the lease. Instead, take a per-path arbitration
                # lock via O_EXCL on a sidecar, re-verify expiry under it, and
                # only then publish. The loser sees a live lease and retries.
                afd = self._try_arbitration()
                if afd is None:
                    time.sleep(0.05)
                    continue
                try:
                    current = self.read_owner()
                    if current is not None and not self._expired(current):
                        # Someone else already took over while we waited.
                        raise LeaseHeldError(self.path, current) from None
                    atomic_write_json(
                        self.path,
                        self._identity() | {"stole_from": owner},
                    )
                    self._held = True
                    return self
                finally:
                    self._release_arbitration(afd)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(self._identity(), handle)
                handle.flush()
                os.fsync(handle.fileno())
            self._held = True
            return self

    def renew(self) -> bool:
        """Extend the TTL if we still hold the lease (IMP-B07: long
        downloads/submission loops must not silently outlive their TTL).
        Returns False if the lease was lost — the caller must stop."""
        afd = self._try_arbitration()
        if afd is None:
            return False
        try:
            if not self.holds_lease():
                return False
            atomic_write_json(self.path, self._identity(), fsync=False)
            return True
        finally:
            self._release_arbitration(afd)

    def release(self) -> None:
        # IMP-B07: only delete the lease if it is STILL OURS. A stale holder
        # releasing after its TTL expired previously deleted the successor's
        # live lease.
        if not self._held:
            return
        afd = self._try_arbitration()
        if afd is None:
            raise LeaseReleaseError(
                f"could not acquire arbitration lock to release lease {self.path!r}"
            )
        try:
            if self.holds_lease():
                try:
                    os.unlink(self.path)
                except FileNotFoundError:
                    pass
                except OSError as exc:
                    raise LeaseReleaseError(
                        f"could not release lease {self.path!r}: {exc}"
                    ) from exc
        finally:
            self._release_arbitration(afd)
        self._held = False
        self._token = None

    def __enter__(self) -> "ExclusiveLease":
        return self.acquire()

    def __exit__(self, _exc_type: object, exc: object, _tb: object) -> None:
        try:
            self.release()
        except LeaseReleaseError as release_exc:
            if isinstance(exc, BaseException):
                add_exception_note(exc, f"lease cleanup also failed: {release_exc}")
                return
            raise


class LeaseHeartbeat:
    """Renew a held lease while a blocking external operation runs.

    The operation cannot always be interrupted safely (for example a library
    download), so callers must invoke :meth:`ensure_held` before publishing
    their candidate.  Lease loss then converts into a failed transaction.
    """

    def __init__(self, lease: ExclusiveLease, *, interval_s: float = 30.0) -> None:
        if (
            isinstance(interval_s, bool)
            or not isinstance(interval_s, (int, float))
            or not math.isfinite(float(interval_s))
            or interval_s <= 0
        ):
            raise ValueError("lease heartbeat interval must be positive")
        self.lease = lease
        self.interval_s = float(interval_s)
        self._stop = threading.Event()
        self._lost = threading.Event()
        self._thread: threading.Thread | None = None
        self._failure: BaseException | None = None

    def __enter__(self) -> "LeaseHeartbeat":
        if not self.lease.holds_lease():
            raise LeaseHeldError(self.lease.path, self.lease.read_owner() or {})

        def _renew() -> None:
            while not self._stop.wait(self.interval_s):
                try:
                    renewed = self.lease.renew()
                except BaseException as exc:
                    self._failure = exc
                    self._lost.set()
                    return
                if not renewed:
                    self._failure = LeaseHeldError(
                        self.lease.path,
                        self.lease.read_owner() or {"note": "lease renewal rejected"},
                    )
                    self._lost.set()
                    return

        self._thread = threading.Thread(target=_renew, name="exaserve-lease-heartbeat", daemon=True)
        self._thread.start()
        return self

    def start(self) -> "LeaseHeartbeat":
        """Start renewal for owners that cannot conveniently nest a ``with`` block."""
        return self.__enter__()

    def ensure_held(self) -> None:
        if self._lost.is_set() or not self.lease.holds_lease():
            error = LeaseHeldError(
                self.lease.path, self.lease.read_owner() or {"note": "lease lost"}
            )
            if self._failure is not None:
                raise error from self._failure
            raise error

    def __exit__(self, _exc_type: object, exc: object, _tb: object) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=min(5.0, self.interval_s + 0.1))
        cleanup_error: BaseException | None = None
        if self._thread is not None and self._thread.is_alive():
            cleanup_error = RuntimeError("lease heartbeat thread did not stop by its deadline")
        elif self._failure is not None:
            cleanup_error = RuntimeError(f"lease heartbeat failed: {self._failure}")
        if cleanup_error is not None:
            if isinstance(exc, BaseException):
                add_exception_note(exc, str(cleanup_error))
                return
            raise cleanup_error from self._failure

    def stop(self, active_error: BaseException | None = None) -> None:
        """Stop renewal, attaching cleanup failure to an active transaction error."""
        self.__exit__(
            type(active_error) if active_error is not None else None,
            active_error,
            active_error.__traceback__ if active_error is not None else None,
        )
