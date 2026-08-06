"""Atomic filesystem primitives (plan WP2.1/WP2.2).

Invariant (WP2 exit gate): interruption at any write boundary leaves either
the previous valid state or the complete new state — never a
successful-looking partial artifact.

Design notes:
- Writes go to a same-directory temp file (mkstemp), are flushed and fsynced,
  then published with ``os.replace`` (atomic on POSIX within one filesystem).
  The directory entry is fsynced best-effort afterwards; on Lustre a
  directory fsync may be a no-op, which is acceptable — the rename itself is
  still atomic, and the KNOWN_ISSUES C3 trailing-byte behavior this replaces
  came from in-place ``open(path, "w")`` truncation, not from rename.
- ``ExclusiveLease`` is the shared-filesystem run lock (PR-013 class): the
  lease file is created with O_CREAT|O_EXCL (atomic on POSIX and on Lustre),
  carries owner identity, and is stealable only when expired by TTL —
  never merely because the owner hostname differs (the audit's
  foreign-host-equals-stale defect).
"""

from __future__ import annotations

import contextlib
import json
import os
import secrets as _secrets
import socket
import tempfile
import time
from typing import Any


def _fsync_dir(dirpath: str) -> None:
    with contextlib.suppress(OSError):
        fd = os.open(dirpath, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def atomic_write_bytes(path: str | os.PathLike, data: bytes, *, fsync: bool = True) -> None:
    path = os.fspath(path)
    dirpath = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp = tempfile.mkstemp(
        prefix=f".{os.path.basename(path)}.", suffix=".tmp", dir=dirpath
    )
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
        _fsync_dir(dirpath)


def atomic_write_text(path: str | os.PathLike, text: str, *, fsync: bool = True) -> None:
    atomic_write_bytes(path, text.encode("utf-8"), fsync=fsync)


def atomic_write_json(path: str | os.PathLike, data: Any, *, fsync: bool = True,
                      **dump_kwargs: Any) -> None:
    dump_kwargs.setdefault("indent", 2)
    dump_kwargs.setdefault("sort_keys", True)
    atomic_write_text(path, json.dumps(data, **dump_kwargs) + "\n", fsync=fsync)


def atomic_write_yaml(path: str | os.PathLike, data: Any, *, fsync: bool = True,
                      **dump_kwargs: Any) -> None:
    import yaml

    dump_kwargs.setdefault("sort_keys", False)
    atomic_write_text(path, yaml.safe_dump(data, **dump_kwargs), fsync=fsync)


class LeaseHeldError(RuntimeError):
    """The lease is validly held by another owner."""

    def __init__(self, path: str, owner: dict[str, Any]):
        self.owner = owner
        super().__init__(f"lease {path} held by {owner}")


class ExclusiveLease:
    """Cross-host exclusive lease on a shared filesystem.

    Acquisition is O_CREAT|O_EXCL — atomic, no check-then-write window.
    A held lease may be taken over only when its TTL has expired; a live
    lease from another host is respected (plan WP2.2; audit PR-013).
    Same-host takeover is additionally allowed when the owning PID is dead.
    """

    def __init__(self, path: str | os.PathLike, *, ttl_s: float = 3600.0,
                 owner_note: str = "") -> None:
        self.path = os.fspath(path)
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
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                return json.load(handle)
        except FileNotFoundError:
            return None
        except (json.JSONDecodeError, OSError):
            # Torn/unreadable lease: treat as present but expired-unknown;
            # only the TTL path may clear it (age from mtime).
            try:
                age = time.time() - os.stat(self.path).st_mtime
            except OSError:
                return None
            return {"host": "?", "pid": -1, "acquired_at": time.time() - age,
                    "ttl_s": self.ttl_s, "note": "unreadable"}

    def _expired(self, owner: dict[str, Any]) -> bool:
        ttl = float(owner.get("ttl_s", self.ttl_s))
        age = time.time() - float(owner.get("acquired_at", 0))
        if age > ttl:
            return True
        if (
            owner.get("host") == socket.gethostname()
            and isinstance(owner.get("pid"), int)
            and owner["pid"] > 0
        ):
            try:
                os.kill(owner["pid"], 0)
            except ProcessLookupError:
                return True
            except OSError:
                return False
        return False

    # -- lifecycle ----------------------------------------------------------
    def acquire(self) -> "ExclusiveLease":
        if self._held:  # idempotent for the holder (with-statement re-entry)
            return self
        while True:
            self._token = _secrets.token_hex(16)  # fresh fencing token
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
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
                arb = self.path + ".takeover.lock"
                try:
                    afd = os.open(arb, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
                except FileExistsError:
                    # Another stealer is arbitrating. If its lock is itself
                    # stale (crashed mid-takeover), clear it; else retry.
                    try:
                        if time.time() - os.stat(arb).st_mtime > 60:
                            os.unlink(arb)
                    except OSError:
                        pass
                    time.sleep(0.05)
                    continue
                try:
                    current = self.read_owner()
                    if current is not None and not self._expired(current):
                        # Someone else already took over while we waited.
                        raise LeaseHeldError(self.path, current) from None
                    takeover = arb + ".new"
                    with open(takeover, "w", encoding="utf-8") as handle:
                        json.dump(self._identity() | {"stole_from": owner}, handle)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.replace(takeover, self.path)
                    self._held = True
                    return self
                finally:
                    os.close(afd)
                    with contextlib.suppress(FileNotFoundError):
                        os.unlink(arb)
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
        if not self.holds_lease():
            return False
        atomic_write_json(self.path, self._identity(), fsync=False)
        return True

    def release(self) -> None:
        # IMP-B07: only delete the lease if it is STILL OURS. A stale holder
        # releasing after its TTL expired previously deleted the successor's
        # live lease.
        if self._held:
            if self.holds_lease():
                with contextlib.suppress(FileNotFoundError):
                    os.unlink(self.path)
            self._held = False
            self._token = None

    def __enter__(self) -> "ExclusiveLease":
        return self.acquire()

    def __exit__(self, *exc: object) -> None:
        self.release()
