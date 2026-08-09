"""Node-local port ownership (plan WP4/WP7, audit PR-012, KI-A2, TD-PORTS).

`get_open_port` bound a probe socket, closed it, and returned the number. That
is a time-of-check/time-of-use race, and at scale it is not theoretical: a node
starts twelve-plus replicas that all scan from the same base port, every one of
them probes the same free port in the same instant, and all but one then fail
with `EADDRINUSE` when the real consumer binds (KI-A2).

A `PortLease` closes the dominant case by making the reservation outlive the
probe. The lock file is created `O_EXCL` and held until released, so no other
ExaServe process on this node will choose that port even after the probe socket
is closed — which is exactly the window the old code left open.

Honest scope: this owns ports *among ExaServe processes on one node*. A foreign
process can still take a port between our probe and the consumer's bind. That
residual race is unavoidable without holding the socket through the handoff,
which we cannot do because the consumer (vLLM, torch.distributed, a proxy)
binds the port itself.
"""

from __future__ import annotations

import errno
import fcntl
import json
import os
import secrets
import socket
import stat
import time
from contextlib import contextmanager
from typing import Optional

from ..exception_notes import add_exception_note
from .atomic import strict_json_loads

DEFAULT_LEASE_DIR = f"/tmp/exaserve-ports-{os.getuid()}"
# A lease older than this with no live owner is reclaimable. Bring-up can be
# slow, so this is generous: reclaiming too eagerly reintroduces the collision.
STALE_AFTER_S = 3600.0


class PortUnavailable(RuntimeError):
    """No port in the requested range could be leased."""


class PortReleaseError(RuntimeError):
    """A lease could not be released and remains available for retry."""


class ListeningSocket:
    """A bound/listening socket handed directly to an owned child process."""

    def __init__(self, sock: socket.socket) -> None:
        self.socket = sock
        self.port = int(sock.getsockname()[1])
        self._closed = False

    def fileno(self) -> int:
        return self.socket.fileno()

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self.socket.close()

    def __enter__(self) -> "ListeningSocket":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def bind_listener(port: int, *, host: str = "0.0.0.0", backlog: int = 4096) -> ListeningSocket:
    """Own a TCP port continuously through ``Popen(pass_fds=...)`` handoff."""
    if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65535:
        raise PortUnavailable(f"invalid listener port {port!r}")
    if isinstance(backlog, bool) or not isinstance(backlog, int) or backlog < 1:
        raise PortUnavailable("listener backlog must be positive")
    if not isinstance(host, str) or not host:
        raise PortUnavailable("listener host must be non-empty text")
    sock = socket.socket(socket.AF_INET6 if ":" in host else socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.listen(backlog)
        sock.set_inheritable(True)
        return ListeningSocket(sock)
    except OSError as exc:
        sock.close()
        raise PortUnavailable(f"could not bind listener {host}:{port}: {exc}") from exc


def _lease_dir() -> str:
    return os.environ.get("EXASERVE_PORT_LEASE_DIR", DEFAULT_LEASE_DIR)


def _secure_lease_dir(directory: str) -> None:
    """Require a private, user-owned, non-symlink lease namespace."""
    os.makedirs(directory, mode=0o700, exist_ok=True)
    metadata = os.lstat(directory)
    if not stat.S_ISDIR(metadata.st_mode):
        raise OSError(f"port lease path {directory!r} is not a directory")
    if metadata.st_uid != os.getuid():
        raise OSError(f"port lease directory {directory!r} is not owned by this user")
    os.chmod(directory, 0o700)


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, owned by someone else
    except OSError:
        return False


def _bindable(port: int, host: str) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        try:
            probe.bind((host, port))
            return True
        except OSError:
            return False


def _read_claim(path: str) -> tuple[int, Optional[str]]:
    """Read both current JSON claims and the legacy ``PID timestamp`` form."""
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags)
    try:
        metadata = os.fstat(fd)
    except BaseException:
        os.close(fd)
        raise
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid():
        os.close(fd)
        raise ValueError("port claim is not a user-owned regular file")
    with os.fdopen(fd, encoding="utf-8") as handle:
        text = handle.read()
    try:
        payload = strict_json_loads(text)
    except json.JSONDecodeError:
        fields = text.split()
        return int(fields[0] if fields else "-1"), None
    if not isinstance(payload, dict):
        raise ValueError("port claim is not an object")
    pid = payload.get("pid")
    token = payload.get("token")
    if isinstance(pid, bool) or not isinstance(pid, int):
        raise ValueError("port claim pid is invalid")
    if not isinstance(token, str) or not token:
        raise ValueError("port claim token is invalid")
    return pid, token


@contextmanager
def _claim_guard(path: str):
    """Serialize reclaim/release on one stable inode.

    The claim itself must be removable, so it cannot also be the arbitration
    inode.  Guard files intentionally persist and remain tiny.
    """
    guard_path = path + ".guard"
    flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(guard_path, flags, 0o600)
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid():
            raise OSError("port claim guard is not a user-owned regular file")
        os.fchmod(fd, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


class PortLease:
    """An owned port. Release only after the consumer has bound it."""

    def __init__(self, port: int, path: str, token: Optional[str] = None) -> None:
        self.port = port
        self.path = path
        self._token = token
        self._released = False

    def __enter__(self) -> "PortLease":
        return self

    def __exit__(self, exc_type, exc, _tb) -> None:
        try:
            self.release()
        except PortReleaseError as release_exc:
            if exc_type is None:
                raise
            # Preserve the causal exception and attach cleanup evidence to the
            # traceback. A warning can be filtered and is not durable enough for
            # an ownership failure.
            add_exception_note(exc, f"port lease cleanup also failed: {release_exc}")

    def release(self) -> None:
        """Idempotent. Only removes a lock file this process still owns."""
        if self._released:
            return
        if self._token is None:
            self._released = True
            return
        try:
            with _claim_guard(self.path):
                owner, token = _read_claim(self.path)
                if owner == os.getpid() and token == self._token:
                    os.unlink(self.path)
        except FileNotFoundError:
            pass
        except (OSError, ValueError) as exc:
            raise PortReleaseError(
                f"could not release port lease {self.path!r}: {type(exc).__name__}: {exc}"
            ) from exc
        self._released = True

    def __repr__(self) -> str:
        return f"PortLease(port={self.port}, released={self._released})"


def _claim(port: int, host: str, directory: str) -> Optional[PortLease]:
    path = os.path.join(directory, f"{port}.lock")
    token = secrets.token_hex(16)
    with _claim_guard(path):
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            # Reclaim only when the holder is provably gone AND the lease is
            # old. This decision and replacement are one guarded transaction;
            # without it, two reclaimers can unlink each other's new claim.
            try:
                claim_stat = os.lstat(path)
                if not stat.S_ISREG(claim_stat.st_mode) or claim_stat.st_uid != os.getuid():
                    return None
                owner, _old_token = _read_claim(path)
            except (OSError, ValueError):
                return None
            if _pid_alive(owner) or time.time() - claim_stat.st_mtime < STALE_AFTER_S:
                return None
            try:
                os.unlink(path)
                fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except OSError:
                return None
        except OSError as exc:
            if exc.errno in (errno.EACCES, errno.EROFS):
                return None
            raise

        try:
            with os.fdopen(fd, "w") as handle:
                json.dump(
                    {"schema_version": 1, "pid": os.getpid(), "token": token, "at": time.time()},
                    handle,
                    sort_keys=True,
                )
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())

            # Claim first, then check bindability: the reverse order reopens
            # the race we are closing. The arbitration remains held through
            # deletion on the negative path.
            if not _bindable(port, host):
                os.unlink(path)
                return None
        except BaseException as exc:
            try:
                owner, current_token = _read_claim(path)
                if owner == os.getpid() and current_token == token:
                    os.unlink(path)
            except (OSError, ValueError) as cleanup_exc:
                add_exception_note(
                    exc, f"failed to unwind partial port claim {path!r}: {cleanup_exc}"
                )
            raise
    return PortLease(port, path, token)


def reserve_port(
    start_port: int,
    *,
    max_retries: int = 100,
    bind_host: str = "127.0.0.1",
    lease_dir: Optional[str] = None,
) -> PortLease:
    """Lease one free port at or above ``start_port``.

    Raises ``PortUnavailable`` rather than returning None: a caller that cannot
    get a port has no meaningful way to continue, and the old ``None`` return
    was reachable into code that used it as an integer.
    """
    if isinstance(start_port, bool) or not isinstance(start_port, int):
        raise PortUnavailable("start_port must be an integer")
    if isinstance(max_retries, bool) or not isinstance(max_retries, int) or max_retries < 1:
        raise PortUnavailable("max_retries must be a positive integer")
    if not 1 <= start_port <= 65535:
        raise PortUnavailable(f"invalid start port {start_port!r}")
    if not isinstance(bind_host, str) or not bind_host:
        raise PortUnavailable("bind_host must be non-empty text")
    if lease_dir is not None and (not isinstance(lease_dir, str) or not lease_dir):
        raise PortUnavailable("lease_dir must be null or non-empty text")
    stop_port = min(65536, start_port + max_retries)
    directory = lease_dir or _lease_dir()
    try:
        _secure_lease_dir(directory)
    except OSError as exc:
        raise PortUnavailable(f"port lease directory {directory!r} is unavailable: {exc}") from exc

    for port in range(start_port, stop_port):
        try:
            lease = _claim(port, bind_host, directory)
        except OSError as exc:
            raise PortUnavailable(
                f"port lease arbitration failed for {port} in {directory!r}: {exc}"
            ) from exc
        if lease is not None:
            return lease
    raise PortUnavailable(
        f"no free port in [{start_port}, {stop_port}) on "
        f"{bind_host}; {_leased_count(directory)} port(s) leased by this node"
    )


def _leased_count(directory: str) -> int:
    try:
        return len([n for n in os.listdir(directory) if n.endswith(".lock")])
    except OSError as exc:
        raise PortUnavailable(
            f"could not inspect port lease directory {directory!r}: {exc}"
        ) from exc
