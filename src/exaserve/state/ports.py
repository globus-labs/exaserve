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
import os
import socket
import time
from typing import Iterable, Optional

DEFAULT_LEASE_DIR = "/tmp/exaserve_ports"
# A lease older than this with no live owner is reclaimable. Bring-up can be
# slow, so this is generous: reclaiming too eagerly reintroduces the collision.
STALE_AFTER_S = 3600.0


class PortUnavailable(RuntimeError):
    """No port in the requested range could be leased."""


def _lease_dir() -> str:
    return os.environ.get("EXASERVE_PORT_LEASE_DIR", DEFAULT_LEASE_DIR)


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True          # exists, owned by someone else
    except OSError:
        return False


def _bindable(port: int, host: str) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        try:
            probe.bind((host, port))
            return True
        except OSError:
            return False


class PortLease:
    """An owned port. Release only after the consumer has bound it."""

    def __init__(self, port: int, path: str) -> None:
        self.port = port
        self.path = path
        self._released = False

    def __enter__(self) -> "PortLease":
        return self

    def __exit__(self, *exc) -> None:
        self.release()

    def release(self) -> None:
        """Idempotent. Only removes a lock file this process still owns."""
        if self._released:
            return
        self._released = True
        try:
            with open(self.path, encoding="utf-8") as handle:
                owner = int((handle.read().split() or ["-1"])[0])
        except (OSError, ValueError):
            owner = -1
        if owner == os.getpid():
            try:
                os.unlink(self.path)
            except OSError:
                pass

    def __repr__(self) -> str:
        return f"PortLease(port={self.port}, released={self._released})"


def _claim(port: int, host: str, directory: str) -> Optional[PortLease]:
    path = os.path.join(directory, f"{port}.lock")
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    except FileExistsError:
        # Reclaim only when the holder is provably gone AND the lease is old.
        try:
            stat = os.stat(path)
            with open(path, encoding="utf-8") as handle:
                owner = int((handle.read().split() or ["-1"])[0])
        except (OSError, ValueError):
            return None
        if _pid_alive(owner) or time.time() - stat.st_mtime < STALE_AFTER_S:
            return None
        try:
            os.unlink(path)
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except OSError:
            return None
    except OSError as exc:
        if exc.errno in (errno.EACCES, errno.EROFS):
            return None
        raise

    with os.fdopen(fd, "w") as handle:
        handle.write(f"{os.getpid()} {time.time():.0f}\n")

    # Claim first, then check bindability: the reverse order reopens the race
    # we are closing (another process could claim between our check and write).
    if not _bindable(port, host):
        lease = PortLease(port, path)
        lease.release()
        return None
    return PortLease(port, path)


def reserve_port(start_port: int, *, max_retries: int = 100,
                 bind_host: str = "127.0.0.1",
                 lease_dir: Optional[str] = None) -> PortLease:
    """Lease one free port at or above ``start_port``.

    Raises ``PortUnavailable`` rather than returning None: a caller that cannot
    get a port has no meaningful way to continue, and the old ``None`` return
    was reachable into code that used it as an integer.
    """
    directory = lease_dir or _lease_dir()
    try:
        os.makedirs(directory, exist_ok=True)
    except OSError:
        directory = None                       # fall back to probe-only below

    for port in range(start_port, start_port + max_retries):
        if directory is None:
            if _bindable(port, bind_host):
                return PortLease(port, os.devnull)
            continue
        lease = _claim(port, bind_host, directory)
        if lease is not None:
            return lease
    raise PortUnavailable(
        f"no free port in [{start_port}, {start_port + max_retries}) on "
        f"{bind_host}; {_leased_count(directory)} port(s) leased by this node")


def _leased_count(directory: Optional[str]) -> int:
    if not directory:
        return 0
    try:
        return len([n for n in os.listdir(directory) if n.endswith(".lock")])
    except OSError:
        return 0


def release_all(ports: Iterable[int], lease_dir: Optional[str] = None) -> int:
    """Release leases this process holds. Used by cleanup paths."""
    directory = lease_dir or _lease_dir()
    released = 0
    for port in ports:
        lease = PortLease(port, os.path.join(directory, f"{port}.lock"))
        before = os.path.exists(lease.path)
        lease.release()
        if before and not os.path.exists(lease.path):
            released += 1
    return released


def sweep_stale(lease_dir: Optional[str] = None) -> int:
    """Remove leases whose owner is gone. Safe to call at job start."""
    directory = lease_dir or _lease_dir()
    removed = 0
    try:
        names = os.listdir(directory)
    except OSError:
        return 0
    for name in names:
        if not name.endswith(".lock"):
            continue
        path = os.path.join(directory, name)
        try:
            with open(path, encoding="utf-8") as handle:
                owner = int((handle.read().split() or ["-1"])[0])
        except (OSError, ValueError):
            continue
        if not _pid_alive(owner):
            try:
                os.unlink(path)
                removed += 1
            except OSError:
                pass
    return removed
