"""Bounded, peer-attributed local JSON IPC.

This is a transport primitive, not a lifecycle or readiness authority.  A
semantic owner must provide a distinct socket and validate both the message
contract and the kernel-reported peer PID before using a payload.  Keeping the
framing here prevents the receipt ingress and the deployment fault-isolation
channel from growing subtly different unbounded socket implementations.
"""

from __future__ import annotations

import hashlib
import fcntl
import json
import math
import os
import socket
import struct
import threading
from typing import Callable, Optional

from ..state.atomic import strict_json_loads

MAX_FRAME_BYTES = 1 << 20
MAX_QUEUED = 4096
_HEADER_BYTES = 12  # b"%011d\n"
_SAFE_UNIX_PATH_BYTES = 103


class LocalDeliveryError(RuntimeError):
    """One local IPC delivery failed before an acceptance ACK was received.

    The Boolean compatibility wrapper remains useful at optional call sites,
    but production lifecycle boundaries need the actual transport cause.  A
    missing socket, an unencodable/oversized payload, a connect/send/read
    failure, and a negative ACK are deliberately distinct diagnostics.
    """


def scoped_socket_path(
    deployment_id: str,
    generation: int,
    socket_name: str,
    *,
    root: Optional[str] = None,
) -> str:
    """Create a bounded path without embedding caller-controlled identity.

    Linux reserves only 108 bytes for ``sockaddr_un.sun_path`` (including its
    terminator).  More importantly, scheduler launchers may assign different
    ``TMPDIR`` values to the composition root and its MPI rank processes.  A
    protocol endpoint must not change with that ambient value, so production
    calls use a deterministic private per-user directory under ``/tmp``.
    Tests and explicitly isolated callers may still provide ``root``; an
    overlong explicit root falls back while retaining that root in the hash.
    """
    if not isinstance(deployment_id, str) or not deployment_id:
        raise ValueError("deployment_id must be a non-empty string")
    if isinstance(generation, bool) or not isinstance(generation, int) or generation < 0:
        raise ValueError("generation must be a non-negative integer")
    if not isinstance(socket_name, str) or not socket_name or "/" in socket_name:
        raise ValueError("socket_name must be one path component")
    if root is not None and (not isinstance(root, str) or not root):
        raise ValueError("socket root must be null or non-empty text")
    private_root = os.path.join("/tmp", f"exaserve-ipc-{os.getuid()}")
    base = os.path.abspath(root) if root is not None else private_root
    identity_digest = hashlib.sha256(f"{deployment_id}\0{generation}".encode()).hexdigest()
    name_digest = hashlib.sha256(socket_name.encode()).hexdigest()[:8]
    preferred = os.path.join(base, f".xsv-{identity_digest[:16]}", socket_name)
    if len(os.fsencode(preferred)) <= _SAFE_UNIX_PATH_BYTES:
        return preferred
    root_digest = hashlib.sha256(os.fsencode(base)).hexdigest()[:8]
    fallback_root = private_root
    fallback = os.path.join(
        fallback_root,
        f"x-{identity_digest[:16]}-{root_digest}-{name_digest}",
    )
    if len(os.fsencode(fallback)) > _SAFE_UNIX_PATH_BYTES:
        raise ValueError("could not construct a bounded Unix socket path")
    return fallback


def encode_object(payload: dict) -> bytes:
    if not isinstance(payload, dict) or any(not isinstance(key, str) for key in payload):
        raise ValueError("local IPC payload must be a string-keyed object")
    blob = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return b"%011d\n" % len(blob) + blob


class BoundedUnixIngress:
    """One-object-per-connection Unix ingress with strict memory bounds."""

    def __init__(
        self,
        path: str,
        *,
        max_frame_bytes: int = MAX_FRAME_BYTES,
        max_queued: int = MAX_QUEUED,
        log: Callable[[str], None] = print,
        label: str = "local IPC",
    ) -> None:
        if not isinstance(path, str) or not path:
            raise ValueError("local IPC path must be non-empty text")
        if (
            isinstance(max_frame_bytes, bool)
            or not isinstance(max_frame_bytes, int)
            or max_frame_bytes < 1
            or isinstance(max_queued, bool)
            or not isinstance(max_queued, int)
            or max_queued < 1
        ):
            raise ValueError("local IPC bounds must be positive")
        if not callable(log) or not isinstance(label, str) or not label:
            raise ValueError("local IPC log/label contract is invalid")
        self.path = path
        self.max_frame_bytes = max_frame_bytes
        self.max_queued = max_queued
        self._log = log
        self._label = label
        self._queue: list[tuple[int, dict]] = []
        self._lock = threading.Lock()
        self._sock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._start_attempted = False
        self._owner_fd: Optional[int] = None
        self._owns_path = False
        self.dropped = 0
        self.refused = 0
        self.accepted = 0
        self.reply_failures = 0

    def start(self) -> bool:
        """Bind and serve; return false without leaving a partial socket."""
        if self._start_attempted:
            self._log(f"[{self._label}] ingress is one-shot and was already started")
            return False
        self._start_attempted = True
        directory = os.path.dirname(self.path)
        try:
            if not hasattr(socket, "SO_PEERCRED"):
                raise OSError("SO_PEERCRED is required for peer attribution")
            os.makedirs(directory, mode=0o700, exist_ok=True)
            os.chmod(directory, 0o700)
            owner_path = self.path + ".owner"
            owner_flags = os.O_CREAT | os.O_RDWR
            if hasattr(os, "O_NOFOLLOW"):
                owner_flags |= os.O_NOFOLLOW
            self._owner_fd = os.open(owner_path, owner_flags, 0o600)
            os.fchmod(self._owner_fd, 0o600)
            fcntl.flock(self._owner_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._owns_path = True
            if os.path.lexists(self.path):
                os.unlink(self.path)
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._sock = sock
            sock.bind(self.path)
            os.chmod(self.path, 0o600)
            sock.listen(64)
            sock.settimeout(0.5)
        except (OSError, BlockingIOError) as exc:
            self._log(f"[{self._label}] ingress unavailable at {self.path}: {exc}")
            if self._sock is not None:
                try:
                    self._sock.close()
                except OSError as cleanup_exc:
                    self._log(f"[{self._label}] listener cleanup failed: {cleanup_exc}")
            self._sock = None
            try:
                if self._owns_path and os.path.lexists(self.path):
                    os.unlink(self.path)
            except OSError as cleanup_exc:
                self._log(
                    f"[{self._label}] partial-start socket cleanup failed at "
                    f"{self.path}: {cleanup_exc}"
                )
            if self._owner_fd is not None:
                try:
                    os.close(self._owner_fd)
                except OSError as cleanup_exc:
                    self._log(f"[{self._label}] owner lock cleanup failed: {cleanup_exc}")
                self._owner_fd = None
            self._owns_path = False
            return False
        self._thread = threading.Thread(
            target=self._serve,
            daemon=True,
            name=f"exaserve-{self._label.lower().replace(' ', '-')}-ingress",
        )
        self._thread.start()
        return True

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                assert self._sock is not None
                conn, _ = self._sock.accept()
            except socket.timeout:
                continue
            except OSError as exc:
                if not self._stop.is_set():
                    self._log(f"[{self._label}] accept loop failed: {exc}")
                return
            try:
                self._handle(conn)
            except (OSError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
                with self._lock:
                    self.refused += 1
            finally:
                try:
                    conn.close()
                except OSError as exc:
                    self._log(f"[{self._label}] client socket close failed: {exc}")

    def _handle(self, conn: socket.socket) -> None:
        conn.settimeout(10.0)
        credentials = conn.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
        peer_pid, peer_uid, _peer_gid = struct.unpack("3i", credentials)
        if peer_pid <= 0 or peer_uid != os.getuid():
            self._refuse(conn)
            return
        header = self._read_exactly(conn, _HEADER_BYTES)
        if (
            header is None
            or len(header) != _HEADER_BYTES
            or header[-1:] != b"\n"
            or not header[:-1].isdigit()
        ):
            self._refuse(conn)
            return
        length = int(header[:-1])
        if length < 1 or length > self.max_frame_bytes:
            self._refuse(conn)
            return
        body = self._read_exactly(conn, length)
        if body is None:
            self._refuse(conn)
            return
        payload = strict_json_loads(body.decode("utf-8"))
        if not isinstance(payload, dict):
            self._refuse(conn)
            return
        with self._lock:
            if len(self._queue) >= self.max_queued:
                self.dropped += 1
                self._reply(conn, b"NO\n")
                return
            self._queue.append((peer_pid, payload))
            self.accepted += 1
        self._reply(conn, b"OK\n")

    def _refuse(self, conn: socket.socket) -> None:
        with self._lock:
            self.refused += 1
        self._reply(conn, b"NO\n")

    def _reply(self, conn: socket.socket, value: bytes) -> None:
        try:
            conn.sendall(value)
        except OSError as exc:
            with self._lock:
                self.reply_failures += 1
                failures = self.reply_failures
            if failures & (failures - 1) == 0:
                self._log(
                    f"[{self._label}] reply failed ({failures} total): {type(exc).__name__}: {exc}"
                )

    @staticmethod
    def _read_exactly(conn: socket.socket, count: int) -> Optional[bytes]:
        chunks: list[bytes] = []
        remaining = count
        while remaining:
            try:
                chunk = conn.recv(min(remaining, 65536))
            except (socket.timeout, OSError):
                return None
            if not chunk:
                return None
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def drain_with_peer(self) -> list[tuple[int, dict]]:
        with self._lock:
            batch = self._queue
            self._queue = []
        return batch

    def drain(self) -> list[dict]:
        return [payload for _pid, payload in self.drain_with_peer()]

    def pending(self) -> int:
        with self._lock:
            return len(self._queue)

    def stop(self, timeout_s: float = 3.0) -> bool:
        """Stop accepting, join the ingress thread, and report exact reaping."""
        if (
            isinstance(timeout_s, bool)
            or not isinstance(timeout_s, (int, float))
            or not math.isfinite(float(timeout_s))
            or timeout_s < 0
        ):
            raise ValueError("local IPC stop timeout must be finite and non-negative")
        self._stop.set()
        stopped = True
        sock = self._sock
        self._sock = None
        if sock is not None:
            try:
                sock.close()
            except OSError as exc:
                self._log(f"[{self._label}] listener close failed: {exc}")
                stopped = False
        if self._thread is not None:
            self._thread.join(timeout=max(0.0, timeout_s))
        thread_stopped = self._thread is None or not self._thread.is_alive()
        if not thread_stopped:
            self._log(
                f"[{self._label}] ingress thread did not stop within {max(0.0, timeout_s):g}s"
            )
            stopped = False
        else:
            self._thread = None
        try:
            if self._owns_path and os.path.lexists(self.path):
                os.unlink(self.path)
        except OSError as exc:
            self._log(f"[{self._label}] could not remove socket {self.path}: {exc}")
            stopped = False
        if self._owner_fd is not None:
            try:
                os.close(self._owner_fd)
            except OSError as exc:
                self._log(f"[{self._label}] owner lock close failed: {exc}")
                stopped = False
            self._owner_fd = None
        self._owns_path = False
        return stopped


def deliver_object_checked(
    payload: dict, *, path: str, timeout_s: float = 10.0, max_frame_bytes: int = MAX_FRAME_BYTES
) -> None:
    """Deliver one bounded object or raise a cause-preserving error."""
    if not isinstance(path, str) or not path:
        raise LocalDeliveryError("local IPC socket path is empty")
    if (
        isinstance(timeout_s, bool)
        or not isinstance(timeout_s, (int, float))
        or not math.isfinite(float(timeout_s))
        or timeout_s <= 0
        or isinstance(max_frame_bytes, bool)
        or not isinstance(max_frame_bytes, int)
        or max_frame_bytes < 1
    ):
        raise LocalDeliveryError("local IPC timeout/frame bounds are invalid")
    try:
        frame = encode_object(payload)
    except (TypeError, ValueError, OverflowError) as exc:
        raise LocalDeliveryError(
            f"local IPC payload could not be encoded: {type(exc).__name__}: {exc}"
        ) from exc
    if len(frame) > max_frame_bytes + _HEADER_BYTES:
        raise LocalDeliveryError(
            f"local IPC frame is {len(frame) - _HEADER_BYTES} bytes, limit is {max_frame_bytes}"
        )
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(timeout_s)
            client.connect(path)
            client.sendall(frame)
            ack = BoundedUnixIngress._read_exactly(client, 3)
    except OSError as exc:
        raise LocalDeliveryError(
            f"local IPC transport failed for {path!r}: {type(exc).__name__}: {exc}"
        ) from exc
    if ack != b"OK\n":
        raise LocalDeliveryError(
            f"local IPC ingress at {path!r} returned {ack!r} instead of an acceptance ACK"
        )


def deliver_object(
    payload: dict, *, path: str, timeout_s: float = 10.0, max_frame_bytes: int = MAX_FRAME_BYTES
) -> bool:
    """Best-effort compatibility wrapper around :func:`deliver_object_checked`."""
    try:
        deliver_object_checked(
            payload,
            path=path,
            timeout_s=timeout_s,
            max_frame_bytes=max_frame_bytes,
        )
    except LocalDeliveryError:
        return False
    return True
