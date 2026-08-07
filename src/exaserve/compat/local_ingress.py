"""Bounded node-local receipt ingress (plan §3.2.1, packet P02/P03, IMP-B04).

Receipts used to travel over a **detached Ray actor**. §3.2.1 rules that out in
one sentence: "A detached Ray actor, stdout, shared file, or node-local file is
not an authoritative readiness source." The actor is unauthenticated, outlives
its deployment, is reachable by anything in the Ray namespace, and carries no
binding to the generation whose readiness it decides.

What the plan *does* allow is exactly one hop:

    "A bounded local IPC hop is allowed only to deliver the exact rank-owned
     receipt to its owning NodeSupervisor, which forwards it unchanged."

This module is that hop and nothing more. A node-local process (a Serve
replica, the Ray daemon wrapper) writes one bounded frame to a unix socket that
its own `NodeSupervisor` owns; the supervisor forwards the payload **byte for
byte** over the authenticated §3.2 channel. The hop never adjudicates, never
merges, never rewrites — if it did, it would be evidence aggregation outside
the authenticated path, which is the thing being removed.

Bounds are enforced here rather than trusted:

* the socket lives in a 0700 directory, mode 0600, scoped to deployment and
  generation, so a stale socket from a previous generation is not reusable;
* one connection carries exactly one length-prefixed frame;
* frames over `max_frame_bytes` are refused without being read into memory;
* the queue is capped and overflow is *counted*, so truncation is visible
  rather than silent.
"""

from __future__ import annotations

import json
import os
import socket
import threading
from typing import Callable, Optional

SOCKET_ENV = "EXASERVE_RECEIPT_SOCKET"
MAX_FRAME_BYTES = 1 << 20
MAX_QUEUED = 4096
_HEADER_BYTES = 12          # b"%011d\n" -- fixed width, so no unbounded read


def socket_path_for(deployment_id: str, generation: int,
                    root: Optional[str] = None) -> str:
    """A node-local path scoped to one deployment generation.

    Scoping matters: an unscoped path lets a receipt produced under generation
    N be delivered to the supervisor of generation N+1, which is precisely the
    stale-evidence class the ledger exists to reject.
    """
    base = root or os.environ.get("TMPDIR") or "/tmp"
    directory = os.path.join(base, f"exaserve-{deployment_id}-{generation}")
    return os.path.join(directory, "receipts.sock")


def _encode(payload: dict) -> bytes:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return b"%011d\n" % len(blob) + blob


class LocalReceiptIngress:
    """The NodeSupervisor's end of the hop. Accepts, bounds, and queues."""

    def __init__(self, path: str, *, max_frame_bytes: int = MAX_FRAME_BYTES,
                 max_queued: int = MAX_QUEUED,
                 log: Callable[[str], None] = print) -> None:
        self.path = path
        self.max_frame_bytes = int(max_frame_bytes)
        self.max_queued = int(max_queued)
        self._log = log
        self._queue: list[dict] = []
        self._lock = threading.Lock()
        self._sock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self.dropped = 0
        self.refused = 0
        self.accepted = 0

    # -- lifecycle ---------------------------------------------------------
    def start(self) -> bool:
        """Bind and serve. Returns False (never raises) if the hop is unusable.

        A failure here is not fatal for the rank: the receipt path degrades and
        the head's ledger blocks by name on the missing slot, which is a better
        outcome than a supervisor that refuses to run because a socket file
        could not be created.
        """
        directory = os.path.dirname(self.path)
        try:
            os.makedirs(directory, mode=0o700, exist_ok=True)
            os.chmod(directory, 0o700)
            if os.path.exists(self.path):
                os.unlink(self.path)
            self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._sock.bind(self.path)
            os.chmod(self.path, 0o600)
            self._sock.listen(64)
            self._sock.settimeout(0.5)
        except OSError as exc:
            self._log(f"[Receipts] local ingress unavailable at {self.path}: {exc}")
            self._sock = None
            return False
        self._thread = threading.Thread(target=self._serve, daemon=True,
                                        name="exaserve-receipt-ingress")
        self._thread.start()
        return True

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()      # type: ignore[union-attr]
            except socket.timeout:
                continue
            except OSError:
                return
            try:
                self._handle(conn)
            except Exception:                       # noqa: BLE001 - never fatal
                pass
            finally:
                try:
                    conn.close()
                except OSError:
                    pass

    def _handle(self, conn: socket.socket) -> None:
        conn.settimeout(10.0)
        header = self._read_exactly(conn, _HEADER_BYTES)
        if header is None:
            return
        try:
            length = int(header[:-1])
        except ValueError:
            self.refused += 1
            return
        if length <= 0 or length > self.max_frame_bytes:
            # Refuse without reading: an oversized frame must not be able to
            # consume node memory just by announcing itself.
            self.refused += 1
            conn.sendall(b"NO\n")
            return
        body = self._read_exactly(conn, length)
        if body is None:
            self.refused += 1
            return
        try:
            payload = json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            self.refused += 1
            conn.sendall(b"NO\n")
            return
        if not isinstance(payload, dict):
            self.refused += 1
            conn.sendall(b"NO\n")
            return
        with self._lock:
            if len(self._queue) >= self.max_queued:
                self.dropped += 1
                conn.sendall(b"NO\n")
                return
            self._queue.append(payload)
            self.accepted += 1
        conn.sendall(b"OK\n")

    @staticmethod
    def _read_exactly(conn: socket.socket, count: int) -> Optional[bytes]:
        chunks: list[bytes] = []
        remaining = count
        while remaining > 0:
            try:
                chunk = conn.recv(min(remaining, 65536))
            except (socket.timeout, OSError):
                return None
            if not chunk:
                return None
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    # -- surface -----------------------------------------------------------
    def drain(self) -> list[dict]:
        """Take everything queued. The caller forwards it unchanged."""
        with self._lock:
            batch = self._queue
            self._queue = []
        return batch

    def pending(self) -> int:
        with self._lock:
            return len(self._queue)

    def stop(self) -> None:
        self._stop.set()
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
        if self._thread is not None:
            self._thread.join(timeout=3)
        try:
            if os.path.exists(self.path):
                os.unlink(self.path)
        except OSError:
            pass


def deliver_receipt(payload: dict, *, path: Optional[str] = None,
                    timeout_s: float = 10.0) -> bool:
    """Node-local producer side. Hands one exact receipt to its supervisor.

    Returns False rather than raising: a producer that cannot reach its
    supervisor must still report the fact through its own error path, and the
    head blocks on the missing slot by name.
    """
    target = path or os.environ.get(SOCKET_ENV, "")
    if not target:
        return False
    frame = _encode(payload)
    if len(frame) > MAX_FRAME_BYTES + _HEADER_BYTES:
        return False
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(timeout_s)
            client.connect(target)
            client.sendall(frame)
            return client.recv(3) == b"OK\n"
    except OSError:
        return False
