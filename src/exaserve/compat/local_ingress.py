"""Bounded node-local ingress for exact rank-owned compatibility receipts.

This socket has one semantic purpose: deliver a producer's exact receipt to
its owning NodeSupervisor.  The supervisor drains dictionaries and forwards
them byte-for-byte over the authenticated control channel.  It does not carry
deployment observations or any other readiness evidence.
"""

from __future__ import annotations

import os
from typing import Callable, Optional

from ..control.local_ipc import (
    MAX_FRAME_BYTES,
    MAX_QUEUED,
    BoundedUnixIngress,
    deliver_object,
    deliver_object_checked,
    scoped_socket_path,
)

SOCKET_ENV = "EXASERVE_RECEIPT_SOCKET"


def socket_path_for(
    deployment_id: str,
    generation: int,
    *,
    owner_rank: int | None = None,
    root: Optional[str] = None,
) -> str:
    """Return the node-local ingress path for one authenticated rank.

    Production callers always provide ``owner_rank``.  Encoding it in the
    endpoint prevents a receipt carrying rank N's authority from accidentally
    entering rank M's authenticated control session when a distributed child
    inherited the coordinator's environment.  ``None`` remains available for
    isolated transport tests and compatibility with non-production callers.
    """
    if owner_rank is not None and (
        isinstance(owner_rank, bool) or not isinstance(owner_rank, int) or owner_rank < 0
    ):
        raise ValueError("receipt socket owner_rank must be null or non-negative")
    socket_name = "receipts.sock" if owner_rank is None else f"receipts-rank-{owner_rank:08d}.sock"
    return scoped_socket_path(deployment_id, generation, socket_name, root=root)


class LocalReceiptIngress(BoundedUnixIngress):
    """Receipt-specific façade over the shared bounded framing primitive."""

    def __init__(
        self,
        path: str,
        *,
        max_frame_bytes: int = MAX_FRAME_BYTES,
        max_queued: int = MAX_QUEUED,
        log: Callable[[str], None] = print,
    ) -> None:
        super().__init__(
            path, max_frame_bytes=max_frame_bytes, max_queued=max_queued, log=log, label="Receipts"
        )


def deliver_receipt(payload: dict, *, path: Optional[str] = None, timeout_s: float = 10.0) -> bool:
    target = path or os.environ.get(SOCKET_ENV, "")
    return deliver_object(payload, path=target, timeout_s=timeout_s)


def deliver_receipt_checked(
    payload: dict, *, path: Optional[str] = None, timeout_s: float = 10.0
) -> None:
    """Deliver a required receipt while preserving the transport failure."""
    target = path or os.environ.get(SOCKET_ENV, "")
    deliver_object_checked(payload, path=target, timeout_s=timeout_s)
