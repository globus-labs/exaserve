"""Exact v2 receipt delivery over the authenticated node-local hop."""

from __future__ import annotations

import os
from typing import Any, Optional

_warned: set[str] = set()
_SOCKET_ENV = "EXASERVE_RECEIPT_SOCKET"


def _warn_once(message: str) -> None:
    if message not in _warned:
        _warned.add(message)
        print(message, flush=True)


def deployment_scope() -> str:
    """Return the canonical deployment identity shared by all processes.

    ``EXASERVE_DEPLOYMENT_ID`` comes from the hash-bearing DeploymentPlan and
    must therefore remain byte-exact.  The legacy implementation truncated it
    to forty characters using the same rule as a raw PBS job ID; sufficiently
    descriptive gate/run IDs then produced receipts for a different
    deployment and failed the initial snapshot.  Normalization is retained
    only for scheduler-derived compatibility fallbacks, where the suffix is
    site metadata rather than semantic identity.
    """
    explicit = os.environ.get("EXASERVE_DEPLOYMENT_ID")
    if explicit:
        return explicit
    for var in ("EXASERVE_SCALING_TRACE_TOKEN", "EXASERVE_JOBID", "PBS_JOBID"):
        value = os.environ.get(var)
        if value:
            return str(value).split(".")[0][:40]
    return "default"


def publish_receipt(receipt: Any) -> bool:
    """Deliver one already-built v2 receipt unchanged.

    There is intentionally no v1-to-v2 adapter: adding an exact slot and
    component identity after the fact would turn a transport into an
    attestation authority.
    """
    from .local_ingress import deliver_receipt
    from .receipt_v2 import CompatibilityReceiptV2

    role = getattr(receipt, "role", "?")
    if not isinstance(receipt, CompatibilityReceiptV2):
        _warn_once(
            f"[Compat] role={role}: receipt rejected — only exact schema v2 "
            "receipts may use the authoritative transport"
        )
        return False
    if not os.environ.get(_SOCKET_ENV):
        _warn_once(
            f"[Compat] role={role}: receipt NOT published — no node-local "
            "supervisor ingress on this node"
        )
        return False
    if deliver_receipt(receipt.to_dict()):
        return True
    _warn_once(
        f"[Compat] role={role}: receipt NOT delivered to node-local ingress "
        f"{os.environ.get(_SOCKET_ENV)}"
    )
    return False


def receipt_from_dict(data: dict) -> Optional[object]:
    """Strict compatibility helper; malformed or v1 data returns None."""
    from .receipt_v2 import ReceiptError, receipt_from_dict as parse_v2

    try:
        return parse_v2(data)
    except ReceiptError:
        return None
