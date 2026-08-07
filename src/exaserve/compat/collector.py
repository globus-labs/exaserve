"""Compatibility receipt publication from managed roles (WP3.13, IMP-B04).

Receipts once travelled over a named detached Ray actor. §3.2.1 rules that out
in one sentence -- "A detached Ray actor, stdout, shared file, or node-local
file is not an authoritative readiness source" -- and WP13 removed it. What
remains here is deployment-id normalization plus one publish function that
delivers over the bounded node-local hop to the owning `NodeSupervisor`, which
forwards the payload unchanged over the authenticated §3.2 channel.

Keeping the actor as a "fallback" would have been worse than deleting it: a
fallback to an unauthenticated transport is not a safety net, it is the
violation with a longer name. When the hop is unavailable the receipt is not
published, and readiness blocks on the missing slot by name -- which is a
diagnosis.
"""

from __future__ import annotations

import os
from typing import Any, Optional

_warned: set[str] = set()


def _warn_once(message: str) -> None:
    if message not in _warned:
        _warned.add(message)
        print(message, flush=True)


def deployment_scope() -> str:
    """Normalized deployment id — MUST match server._deployment_scope().

    The head propagates the normalized value to replicas; if this function
    normalized differently, head and replica would disagree about which
    deployment a receipt belongs to and every one would be rejected as "wrong
    deployment" (a raw `PBS_JOBID` carries a `.aurora-pbs-...` suffix that the
    head strips).
    """
    for var in ("EXASERVE_DEPLOYMENT_ID", "EXASERVE_SCALING_TRACE_TOKEN",
                "EXASERVE_JOBID", "PBS_JOBID"):
        val = os.environ.get(var)
        if val:
            return str(val).split(".")[0][:40]
    return "default"


_SOCKET_ENV = "EXASERVE_RECEIPT_SOCKET"


def _publish_over_local_hop(receipt: Any, role: str) -> bool:
    """Deliver a v2 receipt to the owning NodeSupervisor.

    Replica and engine instances are not exactly-planned slots — the plan
    cannot name a replica that Serve places at runtime — so these carry an
    instance-scoped requirement id. The head files them as evidence and the
    ledger's set equality keeps meaning "exactly the planned slots".
    """
    from .local_ingress import deliver_receipt
    from .producers import attest_self

    node = getattr(receipt, "node_id", "") or ""
    pid = getattr(receipt, "pid", 0) or 0
    try:
        v2 = attest_self(
            requirement_id=f"evidence/{role}/{node}/{pid}",
            role=role, component_id=role,
            instance_id=f"{node}:{pid}", owner_scope="RANK",
            owner_rank=int(os.environ.get("EXASERVE_RECEIPT_RANK", "0") or 0))
    except Exception as exc:                       # noqa: BLE001
        _warn_once(f"[Compat] role={role}: receipt NOT built: "
                   f"{type(exc).__name__}: {exc}")
        return False
    if deliver_receipt(v2.to_dict()):
        return True
    _warn_once(f"[Compat] role={role}: receipt NOT delivered to the node-local "
               f"supervisor ingress at {os.environ.get(_SOCKET_ENV)}")
    return False


def publish_receipt(receipt: Any) -> bool:
    """Publish from a managed role. Never raises; always explains a failure.

    Silence here is what made the first cluster run report "no receipt from
    role X" while the real fault was the transport, so a failure is printed
    once per process with the reason.
    """
    role = getattr(receipt, "role", "?")
    if not os.environ.get(_SOCKET_ENV):
        _warn_once(f"[Compat] role={role}: receipt NOT published — no node-local "
                   "supervisor ingress on this node")
        return False
    return _publish_over_local_hop(receipt, role)


def receipt_from_dict(data: dict) -> Optional[object]:
    """Rehydrate a receipt; returns None when the payload is not one."""
    from .receipt import CompatibilityReceipt

    try:
        return CompatibilityReceipt(
            schema_version=int(data["schema_version"]),
            deployment_id=str(data["deployment_id"]),
            generation=int(data["generation"]),
            profile_id=str(data["profile_id"]),
            profile_name=str(data["profile_name"]),
            role=str(data["role"]),
            node_id=str(data["node_id"]),
            pid=int(data["pid"]),
            versions=dict(data.get("versions") or {}),
            patch_results=dict(data.get("patch_results") or {}),
            capabilities=tuple(data.get("capabilities") or ()),
            attestation=str(data.get("attestation", "self")),
            created_at=float(data.get("created_at", 0.0)),
            not_applicable=tuple(data.get("not_applicable") or ()),
        )
    except (KeyError, TypeError, ValueError):
        return None
