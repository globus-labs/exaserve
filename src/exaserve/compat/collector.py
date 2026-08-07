"""Cluster-wide compatibility receipt collection (plan WP3.13, audit IMP-B04).

Receipts are produced by processes we do not own the stdout of (Serve replicas
scattered across the allocation), so they travel over a named detached Ray
actor rather than a log line. The head drains this actor before declaring
readiness; a role that never published cannot be attested, and the readiness
predicate blocks on it by name.

This channel is ALWAYS on. It is deliberately not gated on tracing: readiness
must not depend on an optional instrumentation flag.
"""

from __future__ import annotations

import os
from typing import Any, Optional

# Same namespace as the replica-stats collector, which is the pattern that is
# proven to be reachable from a Serve replica on this stack. The actor NAME is
# already deployment-scoped, so sharing the namespace costs no isolation and
# removes cross-namespace lookup as a failure mode.
_NAMESPACE = "serve"
_warned: set[str] = set()


def _warn_once(message: str) -> None:
    if message not in _warned:
        _warned.add(message)
        print(message, flush=True)


def deployment_scope() -> str:
    """Normalized deployment id — MUST match server._deployment_scope().

    The head propagates the normalized value to replicas; if this function
    normalized differently, head and replica would compute different actor
    names and every receipt would be unroutable (a raw `PBS_JOBID` carries a
    `.aurora-pbs-...` suffix that the head strips).
    """
    for var in ("EXASERVE_DEPLOYMENT_ID", "EXASERVE_SCALING_TRACE_TOKEN",
                "EXASERVE_JOBID", "PBS_JOBID"):
        val = os.environ.get(var)
        if val:
            return str(val).split(".")[0][:40]
    return "default"


def collector_name() -> str:
    """Per-deployment actor name so two jobs never share a receipt store."""
    return f"exaserve_receipts_{deployment_scope()}"


# KI-A6/PR-029: a detached actor that accumulates without bound and is never
# reaped outlives its deployment and grows with the fleet. Two receipts per
# replica at 256 nodes is thousands of dicts; the cap makes the memory a
# constant and the drop count makes truncation visible rather than silent.
_MAX_RECEIPTS = 8192


class _ReceiptCollectorImpl:
    def __init__(self) -> None:
        self._receipts: list[dict] = []
        self._dropped = 0

    def report(self, receipt: dict) -> None:
        if len(self._receipts) >= _MAX_RECEIPTS:
            self._dropped += 1
            return
        self._receipts.append(receipt)

    def get_all(self) -> list[dict]:
        return list(self._receipts)

    def count(self) -> int:
        return len(self._receipts)

    def dropped(self) -> int:
        return self._dropped


def create_receipt_collector():
    """Create the named collector on the head, before replicas start.

    A failure here is REPORTED, never swallowed: if the channel does not exist,
    no role can publish, and the readiness gate would otherwise blame the roles
    for a fault that belongs to the transport.
    """
    import ray

    actor_cls = ray.remote(_ReceiptCollectorImpl)
    name = collector_name()
    try:
        handle = actor_cls.options(
            name=name, namespace=_NAMESPACE,
            lifetime="detached", num_cpus=0).remote()
        # Prove the channel is reachable BY NAME now, rather than discovering
        # 15 minutes later that no role could route a receipt to it.
        if _get_collector() is None:
            print(f"[Compat] ERROR: collector {name} created but not resolvable "
                  f"by name in namespace {_NAMESPACE}", flush=True)
            return None
        print(f"[Compat] receipt collector ready: {name} (ns={_NAMESPACE})",
              flush=True)
        return handle
    except ValueError:
        # Already exists in this namespace (re-deploy within one Ray cluster).
        existing = _get_collector()
        if existing is not None:
            print(f"[Compat] receipt collector reused: {name}", flush=True)
        return existing
    except Exception as exc:
        print(f"[Compat] ERROR: could not create the receipt collector {name}: "
              f"{type(exc).__name__}: {exc}", flush=True)
        return None


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


def _get_collector():
    try:
        import ray

        return ray.get_actor(collector_name(), namespace=_NAMESPACE)
    except Exception as exc:
        _warn_once(f"[Compat] get_actor({collector_name()}, ns={_NAMESPACE}) "
                   f"failed: {type(exc).__name__}: {exc}")
        return None


def publish_receipt(receipt: Any) -> bool:
    """Publish from a managed role. Never raises; always explains a failure.

    Silence here is what made the first cluster run report "no receipt from
    role X" while the real fault was the channel, so a failure is printed once
    per process with the reason.

    §3.2.1 forbids a detached Ray actor as an authoritative receipt path. When
    the NodeSupervisor's bounded local hop is present this publishes there
    instead, and the supervisor forwards the payload unchanged over the
    authenticated channel. The actor below is the legacy transport and is
    reached only when no hop exists (the shell-lifecycle path, deleted at
    WP13).
    """
    role = getattr(receipt, "role", "?")
    if os.environ.get(_SOCKET_ENV):
        return _publish_over_local_hop(receipt, role)
    collector = _get_collector()
    if collector is None:
        _warn_once(f"[Compat] role={role}: receipt NOT published — collector "
                   f"{collector_name()} not found in namespace {_NAMESPACE}")
        return False
    try:
        payload = receipt.to_dict() if hasattr(receipt, "to_dict") else dict(receipt)
        # ray.get() the report so a transport failure surfaces here rather than
        # vanishing into a dropped fire-and-forget task.
        import ray

        ray.get(collector.report.remote(payload), timeout=30)
        return True
    except Exception as exc:
        _warn_once(f"[Compat] role={role}: receipt publish failed: "
                   f"{type(exc).__name__}: {exc}")
        return False


def shutdown_collector(timeout_s: float = 10.0) -> bool:
    """Reap the collector at deployment teardown (KI-A6).

    Detached actors survive their creator by design, so without this the
    receipt store for every past deployment stays resident in a reused Ray
    cluster.
    """
    collector = _get_collector()
    if collector is None:
        return False
    try:
        import ray

        dropped = 0
        try:
            dropped = int(ray.get(collector.dropped.remote(), timeout=timeout_s))
        except Exception:
            pass
        if dropped:
            print(f"[Compat] receipt collector dropped {dropped} receipt(s) at the "
                  f"cardinality cap before shutdown", flush=True)
        ray.kill(collector)
        return True
    except Exception:
        return False


def drain_receipts(timeout_s: float = 60.0) -> list[dict]:
    """Head-side drain. Returns [] when the channel is unavailable."""
    collector = _get_collector()
    if collector is None:
        return []
    try:
        import ray

        return list(ray.get(collector.get_all.remote(), timeout=timeout_s))
    except Exception:
        return []


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
