"""Typed per-role compatibility receipts (plan WP3.13, audit IMP-B04).

Every managed process role publishes a receipt proving WHICH profile it
activated and which required patches actually applied. READY requires a
matching receipt from every required role; a missing, stale, or mismatched
receipt blocks readiness (``control.readiness``).

Receipts carry no secret.
"""

from __future__ import annotations

import os
import socket
import time
from dataclasses import asdict, dataclass
from typing import Any

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class CompatibilityReceipt:
    schema_version: int
    deployment_id: str
    generation: int
    profile_id: str
    profile_name: str
    role: str
    node_id: str
    pid: int
    versions: dict[str, str]
    # patch_id -> True (applied + postcondition ok) / False (failed)
    patch_results: dict[str, bool]
    capabilities: tuple[str, ...]
    attestation: str          # "self" (managed python) | "supervisor" (daemon)
    created_at: float
    # Patches that are requested by configuration but cannot apply in THIS
    # process because their target module is not imported here. Recorded
    # explicitly rather than reported as applied — a receipt must never claim
    # a patch took effect when it did not.
    not_applicable: tuple[str, ...] = ()

    def is_complete_for(self, required_patch_ids: tuple[str, ...]) -> tuple[bool, str]:
        """Is this receipt sufficient evidence for the required patch set?

        Evidence classes differ by attestation (plan §3.1):
          - ``self``: our own code proved each patch in-process (sentinels);
          - ``supervisor``: an UNMODIFIED daemon attested by its owner, which
            can prove the executable and prepared environment but cannot reach
            inside the process for a sentinel. Such a receipt must therefore
            declare every required patch as not-provable-here AND carry the
            owner's probe. It is deliberately weaker, and recorded as such.
        """
        if self.attestation == "supervisor":
            if not (self.versions.get("executable") or self.versions.get("probe")):
                return False, "external attestation carries no executable/probe evidence"
            unaccounted = [p for p in required_patch_ids
                           if p not in self.not_applicable]
            if unaccounted:
                return False, (f"external attestation does not account for "
                               f"{sorted(unaccounted)}")
            return True, "externally attested (no in-process patch proof)"
        accounted = set(self.patch_results) | set(self.not_applicable)
        missing = [p for p in required_patch_ids if p not in accounted]
        failed = [p for p, ok in self.patch_results.items() if not ok]
        if missing:
            return False, f"missing patch results: {sorted(missing)}"
        if failed:
            return False, f"failed patches: {sorted(failed)}"
        return True, "ok"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_receipt(*, profile, role: str, deployment_id: str, generation: int,
                  patch_results: dict[str, bool], attestation: str = "self",
                  versions: dict[str, str] | None = None,
                  not_applicable: tuple[str, ...] = ()) -> CompatibilityReceipt:
    import platform

    observed = dict(versions or {})
    observed.setdefault("python", platform.python_version())
    for mod in ("ray", "vllm"):
        if mod not in observed:
            try:
                observed[mod] = __import__(mod).__version__
            except Exception:
                pass
    return CompatibilityReceipt(
        schema_version=SCHEMA_VERSION,
        deployment_id=deployment_id,
        generation=generation,
        profile_id=profile.profile_id,
        profile_name=profile.name,
        role=role,
        node_id=socket.gethostname(),
        pid=os.getpid(),
        versions=observed,
        patch_results=dict(patch_results),
        capabilities=profile.capabilities(),
        created_at=time.time(),
        attestation=attestation,
        not_applicable=tuple(not_applicable),
    )


class ReceiptStore:
    """Collects receipts on the supervisor side and answers the READY question.

    Receipts normally arrive over the §3.2 control channel; this store is the
    projection the readiness predicate consults.
    """

    def __init__(self, profile, deployment_id: str, generation: int) -> None:
        self.profile = profile
        self.deployment_id = deployment_id
        self.generation = generation
        self._by_role: dict[str, list[CompatibilityReceipt]] = {}
        self._seen: set[tuple] = set()

    def add(self, receipt: CompatibilityReceipt) -> tuple[bool, str]:
        """Record a receipt. Returns (accepted, reason)."""
        if receipt.schema_version != SCHEMA_VERSION:
            return False, f"unknown receipt schema {receipt.schema_version}"
        if receipt.deployment_id != self.deployment_id:
            return False, "wrong deployment"
        if receipt.generation != self.generation:
            return False, f"stale generation {receipt.generation}"
        if receipt.profile_id != self.profile.profile_id:
            return False, (f"profile mismatch: receipt={receipt.profile_id[:12]} "
                           f"expected={self.profile.profile_id[:12]}")
        ok, reason = receipt.is_complete_for(
            self.profile.required_patch_ids(receipt.role))
        if not ok:
            return False, reason
        # The channel is drained on every readiness poll and drains are not
        # destructive, so the SAME receipt arrives repeatedly. Identity is
        # (role, node, pid, creation) — re-delivery must not inflate counts.
        identity = (receipt.role, receipt.node_id, receipt.pid, receipt.created_at)
        if identity in self._seen:
            return True, "duplicate"
        self._seen.add(identity)
        self._by_role.setdefault(receipt.role, []).append(receipt)
        return True, "ok"

    def missing_roles(self) -> list[str]:
        return [r for r in self.profile.required_roles if not self._by_role.get(r)]

    def satisfied(self) -> tuple[bool, str]:
        missing = self.missing_roles()
        if missing:
            return False, f"no compatibility receipt from role(s): {missing}"
        return True, "all required roles attested"

    def count(self) -> int:
        return sum(len(v) for v in self._by_role.values())

    def externally_attested_roles(self) -> list[str]:
        """Roles whose evidence is an owner attestation, not in-process proof."""
        return sorted(role for role, rs in self._by_role.items()
                      if rs and all(r.attestation == "supervisor" for r in rs))
