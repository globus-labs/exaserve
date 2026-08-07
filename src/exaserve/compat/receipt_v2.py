"""CompatibilityReceipt version 2 (plan §3.2.1, packet P02, IMP-B04).

Version 1 was role-level: one receipt saying "role=replica is fine" satisfied a
fleet of three thousand replicas. The audit's finding was blunt and correct —
one receipt could certify a much larger fleet, and a shim that delivered
nothing could mark every required patch "not applicable" and still pass.

Version 2 fixes the unit of evidence. A receipt covers exactly one

    (deployment_id, generation, deployment_plan_hash, allocation_binding_hash,
     receipt_requirement_id, component_id, instance_id)

and READY requires **exact set equality** between the planned requirement keys
and the accepted current-instance keys. Not a count — equality. A duplicate
adds no coverage, a superseded instance is stale, and one missing requirement
blocks readiness by name.

Three status values replace the old boolean, because "false" conflated two
different facts:

    APPLIED       the patch took effect here, with a true semantic postcondition
    NOT_REQUIRED  the fully resolved manifest excludes this patch for this role
    FAILED        it was required here and did not take effect

`NOT_REQUIRED` is legal only when the *resolved* manifest excludes the patch —
it can never excuse missing proof for a patch that targets this role.

Version-1 payloads fail closed rather than being permissively upgraded.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Mapping, Optional

SCHEMA_VERSION = 2

_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class ReceiptError(ValueError):
    """A receipt is malformed, unauthorized, or insufficient."""


class PatchStatus(str, Enum):
    APPLIED = "APPLIED"
    NOT_REQUIRED = "NOT_REQUIRED"
    FAILED = "FAILED"


class AttestationType(str, Enum):
    SELF = "SELF"
    SUPERVISOR = "SUPERVISOR"


class OwnerScope(str, Enum):
    GLOBAL = "GLOBAL"
    RANK = "RANK"


def canonical_hash(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _require_sha(value: Any, field_name: str, *, allow_none: bool = False) -> None:
    if value is None and allow_none:
        return
    if not isinstance(value, str) or not _SHA256.match(value):
        raise ReceiptError(f"{field_name} must be a lowercase sha256 hex digest, "
                           f"got {value!r}")


@dataclass(frozen=True)
class PatchResult:
    status: str
    postcondition_passed: bool
    evidence_hash: Optional[str] = None

    def __post_init__(self) -> None:
        if self.status not in {s.value for s in PatchStatus}:
            raise ReceiptError(f"patch status {self.status!r} unknown")
        if self.status == PatchStatus.APPLIED.value and not self.postcondition_passed:
            raise ReceiptError("APPLIED requires a true semantic postcondition")
        _require_sha(self.evidence_hash, "evidence_hash", allow_none=True)


@dataclass(frozen=True)
class CompatibilityReceiptV2:
    schema_version: int
    deployment_id: str
    generation: int
    deployment_plan_hash: str
    site_profile_hash: str
    allocation_binding_hash: str
    compatibility_profile_hash: str
    manifest_hash: str
    receipt_requirement_id: str
    role: str
    component_id: str
    instance_id: str
    owner_scope: str
    owner_rank: Optional[int]
    node_id: str
    pid: Optional[int]
    actor_id: Optional[str]
    executable_hash: str
    argv_hash: Optional[str]
    prepared_environment_hash: str
    observed_versions: dict
    observed_package_hashes: dict
    observed_source_hashes: dict
    patch_results: dict          # patch_id -> PatchResult
    capabilities: tuple
    attestation_type: str
    attested_at: str
    receipt_hash: str = ""

    # -- identity ----------------------------------------------------------
    def coverage_key(self) -> tuple:
        """The exact thing this receipt covers. Not a role, not a count."""
        return (self.deployment_id, self.generation, self.deployment_plan_hash,
                self.allocation_binding_hash, self.receipt_requirement_id,
                self.component_id, self.instance_id)

    def slot_key(self) -> str:
        return self.receipt_requirement_id

    def canonical(self) -> dict[str, Any]:
        data = asdict(self)
        data.pop("receipt_hash", None)
        data["patch_results"] = {
            k: (asdict(v) if isinstance(v, PatchResult) else dict(v))
            for k, v in sorted(self.patch_results.items())}
        data["observed_versions"] = dict(sorted(self.observed_versions.items()))
        data["observed_package_hashes"] = dict(sorted(self.observed_package_hashes.items()))
        data["observed_source_hashes"] = dict(sorted(self.observed_source_hashes.items()))
        data["capabilities"] = sorted(set(self.capabilities))
        return data

    def compute_hash(self) -> str:
        return canonical_hash(self.canonical())

    def finalize(self) -> "CompatibilityReceiptV2":
        from dataclasses import replace

        return replace(self, receipt_hash=self.compute_hash())

    def to_dict(self) -> dict[str, Any]:
        data = self.canonical()
        data["receipt_hash"] = self.receipt_hash or self.compute_hash()
        return data


def validate_receipt(receipt: CompatibilityReceiptV2, *, required_patch_ids,
                     resolved_not_required=(), strict_hash: bool = True) -> None:
    """Structural + semantic validation. Raises ReceiptError with the reason."""
    if receipt.schema_version != SCHEMA_VERSION:
        raise ReceiptError(
            f"schema_version {receipt.schema_version} is not {SCHEMA_VERSION}; "
            "version-1 role-only payloads fail closed rather than upgrading")
    for name in ("deployment_id", "receipt_requirement_id", "role",
                 "component_id", "instance_id", "node_id", "attested_at"):
        if not getattr(receipt, name):
            raise ReceiptError(f"{name} must be a non-empty string")
    if receipt.generation < 0:
        raise ReceiptError("generation must be non-negative")
    for name in ("deployment_plan_hash", "site_profile_hash",
                 "allocation_binding_hash", "compatibility_profile_hash",
                 "manifest_hash", "executable_hash", "prepared_environment_hash"):
        _require_sha(getattr(receipt, name), name)
    _require_sha(receipt.argv_hash, "argv_hash", allow_none=True)

    if receipt.owner_scope not in {s.value for s in OwnerScope}:
        raise ReceiptError(f"owner_scope {receipt.owner_scope!r} invalid")
    if receipt.owner_scope == OwnerScope.GLOBAL.value:
        if receipt.owner_rank is not None:
            raise ReceiptError("owner_rank must be null exactly for GLOBAL")
    elif receipt.owner_rank is None or receipt.owner_rank < 0:
        raise ReceiptError("RANK receipts require a non-negative owner_rank")

    if receipt.pid is None and not receipt.actor_id:
        raise ReceiptError("at least one of pid or actor_id must be present")
    if receipt.pid is not None and receipt.pid <= 0:
        raise ReceiptError("pid must be a positive integer when present")

    if receipt.attestation_type not in {a.value for a in AttestationType}:
        raise ReceiptError(f"attestation_type {receipt.attestation_type!r} invalid")

    results = {k: (v if isinstance(v, PatchResult) else PatchResult(**dict(v)))
               for k, v in receipt.patch_results.items()}
    required = set(required_patch_ids)
    resolved_not_required = set(resolved_not_required)

    missing = sorted(required - set(results))
    if missing:
        raise ReceiptError(f"missing required patch key(s): {missing}")
    failed = sorted(k for k, v in results.items()
                    if v.status == PatchStatus.FAILED.value or not (
                        v.postcondition_passed
                        or v.status == PatchStatus.NOT_REQUIRED.value))
    if failed:
        raise ReceiptError(f"failed patch(es): {failed}")
    # NOT_REQUIRED can never excuse a patch the resolved manifest targets here.
    bogus = sorted(k for k, v in results.items()
                   if v.status == PatchStatus.NOT_REQUIRED.value
                   and k in required and k not in resolved_not_required)
    if bogus:
        raise ReceiptError(
            f"patch(es) {bogus} are targeted at role {receipt.role!r} by the "
            "resolved manifest and cannot be reported NOT_REQUIRED")

    if strict_hash:
        expected = receipt.compute_hash()
        if receipt.receipt_hash and receipt.receipt_hash != expected:
            raise ReceiptError(
                f"receipt_hash mismatch: declared {receipt.receipt_hash[:12]}, "
                f"computed {expected[:12]}")


def receipt_from_dict(data: Mapping[str, Any]) -> CompatibilityReceiptV2:
    """Rehydrate. A version-1 payload fails closed here, by design."""
    if not isinstance(data, Mapping):
        raise ReceiptError("receipt payload must be a mapping")
    if int(data.get("schema_version", 1)) != SCHEMA_VERSION:
        raise ReceiptError(
            f"receipt schema_version {data.get('schema_version')} rejected; "
            "version-1 role-only receipts are not permissively upgraded")
    try:
        patches = {str(k): PatchResult(**dict(v))
                   for k, v in (data.get("patch_results") or {}).items()}
        return CompatibilityReceiptV2(
            schema_version=SCHEMA_VERSION,
            deployment_id=str(data["deployment_id"]),
            generation=int(data["generation"]),
            deployment_plan_hash=str(data["deployment_plan_hash"]),
            site_profile_hash=str(data["site_profile_hash"]),
            allocation_binding_hash=str(data["allocation_binding_hash"]),
            compatibility_profile_hash=str(data["compatibility_profile_hash"]),
            manifest_hash=str(data["manifest_hash"]),
            receipt_requirement_id=str(data["receipt_requirement_id"]),
            role=str(data["role"]),
            component_id=str(data["component_id"]),
            instance_id=str(data["instance_id"]),
            owner_scope=str(data["owner_scope"]),
            owner_rank=(None if data.get("owner_rank") is None
                        else int(data["owner_rank"])),
            node_id=str(data["node_id"]),
            pid=(None if data.get("pid") is None else int(data["pid"])),
            actor_id=(None if data.get("actor_id") is None else str(data["actor_id"])),
            executable_hash=str(data["executable_hash"]),
            argv_hash=(None if data.get("argv_hash") is None else str(data["argv_hash"])),
            prepared_environment_hash=str(data["prepared_environment_hash"]),
            observed_versions=dict(data.get("observed_versions") or {}),
            observed_package_hashes=dict(data.get("observed_package_hashes") or {}),
            observed_source_hashes=dict(data.get("observed_source_hashes") or {}),
            patch_results=patches,
            capabilities=tuple(data.get("capabilities") or ()),
            attestation_type=str(data["attestation_type"]),
            attested_at=str(data["attested_at"]),
            receipt_hash=str(data.get("receipt_hash", "")),
        )
    except KeyError as exc:
        raise ReceiptError(f"receipt payload missing field {exc}") from None
    except (TypeError, ValueError) as exc:
        raise ReceiptError(f"receipt payload invalid: {exc}") from None


class ExactReceiptLedger:
    """Accepted receipts, reconciled against the plan's exact slot set.

    The whole point is set equality. `satisfied()` returns the missing and
    unexpected keys, so a caller can name what is wrong rather than reporting a
    count that happens not to match.
    """

    def __init__(self, plan, binding) -> None:
        self.plan = plan
        self.binding = binding
        self._by_slot: dict[str, CompatibilityReceiptV2] = {}
        self._superseded: list[str] = []
        self.rejected: list[tuple[str, str]] = []

    # -- ingestion ---------------------------------------------------------
    def accept(self, receipt: CompatibilityReceiptV2, *, required_patch_ids,
               resolved_not_required=(), session_rank: Optional[int] = None,
               session_node: Optional[str] = None,
               from_global_authority: bool = False) -> tuple[bool, str]:
        """Validate authorization + structure, then bind to exactly one slot."""
        try:
            validate_receipt(receipt, required_patch_ids=required_patch_ids,
                             resolved_not_required=resolved_not_required)
        except ReceiptError as exc:
            self.rejected.append((receipt.receipt_requirement_id, str(exc)))
            return False, str(exc)

        if receipt.deployment_plan_hash != self.plan.deployment_plan_hash:
            return self._reject(receipt, "deployment_plan_hash does not match the plan")
        if receipt.allocation_binding_hash != self.binding.allocation_binding_hash:
            return self._reject(receipt, "allocation_binding_hash is from another generation")
        if receipt.generation != self.binding.generation:
            return self._reject(receipt, f"stale generation {receipt.generation}")

        slot = next((r for r in self.plan.receipt_requirements
                     if r.receipt_requirement_id == receipt.receipt_requirement_id), None)
        if slot is None:
            return self._reject(receipt, "no such planned receipt requirement")
        if slot.role != receipt.role:
            return self._reject(receipt, f"role {receipt.role!r} != planned {slot.role!r}")
        if slot.owner_scope != receipt.owner_scope:
            return self._reject(receipt, "owner_scope does not match the planned slot")

        # Authorization: a rank session may submit only its own RANK receipts;
        # GLOBAL enters only from the in-process supervisor authority.
        if receipt.owner_scope == OwnerScope.GLOBAL.value:
            if not from_global_authority:
                return self._reject(receipt,
                                    "GLOBAL receipts may enter only from the "
                                    "in-process supervisor authority")
        else:
            if session_rank is None:
                return self._reject(receipt, "RANK receipt without an authenticated session")
            if receipt.owner_rank != session_rank:
                return self._reject(receipt,
                                    f"owner_rank {receipt.owner_rank} != authenticated "
                                    f"rank {session_rank}")
            if slot.planned_rank != session_rank:
                return self._reject(receipt,
                                    f"slot is planned for rank {slot.planned_rank}, "
                                    f"session is rank {session_rank}")
            bound_node = self.binding.node_for(session_rank)
            if bound_node is not None and receipt.node_id != bound_node:
                return self._reject(receipt,
                                    f"node_id {receipt.node_id!r} != bound "
                                    f"{bound_node!r} for rank {session_rank}")
            if session_node is not None and receipt.node_id != session_node:
                return self._reject(receipt, "node_id does not match the session")

        existing = self._by_slot.get(receipt.slot_key())
        if existing is not None:
            if existing.instance_id == receipt.instance_id:
                if existing.receipt_hash == receipt.receipt_hash:
                    return True, "duplicate"          # adds no coverage
                return self._reject(receipt, "conflicting duplicate for the same instance")
            # A newer instance supersedes; the old one becomes stale evidence.
            self._superseded.append(existing.instance_id)
        self._by_slot[receipt.slot_key()] = receipt
        return True, "accepted"

    def _reject(self, receipt: CompatibilityReceiptV2, reason: str) -> tuple[bool, str]:
        self.rejected.append((receipt.receipt_requirement_id, reason))
        return False, reason

    def supersede_instance(self, slot_key: str, instance_id: str) -> None:
        """A restart invalidates that slot's evidence until it re-attests."""
        current = self._by_slot.get(slot_key)
        if current is not None and current.instance_id == instance_id:
            del self._by_slot[slot_key]
            self._superseded.append(instance_id)

    def drop_rank(self, rank: int) -> None:
        """Losing a rank's session removes its evidence immediately."""
        for key, receipt in list(self._by_slot.items()):
            if receipt.owner_rank == rank:
                del self._by_slot[key]

    # -- reconciliation ----------------------------------------------------
    def satisfied(self) -> tuple[bool, dict]:
        planned = self.plan.requirement_keys()
        accepted = frozenset(self._by_slot)
        missing = sorted(planned - accepted)
        unexpected = sorted(accepted - planned)
        detail = {
            "planned": len(planned), "accepted": len(accepted),
            "missing": missing[:20], "unexpected": unexpected[:20],
            "superseded": len(self._superseded), "rejected": len(self.rejected),
        }
        return (not missing and not unexpected), detail

    def count(self) -> int:
        return len(self._by_slot)
