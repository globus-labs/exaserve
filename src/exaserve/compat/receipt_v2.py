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

import re
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping, Optional

from ..plan.contracts import same_node

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
    # Reuse the plan family's strict JSON rule: arbitrary repr/default=str
    # values are not stable evidence and must never enter a receipt identity.
    from ..plan.contracts import canonical_hash as strict_canonical_hash

    return strict_canonical_hash(payload)


def _require_sha(value: Any, field_name: str, *, allow_none: bool = False) -> None:
    if value is None and allow_none:
        return
    if not isinstance(value, str) or not _SHA256.match(value):
        raise ReceiptError(f"{field_name} must be a lowercase sha256 hex digest, got {value!r}")


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
        if not isinstance(self.postcondition_passed, bool):
            raise ReceiptError("postcondition_passed must be a boolean")
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
    patch_results: dict  # patch_id -> PatchResult
    capabilities: tuple
    attestation_type: str
    attested_at: str
    receipt_hash: str = ""

    # -- identity ----------------------------------------------------------
    def coverage_key(self) -> tuple:
        """The exact thing this receipt covers. Not a role, not a count."""
        return (
            self.deployment_id,
            self.generation,
            self.deployment_plan_hash,
            self.allocation_binding_hash,
            self.receipt_requirement_id,
            self.component_id,
            self.instance_id,
        )

    def slot_key(self) -> str:
        return self.receipt_requirement_id

    def canonical(self) -> dict[str, Any]:
        data = asdict(self)
        data.pop("receipt_hash", None)
        data["patch_results"] = {
            k: (asdict(v) if isinstance(v, PatchResult) else dict(v))
            for k, v in sorted(self.patch_results.items())
        }
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


def validate_receipt(
    receipt: CompatibilityReceiptV2,
    *,
    required_patch_ids,
    resolved_not_required=(),
    strict_hash: bool = True,
) -> None:
    """Structural + semantic validation. Raises ReceiptError with the reason."""
    if (
        isinstance(receipt.schema_version, bool)
        or not isinstance(receipt.schema_version, int)
        or receipt.schema_version != SCHEMA_VERSION
    ):
        raise ReceiptError(
            f"schema_version {receipt.schema_version} is not {SCHEMA_VERSION}; "
            "version-1 role-only payloads fail closed rather than upgrading"
        )
    for name in (
        "deployment_id",
        "receipt_requirement_id",
        "role",
        "component_id",
        "instance_id",
        "node_id",
        "attested_at",
    ):
        if not isinstance(getattr(receipt, name), str) or not getattr(receipt, name):
            raise ReceiptError(f"{name} must be a non-empty string")
    if (
        isinstance(receipt.generation, bool)
        or not isinstance(receipt.generation, int)
        or receipt.generation < 0
    ):
        raise ReceiptError("generation must be non-negative")
    for name in (
        "deployment_plan_hash",
        "site_profile_hash",
        "allocation_binding_hash",
        "compatibility_profile_hash",
        "manifest_hash",
        "executable_hash",
        "prepared_environment_hash",
    ):
        _require_sha(getattr(receipt, name), name)
    _require_sha(receipt.argv_hash, "argv_hash", allow_none=True)
    _require_sha(receipt.receipt_hash, "receipt_hash")

    if receipt.owner_scope not in {s.value for s in OwnerScope}:
        raise ReceiptError(f"owner_scope {receipt.owner_scope!r} invalid")
    if receipt.owner_scope == OwnerScope.GLOBAL.value:
        if receipt.owner_rank is not None:
            raise ReceiptError("owner_rank must be null exactly for GLOBAL")
    elif (
        receipt.owner_rank is None
        or isinstance(receipt.owner_rank, bool)
        or not isinstance(receipt.owner_rank, int)
        or receipt.owner_rank < 0
    ):
        raise ReceiptError("RANK receipts require a non-negative owner_rank")

    if receipt.pid is None and not receipt.actor_id:
        raise ReceiptError("at least one of pid or actor_id must be present")
    if receipt.pid is not None and (
        isinstance(receipt.pid, bool) or not isinstance(receipt.pid, int) or receipt.pid <= 0
    ):
        raise ReceiptError("pid must be a positive integer when present")
    if receipt.actor_id is not None and (
        not isinstance(receipt.actor_id, str) or not receipt.actor_id
    ):
        raise ReceiptError("actor_id must be null or a non-empty string")

    if receipt.attestation_type not in {a.value for a in AttestationType}:
        raise ReceiptError(f"attestation_type {receipt.attestation_type!r} invalid")

    for name, values, digest_values in (
        ("observed_versions", receipt.observed_versions, False),
        ("observed_package_hashes", receipt.observed_package_hashes, True),
        ("observed_source_hashes", receipt.observed_source_hashes, True),
    ):
        if not isinstance(values, dict):
            raise ReceiptError(f"{name} must be a map")
        for key, value in values.items():
            if not isinstance(key, str) or not key or not isinstance(value, str):
                raise ReceiptError(f"{name} must be map<string,string>")
            if digest_values:
                _require_sha(value, f"{name}.{key}")
    if (
        not isinstance(receipt.capabilities, tuple)
        or any(not isinstance(value, str) or not value for value in receipt.capabilities)
        or tuple(sorted(set(receipt.capabilities))) != receipt.capabilities
    ):
        raise ReceiptError("capabilities must be a sorted unique list of strings")
    try:
        parsed_time = datetime.fromisoformat(receipt.attested_at.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        raise ReceiptError("attested_at must be an RFC3339 timestamp") from None
    if parsed_time.tzinfo is None or parsed_time.utcoffset() != timezone.utc.utcoffset(None):
        raise ReceiptError("attested_at must be an RFC3339 UTC timestamp")

    if not isinstance(receipt.patch_results, dict):
        raise ReceiptError("patch_results must be a map")
    try:
        results = {
            k: (v if isinstance(v, PatchResult) else PatchResult(**dict(v)))
            for k, v in receipt.patch_results.items()
        }
    except (TypeError, ValueError) as exc:
        raise ReceiptError(f"patch_results invalid: {exc}") from None
    required = set(required_patch_ids)
    resolved_not_required = set(resolved_not_required)

    if any(not isinstance(key, str) or not key for key in results):
        raise ReceiptError("patch_results keys must be non-empty strings")
    unknown = sorted(set(results) - required - resolved_not_required)
    if unknown:
        raise ReceiptError(f"unknown patch key(s): {unknown}")

    missing = sorted(required - set(results))
    if missing:
        raise ReceiptError(f"missing required patch key(s): {missing}")
    failed = sorted(
        k
        for k, v in results.items()
        if v.status == PatchStatus.FAILED.value
        or not (v.postcondition_passed or v.status == PatchStatus.NOT_REQUIRED.value)
    )
    if failed:
        raise ReceiptError(f"failed patch(es): {failed}")
    # NOT_REQUIRED can never excuse a patch the resolved manifest targets here.
    bogus = sorted(
        k
        for k, v in results.items()
        if v.status == PatchStatus.NOT_REQUIRED.value
        and k in required
        and k not in resolved_not_required
    )
    if bogus:
        raise ReceiptError(
            f"patch(es) {bogus} are targeted at role {receipt.role!r} by the "
            "resolved manifest and cannot be reported NOT_REQUIRED"
        )

    if strict_hash:
        expected = receipt.compute_hash()
        if receipt.receipt_hash != expected:
            raise ReceiptError(
                f"receipt_hash mismatch: declared {receipt.receipt_hash[:12]}, "
                f"computed {expected[:12]}"
            )


def receipt_from_dict(data: Mapping[str, Any]) -> CompatibilityReceiptV2:
    """Rehydrate. A version-1 payload fails closed here, by design."""
    if not isinstance(data, Mapping):
        raise ReceiptError("receipt payload must be a mapping")
    if isinstance(data.get("schema_version"), bool) or data.get("schema_version") != SCHEMA_VERSION:
        raise ReceiptError(
            f"receipt schema_version {data.get('schema_version')} rejected; "
            "version-1 role-only receipts are not permissively upgraded"
        )
    expected_fields = set(CompatibilityReceiptV2.__dataclass_fields__)
    if set(data) != expected_fields:
        raise ReceiptError(
            f"receipt payload shape mismatch: unknown={sorted(set(data) - expected_fields)}, "
            f"missing={sorted(expected_fields - set(data))}"
        )
    try:
        if not isinstance(data["patch_results"], Mapping):
            raise TypeError("patch_results must be a map")
        patches = {}
        for key, value in data["patch_results"].items():
            if not isinstance(key, str) or not isinstance(value, Mapping):
                raise TypeError("patch_results must be map<string,object>")
            expected_patch = set(PatchResult.__dataclass_fields__)
            if set(value) != expected_patch:
                raise TypeError(
                    f"patch_results.{key} shape mismatch: "
                    f"unknown={sorted(set(value) - expected_patch)}, "
                    f"missing={sorted(expected_patch - set(value))}"
                )
            patches[key] = PatchResult(**dict(value))
        for name in ("observed_versions", "observed_package_hashes", "observed_source_hashes"):
            if not isinstance(data[name], Mapping):
                raise TypeError(f"{name} must be a map")
        if not isinstance(data["capabilities"], list):
            raise TypeError("capabilities must be a JSON array")
        return CompatibilityReceiptV2(
            schema_version=SCHEMA_VERSION,
            deployment_id=data["deployment_id"],
            generation=data["generation"],
            deployment_plan_hash=data["deployment_plan_hash"],
            site_profile_hash=data["site_profile_hash"],
            allocation_binding_hash=data["allocation_binding_hash"],
            compatibility_profile_hash=data["compatibility_profile_hash"],
            manifest_hash=data["manifest_hash"],
            receipt_requirement_id=data["receipt_requirement_id"],
            role=data["role"],
            component_id=data["component_id"],
            instance_id=data["instance_id"],
            owner_scope=data["owner_scope"],
            owner_rank=data["owner_rank"],
            node_id=data["node_id"],
            pid=data["pid"],
            actor_id=data["actor_id"],
            executable_hash=data["executable_hash"],
            argv_hash=data["argv_hash"],
            prepared_environment_hash=data["prepared_environment_hash"],
            observed_versions=dict(data["observed_versions"]),
            observed_package_hashes=dict(data["observed_package_hashes"]),
            observed_source_hashes=dict(data["observed_source_hashes"]),
            patch_results=patches,
            capabilities=tuple(data["capabilities"]),
            attestation_type=data["attestation_type"],
            attested_at=data["attested_at"],
            receipt_hash=data["receipt_hash"],
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

    def __init__(self, plan, binding, *, binding_store=None) -> None:
        if binding.deployment_id != plan.deployment_id:
            raise ReceiptError("allocation binding deployment_id disagrees with plan")
        if binding.deployment_plan_hash != plan.deployment_plan_hash:
            raise ReceiptError("allocation binding deployment_plan_hash disagrees with plan")
        if binding.site_profile_hash != plan.site_profile_hash:
            raise ReceiptError("allocation binding site_profile_hash disagrees with plan")
        if len(binding.rank_to_node) != plan.num_nodes:
            raise ReceiptError("allocation binding rank count disagrees with plan")
        from .producers import manifest_hash
        from .profile import default_profile

        profile = default_profile(plan.vendor)
        if profile.profile_id != plan.compatibility_profile_hash:
            raise ReceiptError("no locally resolved compatibility profile matches the plan")
        if manifest_hash(profile) != plan.manifest_hash:
            raise ReceiptError("locally resolved patch manifest does not match the plan")
        self.plan = plan
        self.binding = binding
        self.profile = profile
        self.binding_store = binding_store
        self._lock = threading.RLock()
        self._by_slot: dict[str, CompatibilityReceiptV2] = {}
        self._superseded: list[str] = []
        self.rejected: list[tuple[str, str]] = []

    # -- ingestion ---------------------------------------------------------
    def accept(
        self,
        receipt: CompatibilityReceiptV2,
        *,
        required_patch_ids=None,
        resolved_not_required=None,
        session_rank: Optional[int] = None,
        session_node: Optional[str] = None,
        from_global_authority: bool = False,
    ) -> tuple[bool, str]:
        """Validate authorization + structure, then bind to exactly one slot."""
        with self._lock:
            return self._accept(
                receipt,
                required_patch_ids=required_patch_ids,
                resolved_not_required=resolved_not_required,
                session_rank=session_rank,
                session_node=session_node,
                from_global_authority=from_global_authority,
            )

    def _accept(
        self,
        receipt: CompatibilityReceiptV2,
        *,
        required_patch_ids=None,
        resolved_not_required=None,
        session_rank: Optional[int] = None,
        session_node: Optional[str] = None,
        from_global_authority: bool = False,
    ) -> tuple[bool, str]:
        try:
            if required_patch_ids is None:
                from .producers import patch_requirements_for_plan

                required_patch_ids, resolved_not_required = patch_requirements_for_plan(
                    self.plan, receipt.receipt_requirement_id, self.profile
                )
            elif resolved_not_required is None:
                resolved_not_required = ()
            validate_receipt(
                receipt,
                required_patch_ids=required_patch_ids,
                resolved_not_required=resolved_not_required,
            )
            # Retain a private value snapshot.  The dataclass is frozen, but
            # its compatibility maps are ordinary dictionaries; keeping the
            # caller's object would let post-accept mutation alter the exact
            # evidence behind READY without another validation/hash check.
            receipt = receipt_from_dict(receipt.to_dict())
        except ReceiptError as exc:
            self.rejected.append((receipt.receipt_requirement_id, str(exc)))
            return False, str(exc)

        if receipt.deployment_plan_hash != self.plan.deployment_plan_hash:
            return self._reject(receipt, "deployment_plan_hash does not match the plan")
        if receipt.deployment_id != self.plan.deployment_id:
            return self._reject(receipt, "deployment_id does not match the plan")
        if receipt.site_profile_hash != self.plan.site_profile_hash:
            return self._reject(receipt, "site_profile_hash does not match the plan")
        if receipt.compatibility_profile_hash != self.plan.compatibility_profile_hash:
            return self._reject(receipt, "compatibility_profile_hash does not match the plan")
        if receipt.manifest_hash != self.plan.manifest_hash:
            return self._reject(receipt, "manifest_hash does not match the plan")
        expected_versions = {
            "python": self.profile.python,
            "ray": self.profile.ray,
            "vllm": self.profile.vllm,
        }
        mismatched_versions = {
            key: (wanted, receipt.observed_versions.get(key))
            for key, wanted in expected_versions.items()
            if receipt.observed_versions.get(key) != wanted
        }
        if mismatched_versions:
            return self._reject(
                receipt, f"observed compatibility versions mismatch: {mismatched_versions}"
            )
        expected_sources = {}
        for patch in self.profile.patches:
            expected_sources[f"target:{patch.patch_id}"] = patch.target_source_hash
            expected_sources[f"artifact:{patch.patch_id}"] = patch.patch_artifact_hash
            expected_sources[f"delivery:{patch.patch_id}"] = patch.delivery_artifact_hash
        if self.profile.vllm_modelinfo_seeds:
            expected_sources["vllm_modelinfo:manifest"] = (
                self.profile.vllm_modelinfo_seed_manifest_hash
            )
            expected_sources["vllm_modelinfo:installer"] = (
                self.profile.vllm_modelinfo_installer_hash
            )
            for source in self.profile.vllm_modelinfo_support_sources:
                expected_sources[f"vllm_modelinfo:support:{source.target_file}"] = (
                    source.target_sha256
                )
            for seed in self.profile.vllm_modelinfo_seeds:
                expected_sources[f"vllm_modelinfo:target:{seed.architecture}"] = seed.target_sha256
                expected_sources[f"vllm_modelinfo:seed:{seed.architecture}"] = seed.seed_sha256
        mismatched_sources = {
            key: (wanted, receipt.observed_source_hashes.get(key))
            for key, wanted in expected_sources.items()
            if receipt.observed_source_hashes.get(key) != wanted
        }
        if mismatched_sources:
            return self._reject(receipt, "observed compatibility source/artifact hashes mismatch")
        if (
            receipt.attestation_type == AttestationType.SELF.value
            and receipt.capabilities != self.profile.capabilities()
        ):
            return self._reject(receipt, "capabilities do not match the compatibility profile")
        if receipt.allocation_binding_hash != self.binding.allocation_binding_hash:
            return self._reject(receipt, "allocation_binding_hash is from another generation")
        if receipt.generation != self.binding.generation:
            return self._reject(receipt, f"stale generation {receipt.generation}")

        slot = next(
            (
                r
                for r in self.plan.receipt_requirements
                if r.receipt_requirement_id == receipt.receipt_requirement_id
            ),
            None,
        )
        if slot is None:
            return self._reject(receipt, "no such planned receipt requirement")
        if slot.role != receipt.role:
            return self._reject(receipt, f"role {receipt.role!r} != planned {slot.role!r}")
        if slot.owner_scope != receipt.owner_scope:
            return self._reject(receipt, "owner_scope does not match the planned slot")
        if slot.component_slot != receipt.component_id:
            return self._reject(
                receipt, f"component_id {receipt.component_id!r} != planned {slot.component_slot!r}"
            )
        if slot.attestation_type != receipt.attestation_type:
            return self._reject(
                receipt,
                f"attestation_type {receipt.attestation_type!r} != planned "
                f"{slot.attestation_type!r}",
            )

        # Authorization: a rank session may submit only its own RANK receipts;
        # GLOBAL enters only from the in-process supervisor authority.
        if receipt.owner_scope == OwnerScope.GLOBAL.value:
            if not from_global_authority:
                return self._reject(
                    receipt,
                    "GLOBAL receipts may enter only from the in-process supervisor authority",
                )
            head_node = self.binding.node_for(0)
            if head_node is None or not same_node(head_node, receipt.node_id):
                return self._reject(
                    receipt,
                    f"GLOBAL node_id {receipt.node_id!r} is not allocation head {head_node!r}",
                )
        else:
            if session_rank is None:
                return self._reject(receipt, "RANK receipt without an authenticated session")
            if receipt.owner_rank != session_rank:
                return self._reject(
                    receipt, f"owner_rank {receipt.owner_rank} != authenticated rank {session_rank}"
                )
            if slot.planned_rank != session_rank:
                return self._reject(
                    receipt,
                    f"slot is planned for rank {slot.planned_rank}, session is rank {session_rank}",
                )
            # Compare canonical node identity: the binding holds the
            # scheduler's (fully qualified) name and a process reports its
            # short hostname, so a literal comparison rejects every receipt
            # from every correctly-placed rank.
            bound_node = self.binding.node_for(session_rank)
            if bound_node is not None and not same_node(bound_node, receipt.node_id):
                return self._reject(
                    receipt,
                    f"node_id {receipt.node_id!r} != bound {bound_node!r} for rank {session_rank}",
                )
            if session_node is not None and not same_node(session_node, receipt.node_id):
                return self._reject(receipt, "node_id does not match the session")

        existing = self._by_slot.get(receipt.slot_key())
        if existing is not None:
            if existing.instance_id == receipt.instance_id:
                if existing.receipt_hash == receipt.receipt_hash:
                    return True, "duplicate"  # adds no coverage
                return self._reject(receipt, "conflicting duplicate for the same instance")
            # A newer instance supersedes; the old one becomes stale evidence.
            self._superseded.append(existing.instance_id)
        if self.binding_store is not None:
            self.binding_store.bind_receipt(receipt)
        self._by_slot[receipt.slot_key()] = receipt
        return True, "accepted"

    def _reject(self, receipt: CompatibilityReceiptV2, reason: str) -> tuple[bool, str]:
        self.rejected.append((receipt.receipt_requirement_id, reason))
        return False, reason

    def supersede_instance(self, slot_key: str, instance_id: str) -> None:
        """A restart invalidates that slot's evidence until it re-attests."""
        with self._lock:
            current = self._by_slot.get(slot_key)
            if current is not None and current.instance_id == instance_id:
                if self.binding_store is not None:
                    self.binding_store.revoke_slot(slot_key, reason="superseded")
                del self._by_slot[slot_key]
                self._superseded.append(instance_id)

    def drop_rank(self, rank: int) -> None:
        """Losing a rank's session removes its evidence immediately."""
        with self._lock:
            for key, receipt in list(self._by_slot.items()):
                if receipt.owner_rank == rank:
                    if self.binding_store is not None:
                        self.binding_store.revoke_slot(key, reason="rank lease lost")
                    del self._by_slot[key]

    def stage_rank_snapshot(
        self,
        rank: int,
        receipts,
        *,
        session_node: str,
    ) -> tuple[bool, str, "ExactReceiptLedger | None"]:
        """Build, but do not publish, an atomic replacement for one rank.

        Patch requirements are re-derived from the immutable exact slot, never
        from process-global gates or the receipt payload. The caller commits
        the returned candidate only after the control
        session state machine also accepts the complete snapshot.
        """
        with self._lock:
            candidate = ExactReceiptLedger(self.plan, self.binding)
            candidate._by_slot = dict(self._by_slot)
            candidate._superseded = list(self._superseded)
            candidate.rejected = list(self.rejected)
            candidate.drop_rank(rank)
            for receipt in receipts:
                ok, detail = candidate.accept(
                    receipt,
                    session_rank=rank,
                    session_node=session_node,
                )
                if not ok:
                    return False, detail, None
            return True, "rank snapshot staged", candidate

    def commit_staged(self, candidate: "ExactReceiptLedger") -> None:
        """Publish a candidate produced by :meth:`stage_rank_snapshot`."""
        with self._lock:
            if candidate.plan is not self.plan or candidate.binding is not self.binding:
                raise ReceiptError("staged receipt ledger belongs to another plan/binding")
            before = dict(self._by_slot)
            if self.binding_store is not None:
                for key, receipt in before.items():
                    replacement = candidate._by_slot.get(key)
                    if replacement is None:
                        self.binding_store.revoke_slot(
                            key, reason="replacement snapshot removed slot"
                        )
                for key, receipt in candidate._by_slot.items():
                    prior = before.get(key)
                    if prior is None or prior.instance_id != receipt.instance_id:
                        self.binding_store.bind_receipt(receipt)
            self._by_slot = dict(candidate._by_slot)
            self._superseded = list(candidate._superseded)
            self.rejected = list(candidate.rejected)

    def commit_rank_snapshot(self, rank: int, candidate: "ExactReceiptLedger") -> None:
        """Publish only one rank from a staged replacement.

        Other ranks may durably submit receipts while the listener awaits this
        snapshot transaction.  Merging the candidate's rank-owned subset keeps
        those concurrent receipts instead of replacing the whole ledger with a
        stale copy.
        """
        with self._lock:
            if candidate.plan is not self.plan or candidate.binding is not self.binding:
                raise ReceiptError("staged receipt ledger belongs to another plan/binding")
            before_rank = {
                key: receipt for key, receipt in self._by_slot.items() if receipt.owner_rank == rank
            }
            after_rank = {
                key: receipt
                for key, receipt in candidate._by_slot.items()
                if receipt.owner_rank == rank
            }
            if self.binding_store is not None:
                for key in before_rank:
                    if key not in after_rank:
                        self.binding_store.revoke_slot(
                            key, reason="replacement snapshot removed slot"
                        )
                for key, receipt in after_rank.items():
                    prior = before_rank.get(key)
                    if prior is None or prior.instance_id != receipt.instance_id:
                        self.binding_store.bind_receipt(receipt)
            for key in before_rank:
                self._by_slot.pop(key, None)
            self._by_slot.update(after_rank)
            self._superseded = list(candidate._superseded)
            self.rejected = list(candidate.rejected)

    # -- reconciliation ----------------------------------------------------
    def satisfied(self) -> tuple[bool, dict]:
        with self._lock:
            planned = self.plan.requirement_keys()
            accepted = frozenset(self._by_slot)
            missing = sorted(planned - accepted)
            unexpected = sorted(accepted - planned)
            detail = {
                "planned": len(planned),
                "accepted": len(accepted),
                # The transport already bounds planned items.  Truncating this set
                # hid the actual blocker (for example rank1 loss sorted after model
                # slots), violating the exact blocker-reporting contract.
                "missing": missing,
                "unexpected": unexpected,
                "superseded": len(self._superseded),
                "rejected": len(self.rejected),
            }
            return (not missing and not unexpected), detail

    def count(self) -> int:
        with self._lock:
            return len(self._by_slot)

    def accepted_receipts(self) -> tuple[CompatibilityReceiptV2, ...]:
        """Return a stable detached projection for readiness publication.

        The coordinator must publish the *exact evidence identities* that made
        READY true.  Exposing the private mutable mapping would let a caller
        accidentally race a reconnect/re-attestation, and receipt objects
        contain mutable maps despite their frozen outer dataclass. Returning
        slot-sorted value snapshots gives the status writer one coherent
        projection without exposing the ledger's retained evidence.
        """
        with self._lock:
            return tuple(
                receipt_from_dict(self._by_slot[key].to_dict()) for key in sorted(self._by_slot)
            )
