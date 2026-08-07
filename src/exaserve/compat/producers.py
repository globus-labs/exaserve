"""v2 receipt producers for the exactly-planned slots (§3.2.1, IMP-B04).

`receipt_v2.py` defines what a receipt *is* and `ExactReceiptLedger` defines
what READY requires of the set. Neither of them produces one, which is why the
ledger stayed empty in the first production run: every planned slot was missing
and readiness blocked by name — correctly, but on evidence nobody was emitting.

This module is the producer side. Each function builds the receipt for exactly
one planned slot, from the process that can honestly attest to it:

    rank{N}/node_supervisor   SELF, from the NodeSupervisor process itself
    rank{N}/ray_head|worker   SELF, from `ray_start.py` -- our own process,
                              which imports ray and applies the in-process
                              raylet patch, so it can attest to its own state
    global/supervisor         SELF, from the composition root
    global/gateway/<kind>     SUPERVISOR, for the unmodified external daemon
                              the root owns and started

The SELF/SUPERVISOR split is not cosmetic. §3.2.1: supervisor attestation "is
legal only for an individually identified unmodified external daemon owned by
that supervisor; if the manifest requires an in-process patch for that role,
supervisor attestation cannot satisfy it." `attest_supervisor` therefore
refuses to build a receipt for a role whose resolved manifest requires patches.
"""

from __future__ import annotations

import hashlib
import json
import os
import socket
import time
from typing import Any, Mapping, Optional, Sequence

from .receipt_v2 import (
    AttestationType,
    CompatibilityReceiptV2,
    OwnerScope,
    PatchResult,
    PatchStatus,
    ReceiptError,
    SCHEMA_VERSION,
)

_ZERO_SHA = "0" * 64
_EXEC_HASH_CACHE: dict[str, str] = {}

# Excluded from the prepared-environment hash: values that legitimately differ
# per process would make the hash useless, and the channel secret must not be
# an input to anything that leaves the node.
_ENV_EXCLUDE_PREFIXES = ("EXASERVE_CONTROL_SECRET", "PMI_", "PALS_", "MPI_",
                         "SLURM_", "PBS_", "LS_COLORS", "SSH_", "_")


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def file_hash(path: str, *, max_bytes: int = 64 << 20) -> str:
    """Hash an executable. A file we cannot read hashes to a stable zero.

    Returning the zero digest rather than raising keeps the receipt structurally
    valid while making "we could not identify this executable" visible in the
    payload instead of aborting a rank over a permissions quirk.
    """
    if not path:
        return _ZERO_SHA
    cached = _EXEC_HASH_CACHE.get(path)
    if cached is not None:
        return cached
    digest = hashlib.sha256()
    try:
        with open(path, "rb") as handle:
            read = 0
            while read < max_bytes:
                chunk = handle.read(1 << 20)
                if not chunk:
                    break
                digest.update(chunk)
                read += len(chunk)
        value = digest.hexdigest()
    except OSError:
        value = _ZERO_SHA
    _EXEC_HASH_CACHE[path] = value
    return value


def argv_hash(argv: Optional[Sequence[str]]) -> Optional[str]:
    if not argv:
        return None
    return sha256_text(json.dumps([str(a) for a in argv], separators=(",", ":")))


def prepared_environment_hash(env: Optional[Mapping[str, str]] = None) -> str:
    """A canonical hash of the environment this process was prepared with.

    Only the deployment-relevant surface is included, so the hash identifies a
    *prepared environment* rather than an accident of the shell.
    """
    source = os.environ if env is None else env
    keep = {}
    for key, value in source.items():
        if key.startswith(_ENV_EXCLUDE_PREFIXES):
            continue
        if key.startswith(("EXASERVE_", "RAY_", "VLLM_", "ZE_", "ONEAPI_", "PYTHON",
                           "LD_LIBRARY_PATH", "PATH", "VIRTUAL_ENV", "CONDA_")):
            keep[key] = value
    return sha256_text(json.dumps(keep, sort_keys=True, separators=(",", ":")))


def manifest_hash(profile) -> str:
    """Hash of the resolved patch manifest, distinct from the profile id."""
    manifest = [
        {"patch_id": p.patch_id, "target": getattr(p, "target", ""),
         "roles": sorted(p.roles), "required": bool(getattr(p, "required", True)),
         "env_gate": getattr(p, "env_gate", "")}
        for p in sorted(profile.patches, key=lambda p: p.patch_id)
    ]
    return sha256_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")))


def _identity(profile) -> dict[str, Any]:
    """Deployment identity from the environment the root exported.

    Anything missing stays empty and the receipt fails validation loudly here
    rather than being accepted against a plan it cannot name.
    """
    from .collector import deployment_scope

    return {
        "deployment_id": deployment_scope(),
        "generation": int(os.environ.get("EXASERVE_GENERATION", "0") or 0),
        "deployment_plan_hash": os.environ.get("EXASERVE_PLAN_HASH", ""),
        "site_profile_hash": os.environ.get("EXASERVE_SITE_PROFILE_HASH", ""),
        "allocation_binding_hash": os.environ.get(
            "EXASERVE_ALLOCATION_BINDING_HASH", ""),
        "compatibility_profile_hash": profile.profile_id,
        "manifest_hash": manifest_hash(profile),
    }


def _observed_versions() -> dict[str, str]:
    import platform

    observed = {"python": platform.python_version()}
    for module in ("ray", "vllm", "torch"):
        found = __import__("sys").modules.get(module)
        if found is not None and getattr(found, "__version__", None):
            observed[module] = str(found.__version__)
    return observed


def _patch_results(profile, role: str, *,
                   postcondition=None) -> tuple[dict, tuple[str, ...]]:
    """Resolve the role's required patches into v2 results.

    A patch the resolved manifest does not target here is NOT_REQUIRED; a
    postcondition that returns None means the check does not apply in this
    process. Neither can excuse a patch that *is* targeted here, which
    `validate_receipt` enforces on the head.
    """
    from .activator import _postcondition_sitecustomize

    check = postcondition or _postcondition_sitecustomize
    required = profile.required_patch_ids(role)
    gated_out = profile.gated_out(role)
    results: dict[str, PatchResult] = {}
    not_required: list[str] = []
    for patch_id in required:
        try:
            verdict = check(patch_id)
        except Exception:                         # noqa: BLE001
            verdict = False
        if verdict is None:
            results[patch_id] = PatchResult(status=PatchStatus.NOT_REQUIRED.value,
                                            postcondition_passed=False)
            not_required.append(patch_id)
        elif verdict:
            results[patch_id] = PatchResult(status=PatchStatus.APPLIED.value,
                                            postcondition_passed=True)
        else:
            results[patch_id] = PatchResult(status=PatchStatus.FAILED.value,
                                            postcondition_passed=False)
    for patch_id in gated_out:
        results.setdefault(patch_id, PatchResult(
            status=PatchStatus.NOT_REQUIRED.value, postcondition_passed=False))
        not_required.append(patch_id)
    return results, tuple(sorted(set(not_required)))


def attest_self(*, requirement_id: str, role: str, component_id: str,
                instance_id: Optional[str] = None,
                owner_scope: str = OwnerScope.RANK.value,
                owner_rank: Optional[int] = None,
                profile=None, executable: Optional[str] = None,
                argv: Optional[Sequence[str]] = None,
                node_id: Optional[str] = None,
                actor_id: Optional[str] = None,
                postcondition=None) -> CompatibilityReceiptV2:
    """Build a SELF receipt for the process that calls this function."""
    from .profile import default_profile

    profile = profile or default_profile(os.environ.get("EXASERVE_VENDOR", "xpu"))
    host = node_id or socket.gethostname()
    pid = os.getpid()
    results, not_required = _patch_results(profile, role, postcondition=postcondition)
    identity = _identity(profile)
    receipt = CompatibilityReceiptV2(
        schema_version=SCHEMA_VERSION,
        receipt_requirement_id=requirement_id,
        role=role,
        component_id=component_id,
        instance_id=instance_id or f"{host}:{pid}",
        owner_scope=owner_scope,
        owner_rank=(None if owner_scope == OwnerScope.GLOBAL.value else owner_rank),
        node_id=host,
        pid=pid,
        actor_id=actor_id,
        executable_hash=file_hash(executable or __import__("sys").executable),
        argv_hash=argv_hash(argv),
        prepared_environment_hash=prepared_environment_hash(),
        observed_versions=_observed_versions(),
        observed_package_hashes={},
        observed_source_hashes={},
        patch_results=results,
        capabilities=profile.capabilities(),
        attestation_type=AttestationType.SELF.value,
        attested_at=_rfc3339(),
        **identity,
    )
    del not_required        # the head re-resolves this from the shared manifest
    return receipt.finalize()


def attest_supervisor(*, requirement_id: str, role: str, component_id: str,
                      executable: str, instance_id: Optional[str] = None,
                      owner_scope: str = OwnerScope.GLOBAL.value,
                      owner_rank: Optional[int] = None,
                      argv: Optional[Sequence[str]] = None,
                      pid: Optional[int] = None,
                      node_id: Optional[str] = None,
                      profile=None) -> CompatibilityReceiptV2:
    """Attest an UNMODIFIED external daemon this supervisor owns.

    Refuses when the resolved manifest requires an in-process patch for the
    role: a supervisor cannot observe the inside of a process it did not build,
    and §3.2.1 says so explicitly. Silently downgrading to "not applicable" is
    exactly the shim-shaped hole the audit named.
    """
    from .profile import default_profile

    profile = profile or default_profile(os.environ.get("EXASERVE_VENDOR", "xpu"))
    required = profile.required_patch_ids(role)
    if required:
        raise ReceiptError(
            f"role {role!r} requires in-process patch(es) {list(required)}; "
            "supervisor attestation cannot satisfy them")
    host = node_id or socket.gethostname()
    identity = _identity(profile)
    receipt = CompatibilityReceiptV2(
        schema_version=SCHEMA_VERSION,
        receipt_requirement_id=requirement_id,
        role=role,
        component_id=component_id,
        instance_id=instance_id or f"{host}:{pid or 0}",
        owner_scope=owner_scope,
        owner_rank=(None if owner_scope == OwnerScope.GLOBAL.value else owner_rank),
        node_id=host,
        pid=pid,
        actor_id=None,
        executable_hash=file_hash(executable),
        argv_hash=argv_hash(argv),
        prepared_environment_hash=prepared_environment_hash(),
        observed_versions=_observed_versions(),
        observed_package_hashes={},
        observed_source_hashes={},
        patch_results={},
        capabilities=(),
        attestation_type=AttestationType.SUPERVISOR.value,
        attested_at=_rfc3339(),
        **identity,
    )
    return receipt.finalize()


def resolved_not_required(role: str, profile=None) -> tuple[str, ...]:
    """What the RESOLVED manifest legitimately excludes for this role.

    Deliberately re-derived on the head from the shared manifest rather than
    trusted from the payload. If the producer could declare its own excuses,
    "NOT_REQUIRED" would mean "I chose not to prove it", which is the shim hole
    the audit named.
    """
    from .profile import default_profile

    profile = profile or default_profile(os.environ.get("EXASERVE_VENDOR", "xpu"))
    return profile.gated_out(role)


def required_patch_ids(role: str, profile=None) -> tuple[str, ...]:
    from .profile import default_profile

    profile = profile or default_profile(os.environ.get("EXASERVE_VENDOR", "xpu"))
    return profile.required_patch_ids(role)


def _rfc3339(when: Optional[float] = None) -> str:
    moment = time.gmtime(when if when is not None else time.time())
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", moment)


def deliver(receipt: CompatibilityReceiptV2, *, path: Optional[str] = None) -> bool:
    """Hand one exact receipt to the owning NodeSupervisor over the local hop."""
    from .local_ingress import deliver_receipt

    return deliver_receipt(receipt.to_dict(), path=path)
