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
import re
import socket
import stat
import threading
import time
from functools import lru_cache
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

# Excluded from the prepared-environment hash: values that legitimately differ
# per process would make the hash useless, and the channel secret must not be
# an input to anything that leaves the node.
_ENV_EXCLUDE_PREFIXES = (
    "EXASERVE_CONTROL_SECRET",
    "PMI_",
    "PALS_",
    "MPI_",
    "SLURM_",
    "PBS_",
    "LS_COLORS",
    "SSH_",
    "_",
)

_FILE_HASH_CACHE: dict[tuple[str, int, int, int, int, int], str] = {}
_FILE_HASH_LOCK = threading.Lock()
_SHA256 = re.compile(r"[0-9a-f]{64}")


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _preverified_qualified_python_hash(path: str) -> str | None:
    """Reuse node-aggregated Python evidence only on the qualified RO image."""

    from ..plan.runtime_environment import (
        LOCAL_RUNTIME_ROOT_ENV,
        QUALIFIED_PYTHON_ENV,
        QUALIFIED_PYTHON_HASH_ENV,
        QUALIFIED_PYTHON_PROFILE_ENV,
    )

    return _preverified_qualified_python_hash_cached(
        os.path.realpath(path),
        os.path.realpath(os.environ.get(QUALIFIED_PYTHON_ENV, "")),
        os.environ.get(QUALIFIED_PYTHON_HASH_ENV, ""),
        os.environ.get(QUALIFIED_PYTHON_PROFILE_ENV, ""),
        os.environ.get("EXASERVE_SITE_PROFILE_HASH", ""),
        os.environ.get(LOCAL_RUNTIME_ROOT_ENV, ""),
    )


@lru_cache(maxsize=16)
def _preverified_qualified_python_hash_cached(
    resolved_path: str,
    qualified: str,
    expected: str,
    profile_hash: str,
    site_profile_hash: str,
    runtime_root: str,
) -> str | None:
    from ..plan.runtime_environment import filesystem_identity

    if (
        not runtime_root
        or not qualified
        or not _SHA256.fullmatch(expected)
        or not profile_hash
        or profile_hash != site_profile_hash
        or resolved_path != qualified
    ):
        return None
    try:
        identity = filesystem_identity(qualified)
    except RuntimeError:
        return None
    if identity.fstype != "squashfs" or not identity.readonly:
        return None
    return expected


def file_hash(path: str) -> str:
    """Hash the complete resolved executable or fail closed.

    A zero digest and a fixed-size prefix hash both produced structurally valid
    receipts without proving which executable ran.  Hash the opened inode in
    full and reject concurrent mutation instead.  Symlinked executable names
    are resolved deliberately; the receipt identifies the target bytes.
    """
    if not isinstance(path, str) or not path:
        raise ReceiptError("receipt executable path must be non-empty text")
    if preverified := _preverified_qualified_python_hash(path):
        return preverified
    resolved = os.path.realpath(path)
    try:
        observed = os.stat(resolved, follow_symlinks=False)
    except OSError as exc:
        raise ReceiptError(f"could not hash receipt executable {path!r}: {exc}") from exc
    cache_key = (
        resolved,
        observed.st_dev,
        observed.st_ino,
        observed.st_size,
        observed.st_mtime_ns,
        observed.st_ctime_ns,
    )
    with _FILE_HASH_LOCK:
        cached = _FILE_HASH_CACHE.get(cache_key)
    if cached is not None:
        return cached
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    digest = hashlib.sha256()
    try:
        fd = os.open(resolved, flags)
        with os.fdopen(fd, "rb") as handle:
            before = os.fstat(handle.fileno())
            if not stat.S_ISREG(before.st_mode):
                raise ReceiptError("receipt executable must resolve to a regular file")
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
            after = os.fstat(handle.fileno())
    except OSError as exc:
        raise ReceiptError(f"could not hash receipt executable {path!r}: {exc}") from exc
    if (
        before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or before.st_ctime_ns != after.st_ctime_ns
    ):
        raise ReceiptError("receipt executable changed while it was being hashed")
    value = digest.hexdigest()
    final_key = (
        resolved,
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )
    with _FILE_HASH_LOCK:
        # One process normally has one executable. Keep only its current inode
        # identity so a replaced test/runtime file cannot reuse an old digest.
        stale = [key for key in _FILE_HASH_CACHE if key[0] == resolved and key != final_key]
        for key in stale:
            _FILE_HASH_CACHE.pop(key, None)
        _FILE_HASH_CACHE[final_key] = value
    return value


def argv_hash(argv: Optional[Sequence[str]]) -> Optional[str]:
    if argv is None:
        return None
    if isinstance(argv, (str, bytes)):
        raise ReceiptError("receipt argv must be a sequence of non-empty strings")
    items = tuple(argv)
    if any(not isinstance(item, str) or not item for item in items):
        raise ReceiptError("receipt argv must be a sequence of non-empty strings")
    if not items:
        return None
    return sha256_text(json.dumps(items, separators=(",", ":")))


def prepared_environment_hash(env: Optional[Mapping[str, str]] = None) -> str:
    """A canonical hash of the environment this process was prepared with.

    Only the deployment-relevant surface is included, so the hash identifies a
    *prepared environment* rather than an accident of the shell.
    """
    source = os.environ if env is None else env
    if not isinstance(source, Mapping):
        raise ReceiptError("prepared environment must be a string mapping")
    keep = {}
    for key, value in source.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise ReceiptError("prepared environment must be a string mapping")
        if key.startswith(_ENV_EXCLUDE_PREFIXES):
            continue
        if key in {
            "PMIX_MCA_mca_base_param_files",
            "PMIX_MCA_mca_base_component_path",
        } or key.startswith(
            (
                "EXASERVE_",
                "RAY_",
                "VLLM_",
                "ZE_",
                "ONEAPI_",
                "PYTHON",
                "LD_LIBRARY_PATH",
                "PATH",
                "VIRTUAL_ENV",
                "CONDA_",
            )
        ):
            keep[key] = value
    return sha256_text(json.dumps(keep, sort_keys=True, separators=(",", ":")))


@lru_cache(maxsize=16)
def manifest_hash(profile) -> str:
    """Hash of the resolved patch manifest, distinct from the profile id."""

    from .profile import _profile_manifest_hash

    return _profile_manifest_hash(profile)


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
        "allocation_binding_hash": os.environ.get("EXASERVE_ALLOCATION_BINDING_HASH", ""),
        "compatibility_profile_hash": profile.profile_id,
        "manifest_hash": manifest_hash(profile),
    }


@lru_cache(maxsize=1)
def _observed_versions_cached() -> tuple[tuple[str, str], ...]:
    import platform
    from importlib import metadata

    observed = {"python": platform.python_version()}
    for distribution, key in (("ray", "ray"), ("vllm", "vllm"), ("torch", "torch")):
        try:
            observed[key] = metadata.version(distribution)
        except metadata.PackageNotFoundError:
            pass
    return tuple(sorted(observed.items()))


def _observed_versions() -> dict[str, str]:
    return dict(_observed_versions_cached())


@lru_cache(maxsize=16)
def _observed_profile_hashes_cached(
    profile,
) -> tuple[tuple[tuple[str, str], ...], tuple[tuple[str, str], ...]]:
    """Hashes re-derived while the producer constructs its own profile.

    A profile mismatch changes ``profile_id`` and is rejected by the head. The
    maps also preserve which exact distribution/source artifact this process
    observed, rather than leaving the v2 evidence fields permanently empty.
    """
    packages: dict[str, str] = {}
    sources: dict[str, str] = {}
    by_distribution: dict[str, list[tuple[str, str]]] = {}
    for patch in profile.patches:
        sources[f"target:{patch.patch_id}"] = patch.target_source_hash
        sources[f"artifact:{patch.patch_id}"] = patch.patch_artifact_hash
        sources[f"delivery:{patch.patch_id}"] = patch.delivery_artifact_hash
        by_distribution.setdefault(patch.target_distribution, []).append(
            (patch.target_version, patch.target_source_hash)
        )
    if profile.vllm_modelinfo_seeds:
        sources["vllm_modelinfo:manifest"] = profile.vllm_modelinfo_seed_manifest_hash
        sources["vllm_modelinfo:installer"] = profile.vllm_modelinfo_installer_hash
        for source in profile.vllm_modelinfo_support_sources:
            sources[f"vllm_modelinfo:support:{source.target_file}"] = source.target_sha256
            by_distribution.setdefault("vllm", []).append((profile.vllm, source.target_sha256))
        for seed in profile.vllm_modelinfo_seeds:
            sources[f"vllm_modelinfo:target:{seed.architecture}"] = seed.target_sha256
            sources[f"vllm_modelinfo:seed:{seed.architecture}"] = seed.seed_sha256
            by_distribution.setdefault("vllm", []).append((profile.vllm, seed.target_sha256))
    for distribution, entries in by_distribution.items():
        packages[distribution] = sha256_text(
            json.dumps(sorted(set(entries)), separators=(",", ":"))
        )
    return tuple(sorted(packages.items())), tuple(sorted(sources.items()))


def _observed_profile_hashes(profile) -> tuple[dict[str, str], dict[str, str]]:
    packages, sources = _observed_profile_hashes_cached(profile)
    return dict(packages), dict(sources)


def _patch_results(profile, role: str, *, postcondition=None) -> tuple[dict, tuple[str, ...]]:
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
        except Exception:  # noqa: BLE001
            verdict = False
        if verdict is None:
            results[patch_id] = PatchResult(
                status=PatchStatus.NOT_REQUIRED.value, postcondition_passed=False
            )
            not_required.append(patch_id)
        elif verdict:
            results[patch_id] = PatchResult(
                status=PatchStatus.APPLIED.value, postcondition_passed=True
            )
        else:
            results[patch_id] = PatchResult(
                status=PatchStatus.FAILED.value, postcondition_passed=False
            )
    for patch_id in gated_out:
        results.setdefault(
            patch_id, PatchResult(status=PatchStatus.NOT_REQUIRED.value, postcondition_passed=False)
        )
        not_required.append(patch_id)
    return results, tuple(sorted(set(not_required)))


def attest_self(
    *,
    requirement_id: str,
    role: str,
    component_id: str,
    instance_id: Optional[str] = None,
    owner_scope: str = OwnerScope.RANK.value,
    owner_rank: Optional[int] = None,
    profile=None,
    executable: Optional[str] = None,
    argv: Optional[Sequence[str]] = None,
    node_id: Optional[str] = None,
    actor_id: Optional[str] = None,
    postcondition=None,
) -> CompatibilityReceiptV2:
    """Build a SELF receipt for the process that calls this function."""
    from .profile import default_profile

    profile = profile or default_profile(os.environ.get("EXASERVE_VENDOR", "xpu"))
    host = node_id or socket.gethostname()
    pid = os.getpid()
    results, not_required = _patch_results(profile, role, postcondition=postcondition)
    identity = _identity(profile)
    package_hashes, source_hashes = _observed_profile_hashes(profile)
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
        observed_package_hashes=package_hashes,
        observed_source_hashes=source_hashes,
        patch_results=results,
        capabilities=profile.capabilities(),
        attestation_type=AttestationType.SELF.value,
        attested_at=_rfc3339(),
        **identity,
    )
    del not_required  # the head re-resolves this from the shared manifest
    return receipt.finalize()


def attest_supervisor(
    *,
    requirement_id: str,
    role: str,
    component_id: str,
    executable: str,
    instance_id: Optional[str] = None,
    owner_scope: str = OwnerScope.GLOBAL.value,
    owner_rank: Optional[int] = None,
    argv: Optional[Sequence[str]] = None,
    pid: Optional[int] = None,
    node_id: Optional[str] = None,
    profile=None,
) -> CompatibilityReceiptV2:
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
            "supervisor attestation cannot satisfy them"
        )
    host = node_id or socket.gethostname()
    identity = _identity(profile)
    package_hashes, source_hashes = _observed_profile_hashes(profile)
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
        observed_package_hashes=package_hashes,
        observed_source_hashes=source_hashes,
        patch_results={},
        capabilities=(),
        attestation_type=AttestationType.SUPERVISOR.value,
        attested_at=_rfc3339(),
        **identity,
    )
    return receipt.finalize()


def _requirement_patch_environment(plan, requirement_id: str) -> dict[str, str]:
    """Resolve patch gates from one immutable planned logical slot."""
    requirement = next(
        (
            item
            for item in plan.receipt_requirements
            if item.receipt_requirement_id == requirement_id
        ),
        None,
    )
    if requirement is None:
        raise ReceiptError(f"no planned receipt requirement {requirement_id!r}")

    pp_enabled = any(model.pipeline_parallel_size > 1 for model in plan.models)
    multiproc_enabled = any(
        model.pipeline_parallel_size == 1 and model.tensor_parallel_size > 1
        for model in plan.models
    )
    if requirement.role in {"replica", "engine_core", "engine_worker"}:
        matching = [
            model
            for model in plan.models
            if requirement_id.startswith(f"model/{model.route_name}/replica/")
        ]
        if len(matching) != 1:
            raise ReceiptError(
                f"model receipt requirement {requirement_id!r} maps to "
                f"{len(matching)} planned models"
            )
        pp_enabled = matching[0].pipeline_parallel_size > 1
        multiproc_enabled = (
            matching[0].pipeline_parallel_size == 1 and matching[0].tensor_parallel_size > 1
        )
    from .profile import (
        MULTIPROC_WORKER_PATCH_GATE,
        PP_PATCH_GATE,
        RAY_WORKER_PATCH_GATE,
    )

    return {
        PP_PATCH_GATE: "1" if pp_enabled else "0",
        RAY_WORKER_PATCH_GATE: "1" if pp_enabled else "0",
        MULTIPROC_WORKER_PATCH_GATE: "1" if multiproc_enabled else "0",
    }


def patch_requirements_for_plan(
    plan, requirement_id: str, profile=None
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Return head-authoritative required/excluded patches for one slot."""
    from .profile import default_profile

    profile = profile or default_profile(plan.vendor)
    requirement = next(
        (
            item
            for item in plan.receipt_requirements
            if item.receipt_requirement_id == requirement_id
        ),
        None,
    )
    if requirement is None:
        raise ReceiptError(f"no planned receipt requirement {requirement_id!r}")
    env = _requirement_patch_environment(plan, requirement_id)
    return (
        profile.required_patch_ids(requirement.role, env),
        profile.gated_out(requirement.role, env),
    )


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
    """Hand one required receipt to its owner or raise the transport cause."""
    from .local_ingress import deliver_receipt_checked

    deliver_receipt_checked(receipt.to_dict(), path=path)
    return True
