"""Fail-closed activation of the manifest-declared compatibility profile.

The stale Ray Serve full-file replacement has been removed. All remaining
behavior is enumerated by :mod:`exaserve.compat.profile` and delivered through
exact-hash generated target modules or the generated engine shim.
:class:`CompatibilityActivator` is the normal public boundary.
"""

from __future__ import annotations

__all__ = ["apply_patch_ids"]


def apply_patch_ids(patch_ids, strict: bool = True) -> None:
    """Apply exactly the named manifest adapters, once per interpreter.

    Importing :mod:`exaserve._sitecustomize` is definition-only.  Lifecycle
    roles must enter through :class:`CompatibilityActivator`, which resolves
    their required IDs from the immutable profile and calls this function.
    That prevents an unrelated role from importing or mutating Ray/vLLM merely
    because another role in the same profile needs an adapter.
    """
    if not strict:
        raise ValueError("strict=False compatibility activation is unsupported")
    if isinstance(patch_ids, str) or not isinstance(patch_ids, (tuple, list, set, frozenset)):
        raise TypeError("patch_ids must be a finite collection of manifest IDs")

    from exaserve import _sitecustomize

    by_id = {
        patch_id: getattr(_sitecustomize, function_name)
        for function_name, patch_id in _sitecustomize.DECLARED_PATCH_FUNCTIONS.items()
    }
    requested = tuple(sorted(set(patch_ids)))
    unknown = [patch_id for patch_id in requested if patch_id not in by_id and patch_id != "EN-01"]
    if unknown:
        raise RuntimeError(f"no declared runtime adapter for patch ID(s) {unknown}")
    for patch_id in requested:
        # EN-01 is the generated pre-interpreter shim itself, not an adapter
        # function. Its semantic proof is produced inside engine_shim.
        if patch_id == "EN-01":
            continue
        by_id[patch_id]()
