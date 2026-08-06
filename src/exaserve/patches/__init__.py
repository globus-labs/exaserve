"""Aurora-specific patches for Ray Serve and vLLM.

Pinned upstream (verified against the live install 2026-08-05, WP3):
    ray==2.53.0 (commit 0de2118), Aurora frameworks 2025.3.1
    vllm==0.15.0 as bundled in Aurora frameworks 2025.3.1

Two patch sets, two delivery mechanisms:

A) ray.serve._private — overlay (PYTHONPATH shadowing)
   Located: exaserve/patches/ray_serve_overlay/ray/serve/_private/
   Activation: launcher prepends the overlay tree to PYTHONPATH so individual
       files override the system Ray serve._private modules.
   Lifecycle: must be in place before ``import ray.serve``. Monkey-patching
       at runtime is too late because Ray instantiates classes during import.

B) vLLM and ray accelerator — runtime monkey-patches
   Located: exaserve/_sitecustomize.py
       (12 live ``_patch_*`` / ``_install_*`` functions; three dead patches
       removed 2026-08-05, see doc/hardening/COMPATIBILITY_INVENTORY.md)
   Activation: explicit via ``exaserve.patches.apply_all()``.
       Each function is gated by an EXASERVE_VLLM_*/EXASERVE_* env var.
       Default-on: EXASERVE_VLLM_PATCH_PP_LAYER_FILTER=1 — required for vLLM
       pipeline-parallel deployments.
   Lifecycle: applied after import but before classes are instantiated.

History note: prior to v0.1.0 the patches in (B) lived in ``src/sitecustomize.py``
and CPython auto-imported them on every Python startup whenever ``src/`` was
on ``sys.path``. That auto-import behavior was a footgun for users who
installed the package and didn't realize their interpreter was being patched.
The file is now ``exaserve/_sitecustomize.py`` (leading underscore)
so CPython no longer auto-imports it — activation is explicit through
``apply_all()`` instead.

The current overlay tree is single-tier and opt-in under
EXASERVE_RAY_SERVE_INSTRUMENTATION. Diffing against upstream ray 2.53.0
(2026-08-05) showed it carries ONLY configuration constants and
instrumentation — no functional fixes; the per-hunk classification lives in
``doc/hardening/COMPATIBILITY_INVENTORY.md`` and the target architecture in
``doc/hardening/decisions/ADR-003-compatibility-delivery.md``.
"""

from __future__ import annotations

import os

__all__ = [
    "apply_all",
    "RAY_PINNED_VERSION",
    "RAY_PINNED_COMMIT",
    "OVERLAY_ENV_VAR",
    "OVERLAY_ENV_VAR_LEGACY",
]

RAY_PINNED_VERSION = "2.53.0"
RAY_PINNED_COMMIT = "0de211850589aea71f842873bc32574c702ab492"

# Env-var rename for the overlay activation gate. EXASERVE_RAY_SERVE_INSTRUMENTATION
# is the new (semantically clearer) name. EXASERVE_INSTRUMENTATION is the legacy
# name and is read as a fallback so existing run scripts keep working.
OVERLAY_ENV_VAR = "EXASERVE_RAY_SERVE_INSTRUMENTATION"
OVERLAY_ENV_VAR_LEGACY = "EXASERVE_INSTRUMENTATION"


def _check_ray_version(strict: bool = False) -> None:
    """Warn (or raise, if strict) when Ray drifts from the pinned version.

    The overlay was written against ray==2.49.1 (commit c057f1e). Internal API
    changes between Ray versions can silently break the patches.
    """
    try:
        from ray import _version as _ray_version_mod
    except Exception:
        return
    actual_version = getattr(_ray_version_mod, "version", None)
    actual_commit = getattr(_ray_version_mod, "commit", None)
    if actual_version != RAY_PINNED_VERSION:
        msg = (
            f"[exaserve.patches] Ray version mismatch: "
            f"pinned={RAY_PINNED_VERSION} (commit {RAY_PINNED_COMMIT[:7]}), "
            f"runtime={actual_version} (commit {(actual_commit or '?')[:7]}). "
            f"Patches may break."
        )
        if strict:
            raise RuntimeError(msg)
        if os.environ.get("EXASERVE_PATCH_VERBOSE", "0") == "1":
            print(msg, flush=True)


def overlay_active() -> bool:
    """True when the Ray Serve overlay should be installed by the launcher.

    Reads EXASERVE_RAY_SERVE_INSTRUMENTATION first, falls back to EXASERVE_INSTRUMENTATION.
    """
    return (
        os.environ.get(OVERLAY_ENV_VAR, "0") == "1"
        or os.environ.get(OVERLAY_ENV_VAR_LEGACY, "0") == "1"
    )


_apply_all_done = False


def apply_all(strict: bool = False) -> None:
    """Aurora's two-tier patch installer.

    MUST be called BEFORE any ``import ray.serve`` or ``import vllm`` in the
    Python process. Ray's serve internals are instantiated during package
    import, so monkey-patching after the fact is too late — the overlay
    must already be in place via PYTHONPATH precedence at that point.

    Why explicit (rather than auto-import):
        * Auto-importing on every Python startup is a footgun for users
          who pip-install the package — their interpreter would silently
          start applying vLLM monkey-patches even when they don't want them.
        * Explicit ``apply_all()`` puts the user in control. The launcher
          and driver entry points call it themselves at the right moment.

    Today the overlay (set A) activation is handled by the launcher (it
    stages the overlay tree onto PYTHONPATH on every node). The vLLM
    monkey-patches (set B) are applied here by importing
    ``exaserve._sitecustomize`` — its module-level ``_patch_*()``
    calls fire then, each respecting its own env-var gate. That replaces
    CPython's old auto-import of ``sitecustomize.py``.

    Idempotent: subsequent calls are no-ops.
    """
    global _apply_all_done
    if _apply_all_done:
        return

    _check_ray_version(strict=strict)

    # Importing _sitecustomize triggers its top-level ``_patch_*()`` calls,
    # each gated on its own env var. This is the new explicit activation
    # point that replaces CPython's auto-import of ``sitecustomize.py``.
    try:
        from exaserve import _sitecustomize  # noqa: F401
    except Exception as exc:
        if strict:
            raise
        if os.environ.get("EXASERVE_PATCH_VERBOSE", "0") == "1":
            print(
                f"[exaserve.patches] _sitecustomize import failed: {exc!r}",
                flush=True,
            )

    _apply_all_done = True
