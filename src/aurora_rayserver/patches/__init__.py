"""Aurora-specific patches for Ray Serve and vLLM.

Pinned upstream:
    ray==2.49.1 (commit c057f1e), Aurora frameworks 2025.2.0
    vllm: as bundled in Aurora frameworks 2025.2.0

Two patch sets, two delivery mechanisms:

A) ray.serve._private — overlay (PYTHONPATH shadowing)
   Located: aurora_rayserver/patches/ray_serve_overlay/ray/serve/_private/
   Activation: launcher prepends the overlay tree to PYTHONPATH so individual
       files override the system Ray serve._private modules.
   Lifecycle: must be in place before ``import ray.serve``. Monkey-patching
       at runtime is too late because Ray instantiates classes during import.

B) vLLM and ray accelerator — runtime monkey-patches
   Located: aurora_rayserver/_sitecustomize.py
       (16 ``_patch_*`` / ``_install_*`` functions)
   Activation: explicit via ``aurora_rayserver.patches.apply_all()``.
       Each function is gated by an AURORA_VLLM_*/AURORA_* env var.
       Default-on: AURORA_VLLM_PATCH_PP_LAYER_FILTER=1 — required for vLLM
       pipeline-parallel deployments.
   Lifecycle: applied after import but before classes are instantiated.

History note: prior to v0.1.0 the patches in (B) lived in ``src/sitecustomize.py``
and CPython auto-imported them on every Python startup whenever ``src/`` was
on ``sys.path``. That auto-import behavior was a footgun for users who
installed the package and didn't realize their interpreter was being patched.
The file is now ``aurora_rayserver/_sitecustomize.py`` (leading underscore)
so CPython no longer auto-imports it — activation is explicit through
``apply_all()`` instead.

The current overlay tree is single-tier (functional + instrumentation mixed,
all-or-nothing under AURORA_RAY_SERVE_INSTRUMENTATION). Splitting the tree
into FUNCTIONAL (default-on) and INSTRUMENTATION (opt-in) is a follow-up
commit (commit 3 of the production_packaging plan). The classification
of each hunk lives in ``plan/production_packaging.md``.
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

RAY_PINNED_VERSION = "2.49.1"
RAY_PINNED_COMMIT = "c057f1ea836f3e93f110e895029caa32136fc156"

# Env-var rename for the overlay activation gate. AURORA_RAY_SERVE_INSTRUMENTATION
# is the new (semantically clearer) name. AURORA_INSTRUMENTATION is the legacy
# name and is read as a fallback so existing run scripts keep working.
OVERLAY_ENV_VAR = "AURORA_RAY_SERVE_INSTRUMENTATION"
OVERLAY_ENV_VAR_LEGACY = "AURORA_INSTRUMENTATION"


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
            f"[aurora_rayserver.patches] Ray version mismatch: "
            f"pinned={RAY_PINNED_VERSION} (commit {RAY_PINNED_COMMIT[:7]}), "
            f"runtime={actual_version} (commit {(actual_commit or '?')[:7]}). "
            f"Patches may break."
        )
        if strict:
            raise RuntimeError(msg)
        if os.environ.get("AURORA_PATCH_VERBOSE", "0") == "1":
            print(msg, flush=True)


def overlay_active() -> bool:
    """True when the Ray Serve overlay should be installed by the launcher.

    Reads AURORA_RAY_SERVE_INSTRUMENTATION first, falls back to AURORA_INSTRUMENTATION.
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
    ``aurora_rayserver._sitecustomize`` — its module-level ``_patch_*()``
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
        from aurora_rayserver import _sitecustomize  # noqa: F401
    except Exception as exc:
        if strict:
            raise
        if os.environ.get("AURORA_PATCH_VERBOSE", "0") == "1":
            print(
                f"[aurora_rayserver.patches] _sitecustomize import failed: {exc!r}",
                flush=True,
            )

    _apply_all_done = True
