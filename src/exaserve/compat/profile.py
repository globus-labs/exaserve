"""Immutable compatibility profile (plan WP3.4/WP3.10, audit IMP-B04).

A profile pins the exact base environment (python/ray/vllm/vendor) and the
patch manifest that may be applied to it. Its ``profile_id`` is a SHA-256 over
the canonical normalized manifest plus base-environment identity, so any drift
in either produces a different id and therefore a receipt mismatch.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any, Mapping

SCHEMA_VERSION = 1


class ProfileMismatch(RuntimeError):
    """The running environment does not match the declared profile."""


@dataclass(frozen=True)
class PatchSpec:
    """One manifest entry (plan WP3.10 field list)."""

    patch_id: str
    target: str                 # module/symbol or env var patched
    classification: str         # configuration|vendor-compat|upstream-fix|instrumentation
    roles: tuple[str, ...]      # roles that MUST carry this patch
    delivery: str               # sitecustomize|overlay|direct|shell-env|generated-shim
    capability: str             # capability name this patch produces
    required: bool = True       # a required patch that fails to apply is fatal
    upstream_ref: str = ""      # tracking/removal reference
    # Env var that REQUESTS this patch. Empty means unconditional. A patch
    # whose gate is off was never requested, so demanding proof that it applied
    # would fail closed on a correct configuration (e.g. the PP patches in a
    # tensor-parallel-only deployment). Head and replica read the same
    # environment, so both derive the same required set.
    env_gate: str = ""


@dataclass(frozen=True)
class CompatibilityProfile:
    schema_version: int
    name: str
    python: str
    ray: str
    vllm: str
    vendor: str
    patches: tuple[PatchSpec, ...]
    # Roles that must publish a receipt before READY (plan WP3.9).
    required_roles: tuple[str, ...] = (
        "supervisor", "ray_head", "ray_worker", "replica", "engine",
    )
    profile_id: str = ""

    # -- identity ----------------------------------------------------------
    def canonical(self) -> dict[str, Any]:
        data = asdict(self)
        data.pop("profile_id", None)
        return data

    def compute_id(self) -> str:
        blob = json.dumps(self.canonical(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(blob.encode()).hexdigest()

    def capabilities(self) -> tuple[str, ...]:
        return tuple(sorted({p.capability for p in self.patches if p.capability}))

    def required_patch_ids(self, role: str,
                           env: Mapping[str, str] | None = None) -> tuple[str, ...]:
        """Patches this role must prove, given the CURRENT patch gates."""
        import os as _os

        env = _os.environ if env is None else env
        return tuple(sorted(
            p.patch_id for p in self.patches
            if p.required and role in p.roles
            and (not p.env_gate or env.get(p.env_gate) == "1")))

    def gated_out(self, role: str, env: Mapping[str, str] | None = None) -> tuple[str, ...]:
        import os as _os

        env = _os.environ if env is None else env
        return tuple(sorted(
            p.patch_id for p in self.patches
            if role in p.roles and p.env_gate and env.get(p.env_gate) != "1"))

    # -- verification ------------------------------------------------------
    def verify_environment(self, observed: Mapping[str, str]) -> None:
        """Raise ProfileMismatch unless the live versions match exactly.

        IMP-B04: this is the fail-closed gate the old ``_check_ray_version``
        (warn-only, ``strict=False`` default) never was.
        """
        mismatches = []
        for key in ("python", "ray", "vllm"):
            want = getattr(self, key)
            got = observed.get(key)
            if want and got and want != got:
                mismatches.append(f"{key}: profile={want} runtime={got}")
            elif want and got is None:
                mismatches.append(f"{key}: profile={want} runtime=<unavailable>")
        if mismatches:
            raise ProfileMismatch(
                f"environment does not match profile {self.name!r}: "
                + "; ".join(mismatches)
                + ". Pin the supported versions (doc/hardening/"
                "COMPATIBILITY_MATRIX.md) or declare a new profile."
            )


def _observed_versions() -> dict[str, str]:
    import platform

    observed = {"python": platform.python_version()}
    for mod, key in (("ray", "ray"), ("vllm", "vllm")):
        try:
            observed[key] = __import__(mod).__version__
        except Exception:
            pass
    return observed


def default_profile(vendor: str = "xpu") -> CompatibilityProfile:
    """The Aurora frameworks 2025.3.1 profile (ADR-003 selected set).

    Patch ids mirror ``doc/hardening/COMPATIBILITY_INVENTORY.md``.
    """
    patches = (
        PatchSpec("SC-01", "vllm.config.get_layers_from_vllm_config", "upstream-fix",
                  ("replica", "engine"), "sitecustomize", "pp_layer_alias",
                  env_gate="EXASERVE_VLLM_PATCH_PP_LAYER_FILTER"),
        PatchSpec("SC-02", "vllm.v1.worker.utils.bind_kv_cache", "upstream-fix",
                  ("replica", "engine"), "sitecustomize", "pp_kv_bind",
                  env_gate="EXASERVE_VLLM_PATCH_PP_LAYER_FILTER"),
        PatchSpec("SC-03", "vllm.forward_context.create_forward_context", "upstream-fix",
                  ("replica", "engine"), "sitecustomize", "pp_forward_ctx",
                  env_gate="EXASERVE_VLLM_PATCH_PP_LAYER_FILTER"),
        PatchSpec("SC-04", "vllm.attention.layer.get_attention_context", "upstream-fix",
                  ("replica", "engine"), "sitecustomize", "pp_attn_ctx",
                  env_gate="EXASERVE_VLLM_PATCH_PP_LAYER_FILTER"),
        PatchSpec("SC-05", "vllm.v1.worker.gpu_model_runner.initialize_attn_backend",
                  "upstream-fix", ("replica", "engine"), "sitecustomize",
                  "pp_backend_lookup",
                  env_gate="EXASERVE_VLLM_PATCH_PP_LAYER_FILTER"),
        PatchSpec("SC-09", "vllm RayDistributedExecutor._init_executor", "vendor-compat",
                  ("replica", "engine"), "sitecustomize", "xpu_channel_type",
                  env_gate="EXASERVE_VLLM_PATCH_PP_LAYER_FILTER"),
        PatchSpec("SC-10", "vllm RayDistributedExecutor._execute_dag", "vendor-compat",
                  ("replica", "engine"), "sitecustomize", "xpu_uncompiled_pp",
                  env_gate="EXASERVE_VLLM_PATCH_PP_LAYER_FILTER"),
        PatchSpec("SC-11", "ray IntelGPUAcceleratorManager", "vendor-compat",
                  ("replica", "engine"), "sitecustomize", "xpu_selector",
                  env_gate="EXASERVE_VLLM_PATCH_PP_LAYER_FILTER"),
        PatchSpec("SC-12", "ray AcceleratorContext.get_accelerator_devices",
                  "vendor-compat", ("replica", "engine"), "sitecustomize",
                  "xpu_accel_ctx",
                  env_gate="EXASERVE_VLLM_PATCH_PP_LAYER_FILTER"),
        PatchSpec("EN-01", "spawned EngineCore sitecustomize shim", "vendor-compat",
                  ("engine",), "generated-shim", "engine_spawn_reach"),
    )
    observed = _observed_versions()
    profile = CompatibilityProfile(
        schema_version=SCHEMA_VERSION,
        name=f"aurora-frameworks-2025.3.1-{vendor}",
        python=observed.get("python", "3.12.12"),
        ray="2.53.0",
        vllm="0.15.0",
        vendor=vendor,
        patches=patches,
    )
    object.__setattr__(profile, "profile_id", profile.compute_id())
    return profile
