"""The sole compatibility activation API (plan WP3.11/WP3.12, audit IMP-B04).

``CompatibilityActivator`` verifies the base environment against an immutable
profile, activates its role-filtered generated modules, checks each patch's
post-condition, and returns a typed receipt. It FAILS CLOSED: an unknown version, a missing
required patch, or a failed post-condition raises ``ActivationError`` rather
than continuing with a partially-patched process (the old ``apply_all()``
defaulted to ``strict=False`` and set its done-flag even after an import
failure).

Materialization is deliberately NOT done here; activation verifies the
pre-staged exact-hash overlay and imports only this process role's targets.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from types import MappingProxyType
from typing import Callable, Mapping, Optional

from .profile import CompatibilityProfile, ProfileMismatch, default_profile


class ActivationError(RuntimeError):
    """Compatibility activation failed; the process must not continue."""


@dataclass(frozen=True)
class ActivationReport:
    """Local activation proof, not a readiness receipt.

    A readiness receipt additionally needs an exact planned slot, allocation
    binding, component instance, and attestation authority.  Keeping those
    concepts separate prevents the old role-level report from accidentally
    certifying a fleet.
    """

    role: str
    profile_id: str
    patch_results: Mapping[str, bool]
    not_applicable: tuple[str, ...]
    capabilities: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.role, str) or not self.role:
            raise ActivationError("activation report role must be non-empty text")
        if not isinstance(self.profile_id, str) or not self.profile_id:
            raise ActivationError("activation report profile_id must be non-empty text")
        if not isinstance(self.patch_results, Mapping) or any(
            not isinstance(key, str) or not key or not isinstance(value, bool)
            for key, value in self.patch_results.items()
        ):
            raise ActivationError("activation report patch_results must be map<string,bool>")
        object.__setattr__(self, "patch_results", MappingProxyType(dict(self.patch_results)))
        for name in ("not_applicable", "capabilities"):
            values = getattr(self, name)
            if not isinstance(values, (tuple, list)) or any(
                not isinstance(value, str) or not value for value in values
            ):
                raise ActivationError(f"activation report {name} must contain non-empty strings")
            object.__setattr__(self, name, tuple(values))


# A post-condition proves the patch actually took effect in THIS process,
# rather than trusting that the patch function returned without raising.
_SENTINELS = {
    "SC-01": ("vllm.config.vllm", "get_layers_from_vllm_config", "_exaserve_pp_layer_filter_patch"),
    "SC-02": ("vllm.v1.worker.utils", "bind_kv_cache", "_exaserve_pp_kv_bind_patch"),
    "SC-03": ("vllm.forward_context", "create_forward_context", "_exaserve_layer_alias_patch"),
    "SC-04": ("vllm.attention.layer", "get_attention_context", "_exaserve_layer_context_patch"),
    "SC-05": (
        "vllm.v1.worker.gpu_model_runner",
        "GPUModelRunner",
        "_exaserve_pp_backend_lookup_patch",
    ),
    "SC-09": (
        "vllm.v1.executor.ray_executor",
        "RayDistributedExecutor",
        "_exaserve_xpu_channel_patch",
    ),
    "SC-10": (
        "vllm.v1.executor.ray_executor",
        "RayDistributedExecutor",
        "_exaserve_uncompiled_pp_patch",
    ),
    "EW-01": (
        "vllm.v1.executor.ray_executor",
        "RayDistributedExecutor",
        "_exaserve_worker_runtime_env_patch",
    ),
    "EW-02": (
        "vllm.v1.executor.multiproc_executor",
        "WorkerProc",
        "_exaserve_worker_identity_patch",
    ),
    "EW-03": (
        "vllm.v1.executor.ray_utils",
        "RayWorkerWrapper",
        "_exaserve_worker_identity_patch",
    ),
    "SC-11": (
        "ray._private.accelerators.intel_gpu",
        "IntelGPUAcceleratorManager",
        "_exaserve_generic_selector_patch",
    ),
    "SC-12": (
        "ray.experimental.channel.accelerator_context",
        "AcceleratorContext",
        "_exaserve_generic_selector_patch",
    ),
    "RS-02": (
        "ray._private.services",
        "start_ray_process",
        "_exaserve_raylet_fanout_patch",
    ),
}


def _postcondition_sitecustomize(patch_id: str) -> Optional[bool]:
    """Prove a patch took effect in THIS process.

    Tri-state on purpose:
      True  — sentinel present, the patch demonstrably applied here;
      False — target loaded but unpatched: fatal, the process is half-patched;
      None  — target module not imported in this process, so the patch neither
              applied nor was needed. Reported as NOT APPLICABLE rather than
              as applied, because a receipt must not claim what did not happen.
    """
    if patch_id == "RS-01":
        import sys

        module = sys.modules.get("ray.serve._private.constants")
        if module is not None:
            return bool(getattr(module, "_exaserve_serve_start_timeout_patch", False))
        return None
    if patch_id == "RS-03":
        import sys

        module = sys.modules.get("ray.serve._private.proxy_state")
        if module is None:
            return None
        target = getattr(module, "wrap_as_future", None)
        return bool(getattr(target, "_exaserve_proxy_timeout_patch", False))
    spec = _SENTINELS.get(patch_id)
    if spec is None:
        return True  # no in-process sentinel defined (shell/shim delivery)
    mod_name, attr, sentinel = spec
    import sys

    mod = sys.modules.get(mod_name)
    if mod is None:
        return None
    target = getattr(mod, attr, None)
    if target is None:
        return None
    if isinstance(target, type):
        # Class-hosted sentinel: the patch marks the replaced FUNCTION, which
        # the class may store wrapped (`staticmethod(fn)`). Attribute lookup on
        # a staticmethod object does not forward to the wrapped function, so
        # resolve through the descriptor and unwrap __func__ before checking —
        # reading vars() raw reported a correctly patched process as unpatched.
        for name in vars(target):
            candidate = getattr(target, name, None)
            candidate = getattr(candidate, "__func__", candidate)
            if getattr(candidate, sentinel, False):
                return True
        return bool(getattr(target, sentinel, False))
    return bool(getattr(target, sentinel, False))


class CompatibilityActivator:
    """Activate exactly one profile for this process role."""

    def __init__(
        self,
        profile: CompatibilityProfile | None = None,
        *,
        deployment_id: str = "",
        generation: int | None = None,
        vendor: str | None = None,
    ) -> None:
        self.profile = profile or default_profile(
            vendor or os.environ.get("EXASERVE_VENDOR", "xpu")
        )
        # NORMALIZED, never the raw env value: a raw PBS_JOBID carries a
        # ".aurora-pbs-..." suffix that the head strips, and a receipt built
        # from the raw string is rejected as "wrong deployment".
        from .collector import deployment_scope

        self.deployment_id = deployment_id or deployment_scope()
        self.generation = (
            int(os.environ.get("EXASERVE_GENERATION", "0") or 0)
            if generation is None
            else generation
        )
        self.report: ActivationReport | None = None

    # -- activation --------------------------------------------------------
    def activate(
        self,
        role: str,
        *,
        apply_fn: Callable[[], None] | None = None,
        postcondition: Callable[[str], Optional[bool]] | None = None,
        verify_environment: bool = True,
    ) -> ActivationReport:
        """Verify + apply + attest for ``role``. Raises on any failure."""
        declared_role = os.environ.get("EXASERVE_COMPAT_ROLE")
        if declared_role not in (None, "", role):
            raise ActivationError(
                f"process compatibility role {declared_role!r} cannot activate as {role!r}"
            )
        if verify_environment:
            observed = {}
            import platform
            from importlib import metadata

            observed["python"] = platform.python_version()
            for distribution, key in (("ray", "ray"), ("vllm", "vllm")):
                try:
                    # Distribution metadata proves the base environment without
                    # importing either version-sensitive package before the
                    # profile has been verified.
                    observed[key] = metadata.version(distribution)
                except metadata.PackageNotFoundError:
                    pass
            try:
                self.profile.verify_environment(observed)
                self.profile.verify_installed_sources()
            except ProfileMismatch as exc:
                raise ActivationError(str(exc)) from exc

        required = self.profile.required_patch_ids(role)
        generated_required = {
            patch.patch_id
            for patch in self.profile.patches
            if patch.patch_id in required and patch.delivery == "generated-overlay"
        }
        if generated_required and apply_fn is None and declared_role != role:
            raise ActivationError(
                f"role {role!r} requires generated overlay {sorted(generated_required)}, "
                "but EXASERVE_COMPAT_ROLE is not bound to this process"
            )
        try:
            if apply_fn is None:
                self._default_apply(required)
            else:
                apply_fn()
        except Exception as exc:  # IMP-B04: never swallow an activation failure
            raise ActivationError(
                f"compatibility activation failed for role {role!r}: {exc}"
            ) from exc

        check = postcondition or _postcondition_sitecustomize
        results: dict[str, bool] = {}
        for patch_id in required:
            try:
                verdict = check(patch_id)
            except Exception as exc:
                raise ActivationError(
                    f"post-condition for {patch_id} raised in role {role!r}: {exc}"
                ) from exc
            if verdict is None:
                raise ActivationError(
                    f"role {role!r}: required patch {patch_id!r} has no "
                    "in-process post-condition yet; activation cannot claim it"
                )
            if not isinstance(verdict, bool):
                raise ActivationError(
                    f"role {role!r}: post-condition for {patch_id!r} returned "
                    f"{type(verdict).__name__}, not a boolean"
                )
            results[patch_id] = verdict
        failed = [p for p, ok in results.items() if not ok]
        if failed:
            raise ActivationError(
                f"role {role!r}: required patch(es) {sorted(failed)} did not take "
                f"effect (profile {self.profile.name}). Refusing to continue with a "
                "partially-patched process."
            )

        self.report = ActivationReport(
            role=role,
            profile_id=self.profile.profile_id,
            patch_results=results,
            # Gated-out entries are resolved by the immutable manifest and are
            # not part of ``required``; expose them diagnostically only.
            not_applicable=self.profile.gated_out(role),
            capabilities=self.profile.capabilities(),
        )
        return self.report

    # -- default role-scoped activation ------------------------------------
    def _default_apply(self, patch_ids: tuple[str, ...]) -> None:
        from .generated_overlay import activate_patch_ids

        activate_patch_ids(self.profile, patch_ids)
        from exaserve.patches import apply_patch_ids

        runtime_ids = tuple(
            patch.patch_id
            for patch in self.profile.patches
            if patch.patch_id in patch_ids
            and patch.delivery in {"runtime-adapter", "sitecustomize"}
        )
        # IMP-B04: strict and role-scoped. An unknown manifest entry, import
        # failure, or missing semantic sentinel must raise.
        apply_patch_ids(runtime_ids, strict=True)
