"""The sole compatibility activation API (plan WP3.11/WP3.12, audit IMP-B04).

``CompatibilityActivator`` verifies the base environment against an immutable
profile, applies the profile's patches, checks each patch's post-condition,
and returns a typed receipt. It FAILS CLOSED: an unknown version, a missing
required patch, or a failed post-condition raises ``ActivationError`` rather
than continuing with a partially-patched process (the old ``apply_all()``
defaulted to ``strict=False`` and set its done-flag even after an import
failure).

Materialization (building an environment/overlay) is deliberately NOT done
here; activation is read-only verification + in-process application.
"""

from __future__ import annotations

import os
from typing import Callable, Optional

from .profile import CompatibilityProfile, ProfileMismatch, default_profile
from .receipt import CompatibilityReceipt, build_receipt


class ActivationError(RuntimeError):
    """Compatibility activation failed; the process must not continue."""


# A post-condition proves the patch actually took effect in THIS process,
# rather than trusting that the patch function returned without raising.
_SENTINELS = {
    "SC-01": ("vllm.config.vllm", "get_layers_from_vllm_config",
              "_exaserve_pp_layer_filter_patch"),
    "SC-02": ("vllm.v1.worker.utils", "bind_kv_cache", "_exaserve_pp_kv_bind_patch"),
    "SC-03": ("vllm.forward_context", "create_forward_context",
              "_exaserve_layer_alias_patch"),
    "SC-04": ("vllm.attention.layer", "get_attention_context",
              "_exaserve_layer_context_patch"),
    "SC-05": ("vllm.v1.worker.gpu_model_runner", "GPUModelRunner",
              "_exaserve_pp_backend_lookup_patch"),
    "SC-09": ("vllm.v1.executor.ray_executor", "RayDistributedExecutor",
              "_exaserve_xpu_channel_patch"),
    "SC-10": ("vllm.v1.executor.ray_executor", "RayDistributedExecutor",
              "_exaserve_uncompiled_pp_patch"),
    "SC-11": ("ray._private.accelerators.intel_gpu", "IntelGPUAcceleratorManager",
              "_exaserve_generic_selector_patch"),
    "SC-12": ("ray.experimental.channel.accelerator_context", "AcceleratorContext",
              "_exaserve_generic_selector_patch"),
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

    def __init__(self, profile: CompatibilityProfile | None = None, *,
                 deployment_id: str = "", generation: int = 0,
                 vendor: str | None = None) -> None:
        self.profile = profile or default_profile(
            vendor or os.environ.get("EXASERVE_VENDOR", "xpu"))
        # NORMALIZED, never the raw env value: a raw PBS_JOBID carries a
        # ".aurora-pbs-..." suffix that the head strips, and a receipt built
        # from the raw string is rejected as "wrong deployment".
        from .collector import deployment_scope

        self.deployment_id = deployment_id or deployment_scope()
        self.generation = generation or int(
            os.environ.get("EXASERVE_GENERATION", "0") or 0)
        self.receipt: CompatibilityReceipt | None = None

    # -- activation --------------------------------------------------------
    def activate(self, role: str, *,
                 apply_fn: Callable[[], None] | None = None,
                 postcondition: Callable[[str], Optional[bool]] | None = None,
                 verify_environment: bool = True) -> CompatibilityReceipt:
        """Verify + apply + attest for ``role``. Raises on any failure."""
        if verify_environment:
            observed = {}
            import platform

            observed["python"] = platform.python_version()
            for mod in ("ray", "vllm"):
                try:
                    observed[mod] = __import__(mod).__version__
                except Exception:
                    pass
            try:
                self.profile.verify_environment(observed)
            except ProfileMismatch as exc:
                raise ActivationError(str(exc)) from exc

        required = self.profile.required_patch_ids(role)
        if apply_fn is None:
            apply_fn = self._default_apply
        try:
            apply_fn()
        except Exception as exc:  # IMP-B04: never swallow an activation failure
            raise ActivationError(
                f"compatibility activation failed for role {role!r}: {exc}") from exc

        check = postcondition or _postcondition_sitecustomize
        results: dict[str, bool] = {}
        not_applicable: list[str] = []
        for patch_id in required:
            try:
                verdict = check(patch_id)
            except Exception as exc:
                raise ActivationError(
                    f"post-condition for {patch_id} raised in role {role!r}: {exc}"
                ) from exc
            if verdict is None:
                not_applicable.append(patch_id)
            else:
                results[patch_id] = bool(verdict)
        failed = [p for p, ok in results.items() if not ok]
        if failed:
            raise ActivationError(
                f"role {role!r}: required patch(es) {sorted(failed)} did not take "
                f"effect (profile {self.profile.name}). Refusing to continue with a "
                "partially-patched process."
            )

        self.receipt = build_receipt(
            profile=self.profile, role=role, deployment_id=self.deployment_id,
            generation=self.generation, patch_results=results, attestation="self",
            not_applicable=tuple(not_applicable))
        return self.receipt

    def attest_external(self, role: str, *, executable: str,
                        version_probe: str = "") -> CompatibilityReceipt:
        """Supervisor-side attestation for an UNMODIFIED external daemon.

        Plan §3.1: unmodified daemons are attested by their owning supervisor
        (executable + prepared environment + version probe); they are never
        described as self-reporting ExaServe code.
        """
        # An external attestation cannot prove a patch took effect INSIDE the
        # daemon, so it declares the required set as not-provable-here rather
        # than reporting it applied. The readiness snapshot records which roles
        # rest on this weaker evidence.
        return build_receipt(
            profile=self.profile, role=role, deployment_id=self.deployment_id,
            generation=self.generation, patch_results={}, attestation="supervisor",
            versions={"executable": executable, "probe": version_probe},
            not_applicable=self.profile.required_patch_ids(role))

    # -- default in-process application ------------------------------------
    @staticmethod
    def _default_apply() -> None:
        from exaserve.patches import apply_all

        # IMP-B04: strict — a version mismatch or import failure must raise.
        apply_all(strict=True)
