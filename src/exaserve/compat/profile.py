"""Immutable compatibility profile (plan WP3.4/WP3.10, audit IMP-B04).

A profile pins the exact base environment (python/ray/vllm/vendor) and the
patch manifest that may be applied to it. Its ``profile_id`` is a SHA-256 over
the canonical normalized manifest plus base-environment identity, so any drift
in either produces a different id and therefore a receipt mismatch.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from importlib import metadata
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping

from .._version import __version__ as EXASERVE_VERSION

SCHEMA_VERSION = 1
_SHA256 = re.compile(r"^[0-9a-f]{64}$")

# Compatibility gates are part of the hash-bearing profile contract. Keep
# their names centralized so the plan projection, per-model Ray runtime_env,
# and head-side receipt validator cannot silently derive different required
# patch sets for the same logical engine slot.
PP_PATCH_GATE = "EXASERVE_VLLM_PATCH_PP_LAYER_FILTER"
RAY_WORKER_PATCH_GATE = "EXASERVE_VLLM_PATCH_RAY_WORKERS"
MULTIPROC_WORKER_PATCH_GATE = "EXASERVE_VLLM_PATCH_MULTIPROC_WORKERS"


class ProfileMismatch(RuntimeError):
    """The running environment does not match the declared profile."""


class _SourceTreeDistribution:
    """Distribution view for an immutable ExaServe source snapshot.

    Evaluation jobs deliberately import ExaServe from a content-addressed
    source snapshot.  Such a tree has package bytes and a canonical version,
    but no installed ``.dist-info`` directory.  Treating that supported
    deployment form as "not installed" made the compatibility gate reject the
    very artifact it was meant to verify.  This adapter supplies only the two
    pieces the verifier needs; all referenced files remain containment-checked
    and SHA-256 verified below.
    """

    version = EXASERVE_VERSION

    def __init__(self) -> None:
        self._root = Path(__file__).resolve().parents[2]

    def locate_file(self, relative: str) -> Path:
        return self._root / relative


def _distribution(distribution_name: str):
    try:
        return metadata.distribution(distribution_name)
    except metadata.PackageNotFoundError:
        if distribution_name == "exaserve":
            return _SourceTreeDistribution()
        raise


@dataclass(frozen=True)
class PatchSpec:
    """One manifest entry (plan WP3.10 field list)."""

    patch_id: str
    target: str  # module/symbol or env var patched
    classification: str  # configuration|vendor-compat|upstream-fix|instrumentation
    roles: tuple[str, ...]  # roles that MUST carry this patch
    delivery: str  # generated-overlay|generated-shim; legacy test profiles may use adapters
    capability: str  # capability name this patch produces
    required: bool = True  # a required patch that fails to apply is fatal
    upstream_ref: str = ""  # tracking/removal reference
    # Env var that REQUESTS this patch. Empty means unconditional. A patch
    # whose gate is off was never requested, so demanding proof that it applied
    # would fail closed on a correct configuration (e.g. the PP patches in a
    # tensor-parallel-only deployment). Head and replica read the same
    # environment, so both derive the same required set.
    env_gate: str = ""
    target_distribution: str = ""
    target_version: str = ""
    target_file: str = ""
    target_source_hash: str = ""
    affected_symbols: tuple[str, ...] = ()
    patch_artifact_path: str = ""
    patch_artifact_hash: str = ""
    delivery_artifact_path: str = ""
    delivery_artifact_hash: str = ""
    import_timing: str = ""
    semantic_probe: str = ""
    removal_ref: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.roles, (tuple, list)) or not self.roles:
            raise ProfileMismatch(f"patch {self.patch_id}: roles must be a non-empty sequence")
        if not isinstance(self.affected_symbols, (tuple, list)) or not self.affected_symbols:
            raise ProfileMismatch(
                f"patch {self.patch_id}: affected_symbols must be a non-empty sequence"
            )
        object.__setattr__(self, "roles", tuple(self.roles))
        object.__setattr__(self, "affected_symbols", tuple(self.affected_symbols))
        for name in (
            "patch_id",
            "target",
            "classification",
            "delivery",
            "target_distribution",
            "target_version",
            "target_file",
            "target_source_hash",
            "patch_artifact_path",
            "patch_artifact_hash",
            "delivery_artifact_path",
            "delivery_artifact_hash",
            "import_timing",
            "semantic_probe",
            "capability",
        ):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ProfileMismatch(f"patch.{name} must be a non-empty string")
        if self.classification not in {
            "configuration",
            "vendor-compat",
            "upstream-fix",
            "instrumentation",
            "generated-shim",
        }:
            raise ProfileMismatch(
                f"patch {self.patch_id}: classification {self.classification!r} invalid"
            )
        if self.delivery not in {
            "sitecustomize",
            "generated-overlay",
            "generated-shim",
            "public-api",
            "environment",
            "runtime-adapter",
        }:
            raise ProfileMismatch(f"patch {self.patch_id}: delivery {self.delivery!r} invalid")
        if not isinstance(self.required, bool):
            raise ProfileMismatch(f"patch {self.patch_id}: required must be boolean")
        for name in ("upstream_ref", "env_gate", "removal_ref"):
            if not isinstance(getattr(self, name), str):
                raise ProfileMismatch(f"patch.{name} must be text")
        if any(not isinstance(role, str) or not role for role in self.roles):
            raise ProfileMismatch(f"patch {self.patch_id}: roles must be non-empty")
        if len(self.roles) != len(set(self.roles)):
            raise ProfileMismatch(f"patch {self.patch_id}: roles must be unique")
        if any(not isinstance(symbol, str) or not symbol for symbol in self.affected_symbols):
            raise ProfileMismatch(f"patch {self.patch_id}: affected_symbols must be non-empty")
        if len(self.affected_symbols) != len(set(self.affected_symbols)):
            raise ProfileMismatch(f"patch {self.patch_id}: affected_symbols must be unique")
        if self.env_gate and not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", self.env_gate):
            raise ProfileMismatch(f"patch {self.patch_id}: env_gate is not an environment name")
        for name in (
            "target_source_hash",
            "patch_artifact_hash",
            "delivery_artifact_hash",
        ):
            if not _SHA256.fullmatch(getattr(self, name)):
                raise ProfileMismatch(f"patch {self.patch_id}: {name} must be lowercase SHA-256")


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
        "supervisor",
        "ray_head",
        "ray_worker",
        "replica",
        "engine_core",
        "engine_worker",
    )
    profile_id: str = ""

    def __post_init__(self) -> None:
        if isinstance(self.schema_version, bool) or self.schema_version != SCHEMA_VERSION:
            raise ProfileMismatch(f"compatibility schema {self.schema_version} != {SCHEMA_VERSION}")
        for name in ("name", "python", "ray", "vllm", "vendor"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ProfileMismatch(f"compatibility.{name} must be non-empty")
        if not isinstance(self.patches, (tuple, list)) or any(
            not isinstance(patch, PatchSpec) for patch in self.patches
        ):
            raise ProfileMismatch("compatibility.patches must contain PatchSpec values")
        if not isinstance(self.required_roles, (tuple, list)) or any(
            not isinstance(role, str) or not role for role in self.required_roles
        ):
            raise ProfileMismatch("compatibility.required_roles must contain non-empty strings")
        object.__setattr__(self, "patches", tuple(self.patches))
        object.__setattr__(self, "required_roles", tuple(self.required_roles))
        patch_ids = [patch.patch_id for patch in self.patches]
        if len(patch_ids) != len(set(patch_ids)):
            raise ProfileMismatch("compatibility manifest has duplicate patch IDs")
        if len(self.required_roles) != len(set(self.required_roles)):
            raise ProfileMismatch("compatibility.required_roles must be unique")
        if not isinstance(self.profile_id, str) or (
            self.profile_id and not _SHA256.fullmatch(self.profile_id)
        ):
            raise ProfileMismatch("compatibility.profile_id must be lowercase SHA-256")

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

    def required_patch_ids(
        self, role: str, env: Mapping[str, str] | None = None
    ) -> tuple[str, ...]:
        """Patches this role must prove, given the CURRENT patch gates."""
        import os as _os

        env = _os.environ if env is None else env
        return tuple(
            sorted(
                p.patch_id
                for p in self.patches
                if p.required and role in p.roles and (not p.env_gate or env.get(p.env_gate) == "1")
            )
        )

    def gated_out(self, role: str, env: Mapping[str, str] | None = None) -> tuple[str, ...]:
        import os as _os

        env = _os.environ if env is None else env
        return tuple(
            sorted(
                p.patch_id
                for p in self.patches
                if role in p.roles and p.env_gate and env.get(p.env_gate) != "1"
            )
        )

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

    def verify_installed_sources(self) -> None:
        """Verify exact dependency and patch bytes without importing targets."""
        checked_targets: set[tuple[str, str, str, str]] = set()
        checked_artifacts: set[tuple[str, str]] = set()
        from ..state.atomic import regular_file_reader

        def content_hash(path: Path, *, label: str) -> str:
            try:
                digest = hashlib.sha256()
                with regular_file_reader(path, binary=True) as handle:
                    for chunk in iter(lambda: handle.read(1 << 20), b""):
                        digest.update(chunk)
                return digest.hexdigest()
            except (OSError, ValueError) as exc:
                raise ProfileMismatch(f"could not read {label}: {exc}") from exc

        try:
            exaserve_dist = _distribution("exaserve")
        except metadata.PackageNotFoundError as exc:
            raise ProfileMismatch("required distribution 'exaserve' is not installed") from exc

        def distribution_file(distribution, relative: str, *, patch_id: str) -> Path:
            root = Path(distribution.locate_file("")).resolve()
            path = Path(distribution.locate_file(relative)).resolve()
            try:
                path.relative_to(root)
            except ValueError as exc:
                raise ProfileMismatch(
                    f"patch {patch_id}: distribution path escapes its installation root"
                ) from exc
            if not path.is_file():
                raise ProfileMismatch(f"patch {patch_id}: required file {relative!r} is missing")
            return path

        for patch in self.patches:
            target_key = (
                patch.target_distribution,
                patch.target_version,
                patch.target_file,
                patch.target_source_hash,
            )
            if target_key not in checked_targets:
                try:
                    distribution = _distribution(patch.target_distribution)
                except metadata.PackageNotFoundError as exc:
                    raise ProfileMismatch(
                        f"required distribution {patch.target_distribution!r} is not installed"
                    ) from exc
                if distribution.version != patch.target_version:
                    raise ProfileMismatch(
                        f"patch {patch.patch_id}: distribution "
                        f"{patch.target_distribution} version "
                        f"{distribution.version!r} != {patch.target_version!r}"
                    )
                target_path = distribution_file(
                    distribution, patch.target_file, patch_id=patch.patch_id
                )
                actual = content_hash(target_path, label=f"patch {patch.patch_id} target source")
                if actual != patch.target_source_hash:
                    raise ProfileMismatch(
                        f"patch {patch.patch_id}: target source hash mismatch "
                        f"for {patch.target_file}"
                    )
                checked_targets.add(target_key)

            for artifact_kind, relative, expected in (
                ("patch", patch.patch_artifact_path, patch.patch_artifact_hash),
                (
                    "delivery",
                    patch.delivery_artifact_path,
                    patch.delivery_artifact_hash,
                ),
            ):
                artifact_key = (relative, expected)
                if artifact_key in checked_artifacts:
                    continue
                artifact_path = distribution_file(exaserve_dist, relative, patch_id=patch.patch_id)
                actual = content_hash(
                    artifact_path, label=f"patch {patch.patch_id} {artifact_kind} artifact"
                )
                if actual != expected:
                    raise ProfileMismatch(
                        f"patch {patch.patch_id}: {artifact_kind} artifact hash mismatch "
                        f"for {relative}"
                    )
                checked_artifacts.add(artifact_key)


def _observed_versions() -> dict[str, str]:
    import platform

    observed = {"python": platform.python_version()}
    for distribution, key in (("ray", "ray"), ("vllm", "vllm")):
        try:
            observed[key] = metadata.version(distribution)
        except metadata.PackageNotFoundError:
            # verify_environment() turns an absent required version into a
            # named, fail-closed mismatch. Metadata avoids importing heavy
            # runtimes merely to discover their installed distribution.
            continue
    return observed


@lru_cache(maxsize=4)
def default_profile(vendor: str = "xpu") -> CompatibilityProfile:
    """The Aurora frameworks 2025.3.1 profile (ADR-003 selected set).

    Patch ids mirror ``doc/hardening/COMPATIBILITY_INVENTORY.md``.
    """
    package_dir = Path(__file__).resolve().parents[1]

    def _hash_file(path: Path) -> str:
        from ..state.atomic import regular_file_reader

        try:
            digest = hashlib.sha256()
            with regular_file_reader(path, binary=True) as handle:
                for chunk in iter(lambda: handle.read(1 << 20), b""):
                    digest.update(chunk)
            return digest.hexdigest()
        except (OSError, ValueError) as exc:
            raise ProfileMismatch(f"could not hash compatibility source {path}: {exc}") from exc

    artifact_hash = _hash_file(package_dir / "_sitecustomize.py")
    engine_artifact_hash = _hash_file(package_dir / "compat" / "engine_shim.py")
    overlay_artifact_hash = _hash_file(package_dir / "compat" / "generated_overlay.py")
    ray_start_artifact_hash = _hash_file(package_dir / "ray_start.py")
    # Qualified immutable source inventory. Profile construction must remain
    # possible in the portable planner/CI environment where Ray and vLLM are
    # intentionally absent; activation verifies these bytes on the target site.
    resolved_sources = {
        "SC-01": (
            "vllm",
            "vllm/config/vllm.py",
            "0.15.0+xpu",
            "940f2c526bbd3f1b7885d836c84388db88d30df54f20212c1c4a3e0da9bcf46a",
        ),
        "SC-02": (
            "vllm",
            "vllm/v1/worker/utils.py",
            "0.15.0+xpu",
            "38ad72193886062cc787474ae8ee2b1066e53e938716cfd56dca52fc58fe0a37",
        ),
        "SC-03": (
            "vllm",
            "vllm/forward_context.py",
            "0.15.0+xpu",
            "2d6d7d5bc92f44602432633c9d034731157b5c0db3aa2827e4c8714032d53bdb",
        ),
        "SC-04": (
            "vllm",
            "vllm/attention/layer.py",
            "0.15.0+xpu",
            "74f359628f762892ab9b28fc6b8d86ba1d04df1d3e4dbe957ee5a3c6cd290086",
        ),
        "SC-05": (
            "vllm",
            "vllm/v1/worker/gpu_model_runner.py",
            "0.15.0+xpu",
            "e8a73e1ccbfff37b880cd47ba9c115db572aef3c9d4d9eee50b18b729117f9bb",
        ),
        "SC-09": (
            "vllm",
            "vllm/v1/executor/ray_executor.py",
            "0.15.0+xpu",
            "7576bec19a3e686c0fef8b2e3d8380d6dcbe09fe72a8d20569a5e86687206a0f",
        ),
        "SC-10": (
            "vllm",
            "vllm/v1/executor/ray_executor.py",
            "0.15.0+xpu",
            "7576bec19a3e686c0fef8b2e3d8380d6dcbe09fe72a8d20569a5e86687206a0f",
        ),
        "EW-01": (
            "vllm",
            "vllm/v1/executor/ray_executor.py",
            "0.15.0+xpu",
            "7576bec19a3e686c0fef8b2e3d8380d6dcbe09fe72a8d20569a5e86687206a0f",
        ),
        "EW-02": (
            "vllm",
            "vllm/v1/executor/multiproc_executor.py",
            "0.15.0+xpu",
            "ea3d0115c1af30d75f3e44c604f413cd88fb6071fd1a3e1da46cbe043fd2c26c",
        ),
        "EW-03": (
            "vllm",
            "vllm/v1/executor/ray_utils.py",
            "0.15.0+xpu",
            "04c2b93ef48b60f5461868f805458df5b4e3ef4b867ba6685c3073ef0d7db9cd",
        ),
        "SC-11": (
            "ray",
            "ray/_private/accelerators/intel_gpu.py",
            "2.53.0",
            "dfcba05544e8818df1909a940fc365bae936fdbffa16250fdc970a560681b952",
        ),
        "SC-12": (
            "ray",
            "ray/experimental/channel/accelerator_context.py",
            "2.53.0",
            "c92c9c9b3b63bf8e4d4eaecf4e1b224059e1c932c98a207054232078292b9802",
        ),
        "RS-01": (
            "ray",
            "ray/serve/_private/constants.py",
            "2.53.0",
            "84c39b95bd3271cd718d22c6432451f963dda3c0b7548e486a8683e421cfebee",
        ),
        "RS-02": (
            "ray",
            "ray/_private/services.py",
            "2.53.0",
            "ae09f861986fcda28111f9a3417f27b4c5ad77a34a12ca05e8887a43cd6e6597",
        ),
        "RS-03": (
            "ray",
            "ray/serve/_private/proxy_state.py",
            "2.53.0",
            "eb0e30bb8eaf57b4e5ae48787c8654fd8d24ce9211382e81d38097750344ce06",
        ),
    }

    def _patch(
        patch_id: str,
        target: str,
        classification: str,
        capability: str,
        symbol: str,
        *,
        roles: tuple[str, ...] = ("replica",),
        removal_ref: str = "",
    ) -> PatchSpec:
        distribution, relative, version, source_hash = resolved_sources[patch_id]
        return PatchSpec(
            patch_id,
            target,
            classification,
            roles,
            "generated-overlay",
            capability,
            upstream_ref=removal_ref,
            env_gate=PP_PATCH_GATE,
            target_distribution=distribution,
            target_version=version,
            target_file=relative,
            target_source_hash=source_hash,
            affected_symbols=(symbol,),
            patch_artifact_path="exaserve/_sitecustomize.py",
            patch_artifact_hash=artifact_hash,
            delivery_artifact_path="exaserve/compat/generated_overlay.py",
            delivery_artifact_hash=overlay_artifact_hash,
            import_timing="after-target-module-import-before-runtime-construction",
            semantic_probe=f"sentinel:{patch_id}",
            removal_ref=removal_ref,
        )

    patches = (
        _patch(
            "SC-01",
            "vllm.config.vllm.get_layers_from_vllm_config",
            "upstream-fix",
            "pp_layer_alias",
            "get_layers_from_vllm_config",
            roles=("replica", "engine_core", "engine_worker"),
        ),
        _patch(
            "SC-02",
            "vllm.v1.worker.utils.bind_kv_cache",
            "upstream-fix",
            "pp_kv_bind",
            "bind_kv_cache",
            roles=("replica", "engine_worker"),
        ),
        _patch(
            "SC-03",
            "vllm.forward_context.create_forward_context",
            "upstream-fix",
            "pp_forward_ctx",
            "create_forward_context",
            roles=("replica", "engine_worker"),
        ),
        _patch(
            "SC-04",
            "vllm.attention.layer.get_attention_context",
            "upstream-fix",
            "pp_attn_ctx",
            "get_attention_context",
            roles=("replica", "engine_worker"),
        ),
        _patch(
            "SC-05",
            "vllm.v1.worker.gpu_model_runner.GPUModelRunner",
            "upstream-fix",
            "pp_backend_lookup",
            "initialize_attn_backend",
            roles=("replica", "engine_worker"),
        ),
        _patch(
            "SC-09",
            "vllm.v1.executor.ray_executor._init_executor",
            "vendor-compat",
            "xpu_channel_type",
            "_init_executor",
            roles=("replica", "engine_core"),
        ),
        _patch(
            "SC-10",
            "vllm.v1.executor.ray_executor._execute_dag",
            "vendor-compat",
            "xpu_uncompiled_pp",
            "_execute_dag",
            roles=("replica", "engine_core"),
        ),
        PatchSpec(
            "EW-01",
            "vllm.v1.executor.ray_executor.RayDistributedExecutor._init_workers_ray",
            "vendor-compat",
            ("engine_core",),
            "generated-overlay",
            "engine_worker_pre_interpreter_bootstrap",
            env_gate=RAY_WORKER_PATCH_GATE,
            target_distribution=resolved_sources["EW-01"][0],
            target_version=resolved_sources["EW-01"][2],
            target_file=resolved_sources["EW-01"][1],
            target_source_hash=resolved_sources["EW-01"][3],
            affected_symbols=("RayDistributedExecutor._init_workers_ray",),
            patch_artifact_path="exaserve/_sitecustomize.py",
            patch_artifact_hash=artifact_hash,
            delivery_artifact_path="exaserve/compat/generated_overlay.py",
            delivery_artifact_hash=overlay_artifact_hash,
            import_timing="before-vllm-ray-worker-actor-construction",
            semantic_probe="sentinel:EW-01",
            removal_ref="remove-when-vllm-exposes-worker-runtime-env-hook",
        ),
        PatchSpec(
            "EW-02",
            "vllm.v1.executor.multiproc_executor.WorkerProc.make_worker_process",
            "vendor-compat",
            ("engine_core",),
            "generated-overlay",
            "engine_multiproc_worker_identity",
            env_gate=MULTIPROC_WORKER_PATCH_GATE,
            target_distribution=resolved_sources["EW-02"][0],
            target_version=resolved_sources["EW-02"][2],
            target_file=resolved_sources["EW-02"][1],
            target_source_hash=resolved_sources["EW-02"][3],
            affected_symbols=("WorkerProc.make_worker_process",),
            patch_artifact_path="exaserve/_sitecustomize.py",
            patch_artifact_hash=artifact_hash,
            delivery_artifact_path="exaserve/compat/generated_overlay.py",
            delivery_artifact_hash=overlay_artifact_hash,
            import_timing="before-vllm-multiprocessing-worker-construction",
            semantic_probe="sentinel:EW-02",
            removal_ref="remove-when-vllm-exposes-worker-identity-bootstrap",
        ),
        PatchSpec(
            "EW-03",
            "vllm.v1.executor.ray_utils.RayWorkerWrapper",
            "vendor-compat",
            ("engine_core",),
            "generated-overlay",
            "engine_ray_worker_identity",
            env_gate=RAY_WORKER_PATCH_GATE,
            target_distribution=resolved_sources["EW-03"][0],
            target_version=resolved_sources["EW-03"][2],
            target_file=resolved_sources["EW-03"][1],
            target_source_hash=resolved_sources["EW-03"][3],
            affected_symbols=("RayWorkerWrapper.__init__", "RayWorkerWrapper.adjust_rank"),
            patch_artifact_path="exaserve/_sitecustomize.py",
            patch_artifact_hash=artifact_hash,
            delivery_artifact_path="exaserve/compat/generated_overlay.py",
            delivery_artifact_hash=overlay_artifact_hash,
            import_timing="before-vllm-Ray-worker-actor-construction",
            semantic_probe="sentinel:EW-03",
            removal_ref="remove-when-vllm-exposes-worker-identity-bootstrap",
        ),
        _patch(
            "SC-11",
            "ray IntelGPUAcceleratorManager",
            "vendor-compat",
            "xpu_selector",
            "get_current_process_visible_accelerator_ids",
            roles=("replica", "engine_worker"),
        ),
        _patch(
            "SC-12",
            "ray AcceleratorContext.get_accelerator_devices",
            "vendor-compat",
            "xpu_accel_ctx",
            "get_accelerator_devices",
            roles=("replica", "engine_worker"),
        ),
        PatchSpec(
            "RS-01",
            "ray.serve._private.constants.HTTP_PROXY_TIMEOUT",
            "configuration",
            ("deployment",),
            "generated-overlay",
            "serve_start_proxy_timeout",
            target_distribution=resolved_sources["RS-01"][0],
            target_version=resolved_sources["RS-01"][2],
            target_file=resolved_sources["RS-01"][1],
            target_source_hash=resolved_sources["RS-01"][3],
            affected_symbols=("HTTP_PROXY_TIMEOUT",),
            patch_artifact_path="exaserve/_sitecustomize.py",
            patch_artifact_hash=artifact_hash,
            delivery_artifact_path="exaserve/compat/generated_overlay.py",
            delivery_artifact_hash=overlay_artifact_hash,
            import_timing="before-ray-serve-constants-first-import",
            semantic_probe="sentinel:RS-01",
            removal_ref="remove-when-Ray-exposes-Serve-start-timeout",
        ),
        PatchSpec(
            "RS-02",
            "ray._private.services.start_ray_process",
            "vendor-compat",
            ("ray_head", "ray_worker"),
            "generated-overlay",
            "raylet_startup_fanout",
            target_distribution=resolved_sources["RS-02"][0],
            target_version=resolved_sources["RS-02"][2],
            target_file=resolved_sources["RS-02"][1],
            target_source_hash=resolved_sources["RS-02"][3],
            affected_symbols=("services.start_ray_process",),
            patch_artifact_path="exaserve/ray_start.py",
            patch_artifact_hash=ray_start_artifact_hash,
            delivery_artifact_path="exaserve/compat/generated_overlay.py",
            delivery_artifact_hash=overlay_artifact_hash,
            import_timing="after-profile-verification-before-first-Ray-import",
            semantic_probe="sentinel:RS-02",
            removal_ref="remove-when-Ray-exposes-raylet-fanout-CLI-options",
        ),
        PatchSpec(
            "RS-03",
            "ray.serve._private.proxy_state.wrap_as_future",
            "upstream-fix",
            ("deployment",),
            "generated-overlay",
            "serve_proxy_timeout_future_isolation",
            target_distribution=resolved_sources["RS-03"][0],
            target_version=resolved_sources["RS-03"][2],
            target_file=resolved_sources["RS-03"][1],
            target_source_hash=resolved_sources["RS-03"][3],
            affected_symbols=("wrap_as_future",),
            patch_artifact_path="exaserve/_sitecustomize.py",
            patch_artifact_hash=artifact_hash,
            delivery_artifact_path="exaserve/compat/generated_overlay.py",
            delivery_artifact_hash=overlay_artifact_hash,
            import_timing="before-Ray-Serve-controller-proxy-checks",
            semantic_probe="sentinel:RS-03",
            removal_ref="remove-when-Ray-wrap_as_future-does-not-complete-chained-future-on-timeout",
        ),
        PatchSpec(
            "EN-01",
            "spawned EngineCore sitecustomize shim",
            "generated-shim",
            ("engine_core", "engine_worker"),
            "generated-shim",
            "engine_spawn_reach",
            target_distribution="exaserve",
            target_version=EXASERVE_VERSION,
            target_file="exaserve/compat/engine_shim.py",
            target_source_hash=engine_artifact_hash,
            affected_symbols=("write_engine_shim",),
            patch_artifact_path="exaserve/compat/engine_shim.py",
            patch_artifact_hash=engine_artifact_hash,
            delivery_artifact_path="exaserve/compat/engine_shim.py",
            delivery_artifact_hash=engine_artifact_hash,
            import_timing="interpreter-site-initialization-before-engine-import",
            semantic_probe="engine-self-receipt-over-local-ingress",
            removal_ref="remove-when-engine-offers-pre-import-hook",
        ),
    )
    # This function is part of pre-import plan compilation/activation.  It must
    # be pure metadata and must not import Ray or vLLM merely to discover the
    # versions that the profile itself pins.  ``verify_environment`` performs
    # live discovery only after the profile has been selected and activated.
    profile = CompatibilityProfile(
        schema_version=SCHEMA_VERSION,
        name=f"aurora-frameworks-2025.3.1-{vendor}",
        python="3.12.12",
        ray="2.53.0",
        vllm="0.15.0+xpu",
        vendor=vendor,
        patches=patches,
    )
    object.__setattr__(profile, "profile_id", profile.compute_id())
    return profile
