"""Install reviewed vLLM model-info cache entries before Ray imports vLLM.

Aurora's pinned XPU build can SIGSEGV in vLLM's cache-miss architecture
inspection subprocess before an EngineWorker constructor becomes observable.
The supported model classes are deterministic properties of the exact vLLM
source files, so the release carries their reviewed ``_ModelInfo`` results.

This adapter is intentionally import-light: it verifies distribution metadata
and source bytes without importing vLLM, and it reads seed bytes only from the
already-verified node-local ExaServe capsule.  The destination is the closed,
generation-local ``VLLM_CACHE_ROOT`` inherited by Ray and Serve actors.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from importlib import metadata
import json
import os
from pathlib import Path
import re
import stat
from typing import Any, Mapping

from .compat.profile import (
    CompatibilityProfile,
    VLLMModelInfoSeedSpec,
    VLLMModelInfoSupportSourceSpec,
)
from .model_staging import ensure_node_local_directory
from .plan.runtime_environment import (
    LOCAL_RUNTIME_ROOT_ENV,
    LOCAL_STATE_ROOT_ENV,
    RuntimePathError,
    require_contained_local_path,
    require_non_shared_path,
)
from .state.atomic import (
    atomic_create_or_verify_bytes,
    regular_file_reader,
    strict_json_loads,
)


PATCH_ID = "VC-01"
CAPABILITY = "vllm_modelinfo_cache_seed"
MANIFEST_RELATIVE_PATH = Path("exaserve/resources/vllm_modelinfo/manifest.json")
EXPECTED_MANIFEST_SHA256 = "c5e82475960ee3094a19cbda25b25264cbecde472ef9f34adedf59380b73d18c"
NOT_REQUIRED_INSTALL_REPORT_HASH = hashlib.sha256(
    b'{"required":false,"schema_version":1}'
).hexdigest()

_SHA256 = re.compile(r"[0-9a-f]{64}")
_VLLM_CACHE_HASH = re.compile(r"[0-9a-f]{32}")
_MAX_ARTIFACT_BYTES = 1 << 20
_MANIFEST_FIELDS = {"schema_version", "vllm_version", "support_sources", "seeds"}
_SUPPORT_SOURCE_FIELDS = {"target_file", "target_sha256"}
_SEED_FIELDS = {
    "architecture",
    "target_module",
    "target_file",
    "target_sha256",
    "vllm_cache_hash_algorithm",
    "vllm_cache_hash",
    "seed_filename",
    "seed_sha256",
    "destination_filename",
}
_MODELINFO_FIELDS = {
    "architecture",
    "is_text_generation_model",
    "is_pooling_model",
    "attn_type",
    "default_seq_pooling_type",
    "default_tok_pooling_type",
    "supports_cross_encoding",
    "supports_multimodal",
    "supports_multimodal_raw_input_only",
    "requires_raw_input_tokens",
    "supports_multimodal_encoder_tp_data",
    "supports_pp",
    "has_inner_state",
    "is_attention_free",
    "is_hybrid",
    "has_noops",
    "supports_mamba_prefix_caching",
    "supports_transcription",
    "supports_transcription_only",
}
_MODELINFO_BOOLEAN_FIELDS = _MODELINFO_FIELDS - {
    "architecture",
    "attn_type",
    "default_seq_pooling_type",
    "default_tok_pooling_type",
}
_SUPPORTED_LAYOUT = {
    "GptOssForCausalLM": (
        "vllm.model_executor.models.gpt_oss",
        "vllm/model_executor/models/gpt_oss.py",
        "vllm-model_executor-models-gpt_oss-GptOssForCausalLM.json",
    ),
    "LlamaForCausalLM": (
        "vllm.model_executor.models.llama",
        "vllm/model_executor/models/llama.py",
        "vllm-model_executor-models-llama-LlamaForCausalLM.json",
    ),
}
SUPPORTED_ARCHITECTURES = frozenset(_SUPPORTED_LAYOUT)


class VLLMModelInfoSeedError(RuntimeError):
    """The reviewed cache bundle or its installation failed validation."""


@dataclass(frozen=True)
class ModelInfoSeedBundle:
    vllm_version: str
    support_sources: tuple[VLLMModelInfoSupportSourceSpec, ...]
    seeds: tuple[VLLMModelInfoSeedSpec, ...]


@dataclass(frozen=True)
class ModelInfoSeedInstallReport:
    vllm_version: str
    architectures: tuple[str, ...]
    created: tuple[str, ...]
    report_hash: str


def _install_report_hash(
    *,
    vllm_version: str,
    manifest_sha256: str,
    seeds: tuple[VLLMModelInfoSeedSpec, ...],
) -> str:
    return hashlib.sha256(
        json.dumps(
            {
                "schema_version": 1,
                "vllm_version": vllm_version,
                "manifest_sha256": manifest_sha256,
                "entries": [
                    {
                        "architecture": seed.architecture,
                        "destination_filename": seed.destination_filename,
                        "seed_sha256": seed.seed_sha256,
                    }
                    for seed in sorted(seeds, key=lambda item: item.architecture)
                ],
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def expected_install_report_hash(profile: CompatibilityProfile) -> str:
    """Return the profile-bound report identity without touching a cache."""

    if not profile.vllm_modelinfo_seeds:
        raise VLLMModelInfoSeedError("CompatibilityProfile has no VC-01 seed contract")
    return _install_report_hash(
        vllm_version=profile.vllm,
        manifest_sha256=profile.vllm_modelinfo_seed_manifest_hash,
        seeds=profile.vllm_modelinfo_seeds,
    )


def expected_source_seed_evidence(
    profile: CompatibilityProfile,
    *,
    install_required: bool,
) -> dict[str, str | int]:
    """Resolve the exact schema-3 evidence expected for one DeploymentPlan."""

    if type(install_required) is not bool:
        raise TypeError("install_required must be a boolean")
    if not profile.vllm_modelinfo_seeds:
        raise VLLMModelInfoSeedError("CompatibilityProfile has no VC-01 seed contract")
    count = len(profile.vllm_modelinfo_seeds)
    return {
        "source_evidence_schema_version": 2,
        "vllm_modelinfo_seed_manifest_hash": profile.vllm_modelinfo_seed_manifest_hash,
        "vllm_modelinfo_seed_count": count,
        "vllm_modelinfo_seed_install_report_hash": (
            expected_install_report_hash(profile)
            if install_required
            else NOT_REQUIRED_INSTALL_REPORT_HASH
        ),
        "vllm_modelinfo_seed_installed_count": count if install_required else 0,
    }


def _exact_object(value: object, fields: set[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise VLLMModelInfoSeedError(f"{label} must contain exactly {sorted(fields)}")
    if any(not isinstance(key, str) for key in value):
        raise VLLMModelInfoSeedError(f"{label} keys must be text")
    return value


def _regular_bytes(
    path: Path,
    *,
    label: str,
    require_private_owner: bool = False,
) -> bytes:
    try:
        with regular_file_reader(path, binary=True) as handle:
            opened = os.fstat(handle.fileno())
            if require_private_owner and (
                opened.st_uid != os.getuid() or stat.S_IMODE(opened.st_mode) & 0o022
            ):
                raise VLLMModelInfoSeedError(
                    f"{label} must be owned by this uid and not group/world-writable"
                )
            payload = handle.read(_MAX_ARTIFACT_BYTES + 1)
    except VLLMModelInfoSeedError:
        raise
    except (OSError, ValueError) as exc:
        raise VLLMModelInfoSeedError(f"{label} is not a readable regular file: {exc}") from exc
    if len(payload) > _MAX_ARTIFACT_BYTES:
        raise VLLMModelInfoSeedError(f"{label} exceeds {_MAX_ARTIFACT_BYTES} bytes")
    return payload


def _verify_private_directory(path: Path, *, label: str) -> None:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise VLLMModelInfoSeedError(f"{label} is not a readable directory: {exc}") from exc
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(opened.st_mode)
            or opened.st_uid != os.getuid()
            or stat.S_IMODE(opened.st_mode) & 0o022
        ):
            raise VLLMModelInfoSeedError(
                f"{label} must be owned by this uid and not group/world-writable"
            )
    finally:
        os.close(descriptor)


def _contained_file(path: Path, root: Path, *, label: str) -> Path:
    try:
        require_contained_local_path(
            path,
            root,
            name=label,
            require_exists=True,
        )
    except RuntimePathError as exc:
        raise VLLMModelInfoSeedError(str(exc)) from exc
    return path


def _lower_hash(value: object, pattern: re.Pattern[str], *, label: str) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise VLLMModelInfoSeedError(f"{label} has an invalid digest")
    return value


def _plain_filename(value: object, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value in {".", ".."}
        or Path(value).name != value
        or "\x00" in value
    ):
        raise VLLMModelInfoSeedError(f"{label} must be one plain filename")
    return value


def _relative_target_file(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise VLLMModelInfoSeedError(f"{label} must be a non-empty relative path")
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or path.as_posix() != value:
        raise VLLMModelInfoSeedError(f"{label} must be a normalized relative path")
    return value


def _parse_support_source(value: object, *, index: int) -> VLLMModelInfoSupportSourceSpec:
    row = _exact_object(
        value,
        _SUPPORT_SOURCE_FIELDS,
        f"manifest.support_sources[{index}]",
    )
    return VLLMModelInfoSupportSourceSpec(
        target_file=_relative_target_file(
            row["target_file"],
            label=f"manifest.support_sources[{index}].target_file",
        ),
        target_sha256=_lower_hash(
            row["target_sha256"],
            _SHA256,
            label=f"manifest.support_sources[{index}].target_sha256",
        ),
    )


def _parse_seed_spec(value: object, *, index: int) -> VLLMModelInfoSeedSpec:
    row = _exact_object(value, _SEED_FIELDS, f"manifest.seeds[{index}]")
    architecture = row["architecture"]
    if not isinstance(architecture, str) or architecture not in _SUPPORTED_LAYOUT:
        raise VLLMModelInfoSeedError(
            f"manifest.seeds[{index}].architecture is not release-supported"
        )
    expected_module, expected_file, expected_destination = _SUPPORTED_LAYOUT[architecture]
    if row["target_module"] != expected_module or row["target_file"] != expected_file:
        raise VLLMModelInfoSeedError(
            f"manifest seed {architecture} does not name its exact vLLM source module"
        )
    if row["vllm_cache_hash_algorithm"] != "md5":
        raise VLLMModelInfoSeedError(
            f"manifest seed {architecture} must use vLLM 0.15's md5 cache identity"
        )
    seed_filename = _plain_filename(
        row["seed_filename"], label=f"manifest seed {architecture}.seed_filename"
    )
    destination = _plain_filename(
        row["destination_filename"],
        label=f"manifest seed {architecture}.destination_filename",
    )
    if seed_filename != expected_destination or destination != expected_destination:
        raise VLLMModelInfoSeedError(
            f"manifest seed {architecture} does not name vLLM's exact cache destination"
        )
    return VLLMModelInfoSeedSpec(
        architecture=architecture,
        target_module=expected_module,
        target_file=expected_file,
        target_sha256=_lower_hash(
            row["target_sha256"],
            _SHA256,
            label=f"manifest seed {architecture}.target_sha256",
        ),
        vllm_cache_hash=_lower_hash(
            row["vllm_cache_hash"],
            _VLLM_CACHE_HASH,
            label=f"manifest seed {architecture}.vllm_cache_hash",
        ),
        seed_filename=seed_filename,
        seed_sha256=_lower_hash(
            row["seed_sha256"],
            _SHA256,
            label=f"manifest seed {architecture}.seed_sha256",
        ),
        destination_filename=destination,
    )


def _load_bundle(
    resource_root: Path,
    *,
    expected_manifest_sha256: str = EXPECTED_MANIFEST_SHA256,
) -> ModelInfoSeedBundle:
    manifest_path = _contained_file(
        resource_root / "manifest.json",
        resource_root,
        label="vLLM model-info seed manifest",
    )
    manifest_bytes = _regular_bytes(manifest_path, label="vLLM model-info seed manifest")
    if hashlib.sha256(manifest_bytes).hexdigest() != expected_manifest_sha256:
        raise VLLMModelInfoSeedError("vLLM model-info seed manifest hash mismatch")
    try:
        decoded = manifest_bytes.decode("utf-8")
        value = strict_json_loads(decoded)
    except (UnicodeDecodeError, ValueError) as exc:
        raise VLLMModelInfoSeedError(
            f"vLLM model-info seed manifest is invalid JSON: {exc}"
        ) from exc
    manifest = _exact_object(value, _MANIFEST_FIELDS, "seed manifest")
    if type(manifest["schema_version"]) is not int or manifest["schema_version"] != 1:
        raise VLLMModelInfoSeedError("vLLM model-info seed manifest schema_version must be 1")
    version = manifest["vllm_version"]
    if not isinstance(version, str) or not version:
        raise VLLMModelInfoSeedError("vLLM model-info seed manifest version is invalid")
    raw_support_sources = manifest["support_sources"]
    if not isinstance(raw_support_sources, list) or not raw_support_sources:
        raise VLLMModelInfoSeedError("vLLM model-info seed manifest support_sources must be a list")
    support_sources = tuple(
        _parse_support_source(row, index=index) for index, row in enumerate(raw_support_sources)
    )
    support_paths = [source.target_file for source in support_sources]
    if support_paths != sorted(support_paths) or len(support_paths) != len(set(support_paths)):
        raise VLLMModelInfoSeedError("vLLM model-info support sources must be unique and sorted")
    raw_seeds = manifest["seeds"]
    if not isinstance(raw_seeds, list) or not raw_seeds:
        raise VLLMModelInfoSeedError("vLLM model-info seed manifest seeds must be a list")
    seeds = tuple(_parse_seed_spec(row, index=index) for index, row in enumerate(raw_seeds))
    architectures = [seed.architecture for seed in seeds]
    if len(architectures) != len(set(architectures)):
        raise VLLMModelInfoSeedError("vLLM model-info seed manifest has duplicate architectures")
    if set(architectures) != SUPPORTED_ARCHITECTURES:
        raise VLLMModelInfoSeedError(
            "vLLM model-info seed manifest does not cover exactly the supported architectures"
        )
    if len({seed.destination_filename for seed in seeds}) != len(seeds):
        raise VLLMModelInfoSeedError("vLLM model-info seed destinations are not unique")
    return ModelInfoSeedBundle(
        version,
        support_sources,
        tuple(sorted(seeds, key=lambda seed: seed.architecture)),
    )


def _validate_modelinfo_seed(payload: bytes, seed: VLLMModelInfoSeedSpec) -> None:
    if hashlib.sha256(payload).hexdigest() != seed.seed_sha256:
        raise VLLMModelInfoSeedError(f"seed artifact hash mismatch for {seed.architecture}")
    try:
        value = strict_json_loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise VLLMModelInfoSeedError(
            f"seed artifact for {seed.architecture} is invalid JSON: {exc}"
        ) from exc
    root = _exact_object(value, {"hash", "modelinfo"}, f"seed {seed.architecture}")
    if root["hash"] != seed.vllm_cache_hash:
        raise VLLMModelInfoSeedError(f"seed artifact cache hash mismatch for {seed.architecture}")
    modelinfo = _exact_object(
        root["modelinfo"], _MODELINFO_FIELDS, f"seed {seed.architecture}.modelinfo"
    )
    if modelinfo["architecture"] != seed.architecture:
        raise VLLMModelInfoSeedError(f"seed artifact architecture mismatch for {seed.architecture}")
    for field in _MODELINFO_BOOLEAN_FIELDS:
        if type(modelinfo[field]) is not bool:
            raise VLLMModelInfoSeedError(
                f"seed {seed.architecture}.modelinfo.{field} must be boolean"
            )
    if modelinfo["attn_type"] != "decoder":
        raise VLLMModelInfoSeedError(
            f"seed {seed.architecture}.modelinfo.attn_type is not the reviewed value"
        )
    if modelinfo["default_seq_pooling_type"] != "LAST":
        raise VLLMModelInfoSeedError(
            f"seed {seed.architecture}.modelinfo.default_seq_pooling_type is not reviewed"
        )
    if modelinfo["default_tok_pooling_type"] != "ALL":
        raise VLLMModelInfoSeedError(
            f"seed {seed.architecture}.modelinfo.default_tok_pooling_type is not reviewed"
        )


def _distribution_root(distribution: Any) -> Path:
    try:
        root = Path(distribution.locate_file(""))
    except Exception as exc:
        raise VLLMModelInfoSeedError(f"could not locate the vLLM distribution: {exc}") from exc
    if not root.is_absolute():
        raise VLLMModelInfoSeedError("vLLM distribution root must be absolute")
    try:
        require_non_shared_path(root, name="vLLM distribution root")
    except RuntimePathError as exc:
        raise VLLMModelInfoSeedError(str(exc)) from exc
    return root


def _verify_target_source(
    distribution: Any,
    distribution_root: Path,
    seed: VLLMModelInfoSeedSpec,
) -> None:
    try:
        target = Path(distribution.locate_file(seed.target_file))
    except Exception as exc:
        raise VLLMModelInfoSeedError(
            f"could not locate vLLM source for {seed.architecture}: {exc}"
        ) from exc
    target = _contained_file(
        target,
        distribution_root,
        label=f"vLLM source for {seed.architecture}",
    )
    source = _regular_bytes(target, label=f"vLLM source for {seed.architecture}")
    if hashlib.sha256(source).hexdigest() != seed.target_sha256:
        raise VLLMModelInfoSeedError(f"vLLM source hash drift for {seed.architecture}")
    observed_cache_hash = hashlib.md5(source, usedforsecurity=False).hexdigest()
    if observed_cache_hash != seed.vllm_cache_hash:
        raise VLLMModelInfoSeedError(f"vLLM cache identity drift for {seed.architecture}")


def _validated_inputs(
    *,
    capsule_python_root: Path,
    expected_vllm_version: str | None,
    expected_manifest_sha256: str,
    expected_support_sources: tuple[VLLMModelInfoSupportSourceSpec, ...] | None,
    expected_seeds: tuple[VLLMModelInfoSeedSpec, ...] | None,
    distribution: Any | None,
) -> tuple[ModelInfoSeedBundle, tuple[tuple[VLLMModelInfoSeedSpec, bytes], ...]]:
    resource_root = capsule_python_root / MANIFEST_RELATIVE_PATH.parent
    _contained_file(
        resource_root / "manifest.json",
        capsule_python_root,
        label="vLLM model-info seed manifest",
    )
    bundle = _load_bundle(
        resource_root,
        expected_manifest_sha256=expected_manifest_sha256,
    )
    if expected_vllm_version is not None and bundle.vllm_version != expected_vllm_version:
        raise VLLMModelInfoSeedError(
            f"seed vLLM version {bundle.vllm_version!r} != profile {expected_vllm_version!r}"
        )
    if expected_support_sources is not None and bundle.support_sources != tuple(
        sorted(expected_support_sources, key=lambda source: source.target_file)
    ):
        raise VLLMModelInfoSeedError(
            "seed support sources do not match the hash-bearing CompatibilityProfile"
        )
    if expected_seeds is not None and bundle.seeds != tuple(
        sorted(expected_seeds, key=lambda seed: seed.architecture)
    ):
        raise VLLMModelInfoSeedError(
            "seed manifest entries do not match the hash-bearing CompatibilityProfile"
        )
    try:
        installed = distribution or metadata.distribution("vllm")
    except metadata.PackageNotFoundError as exc:
        raise VLLMModelInfoSeedError("required vLLM distribution is unavailable") from exc
    observed_version = getattr(installed, "version", None)
    if observed_version != bundle.vllm_version:
        raise VLLMModelInfoSeedError(
            f"installed vLLM version {observed_version!r} != seed {bundle.vllm_version!r}"
        )
    distribution_root = _distribution_root(installed)
    for source in bundle.support_sources:
        try:
            target = Path(installed.locate_file(source.target_file))
        except Exception as exc:
            raise VLLMModelInfoSeedError(
                f"could not locate vLLM support source {source.target_file}: {exc}"
            ) from exc
        target = _contained_file(
            target,
            distribution_root,
            label=f"vLLM model-info support source {source.target_file}",
        )
        observed = _regular_bytes(
            target,
            label=f"vLLM model-info support source {source.target_file}",
        )
        if hashlib.sha256(observed).hexdigest() != source.target_sha256:
            raise VLLMModelInfoSeedError(
                f"vLLM model-info support source drift for {source.target_file}"
            )
    validated: list[tuple[VLLMModelInfoSeedSpec, bytes]] = []
    for seed in bundle.seeds:
        seed_path = _contained_file(
            resource_root / seed.seed_filename,
            resource_root,
            label=f"seed artifact for {seed.architecture}",
        )
        payload = _regular_bytes(seed_path, label=f"seed artifact for {seed.architecture}")
        _validate_modelinfo_seed(payload, seed)
        _verify_target_source(installed, distribution_root, seed)
        validated.append((seed, payload))
    return bundle, tuple(validated)


def validate_profile_seed_bundle(
    *,
    capsule_python_root: str | os.PathLike[str],
    profile: CompatibilityProfile,
    distribution: Any | None = None,
) -> ModelInfoSeedBundle:
    """Verify the profile, manifest, seeds, and exact installed vLLM sources."""

    if profile.vllm_modelinfo_seed_manifest_path != MANIFEST_RELATIVE_PATH.as_posix():
        raise VLLMModelInfoSeedError(
            "CompatibilityProfile names an unsupported model-info manifest path"
        )
    if not profile.vllm_modelinfo_seeds:
        raise VLLMModelInfoSeedError("CompatibilityProfile has no VC-01 seed contract")
    if profile.vllm_modelinfo_installer_path != "exaserve/vllm_modelinfo_seed.py":
        raise VLLMModelInfoSeedError(
            "CompatibilityProfile names an unsupported VC-01 installer path"
        )
    installer_path = _contained_file(
        Path(capsule_python_root) / profile.vllm_modelinfo_installer_path,
        Path(capsule_python_root),
        label="VC-01 installer",
    )
    if (
        hashlib.sha256(_regular_bytes(installer_path, label="VC-01 installer")).hexdigest()
        != profile.vllm_modelinfo_installer_hash
    ):
        raise VLLMModelInfoSeedError("VC-01 installer hash mismatch")
    bundle, _inputs = _validated_inputs(
        capsule_python_root=Path(capsule_python_root),
        expected_vllm_version=profile.vllm,
        expected_manifest_sha256=profile.vllm_modelinfo_seed_manifest_hash,
        expected_support_sources=profile.vllm_modelinfo_support_sources,
        expected_seeds=profile.vllm_modelinfo_seeds,
        distribution=distribution,
    )
    return bundle


def _validated_cache_root(cache_root: Path, state_root: Path) -> Path:
    expected = state_root / "cache" / "vllm"
    if os.path.normpath(cache_root) != os.path.normpath(expected):
        raise VLLMModelInfoSeedError(
            f"VLLM_CACHE_ROOT must be the closed generation-local path {expected}"
        )
    try:
        require_contained_local_path(
            cache_root,
            state_root,
            name="vLLM cache root",
            require_exists=True,
        )
        return ensure_node_local_directory(
            cache_root / "modelinfos",
            mode=0o700,
            enforce_mode=True,
        )
    except (RuntimePathError, OSError, ValueError) as exc:
        raise VLLMModelInfoSeedError(f"vLLM cache root is unsafe: {exc}") from exc


def install_vllm_modelinfo_seeds(
    *,
    capsule_python_root: str | os.PathLike[str],
    state_root: str | os.PathLike[str],
    cache_root: str | os.PathLike[str],
    expected_vllm_version: str | None = None,
    expected_manifest_sha256: str = EXPECTED_MANIFEST_SHA256,
    expected_support_sources: tuple[VLLMModelInfoSupportSourceSpec, ...] | None = None,
    expected_seeds: tuple[VLLMModelInfoSeedSpec, ...] | None = None,
    distribution: Any | None = None,
) -> ModelInfoSeedInstallReport:
    """Validate the complete bundle, then create-or-verify every cache entry."""

    capsule = Path(capsule_python_root)
    state = Path(state_root)
    cache = Path(cache_root)
    for path, label in ((capsule, "capsule Python root"), (state, "local state root")):
        if not path.is_absolute():
            raise VLLMModelInfoSeedError(f"{label} must be absolute")
        try:
            require_contained_local_path(path, path, name=label, require_exists=True)
        except RuntimePathError as exc:
            raise VLLMModelInfoSeedError(str(exc)) from exc
    bundle, inputs = _validated_inputs(
        capsule_python_root=capsule,
        expected_vllm_version=expected_vllm_version,
        expected_manifest_sha256=expected_manifest_sha256,
        expected_support_sources=expected_support_sources,
        expected_seeds=expected_seeds,
        distribution=distribution,
    )
    destination_root = _validated_cache_root(cache, state)
    created = []
    for seed, payload in inputs:
        destination = destination_root / seed.destination_filename
        try:
            published = atomic_create_or_verify_bytes(destination, payload)
        except (OSError, ValueError) as exc:
            raise VLLMModelInfoSeedError(
                f"vLLM model-info cache conflict for {seed.architecture}: {exc}"
            ) from exc
        observed = _regular_bytes(
            destination,
            label=f"installed cache entry for {seed.architecture}",
            require_private_owner=True,
        )
        if observed != payload:
            raise VLLMModelInfoSeedError(f"installed cache entry changed for {seed.architecture}")
        _validate_modelinfo_seed(observed, seed)
        if published:
            created.append(seed.architecture)
    return ModelInfoSeedInstallReport(
        vllm_version=bundle.vllm_version,
        architectures=tuple(seed.architecture for seed in bundle.seeds),
        created=tuple(created),
        report_hash=_install_report_hash(
            vllm_version=bundle.vllm_version,
            manifest_sha256=expected_manifest_sha256,
            seeds=bundle.seeds,
        ),
    )


def verify_vllm_modelinfo_seeds(
    *,
    capsule_python_root: str | os.PathLike[str],
    state_root: str | os.PathLike[str],
    cache_root: str | os.PathLike[str],
    expected_vllm_version: str | None = None,
    expected_manifest_sha256: str = EXPECTED_MANIFEST_SHA256,
    expected_support_sources: tuple[VLLMModelInfoSupportSourceSpec, ...] | None = None,
    expected_seeds: tuple[VLLMModelInfoSeedSpec, ...] | None = None,
    distribution: Any | None = None,
) -> bool:
    """Re-derive every source/cache identity without creating or replacing files."""

    capsule = Path(capsule_python_root)
    state = Path(state_root)
    cache = Path(cache_root)
    bundle, inputs = _validated_inputs(
        capsule_python_root=capsule,
        expected_vllm_version=expected_vllm_version,
        expected_manifest_sha256=expected_manifest_sha256,
        expected_support_sources=expected_support_sources,
        expected_seeds=expected_seeds,
        distribution=distribution,
    )
    expected_cache = state / "cache" / "vllm"
    if os.path.normpath(cache) != os.path.normpath(expected_cache):
        raise VLLMModelInfoSeedError("VLLM_CACHE_ROOT is not the closed generation-local path")
    destination_root = cache / "modelinfos"
    try:
        require_contained_local_path(
            destination_root,
            state,
            name="vLLM model-info cache",
            require_exists=True,
        )
    except RuntimePathError as exc:
        raise VLLMModelInfoSeedError(str(exc)) from exc
    _verify_private_directory(destination_root, label="vLLM model-info cache directory")
    expected_payloads = {seed.architecture: payload for seed, payload in inputs}
    for seed in bundle.seeds:
        destination = _contained_file(
            destination_root / seed.destination_filename,
            destination_root,
            label=f"installed cache entry for {seed.architecture}",
        )
        observed = _regular_bytes(
            destination,
            label=f"installed cache entry for {seed.architecture}",
            require_private_owner=True,
        )
        if observed != expected_payloads[seed.architecture]:
            raise VLLMModelInfoSeedError(
                f"installed cache entry hash mismatch for {seed.architecture}"
            )
        _validate_modelinfo_seed(observed, seed)
    return True


def _environment_paths() -> tuple[Path, Path, Path]:
    runtime_root = os.environ.get(LOCAL_RUNTIME_ROOT_ENV, "")
    state_root = os.environ.get(LOCAL_STATE_ROOT_ENV, "")
    cache_root = os.environ.get("VLLM_CACHE_ROOT", "")
    if not runtime_root or not state_root or not cache_root:
        raise VLLMModelInfoSeedError("VC-01 requires closed runtime, state, and VLLM cache roots")
    capsule_python_root = Path(runtime_root) / "python"
    _contained_file(
        Path(__file__),
        capsule_python_root,
        label="VC-01 installer module",
    )
    return capsule_python_root, Path(state_root), Path(cache_root)


def install_from_environment(*, profile: CompatibilityProfile) -> ModelInfoSeedInstallReport:
    """Install the profile-bound bundle from a rank's closed local environment."""

    if (
        os.environ.get("EXASERVE_ENGINE") != "vllm"
        or os.environ.get("EXASERVE_NULL_COMPUTE") != "0"
    ):
        raise VLLMModelInfoSeedError("VC-01 requires a real vLLM deployment")
    if not profile.vllm_modelinfo_seeds:
        raise VLLMModelInfoSeedError("CompatibilityProfile has no VC-01 seed contract")
    if profile.vllm_modelinfo_seed_manifest_path != MANIFEST_RELATIVE_PATH.as_posix():
        raise VLLMModelInfoSeedError(
            "CompatibilityProfile names an unsupported model-info manifest path"
        )
    capsule, state, cache = _environment_paths()
    return install_vllm_modelinfo_seeds(
        capsule_python_root=capsule,
        state_root=state,
        cache_root=cache,
        expected_vllm_version=profile.vllm,
        expected_manifest_sha256=profile.vllm_modelinfo_seed_manifest_hash,
        expected_support_sources=profile.vllm_modelinfo_support_sources,
        expected_seeds=profile.vllm_modelinfo_seeds,
    )


def verify_from_environment(*, profile: CompatibilityProfile) -> bool:
    """Re-verify the profile-bound bundle and local cache from one rank."""

    capsule, state, cache = _environment_paths()
    return verify_vllm_modelinfo_seeds(
        capsule_python_root=capsule,
        state_root=state,
        cache_root=cache,
        expected_vllm_version=profile.vllm,
        expected_manifest_sha256=profile.vllm_modelinfo_seed_manifest_hash,
        expected_support_sources=profile.vllm_modelinfo_support_sources,
        expected_seeds=profile.vllm_modelinfo_seeds,
    )


__all__ = [
    "CAPABILITY",
    "EXPECTED_MANIFEST_SHA256",
    "MANIFEST_RELATIVE_PATH",
    "NOT_REQUIRED_INSTALL_REPORT_HASH",
    "PATCH_ID",
    "SUPPORTED_ARCHITECTURES",
    "ModelInfoSeedInstallReport",
    "VLLMModelInfoSeedError",
    "expected_install_report_hash",
    "expected_source_seed_evidence",
    "install_from_environment",
    "install_vllm_modelinfo_seeds",
    "validate_profile_seed_bundle",
    "verify_from_environment",
    "verify_vllm_modelinfo_seeds",
]
