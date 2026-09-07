"""Reviewed vLLM model-info seeds are exact, local, and fail closed."""

from __future__ import annotations

from dataclasses import replace
import hashlib
from importlib import metadata
import json
import os
from pathlib import Path
import shutil
from types import SimpleNamespace

import pytest

from exaserve.compat.producers import manifest_hash
from exaserve.compat.profile import (
    SCHEMA_VERSION,
    CompatibilityProfile,
    ProfileMismatch,
    VLLMModelInfoSeedSpec,
    VLLMModelInfoSupportSourceSpec,
    default_profile,
)
from exaserve.vllm_modelinfo_seed import (
    MANIFEST_RELATIVE_PATH,
    VLLMModelInfoSeedError,
    expected_install_report_hash,
    expected_source_seed_evidence,
    install_vllm_modelinfo_seeds,
    validate_profile_seed_bundle,
    verify_vllm_modelinfo_seeds,
)


_LAYOUT = {
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
_SUPPORT_PATHS = (
    "vllm/envs.py",
    "vllm/model_executor/models/interfaces.py",
    "vllm/model_executor/models/interfaces_base.py",
    "vllm/model_executor/models/registry.py",
    "vllm/utils/hashing.py",
    "vllm/utils/network_utils.py",
)


def _modelinfo(architecture: str) -> dict:
    return {
        "architecture": architecture,
        "is_text_generation_model": True,
        "is_pooling_model": False,
        "attn_type": "decoder",
        "default_seq_pooling_type": "LAST",
        "default_tok_pooling_type": "ALL",
        "supports_cross_encoding": False,
        "supports_multimodal": False,
        "supports_multimodal_raw_input_only": False,
        "requires_raw_input_tokens": False,
        "supports_multimodal_encoder_tp_data": False,
        "supports_pp": True,
        "has_inner_state": False,
        "is_attention_free": False,
        "is_hybrid": False,
        "has_noops": False,
        "supports_mamba_prefix_caching": False,
        "supports_transcription": False,
        "supports_transcription_only": False,
    }


class _Distribution:
    def __init__(self, root: Path, version: str):
        self.root = root
        self.version = version

    def locate_file(self, relative: str):
        return self.root / relative


def _write_bundle(tmp_path: Path):
    version = "0.15.0+xpu"
    capsule = tmp_path / "capsule" / "python"
    resources = capsule / MANIFEST_RELATIVE_PATH.parent
    resources.mkdir(parents=True)
    distribution_root = tmp_path / "site-packages"
    support_specs = []
    support_rows = []
    for target_file in _SUPPORT_PATHS:
        source = f"# deterministic support source {target_file}\n".encode()
        target = distribution_root / target_file
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source)
        source_spec = VLLMModelInfoSupportSourceSpec(
            target_file=target_file,
            target_sha256=hashlib.sha256(source).hexdigest(),
        )
        support_specs.append(source_spec)
        support_rows.append(
            {
                "target_file": target_file,
                "target_sha256": source_spec.target_sha256,
            }
        )
    rows = []
    specs = []
    for architecture, (module, target_file, filename) in sorted(_LAYOUT.items()):
        source = f"# deterministic {architecture} source\n".encode()
        target = distribution_root / target_file
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source)
        cache_hash = hashlib.md5(source, usedforsecurity=False).hexdigest()
        seed_bytes = (
            json.dumps(
                {"hash": cache_hash, "modelinfo": _modelinfo(architecture)},
                indent=2,
            )
            + "\n"
        ).encode()
        (resources / filename).write_bytes(seed_bytes)
        spec = VLLMModelInfoSeedSpec(
            architecture=architecture,
            target_module=module,
            target_file=target_file,
            target_sha256=hashlib.sha256(source).hexdigest(),
            vllm_cache_hash=cache_hash,
            seed_filename=filename,
            seed_sha256=hashlib.sha256(seed_bytes).hexdigest(),
            destination_filename=filename,
        )
        specs.append(spec)
        rows.append(
            {
                "architecture": architecture,
                "target_module": module,
                "target_file": target_file,
                "target_sha256": spec.target_sha256,
                "vllm_cache_hash_algorithm": "md5",
                "vllm_cache_hash": cache_hash,
                "seed_filename": filename,
                "seed_sha256": spec.seed_sha256,
                "destination_filename": filename,
            }
        )
    manifest_bytes = (
        json.dumps(
            {
                "schema_version": 1,
                "vllm_version": version,
                "support_sources": support_rows,
                "seeds": rows,
            },
            indent=2,
        )
        + "\n"
    ).encode()
    (resources / "manifest.json").write_bytes(manifest_bytes)
    state = tmp_path / "state"
    cache = state / "cache" / "vllm"
    cache.mkdir(parents=True)
    return SimpleNamespace(
        capsule=capsule,
        resources=resources,
        state=state,
        cache=cache,
        support_specs=tuple(support_specs),
        specs=tuple(specs),
        manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        distribution=_Distribution(distribution_root, version),
        version=version,
    )


def _install(bundle):
    return install_vllm_modelinfo_seeds(
        capsule_python_root=bundle.capsule,
        state_root=bundle.state,
        cache_root=bundle.cache,
        expected_vllm_version=bundle.version,
        expected_manifest_sha256=bundle.manifest_sha256,
        expected_support_sources=bundle.support_specs,
        expected_seeds=bundle.specs,
        distribution=bundle.distribution,
    )


def _verify(bundle):
    return verify_vllm_modelinfo_seeds(
        capsule_python_root=bundle.capsule,
        state_root=bundle.state,
        cache_root=bundle.cache,
        expected_vllm_version=bundle.version,
        expected_manifest_sha256=bundle.manifest_sha256,
        expected_support_sources=bundle.support_specs,
        expected_seeds=bundle.specs,
        distribution=bundle.distribution,
    )


def test_seed_install_is_atomic_idempotent_and_verifiable(tmp_path):
    bundle = _write_bundle(tmp_path)

    first = _install(bundle)
    second = _install(bundle)

    assert first.architectures == tuple(sorted(_LAYOUT))
    assert first.created == first.architectures
    assert second.created == ()
    assert first.report_hash == second.report_hash
    assert _verify(bundle) is True
    for seed in bundle.specs:
        destination = bundle.cache / "modelinfos" / seed.destination_filename
        assert destination.read_bytes() == (bundle.resources / seed.seed_filename).read_bytes()
        assert destination.stat().st_uid == os.getuid()
        assert destination.stat().st_mode & 0o022 == 0


@pytest.mark.parametrize("failure", ["manifest", "seed", "source", "support", "version"])
def test_seed_inputs_fail_closed_on_drift(tmp_path, failure):
    bundle = _write_bundle(tmp_path)
    if failure == "manifest":
        (bundle.resources / "manifest.json").write_bytes(b"{}\n")
    elif failure == "seed":
        (bundle.resources / bundle.specs[0].seed_filename).write_bytes(b"{}\n")
    elif failure == "source":
        target = bundle.distribution.locate_file(bundle.specs[0].target_file)
        target.write_bytes(b"drift\n")
    elif failure == "support":
        target = bundle.distribution.locate_file(bundle.support_specs[0].target_file)
        target.write_bytes(b"drift\n")
    else:
        bundle.distribution.version = "wrong"

    with pytest.raises(VLLMModelInfoSeedError):
        _install(bundle)


def test_seed_install_never_replaces_conflict_or_accepts_permissive_file(tmp_path):
    bundle = _write_bundle(tmp_path)
    destination_root = bundle.cache / "modelinfos"
    destination_root.mkdir()
    destination = destination_root / bundle.specs[0].destination_filename
    destination.write_bytes(b"conflict")

    with pytest.raises(VLLMModelInfoSeedError, match="conflict"):
        _install(bundle)
    assert destination.read_bytes() == b"conflict"

    destination.unlink()
    _install(bundle)
    destination.chmod(0o666)
    with pytest.raises(VLLMModelInfoSeedError, match="not group/world-writable"):
        _verify(bundle)


def test_seed_verify_rejects_permissive_cache_directory(tmp_path):
    bundle = _write_bundle(tmp_path)
    _install(bundle)
    destination_root = bundle.cache / "modelinfos"
    destination_root.chmod(0o777)

    with pytest.raises(VLLMModelInfoSeedError, match="not group/world-writable"):
        _verify(bundle)


def test_seed_verify_is_read_only_and_rejects_missing_entry(tmp_path):
    bundle = _write_bundle(tmp_path)
    _install(bundle)
    missing = bundle.cache / "modelinfos" / bundle.specs[-1].destination_filename
    missing.unlink()

    with pytest.raises(VLLMModelInfoSeedError, match="does not exist|readable regular file"):
        _verify(bundle)
    assert not missing.exists()


def test_seed_cache_root_must_be_exact_generation_local_path(tmp_path):
    bundle = _write_bundle(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()

    with pytest.raises(VLLMModelInfoSeedError, match="closed generation-local"):
        install_vllm_modelinfo_seeds(
            capsule_python_root=bundle.capsule,
            state_root=bundle.state,
            cache_root=outside,
            expected_vllm_version=bundle.version,
            expected_manifest_sha256=bundle.manifest_sha256,
            expected_support_sources=bundle.support_specs,
            expected_seeds=bundle.specs,
            distribution=bundle.distribution,
        )


def test_default_profile_binds_current_seed_surface_and_manifest():
    default_profile.cache_clear()
    profile = default_profile("xpu")
    manifest = Path(__file__).parents[1] / "src" / profile.vllm_modelinfo_seed_manifest_path

    assert {seed.architecture for seed in profile.vllm_modelinfo_seeds} == set(_LAYOUT)
    assert hashlib.sha256(manifest.read_bytes()).hexdigest() == (
        profile.vllm_modelinfo_seed_manifest_hash
    )
    installer = Path(__file__).parents[1] / "src" / profile.vllm_modelinfo_installer_path
    assert hashlib.sha256(installer.read_bytes()).hexdigest() == (
        profile.vllm_modelinfo_installer_hash
    )
    assert "vllm_modelinfo_cache_seed" in profile.capabilities()
    expected_evidence = expected_source_seed_evidence(profile, install_required=True)
    assert expected_evidence["vllm_modelinfo_seed_installed_count"] == len(
        profile.vllm_modelinfo_seeds
    )
    assert expected_evidence["vllm_modelinfo_seed_install_report_hash"] == (
        expected_install_report_hash(profile)
    )
    assert (
        expected_source_seed_evidence(profile, install_required=False)[
            "vllm_modelinfo_seed_installed_count"
        ]
        == 0
    )
    assert manifest_hash(profile) != manifest_hash(
        replace(
            profile,
            vllm_modelinfo_seed_manifest_path="",
            vllm_modelinfo_seed_manifest_hash="",
            vllm_modelinfo_installer_path="",
            vllm_modelinfo_installer_hash="",
            vllm_modelinfo_support_sources=(),
            vllm_modelinfo_seeds=(),
            profile_id="",
        )
    )
    legacy = CompatibilityProfile(
        schema_version=SCHEMA_VERSION,
        name="legacy",
        python="3.12",
        ray="2",
        vllm="1",
        vendor="xpu",
        patches=(),
    )
    assert "mpi4py" not in legacy.canonical()
    assert not any(key.startswith("vllm_modelinfo") for key in legacy.canonical())


def test_profile_bound_bundle_matches_qualified_vllm_sources(tmp_path):
    try:
        observed_version = metadata.version("vllm")
    except metadata.PackageNotFoundError:
        pytest.skip("qualified vLLM distribution is unavailable")
    profile = default_profile("xpu")
    if observed_version != profile.vllm:
        pytest.skip("installed vLLM is outside the Aurora compatibility profile")

    source_root = Path(__file__).parents[1] / "src"
    root = tmp_path / "capsule" / "python"
    installer_source = source_root / profile.vllm_modelinfo_installer_path
    installer_target = root / profile.vllm_modelinfo_installer_path
    installer_target.parent.mkdir(parents=True)
    shutil.copy2(installer_source, installer_target)
    resource_source = source_root / Path(profile.vllm_modelinfo_seed_manifest_path).parent
    resource_target = root / Path(profile.vllm_modelinfo_seed_manifest_path).parent
    shutil.copytree(resource_source, resource_target)

    installed = metadata.distribution("vllm")
    distribution_root = tmp_path / "site-packages"
    for target_file in {
        *(source.target_file for source in profile.vllm_modelinfo_support_sources),
        *(seed.target_file for seed in profile.vllm_modelinfo_seeds),
    }:
        source = Path(installed.locate_file(target_file))
        target = distribution_root / target_file
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    bundle = validate_profile_seed_bundle(
        capsule_python_root=root,
        profile=profile,
        distribution=_Distribution(distribution_root, observed_version),
    )

    assert bundle.vllm_version == profile.vllm
    assert bundle.support_sources == profile.vllm_modelinfo_support_sources
    assert bundle.seeds == profile.vllm_modelinfo_seeds
    profile.verify_installed_sources()


def test_profile_rejects_partial_or_duplicate_seed_contract():
    default_profile.cache_clear()
    profile = default_profile("xpu")
    with pytest.raises(ProfileMismatch, match="declared together"):
        replace(profile, vllm_modelinfo_seed_manifest_hash="", profile_id="")
    duplicate = (*profile.vllm_modelinfo_seeds, profile.vllm_modelinfo_seeds[0])
    with pytest.raises(ProfileMismatch, match="architectures must be unique"):
        replace(profile, vllm_modelinfo_seeds=duplicate, profile_id="")
