"""ADR-003: generated exact-hash overlay is immutable and role-scoped."""

from __future__ import annotations

import hashlib
import importlib
import importlib.metadata
import json
from pathlib import Path
import sys
from types import ModuleType

import pytest

from exaserve.compat import generated_overlay
from exaserve.compat.generated_overlay import GeneratedOverlayError
from exaserve.compat.profile import CompatibilityProfile, PatchSpec

H = "a" * 64


class _Distribution:
    def __init__(self, root, version="1"):
        self.root = root
        self.version = version

    def locate_file(self, relative):
        return self.root / relative


def _profile(source_hash: str, *, roles=("deployment",)) -> CompatibilityProfile:
    patch = PatchSpec(
        patch_id="PX",
        target="fakepkg.target.value",
        classification="vendor-compat",
        roles=roles,
        delivery="generated-overlay",
        capability="fake-capability",
        target_distribution="fakepkg",
        target_version="1",
        target_file="fakepkg/target.py",
        target_source_hash=source_hash,
        affected_symbols=("value",),
        patch_artifact_path="exaserve/fake.py",
        patch_artifact_hash=H,
        delivery_artifact_path="exaserve/compat/generated_overlay.py",
        delivery_artifact_hash=H,
        import_timing="before-target-import",
        semantic_probe="sentinel:PX",
    )
    profile = CompatibilityProfile(
        schema_version=1,
        name="test-overlay",
        python="3.12.12",
        ray="2.53.0",
        vllm="0.15.0",
        vendor="xpu",
        patches=(patch,),
    )
    object.__setattr__(profile, "profile_id", profile.compute_id())
    return profile


@pytest.fixture
def overlay_fixture(tmp_path, monkeypatch):
    original_meta_path = list(sys.meta_path)
    base = tmp_path / "base"
    package = base / "fakepkg"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    source = package / "target.py"
    source.write_text("value = 1\n")
    profile = _profile(hashlib.sha256(source.read_bytes()).hexdigest())
    monkeypatch.setattr(
        importlib.metadata,
        "distribution",
        lambda name: _Distribution(base) if name == "fakepkg" else None,
    )
    monkeypatch.setitem(
        generated_overlay.PATCH_CALLS,
        "PX",
        ("fake_overlay_adapter", "apply"),
    )
    adapter = ModuleType("fake_overlay_adapter")

    def apply():
        sys.modules["fakepkg.target"].patched = True

    adapter.apply = apply
    monkeypatch.setitem(sys.modules, "fake_overlay_adapter", adapter)
    monkeypatch.syspath_prepend(str(base))
    for name in ("fakepkg", "fakepkg.target"):
        monkeypatch.delitem(sys.modules, name, raising=False)
    root = tmp_path / "overlay"
    try:
        yield profile, source, root
    finally:
        sys.meta_path[:] = original_meta_path
        for name in ("fakepkg", "fakepkg.target"):
            sys.modules.pop(name, None)


def test_materialized_overlay_loads_exact_source_and_applies_declared_role(
    overlay_fixture, monkeypatch
):
    from exaserve.compat.engine_shim import shim_source

    profile, source, root = overlay_fixture
    manifest = generated_overlay.materialize(profile, root)
    assert manifest["entries"][0]["base_source_hash"] == profile.patches[0].target_source_hash
    assert (root / "sitecustomize.py").read_text() == shim_source()
    monkeypatch.setenv("EXASERVE_COMPAT_ROLE", "deployment")
    generated_overlay.install(profile, root)
    target = importlib.import_module("fakepkg.target")
    assert target.value == 1
    assert target.patched is True
    assert Path(target.__file__).resolve() == source.resolve()
    assert str(target.__exaserve_overlay_source__).startswith(str(root))
    assert target.__exaserve_overlay_profile_id__ == profile.profile_id
    assert generated_overlay.install(profile, root)["profile_id"] == profile.profile_id


def test_materialization_rejects_base_source_drift(overlay_fixture):
    profile, source, root = overlay_fixture
    source.write_text("value = 2\n")
    with pytest.raises(GeneratedOverlayError, match="base source hash mismatch"):
        generated_overlay.materialize(profile, root)


def test_overlay_rejects_output_tampering(overlay_fixture):
    profile, _source, root = overlay_fixture
    manifest = generated_overlay.materialize(profile, root)
    path = root / manifest["entries"][0]["relative_path"]
    path.write_text("value = 99\n")
    with pytest.raises(GeneratedOverlayError, match="output hash mismatch"):
        generated_overlay.load_manifest(profile, root)


def test_overlay_rejects_bootstrap_tampering(overlay_fixture):
    profile, _source, root = overlay_fixture
    generated_overlay.materialize(profile, root)
    (root / "sitecustomize.py").write_text("raise SystemExit('tampered')\n", encoding="utf-8")
    with pytest.raises(GeneratedOverlayError, match="bootstrap source hash mismatch"):
        generated_overlay.load_manifest(profile, root)


def test_overlay_manifest_must_match_the_profiles_exact_target_mapping(overlay_fixture):
    profile, _source, root = overlay_fixture
    manifest = generated_overlay.materialize(profile, root)
    manifest["entries"][0]["distribution"] = "invented-distribution"
    canonical = json.dumps(
        {key: value for key, value in manifest.items() if key != "manifest_hash"},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    manifest["manifest_hash"] = hashlib.sha256(canonical).hexdigest()
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(GeneratedOverlayError, match="profile target mapping"):
        generated_overlay.load_manifest(profile, root)


def test_install_rejects_base_drift_after_materialization(overlay_fixture, monkeypatch):
    profile, source, root = overlay_fixture
    generated_overlay.materialize(profile, root)
    source.write_text("value = 2\n")
    monkeypatch.setenv("EXASERVE_COMPAT_ROLE", "deployment")
    with pytest.raises(GeneratedOverlayError, match="base source hash mismatch"):
        generated_overlay.install(profile, root)


def test_role_filter_does_not_override_an_unrelated_process(overlay_fixture, monkeypatch):
    profile, _source, root = overlay_fixture
    generated_overlay.materialize(profile, root)
    monkeypatch.setenv("EXASERVE_COMPAT_ROLE", "ray_head")
    generated_overlay.install(profile, root)
    target = importlib.import_module("fakepkg.target")
    assert target.value == 1
    assert not hasattr(target, "patched")
    assert not str(target.__file__).startswith(str(root))


def test_role_can_be_rebound_before_target_import(overlay_fixture, monkeypatch):
    profile, _source, root = overlay_fixture
    generated_overlay.materialize(profile, root)
    monkeypatch.setenv("EXASERVE_COMPAT_ROLE", "ray_head")
    generated_overlay.install(profile, root)

    # Ray applies an actor runtime_env after spawning a generic worker. The
    # already-installed finder must observe that new role at import time.
    monkeypatch.setenv("EXASERVE_COMPAT_ROLE", "deployment")
    generated_overlay.install(profile, root)
    target = importlib.import_module("fakepkg.target")
    assert target.patched is True
    assert not str(target.__file__).startswith(str(root))
    assert str(target.__exaserve_overlay_source__).startswith(str(root))


def test_patch_shared_by_ray_worker_and_replica_survives_role_rebind(overlay_fixture, monkeypatch):
    _fixture_profile, source, root = overlay_fixture
    profile = _profile(
        hashlib.sha256(source.read_bytes()).hexdigest(),
        roles=("ray_worker", "replica"),
    )
    generated_overlay.materialize(profile, root)

    monkeypatch.setenv("EXASERVE_COMPAT_ROLE", "ray_worker")
    generated_overlay.install(profile, root)
    target = importlib.import_module("fakepkg.target")
    assert target.patched is True
    overlay_source = target.__exaserve_overlay_source__

    monkeypatch.setenv("EXASERVE_COMPAT_ROLE", "replica")
    generated_overlay.install(profile, root)
    assert target.__exaserve_overlay_source__ == overlay_source
    assert target.__exaserve_overlay_profile_id__ == profile.profile_id


def test_role_rebinding_rejects_a_target_loaded_from_base(overlay_fixture, monkeypatch):
    profile, _source, root = overlay_fixture
    generated_overlay.materialize(profile, root)
    monkeypatch.setenv("EXASERVE_COMPAT_ROLE", "ray_head")
    generated_overlay.install(profile, root)
    target = importlib.import_module("fakepkg.target")
    assert not str(target.__file__).startswith(str(root))

    monkeypatch.setenv("EXASERVE_COMPAT_ROLE", "deployment")
    with pytest.raises(GeneratedOverlayError, match="after target import from the base"):
        generated_overlay.install(profile, root)
