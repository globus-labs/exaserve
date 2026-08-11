"""WP3 / AC-COMP-01 (audit IMP-B04): fail-closed activation + typed receipts."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path

import pytest

from exaserve.compat.activator import ActivationError, CompatibilityActivator
from exaserve.compat.profile import (
    CompatibilityProfile,
    PatchSpec,
    ProfileMismatch,
    default_profile,
)

H = "a" * 64


def _patch(patch_id, target, classification, roles, delivery, capability, *, required=True):
    return PatchSpec(
        patch_id,
        target,
        classification,
        roles,
        delivery,
        capability,
        required=required,
        target_distribution="test",
        target_version="1",
        target_file="test.py",
        target_source_hash=H,
        affected_symbols=(target,),
        patch_artifact_path="exaserve/_sitecustomize.py",
        patch_artifact_hash=H,
        delivery_artifact_path="exaserve/_sitecustomize.py",
        delivery_artifact_hash=H,
        import_timing="before-use",
        semantic_probe=f"probe:{patch_id}",
    )


def _profile(**over):
    base = dict(
        schema_version=1,
        name="test-profile",
        python="3.12.12",
        ray="2.53.0",
        vllm="0.15.0",
        vendor="xpu",
        patches=(
            _patch("P1", "mod.a", "upstream-fix", ("replica", "engine"), "sitecustomize", "cap_a"),
            _patch("P2", "mod.b", "vendor-compat", ("replica",), "sitecustomize", "cap_b"),
            _patch(
                "P3",
                "mod.c",
                "instrumentation",
                ("replica",),
                "generated-overlay",
                "cap_c",
                required=False,
            ),
        ),
        required_roles=("supervisor", "replica"),
    )
    base.update(over)
    p = CompatibilityProfile(**base)
    object.__setattr__(p, "profile_id", p.compute_id())
    return p


def test_compatibility_contract_snapshots_sequences_and_rejects_scalar_text():
    roles = ["replica"]
    patch = _patch("PX", "mod.x", "vendor-compat", roles, "sitecustomize", "capability")
    roles.append("engine")
    assert patch.roles == ("replica",)

    with pytest.raises(ProfileMismatch, match="roles must be a non-empty sequence"):
        _patch(
            "PY",
            "mod.y",
            "vendor-compat",
            "replica",
            "sitecustomize",
            "capability",
        )
    with pytest.raises(ProfileMismatch, match="capability"):
        _patch(
            "PZ",
            "mod.z",
            "vendor-compat",
            ("replica",),
            "sitecustomize",
            7,
        )

    patches = [patch]
    profile = _profile(patches=patches)
    patches.clear()
    assert profile.patches == (patch,)


# ---------------- profile identity ------------------------------------------


def test_profile_id_changes_with_manifest_or_versions():
    a = _profile()
    b = _profile(ray="2.54.0")
    assert a.profile_id != b.profile_id
    c = _profile(patches=a.patches[:2])
    assert a.profile_id != c.profile_id
    assert _profile().profile_id == a.profile_id  # deterministic


def test_environment_mismatch_fails_closed():
    p = _profile()
    p.verify_environment({"python": "3.12.12", "ray": "2.53.0", "vllm": "0.15.0"})
    with pytest.raises(ProfileMismatch, match="ray"):
        p.verify_environment({"python": "3.12.12", "ray": "2.49.1", "vllm": "0.15.0"})


def test_required_patch_ids_are_per_role():
    p = _profile()
    assert p.required_patch_ids("replica") == ("P1", "P2")
    assert p.required_patch_ids("engine") == ("P1",)
    assert p.required_patch_ids("supervisor") == ()


# ---------------- activation fail-closed ------------------------------------


def test_activation_raises_when_a_required_patch_did_not_take_effect():
    """IMP-B04: the old path set its done-flag even when patches failed."""
    act = CompatibilityActivator(_profile(), deployment_id="d", generation=1)
    with pytest.raises(ActivationError, match="did not take effect"):
        act.activate(
            "replica",
            apply_fn=lambda: None,
            postcondition=lambda pid: pid != "P2",  # P2 silently fails
            verify_environment=False,
        )


def test_activation_raises_when_apply_fails():
    act = CompatibilityActivator(_profile(), deployment_id="d", generation=1)

    def boom():
        raise ImportError("vllm internals moved")

    with pytest.raises(ActivationError, match="activation failed"):
        act.activate("replica", apply_fn=boom, verify_environment=False)


def test_successful_activation_yields_a_local_report_not_a_receipt():
    act = CompatibilityActivator(_profile(), deployment_id="d", generation=1)
    report = act.activate(
        "replica", apply_fn=lambda: None, postcondition=lambda pid: True, verify_environment=False
    )
    assert report.role == "replica"
    assert report.patch_results == {"P1": True, "P2": True}
    assert not hasattr(report, "receipt_requirement_id")
    with pytest.raises(TypeError):
        report.patch_results["P1"] = False  # type: ignore[index]


def test_default_activation_applies_only_the_resolved_role_patch_ids(monkeypatch):
    from exaserve import patches

    applied = []
    monkeypatch.setattr(
        patches,
        "apply_patch_ids",
        lambda patch_ids, strict=True: applied.append((tuple(patch_ids), strict)),
    )
    act = CompatibilityActivator(_profile(), deployment_id="d", generation=1)
    report = act.activate("engine", postcondition=lambda _patch_id: True, verify_environment=False)
    assert report.patch_results == {"P1": True}
    assert applied == [(("P1",), True)]


def test_activation_postconditions_must_return_actual_booleans():
    act = CompatibilityActivator(_profile(), deployment_id="d", generation=1)
    with pytest.raises(ActivationError, match="not a boolean"):
        act.activate(
            "replica",
            apply_fn=lambda: None,
            postcondition=lambda _pid: 1,
            verify_environment=False,
        )


def test_required_patch_without_loaded_postcondition_cannot_be_excused():
    act = CompatibilityActivator(_profile(), deployment_id="d", generation=1)
    with pytest.raises(ActivationError, match="has no in-process post-condition"):
        act.activate(
            "replica",
            apply_fn=lambda: None,
            postcondition=lambda _pid: None,
            verify_environment=False,
        )


def test_default_profile_pins_the_measured_stack():
    p = default_profile("xpu")
    assert p.ray == "2.53.0" and p.vllm == "0.15.0+xpu"


def test_default_profile_construction_does_not_discover_heavy_dependencies(monkeypatch):
    from exaserve.compat import profile as profile_module

    original = profile_module.metadata.distribution

    def guarded(name):
        if name in {"ray", "vllm"}:
            raise AssertionError(f"planner attempted dependency discovery for {name}")
        return original(name)

    default_profile.cache_clear()
    monkeypatch.setattr(profile_module.metadata, "distribution", guarded)
    assert default_profile("xpu").ray == "2.53.0"
    default_profile.cache_clear()


def test_installed_source_verification_rejects_byte_and_version_drift(monkeypatch, tmp_path):
    from exaserve.compat import profile as profile_module

    target_root = tmp_path / "target"
    exaserve_root = tmp_path / "exaserve-dist"
    target = target_root / "pkg" / "target.py"
    artifact = exaserve_root / "exaserve" / "_sitecustomize.py"
    target.parent.mkdir(parents=True)
    artifact.parent.mkdir(parents=True)
    target.write_bytes(b"qualified target\n")
    artifact.write_bytes(b"qualified patch\n")

    class Distribution:
        def __init__(self, root, version):
            self.root = root
            self.version = version

        def locate_file(self, relative):
            return self.root / relative

    distributions = {
        "test": Distribution(target_root, "1"),
        "exaserve": Distribution(exaserve_root, "0.4.0"),
    }
    monkeypatch.setattr(profile_module.metadata, "distribution", lambda name: distributions[name])
    patch = replace(
        _patch("P1", "pkg.target", "upstream-fix", ("replica",), "sitecustomize", "cap"),
        target_file="pkg/target.py",
        target_source_hash=hashlib.sha256(target.read_bytes()).hexdigest(),
        patch_artifact_hash=hashlib.sha256(artifact.read_bytes()).hexdigest(),
        delivery_artifact_hash=hashlib.sha256(artifact.read_bytes()).hexdigest(),
    )
    profile = _profile(patches=(patch,))
    profile.verify_installed_sources()

    target.write_bytes(b"drifted target\n")
    with pytest.raises(ProfileMismatch, match="target source hash mismatch"):
        profile.verify_installed_sources()
    target.write_bytes(b"qualified target\n")
    distributions["test"].version = "2"
    with pytest.raises(ProfileMismatch, match="version"):
        profile.verify_installed_sources()


def test_source_snapshot_verifies_without_installed_exaserve_metadata(monkeypatch, tmp_path):
    from exaserve.compat import profile as profile_module

    target_root = tmp_path / "target"
    target = target_root / "pkg" / "target.py"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"qualified target\n")
    artifact = Path(profile_module.__file__).resolve().parents[1] / "_sitecustomize.py"

    class Distribution:
        version = "1"

        def locate_file(self, relative):
            return target_root / relative

    def distribution(name):
        if name == "exaserve":
            raise profile_module.metadata.PackageNotFoundError(name)
        if name == "test":
            return Distribution()
        raise profile_module.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(profile_module.metadata, "distribution", distribution)
    artifact_hash = hashlib.sha256(artifact.read_bytes()).hexdigest()
    patch = replace(
        _patch("P1", "pkg.target", "upstream-fix", ("replica",), "sitecustomize", "cap"),
        target_file="pkg/target.py",
        target_source_hash=hashlib.sha256(target.read_bytes()).hexdigest(),
        patch_artifact_hash=artifact_hash,
        delivery_artifact_hash=artifact_hash,
    )
    _profile(patches=(patch,)).verify_installed_sources()


def test_default_manifest_has_every_required_wp3_identity_field():
    p = default_profile("xpu")
    assert {patch.patch_id for patch in p.patches} == {
        "SC-01",
        "SC-02",
        "SC-03",
        "SC-04",
        "SC-05",
        "SC-09",
        "SC-10",
        "EW-01",
        "EW-02",
        "EW-03",
        "SC-11",
        "SC-12",
        "RS-01",
        "RS-02",
        "EN-01",
    }
    for patch in p.patches:
        assert patch.target_distribution and patch.target_version
        assert patch.target_file
        assert len(patch.target_source_hash) == 64
        assert patch.affected_symbols
        assert patch.patch_artifact_path
        assert len(patch.patch_artifact_hash) == 64
        assert patch.import_timing and patch.semantic_probe
    assert "engine_core" in p.required_roles
    assert "engine_worker" in p.required_roles
    assert p.profile_id and len(p.profile_id) == 64
    # the spawned-engine shim is a REQUIRED engine-role patch (ADR-003)
    assert "EN-01" in p.required_patch_ids("engine_core")
    assert "EN-01" in p.required_patch_ids("engine_worker")
    assert p.required_patch_ids("deployment") == ("RS-01",)
    assert p.required_patch_ids("ray_head") == ("RS-02",)
    assert p.required_patch_ids("ray_worker") == ("RS-02",)


def test_ray_start_is_import_clean_until_compatibility_activation():
    import ast
    from pathlib import Path

    source = Path(__file__).resolve().parents[1] / "src" / "exaserve" / "ray_start.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    top_level_imports = []
    for node in tree.body:
        if isinstance(node, ast.Import):
            top_level_imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            top_level_imports.append(node.module)
    assert not any(name == "ray" or name.startswith("ray.") for name in top_level_imports)


def test_raylet_adapter_exposes_a_receiptable_semantic_postcondition(monkeypatch):
    import sys
    from types import ModuleType, SimpleNamespace

    from exaserve import ray_start
    from exaserve.compat.activator import _postcondition_sitecustomize

    services = ModuleType("ray._private.services")

    def original(command, process_type, *args, **kwargs):
        return command, process_type, args, kwargs

    services.start_ray_process = original
    private = ModuleType("ray._private")
    private.ray_constants = SimpleNamespace(PROCESS_TYPE_RAYLET="raylet")
    private.services = services
    ray = ModuleType("ray")
    ray.__path__ = []
    monkeypatch.setitem(sys.modules, "ray", ray)
    monkeypatch.setitem(sys.modules, "ray._private", private)
    monkeypatch.setitem(sys.modules, "ray._private.services", services)

    ray_start._patch_raylet_launch(
        max_startup_concurrency=7,
        num_prestart_python_workers=5,
    )
    command, *_ = services.start_ray_process(["raylet"], "raylet")
    assert "--maximum_startup_concurrency=7" in command
    assert "--num_prestart_python_workers=5" in command
    assert _postcondition_sitecustomize("RS-02") is True


def test_sitecustomize_is_definition_only_and_every_adapter_is_manifested():
    import ast
    import inspect

    from exaserve import _sitecustomize

    source = inspect.getsource(_sitecustomize)
    assert "builtins.__import__" not in source
    assert "_patch_vllm_ray_multigpu_bundles" not in source
    assert "_patch_vllm_ray_executor_bundle_indices" not in source
    assert "_exaserve_gpu_model_runner_finder" not in source
    tree = ast.parse(source)
    executed_adapters = {
        node.value.func.id
        for node in tree.body
        if isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
        and (node.value.func.id.startswith("_patch_") or node.value.func.id.startswith("_install_"))
    }
    assert executed_adapters == set()
    assert all(
        callable(getattr(_sitecustomize, function_name, None))
        for function_name in _sitecustomize.DECLARED_PATCH_FUNCTIONS
    )
    manifest_ids = {patch.patch_id for patch in default_profile("xpu").patches}
    assert set(_sitecustomize.DECLARED_PATCH_FUNCTIONS.values()) <= manifest_ids


def test_generated_overlay_has_one_declared_activation_for_every_selected_patch():
    from exaserve.compat.generated_overlay import PATCH_CALLS

    profile = default_profile("xpu")
    selected = {
        patch.patch_id for patch in profile.patches if patch.delivery == "generated-overlay"
    }
    assert set(PATCH_CALLS) == selected


def test_ray_serve_timeout_is_delivered_once_before_import(monkeypatch, tmp_path):
    pytest.importorskip("ray", exc_type=ImportError)
    import os
    import sys
    from pathlib import Path

    from exaserve.control.finite_process import run_finite
    from exaserve.compat.generated_overlay import ROOT_ENV, materialize

    profile = default_profile("xpu")
    overlay = tmp_path / "overlay"
    materialize(profile, overlay)

    env = dict(os.environ)
    source_root = str(Path(__file__).resolve().parents[1] / "src")
    env["PYTHONPATH"] = os.pathsep.join([source_root, env.get("PYTHONPATH", "")])
    env["EXASERVE_RAY_SERVE_START_PROXY_TIMEOUT_S"] = "1234"
    env[ROOT_ENV] = str(overlay)
    env["EXASERVE_COMPAT_PROFILE_ID"] = profile.profile_id
    env["EXASERVE_COMPAT_ROLE"] = "deployment"
    code = """
from exaserve.compat.activator import CompatibilityActivator
CompatibilityActivator().activate('deployment', verify_environment=False)
from ray.serve._private import constants
assert constants.HTTP_PROXY_TIMEOUT == 1234.0
assert constants._exaserve_serve_start_timeout_patch is True
print('verified')
"""
    result = run_finite([sys.executable, "-c", code], timeout_s=30.0, env=env)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().splitlines()[-1] == "verified"
