"""WP3 / AC-COMP-01 (audit IMP-B04): fail-closed activation + typed receipts."""

from __future__ import annotations

import pytest

from exaserve.compat.activator import ActivationError, CompatibilityActivator
from exaserve.compat.profile import (
    CompatibilityProfile,
    PatchSpec,
    ProfileMismatch,
    default_profile,
)
from exaserve.compat.receipt import ReceiptStore, build_receipt


def _profile(**over):
    base = dict(
        schema_version=1, name="test-profile", python="3.12.12", ray="2.53.0",
        vllm="0.15.0", vendor="xpu",
        patches=(
            PatchSpec("P1", "mod.a", "upstream-fix", ("replica", "engine"),
                      "sitecustomize", "cap_a"),
            PatchSpec("P2", "mod.b", "vendor-compat", ("replica",),
                      "sitecustomize", "cap_b"),
            PatchSpec("P3", "mod.c", "instrumentation", ("replica",),
                      "overlay", "cap_c", required=False),
        ),
        required_roles=("supervisor", "replica"),
    )
    base.update(over)
    p = CompatibilityProfile(**base)
    object.__setattr__(p, "profile_id", p.compute_id())
    return p


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
        act.activate("replica", apply_fn=lambda: None,
                     postcondition=lambda pid: pid != "P2",   # P2 silently fails
                     verify_environment=False)


def test_activation_raises_when_apply_fails():
    act = CompatibilityActivator(_profile(), deployment_id="d", generation=1)

    def boom():
        raise ImportError("vllm internals moved")

    with pytest.raises(ActivationError, match="activation failed"):
        act.activate("replica", apply_fn=boom, verify_environment=False)


def test_successful_activation_yields_a_complete_receipt():
    act = CompatibilityActivator(_profile(), deployment_id="d", generation=1)
    receipt = act.activate("replica", apply_fn=lambda: None,
                           postcondition=lambda pid: True,
                           verify_environment=False)
    assert receipt.role == "replica"
    assert receipt.patch_results == {"P1": True, "P2": True}
    assert receipt.attestation == "self"
    ok, why = receipt.is_complete_for(("P1", "P2"))
    assert ok, why


def test_external_daemon_is_supervisor_attested_not_self_reporting():
    act = CompatibilityActivator(_profile(), deployment_id="d", generation=1)
    r = act.attest_external("gateway", executable="/usr/sbin/haproxy",
                            version_probe="HAProxy 3.1.6")
    assert r.attestation == "supervisor"
    assert r.versions["executable"].endswith("haproxy")


# ---------------- receipt store gating --------------------------------------

def test_receipt_store_rejects_wrong_profile_generation_and_incomplete():
    p = _profile()
    store = ReceiptStore(p, "d", 5)

    stale_gen = build_receipt(profile=p, role="replica", deployment_id="d",
                              generation=4, patch_results={"P1": True, "P2": True})
    ok, why = store.add(stale_gen)
    assert not ok and "stale generation" in why

    other = _profile(ray="2.54.0")
    wrong_profile = build_receipt(profile=other, role="replica", deployment_id="d",
                                  generation=5, patch_results={"P1": True, "P2": True})
    ok, why = store.add(wrong_profile)
    assert not ok and "profile mismatch" in why

    incomplete = build_receipt(profile=p, role="replica", deployment_id="d",
                               generation=5, patch_results={"P1": True})
    ok, why = store.add(incomplete)
    assert not ok and "missing patch results" in why

    failed = build_receipt(profile=p, role="replica", deployment_id="d",
                           generation=5, patch_results={"P1": True, "P2": False})
    ok, why = store.add(failed)
    assert not ok and "failed patches" in why


def test_receipt_store_satisfied_only_when_every_required_role_attests():
    p = _profile()
    store = ReceiptStore(p, "d", 5)
    ok, why = store.satisfied()
    assert not ok and "supervisor" in why

    store.add(build_receipt(profile=p, role="supervisor", deployment_id="d",
                            generation=5, patch_results={}))
    assert not store.satisfied()[0]      # replica still missing
    store.add(build_receipt(profile=p, role="replica", deployment_id="d",
                            generation=5, patch_results={"P1": True, "P2": True}))
    assert store.satisfied()[0] is True


def test_default_profile_pins_the_measured_stack():
    p = default_profile("xpu")
    assert p.ray == "2.53.0" and p.vllm == "0.15.0"
    assert "engine" in p.required_roles
    assert p.profile_id and len(p.profile_id) == 64
    # the spawned-engine shim is a REQUIRED engine-role patch (ADR-003)
    assert "EN-01" in p.required_patch_ids("engine")
