"""EN-01: the engine attests itself instead of being described by its owner."""

from __future__ import annotations

import json
import os

from exaserve.compat import engine_shim
from exaserve.compat.profile import default_profile
from exaserve.compat.receipt import ReceiptStore


def test_install_writes_the_shim_and_points_the_child_at_it(tmp_path, monkeypatch):
    monkeypatch.setenv("PYTHONPATH", "/pre/existing")
    shim_dir = tmp_path / "shim"
    receipts = tmp_path / "receipts"
    assert engine_shim.install(str(shim_dir), str(receipts), import_patches=True,
                               deployment_id="d1", generation=7, vendor="xpu",
                               versions={"ray": "2.53.0"})
    source = (shim_dir / "sitecustomize.py").read_text()
    assert "exaserve._sitecustomize" in source
    assert "write_engine_receipt" in source
    assert os.environ["PYTHONPATH"].split(os.pathsep)[0] == str(shim_dir)
    assert "/pre/existing" in os.environ["PYTHONPATH"]
    assert os.environ["EXASERVE_ENGINE_SHIM_PATCHES"] == "1"
    assert os.environ["EXASERVE_VERSION_RAY"] == "2.53.0"


def test_a_non_pp_engine_records_patches_as_undelivered(tmp_path, monkeypatch):
    """Non-PP deliberately does not deliver the patch set to the engine.

    The receipt must say so — not claim the patches applied, and not report a
    failure that would block readiness on a correct deployment.
    """
    monkeypatch.setenv("EXASERVE_VLLM_PATCH_PP_LAYER_FILTER", "1")
    receipts = tmp_path / "receipts"
    engine_shim.install(str(tmp_path / "shim"), str(receipts), import_patches=False,
                        deployment_id="d1", generation=7, vendor="xpu",
                        versions={"ray": "2.53.0", "vllm": "0.15.0"})
    path = engine_shim.write_engine_receipt(patches_imported=False)
    data = json.loads(open(path).read())
    assert data["role"] == "engine"
    assert data["attestation"] == "self"
    assert data["patch_results"] == {}
    assert data["patches_delivered"] is False

    profile = default_profile()
    assert set(data["not_applicable"]) == set(profile.required_patch_ids("engine"))

    # And the store accepts it: everything required is accounted for.
    from exaserve.compat.collector import receipt_from_dict

    ok, why = ReceiptStore(profile, "d1", 7).add(receipt_from_dict(data))
    assert ok, why


def test_a_pp_engine_that_is_half_patched_is_reported_as_failed(tmp_path, monkeypatch):
    """Patches delivered but not effective must NOT pass as not-applicable."""
    monkeypatch.setenv("EXASERVE_VLLM_PATCH_PP_LAYER_FILTER", "1")
    receipts = tmp_path / "receipts"
    engine_shim.install(str(tmp_path / "shim"), str(receipts), import_patches=True,
                        deployment_id="d1", generation=7, vendor="xpu")
    monkeypatch.setattr("exaserve.compat.activator._postcondition_sitecustomize",
                        lambda pid: False)
    path = engine_shim.write_engine_receipt(patches_imported=True)
    data = json.loads(open(path).read())
    assert data["patch_results"] and all(v is False for v in data["patch_results"].values())

    from exaserve.compat.collector import receipt_from_dict

    ok, why = ReceiptStore(default_profile(), "d1", 7).add(receipt_from_dict(data))
    assert not ok and "failed patches" in why


def test_collect_reads_every_engine_receipt(tmp_path, monkeypatch):
    receipts = tmp_path / "receipts"
    engine_shim.install(str(tmp_path / "shim"), str(receipts), import_patches=False,
                        deployment_id="d1", generation=7, vendor="xpu")
    engine_shim.write_engine_receipt(patches_imported=False)
    os.makedirs(receipts, exist_ok=True)
    (receipts / "engine_99999.json").write_text(json.dumps({"role": "engine"}))
    found = engine_shim.collect(str(receipts))
    assert len(found) == 2


def test_collect_returns_empty_without_waiting_forever(tmp_path):
    import time

    start = time.monotonic()
    assert engine_shim.collect(str(tmp_path / "nothing"), timeout_s=0.3) == []
    assert time.monotonic() - start < 5.0


def test_receipt_dir_is_generation_scoped():
    a = engine_shim.receipt_dir_for("d1", 1)
    b = engine_shim.receipt_dir_for("d1", 2)
    assert a != b, "a prior generation's engine receipts must not be readable"


def test_the_shim_never_raises_without_a_receipt_dir(monkeypatch):
    monkeypatch.delenv(engine_shim.RECEIPT_DIR_ENV, raising=False)
    assert engine_shim.write_engine_receipt(patches_imported=True) is None
