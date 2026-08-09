"""WP2/WP9: result completeness is portable, strict, and tamper evident."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from exaserve.state.results import (
    ResultEntry,
    ResultManifest,
    ResultManifestError,
    load_result_manifest,
    write_result_manifest,
)


def _manifest(root, *, complete=True):
    artifact = root / "shards" / "result-0.json"
    artifact.parent.mkdir(parents=True)
    artifact.write_text('{"ok": true}\n', encoding="utf-8")
    entry = ResultEntry.from_file("replay/0", str(artifact), root=str(root))
    return ResultManifest(
        schema_version=2,
        run_id="run-1",
        run_semantic_hash="a" * 64,
        deployment_plan_hash="b" * 64,
        expected_ids=("replay/0",),
        entries=(entry,) if complete else (),
        incomplete_reasons=() if complete else ("required result is missing",),
        generated_at=datetime.now(timezone.utc).isoformat(),
        complete=complete,
    ).finalize()


def test_manifest_paths_are_relative_and_bundle_is_portable(tmp_path):
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    manifest = _manifest(bundle)
    path = bundle / "result_manifest.json"
    write_result_manifest(str(path), manifest)
    assert manifest.entries[0].path == "shards/result-0.json"

    moved = tmp_path / "moved"
    bundle.rename(moved)
    assert load_result_manifest(str(moved / "result_manifest.json")).complete


def test_result_entry_rejects_escape_and_symlink(tmp_path):
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    outside = tmp_path / "outside.json"
    outside.write_text("{}", encoding="utf-8")
    with pytest.raises(ResultManifestError, match="escapes"):
        ResultEntry.from_file("outside", str(outside), root=str(bundle))
    link = bundle / "link.json"
    link.symlink_to(outside)
    with pytest.raises(ResultManifestError, match="regular"):
        ResultEntry.from_file("link", str(link), root=str(bundle))


def test_manifest_is_immutable_and_detects_artifact_tampering(tmp_path):
    manifest = _manifest(tmp_path)
    path = tmp_path / "result_manifest.json"
    write_result_manifest(str(path), manifest)
    write_result_manifest(str(path), manifest)  # idempotent same identity

    replacement = ResultManifest(
        **{**manifest.__dict__, "generated_at": "2026-01-01T00:00:00+00:00", "manifest_hash": ""}
    ).finalize()
    with pytest.raises(ResultManifestError, match="immutable"):
        write_result_manifest(str(path), replacement)

    (tmp_path / manifest.entries[0].path).write_text("tampered", encoding="utf-8")
    with pytest.raises(ResultManifestError, match="content mismatch"):
        load_result_manifest(str(path))


def test_loader_rejects_unknown_shape_and_path_traversal(tmp_path):
    manifest = _manifest(tmp_path)
    path = tmp_path / "result_manifest.json"
    write_result_manifest(str(path), manifest)
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["entries"][0]["path"] = "../escape.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ResultManifestError, match="normalized and relative"):
        load_result_manifest(str(path), verify_files=False)


def test_complete_flag_requires_exact_sorted_expected_set(tmp_path):
    manifest = _manifest(tmp_path)
    with pytest.raises(ResultManifestError, match="sorted and unique"):
        ResultManifest(**{**manifest.__dict__, "expected_ids": ("z", "a"), "manifest_hash": ""})
