"""WP10: optional node diagnostics are bounded and atomically published."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import tarfile

import pytest

import exaserve.state.diagnostics as diagnostics
from exaserve.state.diagnostics import collect_node_diagnostics, validate_diagnostics_manifest
from exaserve.rank_main import _collect_diagnostics_finite


def test_node_diagnostics_archive_is_bounded_and_manifested(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "a.log").write_bytes(b"a" * 8)
    (source / "b.log").write_bytes(b"b" * 8)
    result = collect_node_diagnostics(
        source_root=str(source),
        run_dir=str(tmp_path / "run"),
        rank=2,
        deployment_id="d",
        generation=3,
        max_bytes=8,
        max_files=10,
    )
    assert result["file_count"] == 1
    assert result["skipped_files"] == 1
    assert result["complete"] is False
    archive = tmp_path / "run" / "per_node" / "rank-00002.tar.gz"
    manifest = tmp_path / "run" / "per_node" / "rank-00002.manifest.json"
    assert archive.is_file() and manifest.is_file()
    assert json.loads(manifest.read_text())["archive_sha256"] == result["archive_sha256"]
    with tarfile.open(archive, "r:gz") as handle:
        assert handle.getnames() == ["a.log"]


def test_symlinks_are_never_archived(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    outside = tmp_path / "secret"
    outside.write_text("secret", encoding="utf-8")
    (source / "link").symlink_to(outside)
    result = collect_node_diagnostics(
        source_root=str(source),
        run_dir=str(tmp_path / "run"),
        rank=0,
        deployment_id="d",
        generation=1,
    )
    assert result["file_count"] == 0
    assert result["skipped_files"] == 1


def test_diagnostics_never_creates_output_through_intermediate_symlink(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "ray.log").write_text("diagnostic", encoding="utf-8")
    local = tmp_path / "local"
    shared = tmp_path / "shared-like"
    local.mkdir()
    shared.mkdir()
    (local / "escape").symlink_to(shared, target_is_directory=True)

    with pytest.raises(diagnostics.DiagnosticsError, match="node-local"):
        collect_node_diagnostics(
            source_root=str(source),
            run_dir=str(local / "escape"),
            rank=0,
            deployment_id="d",
            generation=1,
        )

    assert not (shared / "per_node").exists()


def test_diagnostics_rejects_a_symlink_source_before_traversal(tmp_path):
    source = tmp_path / "source-link"
    source.symlink_to("/home", target_is_directory=True)
    with pytest.raises(diagnostics.DiagnosticsError, match="node-local"):
        collect_node_diagnostics(
            source_root=str(source),
            run_dir=str(tmp_path / "run"),
            rank=0,
            deployment_id="d",
            generation=1,
        )


def test_rank_diagnostics_run_as_a_finite_result_checked_process(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    (source / "ray.log").write_text("diagnostic", encoding="utf-8")
    run_dir = tmp_path / "run"
    monkeypatch.setenv("EXASERVE_QUALIFIED_PYTHON", sys.executable)
    monkeypatch.setenv("PYTHONNOUSERSITE", "1")
    monkeypatch.setenv("PYTHONSAFEPATH", "1")
    monkeypatch.setenv("PYTHONPATH", str(Path(__file__).resolve().parents[1] / "src"))

    result = _collect_diagnostics_finite(
        source_root=str(source),
        run_dir=str(run_dir),
        rank=1,
        deployment_id="deployment",
        generation=7,
        timeout_s=5.0,
    )

    assert result["rank"] == 1
    assert result["deployment_id"] == "deployment"
    assert result["generation"] == 7


def test_diagnostics_manifest_validator_rejects_archive_corruption(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "ray.log").write_text("diagnostic", encoding="utf-8")
    run_dir = tmp_path / "run"
    result = collect_node_diagnostics(
        source_root=str(source),
        run_dir=str(run_dir),
        rank=1,
        deployment_id="deployment",
        generation=7,
    )
    (run_dir / "per_node" / "rank-00001.tar.gz").write_bytes(b"corrupt")
    with pytest.raises(diagnostics.DiagnosticsError, match="content hash"):
        validate_diagnostics_manifest(
            result,
            run_dir=str(run_dir),
            expected_rank=1,
            expected_deployment_id="deployment",
            expected_generation=7,
        )


@pytest.mark.parametrize("field,value", [("max_bytes", True), ("max_bytes", 1.5), ("max_files", 0)])
def test_node_diagnostics_reject_invalid_bounds(tmp_path, field, value):
    kwargs = {"max_bytes": 8, "max_files": 2, field: value}
    with pytest.raises(diagnostics.DiagnosticsError, match="bounds/identity"):
        collect_node_diagnostics(
            source_root=str(tmp_path / "source"),
            run_dir=str(tmp_path / "run"),
            rank=0,
            deployment_id="d",
            generation=1,
            **kwargs,
        )


def test_manifest_failure_does_not_leave_an_uncommitted_archive(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    (source / "ray.log").write_text("diagnostic", encoding="utf-8")

    def fail_manifest(*_args, **_kwargs):
        raise OSError("manifest filesystem failed")

    monkeypatch.setattr(diagnostics, "atomic_create_json", fail_manifest)
    with pytest.raises(OSError, match="manifest filesystem failed"):
        collect_node_diagnostics(
            source_root=str(source),
            run_dir=str(tmp_path / "run"),
            rank=0,
            deployment_id="d",
            generation=1,
        )

    assert not (tmp_path / "run" / "per_node" / "rank-00000.tar.gz").exists()
