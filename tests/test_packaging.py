from importlib import resources
import io
from pathlib import Path
import runpy
import tarfile

import pytest

from exaserve.model_bcast import prepare_bcast_tools


_RELEASE_BUILDER = runpy.run_path(
    str(Path(__file__).resolve().parents[1] / "scripts/hardening/build_release_artifacts.py")
)
_extract_sdist = _RELEASE_BUILDER["_extract_sdist"]
_copy_release_input = _RELEASE_BUILDER["_copy_release_input"]
_atomic_copy = _RELEASE_BUILDER["_atomic_copy"]
_verified_local_build_environment = _RELEASE_BUILDER["_verified_local_build_environment"]


def test_packaged_runtime_resources_are_present():
    resource_root = resources.files("exaserve.resources")

    for name in (
        "bcast.c",
        "bcast.Makefile",
    ):
        assert (resource_root / name).is_file()


def test_bcast_sources_materialize_to_writable_build_dir(tmp_path):
    tools_dir = prepare_bcast_tools(tmp_path / "bcast")

    assert (tools_dir / "bcast.c").is_file()
    assert (tools_dir / "Makefile").is_file()


def test_bcast_native_boundary_never_constructs_a_shell_command():
    source = (resources.files("exaserve.resources") / "bcast.c").read_text()
    assert "popen(" not in source
    assert "system(" not in source
    assert 'execlp("tar"' not in source
    assert "emit_entry(&context" in source
    assert "receive_stream(transfer_communicator" in source


def test_bcast_cleanup_rejects_symlink_or_cross_device_candidate_ancestors():
    source = (resources.files("exaserve.resources") / "bcast.c").read_text()
    assert "validate_cleanup_chain(cleanup_root, cleanup_path, &root_metadata)" in source
    assert "S_ISLNK(metadata.st_mode) || metadata.st_dev != root_metadata->st_dev" in source


def test_release_sdist_extraction_rejects_path_escape(tmp_path):
    archive_path = tmp_path / "release.tar.gz"
    with tarfile.open(archive_path, "w:gz") as archive:
        member = tarfile.TarInfo("../escape")
        payload = b"outside"
        member.size = len(payload)
        archive.addfile(member, io.BytesIO(payload))

    with pytest.raises(RuntimeError, match="unsafe entry"):
        _extract_sdist(archive_path, tmp_path / "destination")
    assert not (tmp_path / "escape").exists()


def test_release_builder_stages_only_declared_inputs(tmp_path):
    repo = Path(__file__).resolve().parents[1]
    staged = tmp_path / "input"
    expected = _copy_release_input(repo, staged)
    observed = {path.relative_to(staged).as_posix() for path in staged.rglob("*") if path.is_file()}
    assert observed == set(expected)
    assert not (staged / "src/exaserve.egg-info").exists()
    assert not (staged / "doc").exists()
    assert not (staged / "scripts").exists()


def test_release_artifact_publication_never_replaces_an_existing_identity(tmp_path):
    destination = tmp_path / "release.whl"
    first = tmp_path / "first.whl"
    second = tmp_path / "second.whl"
    first.write_bytes(b"first")
    second.write_bytes(b"second")

    _atomic_copy(first, destination)
    with pytest.raises(FileExistsError):
        _atomic_copy(second, destination)

    assert destination.read_bytes() == b"first"


def test_offline_release_fallback_verifies_exact_local_build_tools(monkeypatch):
    repo = Path(__file__).resolve().parents[1]
    original_entry_points = _RELEASE_BUILDER["metadata"].entry_points

    def declared_entry_points(*, group):
        return [
            item
            for item in original_entry_points(group=group)
            if item.dist.name.lower().replace("_", "-") in {"setuptools", "wheel"}
        ]

    monkeypatch.setattr(
        _RELEASE_BUILDER["metadata"],
        "entry_points",
        declared_entry_points,
    )
    receipt = _verified_local_build_environment(repo)
    assert receipt["mode"] == "verified_local_tools"
    assert receipt["tools"] == {
        "build": "1.4.0",
        "setuptools": "78.1.1",
        "wheel": "0.46.3",
    }
    assert len(receipt["python_executable_sha256"]) == 64
    assert receipt["build_backend_entry_points"]


def test_offline_release_fallback_rejects_an_undeclared_backend_plugin(monkeypatch):
    class Distribution:
        name = "surprise-plugin"
        version = "1"

    class EntryPoint:
        name = "surprise"
        value = "surprise:hook"
        dist = Distribution()

    monkeypatch.setattr(
        _RELEASE_BUILDER["metadata"],
        "entry_points",
        lambda *, group: [EntryPoint()],
    )
    with pytest.raises(RuntimeError, match="undeclared backend plugin"):
        _verified_local_build_environment(Path(__file__).resolve().parents[1])
