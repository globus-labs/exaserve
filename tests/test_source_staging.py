"""Distribution transactions never use shared per-rank result files."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from exaserve.model_bcast import (
    _prepare_model_bcast_source,
    _runtime_rank,
    _rollback_model_candidate,
    _source_model_manifest,
    _validate_cache_probe_result,
    _validate_model_receipt,
    bootstrap_application_environment,
    clean_model_caches_locally,
    cleanup_native_candidates,
    mpi_launch_prefix,
    resolve_bcast_executable,
    validate_model_bcast_result,
    verify_and_publish_model,
)
from exaserve.model_staging import (
    content_addressed_model_path,
    validate_node_local_tree,
    write_completion_marker,
)
from exaserve.source_staging import (
    SourceStagingError,
    _QUALIFIED_VERIFIER_BOOTSTRAP,
    _clean_package_snapshot,
    _freeze_capsule,
    _publish,
    _rank,
    _remove_source_candidate_parent,
    _rollback_source_candidate,
    _run_source_verifier,
    _validate_source_receipt,
    runtime_paths_from_result,
    tree_manifest,
    validate_source_staging_result,
    verify_and_publish,
)
from exaserve.staging_results import (
    StagingCollectiveError,
    load_collective_results,
    result_line_count,
    run_collective_operation,
)


def _candidate(tmp_path: Path) -> Path:
    package = tmp_path / "candidate" / "exaserve"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("VERSION = 1\n")
    (package / "module.py").write_text("VALUE = 2\n")
    return package


def _allow_test_python(monkeypatch) -> Path:
    executable = Path(sys.executable).resolve()
    monkeypatch.setattr("exaserve.plan.io.load_site_profile", lambda _path: SimpleNamespace())
    monkeypatch.setattr(
        "exaserve.site.qualify_site_local_bootstrap",
        lambda path, _profile: str(Path(path).resolve()),
    )
    monkeypatch.setattr(
        "exaserve.source_staging._compatibility_evidence",
        lambda _root: {
            "compatibility_profile_id": "7" * 64,
            "compatibility_manifest_hash": "8" * 64,
        },
    )
    return executable


def _aggregate_source_result(*, nodes=("n0", "n1")) -> dict:
    files = [{"path": "__init__.py", "size": 2, "mode": 0o644, "sha256": "f" * 64}]
    canonical = json.dumps(files, sort_keys=True, separators=(",", ":"))
    source_hash = hashlib.sha256(canonical.encode()).hexdigest()
    attempt = "a" * 32
    runtime_root = f"/tmp/exaserve/runtime/g7/{source_hash}"
    from exaserve.plan.runtime_environment import default_local_state_root

    state_root = default_local_state_root(SimpleNamespace(deployment_id="deployment"), 7)
    receipts = [
        {
            "schema_version": 1,
            "attempt_id": attempt,
            "result_id": f"{rank + 1:032x}",
            "rank": rank,
            "node": node,
            "generation": 7,
            "source_manifest_hash": source_hash,
            "file_count": 1,
            "total_bytes": 2,
            "published_path": runtime_root,
            "published_target": runtime_root,
            "runtime_device_id": 7,
            "state_device_id": 7,
            "runtime_fs_type": "tmpfs",
            "state_fs_type": "tmpfs",
            "qualified_python_path": "/opt/site/python3",
            "qualified_python_sha256": "9" * 64,
            "qualified_python_device_id": 22,
            "qualified_python_fs_type": "squashfs",
            "compatibility_profile_id": "7" * 64,
            "compatibility_manifest_hash": "8" * 64,
            "verification_duration_s": 0.1,
        }
        for rank, node in enumerate(nodes)
    ]
    return {
        "schema_version": 2,
        "deployment_id": "deployment",
        "generation": 7,
        "deployment_plan_hash": "b" * 64,
        "site_profile_hash": "c" * 64,
        "allocation_binding_hash": "d" * 64,
        "source_manifest_hash": source_hash,
        "capsule_manifest_hash": source_hash,
        "local_runtime_root": runtime_root,
        "local_python_root": f"{runtime_root}/python",
        "local_plan_path": f"{runtime_root}/run/deployment.plan.json",
        "local_site_profile_path": f"{runtime_root}/run/site.profile.json",
        "local_binding_path": f"{runtime_root}/run/allocation_binding.json",
        "local_bcast_path": f"{runtime_root}/bin/bcast",
        "local_go_dispatch": f"{runtime_root}/bin/go_dispatch",
        "local_eval_manifest": None,
        "local_run_plan": None,
        "local_state_root": state_root,
        "qualified_python": "/opt/site/python3",
        "qualified_python_sha256": "9" * 64,
        "compatibility_profile_id": "7" * 64,
        "compatibility_manifest_hash": "8" * 64,
        "file_count": 1,
        "total_bytes": 2,
        "files": files,
        "duration_s": 1.25,
        "rank_receipts": receipts,
    }


def test_source_result_contract_exposes_one_exact_runtime_layout():
    result = _aggregate_source_result()
    assert (
        validate_source_staging_result(
            result,
            expected_deployment_id="deployment",
            expected_generation=7,
            expected_plan_hash="b" * 64,
            expected_site_profile_hash="c" * 64,
            expected_binding_hash="d" * 64,
            expected_rank_to_node=((0, "n0.example"), (1, "n1.example")),
        )
        is result
    )
    paths = runtime_paths_from_result(result)
    assert str(paths.root) == result["local_runtime_root"]
    assert str(paths.go_dispatch_path) == result["local_go_dispatch"]
    assert str(paths.state_root) == result["local_state_root"]


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda value: value.update(schema_version=True), "values are invalid"),
        (lambda value: value["files"][0].update(mode=True), "file 0 values are invalid"),
        (lambda value: value.update(file_count=2), "inventory hash/count/bytes mismatch"),
        (lambda value: value.update(local_python_root="/tmp/wrong"), "layout is inconsistent"),
        (lambda value: value["rank_receipts"][0].update(node=True), "receipt values are invalid"),
    ],
)
def test_source_result_contract_rejects_forged_evidence(mutate, message):
    result = _aggregate_source_result()
    mutate(result)
    with pytest.raises(SourceStagingError, match=message):
        validate_source_staging_result(result)


def test_tree_manifest_covers_mode_path_size_and_content(tmp_path):
    package = _candidate(tmp_path)
    (package / "module.py").chmod(0o755)
    first = tree_manifest(package)
    by_path = {entry["path"]: entry for entry in first["files"]}
    assert by_path["module.py"]["mode"] == 0o755
    (package / "module.py").write_text("VALUE = 3\n")
    assert tree_manifest(package)["source_manifest_hash"] != first["source_manifest_hash"]


def test_clean_package_snapshot_rejects_release_symlink(tmp_path, monkeypatch):
    installed = tmp_path / "installed" / "exaserve"
    installed.mkdir(parents=True)
    (installed / "__init__.py").write_text("VERSION = 1\n")
    (installed / "escape.py").symlink_to("/home/shared/module.py")
    monkeypatch.setattr("exaserve.source_staging.resources.files", lambda _name: installed)
    with pytest.raises(SourceStagingError, match="contains a symlink"):
        _clean_package_snapshot(tmp_path / "transaction", vendor="aurora")


def test_clean_package_snapshot_carries_local_distribution_metadata(tmp_path, monkeypatch):
    installed = tmp_path / "installed" / "exaserve"
    installed.mkdir(parents=True)
    (installed / "__init__.py").write_text("VERSION = 1\n")
    metadata = installed.parent / "exaserve.egg-info"
    metadata.mkdir()
    (metadata / "PKG-INFO").write_text("Name: exaserve\nVersion: 1\n")
    monkeypatch.setattr("exaserve.source_staging.resources.files", lambda _name: installed)
    monkeypatch.setattr(
        "exaserve.compat.profile.default_profile",
        lambda _vendor: SimpleNamespace(profile_id="profile"),
    )
    monkeypatch.setattr(
        "exaserve.compat.generated_overlay.materialize",
        lambda _profile, root: Path(root).mkdir(parents=True),
    )
    destination = tmp_path / "capsule" / "python"
    destination.mkdir(parents=True)
    _clean_package_snapshot(destination, vendor="xpu")
    assert (destination / "exaserve.egg-info" / "PKG-INFO").is_file()


def test_verify_then_atomic_publish_creates_real_capsule_and_state(tmp_path, monkeypatch):
    local_base = tmp_path / "exaserve"
    local_base.mkdir()
    package = _candidate(local_base)
    _freeze_capsule(package)
    manifest = tree_manifest(package)
    stable = local_base / "runtime" / "g11" / manifest["source_manifest_hash"]
    state = local_base / "state" / "deployment" / "g11"
    monkeypatch.setenv("PALS_RANKID", "7")
    qualified_python = _allow_test_python(monkeypatch)
    monkeypatch.setattr(
        "exaserve.source_staging._remove_source_candidate_parent",
        lambda *_args, **_kwargs: pytest.fail(
            "the still-running verifier must not clean its candidate parent"
        ),
    )
    receipt = verify_and_publish(
        package,
        manifest["source_manifest_hash"],
        manifest["file_count"],
        manifest["total_bytes"],
        11,
        stable,
        local_base=local_base,
        state_root=state,
        qualified_python=qualified_python,
    )
    assert stable.is_dir() and not stable.is_symlink()
    assert receipt["rank"] == 7 and receipt["published_target"] == str(stable)
    assert all((state / name).is_dir() for name in ("home", "tmp", "cache", "logs", "ray"))


def test_wrong_source_content_never_replaces_previous_publication(tmp_path, monkeypatch):
    local_base = tmp_path / "exaserve"
    local_base.mkdir()
    first = _candidate(local_base / "first")
    _freeze_capsule(first)
    manifest = tree_manifest(first)
    stable = local_base / "runtime" / "g1" / manifest["source_manifest_hash"]
    monkeypatch.setenv("PALS_RANKID", "0")
    qualified_python = _allow_test_python(monkeypatch)
    verify_and_publish(
        first,
        manifest["source_manifest_hash"],
        manifest["file_count"],
        manifest["total_bytes"],
        1,
        stable,
        local_base=local_base,
        qualified_python=qualified_python,
    )
    second = _candidate(local_base / "second")
    _freeze_capsule(second)
    second.chmod(0o755)
    (second / "module.py").chmod(0o644)
    (second / "module.py").write_text("corrupt\n")
    _freeze_capsule(second)
    with pytest.raises(SourceStagingError, match="manifest mismatch"):
        verify_and_publish(
            second,
            manifest["source_manifest_hash"],
            manifest["file_count"],
            manifest["total_bytes"],
            1,
            stable,
            local_base=local_base,
            qualified_python=qualified_python,
        )
    assert (stable / "module.py").read_text() == "VALUE = 2\n"


def test_source_quarantine_never_chmods_descendant_symlink_target(tmp_path):
    local_base = tmp_path / "exaserve"
    local_base.mkdir()
    candidate = _candidate(local_base / "new")
    _freeze_capsule(candidate)
    stable = local_base / "runtime" / "g1" / ("a" * 64)
    stable.mkdir(parents=True)
    external = tmp_path / "shared-like"
    external.mkdir()
    external.chmod(0o500)
    (stable / "escape").symlink_to(external, target_is_directory=True)

    _publish(candidate, stable, local_base=local_base)

    assert stat.S_IMODE(external.stat().st_mode) == 0o500
    assert not (stable / "escape").exists()


def test_failed_source_attempt_removes_only_its_owned_local_candidate(tmp_path):
    local_base = tmp_path / "exaserve"
    attempt = "a" * 32
    parent = local_base / "candidates" / "g7" / f"source.{attempt}"
    candidate = parent / "capsule"
    candidate.mkdir(parents=True)
    (candidate / "partial").write_text("x")
    sibling = parent.parent / f"source.{'b' * 32}"
    sibling.mkdir()
    stable = local_base / "runtime" / "g7" / ("c" * 64)
    stable.mkdir(parents=True)
    _remove_source_candidate_parent(candidate, local_base=local_base, failed=True)
    assert not parent.exists()
    assert sibling.is_dir()
    assert stable.is_dir()


def test_successful_source_attempt_removes_attempt_owned_runtime_scratch(tmp_path):
    local_base = tmp_path / "exaserve"
    attempt = "a" * 32
    parent = local_base / "candidates" / "g7" / f"source.{attempt}"
    candidate = parent / "capsule"
    scratch = parent / ".pmix" / "components"
    scratch.mkdir(parents=True)
    (parent / "python-cache").mkdir()
    sibling = parent.parent / f"source.{'b' * 32}"
    sibling.mkdir(parents=True)

    _remove_source_candidate_parent(candidate, local_base=local_base, failed=False)

    assert not parent.exists()
    assert sibling.is_dir()


def test_successful_source_cleanup_refuses_to_erase_unpublished_candidate(tmp_path):
    local_base = tmp_path / "exaserve"
    attempt = "a" * 32
    parent = local_base / "candidates" / "g7" / f"source.{attempt}"
    candidate = parent / "capsule"
    candidate.mkdir(parents=True)
    (candidate / "payload").write_text("still awaiting publication")

    with pytest.raises(SourceStagingError, match="candidate remains unpublished"):
        _remove_source_candidate_parent(candidate, local_base=local_base, failed=False)

    assert (candidate / "payload").read_text() == "still awaiting publication"


@pytest.mark.parametrize("verifier_fails", [False, True])
def test_source_verifier_cleans_only_after_process_exit(tmp_path, monkeypatch, verifier_fails):
    candidate_parent = tmp_path / "candidates" / "g7" / f"source.{'a' * 32}"
    (candidate_parent / ".pmix").mkdir(parents=True)
    events = []
    completed = SimpleNamespace(stdout="receipts")

    def fake_run(_argv, *, timeout_s, env):
        assert timeout_s == 30.0
        assert env == {"HOME": str(candidate_parent)}
        assert candidate_parent.exists()
        events.extend(("verifier-running", "verifier-exited"))
        if verifier_fails:
            raise SourceStagingError("verifier failed")
        return completed

    def cleanup():
        assert events[-1] == "verifier-exited"
        events.append("cleanup")
        import shutil

        shutil.rmtree(candidate_parent)

    monkeypatch.setattr("exaserve.source_staging._run_checked", fake_run)
    if verifier_fails:
        with pytest.raises(SourceStagingError, match="verifier failed"):
            _run_source_verifier(
                ["verifier"],
                timeout_s=30.0,
                env={"HOME": str(candidate_parent)},
                candidate_parent=candidate_parent,
                cleanup_candidate=cleanup,
            )
    else:
        assert (
            _run_source_verifier(
                ["verifier"],
                timeout_s=30.0,
                env={"HOME": str(candidate_parent)},
                candidate_parent=candidate_parent,
                cleanup_candidate=cleanup,
            )
            is completed
        )

    assert events == ["verifier-running", "verifier-exited", "cleanup"]
    assert not candidate_parent.exists()


def test_source_verifier_requires_candidate_absent_after_cleanup(tmp_path, monkeypatch):
    candidate_parent = tmp_path / "candidates" / "g7" / f"source.{'a' * 32}"
    candidate_parent.mkdir(parents=True)
    monkeypatch.setattr(
        "exaserve.source_staging._run_checked",
        lambda *_args, **_kwargs: SimpleNamespace(stdout="receipts"),
    )

    with pytest.raises(SourceStagingError, match="head candidate still exists"):
        _run_source_verifier(
            ["verifier"],
            timeout_s=30.0,
            env={},
            candidate_parent=candidate_parent,
            cleanup_candidate=lambda: None,
        )


def test_source_verifier_preserves_primary_failure_when_cleanup_also_fails(tmp_path, monkeypatch):
    candidate_parent = tmp_path / "candidates" / "g7" / f"source.{'a' * 32}"
    candidate_parent.mkdir(parents=True)
    primary = SourceStagingError("primary verifier failure")

    def fail_verifier(*_args, **_kwargs):
        raise primary

    def fail_cleanup():
        raise RuntimeError("cleanup failure")

    monkeypatch.setattr("exaserve.source_staging._run_checked", fail_verifier)
    with pytest.raises(SourceStagingError, match="primary verifier failure") as captured:
        _run_source_verifier(
            ["verifier"],
            timeout_s=30.0,
            env={},
            candidate_parent=candidate_parent,
            cleanup_candidate=fail_cleanup,
        )

    assert captured.value is primary
    assert any("cleanup failure" in note for note in primary.__notes__)


def test_supervised_native_failure_cleanup_removes_exact_candidate_only(tmp_path, monkeypatch):
    local_root = tmp_path / "local"
    candidate = local_root / "candidates" / "g7" / f"source.{'a' * 32}"
    candidate.mkdir(parents=True)
    (candidate / "partial").write_text("x")
    sibling = candidate.parent / "keep"
    sibling.mkdir()
    observed = {}

    def fake_run(command, **_kwargs):
        observed["command"] = command
        assert command[-2:] == ["--cleanup", str(candidate)]
        import shutil

        shutil.rmtree(candidate)
        return SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr("exaserve.model_bcast.run_finite", fake_run)
    cleanup_native_candidates(
        Path("/tmp/bcast"),
        candidate,
        local_root=local_root,
        num_nodes=2,
        binding=SimpleNamespace(rank_to_node=((0, "n0"), (1, "n1"))),
        scheduler="pbs",
        application_cwd=Path("/tmp"),
        application_environment={},
        recipient_ranks=[1],
    )
    assert not candidate.exists() and sibling.is_dir()
    assert "--genvnone" in observed["command"] and "--envnone" in observed["command"]


@pytest.mark.parametrize("name", ["", "arbitrary", "runtime/g7/" + "a" * 64])
def test_native_cleanup_refuses_broad_or_published_paths(tmp_path, name, monkeypatch):
    local_root = tmp_path / "local"
    local_root.mkdir()
    candidate = local_root if not name else local_root / name
    monkeypatch.setattr(
        "exaserve.model_bcast.run_finite",
        lambda *_args, **_kwargs: pytest.fail("unsafe cleanup reached launcher"),
    )
    with pytest.raises(RuntimeError, match="cleanup"):
        cleanup_native_candidates(
            Path("/tmp/bcast"),
            candidate,
            local_root=local_root,
            num_nodes=1,
            binding=SimpleNamespace(rank_to_node=((0, "n0"),)),
            scheduler="pbs",
            application_cwd=Path("/tmp"),
            application_environment={},
        )


def test_failed_attempt_rollbacks_preserve_content_addressed_publications(tmp_path):
    source_root = tmp_path / "source-local"
    source_candidate = source_root / "candidates" / "g2" / f"source.{'a' * 32}" / "capsule"
    source_candidate.mkdir(parents=True)
    source_stable = source_root / "runtime" / "g2" / ("b" * 64)
    source_stable.mkdir(parents=True)
    _rollback_source_candidate(source_candidate, local_base=source_root)
    assert source_stable.is_dir()

    model_root = tmp_path / "models"
    model_candidate = model_root / f".exaserve_stage.org--model.2.{'c' * 32}" / "org--model"
    model_candidate.mkdir(parents=True)
    model_stable = model_root / f"org--model.{'d' * 64}"
    model_stable.mkdir()
    _rollback_model_candidate(model_candidate, local_root=model_root)
    assert model_stable.is_dir()


def test_qualified_bootstrap_rejects_tampered_candidate_before_import(tmp_path):
    compile(_QUALIFIED_VERIFIER_BOOTSTRAP, "<qualified-staging-bootstrap>", "exec")
    local_base = tmp_path / "exaserve"
    package = local_base / "candidate" / "capsule"
    module = package / "python" / "exaserve"
    module.mkdir(parents=True)
    (module / "__init__.py").write_text("raise RuntimeError('candidate imported')\n")
    _freeze_capsule(package)
    manifest = tree_manifest(package)
    command = [
        sys.executable,
        "-I",
        "-s",
        "-c",
        _QUALIFIED_VERIFIER_BOOTSTRAP,
        str(package),
        manifest["source_manifest_hash"],
        str(manifest["file_count"]),
        str(manifest["total_bytes"]),
        str(local_base),
        "--preverify-only",
    ]
    assert subprocess.run(command, check=False, capture_output=True).returncode == 0
    package.chmod(0o755)
    (package / "python").chmod(0o755)
    module.chmod(0o755)
    (module / "__init__.py").chmod(0o644)
    (module / "__init__.py").write_text("Path('/tmp/should-not-run').touch()\n")
    _freeze_capsule(package)
    failed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert failed.returncode != 0
    assert "manifest mismatch" in failed.stderr


def test_unresolved_symlink_is_not_a_hashable_source_artifact(tmp_path):
    package = _candidate(tmp_path)
    os.symlink("module.py", package / "alias.py")
    with pytest.raises(SourceStagingError, match="unresolved symlink"):
        tree_manifest(package)


class _FakeComm:
    def __init__(self):
        self.broadcast = None

    def Get_rank(self):
        return 0

    def Get_size(self):
        return 1

    def gather(self, value, root):
        assert root == 0
        return [value]

    def bcast(self, value, root):
        assert root == 0
        self.broadcast = value
        return value


class _FakeMPI:
    COMM_WORLD = _FakeComm()

    @staticmethod
    def Get_processor_name():
        return "n0.example"


def test_mpi_result_transport_emits_one_root_aggregate_and_no_files(capsys, tmp_path):
    attempt = "e" * 32
    assert run_collective_operation(
        lambda: {"rank": 0, "generation": 9},
        attempt_id=attempt,
        expected_world_size=1,
        expected_root_node="n0",
        mpi=_FakeMPI,
    )
    stdout = capsys.readouterr().out
    assert result_line_count(stdout) == 1
    assert not list(tmp_path.iterdir())
    assert (
        load_collective_results(
            stdout,
            attempt_id=attempt,
            expected_world_size=1,
            expected_root_node="n0",
        )[0]["generation"]
        == 9
    )


def test_mpi_result_transport_preserves_rank_local_failure(capsys):
    def fail():
        raise ValueError("candidate digest mismatch")

    attempt = "f" * 32
    assert not run_collective_operation(
        fail,
        attempt_id=attempt,
        expected_world_size=1,
        expected_root_node="n0",
        mpi=_FakeMPI,
    )
    with pytest.raises(StagingCollectiveError, match="candidate digest mismatch"):
        load_collective_results(
            capsys.readouterr().out,
            attempt_id=attempt,
            expected_world_size=1,
            expected_root_node="n0",
        )


def test_pbs_native_launch_explicitly_transfers_executable():
    assert mpi_launch_prefix(4, scheduler="pbs", application_cwd="/tmp", transfer_executable=True)[
        :4
    ] == [
        "mpiexec",
        "--genvnone",
        "--envnone",
        "--transfer",
    ]
    assert "--transfer" not in mpi_launch_prefix(4, scheduler="pbs", application_cwd="/tmp")


def test_configured_bcast_cannot_bypass_site_qualification(monkeypatch):
    monkeypatch.delenv("EXASERVE_LOCAL_RUNTIME_ROOT", raising=False)
    monkeypatch.setenv("EXASERVE_QUALIFIED_BCAST", "/home/user/bcast")
    with pytest.raises(RuntimeError, match="SiteProfile qualification"):
        resolve_bcast_executable(scheduler="slurm")
    profile = SimpleNamespace(
        site_id="alcf-aurora",
        model_storage_path="/lus/flare/models",
        filesystem_semantics=(
            ("shared_root:/home", "shared;fstype=lustre;readonly=false"),
            ("site_root:/opt/aurora", "immutable_read_only_site;fstype=squashfs;readonly=true"),
        ),
    )
    with pytest.raises(RuntimeError, match="failed site evidence"):
        resolve_bcast_executable(scheduler="slurm", site_profile=profile)


def test_mpi_application_environment_is_closed_and_explicit():
    prefix = mpi_launch_prefix(
        2,
        scheduler="pbs",
        application_cwd="/tmp/runtime/python",
        application_environment={"HOME": "/tmp/state/home", "PYTHONNOUSERSITE": "1"},
    )
    assert prefix[:3] == ["mpiexec", "--genvnone", "--envnone"]
    exports = [prefix[index + 1] for index, item in enumerate(prefix[:-1]) if item == "--genv"]
    assert exports == ["HOME=/tmp/state/home", "PYTHONNOUSERSITE=1"]
    assert prefix[prefix.index("--wdir") + 1] == "/tmp/runtime/python"
    assert all(not item.startswith("HOME=/home") and "/lus/flare" not in item for item in prefix)


def test_bootstrap_environment_keeps_only_qualified_loader_and_fabric_values():
    from exaserve.site import AURORA_PMIX_PREPARED_ENVIRONMENT

    profile = SimpleNamespace(
        site_id="alcf-aurora",
        model_storage_path="/lus/flare/models",
        filesystem_semantics=(
            ("shared_root:/home", "lustre"),
            ("shared_root:/lus/flare", "lustre"),
            ("site_root:/opt/aurora", "immutable_read_only_site"),
        ),
        prepared_environment=AURORA_PMIX_PREPARED_ENVIRONMENT,
    )
    result = bootstrap_application_environment(
        profile=profile,
        base_environment={
            "LD_LIBRARY_PATH": "/home/user/lib:/opt/aurora/site/lib:/opt/cray/unqualified",
            "LD_PRELOAD": "/home/user/hook.so",
            "FI_PROVIDER": "cxi",
            "MPICH_ROOT": "/opt/aurora/mpich",
            "PYTHONPATH": "/home/user/project",
            "PMIX_RANK": "ambient-rank",
            "PALS_RANKID": "ambient-rank",
            "PALS_PMI": "ambient-launcher-state",
            "PALS_TRANSFER": "ambient-launcher-state",
            "PMIX_MCA_mca_base_param_files": "/home/user/.pmix/mca-params.conf",
            "PMIX_MCA_mca_base_component_path": "/home/user/.pmix/components",
        },
        additions={"HOME": "/tmp/state/home"},
    )
    assert result == {
        "FI_PROVIDER": "cxi",
        "HOME": "/tmp/state/home",
        "TMPDIR": "/tmp",
        "LD_LIBRARY_PATH": "/opt/aurora/site/lib",
        "MPICH_ROOT": "/opt/aurora/mpich",
        "PMIX_MCA_mca_base_param_files": "/etc/pmix-mca-params.conf",
        "PMIX_MCA_mca_base_component_path": "/usr/lib64/pmix",
    }


def test_precapsule_bootstrap_redirects_pmix_home_from_shared_storage():
    result = bootstrap_application_environment(
        base_environment={"HOME": "/home/user", "TMPDIR": "/lus/flare/tmp"}
    )
    assert result == {"HOME": "/tmp", "TMPDIR": "/tmp"}


def test_read_only_model_uses_head_local_broadcast_view(tmp_path, monkeypatch):
    source = tmp_path / "shared" / "revision"
    source.mkdir(parents=True)
    (source / "config.json").write_text("{}")
    (source / "model.safetensors").write_text("weights")
    run_logs = tmp_path / "run-logs"
    run_logs.mkdir()
    monkeypatch.setenv("EXASERVE_RUN_LOG_DIR", str(run_logs))
    monkeypatch.setattr(
        "exaserve.model_staging.write_completion_marker",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(PermissionError("read-only")),
    )
    manifest = _source_model_manifest(source, model_id="org/model")
    head_local = tmp_path / "head-local"
    head_local.mkdir()
    view = _prepare_model_bcast_source(
        source,
        safe_name="org--model",
        source_manifest=manifest,
        temporary_root=head_local,
    )
    assert view.parent == head_local
    assert view != source
    assert (view / "config.json").is_symlink()


def test_read_only_legacy_sample_marker_is_replaced_in_broadcast_view(tmp_path, monkeypatch):
    source = tmp_path / "shared" / "revision"
    source.mkdir(parents=True)
    config = source / "config.json"
    weight = source / "model.safetensors"
    config.write_text("{}")
    weight.write_bytes(b"w" * (3 * 1024 * 1024))
    sample = 1 << 20
    weight_bytes = weight.read_bytes()
    files = [
        {
            "path": "config.json",
            "size": 2,
            "hash_kind": "sha256-full",
            "sha256": hashlib.sha256(b"{}").hexdigest(),
        },
        {
            "path": "model.safetensors",
            "size": len(weight_bytes),
            "hash_kind": f"sha256-first-last-{sample}",
            "sha256": hashlib.sha256(weight_bytes[:sample] + weight_bytes[-sample:]).hexdigest(),
        },
    ]
    legacy = {
        "version": 2,
        "kind": "full_model",
        "source_identity": "legacy@readonly",
        "file_count": 2,
        "total_bytes": sum(item["size"] for item in files),
        "manifest_hash": hashlib.sha256(
            json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "files": files,
    }
    marker = source / ".exaserve_complete.json"
    marker.write_text(json.dumps(legacy))
    for path in (config, weight, marker):
        path.chmod(0o444)
    source.chmod(0o555)
    run_logs = tmp_path / "run-logs"
    run_logs.mkdir()
    monkeypatch.setenv("EXASERVE_RUN_LOG_DIR", str(run_logs))
    try:
        manifest = _source_model_manifest(source, model_id="org/model")
        assert all(item["hash_kind"] == "sha256-full" for item in manifest["files"])
        assert json.loads(marker.read_text())["manifest_hash"] == legacy["manifest_hash"]
        head_local = tmp_path / "head-local"
        head_local.mkdir()
        view = _prepare_model_bcast_source(
            source,
            safe_name="org--model",
            source_manifest=manifest,
            temporary_root=head_local,
        )
        injected = json.loads((view / ".exaserve_complete.json").read_text())
        assert injected["manifest_hash"] == manifest["manifest_hash"]
        assert injected["manifest_hash"] != legacy["manifest_hash"]
    finally:
        source.chmod(0o755)
        for path in (config, weight, marker):
            path.chmod(0o644)


def _model_candidate(root: Path, *, value: str = "weights") -> tuple[Path, str]:
    candidate = root / "candidate" / "org--model"
    candidate.mkdir(parents=True)
    (candidate / "config.json").write_text("{}")
    (candidate / "model.safetensors").write_text(value)
    write_completion_marker(candidate, source_identity="hf:org/model@revision")
    marker = json.loads((candidate / ".exaserve_complete.json").read_text())
    return candidate, marker["manifest_hash"]


def test_model_candidate_publishes_only_to_content_address(tmp_path, monkeypatch):
    local_root = tmp_path / "models"
    local_root.mkdir()
    candidate, digest = _model_candidate(local_root)
    target = content_addressed_model_path("org/model", local_root, digest)
    monkeypatch.setenv("PALS_RANKID", "0")
    receipt = verify_and_publish_model(
        candidate,
        target,
        local_root=local_root,
        model_id="org/model",
        expected_manifest_hash=digest,
        generation=4,
    )
    assert target.is_dir() and not target.is_symlink()
    assert receipt["manifest_hash"] == digest
    assert (target.stat().st_mode & 0o777) == 0o555
    assert ((target / "model.safetensors").stat().st_mode & 0o777) == 0o444
    with pytest.raises(PermissionError):
        (target / "model.safetensors").write_text("mutated")
    with pytest.raises(PermissionError):
        (target / "model.safetensors").unlink()


def test_model_publication_rejects_unhashed_target(tmp_path, monkeypatch):
    local_root = tmp_path / "models"
    local_root.mkdir()
    candidate, digest = _model_candidate(local_root)
    monkeypatch.setenv("PALS_RANKID", "0")
    with pytest.raises(RuntimeError, match="content address"):
        verify_and_publish_model(
            candidate,
            local_root / "org--model",
            local_root=local_root,
            model_id="org/model",
            expected_manifest_hash=digest,
            generation=4,
        )


def test_model_candidate_rejects_descendant_symlink_escape(tmp_path, monkeypatch):
    local_root = tmp_path / "models"
    local_root.mkdir()
    candidate, digest = _model_candidate(local_root)
    external = tmp_path / "external"
    external.write_text("shared")
    (candidate / "escape").symlink_to(external)
    target = content_addressed_model_path("org/model", local_root, digest)
    monkeypatch.setenv("PALS_RANKID", "0")
    with pytest.raises(RuntimeError, match="not proven node-local"):
        verify_and_publish_model(
            candidate,
            target,
            local_root=local_root,
            model_id="org/model",
            expected_manifest_hash=digest,
            generation=4,
        )


def test_model_publication_replaces_symlink_target_without_touching_external(tmp_path, monkeypatch):
    local_root = tmp_path / "models"
    local_root.mkdir()
    candidate, digest = _model_candidate(local_root)
    external = tmp_path / "external"
    external.mkdir()
    (external / "sentinel").write_text("unchanged")
    target = content_addressed_model_path("org/model", local_root, digest)
    target.symlink_to(external, target_is_directory=True)
    monkeypatch.setenv("PALS_RANKID", "0")
    verify_and_publish_model(
        candidate,
        target,
        local_root=local_root,
        model_id="org/model",
        expected_manifest_hash=digest,
        generation=4,
    )
    assert target.is_dir() and not target.is_symlink()
    assert (external / "sentinel").read_text() == "unchanged"


def test_model_quarantine_never_chmods_descendant_symlink_target(tmp_path, monkeypatch):
    local_root = tmp_path / "models"
    local_root.mkdir()
    candidate, digest = _model_candidate(local_root)
    target = content_addressed_model_path("org/model", local_root, digest)
    target.mkdir()
    external = tmp_path / "shared-like"
    external.mkdir()
    external.chmod(0o500)
    (target / "escape").symlink_to(external, target_is_directory=True)
    monkeypatch.setenv("PALS_RANKID", "0")

    verify_and_publish_model(
        candidate,
        target,
        local_root=local_root,
        model_id="org/model",
        expected_manifest_hash=digest,
        generation=4,
    )

    assert stat.S_IMODE(external.stat().st_mode) == 0o500
    assert not (target / "escape").exists()


def test_local_tree_rejects_symlink_component(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (root / "escape").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink escape"):
        validate_node_local_tree(root, local_root=root)


def test_clean_stage_is_non_destructive_and_preserves_concurrent_debris(tmp_path, monkeypatch):
    root = tmp_path / "hf-home"
    root.mkdir()
    stable = root / f"org--model.{('a' * 64)}"
    stable.mkdir()
    (stable / "content").write_text("x")
    candidate = root / f".exaserve_stage.org--model.7.{'c' * 32}"
    candidate.mkdir()
    invalid = root / f".org--model.{'d' * 64}.invalid.7.123.456"
    invalid.mkdir()
    old_generation = root / f".exaserve_stage.org--model.6.{'e' * 32}"
    old_generation.mkdir()
    unrelated = root / f"other--model.{('b' * 64)}"
    unrelated.mkdir()
    monkeypatch.setenv("PALS_RANKID", "0")
    receipt = clean_model_caches_locally(str(root), ["org/model"], generation=7)
    assert receipt["targets"] == receipt["removed_paths"] == []
    assert all(path.is_dir() for path in (candidate, invalid, stable, old_generation, unrelated))


def test_model_result_contract_requires_content_addressed_path():
    digest = "e" * 64
    target = f"/tmp/models/org--model.{digest}"
    model = SimpleNamespace(
        model_id="org/model",
        pipeline_parallel_size=1,
        num_replicas=2,
        replicas=(
            SimpleNamespace(planned_ranks=(0,)),
            SimpleNamespace(planned_ranks=(1,)),
        ),
    )
    plan = SimpleNamespace(
        deployment_id="deployment",
        deployment_plan_hash="b" * 64,
        site_profile_hash="c" * 64,
        local_stage_path="/tmp/models",
        num_nodes=2,
        models=(model,),
        runtime=SimpleNamespace(pp_shard_aware=False),
    )
    binding = SimpleNamespace(
        generation=7,
        allocation_binding_hash="d" * 64,
        rank_to_node=((0, "n0"), (1, "n1")),
    )
    receipts = [
        {
            "schema_version": 1,
            "attempt_id": "a" * 32,
            "result_id": f"{rank + 1:032x}",
            "rank": rank,
            "node": node,
            "generation": 7,
            "model_id": "org/model",
            "manifest_hash": digest,
            "file_count": 2,
            "total_bytes": 3,
            "target": target,
            "model_device_id": 7,
            "model_fs_type": "tmpfs",
            "verification_duration_s": 0.1,
        }
        for rank, node in binding.rank_to_node
    ]
    result = {
        "schema_version": 2,
        "deployment_id": "deployment",
        "generation": 7,
        "deployment_plan_hash": "b" * 64,
        "site_profile_hash": "c" * 64,
        "allocation_binding_hash": "d" * 64,
        "model_bcast_total_s": 1.0,
        "cleanup_receipts": [],
        "model_paths": {"org/model": target},
        "models": [
            {
                "model_id": "org/model",
                "cache_reused": False,
                "shard_aware": False,
                "manifest_hash": digest,
                "stage_manifest_hashes": [],
                "rank_receipts": receipts,
                "duration_s": 0.5,
            }
        ],
    }
    assert validate_model_bcast_result(result, plan=plan, binding=binding) is result
    result["model_paths"]["org/model"] = "/tmp/models/org--model"
    with pytest.raises(RuntimeError, match="content-addressed"):
        validate_model_bcast_result(result, plan=plan, binding=binding)


def test_receipt_validators_reject_coercible_identity():
    with pytest.raises(RuntimeError, match="values are invalid"):
        _validate_cache_probe_result(
            {
                "schema_version": 1,
                "attempt_id": "a" * 32,
                "result_id": "b" * 32,
                "rank": 0,
                "host": 123,
                "path": "/tmp/model",
                "state": "complete",
                "generation": 1,
            }
        )
    with pytest.raises(SourceStagingError, match="values are invalid"):
        _validate_source_receipt(
            {
                "schema_version": 1,
                "attempt_id": "a" * 32,
                "result_id": "b" * 32,
                "rank": 0,
                "node": True,
                "generation": 1,
                "source_manifest_hash": "c" * 64,
                "file_count": 2,
                "total_bytes": 3,
                "published_path": "/tmp/runtime",
                "published_target": "/tmp/runtime",
                "runtime_device_id": 7,
                "state_device_id": 7,
                "runtime_fs_type": "tmpfs",
                "state_fs_type": "tmpfs",
                "qualified_python_path": "/opt/site/python3",
                "qualified_python_sha256": "9" * 64,
                "qualified_python_device_id": 22,
                "qualified_python_fs_type": "squashfs",
                "compatibility_profile_id": "7" * 64,
                "compatibility_manifest_hash": "8" * 64,
                "verification_duration_s": 0.1,
            }
        )
    with pytest.raises(RuntimeError, match="values are invalid"):
        _validate_model_receipt(
            {
                "schema_version": 1,
                "attempt_id": "a" * 32,
                "result_id": "b" * 32,
                "rank": 0,
                "node": "n0",
                "generation": 1,
                "model_id": "org/model",
                "manifest_hash": "c" * 64,
                "file_count": 2,
                "total_bytes": 3,
                "target": "/tmp/model",
                "model_device_id": 7,
                "model_fs_type": "tmpfs",
                "verification_duration_s": float("nan"),
            }
        )


def test_source_and_model_rank_identity_never_guess_zero(monkeypatch):
    for key in ("PALS_RANKID", "PMI_RANK", "SLURM_PROCID", "OMPI_COMM_WORLD_RANK", "PMIX_RANK"):
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(SourceStagingError, match="no MPI/srun rank identity"):
        _rank()
    with pytest.raises(RuntimeError, match="no MPI/srun rank identity"):
        _runtime_rank()


def test_deployment_loads_model_mapping_once_and_actors_get_only_local_paths(monkeypatch):
    from exaserve import server

    plan = SimpleNamespace()
    binding = SimpleNamespace()
    aggregate = {"model_paths": {"org/model": "/tmp/models/org--model." + "a" * 64}}
    monkeypatch.setenv("EXASERVE_MODEL_BCAST_RESULT", "/lus/flare/head/result.json")
    monkeypatch.setenv("EXASERVE_ALLOCATION_BINDING_PATH", "/tmp/runtime/allocation.json")
    monkeypatch.setattr("exaserve.plan.io.load_allocation_binding", lambda _path: binding)
    monkeypatch.setattr("exaserve.state.atomic.strict_json_load_path", lambda _path: aggregate)
    observed = {}

    def validate(value, *, plan, binding):
        observed.update(value=value, plan=plan, binding=binding)

    monkeypatch.setattr("exaserve.model_bcast.validate_model_bcast_result", validate)
    assert server._load_content_addressed_model_paths(plan) == aggregate["model_paths"]
    assert observed == {"value": aggregate, "plan": plan, "binding": binding}
    from exaserve.actor_runtime import _INHERITED_ACTOR_ENV

    assert "EXASERVE_MODEL_BCAST_RESULT" not in _INHERITED_ACTOR_ENV


def test_server_has_no_ambient_model_id_fallback():
    from importlib import resources

    source = (resources.files("exaserve") / "server.py").read_text()
    assert "local_model_path or model_id" not in source
    assert "ambient model IDs and caches are not a fallback" in source


@pytest.mark.parametrize(
    "mapping",
    [
        {},
        {
            "org/model": "/tmp/models/org--model." + "a" * 64,
            "unexpected/model": "/tmp/models/unexpected--model." + "b" * 64,
        },
    ],
)
def test_server_rejects_missing_or_extra_staged_model_mapping(mapping):
    from exaserve.server import _model_path_for_deployment

    config = SimpleNamespace(
        runtime=SimpleNamespace(null_compute=False),
        models=(SimpleNamespace(model_id="org/model"),),
        local_stage_path="/tmp/models",
    )
    with pytest.raises(RuntimeError, match="exact canonical model set"):
        _model_path_for_deployment("org/model", mapping, config)
