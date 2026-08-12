"""Transactional source staging does not trust an MPI zero exit alone."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from exaserve.source_staging import (
    SourceStagingError,
    _rank,
    _validate_source_receipt,
    main as source_staging_main,
    tree_manifest,
    validate_source_staging_result,
    verify_and_publish,
)
from exaserve.staging_results import create_result_dir, load_rank_results, write_rank_result
from exaserve.model_bcast import (
    _prepare_model_bcast_source,
    _runtime_rank,
    _source_model_manifest,
    _validate_cache_clean_result,
    _validate_cache_probe_result,
    _validate_model_receipt,
    main as model_bcast_main,
    validate_model_bcast_result,
    verify_and_publish_model,
)
from exaserve.model_staging import write_completion_marker


def _candidate(tmp_path: Path) -> Path:
    package = tmp_path / "candidate" / "exaserve"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("VERSION = 1\n")
    (package / "module.py").write_text("VALUE = 2\n")
    return package


def test_read_only_markerless_model_gets_run_owned_broadcast_manifest(tmp_path, monkeypatch):
    source = tmp_path / "shared" / "revision"
    source.mkdir(parents=True)
    (source / "config.json").write_text("{}")
    (source / "model.safetensors").write_text("weights")
    run_logs = tmp_path / "run-logs"
    run_logs.mkdir()
    monkeypatch.setenv("EXASERVE_RUN_LOG_DIR", str(run_logs))

    def refuse_source_write(*_args, **_kwargs):
        raise PermissionError("read-only model store")

    monkeypatch.setattr("exaserve.model_staging.write_completion_marker", refuse_source_write)
    manifest = _source_model_manifest(source, model_id="org/model")

    assert not (source / ".exaserve_complete.json").exists()
    evidence = list((run_logs / "model-source-manifests").glob("*.json"))
    assert len(evidence) == 1
    import json

    assert json.loads(evidence[0].read_text()) == manifest

    temporary_root = tmp_path / "overlay"
    temporary_root.mkdir()
    broadcast_source = _prepare_model_bcast_source(
        source,
        safe_name="org--model",
        source_manifest=manifest,
        temporary_root=temporary_root,
    )
    assert broadcast_source.name == "org--model"
    assert (broadcast_source / "config.json").is_symlink()
    assert json.loads((broadcast_source / ".exaserve_complete.json").read_text()) == manifest


def _aggregate_source_result(tmp_path: Path, *, nodes=("n0", "n1")) -> dict:
    import hashlib
    import json

    files = [{"path": "__init__.py", "size": 2, "sha256": "f" * 64}]
    canonical = json.dumps(files, sort_keys=True, separators=(",", ":"))
    source_hash = hashlib.sha256(canonical.encode()).hexdigest()
    attempt = "a" * 32
    rank_result_dir = tmp_path / "staging_rank_results" / f"source.{attempt}"
    rank_result_dir.mkdir(parents=True)
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
            "published_path": "/tmp/exaserve_src",
            "published_target": f"/tmp/exaserve_stage.7.{source_hash[:12]}.{attempt}",
            "verification_duration_s": 0.1,
        }
        for rank, node in enumerate(nodes)
    ]
    return {
        "schema_version": 1,
        "deployment_id": "deployment",
        "generation": 7,
        "deployment_plan_hash": "b" * 64,
        "site_profile_hash": "c" * 64,
        "allocation_binding_hash": "d" * 64,
        "source_manifest_hash": source_hash,
        "file_count": 1,
        "total_bytes": 2,
        "files": files,
        "duration_s": 1.25,
        "rank_result_dir": str(rank_result_dir),
        "rank_receipts": receipts,
    }


def test_source_result_contract_validates_full_inventory_and_rank_binding(tmp_path):
    result = _aggregate_source_result(tmp_path)
    assert (
        validate_source_staging_result(
            result,
            expected_deployment_id="deployment",
            expected_generation=7,
            expected_plan_hash="b" * 64,
            expected_site_profile_hash="c" * 64,
            expected_binding_hash="d" * 64,
            expected_rank_to_node=((0, "n0.example"), (1, "n1.example")),
            expected_run_dir=tmp_path,
        )
        is result
    )


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda value: value.update(schema_version=True), "values are invalid"),
        (lambda value: value["files"][0].update(size=True), "file 0 values are invalid"),
        (lambda value: value.update(file_count=2), "inventory hash/count/bytes mismatch"),
        (lambda value: value["rank_receipts"][0].update(node=True), "receipt values are invalid"),
        (
            lambda value: value["rank_receipts"][1].update(attempt_id="e" * 32),
            "inconsistent attempt/result identity",
        ),
    ],
)
def test_source_result_contract_rejects_forged_evidence(tmp_path, mutate, message):
    result = _aggregate_source_result(tmp_path)
    mutate(result)
    with pytest.raises(SourceStagingError, match=message):
        validate_source_staging_result(result)


def test_tree_manifest_covers_path_size_and_content(tmp_path):
    package = _candidate(tmp_path)
    first = tree_manifest(package)
    assert first["file_count"] == 2
    assert first["total_bytes"] > 0
    assert {entry["path"] for entry in first["files"]} == {"__init__.py", "module.py"}

    (package / "module.py").write_text("VALUE = 3\n")
    assert tree_manifest(package)["source_manifest_hash"] != first["source_manifest_hash"]


def test_verify_then_atomic_publish_emits_rank_identity(tmp_path, monkeypatch):
    package = _candidate(tmp_path)
    manifest = tree_manifest(package)
    stable = tmp_path / "published"
    monkeypatch.setenv("PALS_RANKID", "7")
    receipt = verify_and_publish(
        package,
        manifest["source_manifest_hash"],
        manifest["file_count"],
        manifest["total_bytes"],
        11,
        stable,
    )
    assert stable.is_symlink()
    assert stable.resolve() == package.parent.resolve()
    assert receipt["rank"] == 7 and receipt["generation"] == 11


def test_rank_results_are_attempt_scoped_atomic_artifacts(tmp_path):
    attempt = "a" * 32
    result_dir = create_result_dir(tmp_path, "source", attempt)
    path = write_rank_result(
        result_dir,
        attempt_id=attempt,
        payload={"rank": 2, "node": "n2", "generation": 4},
    )

    assert path.is_file()
    results = load_rank_results(result_dir, attempt_id=attempt)
    assert len(results) == 1
    assert results[0]["rank"] == 2
    assert results[0]["attempt_id"] == attempt


@pytest.mark.parametrize("reserved", ["schema_version", "attempt_id", "result_id"])
def test_rank_result_payload_cannot_override_envelope_identity(tmp_path, reserved):
    attempt = "f" * 32
    result_dir = create_result_dir(tmp_path, "reserved", attempt)
    with pytest.raises(RuntimeError, match="reserved fields"):
        write_rank_result(
            result_dir,
            attempt_id=attempt,
            payload={"rank": 0, reserved: "forged"},
        )


def test_rank_result_directory_must_not_be_a_symlink(tmp_path):
    actual = tmp_path / "actual"
    actual.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(actual, target_is_directory=True)
    with pytest.raises(RuntimeError, match="real directory"):
        write_rank_result(alias, attempt_id="f" * 32, payload={"rank": 0})


def test_cache_probe_receipt_rejects_coercible_host_identity():
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


def test_model_receipt_rejects_nonfinite_duration():
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
                "verification_duration_s": float("nan"),
            }
        )


@pytest.mark.parametrize(
    "field,value",
    [("schema_version", True), ("attempt_id", "stale"), ("result_id", "wrong")],
)
def test_model_receipt_rejects_invalid_result_envelope(field, value):
    receipt = {
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
        "verification_duration_s": 0.1,
    }
    receipt[field] = value
    with pytest.raises(RuntimeError, match="values are invalid"):
        _validate_model_receipt(receipt)


def _aggregate_model_result():
    from types import SimpleNamespace

    model = SimpleNamespace(
        model_id="org/model",
        pipeline_parallel_size=1,
        num_replicas=1,
        replicas=(SimpleNamespace(planned_ranks=(0,)),),
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
            "manifest_hash": "e" * 64,
            "file_count": 2,
            "total_bytes": 3,
            "target": "/tmp/models/org--model",
            "verification_duration_s": 0.1,
        }
        for rank, node in binding.rank_to_node
    ]
    result = {
        "schema_version": 1,
        "deployment_id": "deployment",
        "generation": 7,
        "deployment_plan_hash": "b" * 64,
        "site_profile_hash": "c" * 64,
        "allocation_binding_hash": "d" * 64,
        "model_bcast_total_s": 1.0,
        "model_paths": {"org/model": "/tmp/models/org--model"},
        "models": [
            {
                "model_id": "org/model",
                "cache_reused": False,
                "shard_aware": False,
                "manifest_hash": "e" * 64,
                "stage_manifest_hashes": [],
                "rank_receipts": receipts,
                "duration_s": 0.5,
            }
        ],
    }
    return result, plan, binding


def test_model_bcast_result_contract_validates_full_receipt_set():
    result, plan, binding = _aggregate_model_result()
    assert validate_model_bcast_result(result, plan=plan, binding=binding) is result


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda value: value.update(schema_version=True), "values are invalid"),
        (
            lambda value: value["models"][0]["rank_receipts"][1].update(rank=True),
            "receipt values are invalid",
        ),
        (
            lambda value: value["models"][0]["rank_receipts"][1].update(node="wrong"),
            "topology/identity is invalid",
        ),
        (
            lambda value: value["models"][0]["rank_receipts"][1].update(total_bytes=4),
            "disagree on content inventory",
        ),
    ],
)
def test_model_bcast_result_contract_rejects_forged_evidence(mutate, message):
    result, plan, binding = _aggregate_model_result()
    mutate(result)
    with pytest.raises(RuntimeError, match=message):
        validate_model_bcast_result(result, plan=plan, binding=binding)


def test_source_receipt_rejects_coercible_node_identity():
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
                "published_path": "/tmp/exaserve_src",
                "published_target": "/tmp/exaserve_stage",
                "verification_duration_s": 0.1,
            }
        )


def test_rank_result_loader_rejects_stale_attempt_identity(tmp_path):
    attempt = "b" * 32
    result_dir = create_result_dir(tmp_path, "model", attempt)
    write_rank_result(
        result_dir,
        attempt_id=attempt,
        payload={"rank": 0, "node": "n0"},
    )

    with pytest.raises(RuntimeError, match="stale or inconsistent identity"):
        load_rank_results(result_dir, attempt_id="c" * 32)


def test_rank_result_loader_rejects_boolean_rank_alias(tmp_path):
    from exaserve.state.atomic import atomic_write_json, strict_json_loads

    attempt = "b" * 32
    result_dir = create_result_dir(tmp_path, "model", attempt)
    path = write_rank_result(
        result_dir,
        attempt_id=attempt,
        payload={"rank": 0, "node": "n0"},
    )
    payload = strict_json_loads(path.read_text(encoding="utf-8"))
    payload["rank"] = False
    atomic_write_json(path, payload)

    with pytest.raises(RuntimeError, match="stale or inconsistent identity"):
        load_rank_results(result_dir, attempt_id=attempt)


def test_source_verifier_publishes_result_without_stdout_protocol(tmp_path, monkeypatch, capsys):
    package = _candidate(tmp_path)
    manifest = tree_manifest(package)
    attempt = "d" * 32
    result_dir = create_result_dir(tmp_path, "source-cli", attempt)
    monkeypatch.setenv("PALS_RANKID", "0")

    assert (
        source_staging_main(
            [
                "--verify-and-publish",
                str(package),
                "--expected-hash",
                manifest["source_manifest_hash"],
                "--expected-files",
                str(manifest["file_count"]),
                "--expected-bytes",
                str(manifest["total_bytes"]),
                "--generation",
                "9",
                "--stable-path",
                str(tmp_path / "stable"),
                "--result-dir",
                str(result_dir),
                "--attempt-id",
                attempt,
            ]
        )
        == 0
    )
    assert capsys.readouterr().out == ""
    assert load_rank_results(result_dir, attempt_id=attempt)[0]["generation"] == 9


def test_model_cache_probe_publishes_result_without_stdout_protocol(tmp_path, monkeypatch, capsys):
    attempt = "e" * 32
    result_dir = create_result_dir(tmp_path, "model-cache-cli", attempt)
    monkeypatch.setenv("PALS_RANKID", "3")

    assert (
        model_bcast_main(
            [
                "--probe-cache",
                str(tmp_path / "missing-model"),
                "--generation",
                "12",
                "--result-dir",
                str(result_dir),
                "--attempt-id",
                attempt,
            ]
        )
        == 0
    )
    assert capsys.readouterr().out == ""
    result = load_rank_results(result_dir, attempt_id=attempt)[0]
    assert result["rank"] == 3
    assert result["generation"] == 12
    assert result["state"] == "missing"


def test_clean_stage_removes_only_exact_plan_owned_model_paths(tmp_path, monkeypatch, capsys):
    root = tmp_path / "hf-home"
    root.mkdir()
    owned = [
        root / "org--model",
        root / f".exaserve_stage.org--model.7.{'a' * 32}",
        root / f".exaserve_pp_candidate.org--model.7.{'b' * 32}.stage0",
        root / ".org--model.invalid.7.123.456",
    ]
    for path in owned:
        path.mkdir()
        (path / "content").write_text("x")
    unrelated = root / f".exaserve_stage.other--model.7.{'c' * 32}"
    unrelated.mkdir()
    (unrelated / "keep").write_text("safe")
    overlapping = root / f".exaserve_stage.org--model.extra.7.{'d' * 32}"
    overlapping.mkdir()
    (overlapping / "keep").write_text("safe")
    attempt = "c" * 32
    result_dir = create_result_dir(tmp_path, "model-clean-cli", attempt)
    monkeypatch.setenv("PALS_RANKID", "0")

    assert (
        model_bcast_main(
            [
                "--clean-cache-root",
                str(root),
                "--clean-model-id",
                "org/model",
                "--generation",
                "7",
                "--result-dir",
                str(result_dir),
                "--attempt-id",
                attempt,
            ]
        )
        == 0
    )
    assert capsys.readouterr().out == ""
    receipt = _validate_cache_clean_result(load_rank_results(result_dir, attempt_id=attempt)[0])
    assert receipt["targets"] == [str(root / "org--model")]
    assert set(receipt["removed_paths"]) == {str(path) for path in owned}
    assert all(not path.exists() for path in owned)
    assert (unrelated / "keep").read_text() == "safe"
    assert (overlapping / "keep").read_text() == "safe"


@pytest.mark.parametrize(("root", "model_id"), [("/", "org/model"), ("/tmp/cache", ".")])
def test_clean_stage_rejects_broad_root_or_unsafe_model_name(root, model_id):
    from exaserve.model_bcast import clean_model_caches_locally

    with pytest.raises(RuntimeError, match="unsafe"):
        clean_model_caches_locally(root, [model_id], generation=1)


def test_wrong_content_never_replaces_the_previous_publication(tmp_path, monkeypatch):
    monkeypatch.setenv("PALS_RANKID", "0")
    first = _candidate(tmp_path / "first")
    first_manifest = tree_manifest(first)
    stable = tmp_path / "published"
    verify_and_publish(
        first,
        first_manifest["source_manifest_hash"],
        first_manifest["file_count"],
        first_manifest["total_bytes"],
        1,
        stable,
    )
    original = stable.resolve()

    second = _candidate(tmp_path / "second")
    (second / "module.py").write_text("corrupt\n")
    with pytest.raises(SourceStagingError, match="manifest mismatch"):
        verify_and_publish(
            second,
            first_manifest["source_manifest_hash"],
            first_manifest["file_count"],
            first_manifest["total_bytes"],
            2,
            stable,
        )
    assert stable.resolve() == original


def test_unresolved_symlink_is_not_a_hashable_source_artifact(tmp_path):
    package = _candidate(tmp_path)
    os.symlink("module.py", package / "alias.py")
    with pytest.raises(SourceStagingError, match="unresolved symlink"):
        tree_manifest(package)


def test_source_publication_refuses_to_guess_rank_zero(tmp_path, monkeypatch):
    package = _candidate(tmp_path)
    manifest = tree_manifest(package)
    for key in ("PALS_RANKID", "PMI_RANK", "SLURM_PROCID", "OMPI_COMM_WORLD_RANK", "PMIX_RANK"):
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(SourceStagingError, match="no MPI/srun rank identity"):
        verify_and_publish(
            package,
            manifest["source_manifest_hash"],
            manifest["file_count"],
            manifest["total_bytes"],
            11,
            tmp_path / "published",
        )


def test_native_staging_rank_identity_rejects_negative_values(monkeypatch):
    monkeypatch.setenv("PALS_RANKID", "-1")
    with pytest.raises(SourceStagingError, match="non-negative"):
        _rank()
    with pytest.raises(RuntimeError, match="non-negative"):
        _runtime_rank()


def test_model_candidate_is_verified_before_atomic_publication(tmp_path, monkeypatch):
    candidate = tmp_path / "candidate" / "org--model"
    candidate.mkdir(parents=True)
    (candidate / "config.json").write_text("{}")
    (candidate / "model.safetensors").write_text("weights")
    write_completion_marker(candidate, source_identity="hf:org/model@revision")
    import json

    marker = json.loads((candidate / ".exaserve_complete.json").read_text())
    target = tmp_path / "models" / "org--model"
    monkeypatch.setenv("PALS_RANKID", "0")
    receipt = verify_and_publish_model(
        candidate,
        target,
        model_id="org/model",
        expected_manifest_hash=marker["manifest_hash"],
        generation=4,
    )
    assert target.is_dir() and not candidate.exists()
    assert receipt["manifest_hash"] == marker["manifest_hash"]


def test_corrupt_model_candidate_does_not_replace_valid_target(tmp_path):
    target = tmp_path / "models" / "org--model"
    target.mkdir(parents=True)
    (target / "config.json").write_text("{}")
    (target / "model.safetensors").write_text("valid")
    write_completion_marker(target, source_identity="hf:org/model@revision")
    import json

    expected = json.loads((target / ".exaserve_complete.json").read_text())["manifest_hash"]
    candidate = tmp_path / "candidate" / "org--model"
    candidate.mkdir(parents=True)
    (candidate / "config.json").write_text("{}")
    (candidate / "model.safetensors").write_text("wrong")
    write_completion_marker(candidate, source_identity="hf:other")
    with pytest.raises(RuntimeError, match="expected"):
        verify_and_publish_model(
            candidate, target, model_id="org/model", expected_manifest_hash=expected, generation=4
        )
    assert (target / "model.safetensors").read_text() == "valid"


def test_model_publication_never_reuses_a_symlink_target(tmp_path, monkeypatch):
    external = tmp_path / "external" / "org--model"
    external.mkdir(parents=True)
    (external / "config.json").write_text("{}")
    (external / "model.safetensors").write_text("weights")
    write_completion_marker(external, source_identity="hf:org/model@revision")
    import json

    expected = json.loads((external / ".exaserve_complete.json").read_text())["manifest_hash"]
    candidate = tmp_path / "candidate" / "org--model"
    candidate.mkdir(parents=True)
    (candidate / "config.json").write_text("{}")
    (candidate / "model.safetensors").write_text("weights")
    write_completion_marker(candidate, source_identity="hf:org/model@revision")
    target = tmp_path / "models" / "org--model"
    target.parent.mkdir()
    target.symlink_to(external, target_is_directory=True)
    monkeypatch.setenv("PALS_RANKID", "0")

    verify_and_publish_model(
        candidate,
        target,
        model_id="org/model",
        expected_manifest_hash=expected,
        generation=4,
    )

    assert target.is_dir() and not target.is_symlink()
    assert external.is_dir()
    quarantined = list(target.parent.glob(".org--model.invalid.4.*"))
    assert len(quarantined) == 1 and quarantined[0].is_symlink()
