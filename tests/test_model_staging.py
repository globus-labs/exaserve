"""PR-005 acceptance: model completeness/manifest checks (hermetic, no net)."""

from __future__ import annotations

import json
import copy
import hashlib
import os
from pathlib import Path
import sys
import types

import pytest

from exaserve.model_staging import (
    COMPLETION_MARKER,
    _resolve_hf_cache_snapshot,
    _validate_model_dir,
    build_model_manifest,
    check_model_exists,
    download_model,
    ensure_node_local_directory,
    get_model_dir_state,
    load_model_config,
    validate_model_manifest,
    validate_tensor_parallel_compatibility,
    validate_vllm_modelinfo_seed_coverage,
)


def _complete_single_file_model(path):
    path.mkdir(parents=True)
    (path / "config.json").write_text("{}")
    (path / "model.safetensors").write_text("weights")


def test_vllm_modelinfo_seed_coverage_accepts_only_reviewed_architectures(tmp_path):
    model = tmp_path / "model"
    model.mkdir()
    config = model / "config.json"
    supported = frozenset({"LlamaForCausalLM", "GptOssForCausalLM"})

    config.write_text(json.dumps({"architectures": ["LlamaForCausalLM"]}))
    assert validate_vllm_modelinfo_seed_coverage("org/model", model, supported) == (
        "LlamaForCausalLM",
    )

    for architectures in (
        None,
        [],
        "LlamaForCausalLM",
        ["LlamaForCausalLM", "LlamaForCausalLM"],
        ["UnknownForCausalLM"],
    ):
        config.write_text(json.dumps({"architectures": architectures}))
        with pytest.raises(RuntimeError, match="architectures|unreviewed"):
            validate_vllm_modelinfo_seed_coverage("org/model", model, supported)


def test_node_local_directory_creation_rejects_intermediate_home_symlink(tmp_path):
    local = tmp_path / "local"
    local.mkdir()
    escape = local / "escape"
    escape.symlink_to("/home", target_is_directory=True)
    escaped_name = f".exaserve-should-not-exist-{os.getpid()}"
    escaped = Path("/home") / escaped_name
    assert not escaped.exists()

    with pytest.raises(ValueError, match="symlink|shared storage"):
        ensure_node_local_directory(
            escape / escaped_name,
            shared_roots=(Path("/home"), Path("/lus/flare")),
        )

    assert not escaped.exists()


def test_node_local_directory_creation_never_writes_through_declared_shared_alias(tmp_path):
    local = tmp_path / "local"
    shared = tmp_path / "declared-shared"
    local.mkdir()
    shared.mkdir()
    (local / "escape").symlink_to(shared, target_is_directory=True)

    with pytest.raises(ValueError, match="symlink|shared storage"):
        ensure_node_local_directory(
            local / "escape" / "created",
            shared_roots=(shared,),
        )

    assert not (shared / "created").exists()


def test_single_file_model_is_complete_and_gets_marker(tmp_path):
    m = tmp_path / "flat"
    _complete_single_file_model(m)
    assert check_model_exists(m) is True
    assert (m / COMPLETION_MARKER).is_file()  # upgraded in place
    manifest = json.loads((m / COMPLETION_MARKER).read_text())
    assert manifest["version"] == 2
    assert manifest["file_count"] == 2
    assert len(manifest["manifest_hash"]) == 64


def test_manifest_detects_same_size_corruption_and_trailing_files(tmp_path):
    m = tmp_path / "model"
    _complete_single_file_model(m)
    assert check_model_exists(m)
    (m / "model.safetensors").write_text("WEIGHTS")  # same size, new content
    assert check_model_exists(m) is False
    (m / "model.safetensors").write_text("weights")
    assert check_model_exists(m) is True
    (m / "unexpected.tmp").write_text("trailing")
    assert check_model_exists(m) is False


def test_large_weight_identity_hashes_interior_bytes(tmp_path):
    model = tmp_path / "large"
    model.mkdir()
    (model / "config.json").write_text("{}")
    weight = model / "model.safetensors"
    weight.write_bytes(b"a" * (3 * 1024 * 1024))
    before = build_model_manifest(model, source_identity="test@one")
    assert {entry["hash_kind"] for entry in before["files"]} == {"sha256-full"}
    with weight.open("r+b") as handle:
        handle.seek(1536 * 1024)
        handle.write(b"b")
    after = build_model_manifest(model, source_identity="test@one")
    assert after["manifest_hash"] != before["manifest_hash"]
    before_weight = next(item for item in before["files"] if item["path"] == weight.name)
    after_weight = next(item for item in after["files"] if item["path"] == weight.name)
    assert after_weight["sha256"] != before_weight["sha256"]


def test_legacy_sampled_marker_migrates_to_full_hash_identity(tmp_path):
    model = tmp_path / "legacy"
    model.mkdir()
    config = model / "config.json"
    weight = model / "model.safetensors"
    config.write_text("{}")
    weight.write_bytes(b"w" * (3 * 1024 * 1024))
    sample_bytes = 1 << 20
    weight_bytes = weight.read_bytes()
    files = [
        {
            "path": "config.json",
            "size": config.stat().st_size,
            "hash_kind": "sha256-full",
            "sha256": hashlib.sha256(config.read_bytes()).hexdigest(),
        },
        {
            "path": "model.safetensors",
            "size": len(weight_bytes),
            "hash_kind": f"sha256-first-last-{sample_bytes}",
            "sha256": hashlib.sha256(
                weight_bytes[:sample_bytes] + weight_bytes[-sample_bytes:]
            ).hexdigest(),
        },
    ]
    legacy = {
        "version": 2,
        "kind": "full_model",
        "source_identity": "legacy@test",
        "file_count": len(files),
        "total_bytes": sum(item["size"] for item in files),
        "manifest_hash": hashlib.sha256(
            json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "files": files,
    }
    (model / COMPLETION_MARKER).write_text(json.dumps(legacy))
    with pytest.raises(ValueError, match="hash_kind"):
        validate_model_manifest(legacy)
    assert check_model_exists(model)
    migrated = json.loads((model / COMPLETION_MARKER).read_text())
    assert all(item["hash_kind"] == "sha256-full" for item in migrated["files"])
    assert migrated["manifest_hash"] != legacy["manifest_hash"]


def test_model_config_must_be_an_object(tmp_path):
    model = tmp_path / "model"
    model.mkdir()
    (model / "config.json").write_text("[]", encoding="utf-8")
    with pytest.raises(ValueError, match="JSON object"):
        load_model_config(model)


def test_boolean_attention_head_count_is_rejected(tmp_path):
    model = tmp_path / "model"
    model.mkdir()
    (model / "config.json").write_text(json.dumps({"num_attention_heads": True}), encoding="utf-8")
    with pytest.raises(RuntimeError, match="invalid num_attention_heads"):
        validate_tensor_parallel_compatibility("org/model", model, 2)


@pytest.mark.parametrize(
    ("mutate", "description"),
    [
        (lambda value: value.update(file_count="2"), "coercible count"),
        (lambda value: value["files"][0].update(size="2"), "coercible size"),
        (lambda value: value["files"][0].update(path="../outside"), "unsafe path"),
        (lambda value: value.update(source_identity=True), "coercible source identity"),
    ],
)
def test_v2_manifest_rejects_malformed_contract_fields(tmp_path, mutate, description):
    m = tmp_path / "model"
    _complete_single_file_model(m)
    assert check_model_exists(m)
    marker = m / COMPLETION_MARKER
    payload = copy.deepcopy(json.loads(marker.read_text(encoding="utf-8")))
    mutate(payload)
    marker.write_text(json.dumps(payload), encoding="utf-8")
    assert check_model_exists(m) is False, description


def test_missing_shard_is_partial_not_complete(tmp_path):
    # PR-005 flagship: index references 3 shards, only 1 present.
    m = tmp_path / "sharded"
    m.mkdir()
    (m / "config.json").write_text("{}")
    (m / "model-00001-of-00003.safetensors").write_text("shard1")
    (m / "model.safetensors.index.json").write_text(
        json.dumps(
            {
                "weight_map": {
                    "a": "model-00001-of-00003.safetensors",
                    "b": "model-00002-of-00003.safetensors",
                    "c": "model-00003-of-00003.safetensors",
                }
            }
        )
    )
    complete, reason = _validate_model_dir(m)
    assert complete is False and "missing shard" in reason
    assert get_model_dir_state(m) == "partial"
    assert check_model_exists(m) is False


def test_shard_index_requires_a_typed_nonempty_weight_map(tmp_path):
    m = tmp_path / "bad-index"
    m.mkdir()
    (m / "config.json").write_text("{}")
    (m / "model.safetensors").write_text("weights")
    index = m / "model.safetensors.index.json"
    for payload in ({"weight_map": []}, {"weight_map": {"a": "../outside"}}, []):
        index.write_text(json.dumps(payload), encoding="utf-8")
        complete, reason = _validate_model_dir(m)
        assert complete is False
        assert "weight_map" in reason or "JSON object" in reason


def test_all_shards_present_is_complete(tmp_path):
    m = tmp_path / "full"
    m.mkdir()
    (m / "config.json").write_text("{}")
    shards = {f"w{i}": f"model-0000{i}-of-00002.safetensors" for i in (1, 2)}
    for fname in set(shards.values()):
        (m / fname).write_text("x")
    (m / "model.safetensors.index.json").write_text(json.dumps({"weight_map": shards}))
    assert _validate_model_dir(m)[0] is True
    assert check_model_exists(m) is True


def test_no_config_or_no_weights_is_incomplete(tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir()
    assert check_model_exists(empty) is False
    cfg_only = tmp_path / "cfg"
    cfg_only.mkdir()
    (cfg_only / "config.json").write_text("{}")
    assert check_model_exists(cfg_only) is False  # no weights


def test_hf_snapshot_without_ref_must_be_unambiguous(tmp_path):
    cache = tmp_path / "models--org--name"
    snaps = cache / "snapshots"
    snaps.mkdir(parents=True)
    only = snaps / "commit-a"
    only.mkdir()
    assert _resolve_hf_cache_snapshot(cache) == only
    (snaps / "commit-b").mkdir()
    with pytest.raises(RuntimeError, match="refusing to select a model revision by mtime"):
        _resolve_hf_cache_snapshot(cache)


def test_download_pins_immutable_revision_and_publishes_once(tmp_path, monkeypatch):
    calls = []

    class FakeApi:
        def model_info(self, model_id):
            assert model_id == "org/model"
            return types.SimpleNamespace(sha="a" * 40)

    def fake_download(**kwargs):
        calls.append(kwargs)
        destination = __import__("pathlib").Path(kwargs["local_dir"])
        _complete_single_file_model(destination)

    monkeypatch.setitem(
        sys.modules,
        "huggingface_hub",
        types.SimpleNamespace(HfApi=FakeApi, snapshot_download=fake_download),
    )
    target = tmp_path / "model"
    assert download_model("org/model", target) == str(target)
    assert calls[0]["revision"] == "a" * 40
    assert check_model_exists(target)
    manifest = json.loads((target / COMPLETION_MARKER).read_text())
    assert manifest["source_identity"] == f"hf:org/model@{'a' * 40}"
    assert not list(tmp_path.glob(".model.staging.*"))


def test_failed_download_never_replaces_existing_partial_tree(tmp_path, monkeypatch):
    class FakeApi:
        def model_info(self, _model_id):
            return types.SimpleNamespace(sha="b" * 40)

    def incomplete_download(**kwargs):
        destination = __import__("pathlib").Path(kwargs["local_dir"])
        destination.mkdir(parents=True)
        (destination / "config.json").write_text("{}")

    monkeypatch.setitem(
        sys.modules,
        "huggingface_hub",
        types.SimpleNamespace(HfApi=FakeApi, snapshot_download=incomplete_download),
    )
    target = tmp_path / "model"
    target.mkdir()
    (target / "operator-note").write_text("preserve")

    with pytest.raises(RuntimeError, match="failed validation"):
        download_model("org/model", target)
    assert (target / "operator-note").read_text() == "preserve"
    assert not list(tmp_path.glob(".model.staging.*"))


def test_publication_failure_rolls_back_existing_partial_tree(tmp_path, monkeypatch):
    class FakeApi:
        def model_info(self, _model_id):
            return types.SimpleNamespace(sha="c" * 40)

    def complete_download(**kwargs):
        destination = __import__("pathlib").Path(kwargs["local_dir"])
        _complete_single_file_model(destination)

    monkeypatch.setitem(
        sys.modules,
        "huggingface_hub",
        types.SimpleNamespace(HfApi=FakeApi, snapshot_download=complete_download),
    )
    target = tmp_path / "model"
    target.mkdir()
    (target / "operator-note").write_text("preserve", encoding="utf-8")
    original_replace = __import__("os").replace

    def fail_final_publish(source, destination):
        if ".model.staging." in str(source) and __import__("pathlib").Path(destination) == target:
            raise OSError("injected publication failure")
        return original_replace(source, destination)

    monkeypatch.setattr("os.replace", fail_final_publish)
    with pytest.raises(OSError, match="injected publication failure"):
        download_model("org/model", target)
    assert (target / "operator-note").read_text(encoding="utf-8") == "preserve"
