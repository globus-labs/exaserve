"""WP6 shard-aware staging is bound to the canonical replica topology."""

from __future__ import annotations

import json
import shutil
from types import SimpleNamespace

import pytest

from exaserve.model_bcast import bcast_models, validate_model_bcast_result
from exaserve.model_staging import (
    COMPLETION_MARKER,
    MODEL_WEIGHT_SUFFIXES,
    check_model_exists,
    write_completion_marker,
)
from exaserve.pp_stage import (
    _validate_pp_receipt,
    assign_pp_nodes,
    stage_node_groups,
    subset_launch_prefix,
)
from exaserve.shard_prune import build_stage_dir


def test_stage_bundle_inventory_matches_dereferenced_broadcast(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "config.json").write_text(json.dumps({"num_hidden_layers": 2}))
    shard0 = "model-00001-of-00002.safetensors"
    shard1 = "model-00002-of-00002.safetensors"
    (source / shard0).write_text("stage-zero-weights")
    (source / shard1).write_text("stage-one-weights")
    (source / "model.safetensors.index.json").write_text(
        json.dumps(
            {
                "weight_map": {
                    "model.embed_tokens.weight": shard0,
                    "model.layers.0.self_attn.q_proj.weight": shard0,
                    "model.layers.1.self_attn.q_proj.weight": shard1,
                    "lm_head.weight": shard1,
                }
            }
        )
    )
    (source / "tokenizer.json").write_text("tokenizer")
    nested = source / "metadata" / "chat"
    nested.mkdir(parents=True)
    (nested / "template.json").write_text("template")
    cache = source / ".cache" / "huggingface" / "download"
    cache.mkdir(parents=True)
    (cache / "undeclared-weight-cache.bin").write_bytes(b"x" * 4096)
    (source / ".git").mkdir()
    (source / ".git" / "config").write_text("not-runtime-data")
    (source / "unused.bin").write_text("unused-weight")
    write_completion_marker(source)

    stage = tmp_path / "stage"
    summary = build_stage_dir(source, 2, 0, stage)
    write_completion_marker(stage, source_identity="test-source#pp2/stage0")

    assert summary["shards"] == [shard0]
    assert (stage / shard0).is_symlink()
    assert not (stage / shard1).exists()
    assert not (stage / "unused.bin").exists()
    assert not (stage / ".cache").exists()
    assert not (stage / ".git").exists()
    assert (stage / "metadata" / "chat" / "template.json").is_symlink()
    assert (stage / COMPLETION_MARKER).is_file()

    # copytree(symlinks=False) models tar -h: the receiver gets regular files.
    received = tmp_path / "received"
    shutil.copytree(stage, received, symlinks=False)
    assert check_model_exists(received)
    assert json.loads((received / COMPLETION_MARKER).read_text()) == json.loads(
        (stage / COMPLETION_MARKER).read_text()
    )


def test_stage_bundle_accepts_huggingface_blob_symlinks_inside_model_repository(tmp_path):
    repo = tmp_path / "models--org--model"
    snapshot = repo / "snapshots" / "revision"
    blobs = repo / "blobs"
    snapshot.mkdir(parents=True)
    blobs.mkdir()
    (blobs / "config").write_text(json.dumps({"num_hidden_layers": 2}))
    (blobs / "index").write_text(
        json.dumps(
            {
                "weight_map": {
                    "model.embed_tokens.weight": "model-00001-of-00001.safetensors",
                    "model.layers.0.self_attn.q_proj.weight": (
                        "model-00001-of-00001.safetensors"
                    ),
                    "model.layers.1.self_attn.q_proj.weight": (
                        "model-00001-of-00001.safetensors"
                    ),
                    "lm_head.weight": "model-00001-of-00001.safetensors",
                }
            }
        )
    )
    (blobs / "weights").write_bytes(b"weights")
    (snapshot / "config.json").symlink_to("../../blobs/config")
    (snapshot / "model.safetensors.index.json").symlink_to("../../blobs/index")
    (snapshot / "model-00001-of-00001.safetensors").symlink_to("../../blobs/weights")

    summary = build_stage_dir(snapshot, 2, 0, tmp_path / "stage")
    assert summary["shards"] == ["model-00001-of-00001.safetensors"]


def test_stage_bundle_rejects_metadata_symlink_outside_model_repository(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    outside = tmp_path / "outside.json"
    outside.write_text(json.dumps({"num_hidden_layers": 2}))
    (source / "config.json").symlink_to(outside)

    with pytest.raises(ValueError, match="outside the model repository"):
        build_stage_dir(source, 2, 0, tmp_path / "stage")


def test_stage_bundle_rejects_metadata_symlink_loop(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "config.json").symlink_to("config.json")

    with pytest.raises(ValueError, match="cannot be resolved safely"):
        build_stage_dir(source, 2, 0, tmp_path / "stage")


def test_stage_bundle_excludes_unselected_weights_at_any_depth(tmp_path):
    """Regression for an inventory class exposed by the failed four-node gate.

    Retained evidence attributes that gate's 24.26 GiB transfer to a
    dereferenced top-level `.cache` directory. A nested alternate-format weight
    is the same class of manifest-versus-`tar -h` defect: it is not a selected
    shard and must never enter shared metadata. Exclusion therefore has to be
    by weight suffix at any depth, and the dereferenced payload has to stay
    inside the planned shard budget.
    """
    source = tmp_path / "source"
    source.mkdir()
    (source / "config.json").write_text(json.dumps({"num_hidden_layers": 2}))
    shard0 = "model-00001-of-00002.safetensors"
    shard1 = "model-00002-of-00002.safetensors"
    (source / shard0).write_bytes(b"0" * 4096)
    (source / shard1).write_bytes(b"1" * 4096)
    (source / "model.safetensors.index.json").write_text(
        json.dumps(
            {
                "weight_map": {
                    "model.embed_tokens.weight": shard0,
                    "model.layers.0.self_attn.q_proj.weight": shard0,
                    "model.layers.1.self_attn.q_proj.weight": shard1,
                    "lm_head.weight": shard1,
                }
            }
        )
    )
    (source / "tokenizer.json").write_text("tokenizer")
    original = source / "original"
    original.mkdir()
    # The exact shape of the Llama-3 HF repo: a second full checkpoint plus
    # two small runtime metadata files that DO belong in every stage.
    (original / "consolidated.00.pth").write_bytes(b"p" * 65536)
    (original / "params.json").write_text(json.dumps({"dim": 4096}))
    (original / "tokenizer.model").write_text("sentencepiece")
    # Cover the rest of the suffix set at depth, too.
    (original / "extra.bin").write_bytes(b"b" * 8192)
    (original / "extra.pt").write_bytes(b"t" * 8192)
    (original / "extra.gguf").write_bytes(b"g" * 8192)

    stage = tmp_path / "stage"
    summary = build_stage_dir(source, 2, 0, stage)
    assert summary["shards"] == [shard0]

    # No unselected weight rides along, at any depth.
    carried_weights = sorted(
        path.relative_to(stage).as_posix()
        for path in stage.rglob("*")
        if path.is_file() and path.name.endswith(MODEL_WEIGHT_SUFFIXES)
    )
    assert carried_weights == [shard0]

    # Small shared runtime metadata in the same subdirectory is still carried.
    assert (stage / "original" / "params.json").is_symlink()
    assert (stage / "original" / "tokenizer.model").is_symlink()

    # tar -h dereferences: the payload the receiver actually gets must not
    # exceed the planned shard bytes by more than small metadata.
    transmitted = sum(path.stat().st_size for path in stage.rglob("*") if path.is_file())
    assert transmitted >= summary["bytes"]
    assert transmitted - summary["bytes"] < 65536, (
        f"stage bundle carries {transmitted - summary['bytes']} bytes beyond its "
        f"{summary['bytes']}-byte shard budget"
    )

    received = tmp_path / "received"
    shutil.copytree(stage, received, symlinks=False)
    write_completion_marker(received, source_identity="test-source#pp2/stage0")
    assert check_model_exists(received)


def test_stage_bundle_refuses_nonempty_output(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "config.json").write_text(json.dumps({"num_hidden_layers": 1}))
    (source / "model.safetensors").write_text("weights")
    (source / "model.safetensors.index.json").write_text(
        json.dumps({"weight_map": {"model.layers.0.weight": "model.safetensors"}})
    )
    out = tmp_path / "stage"
    out.mkdir()
    (out / "stale.cache").write_text("stale")

    with pytest.raises(FileExistsError, match="not empty"):
        build_stage_dir(source, 1, 0, out)


def test_pp_node_assignment_is_exact_replica_major_order():
    nodes = ["n2", "n0", "n3", "n1"]
    assert assign_pp_nodes(nodes, 2, 2) == [
        (0, 0, "n2"),
        (0, 1, "n0"),
        (1, 0, "n3"),
        (1, 1, "n1"),
    ]
    assert stage_node_groups(nodes, 2, 2) == {0: ["n2", "n3"], 1: ["n0", "n1"]}
    assert subset_launch_prefix(["n2", "n3"], "pbs")[-1] == "n2,n3"


def test_pp_receipt_rejects_coercible_node_identity():
    receipt = {
        "schema_version": 1,
        "attempt_id": "a" * 32,
        "result_id": "b" * 32,
        "rank": 0,
        "node": 123,
        "generation": 1,
        "model_id": "org/model",
        "manifest_hash": "c" * 64,
        "file_count": 2,
        "total_bytes": 3,
        "target": "/tmp/model",
        "verification_duration_s": 0.1,
        "pp_stage": 0,
    }
    with pytest.raises(RuntimeError, match="values are invalid"):
        _validate_pp_receipt(receipt)


def test_broadcast_uses_plan_ranks_not_binding_prefix(tmp_path, monkeypatch):
    model_id = "org/pp"
    source = tmp_path / "source"
    source.mkdir()
    (source / "config.json").write_text('{"num_attention_heads": 4}')
    (source / "model.safetensors").write_text("weights")

    model = SimpleNamespace(
        model_id=model_id,
        tensor_parallel_size=1,
        pipeline_parallel_size=2,
        max_model_len=128,
        size=8,
        num_replicas=2,
    )
    binding = SimpleNamespace(
        generation=7,
        rank_to_node=((0, "n0"), (1, "n1"), (2, "n2"), (3, "n3")),
    )
    canonical_model = SimpleNamespace(
        model_id=model_id,
        num_replicas=2,
        pipeline_parallel_size=2,
        replicas=(
            SimpleNamespace(planned_ranks=(2, 0)),
            SimpleNamespace(planned_ranks=(3, 1)),
        ),
    )
    deployment = SimpleNamespace(models=(canonical_model,))
    observed = {}

    monkeypatch.setattr("exaserve.model_bcast.compile_bcast", lambda: tmp_path / "bcast")
    monkeypatch.setattr(
        "exaserve.model_bcast.stage_models",
        lambda _models, _storage: {model_id: str(source)},
    )
    monkeypatch.setattr(
        "exaserve.model_bcast.clean_model_caches",
        lambda local, models, nodes, **kwargs: observed.update(
            clean=(local, list(models), nodes, kwargs["binding"])
        ),
    )

    def fake_pp(*args, **kwargs):
        observed["nodes"] = list(args[5])
        return [
            {"manifest_hash": "a" * 64, "receipts": [{"node": "n2"}, {"node": "n3"}]},
            {"manifest_hash": "b" * 64, "receipts": [{"node": "n0"}, {"node": "n1"}]},
        ]

    monkeypatch.setattr("exaserve.pp_stage.stage_pp_sharded", fake_pp)
    bcast_models(
        [model],
        str(tmp_path / "shared"),
        str(tmp_path / "local"),
        4,
        shard_aware=True,
        clean_stage=True,
        binding=binding,
        deployment_plan=deployment,
    )
    assert observed["nodes"] == ["n2", "n0", "n3", "n1"]
    assert observed["clean"] == (str(tmp_path / "local"), [model_id], 4, binding)


def test_shard_aware_aggregate_contract_binds_every_stage_to_planned_nodes():
    import hashlib
    import json

    model_id = "org/pp"
    stage_hashes = ["a" * 64, "b" * 64]
    manifest_hash = hashlib.sha256(
        json.dumps(stage_hashes, separators=(",", ":")).encode()
    ).hexdigest()
    replicas = (
        SimpleNamespace(planned_ranks=(0, 1)),
        SimpleNamespace(planned_ranks=(2, 3)),
    )
    model = SimpleNamespace(
        model_id=model_id,
        pipeline_parallel_size=2,
        num_replicas=2,
        replicas=replicas,
    )
    plan = SimpleNamespace(
        deployment_id="deployment",
        deployment_plan_hash="c" * 64,
        site_profile_hash="d" * 64,
        local_stage_path="/tmp/models",
        num_nodes=4,
        models=(model,),
        runtime=SimpleNamespace(pp_shard_aware=True),
    )
    binding = SimpleNamespace(
        generation=9,
        allocation_binding_hash="e" * 64,
        rank_to_node=((0, "n0"), (1, "n1"), (2, "n2"), (3, "n3")),
    )
    receipts = []
    for stage, nodes in enumerate((("n0", "n2"), ("n1", "n3"))):
        for rank, node in enumerate(nodes):
            receipts.append(
                {
                    "schema_version": 1,
                    "attempt_id": f"{'f' * 32}-stage{stage}",
                    "result_id": f"{stage * 2 + rank + 1:032x}",
                    "rank": rank,
                    "node": node,
                    "generation": 9,
                    "model_id": model_id,
                    "manifest_hash": stage_hashes[stage],
                    "file_count": 2,
                    "total_bytes": 3,
                    "target": "/tmp/models/org--pp",
                    "verification_duration_s": 0.1,
                    "pp_stage": stage,
                }
            )
    result = {
        "schema_version": 1,
        "deployment_id": "deployment",
        "generation": 9,
        "deployment_plan_hash": "c" * 64,
        "site_profile_hash": "d" * 64,
        "allocation_binding_hash": "e" * 64,
        "model_bcast_total_s": 1.0,
        "model_paths": {model_id: "/tmp/models/org--pp"},
        "models": [
            {
                "model_id": model_id,
                "cache_reused": False,
                "shard_aware": True,
                "manifest_hash": manifest_hash,
                "stage_manifest_hashes": stage_hashes,
                "rank_receipts": receipts,
                "duration_s": 0.5,
            }
        ],
    }

    assert validate_model_bcast_result(result, plan=plan, binding=binding) is result
    result["models"][0]["rank_receipts"][1]["node"] = "n1"
    with pytest.raises(RuntimeError, match="topology/identity is invalid"):
        validate_model_bcast_result(result, plan=plan, binding=binding)
