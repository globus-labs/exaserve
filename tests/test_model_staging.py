"""PR-005 acceptance: model completeness/manifest checks (hermetic, no net)."""

from __future__ import annotations

import json

from exaserve.model_staging import (
    COMPLETION_MARKER,
    _resolve_hf_cache_snapshot,
    _validate_model_dir,
    check_model_exists,
    get_model_dir_state,
)


def _complete_single_file_model(path):
    path.mkdir(parents=True)
    (path / "config.json").write_text("{}")
    (path / "model.safetensors").write_text("weights")


def test_single_file_model_is_complete_and_gets_marker(tmp_path):
    m = tmp_path / "flat"
    _complete_single_file_model(m)
    assert check_model_exists(m) is True
    assert (m / COMPLETION_MARKER).is_file()  # upgraded in place
    manifest = json.loads((m / COMPLETION_MARKER).read_text())
    assert manifest["file_count"] == 2


def test_missing_shard_is_partial_not_complete(tmp_path):
    # PR-005 flagship: index references 3 shards, only 1 present.
    m = tmp_path / "sharded"
    m.mkdir()
    (m / "config.json").write_text("{}")
    (m / "model-00001-of-00003.safetensors").write_text("shard1")
    (m / "model.safetensors.index.json").write_text(json.dumps({
        "weight_map": {
            "a": "model-00001-of-00003.safetensors",
            "b": "model-00002-of-00003.safetensors",
            "c": "model-00003-of-00003.safetensors",
        }
    }))
    complete, reason = _validate_model_dir(m)
    assert complete is False and "missing shard" in reason
    assert get_model_dir_state(m) == "partial"
    assert check_model_exists(m) is False


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


def test_hf_snapshot_fallback_picks_newest_not_lexicographic(tmp_path):
    import os
    import time

    cache = tmp_path / "models--org--name"
    snaps = cache / "snapshots"
    snaps.mkdir(parents=True)
    # "aaa" sorts first lexicographically but is OLDER; "zzz" is newest.
    old = snaps / "aaa_old"
    new = snaps / "zzz_new"
    old.mkdir()
    new.mkdir()
    now = time.time()
    os.utime(old, (now - 1000, now - 1000))
    os.utime(new, (now, now))
    assert _resolve_hf_cache_snapshot(cache) == new
