"""Exact-generation tests for import-light vLLM support helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from exaserve.engines.vllm_support import parse_engine_log


def test_engine_log_parser_uses_an_exact_session_and_pid(tmp_path: Path):
    logs = tmp_path / "session_exact" / "logs"
    logs.mkdir(parents=True)
    (logs / "worker-a-123.out").write_text(
        "Loading weights took 1.25 seconds\n"
        "init engine (profile, create kv cache, warmup model) took 2.5 seconds\n",
        encoding="utf-8",
    )

    assert parse_engine_log(123, session_dir=logs.parent) == {
        "weight_load_s": 1.25,
        "kv_cache_init_s": 2.5,
    }
    assert parse_engine_log(124, session_dir=logs.parent) == {}


def test_engine_log_parser_rejects_ambiguous_or_invalid_identity(tmp_path: Path):
    logs = tmp_path / "session_exact" / "logs"
    logs.mkdir(parents=True)
    (logs / "worker-a-123.out").write_text("", encoding="utf-8")
    (logs / "worker-b-123.out").write_text("", encoding="utf-8")

    result = parse_engine_log(123, session_dir=logs.parent)
    assert "ambiguous Ray worker log" in result["engine_log_error"]
    with pytest.raises(ValueError, match="positive integer"):
        parse_engine_log(True, session_dir=logs.parent)


def test_engine_log_parser_has_no_mutable_latest_session_reference():
    source = Path(parse_engine_log.__code__.co_filename).read_text(encoding="utf-8")
    assert "session_latest" not in source
