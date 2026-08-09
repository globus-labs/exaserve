"""Qualification analysis must reject corrupt evidence instead of undercounting it."""

from __future__ import annotations

import json

import pytest

from eval.tools.analysis_io import (
    EvidenceReadError,
    read_duration_csv,
    read_json_object,
    read_jsonl_objects,
)


def test_json_object_reader_rejects_corrupt_and_non_object_evidence(tmp_path):
    corrupt = tmp_path / "corrupt.json"
    corrupt.write_text("{", encoding="utf-8")
    with pytest.raises(EvidenceReadError, match="corrupt.json"):
        read_json_object(corrupt)

    wrong_shape = tmp_path / "array.json"
    wrong_shape.write_text("[]", encoding="utf-8")
    with pytest.raises(EvidenceReadError, match="must contain an object"):
        read_json_object(wrong_shape)


def test_jsonl_reader_reports_the_exact_bad_line(tmp_path):
    path = tmp_path / "ticks.jsonl"
    path.write_text(json.dumps({"ok": 1}) + "\nnot-json\n", encoding="utf-8")
    with pytest.raises(EvidenceReadError, match=r"ticks\.jsonl:2"):
        read_jsonl_objects(path)


@pytest.mark.parametrize("body", ["1,2,3\n", "1,nope\n", "1,nan\n", "1,-1\n"])
def test_duration_reader_rejects_partial_or_invalid_rows(tmp_path, body):
    path = tmp_path / "durations.csv"
    path.write_text(body, encoding="utf-8")
    with pytest.raises(EvidenceReadError, match=r"durations\.csv:1"):
        read_duration_csv(path)


def test_evidence_readers_accept_the_probe_schemas(tmp_path):
    object_path = tmp_path / "profile.json"
    object_path.write_text('{"hostname":"n0"}', encoding="utf-8")
    assert read_json_object(object_path) == {"hostname": "n0"}

    jsonl_path = tmp_path / "ticks.jsonl"
    jsonl_path.write_text('{"t":1}\n{"t":2}\n', encoding="utf-8")
    assert read_jsonl_objects(jsonl_path) == [{"t": 1}, {"t": 2}]

    csv_path = tmp_path / "durations.csv"
    csv_path.write_text("1,0.5\n2,3\n", encoding="utf-8")
    assert read_duration_csv(csv_path) == [0.5, 3.0]
