import json

import pytest

from exaserve.go_result_contract import LATENCY_QUANTILE_METHOD, read_go_result_stream


def _summary(**overrides):
    payload = {
        "__type__": "summary",
        "requests_completed": 1,
        "requests_scheduled": 1,
        "errors": 0,
        "p50_s": 0.1,
        "p99_s": 0.1,
        "total_input_tokens": 2,
        "total_output_tokens": 3,
        "last_fire_time": 1.0,
        "last_request_start_at": 1.0,
        "last_body_done_at": 1.1,
        "adjusted_run_t0": 0.5,
    }
    payload.update(overrides)
    return payload


def test_go_result_stream_requires_one_terminal_row(tmp_path):
    path = tmp_path / "result.jsonl"
    path.write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match="missing its terminal row"):
        read_go_result_stream(path)


def test_go_result_stream_rejects_data_after_terminal(tmp_path):
    path = tmp_path / "result.jsonl"
    path.write_text(
        json.dumps(_summary()) + "\n" + json.dumps(_summary()) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="after terminal summary"):
        read_go_result_stream(path)


def test_go_result_stream_rejects_inconsistent_summary_counts(tmp_path):
    path = tmp_path / "result.jsonl"
    path.write_text(
        json.dumps(_summary(requests_completed=2, requests_scheduled=1)) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="counts are inconsistent"):
        read_go_result_stream(path)


def test_go_result_stream_never_follows_a_result_symlink(tmp_path):
    target = tmp_path / "target.jsonl"
    target.write_text(json.dumps(_summary()) + "\n", encoding="utf-8")
    link = tmp_path / "result.jsonl"
    link.symlink_to(target)
    with pytest.raises(OSError):
        read_go_result_stream(link)


def test_go_summary_histogram_carries_its_exact_estimator_identity(tmp_path):
    path = tmp_path / "result.jsonl"
    histogram = {
        "bucket_upper_bounds_s": [0.1, 1.0, -1.0],
        "counts": [1, 0, 0],
        "count": 1,
        "sum_s": 0.1,
    }
    path.write_text(
        json.dumps(
            _summary(
                latency_histogram=histogram,
                latency_quantile_method=LATENCY_QUANTILE_METHOD,
            )
        )
        + "\n",
        encoding="utf-8",
    )
    assert (
        read_go_result_stream(path).terminal["latency_quantile_method"] == LATENCY_QUANTILE_METHOD
    )

    path.unlink()
    path.write_text(
        json.dumps(_summary(latency_histogram=histogram)) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="published together"):
        read_go_result_stream(path)
