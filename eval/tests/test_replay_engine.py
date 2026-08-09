import os

import pytest

os.environ.setdefault("MPI4PY_RC_INITIALIZE", "0")

from eval.lib.replay_engine import (
    TraceRequest,
    _summarize_run_results,
    _wait_for_direct_targets,
)


def test_summarize_run_results_raw_rows() -> None:
    request = TraceRequest(
        timestamp=0.0,
        model="test/model",
        prompt="hello",
        input_len=8,
        output_len=4,
        tensor_parallel_size=1,
        req_id="req-1",
    )
    run_results = [
        (request, 1.25, True, "", 1.25, 8, 4),
        (request, 2.50, False, "timeout", 2.50, None, None),
    ]

    summary = _summarize_run_results(0, run_results, requests_scheduled=2, duration_s=5.0)

    assert summary["run_index"] == 0
    assert summary["requests_completed"] == 2
    assert summary["requests_scheduled"] == 2
    assert summary["successes"] == 1
    assert summary["errors"] == 1
    assert summary["rps"] == 0.4
    assert summary["success_rps"] == 0.2
    assert summary["p50_s"] == 1.25
    assert summary["p99_s"] == 1.25


def test_summarize_run_results_sum_only_dict() -> None:
    run_results = {
        "requests_completed": 12,
        "requests_scheduled": 16,
        "errors": 3,
        "total_input_tokens": 10,
        "total_output_tokens": 5,
        "p50_s": 0.9,
        "p99_s": 1.8,
    }

    summary = _summarize_run_results(2, run_results, requests_scheduled=16, duration_s=4.0)

    assert summary["run_index"] == 2
    assert summary["requests_completed"] == 12
    assert summary["requests_scheduled"] == 16
    assert summary["successes"] == 9
    assert summary["errors"] == 3
    assert summary["rps"] == 3.0
    assert summary["success_rps"] == 2.25
    assert summary["p50_s"] == 0.9
    assert summary["p99_s"] == 1.8


def test_summarize_run_results_rejects_coercible_counts() -> None:
    run_results = {
        "requests_completed": "12",
        "requests_scheduled": 12,
        "errors": 0,
        "total_input_tokens": 0,
        "total_output_tokens": 0,
        "p50_s": 0.1,
        "p99_s": 0.2,
    }
    with pytest.raises(ValueError, match="requests_completed"):
        _summarize_run_results(0, run_results, requests_scheduled=12, duration_s=1.0)


def test_direct_target_wait_surfaces_an_unexpected_probe_failure(monkeypatch):
    def crash(*_args, **_kwargs):
        raise AssertionError("probe implementation broke")

    monkeypatch.setattr("eval.lib.replay_engine._probe_direct_target", crash)
    with pytest.raises(RuntimeError, match="health probe crashed.*probe implementation broke"):
        _wait_for_direct_targets(
            ["http://127.0.0.1:1"],
            ["/health"],
            timeout_s=1,
            probe_timeout_s=0.1,
            interval_s=0.01,
            max_workers=1,
        )
