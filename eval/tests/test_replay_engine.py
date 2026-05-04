import os

os.environ.setdefault("MPI4PY_RC_INITIALIZE", "0")

from eval.lib.replay_engine import TraceRequest, _summarize_run_results


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
