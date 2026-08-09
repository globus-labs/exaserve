import io
import json
from pathlib import Path

import pytest

from clientlab.runner.runtime import run_study
from exaserve.state.results import load_result_manifest


def test_client_cleanup_shares_one_deadline_across_every_process(monkeypatch):
    from clientlab.runner import runtime

    deadlines = []

    def terminate(_process, *, grace_s=5.0, deadline=None):
        deadlines.append(deadline)

    class Drain:
        def __init__(self):
            self.timeouts = []

        def join(self, timeout):
            self.timeouts.append(timeout)

        def is_alive(self):
            return False

    drains = [Drain(), Drain()]
    monkeypatch.setattr(runtime, "_terminate_exact_process", terminate)
    logs = [io.StringIO(), io.StringIO()]
    runtime._finish_client_processes(
        [
            {"process": object(), "drains": (drains[0],)},
            {"process": object(), "drains": (drains[1],)},
        ],
        *logs,
        cleanup_s=2.0,
    )
    assert len(deadlines) == 2 and deadlines[0] == deadlines[1]
    assert all(drain.timeouts and 0 <= drain.timeouts[0] <= 2.0 for drain in drains)
    assert all(log.closed for log in logs)


def test_saturation_ceiling_probe_is_bounded(tmp_path, monkeypatch):
    from clientlab.runner import runtime

    monkeypatch.setattr(
        runtime,
        "_run_multi_step",
        lambda *_args, **_kwargs: {"achieved_rate": 1.0, "errors": 0},
    )
    monkeypatch.setattr(runtime, "_evaluate_merged_health", lambda *_args: True)
    config = {"client": {"saturation": {"search_mode": "binary", "initial_rate": 1}}}

    with pytest.raises(RuntimeError, match="bounded attempts"):
        runtime._run_saturation_multi("go", config, tmp_path, ["http://target"], {}, 1)


def test_runtime_aggregations_reject_coercible_counts_and_histogram_drift():
    from clientlab.runner import runtime

    with pytest.raises(ValueError, match="completed"):
        runtime._merge_step_results(
            [
                {
                    "target_rate": 1,
                    "achieved_rate": 1.0,
                    "completed": "1",
                    "failed": 0,
                    "error_rate": 0.0,
                    "duration_s": 1.0,
                    "p50_latency_s": 0.1,
                    "p99_latency_s": 0.2,
                    "mean_latency_s": 0.1,
                    "new_connections": 1,
                    "reused_connections": 0,
                    "max_observed_active": 1,
                    "healthy": True,
                }
            ],
            1.0,
        )

    summary = {
        "requests_completed": True,
        "requests_scheduled": 1,
        "errors": 0,
        "p50_s": 0.1,
        "p99_s": 0.2,
        "total_input_tokens": 1,
        "total_output_tokens": 1,
    }
    with pytest.raises(RuntimeError, match="requests_completed"):
        runtime.merge_summary_results([summary])

    left = {
        "latency": {
            "count": 1,
            "sum_s": 0.1,
            "counts": [1],
            "bucket_upper_bounds_s": [0.1],
        }
    }
    right = {
        "latency": {
            "count": 1,
            "sum_s": 0.2,
            "counts": [1, 0],
            "bucket_upper_bounds_s": [0.1, 1.0],
        }
    }
    with pytest.raises(RuntimeError, match="layouts disagree"):
        runtime.merge_histograms(left, right)


def test_client_result_shard_missing_terminal_is_not_zero_request_success(tmp_path):
    from clientlab.runner import runtime

    path = tmp_path / "result.jsonl"
    path.write_text("", encoding="utf-8")
    with pytest.raises(RuntimeError, match="missing its terminal row"):
        runtime.load_summary_result(path, expected_request_ids=("request-0",), sum_only=True)


def test_netstats_summary_rejects_coercible_counters(tmp_path):
    from clientlab.runner import runtime

    path = tmp_path / "netstats.jsonl"
    path.write_text(
        json.dumps(
            {
                "timestamp": 1.0,
                "hostname": "node",
                "interface": "hsn0",
                "rx_bytes": 1,
                "rx_packets": 1,
                "rx_errors": 0,
                "rx_drops": "0",
                "tx_bytes": 1,
                "tx_packets": 1,
                "tx_errors": 0,
                "tx_drops": 0,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="rx_drops"):
        runtime.summarize_netstats(path)


def test_run_study_local_smoke(tmp_path: Path):
    spec_path = tmp_path / "smoke.json"
    spec_path.write_text(
        json.dumps(
            {
                "study": {"name": "smoke-study", "suite": "client_microbench", "repeats": 1},
                "client": {
                    "duration_s": 1.0,
                    "rate": 5.0,
                    "prompt_words": 8,
                    "output_tokens": 4,
                    "max_active_requests": 2,
                    "queue_capacity": 0,
                    "num_go_workers": 1,
                    "num_go_procs": 1,
                    "phase_trace_sample_rate": 0.5,
                },
                "target": {
                    "type": "synthetic",
                    "host": "127.0.0.1",
                    "port": 19500,
                    "synthetic_nodes": 1,
                },
                "faults": {"service_time": {"distribution": "fixed", "value_ms": 1}},
                "collectors": {"port_monitor": True, "netstats": False},
                "execution": {"mode": "local"},
                "reporting": {"generate_markdown": True},
            }
        ),
        encoding="utf-8",
    )

    study_dir = Path(
        run_study(str(spec_path), output_dir=str(tmp_path / "study"), force_local=True)
    )
    assert (study_dir / "study_manifest.json").is_file()
    assert (study_dir / "results_index.json").is_file()
    assert (study_dir / "report.md").is_file()

    results_index = json.loads((study_dir / "results_index.json").read_text(encoding="utf-8"))
    assert results_index["points"], "expected at least one completed point"
    point_dir = Path(results_index["points"][0]["artifacts"]["point_dir"])
    assert (point_dir / "client_metrics.json").is_file()
    assert (point_dir / "diagnosis.json").is_file()
    manifest = load_result_manifest(point_dir / "result_manifest.json")
    assert manifest.complete
    assert "client_shard/result_p0" in manifest.expected_ids
    assert "client_shard/metrics_p0" in manifest.expected_ids
    run_config = results_index["points"][0]["run_config"]
    assert run_config["canonical_run"]["claim_scope"] == "CLIENT_DIAGNOSTIC_ONLY"


def test_failed_point_is_persisted_but_study_exits_nonzero(tmp_path: Path, monkeypatch):
    from clientlab.runner import runtime

    spec_path = tmp_path / "failure.json"
    spec_path.write_text(
        json.dumps(
            {
                "study": {"name": "failure-study"},
                "target": {"type": "synthetic"},
                "reporting": {"generate_markdown": False, "generate_plots": False},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        runtime, "run_point", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    output = tmp_path / "failed-study"
    with pytest.raises(RuntimeError, match="study failed"):
        run_study(str(spec_path), output_dir=str(output))
    results = json.loads((output / "results_index.json").read_text())
    assert results["points"][0]["summary"]["diagnosis"] == "error"
