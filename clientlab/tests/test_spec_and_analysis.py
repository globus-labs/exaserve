import json
from pathlib import Path

from clientlab.analysis.diagnostics import build_operating_envelope, summarize_point
from clientlab.reports.markdown import render_report
from clientlab.runner.spec_io import expand_matrix, load_study_spec


def test_load_study_spec_and_expand_matrix(tmp_path: Path):
    spec_path = tmp_path / "study.json"
    spec_path.write_text(
        json.dumps(
            {
                "study": {"name": "unit-study", "suite": "client_microbench", "repeats": 2},
                "matrix": {
                    "axes": [
                        {
                            "name": "max_active_requests",
                            "path": "client.max_active_requests",
                            "values": [4, 8],
                        }
                    ]
                },
                "client": {"duration_s": 1.0, "rate": 10.0, "max_active_requests": 4},
                "target": {"type": "synthetic"},
                "execution": {"mode": "local"},
            }
        ),
        encoding="utf-8",
    )
    spec = load_study_spec(str(spec_path))
    points = expand_matrix(spec)
    assert len(points) == 4
    assert {point["client"]["max_active_requests"] for point in points} == {4, 8}


def test_summarize_point_prefers_queue_bound_when_queue_fraction_is_high():
    run_config = {
        "client": {"duration_s": 2.0, "rate": 50.0, "max_active_requests": 8, "queue_capacity": 4},
        "target": {"type": "synthetic"},
    }
    client_metrics = {
        "requests_completed": 100,
        "requests_succeeded": 100,
        "new_connections": 5,
        "reused_connections": 95,
        "max_observed_queue_depth": 4,
        "max_observed_active": 8,
        "histograms": {
            "queue_wait": {"count": 100, "sum_s": 20.0},
            "slot_hold": {"count": 100, "sum_s": 40.0},
            "dispatch_lag": {"count": 100, "sum_s": 1.0},
            "connect": {"count": 5, "sum_s": 0.01},
            "time_to_headers": {"count": 100, "sum_s": 5.0},
            "body_read": {"count": 100, "sum_s": 1.0},
        },
    }
    target_metrics = {"aggregate": {"max_queue_depth": 0, "rejections": 0, "error_fraction": 0.0}}
    port_metrics = {"max_time_wait": 1}
    summary = summarize_point(run_config, client_metrics, target_metrics, port_metrics)
    assert summary["diagnosis"] == "client_queue_bound"
    assert summary["queue_fraction"] > 0.25


def test_render_report_mentions_key_questions():
    study_manifest = {
        "study": {"name": "demo-study", "suite": "client_microbench"},
        "execution": {"mode": "local"},
    }
    points = [
        {
            "point_id": "p0",
            "run_config": {"client": {"max_active_requests": 8}},
            "summary": {
                "requested_rps": 10.0,
                "achieved_rps": 9.0,
                "diagnosis": "inconclusive",
                "queue_fraction": 0.05,
                "safe_active_budget_estimate": 12,
            },
        },
        {
            "point_id": "p1",
            "run_config": {"client": {"max_active_requests": 16}},
            "summary": {
                "requested_rps": 20.0,
                "achieved_rps": 18.0,
                "diagnosis": "client_queue_bound",
                "queue_fraction": 0.30,
                "safe_active_budget_estimate": 20,
            },
        },
    ]
    envelope = build_operating_envelope([point["summary"] for point in points])
    report = render_report(study_manifest, points, envelope)
    assert "What changed when concurrency increased?" in report
    assert "safe for this regime" in report
