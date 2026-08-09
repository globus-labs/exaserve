import json
from pathlib import Path

import pytest

from clientlab.analysis.diagnostics import build_operating_envelope, summarize_point
from clientlab.reports.markdown import render_report
from clientlab.runner.runtime import _validate_results_index, _validate_study_manifest
from clientlab.runner.spec_io import expand_matrix, load_study_spec
from clientlab.utils import load_yaml_file


def test_clientlab_yaml_loader_rejects_duplicate_keys(tmp_path: Path):
    path = tmp_path / "duplicate.yaml"
    path.write_text("client:\n  rate: 1\n  rate: 2\n", encoding="utf-8")

    with pytest.raises(Exception, match="duplicate key 'rate'"):
        load_yaml_file(path)


@pytest.mark.parametrize(
    ("section", "field", "value", "message"),
    [
        ("client", "num_go_procs", 0, "num_go_procs"),
        ("client", "timeout_s", 0.0, "timeout_s"),
        ("target", "port", 70000, "target.port"),
        ("faults", "error_rate", 1.1, "error_rate"),
        ("collectors", "port_monitor_interval_s", 0.0, "port_monitor_interval_s"),
        ("execution", "client_nodes", 0, "client_nodes"),
    ],
)
def test_clientlab_rejects_unsafe_numeric_bounds(tmp_path, section, field, value, message):
    path = tmp_path / "bad.json"
    path.write_text(
        json.dumps({"target": {"type": "synthetic"}, section: {field: value}}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=message):
        load_study_spec(path)


def test_saturation_explicit_ceiling_must_exceed_initial_rate(tmp_path):
    path = tmp_path / "bad-saturation.json"
    path.write_text(
        json.dumps(
            {
                "target": {"type": "synthetic"},
                "client": {"saturation": {"enabled": True, "initial_rate": 100, "max_rate": 100}},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="must exceed"):
        load_study_spec(path)


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


def test_specs_reject_unknown_fields_and_string_booleans(tmp_path: Path):
    for payload, match in (
        ({"client": {"mystery": 1}}, "unknown fields"),
        ({"collectors": {"netstats": "false"}}, "must be boolean"),
        ({"target": {"expected_generation": "3"}}, "must be an integer"),
    ):
        path = tmp_path / f"bad-{len(list(tmp_path.iterdir()))}.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        with pytest.raises(ValueError, match=match):
            load_study_spec(path)


def test_clientlab_schema_version_rejects_boolean_alias(tmp_path: Path):
    path = tmp_path / "bad-schema.json"
    path.write_text(json.dumps({"schema_version": True}), encoding="utf-8")
    with pytest.raises(ValueError, match="schema_version"):
        load_study_spec(path)


def test_clientlab_explicit_supported_schema_version_loads(tmp_path: Path):
    from clientlab import SCHEMA_VERSION

    path = tmp_path / "supported-schema.json"
    path.write_text(json.dumps({"schema_version": SCHEMA_VERSION}), encoding="utf-8")
    assert load_study_spec(path)["schema_version"] == SCHEMA_VERSION


def _persisted_clientlab_artifacts():
    from clientlab import SCHEMA_VERSION

    point_id = "base_abc123"
    study = {"name": "study", "suite": "suite", "repeats": 1, "description": ""}
    execution = {"mode": "local", "python": "", "env_script": "", "client_nodes": 1}
    reporting = {"generate_markdown": True, "generate_plots": True}
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "study": study,
        "execution": execution,
        "reporting": reporting,
        "spec_path": "/tmp/study.json",
        "created_at": "2026-08-08T00:00:00Z",
        "points": [{"point_id": point_id, "axis_values": {}}],
    }
    results = {
        "schema_version": SCHEMA_VERSION,
        "study": study,
        "execution": execution,
        "reporting": reporting,
        "points": [
            {
                "point_id": point_id,
                "axis_values": {},
                "run_config": {"client": {"max_active_requests": 1}},
                "artifacts": {"point_dir": "/tmp/point"},
                "summary": {
                    "diagnosis": "healthy",
                    "requested_rps": 1.0,
                    "achieved_rps": 1.0,
                    "success_fraction": 1.0,
                    "queue_fraction": 0.0,
                    "safe_active_budget_estimate": 1,
                },
            }
        ],
        "operating_envelope": {
            "max_stable_rps": 1.0,
            "safe_active_budget": 1,
            "notes": [],
        },
    }
    return manifest, results


def test_persisted_clientlab_report_contract_is_exact():
    manifest, results = _persisted_clientlab_artifacts()
    assert _validate_study_manifest(manifest) is manifest
    assert _validate_results_index(results) is results

    results["points"][0]["summary"]["achieved_rps"] = "1.0"
    with pytest.raises(ValueError, match="achieved_rps"):
        _validate_results_index(results)


def test_persisted_clientlab_manifest_rejects_unknown_fields():
    manifest, _results = _persisted_clientlab_artifacts()
    manifest["undeclared"] = True
    with pytest.raises(ValueError, match="fields are invalid"):
        _validate_study_manifest(manifest)


def test_every_builtin_clientlab_spec_strictly_loads():
    specs = Path(__file__).resolve().parents[1] / "specs"
    loaded = [load_study_spec(path) for path in sorted(specs.glob("*.yaml"))]
    assert loaded


def test_exaserve_spec_is_hydrated_only_from_verified_run_plan(tmp_path: Path):
    from exaserve.plan.compiler import compile_run_plan
    from exaserve.plan.contracts import ClientPolicy, TracePolicy, WorkloadPolicy
    from exaserve.plan.io import write_run_plan
    from exaserve.state.results import file_sha256

    trace = tmp_path / "trace.jsonl"
    trace.write_text(
        json.dumps({"__type__": "metadata"})
        + "\n"
        + json.dumps(
            {
                "timestamp": 0.0,
                "model": "m",
                "mode": "chat",
                "prompt": "hi",
                "input_len": 1,
                "output_len": 2,
                "tensor_parallel_size": 1,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    plan = compile_run_plan(
        {
            "num_nodes": 1,
            "num_gpus_per_node": 12,
            "validation_mode": True,
            "models": [
                {"model_id": "m", "tensor_parallel_size": 1, "max_model_len": 128, "size": 8}
            ],
        },
        run_id="clientlab/run",
        deployment_id="deployment",
        workload=WorkloadPolicy(
            duration_s=1.0,
            input_len=1,
            output_len=2,
            rate_per_node=7.0,
            client_nodes=1,
            client_dest="direct",
        ),
        trace=TracePolicy(trace_content_hash=file_sha256(trace)),
        client=ClientPolicy(destination="direct", concurrency=3, workers=2, processes=1),
    )
    plan_path = tmp_path / "run.plan.json"
    write_run_plan(plan_path, plan)
    status_dir = tmp_path / "deployment-state"
    status_dir.mkdir()
    spec_path = tmp_path / "study.json"
    spec_path.write_text(
        json.dumps(
            {
                "study": {"name": "canonical"},
                "target": {
                    "type": "exaserve",
                    "run_plan_path": str(plan_path),
                    "trace_path": str(trace),
                    "deployment_status_dir": str(status_dir),
                    "expected_generation": 4,
                },
            }
        ),
        encoding="utf-8",
    )

    spec = load_study_spec(spec_path)
    assert spec["client"]["rate"] == 7.0
    assert spec["client"]["max_active_requests"] == 3
    assert spec["_canonical_run"]["run_semantic_hash"] == plan.run_semantic_hash

    trace.write_text(trace.read_text() + "{}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="content disagrees"):
        load_study_spec(spec_path)


def test_exaserve_spec_cannot_restate_client_semantics(tmp_path: Path):
    path = tmp_path / "bad.json"
    path.write_text(
        json.dumps(
            {
                "target": {
                    "type": "exaserve",
                    "run_plan_path": "x",
                    "trace_path": "y",
                    "deployment_status_dir": "z",
                    "expected_generation": 1,
                },
                "client": {"rate": 1.0},
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="must not restate"):
        load_study_spec(path)
