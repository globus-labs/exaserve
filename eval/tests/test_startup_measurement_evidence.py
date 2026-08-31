"""Startup-only paper measurements are identity-bound and sealed."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from types import SimpleNamespace

from eval.lib.run_executor import (
    _capture_startup_measurement,
    _capture_startup_terminal_evidence,
)


@dataclass
class _Status:
    state: str
    ready: bool
    generation: int
    deployment_plan_hash: str
    revision: int = 7
    updated_at: float = 120.0


def _run(tmp_path: Path):
    results = tmp_path / "results"
    status_dir = tmp_path / "status"
    results.mkdir()
    status_dir.mkdir()
    deployment = SimpleNamespace(
        num_nodes=2,
        models=(SimpleNamespace(num_replicas=2),),
        receipt_requirements=("a", "b", "c"),
        uses_head_only_serve_proxy=lambda: False,
    )
    run_plan = SimpleNamespace(
        deployment_plan_hash="a" * 64,
        run_semantic_hash="b" * 64,
        source_snapshot_hash="c" * 64,
        semantic_plan=SimpleNamespace(deployment=deployment),
        bundle=SimpleNamespace(results_dir=str(results)),
    )
    launched = SimpleNamespace(
        monitor=SimpleNamespace(status_dir=str(status_dir), expected_generation=42)
    )
    return run_plan, launched, results, status_dir


def test_startup_trace_and_summary_are_sealed(monkeypatch, tmp_path):
    run_plan, launched, results, status_dir = _run(tmp_path)
    trace = {
        "metadata": {
            "deployment_plan_hash": "a" * 64,
            "generation": 42,
            "run_semantic_hash": "b" * 64,
            "source_snapshot_hash": "c" * 64,
            "num_nodes": 2,
            "expected_model_replicas": 2,
            "expected_serve_applications": 3,
            "expected_receipt_requirements": 3,
            "trace_start": 100.0,
            "total_duration_s": 18.0,
            "gateway_kind": "haproxy",
            "exposure_mode": "PROXIED_INTERNAL",
            "null_compute": True,
        },
        "phases": [
            {"name": "ray.init", "duration_s": 1.0},
            {"name": "serve.start", "duration_s": 2.0},
            {"name": "deploy_from_canonical_plan", "duration_s": 12.0},
            {"name": "stage3.total", "duration_s": 13.0},
        ],
        "api_calls": [
            {"label": "ray.nodes", "duration_s": 0.25},
            {"label": "ray.nodes", "duration_s": 0.5},
        ],
        "events": [],
        "replicas": [{"id": 0}, {"id": 1}],
    }
    (status_dir / "scaling_trace.json").write_text(json.dumps(trace), encoding="utf-8")
    monkeypatch.setattr(
        "exaserve.control.plan_readiness.planned_application_names",
        lambda _plan: frozenset({"app-0", "app-1", "anchor-0"}),
    )
    monkeypatch.setattr(
        "exaserve.status_api.read_deployment_status",
        lambda _path: _Status("READY", True, 42, "a" * 64),
    )

    entries = _capture_startup_measurement(run_plan, launched)
    assert set(entries) == {"startup_scaling_trace", "startup_metrics"}
    summary = json.loads((results / "startup_metrics.json").read_text(encoding="utf-8"))
    assert summary["ready_after_trace_start_s"] == 20.0
    assert summary["api_call_summary"]["ray.nodes"] == {
        "count": 2,
        "max_duration_s": 0.5,
        "total_duration_s": 0.75,
    }
    assert "legacy deploy_apps/wait_proxies" in summary["timing_semantics"]["phase_timings"]


def test_startup_shutdown_and_terminal_status_are_sealed(monkeypatch, tmp_path):
    run_plan, launched, results, status_dir = _run(tmp_path)
    report = {
        "clean": True,
        "deadline_exhausted": False,
        "errors": [],
        "observed_terminal_state": "STOPPED",
        "deployment_plan_hash": "a" * 64,
        "generation": 42,
    }
    (status_dir / "shutdown_report.json").write_text(json.dumps(report), encoding="utf-8")
    monkeypatch.setattr(
        "exaserve.status_api.read_deployment_status",
        lambda _path: _Status("STOPPED", False, 42, "a" * 64),
    )

    entries = _capture_startup_terminal_evidence(run_plan, launched)
    assert set(entries) == {"deployment_shutdown_report", "deployment_terminal_status"}
    sealed_report = json.loads(
        (results / "deployment_shutdown_report.json").read_text(encoding="utf-8")
    )
    terminal = json.loads(
        (results / "deployment_terminal_status.json").read_text(encoding="utf-8")
    )
    assert sealed_report == report
    assert terminal["state"] == "STOPPED"
