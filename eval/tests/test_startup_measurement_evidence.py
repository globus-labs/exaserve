"""Startup-only paper measurements are identity-bound and sealed."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

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
    replicas = (
        SimpleNamespace(replica_id="replica/m/0", replica_index=0),
        SimpleNamespace(replica_id="replica/m/1", replica_index=1),
    )
    deployment = SimpleNamespace(
        num_nodes=2,
        models=(SimpleNamespace(model_id="m", num_replicas=2, replicas=replicas),),
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
            "trace_start_monotonic": 80.0,
            "trace_end_monotonic": 98.0,
            "trace_clock_boot_id": "boot-a",
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
        "replicas": [
            {
                "component_slot": "replica/m/0",
                "model_id": "m",
                "replica_index": 0,
                "null_compute": True,
                "total_init_s": 2.0,
                "wall_start": 100.0,
                "wall_end": 102.0,
                "monotonic_start": 10.0,
                "monotonic_end": 12.0,
            },
            {
                "component_slot": "replica/m/1",
                "model_id": "m",
                "replica_index": 1,
                "null_compute": True,
                "total_init_s": 3.0,
                "wall_start": 100.0,
                "wall_end": 103.0,
                "monotonic_start": 20.0,
                "monotonic_end": 23.0,
            },
        ],
    }
    (status_dir / "scaling_trace.json").write_text(json.dumps(trace), encoding="utf-8")
    ready_evidence_path = results / "deployment_ready_evidence.json"
    ready_evidence_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "state": "READY",
                "generation": 42,
                "deployment_plan_hash": "a" * 64,
                "run_semantic_hash": "b" * 64,
                "readiness_snapshot": {"ready": True},
                "revision": 9,
                "updated_at": 125.0,
                "state_revision": 7,
                "state_changed_at": 120.0,
                "state_changed_monotonic": 100.0,
                "state_clock_boot_id": "boot-a",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "exaserve.control.plan_readiness.planned_application_names",
        lambda _plan: frozenset({"app-0", "app-1", "anchor-0"}),
    )
    entries = _capture_startup_measurement(
        run_plan, launched, ready_evidence_path=str(ready_evidence_path)
    )
    assert set(entries) == {"startup_scaling_trace", "startup_metrics"}
    summary = json.loads((results / "startup_metrics.json").read_text(encoding="utf-8"))
    assert summary["schema_version"] == 2
    assert summary["ready_after_trace_start_s"] == 20.0
    assert summary["ready_evidence_revision"] == 9
    assert summary["ready_transition_revision"] == 7
    assert summary["api_call_summary"]["ray.nodes"] == {
        "count": 2,
        "max_duration_s": 0.5,
        "total_duration_s": 0.75,
    }
    assert "legacy deploy_apps/wait_proxies" in summary["timing_semantics"]["phase_timings"]

    trace["replicas"][1] = dict(trace["replicas"][0])
    (status_dir / "scaling_trace.json").write_text(json.dumps(trace), encoding="utf-8")
    with pytest.raises(RuntimeError, match="duplicates replica slot"):
        _capture_startup_measurement(
            run_plan, launched, ready_evidence_path=str(ready_evidence_path)
        )


def test_real_ready_status_capture_flows_into_startup_measurement(tmp_path):
    from exaserve.control.plan_readiness import planned_application_names
    from exaserve.evidence import capture_ready_evidence
    from exaserve.plan.compiler import compile_deployment_plan
    from exaserve.plan.contracts import (
        RunProvenance,
        build_allocation_binding,
    )
    from exaserve.plan.io import write_run_provenance
    from exaserve.site import default_site_profile
    from exaserve.state.receipts import ReceiptManifest, write_receipt_manifest
    from exaserve.state.status import DeploymentState
    from exaserve.status_api import DeploymentStatusPublisher, read_deployment_status

    results = tmp_path / "results"
    status_dir = tmp_path / "status"
    results.mkdir()
    status_dir.mkdir()
    plan = compile_deployment_plan(
        {
            "num_nodes": 2,
            "validation_mode": True,
            "models": [
                {
                    "model_id": "m",
                    "tensor_parallel_size": 1,
                    "num_replicas": 2,
                    "max_model_len": 128,
                    "size": 8,
                }
            ],
            "gateway": {"kind": "haproxy", "port": 4001},
        },
        site=default_site_profile(),
        deployment_id="startup-integration",
    )
    generation = 42
    binding = build_allocation_binding(
        plan=plan,
        generation=generation,
        scheduler_allocation_id="job1",
        nodes=["n0", "n1"],
    )
    run_semantic_hash = "b" * 64
    source_snapshot_hash = "c" * 64
    provenance = RunProvenance(
        schema_version=3,
        run_id="n2",
        deployment_id=plan.deployment_id,
        generation=generation,
        deployment_plan_hash=plan.deployment_plan_hash,
        allocation_binding_hash=binding.allocation_binding_hash,
        run_semantic_hash=run_semantic_hash,
        source_snapshot_hash=source_snapshot_hash,
        resolved_input_paths=("/tmp/input",),
        argv=("python",),
        prepared_environment_hash="d" * 64,
        started_at="2026-08-31T00:00:00+00:00",
        output_locations=(str(results),),
    ).finalize()
    write_run_provenance(str(status_dir / "run_provenance.json"), provenance)
    publisher = DeploymentStatusPublisher(
        str(status_dir),
        plan=plan,
        binding=binding,
        generation=generation,
        run_provenance=provenance,
        log=lambda *_: None,
    )
    publisher.initialize()
    publisher.advance_through(
        DeploymentState.STAGING,
        DeploymentState.CLUSTER_STARTING,
        DeploymentState.DEPLOYING,
        DeploymentState.VALIDATING,
        reason_code="TEST",
    )
    manifest = ReceiptManifest(
        schema_version=1,
        deployment_id=plan.deployment_id,
        generation=generation,
        deployment_plan_hash=plan.deployment_plan_hash,
        allocation_binding_hash=binding.allocation_binding_hash,
        receipt_hashes=(),
        receipts=(),
    ).finalize()
    receipt_path = status_dir / "compatibility_receipts" / f"{manifest.manifest_hash}.json"
    write_receipt_manifest(str(receipt_path), manifest)
    nodes = [
        {
            "node_id": f"id-{rank}",
            "node_name": node,
            "node_address": f"10.0.0.{rank + 1}",
            "alive": True,
            "cpu": float(plan.node_cpus),
            "gpu": float(plan.num_gpus_per_node),
        }
        for rank, node in binding.rank_to_node
    ]
    model = plan.models[0]
    model_map = {
        model.model_id: {
            "route_name": model.route_name,
            "expected_replicas": model.num_replicas,
            "observed_replicas": model.num_replicas,
            "observed_target": model.num_replicas,
        }
    }
    endpoint = "http://head:4001"
    snapshot = {
        "ready": True,
        "phase": "READY",
        "blockers": [],
        "satisfied": ["integration predicate"],
        "advertised_endpoint": endpoint,
        "generation": generation,
        "deployment_plan_hash": plan.deployment_plan_hash,
        "allocation_binding_hash": binding.allocation_binding_hash,
        "missing_identities": [],
        "unhealthy_identities": [],
        "receipt_hashes": [],
        "model_map": model_map,
        "capability_map": {},
        "nodes": nodes,
        "proxies": [{"node_id": node["node_id"], "status": "HEALTHY"} for node in nodes],
        "observed_at": __import__("time").time(),
        "receipt_manifest_path": str(receipt_path),
        "receipt_manifest_hash": manifest.manifest_hash,
        "lease_expires_at": __import__("time").time() + 60.0,
    }
    publisher.advance(
        DeploymentState.READY,
        reason_code="READY",
        advertised_endpoint=endpoint,
        receipt_hashes=[],
        readiness_snapshot=snapshot,
        model_map=model_map,
        capability_map={},
    )
    status = read_deployment_status(str(status_dir))
    assert status is not None and status.ready and status.schema_version == 2
    ready_entries = capture_ready_evidence(
        status_dir=str(status_dir),
        destination_dir=str(results),
        expected_generation=generation,
        expected_plan_hash=plan.deployment_plan_hash,
        expected_run_semantic_hash=run_semantic_hash,
    )
    trace_start = status.state_changed_monotonic - 20.0
    trace = {
        "metadata": {
            "deployment_plan_hash": plan.deployment_plan_hash,
            "generation": generation,
            "run_semantic_hash": run_semantic_hash,
            "source_snapshot_hash": source_snapshot_hash,
            "num_nodes": plan.num_nodes,
            "expected_model_replicas": model.num_replicas,
            "expected_serve_applications": len(planned_application_names(plan)),
            "expected_receipt_requirements": len(plan.receipt_requirements),
            "trace_start": __import__("time").time() - 20.0,
            "trace_start_monotonic": trace_start,
            "trace_end_monotonic": trace_start + 15.0,
            "trace_clock_boot_id": status.state_clock_boot_id,
            "total_duration_s": 15.0,
            "gateway_kind": "haproxy",
            "exposure_mode": plan.exposure.mode,
            "null_compute": True,
        },
        "phases": [
            {"name": "ray.init", "duration_s": 1.0},
            {"name": "serve.start", "duration_s": 2.0},
            {"name": "deploy_from_canonical_plan", "duration_s": 10.0},
            {"name": "stage3.total", "duration_s": 12.0},
        ],
        "api_calls": [],
        "events": [],
        "replicas": [
            {
                "component_slot": replica.replica_id,
                "model_id": model.model_id,
                "replica_index": replica.replica_index,
                "null_compute": True,
                "total_init_s": 1.0,
                "wall_start": 10.0,
                "wall_end": 11.0,
                "monotonic_start": 10.0 + replica.replica_index,
                "monotonic_end": 11.0 + replica.replica_index,
            }
            for replica in model.replicas
        ],
    }
    (status_dir / "scaling_trace.json").write_text(json.dumps(trace), encoding="utf-8")
    run_plan = SimpleNamespace(
        deployment_plan_hash=plan.deployment_plan_hash,
        run_semantic_hash=run_semantic_hash,
        source_snapshot_hash=source_snapshot_hash,
        semantic_plan=SimpleNamespace(deployment=plan),
        bundle=SimpleNamespace(results_dir=str(results)),
    )
    launched = SimpleNamespace(
        monitor=SimpleNamespace(status_dir=str(status_dir), expected_generation=generation)
    )
    entries = _capture_startup_measurement(
        run_plan,
        launched,
        ready_evidence_path=ready_entries["deployment_ready_evidence"],
    )
    assert set(entries) == {"startup_scaling_trace", "startup_metrics"}
    metrics = json.loads((results / "startup_metrics.json").read_text(encoding="utf-8"))
    ready_bytes = (results / "deployment_ready_evidence.json").read_bytes()
    assert metrics["deployment_ready_evidence_sha256"] == hashlib.sha256(ready_bytes).hexdigest()
    assert metrics["ready_evidence_revision"] == status.revision
    assert metrics["ready_transition_revision"] == status.state_revision
    assert metrics["ready_after_trace_start_s"] == 20.0


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
    terminal = json.loads((results / "deployment_terminal_status.json").read_text(encoding="utf-8"))
    assert sealed_report == report
    assert terminal["state"] == "STOPPED"
