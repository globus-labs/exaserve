"""Eval consumes the one canonical DeploymentStatus surface."""

from __future__ import annotations

import time

from eval.lib.backends.base import BackendProcessHandle
from exaserve.plan.compiler import compile_deployment_plan
from exaserve.plan.contracts import build_allocation_binding
from exaserve.site import default_site_profile
from exaserve.state.status import DeploymentState
from exaserve.state.receipts import ReceiptManifest, write_receipt_manifest
from exaserve.status_api import DeploymentStatusPublisher


def _plan():
    return compile_deployment_plan(
        {
            "num_nodes": 1,
            "validation_mode": True,
            "models": [
                {"model_id": "m", "tensor_parallel_size": 1, "max_model_len": 128, "size": 8}
            ],
        },
        site=default_site_profile(),
        deployment_id="eval-deployment",
    )


def _publisher(tmp_path, *, generation=1):
    plan = _plan()
    binding = build_allocation_binding(
        plan=plan, generation=generation, scheduler_allocation_id="j", nodes=["n0"]
    )
    publisher = DeploymentStatusPublisher(
        str(tmp_path), plan=plan, binding=binding, generation=generation
    )
    publisher.initialize()
    return publisher, plan


def _handle(tmp_path, *, generation=1, plan_hash=""):
    return BackendProcessHandle(
        process=None,
        log_path=str(tmp_path / "svc.log"),
        status_dir=str(tmp_path),
        expected_generation=generation,
        expected_plan_hash=plan_hash,
    )


def _ready(publisher):
    publisher.advance_through(
        DeploymentState.STAGING,
        DeploymentState.CLUSTER_STARTING,
        DeploymentState.DEPLOYING,
        DeploymentState.VALIDATING,
        reason_code="TEST",
    )
    manifest = ReceiptManifest(
        schema_version=1,
        deployment_id=publisher.plan.deployment_id,
        generation=publisher.generation,
        deployment_plan_hash=publisher.plan.deployment_plan_hash,
        allocation_binding_hash=publisher.binding.allocation_binding_hash,
        receipt_hashes=(),
        receipts=(),
    ).finalize()
    import os

    manifest_path = os.path.join(
        os.path.dirname(publisher.path), "compatibility_receipts", f"{manifest.manifest_hash}.json"
    )
    write_receipt_manifest(manifest_path, manifest)
    model = publisher.plan.models[0]
    model_map = {
        model.model_id: {
            "route_name": model.route_name,
            "expected_replicas": model.num_replicas,
            "observed_replicas": model.num_replicas,
            "observed_target": model.num_replicas,
        }
    }
    nodes = [
        {
            "node_id": "id-0",
            "node_name": "n0",
            "node_address": "10.0.0.1",
            "alive": True,
            "cpu": float(publisher.plan.node_cpus),
            "gpu": float(publisher.plan.num_gpus_per_node),
        }
    ]
    publisher.advance(
        DeploymentState.READY,
        reason_code="READY",
        advertised_endpoint="http://n0:8000",
        receipt_hashes=[],
        readiness_snapshot={
            "ready": True,
            "phase": "READY",
            "blockers": [],
            "satisfied": ["test predicate"],
            "advertised_endpoint": "http://n0:8000",
            "generation": publisher.generation,
            "deployment_plan_hash": publisher.plan.deployment_plan_hash,
            "allocation_binding_hash": publisher.binding.allocation_binding_hash,
            "missing_identities": [],
            "unhealthy_identities": [],
            "receipt_hashes": [],
            "model_map": model_map,
            "capability_map": {},
            "nodes": nodes,
            "proxies": [{"node_id": "id-0", "status": "HEALTHY"}],
            "observed_at": time.time(),
            "lease_expires_at": time.time() + 60,
            "receipt_manifest_path": manifest_path,
            "receipt_manifest_hash": manifest.manifest_hash,
        },
        model_map=model_map,
        capability_map={},
    )


def test_generation_specific_status_ready_satisfies_the_wait(tmp_path):
    publisher, plan = _publisher(tmp_path)
    _ready(publisher)
    handle = _handle(tmp_path, plan_hash=plan.deployment_plan_hash)
    assert handle.wait_for_ready(timeout_s=1) is True
    assert handle.readiness_source == "deployment_status"


def test_terminal_status_fails_the_wait_immediately(tmp_path):
    publisher, _ = _publisher(tmp_path)
    publisher.advance(DeploymentState.FAILED, reason_code="BOOM")
    assert _handle(tmp_path).wait_for_ready(timeout_s=1) is False


def test_status_from_another_generation_cannot_declare_ready(tmp_path):
    publisher, _ = _publisher(tmp_path, generation=2)
    _ready(publisher)
    assert _handle(tmp_path, generation=1).wait_for_ready(timeout_s=0.05) is False


def test_stdout_text_has_no_effect_on_readiness(tmp_path):
    handle = _handle(tmp_path)
    handle.recent_lines.append("[Driver] ALL SERVICES READY")
    assert handle.wait_for_ready(timeout_s=0.05) is False


def test_missing_status_times_out(tmp_path):
    assert _handle(tmp_path).wait_for_ready(timeout_s=0.05) is False
