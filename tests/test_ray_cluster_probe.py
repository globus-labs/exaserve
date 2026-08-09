"""The pre-deployment Ray proof is exact, typed, and plan-derived."""

from __future__ import annotations

import json
import time

import pytest

from exaserve.control.ray_cluster_probe import (
    RayClusterProbeError,
    evaluate_cluster,
    load_probe_snapshot,
    normalize_nodes,
)
from exaserve.plan.compiler import compile_deployment_plan
from exaserve.plan.contracts import SiteProfile, build_allocation_binding


def _plan():
    site = SiteProfile(
        schema_version=3,
        site_id="s",
        max_nodes=4,
        gpus_per_node=4,
        cpus_per_node=32,
        scheduler_types=("pbs",),
        gateway_kinds=("haproxy",),
        vendors=("xpu",),
        engines=("vllm",),
        model_storage_path="/models",
        local_stage_path="/tmp/models",
        launcher_capabilities=("ray_serve.run_many",),
    ).finalize()
    return compile_deployment_plan(
        {
            "num_nodes": 2,
            "num_gpus_per_node": 4,
            "node_cpus": 32,
            "models": [
                {
                    "model_id": "org/model",
                    "tensor_parallel_size": 1,
                    "max_model_len": 128,
                    "size": 8,
                }
            ],
            "gateway": {"kind": "haproxy", "port": 4001},
        },
        site=site,
        deployment_id="deployment",
    )


def _binding(plan):
    return build_allocation_binding(
        plan=plan, generation=3, scheduler_allocation_id="job", nodes=["n0.example", "n1.example"]
    )


def _nodes():
    return [
        {
            "node_id": "id0",
            "node_name": "n0",
            "node_address": "10.0.0.1",
            "alive": True,
            "cpu": 32.0,
            "gpu": 4.0,
        },
        {
            "node_id": "id1",
            "node_name": "n1",
            "node_address": "10.0.0.2",
            "alive": True,
            "cpu": 32.0,
            "gpu": 4.0,
        },
    ]


def test_exact_membership_and_resources_pass():
    plan = _plan()
    ready, blockers = evaluate_cluster(plan, _binding(plan), _nodes(), {"CPU": 64.0, "GPU": 8.0})
    assert ready and blockers == []


@pytest.mark.parametrize(
    "mutation",
    [
        lambda node: node.update(Alive=1),
        lambda node: node.update(NodeID=7),
        lambda node: node["Resources"].update(CPU="32"),
        lambda node: node["Resources"].update(GPU=float("nan")),
    ],
)
def test_public_ray_nodes_reject_coercible_or_nonfinite_fields(mutation):
    node = {
        "NodeID": "id0",
        "NodeManagerHostname": "n0",
        "NodeManagerAddress": "10.0.0.1",
        "Alive": True,
        "Resources": {"CPU": 32.0, "GPU": 4.0},
    }
    mutation(node)
    with pytest.raises(RayClusterProbeError):
        normalize_nodes([node])


@pytest.mark.parametrize(
    "mutation, expected",
    [
        (lambda nodes: nodes.pop(), "0 live matches"),
        (
            lambda nodes: nodes.append(
                {
                    "node_id": "extra",
                    "node_name": "n2",
                    "node_address": "10.0.0.3",
                    "alive": True,
                    "cpu": 32.0,
                    "gpu": 4.0,
                }
            ),
            "unplanned live",
        ),
        (lambda nodes: nodes[1].update(gpu=3.0), "resources"),
    ],
)
def test_missing_extra_and_under_resourced_nodes_fail(mutation, expected):
    plan = _plan()
    nodes = _nodes()
    mutation(nodes)
    ready, blockers = evaluate_cluster(plan, _binding(plan), nodes, {"CPU": 64.0, "GPU": 8.0})
    assert not ready
    assert expected in " ".join(blockers)


def test_snapshot_loader_recomputes_the_verdict(tmp_path):
    plan = _plan()
    binding = _binding(plan)
    payload = {
        "schema_version": 1,
        "deployment_id": plan.deployment_id,
        "generation": binding.generation,
        "deployment_plan_hash": plan.deployment_plan_hash,
        "site_profile_hash": plan.site_profile_hash,
        "allocation_binding_hash": binding.allocation_binding_hash,
        "observed_at": time.time(),
        "ready": True,
        "blockers": [],
        "nodes": _nodes(),
        "cluster_resources": {"CPU": 64.0, "GPU": 8.0},
    }
    path = tmp_path / "snapshot.json"
    path.write_text(json.dumps(payload))
    assert load_probe_snapshot(str(path), plan=plan, binding=binding)["ready"]

    payload["nodes"].pop()
    path.write_text(json.dumps(payload))
    with pytest.raises(RayClusterProbeError, match="verdict"):
        load_probe_snapshot(str(path), plan=plan, binding=binding)
