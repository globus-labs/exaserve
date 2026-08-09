"""Canonical plan/binding artifacts fail closed at the runtime boundary."""

from __future__ import annotations

import json
from dataclasses import asdict

import pytest

from exaserve.plan.compiler import compile_deployment_plan
from exaserve.plan.contracts import PlanError, SiteProfile, build_allocation_binding
from exaserve.plan.io import (
    allocation_binding_from_dict,
    deployment_plan_from_dict,
    load_deployment_plan,
    resolve_engine_worker_receipt_requirement,
    resolve_replica_receipt_requirement,
    load_site_profile,
    site_profile_from_dict,
    write_deployment_plan,
    write_site_profile,
    load_run_plan,
    run_plan_from_dict,
    write_run_plan,
    component_instance_binding_from_dict,
    run_provenance_from_dict,
)
from exaserve.plan.runtime_binding import LiveNodeInventory, bind_runtime_deployment


def _site() -> SiteProfile:
    return SiteProfile(
        schema_version=3,
        site_id="test",
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
        environment_profile_ref="profile-ref",
        prepared_environment=(("RAY_TEST_SETTING", "exact"),),
        environment_unset=("ONEAPI_DEVICE_SELECTOR",),
        stack_size_kb=8192,
    ).finalize()


def _plan():
    return compile_deployment_plan(
        {
            "num_nodes": 2,
            "num_gpus_per_node": 4,
            "validation_mode": True,
            "models": [
                {
                    "model_id": "org/model",
                    "tensor_parallel_size": 2,
                    "num_replicas": 4,
                    "max_model_len": 128,
                    "size": 8,
                }
            ],
        },
        site=_site(),
        deployment_id="deployment",
    )


def _json_artifact(value):
    return json.loads(json.dumps(asdict(value)))


def test_plan_artifact_round_trips_with_the_same_identity(tmp_path):
    plan = _plan()
    path = tmp_path / "deployment.plan.json"
    write_deployment_plan(path, plan)
    loaded = load_deployment_plan(str(path))
    assert loaded == plan
    assert loaded.deployment_plan_hash == plan.deployment_plan_hash


def test_plan_artifact_is_create_once_and_identical_retry_is_idempotent(tmp_path):
    from dataclasses import replace

    plan = _plan()
    path = tmp_path / "deployment.plan.json"
    write_deployment_plan(path, plan)
    write_deployment_plan(path, plan)

    replacement = replace(plan, deployment_id="another-deployment").finalize()
    with pytest.raises(PlanError, match="refusing to replace immutable DeploymentPlan"):
        write_deployment_plan(path, replacement)
    assert load_deployment_plan(path) == plan


def test_plan_writer_refuses_an_existing_symlink(tmp_path):
    plan = _plan()
    target = tmp_path / "target.json"
    target.write_text("do not replace", encoding="utf-8")
    path = tmp_path / "deployment.plan.json"
    path.symlink_to(target)

    with pytest.raises(PlanError, match="not safely readable"):
        write_deployment_plan(path, plan)
    assert target.read_text(encoding="utf-8") == "do not replace"


def test_plan_loader_refuses_a_symlink_even_when_target_is_valid(tmp_path):
    target = tmp_path / "target.json"
    write_deployment_plan(target, _plan())
    path = tmp_path / "deployment.plan.json"
    path.symlink_to(target)

    with pytest.raises(PlanError, match="could not load compiled plan"):
        load_deployment_plan(path)


@pytest.mark.parametrize("alias", [True, 3.0])
def test_identical_retry_requires_json_type_identity(tmp_path, alias):
    plan = _plan()
    path = tmp_path / "deployment.plan.json"
    payload = _json_artifact(plan)
    payload["schema_version"] = alias
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(PlanError, match="refusing to replace immutable DeploymentPlan"):
        write_deployment_plan(path, plan)


def test_plan_file_decoder_rejects_duplicate_keys_and_nonfinite_numbers(tmp_path):
    path = tmp_path / "deployment.plan.json"
    write_deployment_plan(path, _plan())
    text = path.read_text(encoding="utf-8")
    path.write_text(
        text.replace('"schema_version": 3', '"schema_version": 3, "schema_version": 3', 1)
    )
    with pytest.raises(PlanError, match="duplicate JSON key"):
        load_deployment_plan(path)

    path.write_text(text.replace('"ray_port": 6379', '"ray_port": NaN', 1))
    with pytest.raises(PlanError, match="non-finite JSON"):
        load_deployment_plan(path)


def test_run_plan_artifact_round_trips_and_verifies_both_hashes(tmp_path):
    from exaserve.plan.compiler import compile_run_plan

    run = compile_run_plan(
        {
            "num_nodes": 2,
            "num_gpus_per_node": 4,
            "validation_mode": True,
            "models": [
                {
                    "model_id": "org/model",
                    "tensor_parallel_size": 2,
                    "num_replicas": 4,
                    "max_model_len": 128,
                    "size": 8,
                }
            ],
        },
        site=_site(),
        run_id="run-1",
        deployment_id="deployment",
    )
    path = tmp_path / "run.plan.json"
    assert run.client.destination == run.workload.client_dest == "direct"
    assert run.client.nodes == run.workload.client_nodes == run.deployment.num_nodes
    write_run_plan(path, run)
    assert load_run_plan(path) == run

    payload = json.loads(path.read_text())
    payload["workload"]["seed"] += 1
    with pytest.raises(PlanError, match="run plan hash mismatch"):
        run_plan_from_dict(payload)

    payload = json.loads(path.read_text())
    payload["deployment"]["ray_port"] += 1
    with pytest.raises(PlanError, match="compiled plan hash mismatch"):
        run_plan_from_dict(payload)


def test_runtime_binding_and_provenance_artifacts_are_content_verified():
    import time

    from exaserve.plan.contracts import (
        SCHEMA_VERSION,
        ComponentInstanceBinding,
        RunProvenance,
    )

    plan = _plan()
    allocation = build_allocation_binding(
        plan=plan, generation=7, scheduler_allocation_id="job", nodes=["n0", "n1"]
    )
    instance = ComponentInstanceBinding(
        schema_version=SCHEMA_VERSION,
        deployment_id=plan.deployment_id,
        generation=7,
        deployment_plan_hash=plan.deployment_plan_hash,
        site_profile_hash=plan.site_profile_hash,
        allocation_binding_hash=allocation.allocation_binding_hash,
        receipt_requirement_id="rank0/ray_head",
        component_id="ray",
        instance_id="n0:123",
        owner_scope="RANK",
        owner_rank=0,
        node_id="n0",
        bound_at=time.time(),
        state="ACTIVE",
    ).finalize()
    assert component_instance_binding_from_dict(_json_artifact(instance)) == instance
    changed = _json_artifact(instance)
    changed["node_id"] = "n1"
    with pytest.raises(PlanError, match="binding hash mismatch"):
        component_instance_binding_from_dict(changed)

    provenance = RunProvenance(
        schema_version=SCHEMA_VERSION,
        run_id="run-1",
        deployment_id=plan.deployment_id,
        generation=7,
        deployment_plan_hash=plan.deployment_plan_hash,
        allocation_binding_hash=allocation.allocation_binding_hash,
        run_semantic_hash=None,
        source_snapshot_hash="1" * 64,
        resolved_input_paths=("/inputs/config.yaml",),
        argv=("exaserve-launch-cluster", "deployment.plan.json"),
        prepared_environment_hash="2" * 64,
        started_at="2026-08-07T00:00:00Z",
        output_locations=("/outputs/run-1",),
    ).finalize()
    assert run_provenance_from_dict(_json_artifact(provenance)) == provenance
    changed = _json_artifact(provenance)
    changed["started_at"] = "2026-08-08T00:00:00Z"
    with pytest.raises(PlanError, match="provenance hash mismatch"):
        run_provenance_from_dict(changed)


def test_site_profile_is_a_separate_hash_verified_artifact(tmp_path):
    profile = _site()
    path = tmp_path / "site.profile.json"
    write_site_profile(path, profile)
    assert load_site_profile(path) == profile

    payload = json.loads(path.read_text())
    payload["prepared_environment"][0][1] = "drifted"
    with pytest.raises(PlanError, match="site profile hash mismatch"):
        site_profile_from_dict(payload)


def test_site_profile_rejects_unknown_or_ambiguous_environment_fields():
    payload = _json_artifact(_site())
    payload["unknown"] = 1
    with pytest.raises(PlanError, match="shape mismatch"):
        site_profile_from_dict(payload)

    payload = _json_artifact(_site())
    payload["environment_unset"] = ["RAY_TEST_SETTING"]
    with pytest.raises(PlanError, match="both set and unset"):
        site_profile_from_dict(payload)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda data: data.update({"unknown": True}),
        lambda data: data.pop("control"),
        lambda data: data["models"][0].update({"unknown": 1}),
        lambda data: data["models"][0]["replicas"][0].pop("gpu_demand"),
        lambda data: data["receipt_requirements"][0].pop("attestation_type"),
    ],
)
def test_plan_artifact_rejects_unknown_or_missing_fields(mutation):
    payload = _json_artifact(_plan())
    mutation(payload)
    with pytest.raises(PlanError, match="shape mismatch"):
        deployment_plan_from_dict(payload)


def test_plan_artifact_recomputes_the_hash_after_rehydration():
    payload = _json_artifact(_plan())
    payload["models"][0]["max_model_len"] += 1
    with pytest.raises(PlanError, match="hash mismatch"):
        deployment_plan_from_dict(payload)


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_version", True),
        ("collect_stats", 1),
        ("validation_mode", "true"),
    ],
)
def test_plan_artifact_does_not_coerce_value_families(field, value):
    payload = _json_artifact(_plan())
    payload[field] = value
    with pytest.raises(PlanError):
        deployment_plan_from_dict(payload)


def test_allocation_binding_round_trip_and_tamper_detection():
    plan = _plan()
    binding = build_allocation_binding(
        plan=plan, generation=7, scheduler_allocation_id="job", nodes=["n0.example", "n1.example"]
    )
    assert allocation_binding_from_dict(_json_artifact(binding)) == binding

    tampered = _json_artifact(binding)
    tampered["rank_to_node"][1] = [1, "somewhere-else"]
    with pytest.raises(PlanError, match="hash mismatch"):
        allocation_binding_from_dict(tampered)


def test_allocation_binding_rejects_duplicate_canonical_nodes():
    plan = _plan()
    binding = build_allocation_binding(
        plan=plan, generation=7, scheduler_allocation_id="job", nodes=["n0.example", "n1.example"]
    )
    payload = _json_artifact(binding)
    payload["rank_to_node"] = [[0, "n0.example"], [1, "N0"]]
    # Make the declared hash irrelevant: construction itself must reject the map.
    with pytest.raises(PlanError, match="duplicate canonical node"):
        allocation_binding_from_dict(payload)


def test_logical_replica_maps_to_the_exact_precompiled_replica_and_engine_slots():
    plan = _plan()
    replica = plan.models[0].replicas[2]
    rank = replica.planned_ranks[0]
    base = resolve_replica_receipt_requirement(
        plan=plan, model_id="org/model", replica_index=2, role="replica"
    )
    engine_core = resolve_replica_receipt_requirement(
        plan=plan, model_id="org/model", replica_index=2, role="engine_core"
    )
    assert base == "model/org--model/replica/2"
    assert engine_core == base + "/engine/core"
    for worker_rank, device_id in enumerate(replica.planned_device_ids[0]):
        worker = resolve_engine_worker_receipt_requirement(
            plan=plan,
            model_id="org/model",
            replica_index=2,
            worker_rank=worker_rank,
            owner_rank=rank,
        )
        assert worker == base + f"/engine/worker/stage0/device{device_id}"


def test_logical_replica_binding_does_not_depend_on_ray_selected_device_id():
    plan = _plan()
    assert (
        resolve_replica_receipt_requirement(
            plan=plan, model_id="org/model", replica_index=2, role="replica"
        )
        == "model/org--model/replica/2"
    )


def test_logical_receipt_binding_rejects_wrong_replica_worker_or_owner_rank():
    plan = _plan()
    with pytest.raises(PlanError, match="maps to 0 planned replicas"):
        resolve_replica_receipt_requirement(
            plan=plan, model_id="org/model", replica_index=77, role="replica"
        )
    with pytest.raises(PlanError, match="outside replica world size"):
        resolve_engine_worker_receipt_requirement(
            plan=plan,
            model_id="org/model",
            replica_index=2,
            worker_rank=99,
            owner_rank=0,
        )
    replica = plan.models[0].replicas[2]
    with pytest.raises(PlanError, match="planned rank"):
        resolve_engine_worker_receipt_requirement(
            plan=plan,
            model_id="org/model",
            replica_index=2,
            worker_rank=0,
            owner_rank=replica.planned_ranks[0] + 1,
        )


def test_runtime_binds_canonical_topology_without_replanning():
    plan = _plan()
    binding = build_allocation_binding(
        plan=plan, generation=7, scheduler_allocation_id="job", nodes=["n0.example", "n1.example"]
    )
    # Live discovery may order by IP rather than allocation rank. Hostname is
    # the parity key; the adapter must not silently assign rank by list order.
    live = [
        LiveNodeInventory(
            ip="10.0.0.2",
            resource_key="node:10.0.0.2",
            total_gpus=4,
            total_cpus=32,
            hostname="n1",
        ),
        LiveNodeInventory(
            ip="10.0.0.1",
            resource_key="node:10.0.0.1",
            total_gpus=4,
            total_cpus=32,
            hostname="n0",
        ),
    ]
    result = bind_runtime_deployment(plan, binding, live)
    assert [(item.rank, item.inventory.ip) for item in result.nodes] == [
        (0, "10.0.0.1"),
        (1, "10.0.0.2"),
    ]
    placements = result.models[0].replicas
    assert [item.replica_index for item in placements] == [0, 1, 2, 3]
    assert [item.owner_rank for item in placements] == [0, 0, 1, 1]
    assert [item.node_ips[0] for item in placements] == [
        "10.0.0.1",
        "10.0.0.1",
        "10.0.0.2",
        "10.0.0.2",
    ]


def test_runtime_refuses_a_live_inventory_that_does_not_match_binding():
    plan = _plan()
    binding = build_allocation_binding(
        plan=plan, generation=7, scheduler_allocation_id="job", nodes=["n0", "n1"]
    )
    live = [
        LiveNodeInventory(
            ip="10.0.0.1",
            resource_key="node:10.0.0.1",
            total_gpus=4,
            total_cpus=32,
            hostname="someone-else",
        )
    ]
    with pytest.raises(PlanError, match="live Ray GPU nodes"):
        bind_runtime_deployment(plan, binding, live)


def test_runtime_inventory_contract_rejects_coercion_and_scalar_sequences():
    with pytest.raises(PlanError, match="total_gpus"):
        LiveNodeInventory(
            ip="10.0.0.1",
            resource_key="node:10.0.0.1",
            total_gpus="4",  # type: ignore[arg-type]
            total_cpus=32,
        )
    plan = _plan()
    binding = build_allocation_binding(
        plan=plan,
        generation=7,
        scheduler_allocation_id="job",
        nodes=["n0", "n1"],
    )
    with pytest.raises(PlanError, match="inventory must be a sequence"):
        bind_runtime_deployment(plan, binding, "n0")
