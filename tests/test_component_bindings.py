"""WP2: current process/actor slots are durable generation artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from exaserve.plan.compiler import compile_deployment_plan
from exaserve.plan.contracts import SiteProfile, build_allocation_binding
from exaserve.state.bindings import ComponentBindingStore


def _plan_and_binding():
    site = SiteProfile(
        schema_version=3,
        site_id="test",
        max_nodes=2,
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
    plan = compile_deployment_plan(
        {
            "num_nodes": 1,
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
    binding = build_allocation_binding(
        plan=plan, generation=7, scheduler_allocation_id="job", nodes=["n0"]
    )
    return plan, binding


def _receipt(instance_id: str):
    return SimpleNamespace(
        receipt_requirement_id="rank0/ray_head",
        component_id="ray",
        instance_id=instance_id,
        owner_scope="RANK",
        owner_rank=0,
        node_id="n0",
    )


def test_bind_supersede_revoke_is_append_authoritative(tmp_path):
    plan, binding = _plan_and_binding()
    store = ComponentBindingStore(str(tmp_path), plan=plan, binding=binding)

    first = store.bind_receipt(_receipt("n0:100"))
    assert first.state == "ACTIVE" and first.binding_sequence == 1
    # Idempotent duplicate creates no event/revision.
    assert store.bind_receipt(_receipt("n0:100")) == first

    second = store.bind_receipt(_receipt("n0:200"))
    assert second.state == "ACTIVE"
    assert second.supersedes_instance_id == "n0:100"
    assert second.binding_sequence == 3  # SUPERSEDED then ACTIVE
    assert store.current()[first.key()].instance_id == "n0:200"

    assert store.revoke_slot(first.key())
    assert store.current() == {}
    current = json.loads((tmp_path / "component_bindings" / "current.json").read_text())
    assert current["bindings"] == [] and current["revision"] == 4
    assert len(list((tmp_path / "component_bindings" / "events").glob("*.json"))) == 4

    # The materialization is disposable; recovery trusts verified immutable
    # events and reaches the same projection.
    (tmp_path / "component_bindings" / "current.json").unlink()
    recovered = ComponentBindingStore(str(tmp_path), plan=plan, binding=binding)
    assert recovered.current() == {}
    assert json.loads(Path(recovered.current_path).read_text())["revision"] == 4


def test_current_binding_artifact_carries_every_generation_hash(tmp_path):
    plan, binding = _plan_and_binding()
    store = ComponentBindingStore(str(tmp_path), plan=plan, binding=binding)
    store.bind_receipt(_receipt("n0:100"))
    current = json.loads(Path(store.current_path).read_text())
    assert current["deployment_plan_hash"] == plan.deployment_plan_hash
    assert current["site_profile_hash"] == plan.site_profile_hash
    assert current["allocation_binding_hash"] == binding.allocation_binding_hash


def test_current_projection_can_batch_but_flushes_exactly(tmp_path):
    plan, binding = _plan_and_binding()
    store = ComponentBindingStore(
        str(tmp_path), plan=plan, binding=binding, current_publish_batch_size=64
    )
    store.bind_receipt(_receipt("n0:100"))

    # Neither a durable event segment nor its disposable projection is
    # published before the explicit pre-START/READY/shutdown barrier. A crash
    # here cannot leave a successful status record, and synchronized receipts
    # do not each force one shared-filesystem fsync.
    assert len(list((tmp_path / "component_bindings" / "events").glob("*.json"))) == 0
    assert json.loads(Path(store.current_path).read_text())["revision"] == 0
    assert store.current()["rank0/ray_head"].instance_id == "n0:100"

    store.flush()
    assert len(list((tmp_path / "component_bindings" / "events").glob("*.json"))) == 1
    current = json.loads(Path(store.current_path).read_text())
    assert current["revision"] == 1
    assert current["bindings"][0]["instance_id"] == "n0:100"


def test_event_group_commit_recovers_one_segment_for_many_transitions(tmp_path):
    plan, binding = _plan_and_binding()
    store = ComponentBindingStore(
        str(tmp_path),
        plan=plan,
        binding=binding,
        current_publish_batch_size=64,
        event_publish_batch_size=64,
    )
    store.bind_receipt(_receipt("n0:100"))
    store.bind_receipt(_receipt("n0:200"))
    store.revoke_slot("rank0/ray_head")

    assert list((tmp_path / "component_bindings" / "events").glob("*.json")) == []
    store.flush()
    segments = list((tmp_path / "component_bindings" / "events").glob("*.batch.json"))
    assert len(segments) == 1
    payload = json.loads(segments[0].read_text())
    assert payload["start_sequence"] == 1
    assert payload["end_sequence"] == 4
    assert len(payload["events"]) == 4

    recovered = ComponentBindingStore(
        str(tmp_path),
        plan=plan,
        binding=binding,
        current_publish_batch_size=64,
        event_publish_batch_size=64,
    )
    assert recovered.current() == {}
