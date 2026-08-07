"""P01 / IMP-H01: one compiled plan with distinct hash boundaries."""

from __future__ import annotations

import pytest

from exaserve.plan.compiler import compile_deployment_plan, compile_run_plan
from exaserve.plan.contracts import (
    ControlLimits,
    PlanError,
    SiteProfile,
    build_allocation_binding,
    provenance_hash,
)


def _site(**kw) -> SiteProfile:
    base = dict(
        schema_version=2, site_id="aurora", max_nodes=64, gpus_per_node=12,
        cpus_per_node=64, scheduler_types=("pbs", "slurm"),
        gateway_kinds=("haproxy", "nginx"), vendors=("xpu",), engines=("vllm",),
        model_storage_path="/lus/models", local_stage_path="/tmp/hf_home")
    base.update(kw)
    return SiteProfile(**base).finalize()


def _raw(**kw):
    base = {
        "num_nodes": 2, "num_gpus_per_node": 12,
        "models": [{"model_id": "meta-llama/Meta-Llama-3-8B-Instruct",
                    "tensor_parallel_size": 1, "max_model_len": 4096, "size": 8}],
        "gateway": {"kind": "haproxy", "port": 4001},
    }
    base.update(kw)
    return base


# -- gateway / exposure ------------------------------------------------------

def test_production_requires_a_real_gateway():
    """`gateway: none` was BOTH legal and the implicit default."""
    with pytest.raises(PlanError, match="Production exposure requires"):
        compile_deployment_plan(_raw(gateway=None), site=_site(), deployment_id="d")


def test_the_legacy_none_spelling_is_not_a_gateway():
    for spelling in ("none", "direct", {"type": "none"}):
        with pytest.raises(PlanError, match="Production exposure requires"):
            compile_deployment_plan(_raw(gateway=spelling), site=_site(),
                                    deployment_id="d")


def test_direct_exposure_compiles_only_in_explicit_validation_mode():
    plan = compile_deployment_plan(
        _raw(gateway=None, validation_mode=True,
             exposure={"mode": "DIRECT_VALIDATION"}),
        site=_site(), deployment_id="d")
    assert plan.gateway is None
    assert not plan.is_production_exposure()


def test_validation_mode_still_refuses_a_mismatched_exposure_mode():
    with pytest.raises(PlanError, match="DIRECT_VALIDATION"):
        compile_deployment_plan(
            _raw(gateway=None, validation_mode=True,
                 exposure={"mode": "PROXIED_INTERNAL"}),
            site=_site(), deployment_id="d")


def test_a_declared_gateway_cannot_claim_direct_exposure():
    with pytest.raises(PlanError, match="PROXIED_INTERNAL"):
        compile_deployment_plan(
            _raw(exposure={"mode": "DIRECT_VALIDATION"}), site=_site(),
            deployment_id="d")


def test_a_non_first_release_gateway_needs_validation_mode():
    with pytest.raises(PlanError, match="not a first-release production"):
        compile_deployment_plan(_raw(gateway={"kind": "nginx", "port": 4001}),
                                site=_site(), deployment_id="d")


def test_a_gateway_the_site_does_not_support_is_refused():
    with pytest.raises(PlanError, match="does not support"):
        compile_deployment_plan(_raw(gateway={"kind": "envoy", "port": 4001}),
                                site=_site(), deployment_id="d")


# -- hash boundaries ---------------------------------------------------------

def test_serving_changes_move_the_deployment_hash():
    a = compile_deployment_plan(_raw(), site=_site(), deployment_id="d")
    b = compile_deployment_plan(_raw(num_nodes=4), site=_site(), deployment_id="d")
    assert a.deployment_plan_hash != b.deployment_plan_hash


def test_site_drift_moves_the_bound_deployment_hash():
    a = compile_deployment_plan(_raw(), site=_site(), deployment_id="d")
    b = compile_deployment_plan(_raw(), site=_site(cpus_per_node=96),
                                deployment_id="d")
    assert a.deployment_plan_hash != b.deployment_plan_hash


def test_the_same_input_compiles_to_a_byte_identical_hash():
    """CLI/core/eval/ClientLab must agree on identity from one input."""
    hashes = {compile_deployment_plan(_raw(), site=_site(),
                                      deployment_id="d").deployment_plan_hash
              for _ in range(5)}
    assert len(hashes) == 1


def test_workload_changes_move_only_the_run_hash():
    from exaserve.plan.contracts import WorkloadPolicy

    a = compile_run_plan(_raw(), site=_site(), run_id="r", deployment_id="d",
                         workload=WorkloadPolicy(duration_s=10))
    b = compile_run_plan(_raw(), site=_site(), run_id="r", deployment_id="d",
                         workload=WorkloadPolicy(duration_s=30))
    assert a.deployment.deployment_plan_hash == b.deployment.deployment_plan_hash
    assert a.run_semantic_hash != b.run_semantic_hash


def test_paths_and_timestamps_touch_only_provenance():
    plan = compile_deployment_plan(_raw(), site=_site(), deployment_id="d")
    binding = build_allocation_binding(plan=plan, generation=1,
                                       scheduler_allocation_id="job1",
                                       nodes=["n0", "n1"])
    first = provenance_hash(deployment_plan_hash=plan.deployment_plan_hash,
                            allocation_binding_hash=binding.allocation_binding_hash,
                            source_path="/a/x.yaml", started_at="t1")
    second = provenance_hash(deployment_plan_hash=plan.deployment_plan_hash,
                             allocation_binding_hash=binding.allocation_binding_hash,
                             source_path="/b/x.yaml", started_at="t2")
    assert first != second
    assert plan.deployment_plan_hash == plan.compute_hash()


def test_a_restart_on_different_nodes_changes_only_the_binding():
    plan = compile_deployment_plan(_raw(), site=_site(), deployment_id="d")
    first = build_allocation_binding(plan=plan, generation=1,
                                     scheduler_allocation_id="job1",
                                     nodes=["n0", "n1"])
    second = build_allocation_binding(plan=plan, generation=2,
                                      scheduler_allocation_id="job2",
                                      nodes=["n7", "n8"])
    assert first.allocation_binding_hash != second.allocation_binding_hash
    assert first.deployment_plan_hash == second.deployment_plan_hash


def test_a_node_count_mismatch_refuses_to_bind():
    plan = compile_deployment_plan(_raw(), site=_site(), deployment_id="d")
    with pytest.raises(PlanError, match="different deployment"):
        build_allocation_binding(plan=plan, generation=1,
                                 scheduler_allocation_id="j", nodes=["only-one"])


# -- receipt slots -----------------------------------------------------------

def test_the_plan_enumerates_exact_receipt_slots():
    plan = compile_deployment_plan(_raw(num_nodes=3), site=_site(),
                                   deployment_id="d")
    ids = plan.requirement_keys()
    assert "global/supervisor" in ids
    assert "global/gateway/haproxy" in ids
    assert "rank0/ray_head" in ids and "rank2/ray_worker" in ids
    # One slot per rank per component -- not one per role.
    assert len(plan.requirements_for_rank(1)) == 2
    assert len(ids) == 2 + 3 * 2


def test_a_rank_slot_never_pins_an_allocation_hostname():
    """The plan is compiled BEFORE the allocation exists."""
    plan = compile_deployment_plan(_raw(), site=_site(), deployment_id="d")
    for requirement in plan.receipt_requirements:
        assert "n0" not in requirement.receipt_requirement_id
        assert requirement.placement == "" or "host" not in requirement.placement


def test_global_requirements_do_not_pin_a_rank():
    with pytest.raises(PlanError, match="must not pin a rank"):
        from exaserve.plan.contracts import ReceiptRequirement

        ReceiptRequirement(receipt_requirement_id="x", role="r",
                           component_slot="c", owner_scope="GLOBAL",
                           planned_rank=0)


# -- control limits ----------------------------------------------------------

def test_a_lease_shorter_than_three_heartbeats_is_refused():
    with pytest.raises(PlanError, match="3 \\* heartbeat_interval_s"):
        ControlLimits(heartbeat_interval_s=10.0, lease_timeout_s=20.0)


def test_control_limits_reject_nonpositive_values():
    with pytest.raises(PlanError, match="must be positive"):
        ControlLimits(registration_deadline_s=0)
    with pytest.raises(PlanError, match="positive int"):
        ControlLimits(max_frame_bytes=0)


def test_control_limits_are_not_evidence_backed_by_default():
    """A SiteProfile is not production-qualified until values are measured."""
    assert ControlLimits().evidence_backed is False


# -- strictness --------------------------------------------------------------

def test_unknown_deployment_keys_are_refused():
    with pytest.raises(PlanError, match="unknown key"):
        compile_deployment_plan(_raw(num_node=4), site=_site(), deployment_id="d")


def test_model_identity_collisions_are_refused():
    raw = _raw(models=[
        {"model_id": "org/Model-X", "tensor_parallel_size": 1,
         "max_model_len": 4096, "size": 8},
        {"model_id": "org/model.x", "tensor_parallel_size": 1,
         "max_model_len": 4096, "size": 8}])
    with pytest.raises(PlanError, match="identity collision"):
        compile_deployment_plan(raw, site=_site(), deployment_id="d")


def test_node_and_gpu_limits_come_from_the_site():
    with pytest.raises(PlanError, match="exceeds site"):
        compile_deployment_plan(_raw(num_nodes=999), site=_site(), deployment_id="d")
    with pytest.raises(PlanError, match="exceeds gpus_per_node"):
        compile_deployment_plan(
            _raw(models=[{"model_id": "a/b", "tensor_parallel_size": 99,
                          "max_model_len": 4096, "size": 8}]),
            site=_site(), deployment_id="d")
