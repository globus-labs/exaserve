"""P02 / IMP-B04: exact per-instance receipts; v1 fails closed."""

from __future__ import annotations

import pytest

from exaserve.compat.receipt_v2 import (
    SCHEMA_VERSION,
    CompatibilityReceiptV2,
    ExactReceiptLedger,
    PatchResult,
    ReceiptError,
    receipt_from_dict,
    validate_receipt,
)
from exaserve.plan.compiler import compile_deployment_plan
from exaserve.plan.contracts import SiteProfile, build_allocation_binding

H = "a" * 64


def _site():
    return SiteProfile(
        schema_version=2, site_id="s", max_nodes=64, gpus_per_node=12,
        cpus_per_node=64, scheduler_types=("pbs",), gateway_kinds=("haproxy",),
        vendors=("xpu",), engines=("vllm",), model_storage_path="/m",
        local_stage_path="/t").finalize()


def _plan(nodes=2):
    raw = {"num_nodes": nodes, "num_gpus_per_node": 12,
           "models": [{"model_id": "a/b", "tensor_parallel_size": 1,
                       "max_model_len": 4096, "size": 8}],
           "gateway": {"kind": "haproxy", "port": 4001}}
    return compile_deployment_plan(raw, site=_site(), deployment_id="d")


def _binding(plan, nodes=None):
    nodes = nodes or [f"n{i}" for i in range(plan.num_nodes)]
    return build_allocation_binding(plan=plan, generation=1,
                                    scheduler_allocation_id="j", nodes=nodes)


def _receipt(plan, binding, *, requirement, role, rank, node, instance="i1",
             patches=None, scope="RANK", attestation="SELF"):
    return CompatibilityReceiptV2(
        schema_version=SCHEMA_VERSION, deployment_id=plan.deployment_id,
        generation=binding.generation,
        deployment_plan_hash=plan.deployment_plan_hash,
        site_profile_hash=plan.site_profile_hash,
        allocation_binding_hash=binding.allocation_binding_hash,
        compatibility_profile_hash=H, manifest_hash=H,
        receipt_requirement_id=requirement, role=role,
        component_id=f"{role}@{node}", instance_id=instance,
        owner_scope=scope, owner_rank=(None if scope == "GLOBAL" else rank),
        node_id=node, pid=123, actor_id=None,
        executable_hash=H, argv_hash=H, prepared_environment_hash=H,
        observed_versions={"ray": "2.53.0"}, observed_package_hashes={},
        observed_source_hashes={},
        patch_results=(patches if patches is not None else
                       {"SC-01": PatchResult("APPLIED", True)}),
        capabilities=("x",), attestation_type=attestation,
        attested_at="2026-08-07T00:00:00Z").finalize()


# -- schema ------------------------------------------------------------------

def test_a_version_1_payload_fails_closed():
    with pytest.raises(ReceiptError, match="not permissively upgraded"):
        receipt_from_dict({"schema_version": 1, "role": "replica"})


def test_applied_requires_a_true_postcondition():
    with pytest.raises(ReceiptError, match="true semantic postcondition"):
        PatchResult("APPLIED", False)


def test_not_required_cannot_excuse_a_targeted_patch():
    """The audit's #15 in its general form."""
    plan, binding = _plan(), None
    binding = _binding(plan)
    receipt = _receipt(plan, binding, requirement="rank0/ray_head",
                       role="ray_head", rank=0, node="n0",
                       patches={"SC-01": PatchResult("NOT_REQUIRED", True)})
    with pytest.raises(ReceiptError, match="cannot be reported NOT_REQUIRED"):
        validate_receipt(receipt, required_patch_ids={"SC-01"},
                         resolved_not_required=set())


def test_not_required_is_legal_when_the_manifest_excludes_it():
    plan = _plan(); binding = _binding(plan)
    receipt = _receipt(plan, binding, requirement="rank0/ray_head",
                       role="ray_head", rank=0, node="n0",
                       patches={"SC-01": PatchResult("NOT_REQUIRED", True)})
    validate_receipt(receipt, required_patch_ids={"SC-01"},
                     resolved_not_required={"SC-01"})


def test_a_missing_required_patch_key_fails():
    plan = _plan(); binding = _binding(plan)
    receipt = _receipt(plan, binding, requirement="rank0/ray_head",
                       role="ray_head", rank=0, node="n0", patches={})
    with pytest.raises(ReceiptError, match="missing required patch"):
        validate_receipt(receipt, required_patch_ids={"SC-01"})


def test_global_requires_null_rank_and_rank_requires_one():
    plan = _plan(); binding = _binding(plan)
    bad = _receipt(plan, binding, requirement="global/supervisor",
                   role="supervisor", rank=0, node="n0", scope="GLOBAL")
    from dataclasses import replace
    with pytest.raises(ReceiptError, match="null exactly for GLOBAL"):
        validate_receipt(replace(bad, owner_rank=0), required_patch_ids=set())


def test_receipt_hash_mismatch_is_detected():
    from dataclasses import replace

    plan = _plan(); binding = _binding(plan)
    receipt = _receipt(plan, binding, requirement="rank0/ray_head",
                       role="ray_head", rank=0, node="n0")
    tampered = replace(receipt, node_id="somewhere-else")
    with pytest.raises(ReceiptError, match="receipt_hash mismatch"):
        validate_receipt(tampered, required_patch_ids={"SC-01"})


# -- exact-set reconciliation ------------------------------------------------

def _fill(ledger, plan, binding, *, skip=(), extra_instance=None):
    for req in plan.receipt_requirements:
        if req.receipt_requirement_id in skip:
            continue
        rank = req.planned_rank
        node = binding.node_for(rank) if rank is not None else "n0"
        receipt = _receipt(plan, binding, requirement=req.receipt_requirement_id,
                           role=req.role, rank=rank, node=node,
                           scope=req.owner_scope,
                           instance=extra_instance or "i1")
        ledger.accept(receipt, required_patch_ids={"SC-01"},
                      session_rank=rank, session_node=node,
                      from_global_authority=(req.owner_scope == "GLOBAL"))


def test_readiness_needs_exact_set_equality_not_a_count():
    """Duplicate-plus-missing with a matching total must still fail."""
    plan = _plan(3); binding = _binding(plan)
    ledger = ExactReceiptLedger(plan, binding)
    _fill(ledger, plan, binding, skip={"rank2/ray_worker"})
    ok, detail = ledger.satisfied()
    assert not ok and "rank2/ray_worker" in detail["missing"]

    # Add an EXTRA receipt for an already-covered slot: the count now matches
    # the planned total, but the set does not.
    dup = _receipt(plan, binding, requirement="rank1/ray_worker",
                   role="ray_worker", rank=1, node="n1", instance="other")
    ledger.accept(dup, required_patch_ids={"SC-01"}, session_rank=1,
                  session_node="n1")
    ok, detail = ledger.satisfied()
    assert not ok, "count parity must not be mistaken for coverage"
    assert "rank2/ray_worker" in detail["missing"]


def test_a_complete_exact_set_satisfies():
    plan = _plan(2); binding = _binding(plan)
    ledger = ExactReceiptLedger(plan, binding)
    _fill(ledger, plan, binding)
    ok, detail = ledger.satisfied()
    assert ok, detail


def test_one_receipt_cannot_represent_several_instances():
    plan = _plan(3); binding = _binding(plan)
    ledger = ExactReceiptLedger(plan, binding)
    one = _receipt(plan, binding, requirement="rank0/ray_head",
                   role="ray_head", rank=0, node="n0")
    ledger.accept(one, required_patch_ids={"SC-01"}, session_rank=0,
                  session_node="n0")
    ok, detail = ledger.satisfied()
    assert not ok and detail["accepted"] == 1 and detail["planned"] == 8


def test_a_rank_cannot_submit_a_global_receipt():
    plan = _plan(); binding = _binding(plan)
    ledger = ExactReceiptLedger(plan, binding)
    receipt = _receipt(plan, binding, requirement="global/supervisor",
                       role="supervisor", rank=None, node="n0", scope="GLOBAL")
    ok, why = ledger.accept(receipt, required_patch_ids={"SC-01"},
                            session_rank=0, session_node="n0")
    assert not ok and "in-process supervisor authority" in why


def test_a_rank_cannot_submit_another_ranks_receipt():
    plan = _plan(); binding = _binding(plan)
    ledger = ExactReceiptLedger(plan, binding)
    receipt = _receipt(plan, binding, requirement="rank1/ray_worker",
                       role="ray_worker", rank=1, node="n1")
    ok, why = ledger.accept(receipt, required_patch_ids={"SC-01"},
                            session_rank=0, session_node="n0")
    assert not ok and "authenticated rank" in why


def test_a_node_id_that_disagrees_with_the_binding_is_refused():
    plan = _plan(); binding = _binding(plan)
    ledger = ExactReceiptLedger(plan, binding)
    receipt = _receipt(plan, binding, requirement="rank0/ray_head",
                       role="ray_head", rank=0, node="somewhere-else")
    ok, why = ledger.accept(receipt, required_patch_ids={"SC-01"},
                            session_rank=0, session_node="somewhere-else")
    assert not ok and "bound" in why


def test_a_stale_generation_receipt_is_refused():
    plan = _plan(); binding = _binding(plan)
    other = build_allocation_binding(plan=plan, generation=9,
                                     scheduler_allocation_id="j2",
                                     nodes=["n0", "n1"])
    ledger = ExactReceiptLedger(plan, binding)
    receipt = _receipt(plan, other, requirement="rank0/ray_head",
                       role="ray_head", rank=0, node="n0")
    ok, why = ledger.accept(receipt, required_patch_ids={"SC-01"},
                            session_rank=0, session_node="n0")
    assert not ok and "another generation" in why


def test_a_duplicate_adds_no_coverage_and_a_conflict_is_refused():
    plan = _plan(); binding = _binding(plan)
    ledger = ExactReceiptLedger(plan, binding)
    receipt = _receipt(plan, binding, requirement="rank0/ray_head",
                       role="ray_head", rank=0, node="n0")
    assert ledger.accept(receipt, required_patch_ids={"SC-01"},
                         session_rank=0, session_node="n0")[0]
    ok, why = ledger.accept(receipt, required_patch_ids={"SC-01"},
                            session_rank=0, session_node="n0")
    assert ok and why == "duplicate" and ledger.count() == 1


def test_losing_a_rank_removes_its_evidence():
    plan = _plan(2); binding = _binding(plan)
    ledger = ExactReceiptLedger(plan, binding)
    _fill(ledger, plan, binding)
    assert ledger.satisfied()[0]
    ledger.drop_rank(1)
    ok, detail = ledger.satisfied()
    assert not ok and any("rank1" in m for m in detail["missing"])


def test_a_restart_supersedes_the_slots_evidence():
    plan = _plan(2); binding = _binding(plan)
    ledger = ExactReceiptLedger(plan, binding)
    _fill(ledger, plan, binding)
    ledger.supersede_instance("rank0/ray_head", "i1")
    ok, detail = ledger.satisfied()
    assert not ok and "rank0/ray_head" in detail["missing"]


def test_high_cardinality_reconciliation_is_linear():
    """256-equivalent cardinality without hardware (§3.2.1 scale note)."""
    plan = _plan(64); binding = _binding(plan)
    ledger = ExactReceiptLedger(plan, binding)
    _fill(ledger, plan, binding)
    ok, detail = ledger.satisfied()
    assert ok, detail
    assert detail["planned"] == 2 + 64 * 2
