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
from exaserve.compat.producers import patch_requirements_for_plan
from exaserve.plan.compiler import compile_deployment_plan
from exaserve.plan.contracts import SiteProfile, build_allocation_binding

H = "a" * 64


def _site():
    return SiteProfile(
        schema_version=3,
        site_id="s",
        max_nodes=64,
        gpus_per_node=12,
        cpus_per_node=64,
        scheduler_types=("pbs",),
        gateway_kinds=("haproxy",),
        vendors=("xpu",),
        engines=("vllm",),
        model_storage_path="/m",
        local_stage_path="/t",
        launcher_capabilities=("ray_serve.run_many",),
    ).finalize()


def _plan(nodes=2):
    raw = {
        "num_nodes": nodes,
        "num_gpus_per_node": 12,
        "models": [
            {"model_id": "a/b", "tensor_parallel_size": 1, "max_model_len": 4096, "size": 8}
        ],
        "gateway": {"kind": "haproxy", "port": 4001},
    }
    return compile_deployment_plan(raw, site=_site(), deployment_id="d")


def _binding(plan, nodes=None):
    nodes = nodes or [f"n{i}" for i in range(plan.num_nodes)]
    return build_allocation_binding(
        plan=plan, generation=1, scheduler_allocation_id="j", nodes=nodes
    )


def _receipt(
    plan,
    binding,
    *,
    requirement,
    role,
    rank,
    node,
    instance="i1",
    patches=None,
    scope="RANK",
    attestation=None,
):
    from exaserve.compat.profile import default_profile
    from exaserve.compat.producers import _observed_profile_hashes

    slot = next(
        item for item in plan.receipt_requirements if item.receipt_requirement_id == requirement
    )
    profile = default_profile(plan.vendor)
    package_hashes, source_hashes = _observed_profile_hashes(profile)
    attestation_type = attestation or slot.attestation_type
    return CompatibilityReceiptV2(
        schema_version=SCHEMA_VERSION,
        deployment_id=plan.deployment_id,
        generation=binding.generation,
        deployment_plan_hash=plan.deployment_plan_hash,
        site_profile_hash=plan.site_profile_hash,
        allocation_binding_hash=binding.allocation_binding_hash,
        compatibility_profile_hash=plan.compatibility_profile_hash,
        manifest_hash=plan.manifest_hash,
        receipt_requirement_id=requirement,
        role=role,
        component_id=slot.component_slot,
        instance_id=instance,
        owner_scope=scope,
        owner_rank=(None if scope == "GLOBAL" else rank),
        node_id=node,
        pid=123,
        actor_id=None,
        executable_hash=H,
        argv_hash=H,
        prepared_environment_hash=H,
        observed_versions={
            "python": profile.python,
            "ray": profile.ray,
            "vllm": profile.vllm,
            "mpi4py": profile.mpi4py,
        },
        observed_package_hashes=package_hashes,
        observed_source_hashes=source_hashes,
        patch_results=(patches if patches is not None else {"SC-01": PatchResult("APPLIED", True)}),
        capabilities=(profile.capabilities() if attestation_type == "SELF" else ()),
        attestation_type=attestation_type,
        attested_at="2026-08-07T00:00:00Z",
    ).finalize()


# -- schema ------------------------------------------------------------------


def test_a_version_1_payload_fails_closed():
    with pytest.raises(ReceiptError, match="not permissively upgraded"):
        receipt_from_dict({"schema_version": 1, "role": "replica"})


def test_patch_gates_are_resolved_per_exact_model_slot_not_deployment_ambient():
    plan = compile_deployment_plan(
        {
            "num_nodes": 2,
            "num_gpus_per_node": 12,
            "validation_mode": True,
            "models": [
                {
                    "model_id": "a/tp-only",
                    "tensor_parallel_size": 1,
                    "pipeline_parallel_size": 1,
                    "num_replicas": 1,
                    "max_model_len": 64,
                    "size": 1,
                },
                {
                    "model_id": "a/pipeline",
                    "tensor_parallel_size": 1,
                    "pipeline_parallel_size": 2,
                    "num_replicas": 1,
                    "max_model_len": 64,
                    "size": 1,
                },
                {
                    "model_id": "a/tensor",
                    "tensor_parallel_size": 2,
                    "pipeline_parallel_size": 1,
                    "num_replicas": 1,
                    "max_model_len": 64,
                    "size": 1,
                },
            ],
        },
        site=_site(),
        deployment_id="mixed",
    )
    tp_id = "model/a--tp-only/replica/0"
    pp_id = "model/a--pipeline/replica/0"
    multiproc_id = "model/a--tensor/replica/0"
    tp_required, tp_excluded = patch_requirements_for_plan(plan, tp_id)
    pp_required, pp_excluded = patch_requirements_for_plan(plan, pp_id)
    assert "SC-01" not in tp_required and "SC-01" in tp_excluded
    assert "SC-01" in pp_required and "SC-01" not in pp_excluded

    tp_core_required, tp_core_excluded = patch_requirements_for_plan(plan, tp_id + "/engine/core")
    pp_core_required, pp_core_excluded = patch_requirements_for_plan(plan, pp_id + "/engine/core")
    multiproc_required, multiproc_excluded = patch_requirements_for_plan(
        plan, multiproc_id + "/engine/core"
    )
    assert tp_core_required == ("EN-01",)
    assert {"EW-01", "EW-02", "EW-03"} <= set(tp_core_excluded)
    assert {"EW-01", "EW-03"} <= set(pp_core_required)
    assert "EW-02" in pp_core_excluded
    assert "EW-02" in multiproc_required
    assert {"EW-01", "EW-03"} <= set(multiproc_excluded)


def test_applied_requires_a_true_postcondition():
    with pytest.raises(ReceiptError, match="true semantic postcondition"):
        PatchResult("APPLIED", False)


def test_not_required_cannot_excuse_a_targeted_patch():
    """The audit's #15 in its general form."""
    plan, binding = _plan(), None
    binding = _binding(plan)
    receipt = _receipt(
        plan,
        binding,
        requirement="rank0/ray_head",
        role="ray_head",
        rank=0,
        node="n0",
        patches={"SC-01": PatchResult("NOT_REQUIRED", True)},
    )
    with pytest.raises(ReceiptError, match="cannot be reported NOT_REQUIRED"):
        validate_receipt(receipt, required_patch_ids={"SC-01"}, resolved_not_required=set())


def test_not_required_is_legal_when_the_manifest_excludes_it():
    plan = _plan()
    binding = _binding(plan)
    receipt = _receipt(
        plan,
        binding,
        requirement="rank0/ray_head",
        role="ray_head",
        rank=0,
        node="n0",
        patches={"SC-01": PatchResult("NOT_REQUIRED", True)},
    )
    validate_receipt(receipt, required_patch_ids={"SC-01"}, resolved_not_required={"SC-01"})


def test_a_missing_required_patch_key_fails():
    plan = _plan()
    binding = _binding(plan)
    receipt = _receipt(
        plan, binding, requirement="rank0/ray_head", role="ray_head", rank=0, node="n0", patches={}
    )
    with pytest.raises(ReceiptError, match="missing required patch"):
        validate_receipt(receipt, required_patch_ids={"SC-01"})


def test_global_requires_null_rank_and_rank_requires_one():
    plan = _plan()
    binding = _binding(plan)
    bad = _receipt(
        plan,
        binding,
        requirement="global/supervisor",
        role="supervisor",
        rank=0,
        node="n0",
        scope="GLOBAL",
    )
    from dataclasses import replace

    with pytest.raises(ReceiptError, match="null exactly for GLOBAL"):
        validate_receipt(replace(bad, owner_rank=0), required_patch_ids=set())


def test_receipt_hash_mismatch_is_detected():
    from dataclasses import replace

    plan = _plan()
    binding = _binding(plan)
    receipt = _receipt(
        plan, binding, requirement="rank0/ray_head", role="ray_head", rank=0, node="n0"
    )
    tampered = replace(receipt, node_id="somewhere-else")
    with pytest.raises(ReceiptError, match="receipt_hash mismatch"):
        validate_receipt(tampered, required_patch_ids={"SC-01"})


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("deployment_id", "another", "deployment_id"),
        ("site_profile_hash", "b" * 64, "site_profile_hash"),
        ("compatibility_profile_hash", "b" * 64, "compatibility_profile_hash"),
        ("manifest_hash", "b" * 64, "manifest_hash"),
    ],
)
def test_ledger_rejects_every_mismatched_semantic_identity(field, value, reason):
    from dataclasses import replace

    plan = _plan()
    binding = _binding(plan)
    receipt = _receipt(
        plan, binding, requirement="rank0/ray_head", role="ray_head", rank=0, node="n0"
    )
    receipt = replace(receipt, **{field: value}, receipt_hash="").finalize()
    ok, detail = ExactReceiptLedger(plan, binding).accept(
        receipt, required_patch_ids={"SC-01"}, session_rank=0, session_node="n0"
    )
    assert not ok and reason in detail


@pytest.mark.parametrize("observed", [{}, {"mpi4py": "4.1.0"}])
def test_ledger_rejects_missing_or_drifted_mpi4py_identity(observed):
    from dataclasses import replace

    plan = _plan()
    binding = _binding(plan)
    receipt = _receipt(
        plan, binding, requirement="rank0/ray_head", role="ray_head", rank=0, node="n0"
    )
    versions = dict(receipt.observed_versions)
    if observed:
        versions.update(observed)
    else:
        versions.pop("mpi4py")
    receipt = replace(receipt, observed_versions=versions, receipt_hash="").finalize()
    ok, detail = ExactReceiptLedger(plan, binding).accept(
        receipt, required_patch_ids={"SC-01"}, session_rank=0, session_node="n0"
    )
    assert not ok and "mpi4py" in detail


def test_ledger_rejects_component_substitution_within_a_valid_slot():
    from dataclasses import replace

    plan = _plan()
    binding = _binding(plan)
    receipt = _receipt(
        plan, binding, requirement="rank0/ray_head", role="ray_head", rank=0, node="n0"
    )
    receipt = replace(receipt, component_id="node_supervisor", receipt_hash="").finalize()
    ok, detail = ExactReceiptLedger(plan, binding).accept(
        receipt, required_patch_ids={"SC-01"}, session_rank=0, session_node="n0"
    )
    assert not ok and "component_id" in detail


def test_ledger_rejects_missing_vllm_seed_source_or_capability_evidence():
    from dataclasses import replace

    plan = _plan()
    binding = _binding(plan)
    receipt = _receipt(
        plan, binding, requirement="rank0/ray_head", role="ray_head", rank=0, node="n0"
    )
    sources = dict(receipt.observed_source_hashes)
    sources.pop("vllm_modelinfo:manifest")
    missing_source = replace(
        receipt,
        observed_source_hashes=sources,
        receipt_hash="",
    ).finalize()
    ok, detail = ExactReceiptLedger(plan, binding).accept(
        missing_source,
        required_patch_ids={"SC-01"},
        session_rank=0,
        session_node="n0",
    )
    assert not ok and "source/artifact hashes" in detail

    missing_capability = replace(
        receipt,
        capabilities=tuple(
            value for value in receipt.capabilities if value != "vllm_modelinfo_cache_seed"
        ),
        receipt_hash="",
    ).finalize()
    ok, detail = ExactReceiptLedger(plan, binding).accept(
        missing_capability,
        required_patch_ids={"SC-01"},
        session_rank=0,
        session_node="n0",
    )
    assert not ok and "capabilities" in detail


def test_supervisor_attestation_cannot_replace_a_managed_self_report():
    from dataclasses import replace

    plan = _plan()
    binding = _binding(plan)
    receipt = _receipt(
        plan, binding, requirement="rank0/ray_head", role="ray_head", rank=0, node="n0"
    )
    receipt = replace(receipt, attestation_type="SUPERVISOR", receipt_hash="").finalize()
    ok, detail = ExactReceiptLedger(plan, binding).accept(
        receipt, required_patch_ids={"SC-01"}, session_rank=0, session_node="n0"
    )
    assert not ok and "attestation_type" in detail


def test_global_receipt_must_name_the_allocation_head_node():
    plan = _plan()
    binding = _binding(plan)
    receipt = _receipt(
        plan,
        binding,
        requirement="global/supervisor",
        role="supervisor",
        rank=None,
        node="n1",
        scope="GLOBAL",
    )
    ok, detail = ExactReceiptLedger(plan, binding).accept(
        receipt, required_patch_ids={"SC-01"}, from_global_authority=True
    )
    assert not ok and "allocation head" in detail


# -- exact-set reconciliation ------------------------------------------------


def _fill(ledger, plan, binding, *, skip=(), extra_instance=None):
    for req in plan.receipt_requirements:
        if req.receipt_requirement_id in skip:
            continue
        rank = req.planned_rank
        node = binding.node_for(rank) if rank is not None else "n0"
        receipt = _receipt(
            plan,
            binding,
            requirement=req.receipt_requirement_id,
            role=req.role,
            rank=rank,
            node=node,
            scope=req.owner_scope,
            instance=extra_instance or "i1",
        )
        ledger.accept(
            receipt,
            required_patch_ids={"SC-01"},
            session_rank=rank,
            session_node=node,
            from_global_authority=(req.owner_scope == "GLOBAL"),
        )


def test_readiness_needs_exact_set_equality_not_a_count():
    """Duplicate-plus-missing with a matching total must still fail."""
    plan = _plan(3)
    binding = _binding(plan)
    ledger = ExactReceiptLedger(plan, binding)
    _fill(ledger, plan, binding, skip={"rank2/ray_worker"})
    ok, detail = ledger.satisfied()
    assert not ok and "rank2/ray_worker" in detail["missing"]

    # Add an EXTRA receipt for an already-covered slot: the count now matches
    # the planned total, but the set does not.
    dup = _receipt(
        plan,
        binding,
        requirement="rank1/ray_worker",
        role="ray_worker",
        rank=1,
        node="n1",
        instance="other",
    )
    ledger.accept(dup, required_patch_ids={"SC-01"}, session_rank=1, session_node="n1")
    ok, detail = ledger.satisfied()
    assert not ok, "count parity must not be mistaken for coverage"
    assert "rank2/ray_worker" in detail["missing"]


def test_a_complete_exact_set_satisfies():
    plan = _plan(2)
    binding = _binding(plan)
    ledger = ExactReceiptLedger(plan, binding)
    _fill(ledger, plan, binding)
    ok, detail = ledger.satisfied()
    assert ok, detail


def test_one_receipt_cannot_represent_several_instances():
    plan = _plan(3)
    binding = _binding(plan)
    ledger = ExactReceiptLedger(plan, binding)
    one = _receipt(plan, binding, requirement="rank0/ray_head", role="ray_head", rank=0, node="n0")
    ledger.accept(one, required_patch_ids={"SC-01"}, session_rank=0, session_node="n0")
    ok, detail = ledger.satisfied()
    assert not ok and detail["accepted"] == 1
    assert detail["planned"] == len(plan.receipt_requirements)


def test_a_rank_cannot_submit_a_global_receipt():
    plan = _plan()
    binding = _binding(plan)
    ledger = ExactReceiptLedger(plan, binding)
    receipt = _receipt(
        plan,
        binding,
        requirement="global/supervisor",
        role="supervisor",
        rank=None,
        node="n0",
        scope="GLOBAL",
    )
    ok, why = ledger.accept(
        receipt, required_patch_ids={"SC-01"}, session_rank=0, session_node="n0"
    )
    assert not ok and "in-process supervisor authority" in why


def test_a_rank_cannot_submit_another_ranks_receipt():
    plan = _plan()
    binding = _binding(plan)
    ledger = ExactReceiptLedger(plan, binding)
    receipt = _receipt(
        plan, binding, requirement="rank1/ray_worker", role="ray_worker", rank=1, node="n1"
    )
    ok, why = ledger.accept(
        receipt, required_patch_ids={"SC-01"}, session_rank=0, session_node="n0"
    )
    assert not ok and "authenticated rank" in why


def test_a_node_id_that_disagrees_with_the_binding_is_refused():
    plan = _plan()
    binding = _binding(plan)
    ledger = ExactReceiptLedger(plan, binding)
    receipt = _receipt(
        plan, binding, requirement="rank0/ray_head", role="ray_head", rank=0, node="somewhere-else"
    )
    ok, why = ledger.accept(
        receipt, required_patch_ids={"SC-01"}, session_rank=0, session_node="somewhere-else"
    )
    assert not ok and "bound" in why


def test_a_stale_generation_receipt_is_refused():
    plan = _plan()
    binding = _binding(plan)
    other = build_allocation_binding(
        plan=plan, generation=9, scheduler_allocation_id="j2", nodes=["n0", "n1"]
    )
    ledger = ExactReceiptLedger(plan, binding)
    receipt = _receipt(
        plan, other, requirement="rank0/ray_head", role="ray_head", rank=0, node="n0"
    )
    ok, why = ledger.accept(
        receipt, required_patch_ids={"SC-01"}, session_rank=0, session_node="n0"
    )
    assert not ok and "another generation" in why


def test_a_duplicate_adds_no_coverage_and_a_conflict_is_refused():
    plan = _plan()
    binding = _binding(plan)
    ledger = ExactReceiptLedger(plan, binding)
    receipt = _receipt(
        plan, binding, requirement="rank0/ray_head", role="ray_head", rank=0, node="n0"
    )
    assert ledger.accept(receipt, required_patch_ids={"SC-01"}, session_rank=0, session_node="n0")[
        0
    ]
    ok, why = ledger.accept(
        receipt, required_patch_ids={"SC-01"}, session_rank=0, session_node="n0"
    )
    assert ok and why == "duplicate" and ledger.count() == 1


def test_accepted_receipt_evidence_is_a_private_value_snapshot():
    plan = _plan()
    binding = _binding(plan)
    ledger = ExactReceiptLedger(plan, binding)
    receipt = _receipt(
        plan, binding, requirement="rank0/ray_head", role="ray_head", rank=0, node="n0"
    )
    assert ledger.accept(
        receipt,
        required_patch_ids={"SC-01"},
        session_rank=0,
        session_node="n0",
    )[0]

    receipt.observed_versions["ray"] = "tampered-after-accept"
    first_projection = ledger.accepted_receipts()[0]
    assert first_projection.observed_versions["ray"] != "tampered-after-accept"

    first_projection.observed_versions["ray"] = "tampered-projection"
    assert ledger.accepted_receipts()[0].observed_versions["ray"] != "tampered-projection"


def test_losing_a_rank_removes_its_evidence():
    plan = _plan(2)
    binding = _binding(plan)
    ledger = ExactReceiptLedger(plan, binding)
    _fill(ledger, plan, binding)
    assert ledger.satisfied()[0]
    ledger.drop_rank(1)
    ok, detail = ledger.satisfied()
    assert not ok and any("rank1" in m for m in detail["missing"])


def test_a_restart_supersedes_the_slots_evidence():
    plan = _plan(2)
    binding = _binding(plan)
    ledger = ExactReceiptLedger(plan, binding)
    _fill(ledger, plan, binding)
    ledger.supersede_instance("rank0/ray_head", "i1")
    ok, detail = ledger.satisfied()
    assert not ok and "rank0/ray_head" in detail["missing"]


def test_high_cardinality_reconciliation_is_linear():
    """256-equivalent cardinality without hardware (§3.2.1 scale note)."""
    plan = _plan(64)
    binding = _binding(plan)
    ledger = ExactReceiptLedger(plan, binding)
    _fill(ledger, plan, binding)
    ok, detail = ledger.satisfied()
    assert ok, detail
    replica_slots = sum(model.num_replicas for model in plan.models)
    assert detail["planned"] == 2 + 64 * 2 + replica_slots * 2
