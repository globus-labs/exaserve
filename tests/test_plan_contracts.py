"""P01 / IMP-H01: one compiled plan with distinct hash boundaries."""

from __future__ import annotations

from dataclasses import replace

import pytest

from exaserve.plan.compiler import compile_deployment_plan, compile_run_plan
from exaserve.plan.contracts import (
    ArtifactPolicy,
    ClientPolicy,
    ControlLimits,
    ExposurePlan,
    GatewayPlan,
    PlanError,
    ReplicaPlan,
    RunProvenance,
    SchedulerPlan,
    SiteProfile,
    TracePolicy,
    build_allocation_binding,
    canonical_node_id,
    provenance_hash,
    same_node,
)
from exaserve.site import (
    default_model_storage_path,
    default_site_profile,
    require_execution_qualification,
)


def _site(**kw) -> SiteProfile:
    base = dict(
        schema_version=3,
        site_id="aurora",
        max_nodes=64,
        gpus_per_node=12,
        cpus_per_node=64,
        scheduler_types=("pbs", "slurm"),
        gateway_kinds=("haproxy", "nginx"),
        vendors=("xpu",),
        engines=("vllm",),
        model_storage_path="/lus/models",
        local_stage_path="/tmp/hf_home",
        launcher_capabilities=("ray_serve.run_many",),
    )
    base.update(kw)
    return SiteProfile(**base).finalize()


def _raw(**kw):
    base = {
        "num_nodes": 2,
        "num_gpus_per_node": 12,
        "models": [
            {
                "model_id": "meta-llama/Meta-Llama-3-8B-Instruct",
                "tensor_parallel_size": 1,
                "max_model_len": 4096,
                "size": 8,
            }
        ],
        "gateway": {"kind": "haproxy", "port": 4001},
    }
    base.update(kw)
    return base


def test_default_aurora_profile_claims_only_qualified_stack_families():
    site = default_site_profile()
    assert site.scheduler_types == ("pbs",)
    assert site.vendors == ("xpu",)
    assert site.engines == ("vllm",)
    assert len(site.scale_envelopes) == 1
    envelope = site.scale_envelopes[0]
    assert (envelope.gateway_kind, envelope.request_mode, envelope.streaming_mode) == (
        "haproxy",
        "completion",
        "non_streaming",
    )


def test_default_model_storage_path_uses_the_local_account_not_a_developer(
    monkeypatch,
):
    monkeypatch.setenv("EXASERVE_PROJECT_ROOT", "/projects/team")
    monkeypatch.setattr(
        "exaserve.site.pwd.getpwuid", lambda _uid: type("P", (), {"pw_name": "alice"})()
    )
    assert default_model_storage_path() == "/projects/team/alice/models"


def test_default_model_storage_path_rejects_a_relative_project_root(monkeypatch):
    monkeypatch.setenv("EXASERVE_PROJECT_ROOT", "relative/project")
    with pytest.raises(RuntimeError, match="absolute path"):
        default_model_storage_path()


@pytest.mark.parametrize("field", ["model_storage_path", "local_stage_path"])
def test_site_profile_rejects_relative_storage_paths(field):
    with pytest.raises(PlanError, match=rf"site\.{field} must be an absolute path"):
        _site(**{field: "relative/path"})


def test_explicit_model_storage_path_does_not_require_account_lookup(monkeypatch):
    monkeypatch.setenv("EXASERVE_MODEL_STORAGE_PATH", "/models/explicit")
    monkeypatch.setattr(
        "exaserve.site.pwd.getpwuid",
        lambda _uid: (_ for _ in ()).throw(KeyError("no passwd entry")),
    )
    default_site_profile.cache_clear()
    try:
        assert default_site_profile().model_storage_path == "/models/explicit"
    finally:
        default_site_profile.cache_clear()


def test_semantic_option_maps_reject_non_string_and_duplicate_keys():
    with pytest.raises(PlanError, match="key must be a string"):
        GatewayPlan(kind="haproxy", port=4001, options=((1, "value"),))
    with pytest.raises(PlanError, match="duplicate key"):
        GatewayPlan(
            kind="haproxy",
            port=4001,
            options=(("balance", "first"), ("balance", "second")),
        )

    with pytest.raises(PlanError, match="site.filesystem_semantics.*duplicate key"):
        _site(filesystem_semantics=(("shared", "lustre"), ("shared", "other")))
    with pytest.raises(PlanError, match="site.launcher_capabilities"):
        _site(launcher_capabilities=("mpi", "mpi"))


def test_contract_sequences_never_treat_scalar_text_as_items():
    with pytest.raises(PlanError, match="site.scheduler_types must be a sequence"):
        _site(scheduler_types="pbs")  # type: ignore[arg-type]
    with pytest.raises(PlanError, match="scale_envelope.evidence_refs must be a sequence"):
        replace(
            default_site_profile().scale_envelopes[0],
            evidence_refs="evidence.json",  # type: ignore[arg-type]
        )
    with pytest.raises(PlanError, match="scheduler.filesystem_refs must be a sequence"):
        SchedulerPlan(type="pbs", nodes=1, filesystem_refs="/lus")  # type: ignore[arg-type]
    with pytest.raises(PlanError, match="client.dispatch_topologies must be a sequence"):
        ClientPolicy(
            destination="direct",
            dispatch_topologies="mesh",  # type: ignore[arg-type]
        )
    with pytest.raises(PlanError, match="artifacts.expected_artifacts must be a sequence"):
        ArtifactPolicy(expected_artifacts="result.json")  # type: ignore[arg-type]


def test_direct_contract_construction_fails_with_plan_errors():
    with pytest.raises(PlanError, match="gateway.executable_ref"):
        GatewayPlan(kind="haproxy", port=4001, executable_ref=7)  # type: ignore[arg-type]
    with pytest.raises(PlanError, match="exposure.advertised_path"):
        ExposurePlan(
            mode="DIRECT_VALIDATION",
            advertised_path=7,  # type: ignore[arg-type]
        )
    with pytest.raises(PlanError, match="trace.prompt_content_hash"):
        TracePolicy(prompt_content_hash=7)  # type: ignore[arg-type]
    with pytest.raises(PlanError, match="replica.planned_ranks must be a sequence"):
        ReplicaPlan(
            replica_id="replica-0",
            replica_index=0,
            planned_ranks="0",  # type: ignore[arg-type]
            planned_device_ids=((0,),),
            tensor_parallel_size=1,
            pipeline_parallel_size=1,
            gpu_demand=1,
            cpu_demand=1,
        )
    with pytest.raises(PlanError, match="provenance.argv must be a sequence"):
        RunProvenance(
            schema_version=3,
            run_id="run",
            deployment_id="deployment",
            generation=0,
            deployment_plan_hash="a" * 64,
            allocation_binding_hash="b" * 64,
            run_semantic_hash=None,
            source_snapshot_hash="c" * 64,
            resolved_input_paths=(),
            argv="python",  # type: ignore[arg-type]
            prepared_environment_hash="d" * 64,
            started_at="2026-08-08T00:00:00Z",
            output_locations=(),
        )


def test_node_identity_comparison_does_not_coerce_arbitrary_objects():
    assert canonical_node_id(1) == ""  # type: ignore[arg-type]
    assert not same_node(1, "1")  # type: ignore[arg-type]


def test_execution_boundary_distinguishes_validation_from_qualified_production():
    candidate = default_site_profile()
    validation_plan = compile_deployment_plan(
        {**_raw(), "validation_mode": True},
        site=candidate,
        deployment_id="validation",
    )
    assert require_execution_qualification(validation_plan, candidate) is False

    selected = next(
        envelope
        for envelope in candidate.scale_envelopes
        if envelope.gateway_kind == "haproxy"
        and envelope.request_mode == "completion"
        and envelope.streaming_mode == "non_streaming"
    )
    qualified = replace(
        candidate,
        control=replace(candidate.control, evidence_backed=True),
        readiness=replace(candidate.readiness, evidence_backed=True),
        scale_envelopes=(
            replace(
                selected,
                qualification_target_approved=True,
                evidence_refs=("tests/test_plan_contracts.py",),
            ),
        ),
        site_profile_hash="",
    ).finalize()
    production_plan = compile_deployment_plan(
        _raw(num_nodes=1, request_mode="completion", streaming_mode="non_streaming"),
        site=qualified,
        deployment_id="production",
    )

    assert require_execution_qualification(production_plan, qualified) is True
    forged_envelope = replace(production_plan.scale_envelope, envelope_id="forged-envelope")
    forged_plan = replace(
        production_plan,
        scale_envelope=forged_envelope,
        deployment_plan_hash="",
    ).finalize()
    with pytest.raises(RuntimeError, match="not declared by SiteProfile"):
        require_execution_qualification(forged_plan, qualified)


def test_validation_execution_still_requires_the_exact_site_profile_identity():
    candidate = default_site_profile()
    validation_plan = compile_deployment_plan(
        {**_raw(), "validation_mode": True},
        site=candidate,
        deployment_id="validation",
    )
    mismatched = replace(
        candidate,
        environment_profile_ref="profiles/another-site.json",
        site_profile_hash="",
    ).finalize()
    with pytest.raises(RuntimeError, match="identities do not match"):
        require_execution_qualification(validation_plan, mismatched)


def test_run_scheduler_must_match_the_selected_scale_envelope():
    with pytest.raises(PlanError, match="disagrees with scale envelope"):
        compile_run_plan(
            _raw(),
            site=_site(),
            run_id="r",
            deployment_id="d",
            scheduler=SchedulerPlan(type="slurm", nodes=2),
        )


# -- gateway / exposure ------------------------------------------------------


def test_production_requires_a_real_gateway():
    """`gateway: none` was BOTH legal and the implicit default."""
    with pytest.raises(PlanError, match="Production exposure requires"):
        compile_deployment_plan(_raw(gateway=None), site=_site(), deployment_id="d")


def test_the_legacy_none_spelling_is_not_a_gateway():
    for spelling in ("none", "direct", {"type": "none"}):
        with pytest.raises(PlanError, match="mapping|unknown key"):
            compile_deployment_plan(_raw(gateway=spelling), site=_site(), deployment_id="d")


def test_direct_exposure_compiles_only_in_explicit_validation_mode():
    plan = compile_deployment_plan(
        _raw(gateway=None, validation_mode=True, exposure={"mode": "DIRECT_VALIDATION"}),
        site=_site(),
        deployment_id="d",
    )
    assert plan.gateway is None
    assert not plan.is_production_exposure()


def test_head_only_is_an_explicit_gateway_free_benchmark_topology():
    plan = compile_deployment_plan(
        _raw(
            gateway=None,
            validation_mode=True,
            exposure={"mode": "RAY_SERVE_HEAD_ONLY"},
        ),
        site=_site(),
        deployment_id="ray-native",
    )
    assert plan.gateway is None
    assert plan.uses_head_only_serve_proxy()
    assert plan.scale_envelope.gateway_kind is None
    assert plan.scale_envelope.validation_mode is True


def test_head_only_rejects_topologies_that_cannot_bind_native_replica_slots():
    with pytest.raises(PlanError, match="exactly one model"):
        compile_deployment_plan(
            _raw(
                gateway=None,
                validation_mode=True,
                exposure={"mode": "RAY_SERVE_HEAD_ONLY"},
                models=[_raw()["models"][0], {**_raw()["models"][0], "model_id": "other"}],
            ),
            site=_site(),
            deployment_id="ray-native-multimodel",
        )
    with pytest.raises(PlanError, match="tensor-parallel replicas only"):
        compile_deployment_plan(
            _raw(
                gateway=None,
                validation_mode=True,
                exposure={"mode": "RAY_SERVE_HEAD_ONLY"},
                models=[{**_raw()["models"][0], "pipeline_parallel_size": 2}],
            ),
            site=_site(),
            deployment_id="ray-native-pp",
        )


def test_litellm_resolves_an_evidence_based_startup_budget_floor():
    plan = compile_deployment_plan(
        _raw(
            gateway={"kind": "litellm", "port": 4001},
            validation_mode=True,
            readiness={"gateway_start_deadline_s": 10},
        ),
        site=_site(gateway_kinds=("haproxy", "litellm")),
        deployment_id="litellm-benchmark",
    )
    assert plan.readiness.gateway_start_deadline_s == 120.0
    assert dict(plan.gateway.options)["timeout"] == 300
    assert plan.readiness.recovery_deadline_s == 360.0


def test_litellm_request_timeout_is_hash_bound_and_sizes_recovery():
    plan = compile_deployment_plan(
        _raw(
            gateway={
                "kind": "litellm",
                "port": 4001,
                "options": {"timeout": 450},
            },
            validation_mode=True,
        ),
        site=_site(gateway_kinds=("haproxy", "litellm")),
        deployment_id="litellm-benchmark",
    )
    assert dict(plan.gateway.options)["timeout"] == 450
    assert plan.readiness.recovery_deadline_s == 510.0

    with pytest.raises(PlanError, match=r"unknown key.*typo_timeout"):
        compile_deployment_plan(
            _raw(
                gateway={
                    "kind": "litellm",
                    "port": 4001,
                    "options": {"typo_timeout": 450},
                },
                validation_mode=True,
            ),
            site=_site(gateway_kinds=("haproxy", "litellm")),
            deployment_id="litellm-benchmark",
        )

    with pytest.raises(PlanError, match=r"extra_router cannot override.*timeout"):
        compile_deployment_plan(
            _raw(
                gateway={
                    "kind": "litellm",
                    "port": 4001,
                    "options": {"extra_router": {"timeout": 1}},
                },
                validation_mode=True,
            ),
            site=_site(gateway_kinds=("haproxy", "litellm")),
            deployment_id="litellm-benchmark",
        )


def test_validation_mode_still_refuses_a_mismatched_exposure_mode():
    with pytest.raises(PlanError, match="DIRECT_VALIDATION"):
        compile_deployment_plan(
            _raw(gateway=None, validation_mode=True, exposure={"mode": "PROXIED_INTERNAL"}),
            site=_site(),
            deployment_id="d",
        )


def test_a_declared_gateway_cannot_claim_direct_exposure():
    with pytest.raises(PlanError, match="PROXIED_INTERNAL"):
        compile_deployment_plan(
            _raw(exposure={"mode": "DIRECT_VALIDATION"}), site=_site(), deployment_id="d"
        )


def test_direct_construction_rechecks_gateway_and_envelope_dimensions():
    plan = compile_deployment_plan(_raw(), site=_site(), deployment_id="d")
    with pytest.raises(PlanError, match="without a gateway requires DIRECT_VALIDATION"):
        replace(plan, gateway=None)
    with pytest.raises(PlanError, match="deployment.site_id disagrees"):
        replace(plan, scale_envelope=replace(plan.scale_envelope, site_id="other-site"))


def test_a_non_first_release_gateway_needs_validation_mode():
    with pytest.raises(PlanError, match="not a first-release production"):
        compile_deployment_plan(
            _raw(gateway={"kind": "nginx", "port": 4001}), site=_site(), deployment_id="d"
        )


@pytest.mark.parametrize(
    ("request_mode", "streaming_mode"),
    [("chat", "non_streaming"), ("completion", "streaming"), ("mixed", "non_streaming")],
)
def test_unqualified_request_dimensions_need_validation_mode(request_mode, streaming_mode):
    site = default_site_profile()
    with pytest.raises(PlanError, match="evidence-backed site scale envelope"):
        compile_deployment_plan(
            _raw(request_mode=request_mode, streaming_mode=streaming_mode),
            site=site,
            deployment_id="unqualified",
        )
    plan = compile_deployment_plan(
        _raw(
            request_mode=request_mode,
            streaming_mode=streaming_mode,
            validation_mode=True,
        ),
        site=site,
        deployment_id="validation",
    )
    assert plan.validation_mode is True
    assert plan.scale_envelope.validation_tier == "synthetic-site-profile"


def test_a_gateway_the_site_does_not_support_is_refused():
    with pytest.raises(PlanError, match="does not support"):
        compile_deployment_plan(
            _raw(gateway={"kind": "envoy", "port": 4001}), site=_site(), deployment_id="d"
        )


def test_gateway_executable_reference_is_validated_at_compile_time():
    with pytest.raises(PlanError, match="absolute path or a PATH"):
        compile_deployment_plan(
            _raw(gateway={"kind": "haproxy", "port": 4001, "executable_ref": "haproxy"}),
            site=_site(),
            deployment_id="d",
        )
    with pytest.raises(PlanError, match="safe executable"):
        compile_deployment_plan(
            _raw(
                gateway={
                    "kind": "haproxy",
                    "port": 4001,
                    "executable_ref": "PATH:haproxy --bad",
                }
            ),
            site=_site(),
            deployment_id="d",
        )


# -- hash boundaries ---------------------------------------------------------


def test_serving_changes_move_the_deployment_hash():
    a = compile_deployment_plan(_raw(), site=_site(), deployment_id="d")
    b = compile_deployment_plan(_raw(num_nodes=4), site=_site(), deployment_id="d")
    assert a.deployment_plan_hash != b.deployment_plan_hash


def test_site_drift_moves_the_bound_deployment_hash():
    a = compile_deployment_plan(_raw(), site=_site(), deployment_id="d")
    b = compile_deployment_plan(_raw(), site=_site(cpus_per_node=96), deployment_id="d")
    assert a.deployment_plan_hash != b.deployment_plan_hash


def test_the_same_input_compiles_to_a_byte_identical_hash():
    """CLI/core/eval/ClientLab must agree on identity from one input."""
    hashes = {
        compile_deployment_plan(_raw(), site=_site(), deployment_id="d").deployment_plan_hash
        for _ in range(5)
    }
    assert len(hashes) == 1


def test_multi_replica_pp_uses_disjoint_canonical_ranks():
    site = _site()
    plan = compile_deployment_plan(
        _raw(
            num_nodes=4,
            validation_mode=True,
            models=[
                {
                    "model_id": "org/pp",
                    "tensor_parallel_size": 1,
                    "pipeline_parallel_size": 2,
                    "num_replicas": 2,
                    "max_model_len": 128,
                    "size": 8,
                }
            ],
            runtime={"pp_shard_aware": True},
        ),
        site=site,
        deployment_id="pp",
    )
    ranks = [rank for replica in plan.models[0].replicas for rank in replica.planned_ranks]
    assert len(ranks) == len(set(ranks)) == 4

    with pytest.raises(PlanError, match="cannot be placed"):
        compile_deployment_plan(
            _raw(
                num_nodes=3,
                validation_mode=True,
                models=[
                    {
                        "model_id": "org/pp",
                        "tensor_parallel_size": 1,
                        "pipeline_parallel_size": 2,
                        "num_replicas": 2,
                        "max_model_len": 128,
                        "size": 8,
                    }
                ],
                runtime={"pp_shard_aware": True},
            ),
            site=site,
            deployment_id="pp-impossible",
        )


def test_multi_replica_plan_requires_batched_serve_deployment_capability():
    with pytest.raises(PlanError, match="ray_serve.run_many"):
        compile_deployment_plan(
            _raw(
                models=[
                    {
                        "model_id": "org/tp",
                        "tensor_parallel_size": 1,
                        "num_replicas": 2,
                        "max_model_len": 128,
                        "size": 8,
                    }
                ]
            ),
            site=_site(launcher_capabilities=()),
            deployment_id="missing-run-many",
        )


def test_workload_changes_move_only_the_run_hash():
    from exaserve.plan.contracts import WorkloadPolicy

    a = compile_run_plan(
        _raw(), site=_site(), run_id="r", deployment_id="d", workload=WorkloadPolicy(duration_s=10)
    )
    b = compile_run_plan(
        _raw(), site=_site(), run_id="r", deployment_id="d", workload=WorkloadPolicy(duration_s=30)
    )
    assert a.deployment.deployment_plan_hash == b.deployment.deployment_plan_hash
    assert a.run_semantic_hash != b.run_semantic_hash


@pytest.mark.parametrize(
    "override",
    [
        {"engine": "sglang", "collect_stats": True},
        {
            "collect_stats": True,
            "validation_mode": True,
            "runtime": {"null_compute": True},
        },
    ],
)
def test_collect_stats_requires_a_telemetry_capable_real_engine(override):
    site = _site(engines=("vllm", "sglang"))
    with pytest.raises(PlanError, match="complete serving telemetry"):
        compile_deployment_plan(_raw(**override), site=site, deployment_id="telemetry")


def test_run_compilation_rejects_explicit_request_policy_drift():
    from exaserve.plan.contracts import WorkloadPolicy

    with pytest.raises(PlanError, match="typed run policy"):
        compile_run_plan(
            _raw(request_mode="completion"),
            site=_site(),
            run_id="r",
            deployment_id="d",
            workload=WorkloadPolicy(duration_s=10),
        )


def test_paths_and_timestamps_touch_only_provenance():
    plan = compile_deployment_plan(_raw(), site=_site(), deployment_id="d")
    binding = build_allocation_binding(
        plan=plan, generation=1, scheduler_allocation_id="job1", nodes=["n0", "n1"]
    )
    first = provenance_hash(
        deployment_plan_hash=plan.deployment_plan_hash,
        allocation_binding_hash=binding.allocation_binding_hash,
        source_path="/a/x.yaml",
        started_at="t1",
    )
    second = provenance_hash(
        deployment_plan_hash=plan.deployment_plan_hash,
        allocation_binding_hash=binding.allocation_binding_hash,
        source_path="/b/x.yaml",
        started_at="t2",
    )
    assert first != second
    assert plan.deployment_plan_hash == plan.compute_hash()


def test_a_restart_on_different_nodes_changes_only_the_binding():
    plan = compile_deployment_plan(_raw(), site=_site(), deployment_id="d")
    first = build_allocation_binding(
        plan=plan, generation=1, scheduler_allocation_id="job1", nodes=["n0", "n1"]
    )
    second = build_allocation_binding(
        plan=plan, generation=2, scheduler_allocation_id="job2", nodes=["n7", "n8"]
    )
    assert first.allocation_binding_hash != second.allocation_binding_hash
    assert first.deployment_plan_hash == second.deployment_plan_hash


def test_a_node_count_mismatch_refuses_to_bind():
    plan = compile_deployment_plan(_raw(), site=_site(), deployment_id="d")
    with pytest.raises(PlanError, match="different deployment"):
        build_allocation_binding(
            plan=plan, generation=1, scheduler_allocation_id="j", nodes=["only-one"]
        )


# -- receipt slots -----------------------------------------------------------


def test_the_plan_enumerates_exact_receipt_slots():
    plan = compile_deployment_plan(_raw(num_nodes=3), site=_site(), deployment_id="d")
    ids = plan.requirement_keys()
    assert "global/supervisor" in ids
    assert "global/gateway/haproxy" in ids
    assert "rank0/ray_head" in ids and "rank2/ray_worker" in ids
    # Fixed rank infrastructure plus exact replica/engine logical slots.
    rank_one = plan.requirements_for_rank(1)
    assert len(rank_one) > 2
    assert any(item.role == "replica" for item in rank_one)
    assert any(item.role == "engine_core" for item in rank_one)
    replica_slots = sum(model.num_replicas for model in plan.models)
    assert len(ids) == 2 + 3 * 2 + replica_slots * 2


def test_a_rank_slot_never_pins_an_allocation_hostname():
    """The plan is compiled BEFORE the allocation exists."""
    plan = compile_deployment_plan(_raw(), site=_site(), deployment_id="d")
    for requirement in plan.receipt_requirements:
        assert "n0" not in requirement.receipt_requirement_id
        assert requirement.placement == "" or "host" not in requirement.placement


def test_global_requirements_do_not_pin_a_rank():
    with pytest.raises(PlanError, match="must not pin a rank"):
        from exaserve.plan.contracts import ReceiptRequirement

        ReceiptRequirement(
            receipt_requirement_id="x",
            role="r",
            component_slot="c",
            owner_scope="GLOBAL",
            planned_rank=0,
        )


# -- control limits ----------------------------------------------------------


def test_a_lease_shorter_than_three_heartbeats_is_refused():
    with pytest.raises(PlanError, match="3 \\* heartbeat_interval_s"):
        ControlLimits(heartbeat_interval_s=10.0, lease_timeout_s=20.0)


def test_control_limits_reject_nonpositive_values():
    with pytest.raises(PlanError, match="must be positive"):
        ControlLimits(registration_deadline_s=0)
    with pytest.raises(PlanError, match="positive int"):
        ControlLimits(max_frame_bytes=0)
    with pytest.raises(PlanError, match="positive int"):
        ControlLimits(max_snapshot_items=True)


def test_control_limits_are_not_evidence_backed_by_default():
    """A SiteProfile is not production-qualified until values are measured."""
    assert ControlLimits().evidence_backed is False


# -- strictness --------------------------------------------------------------


def test_unknown_deployment_keys_are_refused():
    with pytest.raises(PlanError, match="unknown key"):
        compile_deployment_plan(_raw(num_node=4), site=_site(), deployment_id="d")


def test_model_identity_collisions_are_refused():
    raw = _raw(
        models=[
            {
                "model_id": "org/Model-X",
                "tensor_parallel_size": 1,
                "max_model_len": 4096,
                "size": 8,
            },
            {
                "model_id": "org/model.x",
                "tensor_parallel_size": 1,
                "max_model_len": 4096,
                "size": 8,
            },
        ]
    )
    with pytest.raises(PlanError, match="identity collision"):
        compile_deployment_plan(raw, site=_site(), deployment_id="d")


def test_node_and_gpu_limits_come_from_the_site():
    with pytest.raises(PlanError, match="exceeds site"):
        compile_deployment_plan(_raw(num_nodes=999), site=_site(), deployment_id="d")
    with pytest.raises(PlanError, match="exceeds gpus_per_node"):
        compile_deployment_plan(
            _raw(
                models=[
                    {
                        "model_id": "a/b",
                        "tensor_parallel_size": 99,
                        "max_model_len": 4096,
                        "size": 8,
                    }
                ]
            ),
            site=_site(),
            deployment_id="d",
        )
