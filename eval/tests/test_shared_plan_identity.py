"""P05 / IMP-H01: core and eval derive the SAME deployment identity."""

from __future__ import annotations

import pytest

from eval.lib.models import (
    BackendSpec,
    ClientSpec,
    DeploymentSpec,
    ExperimentSpec,
    MatrixSpec,
    ModelSpec,
    SchedulerSpec,
    TraceSpec,
    WorkloadSpec,
)
from eval.lib.plan_adapter import (
    compile_shared_deployment_plan,
    compile_shared_run_plan,
    deployment_raw_from_spec,
)
from eval.lib.run_planner import materialized_deployment_id
from exaserve.plan.compiler import compile_deployment_plan
from exaserve.plan.contracts import PlanError
from exaserve.site import default_site_profile


def _spec(nodes=2, proxy=None, duration=10.0, *, modes=None, stream=False):
    backend = BackendSpec()
    if proxy is not None:
        backend = BackendSpec(args={"ray": {"proxy": proxy}})
    return ExperimentSpec(
        name="s",
        matrix=MatrixSpec(),
        trace=TraceSpec(kind="synthetic", input_prompt_path="/tmp/p.jsonl"),
        workload=WorkloadSpec(duration=duration, modes=modes or {"chat": 1, "completion": 0}),
        deployment=DeploymentSpec(
            num_nodes=nodes,
            num_gpus_per_node=12,
            validation_mode=True,
            models=[
                ModelSpec(
                    model_id="meta-llama/Meta-Llama-3-8B-Instruct",
                    tensor_parallel_size=1,
                    max_model_len=4096,
                    size=8,
                )
            ],
        ),
        client=ClientSpec(
            dest="proxy" if proxy is not None else "direct", num_nodes=2, stream=stream
        ),
        backend=backend,
        scheduler=SchedulerSpec(nodes=nodes),
    )


def test_core_and_eval_derive_byte_identical_deployment_hashes():
    """The decisive P01 proof: one serving input, one identity."""
    spec = _spec(proxy={"type": "haproxy", "port": 4001})
    via_eval = compile_shared_deployment_plan(spec, deployment_id="d")

    raw = deployment_raw_from_spec(
        spec.deployment,
        gateway={"kind": "haproxy", "port": 4001},
        exposure={"mode": "PROXIED_INTERNAL"},
        request_mode="chat",
        streaming_mode="non_streaming",
    )
    via_core = compile_deployment_plan(raw, site=default_site_profile(), deployment_id="d")
    assert via_eval.deployment_plan_hash == via_core.deployment_plan_hash


def test_bounded_materialized_deployment_ids_preserve_variant_identity():
    spec = "pp405b_pp2_haproxy_nostream_v040"
    n16 = materialized_deployment_id(spec_name=spec, run_group_id="run4", run_id="n16")
    n128 = materialized_deployment_id(spec_name=spec, run_group_id="run4", run_id="n128")
    assert n16 != n128
    assert len(n16) <= 40 and len(n128) <= 40
    assert "n16" in n16 and "n128" in n128
    assert (
        materialized_deployment_id(spec_name="short", run_group_id="run0", run_id="n1")
        == "short-run0-n1"
    )
    assert (
        materialized_deployment_id(
            spec_name=spec,
            run_group_id="run4",
            run_id="n16",
            scheme="legacy_truncate_v1",
        )
        == "pp405b-pp2-haproxy-nostream-v040-run4-n1"
    )
    with pytest.raises(ValueError, match="scheme"):
        materialized_deployment_id(
            spec_name=spec,
            run_group_id="run4",
            run_id="n16",
            scheme="unknown",
        )


def test_eval_proxy_none_becomes_a_declared_validation_exposure():
    """`proxy.type: none` was eval's way of saying 'hit Serve directly'."""
    plan = compile_shared_deployment_plan(_spec(proxy={"type": "none"}), deployment_id="d")
    assert plan.gateway is None
    assert plan.exposure.mode == "DIRECT_VALIDATION"
    assert plan.validation_mode is True


def test_eval_ray_serve_becomes_the_native_head_only_benchmark():
    plan = compile_shared_deployment_plan(
        _spec(proxy={"type": "ray_serve", "backend_port": 8000}), deployment_id="d"
    )
    assert plan.gateway is None
    assert plan.exposure.mode == "RAY_SERVE_HEAD_ONLY"
    assert plan.uses_head_only_serve_proxy()
    assert plan.validation_mode is True


def test_eval_explicit_validation_mode_reaches_the_canonical_plan():
    spec = _spec(nodes=4, proxy={"type": "haproxy", "port": 4001})
    spec.deployment.validation_mode = True
    plan = compile_shared_deployment_plan(spec, deployment_id="qualification")
    assert plan.validation_mode is True
    assert plan.scale_envelope.validation_mode is True


def test_eval_control_and_readiness_overrides_reach_the_canonical_plan():
    spec = _spec(proxy={"type": "ray_serve", "backend_port": 8000})
    spec.deployment.control = {"reconnect_grace_s": 300.0}
    spec.deployment.readiness = {"initial_deadline_s": 7200.0}

    plan = compile_shared_deployment_plan(spec, deployment_id="qualification")

    assert plan.control.reconnect_grace_s == 300.0
    assert plan.readiness.initial_deadline_s == 7200.0


def test_eval_control_overrides_still_use_the_canonical_schema():
    spec = _spec()
    spec.deployment.control = {"invented_timeout_s": 300.0}

    with pytest.raises(PlanError, match=r"deployment\.control: unknown key"):
        compile_shared_deployment_plan(spec, deployment_id="qualification")


def test_a_serving_change_moves_the_deployment_hash():
    a = compile_shared_deployment_plan(_spec(nodes=2), deployment_id="d")
    b = compile_shared_deployment_plan(_spec(nodes=4), deployment_id="d")
    assert a.deployment_plan_hash != b.deployment_plan_hash


def test_a_workload_change_moves_only_the_run_hash():
    a = compile_shared_run_plan(_spec(duration=10.0), run_id="r", deployment_id="d")
    b = compile_shared_run_plan(_spec(duration=30.0), run_id="r", deployment_id="d")
    assert a.deployment.deployment_plan_hash == b.deployment.deployment_plan_hash
    assert a.run_semantic_hash != b.run_semantic_hash


def test_the_shared_run_plan_carries_client_topology_semantically():
    plan = compile_shared_run_plan(
        _spec(proxy={"type": "haproxy", "port": 4001}), run_id="r", deployment_id="d"
    )
    assert plan.workload.client_nodes == 2
    assert plan.workload.client_dest == "proxy"


@pytest.mark.parametrize(
    ("modes", "stream", "request_mode", "streaming_mode"),
    [
        ({"chat": 1, "completion": 0}, False, "chat", "non_streaming"),
        ({"chat": 0, "completion": 1}, True, "completion", "streaming"),
        ({"chat": 1, "completion": 1}, False, "mixed", "non_streaming"),
    ],
)
def test_eval_workload_protocol_is_bound_into_the_deployment_identity(
    modes, stream, request_mode, streaming_mode
):
    plan = compile_shared_deployment_plan(
        _spec(proxy={"type": "haproxy", "port": 4001}, modes=modes, stream=stream),
        deployment_id="d",
    )
    assert plan.scale_envelope.request_mode == request_mode
    assert plan.scale_envelope.streaming_mode == streaming_mode


def test_eval_workload_protocol_changes_the_deployment_hash():
    chat = compile_shared_deployment_plan(
        _spec(proxy={"type": "haproxy", "port": 4001}), deployment_id="d"
    )
    completion = compile_shared_deployment_plan(
        _spec(
            proxy={"type": "haproxy", "port": 4001},
            modes={"chat": 0, "completion": 1},
        ),
        deployment_id="d",
    )
    assert chat.deployment_plan_hash != completion.deployment_plan_hash


def test_the_adapter_does_not_smuggle_unknown_fields():
    """The compiler's unknown-key rejection must stay meaningful."""
    raw = deployment_raw_from_spec(_spec().deployment)
    from exaserve.plan.compiler import _DEPLOYMENT_KEYS

    assert set(raw) <= _DEPLOYMENT_KEYS, f"unmodelled keys: {set(raw) - _DEPLOYMENT_KEYS}"


def test_an_eval_spec_exceeding_the_site_is_refused():
    with pytest.raises(PlanError, match="exceeds site"):
        compile_shared_deployment_plan(_spec(nodes=99999), deployment_id="d")


def test_proxy_run_plan_requires_a_gateway_deployment():
    spec = _spec()
    spec.client.dest = "proxy"
    with pytest.raises(PlanError, match="PROXIED_INTERNAL"):
        compile_shared_run_plan(spec, run_id="r", deployment_id="d")
