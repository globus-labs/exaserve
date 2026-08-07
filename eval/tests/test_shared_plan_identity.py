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
from exaserve.plan.compiler import compile_deployment_plan
from exaserve.site import default_site_profile


def _spec(nodes=2, proxy=None, duration=10.0):
    backend = BackendSpec()
    if proxy is not None:
        backend = BackendSpec(args={"ray": {"proxy": proxy}})
    return ExperimentSpec(
        name="s", matrix=MatrixSpec(),
        trace=TraceSpec(kind="synthetic", input_prompt_path="/tmp/p.jsonl"),
        workload=WorkloadSpec(duration=duration),
        deployment=DeploymentSpec(
            num_nodes=nodes, num_gpus_per_node=12,
            models=[ModelSpec(model_id="meta-llama/Meta-Llama-3-8B-Instruct",
                              tensor_parallel_size=1, max_model_len=4096, size=8)]),
        client=ClientSpec(dest="proxy", num_nodes=2),
        backend=backend, scheduler=SchedulerSpec(nodes=nodes))


def test_core_and_eval_derive_byte_identical_deployment_hashes():
    """The decisive P01 proof: one serving input, one identity."""
    spec = _spec(proxy={"type": "haproxy", "port": 4001})
    via_eval = compile_shared_deployment_plan(spec, deployment_id="d")

    raw = deployment_raw_from_spec(spec.deployment,
                                   gateway={"kind": "haproxy", "port": 4001},
                                   exposure={"mode": "PROXIED_INTERNAL"})
    via_core = compile_deployment_plan(raw, site=default_site_profile(),
                                       deployment_id="d")
    assert via_eval.deployment_plan_hash == via_core.deployment_plan_hash


def test_eval_proxy_none_becomes_a_declared_validation_exposure():
    """`proxy.type: none` was eval's way of saying 'hit Serve directly'."""
    plan = compile_shared_deployment_plan(_spec(proxy={"type": "none"}),
                                          deployment_id="d")
    assert plan.gateway is None
    assert plan.exposure.mode == "DIRECT_VALIDATION"
    assert plan.validation_mode is True


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
    plan = compile_shared_run_plan(_spec(), run_id="r", deployment_id="d")
    assert plan.workload.client_nodes == 2
    assert plan.workload.client_dest == "proxy"


def test_the_adapter_does_not_smuggle_unknown_fields():
    """The compiler's unknown-key rejection must stay meaningful."""
    raw = deployment_raw_from_spec(_spec().deployment)
    from exaserve.plan.compiler import _DEPLOYMENT_KEYS

    assert set(raw) <= _DEPLOYMENT_KEYS, f"unmodelled keys: {set(raw) - _DEPLOYMENT_KEYS}"


def test_an_eval_spec_exceeding_the_site_is_refused():
    from exaserve.plan.contracts import PlanError

    with pytest.raises(PlanError, match="exceeds site"):
        compile_shared_deployment_plan(_spec(nodes=99999), deployment_id="d")
