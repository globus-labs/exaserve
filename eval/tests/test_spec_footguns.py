"""KI-C5: the client topology is explicit, and the footgun is not the default."""

from __future__ import annotations

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
from eval.lib.spec_io import DEFAULT_PROXY_CLIENT_NODES, normalize_experiment_spec


def _spec(dest="proxy", deployment_nodes=256, client_nodes=0):
    return ExperimentSpec(
        name="s",
        matrix=MatrixSpec(),
        trace=TraceSpec(kind="synthetic", input_prompt_path="/tmp/p.jsonl"),
        workload=WorkloadSpec(duration=10.0),
        deployment=DeploymentSpec(
            num_nodes=deployment_nodes,
            models=[ModelSpec(model_id="m", tensor_parallel_size=1,
                              max_model_len=4096, size=8)]),
        client=ClientSpec(dest=dest, num_nodes=client_nodes),
        backend=BackendSpec(),
        scheduler=SchedulerSpec(nodes=deployment_nodes),
    )


def test_a_proxy_run_does_not_aim_the_whole_fleet_at_one_proxy(capsys):
    """256 client ranks on one proxy read as a proxy throughput regression
    until the harness itself was examined."""
    normalized = normalize_experiment_spec(_spec(dest="proxy", deployment_nodes=256))
    assert normalized.client.num_nodes == DEFAULT_PROXY_CLIENT_NODES
    assert "client.num_nodes defaulted" in capsys.readouterr().out


def test_a_small_proxy_run_is_unchanged():
    normalized = normalize_experiment_spec(_spec(dest="proxy", deployment_nodes=2))
    assert normalized.client.num_nodes == 2


def test_direct_still_gets_one_rank_per_node():
    """dest=direct needs exactly that, or un-targeted nodes sit idle."""
    normalized = normalize_experiment_spec(_spec(dest="direct", deployment_nodes=64))
    assert normalized.client.num_nodes == 64


def test_an_explicit_client_topology_is_respected():
    normalized = normalize_experiment_spec(
        _spec(dest="proxy", deployment_nodes=256, client_nodes=32))
    assert normalized.client.num_nodes == 32
