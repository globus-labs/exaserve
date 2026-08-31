"""KI-C5: the client topology is explicit, and the footgun is not the default."""

from __future__ import annotations

from pathlib import Path

import pytest

from eval.lib.matrix import expand_matrix
from eval.lib.models import (
    BackendSpec,
    ClientSpec,
    DeploymentSpec,
    ExperimentSpec,
    MatrixSpec,
    ModelSpec,
    SaturationSpec,
    SchedulerSpec,
    TraceSpec,
    WorkloadSpec,
)
from eval.lib.spec_io import (
    DEFAULT_PROXY_CLIENT_NODES,
    load_experiment_spec,
    normalize_experiment_spec,
    validate_experiment_spec,
)
from eval.lib.plan_adapter import compile_shared_run_plan
from eval.lib.utils import load_yaml_file


def test_eval_yaml_loader_rejects_duplicate_keys(tmp_path):
    path = tmp_path / "duplicate.yaml"
    path.write_text("workload:\n  duration: 1\n  duration: 2\n", encoding="utf-8")

    with pytest.raises(Exception, match="duplicate key 'duration'"):
        load_yaml_file(path)


@pytest.mark.parametrize("value", ["64", True, 64.0])
def test_eval_ray_launch_rejects_coercible_cpu_counts(tmp_path, value):
    path = tmp_path / "bad-backend.yaml"
    path.write_text(
        "\n".join(
            (
                "name: bad-backend",
                "trace:",
                "  kind: weak_scaling",
                "workload:",
                "  duration: 1",
                "deployment:",
                "  num_nodes: 1",
                "  models:",
                "    - model_id: m",
                "client:",
                "  dest: direct",
                "  num_nodes: 1",
                "backend:",
                "  default: ray",
                "  args:",
                "    ray:",
                "      launch:",
                f"        ray_node_cpus: {value!r}",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="ray_node_cpus"):
        load_experiment_spec(path)


def test_every_checked_in_experiment_spec_loads() -> None:
    """Keep the shipped experiment catalog syntactically and semantically valid."""
    spec_root = Path(__file__).parents[1] / "specs"
    paths = sorted(spec_root.rglob("*.yaml"))
    assert paths, "the checked-in experiment catalog is unexpectedly empty"
    failures: list[str] = []
    for path in paths:
        try:
            load_experiment_spec(str(path))
        except Exception as exc:  # noqa: BLE001 - report the complete catalog
            failures.append(f"{path.relative_to(spec_root)}: {type(exc).__name__}: {exc}")
    assert not failures, "invalid checked-in experiment specs:\n" + "\n".join(failures)


def test_every_in_envelope_catalog_cell_compiles_to_the_canonical_run_plan() -> None:
    """Only explicitly unsupported/out-of-scope catalog cells may fail closed."""
    spec_root = Path(__file__).parents[1] / "specs"
    failures: list[str] = []
    rejected = {"site_node_ceiling": 0, "accelerator_ceiling": 0, "sglang_gated": 0}
    for path in sorted(spec_root.rglob("*.yaml")):
        for variant in expand_matrix(load_experiment_spec(str(path))):
            try:
                compile_shared_run_plan(
                    variant.spec,
                    run_id=f"catalog-audit/{variant.variant_name}",
                    deployment_id="catalog-audit",
                )
            except Exception as exc:  # noqa: BLE001 - classify the whole catalog
                message = str(exc)
                if "exceeds site alcf-aurora maximum" in message:
                    rejected["site_node_ceiling"] += 1
                elif "deployment.num_gpus_per_node must be <= 12" in message:
                    rejected["accelerator_ceiling"] += 1
                elif "engine 'sglang' unsupported by site alcf-aurora" in message:
                    rejected["sglang_gated"] += 1
                else:
                    failures.append(
                        f"{path.relative_to(spec_root)}::{variant.variant_name}: "
                        f"{type(exc).__name__}: {exc}"
                    )
    assert not failures, "unexpectedly unmaterializable catalog cells:\n" + "\n".join(failures)
    assert rejected == {
        "site_node_ceiling": 21,
        "accelerator_ceiling": 2,
        "sglang_gated": 27,
    }


def test_missing_paper_scale_specs_preserve_the_declared_current_infra_matrix() -> None:
    spec_root = Path(__file__).parents[1] / "specs" / "sc26workshop" / "full"

    null_variants = expand_matrix(
        load_experiment_spec(str(spec_root / "nullcompute_haproxy_scale_to256_v040.yaml"))
    )
    assert [variant.spec.deployment.num_nodes for variant in null_variants] == [32, 64, 128, 256]
    assert all(variant.spec.client.startup_only for variant in null_variants)
    assert all(
        variant.spec.backend.args["ray"]["launch"]["null_compute"] for variant in null_variants
    )

    pp_variants = expand_matrix(
        load_experiment_spec(str(spec_root / "pp405b_pp2_haproxy_nostream_v040.yaml"))
    )
    assert [variant.spec.deployment.num_nodes for variant in pp_variants] == [
        4,
        8,
        16,
        32,
        64,
        128,
        256,
    ]
    for variant in pp_variants:
        nodes = variant.spec.deployment.num_nodes
        model = variant.spec.deployment.models[0]
        assert model.tensor_parallel_size == 8
        assert model.pipeline_parallel_size == 2
        assert model.num_replicas == nodes // 2
        assert model.gpu_memory_utilization == 0.95
        assert variant.spec.client.num_runs == 2
        assert variant.spec.client.stream is False
        assert variant.spec.backend.args["ray"]["proxy"]["type"] == "haproxy"
        if nodes <= 16:
            assert (variant.spec.scheduler.queue, variant.spec.scheduler.walltime) == (
                "capacity",
                "02:00:00",
            )
        elif nodes < 256:
            assert (variant.spec.scheduler.queue, variant.spec.scheduler.walltime) == (
                "debug-scaling",
                "01:00:00",
            )
        else:
            assert (variant.spec.scheduler.queue, variant.spec.scheduler.walltime) == (
                "prod",
                "04:00:00",
            )


def test_missing_pp_curve_plot_is_pinned_to_the_accepted_run_group() -> None:
    import ast

    source = Path(__file__).parents[1] / "plot" / "sc26_full_figures.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    assignment = next(
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "PP405B_VARIANTS"
            for target in node.targets
        )
    )
    variants = ast.literal_eval(assignment.value)
    current = [row for row in variants if row[0] == "pp405b_pp2_haproxy_nostream_v040"]
    assert current == [
        (
            "pp405b_pp2_haproxy_nostream_v040",
            "run3",
            "haproxy_nonstream",
            "haproxy",
            "nonstream",
            "HAProxy non-stream",
            True,
        )
    ]


def test_missing_null_curve_consumer_pins_two_independent_lifecycles() -> None:
    import ast

    source = Path(__file__).parents[1] / "plot" / "nullcompute_startup_table.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    assignments = {
        target.id: ast.literal_eval(node.value)
        for node in tree.body
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name) and target.id in {"SPEC_NAME", "RUN_GROUPS", "NODE_COUNTS"}
    }
    assert assignments == {
        "SPEC_NAME": "nullcompute_haproxy_scale_to256_v040",
        "RUN_GROUPS": ("run2", "run3"),
        "NODE_COUNTS": (32, 64, 128, 256),
    }


def _spec(dest="proxy", deployment_nodes=256, client_nodes=0):
    return ExperimentSpec(
        name="s",
        matrix=MatrixSpec(),
        trace=TraceSpec(kind="weak_scaling", input_prompt_path="/tmp/p.jsonl"),
        workload=WorkloadSpec(duration=10.0),
        deployment=DeploymentSpec(
            num_nodes=deployment_nodes,
            models=[ModelSpec(model_id="m", tensor_parallel_size=1, max_model_len=4096, size=8)],
        ),
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
        _spec(dest="proxy", deployment_nodes=256, client_nodes=32)
    )
    assert normalized.client.num_nodes == 32


def test_eval_rejects_multi_process_saturation_before_launch():
    spec = _spec(deployment_nodes=1)
    spec.client.num_go_procs = 2
    spec.client.saturation = SaturationSpec(enabled=True)
    with pytest.raises(ValueError, match="ClientLab"):
        validate_experiment_spec(spec)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"step_duration_s": 0.0}, "step_duration_s"),
        ({"tolerance": 1.0}, "tolerance"),
        ({"max_error_rate": -0.1}, "max_error_rate"),
        ({"plateau_ratio": 0.0}, "plateau_ratio"),
        ({"search_mode": "step-up", "step_up_start": 0}, "step-up"),
    ],
)
def test_invalid_saturation_contract_is_rejected(change, message):
    spec = _spec(deployment_nodes=1)
    values = SaturationSpec().__dict__ | {"enabled": True} | change
    spec.client.saturation = SaturationSpec(**values)
    with pytest.raises(ValueError, match=message):
        validate_experiment_spec(spec)
