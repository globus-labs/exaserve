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


def test_pp_curves_use_explicit_complete_evidence_contracts() -> None:
    from eval.plot import sc26_full_figures as figures

    expected_groups = {
        4: "run3",
        8: "run3",
        16: "run3",
        32: "run4",
        64: "run4",
        128: "run4",
        256: "run4",
    }
    assert figures.PP405B_CURRENT_RUN_GROUPS == expected_groups
    variants = {series.key: series for series in figures.PP405B_VARIANTS}
    current = variants["haproxy_nonstream"]
    assert current.loader_kind is figures.PP405BLoaderKind.CURRENT_MANIFEST_V2
    assert {ref.nodes: ref.run_group_id for ref in current.current_refs} == expected_groups
    assert current.legacy_refs == ()

    for key in ("direct_stream", "haproxy_stream"):
        legacy = variants[key]
        assert legacy.loader_kind is figures.PP405BLoaderKind.LEGACY_PINNED_V1
        assert [ref.nodes for ref in legacy.legacy_refs] == list(figures.PP405B_NODE_COUNTS)
        assert [ref.nodes // 2 for ref in legacy.legacy_refs] == [2, 4, 8, 16, 32, 64, 128]
        assert legacy.current_refs == ()
        assert all(".bak" not in ref.result_name for ref in legacy.legacy_refs)

    direct = {ref.nodes: ref for ref in variants["direct_stream"].legacy_refs}
    haproxy = {ref.nodes: ref for ref in variants["haproxy_stream"].legacy_refs}
    assert direct[64].result_name == "result1.json"
    assert haproxy[32].result_name == "result1.json"
    assert haproxy[64].result_name == "result1.json"


def test_legacy_pp_evidence_map_hash_and_corrected_arrays_are_frozen() -> None:
    import hashlib
    import json
    from dataclasses import asdict

    from eval.plot import sc26_full_figures as figures

    selections = {
        key: [(ref.nodes, ref.run_group_id, ref.result_name, ref.pbs_job_id) for ref in refs]
        for key, refs in figures.PP405B_LEGACY_SELECTIONS.items()
    }
    assert selections == {
        "direct_stream": [
            (4, "run1", "result0.json", 8654870),
            (8, "run1", "result0.json", 8654871),
            (16, "run3", "result0.json", 8662386),
            (32, "run1", "result0.json", 8654872),
            (64, "run3", "result1.json", 8726613),
            (128, "run0", "result0.json", 8641122),
            (256, "run0", "result0.json", 8640747),
        ],
        "haproxy_stream": [
            (4, "run3", "result0.json", 8655141),
            (8, "run3", "result0.json", 8655160),
            (16, "run3", "result0.json", 8655268),
            (32, "run5", "result1.json", 8726612),
            (64, "run5", "result1.json", 8702358),
            (128, "run5", "result0.json", 8686262),
            (256, "run5", "result0.json", 8662385),
        ],
    }
    snapshot = [
        (key, [asdict(ref) for ref in refs])
        for key, refs in figures.PP405B_LEGACY_SELECTIONS.items()
    ]
    canonical = json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode()
    assert hashlib.sha256(canonical).hexdigest() == (
        "803024d4e31a4ba5eddad7bc853e9779378db236d7576ce4a9fbbeba69d1a8f1"
    )

    direct = [ref.expected_successful_rps for ref in figures.PP405B_LEGACY_DIRECT_REFS]
    haproxy = [ref.expected_successful_rps for ref in figures.PP405B_LEGACY_HAPROXY_REFS]
    assert direct == pytest.approx(
        [
            0.7194176497627154,
            1.3954681407695033,
            2.6599445991497053,
            4.883635710316778,
            10.29933385964368,
            18.797403417647807,
            31.046752640361486,
        ]
    )
    assert haproxy == pytest.approx(
        [
            0.6963688544063671,
            1.3457169030043798,
            2.5223592373921706,
            4.68211907345158,
            8.665885405226787,
            11.872853632075806,
            22.91585531588077,
        ]
    )
    base_per_node = direct[0] / 4
    direct_efficiency = [
        100 * rps / (base_per_node * nodes)
        for rps, nodes in zip(direct, figures.PP405B_NODE_COUNTS)
    ]
    haproxy_efficiency = [
        100 * rps / (base_per_node * nodes)
        for rps, nodes in zip(haproxy, figures.PP405B_NODE_COUNTS)
    ]
    assert direct_efficiency == pytest.approx(
        [100.0, 96.985954, 92.433950, 84.853974, 89.476310, 81.651994, 67.430304],
        abs=1e-6,
    )
    assert haproxy_efficiency == pytest.approx(
        [96.796187, 93.528210, 87.652813, 81.352589, 75.285592, 51.573196, 49.770844],
        abs=1e-6,
    )


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
        "RUN_GROUPS": ("run6", "run7"),
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
