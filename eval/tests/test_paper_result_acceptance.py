"""Paper consumers reject valid-looking artifacts with broken identity edges."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest


def _paper_identity(cell: Path):
    run_plan = SimpleNamespace(
        run_id="n4",
        run_group_id="run2",
        run_semantic_hash="a" * 64,
        deployment_plan_hash="b" * 64,
        source_snapshot_hash="c" * 64,
        bundle=SimpleNamespace(root_dir=str(cell), state_path=str(cell / "state/status.json")),
        semantic_plan=SimpleNamespace(deployment=SimpleNamespace(deployment_id="paper-run2-n4")),
    )
    manifest = SimpleNamespace(
        complete=True,
        expected_ids=("replay/default", "run_provenance"),
        run_id=run_plan.run_id,
        run_semantic_hash=run_plan.run_semantic_hash,
        deployment_plan_hash=run_plan.deployment_plan_hash,
        manifest_hash="d" * 64,
        entries=(SimpleNamespace(logical_id="run_provenance", path="run_provenance.json"),),
    )
    status = SimpleNamespace(
        state="SUCCEEDED",
        record_id="run2/n4",
        provenance={
            "run_id": "n4",
            "run_group_id": "run2",
            "run_semantic_hash": "a" * 64,
            "deployment_plan_hash": "b" * 64,
            "source_snapshot_hash": "c" * 64,
        },
        data={"result_manifest_hash": manifest.manifest_hash},
    )
    provenance = SimpleNamespace(
        run_id="n4",
        deployment_id="paper-run2-n4",
        deployment_plan_hash="b" * 64,
        run_semantic_hash="a" * 64,
        source_snapshot_hash="c" * 64,
    )
    return run_plan, manifest, status, provenance


def _current_pp_record(*, nodes=4, run_index=None):
    requests = 24 * nodes
    record = {
        "requests_completed": requests,
        "requests_scheduled": requests,
        "errors": 0,
        "duration_s": 120.0,
        "rps": requests / 120.0,
        "p50_s": 1.0,
        "p99_s": 2.0,
    }
    if run_index is not None:
        record["run_index"] = run_index
        record["successes"] = requests
        record["success_rps"] = requests / 120.0
    return record


def _current_pp_document(*, nodes=4):
    return {
        "overall": _current_pp_record(nodes=nodes),
        "per_run": [
            _current_pp_record(nodes=nodes, run_index=0),
            _current_pp_record(nodes=nodes, run_index=1),
        ],
    }


def _current_pp_accepted(figures, nodes: int):
    run_group = figures.PP405B_CURRENT_RUN_GROUPS[nodes]
    source_hash, id_scheme = figures.PP405B_CURRENT_SOURCE_CONTRACTS[run_group]
    model = SimpleNamespace(
        model_id="meta-llama/Llama-3.1-405B-Instruct",
        tensor_parallel_size=8,
        pipeline_parallel_size=2,
        num_replicas=nodes // 2,
        max_model_len=4096,
        gpu_memory_utilization=0.95,
        max_num_seqs=8,
        enforce_eager=True,
    )
    deployment = SimpleNamespace(
        num_nodes=nodes,
        models=(model,),
        gateway=SimpleNamespace(kind="haproxy"),
        runtime=SimpleNamespace(null_compute=False),
    )
    semantic_plan = SimpleNamespace(
        deployment=deployment,
        scheduler=SimpleNamespace(nodes=nodes),
        client=SimpleNamespace(destination="proxy", streaming=False, num_runs=2),
        workload=SimpleNamespace(client_dest="proxy", rate_per_node=0.2, duration_s=120.0),
    )
    return SimpleNamespace(
        run_plan=SimpleNamespace(
            spec_name="pp405b_pp2_haproxy_nostream_v040",
            run_id=f"n{nodes}",
            run_group_id=run_group,
            variant_name=f"n{nodes}",
            axis_values={"num_nodes": nodes},
            source_snapshot_hash=source_hash,
            deployment_id_scheme=id_scheme,
            run_semantic_hash=figures.PP405B_CURRENT_SEMANTIC_HASHES[nodes],
            semantic_plan=semantic_plan,
        )
    )


def _set_nested(root, path: str, value):
    target = root
    parts = path.split(".")
    for part in parts[:-1]:
        target = target[int(part)] if part.isdigit() else getattr(target, part)
    setattr(target, parts[-1], value)


def test_paper_acceptance_binds_status_manifest_provenance_and_plan(tmp_path, monkeypatch):
    from eval.lib import paper_acceptance as acceptance

    run_plan, manifest, status, provenance = _paper_identity(tmp_path)
    monkeypatch.setattr(acceptance, "load_run_plan", lambda _path: run_plan)
    monkeypatch.setattr(acceptance, "load_result_manifest", lambda _path: manifest)
    monkeypatch.setattr(
        acceptance.StatusStore,
        "run",
        lambda _path: SimpleNamespace(load=lambda: status),
    )
    monkeypatch.setattr(acceptance, "load_run_provenance", lambda _path: provenance)

    accepted = acceptance.require_accepted_paper_run(
        tmp_path, required_result_ids={"replay/default", "run_provenance"}
    )
    assert accepted.run_plan is run_plan
    assert accepted.run_provenance is provenance

    status.data["result_manifest_hash"] = "e" * 64
    with pytest.raises(RuntimeError, match="RunStatus identity/completion"):
        acceptance.require_accepted_paper_run(
            tmp_path, required_result_ids={"replay/default", "run_provenance"}
        )
    status.data["result_manifest_hash"] = manifest.manifest_hash
    manifest.run_semantic_hash = "f" * 64
    with pytest.raises(RuntimeError, match="ResultManifest identity"):
        acceptance.require_accepted_paper_run(
            tmp_path, required_result_ids={"replay/default", "run_provenance"}
        )


def test_current_pp_reader_rejects_a_truncated_node_ladder(tmp_path, monkeypatch):
    from eval.plot import sc26_full_figures as figures

    monkeypatch.setattr(figures.P, "RUNS_ROOT", tmp_path)
    monkeypatch.setattr(
        figures,
        "_require_complete_current_result",
        lambda _path: _current_pp_accepted(figures, 4),
    )
    result = (
        tmp_path
        / "current"
        / figures.PP405B_CURRENT_RUN_GROUPS[4]
        / "n4"
        / "results"
        / "result0.json"
    )
    result.parent.mkdir(parents=True)
    result.write_text(json.dumps(_current_pp_document()), encoding="utf-8")
    with pytest.raises(RuntimeError, match=r"expected \[4, 8, 16, 32, 64, 128, 256\]"):
        figures._pp405b_points("current", figures.PP405B_CURRENT_RUN_GROUPS, require_complete=True)


def test_current_pp_plan_accepts_only_the_reviewed_node_to_source_mapping():
    from eval.plot import sc26_full_figures as figures

    for nodes, run_group in figures.PP405B_CURRENT_RUN_GROUPS.items():
        accepted = _current_pp_accepted(figures, nodes)
        figures._require_current_pp405b_plan(accepted, nodes=nodes, run_group=run_group)


@pytest.mark.parametrize(
    ("path", "value", "field"),
    [
        ("run_plan.spec_name", "other_spec", "spec_name"),
        ("run_plan.axis_values", {"num_nodes": 8}, "axis_values"),
        ("run_plan.semantic_plan.deployment.num_nodes", 8, "deployment.num_nodes"),
        ("run_plan.semantic_plan.scheduler.nodes", 8, "scheduler.nodes"),
        (
            "run_plan.semantic_plan.deployment.runtime.null_compute",
            True,
            "runtime.null_compute",
        ),
        (
            "run_plan.semantic_plan.deployment.models.0.model_id",
            "other/model",
            "model_id",
        ),
        (
            "run_plan.semantic_plan.deployment.models.0.tensor_parallel_size",
            4,
            "tensor_parallel_size",
        ),
        (
            "run_plan.semantic_plan.deployment.models.0.pipeline_parallel_size",
            1,
            "pipeline_parallel_size",
        ),
        (
            "run_plan.semantic_plan.deployment.models.0.num_replicas",
            1,
            "num_replicas",
        ),
        (
            "run_plan.semantic_plan.deployment.models.0.max_model_len",
            2048,
            "max_model_len",
        ),
        (
            "run_plan.semantic_plan.deployment.models.0.gpu_memory_utilization",
            0.90,
            "gpu_memory_utilization",
        ),
        (
            "run_plan.semantic_plan.deployment.models.0.max_num_seqs",
            16,
            "max_num_seqs",
        ),
        (
            "run_plan.semantic_plan.deployment.models.0.enforce_eager",
            False,
            "enforce_eager",
        ),
        ("run_plan.semantic_plan.deployment.gateway.kind", "envoy", "gateway.kind"),
        ("run_plan.semantic_plan.client.destination", "direct", "client.destination"),
        ("run_plan.semantic_plan.client.streaming", True, "client.streaming"),
        ("run_plan.semantic_plan.client.num_runs", 1, "client.num_runs"),
        ("run_plan.semantic_plan.workload.client_dest", "direct", "workload.client_dest"),
        ("run_plan.semantic_plan.workload.rate_per_node", 0.4, "rate_per_node"),
        ("run_plan.semantic_plan.workload.duration_s", 60.0, "duration_s"),
    ],
)
def test_current_pp_plan_rejects_semantic_drift(path, value, field):
    from eval.plot import sc26_full_figures as figures

    accepted = _current_pp_accepted(figures, 4)
    _set_nested(accepted, path, value)
    with pytest.raises(RuntimeError, match=field):
        figures._require_current_pp405b_plan(accepted, nodes=4, run_group="run3")


@pytest.mark.parametrize(
    ("nodes", "replacement", "field"),
    [
        (4, "f" * 64, "source_snapshot_hash"),
        (32, "f" * 64, "source_snapshot_hash"),
    ],
)
def test_current_pp_plan_rejects_any_unreviewed_source(nodes, replacement, field):
    from eval.plot import sc26_full_figures as figures

    accepted = _current_pp_accepted(figures, nodes)
    accepted.run_plan.source_snapshot_hash = replacement
    run_group = figures.PP405B_CURRENT_RUN_GROUPS[nodes]
    with pytest.raises(RuntimeError, match=field):
        figures._require_current_pp405b_plan(accepted, nodes=nodes, run_group=run_group)


@pytest.mark.parametrize(
    ("nodes", "replacement"),
    [(4, "bounded_hash_v2"), (32, "legacy_truncate_v1")],
)
def test_current_pp_plan_rejects_wrong_materialization_identity_scheme(nodes, replacement):
    from eval.plot import sc26_full_figures as figures

    accepted = _current_pp_accepted(figures, nodes)
    accepted.run_plan.deployment_id_scheme = replacement
    run_group = figures.PP405B_CURRENT_RUN_GROUPS[nodes]
    with pytest.raises(RuntimeError, match="deployment_id_scheme"):
        figures._require_current_pp405b_plan(accepted, nodes=nodes, run_group=run_group)


@pytest.mark.parametrize("nodes", [4, 32])
def test_current_pp_plan_rejects_unreviewed_semantic_hash(nodes):
    from eval.plot import sc26_full_figures as figures

    accepted = _current_pp_accepted(figures, nodes)
    accepted.run_plan.run_semantic_hash = "f" * 64
    run_group = figures.PP405B_CURRENT_RUN_GROUPS[nodes]
    with pytest.raises(RuntimeError, match="run_semantic_hash"):
        figures._require_current_pp405b_plan(accepted, nodes=nodes, run_group=run_group)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("duration_s", 0.0),
        ("duration_s", -1.0),
        ("duration_s", float("nan")),
        ("duration_s", float("inf")),
        ("duration_s", "120"),
        ("rps", -1.0),
        ("rps", float("nan")),
        ("rps", float("inf")),
        ("requests_completed", 0),
        ("requests_completed", -1),
        ("requests_completed", 1.5),
        ("errors", -1),
        ("errors", 97),
        ("errors", 1.5),
        ("p50_s", -1.0),
        ("p50_s", float("nan")),
        ("p50_s", float("inf")),
        ("p99_s", -1.0),
        ("p99_s", float("nan")),
        ("p99_s", float("inf")),
    ],
)
def test_current_pp_metrics_reject_malformed_or_non_finite_values(field, value):
    from eval.plot import sc26_full_figures as figures

    record = _current_pp_record()
    record[field] = value
    with pytest.raises(RuntimeError, match=field):
        figures._validate_current_pp405b_record(record, context="result", nodes=4)


def test_current_pp_metrics_require_sane_success_counts_and_percentiles():
    from eval.plot import sc26_full_figures as figures

    record = _current_pp_record()
    record["errors"] = record["requests_completed"]
    with pytest.raises(RuntimeError, match="errors must be exactly 0"):
        figures._validate_current_pp405b_record(record, context="result", nodes=4)

    record = _current_pp_record()
    record["requests_scheduled"] *= 2
    with pytest.raises(RuntimeError, match="requests_scheduled"):
        figures._validate_current_pp405b_record(record, context="result", nodes=4)

    record = _current_pp_record()
    record["p99_s"] = record["p50_s"] - 0.1
    with pytest.raises(RuntimeError, match="p99_s"):
        figures._validate_current_pp405b_record(record, context="result", nodes=4)

    record = _current_pp_record()
    record["rps"] = 0.7
    with pytest.raises(RuntimeError, match="requests_completed/duration_s"):
        figures._validate_current_pp405b_record(record, context="result", nodes=4)


def test_current_pp_result_requires_exactly_two_ordered_runs():
    from eval.plot import sc26_full_figures as figures

    document = _current_pp_document()
    overall, data_runs = figures._validate_current_pp405b_result(
        document, result_path=Path("/paper/result0.json"), nodes=4
    )
    assert overall is document["overall"]
    assert data_runs == [document["per_run"][1]]

    document["per_run"][1]["run_index"] = 2
    with pytest.raises(RuntimeError, match=r"expected \[0, 1\]"):
        figures._validate_current_pp405b_result(
            document, result_path=Path("/paper/result0.json"), nodes=4
        )


def test_current_pp_result_requires_overall_to_mirror_reported_run():
    from eval.plot import sc26_full_figures as figures

    document = _current_pp_document()
    document["overall"]["p50_s"] += 0.1
    with pytest.raises(RuntimeError, match=r"overall does not mirror per_run\[1\]"):
        figures._validate_current_pp405b_result(
            document, result_path=Path("/paper/result0.json"), nodes=4
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [("successes", 95), ("success_rps", 0.7), ("success_rps", float("inf"))],
)
def test_current_pp_result_rejects_invalid_per_run_success_fields(field, value):
    from eval.plot import sc26_full_figures as figures

    document = _current_pp_document()
    document["per_run"][1][field] = value
    with pytest.raises(RuntimeError, match=field):
        figures._validate_current_pp405b_result(
            document, result_path=Path("/paper/result0.json"), nodes=4
        )


def _null_trial(nodes: int, run_group: str) -> dict:
    group_index = int(run_group.removeprefix("run"))
    return {
        "run_group": run_group,
        "nodes": nodes,
        "source_snapshot_hash": "a" * 64,
        "generation": nodes * 100 + group_index,
        "run_provenance_hash": f"{nodes * 10 + group_index:064x}",
        "ready_s": float(nodes + group_index),
        "trace_s": float(nodes),
        "deploy_s": float(nodes - 1),
        "stage3_s": float(nodes - 0.5),
    }


def test_null_curve_requires_independent_trials_and_retains_all_timings(monkeypatch):
    from eval.plot import nullcompute_startup_table as table

    monkeypatch.setattr(table, "_load_trial", lambda _root, group, nodes: _null_trial(nodes, group))
    rows = table.load_curve(Path("/unused"))
    assert [row["nodes"] for row in rows] == [32, 64, 128, 256]
    assert rows[0]["trace_trials_s"] == [32.0, 32.0]
    assert rows[0]["stage3_trials_s"] == [31.5, 31.5]
    assert len(set(rows[0]["generations"])) == 2

    def copied_trial(_root, group, nodes):
        trial = _null_trial(nodes, group)
        trial["generation"] = nodes
        trial["run_provenance_hash"] = f"{nodes:064x}"
        return trial

    monkeypatch.setattr(table, "_load_trial", copied_trial)
    with pytest.raises(RuntimeError, match="not independent lifecycles"):
        table.load_curve(Path("/unused"))


def test_null_trial_acceptance_requires_grouped_graph_cardinality(monkeypatch):
    from eval.plot import nullcompute_startup_table as table

    entries = {
        logical_id: SimpleNamespace(
            logical_id=logical_id,
            path=f"{logical_id}.json",
            sha256=("d" * 64 if logical_id == "deployment_ready_evidence" else "e" * 64),
        )
        for logical_id in table.EXPECTED_RESULT_IDS
    }
    accepted = SimpleNamespace(
        manifest=SimpleNamespace(
            expected_ids=tuple(sorted(table.EXPECTED_RESULT_IDS)),
            entries=tuple(entries.values()),
        ),
        run_plan=SimpleNamespace(
            run_semantic_hash="a" * 64,
            deployment_plan_hash="b" * 64,
            source_snapshot_hash="c" * 64,
        ),
        run_provenance=SimpleNamespace(generation=7, run_provenance_hash="f" * 64),
    )
    metrics = {
        "schema_version": 2,
        "num_nodes": 32,
        "null_compute": True,
        "serve_application_layout": "node_grouped_null",
        "expected_model_replicas": 384,
        "replica_measurement_count": 384,
        "expected_serve_applications": 64,
        "expected_receipt_requirements": 450,
        "generation": 7,
        "run_semantic_hash": "a" * 64,
        "deployment_plan_hash": "b" * 64,
        "source_snapshot_hash": "c" * 64,
        "deployment_ready_evidence_sha256": "d" * 64,
        "ready_after_trace_start_s": 10.0,
        "trace_total_duration_s": 9.0,
        "phase_timings": [
            {"name": "deploy_from_canonical_plan", "duration_s": 8.0},
            {"name": "stage3.total", "duration_s": 8.5},
        ],
    }
    ready = {
        "schema_version": 2,
        "state": "READY",
        "generation": 7,
        "deployment_plan_hash": "b" * 64,
        "run_semantic_hash": "a" * 64,
        "run_provenance_hash": "f" * 64,
    }
    terminal = {**ready, "state": "STOPPED"}

    monkeypatch.setattr(table, "require_accepted_paper_run", lambda *_args, **_kwargs: accepted)

    def load(path):
        name = Path(path).name
        if name == "startup_metrics.json":
            return metrics
        if name == "deployment_ready_evidence.json":
            return ready
        if name == "deployment_terminal_status.json":
            return terminal
        raise AssertionError(name)

    # The fake manifest paths use logical IDs; map those exact names too.
    entries["startup_metrics"].path = "startup_metrics.json"
    entries["deployment_ready_evidence"].path = "deployment_ready_evidence.json"
    entries["deployment_terminal_status"].path = "deployment_terminal_status.json"
    monkeypatch.setattr(table, "strict_json_load_path", load)

    trial = table._load_trial(Path("/unused"), "run4", 32)
    assert trial["ready_s"] == 10.0
    metrics["expected_serve_applications"] = 65
    with pytest.raises(RuntimeError, match="metrics identity"):
        table._load_trial(Path("/unused"), "run4", 32)
