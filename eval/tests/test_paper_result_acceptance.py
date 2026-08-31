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
    monkeypatch.setattr(figures, "_require_complete_current_result", lambda _path: None)
    result = tmp_path / "current" / "run2" / "n4" / "results" / "result0.json"
    result.parent.mkdir(parents=True)
    result.write_text(
        json.dumps(
            {
                "overall": {},
                "per_run": [
                    {
                        "run_index": 1,
                        "requests_completed": 96,
                        "errors": 0,
                        "duration_s": 120.0,
                        "p50_s": 1.0,
                        "p99_s": 2.0,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match=r"expected \[4, 8, 16, 32, 64, 128, 256\]"):
        figures._pp405b_points("current", "run2", require_complete=True)


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
