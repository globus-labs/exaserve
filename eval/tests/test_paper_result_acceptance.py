"""Paper consumers reject valid-looking artifacts with broken identity edges."""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import replace
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


def _legacy_pp_record(*, nodes, run_index, errors):
    requests = 24 * nodes
    duration = 120.0 + run_index
    successes = requests - errors
    return {
        "run_index": run_index,
        "requests_completed": requests,
        "requests_scheduled": requests,
        "successes": successes,
        "errors": errors,
        "duration_s": duration,
        "rps": requests / duration,
        "success_rps": successes / duration,
        "p50_s": 1.0 + run_index,
        "p99_s": 2.0 + run_index,
    }


def _legacy_pp_document(*, nodes, errors=(0, 0), destination="direct"):
    per_run = [
        _legacy_pp_record(nodes=nodes, run_index=index, errors=run_errors)
        for index, run_errors in enumerate(errors)
    ]
    overall = {
        key: value
        for key, value in per_run[-1].items()
        if key not in {"run_index", "successes", "success_rps"}
    }
    return {
        "meta": {
            "num_runs": len(per_run),
            "completed_runs": len(per_run),
            "dest": destination,
        },
        "overall": overall,
        "per_run": per_run,
    }


def _legacy_pp_plan(*, stem, nodes, run_group, source_commit, proxy="direct", runs=2):
    destination = "direct" if proxy == "direct" else "proxy"
    gateway = "none" if proxy == "direct" else "haproxy"
    proxy_config = {"type": gateway}
    if proxy == "haproxy":
        proxy_config.update(
            {
                "num_workers": 1,
                "options": {"balance": "leastconn", "maxconn": 50000},
            }
        )
    return {
        "run_id": f"n{nodes}",
        "run_group_id": run_group,
        "repo_root": f"/snapshots/{source_commit}",
        "snapshot_root": f"/snapshots/{source_commit}",
        "spec_name": stem,
        "variant_name": f"n{nodes}",
        "axis_values": {"num_nodes": nodes},
        "backend_name": "ray",
        "backend_args": {"proxy": proxy_config},
        "deployment": {
            "num_nodes": nodes,
            "engine": "vllm",
            "num_gpus_per_node": 12,
            "replica_max_ongoing_requests": 16,
            "models": [
                {
                    "model_id": "meta-llama/Llama-3.1-405B-Instruct",
                    "tensor_parallel_size": 8,
                    "pipeline_parallel_size": 2,
                    "num_replicas": nodes // 2,
                    "num_cpus_per_replica": 4,
                    "max_model_len": 4096,
                    "gpu_memory_utilization": 0.9,
                    "max_num_seqs": 8,
                    "enforce_eager": True,
                }
            ],
        },
        "client": {
            "dest": destination,
            "stream": True,
            "num_runs": runs,
            "startup_only": False,
            "num_nodes": 4,
            "num_go_procs": 8 if proxy == "direct" else 4,
            "num_go_workers": 4,
            "go_concurrency": 256,
            "include_tp": False,
        },
        "scheduler": {"nodes": nodes},
        "workload": {
            "duration": 120.0,
            "input_len": 64,
            "output_len": 64,
            "rate_per_node": 0.2,
            "arrival": "fixed",
            "generation_mode": "deterministic",
            "seed": 42,
        },
        "trace": {"kind": "weak_scaling"},
    }


def _write_legacy_pp_cell(
    root,
    evidence,
    *,
    stem,
    nodes,
    run_group,
    proxy="direct",
    result_name="result0.json",
    errors=(0, 0),
    requires_allocfix=False,
):
    import yaml

    source_commit = f"{nodes:040x}"
    cell = root / stem / run_group / f"n{nodes}"
    (cell / "results").mkdir(parents=True)
    destination = "direct" if proxy == "direct" else "proxy"
    result = _legacy_pp_document(nodes=nodes, errors=errors, destination=destination)
    result_path = cell / "results" / result_name
    result_path.write_text(json.dumps(result, allow_nan=False), encoding="utf-8")
    plan = _legacy_pp_plan(
        stem=stem,
        nodes=nodes,
        run_group=run_group,
        source_commit=source_commit,
        proxy=proxy,
        runs=len(errors),
    )
    plan_path = cell / "run.yaml"
    plan_path.write_text(yaml.safe_dump(plan, sort_keys=False), encoding="utf-8")
    pbs_job_id = 8_000_000 + nodes
    stdout_path = (
        cell
        / "logs"
        / "pbs"
        / "stdout"
        / f"{pbs_job_id}.aurora-pbs-0001.hostmgmt.cm.aurora.alcf.anl.gov.OU"
    )
    stdout_path.parent.mkdir(parents=True)
    stdout_lines = []
    if requires_allocfix:
        stdout_lines.append(
            f"[allocfix] Ray cluster truncated to {nodes} nodes to match the deployment:"
        )
    stdout_lines.extend(
        [
            f"[System] Total Nodes: {nodes}",
            f"[AuroraServe] Wrote {nodes} Ray node IP(s) -> {cell / 'runtime' / 'ray_node_ips.txt'}",
            "  - meta-llama/Llama-3.1-405B-Instruct: "
            f"requested={nodes // 2}, assigned={nodes // 2}",
            f">>> [REPLAY] Saved results to {result_path}",
        ]
    )
    stdout_path.write_text("\n".join(stdout_lines) + "\n", encoding="utf-8")
    return evidence.LegacyPP405BRef(
        nodes=nodes,
        run_group_id=run_group,
        result_name=result_name,
        result_sha256=hashlib.sha256(result_path.read_bytes()).hexdigest(),
        run_yaml_sha256=hashlib.sha256(plan_path.read_bytes()).hexdigest(),
        source_commit=source_commit,
        pbs_job_id=pbs_job_id,
        stdout_sha256=hashlib.sha256(stdout_path.read_bytes()).hexdigest(),
        errors_by_run=errors,
        requires_allocfix=requires_allocfix,
        expected_successful_rps=sum(
            (24 * nodes - run_errors) / (120.0 + index)
            for index, run_errors in enumerate(errors)
            if index > 0
        )
        / (len(errors) - 1),
    )


def _legacy_pp_series(root, evidence, *, error_node=None):
    stem = "legacy_direct"
    refs = tuple(
        _write_legacy_pp_cell(
            root,
            evidence,
            stem=stem,
            nodes=nodes,
            run_group=f"run{index}",
            result_name=("result1.json" if nodes == 64 else "result0.json"),
            errors=((0, 3) if nodes == error_node else (0, 0)),
            requires_allocfix=nodes == 64,
        )
        for index, nodes in enumerate(evidence.PP405B_NODE_COUNTS)
    )
    return evidence.PP405BSeries(
        stem=stem,
        key="legacy_test",
        proxy="direct",
        mode="stream",
        label="legacy",
        loader_kind=evidence.PP405BLoaderKind.LEGACY_PINNED_V1,
        legacy_refs=refs,
    )


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


def test_legacy_pp_reader_uses_exact_refs_and_keeps_declared_errors(tmp_path):
    from eval.lib import pp405b_evidence as evidence

    series = _legacy_pp_series(tmp_path, evidence, error_node=64)
    stray = tmp_path / series.stem / "run999" / "n4" / "results"
    stray.mkdir(parents=True)
    (stray / "result0.json").write_text("not json", encoding="utf-8")

    points = evidence.load_pp405b_points(series, runs_root=tmp_path)

    assert [point["nodes"] for point in points] == [4, 8, 16, 32, 64, 128, 256]
    assert [point["rep"] for point in points] == [2, 4, 8, 16, 32, 64, 128]
    point64 = next(point for point in points if point["nodes"] == 64)
    assert point64["n_iters"] == 1
    assert point64["errfrac"] == pytest.approx(3 / 1536)
    assert point64["srps"] == pytest.approx((1536 - 3) / 121.0)


@pytest.mark.parametrize("mutation", ["missing", "duplicate"])
def test_legacy_pp_reader_requires_the_exact_seven_node_ladder(mutation):
    from eval.lib import pp405b_evidence as evidence
    from eval.plot import sc26_full_figures as figures

    refs = figures.PP405B_LEGACY_DIRECT_REFS
    if mutation == "missing":
        refs = refs[:-1]
    else:
        refs = (refs[0], refs[0], *refs[2:])
    series = replace(
        figures.PP405B_VARIANTS[0],
        legacy_refs=refs,
    )
    with pytest.raises(RuntimeError, match="must select exactly nodes"):
        evidence._validate_series(series)


def test_legacy_pp_json_is_hashed_before_strict_parsing(tmp_path):
    from eval.lib import pp405b_evidence as evidence

    artifact = tmp_path / "result.json"
    artifact.write_text("not json", encoding="utf-8")
    with pytest.raises(RuntimeError, match="SHA-256 mismatch"):
        evidence._load_sha_bound_json(artifact, "0" * 64, label="test result")

    for invalid in ('{"state":"one","state":"two"}', '{"duration_s":NaN}'):
        artifact.write_text(invalid, encoding="utf-8")
        digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
        with pytest.raises(RuntimeError, match="not strict UTF-8 JSON"):
            evidence._load_sha_bound_json(artifact, digest, label="test result")


def test_legacy_pp_yaml_errors_are_normalized(tmp_path):
    from eval.lib import pp405b_evidence as evidence

    duplicate = tmp_path / "duplicate.yaml"
    duplicate.write_text("schema_version: 1\nschema_version: 2\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="ledger is not strict YAML"):
        evidence.load_legacy_pp405b_ledger(duplicate)

    digest = hashlib.sha256(duplicate.read_bytes()).hexdigest()
    with pytest.raises(RuntimeError, match="pinned legacy plan is not strict YAML"):
        evidence._load_sha_bound_yaml(duplicate, digest, label="legacy plan")


def test_legacy_pp_plan_binds_source_run_and_serving_semantics():
    from eval.lib import pp405b_evidence as evidence
    from eval.plot import sc26_full_figures as figures

    ref = figures.PP405B_LEGACY_DIRECT_REFS[0]
    series = figures.PP405B_VARIANTS[0]
    plan = _legacy_pp_plan(
        stem=series.stem,
        nodes=ref.nodes,
        run_group=ref.run_group_id,
        source_commit=ref.source_commit,
        runs=len(ref.expected_run_indices),
    )
    evidence._validate_legacy_plan(
        plan,
        path=Path("/paper/run.yaml"),
        series=series,
        ref=ref,
    )

    for path, value, message in (
        (("run_group_id",), "run999", "run_group_id"),
        (("snapshot_root",), "/snapshots/bad", "snapshot_root"),
        (("backend_args", "proxy", "type"), "haproxy", "proxy.type"),
        (("deployment", "replica_max_ongoing_requests"), 8, "replica_max_ongoing_requests"),
        (("deployment", "models", 0, "pipeline_parallel_size"), 1, "pipeline_parallel_size"),
        (("deployment", "models", 0, "num_cpus_per_replica"), 2, "num_cpus_per_replica"),
        (("client", "stream"), False, "client.stream"),
        (("client", "num_nodes"), 8, "client.num_nodes"),
        (("client", "num_go_procs"), 4, "client.num_go_procs"),
        (("client", "num_go_workers"), 8, "client.num_go_workers"),
        (("client", "go_concurrency"), 128, "client.go_concurrency"),
        (("client", "include_tp"), True, "client.include_tp"),
        (("workload", "arrival"), "poisson", "workload.arrival"),
        (("workload", "generation_mode"), "random", "workload.generation_mode"),
        (("workload", "seed"), 7, "workload.seed"),
    ):
        mutated = copy.deepcopy(plan)
        target = mutated
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = value
        with pytest.raises(RuntimeError, match=message):
            evidence._validate_legacy_plan(
                mutated,
                path=Path("/paper/run.yaml"),
                series=series,
                ref=ref,
            )


def test_legacy_haproxy_plan_binds_worker_and_balance_contract():
    from eval.lib import pp405b_evidence as evidence
    from eval.plot import sc26_full_figures as figures

    ref = figures.PP405B_LEGACY_HAPROXY_REFS[0]
    series = figures.PP405B_VARIANTS[1]
    plan = _legacy_pp_plan(
        stem=series.stem,
        nodes=ref.nodes,
        run_group=ref.run_group_id,
        source_commit=ref.source_commit,
        proxy="haproxy",
        runs=len(ref.errors_by_run),
    )
    evidence._validate_legacy_plan(
        plan,
        path=Path("/paper/run.yaml"),
        series=series,
        ref=ref,
    )
    for path, value, message in (
        (("backend_args", "proxy", "num_workers"), 2, "num_workers"),
        (("backend_args", "proxy", "options", "balance"), "roundrobin", "balance"),
        (("backend_args", "proxy", "options", "maxconn"), 4000, "maxconn"),
        (("client", "num_go_procs"), 8, "num_go_procs"),
    ):
        mutated = copy.deepcopy(plan)
        target = mutated
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = value
        with pytest.raises(RuntimeError, match=message):
            evidence._validate_legacy_plan(
                mutated,
                path=Path("/paper/run.yaml"),
                series=series,
                ref=ref,
            )


@pytest.mark.parametrize(
    "replacement",
    [
        "[System] Total Nodes: 256",
        "[AuroraServe] Wrote 256 Ray node IP(s) -> /wrong/ray_node_ips.txt",
        "  - meta-llama/Llama-3.1-405B-Instruct: requested=32, assigned=31",
        ">>> [REPLAY] Saved results to /wrong/result0.json",
    ],
)
def test_legacy_pp_stdout_rejects_tamper_and_allocation_mismatch(tmp_path, replacement):
    from eval.lib import pp405b_evidence as evidence

    series = _legacy_pp_series(tmp_path, evidence)
    ref = series.legacy_refs[0]
    cell = tmp_path / series.stem / ref.run_group_id / ref.run_id
    result_path = cell / "results" / ref.result_name
    stdout_path = cell / "logs" / "pbs" / "stdout" / ref.stdout_name
    lines = stdout_path.read_text(encoding="utf-8").splitlines()
    lines[0] = replacement
    payload = ("\n".join(lines) + "\n").encode()
    with pytest.raises(RuntimeError, match="must contain exactly one"):
        evidence._validate_producing_stdout(
            payload,
            path=stdout_path,
            result_path=result_path,
            ref=ref,
        )


def test_legacy_pp_stdout_requires_allocfix_only_when_declared(tmp_path):
    from eval.lib import pp405b_evidence as evidence

    series = _legacy_pp_series(tmp_path, evidence)
    ref = series.legacy_refs[4]
    cell = tmp_path / series.stem / ref.run_group_id / ref.run_id
    result_path = cell / "results" / ref.result_name
    stdout_path = cell / "logs" / "pbs" / "stdout" / ref.stdout_name
    payload = stdout_path.read_bytes()
    evidence._validate_producing_stdout(
        payload,
        path=stdout_path,
        result_path=result_path,
        ref=ref,
    )
    with pytest.raises(RuntimeError, match="unexpected allocfix"):
        evidence._validate_producing_stdout(
            payload,
            path=stdout_path,
            result_path=result_path,
            ref=replace(ref, requires_allocfix=False),
        )


def test_legacy_pp_loader_rejects_stdout_byte_tamper(tmp_path):
    from eval.lib import pp405b_evidence as evidence

    series = _legacy_pp_series(tmp_path, evidence)
    ref = series.legacy_refs[0]
    cell = tmp_path / series.stem / ref.run_group_id / ref.run_id
    stdout_path = cell / "logs" / "pbs" / "stdout" / ref.stdout_name
    stdout_path.write_text(stdout_path.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="PBS stdout SHA-256 mismatch"):
        evidence.load_pp405b_points(series, runs_root=tmp_path)


def test_legacy_pp_accounting_accepts_only_the_declared_nonzero_errors():
    from eval.lib import pp405b_evidence as evidence
    from eval.plot import sc26_full_figures as figures

    base = figures.PP405B_LEGACY_HAPROXY_REFS[4]
    ref = replace(
        base,
        nodes=4,
        errors_by_run=(2, 3),
    )
    series = replace(figures.PP405B_VARIANTS[1], legacy_refs=(ref,))
    document = _legacy_pp_document(nodes=4, errors=ref.errors_by_run, destination="proxy")
    _, data_runs = evidence._validate_legacy_result(
        document,
        result_path=Path("result.json"),
        series=series,
        ref=ref,
    )
    point = evidence._summarize_point(nodes=4, data_runs=data_runs)
    assert point["errfrac"] == pytest.approx(3 / 96)

    for field, value in (
        ("requests_scheduled", 95),
        ("requests_completed", 95),
        ("successes", 94),
        ("errors", 0),
        ("run_index", 2),
    ):
        mutated = copy.deepcopy(document)
        mutated["per_run"][1][field] = value
        if field in {"requests_scheduled", "requests_completed", "errors"}:
            mutated["overall"][field] = value
        with pytest.raises(RuntimeError, match=field.replace("run_index", "indices")):
            evidence._validate_legacy_result(
                mutated,
                result_path=Path("result.json"),
                series=series,
                ref=ref,
            )


def test_pp_loader_dispatch_rejects_unknown_kinds(tmp_path):
    from eval.lib import pp405b_evidence as evidence
    from eval.plot import sc26_full_figures as figures

    legacy = figures.PP405B_VARIANTS[0]
    with pytest.raises(RuntimeError, match="unsupported PP=2 loader kind"):
        evidence.load_pp405b_points(
            replace(legacy, loader_kind="legacy-ish"),
            runs_root=tmp_path,
        )


def test_current_pp_reader_rejects_a_truncated_node_ladder(tmp_path):
    from eval.lib import pp405b_evidence as evidence
    from eval.plot import sc26_full_figures as figures

    series = evidence.PP405BSeries(
        stem="pp405b_pp2_haproxy_nostream_v040",
        key="current",
        proxy="haproxy",
        mode="nonstream",
        label="current",
        loader_kind=evidence.PP405BLoaderKind.CURRENT_MANIFEST_V2,
        current_refs=(figures.PP405B_CURRENT_REFS[0],),
    )
    with pytest.raises(RuntimeError, match=r"exactly nodes \[4, 8, 16, 32, 64, 128, 256\]"):
        evidence.load_pp405b_points(series, runs_root=tmp_path)


def test_current_pp_loader_succeeds_through_the_manifest_path(tmp_path, monkeypatch):
    from eval.lib import pp405b_evidence as evidence
    from eval.lib import paper_acceptance
    from eval.plot import sc26_full_figures as figures

    series = figures.PP405B_VARIANTS[2]
    accepted_paths = []

    def accept(cell, *, required_result_ids):
        accepted_paths.append(Path(cell))
        assert required_result_ids == {
            "replay/default",
            "deployment_ready_evidence",
            "compatibility_receipts",
            "run_provenance",
        }
        nodes = int(Path(cell).name.removeprefix("n"))
        accepted = _current_pp_accepted(figures, nodes)
        accepted.manifest = SimpleNamespace(
            entries=(SimpleNamespace(logical_id="replay/default", path="result0.json"),)
        )
        return accepted

    def load(result_path):
        nodes = int(Path(result_path).parents[1].name.removeprefix("n"))
        return _current_pp_document(nodes=nodes)

    monkeypatch.setattr(paper_acceptance, "require_accepted_paper_run", accept)
    monkeypatch.setattr(evidence, "strict_json_load_path", load)
    points = evidence.load_pp405b_points(series, runs_root=tmp_path)

    assert [point["nodes"] for point in points] == list(evidence.PP405B_NODE_COUNTS)
    assert [point["rep"] for point in points] == [2, 4, 8, 16, 32, 64, 128]
    assert len(accepted_paths) == 7
    assert [path.name for path in accepted_paths] == [
        f"n{nodes}" for nodes in evidence.PP405B_NODE_COUNTS
    ]


def test_current_pp_plan_accepts_only_the_reviewed_node_to_source_mapping():
    from eval.lib import pp405b_evidence as evidence
    from eval.plot import sc26_full_figures as figures

    series = figures.PP405B_VARIANTS[2]
    for ref in series.current_refs:
        accepted = _current_pp_accepted(figures, ref.nodes)
        evidence._validate_current_plan(accepted, series=series, ref=ref)


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
    from eval.lib import pp405b_evidence as evidence
    from eval.plot import sc26_full_figures as figures

    series = figures.PP405B_VARIANTS[2]
    ref = series.current_refs[0]
    accepted = _current_pp_accepted(figures, 4)
    _set_nested(accepted, path, value)
    with pytest.raises(RuntimeError, match=field):
        evidence._validate_current_plan(accepted, series=series, ref=ref)


@pytest.mark.parametrize(
    ("nodes", "replacement", "field"),
    [
        (4, "f" * 64, "source_snapshot_hash"),
        (32, "f" * 64, "source_snapshot_hash"),
    ],
)
def test_current_pp_plan_rejects_any_unreviewed_source(nodes, replacement, field):
    from eval.lib import pp405b_evidence as evidence
    from eval.plot import sc26_full_figures as figures

    series = figures.PP405B_VARIANTS[2]
    ref = next(item for item in series.current_refs if item.nodes == nodes)
    accepted = _current_pp_accepted(figures, nodes)
    accepted.run_plan.source_snapshot_hash = replacement
    with pytest.raises(RuntimeError, match=field):
        evidence._validate_current_plan(accepted, series=series, ref=ref)


@pytest.mark.parametrize(
    ("nodes", "replacement"),
    [(4, "bounded_hash_v2"), (32, "legacy_truncate_v1")],
)
def test_current_pp_plan_rejects_wrong_materialization_identity_scheme(nodes, replacement):
    from eval.lib import pp405b_evidence as evidence
    from eval.plot import sc26_full_figures as figures

    series = figures.PP405B_VARIANTS[2]
    ref = next(item for item in series.current_refs if item.nodes == nodes)
    accepted = _current_pp_accepted(figures, nodes)
    accepted.run_plan.deployment_id_scheme = replacement
    with pytest.raises(RuntimeError, match="deployment_id_scheme"):
        evidence._validate_current_plan(accepted, series=series, ref=ref)


@pytest.mark.parametrize("nodes", [4, 32])
def test_current_pp_plan_rejects_unreviewed_semantic_hash(nodes):
    from eval.lib import pp405b_evidence as evidence
    from eval.plot import sc26_full_figures as figures

    series = figures.PP405B_VARIANTS[2]
    ref = next(item for item in series.current_refs if item.nodes == nodes)
    accepted = _current_pp_accepted(figures, nodes)
    accepted.run_plan.run_semantic_hash = "f" * 64
    with pytest.raises(RuntimeError, match="run_semantic_hash"):
        evidence._validate_current_plan(accepted, series=series, ref=ref)


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
    from eval.lib import pp405b_evidence as evidence

    record = _current_pp_record()
    record[field] = value
    with pytest.raises(RuntimeError, match=field):
        evidence._validate_record(
            record,
            context="result",
            requests=96,
            errors=0,
            require_success_fields=False,
        )


def test_current_pp_metrics_require_sane_success_counts_and_percentiles():
    from eval.lib import pp405b_evidence as evidence

    record = _current_pp_record()
    record["errors"] = record["requests_completed"]
    with pytest.raises(RuntimeError, match="errors"):
        evidence._validate_record(
            record, context="result", requests=96, errors=0, require_success_fields=False
        )

    record = _current_pp_record()
    record["requests_scheduled"] *= 2
    with pytest.raises(RuntimeError, match="requests_scheduled"):
        evidence._validate_record(
            record, context="result", requests=96, errors=0, require_success_fields=False
        )

    record = _current_pp_record()
    record["p99_s"] = record["p50_s"] - 0.1
    with pytest.raises(RuntimeError, match="p99_s"):
        evidence._validate_record(
            record, context="result", requests=96, errors=0, require_success_fields=False
        )

    record = _current_pp_record()
    record["rps"] = 0.7
    with pytest.raises(RuntimeError, match="requests_completed/duration_s"):
        evidence._validate_record(
            record, context="result", requests=96, errors=0, require_success_fields=False
        )


def test_current_pp_result_requires_exactly_two_ordered_runs():
    from eval.lib import pp405b_evidence as evidence

    document = _current_pp_document()
    overall, data_runs = evidence._validate_current_result(
        document, result_path=Path("/paper/result0.json"), nodes=4
    )
    assert overall is document["overall"]
    assert data_runs == [document["per_run"][1]]

    document["per_run"][1]["run_index"] = 2
    with pytest.raises(RuntimeError, match=r"expected \[0, 1\]"):
        evidence._validate_current_result(
            document, result_path=Path("/paper/result0.json"), nodes=4
        )


def test_current_pp_result_requires_overall_to_mirror_reported_run():
    from eval.lib import pp405b_evidence as evidence

    document = _current_pp_document()
    document["overall"]["p50_s"] += 0.1
    with pytest.raises(RuntimeError, match=r"overall does not mirror per_run\[1\]"):
        evidence._validate_current_result(
            document, result_path=Path("/paper/result0.json"), nodes=4
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [("successes", 95), ("success_rps", 0.7), ("success_rps", float("inf"))],
)
def test_current_pp_result_rejects_invalid_per_run_success_fields(field, value):
    from eval.lib import pp405b_evidence as evidence

    document = _current_pp_document()
    document["per_run"][1][field] = value
    with pytest.raises(RuntimeError, match=field):
        evidence._validate_current_result(
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
