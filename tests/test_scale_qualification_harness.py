"""Fail-closed contract tests for the future Aurora scale campaign."""

from copy import deepcopy
import hashlib
import inspect
import importlib.util
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]
HARNESS_PATH = ROOT / "scripts/hardening/run_scale_qualification.py"
SPEC = importlib.util.spec_from_file_location("scale_qualification_harness", HARNESS_PATH)
assert SPEC is not None and SPEC.loader is not None
scale = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(scale)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write(path: Path, value: bytes = b"fixture\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(value)
    return path


def _json(path: Path, value: object) -> Path:
    return _write(path, (json.dumps(value, sort_keys=True) + "\n").encode())


def _approval() -> dict:
    return {
        "schema_version": 1,
        "decision_id": "OWNER-SCALE-64",
        "decision": "APPROVE_QUALIFICATION_TARGET",
        "approver_id": "product-owner",
        "approved_at": "2026-08-10T00:00:00Z",
        "approved_max_nodes": 64,
        "required_ladder": [4, 16, 64],
        "dimensions": dict(scale._APPROVAL_DIMENSIONS),
    }


def _materialize_plan(repo: Path) -> tuple[Path, dict]:
    harness = _write(repo / "harness.py", b"# harness\n")
    lifecycle = _write(repo / "support/lifecycle.py", b"VALUE = 1\n")
    approval = _json(repo / "decisions/scale-64.json", _approval())
    release = repo / "release"
    bootstrap = release / "bootstrap"
    bootstrap.mkdir(parents=True)
    artifact = _write(release / "artifact.json")
    wheel = _write(release / "candidate.whl")
    sdist = _write(release / "candidate.tar.gz")
    candidate = {
        "release_path": "release",
        "artifact_manifest_path": "release/artifact.json",
        "artifact_manifest_sha256": _sha(artifact),
        "wheel_path": "release/candidate.whl",
        "wheel_sha256": _sha(wheel),
        "sdist_path": "release/candidate.tar.gz",
        "sdist_sha256": _sha(sdist),
        "bootstrap_path": "release/bootstrap",
        "site_profile_hash": "a" * 64,
        "compatibility_profile_hash": "b" * 64,
        "compatibility_manifest_hash": "c" * 64,
    }
    gates = []
    for nodes, tier in scale._TIER_CONTRACT.items():
        config = _write(repo / f"inputs/{nodes}/config.yaml")
        deployment = _write(repo / f"inputs/{nodes}/deployment.json")
        site = _write(repo / f"inputs/{nodes}/site.json")
        gates.append(
            {
                "gate_id": f"SCALE-{nodes}",
                "lane": "FINAL",
                "logical_nodes": nodes,
                "physical_allocation_nodes": nodes,
                "acquisition_source": "batch_pbs",
                "queue": tier["queue"],
                "lease_ttl": "1h",
                "expected_runtime": "45m",
                "node_hours": nodes,
                "attempt_limit": 1,
                "attempt": 1,
                "output_path": f"outputs/{nodes}",
                "engine_mode": "real",
                "scenario_profile": tier["scenario_profile"],
                "ready_timeout_s": 1200,
                "partial_observation_s": 60,
                "config_path": f"inputs/{nodes}/config.yaml",
                "config_sha256": _sha(config),
                "deployment_plan_path": f"inputs/{nodes}/deployment.json",
                "deployment_plan_sha256": _sha(deployment),
                "site_profile_path": f"inputs/{nodes}/site.json",
                "site_profile_sha256": _sha(site),
                "clean_state_reset_method": "fresh generation and exact cleanup",
                "retry_reason_policy": "no automatic retry",
                "expected_observations": scale._expected_observations(
                    "real", tier["scenario_profile"]
                ),
            }
        )
    document = {
        "schema_version": 3,
        "created_at": "2026-08-10T00:00:00Z",
        "candidate": candidate,
        "harness": {"path": "harness.py", "sha256": _sha(harness)},
        "support": {
            "lifecycle": {"path": "support/lifecycle.py", "sha256": _sha(lifecycle)},
        },
        "scope_approval": {"path": "decisions/scale-64.json", "sha256": _sha(approval)},
        "gates": gates,
    }
    path = _json(repo / "experiment.json", document)
    return path, document


def test_scale_harness_supports_the_established_direct_file_invocation():
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    completed = subprocess.run(
        [sys.executable, str(HARNESS_PATH), "--help"],
        cwd=ROOT,
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=10,
    )
    assert completed.returncode == 0, completed.stderr
    assert "--experiment-plan" in completed.stdout


def test_scale_remote_port_fault_uses_site_qualified_inline_helper():
    source = inspect.getsource(scale._launch_scale_scenario)
    assert "helper_path=" not in source
    assert "site_profile=site_profile" in source


def test_scale_gate_loader_accepts_only_predeclared_code_scope_and_ladder(tmp_path):
    plan_path, _document = _materialize_plan(tmp_path)
    loaded, gate, paths = scale._load_scale_gate(
        plan_path,
        "SCALE-16",
        repo_root=tmp_path,
        harness_path=tmp_path / "harness.py",
    )
    assert loaded["schema_version"] == 3
    assert gate["logical_nodes"] == 16
    assert paths["lifecycle_support"] == (tmp_path / "support/lifecycle.py").resolve()
    assert paths["scope_approval"] == (tmp_path / "decisions/scale-64.json").resolve()


def test_scale_gate_loader_rejects_support_changed_after_declaration(tmp_path):
    plan_path, _document = _materialize_plan(tmp_path)
    (tmp_path / "support/lifecycle.py").write_text("VALUE = 99\n")
    with pytest.raises(RuntimeError, match="support.lifecycle changed"):
        scale._load_scale_gate(
            plan_path,
            "SCALE-4",
            repo_root=tmp_path,
            harness_path=tmp_path / "harness.py",
        )


def test_verified_support_import_executes_only_the_declared_source_bytes(tmp_path):
    support = _write(tmp_path / "lifecycle.py", b"VALUE = 7\n")
    module = scale._load_verified_module(support, _sha(support))
    assert module.VALUE == 7

    with pytest.raises(RuntimeError, match="changed between"):
        scale._load_verified_module(support, "0" * 64)


def test_scale_gate_loader_rejects_missing_owner_identity(tmp_path):
    plan_path, document = _materialize_plan(tmp_path)
    approval_path = tmp_path / document["scope_approval"]["path"]
    approval = _approval()
    approval["approver_id"] = ""
    _json(approval_path, approval)
    document["scope_approval"]["sha256"] = _sha(approval_path)
    _json(plan_path, document)
    with pytest.raises(RuntimeError, match="approver_id must be non-empty"):
        scale._load_scale_gate(
            plan_path,
            "SCALE-4",
            repo_root=tmp_path,
            harness_path=tmp_path / "harness.py",
        )


@pytest.mark.parametrize(
    ("nodes", "field", "value", "message"),
    (
        (4, "queue", "debug", "requires queue='capacity'"),
        (16, "scenario_profile", "scale_boundary", "requires scenario_profile='scale_real'"),
        (64, "scenario_profile", "scale_real", "requires scenario_profile='scale_boundary'"),
    ),
)
def test_scale_gate_loader_rejects_tier_contract_drift(tmp_path, nodes, field, value, message):
    plan_path, document = _materialize_plan(tmp_path)
    gate = next(item for item in document["gates"] if item["logical_nodes"] == nodes)
    gate[field] = value
    if field == "scenario_profile":
        gate["expected_observations"] = scale._expected_observations("real", value)
    _json(plan_path, document)
    with pytest.raises(RuntimeError, match=message):
        scale._load_scale_gate(
            plan_path,
            f"SCALE-{nodes}",
            repo_root=tmp_path,
            harness_path=tmp_path / "harness.py",
        )


def test_candidate_identity_is_bound_to_profile_compatibility_and_gate():
    candidate = {
        "site_profile_hash": "s" * 64,
        "compatibility_profile_hash": "p" * 64,
        "compatibility_manifest_hash": "m" * 64,
    }
    plan = SimpleNamespace(
        site_profile_hash="s" * 64,
        compatibility_profile_hash="p" * 64,
        manifest_hash="m" * 64,
        deployment_id="scale-4",
    )
    profile = SimpleNamespace(site_profile_hash="s" * 64)
    scale._validate_candidate_plan_identity(plan, profile, candidate, gate_id="SCALE-4")

    for field, message in (
        ("site_profile_hash", "SiteProfile differs"),
        ("compatibility_profile_hash", "compatibility profile differs"),
        ("compatibility_manifest_hash", "compatibility manifest differs"),
    ):
        changed = deepcopy(candidate)
        changed[field] = "x" * 64
        with pytest.raises(RuntimeError, match=message):
            scale._validate_candidate_plan_identity(plan, profile, changed, gate_id="SCALE-4")
    with pytest.raises(RuntimeError, match="deployment_id"):
        scale._validate_candidate_plan_identity(plan, profile, candidate, gate_id="SCALE-16")


def test_scale_plan_requires_dense_aurora_placement_and_exact_tier_profile():
    replicas = tuple(SimpleNamespace(planned_ranks=(rank,)) for rank in range(4) for _ in range(12))
    model = SimpleNamespace(
        tensor_parallel_size=1,
        pipeline_parallel_size=1,
        num_replicas=48,
        replicas=replicas,
    )
    plan = SimpleNamespace(
        gateway=SimpleNamespace(kind="haproxy"),
        exposure=SimpleNamespace(mode="PROXIED_INTERNAL"),
        scale_envelope=SimpleNamespace(
            site_id="alcf-aurora",
            scheduler_type="pbs",
            vendor="xpu",
            accelerator="pvc",
            engine="vllm",
            gateway_kind="haproxy",
            exposure_mode="PROXIED_INTERNAL",
            request_mode="completion",
            streaming_mode="non_streaming",
            qualification_target_nodes=64,
            validation_mode=True,
        ),
        site_profile_id="alcf-aurora",
        vendor="xpu",
        engine="vllm",
        validation_mode=True,
        num_nodes=4,
        num_gpus_per_node=12,
        models=(model,),
        runtime=SimpleNamespace(null_compute=False),
        receipt_requirements=tuple(range(100)),
    )
    density = scale._validate_scale_plan(plan, engine_mode="real", scenario_profile="scale_real")
    assert density["replicas"] == 48

    plan.num_nodes = 8
    with pytest.raises(RuntimeError, match="one of"):
        scale._validate_scale_plan(plan, engine_mode="real", scenario_profile="scale_real")


def test_scale_plan_rejects_platform_dimension_drift():
    replicas = tuple(SimpleNamespace(planned_ranks=(rank,)) for rank in range(4) for _ in range(12))
    plan = SimpleNamespace(
        gateway=SimpleNamespace(kind="haproxy"),
        exposure=SimpleNamespace(mode="PROXIED_INTERNAL"),
        scale_envelope=SimpleNamespace(
            site_id="alcf-aurora",
            scheduler_type="pbs",
            vendor="xpu",
            accelerator="pvc",
            engine="vllm",
            gateway_kind="haproxy",
            exposure_mode="PROXIED_INTERNAL",
            request_mode="completion",
            streaming_mode="non_streaming",
            qualification_target_nodes=64,
            validation_mode=True,
        ),
        site_profile_id="alcf-aurora",
        vendor="cuda",
        engine="vllm",
        validation_mode=True,
        num_nodes=4,
        num_gpus_per_node=12,
        models=(
            SimpleNamespace(
                tensor_parallel_size=1,
                pipeline_parallel_size=1,
                num_replicas=48,
                replicas=replicas,
            ),
        ),
        runtime=SimpleNamespace(null_compute=False),
        receipt_requirements=tuple(range(100)),
    )
    with pytest.raises(RuntimeError, match="dimensions differ"):
        scale._validate_scale_plan(plan, engine_mode="real", scenario_profile="scale_real")


def test_batch_scale_allocation_rejects_oneapi_selector(tmp_path, monkeypatch):
    nodefile = tmp_path / "nodes"
    nodefile.write_text(socket.gethostname() + "\n")
    monkeypatch.setenv("PBS_JOBID", "fixture-job")
    monkeypatch.setenv("PBS_NODEFILE", str(nodefile))
    monkeypatch.setenv("PBS_ENVIRONMENT", "PBS_BATCH")
    monkeypatch.delenv("AURORA_SUBJOB", raising=False)
    monkeypatch.setenv("ONEAPI_DEVICE_SELECTOR", "level_zero:gpu")

    with pytest.raises(RuntimeError, match="must be absent"):
        scale._validated_scale_nodes(1, acquisition_source="batch_pbs")
