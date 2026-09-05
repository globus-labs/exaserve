"""Independent adjudication tests for optional 4/16/64 scale evidence."""

from copy import deepcopy
import hashlib
import importlib.util
import json
from pathlib import Path

import pytest


SOURCE_ROOT = Path(__file__).resolve().parents[1]
ADJUDICATOR_PATH = SOURCE_ROOT / "scripts/hardening/adjudicate.py"
SPEC = importlib.util.spec_from_file_location("scale_evidence_adjudicator", ADJUDICATOR_PATH)
assert SPEC is not None and SPEC.loader is not None
adjudicator = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(adjudicator)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write(root: Path, relative: str, value: bytes = b"fixture\n") -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(value)
    return path


def _json(root: Path, relative: str, value: object) -> Path:
    return _write(root, relative, (json.dumps(value, sort_keys=True) + "\n").encode())


def _relative(root: Path, path: Path) -> str:
    return path.relative_to(root).as_posix()


def _cleanup(root: Path, row: dict, scenario: dict) -> dict:
    name = scenario["scenario"]
    relative = f"{row['output_path']}/{name}/exact_generation_cleanup.json"
    payload = {
        "schema_version": 1,
        "reports": [
            {
                "schema_version": 1,
                "deployment_id": row["gate_id"].lower(),
                "generation": scenario["generation"],
                "deployment_plan_hash": scenario["deployment_plan_hash"],
                "hostname": f"node-{rank}",
                "matched": [],
                "signals": [],
                "survivors": [],
            }
            for rank in range(row["logical_nodes"])
        ],
    }
    path = _json(root, relative, payload)
    return {"path": relative, "sha256": _sha(path)}


def _scenario(name: str, *, nodes: int, replicas: int, plan_hash: str, slots: list[str]) -> dict:
    faults = {
        "normal-drain": "operator_drain",
        "gateway-death": "gateway_death",
        "worker-death": "worker_death",
        "duplicate-gateway-port": "duplicate_gateway_port",
        "partial-worker-proxy": "partial_proxy_readiness",
    }
    generation = nodes * 100 + list(faults).index(name) + 1
    fault = faults[name]
    scenario = {
        "scenario": name,
        "passed": True,
        "fault": fault,
        "generation": generation,
        "deployment_plan_hash": plan_hash,
        "advertised_endpoint": "http://head:4001/v1",
        "ready_revision": 2,
        "terminal_revision": 3,
        "terminal_state": "FAILED",
        "terminal_reason_code": "FIRST_CAUSE",
        "terminal_detail": "failure",
        "returncode": 1,
        "ready_evidence": {
            "source_rank_receipts": nodes,
            "source_manifest_hash": "d" * 64,
            "receipt_manifest_hash": "e" * 64,
            "receipt_slots": slots,
            "engine_patch_evidence": {
                "compatibility_profile_hash": "b" * 64,
                "engine_core_count": replicas,
                "engine_worker_count": 0,
                "instances": [
                    {"role": "engine_core", "planned_rank": index // 12}
                    for index in range(replicas)
                ],
            },
        },
        "canary": [
            {
                "status_code": 200,
                "model_id": "org/model",
                "response": {
                    "model": "org/model",
                    "object": "text_completion",
                    "choices": [{"index": 0, "text": "ok"}],
                },
            }
        ],
    }
    if name == "normal-drain":
        scenario.update(
            {
                "returncode": 143,
                "terminal_state": "STOPPED",
                "terminal_reason_code": "DRAINED_AND_REAPED",
                "terminal_detail": "drained",
            }
        )
    elif name == "gateway-death":
        scenario["terminal_detail"] = "gateway/haproxy: UNEXPECTED_EXIT"
        scenario["fault_injection"] = {
            "kind": "owned_gateway_death",
            "node": "node-0",
            "pid": 100,
        }
        scenario["gateway_failure"] = {"classification": "process_dead"}
    elif name == "worker-death":
        scenario["terminal_detail"] = f"rank {nodes - 1} component ray worker: exit=1"
        scenario["fault_injection"] = {
            "kind": "owned_ray_worker_death",
            "worker_rank": nodes - 1,
            "target": {
                "rank": nodes - 1,
                "pid": 101,
                "receipt_requirement_id": f"rank{nodes - 1}/ray_worker",
            },
        }
    elif name == "duplicate-gateway-port":
        scenario["ready_revision"] = None
        scenario["terminal_detail"] = "listener bind failed"
        scenario["fault_precondition"] = {
            "kind": "local_gateway_port_holder",
            "stopped": True,
        }
        scenario.pop("ready_evidence")
        scenario.pop("canary")
    elif name == "partial-worker-proxy":
        scenario.update(
            {
                "ready_revision": None,
                "terminal_state": "CANCELLED",
                "terminal_reason_code": "OPERATOR_CANCELLED",
                "returncode": 143,
                "status_history": [{"state": "DEPLOYING"}],
                "membership_evidence": {"node_count": nodes},
                "fault_precondition": {
                    "kind": "remote_worker_proxy_port_holder",
                    "node": f"node-{nodes - 1}",
                },
                "held_node": f"node-{nodes - 1}",
                "cancelled_after_observation": True,
            }
        )
        scenario.pop("ready_evidence")
        scenario.pop("canary")
    if name not in {"duplicate-gateway-port", "partial-worker-proxy"}:
        scenario["shutdown_report"] = {
            "schema_version": 2,
            "clean": True,
            "deadline_exhausted": False,
            "errors": [],
            "generation": generation,
            "deployment_plan_hash": plan_hash,
            "observed_terminal_state": scenario["terminal_state"],
        }
    return scenario


def _materialize_scale_campaign(root: Path) -> tuple[dict, dict]:
    harness = _write(root, "scripts/hardening/run_scale.py")
    lifecycle = _write(root, "scripts/hardening/lifecycle.py")
    approval_payload = {
        "schema_version": 1,
        "decision_id": "OWNER-SCALE-64",
        "decision": "APPROVE_QUALIFICATION_TARGET",
        "approver_id": "product-owner",
        "approved_at": "2026-08-10T00:00:00Z",
        "approved_max_nodes": 64,
        "required_ladder": [4, 16, 64],
        "dimensions": dict(adjudicator.SCALE_APPROVAL_DIMENSIONS),
    }
    approval = _json(root, "doc/hardening/decisions/scale-64.json", approval_payload)
    release = root / "release"
    (release / "bootstrap").mkdir(parents=True)
    artifact = _write(root, "release/artifact.json")
    wheel = _write(root, "release/candidate.whl")
    sdist = _write(root, "release/candidate.tar.gz")
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
    deployments = {}
    for nodes, tier in adjudicator.SCALE_TIER_CONTRACT.items():
        gate_id = f"SCALE-{nodes}"
        config = _write(root, f"inputs/{nodes}/config.yaml")
        site = _write(root, f"inputs/{nodes}/site.json")
        replicas = nodes * 12
        receipt_requirements = [
            {"receipt_requirement_id": f"slot-{index}"} for index in range(replicas + nodes + 2)
        ]
        deployment = {
            "deployment_id": gate_id.lower(),
            "deployment_plan_hash": f"{nodes:064x}",
            "site_profile_id": "alcf-aurora",
            "num_nodes": nodes,
            "num_gpus_per_node": 12,
            "vendor": "xpu",
            "engine": "vllm",
            "validation_mode": True,
            "site_profile_hash": candidate["site_profile_hash"],
            "compatibility_profile_hash": candidate["compatibility_profile_hash"],
            "manifest_hash": candidate["compatibility_manifest_hash"],
            "gateway": {"kind": "haproxy"},
            "exposure": {"mode": "PROXIED_INTERNAL"},
            "scale_envelope": {
                "site_id": "alcf-aurora",
                "scheduler_type": "pbs",
                "vendor": "xpu",
                "accelerator": "pvc",
                "engine": "vllm",
                "gateway_kind": "haproxy",
                "exposure_mode": "PROXIED_INTERNAL",
                "request_mode": "completion",
                "streaming_mode": "non_streaming",
                "qualification_target_nodes": 64,
                "validation_mode": True,
            },
            "runtime": {"null_compute": False},
            "receipt_requirements": receipt_requirements,
            "models": [
                {
                    "model_id": "org/model",
                    "tensor_parallel_size": 1,
                    "pipeline_parallel_size": 1,
                    "num_replicas": replicas,
                    "replicas": [
                        {"planned_ranks": [rank]} for rank in range(nodes) for _tile in range(12)
                    ],
                }
            ],
        }
        deployment_path = _json(root, f"inputs/{nodes}/deployment.json", deployment)
        row = {
            "gate_id": gate_id,
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
            "config_path": _relative(root, config),
            "config_sha256": _sha(config),
            "deployment_plan_path": _relative(root, deployment_path),
            "deployment_plan_sha256": _sha(deployment_path),
            "site_profile_path": _relative(root, site),
            "site_profile_sha256": _sha(site),
            "clean_state_reset_method": "fresh generation and exact cleanup",
            "retry_reason_policy": "no retry",
            "expected_observations": adjudicator._scale_expected_observations(
                tier["scenario_profile"]
            ),
        }
        gates.append(row)
        deployments[nodes] = deployment
    plan = {
        "schema_version": 3,
        "created_at": "2026-08-10T00:00:00Z",
        "candidate": candidate,
        "harness": {"path": _relative(root, harness), "sha256": _sha(harness)},
        "support": {
            "lifecycle": {"path": _relative(root, lifecycle), "sha256": _sha(lifecycle)},
        },
        "scope_approval": {"path": _relative(root, approval), "sha256": _sha(approval)},
        "gates": gates,
    }
    plan_path = _json(root, "campaign/experiment.json", plan)
    plan_sha = _sha(plan_path)
    results = []
    for row in gates:
        nodes = row["logical_nodes"]
        deployment = deployments[nodes]
        replicas = nodes * 12
        names = adjudicator.SCALE_SCENARIOS[row["scenario_profile"]]
        scenarios = [
            _scenario(
                name,
                nodes=nodes,
                replicas=replicas,
                plan_hash=deployment["deployment_plan_hash"],
                slots=sorted(
                    item["receipt_requirement_id"] for item in deployment["receipt_requirements"]
                ),
            )
            for name in names
        ]
        cleanup = {scenario["scenario"]: _cleanup(root, row, scenario) for scenario in scenarios}
        result = {
            "schema_version": 1,
            "passed": True,
            "candidate": candidate,
            "declared_gate": row,
            "gate_id": row["gate_id"],
            "experiment_plan_path": str(plan_path.resolve()),
            "experiment_plan_sha256": plan_sha,
            "harness": str(harness.resolve()),
            "harness_sha256": _sha(harness),
            "lifecycle_support": str(lifecycle.resolve()),
            "lifecycle_support_sha256": _sha(lifecycle),
            "scope_approval": str(approval.resolve()),
            "scope_approval_sha256": _sha(approval),
            "config_path": str((root / row["config_path"]).resolve()),
            "config_sha256": row["config_sha256"],
            "deployment_plan_path": str((root / row["deployment_plan_path"]).resolve()),
            "deployment_plan_hash": deployment["deployment_plan_hash"],
            "site_profile_path": str((root / row["site_profile_path"]).resolve()),
            "wheel": str(wheel.resolve()),
            "wheel_sha256": _sha(wheel),
            "site_profile_hash": candidate["site_profile_hash"],
            "pbs_job_id": f"job-{nodes}",
            "attempt": 1,
            "attempt_limit": 1,
            "engine_mode": "real",
            "lane": "FINAL",
            "logical_nodes": nodes,
            "physical_allocation_nodes": nodes,
            "queue": row["queue"],
            "ready_timeout_s": 1200.0,
            "scenario_profile": row["scenario_profile"],
            "density": {
                "num_nodes": nodes,
                "num_gpus_per_node": 12,
                "replicas": replicas,
                "replicas_per_rank": 12,
                "serve_applications": replicas + nodes,
                "receipt_requirements": len(deployment["receipt_requirements"]),
            },
            "scenarios": scenarios,
        }
        result_path = _json(root, f"{row['output_path']}/result.json", result)
        results.append(
            {
                "gate_id": row["gate_id"],
                "path": _relative(root, result_path),
                "sha256": _sha(result_path),
                "scenarios": names,
                "cleanup": cleanup,
            }
        )
    review = {
        "candidate": candidate,
        "campaigns": {
            "scale": {
                "plan_path": _relative(root, plan_path),
                "plan_sha256": plan_sha,
                "results": results,
            }
        },
    }
    return review, plan


def test_scale_campaign_accepts_exact_candidate_bound_4_16_64_evidence(tmp_path, monkeypatch):
    review, _plan = _materialize_scale_campaign(tmp_path)
    monkeypatch.setattr(adjudicator, "ROOT", tmp_path)
    adjudicator._verify_scale_campaign(review)


def test_scale_campaign_rejects_candidate_identity_drift(tmp_path, monkeypatch):
    review, _plan = _materialize_scale_campaign(tmp_path)
    monkeypatch.setattr(adjudicator, "ROOT", tmp_path)
    changed = deepcopy(review)
    changed["candidate"]["compatibility_profile_hash"] = "f" * 64
    with pytest.raises(RuntimeError, match="schema or candidate"):
        adjudicator._verify_scale_campaign(changed)


def test_scale_campaign_rejects_incomplete_boundary_fault_evidence(tmp_path, monkeypatch):
    review, _plan = _materialize_scale_campaign(tmp_path)
    monkeypatch.setattr(adjudicator, "ROOT", tmp_path)
    declaration = review["campaigns"]["scale"]["results"][-1]
    result_path = tmp_path / declaration["path"]
    result = json.loads(result_path.read_text())
    result["scenarios"][1]["terminal_detail"] = "some unrelated failure"
    _json(tmp_path, declaration["path"], result)
    declaration["sha256"] = _sha(result_path)
    with pytest.raises(RuntimeError, match="worker fault semantics"):
        adjudicator._verify_scale_campaign(review)


def test_scale_campaign_rejects_receipt_identity_substitution(tmp_path, monkeypatch):
    review, _plan = _materialize_scale_campaign(tmp_path)
    monkeypatch.setattr(adjudicator, "ROOT", tmp_path)
    declaration = review["campaigns"]["scale"]["results"][0]
    result_path = tmp_path / declaration["path"]
    result = json.loads(result_path.read_text())
    slots = result["scenarios"][0]["ready_evidence"]["receipt_slots"]
    slots[-1] = slots[0]
    _json(tmp_path, declaration["path"], result)
    declaration["sha256"] = _sha(result_path)

    with pytest.raises(RuntimeError, match="READY evidence is incomplete"):
        adjudicator._verify_scale_campaign(review)


def test_scale_campaign_rejects_worker_target_drift(tmp_path, monkeypatch):
    review, _plan = _materialize_scale_campaign(tmp_path)
    monkeypatch.setattr(adjudicator, "ROOT", tmp_path)
    declaration = review["campaigns"]["scale"]["results"][-1]
    result_path = tmp_path / declaration["path"]
    result = json.loads(result_path.read_text())
    result["scenarios"][1]["fault_injection"]["worker_rank"] = 1
    _json(tmp_path, declaration["path"], result)
    declaration["sha256"] = _sha(result_path)

    with pytest.raises(RuntimeError, match="worker fault semantics"):
        adjudicator._verify_scale_campaign(review)


def test_scale_campaign_rejects_platform_dimension_drift(tmp_path, monkeypatch):
    review, plan = _materialize_scale_campaign(tmp_path)
    monkeypatch.setattr(adjudicator, "ROOT", tmp_path)
    row = plan["gates"][0]
    deployment_path = tmp_path / row["deployment_plan_path"]
    deployment = json.loads(deployment_path.read_text())
    deployment["vendor"] = "cuda"
    _json(tmp_path, row["deployment_plan_path"], deployment)
    row["deployment_plan_sha256"] = _sha(deployment_path)
    plan_path = tmp_path / review["campaigns"]["scale"]["plan_path"]
    _json(tmp_path, review["campaigns"]["scale"]["plan_path"], plan)
    review["campaigns"]["scale"]["plan_sha256"] = _sha(plan_path)

    with pytest.raises(RuntimeError, match="identity or density drifted"):
        adjudicator._verify_scale_campaign(review)
