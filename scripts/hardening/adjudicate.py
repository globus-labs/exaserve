#!/usr/bin/env python3
"""Fail-closed, candidate-bound adjudication of the hardening findings ledger.

The review manifest is data, not authority: this verifier independently checks
the immutable package, every declared campaign input, every result receipt,
fault semantics, and exact-generation cleanup before it can update a finding.
No candidate name or evidence hash is compiled into this program.
"""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
import sys
from typing import Any
import zipfile

import yaml


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

LEDGER = ROOT / "doc/hardening/FINDINGS.yaml"
DEFAULT_REVIEW = ROOT / "artifacts/hardening/final42-candidate-review.json"

CANONICAL_FIELDS = (
    "id",
    "source",
    "severity",
    "invariant",
    "primary_work_package",
    "affected_regions",
    "acceptance_tests",
    "status",
    "decision",
    "evidence",
    "fallback",
    "residual_risk",
    "support_impact",
    "owner",
    "approval",
    "revisit_condition",
)

REVIEW_FIELDS = {
    "schema_version",
    "candidate_label",
    "adjudicated_on",
    "scope_state",
    "candidate",
    "packaged_gate",
    "campaigns",
    "dispositions",
}
CANDIDATE_FIELDS = {
    "release_path",
    "artifact_manifest_path",
    "artifact_manifest_sha256",
    "wheel_path",
    "wheel_sha256",
    "sdist_path",
    "sdist_sha256",
    "bootstrap_path",
    "site_profile_hash",
    "compatibility_profile_hash",
    "compatibility_manifest_hash",
}

EXPECTED_IN_PROGRESS = {
    "PR-033",
    "KI-A1",
    "KI-A3",
    "KI-A7",
    "KI-B2",
    "KI-D2",
    "TD-COPPER",
    "IMP-B16",
}
EXPECTED_EXTERNAL = {"TD-SLURM-AMD"}
EXPECTED_OUT_OF_SCOPE = {"TD-CACHE", "TD-STAGE-PAR", "TD-CPP-CLIENT"}

IN_PROGRESS_REASONS = {
    "PR-033": "Missing an owner-approved release envelope and qualification above two nodes.",
    "KI-A1": "Missing an approved Envoy/streaming/256-node scope and final-architecture reproduction.",
    "KI-A3": "Missing an approved scale ceiling and measurements at its boundary tier.",
    "KI-A7": "Missing residual-import measurements at the record's 128/256-node scale.",
    "KI-B2": "Missing an approved streaming/256-node scope and captured rare proxy-death record.",
    "KI-D2": "Missing MPI distribution and activation-receipt evidence at the approved scale boundary.",
    "TD-COPPER": "Missing residual shared-filesystem import measurements at the approved boundary.",
    "IMP-B16": "The plan family is proven, but the owner-approved measured scale envelope is absent.",
}
EXTERNAL_REASONS = {
    "TD-SLURM-AMD": "Native Slurm plus CUDA/ROCm qualification requires an offsite allocation unavailable on Aurora."
}
OUT_OF_SCOPE_REASONS = {
    "TD-CACHE": "Proxy-layer request caching is optional and is not an advertised production capability.",
    "TD-STAGE-PAR": "Concurrent multi-model download is an optimization; transactional serial staging is production-correct.",
    "TD-CPP-CLIENT": "The C++ client is a paper-only experiment and not the production replay client.",
}

# Legacy records without a catalog assignment receive the gate proving their
# invariant. Existing non-empty assignments always win.
ACCEPTANCE_DEFAULTS = {
    "PR-014": ["AC-SUP-01"],
    "PR-015": ["AC-PLAN-01"],
    "PR-016": ["AC-PLAN-01"],
    "PR-017": ["AC-PLAN-01"],
    "PR-018": ["AC-PLAN-01"],
    "PR-019": ["AC-STAT-01"],
    "PR-027": ["AC-PLAN-01"],
    "PR-030": ["AC-OBS-01"],
    "PR-034": ["AC-PLAN-01"],
    "PR-035": ["AC-PLAN-01"],
    "KI-B3": ["AC-OBS-01"],
    "KI-C2": ["AC-PLAN-01"],
    "KI-C3": ["AC-DIST-01"],
    "KI-C4": ["AC-STAT-01"],
    "KI-C6": ["AC-OBS-01"],
    "KI-E1": ["AC-PROXY-01"],
    "TD-TESTS": ["AC-TST-01"],
    "TD-NULLTOK": ["AC-PLAN-01"],
    "TD-PORTS": ["AC-PROXY-01"],
    "TD-STAGE-PAR": ["AC-DIST-01"],
    "TD-CHATTPL": ["AC-COMP-01", "AC-OBS-01"],
    "TD-REQID": ["AC-OBS-01"],
    "TD-METRICS": ["AC-OBS-01"],
    "TD-CONSTS": ["AC-PLAN-01"],
    "TD-SITECUST": ["AC-COMP-01"],
    "TD-PP-MULTI": ["AC-PP-01"],
    "TD-COPPER": ["AC-DIST-01"],
    "TD-PROXYPROF": ["AC-INST-01"],
    "TD-SGLANG": ["AC-COMP-01"],
    "TD-SLURM-AMD": ["AC-COMP-01"],
    "TD-PSIJ": ["AC-PLAN-01"],
    "TD-DOCS-REGEN": ["AC-TST-01"],
    "F-08": ["AC-OBS-01"],
    "F-09": ["AC-PROXY-01"],
    "F-10": ["AC-PROXY-01"],
    "F-11": ["AC-PLAN-01"],
}

STATIC_EVIDENCE = {
    "AC-TST-01": [
        ".github/workflows/ci.yml",
        "tests/test_packaging.py",
        "tests/test_findings_validator.py",
        "tests/test_adjudicator_evidence.py",
    ],
    "AC-SUP-01": [
        "tests/test_supervisor.py",
        "tests/test_composition_root.py",
        "tests/test_rank_topology.py",
        "tests/test_control_channel_wiring.py",
        "tests/test_finite_process.py",
        "tests/test_process_ownership.py",
        "tests/test_engine_shutdown.py",
    ],
    "AC-CTL-01": [
        "tests/test_control_channel.py",
        "tests/test_control_channel_wiring.py",
        "tests/test_session_protocol.py",
        "tests/test_status_store.py",
    ],
    "AC-COMP-01": [
        "tests/test_generated_overlay.py",
        "tests/test_compat_activation.py",
        "tests/test_receipt_chain.py",
        "doc/hardening/decisions/ADR-003-compatibility-delivery.md",
    ],
    "AC-RDY-01": [
        "tests/test_plan_readiness.py",
        "tests/test_readiness_coordinator.py",
        "tests/test_serve_readiness.py",
    ],
    "AC-RDY-02": [
        "tests/test_control_channel.py",
        "tests/test_control_channel_wiring.py",
        "tests/test_session_protocol.py",
        "tests/test_status_store.py",
        "tests/test_plan_readiness.py",
        "tests/test_readiness_coordinator.py",
        "tests/test_serve_readiness.py",
        "tests/test_bounded_collectors.py",
    ],
    "AC-PLAN-01": [
        "tests/test_plan_contracts.py",
        "tests/test_plan_io.py",
        "tests/test_config_strictness.py",
        "eval/tests/test_shared_plan_identity.py",
        "eval/tests/test_eval_control_plane.py",
    ],
    "AC-TEL-01": [
        "tests/test_telemetry_contract.py",
        "tests/test_bounded_collectors.py",
        "tests/test_composition_root.py",
    ],
    "AC-STAT-01": [
        "tests/test_result_manifest.py",
        "tests/test_go_result_contract.py",
        "tests/test_status_store.py",
        "eval/tests/test_manifest_contract.py",
        "eval/tests/test_process_supervision.py",
    ],
    "AC-OBS-01": [
        "tests/test_observability.py",
        "tests/test_request_validation.py",
        "clientlab/tests/test_runtime_smoke.py",
        "clientlab/tests/test_spec_and_analysis.py",
    ],
    "AC-DIST-01": [
        "tests/test_model_staging.py",
        "tests/test_source_staging.py",
        "tests/test_pp_staging.py",
    ],
    "AC-PP-01": [
        "tests/test_pp_staging.py",
        "tests/test_plan_contracts.py",
        "tests/test_vllm_support.py",
    ],
    "AC-PROXY-01": [
        "tests/test_proxy_qualification_harness.py",
        "tests/test_gateway_evidence.py",
        "tests/test_haproxy_proxy.py",
        "tests/test_exposure_model.py",
        "tests/test_port_ownership.py",
    ],
    "AC-INST-01": [
        "tests/test_audit_regressions.py",
        "tests/test_generated_overlay.py",
        "tests/test_no_readiness_marker_consumers.py",
    ],
    "AC-SCALE-01": [
        "doc/hardening/decisions/ADR-000-production-envelope.md",
        "doc/hardening/COMPATIBILITY_MATRIX.md",
    ],
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8", errors="strict"))
    if not isinstance(value, dict):
        raise RuntimeError(f"expected a JSON object: {path}")
    return value


def _repository_path(relative: object, field: str) -> Path:
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise RuntimeError(f"{field} must be a non-empty repository-relative path")
    path = (ROOT / relative).resolve()
    try:
        path.relative_to(ROOT.resolve())
    except ValueError as exc:
        raise RuntimeError(f"{field} escapes the repository: {relative}") from exc
    return path


def _hashed_path(row: dict[str, Any], path_field: str, hash_field: str, prefix: str) -> Path:
    path = _repository_path(row.get(path_field), f"{prefix}.{path_field}")
    expected = row.get(hash_field)
    if not isinstance(expected, str) or len(expected) != 64 or _sha256(path) != expected:
        raise RuntimeError(f"{prefix} {path_field} bytes changed")
    return path


def _strict_positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise RuntimeError(f"{field} must be a positive integer")
    return value


def _finite(value: object, field: str, minimum: float = 0.0) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) < minimum
    ):
        raise RuntimeError(f"{field} must be finite and >= {minimum}")
    return float(value)


def _verify_review_shape(review: dict[str, Any]) -> None:
    if set(review) != REVIEW_FIELDS or review.get("schema_version") != 1:
        raise RuntimeError("candidate review has the wrong exact schema")
    if (
        not isinstance(review.get("candidate_label"), str)
        or not review["candidate_label"]
        or not isinstance(review.get("adjudicated_on"), str)
        or review.get("scope_state") != "TECHNICAL_PASS_SCOPE_PENDING"
    ):
        raise RuntimeError("candidate review identity or scope state is invalid")
    candidate = review.get("candidate")
    if not isinstance(candidate, dict) or set(candidate) != CANDIDATE_FIELDS:
        raise RuntimeError("candidate identity has the wrong exact schema")
    campaigns = review.get("campaigns")
    if not isinstance(campaigns, dict) or set(campaigns) != {"lifecycle", "proxy", "supervisor"}:
        raise RuntimeError(
            "candidate review must declare lifecycle, proxy, and supervisor campaigns"
        )
    dispositions = review.get("dispositions")
    if not isinstance(dispositions, dict) or set(dispositions) != {
        "in_progress",
        "external_blocker",
        "out_of_production_scope",
    }:
        raise RuntimeError("candidate review dispositions have the wrong exact schema")
    expected = {
        "in_progress": EXPECTED_IN_PROGRESS,
        "external_blocker": EXPECTED_EXTERNAL,
        "out_of_production_scope": EXPECTED_OUT_OF_SCOPE,
    }
    for name, required in expected.items():
        values = dispositions.get(name)
        if not isinstance(values, list) or any(not isinstance(item, str) for item in values):
            raise RuntimeError(f"candidate review disposition {name} is not a string list")
        if len(values) != len(set(values)) or set(values) != required:
            raise RuntimeError(f"candidate review disposition {name} drifted from release policy")


def _verify_candidate(review: dict[str, Any]) -> None:
    candidate = review["candidate"]
    manifest_path = _hashed_path(
        candidate, "artifact_manifest_path", "artifact_manifest_sha256", "candidate"
    )
    wheel_path = _hashed_path(candidate, "wheel_path", "wheel_sha256", "candidate")
    sdist_path = _hashed_path(candidate, "sdist_path", "sdist_sha256", "candidate")
    release = _repository_path(candidate.get("release_path"), "candidate.release_path")
    bootstrap = _repository_path(candidate.get("bootstrap_path"), "candidate.bootstrap_path")
    if not release.is_dir() or not bootstrap.is_dir():
        raise RuntimeError("candidate release or installed bootstrap is absent")
    if (
        manifest_path.parent != release
        or wheel_path.parent != release
        or sdist_path.parent != release
    ):
        raise RuntimeError("candidate artifacts are not all inside the declared release directory")
    manifest = _load_json(manifest_path)
    if (
        manifest.get("wheel") != wheel_path.name
        or manifest.get("wheel_sha256") != candidate["wheel_sha256"]
        or manifest.get("sdist") != sdist_path.name
        or manifest.get("sdist_sha256") != candidate["sdist_sha256"]
    ):
        raise RuntimeError("artifact manifest does not bind the declared wheel and sdist")
    package_members = manifest.get("package_members")
    if (
        not isinstance(package_members, list)
        or not package_members
        or any(
            not isinstance(item, str) or not item.startswith("exaserve/")
            for item in package_members
        )
        or len(package_members) != len(set(package_members))
    ):
        raise RuntimeError("artifact manifest has an invalid package member set")
    with zipfile.ZipFile(wheel_path) as archive:
        names = [item.filename for item in archive.infolist() if not item.is_dir()]
        if len(names) != len(set(names)):
            raise RuntimeError("candidate wheel contains duplicate members")
        wheel_package_members = sorted(name for name in names if name.startswith("exaserve/"))
        if wheel_package_members != sorted(package_members):
            raise RuntimeError("candidate wheel package members disagree with its manifest")
        for relative in package_members:
            installed = bootstrap / relative
            if installed.is_symlink() or not installed.is_file():
                raise RuntimeError(f"installed bootstrap member is absent or unsafe: {relative}")
            if installed.read_bytes() != archive.read(relative):
                raise RuntimeError(f"installed bootstrap member differs from the wheel: {relative}")
    expected = set(package_members)
    for installed in (bootstrap / "exaserve").rglob("*"):
        if installed.is_dir():
            continue
        relative = installed.relative_to(bootstrap).as_posix()
        is_cache = installed.parent.name == "__pycache__" and installed.suffix == ".pyc"
        if installed.is_symlink() or (relative not in expected and not is_cache):
            raise RuntimeError(f"installed bootstrap contains an unexpected member: {relative}")


def _verify_packaged_gate(review: dict[str, Any]) -> None:
    gate = review.get("packaged_gate")
    required = {
        "receipt_path",
        "receipt_sha256",
        "pytest_log_path",
        "pytest_log_sha256",
        "pytest_summary",
        "mypy_log_path",
        "mypy_log_sha256",
        "mypy_summary",
    }
    if not isinstance(gate, dict) or set(gate) != required:
        raise RuntimeError("packaged gate has the wrong exact schema")
    receipt_path = _hashed_path(gate, "receipt_path", "receipt_sha256", "packaged_gate")
    pytest_path = _hashed_path(gate, "pytest_log_path", "pytest_log_sha256", "packaged_gate")
    mypy_path = _hashed_path(gate, "mypy_log_path", "mypy_log_sha256", "packaged_gate")
    receipt = _load_json(receipt_path)
    candidate = review["candidate"]
    if (
        receipt.get("schema_version") != 1
        or receipt.get("status") != "PASS"
        or receipt.get("wheel_sha256") != candidate["wheel_sha256"]
        or Path(str(receipt.get("wheel", ""))).resolve()
        != _repository_path(candidate["wheel_path"], "candidate.wheel_path")
        or not isinstance(receipt.get("pbs_job_id"), str)
        or not receipt["pbs_job_id"]
    ):
        raise RuntimeError("packaged gate is failed, malformed, or belongs to another wheel")
    pytest_summary = gate.get("pytest_summary")
    mypy_summary = gate.get("mypy_summary")
    if not isinstance(pytest_summary, str) or pytest_summary not in pytest_path.read_text(
        encoding="utf-8", errors="strict"
    ):
        raise RuntimeError("packaged pytest log lacks its declared accepted verdict")
    if not isinstance(mypy_summary, str) or mypy_summary not in mypy_path.read_text(
        encoding="utf-8", errors="strict"
    ):
        raise RuntimeError("packaged mypy log lacks its declared accepted verdict")


def _verify_shutdown(scenario: dict[str, Any], relative: str, *, failed: bool) -> None:
    shutdown = scenario.get("shutdown_report")
    expected_state = "FAILED" if failed else scenario.get("terminal_state")
    if (
        not isinstance(shutdown, dict)
        or shutdown.get("schema_version") != 2
        or shutdown.get("clean") is not True
        or shutdown.get("deadline_exhausted") is not False
        or shutdown.get("errors") != []
        or shutdown.get("generation") != scenario.get("generation")
        or shutdown.get("deployment_plan_hash") != scenario.get("deployment_plan_hash")
        or shutdown.get("observed_terminal_state") != expected_state
    ):
        raise RuntimeError(f"shutdown evidence is incomplete: {relative}")


def _verify_exact_generation_cleanup(
    declaration: object,
    *,
    expected_path: str,
    deployment_id: str,
    generation: object,
    deployment_plan_hash: object,
    nodes: int,
    relative: str,
) -> None:
    if not isinstance(declaration, dict) or set(declaration) != {"path", "sha256"}:
        raise RuntimeError(f"cleanup declaration has the wrong exact schema: {relative}")
    if declaration.get("path") != expected_path:
        raise RuntimeError(f"cleanup declaration escapes its scenario output: {relative}")
    cleanup_path = _hashed_path(declaration, "path", "sha256", "cleanup")
    cleanup = _load_json(cleanup_path)
    reports = cleanup.get("reports")
    if cleanup.get("schema_version") != 1 or not isinstance(reports, list) or len(reports) != nodes:
        raise RuntimeError(f"cleanup evidence has the wrong shape: {relative}")
    hostnames: set[str] = set()
    for report in reports:
        if (
            not isinstance(report, dict)
            or report.get("schema_version") != 1
            or report.get("deployment_id") != deployment_id
            or report.get("generation") != generation
            or report.get("deployment_plan_hash") != deployment_plan_hash
            or report.get("matched") != []
            or report.get("signals") != []
            or report.get("survivors") != []
            or not isinstance(report.get("hostname"), str)
            or not report["hostname"]
        ):
            raise RuntimeError(f"exact-generation cleanup is incomplete: {relative}")
        hostnames.add(report["hostname"])
    if len(hostnames) != nodes:
        raise RuntimeError(f"cleanup does not cover every declared node: {relative}")


def _verify_lifecycle_result(
    review: dict[str, Any],
    plan_path: Path,
    plan_hash: str,
    harness: dict[str, Any],
    row: dict[str, Any],
    declaration: dict[str, Any],
) -> None:
    if not isinstance(declaration, dict) or set(declaration) != {
        "gate_id",
        "path",
        "sha256",
        "scenarios",
        "cleanup",
    }:
        raise RuntimeError("lifecycle result declaration has the wrong exact schema")
    relative = declaration.get("path")
    result_path = _hashed_path(declaration, "path", "sha256", "lifecycle.result")
    if relative != f"{row.get('output_path')}/result.json":
        raise RuntimeError("lifecycle result is outside its declared gate output")
    result = _load_json(result_path)
    candidate = review["candidate"]
    if result.get("declared_gate") != row:
        raise RuntimeError(f"lifecycle result does not embed its exact declared gate: {relative}")
    if (
        result.get("schema_version") != 1
        or result.get("passed") is not True
        or result.get("gate_id") != row.get("gate_id")
        or declaration.get("gate_id") != row.get("gate_id")
        or result.get("experiment_plan_path") != str(plan_path)
        or result.get("experiment_plan_sha256") != plan_hash
        or result.get("harness") != str(_repository_path(harness["path"], "harness.path"))
        or result.get("harness_sha256") != harness["sha256"]
        or result.get("wheel")
        != str(_repository_path(candidate["wheel_path"], "candidate.wheel_path"))
        or result.get("wheel_sha256") != candidate["wheel_sha256"]
        or result.get("site_profile_hash") != candidate["site_profile_hash"]
        or not isinstance(result.get("pbs_job_id"), str)
        or not result["pbs_job_id"]
    ):
        raise RuntimeError(f"lifecycle result identity or verdict is invalid: {relative}")
    mirrored = (
        "attempt",
        "attempt_limit",
        "engine_mode",
        "gate_id",
        "lane",
        "logical_nodes",
        "physical_allocation_nodes",
        "queue",
        "ready_timeout_s",
        "scenario_profile",
    )
    if any(result.get(field) != row.get(field) for field in mirrored):
        raise RuntimeError(f"lifecycle result drifted from its declared gate: {relative}")
    scenarios = result.get("scenarios")
    expected = declaration.get("scenarios")
    if (
        not isinstance(expected, list)
        or not isinstance(scenarios, list)
        or any(not isinstance(item, dict) for item in scenarios)
        or [item.get("scenario") for item in scenarios] != expected
        or any(item.get("passed") is not True for item in scenarios)
    ):
        raise RuntimeError(f"lifecycle scenario evidence is incomplete: {relative}")
    nodes = _strict_positive_int(row.get("logical_nodes"), "gate.logical_nodes")
    cleanup_declarations = declaration.get("cleanup")
    if not isinstance(cleanup_declarations, dict) or set(cleanup_declarations) != set(expected):
        raise RuntimeError(f"lifecycle cleanup declarations are incomplete: {relative}")
    for scenario in scenarios:
        name = scenario["scenario"]
        if name not in {"duplicate-gateway-port", "partial-worker-proxy"}:
            ready = scenario.get("ready_evidence")
            if (
                not isinstance(ready, dict)
                or ready.get("source_rank_receipts") != nodes
                or not isinstance(ready.get("source_manifest_hash"), str)
                or not ready["source_manifest_hash"]
            ):
                raise RuntimeError(f"lifecycle run lacks exact per-rank READY evidence: {relative}")
        if name == "normal-drain":
            if (
                scenario.get("returncode") != 143
                or scenario.get("terminal_state") != "STOPPED"
                or scenario.get("terminal_reason_code") != "DRAINED_AND_REAPED"
            ):
                raise RuntimeError(f"normal drain semantics are invalid: {relative}")
            _verify_shutdown(scenario, relative, failed=False)
        elif name == "gateway-death":
            if (
                scenario.get("returncode") in {0, 143}
                or scenario.get("terminal_state") != "FAILED"
                or scenario.get("terminal_reason_code") != "FIRST_CAUSE"
                or "gateway/haproxy: UNEXPECTED_EXIT" not in str(scenario.get("terminal_detail"))
            ):
                raise RuntimeError(f"gateway fault semantics are invalid: {relative}")
            _verify_shutdown(scenario, relative, failed=True)
        elif name == "worker-death":
            if (
                scenario.get("returncode") in {0, 143}
                or scenario.get("terminal_state") != "FAILED"
                or "rank 1 component ray: exit=1" not in str(scenario.get("terminal_detail"))
            ):
                raise RuntimeError(f"worker fault semantics are invalid: {relative}")
            _verify_shutdown(scenario, relative, failed=True)
        elif name == "duplicate-gateway-port":
            if (
                scenario.get("ready_revision") is not None
                or scenario.get("terminal_state") != "FAILED"
            ):
                raise RuntimeError(f"port collision did not fail before READY: {relative}")
        elif name == "partial-worker-proxy":
            history = scenario.get("status_history")
            if (
                scenario.get("ready_revision") is not None
                or not isinstance(history, list)
                or any(item.get("state") == "READY" for item in history if isinstance(item, dict))
            ):
                raise RuntimeError(
                    f"partial proxy observation incorrectly reached READY: {relative}"
                )
        _verify_exact_generation_cleanup(
            cleanup_declarations[name],
            expected_path=(f"{row.get('output_path')}/{name}/exact_generation_cleanup.json"),
            deployment_id=str(row.get("gate_id")).lower(),
            generation=scenario.get("generation"),
            deployment_plan_hash=scenario.get("deployment_plan_hash"),
            nodes=nodes,
            relative=f"{relative}:{name}",
        )
    if (row.get("logical_nodes"), row.get("engine_mode")) == (2, "real"):
        for scenario in scenarios:
            evidence = scenario["ready_evidence"].get("engine_patch_evidence")
            instances = evidence.get("instances") if isinstance(evidence, dict) else None
            worker_nodes = {
                item.get("node_id")
                for item in instances or []
                if isinstance(item, dict) and item.get("role") == "engine_worker"
            }
            if (
                not isinstance(evidence, dict)
                or evidence.get("engine_core_count") != 1
                or evidence.get("engine_worker_count") != 2
                or len(worker_nodes) != 2
            ):
                raise RuntimeError(f"two-node real run lacks exact PP evidence: {relative}")


def _verify_lifecycle_campaign(review: dict[str, Any]) -> None:
    campaign = review["campaigns"]["lifecycle"]
    if not isinstance(campaign, dict) or set(campaign) != {"plan_path", "plan_sha256", "results"}:
        raise RuntimeError("lifecycle campaign has the wrong exact schema")
    plan_path = _hashed_path(campaign, "plan_path", "plan_sha256", "lifecycle")
    plan = _load_json(plan_path)
    candidate = review["candidate"]
    harness = plan.get("harness")
    if plan.get("schema_version") != 2 or plan.get("candidate") != candidate:
        raise RuntimeError("lifecycle plan has the wrong schema or candidate")
    if not isinstance(harness, dict) or set(harness) != {"path", "sha256"}:
        raise RuntimeError("lifecycle plan has an invalid harness declaration")
    _hashed_path(harness, "path", "sha256", "lifecycle.harness")
    gates = plan.get("gates")
    results = campaign.get("results")
    if (
        not isinstance(gates, list)
        or not isinstance(results, list)
        or len(gates) != 4
        or len(results) != 4
        or any(not isinstance(item, dict) for item in gates + results)
    ):
        raise RuntimeError("lifecycle campaign must contain the exact four-cell matrix")
    result_by_gate = {item.get("gate_id"): item for item in results}
    if len(result_by_gate) != 4:
        raise RuntimeError("lifecycle result declarations contain duplicate gate IDs")
    seen_cells: set[tuple[object, object]] = set()
    for row in gates:
        cell = (row.get("logical_nodes"), row.get("engine_mode"))
        if (
            cell not in {(1, "null"), (1, "real"), (2, "null"), (2, "real")}
            or cell in seen_cells
            or row.get("physical_allocation_nodes") != row.get("logical_nodes")
            or row.get("attempt") != 1
            or row.get("attempt_limit") != 1
            or row.get("lane") != "FINAL"
            or row.get("acquisition_source") != "subjob"
        ):
            raise RuntimeError(f"invalid lifecycle gate declaration: {row!r}")
        for path_field, hash_field in (
            ("config_path", "config_sha256"),
            ("deployment_plan_path", "deployment_plan_sha256"),
            ("site_profile_path", "site_profile_sha256"),
        ):
            _hashed_path(row, path_field, hash_field, f"lifecycle.{row.get('gate_id')}")
        deployment = _load_json(
            _repository_path(row["deployment_plan_path"], "gate.deployment_plan_path")
        )
        if (
            deployment.get("deployment_id") != str(row.get("gate_id")).lower()
            or deployment.get("num_nodes") != row.get("logical_nodes")
            or deployment.get("site_profile_hash") != candidate["site_profile_hash"]
            or deployment.get("compatibility_profile_hash")
            != candidate["compatibility_profile_hash"]
            or deployment.get("manifest_hash") != candidate["compatibility_manifest_hash"]
        ):
            raise RuntimeError(f"lifecycle deployment identity drifted: {row.get('gate_id')}")
        declaration = result_by_gate.pop(row.get("gate_id"), None)
        if not isinstance(declaration, dict):
            raise RuntimeError(f"lifecycle gate has no exact result: {row.get('gate_id')}")
        _verify_lifecycle_result(
            review, plan_path, campaign["plan_sha256"], harness, row, declaration
        )
        seen_cells.add(cell)
    if result_by_gate:
        raise RuntimeError("lifecycle review declares results not present in its plan")


def _verify_proxy_cleanup(
    row: dict[str, Any], result: dict[str, Any], declaration: dict[str, Any], relative: str
) -> None:
    execution = result.get("execution")
    if not isinstance(execution, dict):
        raise RuntimeError(f"proxy cleanup evidence has the wrong shape: {relative}")
    _verify_exact_generation_cleanup(
        declaration.get("cleanup"),
        expected_path=f"{row.get('output_path')}/exact_generation_cleanup.json",
        deployment_id=str(row.get("gate_id")).lower(),
        generation=execution.get("generation"),
        deployment_plan_hash=execution.get("deployment_plan_hash"),
        nodes=1,
        relative=relative,
    )


def _verify_proxy_diagnostics(diagnostics: object, relative: str) -> None:
    if not isinstance(diagnostics, dict) or diagnostics.get("schema_version") != 1:
        raise RuntimeError(f"proxy diagnostics have the wrong schema: {relative}")
    samples = diagnostics.get("samples")
    if (
        not isinstance(samples, list)
        or len(samples) < 2
        or diagnostics.get("sample_count") != len(samples)
    ):
        raise RuntimeError(f"proxy diagnostics lack bounded samples: {relative}")
    process = diagnostics.get("gateway_process")
    if not isinstance(process, dict):
        raise RuntimeError(f"proxy diagnostics lack process identity: {relative}")
    pid = _strict_positive_int(process.get("pid"), "gateway_process.pid")
    start_ticks = _strict_positive_int(
        process.get("process_start_ticks"), "gateway_process.process_start_ticks"
    )
    previous_time = -1.0
    previous_wall_time = -1.0
    previous_cpu = -1
    connection_states = {
        "CLOSE",
        "CLOSE_WAIT",
        "CLOSING",
        "ESTABLISHED",
        "FIN_WAIT1",
        "FIN_WAIT2",
        "LAST_ACK",
        "LISTEN",
        "SYN_RECV",
        "SYN_SENT",
        "TIME_WAIT",
        "TOTAL",
    }
    tcp_fields = {"ActiveOpens", "CurrEstab", "PassiveOpens", "RetransSegs"}
    for index, sample in enumerate(samples):
        if not isinstance(sample, dict):
            raise RuntimeError(f"proxy sample {index} is not an object: {relative}")
        if sample.get("pid") != pid or sample.get("process_start_ticks") != start_ticks:
            raise RuntimeError(f"proxy process identity changed across samples: {relative}")
        observed = _finite(sample.get("observed_monotonic"), f"sample[{index}].time")
        wall_time = _finite(sample.get("observed_at"), f"sample[{index}].observed_at")
        cpu = sample.get("cpu_ticks")
        integers = ("threads", "open_fds", "rss_bytes")
        if (
            isinstance(cpu, bool)
            or not isinstance(cpu, int)
            or cpu < 0
            or any(
                isinstance(sample.get(field), bool)
                or not isinstance(sample.get(field), int)
                or sample[field] < 0
                for field in integers
            )
        ):
            raise RuntimeError(f"proxy sample CPU is invalid: {relative}")
        connections = sample.get("connections")
        tcp = sample.get("tcp")
        if (
            not isinstance(connections, dict)
            or set(connections) != connection_states
            or any(
                isinstance(value, bool) or not isinstance(value, int) or value < 0
                for value in connections.values()
            )
            or connections["TOTAL"]
            != sum(connections[name] for name in connection_states - {"TOTAL"})
            or not isinstance(tcp, dict)
            or set(tcp) != tcp_fields
            or any(
                isinstance(value, bool) or not isinstance(value, int) or value < 0
                for value in tcp.values()
            )
            or not isinstance(sample.get("process_state"), str)
            or not sample["process_state"]
        ):
            raise RuntimeError(f"proxy sample network or process metrics are invalid: {relative}")
        if observed <= previous_time or wall_time <= previous_wall_time or cpu < previous_cpu:
            raise RuntimeError(f"proxy diagnostic samples are not monotonic: {relative}")
        previous_time, previous_wall_time, previous_cpu = observed, wall_time, cpu
    duration = float(samples[-1]["observed_monotonic"]) - float(samples[0]["observed_monotonic"])
    if not math.isclose(
        _finite(diagnostics.get("duration_s"), "diagnostics.duration_s"),
        duration,
        rel_tol=0.0,
        abs_tol=1e-9,
    ):
        raise RuntimeError(f"proxy diagnostic duration does not derive from samples: {relative}")
    peaks = {
        "peak_threads": max(item["threads"] for item in samples),
        "peak_open_fds": max(item["open_fds"] for item in samples),
        "peak_rss_bytes": max(item["rss_bytes"] for item in samples),
        "peak_connections": max(item["connections"]["TOTAL"] for item in samples),
        "peak_established": max(item["connections"]["ESTABLISHED"] for item in samples),
    }
    if any(diagnostics.get(field) != expected for field, expected in peaks.items()):
        raise RuntimeError(f"proxy diagnostic summaries do not derive from samples: {relative}")
    first, last = samples[0], samples[-1]
    clock_ticks = _strict_positive_int(
        diagnostics.get("clock_ticks_per_second"), "diagnostics.clock_ticks_per_second"
    )
    cpu_delta = last["cpu_ticks"] - first["cpu_ticks"]
    derived = {
        "cpu_ticks_delta": cpu_delta,
        "active_opens_delta": last["tcp"]["ActiveOpens"] - first["tcp"]["ActiveOpens"],
        "passive_opens_delta": last["tcp"]["PassiveOpens"] - first["tcp"]["PassiveOpens"],
        "retransmits_delta": last["tcp"]["RetransSegs"] - first["tcp"]["RetransSegs"],
        "process_states": sorted({item["process_state"] for item in samples}),
    }
    if any(diagnostics.get(field) != expected for field, expected in derived.items()):
        raise RuntimeError(f"proxy diagnostic deltas do not derive from samples: {relative}")
    cpu_percent = 100.0 * cpu_delta / clock_ticks / duration
    if not math.isclose(
        _finite(diagnostics.get("cpu_percent"), "diagnostics.cpu_percent"),
        cpu_percent,
        rel_tol=0.0,
        abs_tol=1e-9,
    ):
        raise RuntimeError(f"proxy CPU summary does not derive from samples: {relative}")


def _verify_proxy_result(
    review: dict[str, Any],
    plan_path: Path,
    plan_hash: str,
    plan: dict[str, Any],
    row: dict[str, Any],
    declaration: dict[str, Any],
) -> None:
    if not isinstance(declaration, dict) or set(declaration) != {
        "gate_id",
        "path",
        "sha256",
        "http_no_delay",
        "cleanup",
    }:
        raise RuntimeError("proxy result declaration has the wrong exact schema")
    relative = declaration.get("path")
    result_path = _hashed_path(declaration, "path", "sha256", "proxy.result")
    if relative != f"{row.get('output_path')}/result.json":
        raise RuntimeError("proxy result is outside its declared output")
    result = _load_json(result_path)
    candidate = review["candidate"]
    harness = plan["harness"]
    support = plan["support"]
    if result.get("declared_gate") != row:
        raise RuntimeError(f"proxy result does not embed its exact declared gate: {relative}")
    if (
        result.get("schema_version") != 1
        or result.get("passed") is not True
        or result.get("candidate") != candidate
        or result.get("gate_id") != row.get("gate_id")
        or declaration.get("gate_id") != row.get("gate_id")
        or declaration.get("http_no_delay") is not row.get("http_no_delay")
        or result.get("experiment_plan_path") != str(plan_path)
        or result.get("experiment_plan_sha256") != plan_hash
        or result.get("harness_sha256") != harness["sha256"]
        or result.get("support_sha256") != support["sha256"]
        or result.get("wheel_sha256") != candidate["wheel_sha256"]
        or result.get("site_profile_hash") != candidate["site_profile_hash"]
    ):
        raise RuntimeError(f"proxy result identity or verdict is invalid: {relative}")
    execution = result.get("execution")
    ready = execution.get("ready_evidence") if isinstance(execution, dict) else None
    shutdown = execution.get("shutdown_report") if isinstance(execution, dict) else None
    canary = execution.get("canary") if isinstance(execution, dict) else None
    if (
        not isinstance(execution, dict)
        or execution.get("schema_version") != 1
        or execution.get("passed") is not True
        or execution.get("returncode") != 143
        or execution.get("terminal_state") != "STOPPED"
        or execution.get("terminal_reason_code") != "DRAINED_AND_REAPED"
        or not isinstance(ready, dict)
        or ready.get("source_rank_receipts") != 1
        or not isinstance(canary, list)
        or len(canary) != 1
        or canary[0].get("status_code") != 200
        or not isinstance(shutdown, dict)
        or shutdown.get("clean") is not True
        or shutdown.get("deadline_exhausted") is not False
        or shutdown.get("errors") != []
        or shutdown.get("observed_terminal_state") != "STOPPED"
    ):
        raise RuntimeError(f"proxy lifecycle evidence is incomplete: {relative}")
    workload = execution.get("client_workload")
    requests = workload.get("requests") if isinstance(workload, dict) else None
    issued = workload.get("issued") if isinstance(workload, dict) else None
    if (
        not isinstance(workload, dict)
        or workload.get("schema_version") != 1
        or isinstance(issued, bool)
        or not isinstance(issued, int)
        or issued < row.get("client_workers")
        or workload.get("completed") != issued
        or workload.get("unique_request_ids") != issued
        or workload.get("failed") != 0
        or workload.get("failures") != []
        or not isinstance(requests, list)
        or len(requests) != issued
    ):
        raise RuntimeError(f"proxy bounded workload is incomplete: {relative}")
    expected_ids = {
        f"{str(row.get('gate_id')).lower()}-request-{index:06d}" for index in range(issued)
    }
    observed_ids = {
        item.get("request_id")
        for item in requests
        if isinstance(item, dict) and item.get("status_code") == 200
    }
    if observed_ids != expected_ids:
        raise RuntimeError(f"proxy request identities are missing or duplicated: {relative}")
    _verify_proxy_diagnostics(execution.get("gateway_diagnostics"), relative)
    _verify_proxy_cleanup(row, result, declaration, relative)


def _verify_proxy_campaign(review: dict[str, Any]) -> None:
    campaign = review["campaigns"]["proxy"]
    if not isinstance(campaign, dict) or set(campaign) != {"plan_path", "plan_sha256", "results"}:
        raise RuntimeError("proxy campaign has the wrong exact schema")
    plan_path = _hashed_path(campaign, "plan_path", "plan_sha256", "proxy")
    plan = _load_json(plan_path)
    if plan.get("schema_version") != 1 or plan.get("candidate") != review["candidate"]:
        raise RuntimeError("proxy plan has the wrong schema or candidate")
    for name in ("harness", "support"):
        value = plan.get(name)
        if not isinstance(value, dict) or set(value) != {"path", "sha256"}:
            raise RuntimeError(f"proxy plan has an invalid {name} declaration")
        _hashed_path(value, "path", "sha256", f"proxy.{name}")
    gates = plan.get("gates")
    results = campaign.get("results")
    if (
        not isinstance(gates, list)
        or not isinstance(results, list)
        or len(gates) != 2
        or len(results) != 2
        or any(not isinstance(item, dict) for item in gates + results)
    ):
        raise RuntimeError("proxy campaign must contain exactly two toggle arms")
    result_by_gate = {item.get("gate_id"): item for item in results}
    seen: set[bool] = set()
    for row in gates:
        arm = row.get("http_no_delay")
        if not isinstance(arm, bool) or arm in seen or row.get("logical_nodes") != 1:
            raise RuntimeError(f"invalid proxy gate declaration: {row!r}")
        for path_field, hash_field in (
            ("config_path", "config_sha256"),
            ("deployment_plan_path", "deployment_plan_sha256"),
            ("site_profile_path", "site_profile_sha256"),
        ):
            _hashed_path(row, path_field, hash_field, f"proxy.{row.get('gate_id')}")
        declaration = result_by_gate.pop(row.get("gate_id"), None)
        if not isinstance(declaration, dict):
            raise RuntimeError(f"proxy gate has no exact result: {row.get('gate_id')}")
        _verify_proxy_result(review, plan_path, campaign["plan_sha256"], plan, row, declaration)
        seen.add(arm)
    if seen != {False, True} or result_by_gate:
        raise RuntimeError("proxy toggle matrix is incomplete or has extra results")


def _verify_supervisor_cleanup(
    result: dict[str, Any], scenario: dict[str, Any], relative: str
) -> None:
    all_cleanup = result.get("exact_generation_cleanup")
    cleanup = all_cleanup.get(scenario.get("scenario")) if isinstance(all_cleanup, dict) else None
    reports = cleanup.get("reports") if isinstance(cleanup, dict) else None
    if (
        not isinstance(cleanup, dict)
        or cleanup.get("schema_version") != 1
        or not isinstance(reports, list)
        or len(reports) != 2
    ):
        raise RuntimeError(f"supervisor cleanup has the wrong shape: {relative}")
    hostnames: set[str] = set()
    for report in reports:
        if (
            not isinstance(report, dict)
            or report.get("deployment_plan_hash") != scenario.get("deployment_plan_hash")
            or report.get("generation") != scenario.get("generation")
            or report.get("matched") != []
            or report.get("signals") != []
            or report.get("survivors") != []
            or not isinstance(report.get("hostname"), str)
        ):
            raise RuntimeError(f"supervisor exact-generation cleanup is incomplete: {relative}")
        hostnames.add(report["hostname"])
    if len(hostnames) != 2:
        raise RuntimeError(f"supervisor cleanup does not cover both nodes: {relative}")


def _verify_supervisor_campaign(review: dict[str, Any]) -> None:
    campaign = review["campaigns"]["supervisor"]
    required = {"plan_path", "plan_sha256", "result_path", "result_sha256", "gate_id", "scenarios"}
    if not isinstance(campaign, dict) or set(campaign) != required:
        raise RuntimeError("supervisor campaign has the wrong exact schema")
    plan_path = _hashed_path(campaign, "plan_path", "plan_sha256", "supervisor")
    result_path = _hashed_path(campaign, "result_path", "result_sha256", "supervisor")
    plan = _load_json(plan_path)
    if plan.get("schema_version") != 3 or plan.get("candidate") != review["candidate"]:
        raise RuntimeError("supervisor plan has the wrong schema or candidate")
    harness = plan.get("harness")
    support = plan.get("support")
    if not isinstance(harness, dict) or set(harness) != {"path", "sha256"}:
        raise RuntimeError("supervisor plan has an invalid harness declaration")
    _hashed_path(harness, "path", "sha256", "supervisor.harness")
    if not isinstance(support, dict) or set(support) != {"fault_runtime", "lifecycle"}:
        raise RuntimeError("supervisor plan has invalid support declarations")
    for name, value in support.items():
        if not isinstance(value, dict) or set(value) != {"path", "sha256"}:
            raise RuntimeError(f"supervisor support {name} has the wrong schema")
        _hashed_path(value, "path", "sha256", f"supervisor.support.{name}")
    gates = plan.get("gates")
    if not isinstance(gates, list) or len(gates) != 1 or not isinstance(gates[0], dict):
        raise RuntimeError("supervisor campaign must contain exactly one gate")
    row = gates[0]
    if (
        row.get("gate_id") != campaign.get("gate_id")
        or row.get("logical_nodes") != 2
        or row.get("physical_allocation_nodes") != 2
        or row.get("scenarios") != campaign.get("scenarios")
        or campaign.get("result_path") != f"{row.get('output_path')}/result.json"
    ):
        raise RuntimeError("supervisor gate or result drifted from the review")
    for path_field, hash_field in (
        ("config_path", "config_sha256"),
        ("deployment_plan_path", "deployment_plan_sha256"),
        ("site_profile_path", "site_profile_sha256"),
    ):
        _hashed_path(row, path_field, hash_field, "supervisor.gate")
    result = _load_json(result_path)
    if result.get("declared_gate") != row:
        raise RuntimeError("supervisor result does not embed its exact declared gate")
    if (
        result.get("schema_version") != 3
        or result.get("passed") is not True
        or result.get("candidate") != review["candidate"]
        or result.get("gate_id") != campaign["gate_id"]
        or result.get("experiment_plan_path") != str(plan_path)
        or result.get("experiment_plan_sha256") != campaign["plan_sha256"]
        or result.get("harness") != str(_repository_path(harness["path"], "harness.path"))
        or result.get("harness_sha256") != harness["sha256"]
    ):
        raise RuntimeError("supervisor result identity or verdict is invalid")
    runtime_result_path = Path(str(result.get("runtime_result_path", ""))).resolve()
    runtime_plan_path = Path(str(result.get("runtime_experiment_plan_path", ""))).resolve()
    output = _repository_path(row["output_path"], "supervisor.output_path")
    try:
        runtime_result_path.relative_to(output)
        runtime_plan_path.relative_to(output)
    except ValueError as exc:
        raise RuntimeError(
            "supervisor derived runtime evidence escapes the declared output"
        ) from exc
    if _sha256(runtime_result_path) != result.get("runtime_result_sha256") or _sha256(
        runtime_plan_path
    ) != result.get("runtime_experiment_plan_sha256"):
        raise RuntimeError("supervisor derived runtime evidence bytes changed")
    runtime_result = _load_json(runtime_result_path)
    if runtime_result.get("scenarios") != result.get("scenarios"):
        raise RuntimeError("supervisor wrapper and runtime scenario evidence differ")
    scenarios = result.get("scenarios")
    expected = campaign.get("scenarios")
    if (
        not isinstance(scenarios, list)
        or [item.get("scenario") for item in scenarios if isinstance(item, dict)] != expected
        or any(not isinstance(item, dict) or item.get("passed") is not True for item in scenarios)
    ):
        raise RuntimeError("supervisor scenario set or verdict is invalid")
    exact = {
        "head-ray-child-death": (0, "ray_head", "rank 0 component ray: exit=137"),
        "worker-supervisor-death": (
            1,
            "node_supervisor",
            "authenticated rank control session disappeared without GOODBYE for rank(s) [1]",
        ),
    }
    for scenario in scenarios:
        name = scenario["scenario"]
        rank, role, detail = exact[name]
        target = scenario.get("target")
        injection = scenario.get("fault_injection")
        if (
            scenario.get("schema_version") != 1
            or scenario.get("returncode") in {0, 143}
            or scenario.get("terminal_state") != "FAILED"
            or scenario.get("terminal_reason_code") != "FIRST_CAUSE"
            or detail not in str(scenario.get("terminal_detail"))
            or not isinstance(target, dict)
            or target.get("rank") != rank
            or target.get("role") != role
            or not isinstance(target.get("receipt_hash"), str)
            or not target["receipt_hash"]
            or not isinstance(injection, dict)
            or injection.get("kind") != name
            or injection.get("target") != target
            or injection.get("signal", {}).get("signal") != "KILL"
        ):
            raise RuntimeError(f"supervisor first-cause proof is invalid: {name}")
        _verify_shutdown(scenario, campaign["result_path"], failed=True)
        _verify_supervisor_cleanup(result, scenario, campaign["result_path"])
        status_path = runtime_result_path.parent / name / "deployment" / "deployment_status.json"
        status = _load_json(status_path)
        history = status.get("history")
        failed_records = [
            item
            for item in history or []
            if isinstance(item, dict) and item.get("state") == "FAILED"
        ]
        if status.get("state") != "FAILED" or len(failed_records) != 1:
            raise RuntimeError(
                f"supervisor scenario lacks exactly one FAILED terminal record: {name}"
            )


def verify_candidate_review(review_path: Path) -> dict[str, Any]:
    review_path = review_path.resolve()
    try:
        review_path.relative_to(ROOT.resolve())
    except ValueError as exc:
        raise RuntimeError("review manifest must be inside the repository") from exc
    review = _load_json(review_path)
    _verify_review_shape(review)
    _verify_candidate(review)
    _verify_packaged_gate(review)
    _verify_lifecycle_campaign(review)
    _verify_proxy_campaign(review)
    _verify_supervisor_campaign(review)
    return review


def _campaign_evidence(review: dict[str, Any]) -> dict[str, list[str]]:
    lifecycle = review["campaigns"]["lifecycle"]
    proxy = review["campaigns"]["proxy"]
    supervisor = review["campaigns"]["supervisor"]
    package = review["packaged_gate"]
    lifecycle_paths = [item["path"] for item in lifecycle["results"]]
    by_cell: dict[tuple[int, str], str] = {}
    plan = _load_json(_repository_path(lifecycle["plan_path"], "lifecycle.plan_path"))
    for row in plan["gates"]:
        by_cell[(row["logical_nodes"], row["engine_mode"])] = f"{row['output_path']}/result.json"
    proxy_paths = [item["path"] for item in proxy["results"]]
    supervisor_path = supervisor["result_path"]
    return {
        "AC-TST-01": [
            package["receipt_path"],
            package["pytest_log_path"],
            package["mypy_log_path"],
        ],
        "AC-SUP-01": [supervisor_path, by_cell[(2, "null")]],
        "AC-CTL-01": [supervisor_path, by_cell[(2, "null")]],
        "AC-COMP-01": [by_cell[(1, "real")], by_cell[(2, "real")]],
        "AC-RDY-01": [supervisor_path, by_cell[(2, "null")], by_cell[(2, "real")]],
        "AC-RDY-02": [supervisor_path, by_cell[(2, "null")]],
        "AC-PLAN-01": [],
        "AC-TEL-01": [by_cell[(2, "null")]],
        "AC-STAT-01": [supervisor_path],
        "AC-OBS-01": [],
        "AC-DIST-01": [by_cell[(2, "null")], by_cell[(2, "real")]],
        "AC-PP-01": [by_cell[(2, "real")]],
        "AC-PROXY-01": [
            proxy["plan_path"],
            *proxy_paths,
            by_cell[(1, "null")],
            by_cell[(2, "null")],
        ],
        "AC-INST-01": [],
        "AC-SCALE-01": lifecycle_paths,
    }


def _record_evidence(
    review: dict[str, Any], acceptance: list[str], record: dict[str, Any]
) -> list[str]:
    campaign = _campaign_evidence(review)
    result: list[str] = []
    for acceptance_id in acceptance:
        if acceptance_id not in STATIC_EVIDENCE:
            raise RuntimeError(f"unknown acceptance gate {acceptance_id!r} for {record.get('id')}")
        for reference in [*STATIC_EVIDENCE[acceptance_id], *campaign.get(acceptance_id, [])]:
            if reference not in result:
                result.append(reference)
    for reference in result:
        path = _repository_path(reference, f"evidence for {record.get('id')}")
        if not path.exists():
            raise RuntimeError(f"evidence path does not exist for {record.get('id')}: {reference}")
    return result


def _adjudicate_record(review: dict[str, Any], record: dict[str, Any]) -> dict[str, Any]:
    if set(record) != set(CANONICAL_FIELDS):
        raise RuntimeError(f"record {record.get('id')} is not canonical")
    result = dict(record)
    record_id = str(result["id"])
    acceptance = [str(item) for item in result.get("acceptance_tests", [])]
    if not acceptance:
        acceptance = list(ACCEPTANCE_DEFAULTS.get(record_id, []))
    result["acceptance_tests"] = acceptance
    result["approval"] = None
    result["owner"] = "codex"
    label = review["candidate_label"]
    evidence = _record_evidence(review, acceptance, result) if acceptance else []
    if record_id in EXPECTED_IN_PROGRESS:
        result["status"] = "IN_PROGRESS"
        result["decision"] = IN_PROGRESS_REASONS[record_id]
        result["evidence"] = evidence
        result["fallback"] = (
            f"Reject the unqualified scale or combination; support only the exact {label} dimensions demonstrated at one and two Aurora nodes."
        )
        result["residual_risk"] = "Behavior at an unapproved or unmeasured scale remains unknown."
        result["support_impact"] = (
            "No support claim above two Aurora nodes or for the named unqualified dimension."
        )
        result["revisit_condition"] = (
            "An owner approves the envelope and a predeclared boundary qualification passes."
        )
    elif record_id in EXPECTED_EXTERNAL:
        result["status"] = "EXTERNAL_BLOCKER"
        result["decision"] = EXTERNAL_REASONS[record_id]
        result["evidence"] = evidence
        result["fallback"] = (
            "Reject the offsite scheduler/vendor combination from this release profile."
        )
        result["residual_risk"] = (
            "Portable interfaces are tested, but native offsite behavior is unproven."
        )
        result["support_impact"] = "Slurm with CUDA or ROCm is not a supported production profile."
        result["revisit_condition"] = (
            "A native Slurm CUDA/ROCm allocation and pinned profile become available."
        )
    elif record_id in EXPECTED_OUT_OF_SCOPE:
        result["status"] = "OUT_OF_PRODUCTION_SCOPE"
        result["decision"] = OUT_OF_SCOPE_REASONS[record_id]
        result["evidence"] = evidence
        result["fallback"] = "Use the production implementation without this optional feature."
        result["residual_risk"] = (
            f"None for the declared {label} path; the optional feature is unqualified."
        )
        result["support_impact"] = "The optional capability is not advertised or supported."
        result["revisit_condition"] = (
            "An owner adds the capability to a future production envelope."
        )
    else:
        if not acceptance or not evidence:
            raise RuntimeError(f"closed record has no acceptance evidence: {record_id}")
        result["status"] = "FIXED"
        result["decision"] = (
            f"Verified on the {label} production path: {str(result['invariant']).strip()}"
        )
        result["evidence"] = evidence
        result["fallback"] = "Fail closed; there is no legacy lifecycle or evidence fallback."
        result["residual_risk"] = (
            f"Proof is bound to the {label} Aurora/Ray/vLLM/HAProxy profile and one/two-node dimensions; dependency, profile, or support-envelope changes reopen qualification."
        )
        result["support_impact"] = (
            f"No remaining defect within the exact qualified {label} dimensions."
        )
        result["revisit_condition"] = (
            "Implementation, pinned profile, or declared support dimensions change."
        )
    return {field: result[field] for field in CANONICAL_FIELDS}


def _normalize(record: dict[str, Any]) -> dict[str, Any]:
    source = str(record.get("source", "")).strip()
    regions = record.get("affected_regions")
    if not isinstance(regions, list):
        regions = (
            [str(regions).strip()]
            if str(regions or "").strip()
            else ["doc/PRODUCTION_READINESS_AUDIT.md"]
        )
    acceptance = record.get("acceptance_tests")
    if not isinstance(acceptance, list):
        acceptance = []
    normalized = {
        "id": str(record.get("id", "")).strip(),
        "source": source,
        "severity": str(record.get("severity", "medium")),
        "invariant": str(record.get("invariant", "")).strip(),
        "primary_work_package": str(record.get("primary_work_package", "")).strip(),
        "affected_regions": [str(item) for item in regions],
        "acceptance_tests": [str(item) for item in acceptance],
        "status": "IN_PROGRESS",
        "decision": "Legacy record normalized without closure.",
        "evidence": [],
        "fallback": "none — disposition requires candidate-bound adjudication",
        "residual_risk": "The invariant remains unproven.",
        "support_impact": "The affected capability remains blocked.",
        "owner": "codex",
        "approval": None,
        "revisit_condition": "Candidate-bound acceptance evidence passes.",
    }
    return normalized


def _review_arg(raw: str) -> Path:
    path = Path(raw)
    if path.is_absolute():
        try:
            path.resolve().relative_to(ROOT.resolve())
        except ValueError as exc:
            raise RuntimeError("review manifest must be inside the repository") from exc
        return path.resolve()
    return _repository_path(raw, "review")


def main(argv: list[str] | None = None) -> int:
    from exaserve.state.atomic import atomic_write_text
    from exaserve.yaml_support import load_yaml_mapping

    args = list(sys.argv[1:] if argv is None else argv)
    if args == ["--normalize-legacy"]:
        data = load_yaml_mapping(LEDGER)
        records = data.get("findings") if isinstance(data, dict) else None
        if not isinstance(records, list) or not records:
            print("refusing to mutate: ledger has no findings")
            return 1
        if all(
            isinstance(record, dict) and set(record) == set(CANONICAL_FIELDS) for record in records
        ):
            print("refusing to mutate: ledger already has the canonical exact shape")
            return 2
        data["findings"] = [_normalize(record) for record in records if isinstance(record, dict)]
        atomic_write_text(
            LEDGER, yaml.safe_dump(data, sort_keys=False, width=100, allow_unicode=True)
        )
        print(f"normalized {len(records)} legacy records; no finding was closed")
        return 0
    if len(args) != 2 or args[0] not in {"--verify-candidate", "--apply-candidate"}:
        print(
            "refusing: pass --verify-candidate <review.json>, --apply-candidate <review.json>, or --normalize-legacy"
        )
        return 2
    review_path = _review_arg(args[1])
    review = verify_candidate_review(review_path)
    if args[0] == "--verify-candidate":
        print(f"verified exact {review['candidate_label']} package and campaign evidence")
        return 0
    data = load_yaml_mapping(LEDGER)
    records = data.get("findings") if isinstance(data, dict) else None
    if not isinstance(records, list) or not records:
        print("refusing to adjudicate: ledger has no findings")
        return 1
    record_ids = {str(record.get("id")) for record in records if isinstance(record, dict)}
    required = EXPECTED_IN_PROGRESS | EXPECTED_EXTERNAL | EXPECTED_OUT_OF_SCOPE
    if required - record_ids:
        raise RuntimeError(
            f"review dispositions name unknown ledger IDs: {sorted(required - record_ids)}"
        )
    data["findings"] = [_adjudicate_record(review, record) for record in records]
    data["meta"] = {
        **dict(data.get("meta") or {}),
        "adjudicated": review["adjudicated_on"],
        "candidate": review["candidate_label"],
        "wheel_sha256": review["candidate"]["wheel_sha256"],
        "scope_state": review["scope_state"],
        "review_manifest": str(review_path.relative_to(ROOT)),
        "review_manifest_sha256": _sha256(review_path),
    }
    atomic_write_text(LEDGER, yaml.safe_dump(data, sort_keys=False, width=100, allow_unicode=True))
    print(f"adjudicated {len(records)} records against exact {review['candidate_label']} evidence")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
