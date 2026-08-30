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
BASE_CAMPAIGNS = {"lifecycle", "proxy", "supervisor"}
SCALE_CAMPAIGNS = BASE_CAMPAIGNS | {"scale"}
SCALE_PLAN_FIELDS = {
    "schema_version",
    "created_at",
    "candidate",
    "harness",
    "support",
    "scope_approval",
    "gates",
}
SCALE_SUPPORT_FIELDS = {"lifecycle", "port_holder"}
SCALE_CODE_FIELDS = {"path", "sha256"}
SCALE_GATE_FIELDS = {
    "gate_id",
    "lane",
    "logical_nodes",
    "physical_allocation_nodes",
    "acquisition_source",
    "queue",
    "lease_ttl",
    "expected_runtime",
    "node_hours",
    "attempt_limit",
    "attempt",
    "output_path",
    "engine_mode",
    "scenario_profile",
    "ready_timeout_s",
    "partial_observation_s",
    "config_path",
    "config_sha256",
    "deployment_plan_path",
    "deployment_plan_sha256",
    "site_profile_path",
    "site_profile_sha256",
    "clean_state_reset_method",
    "retry_reason_policy",
    "expected_observations",
}
SCALE_APPROVAL_FIELDS = {
    "schema_version",
    "decision_id",
    "decision",
    "approver_id",
    "approved_at",
    "approved_max_nodes",
    "required_ladder",
    "dimensions",
}
SCALE_APPROVAL_DIMENSIONS = {
    "scheduler": "pbs",
    "vendor": "xpu",
    "engine": "vllm",
    "gateway": "haproxy",
    "exposure_mode": "PROXIED_INTERNAL",
    "request_mode": "completion",
    "streaming_mode": "non_streaming",
}
SCALE_TIER_CONTRACT = {
    4: {"queue": "capacity", "scenario_profile": "scale_real"},
    16: {"queue": "capacity", "scenario_profile": "scale_real"},
    64: {"queue": "debug-scaling", "scenario_profile": "scale_boundary"},
}
SCALE_AURORA_GPUS_PER_NODE = 12
SCALE_SCENARIOS = {
    "scale_real": [
        "normal-drain",
        "gateway-death",
        "worker-death",
        "duplicate-gateway-port",
        "partial-worker-proxy",
    ],
    "scale_boundary": ["normal-drain", "worker-death"],
}


def _scale_expected_observations(profile: str) -> list[str]:
    common = [
        "fresh generation reaches canonical READY",
        "real engine receipts satisfy the exact planned slot set",
        "advertised HAProxy endpoint returns a typed completion",
        "exact receipt slots and per-rank source staging are complete",
        "exact Ray membership and resource totals match the compiled topology",
        "every allocation node hosts its planned Serve proxy and dense replica set",
        "SIGTERM drains and publishes STOPPED with exit 143",
    ]
    faults = [
        "a fresh restart reaches READY",
        "owned gateway death publishes process_dead evidence and FAILED with nonzero exit",
        "exact highest-rank Ray worker death publishes FAILED with nonzero non-143 exit",
        "an already-owned gateway port fails closed before READY",
        "exact N-node Ray membership cannot publish READY while a worker Serve port is held",
        "the bounded partial-readiness observation cancels cleanly if no typed failure wins first",
    ]
    if profile == "scale_real":
        return common + faults
    if profile == "scale_boundary":
        return common + faults[0:1] + faults[2:3]
    raise RuntimeError(f"unsupported scale scenario profile {profile!r}")


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
        "tests/test_scale_qualification_harness.py",
        "tests/test_scale_adjudication.py",
    ],
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


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
    if not isinstance(campaigns, dict) or frozenset(campaigns) not in {
        frozenset(BASE_CAMPAIGNS),
        frozenset(SCALE_CAMPAIGNS),
    }:
        raise RuntimeError(
            "candidate review must declare the three base campaigns and only the optional scale campaign"
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


def _verify_scale_approval(plan: dict[str, Any]) -> Path:
    declaration = plan.get("scope_approval")
    if not isinstance(declaration, dict) or set(declaration) != SCALE_CODE_FIELDS:
        raise RuntimeError("scale plan has an invalid scope approval declaration")
    approval_path = _hashed_path(declaration, "path", "sha256", "scale.scope_approval")
    approval = _load_json(approval_path)
    if not isinstance(approval, dict) or set(approval) != SCALE_APPROVAL_FIELDS:
        raise RuntimeError("scale scope approval has the wrong exact schema")
    dimensions = approval.get("dimensions")
    if (
        approval.get("schema_version") != 1
        or approval.get("decision") != "APPROVE_QUALIFICATION_TARGET"
        or approval.get("approved_max_nodes") != 64
        or approval.get("required_ladder") != sorted(SCALE_TIER_CONTRACT)
        or dimensions != SCALE_APPROVAL_DIMENSIONS
    ):
        raise RuntimeError("scale scope approval does not authorize the exact 4/16/64 contract")
    for field in ("decision_id", "approver_id", "approved_at"):
        if not isinstance(approval.get(field), str) or not approval[field].strip():
            raise RuntimeError(f"scale scope approval {field} must be non-empty text")
    return approval_path


def _verify_scale_canary(value: object, *, model_id: str, relative: str) -> None:
    if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], dict):
        raise RuntimeError(f"scale canary evidence has the wrong shape: {relative}")
    item = value[0]
    response = item.get("response")
    choices = response.get("choices") if isinstance(response, dict) else None
    if (
        item.get("status_code") != 200
        or item.get("model_id") != model_id
        or not isinstance(response, dict)
        or response.get("model") != model_id
        or response.get("object") != "text_completion"
        or not isinstance(choices, list)
        or not choices
        or not isinstance(choices[0], dict)
        or not isinstance(choices[0].get("text"), str)
    ):
        raise RuntimeError(f"scale canary did not return a typed completion: {relative}")


def _verify_scale_ready(
    scenario: dict[str, Any],
    *,
    deployment: dict[str, Any],
    nodes: int,
    replicas: int,
    relative: str,
) -> None:
    evidence = scenario.get("ready_evidence")
    patch = evidence.get("engine_patch_evidence") if isinstance(evidence, dict) else None
    instances = patch.get("instances") if isinstance(patch, dict) else None
    receipt_requirements = deployment.get("receipt_requirements")
    expected_slots = (
        sorted(
            item.get("receipt_requirement_id")
            for item in receipt_requirements
            if isinstance(item, dict)
            and isinstance(item.get("receipt_requirement_id"), str)
            and item["receipt_requirement_id"]
        )
        if isinstance(receipt_requirements, list)
        else []
    )
    planned_ranks = {
        item.get("planned_rank")
        for item in instances or []
        if isinstance(item, dict) and item.get("role") == "engine_core"
    }
    if (
        not isinstance(evidence, dict)
        or evidence.get("source_rank_receipts") != nodes
        or not _is_sha256(evidence.get("source_manifest_hash"))
        or not _is_sha256(evidence.get("receipt_manifest_hash"))
        or not isinstance(receipt_requirements, list)
        or len(expected_slots) != len(receipt_requirements)
        or evidence.get("receipt_slots") != expected_slots
        or not isinstance(patch, dict)
        or patch.get("compatibility_profile_hash") != deployment.get("compatibility_profile_hash")
        or patch.get("engine_core_count") != replicas
        or patch.get("engine_worker_count") != 0
        or not isinstance(instances, list)
        or len(instances) != replicas
        or planned_ranks != set(range(nodes))
    ):
        raise RuntimeError(f"scale READY evidence is incomplete or not dense: {relative}")


def _verify_scale_result(
    review: dict[str, Any],
    plan_path: Path,
    plan_hash: str,
    plan: dict[str, Any],
    row: dict[str, Any],
    deployment: dict[str, Any],
    declaration: dict[str, Any],
) -> None:
    required_declaration = {"gate_id", "path", "sha256", "scenarios", "cleanup"}
    if not isinstance(declaration, dict) or set(declaration) != required_declaration:
        raise RuntimeError("scale result declaration has the wrong exact schema")
    relative = declaration.get("path")
    if relative != f"{row.get('output_path')}/result.json":
        raise RuntimeError("scale result is outside its declared gate output")
    result_path = _hashed_path(declaration, "path", "sha256", "scale.result")
    result = _load_json(result_path)
    candidate = review["candidate"]
    harness = plan["harness"]
    support = plan["support"]
    approval = plan["scope_approval"]
    if (
        result.get("schema_version") != 1
        or result.get("passed") is not True
        or result.get("declared_gate") != row
        or result.get("candidate") != candidate
        or result.get("gate_id") != row.get("gate_id")
        or declaration.get("gate_id") != row.get("gate_id")
        or result.get("experiment_plan_path") != str(plan_path)
        or result.get("experiment_plan_sha256") != plan_hash
        or result.get("harness") != str(_repository_path(harness["path"], "scale.harness.path"))
        or result.get("harness_sha256") != harness["sha256"]
        or result.get("lifecycle_support")
        != str(_repository_path(support["lifecycle"]["path"], "scale.support.lifecycle"))
        or result.get("lifecycle_support_sha256") != support["lifecycle"]["sha256"]
        or result.get("port_holder_helper")
        != str(_repository_path(support["port_holder"]["path"], "scale.support.port_holder"))
        or result.get("port_holder_helper_sha256") != support["port_holder"]["sha256"]
        or result.get("scope_approval")
        != str(_repository_path(approval["path"], "scale.scope_approval"))
        or result.get("scope_approval_sha256") != approval["sha256"]
        or result.get("config_path")
        != str(_repository_path(row["config_path"], "scale.config_path"))
        or result.get("config_sha256") != row["config_sha256"]
        or result.get("deployment_plan_path")
        != str(_repository_path(row["deployment_plan_path"], "scale.deployment_plan_path"))
        or result.get("deployment_plan_hash") != deployment.get("deployment_plan_hash")
        or result.get("site_profile_path")
        != str(_repository_path(row["site_profile_path"], "scale.site_profile_path"))
        or result.get("wheel")
        != str(_repository_path(candidate["wheel_path"], "candidate.wheel_path"))
        or result.get("wheel_sha256") != candidate["wheel_sha256"]
        or result.get("site_profile_hash") != candidate["site_profile_hash"]
        or not isinstance(result.get("pbs_job_id"), str)
        or not result["pbs_job_id"]
    ):
        raise RuntimeError(f"scale result identity or verdict is invalid: {relative}")
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
        raise RuntimeError(f"scale result drifted from its declared gate: {relative}")

    nodes = row["logical_nodes"]
    model = deployment["models"][0]
    replicas = nodes * deployment["num_gpus_per_node"]
    expected_density = {
        "num_nodes": nodes,
        "num_gpus_per_node": deployment["num_gpus_per_node"],
        "replicas": replicas,
        "replicas_per_rank": deployment["num_gpus_per_node"],
        "serve_applications": replicas + nodes,
        "receipt_requirements": len(deployment["receipt_requirements"]),
    }
    if result.get("density") != expected_density or model.get("num_replicas") != replicas:
        raise RuntimeError(f"scale result density differs from its deployment: {relative}")
    scenarios = result.get("scenarios")
    expected_scenarios = SCALE_SCENARIOS[row["scenario_profile"]]
    if (
        declaration.get("scenarios") != expected_scenarios
        or not isinstance(scenarios, list)
        or [item.get("scenario") for item in scenarios if isinstance(item, dict)]
        != expected_scenarios
        or len(scenarios) != len(expected_scenarios)
        or any(not isinstance(item, dict) or item.get("passed") is not True for item in scenarios)
    ):
        raise RuntimeError(f"scale scenario evidence is incomplete: {relative}")
    cleanup = declaration.get("cleanup")
    if not isinstance(cleanup, dict) or set(cleanup) != set(expected_scenarios):
        raise RuntimeError(f"scale cleanup declarations are incomplete: {relative}")
    model_id = model["model_id"]
    generations: set[int] = set()
    for scenario in scenarios:
        name = scenario["scenario"]
        generation = scenario.get("generation")
        expected_fault = {
            "normal-drain": "operator_drain",
            "gateway-death": "gateway_death",
            "worker-death": "worker_death",
            "duplicate-gateway-port": "duplicate_gateway_port",
            "partial-worker-proxy": "partial_proxy_readiness",
        }[name]
        if (
            isinstance(generation, bool)
            or not isinstance(generation, int)
            or generation <= 0
            or generation in generations
            or scenario.get("deployment_plan_hash") != deployment.get("deployment_plan_hash")
            or scenario.get("fault") != expected_fault
        ):
            raise RuntimeError(f"scale scenario identity is invalid: {relative}:{name}")
        generations.add(generation)
        if name not in {"duplicate-gateway-port", "partial-worker-proxy"}:
            if (
                not isinstance(scenario.get("advertised_endpoint"), str)
                or not scenario["advertised_endpoint"]
                or isinstance(scenario.get("ready_revision"), bool)
                or not isinstance(scenario.get("ready_revision"), int)
                or scenario["ready_revision"] < 0
                or isinstance(scenario.get("terminal_revision"), bool)
                or not isinstance(scenario.get("terminal_revision"), int)
                or scenario["terminal_revision"] <= scenario["ready_revision"]
            ):
                raise RuntimeError(f"scale READY/terminal identity is invalid: {relative}:{name}")
            _verify_scale_ready(
                scenario,
                deployment=deployment,
                nodes=nodes,
                replicas=replicas,
                relative=f"{relative}:{name}",
            )
            _verify_scale_canary(
                scenario.get("canary"), model_id=model_id, relative=f"{relative}:{name}"
            )
        if name == "normal-drain":
            if (
                scenario.get("returncode") != 143
                or scenario.get("terminal_state") != "STOPPED"
                or scenario.get("terminal_reason_code") != "DRAINED_AND_REAPED"
            ):
                raise RuntimeError(f"scale normal drain semantics are invalid: {relative}")
            _verify_shutdown(scenario, relative, failed=False)
        elif name == "gateway-death":
            injection = scenario.get("fault_injection")
            gateway_failure = scenario.get("gateway_failure")
            if (
                scenario.get("returncode") in {0, 143}
                or scenario.get("terminal_state") != "FAILED"
                or scenario.get("terminal_reason_code") != "FIRST_CAUSE"
                or "gateway/haproxy: UNEXPECTED_EXIT" not in str(scenario.get("terminal_detail"))
                or not isinstance(injection, dict)
                or injection.get("kind") != "owned_gateway_death"
                or isinstance(injection.get("pid"), bool)
                or not isinstance(injection.get("pid"), int)
                or injection["pid"] <= 0
                or not isinstance(injection.get("node"), str)
                or not injection["node"]
                or not isinstance(gateway_failure, dict)
                or gateway_failure.get("classification") != "process_dead"
            ):
                raise RuntimeError(f"scale gateway fault semantics are invalid: {relative}")
            _verify_shutdown(scenario, relative, failed=True)
        elif name == "worker-death":
            detail = str(scenario.get("terminal_detail", "")).lower()
            injection = scenario.get("fault_injection")
            target = injection.get("target") if isinstance(injection, dict) else None
            if (
                scenario.get("returncode") in {0, 143}
                or scenario.get("terminal_state") != "FAILED"
                or scenario.get("terminal_reason_code") != "FIRST_CAUSE"
                or f"rank {nodes - 1}" not in detail
                or not any(term in detail for term in ("worker", "ray"))
                or not isinstance(injection, dict)
                or injection.get("kind") != "owned_ray_worker_death"
                or injection.get("worker_rank") != nodes - 1
                or not isinstance(target, dict)
                or target.get("rank") != nodes - 1
                or target.get("receipt_requirement_id") != f"rank{nodes - 1}/ray_worker"
                or isinstance(target.get("pid"), bool)
                or not isinstance(target.get("pid"), int)
                or target["pid"] <= 0
            ):
                raise RuntimeError(f"scale worker fault semantics are invalid: {relative}")
            _verify_shutdown(scenario, relative, failed=True)
        elif name == "duplicate-gateway-port":
            precondition = scenario.get("fault_precondition")
            detail = (
                f"{scenario.get('terminal_reason_code')}: {scenario.get('terminal_detail')}".lower()
            )
            if (
                scenario.get("ready_revision") is not None
                or scenario.get("terminal_state") != "FAILED"
                or scenario.get("returncode") in {0, 143}
                or "listener" not in detail
                or "bind" not in detail
                or not isinstance(precondition, dict)
                or precondition.get("kind") != "local_gateway_port_holder"
                or precondition.get("stopped") is not True
            ):
                raise RuntimeError(f"scale port collision did not fail before READY: {relative}")
        elif name == "partial-worker-proxy":
            history = scenario.get("status_history")
            membership = scenario.get("membership_evidence")
            precondition = scenario.get("fault_precondition")
            cancelled = scenario.get("cancelled_after_observation")
            if (
                scenario.get("ready_revision") is not None
                or not isinstance(history, list)
                or any(item.get("state") == "READY" for item in history if isinstance(item, dict))
                or not isinstance(membership, dict)
                or membership.get("node_count") != nodes
                or not isinstance(precondition, dict)
                or precondition.get("kind") != "remote_worker_proxy_port_holder"
                or scenario.get("held_node") != precondition.get("node")
                or (
                    cancelled is True
                    and (
                        scenario.get("terminal_state") != "CANCELLED"
                        or scenario.get("returncode") != 143
                    )
                )
                or (
                    cancelled is False
                    and (
                        scenario.get("terminal_state") != "FAILED"
                        or scenario.get("returncode") in {0, 143}
                    )
                )
                or not isinstance(cancelled, bool)
            ):
                raise RuntimeError(f"scale partial proxy observation reached READY: {relative}")
        _verify_exact_generation_cleanup(
            cleanup[name],
            expected_path=f"{row['output_path']}/{name}/exact_generation_cleanup.json",
            deployment_id=str(row["gate_id"]).lower(),
            generation=scenario.get("generation"),
            deployment_plan_hash=scenario.get("deployment_plan_hash"),
            nodes=nodes,
            relative=f"{relative}:{name}",
        )


def _verify_scale_campaign(review: dict[str, Any]) -> None:
    campaign = review["campaigns"]["scale"]
    if not isinstance(campaign, dict) or set(campaign) != {"plan_path", "plan_sha256", "results"}:
        raise RuntimeError("scale campaign has the wrong exact schema")
    plan_path = _hashed_path(campaign, "plan_path", "plan_sha256", "scale")
    plan = _load_json(plan_path)
    if (
        set(plan) != SCALE_PLAN_FIELDS
        or plan.get("schema_version") != 3
        or plan.get("candidate") != review["candidate"]
    ):
        raise RuntimeError("scale plan has the wrong schema or candidate")
    harness = plan.get("harness")
    support = plan.get("support")
    if not isinstance(harness, dict) or set(harness) != SCALE_CODE_FIELDS:
        raise RuntimeError("scale plan has an invalid harness declaration")
    _hashed_path(harness, "path", "sha256", "scale.harness")
    if not isinstance(support, dict) or set(support) != SCALE_SUPPORT_FIELDS:
        raise RuntimeError("scale plan has invalid support declarations")
    for name, declaration in support.items():
        if not isinstance(declaration, dict) or set(declaration) != SCALE_CODE_FIELDS:
            raise RuntimeError(f"scale support {name} has an invalid declaration")
        _hashed_path(declaration, "path", "sha256", f"scale.support.{name}")
    _verify_scale_approval(plan)

    gates = plan.get("gates")
    results = campaign.get("results")
    if (
        not isinstance(gates, list)
        or not isinstance(results, list)
        or len(gates) != len(SCALE_TIER_CONTRACT)
        or len(results) != len(SCALE_TIER_CONTRACT)
        or any(not isinstance(item, dict) for item in gates + results)
    ):
        raise RuntimeError("scale campaign must contain the exact 4/16/64 matrix")
    result_by_gate = {item.get("gate_id"): item for item in results}
    if len(result_by_gate) != len(results):
        raise RuntimeError("scale result declarations contain duplicate gate IDs")
    seen_nodes: set[int] = set()
    candidate = review["candidate"]
    for row in gates:
        if set(row) != SCALE_GATE_FIELDS:
            raise RuntimeError("scale gate has the wrong exact schema")
        nodes = row.get("logical_nodes")
        tier = SCALE_TIER_CONTRACT.get(nodes)
        gate_id = row.get("gate_id")
        if (
            tier is None
            or nodes in seen_nodes
            or not isinstance(gate_id, str)
            or not gate_id
            or any(
                character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for character in gate_id
            )
            or row.get("physical_allocation_nodes") != nodes
            or row.get("lane") != "FINAL"
            or row.get("engine_mode") != "real"
            or row.get("queue") != tier["queue"]
            or row.get("scenario_profile") != tier["scenario_profile"]
            or row.get("attempt") != 1
            or row.get("attempt_limit") != 1
            or row.get("acquisition_source") not in {"subjob", "interactive_pbs", "batch_pbs"}
            or not isinstance(row.get("node_hours"), int)
            or isinstance(row.get("node_hours"), bool)
            or row["node_hours"] < 1
            or _finite(row.get("ready_timeout_s"), "scale.ready_timeout_s", 1.0) < 1.0
            or _finite(row.get("partial_observation_s"), "scale.partial_observation_s", 1.0) < 1.0
            or any(
                not isinstance(row.get(field), str) or not row[field].strip()
                for field in (
                    "lease_ttl",
                    "expected_runtime",
                    "clean_state_reset_method",
                    "retry_reason_policy",
                )
            )
            or row.get("expected_observations")
            != _scale_expected_observations(tier["scenario_profile"])
        ):
            raise RuntimeError(f"scale gate is outside the approved tier contract: {row!r}")
        for path_field, hash_field in (
            ("config_path", "config_sha256"),
            ("deployment_plan_path", "deployment_plan_sha256"),
            ("site_profile_path", "site_profile_sha256"),
        ):
            _hashed_path(row, path_field, hash_field, f"scale.{row.get('gate_id')}")
        deployment = _load_json(
            _repository_path(row["deployment_plan_path"], "scale.deployment_plan_path")
        )
        models = deployment.get("models")
        model = models[0] if isinstance(models, list) and len(models) == 1 else None
        replicas = model.get("replicas") if isinstance(model, dict) else None
        gpus_per_node = deployment.get("num_gpus_per_node")
        gateway = deployment.get("gateway")
        exposure = deployment.get("exposure")
        scale_envelope = deployment.get("scale_envelope")
        runtime = deployment.get("runtime")
        ranks = [
            rank
            for replica in replicas or []
            if isinstance(replica, dict)
            for rank in replica.get("planned_ranks", [])
        ]
        if (
            deployment.get("deployment_id") != str(row.get("gate_id")).lower()
            or deployment.get("num_nodes") != nodes
            or deployment.get("site_profile_hash") != candidate["site_profile_hash"]
            or deployment.get("compatibility_profile_hash")
            != candidate["compatibility_profile_hash"]
            or deployment.get("manifest_hash") != candidate["compatibility_manifest_hash"]
            or deployment.get("site_profile_id") != "alcf-aurora"
            or deployment.get("vendor") != "xpu"
            or deployment.get("engine") != "vllm"
            or deployment.get("validation_mode") is not True
            or not isinstance(gateway, dict)
            or gateway.get("kind") != "haproxy"
            or not isinstance(exposure, dict)
            or exposure.get("mode") != "PROXIED_INTERNAL"
            or not isinstance(scale_envelope, dict)
            or scale_envelope.get("site_id") != "alcf-aurora"
            or scale_envelope.get("scheduler_type") != "pbs"
            or scale_envelope.get("vendor") != "xpu"
            or scale_envelope.get("accelerator") != "pvc"
            or scale_envelope.get("engine") != "vllm"
            or scale_envelope.get("gateway_kind") != "haproxy"
            or scale_envelope.get("exposure_mode") != "PROXIED_INTERNAL"
            or scale_envelope.get("request_mode") != "completion"
            or scale_envelope.get("streaming_mode") != "non_streaming"
            or scale_envelope.get("qualification_target_nodes") != 64
            or scale_envelope.get("validation_mode") is not True
            or not isinstance(runtime, dict)
            or runtime.get("null_compute") is not False
            or gpus_per_node != SCALE_AURORA_GPUS_PER_NODE
            or not isinstance(model, dict)
            or model.get("tensor_parallel_size") != 1
            or model.get("pipeline_parallel_size") != 1
            or model.get("num_replicas") != nodes * gpus_per_node
            or len(ranks) != model.get("num_replicas")
            or set(ranks) != set(range(nodes))
            or any(ranks.count(rank) != gpus_per_node for rank in range(nodes))
        ):
            raise RuntimeError(
                f"scale deployment identity or density drifted: {row.get('gate_id')}"
            )
        declaration = result_by_gate.pop(row.get("gate_id"), None)
        if not isinstance(declaration, dict):
            raise RuntimeError(f"scale gate has no exact result: {row.get('gate_id')}")
        _verify_scale_result(
            review,
            plan_path,
            campaign["plan_sha256"],
            plan,
            row,
            deployment,
            declaration,
        )
        seen_nodes.add(nodes)
    if seen_nodes != set(SCALE_TIER_CONTRACT) or result_by_gate:
        raise RuntimeError("scale campaign does not cover exactly 4, 16, and 64 nodes")


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
    if "scale" in review["campaigns"]:
        _verify_scale_campaign(review)
    return review


def _campaign_evidence(review: dict[str, Any]) -> dict[str, list[str]]:
    lifecycle = review["campaigns"]["lifecycle"]
    proxy = review["campaigns"]["proxy"]
    supervisor = review["campaigns"]["supervisor"]
    scale = review["campaigns"].get("scale")
    package = review["packaged_gate"]
    lifecycle_paths = [item["path"] for item in lifecycle["results"]]
    by_cell: dict[tuple[int, str], str] = {}
    plan = _load_json(_repository_path(lifecycle["plan_path"], "lifecycle.plan_path"))
    for row in plan["gates"]:
        by_cell[(row["logical_nodes"], row["engine_mode"])] = f"{row['output_path']}/result.json"
    proxy_paths = [item["path"] for item in proxy["results"]]
    supervisor_path = supervisor["result_path"]
    scale_paths = [item["path"] for item in scale["results"]] if scale is not None else []
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
        "AC-SCALE-01": [*lifecycle_paths, *scale_paths],
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
