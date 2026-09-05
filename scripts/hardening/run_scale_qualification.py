#!/usr/bin/env python3
"""Scale-lane qualification for the approved dense 4 -> 16 -> 64 ladder.

The v1 lifecycle runtime (`run_final_null_qualification.py`) owns the qualified
one- and two-node cells. Its bytes are pinned by the archived final35..final38
supervisor declarations, so this lane may not edit it -- a changed support
sha256 retroactively invalidates evidence that is supposed to be immutable.

This file is a small, standard-library-only trust root until it has loaded the
experiment declaration and verified every declared executable byte.  Only then
does it import v1 for the node-count-agnostic runtime helpers (the remote port
holder, receipt targeting, status waits, terminal records, canaries, and
exact-generation cleanup).  It forks exactly three things that v1 cannot
express:

1.  `_load_scale_gate` -- v1's gate loader hard-codes the accepted scenario
    profiles and queues, and neither `scale_real` nor `debug-scaling` is in
    those sets.
2.  `_launch_scale_scenario` -- v1's scenario launcher hard-asserts a two-node
    allocation inside two fault branches (`partial_proxy_readiness` and
    `worker_death`). Those assertions are correct for the two-node battery and
    wrong for the ladder, and they sit in the middle of a function whose
    surrounding launch/verify/cleanup logic is node-count agnostic.
3.  The scenario matrix, observation contract, and dense-plan predicate.

What the fork deliberately does NOT carry over is the `replica_death` branch:
that fault belongs to the four-node PP cell, which keeps running under v1.

The scale lane varies exactly one thing against the qualified lower tiers --
node count at full per-node replica density.  Four and sixteen nodes run the
full fault battery.  The 64-node boundary runs bring-up/drain plus one exact
worker-loss fault, preserving the ADR's READY, canary, fault, terminal, and
cleanup requirements inside the debug-scaling walltime.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shlex
import signal
import socket
import subprocess
import sys
import time
import traceback
from pathlib import Path
from types import ModuleType

# Installed only after the experiment declaration verifies its path and digest.
# A bare annotation deliberately avoids importing checkout support at module load
# time, which also keeps direct ``python scripts/hardening/...`` invocation valid.
lifecycle: ModuleType

_HEX = frozenset("0123456789abcdef")
_PLAN_FIELDS = {
    "schema_version",
    "created_at",
    "candidate",
    "harness",
    "support",
    "scope_approval",
    "gates",
}
_CANDIDATE_FIELDS = {
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
_CODE_FIELDS = {"path", "sha256"}
_SUPPORT_FIELDS = {"lifecycle"}
_GATE_FIELDS = {
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
_APPROVAL_FIELDS = {
    "schema_version",
    "decision_id",
    "decision",
    "approver_id",
    "approved_at",
    "approved_max_nodes",
    "required_ladder",
    "dimensions",
}
_APPROVAL_DIMENSION_FIELDS = {
    "scheduler",
    "vendor",
    "engine",
    "gateway",
    "exposure_mode",
    "request_mode",
    "streaming_mode",
}
_APPROVAL_DIMENSIONS = {
    "scheduler": "pbs",
    "vendor": "xpu",
    "engine": "vllm",
    "gateway": "haproxy",
    "exposure_mode": "PROXIED_INTERNAL",
    "request_mode": "completion",
    "streaming_mode": "non_streaming",
}
_REQUIRED_LADDER = (4, 16, 64)
_AURORA_GPUS_PER_NODE = 12
_TIER_CONTRACT = {
    4: {"queue": "capacity", "scenario_profile": "scale_real"},
    16: {"queue": "capacity", "scenario_profile": "scale_real"},
    64: {"queue": "debug-scaling", "scenario_profile": "scale_boundary"},
}

SCALE_SCENARIO_PROFILES = frozenset({"scale_real", "scale_boundary"})
# Profiles whose matrix injects the remote worker-proxy port hold.
_PORT_HOLDER_PROFILES = frozenset({"scale_real"})
_SCALE_ACQUISITION_SOURCES = frozenset({"subjob", "interactive_pbs", "batch_pbs"})


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _reject_constant(value: str):
    raise ValueError(f"non-finite JSON constant is forbidden: {value}")


def _reject_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key is forbidden: {key!r}")
        result[key] = value
    return result


def _read_json(path: Path) -> dict:
    try:
        with path.open(encoding="utf-8", errors="strict") as handle:
            payload = json.load(
                handle,
                parse_constant=_reject_constant,
                object_pairs_hook=_reject_duplicate_keys,
            )
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"could not load strict JSON object {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"expected an object at {path}")
    return payload


def _require_exact_shape(value: object, fields: set[str], context: str) -> dict:
    if not isinstance(value, dict):
        raise RuntimeError(f"{context} must be an object")
    unknown = sorted(set(value) - fields)
    missing = sorted(fields - set(value))
    if unknown or missing:
        raise RuntimeError(f"{context} shape mismatch: unknown={unknown}, missing={missing}")
    return value


def _require_sha256(value: object, context: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in _HEX for char in value):
        raise RuntimeError(f"{context} must be a lowercase SHA-256 digest")
    return value


def _declared_path(
    repo_root: Path,
    value: object,
    context: str,
    *,
    kind: str,
) -> Path:
    if not isinstance(value, str) or not value or Path(value).is_absolute():
        raise RuntimeError(f"{context} must be a non-empty repository-relative path")
    path = (repo_root / value).resolve()
    try:
        path.relative_to(repo_root)
    except ValueError as exc:
        raise RuntimeError(f"{context} escapes the repository: {value!r}") from exc
    if kind == "file" and not path.is_file():
        raise RuntimeError(f"{context} is not a file: {path}")
    if kind == "directory" and not path.is_dir():
        raise RuntimeError(f"{context} is not a directory: {path}")
    if kind not in {"file", "directory", "output"}:
        raise AssertionError(f"unsupported declared path kind {kind!r}")
    return path


def _load_verified_module(path: Path, expected_sha256: str) -> ModuleType:
    """Execute the exact verified source bytes without consulting bytecode caches."""

    module_name = "_exaserve_scale_lifecycle_support"
    source = path.read_bytes()
    if hashlib.sha256(source).hexdigest() != expected_sha256:
        raise RuntimeError("lifecycle support changed between declaration loading and import")
    module = ModuleType(module_name)
    module.__file__ = str(path)
    module.__package__ = ""
    sys.modules[module_name] = module
    try:
        code = compile(source, str(path), "exec", dont_inherit=True)
        exec(code, module.__dict__)
    except BaseException:
        sys.modules.pop(module_name, None)
        raise
    if Path(str(module.__file__)).resolve() != path:
        raise RuntimeError("loaded lifecycle support path differs from its declaration")
    return module


def _expected_observations(engine_mode: str, scenario_profile: str) -> list[str]:
    if scenario_profile not in SCALE_SCENARIO_PROFILES:
        raise RuntimeError(f"unsupported scale scenario profile {scenario_profile!r}")
    scaled = [
        "fresh generation reaches canonical READY",
        f"{engine_mode} engine receipts satisfy the exact planned slot set",
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
    if scenario_profile == "scale_boundary":
        return scaled + faults[0:1] + faults[2:3]
    return scaled + faults


def _scenario_matrix(profile: str) -> tuple[tuple[str, str], ...]:
    if profile == "scale_boundary":
        return (
            ("normal-drain", "operator_drain"),
            ("worker-death", "worker_death"),
        )
    if profile == "scale_real":
        return (
            ("normal-drain", "operator_drain"),
            ("gateway-death", "gateway_death"),
            ("worker-death", "worker_death"),
            ("duplicate-gateway-port", "duplicate_gateway_port"),
            ("partial-worker-proxy", "partial_proxy_readiness"),
        )
    raise ValueError(f"unknown scale scenario profile {profile!r}")


def _validate_scale_plan(plan, *, engine_mode: str, scenario_profile: str) -> dict:
    """One real TP=1/PP=1 model at full per-node density on every rank.

    A thin plan would scale the control plane without scaling the thing the
    control plane exists to place, so the ladder would prove membership and
    receipts at 64 nodes while never loading more than one engine per tier.
    """
    envelope = plan.scale_envelope
    dimensions = {
        "site_id": plan.site_profile_id,
        "scheduler": envelope.scheduler_type,
        "vendor": plan.vendor,
        "accelerator": envelope.accelerator,
        "engine": plan.engine,
        "gateway": None if plan.gateway is None else plan.gateway.kind,
        "exposure_mode": plan.exposure.mode,
        "request_mode": envelope.request_mode,
        "streaming_mode": envelope.streaming_mode,
    }
    expected_dimensions = {
        "site_id": "alcf-aurora",
        "scheduler": "pbs",
        "vendor": "xpu",
        "accelerator": "pvc",
        "engine": "vllm",
        "gateway": "haproxy",
        "exposure_mode": "PROXIED_INTERNAL",
        "request_mode": "completion",
        "streaming_mode": "non_streaming",
    }
    if dimensions != expected_dimensions:
        raise RuntimeError(
            "scale gate dimensions differ from the approved Aurora/XPU/vLLM/HAProxy/"
            f"completion contract: observed={dimensions!r}"
        )
    if (
        envelope.site_id != plan.site_profile_id
        or envelope.vendor != plan.vendor
        or envelope.engine != plan.engine
        or envelope.gateway_kind != dimensions["gateway"]
        or envelope.exposure_mode != plan.exposure.mode
        or envelope.qualification_target_nodes != 64
        or envelope.validation_mode is not True
        or plan.validation_mode is not True
    ):
        raise RuntimeError("scale gate is not bound to the candidate-64 validation envelope")
    if engine_mode != "real":
        raise RuntimeError("scale gate requires the real engine")
    if plan.num_nodes not in _TIER_CONTRACT:
        raise RuntimeError(
            f"scale gate node count must be one of {list(_REQUIRED_LADDER)}; "
            f"observed {plan.num_nodes}"
        )
    expected_profile = _TIER_CONTRACT[plan.num_nodes]["scenario_profile"]
    if scenario_profile != expected_profile:
        raise RuntimeError(
            f"{plan.num_nodes}-node scale gate requires scenario_profile={expected_profile!r}"
        )
    if len(plan.models) != 1:
        raise RuntimeError("scale gate requires exactly one model")
    if plan.num_gpus_per_node != _AURORA_GPUS_PER_NODE:
        raise RuntimeError(f"Aurora scale gate requires {_AURORA_GPUS_PER_NODE} GPU tiles per node")
    model = plan.models[0]
    dense_replicas = plan.num_nodes * plan.num_gpus_per_node
    covered = sorted({rank for replica in model.replicas for rank in replica.planned_ranks})
    per_rank: dict[int, int] = {}
    for replica in model.replicas:
        for rank in replica.planned_ranks:
            per_rank[rank] = per_rank.get(rank, 0) + 1
    if (
        model.tensor_parallel_size != 1
        or model.pipeline_parallel_size != 1
        or model.num_replicas != dense_replicas
        or covered != list(range(plan.num_nodes))
        or set(per_rank.values()) != {plan.num_gpus_per_node}
    ):
        raise RuntimeError(
            f"{scenario_profile} requires one real TP=1/PP=1 model placed "
            f"{plan.num_gpus_per_node} replicas deep on every one of "
            f"{plan.num_nodes} ranks (observed {model.num_replicas} replicas over "
            f"{len(covered)} ranks)"
        )
    if plan.runtime.null_compute:
        raise RuntimeError("scale gate engine_mode=real disagrees with plan null_compute")
    return {
        "num_nodes": plan.num_nodes,
        "num_gpus_per_node": plan.num_gpus_per_node,
        "replicas": model.num_replicas,
        "replicas_per_rank": plan.num_gpus_per_node,
        "serve_applications": model.num_replicas + plan.num_nodes,
        "receipt_requirements": len(plan.receipt_requirements),
    }


def _validate_candidate_plan_identity(plan, profile, candidate: dict, *, gate_id: str) -> None:
    if plan.site_profile_hash != profile.site_profile_hash:
        raise RuntimeError("declared plan and site profile identities disagree")
    if profile.site_profile_hash != candidate["site_profile_hash"]:
        raise RuntimeError("gate SiteProfile differs from the declared candidate")
    if plan.compatibility_profile_hash != candidate["compatibility_profile_hash"]:
        raise RuntimeError("gate compatibility profile differs from the declared candidate")
    if plan.manifest_hash != candidate["compatibility_manifest_hash"]:
        raise RuntimeError("gate compatibility manifest differs from the declared candidate")
    if plan.deployment_id != gate_id.lower():
        raise RuntimeError("compiled deployment_id is not bound to the declared gate_id")


def _validate_gate_contract(gate: dict, *, context: str) -> None:
    gate_id = gate["gate_id"]
    if (
        not isinstance(gate_id, str)
        or not gate_id
        or any(character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for character in gate_id)
    ):
        raise RuntimeError(f"{context}.gate_id is invalid")
    if gate["lane"] != "FINAL":
        raise RuntimeError(f"{context}.lane must be FINAL")
    for name in ("logical_nodes", "physical_allocation_nodes", "node_hours"):
        if isinstance(gate[name], bool) or not isinstance(gate[name], int) or gate[name] <= 0:
            raise RuntimeError(f"{context}.{name} must be a positive integer")
    if gate["logical_nodes"] != gate["physical_allocation_nodes"]:
        raise RuntimeError(f"{context} cannot silently subset a larger allocation")
    if gate["acquisition_source"] not in _SCALE_ACQUISITION_SOURCES:
        raise RuntimeError(f"{context}.acquisition_source is unsupported")
    tier = _TIER_CONTRACT.get(gate["logical_nodes"])
    if tier is None:
        raise RuntimeError(f"{context}.logical_nodes must be one of {list(_REQUIRED_LADDER)}")
    if gate["queue"] != tier["queue"]:
        raise RuntimeError(
            f"{context} at {gate['logical_nodes']} nodes requires queue={tier['queue']!r}"
        )
    for name in (
        "lease_ttl",
        "expected_runtime",
        "clean_state_reset_method",
        "retry_reason_policy",
    ):
        if not isinstance(gate[name], str) or not gate[name].strip():
            raise RuntimeError(f"{context}.{name} must be non-empty text")
    for name in ("attempt_limit", "attempt"):
        if isinstance(gate[name], bool) or not isinstance(gate[name], int) or gate[name] <= 0:
            raise RuntimeError(f"{context}.{name} must be a positive integer")
    if gate["attempt"] > gate["attempt_limit"]:
        raise RuntimeError(f"{context}.attempt exceeds attempt_limit")
    if gate["engine_mode"] != "real":
        raise RuntimeError(f"{context} must use the real engine")
    if gate["scenario_profile"] != tier["scenario_profile"]:
        raise RuntimeError(
            f"{context} at {gate['logical_nodes']} nodes requires "
            f"scenario_profile={tier['scenario_profile']!r}"
        )
    for name in ("ready_timeout_s", "partial_observation_s"):
        value = gate[name]
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value <= 0
        ):
            raise RuntimeError(f"{context}.{name} must be finite and positive")
    expected = _expected_observations(gate["engine_mode"], gate["scenario_profile"])
    if gate["expected_observations"] != expected:
        raise RuntimeError(f"{context}.expected_observations differs from the executable contract")


def _load_scale_gate(
    experiment_plan_path: Path,
    gate_id: str,
    *,
    repo_root: Path | None = None,
    harness_path: Path | None = None,
) -> tuple[dict, dict, dict[str, Path]]:
    """Load one exact scale gate and verify all code, scope, and input bytes."""
    root = (repo_root or Path(__file__).resolve().parents[2]).resolve()
    current_harness = (harness_path or Path(__file__)).resolve()
    plan_path = experiment_plan_path.resolve()
    try:
        plan_path.relative_to(root)
    except ValueError as exc:
        raise RuntimeError("experiment plan must be inside the repository") from exc
    document = _require_exact_shape(_read_json(plan_path), _PLAN_FIELDS, "experiment plan")
    if document["schema_version"] != 3:
        raise RuntimeError("scale experiment plan schema_version must be 3")
    if not isinstance(document["created_at"], str) or not document["created_at"].strip():
        raise RuntimeError("experiment plan created_at must be non-empty")

    candidate = _require_exact_shape(
        document["candidate"], _CANDIDATE_FIELDS, "experiment plan candidate"
    )
    harness = _require_exact_shape(document["harness"], _CODE_FIELDS, "experiment plan harness")
    harness_file = _declared_path(root, harness["path"], "harness.path", kind="file")
    if harness_file != current_harness:
        raise RuntimeError(
            f"declared harness {harness_file} is not running harness {current_harness}"
        )
    if _sha256_file(harness_file) != _require_sha256(harness["sha256"], "harness.sha256"):
        raise RuntimeError("running qualification harness bytes differ from the declaration")

    support = _require_exact_shape(document["support"], _SUPPORT_FIELDS, "support")
    for name in sorted(_SUPPORT_FIELDS):
        declaration = _require_exact_shape(support[name], _CODE_FIELDS, f"support.{name}")
        support_path = _declared_path(
            root, declaration["path"], f"support.{name}.path", kind="file"
        )
        expected_digest = _require_sha256(declaration["sha256"], f"support.{name}.sha256")
        if _sha256_file(support_path) != expected_digest:
            raise RuntimeError(f"support.{name} changed after experiment declaration")

    approval_ref = _require_exact_shape(document["scope_approval"], _CODE_FIELDS, "scope_approval")
    approval_path = _declared_path(root, approval_ref["path"], "scope_approval.path", kind="file")
    if _sha256_file(approval_path) != _require_sha256(
        approval_ref["sha256"], "scope_approval.sha256"
    ):
        raise RuntimeError("scope approval changed after experiment declaration")
    approval = _require_exact_shape(
        _read_json(approval_path), _APPROVAL_FIELDS, "scope approval document"
    )
    dimensions = _require_exact_shape(
        approval["dimensions"], _APPROVAL_DIMENSION_FIELDS, "scope approval dimensions"
    )
    if (
        approval["schema_version"] != 1
        or approval["decision"] != "APPROVE_QUALIFICATION_TARGET"
        or approval["approved_max_nodes"] != 64
        or approval["required_ladder"] != list(_REQUIRED_LADDER)
        or dimensions != _APPROVAL_DIMENSIONS
    ):
        raise RuntimeError("scope approval does not authorize the exact 4/16/64 scale contract")
    for name in ("decision_id", "approver_id", "approved_at"):
        if not isinstance(approval[name], str) or not approval[name].strip():
            raise RuntimeError(f"scope approval {name} must be non-empty text")

    paths = {
        "release": _declared_path(
            root, candidate["release_path"], "candidate.release_path", kind="directory"
        ),
        "artifact_manifest": _declared_path(
            root,
            candidate["artifact_manifest_path"],
            "candidate.artifact_manifest_path",
            kind="file",
        ),
        "wheel": _declared_path(root, candidate["wheel_path"], "candidate.wheel_path", kind="file"),
        "sdist": _declared_path(root, candidate["sdist_path"], "candidate.sdist_path", kind="file"),
        "bootstrap": _declared_path(
            root, candidate["bootstrap_path"], "candidate.bootstrap_path", kind="directory"
        ),
    }
    for name in (
        "artifact_manifest_sha256",
        "wheel_sha256",
        "sdist_sha256",
        "site_profile_hash",
        "compatibility_profile_hash",
        "compatibility_manifest_hash",
    ):
        _require_sha256(candidate[name], f"candidate.{name}")
    for path_name, digest_name in (
        ("artifact_manifest", "artifact_manifest_sha256"),
        ("wheel", "wheel_sha256"),
        ("sdist", "sdist_sha256"),
    ):
        if _sha256_file(paths[path_name]) != candidate[digest_name]:
            raise RuntimeError(f"candidate {path_name} bytes differ from the declaration")
    paths.update(
        {
            "lifecycle_support": _declared_path(
                root, support["lifecycle"]["path"], "support.lifecycle.path", kind="file"
            ),
            "scope_approval": approval_path,
        }
    )

    gates = document["gates"]
    if not isinstance(gates, list) or not gates:
        raise RuntimeError("experiment plan gates must be a non-empty list")
    normalized: list[dict] = []
    ids: list[str] = []
    for index, raw in enumerate(gates):
        gate = _require_exact_shape(raw, _GATE_FIELDS, f"experiment plan gates[{index}]")
        _validate_gate_contract(gate, context=f"experiment plan gates[{index}]")
        normalized.append(gate)
        ids.append(str(gate["gate_id"]))
    if len(ids) != len(set(ids)):
        raise RuntimeError("experiment plan gate_id values must be unique")
    cells = [gate["logical_nodes"] for gate in normalized]
    if len(cells) != len(_REQUIRED_LADDER) or set(cells) != set(_REQUIRED_LADDER):
        raise RuntimeError(
            f"scale experiment plan must declare the exact ladder {list(_REQUIRED_LADDER)}"
        )
    matches = [gate for gate in normalized if gate["gate_id"] == gate_id]
    if len(matches) != 1:
        raise RuntimeError(f"gate_id {gate_id!r} is not declared exactly once")
    gate = matches[0]

    paths.update(
        {
            "output": _declared_path(root, gate["output_path"], "gate.output_path", kind="output"),
            "config": _declared_path(root, gate["config_path"], "gate.config_path", kind="file"),
            "deployment_plan": _declared_path(
                root,
                gate["deployment_plan_path"],
                "gate.deployment_plan_path",
                kind="file",
            ),
            "site_profile": _declared_path(
                root, gate["site_profile_path"], "gate.site_profile_path", kind="file"
            ),
        }
    )
    for path_name, digest_name in (
        ("config", "config_sha256"),
        ("deployment_plan", "deployment_plan_sha256"),
        ("site_profile", "site_profile_sha256"),
    ):
        digest = _require_sha256(gate[digest_name], f"gate.{digest_name}")
        if _sha256_file(paths[path_name]) != digest:
            raise RuntimeError(f"gate {path_name} bytes differ from the declaration")
    return document, gate, paths


def _validated_scale_nodes(expected: int, *, acquisition_source: str) -> tuple[str, ...]:
    """v1's node validation, plus the batch acquisition the ladder runs under."""
    if acquisition_source != "batch_pbs":
        return lifecycle._validated_nodes(expected, acquisition_source=acquisition_source)
    job_id = os.environ.get("PBS_JOBID", "").strip()
    nodefile = os.environ.get("PBS_NODEFILE", "").strip()
    if not job_id or not nodefile or not os.path.isfile(nodefile):
        raise RuntimeError("a readable PBS_NODEFILE and PBS_JOBID are required")
    if os.environ.get("AURORA_SUBJOB") == "1":
        raise RuntimeError("batch_pbs was declared inside a subjob lease")
    if os.environ.get("PBS_ENVIRONMENT") != "PBS_BATCH":
        raise RuntimeError("batch_pbs qualification requires PBS_ENVIRONMENT=PBS_BATCH")
    with open(nodefile, encoding="utf-8") as handle:
        nodes = tuple(dict.fromkeys(line.strip() for line in handle if line.strip()))
    if len(nodes) != expected:
        raise RuntimeError(f"expected exactly {expected} leased node(s), observed {len(nodes)}")
    host = socket.gethostname().split(".", 1)[0]
    if host not in {node.split(".", 1)[0] for node in nodes}:
        raise RuntimeError(f"qualification host {host} is not inside the leased nodes {nodes}")
    if "ONEAPI_DEVICE_SELECTOR" in os.environ:
        raise RuntimeError("ONEAPI_DEVICE_SELECTOR must be absent on Aurora")
    return nodes


def _launch_scale_scenario(
    *,
    name: str,
    root: Path,
    plan_path: Path,
    site_path: Path,
    generation: int,
    fault: str,
    ready_timeout_s: float,
    nodes: tuple[str, ...],
    partial_observation_s: float,
) -> dict:
    """Forked from v1's `_launch_scenario`, generalized off two nodes.

    The two divergences from v1 are marked SCALE below. Everything else is v1's
    logic verbatim, minus the `replica_death` branch, which stays in the v1
    four-node PP cell.
    """
    from exaserve.plan.io import load_deployment_plan, load_site_profile
    from exaserve.status_api import require_ready_endpoint

    plan = load_deployment_plan(str(plan_path))
    site_profile = load_site_profile(str(site_path))
    if (
        site_profile.site_id != plan.site_profile_id
        or site_profile.site_profile_hash != plan.site_profile_hash
    ):
        raise RuntimeError("scale scenario SiteProfile does not match DeploymentPlan")
    run_dir = root / name / "deployment"
    run_dir.mkdir(parents=True)
    stdout_path = root / name / "stdout.log"
    stderr_path = root / name / "stderr.log"
    fault_evidence_path = root / name / "fault_injection.json"
    local_listener: socket.socket | None = None
    remote_holder: lifecycle._RemotePortHolder | None = None
    precondition: dict[str, object] | None = None
    if fault == "duplicate_gateway_port":
        if plan.gateway is None:
            raise RuntimeError("duplicate gateway-port scenario requires a planned gateway")
        local_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            local_listener.bind(("0.0.0.0", plan.gateway.port))
            local_listener.listen(8)
        except BaseException:
            local_listener.close()
            raise
        precondition = {
            "schema_version": 1,
            "kind": "local_gateway_port_holder",
            "node": socket.gethostname(),
            "port": plan.gateway.port,
            "pid": os.getpid(),
            "stopped": False,
        }
        lifecycle._atomic_json(fault_evidence_path, precondition)
    elif fault == "partial_proxy_readiness":
        # SCALE: v1 requires exactly two nodes and holds the port on nodes[1].
        # Hold it on the LAST leased worker instead. At two nodes that is the
        # same node v1 chose; above two it also makes the held rank the furthest
        # one from the head, so a readiness predicate that only samples nearby
        # ranks cannot pass by accident.
        if plan.gateway is None or len(nodes) < 2:
            raise RuntimeError("partial proxy-readiness scenario requires at least two nodes")
        remote_holder = lifecycle._RemotePortHolder.start(
            node=nodes[-1],
            port=plan.gateway.backend_port,
            evidence_path=fault_evidence_path,
            site_profile=site_profile,
        )
        precondition = {
            "kind": "remote_worker_proxy_port_holder",
            "node": remote_holder.node,
            "port": remote_holder.port,
            "remote_pid": remote_holder.pid,
        }
    argv = [sys.executable, "-u", "-m", "exaserve.launcher", str(plan_path)]
    lifecycle._atomic_text(root / name / "command.txt", shlex.join(argv) + "\n")
    env = os.environ.copy()
    env.update(
        {
            "EXASERVE_GENERATION": str(generation),
            "EXASERVE_DEPLOYMENT_ID": plan.deployment_id,
            "EXASERVE_RUN_LOG_DIR": str(run_dir),
            "EXASERVE_SITE_PROFILE_PATH": str(site_path),
            "EXASERVE_NODEFILE": os.environ["PBS_NODEFILE"],
            "EXASERVE_SCHEDULER": "pbs",
            "EXASERVE_VENDOR": "xpu",
        }
    )
    started = time.time()
    try:
        process = subprocess.Popen(
            argv,
            cwd=root,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            start_new_session=True,
        )
    except BaseException as exc:
        if local_listener is not None:
            local_listener.close()
        if remote_holder is not None:
            try:
                remote_holder.stop()
            except BaseException as cleanup_exc:
                lifecycle._add_note(
                    exc, f"remote fault precondition cleanup also failed: {cleanup_exc}"
                )
        raise
    assert process.stdout is not None and process.stderr is not None
    out_tee = lifecycle._Tee(process.stdout, stdout_path, f"{name}:stdout")
    err_tee = lifecycle._Tee(process.stderr, stderr_path, f"{name}:stderr")
    out_tee.start()
    err_tee.start()
    try:
        if fault == "duplicate_gateway_port":
            status = lifecycle._wait_status(
                run_dir,
                generation,
                plan.deployment_plan_hash,
                ready_timeout_s,
                process=process,
            )
            if status.ready or status.state != "FAILED":
                raise RuntimeError(
                    f"occupied gateway port did not fail closed before READY: {status.state}"
                )
            try:
                returncode = process.wait(timeout=lifecycle._owner_exit_timeout_s(plan))
            except subprocess.TimeoutExpired as exc:
                raise RuntimeError(
                    "launcher did not terminate after gateway port conflict"
                ) from exc
            if returncode == 0 or returncode == 143:
                raise RuntimeError(
                    f"gateway port conflict returned invalid launcher code {returncode}"
                )
            terminal, report = lifecycle._terminal_record(run_dir, "FAILED")
            detail = f"{terminal.reason_code}: {terminal.detail}".lower()
            if "listener" not in detail or "bind" not in detail:
                raise RuntimeError(
                    f"gateway port conflict lacks typed bind/listener evidence: {detail}"
                )
            return {
                "scenario": name,
                "passed": True,
                "fault": fault,
                "generation": generation,
                "deployment_plan_hash": plan.deployment_plan_hash,
                "advertised_endpoint": None,
                "ready_revision": None,
                "terminal_revision": terminal.revision,
                "terminal_state": terminal.state,
                "terminal_reason_code": terminal.reason_code,
                "terminal_detail": terminal.detail,
                "returncode": returncode,
                "duration_s": round(time.time() - started, 3),
                "fault_precondition": precondition,
                "shutdown_report": report,
            }

        if fault == "partial_proxy_readiness":
            observed, history, membership = lifecycle._observe_partial_readiness(
                run_dir,
                generation=generation,
                plan=plan,
                expected_nodes=nodes,
                timeout_s=ready_timeout_s,
                observation_s=partial_observation_s,
                process=process,
            )
            lifecycle._atomic_json(root / name / "status_history.json", history)
            if observed.terminal:
                if observed.state != "FAILED":
                    raise RuntimeError(
                        f"partial readiness ended in unexpected state {observed.state}"
                    )
                expected_state = "FAILED"
                expected_codes = set(range(1, 256)) - {143}
                cancelled_after_observation = False
            else:
                process.send_signal(signal.SIGTERM)
                expected_state = "CANCELLED"
                expected_codes = {143}
                cancelled_after_observation = True
            try:
                returncode = process.wait(timeout=lifecycle._owner_exit_timeout_s(plan))
            except subprocess.TimeoutExpired as exc:
                raise RuntimeError("launcher did not terminate after partial readiness") from exc
            if returncode not in expected_codes:
                raise RuntimeError(
                    f"partial-readiness launcher code {returncode} is invalid for "
                    f"terminal {expected_state}"
                )
            terminal, report = lifecycle._terminal_record(
                run_dir,
                expected_state,
                require_graceful_deployment=cancelled_after_observation,
            )
            if terminal.ready:
                raise RuntimeError("partial-readiness terminal record incorrectly remains ready")
            return {
                "scenario": name,
                "passed": True,
                "fault": fault,
                "generation": generation,
                "deployment_plan_hash": plan.deployment_plan_hash,
                "advertised_endpoint": None,
                "ready_revision": None,
                "terminal_revision": terminal.revision,
                "terminal_state": terminal.state,
                "terminal_reason_code": terminal.reason_code,
                "terminal_detail": terminal.detail,
                "returncode": returncode,
                "duration_s": round(time.time() - started, 3),
                "fault_precondition": precondition,
                "membership_evidence": membership,
                "status_history": history,
                "observation_s": partial_observation_s,
                "cancelled_after_observation": cancelled_after_observation,
                "held_node": nodes[-1],
                "shutdown_report": report,
            }

        status = lifecycle._wait_status(
            run_dir,
            generation,
            plan.deployment_plan_hash,
            ready_timeout_s,
            process=process,
        )
        if not status.ready:
            cleanup = lifecycle._await_premature_terminal_cleanup(process, run_dir, status, plan)
            raise RuntimeError(
                f"deployment became terminal before READY: {status.state} "
                f"{status.reason_code}: {status.detail}; cleanup={cleanup}"
            )
        endpoint = require_ready_endpoint(
            str(run_dir),
            expected_generation=generation,
            expected_plan_hash=plan.deployment_plan_hash,
        )
        canary = [lifecycle._canary(endpoint, model.model_id) for model in plan.models]
        evidence = lifecycle._validate_ready_evidence(status, plan, run_dir)
        manifest = lifecycle._read_json(Path(status.receipt_manifest_path))
        lifecycle._atomic_json(root / name / "canary.json", canary)
        fault_injection = None

        if fault == "operator_drain":
            process.send_signal(signal.SIGTERM)
            expected_state = "STOPPED"
            expected_codes = {143}
        elif fault == "gateway_death":
            gateway_pid = lifecycle._owned_gateway_pid(manifest, plan)
            exact_signal = lifecycle._signal_local_generation_pid(
                gateway_pid,
                "TERM",
                deployment_id=plan.deployment_id,
                generation=generation,
                plan_hash=plan.deployment_plan_hash,
                run_dir=run_dir,
            )
            fault_injection = {
                "kind": "owned_gateway_death",
                "node": nodes[0],
                "pid": gateway_pid,
                "signal": exact_signal,
            }
            lifecycle._atomic_json(fault_evidence_path, fault_injection)
            expected_state = "FAILED"
            expected_codes = set(range(1, 256)) - {143}
        elif fault == "worker_death":
            # SCALE: v1 kills rank 1 and requires exactly two nodes. Kill the
            # HIGHEST planned worker rank instead. At two nodes that is rank 1,
            # exactly as v1 does; above two it exercises the rank whose session,
            # receipts and cleanup travel furthest through the control plane.
            from exaserve.plan.contracts import same_node
            from exaserve.status_api import load_status_allocation_binding

            binding = load_status_allocation_binding(str(run_dir), status)
            worker_rank = len(nodes) - 1
            target = lifecycle._owned_worker_target(manifest, binding, worker_rank=worker_rank)
            if len(nodes) < 2 or not same_node(target["node"], nodes[worker_rank]):
                raise RuntimeError(
                    f"exact rank-{worker_rank} worker {target} is outside the "
                    f"leased workers {nodes[1:]}"
                )
            remote_signal = lifecycle._run_remote_generation_signal(
                target["node"],
                target["pid"],
                "TERM",
                deployment_id=plan.deployment_id,
                generation=generation,
                plan_hash=plan.deployment_plan_hash,
                run_dir=run_dir,
            )
            fault_injection = {
                "kind": "owned_ray_worker_death",
                "target": target,
                "worker_rank": worker_rank,
                "signal": remote_signal,
            }
            lifecycle._atomic_json(fault_evidence_path, fault_injection)
            expected_state = "FAILED"
            expected_codes = set(range(1, 256)) - {143}
        else:
            raise RuntimeError(f"unknown scale fault scenario {fault!r}")

        try:
            returncode = process.wait(timeout=lifecycle._owner_exit_timeout_s(plan))
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(f"launcher did not terminate after {fault}") from exc
        if returncode not in expected_codes:
            raise RuntimeError(
                f"launcher return code {returncode} is invalid for {fault}; "
                f"expected one of {sorted(expected_codes)}"
            )
        terminal, report = lifecycle._terminal_record(
            run_dir,
            expected_state,
            require_graceful_deployment=fault in {"operator_drain", "gateway_death"},
        )
        if fault == "gateway_death":
            gateway_evidence = lifecycle._read_json(run_dir / "gateway_failure.json")
            if gateway_evidence.get("classification") != "process_dead":
                raise RuntimeError(f"gateway death was not classified exactly: {gateway_evidence}")
        else:
            gateway_evidence = None
        if fault == "worker_death":
            causal_text = f"{terminal.reason_code}: {terminal.detail}".lower()
            expected_rank = len(nodes) - 1
            if f"rank {expected_rank}" not in causal_text or not any(
                word in causal_text for word in ("worker", "ray")
            ):
                raise RuntimeError(
                    f"worker death lacks an exact rank-{expected_rank} worker/ray cause: "
                    f"{causal_text}"
                )
        return {
            "scenario": name,
            "passed": True,
            "fault": fault,
            "generation": generation,
            "deployment_plan_hash": plan.deployment_plan_hash,
            "advertised_endpoint": endpoint,
            "ready_revision": status.revision,
            "terminal_revision": terminal.revision,
            "terminal_state": terminal.state,
            "terminal_reason_code": terminal.reason_code,
            "terminal_detail": terminal.detail,
            "returncode": returncode,
            "duration_s": round(time.time() - started, 3),
            "ready_evidence": evidence,
            "canary": canary,
            "fault_injection": fault_injection,
            "shutdown_report": report,
            "gateway_failure": gateway_evidence,
        }
    finally:
        active_error = sys.exc_info()[1]
        cleanup_errors: list[BaseException] = []
        owner_cleanup_s = float(plan.control.watchdog_cleanup_deadline_s)
        cleanup_deadline = time.monotonic() + max(60.0, owner_cleanup_s + 60.0)
        if process.poll() is None:
            try:
                process.send_signal(signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                if process.poll() is None:
                    process.wait(
                        timeout=max(
                            0.0,
                            min(owner_cleanup_s + 10.0, cleanup_deadline - time.monotonic()),
                        )
                    )
            except subprocess.TimeoutExpired:
                pass
            except BaseException as cleanup_exc:
                cleanup_errors.append(cleanup_exc)
        if lifecycle._process_group_exists(process.pid):
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            term_deadline = min(cleanup_deadline, time.monotonic() + 10.0)
            while lifecycle._process_group_exists(process.pid) and time.monotonic() < term_deadline:
                time.sleep(0.1)
            if lifecycle._process_group_exists(process.pid):
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                kill_deadline = min(cleanup_deadline, time.monotonic() + 10.0)
                while (
                    lifecycle._process_group_exists(process.pid)
                    and time.monotonic() < kill_deadline
                ):
                    time.sleep(0.1)
        if process.poll() is None:
            try:
                process.wait(timeout=max(0.0, cleanup_deadline - time.monotonic()))
            except BaseException as cleanup_exc:
                cleanup_errors.append(cleanup_exc)
        try:
            generation_cleanup = lifecycle._cleanup_generation_on_nodes(
                nodes,
                deployment_id=plan.deployment_id,
                generation=generation,
                plan_hash=plan.deployment_plan_hash,
                run_dir=run_dir,
            )
            lifecycle._atomic_json(
                root / name / "exact_generation_cleanup.json",
                {"schema_version": 1, "reports": generation_cleanup},
            )
            reaped = [
                {"hostname": report["hostname"], "process": process_identity}
                for report in generation_cleanup
                for process_identity in report["matched"]
            ]
            if reaped:
                cleanup_errors.append(
                    RuntimeError(
                        "qualification fallback had to reap exact generation "
                        f"processes left by the owner: {reaped}"
                    )
                )
        except BaseException as cleanup_exc:
            cleanup_errors.append(cleanup_exc)
        for tee in (out_tee, err_tee):
            try:
                tee.join(deadline=cleanup_deadline)
            except BaseException as cleanup_exc:
                cleanup_errors.append(cleanup_exc)
        if lifecycle._process_group_exists(process.pid):
            cleanup_errors.append(
                RuntimeError(f"qualification launcher process group {process.pid} survived cleanup")
            )
        if local_listener is not None:
            try:
                local_listener.close()
                assert precondition is not None
                precondition["stopped"] = True
                lifecycle._atomic_json(fault_evidence_path, precondition)
            except BaseException as cleanup_exc:
                cleanup_errors.append(cleanup_exc)
        if remote_holder is not None:
            try:
                remote_holder.stop()
            except BaseException as cleanup_exc:
                cleanup_errors.append(cleanup_exc)
        if cleanup_errors:
            if active_error is not None:
                for cleanup_error in cleanup_errors:
                    lifecycle._add_note(
                        active_error,
                        f"qualification scenario cleanup also failed: {cleanup_error}",
                    )
            else:
                primary_cleanup = cleanup_errors[0]
                for cleanup_error in cleanup_errors[1:]:
                    lifecycle._add_note(
                        primary_cleanup, f"additional cleanup failure: {cleanup_error}"
                    )
                raise primary_cleanup


def _verdict_text(
    passed: bool,
    scenarios: list[dict],
    *,
    gate_id: str,
    logical_nodes: int,
    density: dict,
    error: str = "",
) -> str:
    lines = [f"# {gate_id} verdict", "", f"Verdict: **{'PASS' if passed else 'FAIL'}**", ""]
    lines.append(
        f"Topology: {logical_nodes} nodes x {density['num_gpus_per_node']} tiles = "
        f"{density['replicas']} real vLLM replicas, "
        f"{density['serve_applications']} Serve applications, "
        f"{density['receipt_requirements']} receipt slots."
    )
    lines.append("")
    for item in scenarios:
        lines.append(
            f"- `{item['scenario']}`: terminal `{item['terminal_state']}`, "
            f"exit `{item['returncode']}`, {item['duration_s']} s"
        )
    if error:
        lines.extend(["", "Failure:", "", "```text", error.rstrip(), "```"])
    lines.extend(
        [
            "",
            f"This verdict qualifies only the {logical_nodes}-node dense "
            "Aurora/XPU/vLLM-real/HAProxy scale cell identified by the adjacent",
            "immutable plan, wheel hash, and PBS receipt. It does not qualify another engine",
            "mode, a higher scale, a different density, or product support scope.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    global lifecycle

    parser = argparse.ArgumentParser(description="ExaServe dense scale-ladder qualification")
    parser.add_argument("--experiment-plan", required=True)
    parser.add_argument("--gate-id", required=True)
    args = parser.parse_args()

    gate_id = args.gate_id.strip()
    if not gate_id or any(
        character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for character in gate_id
    ):
        parser.error("--gate-id must contain only uppercase ASCII letters, digits, '-' and '_'")
    experiment_plan_path = Path(args.experiment_plan).resolve()
    try:
        experiment_plan, declared_gate, declared_paths = _load_scale_gate(
            experiment_plan_path,
            gate_id,
            harness_path=Path(__file__).resolve(),
        )
    except RuntimeError as exc:
        parser.error(str(exc))
    try:
        lifecycle = _load_verified_module(
            declared_paths["lifecycle_support"],
            experiment_plan["support"]["lifecycle"]["sha256"],
        )
    except BaseException as exc:
        parser.error(f"verified lifecycle support could not be imported: {exc}")
    output = declared_paths["output"]
    bootstrap = declared_paths["bootstrap"]
    wheel = declared_paths["wheel"]
    config_path = declared_paths["config"]
    plan_path = declared_paths["deployment_plan"]
    site_path = declared_paths["site_profile"]
    logical_nodes = declared_gate["logical_nodes"]
    engine_mode = declared_gate["engine_mode"]
    scenario_profile = declared_gate["scenario_profile"]
    ready_timeout = float(declared_gate["ready_timeout_s"])
    partial_observation = float(declared_gate["partial_observation_s"])

    current = [entry for entry in sys.path if entry]
    if str(bootstrap) not in current:
        raise SystemExit(f"PYTHONPATH must include immutable bootstrap {bootstrap}")
    lifecycle._pin_bootstrap_environment(bootstrap)

    try:
        output.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        parser.error(f"output must be a new directory so evidence cannot be overwritten: {output}")

    scenarios: list[dict] = []
    started = time.time()
    density: dict = {}
    try:
        from exaserve.plan.compiler import compile_deployment_plan
        from exaserve.plan.io import load_deployment_plan, load_site_profile
        from exaserve.yaml_support import load_yaml_mapping

        plan = load_deployment_plan(str(plan_path))
        profile = load_site_profile(str(site_path))
        candidate = experiment_plan["candidate"]
        rebuilt = compile_deployment_plan(
            load_yaml_mapping(config_path),
            site=profile,
            deployment_id=gate_id.lower(),
            compatibility_profile_hash=candidate["compatibility_profile_hash"],
            manifest_hash=candidate["compatibility_manifest_hash"],
        )
        if rebuilt.deployment_plan_hash != plan.deployment_plan_hash:
            raise RuntimeError("compiled config does not reproduce the declared deployment plan")
        _validate_candidate_plan_identity(plan, profile, candidate, gate_id=gate_id)
        density = _validate_scale_plan(
            plan, engine_mode=engine_mode, scenario_profile=scenario_profile
        )
        if plan.num_nodes != logical_nodes:
            raise RuntimeError(
                f"plan declares {plan.num_nodes} nodes, gate declares {logical_nodes}"
            )
        nodes = _validated_scale_nodes(
            logical_nodes, acquisition_source=declared_gate["acquisition_source"]
        )
        environment = lifecycle._environment_receipt(nodes, bootstrap, wheel, gate_id=gate_id)
        if len(nodes) != declared_gate["physical_allocation_nodes"]:
            raise RuntimeError("physical allocation size differs from the declared gate")
        if environment["pbs_queue"] != declared_gate["queue"]:
            raise RuntimeError(
                f"PBS queue {environment['pbs_queue']!r} differs from declared "
                f"queue {declared_gate['queue']!r}"
            )
        lifecycle._atomic_json(output / "environment.json", environment)
        manifest = {
            "schema_version": 1,
            "gate_id": gate_id,
            "lane": declared_gate["lane"],
            "experiment_plan_path": str(experiment_plan_path),
            "experiment_plan_sha256": lifecycle._sha256_file(experiment_plan_path),
            "declared_gate": declared_gate,
            "started_at": started,
            "logical_nodes": logical_nodes,
            "physical_allocation_nodes": len(nodes),
            "acquisition_source": declared_gate["acquisition_source"],
            "pbs_job_id": environment["pbs_job_id"],
            "queue": environment["pbs_queue"],
            "lease_ttl": declared_gate["lease_ttl"].strip(),
            "expected_runtime": declared_gate["expected_runtime"].strip(),
            "engine_mode": engine_mode,
            "scenario_profile": scenario_profile,
            "density": density,
            "ready_timeout_s": ready_timeout,
            "partial_observation_s": partial_observation,
            "attempt_limit": declared_gate["attempt_limit"],
            "attempt": declared_gate["attempt"],
            "deployment_plan_path": str(plan_path),
            "deployment_plan_hash": plan.deployment_plan_hash,
            "config_path": str(config_path),
            "config_sha256": _sha256_file(config_path),
            "site_profile_path": str(site_path),
            "site_profile_hash": profile.site_profile_hash,
            "wheel": str(wheel),
            "wheel_sha256": environment["wheel_sha256"],
            "bootstrap": str(bootstrap),
            "harness": str(Path(__file__).resolve()),
            "harness_sha256": _sha256_file(Path(__file__).resolve()),
            "lifecycle_support": str(declared_paths["lifecycle_support"]),
            "lifecycle_support_sha256": _sha256_file(declared_paths["lifecycle_support"]),
            "scope_approval": str(declared_paths["scope_approval"]),
            "scope_approval_sha256": _sha256_file(declared_paths["scope_approval"]),
            "expected_observations": list(declared_gate["expected_observations"]),
            "candidate": experiment_plan["candidate"],
        }
        lifecycle._atomic_json(output / "manifest.json", manifest)

        base_generation = time.time_ns()
        for offset, (name, fault) in enumerate(_scenario_matrix(scenario_profile)):
            scenarios.append(
                _launch_scale_scenario(
                    name=name,
                    root=output,
                    plan_path=plan_path,
                    site_path=site_path,
                    generation=base_generation + offset,
                    fault=fault,
                    ready_timeout_s=ready_timeout,
                    nodes=nodes,
                    partial_observation_s=partial_observation,
                )
            )
        result = {
            **manifest,
            "passed": True,
            "completed_at": time.time(),
            "duration_s": round(time.time() - started, 3),
            "scenarios": scenarios,
        }
        lifecycle._atomic_json(output / "result.json", result)
        lifecycle._atomic_text(
            output / "verdict.md",
            _verdict_text(
                True,
                scenarios,
                gate_id=gate_id,
                logical_nodes=logical_nodes,
                density=density,
            ),
        )
        print(f"QUALIFICATION_VERDICT|gate={gate_id}|PASS", flush=True)
        return 0
    except BaseException as exc:
        error = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        result = {
            "schema_version": 1,
            "gate_id": gate_id,
            "lane": declared_gate["lane"],
            "experiment_plan_path": str(experiment_plan_path),
            "experiment_plan_sha256": lifecycle._sha256_file(experiment_plan_path),
            "declared_gate": declared_gate,
            "attempt_limit": declared_gate["attempt_limit"],
            "attempt": declared_gate["attempt"],
            "density": density,
            "passed": False,
            "completed_at": time.time(),
            "duration_s": round(time.time() - started, 3),
            "scenarios": scenarios,
            "error": error,
        }
        lifecycle._atomic_json(output / "result.json", result)
        lifecycle._atomic_text(
            output / "verdict.md",
            _verdict_text(
                False,
                scenarios,
                gate_id=gate_id,
                logical_nodes=logical_nodes,
                density=density
                or {
                    "num_gpus_per_node": 0,
                    "replicas": 0,
                    "serve_applications": 0,
                    "receipt_requirements": 0,
                },
                error=error,
            ),
        )
        print(
            f"QUALIFICATION_VERDICT|gate={gate_id}|FAIL|{type(exc).__name__}: {exc}",
            flush=True,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
