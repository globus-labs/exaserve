#!/usr/bin/env python3
"""Canonical AC-SUP-01 cause adjudication over the pinned fault runtime.

The v1 fault runtime owns hardware execution and cleanup.  This wrapper writes
and binds a deterministic nested v1 declaration, runs it once, then applies
the stricter canonical contract to its immutable result.  In particular, the
public status reason remains ``FIRST_CAUSE`` while its detail must preserve the
rank-local ``UNEXPECTED_EXIT`` or authenticated control-session loss.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time
import traceback

from scripts.hardening import run_final_null_qualification as lifecycle
from scripts.hardening import run_supervisor_fault_qualification as fault_runtime


_PLAN_FIELDS = {"schema_version", "created_at", "candidate", "harness", "support", "gates"}
_SUPPORT_FIELDS = {"lifecycle", "fault_runtime"}
_SCENARIOS = ["head-ray-child-death", "worker-supervisor-death"]
_EXPECTED_OBSERVATIONS = [
    "each fresh generation reaches canonical READY with two exact rank owners",
    "the exact SELF-attested rank-0 Ray head child is killed without name matching",
    "the exact SELF-attested rank-1 node supervisor is killed without name matching",
    "the head-Ray FIRST_CAUSE detail preserves rank 0 component ray exit=137",
    "the worker-supervisor FIRST_CAUSE detail preserves rank 1 authenticated control loss without GOODBYE",
    "each injected death returns nonzero but not 143 and publishes one FAILED terminal record",
    "owner cleanup is bounded and exact-generation fallback has zero matches, signals, and survivors",
]


def _load_gate(experiment_path: Path, gate_id: str) -> tuple[dict, dict, dict[str, Path]]:
    root = Path(__file__).resolve().parents[2]
    document = fault_runtime._exact(
        fault_runtime._load_json(experiment_path), _PLAN_FIELDS, "experiment plan"
    )
    if document["schema_version"] != 3:
        raise RuntimeError("canonical supervisor experiment plan schema_version must be 3")
    candidate = fault_runtime._exact(
        document["candidate"], fault_runtime._CANDIDATE_FIELDS, "candidate"
    )
    harness = fault_runtime._exact(document["harness"], fault_runtime._CODE_FIELDS, "harness")
    support = fault_runtime._exact(document["support"], _SUPPORT_FIELDS, "support")
    lifecycle_support = fault_runtime._exact(
        support["lifecycle"], fault_runtime._CODE_FIELDS, "support.lifecycle"
    )
    fault_support = fault_runtime._exact(
        support["fault_runtime"], fault_runtime._CODE_FIELDS, "support.fault_runtime"
    )
    harness_path = fault_runtime._path(root, harness["path"], "harness.path", kind="file")
    lifecycle_path = fault_runtime._path(
        root, lifecycle_support["path"], "support.lifecycle.path", kind="file"
    )
    fault_path = fault_runtime._path(
        root, fault_support["path"], "support.fault_runtime.path", kind="file"
    )
    if harness_path != Path(__file__).resolve():
        raise RuntimeError("experiment plan does not name the running canonical harness")
    if lifecycle_path != Path(lifecycle.__file__).resolve():
        raise RuntimeError("experiment plan does not name the lifecycle support module")
    if fault_path != Path(fault_runtime.__file__).resolve():
        raise RuntimeError("experiment plan does not name the fault runtime module")
    for label, path, declared in (
        ("canonical supervisor harness", harness_path, harness["sha256"]),
        ("lifecycle support", lifecycle_path, lifecycle_support["sha256"]),
        ("fault runtime support", fault_path, fault_support["sha256"]),
    ):
        if lifecycle._sha256_file(path) != declared:
            raise RuntimeError(f"{label} changed after experiment declaration")

    paths = {
        "release": fault_runtime._path(
            root, candidate["release_path"], "candidate.release_path", kind="directory"
        ),
        "artifact_manifest": fault_runtime._path(
            root,
            candidate["artifact_manifest_path"],
            "candidate.artifact_manifest_path",
            kind="file",
        ),
        "wheel": fault_runtime._path(
            root, candidate["wheel_path"], "candidate.wheel_path", kind="file"
        ),
        "sdist": fault_runtime._path(
            root, candidate["sdist_path"], "candidate.sdist_path", kind="file"
        ),
        "bootstrap": fault_runtime._path(
            root, candidate["bootstrap_path"], "candidate.bootstrap_path", kind="directory"
        ),
    }
    for field, path in (
        ("artifact_manifest_sha256", paths["artifact_manifest"]),
        ("wheel_sha256", paths["wheel"]),
        ("sdist_sha256", paths["sdist"]),
    ):
        if lifecycle._sha256_file(path) != candidate[field]:
            raise RuntimeError(f"candidate bytes changed for {field}")

    gates = document["gates"]
    if not isinstance(gates, list) or any(not isinstance(row, dict) for row in gates):
        raise RuntimeError("experiment gates must be a list of objects")
    matches = [row for row in gates if row.get("gate_id") == gate_id]
    if len(matches) != 1 or len({row.get("gate_id") for row in gates}) != len(gates):
        raise RuntimeError(f"experiment plan does not contain one unique gate {gate_id!r}")
    gate = fault_runtime._exact(matches[0], fault_runtime._GATE_FIELDS, f"gate {gate_id}")
    if (
        gate["lane"] != "FINAL"
        or gate["logical_nodes"] != 2
        or gate["physical_allocation_nodes"] != 2
        or gate["acquisition_source"] != "subjob"
        or gate["queue"] != "capacity"
        or gate["attempt"] != 1
        or gate["attempt_limit"] != 1
        or gate["scenarios"] != _SCENARIOS
        or gate["expected_observations"] != _EXPECTED_OBSERVATIONS
    ):
        raise RuntimeError("gate is outside the exact canonical supervisor-fault contract")
    ready_timeout = gate["ready_timeout_s"]
    if (
        isinstance(ready_timeout, bool)
        or not isinstance(ready_timeout, (int, float))
        or ready_timeout < 1
    ):
        raise RuntimeError("ready_timeout_s must be a positive number")
    paths.update(
        {
            "output": fault_runtime._path(
                root, gate["output_path"], "gate.output_path", kind="output"
            ),
            "config": fault_runtime._path(
                root, gate["config_path"], "gate.config_path", kind="file"
            ),
            "deployment_plan": fault_runtime._path(
                root,
                gate["deployment_plan_path"],
                "gate.deployment_plan_path",
                kind="file",
            ),
            "site_profile": fault_runtime._path(
                root, gate["site_profile_path"], "gate.site_profile_path", kind="file"
            ),
        }
    )
    for field, path in (
        ("config_sha256", paths["config"]),
        ("deployment_plan_sha256", paths["deployment_plan"]),
        ("site_profile_sha256", paths["site_profile"]),
    ):
        if lifecycle._sha256_file(path) != gate[field]:
            raise RuntimeError(f"declared input bytes changed for {field}")
    return document, gate, paths


def _require_typed_first_cause(scenario: str, result: dict) -> None:
    if result.get("terminal_state") != "FAILED":
        raise RuntimeError(f"{scenario} did not publish FAILED")
    if result.get("terminal_reason_code") != "FIRST_CAUSE":
        raise RuntimeError(f"{scenario} did not publish the canonical FIRST_CAUSE reason")
    detail = result.get("terminal_detail")
    if not isinstance(detail, str):
        raise RuntimeError(f"{scenario} terminal detail is not text")
    common = ("rank_launcher: UNEXPECTED_EXIT (exit=143)", "rank launcher exit=143")
    if scenario == "head-ray-child-death":
        required = (*common, "rank 0 component ray: exit=137")
    elif scenario == "worker-supervisor-death":
        required = (
            *common,
            "authenticated rank control session disappeared without GOODBYE",
            "rank(s) [1]",
        )
    else:
        raise RuntimeError(f"unknown canonical supervisor scenario {scenario!r}")
    missing = [term for term in required if term not in detail]
    if missing:
        raise RuntimeError(
            f"{scenario} did not preserve its exact first cause; missing={missing}: {detail}"
        )


def _require_zero_fallback(output: Path, scenario: str) -> dict:
    path = output / "runtime" / scenario / "exact_generation_cleanup.json"
    cleanup = fault_runtime._load_json(path)
    reports = cleanup.get("reports")
    if not isinstance(reports, list) or len(reports) != 2:
        raise RuntimeError(f"{scenario} lacks two exact-generation cleanup reports")
    for report in reports:
        if not isinstance(report, dict):
            raise RuntimeError(f"{scenario} has a malformed cleanup report")
        for field in ("matched", "signals", "survivors"):
            if report.get(field) != []:
                raise RuntimeError(
                    f"{scenario} fallback {field} is not empty on {report.get('hostname')}: "
                    f"{report.get(field)}"
                )
    return cleanup


def _derived_runtime_plan(document: dict, gate: dict, output: Path) -> dict:
    root = Path(__file__).resolve().parents[2]
    runtime_gate = dict(gate)
    try:
        runtime_gate["output_path"] = str((output / "runtime").resolve().relative_to(root))
    except ValueError as exc:
        raise RuntimeError("supervisor runtime output escapes the repository") from exc
    runtime_gate["expected_observations"] = list(fault_runtime._EXPECTED_OBSERVATIONS)
    return {
        "schema_version": 1,
        "created_at": document["created_at"],
        "candidate": document["candidate"],
        "harness": {
            "path": str(Path(fault_runtime.__file__).resolve().relative_to(root)),
            "sha256": lifecycle._sha256_file(Path(fault_runtime.__file__).resolve()),
        },
        "support": {
            "path": str(Path(lifecycle.__file__).resolve().relative_to(root)),
            "sha256": lifecycle._sha256_file(Path(lifecycle.__file__).resolve()),
        },
        "gates": [runtime_gate],
    }


def _run_runtime(plan_path: Path, gate_id: str) -> int:
    previous = sys.argv
    try:
        sys.argv = [
            str(Path(fault_runtime.__file__).resolve()),
            "--experiment-plan",
            str(plan_path),
            "--gate-id",
            gate_id,
        ]
        return fault_runtime.main()
    finally:
        sys.argv = previous


def _run(experiment_path: Path, gate_id: str) -> int:
    document, gate, paths = _load_gate(experiment_path, gate_id)
    output = paths["output"]
    try:
        output.mkdir(parents=True, exist_ok=False)
    except FileExistsError as exc:
        raise RuntimeError(f"output must be new and immutable: {output}") from exc
    bootstrap = paths["bootstrap"]
    if str(bootstrap) not in [entry for entry in sys.path if entry]:
        raise RuntimeError(f"PYTHONPATH must include immutable bootstrap {bootstrap}")
    lifecycle._pin_bootstrap_environment(bootstrap)

    started = time.time()
    runtime_plan_path = output / "runtime-experiment-plan.json"
    runtime_plan = _derived_runtime_plan(document, gate, output)
    lifecycle._atomic_json(runtime_plan_path, runtime_plan)
    try:
        runtime_returncode = _run_runtime(runtime_plan_path, gate_id)
        runtime_result_path = output / "runtime" / "result.json"
        runtime_result = fault_runtime._load_json(runtime_result_path)
        if runtime_returncode != 0 or runtime_result.get("passed") is not True:
            raise RuntimeError(
                f"pinned fault runtime failed with {runtime_returncode}: "
                f"{runtime_result.get('error', 'no error detail')}"
            )
        scenarios = runtime_result.get("scenarios")
        if (
            not isinstance(scenarios, list)
            or [row.get("scenario") for row in scenarios] != _SCENARIOS
        ):
            raise RuntimeError("pinned fault runtime returned the wrong scenario set/order")
        cleanups = {}
        for scenario in scenarios:
            name = scenario["scenario"]
            _require_typed_first_cause(name, scenario)
            cleanups[name] = _require_zero_fallback(output, name)
        manifest = {
            "schema_version": 3,
            "gate_id": gate_id,
            "lane": gate["lane"],
            "experiment_plan_path": str(experiment_path),
            "experiment_plan_sha256": lifecycle._sha256_file(experiment_path),
            "declared_gate": gate,
            "candidate": document["candidate"],
            "harness": str(Path(__file__).resolve()),
            "harness_sha256": lifecycle._sha256_file(Path(__file__).resolve()),
            "runtime_experiment_plan_path": str(runtime_plan_path),
            "runtime_experiment_plan_sha256": lifecycle._sha256_file(runtime_plan_path),
            "runtime_result_path": str(runtime_result_path),
            "runtime_result_sha256": lifecycle._sha256_file(runtime_result_path),
            "started_at": started,
        }
        lifecycle._atomic_json(output / "manifest.json", manifest)
        result = {
            **manifest,
            "passed": True,
            "completed_at": time.time(),
            "duration_s": round(time.time() - started, 3),
            "scenarios": scenarios,
            "exact_generation_cleanup": cleanups,
        }
        lifecycle._atomic_json(output / "result.json", result)
        lifecycle._atomic_text(
            output / "verdict.md",
            f"# {gate_id} verdict\n\nVerdict: **PASS**\n\n"
            + "\n".join(
                f"- `{item['scenario']}`: exact `FIRST_CAUSE`, "
                "zero fallback matches/signals/survivors"
                for item in scenarios
            )
            + "\n",
        )
        print(f"QUALIFICATION_VERDICT|gate={gate_id}|PASS", flush=True)
        return 0
    except BaseException as exc:
        error = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        lifecycle._atomic_json(
            output / "result.json",
            {
                "schema_version": 3,
                "gate_id": gate_id,
                "lane": gate["lane"],
                "experiment_plan_path": str(experiment_path),
                "experiment_plan_sha256": lifecycle._sha256_file(experiment_path),
                "declared_gate": gate,
                "attempt": gate["attempt"],
                "attempt_limit": gate["attempt_limit"],
                "passed": False,
                "completed_at": time.time(),
                "duration_s": round(time.time() - started, 3),
                "error": error,
            },
        )
        lifecycle._atomic_text(
            output / "verdict.md",
            f"# {gate_id} verdict\n\nVerdict: **FAIL**\n\n```text\n{error.rstrip()}\n```\n",
        )
        print(f"QUALIFICATION_VERDICT|gate={gate_id}|FAIL", flush=True)
        return 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment-plan", required=True)
    parser.add_argument("--gate-id", required=True)
    args = parser.parse_args()
    gate_id = args.gate_id.strip()
    if not gate_id or any(char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for char in gate_id):
        parser.error("--gate-id must contain uppercase ASCII letters, digits, '-' and '_'")
    try:
        return _run(Path(args.experiment_plan).resolve(), gate_id)
    except RuntimeError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    raise SystemExit(main())
