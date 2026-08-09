#!/usr/bin/env python3
"""Declared two-node process-ownership fault qualification for AC-SUP-01."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys
import time
import traceback

from scripts.hardening import run_final_null_qualification as lifecycle


_PLAN_FIELDS = {"schema_version", "created_at", "candidate", "harness", "support", "gates"}
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
    "ready_timeout_s",
    "config_path",
    "config_sha256",
    "deployment_plan_path",
    "deployment_plan_sha256",
    "site_profile_path",
    "site_profile_sha256",
    "scenarios",
    "clean_state_reset_method",
    "retry_reason_policy",
    "expected_observations",
}
_SCENARIOS = ["head-ray-child-death", "worker-supervisor-death"]
_EXPECTED_OBSERVATIONS = [
    "each fresh generation reaches canonical READY with two exact rank owners",
    "the exact SELF-attested rank-0 Ray head child is killed without name matching",
    "the exact SELF-attested rank-1 node supervisor is killed without name matching",
    "each injected death preserves one typed first cause and returns nonzero but not 143",
    "each failure publishes one FAILED terminal record with clean bounded owner cleanup",
    "exact-generation fallback finds no process left for it to signal or reap",
]


def _load_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError(f"expected a JSON object: {path}")
    return value


def _exact(value: object, fields: set[str], context: str) -> dict:
    if not isinstance(value, dict) or set(value) != fields:
        observed = set(value) if isinstance(value, dict) else set()
        raise RuntimeError(
            f"{context} shape mismatch: unknown={sorted(observed - fields)}, "
            f"missing={sorted(fields - observed)}"
        )
    return value


def _path(root: Path, value: object, context: str, *, kind: str) -> Path:
    if not isinstance(value, str) or not value or Path(value).is_absolute():
        raise RuntimeError(f"{context} must be a non-empty repository-relative path")
    resolved = (root / value).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise RuntimeError(f"{context} escapes the repository") from exc
    if kind == "file" and not resolved.is_file():
        raise RuntimeError(f"{context} is not a file: {resolved}")
    if kind == "directory" and not resolved.is_dir():
        raise RuntimeError(f"{context} is not a directory: {resolved}")
    if kind not in {"file", "directory", "output"}:
        raise AssertionError(f"unsupported path kind {kind!r}")
    return resolved


def _load_gate(experiment_path: Path, gate_id: str) -> tuple[dict, dict, dict[str, Path]]:
    root = Path(__file__).resolve().parents[2]
    document = _exact(_load_json(experiment_path), _PLAN_FIELDS, "experiment plan")
    if document["schema_version"] != 1:
        raise RuntimeError("supervisor experiment plan schema_version must be 1")
    candidate = _exact(document["candidate"], _CANDIDATE_FIELDS, "candidate")
    harness = _exact(document["harness"], _CODE_FIELDS, "harness")
    support = _exact(document["support"], _CODE_FIELDS, "support")
    harness_path = _path(root, harness["path"], "harness.path", kind="file")
    support_path = _path(root, support["path"], "support.path", kind="file")
    if harness_path != Path(__file__).resolve():
        raise RuntimeError("experiment plan does not name the running supervisor harness")
    if support_path != Path(lifecycle.__file__).resolve():
        raise RuntimeError("experiment plan does not name the lifecycle support module")
    if lifecycle._sha256_file(harness_path) != harness["sha256"]:
        raise RuntimeError("supervisor harness changed after experiment declaration")
    if lifecycle._sha256_file(support_path) != support["sha256"]:
        raise RuntimeError("lifecycle support changed after experiment declaration")

    paths = {
        "release": _path(
            root, candidate["release_path"], "candidate.release_path", kind="directory"
        ),
        "artifact_manifest": _path(
            root,
            candidate["artifact_manifest_path"],
            "candidate.artifact_manifest_path",
            kind="file",
        ),
        "wheel": _path(root, candidate["wheel_path"], "candidate.wheel_path", kind="file"),
        "sdist": _path(root, candidate["sdist_path"], "candidate.sdist_path", kind="file"),
        "bootstrap": _path(
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
    gate = _exact(matches[0], _GATE_FIELDS, f"gate {gate_id}")
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
        raise RuntimeError("gate is outside the exact supervisor-fault contract")
    ready_timeout = gate["ready_timeout_s"]
    if (
        isinstance(ready_timeout, bool)
        or not isinstance(ready_timeout, (int, float))
        or ready_timeout < 1
    ):
        raise RuntimeError("ready_timeout_s must be a positive number")
    paths.update(
        {
            "output": _path(root, gate["output_path"], "gate.output_path", kind="output"),
            "config": _path(root, gate["config_path"], "gate.config_path", kind="file"),
            "deployment_plan": _path(
                root,
                gate["deployment_plan_path"],
                "gate.deployment_plan_path",
                kind="file",
            ),
            "site_profile": _path(
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


def _owned_target(receipt_manifest: dict, binding, scenario: str) -> dict:
    from exaserve.plan.contracts import same_node

    if scenario == "head-ray-child-death":
        rank, requirement_id, role, component = 0, "rank0/ray_head", "ray_head", "ray"
    elif scenario == "worker-supervisor-death":
        rank = 1
        requirement_id = "rank1/node_supervisor"
        role, component = "node_supervisor", "node_supervisor"
    else:
        raise ValueError(f"unknown supervisor fault scenario {scenario!r}")
    expected_node = dict(binding.rank_to_node).get(rank)
    matches = [
        item
        for item in receipt_manifest.get("receipts", [])
        if item.get("receipt_requirement_id") == requirement_id
        and item.get("role") == role
        and item.get("component_id") == component
        and item.get("owner_scope") == "RANK"
        and item.get("owner_rank") == rank
        and item.get("attestation_type") == "SELF"
        and expected_node is not None
        and same_node(str(item.get("node_id", "")), expected_node)
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"READY lacks one exact SELF-attested target {requirement_id}: {matches}"
        )
    receipt = matches[0]
    pid = receipt.get("pid")
    if (
        isinstance(pid, bool)
        or not isinstance(pid, int)
        or pid <= 0
        or not isinstance(receipt.get("receipt_hash"), str)
        or not receipt["receipt_hash"]
    ):
        raise RuntimeError(f"fault target lacks a typed process identity: {receipt}")
    return {
        "receipt_requirement_id": requirement_id,
        "role": role,
        "rank": rank,
        "node": expected_node,
        "pid": pid,
        "receipt_hash": receipt["receipt_hash"],
    }


def _launch_fault(
    *,
    scenario: str,
    output: Path,
    plan_path: Path,
    site_path: Path,
    generation: int,
    ready_timeout_s: float,
    nodes: tuple[str, ...],
) -> dict:
    from exaserve.plan.io import load_deployment_plan
    from exaserve.status_api import load_status_allocation_binding, require_ready_endpoint

    plan = load_deployment_plan(str(plan_path))
    scenario_dir = output / scenario
    run_dir = scenario_dir / "deployment"
    run_dir.mkdir(parents=True)
    argv = [sys.executable, "-u", "-m", "exaserve.launcher", str(plan_path)]
    lifecycle._atomic_text(scenario_dir / "command.txt", shlex.join(argv) + "\n")
    environment = os.environ.copy()
    environment.update(
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
    process = subprocess.Popen(
        argv,
        cwd=scenario_dir,
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        start_new_session=True,
    )
    assert process.stdout is not None and process.stderr is not None
    stdout = lifecycle._Tee(process.stdout, scenario_dir / "stdout.log", f"{scenario}:stdout")
    stderr = lifecycle._Tee(process.stderr, scenario_dir / "stderr.log", f"{scenario}:stderr")
    stdout.start()
    stderr.start()
    started = time.time()
    try:
        status = lifecycle._wait_status(
            run_dir,
            generation,
            plan.deployment_plan_hash,
            ready_timeout_s,
            process=process,
        )
        if not status.ready:
            cleanup = lifecycle._await_premature_terminal_cleanup(process, run_dir, status, plan)
            raise RuntimeError(f"supervisor fault run became terminal before READY: {cleanup}")
        endpoint = require_ready_endpoint(
            str(run_dir),
            expected_generation=generation,
            expected_plan_hash=plan.deployment_plan_hash,
        )
        canary = [lifecycle._canary(endpoint, model.model_id) for model in plan.models]
        ready_evidence = lifecycle._validate_ready_evidence(status, plan, run_dir)
        receipt_manifest = lifecycle._read_json(Path(status.receipt_manifest_path))
        binding = load_status_allocation_binding(str(run_dir), status)
        target = _owned_target(receipt_manifest, binding, scenario)
        if target["rank"] >= len(nodes):
            raise RuntimeError(f"fault target rank is outside the allocation: {target}")
        remote_signal = lifecycle._run_remote_generation_signal(
            target["node"],
            target["pid"],
            "KILL",
            deployment_id=plan.deployment_id,
            generation=generation,
            plan_hash=plan.deployment_plan_hash,
            run_dir=run_dir,
        )
        injection = {
            "schema_version": 1,
            "kind": scenario,
            "target": target,
            "signal": remote_signal,
        }
        lifecycle._atomic_json(scenario_dir / "fault_injection.json", injection)
        try:
            returncode = process.wait(timeout=lifecycle._owner_exit_timeout_s(plan))
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(f"launcher did not terminate after {scenario}") from exc
        if returncode in {0, 143} or not 1 <= returncode <= 255:
            raise RuntimeError(f"{scenario} returned invalid scheduler-visible code {returncode}")
        terminal, shutdown = lifecycle._terminal_record(run_dir, "FAILED")
        causal = f"{terminal.reason_code}: {terminal.detail}".lower()
        required_terms = (
            ("ray", "head", "rank")
            if scenario == "head-ray-child-death"
            else ("rank", "supervisor", "control", "launcher")
        )
        if not any(term in causal for term in required_terms):
            raise RuntimeError(f"{scenario} lacks a typed causal terminal detail: {causal}")
        lifecycle._atomic_json(scenario_dir / "canary.json", canary)
        return {
            "schema_version": 1,
            "scenario": scenario,
            "passed": True,
            "generation": generation,
            "deployment_plan_hash": plan.deployment_plan_hash,
            "advertised_endpoint": endpoint,
            "ready_revision": status.revision,
            "ready_evidence": ready_evidence,
            "target": target,
            "fault_injection": injection,
            "terminal_revision": terminal.revision,
            "terminal_state": terminal.state,
            "terminal_reason_code": terminal.reason_code,
            "terminal_detail": terminal.detail,
            "returncode": returncode,
            "shutdown_report": shutdown,
            "duration_s": round(time.time() - started, 3),
        }
    finally:
        active_error = sys.exc_info()[1]
        cleanup_errors: list[BaseException] = []
        cleanup_deadline = time.monotonic() + float(plan.control.watchdog_cleanup_deadline_s) + 60.0
        if process.poll() is None:
            try:
                process.send_signal(signal.SIGTERM)
                process.wait(
                    timeout=max(
                        0.0,
                        min(
                            float(plan.control.watchdog_cleanup_deadline_s) + 10.0,
                            cleanup_deadline - time.monotonic(),
                        ),
                    )
                )
            except subprocess.TimeoutExpired:
                pass
            except BaseException as exc:
                cleanup_errors.append(exc)
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
            except BaseException as exc:
                cleanup_errors.append(exc)
        try:
            reports = lifecycle._cleanup_generation_on_nodes(
                nodes,
                deployment_id=plan.deployment_id,
                generation=generation,
                plan_hash=plan.deployment_plan_hash,
                run_dir=run_dir,
            )
            lifecycle._atomic_json(
                scenario_dir / "exact_generation_cleanup.json",
                {"schema_version": 1, "reports": reports},
            )
            leftovers = [item for report in reports for item in report["matched"]]
            if leftovers:
                cleanup_errors.append(
                    RuntimeError(f"owner left exact-generation processes for fallback: {leftovers}")
                )
        except BaseException as exc:
            cleanup_errors.append(exc)
        for tee in (stdout, stderr):
            try:
                tee.join(deadline=cleanup_deadline)
            except BaseException as exc:
                cleanup_errors.append(exc)
        if lifecycle._process_group_exists(process.pid):
            cleanup_errors.append(RuntimeError(f"launcher process group {process.pid} survived"))
        if cleanup_errors:
            if active_error is not None:
                for error in cleanup_errors:
                    lifecycle._add_note(
                        active_error, f"supervisor fault cleanup also failed: {error}"
                    )
            else:
                primary = cleanup_errors[0]
                for error in cleanup_errors[1:]:
                    lifecycle._add_note(primary, f"additional cleanup failure: {error}")
                raise primary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment-plan", required=True)
    parser.add_argument("--gate-id", required=True)
    args = parser.parse_args()
    gate_id = args.gate_id.strip()
    if not gate_id or any(char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for char in gate_id):
        parser.error("--gate-id must contain uppercase ASCII letters, digits, '-' and '_'")
    experiment_path = Path(args.experiment_plan).resolve()
    try:
        document, gate, paths = _load_gate(experiment_path, gate_id)
    except RuntimeError as exc:
        parser.error(str(exc))
    output = paths["output"]
    try:
        output.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        parser.error(f"output must be new and immutable: {output}")
    bootstrap = paths["bootstrap"]
    if str(bootstrap) not in [entry for entry in sys.path if entry]:
        raise SystemExit(f"PYTHONPATH must include immutable bootstrap {bootstrap}")
    lifecycle._pin_bootstrap_environment(bootstrap)

    started = time.time()
    scenarios: list[dict] = []
    try:
        from exaserve.plan.compiler import compile_deployment_plan
        from exaserve.plan.io import load_deployment_plan, load_site_profile
        from exaserve.yaml_support import load_yaml_mapping

        plan = load_deployment_plan(str(paths["deployment_plan"]))
        profile = load_site_profile(str(paths["site_profile"]))
        candidate = document["candidate"]
        rebuilt = compile_deployment_plan(
            load_yaml_mapping(paths["config"]),
            site=profile,
            deployment_id=gate_id.lower(),
            compatibility_profile_hash=candidate["compatibility_profile_hash"],
            manifest_hash=candidate["compatibility_manifest_hash"],
        )
        if (
            rebuilt.deployment_plan_hash != plan.deployment_plan_hash
            or plan.deployment_id != gate_id.lower()
            or plan.num_nodes != 2
            or plan.runtime.null_compute is not True
            or plan.gateway is None
            or plan.gateway.kind != "haproxy"
            or plan.site_profile_hash != profile.site_profile_hash
            or profile.site_profile_hash != candidate["site_profile_hash"]
            or plan.compatibility_profile_hash != candidate["compatibility_profile_hash"]
            or plan.manifest_hash != candidate["compatibility_manifest_hash"]
        ):
            raise RuntimeError("compiled plan does not match the declared supervisor gate")
        nodes = lifecycle._validated_nodes(2, acquisition_source=gate["acquisition_source"])
        environment = lifecycle._environment_receipt(
            nodes, bootstrap, paths["wheel"], gate_id=gate_id
        )
        if environment["pbs_queue"] != gate["queue"]:
            raise RuntimeError("PBS queue differs from the declared supervisor gate")
        lifecycle._atomic_json(output / "environment.json", environment)
        manifest = {
            "schema_version": 1,
            "gate_id": gate_id,
            "lane": gate["lane"],
            "experiment_plan_path": str(experiment_path),
            "experiment_plan_sha256": lifecycle._sha256_file(experiment_path),
            "declared_gate": gate,
            "candidate": candidate,
            "harness": str(Path(__file__).resolve()),
            "harness_sha256": lifecycle._sha256_file(Path(__file__).resolve()),
            "support": str(Path(lifecycle.__file__).resolve()),
            "support_sha256": lifecycle._sha256_file(Path(lifecycle.__file__).resolve()),
            "pbs_job_id": environment["pbs_job_id"],
            "queue": environment["pbs_queue"],
            "nodes": list(nodes),
            "attempt": gate["attempt"],
            "attempt_limit": gate["attempt_limit"],
            "deployment_plan_path": str(paths["deployment_plan"]),
            "deployment_plan_hash": plan.deployment_plan_hash,
            "site_profile_path": str(paths["site_profile"]),
            "site_profile_hash": profile.site_profile_hash,
            "wheel": str(paths["wheel"]),
            "wheel_sha256": environment["wheel_sha256"],
            "started_at": started,
        }
        lifecycle._atomic_json(output / "manifest.json", manifest)
        for scenario in gate["scenarios"]:
            scenarios.append(
                _launch_fault(
                    scenario=scenario,
                    output=output,
                    plan_path=paths["deployment_plan"],
                    site_path=paths["site_profile"],
                    generation=time.time_ns(),
                    ready_timeout_s=float(gate["ready_timeout_s"]),
                    nodes=nodes,
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
            f"# {gate_id} verdict\n\nVerdict: **PASS**\n\n"
            + "\n".join(
                f"- `{item['scenario']}`: `{item['terminal_state']}`, exit `{item['returncode']}`"
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
                "schema_version": 1,
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
                "scenarios": scenarios,
                "error": error,
            },
        )
        lifecycle._atomic_text(
            output / "verdict.md",
            f"# {gate_id} verdict\n\nVerdict: **FAIL**\n\n```text\n{error.rstrip()}\n```\n",
        )
        print(f"QUALIFICATION_VERDICT|gate={gate_id}|FAIL", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
