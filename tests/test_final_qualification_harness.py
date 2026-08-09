from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import runpy
import socket
import subprocess
import sys
from types import SimpleNamespace
import zipfile

import pytest


_HARNESS = runpy.run_path(
    str(Path(__file__).resolve().parents[1] / "scripts/hardening/run_final_null_qualification.py")
)
_cleanup_local_generation_processes = _HARNESS["_cleanup_local_generation_processes"]
_expected_observations = _HARNESS["_expected_observations"]
_load_declared_gate = _HARNESS["_load_declared_gate"]
_owned_gateway_pid = _HARNESS["_owned_gateway_pid"]
_owned_replica_target = _HARNESS["_owned_replica_target"]
_owned_worker_target = _HARNESS["_owned_worker_target"]
_pin_bootstrap_environment = _HARNESS["_pin_bootstrap_environment"]
_scenario_matrix = _HARNESS["_scenario_matrix"]
_signal_local_generation_pid = _HARNESS["_signal_local_generation_pid"]
_validate_cluster_snapshot = _HARNESS["_validate_cluster_snapshot"]
_validate_shutdown_report = _HARNESS["_validate_shutdown_report"]
_verdict_text = _HARNESS["_verdict_text"]
_verify_bootstrap = _HARNESS["_verify_bootstrap"]
_validated_nodes = _HARNESS["_validated_nodes"]
_wait_status = _HARNESS["_wait_status"]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _declared_qualification(tmp_path: Path) -> tuple[Path, Path, dict]:
    root = tmp_path.resolve()
    harness = root / "scripts/hardening/harness.py"
    harness.parent.mkdir(parents=True)
    harness.write_text("# immutable harness\n", encoding="utf-8")
    release = root / "artifacts/release"
    bootstrap = release / "bootstrap"
    bootstrap.mkdir(parents=True)
    manifest = release / "artifact_manifest.json"
    wheel = release / "candidate.whl"
    sdist = release / "candidate.tar.gz"
    manifest.write_text("{}\n", encoding="utf-8")
    wheel.write_bytes(b"wheel")
    sdist.write_bytes(b"sdist")
    inputs = root / "artifacts/inputs"
    inputs.mkdir(parents=True)
    config = inputs / "config.yaml"
    deployment_plan = inputs / "deployment.plan.json"
    site_profile = inputs / "site.profile.json"
    config.write_text("deployment: {}\n", encoding="utf-8")
    deployment_plan.write_text("{}\n", encoding="utf-8")
    site_profile.write_text("{}\n", encoding="utf-8")
    gate = {
        "gate_id": "FQ-DECLARED-1N-NULL",
        "lane": "FINAL",
        "logical_nodes": 1,
        "physical_allocation_nodes": 1,
        "acquisition_source": "subjob",
        "queue": "capacity",
        "lease_ttl": "1h",
        "expected_runtime": "20m",
        "node_hours": 1,
        "attempt_limit": 1,
        "attempt": 1,
        "output_path": "artifacts/results/qualification",
        "engine_mode": "null",
        "scenario_profile": "lifecycle",
        "ready_timeout_s": 1200.0,
        "partial_observation_s": 20.0,
        "config_path": str(config.relative_to(root)),
        "config_sha256": _sha256(config),
        "deployment_plan_path": str(deployment_plan.relative_to(root)),
        "deployment_plan_sha256": _sha256(deployment_plan),
        "site_profile_path": str(site_profile.relative_to(root)),
        "site_profile_sha256": _sha256(site_profile),
        "clean_state_reset_method": "new output plus bounded cleanup",
        "retry_reason_policy": "no retry",
        "expected_observations": _expected_observations("null", "lifecycle"),
    }
    document = {
        "schema_version": 2,
        "created_at": "2026-08-09T17:00:00Z",
        "candidate": {
            "release_path": str(release.relative_to(root)),
            "artifact_manifest_path": str(manifest.relative_to(root)),
            "artifact_manifest_sha256": _sha256(manifest),
            "wheel_path": str(wheel.relative_to(root)),
            "wheel_sha256": _sha256(wheel),
            "sdist_path": str(sdist.relative_to(root)),
            "sdist_sha256": _sha256(sdist),
            "bootstrap_path": str(bootstrap.relative_to(root)),
            "site_profile_hash": "a" * 64,
            "compatibility_profile_hash": "b" * 64,
            "compatibility_manifest_hash": "c" * 64,
        },
        "harness": {
            "path": str(harness.relative_to(root)),
            "sha256": _sha256(harness),
        },
        "gates": [gate],
    }
    experiment_plan = root / "artifacts/experiment-plan.json"
    experiment_plan.write_text(json.dumps(document), encoding="utf-8")
    return experiment_plan, harness, document


def _plan():
    return SimpleNamespace(gateway=SimpleNamespace(kind="haproxy"))


def test_fault_harness_selects_the_exact_planned_gateway_slot():
    manifest = {
        "receipts": [
            {
                "receipt_requirement_id": "global/gateway/haproxy",
                "role": "gateway",
                "component_id": "gateway/haproxy",
                "owner_scope": "GLOBAL",
                "attestation_type": "SUPERVISOR",
                "pid": 4242,
            }
        ]
    }
    assert _owned_gateway_pid(manifest, _plan()) == 4242


def test_fault_harness_rejects_a_similarly_named_or_unattributed_pid():
    manifest = {
        "receipts": [
            {
                "receipt_requirement_id": "global/gateway/haproxy",
                "role": "gateway",
                "component_id": "gateway",
                "owner_scope": "GLOBAL",
                "attestation_type": "SUPERVISOR",
                "pid": 4242,
            }
        ]
    }
    with pytest.raises(RuntimeError, match="exact owned gateway PID"):
        _owned_gateway_pid(manifest, _plan())


def test_fault_harness_selects_exact_bound_rank_worker():
    binding = SimpleNamespace(rank_to_node=((0, "head"), (1, "worker.example")))
    receipt = {
        "receipt_requirement_id": "rank1/ray_worker",
        "role": "ray_worker",
        "component_id": "ray",
        "owner_scope": "RANK",
        "owner_rank": 1,
        "attestation_type": "SELF",
        "node_id": "worker",
        "pid": 5252,
        "receipt_hash": "a" * 64,
    }

    assert _owned_worker_target({"receipts": [receipt]}, binding, worker_rank=1) == {
        "receipt_requirement_id": "rank1/ray_worker",
        "rank": 1,
        "node": "worker.example",
        "pid": 5252,
        "receipt_hash": "a" * 64,
    }

    receipt["attestation_type"] = "SUPERVISOR"
    with pytest.raises(RuntimeError, match="exact owned Ray worker"):
        _owned_worker_target({"receipts": [receipt]}, binding, worker_rank=1)


def test_fault_harness_selects_exact_compiled_replica_slot():
    binding = SimpleNamespace(rank_to_node=((0, "head"), (1, "stage"), (2, "owner.example")))
    model = SimpleNamespace(model_id="org/model", route_name="org--model")
    replica = SimpleNamespace(
        replica_index=1,
        replica_id="org--model/replica-1",
        planned_ranks=(2, 3),
    )
    receipt = {
        "receipt_requirement_id": "model/org--model/replica/1",
        "role": "replica",
        "component_id": "org--model/replica-1",
        "owner_scope": "RANK",
        "owner_rank": 2,
        "attestation_type": "SELF",
        "node_id": "owner",
        "pid": 6262,
        "instance_id": "owner:6262",
        "receipt_hash": "b" * 64,
    }

    assert _owned_replica_target(
        {"receipts": [receipt]}, binding, model=model, replica=replica
    ) == {
        "receipt_requirement_id": "model/org--model/replica/1",
        "model_id": "org/model",
        "replica_index": 1,
        "rank": 2,
        "node": "owner.example",
        "pid": 6262,
        "instance_id": "owner:6262",
        "receipt_hash": "b" * 64,
    }

    receipt["owner_rank"] = 1
    with pytest.raises(RuntimeError, match="exact owned replica"):
        _owned_replica_target({"receipts": [receipt]}, binding, model=model, replica=replica)


def test_two_node_scenario_profile_is_the_complete_fault_battery():
    assert _scenario_matrix("two_node") == (
        ("normal-drain", "operator_drain"),
        ("gateway-death", "gateway_death"),
        ("worker-death", "worker_death"),
        ("duplicate-gateway-port", "duplicate_gateway_port"),
        ("partial-worker-proxy", "partial_proxy_readiness"),
    )


def test_four_node_real_profile_has_normal_and_exact_replica_failure_only():
    assert _scenario_matrix("four_node_real") == (
        ("normal-drain", "operator_drain"),
        ("replica-death", "replica_death"),
    )


def test_qualification_gate_is_loaded_only_from_exact_predeclared_bytes(tmp_path):
    experiment_plan, harness, document = _declared_qualification(tmp_path)

    loaded, gate, paths = _load_declared_gate(
        experiment_plan,
        "FQ-DECLARED-1N-NULL",
        repo_root=tmp_path,
        harness_path=harness,
    )

    assert loaded == document
    assert gate["attempt_limit"] == 1
    assert gate["attempt"] == 1
    assert paths["output"] == (tmp_path / "artifacts/results/qualification").resolve()


def test_qualification_gate_rejects_attempt_beyond_declared_limit(tmp_path):
    experiment_plan, harness, document = _declared_qualification(tmp_path)
    document["gates"][0]["attempt"] = 2
    experiment_plan.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(RuntimeError, match="attempt exceeds"):
        _load_declared_gate(
            experiment_plan,
            "FQ-DECLARED-1N-NULL",
            repo_root=tmp_path,
            harness_path=harness,
        )


def test_qualification_gate_rejects_mutated_declared_input(tmp_path):
    experiment_plan, harness, document = _declared_qualification(tmp_path)
    config = tmp_path / document["gates"][0]["config_path"]
    config.write_text("deployment: {changed: true}\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="config bytes differ"):
        _load_declared_gate(
            experiment_plan,
            "FQ-DECLARED-1N-NULL",
            repo_root=tmp_path,
            harness_path=harness,
        )


def test_qualification_harness_has_no_hard_coded_attempt_limit():
    source = Path(
        Path(__file__).resolve().parents[1] / "scripts/hardening/run_final_null_qualification.py"
    ).read_text(encoding="utf-8")

    assert '"attempt_limit": 2' not in source


def test_qualification_pins_absolute_bootstrap_for_cwd_changing_children(tmp_path, monkeypatch):
    bootstrap = tmp_path / "relative-bootstrap"
    bootstrap.mkdir()
    monkeypatch.setenv("PYTHONPATH", "relative-bootstrap")

    pinned = _pin_bootstrap_environment(bootstrap)

    assert pinned == str(bootstrap.resolve())
    assert os.environ["PYTHONPATH"] == str(bootstrap.resolve())


def test_status_wait_fails_immediately_when_launcher_exits_without_status(tmp_path):
    process = SimpleNamespace(poll=lambda: 17)

    with pytest.raises(RuntimeError, match="exited before publishing.*returncode=17"):
        _wait_status(tmp_path, 1, "a" * 64, 1200.0, process=process)


def test_partial_readiness_membership_requires_every_exact_live_node():
    plan = SimpleNamespace(
        deployment_id="deployment",
        deployment_plan_hash="p" * 64,
        site_profile_hash="s" * 64,
    )
    snapshot = {
        "schema_version": 1,
        "deployment_id": "deployment",
        "generation": 7,
        "deployment_plan_hash": "p" * 64,
        "site_profile_hash": "s" * 64,
        "ready": True,
        "blockers": [],
        "observed_at": 1.0,
        "nodes": [
            {"node_name": "head.example", "alive": True},
            {"node_name": "worker", "alive": True},
        ],
    }

    evidence = _validate_cluster_snapshot(
        snapshot,
        generation=7,
        plan=plan,
        expected_nodes=("head", "worker.example"),
    )
    assert evidence["node_count"] == 2

    snapshot["nodes"][1]["alive"] = False
    with pytest.raises(RuntimeError, match="non-live"):
        _validate_cluster_snapshot(
            snapshot,
            generation=7,
            plan=plan,
            expected_nodes=("head", "worker.example"),
        )


@pytest.mark.parametrize(
    ("state", "publication"),
    [("STOPPED", "published"), ("CANCELLED", "published"), ("FAILED", "not_required")],
)
def test_shutdown_report_accepts_the_phase_that_owns_terminal_publication(state, publication):
    _validate_shutdown_report(
        {
            "clean": True,
            "terminal_publication": publication,
            "observed_terminal_state": state,
            "components": {"owned": {"state": "STOPPED"}},
        },
        state,
    )


def test_shutdown_report_rejects_failure_relabelled_as_shutdown_publication():
    with pytest.raises(RuntimeError, match="terminal ownership contract"):
        _validate_shutdown_report(
            {
                "clean": True,
                "terminal_publication": "published",
                "observed_terminal_state": "FAILED",
                "components": {"owned": {"state": "STOPPED"}},
            },
            "FAILED",
        )


def test_hardware_gate_rejects_forced_deployment_kill_when_rank_sessions_were_healthy():
    report = {
        "clean": True,
        "terminal_publication": "published",
        "observed_terminal_state": "STOPPED",
        "components": {
            "deployment": {"state": "STOPPED", "returncode": -9},
            "rank_launcher": {"state": "STOPPED", "returncode": 0},
        },
    }
    with pytest.raises(RuntimeError, match="graceful deployment drain"):
        _validate_shutdown_report(
            report,
            "STOPPED",
            require_graceful_deployment=True,
        )

    report["components"]["deployment"]["returncode"] = 0
    _validate_shutdown_report(
        report,
        "STOPPED",
        require_graceful_deployment=True,
    )


def test_qualification_verdict_is_bound_to_the_requested_gate_and_topology():
    rendered = _verdict_text(
        True,
        [],
        gate_id="FQ-2N-NULL-CURRENT",
        logical_nodes=2,
    )

    assert "# FQ-2N-NULL-CURRENT verdict" in rendered
    assert "2-node Aurora/XPU/vLLM-null-compute/HAProxy" in rendered
    assert "one-node" not in rendered

    real = _verdict_text(
        True,
        [],
        gate_id="FQ-2N-REAL-CURRENT",
        logical_nodes=2,
        engine_mode="real",
    )
    assert "2-node Aurora/XPU/vLLM-real-engine/HAProxy" in real


def test_qualification_bootstrap_must_be_byte_identical_to_wheel(tmp_path):
    wheel = tmp_path / "release.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("exaserve/__init__.py", b"release = True\n")
        archive.writestr("exaserve/resources/helper.c", b"int main(void) { return 0; }\n")
    bootstrap = tmp_path / "bootstrap"
    (bootstrap / "exaserve" / "resources").mkdir(parents=True)
    (bootstrap / "exaserve" / "__init__.py").write_bytes(b"release = True\n")
    (bootstrap / "exaserve" / "resources" / "helper.c").write_bytes(
        b"int main(void) { return 0; }\n"
    )

    receipt = _verify_bootstrap(wheel, bootstrap)
    assert receipt["package_members"] == 2
    assert len(receipt["package_tree_sha256"]) == 64

    (bootstrap / "exaserve" / "__init__.py").write_bytes(b"release = False\n")
    with pytest.raises(RuntimeError, match="byte-identical"):
        _verify_bootstrap(wheel, bootstrap)


def test_qualification_accepts_only_the_declared_valid_compute_session(tmp_path, monkeypatch):
    nodefile = tmp_path / "nodefile"
    nodefile.write_text(socket.gethostname() + "\n", encoding="utf-8")
    monkeypatch.setenv("PBS_JOBID", "123.server")
    monkeypatch.setenv("PBS_NODEFILE", str(nodefile))
    monkeypatch.delenv("ONEAPI_DEVICE_SELECTOR", raising=False)

    monkeypatch.setenv("AURORA_SUBJOB", "1")
    assert _validated_nodes(1, acquisition_source="subjob") == (socket.gethostname(),)
    with pytest.raises(RuntimeError, match="interactive_pbs was declared"):
        _validated_nodes(1, acquisition_source="interactive_pbs")

    monkeypatch.delenv("AURORA_SUBJOB")
    monkeypatch.setenv("PBS_ENVIRONMENT", "PBS_INTERACTIVE")
    assert _validated_nodes(1, acquisition_source="interactive_pbs") == (socket.gethostname(),)
    with pytest.raises(RuntimeError, match="AURORA_SUBJOB"):
        _validated_nodes(1, acquisition_source="subjob")


def test_fallback_cleanup_reaps_only_the_exact_generation(tmp_path):
    run_dir = (tmp_path / "qualified-generation").resolve()
    run_dir.mkdir()
    deployment_id = "qualification-cleanup"
    generation = 901
    plan_hash = "a" * 64
    exact_environment = os.environ.copy()
    exact_environment.update(
        {
            "EXASERVE_DEPLOYMENT_ID": deployment_id,
            "EXASERVE_GENERATION": str(generation),
            "EXASERVE_PLAN_HASH": plan_hash,
            "EXASERVE_RUN_LOG_DIR": str(run_dir),
        }
    )
    other_environment = dict(exact_environment)
    other_environment["EXASERVE_GENERATION"] = str(generation + 1)
    exact = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        env=exact_environment,
        start_new_session=True,
    )
    unrelated = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        env=other_environment,
        start_new_session=True,
    )
    try:
        report = _cleanup_local_generation_processes(
            deployment_id=deployment_id,
            generation=generation,
            plan_hash=plan_hash,
            run_dir=run_dir,
        )
        assert [item["pid"] for item in report["matched"]] == [exact.pid]
        assert report["survivors"] == []
        exact.wait(timeout=5)
        assert unrelated.poll() is None
    finally:
        if exact.poll() is None:
            exact.terminate()
            exact.wait(timeout=5)
        if unrelated.poll() is None:
            unrelated.terminate()
            unrelated.wait(timeout=5)


def test_fault_signal_is_fenced_to_the_exact_generation(tmp_path):
    run_dir = (tmp_path / "fault-generation").resolve()
    run_dir.mkdir()
    deployment_id = "qualification-fault"
    generation = 902
    plan_hash = "c" * 64
    exact_environment = os.environ.copy()
    exact_environment.update(
        {
            "EXASERVE_DEPLOYMENT_ID": deployment_id,
            "EXASERVE_GENERATION": str(generation),
            "EXASERVE_PLAN_HASH": plan_hash,
            "EXASERVE_RUN_LOG_DIR": str(run_dir),
        }
    )
    exact = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        env=exact_environment,
        start_new_session=True,
    )
    try:
        with pytest.raises(RuntimeError, match="refusing to signal"):
            _signal_local_generation_pid(
                exact.pid,
                "TERM",
                deployment_id=deployment_id,
                generation=generation + 1,
                plan_hash=plan_hash,
                run_dir=run_dir,
            )
        assert exact.poll() is None

        report = _signal_local_generation_pid(
            exact.pid,
            "TERM",
            deployment_id=deployment_id,
            generation=generation,
            plan_hash=plan_hash,
            run_dir=run_dir,
        )
        assert report["process"]["pid"] == exact.pid
        assert report["signal"] == "TERM"
        exact.wait(timeout=5)
    finally:
        if exact.poll() is None:
            exact.terminate()
            exact.wait(timeout=5)
