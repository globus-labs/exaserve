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
_cleanup_generation_on_nodes = _HARNESS["_cleanup_generation_on_nodes"]
_expected_observations = _HARNESS["_expected_observations"]
_load_declared_gate = _HARNESS["_load_declared_gate"]
_owned_gateway_pid = _HARNESS["_owned_gateway_pid"]
_owned_replica_target = _HARNESS["_owned_replica_target"]
_owned_worker_target = _HARNESS["_owned_worker_target"]
_pals_argv = _HARNESS["_pals_argv"]
_pin_bootstrap_environment = _HARNESS["_pin_bootstrap_environment"]
_qualified_remote_python = _HARNESS["_qualified_remote_python"]
_RemotePortHolder = _HARNESS["_RemotePortHolder"]
_run_remote_generation_signal = _HARNESS["_run_remote_generation_signal"]
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


def _capsule_context():
    return {
        "qualified_python": "/opt/aurora/python",
        "python_root": "/tmp/exaserve/runtime/python",
        "state_root": "/tmp/exaserve/state",
    }


def test_multi_node_signal_uses_only_capsule_local_remote_paths(monkeypatch, tmp_path):
    globals_ = _run_remote_generation_signal.__globals__
    monkeypatch.setitem(globals_, "_runtime_capsule_context", lambda *_a, **_k: _capsule_context())

    def remote_json(argv, **_kwargs):
        return {
            "hostname": "worker",
            "deployment_id": "deployment",
            "generation": 1,
            "deployment_plan_hash": "a" * 64,
            "signal": "TERM",
            "owner_rank": 1,
            "requirement_id": "rank1/ray_worker",
            "process": {"pid": 10},
        }

    monkeypatch.setitem(globals_, "_run_remote_json", remote_json)
    report = _run_remote_generation_signal(
        "worker",
        10,
        "TERM",
        deployment_id="deployment",
        generation=1,
        plan_hash="a" * 64,
        run_dir=tmp_path,
        owner_rank=1,
        requirement_id="rank1/ray_worker",
        role="ray_worker",
    )
    command = report["argv"]
    assert command[0] == "mpiexec"
    assert "--genvnone" in command and "--envnone" in command
    assert "HOME=/tmp/exaserve/state/home" in command
    assert "TMPDIR=/tmp/exaserve/state/tmp" in command
    from exaserve.site import AURORA_PMIX_PREPARED_ENVIRONMENT

    for name, value in AURORA_PMIX_PREPARED_ENVIRONMENT:
        assert f"{name}={value}" in command
    assert command[command.index("--wdir") + 1] == "/tmp"
    assert "/tmp/exaserve/runtime/python" in command
    assert not any(str(tmp_path) in item or "/home/" in item or "/lus/" in item for item in command)


def test_pals_helper_defaults_pre_exec_home_to_local_scratch():
    command = _pals_argv(("worker",), "/opt/aurora/python", "-c", "pass")
    assert "HOME=/tmp" in command
    assert "TMPDIR=/tmp" in command


def test_multi_node_cleanup_uses_capsule_helper_without_shared_argv(monkeypatch, tmp_path):
    globals_ = _cleanup_generation_on_nodes.__globals__
    monkeypatch.setitem(globals_, "_runtime_capsule_context", lambda *_a, **_k: _capsule_context())

    class Process:
        returncode = 0

        def __init__(self, argv, **_kwargs):
            self.argv = argv

        def communicate(self, timeout):
            del timeout
            nodes = self.argv[self.argv.index("--hosts") + 1].split(",")
            return (
                "\n".join(
                    json.dumps(
                        {
                            "schema_version": 1,
                            "hostname": node,
                            "deployment_id": "deployment",
                            "generation": 1,
                            "deployment_plan_hash": "a" * 64,
                            "matched": [],
                            "signals": [],
                            "survivors": [],
                        }
                    )
                    for node in nodes
                ),
                "",
            )

    monkeypatch.setattr(subprocess, "Popen", Process)
    observed = _cleanup_generation_on_nodes(
        ("head", "worker"),
        deployment_id="deployment",
        generation=1,
        plan_hash="a" * 64,
        run_dir=tmp_path,
    )
    assert len(observed) == 2
    for report in observed:
        assert not any(
            str(tmp_path) in item or "/home/" in item or "/lus/" in item for item in report["argv"]
        )


def test_remote_port_holder_has_no_shared_helper_path_parameter():
    import inspect

    signature = inspect.signature(_RemotePortHolder.start)
    assert "helper_path" not in signature.parameters
    source = inspect.getsource(_RemotePortHolder.start)
    assert "hold_port.py" not in source


def test_remote_port_holder_python_is_site_profile_qualified(monkeypatch):
    profile = object()
    observed = {}

    def qualify(path, supplied_profile):
        observed.update(path=path, profile=supplied_profile)
        return "/opt/qualified/python"

    monkeypatch.setattr("exaserve.site.qualify_site_local_bootstrap", qualify)
    assert _qualified_remote_python(profile) == "/opt/qualified/python"
    assert observed == {"path": os.path.realpath(sys.executable), "profile": profile}


def test_capsule_signal_helper_pidfd_fences_the_live_process():
    from exaserve.state.qualification_process import signal_exact_process

    environment = os.environ.copy()
    environment.update(
        {
            "EXASERVE_DEPLOYMENT_ID": "qualification",
            "EXASERVE_GENERATION": "7",
            "EXASERVE_PLAN_HASH": "a" * 64,
            "EXASERVE_RECEIPT_RANK": "1",
            "EXASERVE_RECEIPT_ROLE": "ray_worker",
            "EXASERVE_RECEIPT_SLOT": "rank1/ray_worker",
        }
    )
    process = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        env=environment,
        start_new_session=True,
    )
    try:
        with pytest.raises(RuntimeError, match="exact receipt slot"):
            signal_exact_process(
                pid=process.pid,
                signal_name="TERM",
                deployment_id="qualification",
                generation=7,
                plan_hash="a" * 64,
                owner_rank=1,
                requirement_id="wrong",
                role="ray_worker",
            )
        assert process.poll() is None
        report = signal_exact_process(
            pid=process.pid,
            signal_name="TERM",
            deployment_id="qualification",
            generation=7,
            plan_hash="a" * 64,
            owner_rank=1,
            requirement_id="rank1/ray_worker",
            role="ray_worker",
        )
        assert report["process"]["pid"] == process.pid
        process.wait(timeout=5)
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)


def test_capsule_signal_helper_fences_the_exact_replica_requirement():
    from exaserve.state.qualification_process import signal_exact_process

    environment = os.environ.copy()
    environment.update(
        {
            "EXASERVE_DEPLOYMENT_ID": "qualification",
            "EXASERVE_GENERATION": "8",
            "EXASERVE_PLAN_HASH": "b" * 64,
            "EXASERVE_RECEIPT_RANK": "2",
            "EXASERVE_COMPAT_ROLE": "replica",
            "EXASERVE_RECEIPT_REQUIREMENT_ID_REPLICA": "replica/model-a/0",
        }
    )
    process = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        env=environment,
        start_new_session=True,
    )
    try:
        with pytest.raises(RuntimeError, match="exact receipt requirement"):
            signal_exact_process(
                pid=process.pid,
                signal_name="TERM",
                deployment_id="qualification",
                generation=8,
                plan_hash="b" * 64,
                owner_rank=2,
                requirement_id="replica/model-b/0",
                role="replica",
            )
        assert process.poll() is None
        report = signal_exact_process(
            pid=process.pid,
            signal_name="TERM",
            deployment_id="qualification",
            generation=8,
            plan_hash="b" * 64,
            owner_rank=2,
            requirement_id="replica/model-a/0",
            role="replica",
        )
        assert report["process"]["pid"] == process.pid
        process.wait(timeout=5)
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)


def test_capsule_cleanup_fences_same_generation_by_plan_hash(tmp_path, monkeypatch):
    from exaserve.state.process_ownership import ProcessOwnershipRegistry
    from exaserve.state.qualification_process import cleanup_exact_generation

    monkeypatch.setenv("EXASERVE_PROCESS_OWNERSHIP_ROOT", str(tmp_path / "owned"))
    deployment_id = "qualification-cleanup"
    generation = 9
    exact_hash = "c" * 64
    other_hash = "d" * 64

    def launch(plan_hash):
        environment = os.environ.copy()
        environment.update(
            {
                "EXASERVE_DEPLOYMENT_ID": deployment_id,
                "EXASERVE_GENERATION": str(generation),
                "EXASERVE_PLAN_HASH": plan_hash,
            }
        )
        return subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(60)"],
            env=environment,
            start_new_session=True,
        )

    exact = launch(exact_hash)
    other = launch(other_hash)
    registry = ProcessOwnershipRegistry(
        deployment_id=deployment_id,
        generation=generation,
        rank=0,
    )
    exact_receipt = registry.record(
        "exact",
        pid=exact.pid,
        pgid=os.getpgid(exact.pid),
        argv=[sys.executable, "-c", "import time; time.sleep(60)"],
    )
    other_receipt = registry.record(
        "other-plan",
        pid=other.pid,
        pgid=os.getpgid(other.pid),
        argv=[sys.executable, "-c", "import time; time.sleep(60)"],
    )
    # In production the owning NodeSupervisor/PALS parent reaps the target.
    # This test is that parent, so make the liveness probe poll/reap its child
    # instead of treating the resulting zombie process group as still alive.
    from exaserve.state import process_ownership

    exact_pgid = os.getpgid(exact.pid)
    real_group_alive = process_ownership._group_alive
    monkeypatch.setattr(
        process_ownership,
        "_group_alive",
        lambda pgid: exact.poll() is None if pgid == exact_pgid else real_group_alive(pgid),
    )
    try:
        report = cleanup_exact_generation(
            deployment_id=deployment_id,
            generation=generation,
            plan_hash=exact_hash,
            timeout_s=2.0,
        )
        exact.wait(timeout=5)
        assert other.poll() is None
        assert [item["pid"] for item in report["matched"]] == [exact.pid]
        assert not Path(exact_receipt).exists()
        assert Path(other_receipt).is_file()
    finally:
        for process in (exact, other):
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=5)


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
