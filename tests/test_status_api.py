"""The shared DeploymentStatus boundary (plan §3.4 table, WP9).

`state/status.py` had the durable CAS-guarded record and nothing wrote to it,
so consumers had no typed surface and the only way to learn a deployment's
state was to parse the root's private readiness file or grep a log. These tests
pin the writer, the reader, and the two things the boundary exists to prevent:
a stale generation's record being mistaken for this one's, and a client getting
an endpoint for a deployment that is not READY.
"""

from __future__ import annotations

import time

import pytest

from clientlab.targets import exaserve_target
from exaserve.plan.compiler import compile_deployment_plan
from exaserve.plan.contracts import build_allocation_binding
from exaserve.plan.io import write_allocation_binding
from exaserve.site import default_site_profile
from exaserve.state.status import DeploymentState
from exaserve.state.receipts import ReceiptManifest, write_receipt_manifest
from exaserve.status_api import (
    DeploymentNotReady,
    InvalidDeploymentStatus,
    DeploymentStatusPublisher,
    StatusClock,
    StatusPublicationError,
    load_status_allocation_binding,
    read_deployment_status,
    require_ready_endpoint,
)


def _plan(num_nodes: int = 2, *, head_only: bool = False):
    raw = {
        "num_nodes": num_nodes,
        "num_gpus_per_node": 12,
        "validation_mode": True,
        "models": [{"model_id": "m", "tensor_parallel_size": 1, "max_model_len": 128, "size": 8}],
    }
    if head_only:
        raw["exposure"] = {"mode": "RAY_SERVE_HEAD_ONLY", "serve_port": 8000}
    return compile_deployment_plan(
        raw,
        site=default_site_profile(),
        deployment_id="d1",
    )


def _binding(plan, generation=3):
    return build_allocation_binding(
        plan=plan,
        generation=generation,
        scheduler_allocation_id="job1",
        nodes=[f"n{i}" for i in range(plan.num_nodes)],
    )


def _publisher(tmp_path, generation=3, *, head_only: bool = False, clock=None):
    plan = _plan(head_only=head_only)
    binding = _binding(plan, generation)
    pub = DeploymentStatusPublisher(
        str(tmp_path),
        plan=plan,
        binding=binding,
        generation=generation,
        log=lambda *_: None,
        clock=clock,
    )
    pub.initialize()
    return pub, plan, binding


def _walk_to_ready(pub, endpoint="http://h:8000"):
    pub.advance_through(
        DeploymentState.STAGING,
        DeploymentState.CLUSTER_STARTING,
        DeploymentState.DEPLOYING,
        reason_code="X",
    )
    pub.advance(DeploymentState.VALIDATING, reason_code="X", advertised_endpoint=endpoint)
    path, digest = _empty_receipt_manifest(pub)
    snapshot, model_map = _ready_snapshot(pub, path, digest, endpoint)
    pub.advance(
        DeploymentState.READY,
        reason_code="READY",
        advertised_endpoint=endpoint,
        receipt_hashes=[],
        readiness_snapshot=snapshot,
        model_map=model_map,
        capability_map={},
    )


def _empty_receipt_manifest(pub):
    manifest = ReceiptManifest(
        schema_version=1,
        deployment_id=pub.plan.deployment_id,
        generation=pub.generation,
        deployment_plan_hash=pub.plan.deployment_plan_hash,
        allocation_binding_hash=pub.binding.allocation_binding_hash,
        receipt_hashes=(),
        receipts=(),
    ).finalize()
    path = __import__("os").path.join(
        __import__("os").path.dirname(pub.path),
        "compatibility_receipts",
        f"{manifest.manifest_hash}.json",
    )
    write_receipt_manifest(path, manifest)
    return path, manifest.manifest_hash


def _ready_snapshot(pub, receipt_path, receipt_hash, endpoint="http://h:8000"):
    nodes = [
        {
            "node_id": f"id-{rank}",
            "node_name": node,
            "node_address": f"10.0.0.{rank + 1}",
            "alive": True,
            "cpu": float(pub.plan.node_cpus),
            "gpu": float(pub.plan.num_gpus_per_node),
        }
        for rank, node in pub.binding.rank_to_node
    ]
    proxy_nodes = nodes[:1] if pub.plan.uses_head_only_serve_proxy() else nodes
    proxies = [{"node_id": item["node_id"], "status": "HEALTHY"} for item in proxy_nodes]
    model_map = {
        model.model_id: {
            "route_name": model.route_name,
            "expected_replicas": model.num_replicas,
            "observed_replicas": model.num_replicas,
            "observed_target": model.num_replicas,
        }
        for model in pub.plan.models
    }
    snapshot = {
        "ready": True,
        "phase": "READY",
        "blockers": [],
        "satisfied": ["test predicate"],
        "advertised_endpoint": endpoint,
        "generation": pub.generation,
        "deployment_plan_hash": pub.plan.deployment_plan_hash,
        "allocation_binding_hash": pub.binding.allocation_binding_hash,
        "missing_identities": [],
        "unhealthy_identities": [],
        "receipt_hashes": [],
        "model_map": model_map,
        "capability_map": {},
        "nodes": nodes,
        "proxies": proxies,
        "observed_at": time.time(),
        "receipt_manifest_path": receipt_path,
        "receipt_manifest_hash": receipt_hash,
        # Publisher overwrites this with its authoritative lease.
        "lease_expires_at": time.time() + 60,
    }
    return snapshot, model_map


# -- writer ---------------------------------------------------------------
def test_publisher_walks_the_real_lifecycle(tmp_path):
    pub, _, _ = _publisher(tmp_path)
    assert pub.state == "PLANNED"
    _walk_to_ready(pub)
    assert pub.state == "READY"
    assert read_deployment_status(str(tmp_path)).ready


def test_head_only_ready_status_uses_one_planned_proxy_at_multiple_nodes(tmp_path):
    pub, _, _ = _publisher(tmp_path, head_only=True)
    _walk_to_ready(pub)
    status = read_deployment_status(str(tmp_path))
    assert status.ready
    assert status.exposure_mode == "RAY_SERVE_HEAD_ONLY"
    assert status.readiness_snapshot["proxies"] == [{"node_id": "id-0", "status": "HEALTHY"}]


def test_an_illegal_transition_is_refused_not_written(tmp_path):
    pub, _, _ = _publisher(tmp_path)
    with pytest.raises(StatusPublicationError, match="PLANNED -> READY"):
        pub.advance(DeploymentState.READY, reason_code="X")
    assert read_deployment_status(str(tmp_path)).state == "PLANNED"


def test_a_second_publisher_does_not_take_over_the_record(tmp_path):
    """Two generations writing one file is how a stale READY survives."""
    pub, plan, binding = _publisher(tmp_path)
    _walk_to_ready(pub)
    intruder = DeploymentStatusPublisher(
        str(tmp_path), plan=plan, binding=binding, generation=9, log=lambda *_: None
    )
    with pytest.raises(StatusPublicationError, match="initialize"):
        intruder.initialize()
    assert read_deployment_status(str(tmp_path)).ready


def test_ready_transition_carries_the_readiness_evidence_atomically(tmp_path):
    pub, _, _ = _publisher(tmp_path)
    pub.advance_through(
        DeploymentState.STAGING,
        DeploymentState.CLUSTER_STARTING,
        DeploymentState.DEPLOYING,
        DeploymentState.VALIDATING,
        reason_code="X",
    )
    receipt_path, receipt_hash = _empty_receipt_manifest(pub)
    snapshot, model_map = _ready_snapshot(pub, receipt_path, receipt_hash, "http://h:8000")
    pub.advance(
        DeploymentState.READY,
        reason_code="READY",
        advertised_endpoint="http://h:8000",
        readiness_snapshot=snapshot,
        model_map=model_map,
        capability_map={},
        receipt_hashes=[],
    )
    status = read_deployment_status(str(tmp_path))
    assert status.ready
    assert status.readiness_snapshot["ready"] is True
    assert status.model_map["m"]["observed_replicas"] == pub.plan.models[0].num_replicas
    assert status.receipt_hashes == ()
    assert status.receipt_manifest_hash == receipt_hash


def test_failure_is_published_with_its_first_cause(tmp_path):
    pub, _, _ = _publisher(tmp_path)
    pub.advance(
        DeploymentState.FAILED, reason_code="FIRST_CAUSE", detail="the gateway never started"
    )
    status = read_deployment_status(str(tmp_path))
    assert status.terminal and status.detail == "the gateway never started"


def test_provenance_carries_the_generation_and_hashes(tmp_path):
    from dataclasses import asdict

    pub, plan, binding = _publisher(tmp_path)
    status = read_deployment_status(str(tmp_path))
    record = pub.store.load()
    assert record is not None
    assert status.updated_at == record.updated_at
    assert status.schema_version == 2
    assert status.state_revision == 0
    assert status.state_changed_at >= 0
    assert status.state_changed_monotonic >= 0
    assert status.state_clock_boot_id
    serialized = asdict(status)
    assert serialized["schema_version"] == 2
    assert serialized["state_revision"] == 0
    assert serialized["state_changed_monotonic"] == status.state_changed_monotonic
    assert status.generation == 3
    assert status.deployment_plan_hash == plan.deployment_plan_hash
    assert status.site_profile_hash == plan.site_profile_hash
    assert status.allocation_binding_hash == binding.allocation_binding_hash
    assert status.num_nodes == plan.num_nodes


def test_legacy_public_status_remains_readable_without_fabricating_monotonic_time(tmp_path):
    import json

    pub, _, _ = _publisher(tmp_path)
    raw = json.loads((tmp_path / "deployment_status.json").read_text(encoding="utf-8"))
    for name in (
        "deployment_status_schema_version",
        "state_revision",
        "state_changed_at",
        "state_changed_monotonic",
        "state_clock_boot_id",
    ):
        raw["data"].pop(name)
    (tmp_path / "deployment_status.json").write_text(json.dumps(raw), encoding="utf-8")

    legacy = read_deployment_status(str(tmp_path))
    assert legacy.schema_version == 1
    assert legacy.state_revision is None
    assert legacy.state_changed_at == raw["history"][-1]["at"]
    assert legacy.state_changed_monotonic is None
    assert legacy.state_clock_boot_id == ""
    assert pub.store.load() is not None


def test_unknown_public_status_schema_is_rejected_explicitly(tmp_path):
    import json

    _publisher(tmp_path)
    path = tmp_path / "deployment_status.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["data"]["deployment_status_schema_version"] = 99
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(InvalidDeploymentStatus, match="schema_version=99"):
        read_deployment_status(str(tmp_path))


def test_status_loader_verifies_the_separate_allocation_binding(tmp_path):
    _, _, binding = _publisher(tmp_path)
    write_allocation_binding(str(tmp_path / "allocation_binding.json"), binding)
    status = read_deployment_status(str(tmp_path))

    assert load_status_allocation_binding(str(tmp_path), status) == binding


def test_status_loader_rejects_a_binding_from_another_generation(tmp_path):
    _, plan, _ = _publisher(tmp_path)
    other = _binding(plan, generation=4)
    write_allocation_binding(str(tmp_path / "allocation_binding.json"), other)

    with pytest.raises(InvalidDeploymentStatus, match="identity disagrees"):
        load_status_allocation_binding(str(tmp_path), read_deployment_status(str(tmp_path)))


def test_status_loader_rejects_a_symlinked_binding(tmp_path):
    _, _, binding = _publisher(tmp_path)
    target = tmp_path / "real_binding.json"
    write_allocation_binding(str(target), binding)
    (tmp_path / "allocation_binding.json").symlink_to(target)

    with pytest.raises(InvalidDeploymentStatus, match="regular, non-symlink"):
        load_status_allocation_binding(str(tmp_path), read_deployment_status(str(tmp_path)))


# -- reader ---------------------------------------------------------------
def test_no_record_is_not_an_endpoint(tmp_path):
    assert read_deployment_status(str(tmp_path)) is None
    with pytest.raises(DeploymentNotReady, match="no deployment status"):
        require_ready_endpoint(str(tmp_path))


def test_a_deploying_deployment_yields_no_endpoint(tmp_path):
    pub, _, _ = _publisher(tmp_path)
    pub.advance_through(
        DeploymentState.STAGING,
        DeploymentState.CLUSTER_STARTING,
        DeploymentState.DEPLOYING,
        reason_code="X",
    )
    with pytest.raises(DeploymentNotReady, match="DEPLOYING"):
        require_ready_endpoint(str(tmp_path))


def test_a_stale_generation_is_rejected_by_the_reader(tmp_path):
    pub, plan, _ = _publisher(tmp_path, generation=3)
    _walk_to_ready(pub)
    with pytest.raises(DeploymentNotReady, match="generation 3"):
        require_ready_endpoint(str(tmp_path), expected_generation=4)
    with pytest.raises(DeploymentNotReady, match="!="):
        require_ready_endpoint(str(tmp_path), expected_plan_hash="f" * 64)
    assert (
        require_ready_endpoint(
            str(tmp_path), expected_generation=3, expected_plan_hash=plan.deployment_plan_hash
        )
        == "http://h:8000"
    )


def test_ready_without_an_endpoint_is_refused_by_the_writer(tmp_path):
    pub, _, _ = _publisher(tmp_path)
    pub.advance_through(
        DeploymentState.STAGING,
        DeploymentState.CLUSTER_STARTING,
        DeploymentState.DEPLOYING,
        DeploymentState.VALIDATING,
        reason_code="X",
    )
    with pytest.raises(StatusPublicationError, match="non-empty advertised endpoint"):
        pub.advance(DeploymentState.READY, reason_code="READY", advertised_endpoint="")


def test_ready_without_a_receipt_manifest_is_refused(tmp_path):
    pub, _, _ = _publisher(tmp_path)
    pub.advance_through(
        DeploymentState.STAGING,
        DeploymentState.CLUSTER_STARTING,
        DeploymentState.DEPLOYING,
        DeploymentState.VALIDATING,
        reason_code="X",
    )
    with pytest.raises(StatusPublicationError, match="receipt manifest"):
        pub.advance(DeploymentState.READY, reason_code="READY", advertised_endpoint="http://h:8000")


def test_tampered_ready_receipt_manifest_fails_closed(tmp_path):
    pub, _, _ = _publisher(tmp_path)
    _walk_to_ready(pub)
    status = read_deployment_status(str(tmp_path))
    with open(status.receipt_manifest_path, "w", encoding="utf-8") as handle:
        handle.write("{}")
    from exaserve.status_api import InvalidDeploymentStatus

    with pytest.raises(InvalidDeploymentStatus, match="receipt manifest"):
        read_deployment_status(str(tmp_path))


def _fake_status_clock(state, *, boot_id="boot-a"):
    return StatusClock(
        wall_time=lambda: state["wall"],
        monotonic_time=lambda: state["monotonic"],
        boot_id=lambda: boot_id,
    )


def test_ready_evidence_uses_monotonic_expiry_and_refresh_is_cas_published(tmp_path):
    state = {"wall": 2_000_000_000.0, "monotonic": 100.0}
    clock = _fake_status_clock(state)
    pub, _, _ = _publisher(tmp_path, clock=clock)
    _walk_to_ready(pub)
    status = read_deployment_status(str(tmp_path), clock=clock)
    revision = status.revision
    state_revision = status.state_revision
    state_changed_at = status.state_changed_at
    state_changed_monotonic = status.state_changed_monotonic
    expires = status.readiness_snapshot["lease_expires_monotonic"]

    # Wall-clock jumps in either direction are evidence changes only. They
    # cannot revoke or extend the monotonic READY lease.
    state["wall"] += 1_000_000_000.0
    assert read_deployment_status(str(tmp_path), clock=clock).ready
    state["wall"] = 1.0
    assert read_deployment_status(str(tmp_path), clock=clock).ready

    state["monotonic"] += 1.0
    pub.refresh_ready(
        readiness_snapshot=status.readiness_snapshot,
        model_map=status.model_map,
        capability_map=status.capability_map,
        receipt_hashes=list(status.receipt_hashes),
    )
    refreshed = read_deployment_status(str(tmp_path), clock=clock)
    assert refreshed.revision == revision + 1
    assert refreshed.state_revision == state_revision
    assert refreshed.state_changed_at == state_changed_at
    assert refreshed.state_changed_monotonic == state_changed_monotonic
    assert refreshed.readiness_snapshot["lease_expires_monotonic"] > expires

    state["monotonic"] = refreshed.readiness_snapshot["lease_expires_monotonic"]
    assert read_deployment_status(str(tmp_path), clock=clock).ready is False
    with pytest.raises(DeploymentNotReady, match="lease expired"):
        require_ready_endpoint(str(tmp_path), clock=clock)


def test_ready_recovery_commits_a_new_state_transition_anchor(tmp_path):
    state = {"wall": 2_000_000_000.0, "monotonic": 100.0}
    clock = _fake_status_clock(state)
    pub, _, _ = _publisher(tmp_path, clock=clock)
    _walk_to_ready(pub)
    first = read_deployment_status(str(tmp_path), clock=clock)

    state["wall"] += 10.0
    state["monotonic"] += 10.0
    pub.advance(DeploymentState.VALIDATING, reason_code="READINESS_REVOKED")
    path, digest = _empty_receipt_manifest(pub)
    snapshot, model_map = _ready_snapshot(pub, path, digest)
    state["wall"] += 5.0
    state["monotonic"] += 5.0
    pub.advance(
        DeploymentState.READY,
        reason_code="READY_RECOVERED",
        advertised_endpoint="http://h:8000",
        receipt_hashes=[],
        readiness_snapshot=snapshot,
        model_map=model_map,
        capability_map={},
    )
    recovered = read_deployment_status(str(tmp_path), clock=clock)
    assert recovered.state_revision > first.state_revision
    assert recovered.state_changed_at == state["wall"]
    assert recovered.state_changed_monotonic == state["monotonic"]


def test_ready_transition_anchor_is_sampled_after_payload_validation(tmp_path, monkeypatch):
    from exaserve.state import receipts as receipt_module

    state = {"wall": 2_000_000_000.0, "monotonic": 100.0}
    clock = _fake_status_clock(state)
    pub, _, _ = _publisher(tmp_path, clock=clock)
    pub.advance_through(
        DeploymentState.STAGING,
        DeploymentState.CLUSTER_STARTING,
        DeploymentState.DEPLOYING,
        DeploymentState.VALIDATING,
        reason_code="X",
    )
    path, digest = _empty_receipt_manifest(pub)
    snapshot, model_map = _ready_snapshot(pub, path, digest)
    real_load = receipt_module.load_receipt_manifest

    def delayed_manifest_validation(manifest_path):
        state["wall"] += 50.0
        state["monotonic"] += 50.0
        return real_load(manifest_path)

    monkeypatch.setattr(receipt_module, "load_receipt_manifest", delayed_manifest_validation)
    pub.advance(
        DeploymentState.READY,
        reason_code="READY",
        advertised_endpoint="http://h:8000",
        receipt_hashes=[],
        readiness_snapshot=snapshot,
        model_map=model_map,
        capability_map={},
    )
    status = read_deployment_status(str(tmp_path), clock=clock)
    assert status.state_changed_at == 2_000_000_050.0
    assert status.state_changed_monotonic == 150.0


def test_ready_lease_survives_reader_restart_on_same_boot_and_expires_on_boot_change(tmp_path):
    state = {"wall": 2_000_000_000.0, "monotonic": 100.0}
    publisher_clock = _fake_status_clock(state, boot_id="boot-a")
    pub, _, _ = _publisher(tmp_path, clock=publisher_clock)
    _walk_to_ready(pub)

    # A separately constructed clock models a new reader process. Kernel
    # monotonic time remains comparable for the duration of the same boot.
    restarted_reader_clock = _fake_status_clock(state, boot_id="boot-a")
    assert read_deployment_status(str(tmp_path), clock=restarted_reader_clock).ready

    rebooted_reader_clock = _fake_status_clock(state, boot_id="boot-b")
    assert not read_deployment_status(str(tmp_path), clock=rebooted_reader_clock).ready
    with pytest.raises(DeploymentNotReady, match="lease expired"):
        require_ready_endpoint(str(tmp_path), clock=rebooted_reader_clock)


def test_ready_heartbeats_do_not_grow_transition_history_without_bound(tmp_path):
    pub, _, _ = _publisher(tmp_path)
    _walk_to_ready(pub)
    status = read_deployment_status(str(tmp_path))
    before = pub.store.load()
    assert before is not None
    history_length = len(before.history)

    for _ in range(25):
        pub.refresh_ready(
            readiness_snapshot=status.readiness_snapshot,
            model_map=status.model_map,
            capability_map=status.capability_map,
            receipt_hashes=list(status.receipt_hashes),
        )

    after = pub.store.load()
    assert after is not None
    assert after.revision == before.revision + 25
    assert len(after.history) == history_length
    assert after.reason_code == "READINESS_HEARTBEAT"


# -- ClientLab consumer ---------------------------------------------------
def test_clientlab_resolves_only_a_ready_deployment(tmp_path):
    pub, plan, _ = _publisher(tmp_path)
    with pytest.raises(exaserve_target.DeploymentTargetError):
        exaserve_target.resolve(str(tmp_path))
    _walk_to_ready(pub)
    target = exaserve_target.resolve(str(tmp_path), expected_plan_hash=plan.deployment_plan_hash)
    assert target.base_url == "http://h:8000"
    assert target.generation == 3
    assert target.is_validation_only  # this plan is DIRECT_VALIDATION


def test_clientlab_stops_waiting_on_a_terminal_deployment(tmp_path):
    pub, _, _ = _publisher(tmp_path)
    pub.advance(DeploymentState.FAILED, reason_code="FIRST_CAUSE", detail="boom")
    with pytest.raises(exaserve_target.DeploymentTargetError, match="FAILED"):
        exaserve_target.wait_until_ready(str(tmp_path), timeout_s=30.0, poll_s=0.01)


def test_clientlab_ignores_a_terminal_record_from_another_generation(tmp_path):
    pub, _, _ = _publisher(tmp_path)
    pub.advance(DeploymentState.FAILED, reason_code="FIRST_CAUSE", detail="old run")
    with pytest.raises(exaserve_target.DeploymentTargetError, match="stale identity") as caught:
        exaserve_target.wait_until_ready(
            str(tmp_path),
            expected_generation=4,
            timeout_s=0.04,
            poll_s=0.01,
        )
    assert "reached FAILED" not in str(caught.value)


def test_clientlab_wait_rejects_nonfinite_polling_policy(tmp_path):
    with pytest.raises(ValueError, match="poll_s"):
        exaserve_target.wait_until_ready(str(tmp_path), timeout_s=1.0, poll_s=float("nan"))


def test_clientlab_wait_times_out_with_the_last_state(tmp_path):
    pub, _, _ = _publisher(tmp_path)
    pub.advance(DeploymentState.STAGING, reason_code="X")
    with pytest.raises(exaserve_target.DeploymentTargetError, match="STAGING"):
        exaserve_target.wait_until_ready(str(tmp_path), timeout_s=0.05, poll_s=0.01)


def test_clientlab_does_not_monitor_processes_or_grep_logs():
    """The boundary is the point; a fallback would quietly reinstate the coupling."""
    import inspect

    source = inspect.getsource(exaserve_target)
    for forbidden in (
        "subprocess",
        "Popen",
        "psutil",
        "pgrep",
        "CLUSTER FULLY READY",
        "readiness.json",
        "launch.log",
    ):
        assert forbidden not in source, f"{forbidden} reintroduces a private view"


def test_clientlab_has_no_deployment_plan_compiler_of_its_own():
    """Deployment identity has exactly one producer (IMP-H01)."""
    import pathlib

    root = pathlib.Path(__file__).resolve().parents[1] / "clientlab"
    offenders = []
    for path in root.rglob("*.py"):
        text = path.read_text(encoding="utf-8", errors="ignore")
        if "compile_deployment_plan" in text or "build_receipt_requirements" in text:
            offenders.append(str(path))
    assert offenders == []


# -- readiness inputs derived from the child's evidence --------------------
def test_a_single_model_matches_the_default_serve_application(tmp_path):
    """Every bound replica application contributes to model evidence."""
    from exaserve.composition import CompositionRoot

    plan = _plan()
    model = plan.models[0]
    apps = {
        f"{model.route_name}_r{index}": {
            "running": 1,
            "target": 1,
            "route_prefix": f"/{model.route_name}_r{index}",
            "status": "RUNNING",
        }
        for index in range(model.num_replicas)
    }
    root = CompositionRoot(plan=plan, generation=1, run_dir=str(tmp_path))
    assert root.application_for_model(model, apps)["running"] == 24


def test_node_grouped_null_applications_preserve_exact_replica_totals(tmp_path):
    from exaserve.composition import CompositionRoot

    plan = compile_deployment_plan(
        {
            "num_nodes": 2,
            "validation_mode": True,
            "runtime": {"null_compute": True},
            "gateway": {"kind": "haproxy", "port": 4001},
            "models": [
                {
                    "model_id": "m",
                    "tensor_parallel_size": 1,
                    "num_replicas": 24,
                    "max_model_len": 128,
                    "size": 8,
                }
            ],
        },
        site=default_site_profile(),
        deployment_id="grouped-null",
    )
    model = plan.models[0]
    apps = {
        f"{model.route_name}_g{group_index}": {
            "running": 12,
            "target": 12,
            "route_prefix": f"/{model.route_name}_g{group_index}",
            "status": "RUNNING",
        }
        for group_index in range(2)
    }
    root = CompositionRoot(plan=plan, generation=1, run_dir=str(tmp_path))
    complete = root.application_for_model(model, apps)
    assert complete["running"] == complete["target"] == 24
    assert complete["_observation_state"] == "READY"
    apps[f"{model.route_name}_g1"]["running"] = 11
    degraded = root.application_for_model(model, apps)
    assert degraded["running"] == 23
    assert degraded["target"] == 24
    assert degraded["_observation_state"] == "STARTING"
    apps[f"{model.route_name}_g1"]["running"] = 12
    apps.pop(f"{model.route_name}_g1")
    incomplete = root.application_for_model(model, apps)
    assert incomplete["running"] == 12
    assert incomplete["_observation_state"] == "STARTING"


def test_multi_model_matches_by_route_not_by_luck(tmp_path):
    from exaserve.composition import CompositionRoot

    plan = compile_deployment_plan(
        {
            "num_nodes": 2,
            "num_gpus_per_node": 12,
            "validation_mode": True,
            "models": [
                {
                    "model_id": "org/alpha",
                    "tensor_parallel_size": 1,
                    "num_replicas": 2,
                    "max_model_len": 128,
                    "size": 8,
                },
                {
                    "model_id": "org/beta",
                    "tensor_parallel_size": 1,
                    "num_replicas": 3,
                    "max_model_len": 128,
                    "size": 8,
                },
            ],
        },
        site=default_site_profile(),
        deployment_id="d1",
    )
    apps = {}
    for model in plan.models:
        for index in range(model.num_replicas):
            apps[f"{model.route_name}_r{index}"] = {
                "running": 1,
                "target": 1,
                "route_prefix": f"/{model.route_name}_r{index}",
                "status": "RUNNING",
            }
    root = CompositionRoot(plan=plan, generation=1, run_dir=str(tmp_path))
    assert root.application_for_model(plan.models[0], apps)["running"] == 2
    assert root.application_for_model(plan.models[1], apps)["running"] == 3


def test_no_matching_application_is_none_not_a_guess(tmp_path):
    from exaserve.composition import CompositionRoot

    plan = _plan()
    root = CompositionRoot(plan=plan, generation=1, run_dir=str(tmp_path))
    assert root.application_for_model(plan.models[0], {}) is None


def test_single_replica_application_matching_never_uses_model_substrings(tmp_path):
    from exaserve.composition import CompositionRoot

    plan = compile_deployment_plan(
        {
            "num_nodes": 1,
            "validation_mode": True,
            "models": [
                {
                    "model_id": "org/alpha",
                    "tensor_parallel_size": 1,
                    "num_replicas": 1,
                    "max_model_len": 128,
                    "size": 8,
                }
            ],
        },
        site=default_site_profile(),
        deployment_id="d1",
    )
    root = CompositionRoot(plan=plan, generation=1, run_dir=str(tmp_path))
    misleading = {
        "old-org/alpha-copy": {
            "running": 1,
            "target": 1,
            "status": "RUNNING",
        }
    }

    assert root.application_for_model(plan.models[0], misleading) is None


def test_the_root_exports_its_own_binding_hash(tmp_path, monkeypatch):
    """The root's own receipts are built from the environment, like everyone's.

    Exporting the binding hash only into the ranks' env left the root's GLOBAL
    receipt with an empty allocation_binding_hash, which the strict validator
    rejected -- blocking readiness on `global/supervisor`.
    """
    import os

    from exaserve.composition import CompositionRoot

    monkeypatch.delenv("EXASERVE_ALLOCATION_BINDING_HASH", raising=False)
    plan = _plan()
    root = CompositionRoot(plan=plan, generation=3, run_dir=str(tmp_path), log=lambda *_: None)
    binding = root.bind_allocation([f"n{i}" for i in range(plan.num_nodes)], "job1")
    assert os.environ["EXASERVE_ALLOCATION_BINDING_HASH"] == binding.allocation_binding_hash


def test_a_requested_shutdown_is_not_published_as_a_failure(tmp_path, monkeypatch):
    """exit_code() already keeps 143 distinct from a fault.

    Publishing FAILED on the shared record threw that distinction away again,
    so a consumer read an orderly teardown as a fault.
    """
    from exaserve.composition import CompositionRoot, _is_requested_shutdown

    assert _is_requested_shutdown("supervisor: SHUTDOWN_REQUESTED (exit=None) signal 15")
    assert not _is_requested_shutdown("ray: UNEXPECTED_EXIT (exit=1)")
    assert not _is_requested_shutdown("gateway: UNEXPECTED_EXIT (exit=-15) SIGTERM")

    plan = _plan()
    root = CompositionRoot(plan=plan, generation=1, run_dir=str(tmp_path), log=lambda *_: None)
    root.bind_allocation([f"n{i}" for i in range(plan.num_nodes)], "job1")
    root.supervisor.request_shutdown("signal 15")
    root.fail(str(root.supervisor.first_cause))
    assert read_deployment_status(str(tmp_path)).state == "PLANNED"

    failure_dir = tmp_path / "failure"
    failure_root = CompositionRoot(
        plan=plan, generation=2, run_dir=str(failure_dir), log=lambda *_: None
    )
    failure_root.bind_allocation([f"n{i}" for i in range(plan.num_nodes)], "job1")
    failure_root.fail("ray: UNEXPECTED_EXIT (exit=1) rank 0 component exited unexpectedly")
    assert read_deployment_status(str(failure_dir)).state == "FAILED"
