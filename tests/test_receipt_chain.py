"""The receipt path from producer to ledger (§3.2.1, IMP-B04).

Each piece of this chain existed before this test and none of them were
connected: producers did not exist, the local hop did not exist, and the head
appended receipts to a list that nothing adjudicated. Every test here pins one
joint of the chain, because "the component exists" was exactly the failure
mode the audit found.
"""

from __future__ import annotations

import inspect
import json
import os
import socket
import threading
import time
from types import SimpleNamespace

import pytest

from exaserve.compat import producers
from exaserve.compat.local_ingress import (
    LocalReceiptIngress,
    deliver_receipt,
    deliver_receipt_checked,
    socket_path_for,
)
from exaserve.control.local_ipc import LocalDeliveryError
from exaserve.compat.receipt_v2 import (
    ExactReceiptLedger,
    ReceiptError,
)
from exaserve.plan.compiler import compile_deployment_plan
from exaserve.plan.contracts import build_allocation_binding
from exaserve.site import default_site_profile

HOST = socket.gethostname()


@pytest.fixture(autouse=True)
def _simulate_qualified_dependency_observation(monkeypatch):
    """Receipt protocol tests do not require installing the GPU/Ray stack."""
    from exaserve.compat.profile import default_profile

    profile = default_profile("xpu")
    monkeypatch.setattr(
        producers,
        "_observed_versions",
        lambda: {"python": profile.python, "ray": profile.ray, "vllm": profile.vllm},
    )


def _plan(num_nodes: int = 2, gateway: bool = False, control=None):
    raw = {
        "num_nodes": num_nodes,
        "num_gpus_per_node": 12,
        "models": [{"model_id": "m", "tensor_parallel_size": 1, "max_model_len": 128, "size": 8}],
    }
    if gateway:
        raw["gateway"] = {"kind": "haproxy", "port": 4001}
    else:
        raw["validation_mode"] = True
    if control is not None:
        raw["control"] = dict(control)
    return compile_deployment_plan(raw, site=default_site_profile(), deployment_id="d1")


def _binding(plan, nodes=None):
    return build_allocation_binding(
        plan=plan,
        generation=7,
        scheduler_allocation_id="job1",
        nodes=nodes or [HOST, "other-node"][: plan.num_nodes],
    )


@pytest.fixture
def identity(monkeypatch, tmp_path):
    plan = _plan()
    binding = _binding(plan)
    monkeypatch.setenv("EXASERVE_DEPLOYMENT_ID", "d1")
    monkeypatch.setenv("EXASERVE_GENERATION", "7")
    monkeypatch.setenv("EXASERVE_PLAN_HASH", plan.deployment_plan_hash)
    monkeypatch.setenv("EXASERVE_SITE_PROFILE_HASH", plan.site_profile_hash)
    monkeypatch.setenv("EXASERVE_ALLOCATION_BINDING_HASH", binding.allocation_binding_hash)
    monkeypatch.setenv("TMPDIR", str(tmp_path))
    return plan, binding


# -- the local hop --------------------------------------------------------
def test_local_hop_delivers_one_payload_unchanged(tmp_path):
    path = socket_path_for("local-hop-test", 0, root=str(tmp_path))
    ingress = LocalReceiptIngress(path)
    assert ingress.start()
    try:
        payload = {"receipt_requirement_id": "rank0/ray_head", "nested": {"a": [1, 2]}}
        assert deliver_receipt(payload, path=path)
        drained = ingress.drain()
    finally:
        ingress.stop()
    assert drained == [payload]


def test_local_hop_socket_is_private_and_generation_scoped(tmp_path):
    path = socket_path_for("dep", 3, root=str(tmp_path))
    assert len(os.fsencode(path)) <= 103
    assert socket_path_for("dep", 4, root=str(tmp_path)) != path
    ingress = LocalReceiptIngress(path)
    assert ingress.start()
    try:
        assert oct(os.stat(path).st_mode)[-3:] == "600"
        assert oct(os.stat(os.path.dirname(path)).st_mode)[-3:] == "700"
    finally:
        ingress.stop()


def test_production_receipt_sockets_are_rank_scoped(tmp_path):
    rank0 = socket_path_for("dep", 3, owner_rank=0, root=str(tmp_path))
    rank1 = socket_path_for("dep", 3, owner_rank=1, root=str(tmp_path))
    assert rank0 != rank1
    assert len(os.fsencode(rank0)) <= 103
    assert len(os.fsencode(rank1)) <= 103


@pytest.mark.parametrize("owner_rank", [-1, True, 1.5])
def test_receipt_socket_rejects_invalid_rank_identity(owner_rank):
    with pytest.raises(ValueError, match="owner_rank"):
        socket_path_for("dep", 3, owner_rank=owner_rank)


def test_local_hop_path_never_embeds_untrusted_deployment_identity(tmp_path):
    path = socket_path_for("../../escape", 3, root=str(tmp_path))
    assert "escape" not in path
    assert len(os.fsencode(path)) <= 103
    assert os.path.isabs(path)


def test_production_socket_path_is_independent_of_scheduler_tmpdir(monkeypatch):
    monkeypatch.setenv("TMPDIR", "/tmp/pals-root-process")
    root_path = socket_path_for("dep", 3)
    monkeypatch.setenv("TMPDIR", "/tmp/pals-rank-process")
    rank_path = socket_path_for("dep", 3)
    assert root_path == rank_path
    assert root_path.startswith(f"/tmp/exaserve-ipc-{os.getuid()}/")


def test_local_hop_refuses_an_oversized_frame_without_reading_it(tmp_path):
    path = socket_path_for("oversized-frame-test", 0, root=str(tmp_path))
    ingress = LocalReceiptIngress(path, max_frame_bytes=64)
    assert ingress.start()
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(5)
            client.connect(path)
            client.sendall(b"%011d\n" % (1 << 20))
            assert client.recv(3) == b"NO\n"
        assert ingress.drain() == []
        assert ingress.refused == 1
    finally:
        ingress.stop()


def test_local_hop_caps_the_queue_and_counts_the_drops(tmp_path):
    path = socket_path_for("queue-cap-test", 0, root=str(tmp_path))
    ingress = LocalReceiptIngress(path, max_queued=2)
    assert ingress.start()
    try:
        results = [deliver_receipt({"n": i}, path=path) for i in range(4)]
    finally:
        ingress.stop()
    assert results[:2] == [True, True]
    assert results[2:] == [False, False]
    assert ingress.dropped == 2


def test_delivery_without_a_socket_is_false_not_an_exception(monkeypatch):
    monkeypatch.delenv("EXASERVE_RECEIPT_SOCKET", raising=False)
    assert deliver_receipt({"a": 1}) is False


def test_delivery_to_a_dead_socket_is_false(tmp_path):
    path = socket_path_for("dead-socket-test", 0, root=str(tmp_path))
    assert deliver_receipt({"a": 1}, path=path) is False


def test_required_delivery_names_a_missing_socket(monkeypatch):
    monkeypatch.delenv("EXASERVE_RECEIPT_SOCKET", raising=False)
    with pytest.raises(LocalDeliveryError, match="socket path is empty"):
        deliver_receipt_checked({"a": 1})


def test_required_delivery_preserves_the_transport_cause(tmp_path):
    path = socket_path_for("missing-socket-test", 0, root=str(tmp_path))
    with pytest.raises(LocalDeliveryError, match=r"transport failed.*FileNotFoundError"):
        deliver_receipt_checked({"a": 1}, path=path)


def test_required_delivery_names_a_negative_ingress_ack(tmp_path):
    path = socket_path_for("negative-ack-test", 0, root=str(tmp_path))
    ingress = LocalReceiptIngress(path, max_queued=1)
    assert ingress.start()
    try:
        deliver_receipt_checked({"n": 1}, path=path)
        with pytest.raises(LocalDeliveryError, match="instead of an acceptance ACK"):
            deliver_receipt_checked({"n": 2}, path=path)
    finally:
        ingress.stop()


def test_local_ingress_bounds_reject_boolean_and_scalar_values(tmp_path):
    path = str(tmp_path / "strict.sock")
    with pytest.raises(ValueError, match="bounds"):
        LocalReceiptIngress(path, max_frame_bytes=True)
    with pytest.raises(ValueError, match="path"):
        LocalReceiptIngress(7)  # type: ignore[arg-type]


# -- producers ------------------------------------------------------------
def test_self_receipt_validates_against_the_plan(identity):
    plan, binding = identity
    receipt = producers.attest_self(
        requirement_id="rank0/node_supervisor",
        role="node_supervisor",
        component_id="node_supervisor",
        owner_scope="RANK",
        owner_rank=0,
        node_id=HOST,
    )
    ledger = ExactReceiptLedger(plan, binding)
    ok, detail = ledger.accept(receipt, required_patch_ids=(), session_rank=0, session_node=HOST)
    assert ok, detail


def test_supervisor_attestation_refuses_a_patched_role(identity, monkeypatch):
    """§3.2.1: a supervisor cannot attest what it cannot see inside."""
    from exaserve.compat.profile import default_profile

    monkeypatch.setenv("EXASERVE_VLLM_PATCH_PP_LAYER_FILTER", "1")
    with pytest.raises(ReceiptError, match="in-process patch"):
        producers.attest_supervisor(
            requirement_id="global/x",
            role="replica",
            component_id="replica",
            executable="/bin/true",
            profile=default_profile("xpu"),
        )


def test_supervisor_attestation_of_an_unmodified_daemon(identity, monkeypatch):
    plan = _plan(gateway=True)
    binding = _binding(plan)
    monkeypatch.setenv("EXASERVE_PLAN_HASH", plan.deployment_plan_hash)
    monkeypatch.setenv("EXASERVE_ALLOCATION_BINDING_HASH", binding.allocation_binding_hash)
    receipt = producers.attest_supervisor(
        requirement_id="global/gateway/haproxy",
        role="gateway",
        component_id="gateway/haproxy",
        executable="/bin/true",
        argv=["/bin/true", "-f", "x"],
        pid=4242,
    )
    assert receipt.attestation_type == "SUPERVISOR"
    assert receipt.owner_rank is None
    ledger = ExactReceiptLedger(plan, binding)
    ok, detail = ledger.accept(receipt, required_patch_ids=(), from_global_authority=True)
    assert ok, detail


def test_receipt_provenance_hashes_fail_closed_without_coercion(tmp_path):
    with pytest.raises(ReceiptError, match="argv must be a sequence"):
        producers.argv_hash("python")
    with pytest.raises(ReceiptError, match="non-empty strings"):
        producers.argv_hash(["python", 7])  # type: ignore[list-item]
    with pytest.raises(ReceiptError, match="string mapping"):
        producers.prepared_environment_hash({"PATH": 7})  # type: ignore[dict-item]
    with pytest.raises(ReceiptError, match="could not hash"):
        producers.file_hash(str(tmp_path / "missing"))

    executable = tmp_path / "executable"
    executable.write_bytes(b"first")
    first = producers.file_hash(str(executable))
    executable.write_bytes(b"second")
    assert producers.file_hash(str(executable)) != first


def test_prepared_environment_hash_excludes_the_channel_secret(monkeypatch):
    monkeypatch.setenv("EXASERVE_VENDOR", "xpu")
    monkeypatch.setenv("EXASERVE_CONTROL_SECRET", "aa" * 16)
    first = producers.prepared_environment_hash()
    monkeypatch.setenv("EXASERVE_CONTROL_SECRET", "bb" * 16)
    assert producers.prepared_environment_hash() == first


def test_environment_hash_moves_with_a_deployment_relevant_change(monkeypatch):
    monkeypatch.setenv("EXASERVE_VENDOR", "xpu")
    first = producers.prepared_environment_hash()
    monkeypatch.setenv("EXASERVE_VENDOR", "cuda")
    assert producers.prepared_environment_hash() != first


# -- head-side adjudication ----------------------------------------------
class _Session:
    def __init__(self, node_id):
        self.node_id = node_id


class _Coordinator:
    def __init__(self, sessions):
        self.sessions = sessions


def _head(plan, binding, sessions=None):
    """A HeadChannel-shaped object without binding a real socket."""
    from exaserve.control.channel_runtime import HeadChannel

    head = HeadChannel.__new__(HeadChannel)
    head.ledger = ExactReceiptLedger(plan, binding)
    head.receipt_payloads = []
    head.evidence_receipts = []
    head.receipt_rejections = []
    head.sessions_coordinator = sessions
    return head


def test_head_feeds_a_planned_receipt_into_the_ledger(identity):
    plan, binding = identity
    head = _head(plan, binding, _Coordinator({0: _Session(HOST)}))
    receipt = producers.attest_self(
        requirement_id="rank0/node_supervisor",
        role="node_supervisor",
        component_id="node_supervisor",
        owner_scope="RANK",
        owner_rank=0,
        node_id=HOST,
    )
    head._on_receipt(0, receipt.to_dict())
    assert head.ledger.count() == 1
    assert head.receipt_rejections == []


def test_head_rejects_a_non_slot_receipt(identity):
    """Unplanned evidence cannot enter the authoritative exact-set ledger."""
    plan, binding = identity
    head = _head(plan, binding, _Coordinator({0: _Session(HOST)}))
    head._on_receipt(0, {"receipt_requirement_id": "evidence/replica/n/1"})
    assert head.ledger.count() == 0
    assert not head.evidence_receipts
    assert any("no such planned requirement" in reason for reason in head.receipt_rejections)


def test_real_channel_crosses_snapshot_receipt_ack_and_start_barriers(monkeypatch, tmp_path):
    """Exercise the production coordinator and exact ledger through the wire."""
    from exaserve.control.channel_runtime import (
        HOST_ENV,
        HeadChannel,
        RankClient,
    )
    from exaserve.control.session import SessionCoordinator

    plan = _plan(num_nodes=1)
    binding = _binding(plan, [HOST])
    for key, value in {
        "EXASERVE_DEPLOYMENT_ID": plan.deployment_id,
        "EXASERVE_GENERATION": str(binding.generation),
        "EXASERVE_PLAN_HASH": plan.deployment_plan_hash,
        "EXASERVE_SITE_PROFILE_HASH": plan.site_profile_hash,
        "EXASERVE_ALLOCATION_BINDING_HASH": binding.allocation_binding_hash,
        "TMPDIR": str(tmp_path),
    }.items():
        monkeypatch.setenv(key, value)
    coordinator = SessionCoordinator(plan=plan, binding=binding, log=lambda _: None)
    coordinator.begin_registration()  # this wire test begins at RankLauncher start
    ledger = ExactReceiptLedger(plan, binding)
    head = HeadChannel(
        deployment_id=plan.deployment_id,
        generation=binding.generation,
        plan_hash=plan.deployment_plan_hash,
        expected_ranks=1,
        host="127.0.0.1",
        sessions=coordinator,
        receipts=ledger,
    )
    env = head.env(reachable_host="127.0.0.1")
    env[HOST_ENV] = "127.0.0.1"
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    client = RankClient(rank=0, node_id=HOST)
    try:
        assert client.connect(timeout=10)
        assert not coordinator.all_registered(), "REGISTER crossed the barrier"
        receipt = producers.attest_self(
            requirement_id="rank0/node_supervisor",
            role="node_supervisor",
            component_id="node_supervisor",
            owner_scope="RANK",
            owner_rank=0,
            node_id=HOST,
        )
        assert client.establish(receipt, timeout=10)
        assert head.wait_all_registered(5)
        assert coordinator.all_registered()
        assert ledger.count() == 1

        starter = threading.Thread(target=lambda: client.poll_start(timeout=10))
        starter.start()
        assert head.broadcast_start(timeout=10) == 1
        starter.join(timeout=10)
        assert client.start_received()
    finally:
        client.close()
        head.stop()


def test_live_heartbeat_reconnect_snapshot_and_ordered_drain(monkeypatch, tmp_path):
    import time

    """The runtime—not just the state-machine unit—enforces loss semantics."""
    from exaserve.control.channel_runtime import HeadChannel, RankClient
    from exaserve.control.session import SessionCoordinator

    plan = _plan(
        num_nodes=1,
        control={
            "registration_deadline_s": 5.0,
            "heartbeat_interval_s": 0.05,
            "lease_timeout_s": 0.2,
            "reconnect_grace_s": 1.0,
            "snapshot_assembly_deadline_s": 1.0,
            "watchdog_cleanup_deadline_s": 1.0,
        },
    )
    binding = _binding(plan, [HOST])
    for key, value in {
        "EXASERVE_DEPLOYMENT_ID": plan.deployment_id,
        "EXASERVE_GENERATION": str(binding.generation),
        "EXASERVE_PLAN_HASH": plan.deployment_plan_hash,
        "EXASERVE_SITE_PROFILE_HASH": plan.site_profile_hash,
        "EXASERVE_ALLOCATION_BINDING_HASH": binding.allocation_binding_hash,
        "TMPDIR": str(tmp_path),
    }.items():
        monkeypatch.setenv(key, value)
    coordinator = SessionCoordinator(plan=plan, binding=binding, log=lambda _: None)
    coordinator.begin_registration()  # this wire test begins at RankLauncher start
    ledger = ExactReceiptLedger(plan, binding)
    head = HeadChannel(
        deployment_id=plan.deployment_id,
        generation=binding.generation,
        plan_hash=plan.deployment_plan_hash,
        expected_ranks=1,
        host="127.0.0.1",
        sessions=coordinator,
        receipts=ledger,
    )
    env = head.env(reachable_host="127.0.0.1")
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    client = RankClient(rank=0, node_id=HOST)
    try:
        assert client.connect(timeout=5)
        receipt = producers.attest_self(
            requirement_id="rank0/node_supervisor",
            role="node_supervisor",
            component_id="node_supervisor",
            owner_scope="RANK",
            owner_rank=0,
            node_id=HOST,
        )
        assert client.establish(receipt, timeout=5)
        assert head.wait_all_registered(5)
        starter = threading.Thread(target=lambda: client.poll_start(timeout=5))
        starter.start()
        assert head.broadcast_start(timeout=5) == 1
        starter.join(timeout=5)

        # Drop the live socket. The maintenance loop must reconnect with a
        # complete replacement snapshot, not resume with an incremental.
        client._loop.loop.call_soon_threadsafe(client._channel.drop_connection)
        for _ in range(100):
            if (
                coordinator.sessions[0].reconnects >= 1
                and coordinator.all_registered()
                and client.established
            ):
                break
            import time

            time.sleep(0.03)
        assert coordinator.sessions[0].reconnects >= 1
        assert coordinator.all_registered()
        assert ledger.count() == 1
        assert client.control_failure() is None

        assert head.broadcast_shutdown("DRAIN", timeout=5) == 1
        for _ in range(100):
            if client.shutdown_requested():
                break
            import time

            time.sleep(0.02)
        assert client.shutdown_requested()
        client.close(expected=True)
        assert head.wait_shutdown_goodbyes(deadline=time.monotonic() + 2.0) == 1
        for _ in range(50):
            if coordinator.sessions[0].state == "TERMINAL":
                break
            import time

            time.sleep(0.02)
        assert coordinator.terminal_reason is None
        assert head.rank_failure() is None
    finally:
        client.close(expected=client.shutdown_requested())
        head.stop()


def test_outer_supervisor_loss_triggers_bounded_node_local_cleanup(monkeypatch, tmp_path):
    import sys
    import time

    from exaserve.control.channel_runtime import HeadChannel, RankClient
    from exaserve.control.node_supervisor import NodeSupervisor, ray_component
    from exaserve.control.session import SessionCoordinator

    plan = _plan(
        num_nodes=1,
        control={
            "registration_deadline_s": 5.0,
            "heartbeat_interval_s": 0.05,
            "lease_timeout_s": 0.2,
            "reconnect_grace_s": 0.25,
            "snapshot_assembly_deadline_s": 1.0,
            "watchdog_cleanup_deadline_s": 0.5,
        },
    )
    binding = _binding(plan, [HOST])
    for key, value in {
        "EXASERVE_DEPLOYMENT_ID": plan.deployment_id,
        "EXASERVE_GENERATION": str(binding.generation),
        "EXASERVE_PLAN_HASH": plan.deployment_plan_hash,
        "EXASERVE_SITE_PROFILE_HASH": plan.site_profile_hash,
        "EXASERVE_ALLOCATION_BINDING_HASH": binding.allocation_binding_hash,
        "TMPDIR": str(tmp_path),
    }.items():
        monkeypatch.setenv(key, value)
    coordinator = SessionCoordinator(plan=plan, binding=binding, log=lambda _: None)
    coordinator.begin_registration()  # this wire test begins at RankLauncher start
    ledger = ExactReceiptLedger(plan, binding)
    head = HeadChannel(
        deployment_id=plan.deployment_id,
        generation=binding.generation,
        plan_hash=plan.deployment_plan_hash,
        expected_ranks=1,
        host="127.0.0.1",
        sessions=coordinator,
        receipts=ledger,
    )
    env = head.env(reachable_host="127.0.0.1")
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    client = RankClient(rank=0, node_id=HOST)
    head_stopped = False
    node = NodeSupervisor(
        deployment_id=plan.deployment_id,
        generation=binding.generation,
        plan_hash=plan.deployment_plan_hash,
        rank=0,
        node_id=HOST,
        poll_interval_s=0.02,
    )
    child = node.adopt(ray_component([sys.executable, "-c", "import time; time.sleep(60)"]))
    try:
        assert client.connect(timeout=5)
        receipt = producers.attest_self(
            requirement_id="rank0/node_supervisor",
            role="node_supervisor",
            component_id="node_supervisor",
            owner_scope="RANK",
            owner_rank=0,
            node_id=HOST,
        )
        assert client.establish(receipt, timeout=5)
        assert head.wait_all_registered(5)
        starter = threading.Thread(target=lambda: client.poll_start(timeout=5))
        starter.start()
        assert head.broadcast_start(timeout=5) == 1
        starter.join(timeout=5)
        node.start_all()
        assert child.process is not None and child.process.poll() is None

        head.stop()
        head_stopped = True

        def watchdog_expired():
            failure = client.control_failure()
            if failure:
                node.record_cause("control", "CONTROL_LEASE_LOST", failure)
                return True
            return False

        node.supervise(until=watchdog_expired, timeout_s=3)
        assert client.control_failure() and "grace" in client.control_failure()
        cleanup_started = time.monotonic()
        node.shutdown(drain_s=plan.control.watchdog_cleanup_deadline_s)
        assert time.monotonic() - cleanup_started < 2.0
        assert child.process.poll() is not None
        assert node.exit_code() != 0
    finally:
        node.shutdown(drain_s=0.2)
        client.close()
        if not head_stopped:
            head.stop()


def test_head_rejects_a_receipt_claiming_another_rank(identity):
    plan, binding = identity
    head = _head(plan, binding, _Coordinator({1: _Session(HOST)}))
    receipt = producers.attest_self(
        requirement_id="rank0/node_supervisor",
        role="node_supervisor",
        component_id="node_supervisor",
        owner_scope="RANK",
        owner_rank=0,
        node_id=HOST,
    )
    head._on_receipt(1, receipt.to_dict())  # authenticated as rank 1
    assert head.ledger.count() == 0
    assert any("owner_rank" in r for r in head.receipt_rejections)


def test_head_rejects_a_global_receipt_arriving_from_a_rank(identity):
    plan, binding = identity
    head = _head(plan, binding, _Coordinator({0: _Session(HOST)}))
    receipt = producers.attest_self(
        requirement_id="global/supervisor",
        role="supervisor",
        component_id="supervisor",
        owner_scope="GLOBAL",
        node_id=HOST,
    )
    head._on_receipt(0, receipt.to_dict())
    assert head.ledger.count() == 0
    assert any("GLOBAL" in r for r in head.receipt_rejections)


def test_head_rejects_a_version_one_payload(identity):
    plan, binding = identity
    head = _head(plan, binding, _Coordinator({0: _Session(HOST)}))
    head._on_receipt(
        0,
        {
            "receipt_requirement_id": "rank0/node_supervisor",
            "schema_version": 1,
            "role": "node_supervisor",
        },
    )
    assert head.ledger.count() == 0
    assert any("schema_version" in r for r in head.receipt_rejections)


def test_head_without_a_ledger_does_not_crash(identity):
    plan, binding = identity
    head = _head(plan, binding)
    head.ledger = None
    head._on_receipt(0, {"receipt_requirement_id": "rank0/node_supervisor"})
    assert head.receipt_payloads


def test_production_head_queues_slow_durable_receipts_off_the_listener(
    identity, monkeypatch
):
    """Shared-filesystem latency must not stall heartbeats/reconnect I/O."""
    import threading

    from exaserve.control.channel_runtime import HeadChannel
    from exaserve.compat import receipt_v2

    plan, binding = identity
    started = threading.Event()
    release = threading.Event()

    class SlowLedger:
        def __init__(self):
            self.plan = plan
            self.binding_store = None
            self.accepted = 0

        def accept(self, _receipt, **_kwargs):
            started.set()
            assert release.wait(5), "test did not release the durable writer"
            self.accepted += 1
            return True, "accepted"

    ledger = SlowLedger()
    monkeypatch.setattr(receipt_v2, "receipt_from_dict", lambda payload: dict(payload))
    head = HeadChannel(
        deployment_id=plan.deployment_id,
        generation=binding.generation,
        plan_hash=plan.deployment_plan_hash,
        expected_ranks=plan.num_nodes,
        host="127.0.0.1",
        receipts=ledger,
    )
    try:
        before = time.monotonic()
        head._on_receipt(
            0,
            {
                "receipt_requirement_id": "rank0/node_supervisor",
                "component_id": "node_supervisor",
                "instance_id": "slow",
            },
        )
        assert time.monotonic() - before < 0.25
        assert started.wait(1)
        assert ledger.accepted == 0
        release.set()
        head._durable_tail.result(timeout=2)
        assert ledger.accepted == 1
    finally:
        release.set()
        head.stop()


# -- end to end -----------------------------------------------------------
def test_producer_to_hop_to_ledger(identity, tmp_path):
    """The full chain: SELF receipt -> local hop -> forward -> ledger slot."""
    plan, binding = identity
    path = socket_path_for("producer-ledger-test", 0, root=str(tmp_path))
    ingress = LocalReceiptIngress(path)
    assert ingress.start()
    head = _head(plan, binding, _Coordinator({0: _Session(HOST)}))
    try:
        receipt = producers.attest_self(
            requirement_id="rank0/ray_head",
            role="ray_head",
            component_id="ray",
            owner_scope="RANK",
            owner_rank=0,
            node_id=HOST,
            postcondition=lambda patch_id: patch_id in {"RS-02", "RS-03"},
        )
        assert producers.deliver(receipt, path=path)
        for payload in ingress.drain():
            # The supervisor forwards it UNCHANGED; nothing is re-signed here.
            assert payload == json.loads(json.dumps(receipt.to_dict()))
            head._on_receipt(0, payload)
    finally:
        ingress.stop()
    assert head.ledger.count() == 1
    ok, detail = head.ledger.satisfied()
    assert not ok  # the other slots are still open
    assert "rank0/ray_head" not in detail["missing"]


def test_ledger_is_satisfied_only_by_the_exact_planned_set(identity, monkeypatch):
    plan, binding = identity
    head = _head(plan, binding, _Coordinator({0: _Session(HOST), 1: _Session("other-node")}))
    for rank, node in ((0, HOST), (1, "other-node")):
        for role, component in (
            ("node_supervisor", "node_supervisor"),
            ("ray_head" if rank == 0 else "ray_worker", "ray"),
        ):
            monkeypatch.setenv("EXASERVE_RECEIPT_RANK", str(rank))
            receipt = producers.attest_self(
                requirement_id=f"rank{rank}/{role}",
                role=role,
                component_id=component,
                owner_scope="RANK",
                owner_rank=rank,
                node_id=node,
                postcondition=lambda patch_id, role=role: patch_id == "RS-02"
                or (role == "ray_head" and patch_id == "RS-03"),
            )
            head._on_receipt(rank, receipt.to_dict())
    ok, detail = head.ledger.satisfied()
    assert not ok
    assert "global/supervisor" in detail["missing"]  # GLOBAL is not a rank's to give
    assert any(item.startswith("model/") for item in detail["missing"])
    supervisor = producers.attest_self(
        requirement_id="global/supervisor",
        role="supervisor",
        component_id="supervisor",
        owner_scope="GLOBAL",
        node_id=HOST,
    )
    ok, _ = head.ledger.accept(supervisor, required_patch_ids=(), from_global_authority=True)
    assert ok
    ok, detail = head.ledger.satisfied()
    assert not ok
    assert all(item.startswith("model/") for item in detail["missing"])


def test_concurrent_producers_all_land(tmp_path):
    path = socket_path_for("concurrent-producers-test", 0, root=str(tmp_path))
    ingress = LocalReceiptIngress(path)
    assert ingress.start()
    try:
        results: list = []
        threads = [
            threading.Thread(
                target=lambda i=i: results.append(deliver_receipt({"i": i}, path=path))
            )
            for i in range(16)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=15)
        drained = ingress.drain()
    finally:
        ingress.stop()
    assert all(results) and len(drained) == 16
    assert sorted(d["i"] for d in drained) == list(range(16))


def test_deployment_evidence_has_no_shared_file_contract():
    """The child's typed IPC must not regress into a second status file."""
    from exaserve.control import serve_readiness

    assert not hasattr(serve_readiness, "EVIDENCE_FILENAME")


# -- the Ray-actor authority is retired -----------------------------------
def test_publish_delivers_exact_v2_unchanged(monkeypatch, tmp_path, identity):
    """A detached Ray actor is not an authoritative receipt path (§3.2.1)."""
    from exaserve.compat import collector

    path = socket_path_for("collector-publish-test", 0, root=str(tmp_path))
    ingress = LocalReceiptIngress(path)
    assert ingress.start()
    monkeypatch.setenv("EXASERVE_RECEIPT_SOCKET", path)
    try:
        receipt = producers.attest_self(
            requirement_id="rank0/node_supervisor",
            role="node_supervisor",
            component_id="node_supervisor",
            owner_scope="RANK",
            owner_rank=0,
            node_id=HOST,
        )
        assert collector.publish_receipt(receipt)
        drained = ingress.drain()
    finally:
        ingress.stop()
    assert len(drained) == 1
    assert drained[0] == receipt.to_dict()


def test_the_child_never_decides_readiness():
    """WP13: there is no switch left. The child is always a witness."""
    import exaserve
    from pathlib import Path

    source = (Path(exaserve.__file__).parent / "server.py").read_text(encoding="utf-8")
    assert "_root_owns_readiness" not in source
    assert "observe_deployment(" in source
    assert "enforce_readiness" not in source
    assert "_deploy_manager.validate()" not in source


def test_the_child_cannot_create_a_receipt_actor_at_all():
    """Not guarded -- deleted. A guarded violation is still reachable."""
    import exaserve
    from pathlib import Path

    source = (Path(exaserve.__file__).parent / "server.py").read_text(encoding="utf-8")
    assert "create_receipt_collector" not in source
    assert "drain_receipts" not in source


def test_serving_stats_retention_is_bounded_but_total_is_exact(monkeypatch):
    pytest.importorskip("ray", exc_type=ImportError)
    import exaserve.server as server

    monkeypatch.setenv("EXASERVE_STATS_RETENTION", "9999")
    monkeypatch.setattr(server.CollectingStatLogger, "_retention_limit", 5)
    logger = server.CollectingStatLogger()
    for index in range(25):
        request = SimpleNamespace(
            e2e_latency=float(index),
            queued_time=0.1,
            prefill_time=0.2,
            decode_time=0.3,
            num_generation_tokens=4,
        )
        logger.record(
            scheduler_stats=SimpleNamespace(
                num_running_reqs=1, num_waiting_reqs=0, kv_cache_usage=0.5
            ),
            iteration_stats=SimpleNamespace(finished_requests=[request]),
        )
    assert len(logger.finished_requests) == 5
    assert logger.summary()["total_requests"] == 25
    live = logger.live_snapshot()
    assert live["total_finished_requests"] == 25
    assert live["retained_finished_requests"] == 5


def test_rank_main_forwards_receipts_unchanged():
    """The supervisor is a transport, not a co-author."""
    from exaserve import rank_main

    payload = {"receipt_requirement_id": "rank0/ray_head", "opaque": [1, 2, 3]}

    class Ingress:
        def __init__(self):
            self.batch = [payload]

        def drain(self):
            batch, self.batch = self.batch, []
            return batch

    class Channel:
        def __init__(self):
            self.received = []

        def submit_receipt(self, value):
            self.received.append(value)
            return True

        def control_failure(self):
            return None

    ingress = Ingress()
    channel = Channel()
    forwarder = rank_main._forward_receipts(channel, ingress, 0, poll_s=0.01)
    assert forwarder.stop(timeout_s=1.0)
    assert channel.received == [payload]
    assert channel.received[0] is payload


def test_rank_main_receipt_forwarder_surfaces_delivery_failure():
    from exaserve import rank_main

    class Ingress:
        def __init__(self):
            self.batch = [{"receipt_requirement_id": "rank0/ray_head"}]

        def drain(self):
            batch, self.batch = self.batch, []
            return batch

    class Channel:
        def submit_receipt(self, _value):
            return False

        def control_failure(self):
            return "receipt delivery failed"

    forwarder = rank_main._forward_receipts(Channel(), Ingress(), 0, poll_s=0.01)
    assert not forwarder.stop(timeout_s=1.0)
    assert forwarder.failure == "receipt delivery failed"


def test_rank_main_receipt_forwarder_retains_batch_across_reconnect():
    """A permitted transient disconnect must neither kill the rank nor lose evidence."""
    from exaserve import rank_main

    first = {"receipt_requirement_id": "rank0/first"}
    second = {"receipt_requirement_id": "rank0/second"}

    class Ingress:
        def __init__(self):
            self.batch = [first, second]
            self.drain_count = 0

        def drain(self):
            self.drain_count += 1
            batch, self.batch = self.batch, []
            return batch

    class Channel:
        def __init__(self):
            self.available = False
            self.received = []

        def submit_receipt(self, value):
            if not self.available:
                self.available = True
                return False
            self.received.append(value)
            return True

        def control_failure(self):
            return None

    ingress = Ingress()
    channel = Channel()
    forwarder = rank_main._forward_receipts(channel, ingress, 0, poll_s=0.01)
    deadline = time.monotonic() + 1.0
    while len(channel.received) != 2 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert forwarder.stop(timeout_s=1.0)
    assert channel.received == [first, second]
    assert channel.received[0] is first
    assert channel.received[1] is second
    assert ingress.drain_count >= 1


def test_rank_main_receipt_forwarder_fails_if_shutdown_strands_pending_batch():
    from exaserve import rank_main

    class Ingress:
        def __init__(self):
            self.batch = [{"receipt_requirement_id": "rank0/ray_head"}]

        def drain(self):
            batch, self.batch = self.batch, []
            return batch

    class Channel:
        def submit_receipt(self, _value):
            return False

        def control_failure(self):
            return None

    forwarder = rank_main._forward_receipts(Channel(), Ingress(), 0, poll_s=0.01)
    assert not forwarder.stop(timeout_s=1.0)
    assert forwarder.failure == "1 receipt(s) remained undelivered at shutdown"


def test_rank_registration_cleanup_shares_one_absolute_deadline(monkeypatch):
    from exaserve import rank_main

    ticks = iter((100.0, 101.0, 102.0, 103.0))
    monkeypatch.setattr(rank_main.time, "monotonic", lambda: next(ticks))

    class Forwarder:
        failure = None

        def __init__(self):
            self.timeouts = []

        def stop(self, *, timeout_s):
            self.timeouts.append(timeout_s)
            return True

    class Ingress:
        def __init__(self):
            self.timeouts = []

        def stop(self, *, timeout_s):
            self.timeouts.append(timeout_s)
            return True

    class Channel:
        def __init__(self):
            self.deadlines = []

        def close(self, *, expected, deadline):
            assert expected is False
            self.deadlines.append(deadline)

    forwarder = Forwarder()
    ingress = Ingress()
    channel = Channel()
    assert rank_main._cleanup_registration_resources(
        channel=channel,
        ingress=ingress,
        rank=0,
        forwarder=forwarder,
        timeout_s=10.0,
    )
    assert forwarder.timeouts == [9.0]
    assert ingress.timeouts == [8.0]
    assert channel.deadlines == [110.0]


def test_rank_registration_cleanup_rejects_invalid_budget():
    import pytest

    from exaserve import rank_main

    with pytest.raises(ValueError, match="finite and nonnegative"):
        rank_main._cleanup_registration_resources(
            channel=object(), ingress=object(), rank=0, timeout_s=float("inf")
        )


def test_rank_main_starts_the_ingress_before_any_child():
    import inspect

    from exaserve import rank_main

    source = inspect.getsource(rank_main.run)
    assert source.index("ingress.start()") < source.index("node.adopt(")
    assert source.index("_build_node_supervisor_receipt") < source.index("node.start_all(")
    assert source.index("channel.establish") < source.index("node.start_all(")


def test_launcher_issues_the_global_receipts_before_committing_ready():
    import inspect

    from exaserve import launcher

    source = inspect.getsource(launcher._drive_readiness)
    assert source.index("attest_global()") < source.index("commit_ready(")


def test_deployment_bootstrap_verifies_plan_and_profile_before_ray_import():
    """The implementation module imports Ray; its bootstrap must not."""
    import inspect

    from exaserve import server_bootstrap
    from exaserve.control import ray_runtime

    source = inspect.getsource(server_bootstrap.main)
    verify = source.index("CompatibilityActivator(profile=profile).activate")
    implementation_import = source.index("from . import server")
    assert verify < implementation_import
    assert "import ray" not in source[:implementation_import]
    assert ray_runtime.server_argv("p.plan.json")[2:4] == ["exaserve.server_bootstrap", "--plan"]


# -- node identity --------------------------------------------------------
def test_a_short_hostname_matches_its_bound_fqdn(monkeypatch, tmp_path):
    """The first real run rejected EVERY receipt on this exact mismatch.

    The scheduler's node file is fully qualified; a process reports
    socket.gethostname(), which is not. A literal comparison rejected every
    correctly-placed rank, so readiness could never be satisfied.
    """
    plan = _plan()
    binding = build_allocation_binding(
        plan=plan,
        generation=7,
        scheduler_allocation_id="job1",
        nodes=[
            "x4303c4s1b0n0.hsn.cm.aurora.alcf.anl.gov",
            "x4310c4s0b0n0.hsn.cm.aurora.alcf.anl.gov",
        ],
    )
    monkeypatch.setenv("EXASERVE_DEPLOYMENT_ID", "d1")
    monkeypatch.setenv("EXASERVE_GENERATION", "7")
    monkeypatch.setenv("EXASERVE_PLAN_HASH", plan.deployment_plan_hash)
    monkeypatch.setenv("EXASERVE_SITE_PROFILE_HASH", plan.site_profile_hash)
    monkeypatch.setenv("EXASERVE_ALLOCATION_BINDING_HASH", binding.allocation_binding_hash)
    receipt = producers.attest_self(
        requirement_id="rank1/ray_worker",
        role="ray_worker",
        component_id="ray",
        owner_scope="RANK",
        owner_rank=1,
        node_id="x4310c4s0b0n0",
        postcondition=lambda patch_id: patch_id == "RS-02",
    )
    ledger = ExactReceiptLedger(plan, binding)
    ok, detail = ledger.accept(
        receipt,
        required_patch_ids=("RS-02",),
        session_rank=1,
        session_node="x4310c4s0b0n0",
    )
    assert ok, detail


def test_a_different_host_is_still_rejected():
    """Canonicalizing must not blur two real hosts together."""
    from exaserve.plan.contracts import same_node

    assert same_node("n7.example.gov", "N7")
    assert not same_node("n7.example.gov", "n8.example.gov")
    assert not same_node("", "n7")


def test_the_binding_keeps_the_name_the_scheduler_wrote():
    """Comparison is canonical; the RECORD is faithful."""
    plan = _plan()
    binding = build_allocation_binding(
        plan=plan,
        generation=1,
        scheduler_allocation_id="j",
        nodes=["a.long.domain", "b.long.domain"],
    )
    assert binding.node_for(0) == "a.long.domain"
    assert binding.is_bound_node(0, "a")
    assert not binding.is_bound_node(0, "b")


def test_the_supervisor_receipt_is_produced_before_the_start_gate():
    """§3.2.1: REGISTER does not count until the supervisor receipt is accepted.

    Producing it after START would mean the head released the gate on evidence
    it did not yet have.
    """
    import inspect

    from exaserve import rank_main

    source = inspect.getsource(rank_main.run)
    assert source.index("_build_node_supervisor_receipt") < source.index("poll_start")
    assert source.index("channel.establish") < source.index("poll_start")
    assert source.index("ingress.start()") < source.index("poll_start")


def test_stale_ray_state_is_cleared_before_any_child_starts():
    """A generation must not inherit a prior generation's node-local state.

    The ordering IS the safety argument: nothing of this generation exists on
    the node yet when the preflight runs.
    """
    import inspect

    from exaserve import rank_main

    source = inspect.getsource(rank_main.run)
    assert source.index("_clear_stale_ray_state") < source.index("node.adopt(")
    assert source.index("poll_start") < source.index("_clear_stale_ray_state")


def test_the_ray_preflight_has_no_ambient_bypass():
    from exaserve import rank_main

    source = inspect.getsource(rank_main._clear_stale_ray_state)
    assert "EXASERVE_SKIP_RAY_PREFLIGHT" not in source


def test_the_preflight_uses_exact_ownership_not_process_name_patterns():
    """A cleanup must never signal a PID merely because its argv looks familiar."""
    import inspect

    from exaserve import rank_main

    source = inspect.getsource(rank_main._clear_stale_ray_state)
    assert "cleanup_stale_owned_processes" in source
    assert "pkill" not in source
    assert "ray.scripts.scripts" not in source
