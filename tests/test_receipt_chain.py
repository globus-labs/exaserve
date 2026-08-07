"""The receipt path from producer to ledger (§3.2.1, IMP-B04).

Each piece of this chain existed before this test and none of them were
connected: producers did not exist, the local hop did not exist, and the head
appended receipts to a list that nothing adjudicated. Every test here pins one
joint of the chain, because "the component exists" was exactly the failure
mode the audit found.
"""

from __future__ import annotations

import json
import os
import socket
import threading

import pytest

from exaserve.compat import producers
from exaserve.compat.local_ingress import (
    LocalReceiptIngress,
    deliver_receipt,
    socket_path_for,
)
from exaserve.compat.receipt_v2 import (
    ExactReceiptLedger,
    ReceiptError,
)
from exaserve.plan.compiler import compile_deployment_plan
from exaserve.plan.contracts import build_allocation_binding
from exaserve.site import default_site_profile

HOST = socket.gethostname()


def _plan(num_nodes: int = 2, gateway: bool = False):
    raw = {
        "num_nodes": num_nodes, "num_gpus_per_node": 12,
        "models": [{"model_id": "m", "tensor_parallel_size": 1,
                    "max_model_len": 128, "size": 8}],
    }
    if gateway:
        raw["gateway"] = {"kind": "haproxy", "port": 4001}
    else:
        raw["validation_mode"] = True
    return compile_deployment_plan(raw, site=default_site_profile(),
                                   deployment_id="d1")


def _binding(plan, nodes=None):
    return build_allocation_binding(
        plan=plan, generation=7, scheduler_allocation_id="job1",
        nodes=nodes or [HOST, "other-node"][:plan.num_nodes])


@pytest.fixture
def identity(monkeypatch, tmp_path):
    plan = _plan()
    binding = _binding(plan)
    monkeypatch.setenv("EXASERVE_DEPLOYMENT_ID", "d1")
    monkeypatch.setenv("EXASERVE_GENERATION", "7")
    monkeypatch.setenv("EXASERVE_PLAN_HASH", plan.deployment_plan_hash)
    monkeypatch.setenv("EXASERVE_SITE_PROFILE_HASH", plan.site_profile_hash)
    monkeypatch.setenv("EXASERVE_ALLOCATION_BINDING_HASH",
                       binding.allocation_binding_hash)
    monkeypatch.setenv("TMPDIR", str(tmp_path))
    return plan, binding


# -- the local hop --------------------------------------------------------
def test_local_hop_delivers_one_payload_unchanged(tmp_path):
    path = str(tmp_path / "d" / "receipts.sock")
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
    assert "dep-3" in path
    assert socket_path_for("dep", 4, root=str(tmp_path)) != path
    ingress = LocalReceiptIngress(path)
    assert ingress.start()
    try:
        assert oct(os.stat(path).st_mode)[-3:] == "600"
        assert oct(os.stat(os.path.dirname(path)).st_mode)[-3:] == "700"
    finally:
        ingress.stop()


def test_local_hop_refuses_an_oversized_frame_without_reading_it(tmp_path):
    path = str(tmp_path / "d" / "receipts.sock")
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
    path = str(tmp_path / "d" / "receipts.sock")
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
    assert deliver_receipt({"a": 1}, path=str(tmp_path / "nope.sock")) is False


# -- producers ------------------------------------------------------------
def test_self_receipt_validates_against_the_plan(identity):
    plan, binding = identity
    receipt = producers.attest_self(
        requirement_id="rank0/node_supervisor", role="node_supervisor",
        component_id="node_supervisor", owner_scope="RANK", owner_rank=0,
        node_id=HOST)
    ledger = ExactReceiptLedger(plan, binding)
    ok, detail = ledger.accept(receipt, required_patch_ids=(),
                               session_rank=0, session_node=HOST)
    assert ok, detail


def test_supervisor_attestation_refuses_a_patched_role(identity, monkeypatch):
    """§3.2.1: a supervisor cannot attest what it cannot see inside."""
    from exaserve.compat.profile import default_profile

    monkeypatch.setenv("EXASERVE_VLLM_PATCH_PP_LAYER_FILTER", "1")
    with pytest.raises(ReceiptError, match="in-process patch"):
        producers.attest_supervisor(
            requirement_id="global/x", role="replica", component_id="replica",
            executable="/bin/true", profile=default_profile("xpu"))


def test_supervisor_attestation_of_an_unmodified_daemon(identity, monkeypatch):
    plan = _plan(gateway=True)
    binding = _binding(plan)
    monkeypatch.setenv("EXASERVE_PLAN_HASH", plan.deployment_plan_hash)
    monkeypatch.setenv("EXASERVE_ALLOCATION_BINDING_HASH",
                       binding.allocation_binding_hash)
    receipt = producers.attest_supervisor(
        requirement_id="global/gateway/haproxy", role="gateway",
        component_id="gateway/haproxy", executable="/bin/true",
        argv=["/bin/true", "-f", "x"], pid=4242)
    assert receipt.attestation_type == "SUPERVISOR"
    assert receipt.owner_rank is None
    ledger = ExactReceiptLedger(plan, binding)
    ok, detail = ledger.accept(receipt, required_patch_ids=(),
                               from_global_authority=True)
    assert ok, detail


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
        requirement_id="rank0/node_supervisor", role="node_supervisor",
        component_id="node_supervisor", owner_scope="RANK", owner_rank=0,
        node_id=HOST)
    head._on_receipt(0, receipt.to_dict())
    assert head.ledger.count() == 1
    assert head.receipt_rejections == []


def test_head_files_a_non_slot_receipt_as_evidence(identity):
    """A replica is not an exactly-planned slot; it must not enter the ledger."""
    plan, binding = identity
    head = _head(plan, binding, _Coordinator({0: _Session(HOST)}))
    head._on_receipt(0, {"receipt_requirement_id": "evidence/replica/n/1"})
    assert head.ledger.count() == 0
    assert len(head.evidence_receipts) == 1
    assert head.receipt_rejections == []


def test_head_rejects_a_receipt_claiming_another_rank(identity):
    plan, binding = identity
    head = _head(plan, binding, _Coordinator({1: _Session(HOST)}))
    receipt = producers.attest_self(
        requirement_id="rank0/node_supervisor", role="node_supervisor",
        component_id="node_supervisor", owner_scope="RANK", owner_rank=0,
        node_id=HOST)
    head._on_receipt(1, receipt.to_dict())      # authenticated as rank 1
    assert head.ledger.count() == 0
    assert any("owner_rank" in r for r in head.receipt_rejections)


def test_head_rejects_a_global_receipt_arriving_from_a_rank(identity):
    plan, binding = identity
    head = _head(plan, binding, _Coordinator({0: _Session(HOST)}))
    receipt = producers.attest_self(
        requirement_id="global/supervisor", role="supervisor",
        component_id="supervisor", owner_scope="GLOBAL", node_id=HOST)
    head._on_receipt(0, receipt.to_dict())
    assert head.ledger.count() == 0
    assert any("GLOBAL" in r for r in head.receipt_rejections)


def test_head_rejects_a_version_one_payload(identity):
    plan, binding = identity
    head = _head(plan, binding, _Coordinator({0: _Session(HOST)}))
    head._on_receipt(0, {"receipt_requirement_id": "rank0/node_supervisor",
                         "schema_version": 1, "role": "node_supervisor"})
    assert head.ledger.count() == 0
    assert any("schema_version" in r for r in head.receipt_rejections)


def test_head_without_a_ledger_does_not_crash(identity):
    plan, binding = identity
    head = _head(plan, binding)
    head.ledger = None
    head._on_receipt(0, {"receipt_requirement_id": "rank0/node_supervisor"})
    assert head.receipt_payloads


# -- end to end -----------------------------------------------------------
def test_producer_to_hop_to_ledger(identity, tmp_path):
    """The full chain: SELF receipt -> local hop -> forward -> ledger slot."""
    plan, binding = identity
    path = str(tmp_path / "hop" / "receipts.sock")
    ingress = LocalReceiptIngress(path)
    assert ingress.start()
    head = _head(plan, binding, _Coordinator({0: _Session(HOST)}))
    try:
        receipt = producers.attest_self(
            requirement_id="rank0/ray_head", role="ray_head",
            component_id="ray", owner_scope="RANK", owner_rank=0, node_id=HOST)
        assert producers.deliver(receipt, path=path)
        for payload in ingress.drain():
            # The supervisor forwards it UNCHANGED; nothing is re-signed here.
            assert payload == json.loads(json.dumps(receipt.to_dict()))
            head._on_receipt(0, payload)
    finally:
        ingress.stop()
    assert head.ledger.count() == 1
    ok, detail = head.ledger.satisfied()
    assert not ok                              # the other slots are still open
    assert "rank0/ray_head" not in detail["missing"]


def test_ledger_is_satisfied_only_by_the_exact_planned_set(identity, monkeypatch):
    plan, binding = identity
    head = _head(plan, binding, _Coordinator({0: _Session(HOST), 1: _Session("other-node")}))
    for rank, node in ((0, HOST), (1, "other-node")):
        for role, component in (("node_supervisor", "node_supervisor"),
                                ("ray_head" if rank == 0 else "ray_worker", "ray")):
            monkeypatch.setenv("EXASERVE_RECEIPT_RANK", str(rank))
            receipt = producers.attest_self(
                requirement_id=f"rank{rank}/{role}", role=role,
                component_id=component, owner_scope="RANK", owner_rank=rank,
                node_id=node)
            head._on_receipt(rank, receipt.to_dict())
    ok, detail = head.ledger.satisfied()
    assert not ok
    assert detail["missing"] == ["global/supervisor"]      # GLOBAL is not a rank's to give
    supervisor = producers.attest_self(
        requirement_id="global/supervisor", role="supervisor",
        component_id="supervisor", owner_scope="GLOBAL", node_id=HOST)
    ok, _ = head.ledger.accept(supervisor, required_patch_ids=(),
                               from_global_authority=True)
    assert ok
    ok, detail = head.ledger.satisfied()
    assert ok, detail


def test_concurrent_producers_all_land(tmp_path):
    path = str(tmp_path / "hop" / "receipts.sock")
    ingress = LocalReceiptIngress(path)
    assert ingress.start()
    try:
        results: list = []
        threads = [threading.Thread(target=lambda i=i: results.append(
            deliver_receipt({"i": i}, path=path))) for i in range(16)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=15)
        drained = ingress.drain()
    finally:
        ingress.stop()
    assert all(results) and len(drained) == 16
    assert sorted(d["i"] for d in drained) == list(range(16))


def test_evidence_filename_is_not_the_ready_record():
    """One file cannot be both the child's evidence and the root's verdict."""
    from exaserve.control.serve_readiness import EVIDENCE_FILENAME

    assert EVIDENCE_FILENAME != "readiness.json"


# -- the Ray-actor authority is retired -----------------------------------
def test_publish_prefers_the_local_hop_over_the_ray_actor(monkeypatch, tmp_path,
                                                          identity):
    """A detached Ray actor is not an authoritative receipt path (§3.2.1)."""
    from exaserve.compat import collector

    path = str(tmp_path / "hop" / "receipts.sock")
    ingress = LocalReceiptIngress(path)
    assert ingress.start()
    monkeypatch.setenv("EXASERVE_RECEIPT_SOCKET", path)
    monkeypatch.setattr(collector, "_get_collector",
                        lambda: pytest.fail("the Ray actor must not be consulted"))
    try:
        v1 = _v1_receipt("replica")
        assert collector.publish_receipt(v1)
        drained = ingress.drain()
    finally:
        ingress.stop()
    assert len(drained) == 1
    assert drained[0]["schema_version"] == 2
    assert drained[0]["receipt_requirement_id"].startswith("evidence/replica/")


def _v1_receipt(role):
    from exaserve.compat.profile import default_profile
    from exaserve.compat.receipt import build_receipt

    return build_receipt(profile=default_profile("xpu"), role=role,
                         deployment_id="d1", generation=7, patch_results={})


def test_server_routes_readiness_to_the_root_when_the_hop_exists(monkeypatch):
    import exaserve.server as server

    monkeypatch.delenv("EXASERVE_ROOT_OWNS_READINESS", raising=False)
    monkeypatch.delenv("EXASERVE_RECEIPT_SOCKET", raising=False)
    assert server._root_owns_readiness() is False
    monkeypatch.setenv("EXASERVE_RECEIPT_SOCKET", "/tmp/x.sock")
    assert server._root_owns_readiness() is True
    monkeypatch.setenv("EXASERVE_ROOT_OWNS_READINESS", "0")
    assert server._root_owns_readiness() is False


def test_the_child_no_longer_creates_the_receipt_actor_on_the_new_path():
    """The creation call must be GUARDED, not merely present."""
    import inspect

    import exaserve.server as server

    source = inspect.getsource(server.main) if hasattr(server, "main") else ""
    if "create_receipt_collector" not in source:
        source = _read_server_source()
    index = source.index("create_receipt_collector()")
    preceding = source[:index]
    assert "_root_owns_readiness()" in preceding.rsplit("if ", 1)[-1] or \
        "not _root_owns_readiness()" in preceding[-400:]


def _read_server_source() -> str:
    import exaserve.server as server

    with open(server.__file__, encoding="utf-8") as handle:
        return handle.read()


def test_rank_main_forwards_receipts_unchanged():
    """The supervisor is a transport, not a co-author."""
    import inspect

    from exaserve import rank_main

    source = inspect.getsource(rank_main._forward_receipts)
    assert "submit_receipt(payload)" in source
    # No rebuilding, re-signing or filtering in the forward path.
    for forbidden in ("attest_self", "finalize()", "to_dict()"):
        assert forbidden not in source


def test_rank_main_starts_the_ingress_before_any_child():
    import inspect

    from exaserve import rank_main

    source = inspect.getsource(rank_main.run)
    assert source.index("ingress.start()") < source.index("node.adopt(")
    assert source.index("_attest_node_supervisor") < source.index("node.start_all()")


def test_launcher_issues_the_global_receipts_before_committing_ready():
    import inspect

    from exaserve import launcher

    source = inspect.getsource(launcher._drive_readiness)
    assert source.index("attest_global()") < source.index("commit_ready(")


# -- node identity --------------------------------------------------------
def test_a_short_hostname_matches_its_bound_fqdn(monkeypatch, tmp_path):
    """The first real run rejected EVERY receipt on this exact mismatch.

    The scheduler's node file is fully qualified; a process reports
    socket.gethostname(), which is not. A literal comparison rejected every
    correctly-placed rank, so readiness could never be satisfied.
    """
    plan = _plan()
    binding = build_allocation_binding(
        plan=plan, generation=7, scheduler_allocation_id="job1",
        nodes=["x4303c4s1b0n0.hsn.cm.aurora.alcf.anl.gov",
               "x4310c4s0b0n0.hsn.cm.aurora.alcf.anl.gov"])
    monkeypatch.setenv("EXASERVE_DEPLOYMENT_ID", "d1")
    monkeypatch.setenv("EXASERVE_GENERATION", "7")
    monkeypatch.setenv("EXASERVE_PLAN_HASH", plan.deployment_plan_hash)
    monkeypatch.setenv("EXASERVE_SITE_PROFILE_HASH", plan.site_profile_hash)
    monkeypatch.setenv("EXASERVE_ALLOCATION_BINDING_HASH",
                       binding.allocation_binding_hash)
    receipt = producers.attest_self(
        requirement_id="rank1/ray_worker", role="ray_worker", component_id="ray",
        owner_scope="RANK", owner_rank=1, node_id="x4310c4s0b0n0")
    ledger = ExactReceiptLedger(plan, binding)
    ok, detail = ledger.accept(receipt, required_patch_ids=(), session_rank=1,
                               session_node="x4310c4s0b0n0")
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
        plan=plan, generation=1, scheduler_allocation_id="j",
        nodes=["a.long.domain", "b.long.domain"])
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
    assert source.index("_attest_node_supervisor") < source.index("poll_start")
    assert source.index("ingress.start()") < source.index("poll_start")
