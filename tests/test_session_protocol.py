"""P03 / IMP-B06: fail-closed registration, START gating, reconnect, loss anchors."""

from __future__ import annotations

import pytest

from exaserve.compat.receipt_v2 import canonical_hash
from exaserve.control.session import GenerationState, SessionCoordinator, SessionState
from exaserve.plan.compiler import compile_deployment_plan
from exaserve.plan.contracts import ControlLimits, SiteProfile, build_allocation_binding


class _Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


def _plan(nodes=2, **limits):
    control = ControlLimits(
        registration_deadline_s=100.0,
        reconnect_grace_s=30.0,
        heartbeat_interval_s=1.0,
        lease_timeout_s=10.0,
        snapshot_assembly_deadline_s=20.0,
        watchdog_cleanup_deadline_s=40.0,
        **limits,
    )
    site = SiteProfile(
        schema_version=3,
        site_id="s",
        max_nodes=64,
        gpus_per_node=12,
        cpus_per_node=64,
        scheduler_types=("pbs",),
        gateway_kinds=("haproxy",),
        vendors=("xpu",),
        engines=("vllm",),
        model_storage_path="/m",
        local_stage_path="/t",
        launcher_capabilities=("ray_serve.run_many",),
        control=control,
    ).finalize()
    raw = {
        "num_nodes": nodes,
        "models": [
            {"model_id": "a/b", "tensor_parallel_size": 1, "max_model_len": 4096, "size": 8}
        ],
        "gateway": {"kind": "haproxy", "port": 4001},
    }
    return compile_deployment_plan(raw, site=site, deployment_id="d")


def _coord(nodes=2, clock=None):
    plan = _plan(nodes)
    binding = build_allocation_binding(
        plan=plan, generation=1, scheduler_allocation_id="j", nodes=[f"n{i}" for i in range(nodes)]
    )
    return SessionCoordinator(
        plan=plan, binding=binding, clock=clock or _Clock(), log=lambda *_: None
    )


def _establish(coord, rank, *, items=None, instance="i1"):
    node = coord.binding.node_for(rank)
    assert coord.register(rank, node, instance)[0]
    items = items if items is not None else [{"obs": rank}]
    assert coord.begin_snapshot(rank, f"snap{rank}", 1, canonical_hash(items))[0]
    assert coord.add_snapshot_chunk(rank, 0, items)[0]
    ok, why = coord.complete_snapshot(rank, supervisor_receipt=True)
    assert ok, why
    session = coord.sessions[rank]
    return coord.acknowledge_snapshot(
        rank,
        command_id=session.snapshot_command_id,
        snapshot_id=session.snapshot_id,
        complete_set_hash=session.complete_set_hash,
        succeeded=True,
    )


# -- registration is the START gate ------------------------------------------


def test_no_start_until_every_rank_is_established():
    coord = _coord(3)
    _establish(coord, 0)
    ok, why = coord.may_start()
    assert not ok and "not established" in why
    _establish(coord, 1)
    _establish(coord, 2)
    ok, why = coord.may_start()
    assert ok, why
    assert coord.start()[0] and coord.generation_state == GenerationState.STARTED.value


def test_register_alone_does_not_count_as_registered():
    """REGISTER without a complete snapshot + supervisor receipt is not enough."""
    coord = _coord(1)
    assert coord.register(0, "n0", "i1")[0]
    assert not coord.all_registered()
    assert not coord.may_start()[0]


def test_validated_snapshot_does_not_count_until_matching_ack_result():
    coord = _coord(1)
    assert coord.register(0, "n0", "i1")[0]
    items = [{"obs": 1}]
    assert coord.begin_snapshot(0, "s", 1, canonical_hash(items))[0]
    assert coord.add_snapshot_chunk(0, 0, items)[0]
    ok, why = coord.complete_snapshot(0, supervisor_receipt=True)
    assert ok and "acknowledgment" in why
    assert coord.sessions[0].state == SessionState.SNAPSHOT_ACK_PENDING.value
    assert not coord.all_registered()
    session = coord.sessions[0]
    ok, why = coord.acknowledge_snapshot(
        0,
        command_id="wrong",
        snapshot_id=session.snapshot_id,
        complete_set_hash=session.complete_set_hash,
        succeeded=True,
    )
    assert not ok and "command_id mismatch" in why
    assert not coord.all_registered()
    assert coord.acknowledge_snapshot(
        0,
        command_id=session.snapshot_command_id,
        snapshot_id=session.snapshot_id,
        complete_set_hash=session.complete_set_hash,
        succeeded=True,
    )[0]
    assert coord.all_registered()


def test_a_snapshot_without_the_supervisor_receipt_is_refused():
    coord = _coord(1)
    coord.register(0, "n0", "i1")
    items = [{"obs": 1}]
    coord.begin_snapshot(0, "s", 1, canonical_hash(items))
    coord.add_snapshot_chunk(0, 0, items)
    ok, why = coord.complete_snapshot(0, supervisor_receipt=False)
    assert not ok and "supervisor receipt" in why


def test_short_and_fully_qualified_bound_node_names_are_equivalent():
    coord = _coord(1)
    # Replace the test binding's short node with the scheduler-style FQDN.
    from dataclasses import replace

    coord.binding = replace(coord.binding, rank_to_node=((0, "n0.example.org"),))
    assert coord.register(0, "n0", "i1")[0]


def test_one_missing_rank_at_the_deadline_is_terminal():
    clock = _Clock()
    coord = _coord(2, clock=clock)
    _establish(coord, 0)
    clock.advance(101.0)
    reason = coord.check_registration_deadline()
    assert reason and "not established" in reason
    assert coord.generation_state == GenerationState.TERMINAL.value
    assert not coord.may_start()[0]


def test_delayed_registration_within_the_deadline_succeeds():
    clock = _Clock()
    coord = _coord(2, clock=clock)
    _establish(coord, 0)
    clock.advance(99.0)
    _establish(coord, 1)
    assert coord.check_registration_deadline() is None
    assert coord.may_start()[0]


def test_an_unplanned_rank_is_session_local_not_generation_fatal():
    coord = _coord(2)
    ok, why = coord.register(7, "n7", "i")
    assert not ok and "not in the allocation binding" in why
    assert coord.generation_state == GenerationState.REGISTERING.value


def test_an_authenticated_rank_asserting_the_wrong_node_is_generation_fatal():
    coord = _coord(2)
    ok, why = coord.register(0, "somewhere-else", "i1")
    assert not ok
    assert coord.generation_state == GenerationState.TERMINAL.value


# -- snapshots ---------------------------------------------------------------


def test_an_incomplete_snapshot_is_refused():
    coord = _coord(1)
    coord.register(0, "n0", "i1")
    coord.begin_snapshot(0, "s", 3, "")
    coord.add_snapshot_chunk(0, 0, [{"a": 1}])
    ok, why = coord.complete_snapshot(0, supervisor_receipt=True)
    assert not ok and "incomplete snapshot" in why


def test_a_mixed_snapshot_fails_the_content_hash():
    coord = _coord(1)
    coord.register(0, "n0", "i1")
    coord.begin_snapshot(0, "s", 1, canonical_hash([{"expected": 1}]))
    coord.add_snapshot_chunk(0, 0, [{"different": 2}])
    ok, why = coord.complete_snapshot(0, supervisor_receipt=True)
    assert not ok and "hash mismatch" in why


def test_conflicting_duplicate_chunks_are_refused():
    coord = _coord(1)
    coord.register(0, "n0", "i1")
    coord.begin_snapshot(0, "s", 2, "")
    coord.add_snapshot_chunk(0, 0, [{"a": 1}])
    ok, why = coord.add_snapshot_chunk(0, 0, [{"a": 2}])
    assert not ok and "conflicting duplicate" in why


def test_snapshot_bounds_are_enforced():
    coord = _coord(1)
    coord.register(0, "n0", "i1")
    ok, why = coord.begin_snapshot(0, "s", 10_000, "")
    assert not ok and "max_snapshot_chunks" in why


def test_snapshot_assembly_has_a_deadline():
    clock = _Clock()
    coord = _coord(1, clock=clock)
    coord.register(0, "n0", "i1")
    coord.begin_snapshot(0, "s", 2, "")
    clock.advance(25.0)
    ok, why = coord.add_snapshot_chunk(0, 0, [{"a": 1}])
    assert not ok and "deadline expired" in why


# -- loss, grace, reconnect --------------------------------------------------


def test_disconnect_revokes_readiness_immediately():
    coord = _coord(2)
    _establish(coord, 0)
    _establish(coord, 1)
    assert coord.readiness_revoked_ranks() == ()
    coord.on_disconnect(1)
    assert coord.readiness_revoked_ranks() == (1,)


def test_reconnect_inside_grace_recovers_only_with_a_full_snapshot():
    clock = _Clock()
    coord = _coord(1, clock=clock)
    _establish(coord, 0)
    coord.on_disconnect(0)
    clock.advance(10.0)
    ok, why = coord.reconnect(0, "n0", "i2")
    assert ok and "complete snapshot required" in why
    # An incremental before the replacement snapshot is refused.
    ok, why = coord.accept_incremental(0)
    assert not ok and "before a complete snapshot" in why
    assert _establish(coord, 0, instance="i2")[0]
    assert coord.accept_incremental(0)[0]


def test_reconnect_after_grace_is_terminal():
    clock = _Clock()
    coord = _coord(1, clock=clock)
    _establish(coord, 0)
    coord.on_disconnect(0)
    clock.advance(31.0)
    ok, why = coord.reconnect(0, "n0", "i2")
    assert not ok and "grace expired" in why
    assert coord.generation_state == GenerationState.TERMINAL.value


def test_a_later_reconnect_cannot_resurrect_a_terminal_generation():
    clock = _Clock()
    coord = _coord(1, clock=clock)
    _establish(coord, 0)
    coord.on_disconnect(0)
    clock.advance(31.0)
    coord.check_grace_deadlines()
    ok, why = coord.reconnect(0, "n0", "i3")
    assert not ok and "terminal" in why


def test_silent_loss_anchors_at_last_heartbeat_plus_lease():
    clock = _Clock()
    coord = _coord(1, clock=clock)
    _establish(coord, 0)
    heartbeat_at = clock.t
    clock.advance(11.0)
    lost = coord.poll_leases()
    assert lost == [0]
    assert coord.sessions[0].loss_time == pytest.approx(heartbeat_at + 10.0)


def test_grace_is_not_double_counted_with_the_lease():
    clock = _Clock()
    coord = _coord(1, clock=clock)
    _establish(coord, 0)
    anchor = clock.t
    clock.advance(11.0)
    coord.poll_leases()
    deadline = coord.cleanup_deadline_for(0)
    # loss_time(=anchor+10) + grace(30) + cleanup(40); NOT lease+grace+lease.
    assert deadline == pytest.approx(anchor + 10.0 + 30.0 + 40.0)


def test_expected_and_unsolicited_goodbye_differ():
    coord = _coord(2)
    _establish(coord, 0)
    _establish(coord, 1)
    coord.request_drain(0)
    # Authorizing GOODBYE does not turn an abrupt EOF into GOODBYE.
    assert coord.on_disconnect(0, expected=False) == "lost"
    assert coord.sessions[0].state == SessionState.LOST.value
    # Rank 1 emits the actual protocol frame after an acknowledged drain.
    coord.request_drain(1)
    assert coord.on_disconnect(1, expected=True) == "expected goodbye"
    assert coord.sessions[1].state == SessionState.TERMINAL.value


def test_disconnect_without_drain_is_a_loss():
    coord = _coord(1)
    _establish(coord, 0)
    assert coord.on_disconnect(0) == "lost"
    assert coord.sessions[0].state == SessionState.LOST.value
    assert coord.sessions[0].loss_time is not None


def test_heartbeats_keep_a_session_alive():
    clock = _Clock()
    coord = _coord(1, clock=clock)
    _establish(coord, 0)
    for _ in range(20):
        clock.advance(5.0)
        coord.on_heartbeat(0)
        assert coord.poll_leases() == []
    assert coord.sessions[0].is_established()
