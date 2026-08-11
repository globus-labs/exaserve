"""WP4.4: the head learns of rank failure without waiting for the launch."""

from __future__ import annotations


import pytest

from exaserve.control.channel_runtime import (
    DEPLOYMENT_ENV,
    GENERATION_ENV,
    HOST_ENV,
    PLAN_HASH_ENV,
    PORT_ENV,
    SECRET_ENV,
    HeadChannel,
    RankClient,
    _completion_timeout,
)
from exaserve.control.contracts import ComponentState


@pytest.mark.parametrize(
    "kwargs",
    [
        {"generation": True},
        {"expected_ranks": 0},
        {"deployment_id": ""},
        {"host": ""},
    ],
)
def test_head_channel_identity_is_strict(kwargs):
    parameters = {
        "deployment_id": "d1",
        "generation": 7,
        "plan_hash": "h",
        "expected_ranks": 1,
        "host": "127.0.0.1",
    }
    parameters.update(kwargs)
    with pytest.raises(ValueError):
        HeadChannel(**parameters)


@pytest.mark.parametrize("rank", [True, "0", -1])
def test_rank_client_rank_is_strict(rank):
    with pytest.raises(ValueError):
        RankClient(rank=rank)


def test_head_listener_startup_failure_reaps_its_event_loop(monkeypatch):
    import threading
    import time

    from exaserve.control import channel_runtime

    before = sum(thread.name == "exaserve-control" for thread in threading.enumerate())

    async def fail_start(_self):
        raise OSError("injected bind failure")

    monkeypatch.setattr(channel_runtime.ControlListener, "start", fail_start)
    with pytest.raises(OSError, match="injected bind failure"):
        HeadChannel(
            deployment_id="d",
            generation=1,
            plan_hash="p",
            expected_ranks=1,
            host="127.0.0.1",
        )
    for _ in range(50):
        after = sum(thread.name == "exaserve-control" for thread in threading.enumerate())
        if after == before:
            break
        time.sleep(0.01)
    assert after == before


def test_head_channel_stop_is_idempotent(head):
    assert head.stop()
    assert head.stop()


@pytest.fixture
def head():
    channel = HeadChannel(
        deployment_id="d1", generation=7, plan_hash="h", expected_ranks=2, host="127.0.0.1"
    )
    try:
        yield channel
    finally:
        channel.stop()


def _rank_env(head: HeadChannel, monkeypatch) -> None:
    env = head.env(reachable_host="127.0.0.1")
    env[HOST_ENV] = "127.0.0.1"  # the fixture binds loopback
    for key, value in env.items():
        monkeypatch.setenv(key, value)


def _supervisor_receipt(client: RankClient) -> dict:
    return {
        "receipt_requirement_id": f"rank{client.rank}/node_supervisor",
        "component_id": "node_supervisor",
        "instance_id": f"{client.node_id}:supervisor",
        "owner_scope": "RANK",
        "owner_rank": client.rank,
        "node_id": client.node_id,
    }


def _establish(client: RankClient, observations=()) -> None:
    assert client.establish(
        _supervisor_receipt(client), observations=list(observations), timeout=10
    )


def test_a_rank_registers_and_its_observations_reach_the_head(head, monkeypatch):
    _rank_env(head, monkeypatch)
    client = RankClient(rank=0)
    assert client.connect(timeout=10), "rank could not register"
    _establish(client)
    try:
        assert client.observe("ray_head", ComponentState.RUNNING.value)
        for _ in range(50):
            if head.observations:
                break
            import time

            time.sleep(0.1)
        assert head.observations, "no observation reached the head"
        obs = head.observations[-1]
        assert obs.component_id == "ray_head" and obs.owner_rank == 0
        assert head.rank_failure() is None
    finally:
        client.close()


def test_a_fatal_rank_observation_is_a_run_failure(head, monkeypatch):
    _rank_env(head, monkeypatch)
    client = RankClient(rank=1)
    assert client.connect(timeout=10)
    _establish(client)
    try:
        client.observe(
            "deployment",
            ComponentState.FAILED.value,
            reason_code="UNEXPECTED_EXIT",
            detail="server exited 9",
        )
        import time

        for _ in range(50):
            if head.rank_failure():
                break
            time.sleep(0.1)
        failure = head.rank_failure()
        assert failure and "rank 1" in failure and "server exited 9" in failure
    finally:
        client.close()


def test_launcher_exit_evidence_prefers_the_typed_component_failure(head):
    with head._state_lock:
        head.failures.append("rank 0 component ray: exit=137")
        head._unexpected_disconnects.append(1)

    assert head.launcher_exit_evidence(143, wait_s=0.01) == (
        "rank 0 component ray: exit=137; rank launcher exit=143"
    )


def test_launcher_exit_evidence_preserves_unexpected_authenticated_disconnect(head):
    with head._state_lock:
        head._unexpected_disconnects.append(1)

    assert head.launcher_exit_evidence(143, wait_s=0.01) == (
        "authenticated rank control session disappeared without GOODBYE "
        "for rank(s) [1]; rank launcher exit=143"
    )


def test_launcher_exit_evidence_keeps_the_first_disconnect_not_sorted_collateral(head):
    with head._state_lock:
        # Rank 1 is the injected failure.  Rank 0 disconnects only because the
        # MPI launcher tears down its surviving sibling afterward.
        head._unexpected_disconnects.extend((1, 0))

    assert head.launcher_exit_evidence(143, wait_s=0.01) == (
        "authenticated rank control session disappeared without GOODBYE "
        "for rank(s) [1]; rank launcher exit=143"
    )


@pytest.mark.parametrize("value", [True, 1.5, "143"])
def test_launcher_exit_evidence_rejects_invalid_exit_codes(head, value):
    with pytest.raises(ValueError, match="exit code"):
        head.launcher_exit_evidence(value, wait_s=0.01)


@pytest.mark.parametrize("value", [True, 0, -1, float("nan"), float("inf")])
def test_launcher_exit_evidence_has_a_bounded_positive_wait(head, value):
    with pytest.raises(ValueError, match="finite and positive"):
        head.launcher_exit_evidence(1, wait_s=value)


def test_losing_a_rank_lease_is_itself_a_failure(head, monkeypatch):
    """A rank we can no longer observe is not a quiet event."""
    _rank_env(head, monkeypatch)
    client = RankClient(rank=0)
    assert client.connect(timeout=10)
    _establish(client)
    # The close must be scheduled on the client's OWN loop; calling it from
    # this thread never actually shuts the socket down.
    client._loop.loop.call_soon_threadsafe(client._channel.drop_connection)
    import time

    for _ in range(60):
        if head.rank_failure():
            break
        time.sleep(0.1)
    assert head.rank_failure(), "a lost control lease must fail the run"
    assert "lease lost" in head.rank_failure()
    client.close()


def test_the_rank_client_is_a_noop_without_channel_env(monkeypatch):
    """The legacy path must be unaffected."""
    for key in (HOST_ENV, PORT_ENV, SECRET_ENV):
        monkeypatch.delenv(key, raising=False)
    client = RankClient(rank=3)
    assert client.enabled is False
    assert client.connect() is False
    assert client.observe("ray", ComponentState.RUNNING.value) is False
    client.close()


def test_an_unreachable_channel_fails_closed(monkeypatch):
    monkeypatch.setenv(HOST_ENV, "127.0.0.1")
    monkeypatch.setenv(PORT_ENV, "1")  # nothing listens here
    monkeypatch.setenv(SECRET_ENV, "00" * 32)
    monkeypatch.setenv(DEPLOYMENT_ENV, "d1")
    monkeypatch.setenv(GENERATION_ENV, "7")
    monkeypatch.setenv(PLAN_HASH_ENV, "h")
    client = RankClient(rank=2)
    assert client.enabled is True
    assert client.connect(timeout=2) is False
    assert client.observe("ray", ComponentState.RUNNING.value) is False
    client.close()


def test_the_secret_is_per_deployment():
    a = HeadChannel(
        deployment_id="d1", generation=1, plan_hash="h", expected_ranks=1, host="127.0.0.1"
    )
    b = HeadChannel(
        deployment_id="d1", generation=2, plan_hash="h", expected_ranks=1, host="127.0.0.1"
    )
    try:
        assert a.secret != b.secret and len(a.secret) >= 32
    finally:
        a.stop()
        b.stop()


def test_startup_and_registration_deadlines_reject_unbounded_values(head):
    for value in (True, 0, -1, float("nan"), float("inf")):
        with pytest.raises(ValueError, match="finite and positive"):
            head.wait_all_registered(value)
        with pytest.raises(ValueError, match="finite and positive"):
            head.start_head(value)
        with pytest.raises(ValueError, match="finite and positive"):
            head.start_workers(value)
        with pytest.raises(ValueError, match="finite and positive"):
            head.broadcast_start(value)


def test_nested_protocol_timeout_has_a_separate_bounded_thread_handoff_budget():
    assert 2.0 < _completion_timeout(2.0) <= 2.5
    assert 3600.0 < _completion_timeout(3600.0) <= 3600.5


def test_owner_cancellation_interrupts_a_blocked_start_barrier(head):
    import threading
    import time

    cancellation = threading.Event()
    timer = threading.Timer(0.2, cancellation.set)
    timer.start()
    started = time.monotonic()
    try:
        assert not head.start_head(timeout=30, cancel_requested=cancellation.is_set)
    finally:
        timer.join()
    assert time.monotonic() - started < 2
    assert "cancelled by its owner" in (head.rank_failure() or "")


def test_start_polling_preserves_the_cross_thread_failure_cause():
    class Channel:
        async def receive_command(self, _timeout):
            return None

    class FailingLoop:
        def call(self, coroutine, timeout):
            coroutine.close()
            raise TimeoutError(f"outer handoff expired after {timeout}")

    client = RankClient(rank=1)
    client.connected = True
    client.established = True
    client._channel = Channel()
    client._loop = FailingLoop()

    assert not client.poll_start(timeout=2.0, expected_operation="START_WORKER")
    assert not client.connected
    assert client.control_failure().startswith(
        "START_WORKER receive failed: TimeoutError: outer handoff expired after "
    )


def test_pre_start_heartbeat_uses_the_control_lease_not_poll_remainder():
    import asyncio
    import time

    class Channel:
        control_limits = {"lease_timeout_s": 3.0}

        async def receive_command(self, _timeout):
            return None

        async def heartbeat_round_trip(self, timeout):
            # The old implementation passed only 25% of the 0.2s command-poll
            # window here.  A normal 0.1s acknowledgment consequently killed
            # the rank even though its three-second lease was healthy.
            assert timeout > 2.0
            await asyncio.sleep(0.1)
            return True

    class Loop:
        def call(self, coroutine, timeout):
            return asyncio.run(asyncio.wait_for(coroutine, timeout))

    client = RankClient(rank=1)
    client.connected = True
    client.established = True
    client._channel = Channel()
    client._loop = Loop()
    client._last_control_ack = time.monotonic()

    assert not client.poll_start(timeout=0.2, expected_operation="START_WORKER")
    assert client.connected
    assert client.established
    assert client.control_failure() is None


def test_observation_freshness_rejects_nonfinite_values(head):
    for value in (True, 0, -1, float("nan"), float("inf")):
        with pytest.raises(ValueError, match="finite and positive"):
            head.current_observations(max_age_s=value)


def test_canonical_rank_entry_publishes_lifecycle_and_driver_is_only_alias():
    """Lifecycle authority lives in rank_main, never in the old driver root."""
    from importlib import resources

    driver = (resources.files("exaserve") / "driver.py").read_text()
    rank_main = (resources.files("exaserve") / "rank_main.py").read_text()
    assert "RankClient" not in driver
    assert "launcher_main(argv)" in driver
    assert "RankClient" in rank_main
    assert "channel.observe(" in rank_main
    assert "node.supervise(" in rank_main


def test_start_is_actually_delivered_and_acknowledged(head, monkeypatch):
    """The gate used to be a head-side flag no rank could observe."""
    import time

    _rank_env(head, monkeypatch)
    clients = [RankClient(rank=rank, node_id=f"node{rank}") for rank in (0, 1)]
    for client in clients:
        assert client.connect(timeout=10)
        _establish(client)
    try:
        import threading

        threads = [
            threading.Thread(target=lambda c=client: c.poll_start(timeout=10)) for client in clients
        ]
        for thread in threads:
            thread.start()
        assert head.broadcast_start(timeout=10) == 2, "START reached no session"
        for thread in threads:
            thread.join(timeout=10)
        assert all(client.start_received() for client in clients)
        # The head must see the acknowledgement.
        for _ in range(50):
            if head._listener.command_result("START:0"):
                break
            time.sleep(0.1)
        result = head._listener.command_result("START:0")
        assert result and result["ok"], "no COMMAND_RESULT for START"
    finally:
        for client in clients:
            client.close()


def test_production_start_releases_head_then_workers(head, monkeypatch):
    """A worker must remain fenced until the typed head phase is complete."""
    import threading

    _rank_env(head, monkeypatch)
    clients = [RankClient(rank=rank, node_id=f"node{rank}") for rank in (0, 1)]
    for client in clients:
        assert client.connect(timeout=10)
        _establish(client)
    head_wait = threading.Thread(
        target=lambda: clients[0].poll_start(timeout=10, expected_operation="START_HEAD")
    )
    worker_wait = threading.Thread(
        target=lambda: clients[1].poll_start(timeout=10, expected_operation="START_WORKER")
    )
    try:
        head_wait.start()
        worker_wait.start()
        assert head.start_head(timeout=10)
        head_wait.join(timeout=10)
        assert clients[0].start_received()
        assert not clients[1].start_received()
        assert worker_wait.is_alive()
        assert head.start_workers(timeout=10) == 1
        worker_wait.join(timeout=10)
        assert clients[1].start_received()
        assert head.start_broadcast()
        assert head._listener.command_result("START_HEAD:0")["ok"]
        assert head._listener.command_result("START_WORKER:1")["ok"]
    finally:
        for client in clients:
            client.close()
        head_wait.join(timeout=2)
        worker_wait.join(timeout=2)


def test_the_start_gate_requires_snapshot_establishment(head, monkeypatch):
    _rank_env(head, monkeypatch)
    client = RankClient(rank=1)
    try:
        assert client.connect(timeout=10)
        assert client.start_gate_available() is False
        _establish(client)
        assert client.start_gate_available() is True
    finally:
        client.close()


def test_an_unconnected_rank_never_passes_the_mandatory_gate(monkeypatch):
    for key in (HOST_ENV, PORT_ENV, SECRET_ENV):
        monkeypatch.delenv(key, raising=False)
    client = RankClient(rank=0)
    assert client.start_gate_available() is False
    assert client.start_received() is False


def test_a_rank_receipt_travels_over_the_authenticated_channel(head, monkeypatch):
    """A detached Ray actor is not an authoritative readiness source."""
    import time

    _rank_env(head, monkeypatch)
    client = RankClient(rank=0)
    assert client.connect(timeout=10)
    _establish(client)
    try:
        receipt = {
            "schema_version": 2,
            "owner_scope": "RANK",
            "owner_rank": 0,
            "receipt_requirement_id": "rank0/ray_head",
            "role": "ray_head",
        }
        assert client.submit_receipt(receipt)
        for _ in range(50):
            if any(
                payload.get("receipt_requirement_id") == "rank0/ray_head"
                for _, payload in head.receipt_payloads
            ):
                break
            time.sleep(0.1)
        matching = [
            (rank, payload)
            for rank, payload in head.receipt_payloads
            if payload.get("receipt_requirement_id") == "rank0/ray_head"
        ]
        assert matching, "no receipt reached the head"
        rank, payload = matching[-1]
        assert rank == 0 and payload["receipt_requirement_id"] == "rank0/ray_head"
    finally:
        client.close()


def test_a_rank_cannot_submit_a_global_receipt_over_the_channel(head, monkeypatch):
    """GLOBAL receipts enter only from the in-process supervisor authority."""
    import time

    _rank_env(head, monkeypatch)
    client = RankClient(rank=1)
    assert client.connect(timeout=10)
    _establish(client)
    try:
        client.submit_receipt(
            {
                "schema_version": 2,
                "owner_scope": "GLOBAL",
                "owner_rank": None,
                "receipt_requirement_id": "global/supervisor",
                "role": "supervisor",
            }
        )
        time.sleep(0.5)
        assert not any(p.get("owner_scope") == "GLOBAL" for _, p in head.receipt_payloads), (
            "a rank session smuggled a GLOBAL receipt"
        )
        assert any("only RANK receipts" in a.detail for a in head._listener.audit), (
            "the attempt was not audited"
        )
    finally:
        client.close()


def test_a_chunked_snapshot_is_applied_only_when_complete(head, monkeypatch):
    """A partial or mixed snapshot must never become the rank's projection."""
    import time

    import socket

    from exaserve.control.contracts import SCHEMA_VERSION, ComponentObservation, OwnerScope

    _rank_env(head, monkeypatch)
    client = RankClient(rank=0)
    assert client.connect(timeout=10)
    _establish(client)
    try:
        # node_id must match the authenticated session: the listener refuses a
        # rank asserting another node, which is the identity binding working.
        node_id = socket.gethostname()
        observations = [
            ComponentObservation(
                schema_version=SCHEMA_VERSION,
                deployment_id="d1",
                plan_hash="h",
                generation=7,
                component_id=f"c{i}",
                instance_id="i",
                sequence=i + 1,
                owner_scope=OwnerScope.RANK.value,
                role="ray",
                node_id=node_id,
                state="RUNNING",
                observed_at=time.time(),
                owner_rank=0,
            )
            for i in range(5)
        ]
        # Force multiple chunks so assembly is genuinely exercised.
        snapshot_id = client._loop.call(
            client._channel.send_snapshot(
                observations, receipts=[_supervisor_receipt(client)], chunk_items=2
            ),
            timeout=15,
        )
        assert client._loop.call(
            client._channel.await_snapshot_accepted(snapshot_id, 10), timeout=15
        )
        for _ in range(60):
            if len(head.observations) >= 5:
                break
            time.sleep(0.1)
        assert len(head.observations) >= 5, (
            f"chunked snapshot did not assemble ({len(head.observations)} items)"
        )
    finally:
        client.close()


def test_a_snapshot_with_a_wrong_hash_is_refused(head, monkeypatch):
    import time

    _rank_env(head, monkeypatch)
    client = RankClient(rank=1)
    assert client.connect(timeout=10)
    _establish(client)
    try:
        # Hand-craft a frame whose declared hash cannot match its items.
        from exaserve.control.contracts import EnvelopeKind

        client._loop.call(client._channel._writer.drain(), timeout=5)
        from exaserve.control.transport import write_frame

        valid = client.make_observation("tampered", ComponentState.RUNNING.value, role="ray")
        env = client._channel._env(
            EnvelopeKind.SNAPSHOT.value,
            {
                "payload_version": 1,
                "snapshot_id": "bogus",
                "chunk_index": 0,
                "chunk_count": 1,
                "complete_set_hash": "0" * 64,
                "observations": [valid.to_dict()],
                "compatibility_receipts": [_supervisor_receipt(client)],
            },
        )
        client._loop.call(
            write_frame(client._channel._writer, client._channel._secret, env), timeout=10
        )
        time.sleep(0.6)
        assert any("hash mismatch" in a.detail for a in head._listener.audit), (
            "a snapshot whose content does not match its declared hash was accepted"
        )
    finally:
        client.close()


def test_a_snapshot_observation_claiming_another_node_is_refused(head, monkeypatch):
    """A rank must not assert a node it is not bound to, even in a snapshot."""
    import time

    from exaserve.control.contracts import SCHEMA_VERSION, ComponentObservation, OwnerScope

    _rank_env(head, monkeypatch)
    client = RankClient(rank=0)
    assert client.connect(timeout=10)
    _establish(client)
    try:
        forged = [
            ComponentObservation(
                schema_version=SCHEMA_VERSION,
                deployment_id="d1",
                plan_hash="h",
                generation=7,
                component_id="c0",
                instance_id="i",
                sequence=1,
                owner_scope=OwnerScope.RANK.value,
                role="ray",
                node_id="somebody-elses-node",
                state="RUNNING",
                observed_at=time.time(),
                owner_rank=0,
            )
        ]
        client._loop.call(client._channel.send_snapshot(forged), timeout=15)
        time.sleep(0.6)
        assert not head.observations, "a forged node_id was accepted"
        assert any("node_id mismatch" in a.detail for a in head._listener.audit)
    finally:
        client.close()
