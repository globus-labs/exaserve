"""S01 spike / AC-CTL-01 (local slice): authenticated control channel.

Hermetic, login-node-safe. Two-node compute proof runs in WP0-Early Compute.
"""

from __future__ import annotations

import asyncio
import struct

import pytest

from exaserve.control.contracts import (
    SCHEMA_VERSION,
    ComponentObservation,
    ContractError,
    Envelope,
    RejectReason,
    decode_envelope,
    encode_envelope,
    validate_observation,
)
from exaserve.control.transport import (
    ControlListener,
    NodeChannel,
    _validate_command_payload,
    _validate_command_result_payload,
    new_deployment_secret,
)

DEP = dict(deployment_id="dep-1", plan_hash="ph-1", generation=3)


@pytest.mark.parametrize(
    "overrides",
    [
        {"expected_ranks": True},
        {"generation": True},
        {"secret": b"short"},
        {"host": ""},
        {"expected_nodes": {True: "node0"}},
    ],
)
def test_listener_identity_and_secret_are_never_coerced(overrides):
    parameters = {
        **DEP,
        "expected_ranks": 1,
        "secret": new_deployment_secret(),
        "on_observation": lambda *_: None,
    }
    parameters.update(overrides)
    with pytest.raises(ValueError):
        ControlListener(**parameters)


@pytest.mark.parametrize(
    "overrides",
    [
        {"port": True},
        {"rank": "0"},
        {"generation": True},
        {"secret": b"short"},
        {"node_id": ""},
    ],
)
def test_rank_channel_identity_and_secret_are_never_coerced(overrides):
    parameters = {
        **DEP,
        "host": "127.0.0.1",
        "port": 1,
        "secret": new_deployment_secret(),
        "rank": 0,
        "node_id": "node0",
    }
    parameters.update(overrides)
    with pytest.raises(ValueError):
        NodeChannel(**parameters)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("payload_version", True),
        ("command_id", 7),
        ("operation", 7),
        ("ok", 1),
        ("detail", None),
        ("status", "DONE"),
    ],
)
def test_command_results_reject_coercible_field_types(field, value):
    payload = {
        "payload_version": 1,
        "command_id": "START:0",
        "operation": "START",
        "ok": True,
        "detail": "",
        "status": "SUCCEEDED",
    }
    payload[field] = value
    with pytest.raises(ContractError):
        _validate_command_result_payload(payload)


def _command(command_id: str, operation: str = "START") -> dict:
    return _validate_command_payload(
        {"payload_version": 1, "command_id": command_id, "operation": operation}
    )


def test_listener_command_identity_cache_is_bounded_without_eviction(monkeypatch):
    from exaserve.control import transport

    monkeypatch.setattr(transport, "_MAX_COMMAND_RECORDS", 2)
    listener = ControlListener(
        **DEP,
        expected_ranks=1,
        secret=new_deployment_secret(),
        on_observation=lambda *_: None,
    )
    first = (0, _command("a"))
    listener._remember_issued_command("a", first)
    listener._remember_issued_command("b", (0, _command("b")))
    listener._remember_issued_command("a", first)  # exact retry remains idempotent
    with pytest.raises(ContractError, match="capacity exhausted"):
        listener._remember_issued_command("c", (0, _command("c")))
    assert set(listener._issued_commands) == {"a", "b"}


def test_rank_command_and_pending_caches_are_bounded_without_eviction(monkeypatch):
    from exaserve.control import transport

    monkeypatch.setattr(transport, "_MAX_COMMAND_RECORDS", 2)
    monkeypatch.setattr(transport, "_MAX_PENDING_HEAD_COMMANDS", 2)
    channel = NodeChannel(
        host="127.0.0.1",
        port=1,
        secret=new_deployment_secret(),
        rank=0,
        node_id="node0",
        **DEP,
    )
    first = _command("a")
    channel._remember_received_command(first)
    channel._remember_received_command(_command("b"))
    channel._remember_received_command(first)
    with pytest.raises(ContractError, match="capacity exhausted"):
        channel._remember_received_command(_command("c"))
    channel._queue_head_command(_command("pending-a"))
    channel._queue_head_command(_command("pending-b"))
    with pytest.raises(ContractError, match="queue capacity exhausted"):
        channel._queue_head_command(_command("pending-c"))


def test_rank_allows_only_one_bounded_snapshot_in_flight():
    async def scenario():
        channel = NodeChannel(
            host="127.0.0.1",
            port=1,
            secret=new_deployment_secret(),
            rank=0,
            node_id="node0",
            **DEP,
        )

        async def discard(_kind, _payload):
            return None

        channel._write = discard
        assert await channel.send_snapshot([], snapshot_id="one") == "one"
        with pytest.raises(ContractError, match="one local snapshot"):
            await channel.send_snapshot([], snapshot_id="two")
        assert channel._snapshot_hashes == {"one": channel._snapshot_hashes["one"]}

    _run(scenario())


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("payload_version", True),
        ("snapshot_id", 123),
        ("complete_set_hash", 0),
        ("chunk_count", True),
        ("chunk_index", "0"),
    ],
)
def test_snapshot_chunks_reject_coercible_field_types(field, value):
    listener = ControlListener(
        **DEP,
        expected_ranks=1,
        secret=new_deployment_secret(),
        on_observation=lambda *_: None,
    )
    payload = {
        "payload_version": 1,
        "snapshot_id": "snapshot-1",
        "chunk_index": 0,
        "chunk_count": 1,
        "complete_set_hash": "0" * 64,
        "observations": [],
        "compatibility_receipts": [],
    }
    payload[field] = value
    accepted, reason, _ = listener._assemble_snapshot(0, payload)
    assert not accepted and reason


@pytest.mark.parametrize("field", ["schema_version", "generation", "sequence", "owner_rank"])
def test_observation_contract_never_accepts_boolean_counters(field):
    payload = _obs(0, 1).to_dict()
    payload[field] = True
    with pytest.raises(ContractError, match="MALFORMED|UNKNOWN_SCHEMA"):
        validate_observation(payload)


@pytest.mark.parametrize(
    "payload",
    [
        b'{"v":true,"kind":"HEARTBEAT","deployment_id":"d","plan_hash":"p",'
        b'"generation":1,"sender_rank":0,"sender_node":"n","seq":1,"payload":{}}',
        b'{"v":1,"kind":"HEARTBEAT","deployment_id":"d","plan_hash":"p",'
        b'"generation":NaN,"sender_rank":0,"sender_node":"n","seq":1,"payload":{}}',
        b'{"v":1,"v":1,"kind":"HEARTBEAT","deployment_id":"d","plan_hash":"p",'
        b'"generation":1,"sender_rank":0,"sender_node":"n","seq":1,"payload":{}}',
    ],
)
def test_envelope_contract_rejects_boolean_or_nonfinite_wire_numbers(payload):
    with pytest.raises(ContractError):
        decode_envelope(payload)


def test_envelope_encoder_rejects_non_string_payload_keys():
    envelope = Envelope(
        v=1,
        kind="HEARTBEAT",
        deployment_id="d",
        plan_hash="p",
        generation=1,
        sender_rank=0,
        sender_node="n",
        seq=1,
        payload={1: "coerced"},
    )
    with pytest.raises(ContractError, match="non-string key"):
        encode_envelope(envelope)


def _obs(
    rank: int,
    seq: int,
    state: str = "RUNNING",
    component: str = "ray-worker",
    generation: int = DEP["generation"],
    instance: str = "i0",
) -> ComponentObservation:
    return ComponentObservation(
        schema_version=SCHEMA_VERSION,
        deployment_id=DEP["deployment_id"],
        plan_hash=DEP["plan_hash"],
        generation=generation,
        component_id=f"{component}-{rank}",
        instance_id=instance,
        sequence=seq,
        owner_scope="RANK",
        owner_rank=rank,
        role=component,
        node_id=f"node{rank}",
        state=state,
        observed_at=0.0,
    )


def _run(coro):
    return asyncio.run(asyncio.wait_for(coro, 30))


async def _listener(received, changes, expected_ranks=2, secret=None):
    listener = ControlListener(
        **DEP,
        expected_ranks=expected_ranks,
        secret=secret or new_deployment_secret(),
        on_observation=lambda rank, obs: received.append((rank, obs)),
        on_session_change=lambda rank, up: changes.append((rank, up)),
        host="127.0.0.1",
    )
    await listener.start()
    return listener


def test_register_snapshot_ack_is_the_transport_establishment_barrier():
    async def scenario():
        received, changes, accepted = [], [], []
        secret = new_deployment_secret()

        def on_register(rank, node, instance):
            assert (rank, node, instance) == (0, "node0", "node0:supervisor")
            return True, "authenticated"

        def on_snapshot(rank, snapshot_id, complete_hash, items):
            assert rank == 0 and snapshot_id and complete_hash
            assert any(item["kind"] == "receipt" for item in items)
            accepted.append((snapshot_id, complete_hash))
            return True, "validated"

        def on_snapshot_ack(rank, payload):
            assert rank == 0
            assert payload["status"] == "SUCCEEDED"
            return True, "established"

        listener = ControlListener(
            **DEP,
            expected_ranks=1,
            secret=secret,
            on_observation=lambda rank, obs: received.append((rank, obs)),
            on_session_change=lambda rank, up: changes.append((rank, up)),
            on_register=on_register,
            on_snapshot=on_snapshot,
            on_snapshot_ack=on_snapshot_ack,
            host="127.0.0.1",
        )
        await listener.start()
        ch = _channel(listener, secret, 0)
        await ch.connect_and_register(instance_id="node0:supervisor")
        assert not listener.is_established(0), "REGISTER crossed the barrier"
        snapshot_id = await ch.send_snapshot(
            [_obs(0, seq=1)],
            receipts=[
                {
                    "receipt_requirement_id": "rank0/node_supervisor",
                    "component_id": "node_supervisor",
                    "instance_id": "node0:supervisor",
                    "owner_scope": "RANK",
                    "owner_rank": 0,
                }
            ],
        )
        assert not listener.is_established(0), "snapshot crossed before ACK result"
        assert await ch.await_snapshot_accepted(snapshot_id, timeout=5)
        assert await listener.wait_all_established(5)
        assert listener.is_established(0)
        assert accepted
        await ch.close()
        await listener.stop()

    _run(scenario())


def test_snapshot_payload_version_is_required_and_violation_is_reported():
    async def scenario():
        received, changes, violations = [], [], []
        secret = new_deployment_secret()
        listener = ControlListener(
            **DEP,
            expected_ranks=1,
            secret=secret,
            on_observation=lambda rank, obs: received.append((rank, obs)),
            on_session_change=lambda rank, up: changes.append((rank, up)),
            on_register=lambda *_: (True, "ok"),
            on_protocol_violation=lambda rank, reason: violations.append((rank, reason)),
            host="127.0.0.1",
        )
        await listener.start()
        ch = _channel(listener, secret, 0)
        await ch.connect_and_register(instance_id="i0")
        from exaserve.control.contracts import EnvelopeKind
        from exaserve.control.transport import write_frame

        env = ch._env(
            EnvelopeKind.SNAPSHOT.value,
            {
                "snapshot_id": "missing-version",
                "chunk_index": 0,
                "chunk_count": 1,
                "complete_set_hash": "0" * 64,
                "items": [],
            },
        )
        await write_frame(ch._writer, secret, env)
        await asyncio.sleep(0.1)
        assert violations and "payload_version" in violations[0][1]
        assert not listener.is_established(0)
        await listener.stop()

    _run(scenario())


def _channel(listener, secret, rank, node=None, **overrides):
    params = dict(DEP)
    params.update(overrides)
    return NodeChannel(
        host="127.0.0.1",
        port=listener.port,
        secret=secret,
        rank=rank,
        node_id=node or f"node{rank}",
        **params,
    )


def _supervisor_receipt(rank: int) -> dict:
    return {
        "receipt_requirement_id": f"rank{rank}/node_supervisor",
        "component_id": "node_supervisor",
        "instance_id": f"node{rank}:supervisor",
        "owner_scope": "RANK",
        "owner_rank": rank,
        "node_id": f"node{rank}",
    }


async def _establish(ch: NodeChannel, observations=None) -> str:
    snapshot_id = await ch.send_snapshot(
        list(observations or []), receipts=[_supervisor_receipt(ch.rank)]
    )
    assert await ch.await_snapshot_accepted(snapshot_id, timeout=5)
    return snapshot_id


def test_register_observe_dedup_and_disconnect():
    async def scenario():
        received, changes = [], []
        secret = new_deployment_secret()
        listener = await _listener(received, changes, secret=secret)

        ch0 = _channel(listener, secret, 0)
        ch1 = _channel(listener, secret, 1)
        await ch0.connect_and_register()
        await ch1.connect_and_register()
        await _establish(ch0)
        await _establish(ch1)
        assert await listener.wait_all_registered(5)

        await ch0.send_observation(_obs(0, seq=1))
        await ch0.send_observation(_obs(0, seq=1))  # at-least-once duplicate
        await ch1.send_observation(_obs(1, seq=1, state="READY"))
        await ch0.send_heartbeat()
        await asyncio.sleep(0.1)
        assert [(r, o.state) for r, o in sorted(received)] == [(0, "RUNNING"), (1, "READY")]

        await ch0.close()  # GOODBYE -> session change
        await asyncio.sleep(0.1)
        assert (0, False) in changes and (0, True) in changes and (1, True) in changes
        await ch1.close()
        await listener.stop()

    _run(scenario())


def test_unexpected_handler_callback_failure_is_observable():
    async def scenario():
        secret = new_deployment_secret()

        def fail_observation(_rank, _observation):
            raise RuntimeError("injected observation sink failure")

        listener = ControlListener(
            **DEP,
            expected_ranks=1,
            secret=secret,
            on_observation=fail_observation,
            on_register=lambda *_: (True, "ok"),
            on_snapshot=lambda *_: (True, "ok"),
            on_snapshot_ack=lambda *_: (True, "ok"),
            host="127.0.0.1",
        )
        await listener.start()
        channel = _channel(listener, secret, 0)
        await channel.connect_and_register()
        await _establish(channel)
        await channel.send_observation(_obs(0, seq=2))
        for _ in range(50):
            if listener.unexpected_failure():
                break
            await asyncio.sleep(0.01)
        assert "injected observation sink failure" in (listener.unexpected_failure() or "")
        await listener.stop()

    _run(scenario())


def test_rejections_fail_closed():
    async def scenario():
        received, changes = [], []
        secret = new_deployment_secret()
        listener = await _listener(received, changes, secret=secret)

        # Bad MAC: raw frame with wrong secret terminates the session.
        bad = _channel(listener, new_deployment_secret(), 0)
        with pytest.raises((Exception,)):
            await bad.connect_and_register(timeout=2)

        # Wrong generation fails closed before registration.
        stale = _channel(listener, secret, 0, generation=99)
        with pytest.raises((Exception,)):
            await stale.connect_and_register(timeout=2)

        # Out-of-range rank rejected.
        rogue = _channel(listener, secret, 7)
        with pytest.raises((Exception,)):
            await rogue.connect_and_register(timeout=2)

        # Legit registration, then rank impersonation in a later message.
        ch0 = _channel(listener, secret, 0)
        await ch0.connect_and_register()
        await _establish(ch0)

        # Duplicate rank while rank 0 session is live.
        dup = _channel(listener, secret, 0, node="other-node")
        with pytest.raises((Exception,)):
            await dup.connect_and_register(timeout=2)

        ch0.rank = 1  # lie about rank after registering as 0
        await ch0.send_observation(_obs(1, seq=1))
        await asyncio.sleep(0.1)
        assert received == []

        reasons = {a.reason for a in listener.audit}
        assert RejectReason.BAD_MAC.value in reasons
        assert RejectReason.STALE_GENERATION.value in reasons
        assert RejectReason.RANK_MISMATCH.value in reasons
        await listener.stop()

    _run(scenario())


def test_sequence_regression_and_stale_observation_rejected():
    async def scenario():
        received, changes = [], []
        secret = new_deployment_secret()
        listener = await _listener(received, changes, expected_ranks=1, secret=secret)
        ch = _channel(listener, secret, 0)
        await ch.connect_and_register()
        await _establish(ch)

        await ch.send_observation(_obs(0, seq=1))
        # IMP-B06: a stale-generation observation is now FAIL-CLOSED — the
        # offending session is terminated rather than the message skipped.
        await ch.send_observation(_obs(0, seq=2, generation=1))
        await asyncio.sleep(0.15)

        states = [o.generation for _, o in received]
        assert states == [3]  # only the valid observation was accepted
        assert any(a.reason == RejectReason.STALE_GENERATION.value for a in listener.audit)
        await listener.stop()

    _run(scenario())


def test_envelope_sequence_regression_terminates_session():
    async def scenario():
        received, changes = [], []
        secret = new_deployment_secret()
        listener = await _listener(received, changes, expected_ranks=1, secret=secret)
        ch = _channel(listener, secret, 0)
        await ch.connect_and_register()
        await _establish(ch)
        await ch.send_observation(_obs(0, seq=1))
        await asyncio.sleep(0.1)
        ch._seq = 0  # force envelope-level sequence regression (replay)
        await ch.send_observation(_obs(0, seq=2))
        await asyncio.sleep(0.15)
        assert any(a.reason == RejectReason.SEQUENCE_REGRESSION.value for a in listener.audit)
        assert len(received) == 1  # the replayed frame was not delivered
        await listener.stop()

    _run(scenario())


def test_rank_cannot_forge_global_or_other_rank_observations():
    """IMP-B06: observation identity must bind to the authenticated session."""

    async def scenario():
        received, changes = [], []
        secret = new_deployment_secret()
        listener = await _listener(received, changes, expected_ranks=2, secret=secret)
        ch = _channel(listener, secret, 0)
        await ch.connect_and_register()
        await _establish(ch)
        forged = ComponentObservation(
            schema_version=SCHEMA_VERSION,
            deployment_id=DEP["deployment_id"],
            plan_hash=DEP["plan_hash"],
            generation=DEP["generation"],
            component_id="gateway",
            instance_id="i0",
            sequence=1,
            owner_scope="GLOBAL",
            owner_rank=None,
            role="gateway",
            node_id="head",
            state="READY",
            observed_at=0.0,
        )
        await ch.send_observation(forged)
        await asyncio.sleep(0.15)
        assert received == []  # GLOBAL forgery rejected
        assert any(a.reason == RejectReason.RANK_MISMATCH.value for a in listener.audit)
        await listener.stop()

    _run(scenario())


def test_incremental_observation_rejects_a_stale_component_instance():
    async def scenario():
        received, changes = [], []
        secret = new_deployment_secret()
        listener = await _listener(received, changes, expected_ranks=1, secret=secret)
        channel = _channel(listener, secret, 0)
        await channel.connect_and_register()
        await _establish(channel)
        await channel.send_observation(_obs(0, seq=1, instance="current"))
        await channel.send_observation(_obs(0, seq=2, instance="retired"))
        await asyncio.sleep(0.15)
        assert len(received) == 1
        assert any(record.reason == RejectReason.STALE_INSTANCE.value for record in listener.audit)
        await listener.stop()

    _run(scenario())


def test_conflicting_same_sequence_observation_is_not_an_idempotent_duplicate():
    async def scenario():
        received, changes = [], []
        secret = new_deployment_secret()
        listener = await _listener(received, changes, expected_ranks=1, secret=secret)
        channel = _channel(listener, secret, 0)
        await channel.connect_and_register()
        await _establish(channel)
        await channel.send_observation(_obs(0, seq=1, state="RUNNING"))
        await channel.send_observation(_obs(0, seq=1, state="READY"))
        await asyncio.sleep(0.15)
        assert len(received) == 1
        assert any(
            "conflicting duplicate observation" in record.detail for record in listener.audit
        )
        await listener.stop()

    _run(scenario())


def test_snapshot_rejects_two_instances_for_one_component():
    listener = ControlListener(
        **DEP,
        expected_ranks=1,
        secret=new_deployment_secret(),
        on_observation=lambda *_: None,
    )
    session = listener._register(
        Envelope(
            v=SCHEMA_VERSION,
            kind="REGISTER",
            deployment_id=DEP["deployment_id"],
            plan_hash=DEP["plan_hash"],
            generation=DEP["generation"],
            sender_rank=0,
            sender_node="node0",
            seq=1,
            payload={"payload_version": 1, "instance_id": "supervisor"},
        )
    )[0]
    assert session is not None
    items = [
        {"kind": "observation", "body": _obs(0, 1, instance="one").to_dict()},
        {"kind": "observation", "body": _obs(0, 2, instance="two").to_dict()},
    ]
    with pytest.raises(ContractError, match="repeats component_id"):
        listener._validate_snapshot_items(session, items)


def test_all_registered_clears_when_a_rank_disconnects():
    """IMP-B06: readiness must not keep believing the rank set is complete."""

    async def scenario():
        received, changes = [], []
        secret = new_deployment_secret()
        listener = await _listener(received, changes, expected_ranks=1, secret=secret)
        ch = _channel(listener, secret, 0)
        await ch.connect_and_register()
        await _establish(ch)
        assert await listener.wait_all_registered(2)
        await ch.close()
        await asyncio.sleep(0.15)
        assert not await listener.wait_all_registered(0.2)
        await listener.stop()

    _run(scenario())


def test_oversized_frame_rejected():
    async def scenario():
        received, changes = [], []
        secret = new_deployment_secret()
        listener = await _listener(received, changes, expected_ranks=1, secret=secret)
        reader, writer = await asyncio.open_connection("127.0.0.1", listener.port)
        writer.write(struct.pack(">I", (1 << 20) + 33))  # oversized declared length
        writer.write(b"x" * 64)
        await writer.drain()
        await asyncio.sleep(0.1)
        assert any(a.reason == RejectReason.OVERSIZED.value for a in listener.audit)
        writer.close()
        await listener.stop()

    _run(scenario())


def test_stop_closes_unauthenticated_connections_and_reaps_handlers():
    """Listener teardown must not leak a pre-REGISTER connection task."""

    async def scenario():
        received, changes = [], []
        listener = await _listener(received, changes, expected_ranks=1)
        reader, writer = await asyncio.open_connection("127.0.0.1", listener.port)
        await asyncio.sleep(0)  # allow the accepted-connection handler to start

        await listener.stop()

        assert not listener._handler_tasks
        assert not listener._connection_writers
        assert await asyncio.wait_for(reader.read(), 1) == b""
        writer.close()
        await writer.wait_closed()

    _run(scenario())


def test_snapshot_batches_observations():
    async def scenario():
        received, changes = [], []
        secret = new_deployment_secret()
        listener = await _listener(received, changes, expected_ranks=1, secret=secret)
        ch = _channel(listener, secret, 0)
        await ch.connect_and_register()
        await _establish(ch, [_obs(0, seq=1), _obs(0, seq=2, state="READY", component="engine")])
        await asyncio.sleep(0.1)
        assert {o.component_id for _, o in received} == {"ray-worker-0", "engine-0"}
        await ch.close()
        await listener.stop()

    _run(scenario())


def test_a_failed_preconnection_command_is_never_delivered_after_registration():
    """A stale START must not escape after its caller already saw failure."""

    async def scenario():
        received, changes = [], []
        secret = new_deployment_secret()
        listener = await _listener(received, changes, expected_ranks=1, secret=secret)

        assert not await listener._send_command(0, "START:0", "START")

        channel = _channel(listener, secret, 0)
        await channel.connect_and_register()
        await _establish(channel)
        assert await listener.wait_all_established(2)
        assert await channel.receive_command(0.1) is None
        await channel.close()
        await listener.stop()

    _run(scenario())


def test_command_result_is_bound_to_the_rank_that_received_the_command():
    async def scenario():
        from exaserve.control.contracts import EnvelopeKind

        received, changes = [], []
        secret = new_deployment_secret()
        listener = await _listener(received, changes, expected_ranks=2, secret=secret)
        channels = [_channel(listener, secret, rank) for rank in range(2)]
        for channel in channels:
            await channel.connect_and_register()
            await _establish(channel)
        assert await listener.send_command(0, "START:0", "START")

        # Rank one knows the deployment secret but was not the recipient. Use
        # the raw authenticated write surface to model a compromised rank.
        await channels[1]._write(
            EnvelopeKind.COMMAND_RESULT.value,
            {
                "payload_version": 1,
                "command_id": "START:0",
                "operation": "START",
                "ok": True,
                "detail": "",
                "status": "SUCCEEDED",
            },
        )
        await asyncio.sleep(0.1)

        assert listener.command_result("START:0") is None
        assert any(a.reason == RejectReason.RANK_MISMATCH.value for a in listener.audit)
        await channels[0].close()
        await listener.stop()

    _run(scenario())


def test_an_exact_duplicate_command_replays_the_same_cached_result():
    async def scenario():
        received, changes = [], []
        secret = new_deployment_secret()
        listener = await _listener(received, changes, expected_ranks=1, secret=secret)
        channel = _channel(listener, secret, 0)
        await channel.connect_and_register()
        await _establish(channel)
        assert await listener.wait_all_established(2)

        for attempt in range(2):
            assert await listener.send_command(0, "START:0", "START")
            command = await channel.receive_command(2)
            assert command is not None
            if attempt == 0:
                await channel.send_command_result(
                    "START:0", True, operation="START", status="SUCCEEDED"
                )
            else:
                assert await channel.replay_command_result(command)
            assert (await listener.wait_command_result("START:0", 2))["ok"] is True

        assert not listener.audit
        await channel.close()
        await listener.stop()

    _run(scenario())


def test_shutdown_ack_atomically_authorizes_immediate_goodbye():
    """Fast ranks may close before a sequential head waiter reaches them."""

    async def scenario():
        received, changes = [], []
        secret = new_deployment_secret()
        listener = await _listener(received, changes, expected_ranks=2, secret=secret)
        channels = [_channel(listener, secret, rank) for rank in range(2)]
        for channel in channels:
            await channel.connect_and_register()
            await _establish(channel)
        assert await listener.wait_all_established(2)

        assert await listener.broadcast_command("DRAIN", "DRAIN") == 2

        async def acknowledge_and_close(channel):
            command = await channel.receive_command(2)
            assert command is not None
            await channel.send_command_result(
                command["command_id"],
                True,
                operation="DRAIN",
                status="SUCCEEDED",
            )
            await channel.close(expected=True)

        await asyncio.gather(*(acknowledge_and_close(channel) for channel in channels))
        results = await listener.wait_command_results(("DRAIN:0", "DRAIN:1"), 2)
        for rank in range(2):
            result = results[f"DRAIN:{rank}"]
            assert result is not None and result["ok"] is True
            # The head-side authorization happens after the rank has already
            # closed in this test; it must not erase the accepted GOODBYE.
            listener.expect_goodbye(rank)
            assert listener.received_goodbye(rank)
        assert not listener.audit
        await listener.stop()

    _run(scenario())


def test_a_command_id_cannot_be_reused_for_a_different_request():
    async def scenario():
        received, changes = [], []
        secret = new_deployment_secret()
        listener = await _listener(received, changes, expected_ranks=1, secret=secret)
        channel = _channel(listener, secret, 0)
        await channel.connect_and_register()
        await _establish(channel)
        assert await listener.wait_all_established(2)
        assert await listener.send_command(0, "command-1", "START")
        with pytest.raises(ContractError, match="already issued differently"):
            await listener.send_command(0, "command-1", "STOP")
        await channel.close()
        await listener.stop()

    _run(scenario())
