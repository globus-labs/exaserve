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
    RejectReason,
)
from exaserve.control.transport import (
    ControlListener,
    NodeChannel,
    new_deployment_secret,
)

DEP = dict(deployment_id="dep-1", plan_hash="ph-1", generation=3)


def _obs(rank: int, seq: int, state: str = "RUNNING", component: str = "ray-worker",
         generation: int = DEP["generation"], instance: str = "i0") -> ComponentObservation:
    return ComponentObservation(
        schema_version=SCHEMA_VERSION, deployment_id=DEP["deployment_id"],
        plan_hash=DEP["plan_hash"], generation=generation,
        component_id=f"{component}-{rank}", instance_id=instance, sequence=seq,
        owner_scope="RANK", owner_rank=rank, role=component,
        node_id=f"node{rank}", state=state, observed_at=0.0)


def _run(coro):
    return asyncio.run(asyncio.wait_for(coro, 30))


async def _listener(received, changes, expected_ranks=2, secret=None):
    listener = ControlListener(
        **DEP, expected_ranks=expected_ranks, secret=secret or new_deployment_secret(),
        on_observation=lambda rank, obs: received.append((rank, obs)),
        on_session_change=lambda rank, up: changes.append((rank, up)),
        host="127.0.0.1")
    await listener.start()
    return listener


def _channel(listener, secret, rank, node=None, **overrides):
    params = dict(DEP)
    params.update(overrides)
    return NodeChannel(host="127.0.0.1", port=listener.port, secret=secret,
                       rank=rank, node_id=node or f"node{rank}", **params)


def test_register_observe_dedup_and_disconnect():
    async def scenario():
        received, changes = [], []
        secret = new_deployment_secret()
        listener = await _listener(received, changes, secret=secret)

        ch0 = _channel(listener, secret, 0)
        ch1 = _channel(listener, secret, 1)
        await ch0.connect_and_register()
        await ch1.connect_and_register()
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
        ch0.rank = 1  # lie about rank after registering as 0
        await ch0.send_observation(_obs(1, seq=1))
        await asyncio.sleep(0.1)
        assert received == []

        # Duplicate rank while rank 0 session is live.
        dup = _channel(listener, secret, 0, node="other-node")
        with pytest.raises((Exception,)):
            await dup.connect_and_register(timeout=2)

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

        await ch.send_observation(_obs(0, seq=1))
        # Stale-generation observation inside a valid envelope is rejected
        # by validate/scope checks at the observation layer.
        await ch.send_observation(_obs(0, seq=2, generation=1))
        await asyncio.sleep(0.1)
        # Envelope-level sequence regression terminates the session.
        ch._seq = 0  # force regression
        await ch.send_observation(_obs(0, seq=3))
        await asyncio.sleep(0.1)

        states = [o.generation for _, o in received]
        assert states == [3]  # stale-generation observation rejected + audited
        assert any(a.reason == RejectReason.STALE_GENERATION.value for a in listener.audit)
        assert any(a.reason == RejectReason.SEQUENCE_REGRESSION.value for a in listener.audit)
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


def test_snapshot_batches_observations():
    async def scenario():
        received, changes = [], []
        secret = new_deployment_secret()
        listener = await _listener(received, changes, expected_ranks=1, secret=secret)
        ch = _channel(listener, secret, 0)
        await ch.connect_and_register()
        await ch.send_snapshot([_obs(0, seq=1), _obs(0, seq=2, state="READY",
                                                     component="engine")])
        await asyncio.sleep(0.1)
        assert {o.component_id for _, o in received} == {"ray-worker-0", "engine-0"}
        await ch.close()
        await listener.stop()

    _run(scenario())
