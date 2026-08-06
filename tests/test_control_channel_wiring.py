"""WP4.4: the head learns of rank failure without waiting for the launch."""

from __future__ import annotations


import pytest

from exaserve.control.channel_runtime import (
    HOST_ENV,
    PORT_ENV,
    SECRET_ENV,
    HeadChannel,
    RankClient,
)
from exaserve.control.contracts import ComponentState


@pytest.fixture
def head():
    channel = HeadChannel(deployment_id="d1", generation=7, plan_hash="h",
                          expected_ranks=2, host="127.0.0.1")
    try:
        yield channel
    finally:
        channel.stop()


def _rank_env(head: HeadChannel, monkeypatch) -> None:
    env = head.env()
    env[HOST_ENV] = "127.0.0.1"          # the fixture binds loopback
    for key, value in env.items():
        monkeypatch.setenv(key, value)


def test_a_rank_registers_and_its_observations_reach_the_head(head, monkeypatch):
    _rank_env(head, monkeypatch)
    client = RankClient(rank=0)
    assert client.connect(timeout=10), "rank could not register"
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
    try:
        client.observe("deployment", ComponentState.FAILED.value,
                       reason_code="UNEXPECTED_EXIT", detail="server exited 9")
        import time

        for _ in range(50):
            if head.rank_failure():
                break
            time.sleep(0.1)
        failure = head.rank_failure()
        assert failure and "rank 1" in failure and "server exited 9" in failure
    finally:
        client.close()


def test_losing_a_rank_lease_is_itself_a_failure(head, monkeypatch):
    """A rank we can no longer observe is not a quiet event."""
    _rank_env(head, monkeypatch)
    client = RankClient(rank=0)
    assert client.connect(timeout=10)
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


def test_an_unreachable_channel_degrades_instead_of_failing_the_rank(monkeypatch):
    monkeypatch.setenv(HOST_ENV, "127.0.0.1")
    monkeypatch.setenv(PORT_ENV, "1")          # nothing listens here
    monkeypatch.setenv(SECRET_ENV, "00" * 32)
    client = RankClient(rank=2)
    assert client.enabled is True
    assert client.connect(timeout=2) is False, "must not raise"
    assert client.observe("ray", ComponentState.RUNNING.value) is False
    client.close()


def test_the_head_hands_ranks_the_address_before_launching_them(tmp_path, monkeypatch):
    from exaserve import supervisor_main

    nodefile = tmp_path / "nodes"
    nodefile.write_text("hostA\nhostB\n")
    monkeypatch.setenv("EXASERVE_NODEFILE", str(nodefile))
    monkeypatch.delenv("EXASERVE_MPILAUNCH", raising=False)
    channel = HeadChannel(deployment_id="d1", generation=7, plan_hash="h",
                          expected_ranks=2, host="127.0.0.1")
    try:
        _, _, component = supervisor_main.build("/tmp/cfg.yaml", channel=channel)
        assert component.env[PORT_ENV] == str(channel.port)
        assert component.env[SECRET_ENV] == channel.secret.hex()
        # A zero exit is refused while a rank failure stands.
        channel.failures.append("rank 1: raylet died")
        ok, why = component.result_check()
        assert not ok and "raylet died" in why
    finally:
        channel.stop()


def test_the_secret_is_per_deployment():
    a = HeadChannel(deployment_id="d1", generation=1, plan_hash="h",
                    expected_ranks=1, host="127.0.0.1")
    b = HeadChannel(deployment_id="d1", generation=2, plan_hash="h",
                    expected_ranks=1, host="127.0.0.1")
    try:
        assert a.secret != b.secret and len(a.secret) >= 32
    finally:
        a.stop()
        b.stop()


def test_driver_publishes_rank_lifecycle():
    """The rank entry point must report, not just log."""
    from importlib import resources

    source = (resources.files("exaserve") / "driver.py").read_text()
    assert "RankClient" in source
    for marker in ('_channel.observe("ray_head"', '_channel.observe("ray_worker"',
                   '_channel.observe("deployment"', '_channel.observe("proxy"'):
        assert marker in source, f"driver does not report {marker}"
