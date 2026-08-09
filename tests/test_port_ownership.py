"""PR-012 / KI-A2 / TD-PORTS: a leased port is owned, not merely probed."""

from __future__ import annotations

import json
import os
import socket
import stat
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from exaserve.state.ports import (
    PortLease,
    PortReleaseError,
    PortUnavailable,
    bind_listener,
    reserve_port,
)


def _free_base() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_two_reservations_never_return_the_same_port(tmp_path):
    """The exact KI-A2 failure: siblings scanning from one base port."""
    base = _free_base()
    leases = [reserve_port(base, lease_dir=str(tmp_path)) for _ in range(12)]
    try:
        ports = [lease.port for lease in leases]
        assert len(set(ports)) == len(ports), f"duplicate ports handed out: {ports}"
    finally:
        for lease in leases:
            lease.release()


def test_the_reservation_outlives_the_probe_socket(tmp_path):
    """The old code closed the probe and returned; the window is what bit us."""
    base = _free_base()
    first = reserve_port(base, lease_dir=str(tmp_path))
    try:
        # Nothing is bound to first.port right now — it is bindable — yet a
        # second reservation must still refuse to hand it out.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind(("127.0.0.1", first.port))  # provably free
        second = reserve_port(base, lease_dir=str(tmp_path))
        try:
            assert second.port != first.port
        finally:
            second.release()
    finally:
        first.release()


def test_release_makes_the_port_reusable(tmp_path):
    base = _free_base()
    first = reserve_port(base, lease_dir=str(tmp_path))
    port = first.port
    first.release()
    second = reserve_port(port, max_retries=1, lease_dir=str(tmp_path))
    try:
        assert second.port == port
    finally:
        second.release()


def test_release_is_idempotent_and_only_removes_our_own_lock(tmp_path):
    base = _free_base()
    lease = reserve_port(base, lease_dir=str(tmp_path))
    lease.release()
    lease.release()  # must not raise

    # A lock owned by another PID must survive our release.
    foreign = tmp_path / f"{lease.port}.lock"
    foreign.write_text("1 0\n")  # pid 1 is alive and not us
    PortLease(lease.port, str(foreign)).release()
    assert foreign.exists(), "released a lease this process does not own"


def test_exhaustion_raises_instead_of_returning_none(tmp_path, monkeypatch):
    """Deterministic: whether real OS ports happen to be free is not the point.

    The old shape leased three consecutive real ports, which made the result
    depend on what else on the machine held a port at that instant — a flaky
    release gate is worse than none.
    """
    from exaserve.state import ports as ports_mod

    monkeypatch.setattr(ports_mod, "_bindable", lambda port, host: False)
    with pytest.raises(PortUnavailable, match="no free port"):
        reserve_port(31000, max_retries=3, lease_dir=str(tmp_path))


def test_exhaustion_message_says_how_many_are_leased(tmp_path, monkeypatch):
    from exaserve.state import ports as ports_mod

    monkeypatch.setattr(ports_mod, "_bindable", lambda port, host: False)
    with pytest.raises(PortUnavailable) as excinfo:
        reserve_port(31000, max_retries=2, lease_dir=str(tmp_path))
    assert "31000" in str(excinfo.value) and "31002" in str(excinfo.value)


def test_a_dead_owners_stale_lease_is_reclaimed_on_claim(tmp_path):
    stale = tmp_path / "31000.lock"
    stale.write_text("999999 0\n")  # a PID that does not exist
    old = time.time() - 7200
    os.utime(stale, (old, old))
    live = tmp_path / "31001.lock"
    live.write_text(f"{os.getpid()} 0\n")
    lease = reserve_port(31000, max_retries=1, lease_dir=str(tmp_path))
    try:
        assert lease.port == 31000
        assert live.exists()
    finally:
        lease.release()


def test_concurrent_claimers_receive_unique_ports(tmp_path):
    base = _free_base()

    def claim(_index):
        return reserve_port(base, max_retries=16, lease_dir=str(tmp_path))

    with ThreadPoolExecutor(max_workers=8) as pool:
        leases = list(pool.map(claim, range(8)))
    try:
        assert len({lease.port for lease in leases}) == len(leases)
    finally:
        for lease in leases:
            lease.release()


def test_old_lease_token_cannot_delete_a_successor_claim(tmp_path):
    lease = reserve_port(_free_base(), lease_dir=str(tmp_path))
    successor_token = "successor-token"
    with open(lease.path, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "schema_version": 1,
                "pid": os.getpid(),
                "token": successor_token,
                "at": time.time(),
            },
            handle,
        )
    lease.release()
    with open(lease.path, encoding="utf-8") as handle:
        assert json.load(handle)["token"] == successor_token
    os.unlink(lease.path)


def test_lease_directory_failure_is_fail_closed(tmp_path):
    blocker = tmp_path / "not-a-directory"
    blocker.write_text("x")
    with pytest.raises(PortUnavailable, match="lease directory"):
        reserve_port(_free_base(), lease_dir=str(blocker / "leases"))


def test_lease_directory_symlink_is_rejected_without_writing_target(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    lease_dir = tmp_path / "lease-link"
    lease_dir.symlink_to(target, target_is_directory=True)

    with pytest.raises(PortUnavailable, match="lease directory"):
        reserve_port(_free_base(), lease_dir=str(lease_dir))

    assert list(target.iterdir()) == []


def test_guard_symlink_is_rejected_without_modifying_victim(tmp_path):
    base = _free_base()
    victim = tmp_path / "victim"
    victim.write_text("do-not-touch", encoding="utf-8")
    (tmp_path / f"{base}.lock.guard").symlink_to(victim)

    with pytest.raises(PortUnavailable, match="arbitration failed"):
        reserve_port(base, max_retries=1, lease_dir=str(tmp_path))

    assert victim.read_text(encoding="utf-8") == "do-not-touch"
    assert not (tmp_path / f"{base}.lock").exists()


def test_lease_namespace_and_claim_files_are_private(tmp_path):
    lease_dir = tmp_path / "leases"
    lease = reserve_port(_free_base(), lease_dir=str(lease_dir))
    try:
        assert stat.S_IMODE(lease_dir.stat().st_mode) == 0o700
        assert stat.S_IMODE(os.stat(lease.path).st_mode) == 0o600
        assert stat.S_IMODE(os.stat(lease.path + ".guard").st_mode) == 0o600
        assert lease_dir.stat().st_uid == os.getuid()
    finally:
        lease.release()


def test_release_failure_is_typed_and_retryable(tmp_path, monkeypatch):
    from exaserve.state import ports as ports_mod

    lease = reserve_port(_free_base(), lease_dir=str(tmp_path))
    real_unlink = ports_mod.os.unlink
    failures = 0

    def fail_once(path):
        nonlocal failures
        if path == lease.path and failures == 0:
            failures += 1
            raise OSError("injected unlink failure")
        real_unlink(path)

    monkeypatch.setattr(ports_mod.os, "unlink", fail_once)
    with pytest.raises(PortReleaseError, match="injected unlink failure"):
        lease.release()
    assert "released=False" in repr(lease)
    lease.release()
    assert "released=True" in repr(lease)


def test_socket_activation_listener_owns_port_until_child_inherits_it():
    listener = bind_listener(0, host="127.0.0.1")
    try:
        port = listener.port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as competing:
            with pytest.raises(OSError):
                competing.bind(("127.0.0.1", port))
        assert listener.fileno() >= 0
        assert os.get_inheritable(listener.fileno()) is True
    finally:
        listener.close()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reused:
        reused.bind(("127.0.0.1", port))
