"""PR-012 / KI-A2 / TD-PORTS: a leased port is owned, not merely probed."""

from __future__ import annotations

import os
import socket

import pytest

from exaserve.state.ports import (
    PortLease,
    PortUnavailable,
    release_all,
    reserve_port,
    sweep_stale,
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
            probe.bind(("127.0.0.1", first.port))   # provably free
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
    lease.release()                       # must not raise

    # A lock owned by another PID must survive our release.
    foreign = tmp_path / f"{lease.port}.lock"
    foreign.write_text("1 0\n")           # pid 1 is alive and not us
    PortLease(lease.port, str(foreign)).release()
    assert foreign.exists(), "released a lease this process does not own"


def test_exhaustion_raises_instead_of_returning_none(tmp_path):
    base = _free_base()
    held = [reserve_port(base + i, max_retries=1, lease_dir=str(tmp_path))
            for i in range(3)]
    try:
        with pytest.raises(PortUnavailable, match="no free port"):
            reserve_port(base, max_retries=3, lease_dir=str(tmp_path))
    finally:
        for lease in held:
            lease.release()


def test_a_dead_owners_lease_is_swept(tmp_path):
    stale = tmp_path / "31000.lock"
    stale.write_text("999999 0\n")        # a PID that does not exist
    live = tmp_path / "31001.lock"
    live.write_text(f"{os.getpid()} 0\n")
    assert sweep_stale(str(tmp_path)) == 1
    assert not stale.exists() and live.exists()


def test_release_all_reports_what_it_freed(tmp_path):
    base = _free_base()
    lease = reserve_port(base, lease_dir=str(tmp_path))
    assert release_all([lease.port], lease_dir=str(tmp_path)) == 1
    assert release_all([lease.port], lease_dir=str(tmp_path)) == 0


def test_server_get_open_port_uses_the_lease(monkeypatch, tmp_path):
    monkeypatch.setenv("EXASERVE_PORT_LEASE_DIR", str(tmp_path))
    from exaserve import server

    base = _free_base()
    first = server.get_open_port(base)
    second = server.get_open_port(base)
    try:
        assert first is not None and second is not None and first != second
    finally:
        server.release_port(first)
        server.release_port(second)
