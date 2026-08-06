"""WP2.1/WP2.2 acceptance: crash-at-boundary and lease semantics (hermetic)."""

from __future__ import annotations

import json
import os
import threading

import pytest

from exaserve.state.atomic import (
    ExclusiveLease,
    LeaseHeldError,
    atomic_write_json,
    atomic_write_text,
)


def test_crash_before_publish_preserves_previous_state(tmp_path, monkeypatch):
    target = tmp_path / "state.json"
    atomic_write_json(target, {"v": 1})

    real_replace = os.replace

    def crashing_replace(src, dst):  # crash injected exactly at the boundary
        raise OSError("simulated crash before rename")

    monkeypatch.setattr(os, "replace", crashing_replace)
    with pytest.raises(OSError):
        atomic_write_json(target, {"v": 2})
    monkeypatch.setattr(os, "replace", real_replace)

    assert json.loads(target.read_text()) == {"v": 1}  # old state intact
    assert [p for p in tmp_path.iterdir() if p.name != "state.json"] == []  # no litter


def test_concurrent_writers_never_expose_torn_content(tmp_path):
    target = tmp_path / "hot.json"
    atomic_write_json(target, {"n": -1, "pad": "x" * 4096})
    stop = threading.Event()
    errors: list[str] = []

    def writer(idx: int) -> None:
        n = 0
        while not stop.is_set():
            atomic_write_json(target, {"n": idx, "seq": n, "pad": "x" * 4096},
                              fsync=False)
            n += 1

    def reader() -> None:
        while not stop.is_set():
            try:
                data = json.loads(target.read_text())
            except json.JSONDecodeError as exc:  # torn read = failure
                errors.append(str(exc))
                return
            assert "pad" in data

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
    threads += [threading.Thread(target=reader) for _ in range(2)]
    for thread in threads:
        thread.start()
    threading.Event().wait(1.0)
    stop.set()
    for thread in threads:
        thread.join()
    assert errors == []


def test_lease_exclusive_and_foreign_live_lease_respected(tmp_path):
    lease_path = tmp_path / "run.lease"
    with ExclusiveLease(lease_path, ttl_s=3600):
        # Second acquire while held (same host, live pid) must fail…
        with pytest.raises(LeaseHeldError):
            ExclusiveLease(lease_path, ttl_s=3600).acquire()
        # …including when it claims a different host (PR-013: foreign live
        # leases are NOT stale).
        owner = json.loads(lease_path.read_text())
        owner["host"] = "some-other-node"
        owner["pid"] = 1  # pid liveness must not be consulted cross-host
        lease_path.write_text(json.dumps(owner))
        with pytest.raises(LeaseHeldError):
            ExclusiveLease(lease_path, ttl_s=3600).acquire()
    assert not lease_path.exists()  # released


def test_lease_ttl_expiry_and_dead_pid_takeover(tmp_path):
    lease_path = tmp_path / "run.lease"
    # Expired foreign lease → takeover allowed.
    lease_path.write_text(json.dumps({
        "host": "other", "pid": 1, "acquired_at": 0, "ttl_s": 1}))
    with ExclusiveLease(lease_path, ttl_s=60) as lease:
        assert lease.read_owner()["host"] == os.uname().nodename
        assert lease.read_owner()["stole_from"]["host"] == "other"

    # Same-host dead-pid lease → takeover allowed even before TTL.
    lease_path.write_text(json.dumps({
        "host": os.uname().nodename, "pid": 2 ** 22 + 1234,
        "acquired_at": __import__("time").time(), "ttl_s": 3600}))
    with ExclusiveLease(lease_path, ttl_s=3600):
        pass
    assert not lease_path.exists()


def test_unreadable_lease_is_not_silently_cleared(tmp_path):
    lease_path = tmp_path / "run.lease"
    lease_path.write_text("{torn")
    with pytest.raises(LeaseHeldError):
        ExclusiveLease(lease_path, ttl_s=3600).acquire()


def test_atomic_text_shrinking_write_leaves_no_trailing_bytes(tmp_path):
    # KNOWN_ISSUES C3 class: shrinking rewrite must not leave old tail bytes.
    target = tmp_path / "result.json"
    atomic_write_text(target, "A" * 10_000)
    atomic_write_text(target, json.dumps({"ok": 1}))
    assert json.loads(target.read_text()) == {"ok": 1}
    assert target.stat().st_size == len(json.dumps({"ok": 1}))
