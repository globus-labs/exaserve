"""WP2.1/WP2.2 acceptance: crash-at-boundary and lease semantics (hermetic)."""

from __future__ import annotations

import json
import os
import pickle
import stat
import threading

import pytest

from exaserve.state.atomic import (
    ExclusiveLease,
    LeaseHeartbeat,
    LeaseHeldError,
    LeaseReleaseError,
    atomic_create_json,
    atomic_create_or_verify_json,
    atomic_create_yaml,
    atomic_write_json,
    atomic_write_text,
    atomic_text_writer,
    ensure_owned_directory,
)


def test_atomic_create_or_verify_accepts_only_exact_regular_artifact(tmp_path):
    target = tmp_path / "immutable.json"
    assert atomic_create_or_verify_json(target, {"owner": "same"}) is True
    assert atomic_create_or_verify_json(target, {"owner": "same"}) is False
    with pytest.raises(FileExistsError, match="different content"):
        atomic_create_or_verify_json(target, {"owner": "different"})
    assert json.loads(target.read_text(encoding="utf-8")) == {"owner": "same"}


def test_atomic_create_or_verify_never_follows_existing_symlink(tmp_path):
    victim = tmp_path / "victim.json"
    victim.write_text('{"owner": "victim"}\n', encoding="utf-8")
    target = tmp_path / "immutable.json"
    target.symlink_to(victim)
    with pytest.raises(FileExistsError, match="verifiable regular file"):
        atomic_create_or_verify_json(target, {"owner": "victim"})
    assert json.loads(victim.read_text(encoding="utf-8")) == {"owner": "victim"}


def test_owned_directory_rejects_final_symlink_and_foreign_writable_mode(tmp_path):
    victim = tmp_path / "victim"
    victim.mkdir()
    link = tmp_path / "run"
    link.symlink_to(victim, target_is_directory=True)
    with pytest.raises(OSError):
        ensure_owned_directory(link)

    writable = tmp_path / "writable"
    writable.mkdir(mode=0o777)
    writable.chmod(0o777)
    with pytest.raises(PermissionError, match="writable by another"):
        ensure_owned_directory(writable)


def test_atomic_create_yaml_never_replaces_existing_artifact(tmp_path):
    target = tmp_path / "immutable.yaml"
    atomic_create_yaml(target, {"owner": "first"})
    with pytest.raises(FileExistsError):
        atomic_create_yaml(target, {"owner": "second"})
    assert target.read_text(encoding="utf-8") == "owner: first\n"


def test_atomic_create_never_replaces_an_existing_file(tmp_path):
    target = tmp_path / "immutable.json"
    atomic_create_json(target, {"owner": "first"})
    with pytest.raises(FileExistsError):
        atomic_create_json(target, {"owner": "second"})
    assert json.loads(target.read_text(encoding="utf-8")) == {"owner": "first"}
    assert list(tmp_path.glob(".*.tmp")) == []


def test_concurrent_atomic_create_elects_exactly_one_complete_writer(tmp_path):
    target = tmp_path / "immutable.json"
    barrier = threading.Barrier(2)
    outcomes = []

    def create(owner: str) -> None:
        barrier.wait()
        try:
            atomic_create_json(target, {"owner": owner, "pad": owner * 4096}, fsync=False)
            outcomes.append((owner, "created"))
        except FileExistsError:
            outcomes.append((owner, "exists"))

    threads = [threading.Thread(target=create, args=(owner,)) for owner in ("a", "b")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(status for _, status in outcomes) == ["created", "exists"]
    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload["owner"] in {"a", "b"}
    assert payload["pad"] == payload["owner"] * 4096


def test_atomic_text_writer_does_not_publish_partial_output(tmp_path):
    target = tmp_path / "stream.txt"
    target.write_text("old", encoding="utf-8")
    with pytest.raises(RuntimeError, match="interrupted"):
        with atomic_text_writer(target) as handle:
            handle.write("partial")
            raise RuntimeError("interrupted")
    assert target.read_text(encoding="utf-8") == "old"

    with atomic_text_writer(target) as handle:
        handle.write("complete")
    assert target.read_text(encoding="utf-8") == "complete"


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


def test_directory_fsync_failure_is_not_silently_reported_as_durable(tmp_path, monkeypatch):
    target = tmp_path / "state.json"
    real_fsync = os.fsync

    def fail_directory_sync(fd):
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError("simulated directory durability failure")
        return real_fsync(fd)

    monkeypatch.setattr(os, "fsync", fail_directory_sync)
    with pytest.raises(OSError, match="directory durability failure"):
        atomic_write_json(target, {"v": 1})

    # The rename preceded the durability error. Exact reconciliation is safe,
    # but the caller was not falsely told the publication was durable.
    assert json.loads(target.read_text(encoding="utf-8")) == {"v": 1}


def test_concurrent_writers_never_expose_torn_content(tmp_path):
    target = tmp_path / "hot.json"
    atomic_write_json(target, {"n": -1, "pad": "x" * 4096})
    stop = threading.Event()
    errors: list[str] = []

    def writer(idx: int) -> None:
        n = 0
        while not stop.is_set():
            atomic_write_json(target, {"n": idx, "seq": n, "pad": "x" * 4096}, fsync=False)
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


def test_lease_held_error_round_trips_through_multiprocessing_pickle(tmp_path):
    error = LeaseHeldError(str(tmp_path / "run.lease"), {"host": "node", "pid": 7})
    restored = pickle.loads(pickle.dumps(error))

    assert restored.path == error.path
    assert restored.owner == error.owner
    assert str(restored) == str(error)


def test_lease_ttl_expiry_and_dead_pid_takeover(tmp_path):
    lease_path = tmp_path / "run.lease"
    # Expired foreign lease → takeover allowed.
    lease_path.write_text(json.dumps({"host": "other", "pid": 1, "acquired_at": 0, "ttl_s": 1}))
    with ExclusiveLease(lease_path, ttl_s=60) as lease:
        assert lease.read_owner()["host"] == os.uname().nodename
        assert lease.read_owner()["stole_from"]["host"] == "other"

    # Same-host dead-pid lease → takeover allowed even before TTL.
    lease_path.write_text(
        json.dumps(
            {
                "host": os.uname().nodename,
                "pid": 2**22 + 1234,
                "acquired_at": __import__("time").time(),
                "ttl_s": 3600,
            }
        )
    )
    with ExclusiveLease(lease_path, ttl_s=3600):
        pass
    assert not lease_path.exists()


def test_unreadable_lease_is_not_silently_cleared(tmp_path):
    lease_path = tmp_path / "run.lease"
    lease_path.write_text("{torn")
    with pytest.raises(LeaseHeldError):
        ExclusiveLease(lease_path, ttl_s=3600).acquire()


@pytest.mark.parametrize("field", ["ttl_s", "acquired_at"])
def test_coercible_lease_timing_is_not_treated_as_expired(tmp_path, field):
    lease_path = tmp_path / "run.lease"
    owner = {"host": "other", "pid": 1, "acquired_at": 0, "ttl_s": 1}
    owner[field] = "0"
    lease_path.write_text(json.dumps(owner), encoding="utf-8")
    with pytest.raises(LeaseHeldError):
        ExclusiveLease(lease_path, ttl_s=1).acquire()


def test_atomic_text_shrinking_write_leaves_no_trailing_bytes(tmp_path):
    # KNOWN_ISSUES C3 class: shrinking rewrite must not leave old tail bytes.
    target = tmp_path / "result.json"
    atomic_write_text(target, "A" * 10_000)
    atomic_write_text(target, json.dumps({"ok": 1}))
    assert json.loads(target.read_text()) == {"ok": 1}
    assert target.stat().st_size == len(json.dumps({"ok": 1}))


def test_lease_mutations_share_takeover_arbitration(tmp_path):
    """Renew/release fail closed while another lease mutator is active."""
    lease_path = tmp_path / "run.lease"
    lease = ExclusiveLease(lease_path, ttl_s=60).acquire()
    (tmp_path / "run.lease.takeover.lock").write_text("held")

    assert lease.renew() is False
    with pytest.raises(LeaseReleaseError, match="arbitration"):
        lease.release()
    assert lease_path.exists()
    (tmp_path / "run.lease.takeover.lock").unlink()
    lease.release()
    assert not lease_path.exists()


def test_lease_symlink_is_never_read_or_replaced(tmp_path):
    victim = tmp_path / "victim"
    victim.write_text('{"secret": true}', encoding="utf-8")
    lease_path = tmp_path / "run.lease"
    lease_path.symlink_to(victim)

    with pytest.raises(LeaseHeldError):
        ExclusiveLease(lease_path, ttl_s=60).acquire()

    assert victim.read_text(encoding="utf-8") == '{"secret": true}'
    assert lease_path.is_symlink()


def test_lease_files_are_private(tmp_path):
    lease_path = tmp_path / "run.lease"
    lease = ExclusiveLease(lease_path, ttl_s=60).acquire()
    try:
        assert (lease_path.stat().st_mode & 0o777) == 0o600
    finally:
        lease.release()


def test_lease_rejects_nonfinite_or_nonpositive_timing(tmp_path):
    for ttl in (True, 0, -1, float("nan"), float("inf")):
        with pytest.raises(ValueError, match="TTL"):
            ExclusiveLease(tmp_path / "run.lease", ttl_s=ttl)
    lease = ExclusiveLease(tmp_path / "run.lease", ttl_s=60)
    for interval in (True, 0, -1, float("nan"), float("inf")):
        with pytest.raises(ValueError, match="heartbeat"):
            LeaseHeartbeat(lease, interval_s=interval)


def test_heartbeat_detects_lease_loss_before_publish(tmp_path):
    lease_path = tmp_path / "run.lease"
    lease = ExclusiveLease(lease_path, ttl_s=60).acquire()
    heartbeat = LeaseHeartbeat(lease, interval_s=60)
    with heartbeat:
        owner = lease.read_owner()
        assert owner is not None
        owner["token"] = "successor-token"
        lease_path.write_text(json.dumps(owner))
        with pytest.raises(LeaseHeldError):
            heartbeat.ensure_held()
    lease.release()
    assert lease_path.exists()  # stale owner cannot remove the successor
