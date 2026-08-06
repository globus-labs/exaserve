"""IMP-B02 cutover: the eval harness trusts the snapshot, not the marker."""

from __future__ import annotations

import json
import os

from eval.lib.backends.base import ProcessMonitor


def _monitor(tmp_path, **kw) -> ProcessMonitor:
    mon = ProcessMonitor(process=None, log_path=str(tmp_path / "svc.log"),
                         ready_marker="[Driver] ALL SERVICES READY",
                         readiness_dir=str(tmp_path / "run_logs"), **kw)
    mon.ready_event.clear()
    return mon


def _write_snapshot(tmp_path, ready: bool, sub="run_logs/deploy1") -> str:
    directory = tmp_path / sub
    os.makedirs(directory, exist_ok=True)
    path = directory / "readiness.json"
    path.write_text(json.dumps({"ready": ready, "generation": 1,
                                "blockers": [] if ready else ["model m: 0/2 replicas"]}))
    return str(path)


def test_snapshot_ready_satisfies_the_wait(tmp_path):
    mon = _monitor(tmp_path)
    _write_snapshot(tmp_path, ready=True)
    assert mon.wait_for_ready(timeout_s=5) is True
    assert mon.readiness_source == "snapshot"


def test_marker_alone_cannot_declare_ready_when_the_snapshot_says_otherwise(tmp_path):
    """The regression this closes: stdout text outrunning the actual state."""
    mon = _monitor(tmp_path, readiness_marker_grace_s=0.0)
    _write_snapshot(tmp_path, ready=False)
    mon.ready_event.set()                      # the marker was printed
    assert mon.wait_for_ready(timeout_s=2) is False


def test_marker_is_accepted_after_the_grace_for_a_legacy_backend(tmp_path):
    mon = _monitor(tmp_path, readiness_marker_grace_s=0.0)
    mon.ready_event.set()
    assert mon.wait_for_ready(timeout_s=5) is True
    assert mon.readiness_source == "marker"


def test_no_signal_at_all_times_out(tmp_path):
    mon = _monitor(tmp_path, readiness_marker_grace_s=0.0)
    assert mon.wait_for_ready(timeout_s=1) is False


def test_snapshot_wins_over_a_stale_not_ready_file_once_it_flips(tmp_path):
    mon = _monitor(tmp_path)
    _write_snapshot(tmp_path, ready=False)
    assert mon.wait_for_ready(timeout_s=1) is False
    _write_snapshot(tmp_path, ready=True)
    assert mon.wait_for_ready(timeout_s=5) is True
