from __future__ import annotations

import threading
import time

import pytest

from clientlab.collectors import ports


def test_port_collector_surfaces_worker_failure(monkeypatch):
    attempted = threading.Event()

    def fail_snapshot():
        attempted.set()
        raise RuntimeError("procfs parser failed")

    monkeypatch.setattr(ports, "take_snapshot", fail_snapshot)
    collector = ports.PortCollector(interval_s=0.01)
    collector.start()
    assert attempted.wait(1.0)

    with pytest.raises(RuntimeError, match="procfs parser failed"):
        collector.stop()


def test_port_collector_refuses_to_hide_a_live_thread(monkeypatch):
    entered = threading.Event()
    release = threading.Event()

    def blocked_snapshot():
        entered.set()
        release.wait(2.0)
        return {"ephemeral_in_use": 0, "established": 0, "time_wait": 0}

    monkeypatch.setattr(ports, "take_snapshot", blocked_snapshot)
    collector = ports.PortCollector(interval_s=0.01)
    collector.start()
    assert entered.wait(1.0)
    started = time.monotonic()
    try:
        with pytest.raises(RuntimeError, match="did not stop"):
            collector.stop(timeout_s=0.01)
        assert time.monotonic() - started < 0.5
    finally:
        release.set()
        collector._thread.join(timeout=1.0)
