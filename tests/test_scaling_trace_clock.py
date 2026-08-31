"""Scaling evidence forms durations only from same-boot monotonic time."""

from __future__ import annotations

import json

from exaserve.scaling_trace import ScalingTracer, merge_replica_init_evidence


def test_trace_total_ignores_a_backward_wall_clock_step(tmp_path, monkeypatch):
    monkeypatch.setenv("EXASERVE_SCALING_TRACE", "1")
    wall_values = iter((1_000.0, 900.0))
    monotonic_values = iter((10.0, 15.5))
    tracer = ScalingTracer(
        wall_time=lambda: next(wall_values),
        monotonic_time=lambda: next(monotonic_values),
        boot_id=lambda: "boot-a",
    )
    path = tmp_path / "trace.json"
    tracer.save(str(path))

    metadata = json.loads(path.read_text(encoding="utf-8"))["metadata"]
    assert metadata["trace_start"] == 1_000.0
    assert metadata["trace_end"] == 900.0
    assert metadata["trace_clock_boot_id"] == "boot-a"
    assert metadata["total_duration_s"] == 5.5


def test_engine_init_stats_cannot_overwrite_actor_slot_timing():
    merged = merge_replica_init_evidence(
        actor_fields={
            "component_slot": "replica/m/0",
            "total_init_s": 12.0,
            "wall_end": 1_012.0,
            "monotonic_start": 20.0,
            "monotonic_end": 32.0,
        },
        engine_fields={
            # Real vLLM-shaped collision fields plus one non-collision field.
            "total_init_s": 10.0,
            "wall_end": 1_010.0,
            "engine_create_s": 8.0,
        },
    )
    assert merged["total_init_s"] == 12.0
    assert merged["engine_total_init_s"] == 10.0
    assert merged["wall_end"] == 1_012.0
    assert merged["engine_wall_end"] == 1_010.0
    assert merged["engine_create_s"] == 8.0
