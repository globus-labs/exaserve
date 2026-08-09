"""AC-TEL-01 / AC-STAT-01: typed, bounded, generation-fenced push stats."""

from __future__ import annotations

import copy
import sys
from types import SimpleNamespace

import pytest

from exaserve.telemetry import (
    OwnedTelemetryActors,
    ReplicaInitStatsStore,
    ReplicaLifecycleStore,
    ServingStatsStore,
    TelemetryContractError,
    TelemetryIdentity,
    lifecycle_event_envelope,
    replica_init_envelope,
    serving_envelope,
    telemetry_actor_name,
    validate_lifecycle_snapshot,
    validate_replica_init_snapshot,
    validate_serving_snapshot,
)


def _identity(generation=1):
    return TelemetryIdentity(
        deployment_id="deployment-a",
        generation=generation,
        deployment_plan_hash="a" * 64,
        allocation_binding_hash="b" * 64,
    )


def _summary(total=1):
    nonempty = {"n": 1, "mean": 0.1, "p50": 0.1, "p90": 0.1, "p99": 0.1, "max": 0.1}
    return {
        "total_requests": total,
        "server_ttft": dict(nonempty),
        "server_tbt": dict(nonempty),
        "e2e": dict(nonempty),
        "queued_time": dict(nonempty),
        "prefill_time": dict(nonempty),
        "decode_time": dict(nonempty),
        "mean_batch_size": 1.0,
        "max_batch_size": 1,
        "kv_cache_peak": 0.5,
    }


def _envelope(identity, replica="r0", sequence=0):
    return serving_envelope(
        identity=identity,
        replica_id=replica,
        sequence=sequence,
        model_id="m",
        node_ip="10.0.0.1",
        pid=123,
        summary=_summary(),
        sample=[{"finished_at": 1.0, "ttft": 0.1, "tbt": 0.1, "e2e": 0.2}],
    )


def test_two_generations_have_distinct_actor_names_and_stale_writes_fail():
    current = _identity(2)
    assert telemetry_actor_name("serving", current) != telemetry_actor_name("serving", _identity(1))
    store = ServingStatsStore(current.to_dict(), expected_replicas=1)
    with pytest.raises(TelemetryContractError, match="stale or foreign"):
        store.report(_envelope(_identity(1)))
    snapshot = store.snapshot()
    assert snapshot["received_replicas"] == 0
    assert snapshot["rejected_reports"] == 1


def test_expected_vs_received_and_sequence_dedup_are_explicit():
    identity = _identity()
    store = ServingStatsStore(identity.to_dict(), expected_replicas=2)
    first = _envelope(identity, "r0", 0)
    assert store.report(first) is True
    assert store.report(first) is False
    assert store.snapshot()["complete"] is False
    assert store.report(_envelope(identity, "r1", 0)) is True
    snapshot = store.snapshot()
    assert snapshot["complete"] is True
    assert snapshot["received_replicas"] == snapshot["expected_replicas"] == 2
    assert snapshot["duplicates_ignored"] == 1
    assert (
        validate_serving_snapshot(snapshot, expected_identity=identity, expected_replicas=2)
        == snapshot
    )


def test_conflicting_or_regressed_serving_sequence_is_rejected():
    identity = _identity()
    store = ServingStatsStore(identity.to_dict(), expected_replicas=1)
    first = _envelope(identity, sequence=2)
    store.report(first)
    conflict = copy.deepcopy(first)
    conflict["payload"]["summary"]["total_requests"] = 2
    with pytest.raises(TelemetryContractError, match="conflicting"):
        store.report(conflict)
    with pytest.raises(TelemetryContractError, match="regressed"):
        store.report(_envelope(identity, sequence=1))
    assert store.snapshot()["rejected_reports"] == 2


def test_serving_snapshot_rejects_inconsistent_completion_flag():
    identity = _identity()
    store = ServingStatsStore(identity.to_dict(), expected_replicas=1)
    store.report(_envelope(identity))
    snapshot = store.snapshot()
    snapshot["complete"] = False
    with pytest.raises(TelemetryContractError, match="complete flag"):
        validate_serving_snapshot(snapshot, expected_identity=identity)


def test_replica_init_snapshot_is_validated_after_transport():
    identity = _identity()
    store = ReplicaInitStatsStore(identity.to_dict(), expected_replicas=1)
    store.report(
        replica_init_envelope(identity=identity, replica_id="r0", payload={"duration_s": 1.0})
    )
    snapshot = store.snapshot()
    assert validate_replica_init_snapshot(snapshot, expected_identity=identity) == snapshot
    snapshot["replicas"]["r0"]["duration_s"] = float("nan")
    with pytest.raises(TelemetryContractError, match="finite JSON"):
        validate_replica_init_snapshot(snapshot, expected_identity=identity)


def test_replica_init_conflicting_duplicate_is_rejected():
    identity = _identity()
    store = ReplicaInitStatsStore(identity.to_dict(), expected_replicas=1)
    first = replica_init_envelope(identity=identity, replica_id="r0", payload={"duration_s": 1.0})
    store.report(first)
    assert store.report(first) is False
    conflict = copy.deepcopy(first)
    conflict["payload"]["duration_s"] = 2.0
    with pytest.raises(TelemetryContractError, match="conflicting"):
        store.report(conflict)


def test_invalid_or_excess_replica_stats_cannot_count_as_complete():
    identity = _identity()
    store = ServingStatsStore(identity.to_dict(), expected_replicas=1)
    malformed = _envelope(identity)
    malformed["payload"]["summary"] = {"total_requests": 1}
    with pytest.raises(TelemetryContractError, match="shape"):
        store.report(malformed)
    assert store.report(_envelope(identity, "r0"))
    with pytest.raises(TelemetryContractError, match="more replicas"):
        store.report(_envelope(identity, "r1"))
    assert store.snapshot()["received_replicas"] == 1


def test_nonfinite_and_oversized_samples_are_rejected():
    identity = _identity()
    store = ServingStatsStore(identity.to_dict(), expected_replicas=1)
    bad = copy.deepcopy(_envelope(identity))
    bad["payload"]["sample"][0]["ttft"] = float("nan")
    with pytest.raises(TelemetryContractError, match="finite"):
        store.report(bad)


def test_serving_stats_reject_non_ip_nodes_and_noninteger_max_batch():
    identity = _identity()
    store = ServingStatsStore(identity.to_dict(), expected_replicas=1)
    bad_ip = _envelope(identity)
    bad_ip["payload"]["node_ip"] = "?"
    with pytest.raises(TelemetryContractError, match="IP address"):
        store.report(bad_ip)

    bad_batch = _envelope(identity)
    bad_batch["payload"]["summary"]["max_batch_size"] = 1.0
    with pytest.raises(TelemetryContractError, match="max_batch_size"):
        store.report(bad_batch)


def test_serving_stats_store_owns_and_returns_deep_copies():
    identity = _identity()
    store = ServingStatsStore(identity.to_dict(), expected_replicas=1)
    envelope = _envelope(identity)
    store.report(envelope)
    envelope["payload"]["summary"]["total_requests"] = 999
    first = store.snapshot()
    assert first["replicas"]["r0"]["payload"]["summary"]["total_requests"] == 1
    first["replicas"]["r0"]["payload"]["summary"]["total_requests"] = 500
    assert store.snapshot()["replicas"]["r0"]["payload"]["summary"]["total_requests"] == 1


def test_owned_telemetry_cleanup_covers_success_and_is_idempotent():
    actors = OwnedTelemetryActors()
    handle = object()
    actors.register("serving", handle)
    killed = []
    assert actors.cleanup(killed.append) == {}
    assert killed == [handle]
    assert actors.snapshot() == ()
    assert actors.cleanup(killed.append) == {}
    assert killed == [handle]


def test_failed_telemetry_cleanup_is_observable_and_retryable():
    actors = OwnedTelemetryActors()
    handle = object()
    actors.register("replica_init", handle)

    def fail(_handle):
        raise RuntimeError("kill failed")

    assert actors.cleanup(fail) == {"replica_init": "RuntimeError: kill failed"}
    assert actors.snapshot() == ("replica_init",)
    assert actors.cleanup(lambda _handle: None) == {}
    assert actors.snapshot() == ()


def test_required_server_stats_fail_when_consumed_actor_cleanup_fails(tmp_path, monkeypatch):
    from eval.lib.server_stats import collect_server_stats

    identity = _identity()
    store = ServingStatsStore(identity.to_dict(), expected_replicas=1)
    store.report(_envelope(identity))
    actor = SimpleNamespace(snapshot=SimpleNamespace(remote=store.snapshot))

    def kill_failed(_actor, *, no_restart):
        assert no_restart is True
        raise RuntimeError("kill failed")

    fake_ray = SimpleNamespace(
        is_initialized=lambda: True,
        get_actor=lambda _name, namespace: actor,
        get=lambda value, timeout: value,
        kill=kill_failed,
    )
    monkeypatch.setitem(sys.modules, "ray", fake_ray)

    result = collect_server_stats(
        str(tmp_path), identity=identity, expected_replicas=1, ray_address="10.0.0.1:6379"
    )

    assert "could not terminate" in result["error"]
    assert list(tmp_path.iterdir()) == []


def _lifecycle_event(identity, component, instance, event, detail=""):
    return lifecycle_event_envelope(
        identity=identity,
        component_id=component,
        instance_id=instance,
        event=event,
        model_id="model-a",
        node_id="node-a",
        pid=123,
        detail=detail,
    )


def test_replica_lifecycle_requires_every_exact_component_to_stop_cleanly():
    identity = _identity()
    store = ReplicaLifecycleStore(identity.to_dict(), ["replica-a", "replica-b"])
    store.report(_lifecycle_event(identity, "replica-a", "actor-a", "STARTED"))
    store.report(_lifecycle_event(identity, "replica-b", "actor-b", "STARTED"))
    assert store.snapshot()["clean"] is False
    store.report(_lifecycle_event(identity, "replica-a", "actor-a", "STOPPED"))
    store.report(_lifecycle_event(identity, "replica-b", "actor-b", "STOPPED"))
    snapshot = store.snapshot()
    assert snapshot["clean"] is True
    assert validate_lifecycle_snapshot(snapshot, expected_identity=identity) == snapshot
    assert snapshot["stopped_components"] == ["replica-a", "replica-b"]


def test_lifecycle_conflicting_started_duplicate_is_rejected():
    identity = _identity()
    store = ReplicaLifecycleStore(identity.to_dict(), ["replica-a"])
    started = _lifecycle_event(identity, "replica-a", "actor-a", "STARTED")
    store.report(started)
    assert store.report(started) is False
    conflict = copy.deepcopy(started)
    conflict["detail"] = "different"
    with pytest.raises(TelemetryContractError, match="conflicting"):
        store.report(conflict)


def test_lifecycle_snapshot_rejects_forged_clean_state():
    identity = _identity()
    store = ReplicaLifecycleStore(identity.to_dict(), ["replica-a"])
    store.report(_lifecycle_event(identity, "replica-a", "actor-a", "STARTED"))
    snapshot = store.snapshot()
    snapshot["clean"] = True
    with pytest.raises(TelemetryContractError, match="clean flag"):
        validate_lifecycle_snapshot(snapshot, expected_identity=identity)


def test_replica_lifecycle_surfaces_destructor_failure_or_missing_event():
    identity = _identity()
    store = ReplicaLifecycleStore(identity.to_dict(), ["replica-a", "replica-b"])
    store.report(_lifecycle_event(identity, "replica-a", "actor-a", "STARTED"))
    store.report(_lifecycle_event(identity, "replica-a", "actor-a", "FAILED", "core stuck"))
    snapshot = store.snapshot()
    assert snapshot["complete"] is False
    assert snapshot["clean"] is False
    assert snapshot["failed_components"] == ["replica-a"]
    assert snapshot["missing_components"] == ["replica-b"]


def test_replica_lifecycle_rejects_wrong_instance_and_allows_terminal_replacement():
    identity = _identity()
    store = ReplicaLifecycleStore(identity.to_dict(), ["replica-a"])
    store.report(_lifecycle_event(identity, "replica-a", "actor-a", "STARTED"))
    with pytest.raises(TelemetryContractError, match="wrong instance"):
        store.report(_lifecycle_event(identity, "replica-a", "actor-b", "STOPPED"))
    store.report(_lifecycle_event(identity, "replica-a", "actor-a", "STOPPED"))
    store.report(_lifecycle_event(identity, "replica-a", "actor-b", "STARTED"))
    store.report(_lifecycle_event(identity, "replica-a", "actor-b", "STOPPED"))
    snapshot = store.snapshot()
    assert snapshot["clean"] is True
    assert snapshot["replacements"] == {"replica-a": 1}
