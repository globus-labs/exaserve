"""WP2.3 acceptance: status lifecycle, CAS conflicts, illegal transitions."""

from __future__ import annotations

import json
import threading

import pytest

from exaserve.state.status import (
    MAX_STATUS_HISTORY_EVENTS,
    DeploymentState,
    IllegalTransition,
    RunState,
    StatusConflict,
    StatusStore,
)


def _walk_to_ready(store: StatusStore) -> None:
    store.initialize("dep-1", DeploymentState.PLANNED)
    chain = [
        DeploymentState.PLANNED,
        DeploymentState.STAGING,
        DeploymentState.CLUSTER_STARTING,
        DeploymentState.DEPLOYING,
        DeploymentState.VALIDATING,
        DeploymentState.READY,
    ]
    for current, nxt in zip(chain, chain[1:]):
        store.transition(current, nxt, reason_code="STEP")


def test_deployment_lifecycle_and_history(tmp_path):
    store = StatusStore.deployment(tmp_path / "deployment_status.json")
    _walk_to_ready(store)
    record = store.load()
    assert record.state == "READY" and record.revision == 5
    assert [h["state"] for h in record.history][0] == "PLANNED"

    # Post-READY loss revalidates; READY is not a latch (plan WP5.1/ADR-002).
    store.transition(DeploymentState.READY, DeploymentState.VALIDATING, reason_code="REPLICA_LOST")
    # Terminal FAILED reachable from active state.
    store.transition(DeploymentState.VALIDATING, DeploymentState.FAILED, reason_code="DEADLINE")
    with pytest.raises(IllegalTransition):
        store.transition(DeploymentState.FAILED, DeploymentState.READY, reason_code="NOPE")


def test_illegal_skip_and_stale_expectation(tmp_path):
    store = StatusStore.deployment(tmp_path / "s.json")
    store.initialize("dep-2", DeploymentState.PLANNED)
    with pytest.raises(IllegalTransition):
        store.transition(DeploymentState.PLANNED, DeploymentState.READY, reason_code="SKIP")
    store.transition(DeploymentState.PLANNED, DeploymentState.STAGING, reason_code="STEP")
    with pytest.raises(StatusConflict):  # caller's view is stale
        store.transition(DeploymentState.PLANNED, DeploymentState.STAGING, reason_code="STALE")


def test_run_lifecycle_distinguishes_partial_and_invalid(tmp_path):
    store = StatusStore.run(tmp_path / "run_status.json")
    store.initialize("run-1", RunState.PLANNED)
    store.transition(
        RunState.PLANNED,
        RunState.SUBMITTED,
        reason_code="QSUB",
        data_update={"scheduler_job_id": "123.aurora"},
    )
    store.transition(RunState.SUBMITTED, RunState.RUNNING, reason_code="R")
    store.transition(
        RunState.RUNNING,
        RunState.PARTIAL,
        reason_code="MISSING_SHARDS",
        data_update={"collected_ranks": 3, "expected_ranks": 4},
    )
    record = store.load()
    assert record.state == "PARTIAL"
    assert record.data["scheduler_job_id"] == "123.aurora"  # durable job identity
    with pytest.raises(IllegalTransition):  # PARTIAL is terminal
        store.transition(RunState.PARTIAL, RunState.SUCCEEDED, reason_code="NO")


def test_failed_run_is_terminal_and_retry_requires_a_new_materialization(tmp_path):
    store = StatusStore.run(tmp_path / "run_status.json")
    store.initialize("run-failed", RunState.PLANNED)
    store.transition(RunState.PLANNED, RunState.SUBMITTED, reason_code="QSUB")
    store.transition(RunState.SUBMITTED, RunState.FAILED, reason_code="FAILED")

    with pytest.raises(IllegalTransition):
        store.transition(RunState.FAILED, RunState.SUBMITTED, reason_code="RETRY")


def test_concurrent_cas_exactly_one_winner(tmp_path):
    store = StatusStore.run(tmp_path / "run_status.json")
    store.initialize("run-2", RunState.PLANNED)
    outcomes: list[str] = []
    barrier = threading.Barrier(2)

    def submit(tag: str) -> None:
        barrier.wait()
        try:
            StatusStore.run(tmp_path / "run_status.json").transition(
                RunState.PLANNED, RunState.SUBMITTED, reason_code=tag
            )
            outcomes.append(f"win:{tag}")
        except StatusConflict:
            outcomes.append(f"conflict:{tag}")

    threads = [threading.Thread(target=submit, args=(t,)) for t in ("a", "b")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(o.split(":")[0] for o in outcomes) == ["conflict", "win"]
    assert store.load().revision == 1  # exactly one submission recorded


def test_status_history_is_bounded_without_losing_creation_or_current_state(tmp_path):
    store = StatusStore.deployment(tmp_path / "bounded.json")
    _walk_to_ready(store)
    transitions = 600
    state = DeploymentState.READY
    for _ in range(transitions):
        new = (
            DeploymentState.VALIDATING if state == DeploymentState.READY else DeploymentState.READY
        )
        store.transition(state, new, reason_code="FLAP")
        state = new
    record = store.load()
    assert len(record.history) == MAX_STATUS_HISTORY_EVENTS
    assert record.history[0]["state"] == DeploymentState.PLANNED.value
    assert record.history[-1]["state"] == record.state == state.value
    total_events = record.revision + 1
    assert record.data["status_history_events_dropped"] == (
        total_events - MAX_STATUS_HISTORY_EVENTS
    )


@pytest.mark.parametrize(
    ("method", "kwargs"),
    [
        ("initialize", {"record_id": "", "state": DeploymentState.PLANNED}),
        (
            "initialize",
            {
                "record_id": "d",
                "state": DeploymentState.PLANNED,
                "data": {"status_history_events_dropped": 0},
            },
        ),
    ],
)
def test_status_store_rejects_invalid_or_reserved_initial_fields(tmp_path, method, kwargs):
    store = StatusStore.deployment(tmp_path / f"{method}.json")
    with pytest.raises(ValueError):
        getattr(store, method)(**kwargs)


def test_status_store_rejects_inputs_that_would_poison_the_next_load(tmp_path):
    store = StatusStore.deployment(tmp_path / "strict.json")
    store.initialize("d", DeploymentState.PLANNED)
    with pytest.raises(ValueError, match="reason_code"):
        store.transition(DeploymentState.PLANNED, DeploymentState.STAGING, reason_code="")
    with pytest.raises(ValueError, match="detail"):
        store.transition(
            DeploymentState.PLANNED,
            DeploymentState.STAGING,
            reason_code="STEP",
            detail=7,
        )
    with pytest.raises(ValueError, match="expected_revision"):
        store.transition(
            DeploymentState.PLANNED,
            DeploymentState.STAGING,
            reason_code="STEP",
            expected_revision=True,
        )


def test_nonhistorical_updates_are_bounded_and_revision_accounted(tmp_path):
    store = StatusStore.deployment(tmp_path / "omitted.json")
    store.initialize("d", DeploymentState.PLANNED)
    for index in range(20):
        store.update(
            DeploymentState.PLANNED,
            reason_code="HEARTBEAT",
            data_update={"beat": index},
            record_history=False,
        )
    record = store.load()
    assert record.revision == 20
    assert len(record.history) == 1
    assert record.data["status_history_events_omitted"] == 20


@pytest.mark.parametrize("corruption", ["fabricated_ready", "illegal_edge", "bad_accounting"])
def test_load_rejects_shape_valid_but_fabricated_history(tmp_path, corruption):
    path = tmp_path / f"{corruption}.json"
    store = StatusStore.deployment(path)
    store.initialize("d", DeploymentState.PLANNED)
    raw = json.loads(path.read_text(encoding="utf-8"))
    if corruption == "fabricated_ready":
        raw["state"] = DeploymentState.READY.value
        raw["history"][0]["state"] = DeploymentState.READY.value
    elif corruption == "illegal_edge":
        raw["revision"] = 1
        raw["state"] = DeploymentState.READY.value
        raw["history"].append(
            {
                "state": DeploymentState.READY.value,
                "at": raw["updated_at"],
                "reason_code": "SKIP",
            }
        )
    else:
        raw["revision"] = 7
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(StatusConflict):
        store.load()
