"""WP2.3 acceptance: status lifecycle, CAS conflicts, illegal transitions."""

from __future__ import annotations

import threading

import pytest

from exaserve.state.status import (
    DeploymentState,
    IllegalTransition,
    RunState,
    StatusConflict,
    StatusStore,
)


def _walk_to_ready(store: StatusStore) -> None:
    store.initialize("dep-1", DeploymentState.PLANNED)
    chain = [DeploymentState.PLANNED, DeploymentState.STAGING,
             DeploymentState.CLUSTER_STARTING, DeploymentState.DEPLOYING,
             DeploymentState.VALIDATING, DeploymentState.READY]
    for current, nxt in zip(chain, chain[1:]):
        store.transition(current, nxt, reason_code="STEP")


def test_deployment_lifecycle_and_history(tmp_path):
    store = StatusStore.deployment(tmp_path / "deployment_status.json")
    _walk_to_ready(store)
    record = store.load()
    assert record.state == "READY" and record.revision == 5
    assert [h["state"] for h in record.history][0] == "PLANNED"

    # Post-READY loss revalidates; READY is not a latch (plan WP5.1/ADR-002).
    store.transition(DeploymentState.READY, DeploymentState.VALIDATING,
                     reason_code="REPLICA_LOST")
    # Terminal FAILED reachable from active state.
    store.transition(DeploymentState.VALIDATING, DeploymentState.FAILED,
                     reason_code="DEADLINE")
    with pytest.raises(IllegalTransition):
        store.transition(DeploymentState.FAILED, DeploymentState.READY,
                         reason_code="NOPE")


def test_illegal_skip_and_stale_expectation(tmp_path):
    store = StatusStore.deployment(tmp_path / "s.json")
    store.initialize("dep-2", DeploymentState.PLANNED)
    with pytest.raises(IllegalTransition):
        store.transition(DeploymentState.PLANNED, DeploymentState.READY,
                         reason_code="SKIP")
    store.transition(DeploymentState.PLANNED, DeploymentState.STAGING,
                     reason_code="STEP")
    with pytest.raises(StatusConflict):  # caller's view is stale
        store.transition(DeploymentState.PLANNED, DeploymentState.STAGING,
                         reason_code="STALE")


def test_run_lifecycle_distinguishes_partial_and_invalid(tmp_path):
    store = StatusStore.run(tmp_path / "run_status.json")
    store.initialize("run-1", RunState.PLANNED)
    store.transition(RunState.PLANNED, RunState.SUBMITTED, reason_code="QSUB",
                     data_update={"scheduler_job_id": "123.aurora"})
    store.transition(RunState.SUBMITTED, RunState.RUNNING, reason_code="R")
    store.transition(RunState.RUNNING, RunState.PARTIAL,
                     reason_code="MISSING_SHARDS",
                     data_update={"collected_ranks": 3, "expected_ranks": 4})
    record = store.load()
    assert record.state == "PARTIAL"
    assert record.data["scheduler_job_id"] == "123.aurora"  # durable job identity
    with pytest.raises(IllegalTransition):  # PARTIAL is terminal
        store.transition(RunState.PARTIAL, RunState.SUCCEEDED, reason_code="NO")


def test_concurrent_cas_exactly_one_winner(tmp_path):
    store = StatusStore.run(tmp_path / "run_status.json")
    store.initialize("run-2", RunState.PLANNED)
    outcomes: list[str] = []
    barrier = threading.Barrier(2)

    def submit(tag: str) -> None:
        barrier.wait()
        try:
            StatusStore.run(tmp_path / "run_status.json").transition(
                RunState.PLANNED, RunState.SUBMITTED, reason_code=tag)
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
