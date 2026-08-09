"""PR-013 acceptance: submit-all idempotency + fail-closed lock (hermetic)."""

from __future__ import annotations

import json
import os
import time

import pytest

from eval.lib.run_executor import _is_completed


def _make_run(group_dir, name, status=None, job_id=None, with_result=False):
    run_dir = os.path.join(group_dir, name)
    os.makedirs(os.path.join(run_dir, "state"), exist_ok=True)
    # A run.yaml must exist for discovery to consider it.
    open(os.path.join(run_dir, "run.yaml"), "w").close()
    if status is not None:
        payload = {
            "schema_version": 1,
            "kind": "run",
            "record_id": name,
            "state": status.upper(),
            "revision": 0,
            "updated_at": time.time(),
            "reason_code": "TEST",
            "detail": None,
            "provenance": {},
            "data": {},
            "history": [],
        }
        if job_id:
            payload["data"]["scheduler_job_id"] = job_id
        with open(os.path.join(run_dir, "state", "status.json"), "w") as fh:
            json.dump(payload, fh)
    if with_result:
        os.makedirs(os.path.join(run_dir, "results"), exist_ok=True)
        open(os.path.join(run_dir, "results", "result0.json"), "w").close()
    return run_dir


def test_submitted_run_is_skipped_not_resubmitted(tmp_path):
    g = str(tmp_path)
    submitted = _make_run(g, "1-nodes", status="submitted", job_id="123.aurora")
    running = _make_run(g, "2-nodes", status="running", job_id="124.aurora")
    assert _is_completed(submitted) is True  # PR-013: in-flight -> skip
    assert _is_completed(running) is True


def test_failed_runs_require_explicit_resubmission(tmp_path):
    g = str(tmp_path)
    planned = _make_run(g, "1-nodes", status=None)  # never submitted
    failed = _make_run(g, "2-nodes", status="failed")  # eligible to retry
    assert _is_completed(planned) is False
    assert _is_completed(failed) is True


def test_succeeded_without_results_is_not_complete(tmp_path):
    g = str(tmp_path)
    run = _make_run(g, "1-nodes", status="succeeded", with_result=False)
    assert _is_completed(run) is False  # status alone is insufficient


def test_unreadable_state_blocks_discovery_instead_of_resubmitting(tmp_path):
    run = _make_run(str(tmp_path), "ambiguous", status=None)
    state_path = os.path.join(run, "state", "status.json")
    with open(state_path, "w", encoding="utf-8") as handle:
        handle.write('{"state":"RUNNING","state":"PLANNED"}')
    with pytest.raises(RuntimeError, match="refusing submit discovery"):
        _is_completed(run)
