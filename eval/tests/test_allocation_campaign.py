"""Hermetic safety checks for the finite physical-allocation adapter.

These tests never submit jobs, start MPI, or import a serving engine. Live
qualification is a separate, mandatory gate before production admission.
"""

from __future__ import annotations

import copy
import json
import signal
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from eval.cli import build_parser
from eval.lib import allocation_campaign as campaign
from exaserve.schedulers import SubmissionAmbiguous, SubmissionRejected
from exaserve.plan.contracts import SchedulerPlan
from eval.lib.utils import dataclass_to_dict


def test_cli_campaign_execution_requires_exact_artifact():
    args = build_parser().parse_args(["allocation", "execute", "/tmp/campaign.json"])
    assert args.area == "allocation"
    assert args.command == "execute"
    with pytest.raises(SystemExit):
        build_parser().parse_args(["allocation", "execute"])


@pytest.mark.parametrize(
    "payload",
    [
        "[]",
        "null",
        '{"schema_version":1,"schema_version":2}',
        '{"content_hash":"' + "0" * 64 + '"}',
        '{"schema_version":true,"content_hash":"' + "0" * 64 + '"}',
    ],
)
def test_campaign_loader_rejects_unbound_or_malformed_artifact(tmp_path, payload):
    path = tmp_path / "campaign.json"
    path.write_text(payload, encoding="utf-8")
    with pytest.raises((RuntimeError, ValueError)):
        campaign._load_campaign(str(path))


def test_campaign_loader_rejects_symlink(tmp_path):
    real = tmp_path / "real.json"
    real.write_text(json.dumps({"schema_version": 1}), encoding="utf-8")
    link = tmp_path / "campaign.json"
    link.symlink_to(real)
    with pytest.raises((RuntimeError, ValueError, OSError)):
        campaign._load_campaign(str(link))


def test_qualification_config_has_a_real_cancellation_window():
    import yaml

    root = Path(__file__).resolve().parents[2]
    config = yaml.safe_load(
        (
            root / "eval/specs/sc26workshop/smokes/nullcompute_haproxy_subset_cancel_2node.yaml"
        ).read_text(encoding="utf-8")
    )
    assert config["deployment"]["num_nodes"] == 2
    assert config["client"]["startup_only"] is False
    assert config["client"]["num_runs"] == 2
    assert config["workload"]["duration"] >= 60
    assert config["backend"]["args"]["ray"]["launch"]["null_compute"] is True
    assert config["backend"]["args"]["ray"]["launch"]["clean_stage"] is True


def test_subset_keeps_native_head_and_exact_rank_order():
    nodes = ["head.aurora", "worker1", "worker2", "excluded"]
    assert campaign._select_subset(nodes, 2, "HEAD") == nodes[:2]
    assert nodes == ["head.aurora", "worker1", "worker2", "excluded"]


@pytest.mark.parametrize("size", [True, False, 0, -1, 2.0, "2", 4, 5])
def test_subset_rejects_invalid_or_non_subset_sizes(size):
    with pytest.raises(ValueError):
        campaign._select_subset(["head", "n1", "n2", "n3"], size, "head")


@pytest.mark.parametrize(
    "nodes,head",
    [
        (["head", "n1", "n2", "n3"], "n1"),
        (["head", "n1", "n2", "n3"], "outside"),
        (["head", "HEAD.aurora", "n2", "n3"], "head"),
        (["head", "", "n2", "n3"], "head"),
        (["", "n1", "n2", "n3"], ""),
    ],
)
def test_subset_rejects_non_head_execution_and_ambiguous_nodes(nodes, head):
    with pytest.raises(ValueError):
        campaign._select_subset(nodes, 2, head)


def test_child_environment_preserves_native_job_and_pals_but_not_controller_code(monkeypatch):
    monkeypatch.setenv("PBS_JOBID", "123.aurora")
    monkeypatch.setenv("PBS_NODEFILE", "/native/physical.nodes")
    monkeypatch.setenv("PALS_TEST_BOUNDARY", "original-native-value")
    monkeypatch.setenv("PYTHONPATH", "/controller:/dirty-checkout")
    monkeypatch.setenv("EXASERVE_GENERATION", "stale-generation")
    monkeypatch.setenv("VIRTUAL_ENV", "/wrong/venv")
    monkeypatch.delenv("ONEAPI_DEVICE_SELECTOR", raising=False)
    monkeypatch.delenv("SLURM_JOB_ID", raising=False)
    child = {"snapshot_root": "/sealed/child"}
    env = campaign._child_environment(child, "/tmp/subset.nodes", "123.aurora")
    assert env["PBS_JOBID"] == "123.aurora"
    assert env["PALS_TEST_BOUNDARY"] == "original-native-value"
    assert env["PBS_NODEFILE"] == env["EXASERVE_NODEFILE"] == "/tmp/subset.nodes"
    assert env["PYTHONPATH"] == "/sealed/child:/sealed/child/src"
    assert env["PYTHONNOUSERSITE"] == env["PYTHONSAFEPATH"] == "1"
    assert "EXASERVE_GENERATION" not in env
    assert "VIRTUAL_ENV" not in env
    import os

    assert os.environ["PBS_NODEFILE"] == "/native/physical.nodes"
    assert os.environ["PYTHONPATH"] == "/controller:/dirty-checkout"


def test_child_environment_rejects_fabricated_pbs_identity(monkeypatch):
    monkeypatch.setenv("PBS_JOBID", "123.aurora")
    with pytest.raises(ValueError, match="PBS_JOBID"):
        campaign._child_environment({"snapshot_root": "/sealed"}, "/tmp/nodes", "456.aurora")


@pytest.mark.parametrize("name,value", [("ONEAPI_DEVICE_SELECTOR", "bad"), ("SLURM_JOB_ID", "9")])
def test_child_environment_rejects_conflicting_runtime(monkeypatch, name, value):
    monkeypatch.setenv("PBS_JOBID", "123.aurora")
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError):
        campaign._child_environment({"snapshot_root": "/sealed"}, "/tmp/nodes", "123.aurora")


@pytest.fixture
def submit_fixture(tmp_path, monkeypatch):
    value = {"root": str(tmp_path), "campaign_hash": "a" * 64, "children": []}
    state_path = tmp_path / "campaign.state.json"
    state_path.write_text(
        json.dumps({"campaign_hash": "a" * 64, "phase": "PREPARED", "job_id": ""}),
        encoding="utf-8",
    )
    monkeypatch.setattr(campaign, "_load_campaign", lambda path: value)
    monkeypatch.setattr(
        campaign, "_job_spec", lambda value: SimpleNamespace(run_identity="ac-exact")
    )
    fake = SimpleNamespace(submissions=0, matches=[], failure=None)

    def submit(path):
        fake.submissions += 1
        # Submission intent must be durable before any external qsub action.
        assert json.loads(state_path.read_text())["phase"] == "SUBMITTING"
        if fake.failure is not None:
            raise fake.failure
        return SimpleNamespace(job_id="123.aurora")

    fake.submit = submit
    fake.find_by_run_identity = lambda identity: fake.matches
    monkeypatch.setattr(campaign, "get_scheduler", lambda name: fake)
    return str(tmp_path / "campaign.json"), state_path, fake


def test_submit_is_idempotent_and_records_intent_first(submit_fixture):
    path, state_path, scheduler = submit_fixture
    assert campaign.submit_campaign(path) == "123.aurora"
    assert campaign.submit_campaign(path) == "123.aurora"
    assert scheduler.submissions == 1
    state = json.loads(state_path.read_text())
    assert state["phase"] == "SUBMITTED"
    assert state["job_id"] == "123.aurora"


def test_ambiguous_submit_stays_fenced_then_reconciles_exactly_once(submit_fixture):
    path, state_path, scheduler = submit_fixture
    scheduler.failure = SubmissionAmbiguous("qsub response unavailable")
    with pytest.raises(SubmissionAmbiguous):
        campaign.submit_campaign(path)
    assert json.loads(state_path.read_text())["phase"] == "SUBMITTING"
    scheduler.failure = None
    with pytest.raises(RuntimeError, match="ambiguous"):
        campaign.submit_campaign(path)
    assert scheduler.submissions == 1
    scheduler.matches = [SimpleNamespace(job_id="123.aurora")]
    assert campaign.submit_campaign(path) == "123.aurora"
    assert scheduler.submissions == 1


def test_ambiguous_submit_rejects_duplicate_scheduler_matches(submit_fixture):
    path, state_path, scheduler = submit_fixture
    state = json.loads(state_path.read_text())
    state["phase"] = "SUBMITTING"
    state_path.write_text(json.dumps(state), encoding="utf-8")
    scheduler.matches = [SimpleNamespace(job_id="123.aurora"), SimpleNamespace(job_id="456.aurora")]
    with pytest.raises(RuntimeError, match="ambiguous"):
        campaign.submit_campaign(path)
    assert scheduler.submissions == 0
    assert json.loads(state_path.read_text())["phase"] == "SUBMITTING"


def test_definite_scheduler_rejection_is_not_ambiguous(submit_fixture):
    path, state_path, scheduler = submit_fixture
    scheduler.failure = SubmissionRejected("queue limit reached")
    with pytest.raises(SubmissionRejected):
        campaign.submit_campaign(path)
    assert json.loads(state_path.read_text())["phase"] == "REJECTED"
    scheduler.failure = None
    assert campaign.submit_campaign(path) == "123.aurora"
    assert scheduler.submissions == 2


def test_owned_executor_stop_uses_interrupt_and_full_grace(monkeypatch):
    from eval.lib.backends import base

    signals = []
    waits = []
    process = SimpleNamespace(
        pid=12345, poll=lambda: None, wait=lambda timeout: waits.append(timeout)
    )
    monkeypatch.setattr(campaign.os, "killpg", lambda pid, sig: signals.append((pid, sig)))
    monkeypatch.setattr(base, "process_group_exists", lambda pid: False)
    campaign._owned_stop(process, grace_s=930)
    assert signals == [(12345, signal.SIGINT)]
    assert waits == [930]


def test_owned_executor_timeout_is_not_successful_cleanup(monkeypatch):
    from eval.lib.backends import base

    forced = []

    def wait(timeout):
        raise subprocess.TimeoutExpired("executor", timeout)

    process = SimpleNamespace(pid=12345, poll=lambda: None, wait=wait)
    monkeypatch.setattr(campaign.os, "killpg", lambda pid, sig: None)
    monkeypatch.setattr(
        base, "terminate_process_tree", lambda *args, **kwargs: forced.append(kwargs)
    )
    with pytest.raises(RuntimeError, match="cleanup"):
        campaign._owned_stop(process, grace_s=930)
    assert len(forced) == 1
    assert forced[0]["process_group"] == 12345


def test_duplicate_controller_cannot_publish_failure_for_another_owner(tmp_path, monkeypatch):
    from contextlib import contextmanager

    root = Path(campaign.__file__).resolve().parents[2]
    value = {"root": str(tmp_path), "controller": {"snapshot_root": str(root)}}
    writes = []
    monkeypatch.setattr(campaign, "_load_campaign", lambda path: value)
    monkeypatch.setattr(campaign, "_write_state", lambda *args, **kwargs: writes.append(kwargs))

    @contextmanager
    def unavailable_lease(*args, **kwargs):
        raise RuntimeError("another executor owns this campaign")
        yield  # pragma: no cover - makes this a context manager without entering it

    monkeypatch.setattr(campaign, "ExclusiveLease", unavailable_lease)
    with pytest.raises(RuntimeError, match="another executor"):
        campaign.execute_campaign(str(tmp_path / "campaign.json"))
    assert writes == []


def test_qualification_cannot_be_manufactured_from_pass_flags(tmp_path):
    report = {
        "schema_version": 1,
        "campaign_hash": "a" * 64,
        "controller_source_snapshot_hash": "b" * 64,
        "child_source_snapshot_hash": "c" * 64,
        "acquisition": {},
        "mpi_proof": {"passed": True},
        "children": [
            {"outcome": "SUCCEEDED", "evidence": {}},
            {"outcome": "CANCELLED_AFTER_READY", "evidence": {}},
        ],
        "sentinel": {"survived_cleanup": True, "reaped": True},
        "process_audit": {"clean": True},
        "verdict": "PASS",
        "completed_at": "2026-09-09T00:00:00+00:00",
    }
    path = tmp_path / "qualification.json"
    path.write_text(json.dumps(report), encoding="utf-8")
    reference = {"path": str(path), "sha256": campaign._sha(path)}
    with pytest.raises((ValueError, RuntimeError, OSError)):
        campaign._validate_qualification(
            reference,
            {"source_snapshot_hash": "b" * 64},
            [{"source_snapshot_hash": "c" * 64}],
        )


def test_failed_child_prevents_remaining_children_from_launching(tmp_path, monkeypatch):
    from exaserve import source_staging
    from exaserve.plan import io

    source_root = Path(campaign.__file__).resolve().parents[2]
    scratch = tmp_path / "local"
    scratch.mkdir()
    children = [{"logical_nodes": n, "deployment_id": f"deployment-{n}"} for n in (1, 2, 3)]
    value = {
        "root": str(tmp_path),
        "campaign_hash": "a" * 64,
        "controller": {"snapshot_root": str(source_root)},
        "qualification_mode": False,
        "children": children,
    }
    writes = []
    launched = []
    audited = []
    monkeypatch.setattr(campaign, "_load_campaign", lambda path: value)
    monkeypatch.setattr(campaign, "_write_state", lambda *args, **kwargs: writes.append(kwargs))
    monkeypatch.setattr(
        campaign,
        "_validate_allocation",
        lambda *args, **kwargs: (
            {"job_id": "123.aurora"},
            ["head", "n1", "n2", "n3"],
            time.monotonic() + 3600,
        ),
    )
    monkeypatch.setattr(
        campaign,
        "_validate_child_inputs",
        lambda child: SimpleNamespace(site_profile_path="unused"),
    )
    monkeypatch.setattr(campaign, "_child_environment", lambda *args: {})
    monkeypatch.setattr(campaign, "_audit_paths", lambda *args: ["/tmp/owned-generation"])
    monkeypatch.setattr(campaign.socket, "gethostname", lambda: "head")
    monkeypatch.setattr(io, "load_site_profile", lambda path: None)
    monkeypatch.setattr(source_staging, "qualify_runtime_staging_base", lambda *args: scratch)

    def run_child(*args):
        index = args[2]
        launched.append(index)
        if index == 1:
            raise RuntimeError("second child failed its cleanup gate")
        return {"outcome": "SUCCEEDED"}

    def audit(*args, **kwargs):
        audited.append(True)
        return {"clean": True}

    monkeypatch.setattr(campaign, "_run_child", run_child)
    monkeypatch.setattr(campaign, "_process_audit", audit)
    with pytest.raises(RuntimeError, match="second child"):
        campaign.execute_campaign(str(tmp_path / "campaign.json"))
    assert launched == [0, 1]
    assert audited == [True]
    assert [entry["phase"] for entry in writes] == ["RUNNING", "FAILED"]


@pytest.fixture
def qualification_contract(tmp_path):
    children = []
    for index in range(2):
        children.append(
            {
                "run_yaml": str(tmp_path / f"child-{index}/run.yaml"),
                "snapshot_root": str(tmp_path / "frozen"),
                "source_snapshot_hash": "a" * 64,
                "run_semantic_hash": str(index + 1) * 64,
                "deployment_plan_hash": str(index + 3) * 64,
                "deployment_id": f"child-{index}",
                "logical_nodes": 2,
                "null_compute": True,
                "inputs": {str(tmp_path / f"input-{index}-{j}"): "d" * 64 for j in range(8)},
            }
        )
    return {
        "schema_version": 1,
        "root": str(tmp_path),
        "qualification_mode": True,
        "qualification": None,
        "cleanup_reserve_s": 900,
        "child_timeout_s": 480,
        "scheduler": dataclass_to_dict(
            SchedulerPlan(
                type="pbs",
                nodes=4,
                queue="capacity",
                walltime="01:00:00",
                account="AuroraGPT",
                filesystem_refs=("home", "flare"),
                launcher="mpi",
            )
        ),
        "controller": {
            "snapshot_root": str(tmp_path / "controller"),
            "source_snapshot_hash": "b" * 64,
            "commit": "c" * 40,
            "module_sha256": "d" * 64,
            "excluded_dirty_files": [],
        },
        "children": children,
    }


def test_small_qualification_contract_is_admitted(qualification_contract):
    campaign._validate_contract(qualification_contract)


@pytest.mark.parametrize(
    "field,value",
    [("schema_version", True), ("qualification_mode", 1), ("cleanup_reserve_s", 900.0)],
)
def test_campaign_contract_types_are_not_coerced(qualification_contract, field, value):
    qualification_contract[field] = value
    with pytest.raises(ValueError):
        campaign._validate_contract(qualification_contract)


@pytest.mark.parametrize("timeout", [True, float("inf"), float("nan"), 0, -1, 900])
def test_campaign_budget_is_finite_and_reserves_every_child_cleanup(
    qualification_contract, timeout
):
    qualification_contract["child_timeout_s"] = timeout
    with pytest.raises(ValueError):
        campaign._validate_contract(qualification_contract)


@pytest.mark.parametrize("field", ["run_yaml", "deployment_id"])
def test_campaign_cannot_alias_child_ownership(qualification_contract, field):
    children = qualification_contract["children"]
    children[1][field] = children[0][field]
    with pytest.raises(ValueError, match="distinct"):
        campaign._validate_contract(qualification_contract)


def test_campaign_cannot_mix_frozen_child_sources(qualification_contract):
    qualification_contract["children"][1]["source_snapshot_hash"] = "e" * 64
    with pytest.raises(ValueError, match="source"):
        campaign._validate_contract(qualification_contract)


def test_campaign_cannot_smuggle_scheduler_policy(qualification_contract):
    qualification_contract["scheduler"]["reservation_topology"] = "implicit-subset"
    with pytest.raises(ValueError, match="policy"):
        campaign._validate_contract(qualification_contract)


def test_rehashed_artifact_still_must_obey_schema_and_budget(tmp_path, qualification_contract):
    value = copy.deepcopy(qualification_contract)
    value.update(schema_version=True, created_at="2026-09-09T00:00:00+00:00")
    value["campaign_hash"] = campaign.canonical_hash(value)
    path = tmp_path / "campaign.json"
    path.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(ValueError, match="schema"):
        campaign._load_campaign(str(path))


@pytest.mark.parametrize("exit_value", [None, "", "1", "-20", 0, False])
def test_qualification_native_completion_requires_explicit_zero(monkeypatch, exit_value):
    record = {
        "job_id": "123.aurora",
        "Job_Name": "ac-exact",
        "job_state": "F",
        "Exit_status": exit_value,
    }
    scheduler = SimpleNamespace(_query=lambda *args: [record])
    monkeypatch.setattr(campaign, "get_scheduler", lambda name: scheduler)
    with pytest.raises(ValueError, match="exit status zero"):
        campaign._require_pbs_zero_exit("123.aurora", "ac-exact")


def test_qualification_native_completion_accepts_only_exact_job(monkeypatch):
    record = {
        "job_id": "123.aurora",
        "Job_Name": "ac-exact",
        "job_state": "F",
        "Exit_status": "0",
    }
    scheduler = SimpleNamespace(_query=lambda *args: [record])
    monkeypatch.setattr(campaign, "get_scheduler", lambda name: scheduler)
    assert campaign._require_pbs_zero_exit("123.aurora", "ac-exact") == record
    with pytest.raises(ValueError):
        campaign._require_pbs_zero_exit("456.aurora", "ac-exact")


def test_publication_guard_requires_live_lease_and_no_cancellation():
    checked = []
    heartbeat = SimpleNamespace(ensure_held=lambda: checked.append(True))
    campaign._campaign_guard([False], heartbeat, time.monotonic() + 30)
    assert checked
    with pytest.raises(RuntimeError):
        campaign._campaign_guard([True], heartbeat, time.monotonic() + 30)
    with pytest.raises(RuntimeError):
        campaign._campaign_guard([False], heartbeat, time.monotonic() - 1)


def test_publication_guard_rejects_lost_parent_ownership():
    def lost():
        raise RuntimeError("parent lease was lost")

    with pytest.raises(RuntimeError, match="lease was lost"):
        campaign._campaign_guard([False], SimpleNamespace(ensure_held=lost), time.monotonic() + 30)


@pytest.fixture
def sentinel_fixture(tmp_path):
    token = "--exaserve-subset-sentinel=" + "a" * 64
    heartbeat = {
        "time": time.time(),
        "host": "excluded.aurora",
        "pid": 4242,
        "token": token,
        "sequence": 1,
    }
    log = tmp_path / "sentinel.log"
    log.write_text(json.dumps(heartbeat) + "\n", encoding="utf-8")
    kwargs = {
        "expected_host": "excluded",
        "token": token,
        "stop_requested": [False],
        "heartbeat": SimpleNamespace(ensure_held=lambda: None),
        "deadline": time.monotonic() + 30,
        "timeout_s": 0.02,
    }
    return SimpleNamespace(poll=lambda: None), log, heartbeat, kwargs


def test_sentinel_startup_requires_a_positive_matching_heartbeat(sentinel_fixture):
    process, log, heartbeat, kwargs = sentinel_fixture
    witness = campaign._sentinel_witness(process, log, **kwargs)
    assert witness["heartbeat"] == heartbeat
    assert witness["transcript_bytes"] == log.stat().st_size


@pytest.mark.parametrize(
    "field,value",
    [("host", "wrong-node"), ("pid", False), ("time", 0), ("token", "wrong-campaign")],
)
def test_sentinel_does_not_accept_stale_or_wrong_identity(sentinel_fixture, field, value):
    process, log, heartbeat, kwargs = sentinel_fixture
    heartbeat[field] = value
    log.write_text(json.dumps(heartbeat) + "\n", encoding="utf-8")
    with pytest.raises((RuntimeError, ValueError)):
        campaign._sentinel_witness(process, log, **kwargs)


def test_sentinel_survival_requires_same_process_after_cleanup(sentinel_fixture):
    process, log, heartbeat, kwargs = sentinel_fixture
    original = dict(heartbeat)
    heartbeat["pid"] += 1
    heartbeat["sequence"] += 1
    log.write_text(json.dumps(heartbeat) + "\n", encoding="utf-8")
    with pytest.raises((RuntimeError, ValueError)):
        campaign._sentinel_witness(process, log, expected_identity=original, **kwargs)


def test_sentinel_survival_requires_a_post_cleanup_heartbeat(sentinel_fixture):
    process, log, heartbeat, kwargs = sentinel_fixture
    with pytest.raises((RuntimeError, ValueError)):
        campaign._sentinel_witness(
            process, log, expected_identity=heartbeat, after_time=time.time() + 10, **kwargs
        )


def test_exited_sentinel_does_not_attest_survival(sentinel_fixture):
    process, log, heartbeat, kwargs = sentinel_fixture
    process.poll = lambda: 0
    with pytest.raises(RuntimeError):
        campaign._sentinel_witness(process, log, **kwargs)


@pytest.fixture
def cancellation_checkpoint(monkeypatch):
    from exaserve.state.status import StatusStore

    plan = SimpleNamespace(
        run_group_id="run0",
        run_id="n2",
        run_semantic_hash="a" * 64,
        deployment_plan_hash="b" * 64,
        source_snapshot_hash="c" * 64,
        bundle=SimpleNamespace(state_path="unused"),
    )
    record = SimpleNamespace(
        record_id="run0/n2",
        state="RUNNING",
        data={"phase": "running"},
        provenance={
            "run_id": "n2",
            "run_group_id": "run0",
            "run_semantic_hash": "a" * 64,
            "deployment_plan_hash": "b" * 64,
            "source_snapshot_hash": "c" * 64,
        },
    )
    monkeypatch.setattr(StatusStore, "run", lambda path: SimpleNamespace(load=lambda: record))
    return plan, record


def test_ready_alone_does_not_race_executor_cancellation(cancellation_checkpoint):
    plan, record = cancellation_checkpoint
    status = SimpleNamespace(ready=True)
    assert campaign._can_cancel_after_ready(plan, status) is False
    record.data["phase"] = "replaying"
    assert campaign._can_cancel_after_ready(plan, status) is True
    assert campaign._can_cancel_after_ready(plan, SimpleNamespace(ready=False)) is False


def test_cancellation_checkpoint_rejects_stale_run_identity(cancellation_checkpoint):
    plan, record = cancellation_checkpoint
    record.record_id = "another-run/n2"
    with pytest.raises(RuntimeError, match="identity"):
        campaign._can_cancel_after_ready(plan, SimpleNamespace(ready=True))


@pytest.mark.parametrize("error", ["cleanup failed", "", None])
def test_cancellation_never_hides_a_recorded_cleanup_error(cancellation_checkpoint, error):
    plan, record = cancellation_checkpoint
    record.state = "CANCELLED"
    campaign._require_clean_cancelled_status(plan, record)
    record.data["cleanup_error"] = error
    with pytest.raises(RuntimeError, match="clean canonical"):
        campaign._require_clean_cancelled_status(plan, record)
