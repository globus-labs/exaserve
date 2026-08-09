from __future__ import annotations

import json

import pytest

from exaserve.schedulers import (
    JobObservation,
    SchedulerState,
    Submission,
    SubmissionRejected,
    default_queue_and_walltime,
)
from exaserve.submit import serve_url, submit_serve


def _write_config(path):
    path.write_text(
        """
num_nodes: 1
validation_mode: true
local_stage_path: /tmp/aurora-models
models:
  - model_id: test/model
    tensor_parallel_size: 1
    max_model_len: 128
    size: 1
gateway:
  kind: haproxy
  port: 4001
""".lstrip(),
        encoding="utf-8",
    )


def test_submit_rejects_duplicate_yaml_before_compilation(tmp_path):
    config_path = tmp_path / "duplicate.yaml"
    config_path.write_text("num_nodes: 1\nnum_nodes: 2\n", encoding="utf-8")
    with pytest.raises(Exception, match="duplicate key 'num_nodes'"):
        submit_serve(config_path, dry_run=True)


def test_submit_serve_dry_run_persists_plan_and_renders_direct_python(tmp_path, monkeypatch):
    config_path = tmp_path / "deploy.yaml"
    _write_config(config_path)
    monkeypatch.setenv("EXASERVE_SCHEDULER", "pbs")

    log_dir = tmp_path / "pbs_logs"
    assert submit_serve(config_path, log_dir=log_dir, dry_run=True) == ""

    job_paths = list(log_dir.glob("submissions/*/job.pbs"))
    assert len(job_paths) == 1
    pbs_text = job_paths[0].read_text(encoding="utf-8")
    assert 'source "$HOME/script/env_aurora"' in pbs_text
    assert "EXASERVE_RUN_LOG_DIR=" in pbs_text
    assert "EXASERVE_SITE_PROFILE_PATH=" in pbs_text
    assert "-m exaserve.launcher" in pbs_text
    assert "deployment.plan.json" in pbs_text
    assert "deploy.yaml" not in pbs_text
    assert "launch_cluster.sh" not in pbs_text
    artifact_dir = job_paths[0].parent
    assert (artifact_dir / "deployment.plan.json").is_file()
    assert (artifact_dir / "site.profile.json").is_file()


def test_submit_refuses_unqualified_production_before_creating_artifacts(tmp_path, monkeypatch):
    config_path = tmp_path / "deploy.yaml"
    _write_config(config_path)
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace("validation_mode: true\n", ""),
        encoding="utf-8",
    )
    log_dir = tmp_path / "logs"
    monkeypatch.setenv("EXASERVE_SCHEDULER", "pbs")

    with pytest.raises(RuntimeError, match="production execution is not qualified"):
        submit_serve(config_path, log_dir=log_dir, dry_run=True)

    assert not log_dir.exists()


def test_default_submit_backend_is_release_profile_pbs(tmp_path, monkeypatch):
    monkeypatch.delenv("EXASERVE_SCHEDULER", raising=False)
    config_path = tmp_path / "deploy.yaml"
    _write_config(config_path)
    log_dir = tmp_path / "logs"
    assert submit_serve(config_path, log_dir=log_dir, dry_run=True) == ""
    assert len(list(log_dir.glob("submissions/*/job.pbs"))) == 1
    assert not list(log_dir.glob("submissions/*/job.psij.sh"))


@pytest.mark.parametrize("scheduler_name", ["slurm", "psij", "exawork"])
def test_submit_rejects_scheduler_not_qualified_by_release_site_profile(
    tmp_path, monkeypatch, scheduler_name
):
    config_path = tmp_path / "deploy.yaml"
    _write_config(config_path)
    monkeypatch.setenv("EXASERVE_SCHEDULER", scheduler_name)

    with pytest.raises(ValueError, match="not qualified by SiteProfile"):
        submit_serve(config_path, log_dir=tmp_path / "logs", dry_run=True)

    assert not (tmp_path / "logs").exists()


@pytest.mark.parametrize(
    ("nodes", "expected_queue", "expected_walltime"),
    [
        (1, "capacity", "01:00:00"),
        (2, "capacity", "01:00:00"),
    ],
)
def test_submit_defaults_follow_compiled_topology(
    tmp_path, monkeypatch, nodes, expected_queue, expected_walltime
):
    config_path = tmp_path / "deploy.yaml"
    _write_config(config_path)
    text = config_path.read_text(encoding="utf-8").replace("num_nodes: 1", f"num_nodes: {nodes}")
    config_path.write_text(text, encoding="utf-8")
    monkeypatch.delenv("EXASERVE_DEFAULT_QUEUE", raising=False)
    monkeypatch.delenv("EXASERVE_DEFAULT_WALLTIME", raising=False)

    log_dir = tmp_path / "logs"
    assert submit_serve(config_path, log_dir=log_dir, dry_run=True) == ""
    job_text = next(log_dir.glob("submissions/*/job.pbs")).read_text(encoding="utf-8")
    assert f"#PBS -q {expected_queue}" in job_text
    assert f"#PBS -l walltime={expected_walltime}" in job_text


@pytest.mark.parametrize(
    ("nodes", "expected"),
    [
        (1, ("capacity", "01:00:00")),
        (16, ("capacity", "01:00:00")),
        (17, ("debug-scaling", "01:00:00")),
        (255, ("debug-scaling", "01:00:00")),
        (256, ("prod", "02:00:00")),
    ],
)
def test_aurora_scheduler_defaults_cover_exact_queue_boundaries(nodes, expected):
    assert default_queue_and_walltime(nodes) == expected


@pytest.mark.parametrize("invalid", [True, 0, -1, 1.5, "1"])
def test_aurora_scheduler_defaults_reject_invalid_node_counts(invalid):
    with pytest.raises(ValueError, match="positive integer"):
        default_queue_and_walltime(invalid)


def test_submit_environment_overrides_topology_scheduler_defaults(tmp_path, monkeypatch):
    config_path = tmp_path / "deploy.yaml"
    _write_config(config_path)
    monkeypatch.setenv("EXASERVE_DEFAULT_QUEUE", "site-override")
    monkeypatch.setenv("EXASERVE_DEFAULT_WALLTIME", "03:04:05")

    log_dir = tmp_path / "logs"
    assert submit_serve(config_path, log_dir=log_dir, dry_run=True) == ""
    job_text = next(log_dir.glob("submissions/*/job.pbs")).read_text(encoding="utf-8")
    assert "#PBS -q site-override" in job_text
    assert "#PBS -l walltime=03:04:05" in job_text


class _FakeScheduler:
    name = "pbs"

    def __init__(self):
        self.submits = 0
        self.observation = JobObservation("123.test", SchedulerState.RUNNING)
        self.reconciled = ()
        self.reject = False

    def render_job(self, spec):
        return f"#!/bin/sh\n# {spec.run_identity}\n"

    def submit(self, _path):
        self.submits += 1
        if self.reject:
            raise SubmissionRejected("definite scheduler rejection")
        return Submission("123.test", "123.test")

    def find_by_run_identity(self, _identity):
        return self.reconciled

    def observe(self, _job_id):
        return self.observation


def test_submit_is_idempotent_and_registry_is_exact(tmp_path, monkeypatch):
    config_path = tmp_path / "deploy.yaml"
    _write_config(config_path)
    registry = tmp_path / "registry"
    monkeypatch.setenv("EXASERVE_SUBMISSION_REGISTRY", str(registry))
    scheduler = _FakeScheduler()
    monkeypatch.setattr("exaserve.submit.get_scheduler", lambda _name=None: scheduler)

    first = submit_serve(config_path, log_dir=tmp_path / "logs")
    second = submit_serve(config_path, log_dir=tmp_path / "logs")

    assert first == second == "123.test"
    assert scheduler.submits == 1
    records = list(registry.glob("*.json"))
    assert len(records) == 1
    record = json.loads(records[0].read_text(encoding="utf-8"))
    assert record["job_id"] == "123.test"
    assert record["phase"] == "SUBMITTED"
    assert (config_path.with_suffix(".jobid")).read_text().strip() == "123.test"


def test_submission_intent_is_canonical_plan_content_not_yaml_path(tmp_path, monkeypatch):
    first_path = tmp_path / "first.yaml"
    second_path = tmp_path / "second.yaml"
    _write_config(first_path)
    # Comments and the source filename are not serving semantics.
    second_path.write_text("# presentation-only comment\n" + first_path.read_text())
    monkeypatch.setenv("EXASERVE_SUBMISSION_REGISTRY", str(tmp_path / "registry"))
    scheduler = _FakeScheduler()
    monkeypatch.setattr("exaserve.submit.get_scheduler", lambda _name=None: scheduler)
    log_dir = tmp_path / "logs"

    assert submit_serve(first_path, log_dir=log_dir) == "123.test"
    assert submit_serve(second_path, log_dir=log_dir) == "123.test"
    assert scheduler.submits == 1
    assert len(list((log_dir / "submission_intents").glob("*.json"))) == 1


def test_prepared_intent_recovers_without_second_submit(tmp_path, monkeypatch):
    config_path = tmp_path / "deploy.yaml"
    _write_config(config_path)
    monkeypatch.setenv("EXASERVE_SUBMISSION_REGISTRY", str(tmp_path / "registry"))
    scheduler = _FakeScheduler()
    monkeypatch.setattr("exaserve.submit.get_scheduler", lambda _name=None: scheduler)

    import exaserve.submit as submit_module

    original_publish = submit_module._publish_submission_record
    failed = False

    def fail_after_scheduler(payload, *, intent_path):
        nonlocal failed
        if payload["phase"] == "SUBMITTED" and not failed:
            failed = True
            raise OSError("injected state-write failure")
        return original_publish(payload, intent_path=intent_path)

    monkeypatch.setattr(submit_module, "_publish_submission_record", fail_after_scheduler)
    with pytest.raises(OSError, match="injected"):
        submit_serve(config_path, log_dir=tmp_path / "logs")
    assert scheduler.submits == 1

    # A temporarily invisible exact job is not evidence that the scheduler
    # rejected the first request. The durable SUBMITTING intent must prevent a
    # second submission.
    monkeypatch.setattr(submit_module, "_publish_submission_record", original_publish)
    with pytest.raises(RuntimeError, match="absence is not proof"):
        submit_serve(config_path, log_dir=tmp_path / "logs")
    assert scheduler.submits == 1

    scheduler.reconciled = (JobObservation("123.test", SchedulerState.RUNNING),)
    assert submit_serve(config_path, log_dir=tmp_path / "logs") == "123.test"
    assert scheduler.submits == 1


def test_definite_rejection_is_the_only_safe_retry_path(tmp_path, monkeypatch):
    config_path = tmp_path / "deploy.yaml"
    _write_config(config_path)
    monkeypatch.setenv("EXASERVE_SUBMISSION_REGISTRY", str(tmp_path / "registry"))
    scheduler = _FakeScheduler()
    scheduler.reject = True
    monkeypatch.setattr("exaserve.submit.get_scheduler", lambda _name=None: scheduler)

    with pytest.raises(SubmissionRejected, match="definite"):
        submit_serve(config_path, log_dir=tmp_path / "logs")
    intent = next((tmp_path / "logs" / "submission_intents").glob("*.json"))
    assert json.loads(intent.read_text(encoding="utf-8"))["phase"] == "REJECTED"

    scheduler.reject = False
    assert submit_serve(config_path, log_dir=tmp_path / "logs") == "123.test"
    assert scheduler.submits == 2


def test_serve_url_requires_canonical_ready_status(tmp_path, monkeypatch):
    monkeypatch.setenv("EXASERVE_SUBMISSION_REGISTRY", str(tmp_path / "registry"))
    scheduler = _FakeScheduler()
    monkeypatch.setattr("exaserve.submit.get_scheduler", lambda _name=None: scheduler)
    record = {
        "schema_version": 2,
        "phase": "SUBMITTED",
        "intent_id": "b" * 64,
        "run_identity": "es-123",
        "scheduler": "pbs",
        "job_id": "123.test",
        "deployment_id": "dep",
        "generation": 7,
        "deployment_plan_hash": "a" * 64,
        "run_dir": str(tmp_path / "run"),
        "plan_path": "/plan",
        "site_profile_path": "/site",
        "job_script_path": "/job",
        "submitted_at": "now",
    }
    import exaserve.submit as submit_module

    submit_module._publish_submission_record(record, intent_path=tmp_path / "intent.json")
    monkeypatch.setattr(
        "exaserve.status_api.require_ready_endpoint",
        lambda run_dir, **identity: (
            "http://advertised:4001"
            if run_dir == record["run_dir"] and identity["expected_generation"] == 7
            else None
        ),
    )
    assert serve_url("123.test") == "http://advertised:4001"


def test_serve_url_never_infers_from_running_scheduler(tmp_path, monkeypatch):
    monkeypatch.setenv("EXASERVE_SUBMISSION_REGISTRY", str(tmp_path / "registry"))
    scheduler = _FakeScheduler()
    monkeypatch.setattr("exaserve.submit.get_scheduler", lambda _name=None: scheduler)
    with pytest.raises(RuntimeError, match="no canonical submission registry"):
        serve_url("unregistered.test")
