"""PR-015 acceptance: scheduler job-script fields reject injection (hermetic)."""

from __future__ import annotations

from pathlib import Path
import subprocess

import pytest

from exaserve.control.finite_process import FiniteProcessTimeout
from exaserve.schedulers.base import (
    AllocationMetadata,
    JobSpec,
    JobObservation,
    SchedulerState,
    SubmissionAmbiguous,
    SubmissionRejected,
    run_cmd,
    run_submission_cmd,
)


def _spec(**over):
    base = dict(
        num_nodes=1,
        walltime="01:00:00",
        account="AuroraGPT",
        job_name="exaserve-serve",
        stdout_dir=Path("/tmp/logs"),
        stderr_dir=Path("/tmp/logs"),
        queue="debug",
        command_argv=("python3", "-m", "exaserve.launcher", "/tmp/c.json"),
    )
    base.update(over)
    return JobSpec(**base)


def test_valid_spec_ok():
    _spec()  # should not raise


def test_scheduler_spec_snapshots_mutable_environment_and_paths():
    environment = {"EXASERVE_MODE": "planned"}
    spec = _spec(environment=environment, pythonpath=("/one", "/two"))
    environment["EXASERVE_MODE"] = "mutated"
    assert spec.environment["EXASERVE_MODE"] == "planned"
    assert spec.pythonpath == (Path("/one"), Path("/two"))
    with pytest.raises(TypeError):
        spec.environment["EXASERVE_MODE"] = "mutated"  # type: ignore[index]


def test_scheduler_unsets_site_environment_after_bootstrap():
    from exaserve.schedulers.pbs import PBSScheduler

    script = PBSScheduler().render_job(
        _spec(
            source_env_script=Path("/tmp/env_aurora"),
            environment_unset=("ONEAPI_DEVICE_SELECTOR",),
        )
    )
    source_at = script.index("source /tmp/env_aurora")
    no_user_site_at = script.index("export PYTHONNOUSERSITE=1", source_at)
    safe_path_at = script.index("export PYTHONSAFEPATH=1", source_at)
    unset_at = script.index("unset ONEAPI_DEVICE_SELECTOR")
    nounset_at = script.index("set -u")
    exec_at = script.index("exec python3")
    assert source_at < no_user_site_at < safe_path_at < unset_at < nounset_at < exec_at
    assert script.index("set -eo pipefail") < source_at


def test_scheduler_observation_and_allocation_contracts_are_typed():
    with pytest.raises(ValueError, match="SchedulerState"):
        JobObservation("job", "RUNNING")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="must be in nodes"):
        AllocationMetadata("job", "head", ("worker",))
    observation = JobObservation("job", SchedulerState.RUNNING, head_node="head")
    assert observation.state is SchedulerState.RUNNING


@pytest.mark.parametrize(
    "override",
    [
        {"num_nodes": True},
        {"command_argv": ("python3", 7)},
        {"environment": {"SAFE": 7}},
        {"gpus_per_node": True},
        {"exclusive": 1},
    ],
)
def test_scheduler_contract_never_coerces_typed_fields(override):
    with pytest.raises(ValueError):
        _spec(**override)


def test_scheduler_process_boundary_never_coerces_an_argument_vector():
    with pytest.raises(ValueError, match="argument vector"):
        run_cmd(["qstat", 7], 1)
    with pytest.raises(ValueError, match="argument vector"):
        run_cmd("qstat", 1)


@pytest.mark.parametrize(
    "field,value",
    [
        ("job_name", "evil\n#PBS -l walltime=99:00:00"),  # directive injection
        ("account", "A; rm -rf /"),
        ("queue", "debug`whoami`"),
        ("walltime", "01:00:00\nmalicious"),
        ("job_name", "$(touch /tmp/pwned)"),
        ("stdout_dir", "/tmp/logs\n#PBS -q prod"),
        ("stderr_dir", "/tmp/$(touch pwned)"),
    ],
)
def test_injection_fields_rejected(field, value):
    with pytest.raises(ValueError):
        _spec(**{field: value})


def test_bootstrap_is_exempt_privileged_shell():
    # bootstrap_script is intentionally raw operator shell (documented), so a value
    # with shell syntax must NOT be rejected here.
    _spec(bootstrap_script="module load frameworks && export FOO=$(hostname)")


def test_eval_body_quotes_and_validates():
    from exaserve.schedulers.pbs import PBSScheduler

    sched = PBSScheduler()
    with pytest.raises(ValueError):
        sched.render_job(
            JobSpec(
                job_name="ok",
                num_nodes=1,
                queue="debug",
                walltime="01:00:00",
                account="A\ninjected",
                filesystems="home:flare",
                keep_flag="doe",
                stdout_dir="/tmp",
                stderr_dir="/tmp",
                mail_user="",
                mail_events="",
                cwd=Path("/tmp/repo"),
                source_env_script=Path("/tmp/env"),
                command_argv=("python3", "-m", "eval.cli", "run", "execute", "/tmp/run.yaml"),
            )
        )
    # A clean render succeeds and quotes the body fields.
    script = sched.render_job(
        JobSpec(
            job_name="ok",
            num_nodes=1,
            queue="debug",
            walltime="01:00:00",
            account="AuroraGPT",
            filesystems="home:flare",
            keep_flag="doe",
            stdout_dir="/tmp",
            stderr_dir="/tmp",
            mail_user="",
            mail_events="",
            cwd=Path("/tmp/repo"),
            source_env_script=Path("/tmp/env"),
            command_argv=("python3", "-m", "eval.cli", "run", "execute", "/tmp/run.yaml"),
        )
    )
    assert "cd /tmp/repo" in script
    assert script.index("export PYTHONNOUSERSITE=1") < script.index("source /tmp/env")
    assert script.index(
        "export PYTHONNOUSERSITE=1", script.index("source /tmp/env")
    ) < script.index("exec python3")
    assert "export PYTHONDONTWRITEBYTECODE=1" in script
    assert "PYTHONPYCACHEPREFIX" in script
    assert "exec python3 -m eval.cli run execute /tmp/run.yaml" in script


@pytest.mark.parametrize(
    "override",
    [
        {"environment": {"PYTHONNOUSERSITE": "0"}},
        {"environment_unset": ("PYTHONNOUSERSITE",)},
        {"environment": {"PYTHONSAFEPATH": "0"}},
        {"environment_unset": ("PYTHONSAFEPATH",)},
    ],
)
def test_scheduler_cannot_disable_python_user_site_isolation(override):
    with pytest.raises(ValueError, match="PYTHON"):
        _spec(**override)


def test_eval_body_never_expands_metacharacters_in_paths():
    """IMP-B09 regression: a shlex.quote()'d value pasted INSIDE double quotes
    still executes `$(...)`. Every interpolated path must be inert."""
    from exaserve.schedulers.pbs import PBSScheduler

    evil = "/tmp/$(touch /tmp/PWNED)`id`"
    script = PBSScheduler().render_job(
        JobSpec(
            job_name="ok",
            num_nodes=1,
            queue="debug",
            walltime="01:00:00",
            account="AuroraGPT",
            filesystems="home:flare",
            keep_flag="doe",
            stdout_dir="/tmp",
            stderr_dir="/tmp",
            mail_user="",
            mail_events="",
            cwd=Path(evil),
            source_env_script=Path(evil),
            command_argv=("python3", evil),
        )
    )
    for line in script.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        # No line may contain an unquoted (double-quote-context) substitution
        # of attacker text. The only permitted occurrences of the payload are
        # inside single quotes.
        if "touch /tmp/PWNED" in line or "`id`" in line:
            # find every occurrence and require it be single-quoted
            assert line.count("'") >= 2, f"payload not single-quoted: {line}"
            in_double = line.split("=", 1)[1].startswith('"') if "=" in line else False
            assert not in_double, f"payload inside double quotes (would expand): {line}"


def test_scheduler_nonzero_is_a_definite_rejection(monkeypatch):
    def rejected(*_args, **_kwargs):
        raise subprocess.CalledProcessError(1, ["qsub"])

    monkeypatch.setattr("exaserve.schedulers.base.run_cmd", rejected)
    with pytest.raises(SubmissionRejected):
        run_submission_cmd(["qsub", "/tmp/job"], 1)


def test_scheduler_timeout_is_ambiguous_not_retryable(monkeypatch):
    def timed_out(*_args, **_kwargs):
        raise FiniteProcessTimeout(["qsub"], 1.0, "", "")

    monkeypatch.setattr("exaserve.schedulers.base.run_cmd", timed_out)
    with pytest.raises(SubmissionAmbiguous):
        run_submission_cmd(["qsub", "/tmp/job"], 1)


@pytest.mark.parametrize(
    "scheduler_path,output",
    [
        ("exaserve.schedulers.pbs.run_submission_cmd", "accepted maybe\n"),
        ("exaserve.schedulers.slurm.run_submission_cmd", "accepted maybe\n"),
    ],
)
def test_zero_exit_without_native_identity_is_ambiguous(
    monkeypatch, tmp_path, scheduler_path, output
):
    from exaserve.schedulers.pbs import PBSScheduler
    from exaserve.schedulers.slurm import SlurmScheduler

    monkeypatch.setattr(
        scheduler_path,
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, output, ""),
    )
    scheduler = PBSScheduler() if ".pbs." in scheduler_path else SlurmScheduler()
    with pytest.raises(SubmissionAmbiguous):
        scheduler.submit(tmp_path / "job")
