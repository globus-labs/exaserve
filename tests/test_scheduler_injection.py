"""PR-015 acceptance: scheduler job-script fields reject injection (hermetic)."""

from __future__ import annotations

from pathlib import Path

import pytest

from exaserve.schedulers.base import JobSpec


def _spec(**over):
    base = dict(
        config_path=Path("/tmp/c.yaml"), launch_script=Path("/tmp/launch.sh"),
        package_root=Path("/tmp/pkg"), package_parent=Path("/tmp"),
        num_nodes=1, walltime="01:00:00", account="AuroraGPT",
        job_name="exaserve-serve", log_dir=Path("/tmp/logs"), queue="debug",
    )
    base.update(over)
    return JobSpec(**base)


def test_valid_spec_ok():
    _spec()  # should not raise


@pytest.mark.parametrize("field,value", [
    ("job_name", "evil\n#PBS -l walltime=99:00:00"),   # directive injection
    ("account", "A; rm -rf /"),
    ("queue", "debug`whoami`"),
    ("walltime", "01:00:00\nmalicious"),
    ("job_name", "$(touch /tmp/pwned)"),
])
def test_injection_fields_rejected(field, value):
    with pytest.raises(ValueError):
        _spec(**{field: value})


def test_env_setup_is_exempt_privileged_shell():
    # env_setup is intentionally raw operator shell (documented), so a value
    # with shell syntax must NOT be rejected here.
    _spec(env_setup="module load frameworks && export FOO=$(hostname)")


def test_eval_body_quotes_and_validates():
    from eval.lib.schedulers.pbs import PBSScheduler

    sched = PBSScheduler()
    with pytest.raises(ValueError):
        sched.render_job(
            job_name="ok", num_nodes=1, queue="debug", walltime="01:00:00",
            project="A\ninjected", filesystems="home:flare", keep_output="doe",
            stdout_dir="/tmp", stderr_dir="/tmp", mail_user="", mail_events="",
            code_root="/tmp/repo", env_script="/tmp/env", run_yaml_path="/tmp/run.yaml",
        )
    # A clean render succeeds and quotes the body fields.
    script = sched.render_job(
        job_name="ok", num_nodes=1, queue="debug", walltime="01:00:00",
        project="AuroraGPT", filesystems="home:flare", keep_output="doe",
        stdout_dir="/tmp", stderr_dir="/tmp", mail_user="", mail_events="",
        code_root="/tmp/repo", env_script="/tmp/env", run_yaml_path="/tmp/run.yaml",
    )
    assert "cd /tmp/repo" in script and "python3 -m eval.cli run execute /tmp/run.yaml" in script
