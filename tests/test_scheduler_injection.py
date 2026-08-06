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
    assert "_ES_CODE_ROOT=/tmp/repo" in script
    assert 'cd "$_ES_CODE_ROOT"' in script
    assert "python3 -m eval.cli run execute /tmp/run.yaml" in script


def test_eval_body_never_expands_metacharacters_in_paths():
    """IMP-B09 regression: a shlex.quote()'d value pasted INSIDE double quotes
    still executes `$(...)`. Every interpolated path must be inert."""
    from eval.lib.schedulers.pbs import PBSScheduler

    evil = "/tmp/$(touch /tmp/PWNED)`id`"
    script = PBSScheduler().render_job(
        job_name="ok", num_nodes=1, queue="debug", walltime="01:00:00",
        project="AuroraGPT", filesystems="home:flare", keep_output="doe",
        stdout_dir="/tmp", stderr_dir="/tmp", mail_user="", mail_events="",
        code_root=evil, env_script=evil, run_yaml_path=evil,
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
