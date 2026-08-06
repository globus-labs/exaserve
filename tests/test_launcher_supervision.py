"""IMP-B01 wiring: the CLI launch path is supervised, not `exec bash`."""

from __future__ import annotations

import os
import sys
import textwrap

import pytest

from exaserve import launcher


def test_switch_defaults_to_supervisor(monkeypatch):
    monkeypatch.delenv("EXASERVE_USE_SUPERVISOR", raising=False)
    assert launcher.use_supervisor() is True
    monkeypatch.setenv("EXASERVE_USE_SUPERVISOR", "0")
    assert launcher.use_supervisor() is False


def test_cli_launch_cluster_no_longer_execs_bash():
    """The old implementation called os.execvp and never returned."""
    import inspect

    from exaserve import cli

    src = inspect.getsource(cli.launch_cluster)
    assert "execvp" not in src, "CLI still replaces itself with bash"
    assert "launcher" in src


def test_supervised_launch_propagates_child_failure(tmp_path, monkeypatch):
    """A failing launcher script must yield a nonzero supervised exit code."""
    fake = tmp_path / "launch_cluster.sh"
    fake.write_text("#!/bin/bash\necho staging...\nexit 9\n")
    monkeypatch.setattr(launcher, "_resources",
                        lambda: ("/pkg", "/", str(fake)))
    assert launcher.launch([]) == 9


def test_supervised_launch_returns_zero_on_success(tmp_path, monkeypatch):
    fake = tmp_path / "launch_cluster.sh"
    fake.write_text("#!/bin/bash\necho ok\nexit 0\n")
    monkeypatch.setattr(launcher, "_resources",
                        lambda: ("/pkg", "/", str(fake)))
    assert launcher.launch([]) == 0


def test_supervised_launch_reaps_descendants(tmp_path, monkeypatch):
    """IMP-B03: a grandchild spawned by the launcher must not survive."""
    marker = tmp_path / "grandchild.pid"
    fake = tmp_path / "launch_cluster.sh"
    fake.write_text(textwrap.dedent(f"""\
        #!/bin/bash
        {sys.executable} -c "import time; time.sleep(120)" &
        echo $! > {marker}
        sleep 0.5
        exit 0
        """))
    monkeypatch.setattr(launcher, "_resources",
                        lambda: ("/pkg", "/", str(fake)))
    assert launcher.launch([]) == 0
    pid = int(marker.read_text().strip())
    # After the supervised launch returns, the grandchild must be gone.
    import time
    for _ in range(30):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return  # reaped
        time.sleep(0.2)
    pytest.fail(f"grandchild {pid} survived supervised cleanup")
