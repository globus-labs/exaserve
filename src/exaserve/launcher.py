"""Python supervisor entry point for cluster launch (plan WP4, audit IMP-B01).

This is the production wiring of the new control plane. It replaces the
"exec bash and hope" CLI path with a real ``RuntimeSupervisor`` that owns the
launcher as a managed component, installs signal handlers *before* starting
anything, captures the first causal failure, and returns a typed exit code.

Migration switch (plan §4.1): ``EXASERVE_USE_SUPERVISOR``.
  - ``1`` (default): supervise the launcher from Python.
  - ``0``: legacy ``exec bash`` behavior, byte-for-byte.
Registered in ``doc/hardening/MIGRATION_LOG.md``; the legacy branch is deleted
at the WP13 cutover once the supervised path has parity evidence at every
qualified scale tier.

What this closes today (IMP-B03): signal handling covers the whole launch,
the child runs in its own process group so descendants cannot escape cleanup,
an unexpected exit is fatal even with code 0, and the first cause survives
later cleanup errors. What it does NOT yet do (IMP-B01, WP13): replace the
in-allocation topology — the launcher still fans out per-rank drivers.
"""

from __future__ import annotations

import os
import sys
from importlib import resources
from typing import Sequence


def _resources() -> tuple[str, str, str]:
    package_root = resources.files("exaserve")
    return (str(package_root), str(package_root.parent),
            str(package_root / "resources" / "launch_cluster.sh"))


def use_supervisor() -> bool:
    return os.environ.get("EXASERVE_USE_SUPERVISOR", "1") != "0"


def launch(argv: Sequence[str] | None = None) -> int:
    """Supervised launch. Returns the process exit code."""
    from .compat.activator import ActivationError, CompatibilityActivator
    from .control.supervisor import ManagedComponent, RuntimeSupervisor

    argv = list(argv if argv is not None else sys.argv[1:])
    package_root, package_parent, script = _resources()
    env = os.environ.copy()
    env.setdefault("EXASERVE_PACKAGE_ROOT", package_root)
    env.setdefault("EXASERVE_PACKAGE_PARENT", package_parent)

    supervisor = RuntimeSupervisor(poll_interval_s=2.0)
    # IMP-B03: handlers installed BEFORE any child exists, so a SIGTERM during
    # staging/bring-up still drains instead of killing the process abruptly.
    supervisor.install_signal_handlers()

    # Supervisor-role compatibility receipt. Activation is advisory for the
    # supervisor process itself (it imports neither Ray nor vLLM); a failure
    # is reported, not fatal, because the supervisor's job is to report the
    # failure of the roles that DO require the profile.
    try:
        activator = CompatibilityActivator()
        activator.attest_external(
            "supervisor", executable=sys.executable,
            version_probe=f"python {sys.version.split()[0]}")
        env["EXASERVE_COMPAT_PROFILE_ID"] = activator.profile.profile_id
        print(f"[launcher] compatibility profile {activator.profile.name} "
              f"({activator.profile.profile_id[:12]})", flush=True)
    except ActivationError as exc:
        print(f"[launcher] WARNING: supervisor compatibility attestation "
              f"failed: {exc}", flush=True)

    launcher = supervisor.register(ManagedComponent(
        component_id="launch_cluster",
        argv=["bash", script, *argv],
        env=env,
        long_lived=False,          # finite: it returns when the job ends
    ))
    print(f"[launcher] supervising: bash {script} {' '.join(argv)}", flush=True)
    supervisor.start_all()

    cause = supervisor.supervise(
        until=lambda: launcher.process is not None and launcher.process.poll() is not None,
    )
    # Drain whatever is left, bounded, then report the FIRST cause.
    supervisor.shutdown(drain_s=30.0)

    code = launcher.process.returncode if launcher.process else 1
    if cause is not None:
        print(f"[launcher] FIRST CAUSE: {cause}", flush=True)
        return supervisor.exit_code()
    if code:
        print(f"[launcher] launch_cluster exited {code}", flush=True)
    return code or 0


def main() -> None:
    if not use_supervisor():
        # Legacy path, retained until the WP13 cutover.
        package_root, package_parent, script = _resources()
        os.environ.setdefault("EXASERVE_PACKAGE_ROOT", package_root)
        os.environ.setdefault("EXASERVE_PACKAGE_PARENT", package_parent)
        os.execvp("bash", ["bash", script, *sys.argv[1:]])
    raise SystemExit(launch())
