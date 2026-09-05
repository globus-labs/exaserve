"""The allocation-head's single owned launch boundary (plan WP4.3/WP4.4).

`RuntimeSupervisor` owns exactly one `RankLauncher`; the launcher owns one
MPI/srun process; that process owns the per-rank `NodeSupervisor`s. The head
never holds a remote PID, so there is no path by which it could signal or reap
one — the property is structural, not a rule someone has to remember.

Two independent failure signals, per WP4.4:

1. **Launcher exit aggregation.** `mpiexec`/`srun` returns nonzero when any
   rank fails, and that status becomes the scheduler-visible exit. This is
   causal and bounded, so it is used directly.
2. **The typed control channel.** A rank reporting a fatal observation is a
   failure even if the launcher has not yet exited; the head then terminates
   the launcher group explicitly rather than waiting for aggregation.

Either signal alone must produce a nonzero global result. Neither depends on
matching stdout.
"""

from __future__ import annotations

import re
import shlex
from types import MappingProxyType
from typing import Optional, Sequence

from .supervisor import ManagedComponent


class RankLaunchError(RuntimeError):
    pass


_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def resolve_launch_prefix(
    node_count: int,
    *,
    scheduler: str = "pbs",
    override: Optional[str] = None,
    application_env_names: Sequence[str] = (),
    application_cwd: Optional[str] = None,
) -> list[str]:
    """One task per node; an explicit caller override is validation-only."""
    if isinstance(node_count, bool) or not isinstance(node_count, int) or node_count < 1:
        raise RankLaunchError(f"node_count must be >= 1, got {node_count}")
    if not isinstance(scheduler, str) or not scheduler:
        raise RankLaunchError("scheduler must be non-empty text")
    if override is not None:
        if scheduler != "test":
            raise RankLaunchError("custom launch overrides are permitted only for test scheduler")
        if not isinstance(override, str) or not override:
            raise RankLaunchError("launch override must be null or non-empty text")
        parsed = shlex.split(override)
        if not parsed:
            raise RankLaunchError("launch override produced an empty argument vector")
        return parsed
    names = tuple(application_env_names)
    if any(not isinstance(name, str) or _ENV_NAME.fullmatch(name) is None for name in names):
        raise RankLaunchError("application environment names must be valid identifiers")
    if len(names) != len(set(names)):
        raise RankLaunchError("application environment names must be unique")
    if application_cwd is not None and (
        not isinstance(application_cwd, str) or not application_cwd.startswith("/")
    ):
        raise RankLaunchError("application cwd must be an absolute path")
    if scheduler == "slurm":
        prefix = ["srun", f"--nodes={node_count}", "--ntasks-per-node=1", "--cpu-bind=none"]
        exports = "NONE" + (f",{','.join(sorted(names))}" if names else "")
        prefix.append(f"--export={exports}")
        if application_cwd is not None:
            prefix.append(f"--chdir={application_cwd}")
        return prefix
    prefix = ["mpiexec", "--genvnone", "--envnone"]
    if names:
        prefix.extend(("--envlist", ",".join(sorted(names))))
    if application_cwd is not None:
        prefix.extend(("--wdir", application_cwd))
    return [*prefix, "-n", str(node_count), "-ppn", "1", "--cpu-bind", "none"]


class RankLauncher:
    """One MPI/srun launch of the per-rank entry point, as an owned component."""

    def __init__(
        self,
        *,
        node_count: int,
        rank_argv: Sequence[str],
        scheduler: str = "pbs",
        launch_prefix: Optional[Sequence[str]] = None,
        env: Optional[dict] = None,
        application_env: Optional[dict] = None,
        cwd: Optional[str] = None,
        stdout=None,
        component_id: str = "rank_launcher",
    ) -> None:
        if isinstance(node_count, bool) or not isinstance(node_count, int) or node_count < 1:
            raise RankLaunchError(f"node_count must be >= 1, got {node_count}")
        if isinstance(rank_argv, (str, bytes)):
            raise RankLaunchError("rank_argv must be an argument vector")
        rank_items = tuple(rank_argv)
        if not rank_items or any(
            not isinstance(item, str) or not item or "\x00" in item for item in rank_items
        ):
            raise RankLaunchError("rank_argv must contain non-empty string arguments")
        if application_env is not None:
            if not isinstance(application_env, dict) or any(
                not isinstance(key, str)
                or _ENV_NAME.fullmatch(key) is None
                or not isinstance(value, str)
                or "\x00" in key
                or "\x00" in value
                for key, value in application_env.items()
            ):
                raise RankLaunchError(
                    "rank application environment must be a string mapping without NUL"
                )
            from ..site import AURORA_PMIX_PREPARED_ENVIRONMENT

            qualified_pmix = dict(AURORA_PMIX_PREPARED_ENVIRONMENT)
            invalid_transport = sorted(
                key
                for key, value in application_env.items()
                if key.startswith(("PBS_", "SLURM_", "PALS_", "PMI_", "PMIX_", "OMPI_"))
                and qualified_pmix.get(key) != value
            )
            if invalid_transport:
                raise RankLaunchError(
                    "rank application environment contains launcher-only state: "
                    f"{invalid_transport}"
                )
            application_env = MappingProxyType(dict(application_env))
        if launch_prefix is not None:
            if scheduler != "test":
                raise RankLaunchError("custom launch_prefix is permitted only for test scheduler")
            if isinstance(launch_prefix, (str, bytes)):
                raise RankLaunchError("launch_prefix must be an argument vector")
            prefix_items = tuple(launch_prefix)
            if not prefix_items or any(
                not isinstance(item, str) or not item or "\x00" in item for item in prefix_items
            ):
                raise RankLaunchError("launch_prefix must contain non-empty string arguments")
        else:
            prefix_items = tuple(
                resolve_launch_prefix(
                    node_count,
                    scheduler=scheduler,
                    application_env_names=tuple((application_env or {}).keys()),
                    application_cwd=cwd,
                )
            )
        if env is not None:
            if not isinstance(env, dict) or any(
                not isinstance(key, str)
                or not key
                or "=" in key
                or not isinstance(value, str)
                or "\x00" in key
                or "\x00" in value
                for key, value in env.items()
            ):
                raise RankLaunchError("rank environment must be a string mapping without NUL")
            launcher_environment = dict(env)
            launcher_environment.update(application_env or {})
            env = MappingProxyType(launcher_environment)
        elif application_env:
            env = MappingProxyType(dict(application_env))
        if cwd is not None and (not isinstance(cwd, str) or not cwd):
            raise RankLaunchError("rank cwd must be null or non-empty text")
        if not isinstance(component_id, str) or not component_id:
            raise RankLaunchError("rank launcher component_id must be non-empty text")
        self.node_count = node_count
        self.rank_argv = list(rank_items)
        self.launch_prefix = list(prefix_items)
        self.env = env
        self.application_env = application_env
        self.cwd = cwd
        self.stdout = stdout
        self.component_id = component_id

    def argv(self) -> list[str]:
        return [*self.launch_prefix, *self.rank_argv]

    def component(self) -> ManagedComponent:
        """The supervised component. GLOBAL scope: it is the head's own child.

        `long_lived=False` because the launch is finite — it returns when the
        allocation's work ends — but an unexpected *nonzero* exit is still a
        supervisor failure, and `result_check` makes a zero exit insufficient
        on its own when a rank has already reported a fatal observation.
        """
        return ManagedComponent(
            component_id=self.component_id,
            argv=self.argv(),
            env=self.env,
            cwd=self.cwd,
            stdout=self.stdout,
            long_lived=False,
        )

    def describe(self) -> dict:
        return {
            "component_id": self.component_id,
            "node_count": self.node_count,
            "launch_prefix": self.launch_prefix,
            "rank_argv": self.rank_argv,
            "application_env_names": sorted((self.application_env or {}).keys()),
            "application_cwd": self.cwd,
        }


def rank_result_check(rank_failure_reported) -> "callable":
    """Build a finite-component result check for the launcher.

    A zero exit from the launcher is NOT sufficient: if any rank reported a
    fatal observation over the control channel, the deployment failed even
    though the launch aggregated to success. The two signals are independent by
    design (WP4.4), and this is where the second one is enforced.
    """

    def _check() -> tuple[bool, str]:
        failure = rank_failure_reported()
        if failure:
            return False, f"rank reported a fatal observation: {failure}"
        return True, "launcher exited 0 and no rank reported a failure"

    return _check
