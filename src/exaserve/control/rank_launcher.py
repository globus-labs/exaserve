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

import shlex
from types import MappingProxyType
from typing import Optional, Sequence

from .supervisor import ManagedComponent


class RankLaunchError(RuntimeError):
    pass


def resolve_launch_prefix(
    node_count: int, *, scheduler: str = "pbs", override: Optional[str] = None
) -> list[str]:
    """One task per node; an explicit caller override is validation-only."""
    if isinstance(node_count, bool) or not isinstance(node_count, int) or node_count < 1:
        raise RankLaunchError(f"node_count must be >= 1, got {node_count}")
    if not isinstance(scheduler, str) or not scheduler:
        raise RankLaunchError("scheduler must be non-empty text")
    if override is not None:
        if not isinstance(override, str) or not override:
            raise RankLaunchError("launch override must be null or non-empty text")
        parsed = shlex.split(override)
        if not parsed:
            raise RankLaunchError("launch override produced an empty argument vector")
        return parsed
    if scheduler == "slurm":
        return ["srun", f"--nodes={node_count}", "--ntasks-per-node=1", "--cpu-bind=none"]
    return ["mpiexec", "-n", str(node_count), "-ppn", "1", "--cpu-bind", "none"]


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
        if launch_prefix is not None:
            if isinstance(launch_prefix, (str, bytes)):
                raise RankLaunchError("launch_prefix must be an argument vector")
            prefix_items = tuple(launch_prefix)
            if not prefix_items or any(
                not isinstance(item, str) or not item or "\x00" in item for item in prefix_items
            ):
                raise RankLaunchError("launch_prefix must contain non-empty string arguments")
        else:
            prefix_items = tuple(resolve_launch_prefix(node_count, scheduler=scheduler))
        if env is not None:
            if not isinstance(env, dict) or any(
                not isinstance(key, str)
                or not key
                or not isinstance(value, str)
                or "\x00" in key
                or "\x00" in value
                for key, value in env.items()
            ):
                raise RankLaunchError("rank environment must be a string mapping without NUL")
            env = MappingProxyType(dict(env))
        if cwd is not None and (not isinstance(cwd, str) or not cwd):
            raise RankLaunchError("rank cwd must be null or non-empty text")
        if not isinstance(component_id, str) or not component_id:
            raise RankLaunchError("rank launcher component_id must be non-empty text")
        self.node_count = node_count
        self.rank_argv = list(rank_items)
        self.launch_prefix = list(prefix_items)
        self.env = env
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
