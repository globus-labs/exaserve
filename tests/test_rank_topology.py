"""IMP-B01 / WP4.3: explicit per-rank ownership, no unowned remote PIDs."""

from __future__ import annotations

import os
import sys

import pytest

from exaserve.control.deployment import (
    DeployError,
    DeploymentManager,
    DeploymentState,
    PrepareError,
    ValidationError,
)
from exaserve.control.node_supervisor import (
    NodeSupervisor,
    deployment_component,
    ray_component,
)
from exaserve.control.rank_launcher import (
    RankLaunchError,
    RankLauncher,
    rank_result_check,
    resolve_launch_prefix,
)


# -- RankLauncher -----------------------------------------------------------

def test_launch_prefix_is_one_task_per_node_per_scheduler(monkeypatch):
    monkeypatch.delenv("EXASERVE_MPILAUNCH", raising=False)
    assert resolve_launch_prefix(8) == [
        "mpiexec", "-n", "8", "-ppn", "1", "--cpu-bind", "none"]
    assert resolve_launch_prefix(8, scheduler="slurm") == [
        "srun", "--nodes=8", "--ntasks-per-node=1", "--cpu-bind=none"]


def test_launch_prefix_override_wins_verbatim(monkeypatch):
    monkeypatch.setenv("EXASERVE_MPILAUNCH", "mpiexec -n 3 --custom flag")
    assert resolve_launch_prefix(99) == ["mpiexec", "-n", "3", "--custom", "flag"]


def test_launcher_rejects_nonsense_shapes():
    with pytest.raises(RankLaunchError):
        RankLauncher(node_count=0, rank_argv=["python"])
    with pytest.raises(RankLaunchError):
        RankLauncher(node_count=1, rank_argv=[])


def test_launcher_component_is_the_heads_only_child(monkeypatch):
    monkeypatch.delenv("EXASERVE_MPILAUNCH", raising=False)
    launcher = RankLauncher(node_count=4, rank_argv=["python", "-m", "exaserve.driver"])
    component = launcher.component()
    assert component.argv[:5] == ["mpiexec", "-n", "4", "-ppn", "1"]
    assert component.argv[-3:] == ["python", "-m", "exaserve.driver"]
    # GLOBAL: it is the head's own child, not a rank-scoped one.
    assert component.owner_scope == "GLOBAL"


def test_a_zero_exit_is_not_enough_when_a_rank_reported_failure():
    """The two failure signals are independent (WP4.4)."""
    ok, why = rank_result_check(lambda: None)()
    assert ok, why
    ok, why = rank_result_check(lambda: "rank 3 raylet died")()
    assert not ok and "rank 3 raylet died" in why


# -- NodeSupervisor ---------------------------------------------------------

def test_a_rank_refuses_to_adopt_a_pid_it_did_not_create():
    """The invariant: no process manages a PID it did not create."""
    node = NodeSupervisor(deployment_id="d1", generation=1, plan_hash="h", rank=2)
    component = ray_component([sys.executable, "-c", "pass"])
    component.process = object()          # pretend somebody else started it
    with pytest.raises(ValueError, match="did not create"):
        node.adopt(component)


def test_adopted_components_carry_this_ranks_identity():
    node = NodeSupervisor(deployment_id="d1", generation=1, plan_hash="h", rank=5)
    component = node.adopt(ray_component(["/bin/true"]))
    assert component.owner_scope == "RANK"
    assert component.owner_rank == 5


def test_observations_are_typed_and_rank_scoped():
    seen = []
    node = NodeSupervisor(deployment_id="d1", generation=3, plan_hash="h", rank=7,
                          node_id="nodeA", publish=seen.append)
    node.adopt(ray_component([sys.executable, "-c", "import time; time.sleep(30)"]))
    node.start_all()
    try:
        assert seen, "starting a component must publish an observation"
        obs = seen[-1]
        assert obs.owner_scope == "RANK" and obs.owner_rank == 7
        assert obs.node_id == "nodeA" and obs.generation == 3
        assert obs.sequence >= 1
        # Sequence is monotonic per rank, which is what the channel dedups on.
        node.observe_once()
        assert seen[-1].sequence > obs.sequence
    finally:
        node.shutdown(drain_s=5)


def test_a_ray_child_that_exits_zero_is_still_a_rank_failure():
    """A long-lived component that returns 0 has still disappeared."""
    node = NodeSupervisor(deployment_id="d1", generation=1, plan_hash="h", rank=0)
    node.adopt(ray_component([sys.executable, "-c", "pass"]))
    node.start_all()
    import time

    for _ in range(50):
        cause = node.observe_once()
        if cause is not None:
            assert node.exit_code() != 0
            return
        time.sleep(0.1)
    pytest.fail("an unexpected exit-0 of the ray child was not reported")


def test_shutdown_only_touches_this_nodes_children(tmp_path):
    marker = tmp_path / "grandchild.pid"
    script = (f"import os,subprocess,sys,time;"
              f"p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)']);"
              f"open({str(marker)!r},'w').write(str(p.pid));time.sleep(60)")
    node = NodeSupervisor(deployment_id="d1", generation=1, plan_hash="h", rank=1)
    node.adopt(deployment_component([sys.executable, "-c", script]))
    node.start_all()
    import time

    for _ in range(50):
        if marker.exists():
            break
        time.sleep(0.1)
    node.shutdown(drain_s=10)
    pid = int(marker.read_text().strip())
    for _ in range(50):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.2)
    pytest.fail(f"grandchild {pid} survived this rank's shutdown")


# -- DeploymentManager ------------------------------------------------------

def _manager(**kw):
    return DeploymentManager(deployment_id="d1", generation=1, **kw)


def test_lifecycle_reaches_ready_through_the_declared_states():
    calls = []
    manager = _manager(prepare_fn=lambda: calls.append("prepare"),
                       deploy_fn=lambda: calls.append("deploy"),
                       validate_fn=lambda: type("S", (), {"ready": True})())
    manager.prepare()
    assert manager.state == DeploymentState.STAGING
    manager.deploy()
    assert manager.state == DeploymentState.DEPLOYING
    manager.validate()
    assert manager.state == DeploymentState.READY and manager.is_ready()
    assert calls == ["prepare", "deploy"]


def test_ready_is_revocable_when_observation_says_otherwise():
    ready = {"value": True}
    manager = _manager(validate_fn=lambda: type("S", (), {"ready": True})(),
                       observe_fn=lambda: type("S", (), {"ready": ready["value"]})())
    manager.prepare(); manager.deploy(); manager.validate()
    assert manager.is_ready()
    ready["value"] = False
    manager.observe()
    assert manager.state == DeploymentState.VALIDATING, "READY must be revocable"
    ready["value"] = True
    manager.observe()
    assert manager.is_ready(), "and recoverable once the predicate holds again"


def test_a_degraded_start_does_not_read_as_ready():
    manager = _manager(validate_fn=lambda: type("S", (), {"ready": False})())
    manager.prepare(); manager.deploy(); manager.validate()
    assert manager.state == DeploymentState.VALIDATING
    assert not manager.is_ready()


def test_failures_are_typed_and_the_first_cause_wins():
    def _boom():
        raise OSError("staging blew up")

    manager = _manager(prepare_fn=_boom)
    with pytest.raises(PrepareError):
        manager.prepare()
    assert manager.state == DeploymentState.FAILED
    first = str(manager.first_cause)
    manager.stop()                       # cleanup must not overwrite the cause
    assert str(manager.first_cause) == first


def test_deploy_and_validate_raise_their_own_types():
    with pytest.raises(DeployError):
        m = _manager(deploy_fn=lambda: (_ for _ in ()).throw(RuntimeError("no")))
        m.prepare(); m.deploy()
    with pytest.raises(ValidationError):
        m = _manager(validate_fn=lambda: (_ for _ in ()).throw(RuntimeError("nope")))
        m.prepare(); m.deploy(); m.validate()


def test_illegal_transitions_are_refused():
    manager = _manager()
    with pytest.raises(Exception, match="illegal transition"):
        manager.deploy()                 # PLANNED -> DEPLOYING skips STAGING


def test_stop_is_idempotent_and_drains_first():
    stopped = []
    manager = _manager(validate_fn=lambda: type("S", (), {"ready": True})(),
                       drain_fn=lambda s: stopped.append(("drain", s)),
                       stop_fn=lambda: stopped.append(("stop", None)))
    manager.prepare(); manager.deploy(); manager.validate()
    manager.drain(deadline_s=7)
    manager.stop()
    manager.stop()
    assert manager.state == DeploymentState.STOPPED
    assert stopped[0] == ("drain", 7)


# -- head entry point -------------------------------------------------------

def test_head_supervises_exactly_one_launcher(tmp_path, monkeypatch):
    """The head's child set is {the launcher}. Nothing remote is held."""
    from exaserve import supervisor_main

    nodefile = tmp_path / "nodes"
    nodefile.write_text("hostA\nhostB\nhostA\n")     # duplicates collapse
    monkeypatch.setenv("EXASERVE_NODEFILE", str(nodefile))
    monkeypatch.setenv("EXASERVE_SCHEDULER", "pbs")
    monkeypatch.delenv("EXASERVE_MPILAUNCH", raising=False)

    supervisor, launcher, component = supervisor_main.build("/tmp/cfg.yaml")
    assert launcher.node_count == 2
    assert list(supervisor.components) == [component.component_id]
    assert component.argv[:3] == ["mpiexec", "-n", "2"]
    # IMP-B01: the rank entry point must be the NodeSupervisor. The previous
    # expectation here encoded the WEAKER behaviour and let the gap pass.
    assert "exaserve.rank_main" in component.argv
    assert "exaserve.driver" not in component.argv
    assert "/tmp/cfg.yaml" in component.argv


def test_head_requires_a_nodefile(monkeypatch):
    from exaserve import supervisor_main

    monkeypatch.setenv("EXASERVE_NODEFILE", "/nonexistent/nodefile")
    with pytest.raises(SystemExit, match="nodefile"):
        supervisor_main.build("/tmp/cfg.yaml")


def test_python_rank_launch_switch_defaults_on(monkeypatch):
    from exaserve import supervisor_main

    monkeypatch.delenv("EXASERVE_PYTHON_RANK_LAUNCH", raising=False)
    assert supervisor_main.use_python_rank_launch() is True
    monkeypatch.setenv("EXASERVE_PYTHON_RANK_LAUNCH", "0")
    assert supervisor_main.use_python_rank_launch() is False


def test_head_propagates_a_launch_failure_as_a_nonzero_exit(tmp_path, monkeypatch):
    from exaserve import supervisor_main

    nodefile = tmp_path / "nodes"
    nodefile.write_text("hostA\n")
    monkeypatch.setenv("EXASERVE_NODEFILE", str(nodefile))
    # Stand in for mpiexec with a command that fails the way a rank would.
    monkeypatch.setenv("EXASERVE_MPILAUNCH", f"{sys.executable} -c "
                       "'import sys;sys.exit(7)' --")
    rc = supervisor_main.run("/tmp/cfg.yaml")
    assert rc == 7, "a failing rank launch must be scheduler-visible"


def test_the_site_adapter_execs_the_supervisor():
    """launch_cluster.sh must hand the run to Python, not own it."""
    from importlib import resources

    script = (resources.files("exaserve") / "resources" / "launch_cluster.sh").read_text()
    assert "-m exaserve.launcher" in script, (
        "the adapter must exec the composition root")
    assert "-m exaserve.driver" not in script, (
        "the shell must not launch ranks itself any more")


def test_the_deployment_entry_point_is_callable():
    """WP4.1: the deployment was a bare `__main__` block, callable by nobody."""
    pytest.importorskip("ray", exc_type=ImportError)
    import inspect

    from exaserve import server

    assert callable(server.main)
    source = inspect.getsource(server.main)
    for call in ("_deploy_manager.prepare()", "_deploy_manager.deploy()",
                 "_deploy_manager.validate()", "_deploy_manager.drain(",
                 "_deploy_manager.stop()"):
        assert call in source, f"main() does not drive {call}"


def test_the_module_level_app_is_not_clobbered_by_the_entry_point():
    """As a block, `for app in built_apps:` rebound the module-level FastAPI
    object at import time. As a function, that loop variable stays local."""
    pytest.importorskip("ray", exc_type=ImportError)
    from fastapi import FastAPI

    from exaserve import server

    assert isinstance(server.app, FastAPI)


def test_the_rank_entry_point_owns_its_children(monkeypatch, tmp_path):
    """IMP-B01 §4.1.1: NodeSupervisor must have a PRODUCTION consumer.

    Before this, its only consumers were tests -- the documented ownership
    tree described a design, not the process tree that ran.
    """
    from exaserve import rank_main

    assert rank_main.use_node_supervisor() is True
    monkeypatch.setenv("EXASERVE_RANK_ENTRY", "driver")
    assert rank_main.use_node_supervisor() is False


def test_ray_argv_is_separable_so_the_supervisor_can_create_the_child():
    """A rank must not adopt a PID somebody else created."""
    pytest.importorskip("ray", exc_type=ImportError)
    from exaserve.driver import RayClusterConfig, ray_head_argv, ray_worker_argv

    cluster = RayClusterConfig(head_ip="10.0.0.1", port=6379, node_cpus=64)
    head = ray_head_argv(cluster, 12)
    worker = ray_worker_argv(cluster, 12, worker_ip="10.0.0.2")
    assert "--head" in head and "--block" in head
    assert "--address=10.0.0.1:6379" in worker
    assert all(isinstance(a, str) for a in head + worker)


def test_a_ray_worker_exiting_zero_is_fatal_for_the_rank():
    """The audit's hole: a long-lived Ray worker exiting 0 became a success."""
    import time

    node = NodeSupervisor(deployment_id="d", generation=1, plan_hash="h", rank=2)
    node.adopt(ray_component([sys.executable, "-c", "raise SystemExit(0)"]))
    node.start_all()
    for _ in range(50):
        if node.observe_once() is not None:
            assert node.exit_code() != 0
            return
        time.sleep(0.1)
    pytest.fail("a Ray worker exiting 0 was not fatal for the rank")


def test_a_rank_drains_when_shutdown_is_requested():
    """The supervise loop ignored its own shutdown flag, so SIGTERM left the
    rank spinning and its children orphaned (2-node: tree_reaped FAIL)."""
    import threading
    import time

    node = NodeSupervisor(deployment_id="d", generation=1, plan_hash="h", rank=0)
    node.adopt(ray_component([sys.executable, "-c", "import time; time.sleep(60)"]))
    node.start_all()
    threading.Timer(0.5, lambda: node._supervisor.request_shutdown("test")).start()
    start = time.monotonic()
    node.supervise()
    assert time.monotonic() - start < 20, "supervise() ignored the shutdown request"
    node.shutdown(drain_s=10)
    assert node.exit_code() == 143
