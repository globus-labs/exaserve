"""IMP-B01 / WP4.3: explicit per-rank ownership, no unowned remote PIDs."""

from __future__ import annotations

import os
from pathlib import Path
import sys

import pytest

from exaserve.control.deployment import (
    MAX_DEPLOYMENT_HISTORY_EVENTS,
    DeployError,
    DrainError,
    DeploymentManager,
    DeploymentState,
    PrepareError,
    ValidationError,
)
from exaserve.control.node_supervisor import (
    NodeSupervisor,
    ray_component,
)
from exaserve.control.rank_launcher import (
    RankLaunchError,
    RankLauncher,
    rank_result_check,
    resolve_launch_prefix,
)


# -- RankLauncher -----------------------------------------------------------


def test_vllm_seed_verification_precedes_rank_registration_and_ray_start():
    import exaserve.rank_main as rank_main

    source = Path(rank_main.__file__).read_text(encoding="utf-8")
    seed_verify = source.index("verify_from_environment(profile=compatibility)")
    registration = source.index("channel = RankClient(")
    ray_environment = source.index("ray_child_environment(", registration)
    stale_cleanup = source.index("_clear_stale_ray_state(rank)")
    post_cleanup_verify = source.index(
        "verify_from_environment(profile=compatibility)", seed_verify + 1
    )
    ray_child = source.index("ray_component(argv, env=ray_env)")

    assert seed_verify < registration < ray_environment
    assert stale_cleanup < post_cleanup_verify < ray_child


def test_launch_prefix_is_one_task_per_node_per_scheduler(monkeypatch):
    monkeypatch.delenv("EXASERVE_MPILAUNCH", raising=False)
    assert resolve_launch_prefix(8) == [
        "mpiexec",
        "--genvnone",
        "--envnone",
        "-n",
        "8",
        "-ppn",
        "1",
        "--cpu-bind",
        "none",
    ]
    assert resolve_launch_prefix(8, scheduler="slurm") == [
        "srun",
        "--nodes=8",
        "--ntasks-per-node=1",
        "--cpu-bind=none",
        "--export=NONE",
    ]


def test_launch_prefix_override_wins_verbatim(monkeypatch):
    monkeypatch.setenv("EXASERVE_MPILAUNCH", "mpiexec -n 3 --custom flag")
    assert resolve_launch_prefix(99) != ["mpiexec", "-n", "3", "--custom", "flag"]
    with pytest.raises(RankLaunchError, match="only for test scheduler"):
        resolve_launch_prefix(99, override="mpiexec -n 3 --custom flag")
    assert resolve_launch_prefix(
        99,
        scheduler="test",
        override="mpiexec -n 3 --custom flag",
    ) == [
        "mpiexec",
        "-n",
        "3",
        "--custom",
        "flag",
    ]


def test_rank_launcher_splits_head_scheduler_env_from_rank_application_env():
    from exaserve.site import AURORA_PMIX_PREPARED_ENVIRONMENT

    pmix = dict(AURORA_PMIX_PREPARED_ENVIRONMENT)
    launcher = RankLauncher(
        node_count=2,
        rank_argv=["/opt/site/python", "-m", "exaserve.rank_main"],
        scheduler="pbs",
        env={
            "PBS_JOBID": "job",
            "PBS_NODEFILE": "/home/user/.aurora_leases/nodes",
            "PMIX_RANK": "head-only-rank",
            "PALS_RANKID": "head-only-rank",
        },
        application_env={
            "PYTHONNOUSERSITE": "1",
            "HOME": "/tmp/exaserve/state/home",
            **pmix,
        },
        cwd="/tmp/exaserve/runtime/python",
    )
    component = launcher.component()
    assert component.env["PBS_NODEFILE"].startswith("/home/")
    assert "--genvnone" in component.argv and "--envnone" in component.argv
    exported = component.argv[component.argv.index("--envlist") + 1].split(",")
    assert set(exported) == {
        "HOME",
        "PYTHONNOUSERSITE",
        *pmix,
    }
    assert "PBS_NODEFILE" not in exported
    assert "PMIX_RANK" not in exported
    assert "PALS_RANKID" not in exported
    assert component.argv[component.argv.index("--wdir") + 1] == ("/tmp/exaserve/runtime/python")


@pytest.mark.parametrize(
    "application_env",
    [
        {"PMIX_RANK": "1"},
        {"PALS_RANKID": "1"},
        {"PMIX_MCA_mca_base_param_files": "/home/user/.pmix/mca-params.conf"},
        {"PMIX_MCA_mca_base_component_path": "/home/user/.pmix/components"},
    ],
)
def test_rank_launcher_rejects_ambient_or_forged_pmix_state(application_env):
    with pytest.raises(RankLaunchError, match="launcher-only state"):
        RankLauncher(
            node_count=2,
            rank_argv=["/opt/site/python", "-m", "exaserve.rank_main"],
            env={},
            application_env=application_env,
            cwd="/tmp/exaserve/runtime/python",
        )


def test_launcher_rejects_nonsense_shapes():
    with pytest.raises(RankLaunchError):
        RankLauncher(node_count=0, rank_argv=["python"])
    with pytest.raises(RankLaunchError):
        RankLauncher(node_count=1, rank_argv=[])
    with pytest.raises(RankLaunchError, match="argument vector"):
        RankLauncher(node_count=1, rank_argv="python")
    with pytest.raises(RankLaunchError, match="node_count"):
        RankLauncher(node_count=True, rank_argv=["python"])
    with pytest.raises(RankLaunchError, match="string mapping"):
        RankLauncher(node_count=1, rank_argv=["python"], env={"RANK": 1})


def test_node_supervisor_identity_never_coerces_rank_values():
    with pytest.raises(ValueError, match="generation/rank"):
        NodeSupervisor(deployment_id="d", generation=1, plan_hash="h", rank="0")
    with pytest.raises(ValueError, match="argument vector"):
        ray_component("python")


def test_health_probe_results_are_typed():
    from exaserve.control.node_supervisor import HealthProbe

    probe = HealthProbe("probe", "probe", lambda: (1, "not-a-bool"))
    with pytest.raises(ValueError, match=r"\(bool, str\)"):
        probe.observe()


def test_launcher_component_is_the_heads_only_child(monkeypatch):
    monkeypatch.delenv("EXASERVE_MPILAUNCH", raising=False)
    launcher = RankLauncher(node_count=4, rank_argv=["python", "-m", "exaserve.driver"])
    component = launcher.component()
    assert component.argv[:5] == ("mpiexec", "--genvnone", "--envnone", "-n", "4")
    assert "--envlist" not in component.argv
    assert component.argv[-3:] == ("python", "-m", "exaserve.driver")
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
    component.process = object()  # pretend somebody else started it
    with pytest.raises(ValueError, match="did not create"):
        node.adopt(component)


def test_adopted_components_carry_this_ranks_identity():
    node = NodeSupervisor(deployment_id="d1", generation=1, plan_hash="h", rank=5)
    component = node.adopt(ray_component(["/bin/true"]))
    assert component.owner_scope == "RANK"
    assert component.owner_rank == 5


def test_observations_are_typed_and_rank_scoped():
    seen = []
    node = NodeSupervisor(
        deployment_id="d1",
        generation=3,
        plan_hash="h",
        rank=7,
        node_id="nodeA",
        publish=seen.append,
    )
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
    script = (
        f"import os,subprocess,sys,time;"
        f"p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)']);"
        f"open({str(marker)!r},'w').write(str(p.pid));time.sleep(60)"
    )
    node = NodeSupervisor(deployment_id="d1", generation=1, plan_hash="h", rank=1)
    node.adopt(ray_component([sys.executable, "-c", script]))
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


def test_deployment_manager_identity_and_deadlines_are_typed():
    with pytest.raises(ValueError, match="generation"):
        DeploymentManager(deployment_id="d", generation=True)
    manager = _manager()
    with pytest.raises(ValueError, match="drain deadline"):
        manager.drain(float("nan"))


def test_lifecycle_reaches_ready_through_the_declared_states():
    calls = []
    manager = _manager(
        prepare_fn=lambda: calls.append("prepare"),
        deploy_fn=lambda: calls.append("deploy"),
        validate_fn=lambda: type("S", (), {"ready": True})(),
    )
    manager.prepare()
    assert manager.state == DeploymentState.STAGING
    manager.deploy()
    assert manager.state == DeploymentState.DEPLOYING
    manager.validate()
    assert manager.state == DeploymentState.READY and manager.is_ready()
    assert calls == ["prepare", "deploy"]


def test_ready_is_revocable_when_observation_says_otherwise():
    ready = {"value": True}
    manager = _manager(
        validate_fn=lambda: type("S", (), {"ready": True})(),
        observe_fn=lambda: type("S", (), {"ready": ready["value"]})(),
    )
    manager.prepare()
    manager.deploy()
    manager.validate()
    assert manager.is_ready()
    ready["value"] = False
    manager.observe()
    assert manager.state == DeploymentState.VALIDATING, "READY must be revocable"
    ready["value"] = True
    manager.observe()
    assert manager.is_ready(), "and recoverable once the predicate holds again"


def test_a_degraded_start_does_not_read_as_ready():
    manager = _manager(validate_fn=lambda: type("S", (), {"ready": False})())
    manager.prepare()
    manager.deploy()
    manager.validate()
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
    manager.stop()  # cleanup must not overwrite the cause
    assert str(manager.first_cause) == first


def test_deploy_and_validate_raise_their_own_types():
    with pytest.raises(DeployError):
        m = _manager(deploy_fn=lambda: (_ for _ in ()).throw(RuntimeError("no")))
        m.prepare()
        m.deploy()
    with pytest.raises(ValidationError):
        m = _manager(validate_fn=lambda: (_ for _ in ()).throw(RuntimeError("nope")))
        m.prepare()
        m.deploy()
        m.validate()


@pytest.mark.parametrize("ready", [None, 0, 1, "false", {}])
def test_deployment_manager_rejects_coercible_or_missing_readiness(ready):
    manager = _manager(validate_fn=lambda: type("S", (), {"ready": ready})())
    manager.prepare()
    manager.deploy()
    with pytest.raises(ValidationError, match="exact boolean"):
        manager.validate()
    assert manager.state == DeploymentState.FAILED
    assert manager.first_cause.reason_code == "INVALID_VERDICT"


def test_empty_exception_text_does_not_hide_the_typed_first_cause():
    manager = _manager(prepare_fn=lambda: (_ for _ in ()).throw(RuntimeError()))
    with pytest.raises(PrepareError, match="without diagnostic detail"):
        manager.prepare()
    assert manager.first_cause.reason_code == "STAGING_FAILED"


def test_illegal_transitions_are_refused():
    manager = _manager()
    with pytest.raises(Exception, match="illegal transition"):
        manager.deploy()  # PLANNED -> DEPLOYING skips STAGING


def test_stop_is_idempotent_and_drains_first():
    stopped = []
    manager = _manager(
        validate_fn=lambda: type("S", (), {"ready": True})(),
        drain_fn=lambda s: stopped.append(("drain", s)),
        stop_fn=lambda: stopped.append(("stop", None)),
    )
    manager.prepare()
    manager.deploy()
    manager.validate()
    manager.drain(deadline_s=7)
    manager.stop()
    manager.stop()
    assert manager.state == DeploymentState.STOPPED
    assert stopped == [("drain", 7), ("stop", None)]


def test_cleanup_failure_never_publishes_stopped_or_overwrites_first_cause():
    manager = _manager(stop_fn=lambda: (_ for _ in ()).throw(OSError("cannot reap")))
    manager.prepare()
    manager.deploy()
    manager.drain()
    with pytest.raises(DrainError, match="cannot reap"):
        manager.stop()
    assert manager.state == DeploymentState.FAILED
    assert manager.first_cause.reason_code == "CLEANUP_ERROR"

    prior = _manager(
        prepare_fn=lambda: (_ for _ in ()).throw(OSError("first")),
        stop_fn=lambda: (_ for _ in ()).throw(OSError("second")),
    )
    with pytest.raises(PrepareError):
        prior.prepare()
    with pytest.raises(DrainError, match="second"):
        prior.stop()
    assert "first" in str(prior.first_cause)
    assert any("second" in str(cause) for cause in prior.secondary_causes)
    assert prior.state == DeploymentState.FAILED


def test_deployment_manager_history_is_bounded_and_keeps_first_and_latest():
    ready = {"value": True}
    manager = _manager(
        validate_fn=lambda: type("S", (), {"ready": True})(),
        observe_fn=lambda: type("S", (), {"ready": ready["value"]})(),
    )
    manager.prepare()
    manager.deploy()
    manager.validate()
    transitions = 600
    for _ in range(transitions):
        ready["value"] = not ready["value"]
        manager.observe()
    assert len(manager.history) == MAX_DEPLOYMENT_HISTORY_EVENTS
    assert manager.history[0][0] == DeploymentState.PLANNED
    assert manager.history[-1][0] == manager.state
    total_events = 5 + transitions
    assert manager.history_events_dropped == total_events - MAX_DEPLOYMENT_HISTORY_EVENTS
    assert manager.to_dict()["history_events_dropped"] == manager.history_events_dropped


def test_the_site_adapter_was_removed_after_python_cutover():
    """WP13: there is no shell between the scheduler and composition root."""
    from importlib import resources

    assert not (resources.files("exaserve") / "resources" / "launch_cluster.sh").is_file()


def test_the_deployment_entry_point_is_callable():
    """WP4.1: the deployment was a bare `__main__` block, callable by nobody."""
    pytest.importorskip("ray", exc_type=ImportError)
    import inspect

    from exaserve import server

    assert callable(server.main)
    source = inspect.getsource(server.main)
    for call in (
        "_deploy_manager.prepare()",
        "_deploy_manager.deploy()",
        "_deploy_manager.drain(",
        "_deploy_manager.stop()",
    ):
        assert call in source, f"main() does not drive {call}"
    # WP13: the child publishes EVIDENCE and does not validate itself. It can
    # see neither the gateway nor the other ranks' sessions, so it is the wrong
    # process to decide readiness.
    assert "_deploy_manager.validate()" not in source
    assert "observe_deployment(" in source
    assert "CLUSTER FULLY READY" not in source


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
    import inspect

    from exaserve import rank_main

    assert rank_main.use_node_supervisor() is True
    # WP13 deleted the driver fallback: two per-rank entry points meant two
    # ownership trees, and only one was the one the documentation described.
    monkeypatch.setenv("EXASERVE_RANK_ENTRY", "driver")
    assert rank_main.use_node_supervisor() is True
    assert "_driver_main" not in inspect.getsource(rank_main.main)


def test_ray_argv_is_separable_so_the_supervisor_can_create_the_child(monkeypatch):
    """A rank must not adopt a PID somebody else created."""
    pytest.importorskip("ray", exc_type=ImportError)
    from exaserve.control.ray_runtime import RayClusterConfig, ray_head_argv, ray_worker_argv

    monkeypatch.setenv("EXASERVE_RAY_INTERNAL_STARTUP_LIMIT", "8")
    cluster = RayClusterConfig(head_ip="10.0.0.1", port=6379, node_cpus=64)
    head = ray_head_argv(cluster, 12)
    worker = ray_worker_argv(cluster, 12, worker_ip="10.0.0.2")
    assert "--head" in head and "--block" in head
    assert "--address=10.0.0.1:6379" in worker
    assert all(isinstance(a, str) for a in head + worker)


def test_ray_and_vllm_share_one_resolved_node_ip(monkeypatch):
    from types import SimpleNamespace

    from exaserve.control import ray_runtime

    plan = SimpleNamespace(
        deployment_plan_hash="a" * 64,
        site_profile_hash="b" * 64,
        vendor="xpu",
        engine="vllm",
    )
    for key, value in {
        "EXASERVE_PLAN_HASH": plan.deployment_plan_hash,
        "EXASERVE_SITE_PROFILE_HASH": plan.site_profile_hash,
        "EXASERVE_VENDOR": plan.vendor,
        "EXASERVE_ENGINE": plan.engine,
        "ONEAPI_DEVICE_SELECTOR": "must-not-reach-ray",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("EXASERVE_RAY_INTERNAL_STARTUP_LIMIT", "8")
    monkeypatch.setattr(ray_runtime, "_local_hsn_ip", lambda: "10.0.0.2")

    cluster = ray_runtime.RayClusterConfig(head_ip="10.0.0.1", port=6379, node_cpus=64)
    for rank, expected_ip in ((0, "10.0.0.1"), (1, "10.0.0.2")):
        node_ip = ray_runtime.ray_node_ip(cluster, rank)
        environment = ray_runtime.ray_child_environment(plan, node_ip=node_ip)
        argv = (
            ray_runtime.ray_head_argv(cluster, 12)
            if rank == 0
            else ray_runtime.ray_worker_argv(cluster, 12, worker_ip=node_ip)
        )
        assert node_ip == expected_ip
        assert environment["VLLM_HOST_IP"] == expected_ip
        assert f"--node-ip-address={expected_ip}" in argv
        assert "ONEAPI_DEVICE_SELECTOR" not in environment


@pytest.mark.parametrize("node_ip", ["", "not-an-ip"])
def test_ray_child_environment_rejects_invalid_vllm_network_identity(monkeypatch, node_ip):
    from types import SimpleNamespace

    from exaserve.control.ray_runtime import ray_child_environment

    plan = SimpleNamespace(
        deployment_plan_hash="a" * 64,
        site_profile_hash="b" * 64,
        vendor="xpu",
        engine="vllm",
    )
    for key, value in {
        "EXASERVE_PLAN_HASH": plan.deployment_plan_hash,
        "EXASERVE_SITE_PROFILE_HASH": plan.site_profile_hash,
        "EXASERVE_VENDOR": plan.vendor,
        "EXASERVE_ENGINE": plan.engine,
    }.items():
        monkeypatch.setenv(key, value)
    with pytest.raises(ValueError, match="node_ip"):
        ray_child_environment(plan, node_ip=node_ip)


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


def test_the_outer_root_carries_run_identity_into_the_deployment_child():
    """The fallback child belongs to the allocation-head root, not rank zero."""
    from importlib import resources

    source = (resources.files("exaserve") / "composition.py").read_text()
    for key in ("EXASERVE_RUN_LOG_DIR", "EXASERVE_DEPLOYMENT_ID", "EXASERVE_PLAN_HASH"):
        assert key in source, f"{key} is not carried into the deployment child"
    rank_source = (resources.files("exaserve") / "rank_main.py").read_text()
    assert "deployment_component(" not in rank_source


def test_node_supervisor_never_delegates_control_credentials_to_children():
    from exaserve.rank_main import _without_control_credentials

    original = {
        "EXASERVE_CONTROL_HOST": "head",
        "EXASERVE_CONTROL_PORT": "1234",
        "EXASERVE_CONTROL_SECRET": "do-not-delegate",
        "EXASERVE_RECEIPT_SOCKET": "/tmp/receipts.sock",
        "PATH": "/bin",
    }
    child = _without_control_credentials(original)
    assert original["EXASERVE_CONTROL_SECRET"] == "do-not-delegate"
    assert not any(key.startswith("EXASERVE_CONTROL_") for key in child)
    assert child["EXASERVE_RECEIPT_SOCKET"] == "/tmp/receipts.sock"
