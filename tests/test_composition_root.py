"""P04 / IMP-H03: Python owns the complete lifecycle."""

from __future__ import annotations

import json
import os
import threading
import time
from types import SimpleNamespace

import pytest

from exaserve.composition import (
    CompositionError,
    CompositionRoot,
    StagingStep,
    read_nodefile,
)
from exaserve.plan.compiler import compile_deployment_plan
from exaserve.plan.contracts import SiteProfile
from exaserve.state.status import DeploymentState


def _site():
    return SiteProfile(
        schema_version=3,
        site_id="s",
        max_nodes=64,
        gpus_per_node=12,
        cpus_per_node=64,
        scheduler_types=("pbs",),
        gateway_kinds=("haproxy",),
        vendors=("xpu",),
        engines=("vllm",),
        model_storage_path="/m",
        local_stage_path="/t",
        launcher_capabilities=("ray_serve.run_many",),
    ).finalize()


def _plan(nodes=2, *, runtime=None):
    raw = {
        "num_nodes": nodes,
        "models": [
            {"model_id": "a/b", "tensor_parallel_size": 1, "max_model_len": 4096, "size": 8}
        ],
        "gateway": {"kind": "haproxy", "port": 4001},
    }
    if runtime:
        raw["runtime"] = runtime
        raw["validation_mode"] = True
    return compile_deployment_plan(raw, site=_site(), deployment_id="d")


def _root(tmp_path, nodes=2):
    return CompositionRoot(
        plan=_plan(nodes), generation=7, run_dir=str(tmp_path), log=lambda *_: None
    )


def test_composition_root_overwrites_ambient_deployment_identity(tmp_path, monkeypatch):
    monkeypatch.setenv("EXASERVE_DEPLOYMENT_ID", "forged-ambient-id")
    root = _root(tmp_path)
    assert os.environ["EXASERVE_DEPLOYMENT_ID"] == root.plan.deployment_id


def _give_test_gateway_listener(root):
    """Unit children do not consume the socket, but production always hands one."""
    from exaserve.state.ports import bind_listener

    root._gateway_listener = bind_listener(0, host="127.0.0.1")


# -- no shell lifecycle adapter ---------------------------------------------


def test_the_retired_shell_adapter_is_not_packaged():
    from importlib import resources

    assert not (resources.files("exaserve") / "resources" / "launch_cluster.sh").is_file()


# -- fail-closed listener ----------------------------------------------------


def test_listener_failure_launches_no_ranks(tmp_path, monkeypatch):
    """A run nobody can observe is not a run worth starting."""
    root = _root(tmp_path)
    root.bind_allocation(["n0", "n1"], "job1")

    import exaserve.control.channel_runtime as cr

    def _boom(**_kw):
        raise OSError("address in use")

    monkeypatch.setattr(cr, "HeadChannel", _boom)
    with pytest.raises(CompositionError, match="refusing to launch ranks"):
        root.bind_control_listener(retries=2, backoff_s=0.01)
    assert root.head_channel is None


# -- allocation binding ------------------------------------------------------


def test_the_binding_is_persisted_atomically(tmp_path):
    root = _root(tmp_path)
    binding = root.bind_allocation(["n0", "n1"], "job42")
    data = json.loads((tmp_path / "allocation_binding.json").read_text())
    assert data["allocation_binding_hash"] == binding.allocation_binding_hash
    assert data["rank_to_node"] == [[0, "n0"], [1, "n1"]]
    assert not list(tmp_path.glob("*.tmp*"))


def test_a_node_count_mismatch_refuses_to_bind(tmp_path):
    from exaserve.plan.contracts import PlanError

    root = _root(tmp_path, nodes=4)
    with pytest.raises(PlanError, match="different deployment"):
        root.bind_allocation(["n0", "n1"], "job1")


# -- staging as owned finite components --------------------------------------


def test_a_staging_step_that_exits_zero_without_its_result_fails(tmp_path):
    root = _root(tmp_path)
    step = StagingStep(
        name="distribute", argv=["/bin/true"], result_paths=(str(tmp_path / "never_written"),)
    )
    with pytest.raises(CompositionError, match="exited 0 but"):
        root.run_staging([step])


def test_a_staging_step_that_produces_its_result_succeeds(tmp_path):
    root = _root(tmp_path)
    marker = tmp_path / "manifest.json"
    step = StagingStep(
        name="stage", argv=["/bin/sh", "-c", f"echo x > {marker}"], result_paths=(str(marker),)
    )
    root.run_staging([step])
    assert marker.exists()


def test_a_nonzero_staging_step_fails(tmp_path):
    root = _root(tmp_path)
    with pytest.raises(CompositionError, match="exited 3"):
        root.run_staging([StagingStep(name="bad", argv=["/bin/sh", "-c", "exit 3"])])


def test_a_staging_step_has_a_bounded_deadline(tmp_path):
    root = _root(tmp_path)
    step = StagingStep(name="slow", argv=["/bin/sleep", "5"], deadline_s=0.3)
    with pytest.raises(CompositionError, match="deadline"):
        root.run_staging([step])


def test_staging_preserves_start_cause_and_reports_cleanup_failure(tmp_path, monkeypatch):
    from exaserve.control.supervisor import ManagedComponent

    root = _root(tmp_path)

    def fail_start(_component, **_kwargs):
        raise ValueError("injected start failure")

    def fail_stop(_component, *_args, **_kwargs):
        raise OSError("injected cleanup failure")

    monkeypatch.setattr(ManagedComponent, "start", fail_start)
    monkeypatch.setattr(ManagedComponent, "stop", fail_stop)
    with pytest.raises(CompositionError, match="injected start failure") as caught:
        root.run_staging([StagingStep(name="bad-start", argv=["/bin/true"])])

    assert any("cleanup also failed" in note for note in caught.value.__notes__)
    assert "staging/bad-start" not in root.supervisor.components


# -- gateway ownership -------------------------------------------------------


def test_the_head_owns_the_gateway_not_rank_zero(tmp_path):
    root = _root(tmp_path)
    root.bind_allocation(["n0", "n1"], "j")
    _give_test_gateway_listener(root)
    component = root.start_gateway(["/bin/sleep", "30"])
    try:
        assert component is not None
        assert component.component_id == "gateway/haproxy"
        assert component.owner_scope == "GLOBAL"
        assert root.gateway_alive() is True
    finally:
        root.shutdown(drain_s=5)


def test_gateway_start_validation_failure_releases_every_owned_resource(tmp_path, monkeypatch):
    from exaserve.control.supervisor import ManagedComponent

    root = _root(tmp_path)
    root.bind_allocation(["n0", "n1"], "j")
    _give_test_gateway_listener(root)
    listener = root._gateway_listener

    def fail_start(_component, **_kwargs):
        raise ValueError("injected invalid process contract")

    monkeypatch.setattr(ManagedComponent, "start", fail_start)
    with pytest.raises(ValueError, match="injected invalid"):
        root.start_gateway(["/bin/true"])
    assert listener.fileno() == -1
    assert root._gateway_listener is None
    assert "gateway/haproxy" not in root.supervisor.components


@pytest.mark.parametrize("qualified", [False, True])
def test_gateway_manifest_records_execution_qualification_truthfully(
    tmp_path, monkeypatch, qualified
):
    import shutil
    from types import SimpleNamespace

    import exaserve.control.finite_process as finite_process
    import exaserve.state.ports as ports

    real_bind_listener = ports.bind_listener
    monkeypatch.setattr(shutil, "which", lambda _name: "/bin/true")
    monkeypatch.setattr(
        finite_process,
        "run_finite",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=0, stdout="", stderr=""),
    )
    monkeypatch.setattr(
        ports,
        "bind_listener",
        lambda _port, *, host: real_bind_listener(0, host=host),
    )
    root = CompositionRoot(
        plan=_plan(),
        generation=7,
        run_dir=str(tmp_path),
        log=lambda *_: None,
        production_qualified=qualified,
    )
    root.bind_allocation(["n0", "n1"], "j")
    try:
        assert root.gateway_argv(str(tmp_path))
        manifest = json.loads((tmp_path / "gateway_config_manifest.json").read_text())
        assert manifest["production_qualified"] is qualified
        assert manifest["port_handoff"] == "inherited_listening_fd"
    finally:
        if root._gateway_listener is not None:
            root._gateway_listener.close()
            root._gateway_listener = None


def test_the_advertised_endpoint_is_the_gateway_port(tmp_path):
    root = _root(tmp_path)
    assert root.advertised_endpoint("10.0.0.1") == "http://10.0.0.1:4001"


def test_validation_direct_advertises_the_serve_port(tmp_path):
    raw = {
        "num_nodes": 1,
        "validation_mode": True,
        "exposure": {"mode": "DIRECT_VALIDATION", "serve_port": 8000},
        "models": [
            {"model_id": "a/b", "tensor_parallel_size": 1, "max_model_len": 4096, "size": 8}
        ],
    }
    plan = compile_deployment_plan(raw, site=_site(), deployment_id="d")
    root = CompositionRoot(plan=plan, generation=1, run_dir=str(tmp_path), log=lambda *_: None)
    assert root.advertised_endpoint("10.0.0.1") == "http://10.0.0.1:8000"
    assert root.start_gateway(["/bin/true"]) is None


# -- nodefile ----------------------------------------------------------------


def test_a_missing_explicit_nodefile_does_not_fall_back_to_pbs(tmp_path, monkeypatch):
    pbs_nodefile = tmp_path / "pbs-nodes"
    pbs_nodefile.write_text("fallback-node\n")
    monkeypatch.setenv("PBS_NODEFILE", str(pbs_nodefile))
    monkeypatch.setenv("EXASERVE_NODEFILE", "/nonexistent")
    with pytest.raises(CompositionError, match="no nodefile"):
        read_nodefile()


def test_the_nodefile_deduplicates_in_order(tmp_path, monkeypatch):
    path = tmp_path / "nodes"
    path.write_text("nB\nnA\nnB\nnC\n")
    monkeypatch.setenv("EXASERVE_NODEFILE", str(path))
    assert read_nodefile() == ["nB", "nA", "nC"]


def test_the_run_directory_is_propagated_to_ranks(tmp_path, monkeypatch):
    """Durable artifacts must land where consumers look, not in /tmp.

    The shell used to export EXASERVE_RUN_LOG_DIR; when the root took over the
    lifecycle without passing it on, the readiness snapshot was written to a
    scaling-trace fallback under /tmp and every consumer reported "not ready"
    for a deployment that was in fact READY.
    """
    root = _root(tmp_path)
    root.bind_allocation(["n0", "n1"], "j")
    root._head_address = "10.0.0.1"
    component = root.launch_ranks(["/bin/true"], launch_prefix=["/bin/true"])
    try:
        assert component.env["EXASERVE_RUN_LOG_DIR"] == str(tmp_path)
        assert component.env["EXASERVE_HEAD_IP"]
        assert component.env["EXASERVE_PLAN_HASH"] == root.plan.deployment_plan_hash
        assert component.result_check is not None
        ok, why = component.result_check()
        assert not ok and "control channel" in why
    finally:
        root.shutdown(drain_s=5)


def test_rank_launcher_captures_authenticated_rank_failure_before_global_cause(
    tmp_path, monkeypatch
):
    root = _root(tmp_path)
    root.bind_allocation(["n0", "n1"], "j")
    root._head_address = "10.0.0.1"

    class TypedHeadChannel:
        def env(self, *, reachable_host):
            assert reachable_host == "10.0.0.1"
            return {}

        def rank_failure(self):
            return None

        def launcher_exit_evidence(self, code):
            return f"rank 1 component ray: exit=137; rank launcher exit={code}"

    root.head_channel = TypedHeadChannel()
    component = root.launch_ranks(["/bin/true"], launch_prefix=["/bin/true"])
    try:
        assert component.on_unexpected_exit is not None
        assert component.on_unexpected_exit(143) == (
            "rank 1 component ray: exit=137; rank launcher exit=143"
        )
    finally:
        # This narrow fake covers the launch boundary, not the shutdown
        # protocol exercised by the real HeadChannel tests.
        root.head_channel = None
        root.shutdown(drain_s=5)


def test_head_address_is_derived_from_binding_not_ambient_env(tmp_path, monkeypatch):
    from exaserve import site as site_module

    root = _root(tmp_path)
    root.bind_allocation(["bound-head", "bound-worker"], "j")
    monkeypatch.setenv("EXASERVE_HEAD_IP", "203.0.113.99")
    seen = []

    def resolve(node, *, site_id):
        seen.append((node, site_id))
        return "10.2.3.4"

    monkeypatch.setattr(site_module, "resolve_allocation_node_address", resolve)
    assert root.head_address() == "10.2.3.4"
    assert seen == [("bound-head", "s")]


def test_staging_steps_are_owned_with_deadlines_and_results(tmp_path):
    """The shell owned staging; the root owns it as finite components."""
    root = _root(tmp_path)
    root.bind_allocation(["n0", "n1"], "job1")
    steps = root.default_staging_steps("/tmp/deployment.plan.json", python_exec="/usr/bin/python3")
    names = [s.name for s in steps]
    assert "model_bcast" in names and "distribute_source" in names
    for step in steps:
        assert step.deadline_s > 0, f"{step.name} has no deadline"
        assert isinstance(step.argv, list) and step.argv, "argv vector required"
        assert not any(";" in str(a) or "|" in str(a) for a in step.argv), (
            "staging must not build shell strings"
        )
    bcast = next(s for s in steps if s.name == "model_bcast")
    assert bcast.result_paths, "model staging must declare a result manifest"
    assert "--plan" in bcast.argv and "--config" not in bcast.argv


def test_null_compute_skips_model_staging(tmp_path, monkeypatch):
    monkeypatch.setenv("EXASERVE_NULL_COMPUTE", "0")
    root = CompositionRoot(
        plan=_plan(runtime={"null_compute": True}),
        generation=7,
        run_dir=str(tmp_path),
        log=lambda *_: None,
    )
    root.bind_allocation(["n0", "n1"], "job1")
    names = [s.name for s in root.default_staging_steps("/tmp/c.yaml")]
    assert "model_bcast" not in names and "distribute_source" in names


def test_a_requested_shutdown_exits_143_not_1(tmp_path):
    """A SIGTERM-driven shutdown is not a generic fault; flattening it to 1
    loses the distinction the supervisor already makes."""
    root = _root(tmp_path)
    root.supervisor.request_shutdown("signal 15")
    root.fail(str(root.supervisor.first_cause))
    assert root.exit_code() == 143


def test_a_real_failure_still_exits_nonzero_and_not_143(tmp_path):
    root = _root(tmp_path)
    root.fail("staging step 'distribute' exited 3")
    assert root.exit_code() not in (0, 143)


def test_an_unexpected_child_sigterm_is_not_an_operator_shutdown(tmp_path):
    """143 is reserved for a typed SHUTDOWN_REQUESTED cause.

    A supervised component killed with SIGTERM has the same shell-visible
    numeric status, but remains a deployment failure.
    """
    root = _root(tmp_path)
    root.supervisor.record_cause(
        "gateway/haproxy",
        "UNEXPECTED_EXIT",
        "gateway process_dead; signal=15",
        exit_code=-15,
    )
    root.fail(str(root.supervisor.first_cause))
    assert root.exit_code() == 1


def test_pre_ready_cancellation_is_published_only_after_clean_cleanup(tmp_path):
    root = _root(tmp_path)
    root.bind_allocation(["n0", "n1"], "job1")
    root.shutdown(drain_s=1)
    assert root.status.state == DeploymentState.CANCELLED.value
    report = json.loads((tmp_path / "shutdown_report.json").read_text())
    assert report["clean"] is True and report["errors"] == []
    assert report["terminal_publication"] == "published"
    assert report["observed_terminal_state"] == DeploymentState.CANCELLED.value


def test_pre_ready_cleanup_failure_is_failed_not_cleanly_cancelled(tmp_path, monkeypatch):
    root = _root(tmp_path)
    root.bind_allocation(["n0", "n1"], "job1")
    monkeypatch.setattr(root.supervisor, "shutdown", lambda **_kwargs: False)
    root.shutdown(drain_s=1)
    assert root.status.state == DeploymentState.FAILED.value
    report = json.loads((tmp_path / "shutdown_report.json").read_text())
    assert report["clean"] is False
    assert any("process groups" in item for item in report["errors"])
    assert report["observed_terminal_state"] == DeploymentState.FAILED.value


def test_fatal_rank_loss_does_not_require_ack_from_the_lost_session(tmp_path, monkeypatch):
    """A dead rank must still be reaped, but cannot acknowledge its own death."""
    root = _root(tmp_path)
    root.bind_allocation(["n0", "n1"], "job1")
    root.supervisor.record_cause(
        "rank_launcher", "UNEXPECTED_EXIT", "rank 1 Ray worker exited", exit_code=1
    )
    root.fail(str(root.supervisor.first_cause))
    observed = {}

    class LostRankChannel:
        def start_broadcast(self):
            return True

        def broadcast_shutdown(self, *_args, **_kwargs):
            observed["broadcast_deadline"] = _kwargs["deadline"]
            return 0

        def wait_shutdown_goodbyes(self, **_kwargs):
            observed["goodbye_deadline"] = _kwargs["deadline"]
            return 0

        def stop(self, **_kwargs):
            return True

    root.head_channel = LostRankChannel()

    def clean_shutdown(**kwargs):
        observed["supervisor_deadline"] = kwargs["deadline"]
        return True

    monkeypatch.setattr(root.supervisor, "shutdown", clean_shutdown)
    started = time.monotonic()
    root.shutdown(drain_s=100)

    report = json.loads((tmp_path / "shutdown_report.json").read_text())
    assert report["clean"] is True
    assert report["errors"] == []
    assert report["terminal_publication"] == "not_required"
    assert report["observed_terminal_state"] == DeploymentState.FAILED.value
    assert observed["broadcast_deadline"] == observed["goodbye_deadline"]
    assert observed["broadcast_deadline"] <= started + 5.1
    assert observed["supervisor_deadline"] >= started + 99.9


def test_healthy_shutdown_drains_global_dependencies_before_rank_local_ray(tmp_path, monkeypatch):
    """Ingress closes, Serve drains with live Ray, then ranks receive DRAIN."""
    root = _root(tmp_path)
    root.bind_allocation(["n0", "n1"], "job1")
    order = []

    class OrderedComponent:
        def __init__(self, component_id):
            self.component_id = component_id
            self.state = "RUNNING"
            self.process = None

        def stop(self, reason, *, deadline):
            assert deadline <= shutdown_deadline
            order.append((self.component_id, reason))
            self.state = "STOPPED"

    class OrderedChannel:
        def start_broadcast(self):
            return True

        def broadcast_shutdown(self, *_args, **_kwargs):
            order.append(("ranks", "DRAIN"))
            return 2

        def wait_shutdown_goodbyes(self, **_kwargs):
            order.append(("ranks", "GOODBYE"))
            return 2

        def stop(self, **_kwargs):
            return True

    gateway = OrderedComponent("gateway/haproxy")
    deployment = OrderedComponent("deployment")
    root.gateway_component = gateway
    root.deployment_component = deployment
    root.supervisor.components = {
        gateway.component_id: gateway,
        deployment.component_id: deployment,
    }
    root.head_channel = OrderedChannel()
    started = time.monotonic()
    shutdown_deadline = started + 5.1

    def finish_reap(**kwargs):
        order.append(("supervisor", "final-reap"))
        return True

    monkeypatch.setattr(root.supervisor, "shutdown", finish_reap)
    root.shutdown(drain_s=5)

    assert [item[0] for item in order] == [
        "gateway/haproxy",
        "deployment",
        "ranks",
        "ranks",
        "supervisor",
    ]
    report = json.loads((tmp_path / "shutdown_report.json").read_text())
    assert report["clean"] is True
    assert report["components"]["deployment"]["state"] == "STOPPED"


def test_rank_control_loss_skips_dependency_drain_that_requires_live_ray(tmp_path, monkeypatch):
    root = _root(tmp_path)
    root.bind_allocation(["n0", "n1"], "job1")
    root.supervisor.record_cause("control", "CONTROL_FAILURE", "rank 1 lost")
    root.fail(str(root.supervisor.first_cause))
    ordered_stops = []

    class ShouldNotStopEarly:
        component_id = "deployment"
        state = "RUNNING"
        process = None

        def stop(self, *_args, **_kwargs):
            ordered_stops.append("deployment")

    class LostRankChannel:
        def start_broadcast(self):
            return True

        def broadcast_shutdown(self, *_args, **_kwargs):
            ordered_stops.append("broadcast")
            return 0

        def wait_shutdown_goodbyes(self, **_kwargs):
            return 0

        def stop(self, **_kwargs):
            return True

    deployment = ShouldNotStopEarly()
    root.deployment_component = deployment
    root.supervisor.components = {"deployment": deployment}
    root.head_channel = LostRankChannel()
    monkeypatch.setattr(root.supervisor, "shutdown", lambda **_kwargs: True)
    root.shutdown(drain_s=1)

    assert ordered_stops == ["broadcast"]


def test_non_rank_failure_still_requires_every_drain_ack(tmp_path, monkeypatch):
    root = _root(tmp_path)
    root.bind_allocation(["n0", "n1"], "job1")

    class MissingAckChannel:
        def start_broadcast(self):
            return True

        def broadcast_shutdown(self, *_args, **_kwargs):
            return 0

        def wait_shutdown_goodbyes(self, **_kwargs):
            return 0

        def stop(self, **_kwargs):
            return True

    root.head_channel = MissingAckChannel()
    monkeypatch.setattr(root.supervisor, "shutdown", lambda **_kwargs: True)
    root.shutdown(drain_s=1)

    report = json.loads((tmp_path / "shutdown_report.json").read_text())
    assert report["clean"] is False
    assert any("rank drain protocol incomplete" in item for item in report["errors"])


def test_terminal_publication_failure_is_not_reported_as_clean(tmp_path, monkeypatch):
    root = _root(tmp_path)
    root.bind_allocation(["n0", "n1"], "job1")
    real_advance = root.status.advance

    def fail_terminal(target, **kwargs):
        if target in {DeploymentState.CANCELLED, DeploymentState.STOPPED}:
            raise RuntimeError("injected terminal status failure")
        return real_advance(target, **kwargs)

    monkeypatch.setattr(root.status, "advance", fail_terminal)
    root.shutdown(drain_s=1)
    report = json.loads((tmp_path / "shutdown_report.json").read_text())
    assert report["clean"] is False
    assert report["terminal_publication"].startswith("failed:")
    assert any("terminal status publication" in item for item in report["errors"])


def test_the_advertised_endpoint_is_established_before_readiness(tmp_path):
    """§3.2.1 Q3: VALIDATING establishes the endpoint that readiness verifies."""
    root = _root(tmp_path)
    root.bind_allocation(["n0", "n1"], "j")

    class _Receipts:
        def satisfied(self):
            return True, {"planned": 6, "accepted": 6, "missing": [], "unexpected": []}

    root.receipts = _Receipts()
    root.status.advance_through(
        DeploymentState.STAGING,
        DeploymentState.CLUSTER_STARTING,
        DeploymentState.DEPLOYING,
        reason_code="TEST_SETUP",
    )
    endpoint = root.establish_advertised_endpoint("10.0.0.1")
    assert endpoint == "http://10.0.0.1:4001"
    assert root.readiness.phase == "VALIDATING"
    assert root.readiness.advertised_endpoint == endpoint


def test_initial_readiness_waits_for_independent_evidence_streams(tmp_path, monkeypatch):
    root = _root(tmp_path)

    class ConvergingReadiness:
        phase = "VALIDATING"
        blockers = ("serve proxy worker: status=STARTING",)

        def __init__(self):
            self.calls = 0

        def set_gateway(self, **_kwargs):
            pass

        def set_canary(self, *_args):
            pass

        def evaluate(self):
            self.calls += 1
            ready = self.calls >= 2
            return SimpleNamespace(
                ready=ready,
                blockers=() if ready else self.blockers,
            )

    readiness = ConvergingReadiness()
    root.readiness = readiness
    monkeypatch.setattr(root, "gateway_alive", lambda: True)
    monkeypatch.setattr(root, "gateway_health_check", lambda **_kwargs: True)
    monkeypatch.setattr(root, "apply_deployment_evidence", lambda: {})
    monkeypatch.setattr(root, "canary_advertised_endpoint", lambda _model, **_kwargs: (True, "ok"))

    verdict = root.await_initial_readiness(timeout_s=0.1, poll_s=0.001)
    assert verdict.ready
    assert readiness.calls == 2, "a transient partial observation was treated as terminal"


def test_initial_readiness_has_a_typed_bounded_failure(tmp_path, monkeypatch):
    root = _root(tmp_path)

    class NeverReady:
        phase = "VALIDATING"

        def set_gateway(self, **_kwargs):
            pass

        def set_canary(self, *_args):
            pass

        def evaluate(self):
            return SimpleNamespace(ready=False, blockers=("worker proxy missing",))

    root.readiness = NeverReady()
    monkeypatch.setattr(root, "gateway_alive", lambda: True)
    monkeypatch.setattr(root, "gateway_health_check", lambda **_kwargs: True)
    monkeypatch.setattr(root, "apply_deployment_evidence", lambda: {})
    monkeypatch.setattr(root, "canary_advertised_endpoint", lambda _model, **_kwargs: (True, "ok"))

    with pytest.raises(CompositionError, match="within 0.01s.*worker proxy missing"):
        root.await_initial_readiness(timeout_s=0.01, poll_s=0.002)


def test_operator_shutdown_aborts_initial_readiness_without_waiting_for_deadline(
    tmp_path, monkeypatch
):
    root = _root(tmp_path)

    class NeverReady:
        phase = "VALIDATING"

        def set_gateway(self, **_kwargs):
            pass

        def set_canary(self, *_args):
            pass

        def evaluate(self):
            return SimpleNamespace(ready=False, blockers=("worker proxy missing",))

    root.readiness = NeverReady()
    monkeypatch.setattr(root, "gateway_alive", lambda: True)
    monkeypatch.setattr(root, "gateway_health_check", lambda **_kwargs: True)
    monkeypatch.setattr(root, "apply_deployment_evidence", lambda: {})
    monkeypatch.setattr(root, "canary_advertised_endpoint", lambda _model, **_kwargs: (True, "ok"))
    root.supervisor.request_shutdown("signal 15")

    started = time.monotonic()
    with pytest.raises(CompositionError, match="initial readiness interrupted.*SHUTDOWN_REQUESTED"):
        root.await_initial_readiness(timeout_s=30, poll_s=1)
    assert time.monotonic() - started < 1


def test_staging_shutdown_reaps_the_owned_finite_process(tmp_path, monkeypatch):
    root = _root(tmp_path)
    root.bind_allocation(["n0", "n1"], "job1")
    registered = []
    real_register = root.supervisor.register

    def capture(component):
        registered.append(component)
        return real_register(component)

    monkeypatch.setattr(root.supervisor, "register", capture)
    timer = threading.Timer(0.1, lambda: root.supervisor.request_shutdown("signal 15"))
    timer.start()
    started = time.monotonic()
    try:
        with pytest.raises(CompositionError, match="staging step 'slow' interrupted"):
            root.run_staging([StagingStep(name="slow", argv=["/bin/sleep", "30"], deadline_s=30)])
    finally:
        timer.join()
    assert time.monotonic() - started < 3
    assert len(registered) == 1
    assert registered[0].process is not None
    assert registered[0].process.poll() is not None
    assert registered[0].state == "STOPPED"
    assert root.supervisor.components == {}


@pytest.mark.parametrize("value", [True, 0, -1, float("nan"), float("inf")])
def test_initial_readiness_rejects_invalid_deadlines(tmp_path, value):
    root = _root(tmp_path)
    root.readiness = SimpleNamespace(phase="VALIDATING")
    with pytest.raises(ValueError, match="positive/finite"):
        root.await_initial_readiness(timeout_s=value, poll_s=1)
    with pytest.raises(ValueError, match="positive/finite"):
        root.await_initial_readiness(timeout_s=1, poll_s=value)


def test_a_dead_gateway_after_ready_is_terminal(tmp_path):
    root = _root(tmp_path)
    root.bind_allocation(["n0", "n1"], "j")

    class _Receipts:
        def satisfied(self):
            return True, {"planned": 6, "accepted": 6, "missing": [], "unexpected": []}

    root.receipts = _Receipts()
    root.status.advance_through(
        DeploymentState.STAGING,
        DeploymentState.CLUSTER_STARTING,
        DeploymentState.DEPLOYING,
        reason_code="TEST_SETUP",
    )
    root.establish_advertised_endpoint("10.0.0.1")
    # Gateway component that has already exited.
    _give_test_gateway_listener(root)
    component = root.start_gateway(["/bin/true"])
    assert component is not None
    import time

    for _ in range(50):
        if root.gateway_alive() is False:
            break
        time.sleep(0.1)
    from exaserve.launcher import _deployment_done_or_failed

    root.head_channel = SimpleNamespace(poll=lambda: None)
    assert _deployment_done_or_failed(root)
    assert root.readiness.phase == "FAILED"
    assert root.supervisor.first_cause is not None
    assert root.supervisor.first_cause.component_id == "gateway"
    assert root.supervisor.first_cause.reason_code == "GATEWAY_FAILURE"
    assert "gateway process exited" in root.supervisor.first_cause.detail
    evidence = json.loads((tmp_path / "gateway_failure.json").read_text())
    assert evidence["classification"] == "process_dead"
    assert evidence["exit_code"] == 0
    assert evidence["signal"] is None
    root.head_channel = None
    root.shutdown(drain_s=5)


def test_ambient_copper_flag_cannot_add_an_unplanned_runtime_component(tmp_path, monkeypatch):
    """Copper is not in DeploymentPlan, so ambient state cannot launch it."""
    monkeypatch.setenv("EXASERVE_AURORA_USE_COPPER", "1")
    root = _root(tmp_path)
    assert not hasattr(root, "start_copper")
    assert "copper" not in root.supervisor.components


def test_composition_root_has_no_shell_owned_result_collection(tmp_path):
    """Each rank publishes bounded diagnostics; the root owns no EXIT script."""
    root = _root(tmp_path)
    assert not hasattr(root, "collect_results")


def test_python_launcher_sanitizes_retired_overlay_paths(monkeypatch):
    from exaserve.launcher import _sanitize_child_pythonpath

    monkeypatch.setenv(
        "PYTHONPATH",
        "/tmp/exaserve_src.1:/kept:/tmp/exaserve_overlay:/tmp/exaserve_src:/also-kept",
    )
    _sanitize_child_pythonpath()
    assert __import__("os").environ["PYTHONPATH"] == "/kept:/also-kept"


def test_multi_replica_application_evidence_requires_every_sibling(tmp_path):
    root = _root(tmp_path)
    model = root.plan.models[0]
    applications = {
        f"{model.route_name}_r{index}": {
            "running": 1,
            "target": 1,
            "route_prefix": f"/{model.route_name}_r{index}",
            "status": "RUNNING",
            "_observation_state": "READY",
        }
        for index in range(model.num_replicas)
    }
    complete = root.application_for_model(model, applications)
    assert complete["_observation_state"] == "READY"
    applications.pop(f"{model.route_name}_r{model.num_replicas - 1}")
    incomplete = root.application_for_model(model, applications)
    assert incomplete["_observation_state"] == "STARTING"
    assert incomplete["running"] == model.num_replicas - 1
