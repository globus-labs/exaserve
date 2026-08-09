"""WP4 / AC-SUP-01 (audit IMP-B03): essential-child supervision + first cause."""

from __future__ import annotations

import os
import subprocess
import sys
import time

import pytest


from exaserve.control.supervisor import (
    BoundedOutputCapture,
    ManagedComponent,
    RuntimeSupervisor,
)

PY = sys.executable


def _sleeper(seconds=300):
    return [PY, "-c", f"import time; time.sleep({seconds})"]


def _exits(code, after=0.1):
    return [PY, "-c", f"import time,sys; time.sleep({after}); sys.exit({code})"]


def _spawns_child_then_sleeps():
    # parent spawns a grandchild, both sleep: proves group cleanup
    return [
        PY,
        "-c",
        "import subprocess,sys,time;"
        f"subprocess.Popen([{PY!r},'-c','import time; time.sleep(300)']);"
        "time.sleep(300)",
    ]


def test_managed_component_execution_contract_never_coerces_argv_or_environment():
    with pytest.raises(ValueError, match="argument vector"):
        ManagedComponent("child", "python")
    with pytest.raises(ValueError, match="string mapping"):
        ManagedComponent("child", [PY, "-c", "pass"], env={"COUNT": 1})

    environment = {"MODE": "planned"}
    component = ManagedComponent("child", [PY, "-c", "pass"], env=environment)
    environment["MODE"] = "mutated"
    assert component.env["MODE"] == "planned"
    component.argv = "python"
    with pytest.raises(ValueError, match="argument vector"):
        component.start()


def test_managed_component_cannot_be_started_twice():
    component = ManagedComponent("child", [PY, "-c", "pass"], long_lived=False)
    component.start()
    component.wait(time.monotonic() + 5)
    with pytest.raises(Exception, match="already been started"):
        component.start()


def test_unexpected_exit_zero_is_fatal_for_long_lived_component():
    """IMP-B03: the legacy loop treated an exit-0 disappearance as success."""
    sup = RuntimeSupervisor(poll_interval_s=0.05)
    sup.register(ManagedComponent("serve", _exits(0), long_lived=True))
    sup.start_all()
    cause = sup.supervise(timeout_s=15)
    assert cause is not None
    assert cause.component_id == "serve"
    assert cause.reason_code == "UNEXPECTED_EXIT"
    assert sup.exit_code() != 0, "exit-0 disappearance must not be success"
    sup.shutdown()


def test_ray_head_is_polled_not_just_serve():
    """IMP-B03: the Ray head was started but never polled."""
    sup = RuntimeSupervisor(poll_interval_s=0.05)
    sup.register(ManagedComponent("ray-head", _exits(3)))
    sup.register(ManagedComponent("serve", _sleeper()))
    sup.start_all()
    cause = sup.supervise(timeout_s=15)
    assert cause is not None and cause.component_id == "ray-head"
    assert sup.exit_code() == 3
    sup.shutdown()


def test_first_cause_survives_later_cleanup_errors():
    sup = RuntimeSupervisor(poll_interval_s=0.05)
    sup.register(ManagedComponent("proxy", _exits(7)))
    sup.start_all()
    cause = sup.supervise(timeout_s=15)
    assert cause.component_id == "proxy" and cause.exit_code == 7

    class Exploding(ManagedComponent):
        def stop(self, reason="shutdown", deadline_s=30.0, *, deadline=None):
            raise RuntimeError("cleanup blew up")

    sup.components["boom"] = Exploding("boom", _sleeper())
    sup.shutdown()
    assert sup.first_cause.component_id == "proxy", "first cause was overwritten"
    assert any(cause.component_id == "boom" for cause in sup.secondary_causes)
    assert sup.exit_code() == 7


def test_startup_is_transactional_and_rolls_back_started_children():
    """A later start failure cannot leak an earlier process group."""
    sup = RuntimeSupervisor(poll_interval_s=0.01)
    first = sup.register(ManagedComponent("first", _sleeper()))
    sup.register(ManagedComponent("missing", ["/definitely/not/a/program"]))

    with pytest.raises(Exception, match="startup transaction"):
        sup.start_all(rollback_s=5)

    assert first.process is not None
    assert first.process.poll() is not None
    assert first.state == "STOPPED"
    assert sup.first_cause is not None
    assert sup.first_cause.reason_code == "START_FAILED"


def test_startup_failure_and_sibling_rollback_share_one_deadline():
    seen = {"start": [], "stop": []}

    class Recording(ManagedComponent):
        def start(self, *, rollback_deadline=None):
            seen["start"].append(rollback_deadline)
            self.state = "RUNNING"
            if self.component_id == "second":
                self.state = "FAILED"
                raise RuntimeError("injected post-spawn failure")
            return {"pid": 1, "pgid": 1}

        def stop(self, reason="shutdown", deadline_s=30.0, *, deadline=None):
            seen["stop"].append(deadline)
            self.state = "STOPPED"

    sup = RuntimeSupervisor()
    sup.register(Recording("first", _sleeper()))
    sup.register(Recording("second", _sleeper()))
    with pytest.raises(Exception, match="startup transaction"):
        sup.start_all(rollback_s=5.0)
    assert len(seen["start"]) == 2
    assert seen["start"][-1] is not None
    assert seen["stop"] == [seen["start"][-1], seen["start"][-1]]


def test_shutdown_passes_one_absolute_deadline_to_every_component(monkeypatch):
    seen = []

    class Recording(ManagedComponent):
        def stop(self, reason="shutdown", deadline_s=30.0, *, deadline=None):
            seen.append(deadline)
            self.state = "STOPPED"

    sup = RuntimeSupervisor()
    sup.register(Recording("a", _sleeper()))
    sup.register(Recording("b", _sleeper()))
    assert sup.shutdown(drain_s=3)
    assert len(seen) == 2
    assert seen[0] is not None and seen[0] == seen[1]


def test_stop_kills_the_whole_process_group():
    """IMP-B03: descendants previously escaped cleanup."""
    sup = RuntimeSupervisor(poll_interval_s=0.05)
    comp = sup.register(ManagedComponent("tree", _spawns_child_then_sleeps()))
    comp.start()
    time.sleep(1.0)
    pgid = os.getpgid(comp.process.pid)
    # the grandchild shares the group
    out = subprocess.run(["pgrep", "-g", str(pgid)], capture_output=True, text=True)
    assert len(out.stdout.split()) >= 2, "expected parent + grandchild in the group"
    comp.stop(deadline_s=5)
    time.sleep(0.5)
    out2 = subprocess.run(["pgrep", "-g", str(pgid)], capture_output=True, text=True)
    survivors = [p for p in out2.stdout.split() if p.strip()]
    assert not survivors, f"process group survived cleanup: {survivors}"


def test_shutdown_is_idempotent_and_bounded():
    sup = RuntimeSupervisor(poll_interval_s=0.05)
    sup.register(ManagedComponent("a", _sleeper()))
    sup.start_all()
    t0 = time.monotonic()
    sup.shutdown(drain_s=5)
    sup.shutdown(drain_s=5)  # second call must be a no-op
    assert time.monotonic() - t0 < 20


def test_finite_component_with_invalid_result_fails():
    """A finite child succeeds only after zero exit AND a validated result."""
    sup = RuntimeSupervisor(poll_interval_s=0.05)
    sup.register(
        ManagedComponent(
            "stage",
            _exits(0),
            long_lived=False,
            result_check=lambda: (False, "expected artifact missing"),
        )
    )
    sup.start_all()
    cause = sup.supervise(timeout_s=15)
    assert cause is not None and cause.reason_code == "INVALID_RESULT"
    sup.shutdown()


def test_shutdown_request_yields_143_not_success():
    sup = RuntimeSupervisor(poll_interval_s=0.05)
    sup.register(ManagedComponent("a", _sleeper()))
    sup.start_all()
    sup.request_shutdown("signal 15")
    sup.supervise(timeout_s=5)
    sup.shutdown()
    assert sup.exit_code() == 143


def test_clean_completion_is_exit_zero():
    sup = RuntimeSupervisor(poll_interval_s=0.05)
    sup.register(ManagedComponent("a", _sleeper()))
    sup.start_all()
    done = {"v": False}

    def until():
        done["v"] = True
        return True

    assert sup.supervise(until=until, timeout_s=5) is None
    sup.shutdown()
    assert sup.exit_code() == 0


def test_output_capture_drains_noisy_child_and_retains_a_bounded_tail():
    capture = BoundedOutputCapture(max_bytes=128)
    comp = ManagedComponent(
        "noisy",
        [PY, "-c", "print('x' * 10000); print('THE-END')"],
        output_capture=capture,
        long_lived=False,
    )
    comp.start()
    state, code = comp.wait(time.monotonic() + 10)
    snapshot = capture.snapshot()
    assert state == "STOPPED" and code == 0
    assert snapshot["total_bytes"] > snapshot["retained_bytes"]
    assert snapshot["retained_bytes"] <= 128
    assert snapshot["truncated"] is True
    assert "THE-END" in snapshot["tail"]
    assert snapshot["error"] is None


def test_output_capture_thread_failure_is_a_component_failure():
    capture = BoundedOutputCapture(max_bytes=128)
    comp = ManagedComponent("capture", _sleeper(), output_capture=capture)
    comp.start()
    capture._error = OSError("diagnostic sink broke")
    state, _code = comp.observe()
    assert state == "FAILED"
    assert capture.snapshot()["error"] == "OSError: diagnostic sink broke"
    with pytest.raises(Exception, match="output drain failed"):
        comp.stop(deadline_s=5)


def test_unexpected_exit_callback_runs_before_first_cause_assignment():
    observed = []

    def capture_before_cause(code):
        observed.append((code, sup.first_cause))
        return "durable evidence captured"

    sup = RuntimeSupervisor(poll_interval_s=0.01)
    sup.register(
        ManagedComponent("gateway", _exits(9, after=0.01), on_unexpected_exit=capture_before_cause)
    )
    sup.start_all()
    cause = sup.supervise(timeout_s=5)
    assert observed == [(9, None)]
    assert cause is not None and cause.detail == "durable evidence captured"
    sup.shutdown()


def test_signal_exit_is_normalized_to_shell_visible_status():
    sup = RuntimeSupervisor(poll_interval_s=0.01)
    sup.record_cause("child", "UNEXPECTED_EXIT", "signal", exit_code=-9)
    assert sup.exit_code() == 137


def test_post_spawn_callback_failure_rolls_back_the_process_group():
    def reject_ownership(_identity):
        raise RuntimeError("ownership receipt failed")

    comp = ManagedComponent("child", _sleeper(), on_started=reject_ownership)
    with pytest.raises(RuntimeError, match="ownership receipt failed"):
        comp.start()
    assert comp.process is not None and comp.process.poll() is not None
    assert comp.state == "FAILED"
