"""WP4 / AC-SUP-01 (audit IMP-B03): essential-child supervision + first cause."""

from __future__ import annotations

import os
import subprocess
import sys
import time


from exaserve.control.supervisor import ManagedComponent, RuntimeSupervisor

PY = sys.executable


def _sleeper(seconds=300):
    return [PY, "-c", f"import time; time.sleep({seconds})"]


def _exits(code, after=0.1):
    return [PY, "-c", f"import time,sys; time.sleep({after}); sys.exit({code})"]


def _spawns_child_then_sleeps():
    # parent spawns a grandchild, both sleep: proves group cleanup
    return [PY, "-c",
            "import subprocess,sys,time;"
            f"subprocess.Popen([{PY!r},'-c','import time; time.sleep(300)']);"
            "time.sleep(300)"]


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
        def stop(self, reason="shutdown", deadline_s=30.0):
            raise RuntimeError("cleanup blew up")

    sup.components["boom"] = Exploding("boom", _sleeper())
    sup.shutdown()
    assert sup.first_cause.component_id == "proxy", "first cause was overwritten"
    assert sup.exit_code() == 7


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
    sup.shutdown(drain_s=5)     # second call must be a no-op
    assert time.monotonic() - t0 < 20


def test_finite_component_with_invalid_result_fails():
    """A finite child succeeds only after zero exit AND a validated result."""
    sup = RuntimeSupervisor(poll_interval_s=0.05)
    sup.register(ManagedComponent(
        "stage", _exits(0), long_lived=False,
        result_check=lambda: (False, "expected artifact missing")))
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
