"""The operator CLI consumes only the canonical typed status artifact."""

from __future__ import annotations

from exaserve.plan.compiler import compile_deployment_plan
from exaserve.plan.contracts import build_allocation_binding
from exaserve.site import default_site_profile
from exaserve.state.status import DeploymentState
from exaserve.status_api import DeploymentStatusPublisher
from exaserve.status_cli import EXIT_INVALID, EXIT_NOT_READY, main


def _publisher(tmp_path):
    plan = compile_deployment_plan(
        {
            "num_nodes": 1,
            "validation_mode": True,
            "models": [
                {"model_id": "m", "tensor_parallel_size": 1, "max_model_len": 64, "size": 1}
            ],
        },
        site=default_site_profile(),
        deployment_id="status-cli",
    )
    binding = build_allocation_binding(
        plan=plan, generation=9, scheduler_allocation_id="job", nodes=["n0"]
    )
    publisher = DeploymentStatusPublisher(str(tmp_path), plan=plan, binding=binding, generation=9)
    publisher.initialize()
    return publisher, plan


def test_show_explains_nonready_state(tmp_path, capsys):
    publisher, _plan = _publisher(tmp_path)
    publisher.advance(DeploymentState.STAGING, reason_code="STAGING_SOURCE")
    assert main(["show", "--run-dir", str(tmp_path)]) == EXIT_NOT_READY
    output = capsys.readouterr().out
    assert "STAGING" in output and "generation=9" in output


def test_wait_rejects_a_stale_plan_identity(tmp_path):
    _publisher(tmp_path)
    assert (
        main(
            [
                "wait",
                "--run-dir",
                str(tmp_path),
                "--generation",
                "9",
                "--plan-hash",
                "f" * 64,
                "--timeout",
                "0.01",
            ]
        )
        == EXIT_INVALID
    )


def test_wait_stops_immediately_on_terminal_state(tmp_path, capsys):
    publisher, plan = _publisher(tmp_path)
    publisher.advance(DeploymentState.FAILED, reason_code="CHILD_EXIT", detail="ray died")
    assert (
        main(
            [
                "wait",
                "--run-dir",
                str(tmp_path),
                "--generation",
                "9",
                "--plan-hash",
                plan.deployment_plan_hash,
                "--timeout",
                "30",
            ]
        )
        == EXIT_NOT_READY
    )
    assert "CHILD_EXIT" in capsys.readouterr().out
