"""Mock backend adapter for testing the eval control plane without a real cluster.

Returns a dummy endpoint (127.0.0.1:65535) and immediately signals ready.
Used by test_eval_control_plane.py and for dry-run validation of the
materialize -> execute pipeline.
"""

from __future__ import annotations

from .base import (
    BackendAdapter,
    BackendProcessHandle,
    BackendRunContext,
    LaunchedBackend,
    RuntimeEnvSpec,
)


class MockBackendAdapter(BackendAdapter):
    name = "mock"

    def validate(self, run_plan) -> None:
        return

    def build_runtime_manifest(self, run_plan) -> str:
        from exaserve.state.atomic import atomic_create_yaml

        atomic_create_yaml(
            run_plan.runtime_manifest_path,
            {
                "mock": True,
                "run_id": run_plan.run_id,
                "spec_name": run_plan.spec_name,
            },
        )
        return run_plan.runtime_manifest_path

    def runtime_env(self, run_plan) -> RuntimeEnvSpec:
        return RuntimeEnvSpec(env_script="~/script/env_aurora")

    def launch(self, run_ctx: BackendRunContext) -> LaunchedBackend:
        monitor = BackendProcessHandle(
            process=None, log_path=f"{run_ctx.run_plan.bundle.logs_dir}/mock_backend.log"
        )
        return LaunchedBackend(monitor=monitor)

    def wait_ready(self, run_ctx: BackendRunContext, launched: LaunchedBackend) -> None:
        return

    def discover_targets(self, run_ctx: BackendRunContext, launched: LaunchedBackend) -> list[str]:
        return ["http://127.0.0.1:65535"]

    def stop(self, run_ctx: BackendRunContext, launched: LaunchedBackend) -> None:
        return
