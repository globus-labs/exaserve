"""Ray backend adapter.

This adapter bridges the eval control plane to the canonical packaged ExaServe
composition root.

Key responsibilities:
  - build_runtime_manifest: materializes replay settings bound by hash to the
    already-compiled RunPlan and its exact DeploymentPlan/trace artifacts.
  - launch: starts the Python composition root as a child process group; diagnostics are
    tailed while readiness is read only from canonical DeploymentStatus.
  - runtime_env: selects the shell env used for the Ray/vLLM stack and sets
    launcher exports such as EXASERVE_NULL_COMPUTE. Compiled plan artifacts are
    the runtime source of truth; this adapter does not reinterpret them.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import asdict

from eval.site_config import get_site_config
from exaserve.exception_notes import add_exception_note

from ..manifest import (
    EvalManifest,
    ReplayClientConfig,
    TraceGeneratorConfig,
    WeakScalingConfig,
)

from ..models import RunMaterialization
from .base import (
    BackendAdapter,
    BackendRunContext,
    LaunchedBackend,
    BackendProcessHandle,
    RuntimeEnvSpec,
    terminate_process_tree,
)


_VENV_SITE_PACKAGES_MARKERS = ("/venv/", "/.venv/")
_DROP_ENV_KEYS = {
    "PYTHONHOME",
    "VIRTUAL_ENV",
    "CONDA_DEFAULT_ENV",
    "CONDA_PREFIX",
    "CONDA_PROMPT_MODIFIER",
    "_CE_CONDA",
    "_CE_M",
}


def _sanitize_pythonpath(value: str) -> str:
    entries: list[str] = []
    for raw_entry in value.split(os.pathsep):
        entry = raw_entry.strip()
        if not entry:
            continue
        if "/site-packages" in entry and any(
            marker in entry for marker in _VENV_SITE_PACKAGES_MARKERS
        ):
            continue
        entries.append(entry)
    return os.pathsep.join(entries)


def _sanitize_launch_env(env: dict[str, str]) -> dict[str, str]:
    clean_env: dict[str, str] = {}
    for key, value in env.items():
        if key.startswith("BASH_FUNC_"):
            continue
        if key in _DROP_ENV_KEYS:
            continue
        clean_env[key] = value

    pythonpath = _sanitize_pythonpath(clean_env.get("PYTHONPATH", ""))
    if pythonpath:
        clean_env["PYTHONPATH"] = pythonpath
    else:
        clean_env.pop("PYTHONPATH", None)

    return clean_env


class RayBackendAdapter(BackendAdapter):
    name = "ray"
    ready_timeout_s = 7200.0

    def validate(self, run_plan: RunMaterialization) -> None:
        if run_plan.scheduler.type != "pbs":
            raise ValueError("ray backend currently requires scheduler.type == 'pbs'")
        if run_plan.client.dest == "proxy":
            proxy_cfg = self._proxy_settings(run_plan)
            if proxy_cfg.get("type", "none") == "none":
                raise ValueError("client.dest=proxy requires backend.args.ray.proxy.type != 'none'")
        elif run_plan.client.dest == "direct" and len(run_plan.deployment.models) != 1:
            raise ValueError("client.dest=direct requires exactly one deployment model")
        launch_cfg = self._launch_settings(run_plan)
        if "ray_node_cpus" in launch_cfg:
            ray_node_cpus = launch_cfg["ray_node_cpus"]
            if isinstance(ray_node_cpus, bool) or not isinstance(ray_node_cpus, int):
                raise ValueError("backend.args.ray.launch.ray_node_cpus must be an integer")
            if ray_node_cpus < 1:
                raise ValueError("backend.args.ray.launch.ray_node_cpus must be >= 1")
        if "ray_head_port" in launch_cfg:
            ray_head_port = launch_cfg["ray_head_port"]
            if isinstance(ray_head_port, bool) or not isinstance(ray_head_port, int):
                raise ValueError("backend.args.ray.launch.ray_head_port must be an integer")
            if ray_head_port < 1:
                raise ValueError("backend.args.ray.launch.ray_head_port must be >= 1")

    def build_runtime_manifest(self, run_plan: RunMaterialization) -> str:
        trace_config = self._build_trace_config(run_plan)
        manifest = EvalManifest(
            pbs_result_dir=run_plan.bundle.results_dir,
            pbs_stdout_dir=run_plan.bundle.pbs_stdout_dir,
            pbs_stderr_dir=run_plan.bundle.pbs_stderr_dir,
            pbs_num_nodes=run_plan.scheduler.nodes,
            pbs_walltime=run_plan.scheduler.walltime,
            pbs_queue_name=run_plan.scheduler.queue,
            pbs_job_name=f"{run_plan.spec_name}_{run_plan.run_group_id}_{run_plan.variant_name}",
            pbs_working_dir=run_plan.bundle.runtime_dir,
            job_trace_config=trace_config,
            job_replay_client_config=ReplayClientConfig(
                config_path=run_plan.runtime_manifest_path,
                include_tp=run_plan.client.include_tp,
                early_stop=run_plan.client.early_stop,
                num_runs=run_plan.client.num_runs,
                generation_mode=run_plan.workload.generation_mode,
                dest=run_plan.client.dest,
                num_nodes=run_plan.client.num_nodes,
                num_go_procs=run_plan.client.num_go_procs,
                num_go_workers=run_plan.client.num_go_workers,
                go_concurrency=run_plan.client.go_concurrency,
                warmup_rps=run_plan.client.warmup_rps,
                warmup_duration_s=run_plan.client.warmup_duration_s,
                sum_only=run_plan.client.sum_only,
                stream=run_plan.client.stream,
                direct_dispatch=run_plan.client.direct_dispatch,
                dispatch_topologies=list(run_plan.client.dispatch_topologies),
                direct_pair_shift=run_plan.client.direct_pair_shift,
                request_timeout_s=run_plan.client.request_timeout_s,
                drain_wait_timeout_s=run_plan.client.drain_wait_timeout_s,
                shard_timeout_s=run_plan.client.shard_timeout_s,
                direct_target_ready_timeout_s=(run_plan.client.direct_target_ready_timeout_s),
                direct_target_probe_timeout_s=(run_plan.client.direct_target_probe_timeout_s),
                direct_target_interval_s=run_plan.client.direct_target_interval_s,
                direct_target_max_workers=run_plan.client.direct_target_max_workers,
                saturation=asdict(run_plan.client.saturation)
                if hasattr(run_plan.client, "saturation")
                else {},
            ),
            job_seed=run_plan.workload.seed,
            deployment_plan_path=run_plan.deployment_plan_path,
            deployment_plan_hash=run_plan.deployment_plan_hash,
            run_plan_path=run_plan.semantic_plan_path,
            run_semantic_hash=run_plan.run_semantic_hash,
            trace_content_hash=run_plan.semantic_plan.trace.trace_content_hash or "",
        )
        manifest.save_yaml(run_plan.runtime_manifest_path)
        return run_plan.runtime_manifest_path

    def runtime_env(self, run_plan: RunMaterialization) -> RuntimeEnvSpec:
        launch_settings = self._launch_settings(run_plan)
        env_script = launch_settings.get("env_script", "")
        if not isinstance(env_script, str):
            raise ValueError("backend.args.ray.launch.env_script must be text")
        env_script = env_script.strip()
        if not env_script:
            cfg = get_site_config()
            env_script = cfg.env_script_aurora
        # Plan semantics are projected by exaserve.plan.runtime_environment in
        # the launcher. The eval adapter contributes no competing ambient flags.
        return RuntimeEnvSpec(env_script=env_script, exports={})

    def launch(self, run_ctx: BackendRunContext) -> LaunchedBackend:
        run_plan = run_ctx.run_plan
        env = _sanitize_launch_env(os.environ.copy())
        runtime_env = self.runtime_env(run_plan)
        env.update(runtime_env.exports)
        # PYTHONPATH must include both the repo root (so 'from eval.X' resolves)
        # and repo_root/src (so 'from exaserve.X' resolves).
        env["PYTHONPATH"] = (
            run_plan.repo_root
            + os.pathsep
            + os.path.join(run_plan.repo_root, "src")
            + os.pathsep
            + env.get("PYTHONPATH", "")
        )
        deployment_run_dir = os.path.join(run_plan.bundle.logs_dir, "backend", "deployment")
        env["EXASERVE_RUN_LOG_DIR"] = deployment_run_dir
        generation = time.time_ns()
        env.update(
            {
                "EXASERVE_GENERATION": str(generation),
                "EXASERVE_RUN_ID": run_plan.run_id,
                "EXASERVE_RUN_SEMANTIC_HASH": run_plan.run_semantic_hash,
                "EXASERVE_SOURCE_SNAPSHOT_HASH": run_plan.source_snapshot_hash,
                "EXASERVE_SITE_PROFILE_PATH": run_plan.site_profile_path,
                "EXASERVE_RUN_PLAN_PATH": run_plan.semantic_plan_path,
                "EXASERVE_OUTPUT_LOCATIONS": run_plan.bundle.results_dir,
            }
        )
        # The batch environment is already prepared by the scheduler template;
        # invoke the Python composition root directly. The shell adapter remains
        # only for manual allocation entry, not as a hidden lifecycle layer.
        cmd = [sys.executable, "-u", "-m", "exaserve.launcher", run_plan.deployment_plan_path]
        process = subprocess.Popen(
            cmd,
            cwd=run_plan.repo_root,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            start_new_session=True,
        )
        monitor = BackendProcessHandle(
            process=process,
            log_path=os.path.join(run_plan.bundle.logs_dir, "backend", "service.log"),
            status_dir=deployment_run_dir,
            expected_generation=generation,
            expected_plan_hash=run_plan.deployment_plan_hash,
        ).start()
        gateway = run_plan.semantic_plan.deployment.gateway
        backend_port = (
            gateway.backend_port
            if gateway is not None
            else run_plan.semantic_plan.deployment.exposure.serve_port
        )
        return LaunchedBackend(
            monitor=monitor,
            metadata={
                "deployment_status_dir": deployment_run_dir,
                "backend_port": backend_port,
            },
        )

    def wait_ready(self, run_ctx: BackendRunContext, launched: LaunchedBackend) -> None:
        if launched.monitor.wait_for_ready(self.ready_timeout_s):
            return
        process = launched.monitor.process
        if process is not None and process.poll() is not None:
            tail = "\n".join(launched.monitor.recent_lines)
            raise RuntimeError(
                f"Ray backend exited before becoming ready (exit code {process.returncode}).\n{tail}"
            )
        raise TimeoutError("Timed out waiting for canonical deployment readiness")

    def discover_targets(
        self,
        run_ctx: BackendRunContext,
        launched: LaunchedBackend,
    ) -> list[str]:
        run_plan = run_ctx.run_plan
        from exaserve.status_api import read_deployment_status

        status = read_deployment_status(launched.monitor.status_dir)
        if status is None or not status.ready:
            raise RuntimeError("target discovery requires the current canonical READY status")
        if run_plan.client.dest == "direct":
            backend_port = launched.metadata.get("backend_port")
            if isinstance(backend_port, bool) or not isinstance(backend_port, int):
                raise RuntimeError("launched backend metadata lacks an integer backend_port")
            from exaserve.plan.io import (
                load_deployment_plan,
            )
            from exaserve.plan.contracts import same_node
            from exaserve.status_api import load_status_allocation_binding

            binding = load_status_allocation_binding(launched.monitor.status_dir, status)
            plan = load_deployment_plan(run_plan.deployment_plan_path)
            if (
                binding.allocation_binding_hash != status.allocation_binding_hash
                or plan.deployment_plan_hash != status.deployment_plan_hash
            ):
                raise RuntimeError("target discovery artifacts disagree with READY status")
            observed_nodes = status.readiness_snapshot.get("nodes", [])
            if not isinstance(observed_nodes, list):
                raise RuntimeError("READY node evidence must be a list")
            addresses_by_rank: dict[int, str] = {}
            for rank, planned_node in binding.rank_to_node:
                matches = [
                    item
                    for item in observed_nodes
                    if isinstance(item, dict)
                    and item.get("alive") is True
                    and isinstance(item.get("node_name"), str)
                    and same_node(item["node_name"], planned_node)
                ]
                if len(matches) != 1:
                    raise RuntimeError(
                        f"READY node evidence maps rank {rank} to {len(matches)} addresses"
                    )
                address = matches[0].get("node_address")
                if not isinstance(address, str) or not address:
                    raise RuntimeError(f"READY node evidence for rank {rank} lacks an address")
                addresses_by_rank[rank] = address
            replica_urls = _canonical_direct_replica_urls(plan, addresses_by_rank, backend_port)
            if replica_urls is not None:
                return replica_urls
            return [f"http://{addresses_by_rank[rank]}:{backend_port}" for rank in binding.ranks()]

        return [status.advertised_endpoint]

    def stop(self, run_ctx: BackendRunContext, launched: LaunchedBackend) -> None:
        failures: list[BaseException] = []
        from exaserve.status_api import read_deployment_status

        try:
            status_before_stop = read_deployment_status(launched.monitor.status_dir)
        except (OSError, RuntimeError, TypeError, ValueError):
            # Cleanup must still run when terminal evidence is missing or
            # malformed; the post-stop validator will report that defect.
            status_before_stop = None
        if status_before_stop is not None and status_before_stop.state in {
            "STOPPED",
            "CANCELLED",
            "FAILED",
        }:
            expected_terminal_state = status_before_stop.state
        elif getattr(launched.monitor, "readiness_source", "") == "deployment_status":
            expected_terminal_state = "STOPPED"
        else:
            expected_terminal_state = "CANCELLED"
        watchdog_s = float(
            run_ctx.run_plan.semantic_plan.deployment.control.watchdog_cleanup_deadline_s
        )
        # The composition root owns graceful cleanup and is contractually
        # allowed the complete watchdog window.  The eval owner adds only a
        # bounded forced-reap margin after that window; a hard-coded 20-second
        # outer deadline previously killed a correct 120-second inner cleanup.
        force_reap_s = min(30.0, max(5.0, watchdog_s * 0.1))
        cleanup_deadline = time.monotonic() + watchdog_s + force_reap_s
        try:
            terminate_process_tree(
                launched.monitor.process,
                process_group=launched.monitor.process_group,
                deadline=cleanup_deadline,
                graceful_s=watchdog_s,
            )
        except BaseException as exc:
            failures.append(exc)
        try:
            launched.monitor.close(deadline=cleanup_deadline)
        except BaseException as exc:
            failures.append(exc)
        try:
            self._validate_shutdown_evidence(
                run_ctx,
                launched,
                expected_terminal_state=expected_terminal_state,
            )
        except BaseException as exc:
            failures.append(exc)
        if failures:
            primary = RuntimeError(f"Ray backend cleanup failed: {failures[0]}")
            for secondary in failures[1:]:
                add_exception_note(primary, f"additional cleanup failure: {secondary}")
            raise primary from failures[0]

    @staticmethod
    def _validate_shutdown_evidence(
        run_ctx: BackendRunContext,
        launched: LaunchedBackend,
        *,
        expected_terminal_state: str,
    ) -> None:
        """Require typed clean teardown before the run may publish success."""
        from exaserve.status_api import read_deployment_status

        status_dir = launched.monitor.status_dir
        if not isinstance(status_dir, str) or not status_dir:
            raise RuntimeError("Ray backend cleanup lacks a deployment status directory")
        report_path = os.path.join(status_dir, "shutdown_report.json")
        try:
            with open(report_path, encoding="utf-8") as handle:
                report = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"cannot read shutdown report {report_path}: {exc}") from exc
        if not isinstance(report, dict):
            raise RuntimeError("shutdown report must be a JSON object")

        expected_hash = run_ctx.run_plan.deployment_plan_hash
        if report.get("deployment_plan_hash") != expected_hash:
            raise RuntimeError("shutdown report deployment identity does not match the run plan")
        expected_publication = (
            "not_required" if expected_terminal_state == "FAILED" else "published"
        )
        if (
            report.get("clean") is not True
            or report.get("terminal_publication") != expected_publication
            or report.get("observed_terminal_state") != expected_terminal_state
            or report.get("errors") not in (None, [])
        ):
            raise RuntimeError(
                "deployment cleanup did not satisfy its clean terminal contract: "
                f"{report.get('errors') or report}"
            )
        components = report.get("components")
        if not isinstance(components, dict) or not components:
            raise RuntimeError("shutdown report has no owned-component evidence")
        invalid_components = {
            component_id: evidence
            for component_id, evidence in components.items()
            if not isinstance(evidence, dict) or evidence.get("state") != "STOPPED"
        }
        if invalid_components:
            raise RuntimeError(
                f"shutdown report retains non-stopped components: {invalid_components}"
            )
        if expected_terminal_state == "STOPPED":
            deployment = components.get("deployment")
            if not isinstance(deployment, dict) or deployment.get("returncode") != 0:
                raise RuntimeError(f"deployment did not complete its graceful drain: {deployment}")

        status = read_deployment_status(status_dir)
        if status is None:
            raise RuntimeError("canonical deployment status is missing after cleanup")
        if (
            status.deployment_plan_hash != expected_hash
            or status.generation != launched.monitor.expected_generation
        ):
            raise RuntimeError("terminal deployment status identity does not match the launch")
        if status.state != expected_terminal_state:
            raise RuntimeError(
                f"canonical deployment status is {status.state}, "
                f"expected clean {expected_terminal_state}"
            )

    def _proxy_settings(self, run_plan: RunMaterialization) -> dict:
        return dict(run_plan.backend_args.get("proxy", {}))

    def _launch_settings(self, run_plan: RunMaterialization) -> dict:
        return dict(run_plan.backend_args.get("launch", {}))

    def _build_trace_config(self, run_plan: RunMaterialization):
        if run_plan.trace.kind == "weak_scaling":
            return WeakScalingConfig(
                input_prompt_path=run_plan.trace.input_prompt_path,
                duration=run_plan.workload.duration,
                rpn=run_plan.workload.rate_per_node,
                input_len=run_plan.workload.input_len,
                output_len=run_plan.workload.output_len,
                output_trace_path=run_plan.trace_artifact.trace_path,
            )
        return TraceGeneratorConfig(
            input_trace_path=run_plan.trace.input_trace_path,
            input_prompt_path=run_plan.trace.input_prompt_path,
            duration=run_plan.workload.duration,
            sampling_strategy=run_plan.workload.sampling_strategy,
            speedup=run_plan.workload.speedup,
            output_len=run_plan.workload.output_len,
            output_trace_path=run_plan.trace_artifact.trace_path,
            input_len=run_plan.workload.input_len,
            modes=dict(run_plan.workload.modes),
        )


def _canonical_direct_replica_urls(
    plan, addresses_by_rank: dict[int, str], backend_port: int
) -> list[str] | None:
    """Derive bound replica routes from canonical ranks, never IP sorting."""
    models = list(plan.models)
    if len(models) != 1:
        return None  # one replay target set cannot represent multiple model routes
    model = models[0]
    n_rep = model.num_replicas
    if n_rep <= 1:
        return None
    route = model.route_name
    urls = [
        f"http://{addresses_by_rank[replica.planned_ranks[0]]}:"
        f"{backend_port}/{route}_r{replica.replica_index}"
        for replica in model.replicas
    ]
    print(
        f"[RayBackend] canonical direct mode: {n_rep} bound replica URLs "
        f"(TP={model.tensor_parallel_size}, PP={model.pipeline_parallel_size}, "
        "gateway bypassed) -> primary node of each replica.",
        flush=True,
    )
    return urls
