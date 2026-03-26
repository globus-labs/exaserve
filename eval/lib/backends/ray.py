"""Ray backend adapter.

This adapter bridges the new eval control plane with the existing serving
infrastructure (scripts/launch_cluster.sh, src/driver.py, src/aurora_serve.py).

Key responsibilities:
  - build_runtime_manifest: translates the eval-layer RunPlan into the
    serving-layer ExpConfig YAML that launch_cluster.sh and replay_client.py
    expect. This is the bridge between the two config schemas.
  - launch: starts `bash scripts/launch_cluster.sh <manifest>` as a child
    process group, monitored by ProcessMonitor for the readiness marker.
  - runtime_env: selects the correct env script (env_aurora vs env_litellm)
    based on proxy type, and sets AURORA_NULL_COMPUTE for stub experiments.
"""

from __future__ import annotations

import os
import subprocess

from src.schemas import (
    DeploymentConfig,
    ExpConfig,
    ModelConfig,
    ProxyConfig,
    ReplayClientConfig,
    TraceGeneratorConfig,
    WeakScalingConfig,
)
from src.site_config import get_site_config

from ..models import RunPlan
from .base import (
    BackendAdapter,
    BackendRunContext,
    LaunchedBackend,
    ProcessMonitor,
    RuntimeEnvSpec,
    terminate_process_tree,
)


class RayBackendAdapter(BackendAdapter):
    name = "ray"
    ready_marker = "[Driver] ALL SERVICES READY"
    ready_timeout_s = 7200.0

    def validate(self, run_plan: RunPlan) -> None:
        if run_plan.scheduler.type != "pbs":
            raise ValueError("ray backend currently requires scheduler.type == 'pbs'")
        if run_plan.client.dest == "proxy":
            proxy_cfg = self._proxy_settings(run_plan)
            if proxy_cfg.get("type", "none") == "none":
                raise ValueError("client.dest=proxy requires backend.args.ray.proxy.type != 'none'")

    def build_runtime_manifest(self, run_plan: RunPlan) -> str:
        proxy_settings = self._proxy_settings(run_plan)
        trace_config = self._build_trace_config(run_plan)
        model_configs = [
            ModelConfig(
                model_id=model.model_id,
                tensor_parallel_size=model.tensor_parallel_size,
                pipeline_parallel_size=model.pipeline_parallel_size,
                max_model_len=model.max_model_len,
                size=model.size,
                gpu_memory_utilization=model.gpu_memory_utilization,
                enforce_eager=model.enforce_eager,
                enable_log_requests=model.enable_log_requests,
                num_replicas=model.num_replicas,
                num_cpus_per_replica=model.num_cpus_per_replica,
            )
            for model in run_plan.deployment.models
        ]
        exp_config = ExpConfig(
            pbs_result_dir=run_plan.bundle.results_dir,
            pbs_stdout_dir=run_plan.bundle.pbs_stdout_dir,
            pbs_stderr_dir=run_plan.bundle.pbs_stderr_dir,
            pbs_num_nodes=run_plan.scheduler.nodes,
            pbs_walltime=run_plan.scheduler.walltime,
            pbs_queue_name=run_plan.scheduler.queue,
            pbs_job_name=f"{run_plan.spec_name}_{run_plan.variant_name}",
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
            ),
            job_seed=run_plan.workload.seed,
            model_deployment_config=DeploymentConfig(
                num_nodes=run_plan.deployment.num_nodes,
                model_configs=model_configs,
                model_storage_path=run_plan.deployment.model_storage_path,
                local_stage_path=run_plan.deployment.local_stage_path,
                deployment_name=run_plan.spec_name,
                worker_max_ongoing=run_plan.deployment.worker_max_ongoing,
                num_gpus_per_node=run_plan.deployment.num_gpus_per_node,
            ),
            proxy_config=ProxyConfig(
                type=str(proxy_settings.get("type", "none")),
                port=int(proxy_settings.get("port", 4001)),
                backend_port=int(proxy_settings.get("backend_port", 8000)),
                python_path=str(
                    proxy_settings.get("python_path", get_site_config().litellm_python_path)
                ),
                num_workers=int(proxy_settings.get("num_workers", 1)),
                options=dict(proxy_settings.get("options", {})),
            ),
        )
        exp_config.save_yaml(run_plan.runtime_manifest_path)
        return run_plan.runtime_manifest_path

    def runtime_env(self, run_plan: RunPlan) -> RuntimeEnvSpec:
        launch_settings = self._launch_settings(run_plan)
        proxy_type = self._proxy_settings(run_plan).get("type", "none")
        env_script = str(launch_settings.get("env_script", "")).strip()
        if not env_script:
            cfg = get_site_config()
            env_script = cfg.env_script_litellm if proxy_type == "litellm" else cfg.env_script_aurora
        exports = {}
        if bool(launch_settings.get("null_compute", False)):
            exports["AURORA_NULL_COMPUTE"] = "1"
        return RuntimeEnvSpec(env_script=env_script, exports=exports)

    def launch(self, run_ctx: BackendRunContext) -> LaunchedBackend:
        run_plan = run_ctx.run_plan
        env = os.environ.copy()
        runtime_env = self.runtime_env(run_plan)
        env.update(runtime_env.exports)
        env["PYTHONPATH"] = run_plan.repo_root + os.pathsep + env.get("PYTHONPATH", "")
        env["AURORA_RUN_LOG_ROOT"] = os.path.join(run_plan.bundle.logs_dir, "backend")
        launch_script = os.path.join(run_plan.repo_root, "scripts", "launch_cluster.sh")
        cmd = ["bash", launch_script, run_plan.runtime_manifest_path]
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
        monitor = ProcessMonitor(
            process=process,
            log_path=os.path.join(run_plan.bundle.logs_dir, "backend", "service.log"),
            ready_marker=self.ready_marker,
        ).start()
        return LaunchedBackend(
            monitor=monitor,
            metadata={
                "proxy_port_file": os.path.join(
                    os.path.dirname(run_plan.runtime_manifest_path),
                    "proxy_out",
                    "proxy_port",
                ),
                "backend_port": int(self._proxy_settings(run_plan).get("backend_port", 8000)),
                "proxy_port": int(self._proxy_settings(run_plan).get("port", 4001)),
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
        raise TimeoutError("Timed out waiting for Ray backend readiness marker")

    def discover_targets(
        self,
        run_ctx: BackendRunContext,
        launched: LaunchedBackend,
    ) -> list[str]:
        run_plan = run_ctx.run_plan
        if run_plan.client.dest == "direct":
            nodefile = os.environ.get("PBS_NODEFILE")
            if not nodefile or not os.path.isfile(nodefile):
                raise RuntimeError("PBS_NODEFILE is required for direct-mode Ray execution")
            with open(nodefile, "r", encoding="utf-8") as handle:
                nodes = []
                for line in handle:
                    node = line.strip()
                    if node and node not in nodes:
                        nodes.append(node)
            backend_port = int(launched.metadata.get("backend_port", 8000))
            return [f"http://{node}:{backend_port}" for node in nodes]

        port_file = str(launched.metadata["proxy_port_file"])
        port = int(launched.metadata.get("proxy_port", 4001))
        if os.path.isfile(port_file):
            with open(port_file, "r", encoding="utf-8") as handle:
                try:
                    port = int(handle.read().strip())
                except ValueError:
                    pass
        return [f"http://0.0.0.0:{port}"]

    def stop(self, run_ctx: BackendRunContext, launched: LaunchedBackend) -> None:
        terminate_process_tree(launched.monitor.process)
        launched.monitor.close()

    def _proxy_settings(self, run_plan: RunPlan) -> dict:
        return dict(run_plan.backend_args.get("proxy", {}))

    def _launch_settings(self, run_plan: RunPlan) -> dict:
        return dict(run_plan.backend_args.get("launch", {}))

    def _build_trace_config(self, run_plan: RunPlan):
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
            modes=dict(run_plan.workload.modes),
        )
