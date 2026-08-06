"""Ray backend adapter.

This adapter bridges the new eval control plane with the existing serving
infrastructure (src/exaserve/resources/launch_cluster.sh, src/driver.py, src/exaserve_serve.py).

Key responsibilities:
  - build_runtime_manifest: translates the eval-layer RunPlan into an
    EvalManifest YAML that launch_cluster.sh and replay_client.py expect.
    This is the bridge between the two config schemas.
  - launch: starts `bash src/exaserve/resources/launch_cluster.sh <manifest>` as a child
    process group, monitored by ProcessMonitor for the readiness marker.
  - runtime_env: selects the shell env used for the Ray/vLLM stack and sets
    launcher exports such as EXASERVE_NULL_COMPUTE. Ray cluster settings such as
    head_ip, port, and node_cpus live in the runtime manifest, which is the
    single source of truth for driver.py. LiteLLM itself is launched as a
    separate subprocess via proxy_config.python_path, so the backend should
    stay on the Aurora frameworks env by default.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import asdict

from exaserve.schemas import DeploymentConfig, ModelConfig, ProxyConfig
from eval.site_config import get_site_config

from ..manifest import (
    EvalManifest,
    ReplayClientConfig,
    RayClusterConfig,
    TraceGeneratorConfig,
    WeakScalingConfig,
)

from ..models import RunPlan
from .base import (
    BackendAdapter,
    BackendRunContext,
    LaunchedBackend,
    ProcessMonitor,
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
        if "/site-packages" in entry and any(marker in entry for marker in _VENV_SITE_PACKAGES_MARKERS):
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
    ready_marker = "[Driver] ALL SERVICES READY"
    ready_timeout_s = 7200.0

    def validate(self, run_plan: RunPlan) -> None:
        if run_plan.scheduler.type != "pbs":
            raise ValueError("ray backend currently requires scheduler.type == 'pbs'")
        if run_plan.client.dest == "proxy":
            proxy_cfg = self._proxy_settings(run_plan)
            if proxy_cfg.get("type", "none") == "none":
                raise ValueError("client.dest=proxy requires backend.args.ray.proxy.type != 'none'")
        launch_cfg = self._launch_settings(run_plan)
        if "ray_node_cpus" in launch_cfg:
            try:
                ray_node_cpus = int(launch_cfg["ray_node_cpus"])
            except (TypeError, ValueError) as exc:
                raise ValueError("backend.args.ray.launch.ray_node_cpus must be an integer") from exc
            if ray_node_cpus < 1:
                raise ValueError("backend.args.ray.launch.ray_node_cpus must be >= 1")
        if "ray_head_port" in launch_cfg:
            try:
                ray_head_port = int(launch_cfg["ray_head_port"])
            except (TypeError, ValueError) as exc:
                raise ValueError("backend.args.ray.launch.ray_head_port must be an integer") from exc
            if ray_head_port < 1:
                raise ValueError("backend.args.ray.launch.ray_head_port must be >= 1")

    def build_runtime_manifest(self, run_plan: RunPlan) -> str:
        proxy_settings = self._proxy_settings(run_plan)
        launch_settings = self._launch_settings(run_plan)
        trace_config = self._build_trace_config(run_plan)
        model_configs = [
            ModelConfig.from_model_spec(model)
            for model in run_plan.deployment.models
        ]
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
                saturation=asdict(run_plan.client.saturation) if hasattr(run_plan.client, "saturation") else {},
            ),
            job_seed=run_plan.workload.seed,
            model_deployment_config=DeploymentConfig(
                num_nodes=run_plan.deployment.num_nodes,
                model_configs=model_configs,
                model_storage_path=run_plan.deployment.model_storage_path,
                local_stage_path=run_plan.deployment.local_stage_path,
                deployment_name=(
                    f"{run_plan.spec_name}_{run_plan.run_group_id}_{run_plan.run_id}"
                ),
                replica_max_ongoing_requests=run_plan.deployment.replica_max_ongoing_requests,
                num_gpus_per_node=run_plan.deployment.num_gpus_per_node,
                collect_stats=getattr(run_plan.deployment, "collect_stats", False),
            ),
            ray_cluster_config=RayClusterConfig(
                port=int(launch_settings.get("ray_head_port", 6379)),
                node_cpus=int(launch_settings.get("ray_node_cpus", 8)),
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
        manifest.save_yaml(run_plan.runtime_manifest_path)
        return run_plan.runtime_manifest_path

    def runtime_env(self, run_plan: RunPlan) -> RuntimeEnvSpec:
        launch_settings = self._launch_settings(run_plan)
        env_script = str(launch_settings.get("env_script", "")).strip()
        if not env_script:
            cfg = get_site_config()
            env_script = cfg.env_script_aurora
        exports = {}
        if bool(launch_settings.get("null_compute", False)):
            exports["EXASERVE_NULL_COMPUTE"] = "1"
        if bool(launch_settings.get("instrumentation", False)):
            # Stages the Ray Serve overlay probes (GetActorInfo counts, per-proxy
            # ray.get_actor timing) read by launch_cluster.sh. Needed for EXP SET 3.
            exports["EXASERVE_INSTRUMENTATION"] = "1"
        if bool(launch_settings.get("clean_stage", False)):
            # Wipe node-local artifacts before staging so Phase-2 MPI weight
            # broadcast is re-done and timed every run (resources/cleanup_run.sh).
            exports["EXASERVE_CLEAN_STAGE"] = "1"
        # Engine backend selection (deployment.engine). "sglang" routes deploy_model
        # to SGLangWorker (EXASERVE_ENGINE) and the whole serving stack to the SGLang
        # venv (EXASERVE_PYTHON_EXEC, honored by launch_cluster.sh). Default "vllm" is a
        # no-op so existing specs are unchanged.
        engine = str(getattr(run_plan.deployment, "engine", "vllm") or "vllm").lower()
        if engine == "sglang":
            exports["EXASERVE_ENGINE"] = "sglang"
            sglang_py = str(getattr(get_site_config(), "sglang_python_path", "")).strip()
            if sglang_py:
                exports["EXASERVE_PYTHON_EXEC"] = sglang_py
        return RuntimeEnvSpec(env_script=env_script, exports=exports)

    def job_env_exports(self, run_plan: RunPlan) -> dict[str, str]:
        exports: dict[str, str] = {}
        # Shard-aware PP must be signalled to the WHOLE job, not just the launch
        # subprocess: the server keys its per-stage staging + node-pinned
        # per-replica deploy off EXASERVE_PP_SHARD_AWARE, and discover_targets keys
        # per-replica direct routing off it. Derive from the deployment
        # (pp>1 AND num_replicas>1) — the exact condition server.py gates on — so
        # shard-aware multi-replica PP specs are self-contained (no hand-edited
        # job.pbs). Single-replica or PP=1 deployments are unaffected.
        if any(
            int(getattr(m, "pipeline_parallel_size", 1) or 1) > 1
            and int(getattr(m, "num_replicas", 0) or 0) > 1
            for m in run_plan.deployment.models
        ):
            exports["EXASERVE_PP_SHARD_AWARE"] = "1"
        return exports

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
        env["EXASERVE_RUN_LOG_ROOT"] = os.path.join(run_plan.bundle.logs_dir, "backend")
        # The launcher lives inside the package's resources/ data dir as of v0.1.0.
        launch_script = os.path.join(
            run_plan.repo_root, "src", "exaserve", "resources", "launch_cluster.sh"
        )
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
            readiness_dir=env["EXASERVE_RUN_LOG_ROOT"],
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
            backend_port = int(launched.metadata.get("backend_port", 8000))
            # Prefer the per-node Ray IPs the server wrote (NodeManagerAddress):
            # the PBS .hsn. FQDN resolves to an address whose :8000 returns 503,
            # while the Ray-bound IP serves /health. Fall back to PBS hostnames.
            nodes: list[str] = []
            ips_path = os.path.join(
                os.path.dirname(run_plan.runtime_manifest_path), "ray_node_ips.txt"
            )
            if os.path.isfile(ips_path):
                with open(ips_path, "r", encoding="utf-8") as handle:
                    for line in handle:
                        ip = line.strip()
                        if ip and ip not in nodes:
                            nodes.append(ip)
            if not nodes:
                nodefile = os.environ.get("EXASERVE_NODEFILE") or os.environ.get("PBS_NODEFILE")
                if not nodefile or not os.path.isfile(nodefile):
                    raise RuntimeError("EXASERVE_NODEFILE (or PBS_NODEFILE) is required for direct-mode Ray execution")
                with open(nodefile, "r", encoding="utf-8") as handle:
                    for line in handle:
                        node = line.strip()
                        if node and node not in nodes:
                            nodes.append(node)
            # Shard-aware PP has no root route: each replica is a node-pinned app
            # at /<route>_r{i}, normally reached via the proxy's set-path rewrite.
            # Direct mode bypasses the proxy, so address each replica explicitly.
            shard_urls = _shard_aware_direct_urls(run_plan, nodes, backend_port)
            if shard_urls is not None:
                return shard_urls
            return [f"http://{node}:{backend_port}" for node in nodes]

        port_file = str(launched.metadata["proxy_port_file"])
        port = int(launched.metadata.get("proxy_port", 4001))
        if os.path.isfile(port_file):
            with open(port_file, "r", encoding="utf-8") as handle:
                try:
                    port = int(handle.read().strip())
                except ValueError:
                    pass
        # Use the head node hostname so client ranks on other nodes can reach
        # the proxy (0.0.0.0 only works on the head node itself).
        nodefile = os.environ.get("EXASERVE_NODEFILE") or os.environ.get("PBS_NODEFILE")
        head_host = "0.0.0.0"
        if nodefile and os.path.isfile(nodefile):
            with open(nodefile, "r", encoding="utf-8") as handle:
                first = handle.readline().strip()
                if first:
                    head_host = first
        return [f"http://{head_host}:{port}"]

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
            input_len=run_plan.workload.input_len,
            modes=dict(run_plan.workload.modes),
        )


def _shard_aware_direct_urls(
    run_plan: RunPlan, node_ips: list[str], backend_port: int
) -> list[str] | None:
    """Per-replica direct URLs for shard-aware PP, or None if not applicable.

    Shard-aware PP (EXASERVE_PP_SHARD_AWARE=1, pp>1, num_replicas>1) serves each
    replica as a node-pinned single-replica app at route /<route>_r{i}; there is
    no root route (see driver.py / server.ordered_pp_nodes). The HAProxy proxy
    normally reaches them by rewriting the path to /<route>_r{rand}. Direct mode
    bypasses the proxy, so the client must address each replica explicitly.

    Node-assignment contract (server.ordered_pp_nodes + pp_stage.assign_pp_nodes):
    replica i's stage-0 node is (alive Ray GPU IPs sorted by ip string)[i*pp].
    ray_node_ips.txt holds that same IP set (NodeManagerAddress) but unsorted, so
    we sort here to reproduce the deployment's ordering exactly. Even if the
    ordering were off, Ray Serve routes /<route>_r{i} to replica i from any node's
    HTTP proxy, so requests still succeed (only node-locality would be lost).
    """
    if os.environ.get("EXASERVE_PP_SHARD_AWARE", "0") != "1":
        return None
    models = list(run_plan.deployment.models)
    if len(models) != 1:
        return None  # per-replica direct addressing is only defined for one model
    mc = models[0]
    pp = int(getattr(mc, "pipeline_parallel_size", 1) or 1)
    n_rep = int(getattr(mc, "num_replicas", 0) or 0)
    if pp <= 1 or n_rep <= 1:
        return None
    need = n_rep * pp
    if len(node_ips) < need:
        raise RuntimeError(
            f"shard-aware direct: need {need} nodes ({n_rep} replicas x PP={pp}) "
            f"but only {len(node_ips)} Ray node IP(s) available"
        )
    ordered = sorted(node_ips)  # match ordered_pp_nodes(): lexicographic ip sort
    try:
        from exaserve.model_paths import get_model_route_name
    except ImportError:  # pragma: no cover - snapshot import fallback
        from src.exaserve.model_paths import get_model_route_name
    route = get_model_route_name(mc.model_id)
    urls = [
        f"http://{ordered[i * pp]}:{backend_port}/{route}_r{i}"
        for i in range(n_rep)
    ]
    print(
        f"[RayBackend] shard-aware direct mode: {n_rep} per-replica URLs "
        f"(PP={pp}, proxy bypassed) -> stage-0 node of each replica.",
        flush=True,
    )
    return urls
