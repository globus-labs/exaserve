"""Eval spec frontend for the shared plan compiler (packet P05, IMP-H01).

Eval built its own `RunPlan` from an eval-shaped `DeploymentSpec`, and core
compiled a different object from YAML. Nothing could prove the two described
the same deployment, so a serving change could alter one and not the other with
no diagnostic.

This module makes the eval path derive the **same** `DeploymentPlan` the core
compiler produces, so both sides agree on `deployment_plan_hash` by
construction. Eval retains workload/trace/client input syntax, but it has no
second serving schema or runtime topology. This is a permanent compiler
frontend, not a runtime compatibility adapter.
"""

from __future__ import annotations

from typing import Any, Optional

from exaserve.plan.compiler import compile_deployment_plan, compile_run_plan
from exaserve.plan.contracts import (
    ArtifactPolicy,
    BackendPolicy,
    ClientPolicy,
    DeploymentPlan,
    SchedulerPlan,
    SiteProfile,
    SaturationPolicy,
    TracePolicy,
    WorkloadPolicy,
)
from exaserve.site import default_site_profile


def deployment_raw_from_spec(
    deployment,
    *,
    gateway: Optional[dict] = None,
    validation_mode: bool = False,
    exposure: Optional[dict] = None,
    launch: Optional[dict] = None,
    request_mode: str = "completion",
    streaming_mode: str = "non_streaming",
) -> dict:
    """Render an eval `DeploymentSpec` in the compiler's input shape.

    Every field the compiler accepts is passed explicitly; anything eval knows
    that the plan does not model is left out here rather than smuggled in, so
    the compiler's unknown-key rejection stays meaningful.
    """
    raw: dict[str, Any] = {
        "num_nodes": int(deployment.num_nodes),
        "request_mode": request_mode,
        "streaming_mode": streaming_mode,
        "models": [
            {
                "model_id": model.model_id,
                "tensor_parallel_size": int(model.tensor_parallel_size),
                "pipeline_parallel_size": int(getattr(model, "pipeline_parallel_size", 1)),
                "max_model_len": int(model.max_model_len),
                "size": int(model.size),
                "num_replicas": getattr(model, "num_replicas", None),
                "gpu_memory_utilization": float(getattr(model, "gpu_memory_utilization", 0.90)),
                "enforce_eager": bool(getattr(model, "enforce_eager", True)),
                "enable_log_requests": bool(getattr(model, "enable_log_requests", True)),
                "max_num_seqs": getattr(model, "max_num_seqs", None),
                "num_cpus_per_replica": int(getattr(model, "num_cpus_per_replica", 4)),
            }
            for model in deployment.models
        ],
    }
    if getattr(deployment, "num_gpus_per_node", 0):
        raw["num_gpus_per_node"] = int(deployment.num_gpus_per_node)
    if getattr(deployment, "model_storage_path", ""):
        raw["model_storage_path"] = deployment.model_storage_path
    if getattr(deployment, "local_stage_path", ""):
        raw["local_stage_path"] = deployment.local_stage_path
    if getattr(deployment, "engine", ""):
        raw["engine"] = deployment.engine
    raw["replica_max_ongoing_requests"] = int(
        getattr(deployment, "replica_max_ongoing_requests", 32)
    )
    raw["collect_stats"] = bool(getattr(deployment, "collect_stats", False))
    launch = dict(launch or {})
    if "ray_node_cpus" in launch:
        raw["node_cpus"] = launch["ray_node_cpus"]
    if "ray_head_port" in launch:
        raw["ray_port"] = launch["ray_head_port"]
    runtime = {
        "null_compute": launch.get("null_compute", False),
        "null_compute_latency_s": launch.get("null_compute_latency_s", 1.0),
        "instrumentation": launch.get("instrumentation", False),
        "clean_stage": launch.get("clean_stage", False),
    }
    if runtime["null_compute"] or runtime["instrumentation"]:
        validation_mode = True
    raw["runtime"] = runtime
    if gateway is not None:
        raw["gateway"] = gateway
    if validation_mode or bool(getattr(deployment, "validation_mode", False)):
        raw["validation_mode"] = True
    if exposure is not None:
        raw["exposure"] = exposure
    return raw


def _gateway_from_backend(backend_args: dict) -> tuple[Optional[dict], bool, Optional[dict]]:
    """Translate eval's proxy settings into gateway/exposure.

    `proxy.type: none` was eval's way of saying "hit Serve directly". That is a
    validation exposure, not a production gateway, and it now has to say so.
    """
    proxy = ((backend_args or {}).get("ray") or {}).get("proxy") or {}
    if not isinstance(proxy, dict):
        raise ValueError("backend.args.ray.proxy must be a mapping")
    kind_value = proxy.get("type", "none")
    if not isinstance(kind_value, str):
        raise ValueError("backend.args.ray.proxy.type must be text")
    kind = kind_value.lower()
    backend_port = proxy.get("backend_port", 8000)
    if isinstance(backend_port, bool) or not isinstance(backend_port, int):
        raise ValueError("backend.args.ray.proxy.backend_port must be an integer")
    if kind in ("none", "direct", ""):
        return (
            None,
            True,
            {"mode": "DIRECT_VALIDATION", "serve_port": backend_port},
        )
    if kind == "ray_serve":
        # Ray Serve's native HeadOnly proxy is itself the benchmark endpoint.
        # It is neither a managed external gateway nor direct EveryNode
        # validation, so preserve that topology as its own exposure mode.
        return (
            None,
            True,
            {"mode": "RAY_SERVE_HEAD_ONLY", "serve_port": backend_port},
        )
    executable_ref = proxy.get("executable_ref", "")
    if not isinstance(executable_ref, str):
        raise ValueError("backend.args.ray.proxy.executable_ref must be text")
    if not executable_ref and kind == "litellm" and proxy.get("python_path"):
        from pathlib import Path

        python_path = proxy["python_path"]
        if not isinstance(python_path, str):
            raise ValueError("backend.args.ray.proxy.python_path must be text")
        executable_ref = str(Path(python_path).parent / "litellm")
    port = proxy.get("port", 4001)
    workers = proxy.get("num_workers", 1)
    if isinstance(port, bool) or not isinstance(port, int):
        raise ValueError("backend.args.ray.proxy.port must be an integer")
    if isinstance(workers, bool) or not isinstance(workers, int):
        raise ValueError("backend.args.ray.proxy.num_workers must be an integer")
    options = proxy.get("options", {})
    if not isinstance(options, dict):
        raise ValueError("backend.args.ray.proxy.options must be a mapping")
    gateway = {
        "kind": kind,
        "port": port,
        "backend_port": backend_port,
        "executable_ref": executable_ref or f"PATH:{kind}",
        "worker_count": workers,
        "options": dict(options),
    }
    # HAProxy is the first production gateway. Other managed gateways are
    # explicit benchmark/qualification modes until their capability gates pass.
    return gateway, kind != "haproxy", {"mode": "PROXIED_INTERNAL"}


def _request_and_streaming_modes(spec) -> tuple[str, str]:
    """Convert eval workload protocol choices into scale-envelope identity."""
    weights = dict(getattr(spec.workload, "modes", {}) or {})
    active = {name for name, weight in weights.items() if int(weight) > 0}
    if active == {"chat"}:
        request_mode = "chat"
    elif active == {"completion"}:
        request_mode = "completion"
    elif active == {"chat", "completion"}:
        request_mode = "mixed"
    else:  # The normal spec loader rejects this before plan compilation.
        raise ValueError(f"unsupported active workload modes: {sorted(active)}")
    saturation = getattr(spec.client, "saturation", None)
    streaming = bool(getattr(spec.client, "stream", False)) or bool(
        saturation is not None
        and getattr(saturation, "enabled", False)
        and getattr(saturation, "stream", False)
    )
    return request_mode, "streaming" if streaming else "non_streaming"


def compile_shared_deployment_plan(
    spec, *, deployment_id: str, site: Optional[SiteProfile] = None
) -> DeploymentPlan:
    """The DeploymentPlan for an eval spec — identical to core's for one input."""
    gateway, validation_mode, exposure = _gateway_from_backend(
        getattr(spec.backend, "args", {}) or {}
    )
    launch = ((getattr(spec.backend, "args", {}) or {}).get("ray") or {}).get("launch") or {}
    request_mode, streaming_mode = _request_and_streaming_modes(spec)
    raw = deployment_raw_from_spec(
        spec.deployment,
        gateway=gateway,
        validation_mode=validation_mode,
        exposure=exposure,
        launch=launch,
        request_mode=request_mode,
        streaming_mode=streaming_mode,
    )
    return compile_deployment_plan(
        raw, site=site or default_site_profile(), deployment_id=deployment_id
    )


def compile_shared_run_plan(
    spec, *, run_id: str, deployment_id: str, site: Optional[SiteProfile] = None
):
    """The shared RunPlan: that exact DeploymentPlan plus run semantics."""
    gateway, validation_mode, exposure = _gateway_from_backend(
        getattr(spec.backend, "args", {}) or {}
    )
    launch = ((getattr(spec.backend, "args", {}) or {}).get("ray") or {}).get("launch") or {}
    request_mode, streaming_mode = _request_and_streaming_modes(spec)
    raw = deployment_raw_from_spec(
        spec.deployment,
        gateway=gateway,
        validation_mode=validation_mode,
        exposure=exposure,
        launch=launch,
        request_mode=request_mode,
        streaming_mode=streaming_mode,
    )
    workload = WorkloadPolicy(
        kind=getattr(spec.trace, "kind", "synthetic"),
        duration_s=float(getattr(spec.workload, "duration", 0.0)),
        input_len=int(getattr(spec.workload, "input_len", 0)),
        output_len=int(getattr(spec.workload, "output_len", 0)),
        rate_per_node=float(getattr(spec.workload, "rate_per_node", 0.0)),
        seed=int(getattr(spec.workload, "seed", 0)),
        arrival=str(getattr(spec.workload, "arrival", "fixed")),
        speedup=float(getattr(spec.workload, "speedup", 1.0)),
        sampling_strategy=str(getattr(spec.workload, "sampling_strategy", "peak")),
        generation_mode=str(getattr(spec.workload, "generation_mode", "deterministic")),
        modes=tuple(sorted(dict(getattr(spec.workload, "modes", {})).items())),
        client_nodes=int(getattr(spec.client, "num_nodes", 1)),
        client_dest=str(getattr(spec.client, "dest", "proxy")),
    )
    filesystems = str(getattr(spec.scheduler, "filesystems", ""))
    scheduler = SchedulerPlan(
        type=str(getattr(spec.scheduler, "type", "pbs")),
        nodes=int(getattr(spec.scheduler, "nodes", spec.deployment.num_nodes)),
        queue=str(getattr(spec.scheduler, "queue", "")),
        account=str(getattr(spec.scheduler, "project", "")),
        walltime=str(getattr(spec.scheduler, "walltime", "")),
        filesystem_refs=tuple(item for item in filesystems.split(":") if item),
        policy={
            "keep_output": str(getattr(spec.scheduler, "keep_output", "")),
            "mail_user": str(getattr(spec.scheduler, "mail_user", "")),
            "mail_events": str(getattr(spec.scheduler, "mail_events", "")),
        },
    )
    sat = getattr(spec.client, "saturation", None)
    client = ClientPolicy(
        destination=str(getattr(spec.client, "dest", "proxy")),
        nodes=int(getattr(spec.client, "num_nodes", 1)),
        # Saturation has its own stream switch, but it is still an execution
        # protocol choice and must agree with the deployment scale envelope.
        streaming=streaming_mode == "streaming",
        dispatch_topology=str(getattr(spec.client, "direct_dispatch", "local")),
        direct_pair_shift=int(getattr(spec.client, "direct_pair_shift", 1)),
        request_timeout_s=float(getattr(spec.client, "request_timeout_s", 3600.0)),
        drain_wait_timeout_s=float(getattr(spec.client, "drain_wait_timeout_s", 3780.0)),
        shard_timeout_s=float(getattr(spec.client, "shard_timeout_s", 600.0)),
        direct_target_ready_timeout_s=float(
            getattr(spec.client, "direct_target_ready_timeout_s", 300.0)
        ),
        direct_target_probe_timeout_s=float(
            getattr(spec.client, "direct_target_probe_timeout_s", 2.0)
        ),
        direct_target_interval_s=float(getattr(spec.client, "direct_target_interval_s", 5.0)),
        direct_target_max_workers=int(getattr(spec.client, "direct_target_max_workers", 64)),
        concurrency=int(getattr(spec.client, "go_concurrency", 0)),
        workers=int(getattr(spec.client, "num_go_workers", 1)),
        startup_only=bool(getattr(spec.client, "startup_only", False)),
        num_runs=int(getattr(spec.client, "num_runs", 1)),
        include_tp=bool(getattr(spec.client, "include_tp", False)),
        early_stop=float(getattr(spec.client, "early_stop", 0.0)),
        processes=int(getattr(spec.client, "num_go_procs", 1)),
        warmup_rps=int(getattr(spec.client, "warmup_rps", 0)),
        warmup_duration_s=float(getattr(spec.client, "warmup_duration_s", 0.0)),
        sum_only=bool(getattr(spec.client, "sum_only", False)),
        dispatch_topologies=tuple(getattr(spec.client, "dispatch_topologies", ()) or ()),
        saturation=SaturationPolicy(
            **(
                {name: getattr(sat, name) for name in SaturationPolicy.__dataclass_fields__}
                if sat is not None
                else {}
            )
        ),
    )
    trace = TracePolicy(
        kind=str(getattr(spec.trace, "kind", "synthetic")),
        tokenizer_builder_ref=(str(getattr(spec.trace, "tokenizer_builder", "")) or "default"),
        generation_policy=(
            ("arrival", str(getattr(spec.workload, "arrival", "fixed"))),
            ("seed", int(getattr(spec.workload, "seed", 0))),
        ),
    )
    selected_backend = str(getattr(spec.backend, "default", "ray"))
    backend = BackendPolicy(
        name=selected_backend,
        options=dict((getattr(spec.backend, "args", {}) or {}).get(selected_backend, {}) or {}),
    )
    return compile_run_plan(
        raw,
        site=site or default_site_profile(),
        run_id=run_id,
        deployment_id=deployment_id,
        scheduler=scheduler,
        workload=workload,
        trace=trace,
        client=client,
        backend=backend,
        artifacts=ArtifactPolicy(),
    )
