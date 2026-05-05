"""Eval-layer runtime manifest: config types used only by the eval control plane.

The serving runtime (driver.py, aurora_serve.py) reads only DeploymentConfig
and ProxyConfig from the YAML manifest.  Everything else — PBS metadata,
trace generator config, replay client config — is consumed exclusively by
the eval layer (replay_engine.py, run_executor.py).

This module owns those eval-only types so that src/schemas.py can stay
focused on the serving contract.

The YAML format is backward-compatible: save_runtime_manifest() writes a
single file that both load_deployment_config()/load_proxy_config() (serving)
and load_eval_manifest() (eval) can read from their respective subtrees.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, Union

from aurora_rayserver.schemas import (
    DeploymentConfig,
    ProxyConfig,
    _path_to_str,
    require_yaml,
    validate_deployment_config,
    _deployment_config_from_dict,
    _proxy_config_from_dict,
)


@dataclass
class TraceGeneratorConfig:
    input_trace_path: str
    input_prompt_path: str
    duration: float
    sampling_strategy: str
    speedup: float
    output_len: int
    output_trace_path: str
    input_len: int = 0
    # mode -> weight for distribution; e.g. {"chat": 1, "completion": 0} = all chat
    modes: Dict[str, int] = field(default_factory=lambda: {"chat": 1, "completion": 0})


@dataclass
class WeakScalingConfig:
    input_prompt_path: str
    duration: float
    rpn: float  # requests per node
    input_len: int
    output_len: int
    output_trace_path: str


@dataclass
class ReplayClientConfig:
    config_path: str = "config.yaml"
    include_tp: bool = False
    early_stop: float = 0.0
    num_runs: int = 1
    generation_mode: str = "deterministic"  # "deterministic" or "natural"
    dest: str = "proxy"  # "proxy": local workers -> proxy; "direct": hash-shard across per-node servers
    num_nodes: int = 1      # total PBS nodes (= pbs_num_nodes); used to compute actual client count
    num_go_procs: int = 16   # number of Go processes per replay_client node
    num_go_workers: int = 2 # dispatch goroutines (N) inside each Go process
    go_concurrency: int = 0  # 0 = auto-derive from ephemeral port range in Go client
    warmup_rps: int = 0     # warm-up requests per second (0 = no warmup)
    warmup_duration_s: float = 0.0  # warm-up duration in seconds
    sum_only: bool = False  # Go client writes only summary instead of per-request results
    stream: bool = False    # Enable SSE streaming for TTFT measurement
    saturation: dict = field(default_factory=dict)  # SaturationSpec as dict (empty = disabled)


@dataclass
class RayClusterConfig:
    head_ip: str = ""
    port: int = 6379
    node_cpus: int = 8


@dataclass
class EvalManifest:
    """Full experiment manifest written by the eval control plane.

    Bundles PBS metadata, trace/replay client configs, and pointers to the
    serving-layer DeploymentConfig and ProxyConfig.  The serving code never
    loads this class — it reads only its subtrees via load_deployment_config()
    and load_proxy_config().
    """
    # PBS Job Header Config
    pbs_result_dir: str
    pbs_stdout_dir: str
    pbs_stderr_dir: str
    pbs_num_nodes: int
    pbs_walltime: str
    pbs_queue_name: str
    pbs_job_name: str
    pbs_working_dir: str
    # Job Config
    job_trace_config: Union[TraceGeneratorConfig, WeakScalingConfig]
    job_replay_client_config: ReplayClientConfig
    job_seed: int
    # Model Serving (included so save_runtime_manifest can write the full YAML)
    model_deployment_config: DeploymentConfig
    ray_cluster_config: RayClusterConfig = field(default_factory=RayClusterConfig)
    # Optional proxy layer (defaults to disabled for backward compat)
    proxy_config: ProxyConfig = field(default_factory=ProxyConfig)

    def to_yaml_dict(self) -> Dict[str, Any]:
        """Serialize to a dict that rigorously mirrors the data structure."""
        return _path_to_str(asdict(self))

    def save_yaml(self, path: str):
        """Save the manifest as a YAML file."""
        yaml = require_yaml()
        yaml_dict = self.to_yaml_dict()
        parent = os.path.dirname(os.path.abspath(path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(path, "w") as f:
            yaml.dump(yaml_dict, f, default_flow_style=False)


def _trace_config_from_dict(d: Dict[str, Any]) -> Union[TraceGeneratorConfig, WeakScalingConfig]:
    if "rpn" in d:
        return WeakScalingConfig(
            input_prompt_path=str(d.get("input_prompt_path", "")),
            duration=float(d.get("duration", 0.0)),
            rpn=float(d.get("rpn", 0.0)),
            input_len=int(d.get("input_len", 0)),
            output_len=int(d.get("output_len", 0)),
            output_trace_path=str(d.get("output_trace_path", "")),
        )
    return TraceGeneratorConfig(
        input_trace_path=str(d.get("input_trace_path", "")),
        input_prompt_path=str(d.get("input_prompt_path", "")),
        duration=float(d.get("duration", 0.0)),
        sampling_strategy=str(d.get("sampling_strategy", "peak")),
        speedup=float(d.get("speedup", 1.0)),
        output_len=int(d.get("output_len", 0)),
        output_trace_path=str(d.get("output_trace_path", "")),
        input_len=int(d.get("input_len", 0)),
        modes=dict(d.get("modes", {"chat": 1, "completion": 0})),
    )


def _replay_config_from_dict(d: Dict[str, Any]) -> ReplayClientConfig:
    return ReplayClientConfig(
        config_path=str(d.get("config_path", "config.yaml")),
        include_tp=bool(d.get("include_tp", False)),
        early_stop=float(d.get("early_stop", 0.0)),
        num_runs=int(d.get("num_runs", 1)),
        generation_mode=str(d.get("generation_mode", "deterministic")),
        dest=str(d.get("dest", "proxy")),
        num_nodes=int(d.get("num_nodes", 1)),
        num_go_procs=int(d.get("num_go_procs", 1)),
        num_go_workers=int(d.get("num_go_workers", 4)),
        go_concurrency=int(d.get("go_concurrency", 0)),
        warmup_rps=int(d.get("warmup_rps", 0)),
        warmup_duration_s=float(d.get("warmup_duration_s", 0.0)),
        sum_only=bool(d.get("sum_only", False)),
        stream=bool(d.get("stream", False)),
        saturation=dict(d.get("saturation", {})),
    )


def load_eval_manifest(path: str) -> EvalManifest:
    """Load an EvalManifest from a runtime manifest YAML.

    The YAML is the same file that load_deployment_config() and
    load_proxy_config() read from — this function just reads the
    additional eval-only fields.
    """
    yaml = require_yaml()
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    return EvalManifest(
        pbs_result_dir=str(data.get("pbs_result_dir", "")),
        pbs_stdout_dir=str(data.get("pbs_stdout_dir", "")),
        pbs_stderr_dir=str(data.get("pbs_stderr_dir", "")),
        pbs_num_nodes=int(data.get("pbs_num_nodes", 1)),
        pbs_walltime=str(data.get("pbs_walltime", "")),
        pbs_queue_name=str(data.get("pbs_queue_name", "")),
        pbs_job_name=str(data.get("pbs_job_name", "")),
        pbs_working_dir=str(data.get("pbs_working_dir", "")),
        job_trace_config=_trace_config_from_dict(data.get("job_trace_config", {})),
        job_replay_client_config=_replay_config_from_dict(
            data.get("job_replay_client_config", {})
        ),
        job_seed=int(data.get("job_seed", 42)),
        model_deployment_config=validate_deployment_config(
            _deployment_config_from_dict(data.get("model_deployment_config", {}))
        ),
        ray_cluster_config=RayClusterConfig(
            head_ip=str(data.get("ray_cluster_config", {}).get("head_ip", "")),
            port=int(data.get("ray_cluster_config", {}).get("port", 6379)),
            node_cpus=int(data.get("ray_cluster_config", {}).get("node_cpus", 8)),
        ),
        proxy_config=_proxy_config_from_dict(data.get("proxy_config", {})),
    )
