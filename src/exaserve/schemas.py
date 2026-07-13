import os
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any
from pathlib import Path

try:
    from .model_paths import iter_unique_model_ids
except ImportError:  # pragma: no cover - script-mode fallback
    from .model_paths import iter_unique_model_ids


def require_yaml():
    import yaml

    return yaml


@dataclass
class ModelConfig: # model configs for the engine
    model_id: str
    tensor_parallel_size: int # EngineArgs
    max_model_len: int # TraceGenerator & EngineArgs
    size: int # TraceGenerator - used when generating trace
    pipeline_parallel_size: int = 1 # EngineArgs
    # model_path: Optional[Union[str, Path]] = None # EngineArgs - path to the model
    gpu_memory_utilization: float = 0.90 # EngineArgs
    enforce_eager: bool = True # EngineArgs
    enable_log_requests: bool = True # EngineArgs
    max_num_seqs: Optional[int] = None # EngineArgs - max concurrent sequences in vLLM scheduler (None = vLLM default)
    num_replicas: Optional[int] = None # Deployment - number of replicas total, auto-scale based on tensor parallel size
    num_cpus_per_replica: int = 4 # Deployment - number of CPUs per replica

    @classmethod
    def from_model_spec(cls, spec) -> "ModelConfig":
        """Build ModelConfig from an eval-layer ModelSpec (eval/lib/models.py)."""
        return cls(
            model_id=spec.model_id,
            tensor_parallel_size=spec.tensor_parallel_size,
            pipeline_parallel_size=spec.pipeline_parallel_size,
            max_model_len=spec.max_model_len,
            size=spec.size,
            gpu_memory_utilization=spec.gpu_memory_utilization,
            enforce_eager=spec.enforce_eager,
            enable_log_requests=spec.enable_log_requests,
            max_num_seqs=getattr(spec, "max_num_seqs", None),
            num_replicas=spec.num_replicas,
            num_cpus_per_replica=spec.num_cpus_per_replica,
        )

@dataclass
class DeploymentConfig:
    num_nodes: int # TODO: right now just keep num_nodes = pbs_num_nodes, future support smaller num_nodes.
    model_configs: List[ModelConfig]
    model_storage_path: str = ""
    local_stage_path: str = ""
    deployment_name: str = "exaserve_serve"
    replica_max_ongoing_requests: int = 32
    num_gpus_per_node: int = 12  # Aurora default: 12 GPU tiles per node
    collect_stats: bool = False  # Enable per-replica vLLM stats collection
  
@dataclass
class ProxyConfig:
    """
    Configuration for the optional proxy layer launched on the head node.

    type: str
        "litellm" -- LiteLLM proxy (API keys, rate limiting, model routing, usage tracking)
        "none"    -- no proxy (default; backward-compatible with all existing experiments)
        "haproxy" -- HAProxy (pure L7 load balancing; good performance baseline)

    port: int
        The port the proxy listens on. Users hit http://<head>:<port>.

    backend_port: int
        The port where Ray Serve HTTP proxies listen on each node (default 8000).

    options: dict
        Passed verbatim to the proxy backend's generate_config() as **kwargs.
        See litellm_proxy.py / haproxy_proxy.py for supported keys.
    """
    type: str = "none"
    port: int = 4001
    backend_port: int = 8000
    python_path: str = ""   # Python interpreter for the proxy process (empty = sys.executable)
    num_workers: int = 8    # uvicorn worker count for the proxy (LiteLLM --num_workers)
    options: Dict[str, Any] = field(default_factory=dict)


def _model_config_from_dict(d: Dict[str, Any]) -> ModelConfig:
    """Build ModelConfig from a dict (e.g. YAML-loaded)."""
    raw_replicas = d.get("num_replicas")
    if raw_replicas is None:
        num_replicas = None
    elif isinstance(raw_replicas, dict):
        num_replicas = None  # YAML sometimes nests unexpectedly
    else:
        try:
            num_replicas = int(raw_replicas)
        except (TypeError, ValueError):
            num_replicas = None
    return ModelConfig(
        model_id=d["model_id"],
        tensor_parallel_size=int(d.get("tensor_parallel_size", 1)),
        pipeline_parallel_size=int(d.get("pipeline_parallel_size", 1)),
        max_model_len=int(d.get("max_model_len", 4096)),
        size=int(d.get("size", 8)),
        gpu_memory_utilization=float(d.get("gpu_memory_utilization", 0.90)),
        enforce_eager=bool(d.get("enforce_eager", True)),
        enable_log_requests=bool(d.get("enable_log_requests", True)),
        max_num_seqs=int(d["max_num_seqs"]) if d.get("max_num_seqs") is not None else None,
        num_replicas=num_replicas,
        num_cpus_per_replica=int(d.get("num_cpus_per_replica", 4)),
    )


def _deployment_config_from_dict(d: Dict[str, Any]) -> DeploymentConfig:
    """Build DeploymentConfig from a dict (e.g. YAML-loaded)."""
    model_configs = [
        _model_config_from_dict(m) if isinstance(m, dict) else m
        for m in d.get("model_configs", [])
    ]
    return DeploymentConfig(
        num_nodes=int(d.get("num_nodes", 1)),
        model_configs=model_configs,
        model_storage_path=str(d.get("model_storage_path", "")),
        local_stage_path=str(d.get("local_stage_path", "")),
        deployment_name=str(d.get("deployment_name", "exaserve_serve")),
        replica_max_ongoing_requests=int(d.get("replica_max_ongoing_requests", 32)),
        num_gpus_per_node=int(d.get("num_gpus_per_node", 12)),
        collect_stats=bool(d.get("collect_stats", False)),
    )


def validate_deployment_config(config: DeploymentConfig) -> DeploymentConfig:
    """
    Validate deployment settings that affect startup, placement, and staging.
    """
    if config.num_nodes < 1:
        raise ValueError(f"num_nodes must be >= 1, got {config.num_nodes}")
    if config.num_gpus_per_node < 1:
        raise ValueError(f"num_gpus_per_node must be >= 1, got {config.num_gpus_per_node}")
    if config.replica_max_ongoing_requests < 1:
        raise ValueError(
            "replica_max_ongoing_requests must be >= 1, "
            f"got {config.replica_max_ongoing_requests}"
        )
    if not config.model_configs:
        raise ValueError("model_configs must contain at least one model")
    if not Path(config.local_stage_path).is_absolute():
        raise ValueError(f"local_stage_path must be an absolute path, got {config.local_stage_path}")

    unique_model_ids = list(iter_unique_model_ids(config.model_configs))
    if len(unique_model_ids) != len(config.model_configs):
        raise ValueError("model_ids must be unique within model_configs")

    for model_cfg in config.model_configs:
        tp = model_cfg.tensor_parallel_size
        pp = model_cfg.pipeline_parallel_size

        if tp < 1:
            raise ValueError(f"tensor_parallel_size must be >= 1 for {model_cfg.model_id}, got {tp}")
        if pp < 1:
            raise ValueError(f"pipeline_parallel_size must be >= 1 for {model_cfg.model_id}, got {pp}")
        if tp > config.num_gpus_per_node:
            raise ValueError(
                f"tensor_parallel_size for {model_cfg.model_id} exceeds num_gpus_per_node: "
                f"{tp} > {config.num_gpus_per_node}"
            )
        if pp > config.num_nodes:
            raise ValueError(
                f"pipeline_parallel_size for {model_cfg.model_id} exceeds num_nodes: "
                f"{pp} > {config.num_nodes}"
            )
        if model_cfg.num_replicas is not None and model_cfg.num_replicas < 1:
            raise ValueError(
                f"num_replicas must be >= 1 for {model_cfg.model_id}, got {model_cfg.num_replicas}"
            )

        if pp > 1 and config.num_nodes < pp:
            raise ValueError(
                f"Not enough nodes to run {model_cfg.model_id} with "
                f"pipeline_parallel_size={pp}"
            )

    return config


def _proxy_config_from_dict(d: Dict[str, Any]) -> ProxyConfig:
    """Build ProxyConfig from a dict (e.g. YAML-loaded)."""
    return ProxyConfig(
        type=str(d.get("type", "none")),
        port=int(d.get("port", 4001)),
        backend_port=int(d.get("backend_port", 8000)),
        python_path=str(d.get("python_path", "")),
        num_workers=int(d.get("num_workers", 8)),
        options=d.get("options", {}),
    )


def load_proxy_config(path: str) -> ProxyConfig:
    """
    Load ProxyConfig from a YAML file.
    Reads the optional top-level 'proxy_config' key.
    Returns a default (type="none") ProxyConfig if the key is absent.
    """
    yaml = require_yaml()
    with open(path, "r") as f:
        data = yaml.safe_load(f) or {}
    proxy_data = data.get("proxy_config", {})
    return _proxy_config_from_dict(proxy_data)


def load_deployment_config(path: str) -> DeploymentConfig:
    """
    Load DeploymentConfig from a YAML file.
    If the file has top-level key 'model_deployment_config', that subtree is used
    (experiment config format). Otherwise the root dict is treated as deployment config.
    """
    yaml = require_yaml()
    with open(path, "r") as f:
        data = yaml.safe_load(f) or {}
    if "model_deployment_config" in data:
        data = data["model_deployment_config"]
    return validate_deployment_config(_deployment_config_from_dict(data))


def _path_to_str(obj: Any) -> Any:
    """Recursively convert Path values to str for YAML serialization."""
    if isinstance(obj, dict):
        return {k: _path_to_str(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_path_to_str(v) for v in obj]
    if isinstance(obj, Path):
        return str(obj)
    return obj
