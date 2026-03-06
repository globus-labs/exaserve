import os
import yaml
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Union, Dict, Any
from pathlib import Path


@dataclass
class ModelConfig: # model configs for the engine
    model_id: str
    tensor_parallel_size: int # EngineArgs
    max_model_len: int # TraceGenerator & EngineArgs
    size: int # TraceGenerator - used when generating trace
    # model_path: Optional[Union[str, Path]] = None # EngineArgs - path to the model
    gpu_memory_utilization: float = 0.90 # EngineArgs
    enforce_eager: bool = True # EngineArgs
    enable_log_requests: bool = True # EngineArgs
    num_replicas: Optional[int] = None # Deployment - number of replicas total, auto-scale based on tensor parallel size
    num_cpus_per_replica: int = 4 # Deployment - number of CPUs per replica

@dataclass
class DeploymentConfig:
    num_nodes: int # TODO: right now just keep num_nodes = pbs_num_nodes, future support smaller num_nodes.
    model_configs: List[ModelConfig]
    model_storage_path: str = "/lus/flare/projects/AuroraGPT/wenyiw/models"
    deployment_name: str = "aurora_serve"
    worker_max_ongoing: int = 32
    num_gpus_per_node: int = 12 # machine spec
  
@dataclass
class TraceGeneratorConfig:
    input_trace_path: str
    input_prompt_path: str
    duration: float
    sampling_strategy: str
    speedup: float
    output_len: int
    output_trace_path: str
    # mode -> weight for distribution; e.g. {"chat": 1, "completion": 0} = all chat
    modes: Dict[str, int] = field(default_factory=lambda: {"chat": 1, "completion": 0})

@dataclass
class WeakScalingConfig:
    input_prompt_path: str
    duration: float
    rpn: float # requests per node
    input_len: int
    output_len: int
    output_trace_path: str

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


@dataclass
class ReplayClientConfig:
    config_path: str = "config.yaml"
    include_tp: bool = False
    early_stop: float = 0.0
    no_warmup: bool = False
    num_runs: int = 1
    generation_mode: str = "deterministic"  # "deterministic" or "natural"
    dest: str = "proxy"  # "proxy": local workers → proxy; "direct": MPI round-robin to servers
    num_nodes: int = 1      # total PBS nodes (= pbs_num_nodes); used to compute actual client count
    num_cli_per_node: float = 1.0  # fraction of total nodes used as MPI client ranks
                                   # actual_client_nodes = max(1, min(num_nodes, round(num_nodes * num_cli_per_node)))
    num_workers_per_node: int = 4    # multiprocessing workers per MPI rank (typically one rank per node)


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
        max_model_len=int(d.get("max_model_len", 4096)),
        size=int(d.get("size", 8)),
        gpu_memory_utilization=float(d.get("gpu_memory_utilization", 0.90)),
        enforce_eager=bool(d.get("enforce_eager", True)),
        enable_log_requests=bool(d.get("enable_log_requests", True)),
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
        model_storage_path=str(d.get("model_storage_path", "/lus/flare/projects/AuroraGPT/wenyiw/models")),
        deployment_name=str(d.get("deployment_name", "aurora_serve")),
        worker_max_ongoing=int(d.get("worker_max_ongoing", 32)),
        num_gpus_per_node=int(d.get("num_gpus_per_node", 12)),
    )


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
    with open(path, "r") as f:
        data = yaml.safe_load(f) or {}
    if "model_deployment_config" in data:
        data = data["model_deployment_config"]
    return _deployment_config_from_dict(data)


@dataclass
class ExpConfig:
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
    # Model Serving
    model_deployment_config: DeploymentConfig
    # Optional proxy layer (defaults to disabled for backward compat)
    proxy_config: ProxyConfig = field(default_factory=ProxyConfig)
    
    
    def to_yaml_dict(self) -> Dict[str, Any]:
        """Serialize ExpConfig to a dict that rigorously mirrors its data structure."""
        return _path_to_str(asdict(self))

    def save_yaml(self, path: str):
        """Save the config as a YAML file."""
        yaml_dict = self.to_yaml_dict()
        parent = os.path.dirname(os.path.abspath(path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(path, 'w') as f:
            yaml.dump(yaml_dict, f, default_flow_style=False)


def _path_to_str(obj: Any) -> Any:
    """Recursively convert Path values to str for YAML serialization."""
    if isinstance(obj, dict):
        return {k: _path_to_str(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_path_to_str(v) for v in obj]
    if isinstance(obj, Path):
        return str(obj)
    return obj
    
