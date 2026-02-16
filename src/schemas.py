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
    num_routers_per_replica: Optional[float] = 0.5 # Deployment - number of routers per replica, default 0.5x replicas
    num_router_cpus: int = 1 # Deployment - number of CPUs per router

@dataclass
class DeploymentConfig:
    num_nodes: int # TODO: right now just keep num_nodes = pbs_num_nodes, future support smaller num_nodes.
    model_configs: List[ModelConfig]
    model_storage_path: str = "/lus/flare/projects/AuroraGPT/wenyiw/models"
    deployment_name: str = "aurora_serve"
    worker_max_ongoing: int = 32
    router_max_ongoing: int = 200
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
class ReplayClientConfig:
    config_path: str = "config.yaml"
    include_tp: bool = False
    early_stop: float = 0.0
    no_warmup: bool = False
    num_runs: int = 1

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
    
