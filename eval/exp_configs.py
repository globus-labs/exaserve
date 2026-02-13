import os
import yaml
from dataclasses import dataclass, asdict, field
from typing import List, Union, Optional, Dict, Any

# Constants
DEFAULT_DATA_ROOT = "/home/wenyiw/aurora_rayserver/data_local"

# Download before running the experiment
# wget https://azurepublicdatasettraces.blob.core.windows.net/azurellminfererencetrace/AzureLLMInferenceTrace_code_1week.csv
# wget https://huggingface.co/datasets/anon8231489123/ShareGPT_Vicuna_unfiltered/resolve/main/ShareGPT_V3_unfiltered_cleaned_split_no_imsorry.json
DEFAULT_INPUT_TRACE_PATH = os.path.join(DEFAULT_DATA_ROOT, "input_traces/AzureLLMInferenceTrace_code_1week.csv") 
DEFAULT_INPUT_PROMPT_PATH = os.path.join(DEFAULT_DATA_ROOT, "input_traces/ShareGPT_V3_unfiltered_cleaned_split_no_imsorry.json")
DEFAULT_OUTPUT_TRACE_DIR = os.path.join(DEFAULT_DATA_ROOT, "output_traces")
DEFAULT_RESULTS_ROOT = os.path.join(DEFAULT_DATA_ROOT, "results")
DEFAULT_PBS_OUTPUT_ROOT = os.path.join(DEFAULT_DATA_ROOT, "pbs_output")
DEFAULT_STORAGE_PATH = "/home/wenyiw/agpt/mpi-llm/models"
DEFAULT_LOCAL_STORAGE_PATH = "/local/scratch/models"


@dataclass
class ModelConfig:
    model_id: str
    mode: str
    tensor_parallel_size: int
    size: int
    num_replicas: Optional[int] = None
    node_index: Optional[int] = None
    tokenizer_path: Optional[str] = None

@dataclass
class TraceGeneratorConfig:
    input_trace_path: str
    input_prompt_path: str
    duration: float
    sampling_strategy: str
    speedup: float
    output_len: int
    output_trace_path: str
    max_model_len: int = 4096

@dataclass
class WeakScalingConfig:
    input_prompt_path: str
    duration: float
    rpn: float
    input_len: int
    output_len: int
    output_trace_path: str

@dataclass
class GpuTopologyConfig:
    num_gpus_per_node: int = 4
    num_nodes: int = 1

@dataclass
class ApiServerConfig:
    launch_sllm_service: bool = True
    path_save: str = DEFAULT_RESULTS_ROOT
    requests_number: int = -1
    sorted_policy: str = "FCFS_batch"

@dataclass
class BenchmarkConfig:
    output_result_dir: str = DEFAULT_RESULTS_ROOT
    output_trace_path: str = ""

@dataclass
class ExpConfig:
    # PBS/Job Config
    result_dir: str
    pbs_output_dir: str
    working_dir: str
    walltime: str
    queue_name: str
    job_name: str
    num_nodes: int

    
    # Trace Config
    trace_config: Union[TraceGeneratorConfig, WeakScalingConfig]
    
    # Config Path - Path to config.yaml, defaults to script's directory
    config_path: str = "config.yaml"  
    # Experiment Config
    no_warmup: bool = False
    num_runs: int = 1
    
    # Model Configs - replaces the models section from config.yaml
    model_configs: List[ModelConfig] = field(default_factory=list)
    
    # Server Config - from config.yaml
    port: int = 8000
    storage_path: str = DEFAULT_STORAGE_PATH
    local_storage_path: str = DEFAULT_LOCAL_STORAGE_PATH
    disable_continuous_batching: bool = True
    router_replicas: int = 16
    sllm_mem_pool_size: str = "128GB"
    sllm_store_timeout: int = 900
    seed: int = 42
    
    # GPU Topology
    gpu_topology: GpuTopologyConfig = field(default_factory=lambda: GpuTopologyConfig())
    
    # API Server Config
    api_server: ApiServerConfig = field(default_factory=lambda: ApiServerConfig())
    
    def to_yaml_dict(self) -> Dict[str, Any]:
        """Convert ExpConfig to config.yaml format for replay_client compatibility"""
        # Build models list in the format expected by config.yaml
        models_list = []
        for model_cfg in self.model_configs:
            model_dict = {
                model_cfg.model_id: {
                    'mode': model_cfg.mode,
                    'tensor_parallel_size': model_cfg.tensor_parallel_size,
                    'size': model_cfg.size,
                    'num_replicas': model_cfg.num_replicas,
                    'node_index': model_cfg.node_index,
                }
            }
            # Remove None values
            model_dict[model_cfg.model_id] = {k: v for k, v in model_dict[model_cfg.model_id].items() if v is not None}
            models_list.append(model_dict)
        
        # Determine output_trace_path from trace_config
        if isinstance(self.trace_config, TraceGeneratorConfig):
            output_trace_path = self.trace_config.output_trace_path
        elif isinstance(self.trace_config, WeakScalingConfig):
            output_trace_path = self.trace_config.output_trace_path
        else:
            output_trace_path = ""
        
        yaml_dict = {
            'api_server': {
                'launch_sllm_service': self.api_server.launch_sllm_service,
                'path_save': self.api_server.path_save or self.result_dir,
                'requests_number': self.api_server.requests_number,
                'sorted_policy': self.api_server.sorted_policy,
            },
            'benchmark': {
                'output_result_dir': self.result_dir,
                'output_trace_path': output_trace_path,
            },
            'gpu_topology': {
                'num_gpus_per_node': self.gpu_topology.num_gpus_per_node,
                'num_nodes': self.num_nodes,  # Use num_nodes from ExpConfig
            },
            'disable_continuous_batching': self.disable_continuous_batching,
            'router_replicas': self.router_replicas,
            'local_storage_path': self.local_storage_path,
            'models': models_list,
            'port': self.port,
            'sllm_mem_pool_size': self.sllm_mem_pool_size,
            'sllm_store_timeout': self.sllm_store_timeout,
            'storage_path': self.storage_path,
            'seed': self.seed,
        }
        
        return yaml_dict
    
    def save_yaml(self, path: str):
        """Save the config as a YAML file for replay_client"""
        yaml_dict = self.to_yaml_dict()
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, 'w') as f:
            yaml.dump(yaml_dict, f, default_flow_style=False)


def peak_trace_config() -> ExpConfig:
    model_cfgs = [
        ModelConfig(model_id="meta-llama/Meta-Llama-3-8B", mode="chat", tensor_parallel_size=1, size=8, num_replicas=4),
    ]
    trace_cfg = TraceGeneratorConfig(
        input_trace_path=DEFAULT_INPUT_TRACE_PATH,
        input_prompt_path=DEFAULT_INPUT_PROMPT_PATH,
        duration=30.0,
        sampling_strategy='peak',
        speedup=10.0,
        output_len=128,
        output_trace_path=os.path.join(DEFAULT_OUTPUT_TRACE_DIR, 'peak_30s_10x.jsonl'),
        max_model_len=4096,
    )
    return ExpConfig(
        result_dir=os.path.join(DEFAULT_RESULTS_ROOT, "peak"),
        pbs_output_dir=os.path.join(DEFAULT_PBS_OUTPUT_ROOT, "peak"),
        working_dir=".",
        walltime="00:30:00",
        queue_name="debug",
        job_name="peak",
        num_nodes=1,
        trace_config=trace_cfg,
        model_configs=model_cfgs,
        config_path="config.yaml",
    )

def burst_trace_config() -> ExpConfig:
    model_cfgs = [
        ModelConfig(model_id="meta-llama/Meta-Llama-3-8B-Instruct", mode="chat", tensor_parallel_size=1, size=8, num_replicas=4),
    ]
    trace_cfg = TraceGeneratorConfig(
        input_trace_path=DEFAULT_INPUT_TRACE_PATH,
        input_prompt_path=DEFAULT_INPUT_PROMPT_PATH,
        duration=30.0,
        sampling_strategy='peak',
        speedup=10.0,
        output_len=128,
        output_trace_path=os.path.join(DEFAULT_OUTPUT_TRACE_DIR, 'peak_30s_10x.jsonl'),
        max_model_len=4096,
    )
    return ExpConfig(
        result_dir=os.path.join(DEFAULT_RESULTS_ROOT, "burst"),
        pbs_output_dir=os.path.join(DEFAULT_PBS_OUTPUT_ROOT, "burst"),
        working_dir=".",
        walltime="00:30:00",
        queue_name="debug",
        job_name="burst",
        num_nodes=1,
        trace_config=trace_cfg,
        model_configs=model_cfgs,
        config_path="config.yaml",
    )

def sparse_trace_config() -> ExpConfig:
    model_cfgs = [
        ModelConfig(model_id="meta-llama/Meta-Llama-3-8B-Instruct", mode="chat", tensor_parallel_size=1, size=8, num_replicas=4),
    ]
    trace_cfg = TraceGeneratorConfig(
        input_trace_path=DEFAULT_INPUT_TRACE_PATH,
        input_prompt_path=DEFAULT_INPUT_PROMPT_PATH,
        duration=300.0,
        sampling_strategy='sparse',
        speedup=300.0,
        output_len=128,
        output_trace_path=os.path.join(DEFAULT_OUTPUT_TRACE_DIR, 'sparse_300s_300x.jsonl'),
        max_model_len=4096,
    )
    return ExpConfig(   
        result_dir=os.path.join(DEFAULT_RESULTS_ROOT, "sparse"),
        pbs_output_dir=os.path.join(DEFAULT_PBS_OUTPUT_ROOT, "sparse"),
        working_dir=".",
        walltime="00:30:00",
        queue_name="debug",
        job_name="sparse",
        num_nodes=1,
        trace_config=trace_cfg,
        model_configs=model_cfgs,
        config_path="config.yaml",
    )

def get_example_trace_config() -> ExpConfig:
    """
    Returns an example ExpConfig for testing trace_generator.py
    """
    model_cfgs = [
        ModelConfig(model_id="meta-llama/Meta-Llama-3-8B", mode="chat", tensor_parallel_size=1, size=8, num_replicas=4),
    ]
    trace_cfg = TraceGeneratorConfig(
        input_trace_path=DEFAULT_INPUT_TRACE_PATH,
        input_prompt_path=DEFAULT_INPUT_PROMPT_PATH,
        duration=30.0,
        sampling_strategy='peak',
        speedup=10.0,
        output_len=128,
        max_model_len=4096,
        output_trace_path=os.path.join(DEFAULT_OUTPUT_TRACE_DIR, 'peak_300s_10x_0.jsonl')
    )
    
    return ExpConfig(
        result_dir=os.path.join(DEFAULT_RESULTS_ROOT, "manual_results"),
        pbs_output_dir="manual_pbs",
        working_dir=".",
        walltime="00:30:00",
        queue_name="debug",
        job_name="manual_run",
        num_nodes=1,
        trace_config=trace_cfg,
        model_configs=model_cfgs,
        config_path="config.yaml",
        no_warmup=False,
        num_runs=1
    )

# Weak scaling experiments, appear in pairs (MPI or Ray).
def get_weak_scaling_configs(backend: str = "ray") -> List[ExpConfig]:
    """
    Returns a list of ExpConfig objects for weak scaling experiments
    ranging from 1 to 16 nodes.
    
    Args:
        backend: Either "ray" or "mpi" to determine which backend to use
    """
    configs = []
    
    # Common parameters for weak scaling
    # nodes_list = [1, 2, 4, 8, 16]
    # nodes_list = [32]
    nodes_list = [64]
    rate_per_node = 32.0  # requests per node per second
    duration = 5.0                 
    input_len = 2048
    output_len = 512
    
    # Model configs
    model_cfgs = [
        ModelConfig(model_id="meta-llama/Meta-Llama-3-8B", mode="chat", tensor_parallel_size=1, size=8),
    ]
   
    # Root directory for this batch of experiments
    batch_name = f"weak_scaling_{backend}"
    base_working_dir = os.path.join("experiments", batch_name)

    for num_nodes in nodes_list:
        if num_nodes > 8:
            walltime = "3:00:00"
            queue_name = "prod"
        else:
            walltime = "01:00:00"
            queue_name = "debug-scaling"
        
        node_dir_name = f"{num_nodes}_nodes"
        
        # Paths
        exp_working_dir = os.path.join(base_working_dir, node_dir_name)
        exp_result_dir = os.path.join(DEFAULT_RESULTS_ROOT, batch_name, node_dir_name)
        exp_pbs_out_dir = os.path.join(DEFAULT_PBS_OUTPUT_ROOT, batch_name, node_dir_name)
        
        # Trace file in centralized location
        trace_output_path = os.path.join(DEFAULT_OUTPUT_TRACE_DIR, f"trace_weak_{backend}_{num_nodes}n.jsonl")

        # Create Trace Config
        trace_cfg = WeakScalingConfig(
            input_prompt_path=DEFAULT_INPUT_PROMPT_PATH,
            duration=duration,
            rpn=rate_per_node,
            input_len=input_len,
            output_len=output_len,
            output_trace_path=trace_output_path
        )

        # Config path inside the experiment's working directory
        config_file_path = os.path.join(exp_working_dir, "config.yaml")
        
        exp_cfg = ExpConfig(
            result_dir=exp_result_dir,
            pbs_output_dir=exp_pbs_out_dir,
            working_dir=exp_working_dir,
            walltime=walltime,
            queue_name=queue_name,
            job_name=f"{batch_name}_{num_nodes}n",
            num_nodes=num_nodes,
            trace_config=trace_cfg,
            model_configs=model_cfgs,
            config_path=config_file_path,
            no_warmup=False,
            num_runs=1
        )
        
        configs.append(exp_cfg)
    
    return configs

# Now generate weak scaling configs with warup and num_runs
def get_weak_scaling_configs_with_num_runs(backend: str = "ray", num_runs: int = 1) -> List[ExpConfig]:
    """
    Returns a list of ExpConfig objects for weak scaling experiments
    ranging from 1 to 16 nodes.
    
    Args:
        backend: Either "ray" or "mpi" to determine which backend to use
    """
    configs = []
    
    # Common parameters for weak scaling
    # nodes_list = [1, 2, 4, 8, 16]
    # nodes_list = [32]
    nodes_list = [1,2,4,8,16,32,64]
    rate_per_node = 32.0  # requests per node per second
    duration = 5.0                 
    input_len = 2048
    output_len = 512
    
    # Model configs
    model_cfgs = [
        ModelConfig(model_id="meta-llama/Meta-Llama-3-8B", mode="chat", tensor_parallel_size=1, size=8),
    ]
    
    # Root directory for this batch of experiments
    batch_name = f"weak_scaling_{backend}2"
    base_working_dir = os.path.join("experiments", batch_name)

    for num_nodes in nodes_list:
        if num_nodes > 8:
            walltime = "3:00:00"
            queue_name = "prod"
        else:
            walltime = "01:00:00"
            queue_name = "debug-scaling"
        
        node_dir_name = f"{num_nodes}_nodes"
        
        # Paths
        exp_working_dir = os.path.join(base_working_dir, node_dir_name)
        exp_result_dir = os.path.join(DEFAULT_RESULTS_ROOT, batch_name, node_dir_name)
        exp_pbs_out_dir = os.path.join(DEFAULT_PBS_OUTPUT_ROOT, batch_name, node_dir_name)
        
        # Trace file in centralized location
        trace_output_path = os.path.join(DEFAULT_OUTPUT_TRACE_DIR, f"trace_{batch_name}_{num_nodes}n.jsonl")

        # Create Trace Config
        trace_cfg = WeakScalingConfig(
            input_prompt_path=DEFAULT_INPUT_PROMPT_PATH,
            duration=duration,
            rpn=rate_per_node,
            input_len=input_len,
            output_len=output_len,
            output_trace_path=trace_output_path
        )

        # Config path inside the experiment's working directory
        config_file_path = os.path.join(exp_working_dir, "config.yaml")
        
        exp_cfg = ExpConfig(
            result_dir=exp_result_dir,
            pbs_output_dir=exp_pbs_out_dir,
            working_dir=exp_working_dir,
            walltime=walltime,
            queue_name=queue_name,
            job_name=f"{batch_name}_{num_nodes}n",
            num_nodes=num_nodes,
            trace_config=trace_cfg,
            model_configs=model_cfgs,
            config_path=config_file_path,
            no_warmup=False,
            num_runs=num_runs
        )
        
        configs.append(exp_cfg)
    
    return configs

    