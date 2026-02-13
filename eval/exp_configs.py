import os
from dataclasses import dataclass
from typing import List, Union, Optional

# Constants
DATA_ROOT = "/home/wenyiw/agpt/mpi-vllm-nccl/data"
RESULTS_ROOT = os.path.join(DATA_ROOT, "results")
PBS_OUTPUT_ROOT = os.path.join(DATA_ROOT, "pbs_output")
# Download before running the experiment
# wget https://azurepublicdatasettraces.blob.core.windows.net/azurellminfererencetrace/AzureLLMInferenceTrace_code_1week.csv
# wget https://huggingface.co/datasets/anon8231489123/ShareGPT_Vicuna_unfiltered/resolve/main/ShareGPT_V3_unfiltered_cleaned_split_no_imsorry.json
DEFAULT_SRC_TRACE_PATH = os.path.join(DATA_ROOT, "input_traces/AzureLLMInferenceTrace_code_1week.csv") 
DEFAULT_PROMPT_PATH = os.path.join(DATA_ROOT, "input_traces/ShareGPT_V3_unfiltered_cleaned_split_no_imsorry.json")
DEFAULT_OUTPUT_TRACE_DIR = os.path.join(DATA_ROOT, "output_traces")

@dataclass
class TraceConfig:
    src_prompt_path: str
    src_trace_path: str
    duration: float
    sampling_strategy: str
    speedup: float
    max_model_len: int
    output_path: str

@dataclass
class WeakScalingConfig:
    src_prompt_path: str
    duration: float
    rpn: float
    input_len: int
    output_len: int
    output_path: str

@dataclass
class ExpConfig:
    result_dir: str
    pbs_output_dir: str
    working_dir: str
    walltime: str
    queue_name: str
    job_name: str
    num_nodes: int
    trace_config: Union[TraceConfig, WeakScalingConfig]
    no_warmup: bool = False
    num_runs: int = 1

def get_example_trace_config() -> ExpConfig:
    """
    Returns an example ExpConfig for testing trace_generator.py
    """
    trace_cfg = TraceConfig(
        src_prompt_path=DEFAULT_PROMPT_PATH,
        src_trace_path=DEFAULT_SRC_TRACE_PATH,
        duration=30.0,
        sampling_strategy='peak',
        speedup=10.0,
        max_model_len=4096,
        output_path=os.path.join(DEFAULT_OUTPUT_TRACE_DIR, 'peak_300s_10x_0.jsonl')
    )
    
    return ExpConfig(
        result_dir=os.path.join(RESULTS_ROOT, "manual_results"),
        pbs_output_dir="manual_pbs",
        working_dir=".",
        walltime="00:30:00",
        queue_name="debug",
        job_name="manual_run",
        num_nodes=1,
        trace_config=trace_cfg,
        no_warmup=False,
        num_runs=1
    )

def get_example_sparse_trace_config() -> ExpConfig:
    """
    Returns an example ExpConfig for testing trace_generator.py
    """
    trace_cfg = TraceConfig(
        src_prompt_path=DEFAULT_PROMPT_PATH,
        src_trace_path=DEFAULT_SRC_TRACE_PATH,
        duration=300.0,
        sampling_strategy='sparse',
        speedup=300.0,
        max_model_len=4096,
        output_path=os.path.join(DEFAULT_OUTPUT_TRACE_DIR, 'sparse_300s_300x_0.jsonl')
    )
    
    return ExpConfig(
        result_dir=os.path.join(RESULTS_ROOT, "manual_results"),
        pbs_output_dir="manual_pbs",
        working_dir=".",
        walltime="00:30:00",
        queue_name="debug",
        job_name="manual_run",
        num_nodes=1,
        trace_config=trace_cfg,
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
        # Unique identifier logic could be handled here or in manager, 
        # but ExpConfig defines the intent.
        
        # Calculate derived metrics
        # total_rps = rate_per_node * num_nodes
        
        node_dir_name = f"{num_nodes}_nodes"
        
        # Paths
        exp_working_dir = os.path.join(base_working_dir, node_dir_name)
        exp_result_dir = os.path.join(RESULTS_ROOT, batch_name, node_dir_name)
        exp_pbs_out_dir = os.path.join(PBS_OUTPUT_ROOT, batch_name, node_dir_name)
        
        # [NEW] Trace file in centralized location, NOT inside working_dir
        trace_output_path = os.path.join(DEFAULT_OUTPUT_TRACE_DIR, f"trace_weak_{backend}_{num_nodes}n.jsonl")

        # Create Trace Config
        trace_cfg = WeakScalingConfig(
            src_prompt_path=DEFAULT_PROMPT_PATH,
            duration=duration,
            rpn=rate_per_node,
            input_len=input_len,
            output_len=output_len,
            output_path=trace_output_path
        )

        exp_cfg = ExpConfig(
            result_dir=exp_result_dir,
            pbs_output_dir=exp_pbs_out_dir,
            working_dir=exp_working_dir,
            walltime=walltime,
            queue_name=queue_name,
            job_name=f"{base_job_name}_{num_nodes}n",
            num_nodes=num_nodes,
            trace_config=trace_cfg,
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
        # Unique identifier logic could be handled here or in manager, 
        # but ExpConfig defines the intent.
        
        # Calculate derived metrics
        # total_rps = rate_per_node * num_nodes
        
        node_dir_name = f"{num_nodes}_nodes"
        
        # Paths
        exp_working_dir = os.path.join(base_working_dir, node_dir_name)
        exp_result_dir = os.path.join(RESULTS_ROOT, batch_name, node_dir_name)
        exp_pbs_out_dir = os.path.join(PBS_OUTPUT_ROOT, batch_name, node_dir_name)
        
        # [NEW] Trace file in centralized location, NOT inside working_dir
        trace_output_path = os.path.join(DEFAULT_OUTPUT_TRACE_DIR, f"trace_{batch_name}_{num_nodes}n.jsonl")

        # Create Trace Config
        trace_cfg = WeakScalingConfig(
            src_prompt_path=DEFAULT_PROMPT_PATH,
            duration=duration,
            rpn=rate_per_node,
            input_len=input_len,
            output_len=output_len,
            output_path=trace_output_path
        )

        exp_cfg = ExpConfig(
            result_dir=exp_result_dir,
            pbs_output_dir=exp_pbs_out_dir,
            working_dir=exp_working_dir,
            walltime=walltime,
            queue_name=queue_name,
            job_name=f"{batch_name}_{num_nodes}n",
            num_nodes=num_nodes,
            trace_config=trace_cfg,
            no_warmup=False,
            num_runs=num_runs
        )
        
        configs.append(exp_cfg)
    
    return configs

    