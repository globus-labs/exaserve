import os
import sys
from typing import List

# Add src directory to path for schemas
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src'))
from schemas import (
    ModelConfig,
    DeploymentConfig,
    ReplayClientConfig,
    TraceGeneratorConfig,
    WeakScalingConfig,
    ExpConfig,
)

# Constants
DEFAULT_DATA_ROOT = "/lus/flare/projects/AuroraGPT/wenyiw/data"
DEFAULT_MODEL_PATH = "/lus/flare/projects/AuroraGPT/wenyiw/models"

# Download before running the experiment
# wget https://azurepublicdatasettraces.blob.core.windows.net/azurellminfererencetrace/AzureLLMInferenceTrace_code_1week.csv
# wget https://huggingface.co/datasets/anon8231489123/ShareGPT_Vicuna_unfiltered/resolve/main/ShareGPT_V3_unfiltered_cleaned_split_no_imsorry.json
DEFAULT_INPUT_TRACE_PATH = os.path.join(DEFAULT_DATA_ROOT, "input_traces/AzureLLMInferenceTrace_code_1week.csv") 
DEFAULT_INPUT_PROMPT_PATH = os.path.join(DEFAULT_DATA_ROOT, "input_traces/ShareGPT_V3_unfiltered_cleaned_split_no_imsorry.json")
DEFAULT_OUTPUT_TRACE_DIR = os.path.join(DEFAULT_DATA_ROOT, "output_traces")
DEFAULT_RESULTS_ROOT = os.path.join(DEFAULT_DATA_ROOT, "results")
DEFAULT_PBS_OUTPUT_ROOT = os.path.join(DEFAULT_DATA_ROOT, "pbs_output")


def peak_trace_config() -> ExpConfig:
    model_cfgs = [
        ModelConfig(
            model_id="meta-llama/Meta-Llama-3-8B",
            tensor_parallel_size=1,
            max_model_len=4096,
            size=8,
            num_replicas=4,
        ),
    ]
    trace_cfg = TraceGeneratorConfig(
        input_trace_path=DEFAULT_INPUT_TRACE_PATH,
        input_prompt_path=DEFAULT_INPUT_PROMPT_PATH,
        duration=30.0,
        sampling_strategy='peak',
        speedup=10.0,
        output_len=128,
        output_trace_path=os.path.join(DEFAULT_OUTPUT_TRACE_DIR, 'peak_30s_10x.jsonl'),
    )
    pbs_out = os.path.join(DEFAULT_PBS_OUTPUT_ROOT, "peak")
    dep = DeploymentConfig(
        model_configs=model_cfgs,
        num_gpus_per_node=4,
        num_nodes=1,
    )
    return ExpConfig(
        pbs_result_dir=os.path.join(DEFAULT_RESULTS_ROOT, "peak"),
        pbs_stdout_dir=os.path.join(pbs_out, "stdout"),
        pbs_stderr_dir=os.path.join(pbs_out, "stderr"),
        pbs_num_nodes=1,
        pbs_walltime="00:30:00",
        pbs_queue_name="debug",
        pbs_job_name="peak",
        pbs_working_dir=".",
        job_replay_client_config=ReplayClientConfig(config_path="config.yaml"),
        job_trace_config=trace_cfg,
        job_seed=42,
        model_deployment_config=dep,
    )

def burst_trace_config() -> ExpConfig:
    model_cfgs = [
        ModelConfig(
            model_id="meta-llama/Meta-Llama-3-8B-Instruct",
            tensor_parallel_size=1,
            max_model_len=4096,
            size=8,
            num_replicas=4,
        ),
    ]
    trace_cfg = TraceGeneratorConfig(
        input_trace_path=DEFAULT_INPUT_TRACE_PATH,
        input_prompt_path=DEFAULT_INPUT_PROMPT_PATH,
        duration=30.0,
        sampling_strategy='peak',
        speedup=10.0,
        output_len=128,
        output_trace_path=os.path.join(DEFAULT_OUTPUT_TRACE_DIR, 'peak_30s_10x.jsonl'),
    )
    pbs_out = os.path.join(DEFAULT_PBS_OUTPUT_ROOT, "burst")
    dep = DeploymentConfig(model_configs=model_cfgs, num_gpus_per_node=4, num_nodes=1)
    return ExpConfig(
        pbs_result_dir=os.path.join(DEFAULT_RESULTS_ROOT, "burst"),
        pbs_stdout_dir=os.path.join(pbs_out, "stdout"),
        pbs_stderr_dir=os.path.join(pbs_out, "stderr"),
        pbs_num_nodes=1,
        pbs_walltime="00:30:00",
        pbs_queue_name="debug",
        pbs_job_name="burst",
        pbs_working_dir=".",
        job_replay_client_config=ReplayClientConfig(config_path="config.yaml"),
        job_trace_config=trace_cfg,
        job_seed=42,
        model_deployment_config=dep,
    )

def sparse_trace_config() -> ExpConfig:
    num_nodes = 2
    model_cfgs = [
        ModelConfig(
            model_id="meta-llama/Meta-Llama-3-8B-Instruct",
            tensor_parallel_size=1,
            max_model_len=4096,
            size=8,
            num_replicas=None,
        ),
    ]
    trace_cfg = TraceGeneratorConfig(
        input_trace_path=DEFAULT_INPUT_TRACE_PATH,
        input_prompt_path=DEFAULT_INPUT_PROMPT_PATH,
        duration=300.0,
        sampling_strategy='sparse',
        speedup=300.0,
        output_len=128,
        output_trace_path=os.path.join(DEFAULT_OUTPUT_TRACE_DIR, 'sparse_300s_300x.jsonl'),
    )
    pbs_out = os.path.join(DEFAULT_PBS_OUTPUT_ROOT, "sparse")
    dep = DeploymentConfig(model_configs=model_cfgs, num_nodes=num_nodes)
    return ExpConfig(
        pbs_result_dir=os.path.join(DEFAULT_RESULTS_ROOT, "sparse"),
        pbs_stdout_dir=os.path.join(pbs_out, "stdout"),
        pbs_stderr_dir=os.path.join(pbs_out, "stderr"),
        pbs_num_nodes=2,
        pbs_walltime="00:30:00",
        pbs_queue_name="debug",
        pbs_job_name="sparse",
        pbs_working_dir=".",
        job_replay_client_config=ReplayClientConfig(config_path="config.yaml", dest="cluster"),
        job_trace_config=trace_cfg,
        job_seed=42,
        model_deployment_config=dep,
    )

def get_example_trace_config() -> ExpConfig:
    """
    Returns an example ExpConfig for testing trace_generator.py
    """
    model_cfgs = [
        ModelConfig(
            model_id="meta-llama/Meta-Llama-3-8B",
            tensor_parallel_size=1,
            max_model_len=4096,
            size=8,
            num_replicas=4,
        ),
    ]
    trace_cfg = TraceGeneratorConfig(
        input_trace_path=DEFAULT_INPUT_TRACE_PATH,
        input_prompt_path=DEFAULT_INPUT_PROMPT_PATH,
        duration=30.0,
        sampling_strategy='peak',
        speedup=10.0,
        output_len=128,
        output_trace_path=os.path.join(DEFAULT_OUTPUT_TRACE_DIR, 'peak_300s_10x_0.jsonl'),
    )
    dep = DeploymentConfig(model_configs=model_cfgs, num_gpus_per_node=4, num_nodes=1)
    return ExpConfig(
        pbs_result_dir=os.path.join(DEFAULT_RESULTS_ROOT, "manual_results"),
        pbs_stdout_dir=os.path.join("manual_pbs", "stdout"),
        pbs_stderr_dir=os.path.join("manual_pbs", "stderr"),
        pbs_num_nodes=1,
        pbs_walltime="00:30:00",
        pbs_queue_name="debug",
        pbs_job_name="manual_run",
        pbs_working_dir=".",
        job_replay_client_config=ReplayClientConfig(config_path="config.yaml", no_warmup=False, num_runs=1),
        job_trace_config=trace_cfg,
        job_seed=42,
        model_deployment_config=dep,
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
    num_nodes_list = [1, 2, 4, 8, 16, 32, 64]
    rate_per_node = 32.0  # requests per node per second
    duration = 5.0                 
    input_len = 2048
    output_len = 512
    
    # Model configs
    model_cfgs = [
        ModelConfig(
            model_id="meta-llama/Meta-Llama-3-8B-Instruct",
            tensor_parallel_size=1,
            max_model_len=4096,
            size=8,
        ),
    ]
    batch_name = f"weak_scaling_{backend}"
    base_working_dir = os.path.join("experiments", batch_name)

    for num_nodes in num_nodes_list:
        if num_nodes > 1:
            walltime = "1:00:00"
            queue_name = "debug-scaling"
        else:
            walltime = "01:00:00"
            queue_name = "debug"
        node_dir_name = f"{num_nodes}_nodes"
        pbs_working_dir = os.path.join(base_working_dir, node_dir_name)
        pbs_result_dir = os.path.join(DEFAULT_RESULTS_ROOT, batch_name, node_dir_name)
        pbs_stdout_dir = os.path.join(DEFAULT_PBS_OUTPUT_ROOT, batch_name, node_dir_name, "stdout")
        pbs_stderr_dir = os.path.join(DEFAULT_PBS_OUTPUT_ROOT, batch_name, node_dir_name, "stderr")
        trace_output_path = os.path.join(DEFAULT_OUTPUT_TRACE_DIR, f"{batch_name}_{backend}_{num_nodes}n.jsonl")
        trace_cfg = WeakScalingConfig(
            input_prompt_path=DEFAULT_INPUT_PROMPT_PATH,
            duration=duration,
            rpn=rate_per_node,
            input_len=input_len,
            output_len=output_len,
            output_trace_path=trace_output_path,
        )
        config_file_path = os.path.join(exp_working_dir, "config.yaml")
        dep = DeploymentConfig(model_configs=model_cfgs, num_gpus_per_node=4, num_nodes=num_nodes)
        exp_cfg = ExpConfig(
            pbs_result_dir=exp_result_dir,
            pbs_stdout_dir=os.path.join(exp_pbs_out_dir, "stdout"),
            pbs_stderr_dir=os.path.join(exp_pbs_out_dir, "stderr"),
            pbs_num_nodes=num_nodes,
            pbs_walltime=walltime,
            pbs_queue_name=queue_name,
            pbs_job_name=f"{batch_name}_{num_nodes}n",
            pbs_working_dir=exp_working_dir,
            job_replay_client_config=ReplayClientConfig(config_path=config_file_path, no_warmup=False, num_runs=1, dest="cluster" if num_nodes > 1 else "node", num_nodes=num_nodes),
            job_trace_config=trace_cfg,
            job_seed=42,
            model_deployment_config=dep,
        )
        configs.append(exp_cfg)
    return configs


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
    num_nodes_list = [1,2,4,8,16,32,64,128,256]
    # num_nodes_list = [128, 256]
    rate_per_node = 80 # requests per node per second
    duration = 5.0                 
    input_len = 2048
    output_len = 512
    
    # Model configs
    model_cfgs = [
        ModelConfig(
            model_id="meta-llama/Meta-Llama-3-8B-Instruct",
            tensor_parallel_size=1,
            max_model_len=4096,
            size=8,
        ),
    ]
    batch_name = f"weak_scaling_{backend}"
    EVAL_DIR = os.path.join(os.path.dirname(__file__))
    base_working_dir = os.path.join(EVAL_DIR, "experiments", batch_name)

    for num_nodes in num_nodes_list:
        if num_nodes > 1:
            walltime = "1:00:00"
            queue_name = "debug-scaling"
        else:
            walltime = "01:00:00"
            queue_name = "debug"
        node_dir_name = f"{num_nodes}_nodes"
        pbs_working_dir = os.path.join(base_working_dir, node_dir_name)
        pbs_result_dir = os.path.join(DEFAULT_RESULTS_ROOT, batch_name, node_dir_name)
        pbs_stdout_dir = os.path.join(DEFAULT_PBS_OUTPUT_ROOT, batch_name, node_dir_name, "stdout")
        pbs_stderr_dir = os.path.join(DEFAULT_PBS_OUTPUT_ROOT, batch_name, node_dir_name, "stderr")
        trace_output_path = os.path.join(DEFAULT_OUTPUT_TRACE_DIR, f"{batch_name}_{num_nodes}n.jsonl")
        trace_cfg = WeakScalingConfig(
            input_prompt_path=DEFAULT_INPUT_PROMPT_PATH,
            duration=duration,
            rpn=rate_per_node,
            input_len=input_len,
            output_len=output_len,
            output_trace_path=trace_output_path,
        )
        config_file_path = os.path.join(pbs_working_dir, "config.yaml")
        dep = DeploymentConfig(model_configs=model_cfgs, num_nodes=num_nodes)
        exp_cfg = ExpConfig(
            pbs_result_dir=pbs_result_dir,
            pbs_stdout_dir=pbs_stdout_dir,
            pbs_stderr_dir=pbs_stderr_dir,
            pbs_num_nodes=num_nodes,
            pbs_walltime=walltime,
            pbs_queue_name=queue_name,
            pbs_job_name=f"{batch_name}_{num_nodes}n",
            pbs_working_dir=pbs_working_dir,
            job_replay_client_config=ReplayClientConfig(
                config_path=config_file_path,
                no_warmup=False,
                num_runs=num_runs,
                dest="cluster" if num_nodes > 1 else "node",
                num_nodes=num_nodes,
                num_cli_per_node=0.125,
            ),
            job_trace_config=trace_cfg,
            job_seed=42,
            model_deployment_config=dep,
        )
        configs.append(exp_cfg)
    return configs


def get_weak_scaling_null_compute_configs_with_num_runs(backend: str = "ray", num_runs: int = 1) -> List[ExpConfig]:
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
    num_nodes_list = [1,2,4,8,16,32,64,128]
    # num_nodes_list = [128, 256]
    rate_per_node = 80 # requests per node per second
    duration = 5.0                 
    input_len = 2048
    output_len = 512
    
    # Model configs
    model_cfgs = [
        ModelConfig(
            model_id="meta-llama/Meta-Llama-3-8B-Instruct",
            tensor_parallel_size=1,
            max_model_len=4096,
            size=8,
        ),
    ]
    batch_name = f"null_compute_{backend}"
    EVAL_DIR = os.path.join(os.path.dirname(__file__))
    base_working_dir = os.path.join(EVAL_DIR, "experiments", batch_name)

    for num_nodes in num_nodes_list:
        if num_nodes > 1:
            walltime = "1:00:00"
            queue_name = "debug-scaling"
        else:
            walltime = "01:00:00"
            queue_name = "debug"
        node_dir_name = f"{num_nodes}_nodes"
        pbs_working_dir = os.path.join(base_working_dir, node_dir_name)
        pbs_result_dir = os.path.join(DEFAULT_RESULTS_ROOT, batch_name, node_dir_name)
        pbs_stdout_dir = os.path.join(DEFAULT_PBS_OUTPUT_ROOT, batch_name, node_dir_name, "stdout")
        pbs_stderr_dir = os.path.join(DEFAULT_PBS_OUTPUT_ROOT, batch_name, node_dir_name, "stderr")
        trace_output_path = os.path.join(DEFAULT_OUTPUT_TRACE_DIR, f"{batch_name}_{num_nodes}n.jsonl")
        trace_cfg = WeakScalingConfig(
            input_prompt_path=DEFAULT_INPUT_PROMPT_PATH,
            duration=duration,
            rpn=rate_per_node,
            input_len=input_len,
            output_len=output_len,
            output_trace_path=trace_output_path,
        )
        config_file_path = os.path.join(pbs_working_dir, "config.yaml")
        dep = DeploymentConfig(model_configs=model_cfgs, num_nodes=num_nodes)
        exp_cfg = ExpConfig(
            pbs_result_dir=pbs_result_dir,
            pbs_stdout_dir=pbs_stdout_dir,
            pbs_stderr_dir=pbs_stderr_dir,
            pbs_num_nodes=num_nodes,
            pbs_walltime=walltime,
            pbs_queue_name=queue_name,
            pbs_job_name=f"{batch_name}_{num_nodes}n",
            pbs_working_dir=pbs_working_dir,
            job_replay_client_config=ReplayClientConfig(
                config_path=config_file_path,
                no_warmup=False,
                num_runs=num_runs,
                dest="cluster" if num_nodes > 1 else "node",
                num_nodes=num_nodes,
                num_cli_per_node=0.125,
            ),
            job_trace_config=trace_cfg,
            job_seed=42,
            model_deployment_config=dep,
        )
        configs.append(exp_cfg)
    return configs
