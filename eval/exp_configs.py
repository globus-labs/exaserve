import os
import sys
from dataclasses import dataclass, replace
from typing import List, Dict, Optional

# Add src directory to path for schemas
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src'))
from schemas import (
    ModelConfig,
    DeploymentConfig,
    ReplayClientConfig,
    TraceGeneratorConfig,
    WeakScalingConfig,
    ProxyConfig,
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
DEFAULT_EXPERIMENTS_ROOT = os.path.join(DEFAULT_DATA_ROOT, "experiments")


# ---------------------------------------------------------------------------
# WeakScalingExpParams — all per-experiment-type knobs in one place
# ---------------------------------------------------------------------------

@dataclass
class WeakScalingExpParams:
    """
    Describes one weak-scaling experiment variant.
    Pass an instance to build_weak_scaling_configs() to get the full ExpConfig list.
    """
    # Experiment identity
    batch_name: str                       # supports {backend} placeholder, e.g. "weak_scaling_{backend}"
    num_nodes_list: List[int]
    null_compute: bool = False            # injects AURORA_NULL_COMPUTE=1 into the PBS job

    # Trace / workload
    rate_per_node: float = 80.0           # requests per node per second
    duration: float = 5.0
    input_len: int = 2048
    output_len: int = 512

    # Model (→ ModelConfig)
    model_id: str = "meta-llama/Meta-Llama-3-8B-Instruct"
    model_tensor_parallel_size: int = 1
    model_max_model_len: int = 4096
    model_size: int = 8                   # used by trace generator
    model_storage_path: str = DEFAULT_MODEL_PATH

    # Deployment (→ DeploymentConfig)
    deployment_worker_max_ongoing: int = 64

    # Client (→ ReplayClientConfig)
    client_num_runs: int = 1
    client_num_go_procs: int = 16          # number of Go processes per replay_client node
    client_num_go_workers: int = 2        # dispatch goroutines (N) inside each Go process
    client_go_concurrency: int = 40     # max in-flight requests per Go process
    client_warmup_rps: int = 0            # warm-up requests per second (0 = no warmup)
    client_warmup_duration_s: float = 0.0 # warm-up duration in seconds
    client_dest: str = "proxy"            # "proxy": local workers → proxy (ignores MPI args)
                                          # "direct": MPI round-robin to servers (no proxy)

    # Proxy (→ ProxyConfig)
    proxy_type: str = "litellm"
    proxy_python_path: str = "/home/wenyiw/agpt/venv/litellm/bin/python3"
    proxy_num_workers: int = 1            # fixed litellm uvicorn worker count; does not scale with num_nodes


def _walltime_and_queue(num_nodes: int):
    """Return (walltime, queue_name) based on node count."""
    if num_nodes <= 2:
        return "01:00:00", "debug"
    elif num_nodes <= 256:
        return "01:00:00", "debug-scaling"
    else:
        return "02:00:00", "prod"


def build_weak_scaling_configs(backend: str, params: WeakScalingExpParams) -> List[ExpConfig]:
    """
    Build a list of ExpConfig objects from a WeakScalingExpParams descriptor.

    Args:
        backend: "ray" or "mpi"
        params:  A WeakScalingExpParams instance from EXPERIMENT_REGISTRY
    """
    batch_name = params.batch_name.format(backend=backend)
    base_exp_dir = os.path.join(DEFAULT_EXPERIMENTS_ROOT, batch_name)

    model_cfgs = [
        ModelConfig(
            model_id=params.model_id,
            tensor_parallel_size=params.model_tensor_parallel_size,
            max_model_len=params.model_max_model_len,
            size=params.model_size,
        ),
    ]

    configs = []
    for num_nodes in params.num_nodes_list:
        if num_nodes < 1:
            raise ValueError(f"num_nodes must be >= 1, got {num_nodes}")

        walltime, queue_name = _walltime_and_queue(num_nodes)

        node_dir_name = f"{num_nodes}_nodes"
        node_base       = os.path.join(base_exp_dir, node_dir_name)
        pbs_working_dir = os.path.join(node_base, "config")
        pbs_result_dir  = os.path.join(node_base, "results")
        pbs_stdout_dir  = os.path.join(node_base, "pbs_output", "stdout")
        pbs_stderr_dir  = os.path.join(node_base, "pbs_output", "stderr")
        trace_output_path = os.path.join(DEFAULT_OUTPUT_TRACE_DIR, f"{batch_name}_{num_nodes}n.jsonl")
        config_file_path  = os.path.join(pbs_working_dir, "config.yaml")

        trace_cfg = WeakScalingConfig(
            input_prompt_path=DEFAULT_INPUT_PROMPT_PATH,
            duration=params.duration,
            rpn=params.rate_per_node,
            input_len=params.input_len,
            output_len=params.output_len,
            output_trace_path=trace_output_path,
        )
        dep = DeploymentConfig(
            deployment_name=batch_name,
            model_configs=model_cfgs,
            num_nodes=num_nodes,
            model_storage_path=params.model_storage_path,
            worker_max_ongoing=params.deployment_worker_max_ongoing,
        )
        replay_cfg = ReplayClientConfig(
            config_path=config_file_path,
            num_runs=params.client_num_runs,
            dest=params.client_dest,
            num_nodes=num_nodes,
            num_go_procs=params.client_num_go_procs,
            num_go_workers=params.client_num_go_workers,
            go_concurrency=params.client_go_concurrency,
            warmup_rps=params.client_warmup_rps,
            warmup_duration_s=params.client_warmup_duration_s,
        )
        proxy_cfg = ProxyConfig(
            type=params.proxy_type,
            python_path=params.proxy_python_path,
            num_workers=params.proxy_num_workers,
        )

        exp_cfg = ExpConfig(
            pbs_result_dir=pbs_result_dir,
            pbs_stdout_dir=pbs_stdout_dir,
            pbs_stderr_dir=pbs_stderr_dir,
            pbs_num_nodes=num_nodes,
            pbs_walltime=walltime,
            pbs_queue_name=queue_name,
            pbs_job_name=f"{batch_name}_{num_nodes}n",
            pbs_working_dir=pbs_working_dir,
            job_replay_client_config=replay_cfg,
            job_trace_config=trace_cfg,
            job_seed=42,
            model_deployment_config=dep,
            proxy_config=proxy_cfg,
        )
        configs.append(exp_cfg)
    return configs


# ---------------------------------------------------------------------------
# Experiment registry — add new variants here, one dict entry each
# ---------------------------------------------------------------------------

EXPERIMENT_REGISTRY: Dict[str, WeakScalingExpParams] = {
    # -- proxy-mode null_compute
    # "NC_litellm_pnw_2": WeakScalingExpParams( # pnw=proxy_num_worker
    #     batch_name="NC_litellm_pnw_2_{backend}",
    #     num_nodes_list=[1,2,4,8,16,32],
    #     null_compute=True,
    #     rate_per_node=100,
    #     client_num_runs=3,
    #     client_dest="proxy",
    #     proxy_type="litellm",
    #     proxy_num_workers=2
    # ),
    
    # --- proxy-mode experiments (litellm in front, local client workers) ---
    "null_compute_litellm": WeakScalingExpParams(
        batch_name="null_compute_litellm_{backend}",
        num_nodes_list=[1, 2, 4, 8, 16, 32, 64],
        null_compute=True,
        rate_per_node=80,
        client_num_runs=3,
        client_dest="proxy",
    ),
    "weak_scaling_litellm": WeakScalingExpParams(
        batch_name="weak_scaling_litellm_{backend}",
        num_nodes_list=[1, 2, 4, 8, 16, 32, 64],
        rate_per_node=80,
        client_num_runs=3,
        client_dest="proxy",
    ),
    # --- direct-mode experiments (MPI round-robin, no proxy, lower-bound baseline) ---
    "weak_scaling": WeakScalingExpParams(
        batch_name="weak_scaling_{backend}_2",
        num_nodes_list=[1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024],
        rate_per_node=80,
        client_num_runs=3,
        client_dest="direct",
        proxy_type="none",
    ),
    "null_compute": WeakScalingExpParams(
        batch_name="null_compute_{backend}",
        num_nodes_list=[256, 512, 1024, 2048],
        null_compute=True,
        rate_per_node=80,
        client_num_runs=3,
        client_dest="direct",
        proxy_type="none",
    ),
    "weak_scaling_tests": WeakScalingExpParams(
        batch_name="weak_scaling_tests_0125_cli_1_workers_{backend}",
        num_nodes_list=[1, 2, 4, 8],
        rate_per_node=80,
        client_num_runs=1,
        client_dest="direct",
        proxy_type="none",
    ),
    "null_compute_tests": WeakScalingExpParams(
        batch_name="null_compute_tests_{backend}",
        num_nodes_list=[1, 2, 4, 8, 16],
        null_compute=True,
        rate_per_node=80,
        client_num_runs=1,
        client_dest="direct",
        proxy_type="none",
    ),
}
for experiment_name in ("null_compute_litellm", "weak_scaling_litellm"):
    base_params = EXPERIMENT_REGISTRY[experiment_name]
    for proxy_num_workers in (2, 4, 8):
        EXPERIMENT_REGISTRY[f"{experiment_name}_pnw_{proxy_num_workers}"] = replace(
            base_params,
            batch_name=base_params.batch_name.replace(
                "_{backend}", f"_pnw_{proxy_num_workers}" + "_{backend}"
            ),
            proxy_num_workers=proxy_num_workers,
        )


# ---------------------------------------------------------------------------
# One-off / legacy single-config functions (kept for manual / ad-hoc use)
# ---------------------------------------------------------------------------

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
    node_base = os.path.join(DEFAULT_EXPERIMENTS_ROOT, "peak", "1_nodes")
    dep = DeploymentConfig(
        model_configs=model_cfgs,
        num_gpus_per_node=4,
        num_nodes=1,
    )
    return ExpConfig(
        pbs_result_dir=os.path.join(node_base, "results"),
        pbs_stdout_dir=os.path.join(node_base, "pbs_output", "stdout"),
        pbs_stderr_dir=os.path.join(node_base, "pbs_output", "stderr"),
        pbs_num_nodes=1,
        pbs_walltime="00:30:00",
        pbs_queue_name="debug",
        pbs_job_name="peak",
        pbs_working_dir=os.path.join(node_base, "config"),
        job_replay_client_config=ReplayClientConfig(config_path=os.path.join(node_base, "config", "config.yaml")),
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
    node_base = os.path.join(DEFAULT_EXPERIMENTS_ROOT, "burst", "1_nodes")
    dep = DeploymentConfig(model_configs=model_cfgs, num_gpus_per_node=4, num_nodes=1)
    return ExpConfig(
        pbs_result_dir=os.path.join(node_base, "results"),
        pbs_stdout_dir=os.path.join(node_base, "pbs_output", "stdout"),
        pbs_stderr_dir=os.path.join(node_base, "pbs_output", "stderr"),
        pbs_num_nodes=1,
        pbs_walltime="00:30:00",
        pbs_queue_name="debug",
        pbs_job_name="burst",
        pbs_working_dir=os.path.join(node_base, "config"),
        job_replay_client_config=ReplayClientConfig(config_path=os.path.join(node_base, "config", "config.yaml")),
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
    node_base = os.path.join(DEFAULT_EXPERIMENTS_ROOT, "sparse", "2_nodes")
    dep = DeploymentConfig(model_configs=model_cfgs, num_nodes=num_nodes)
    return ExpConfig(
        pbs_result_dir=os.path.join(node_base, "results"),
        pbs_stdout_dir=os.path.join(node_base, "pbs_output", "stdout"),
        pbs_stderr_dir=os.path.join(node_base, "pbs_output", "stderr"),
        pbs_num_nodes=2,
        pbs_walltime="00:30:00",
        pbs_queue_name="debug",
        pbs_job_name="sparse",
        pbs_working_dir=os.path.join(node_base, "config"),
        job_replay_client_config=ReplayClientConfig(config_path=os.path.join(node_base, "config", "config.yaml"), dest="direct"),
        job_trace_config=trace_cfg,
        job_seed=42,
        model_deployment_config=dep,
    )

def get_example_trace_config() -> ExpConfig:
    """Returns an example ExpConfig for testing trace_generator.py"""
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
    node_base = os.path.join(DEFAULT_EXPERIMENTS_ROOT, "manual", "1_nodes")
    dep = DeploymentConfig(model_configs=model_cfgs, num_gpus_per_node=4, num_nodes=1)
    return ExpConfig(
        pbs_result_dir=os.path.join(node_base, "results"),
        pbs_stdout_dir=os.path.join(node_base, "pbs_output", "stdout"),
        pbs_stderr_dir=os.path.join(node_base, "pbs_output", "stderr"),
        pbs_num_nodes=1,
        pbs_walltime="00:30:00",
        pbs_queue_name="debug",
        pbs_job_name="manual_run",
        pbs_working_dir=os.path.join(node_base, "config"),
        job_replay_client_config=ReplayClientConfig(config_path=os.path.join(node_base, "config", "config.yaml"), num_runs=1),
        job_trace_config=trace_cfg,
        job_seed=42,
        model_deployment_config=dep,
    )
