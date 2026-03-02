import argparse
import yaml
import os
import sys
import shutil
import stat
from concurrent.futures import ProcessPoolExecutor, as_completed

# Ensure the script directory is in the Python path for reliable imports
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from exp_configs import *
from trace_generator import TraceGenerator
VALID_BACKENDS = ["ray", "mpi"]


def _setup_one_weak_scaling_experiment(item):
    """
    Worker: generate one experiment (trace, config, job.pbs).
    item: (index, exp_cfg, pbs_template_content, run_exp_script, backend, null_compute)
    Returns: (index, submit_cmd)
    """
    idx, exp_cfg, pbs_template_content, run_exp_script, backend, null_compute = item
    os.makedirs(exp_cfg.pbs_working_dir, exist_ok=True)
    os.makedirs(exp_cfg.pbs_stdout_dir, exist_ok=True)
    os.makedirs(exp_cfg.pbs_stderr_dir, exist_ok=True)
    os.makedirs(exp_cfg.pbs_result_dir, exist_ok=True)
    assert exp_cfg.pbs_num_nodes == exp_cfg.model_deployment_config.num_nodes, "PBS nodes and deployment nodes must match"
    trace_gen = TraceGenerator(exp_cfg)
    if isinstance(exp_cfg.job_trace_config, WeakScalingConfig):
        trace_gen.generate_weak_scaling(exp_cfg)
    elif isinstance(exp_cfg.job_trace_config, TraceGeneratorConfig):
        trace_gen.generate_trace(exp_cfg)
    else:
        raise ValueError(f"Unknown trace_config type: {type(exp_cfg.job_trace_config)}")
    dep = exp_cfg.model_deployment_config
    gpus_per_node = dep.num_gpus_per_node
    for model_cfg in dep.model_configs:
        tp_size = model_cfg.tensor_parallel_size
        replicas_per_node = gpus_per_node // tp_size
        total_replicas = replicas_per_node * exp_cfg.model_deployment_config.num_nodes
    config_path = exp_cfg.job_replay_client_config.config_path
    exp_cfg.save_yaml(config_path)
    pbs_output_dir = os.path.dirname(exp_cfg.pbs_stdout_dir)
    env_exports = "export AURORA_NULL_COMPUTE=1" if null_compute else ""
    pbs_content = pbs_template_content \
        .replace("{{JOB_NAME}}", exp_cfg.pbs_job_name) \
        .replace("{{NUM_NODES}}", str(exp_cfg.model_deployment_config.num_nodes)) \
        .replace("{{WALLTIME}}", exp_cfg.pbs_walltime) \
        .replace("{{QUEUE}}", exp_cfg.pbs_queue_name) \
        .replace("{{PBS_OUT_DIR}}", pbs_output_dir) \
        .replace("{{RUN_DIR}}", os.path.abspath(exp_cfg.pbs_working_dir)) \
        .replace("{{CONFIG_FILE}}", os.path.abspath(config_path)) \
        .replace("{{RUN_EXP_SCRIPT}}", run_exp_script) \
        .replace("{{BACKEND}}", backend) \
        .replace("{{NO_WARMUP}}", "--no-warmup" if exp_cfg.job_replay_client_config.no_warmup else "") \
        .replace("{{NUM_RUNS}}", str(exp_cfg.job_replay_client_config.num_runs)) \
        .replace("{{ENV_EXPORTS}}", env_exports)
    pbs_path = os.path.join(exp_cfg.pbs_working_dir, "job.pbs")
    with open(pbs_path, 'w') as f:
        f.write(pbs_content)
    submit_cmd = f"qsub {os.path.abspath(pbs_path)}"
    return (idx, submit_cmd)

def load_file(path):
    with open(path, 'r') as f:
        return f.read()

def save_yaml(data, path):
    with open(path, 'w') as f:
        yaml.dump(data, f, default_flow_style=False)

def setup_weak_scaling(args, experiments, backend="ray", null_compute=False):
    """
    Setup weak scaling experiments for the specified backend.
    
    Args:
        args: Command line arguments
        experiments: List of ExpConfig objects
        backend: Either "ray" or "mpi"
        null_compute: If True, export AURORA_NULL_COMPUTE=1 inside each job.pbs
    """
    # Prepare Submission List
    submit_cmds = []
    
    # Ensure run_exp.sh path
    run_exp_script = os.path.join(SCRIPT_DIR, "templates", "run_exp.sh")
    
    if not os.path.exists(args.pbs_template):
        print(f"!!! ERROR: PBS template not found: {args.pbs_template}")
        return

    pbs_template_content = load_file(args.pbs_template)
    workers = getattr(args, 'workers', 8)

    print(f"Generating {len(experiments)} weak scaling experiments for backend: {backend} (workers={workers})")

    work_items = [
        (idx, exp_cfg, pbs_template_content, run_exp_script, backend, null_compute)
        for idx, exp_cfg in enumerate(experiments)
    ]
    submit_cmds = [None] * len(experiments)
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_setup_one_weak_scaling_experiment, item): item[0] for item in work_items}
        for future in as_completed(futures):
            try:
                idx, submit_cmd = future.result()
                submit_cmds[idx] = submit_cmd
            except Exception as e:
                print(f"!!! ERROR: Experiment generation failed: {e}")
                raise
    submit_cmds = [c for c in submit_cmds if c is not None]

    if submit_cmds:
        first_dir = experiments[0].pbs_working_dir
        # Parent of "1_nodes" etc.
        parent_dir = os.path.abspath(os.path.dirname(first_dir))
        submit_src = args.submit_script
        submit_script_path = os.path.join(parent_dir, "submit_all.py")

        if not os.path.exists(submit_src):
            print(f"!!! ERROR: Submit script not found: {submit_src}")
            return

        shutil.copy(submit_src, submit_script_path)
        st = os.stat(submit_script_path)
        os.chmod(submit_script_path, st.st_mode | stat.S_IEXEC)

        print(f"\nGeneration complete. Run:\n  {submit_script_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate weak scaling experiment configurations"
    )
    parser.add_argument(
        "--pbs-template",
        default=os.path.join(SCRIPT_DIR, "templates", "job.pbs.tmpl"),
    )
    parser.add_argument(
        "--submit-script",
        default=os.path.join(SCRIPT_DIR, "submit_all.py"),
        help="Submit script to copy (default: templates/submit_all.py)",
    )
    parser.add_argument(
        "--backend", 
        choices=VALID_BACKENDS,
        default="ray",
        help="Backend to use: 'ray' for orchestrator.py or 'mpi' for MPI API server (default: ray)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of parallel workers for experiment generation (default: 8)",
    )

    args = parser.parse_args()
    
    # Validate backend
    if args.backend not in VALID_BACKENDS:
        print(f"!!! ERROR: Invalid backend '{args.backend}'. Must be one of: {VALID_BACKENDS}")
        exit(1)
    
    print(f">>> Generating experiments for backend: {args.backend}")
    experiments = get_weak_scaling_configs_with_num_runs(backend=args.backend, num_runs=3)
    # experiments = get_weak_scaling_null_compute_configs_with_num_runs(backend=args.backend, num_runs=3)
    # experiments = get_weak_scaling_null_compute_tests_configs_with_num_runs(backend=args.backend, num_runs=3)
    # experiments = get_weak_scaling_tests_configs_with_num_runs(backend=args.backend, num_runs=3)
    
    # setup_weak_scaling(args, experiments, backend=args.backend, null_compute=True)
    setup_weak_scaling(args, experiments, backend=args.backend, null_compute=False)
