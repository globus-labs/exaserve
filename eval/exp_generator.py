import argparse
import yaml
import os
import sys
import shutil
import stat
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed

# Ensure the script directory is in the Python path for reliable imports
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from exp_configs import (
    EXPERIMENT_REGISTRY,
    WeakScalingConfig,
    TraceGeneratorConfig,
    build_weak_scaling_configs,
)
from trace_generator import TraceGenerator
VALID_BACKENDS = ["ray", "mpi"]


def create_snapshot(repo_root, snapshot_base_dir):
    """
    Create a frozen copy of the current git commit via `git archive`.
    Reuses an existing snapshot if the commit hash matches.
    Returns (snapshot_dir, commit_hash).
    """
    result = subprocess.run(
        ["git", "rev-parse", "--short=8", "HEAD"],
        cwd=repo_root, capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"!!! ERROR: Failed to get git commit hash in {repo_root}")
        print(f"    {result.stderr.strip()}")
        sys.exit(1)
    commit_hash = result.stdout.strip()

    status = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=repo_root, capture_output=True, text=True,
    )
    if status.stdout.strip():
        print(f"!!! WARNING: Uncommitted changes detected in {repo_root}")
        print(f"    Only committed changes (at {commit_hash}) will be in the snapshot.")
        print(f"    Commit your changes for a complete snapshot.")

    snapshot_dir = os.path.join(snapshot_base_dir, commit_hash)

    if os.path.isdir(snapshot_dir):
        print(f">>> [SNAPSHOT] Reusing existing snapshot: {snapshot_dir} ({commit_hash})")
        return snapshot_dir, commit_hash

    os.makedirs(snapshot_dir, exist_ok=True)
    result = subprocess.run(
        f"git archive HEAD | tar -x -C '{snapshot_dir}'",
        cwd=repo_root, shell=True, capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"!!! ERROR: Failed to create snapshot: {result.stderr.strip()}")
        shutil.rmtree(snapshot_dir, ignore_errors=True)
        sys.exit(1)

    print(f">>> [SNAPSHOT] Created: {snapshot_dir} (commit: {commit_hash})")
    return snapshot_dir, commit_hash


def _setup_one_weak_scaling_experiment(item):
    """
    Worker: generate one experiment (trace, config, job.pbs).
    item: (index, exp_cfg, pbs_template_content, run_exp_script, backend, null_compute, setup_only, clear_results)
    Returns: (index, submit_cmd, missing_trace_path_or_None)
    """
    idx, exp_cfg, pbs_template_content, run_exp_script, backend, null_compute, setup_only, clear_results = item
    os.makedirs(exp_cfg.pbs_working_dir, exist_ok=True)
    os.makedirs(exp_cfg.pbs_stdout_dir, exist_ok=True)
    os.makedirs(exp_cfg.pbs_stderr_dir, exist_ok=True)
    if clear_results and os.path.isdir(exp_cfg.pbs_result_dir):
        shutil.rmtree(exp_cfg.pbs_result_dir)
    os.makedirs(exp_cfg.pbs_result_dir, exist_ok=True)
    assert exp_cfg.pbs_num_nodes == exp_cfg.model_deployment_config.num_nodes, "PBS nodes and deployment nodes must match"
    missing_trace = None
    if setup_only:
        trace_path = exp_cfg.job_trace_config.output_trace_path
        if not os.path.exists(trace_path):
            missing_trace = trace_path
    else:
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
    return (idx, submit_cmd, missing_trace)

def load_file(path):
    with open(path, 'r') as f:
        return f.read()

def save_yaml(data, path):
    with open(path, 'w') as f:
        yaml.dump(data, f, default_flow_style=False)

def setup_weak_scaling(args, experiments, backend="ray", null_compute=False,
                       setup_only=False, snapshot_dir=None, clear_results=False):
    """
    Setup weak scaling experiments for the specified backend.
    
    Args:
        args: Command line arguments
        experiments: List of ExpConfig objects
        backend: Either "ray" or "mpi"
        null_compute: If True, export AURORA_NULL_COMPUTE=1 inside each job.pbs
        setup_only: If True, skip trace generation (configs + PBS scripts only)
        snapshot_dir: Path to the code snapshot; PBS jobs will reference this instead of the dev repo
        clear_results: If True, delete and recreate each experiment's results directory
    """
    submit_cmds = []

    if snapshot_dir:
        run_exp_script = os.path.join(snapshot_dir, "eval", "templates", "run_exp.sh")
    else:
        run_exp_script = os.path.join(SCRIPT_DIR, "templates", "run_exp.sh")
    
    if not os.path.exists(args.pbs_template):
        print(f"!!! ERROR: PBS template not found: {args.pbs_template}")
        return

    pbs_template_content = load_file(args.pbs_template)
    workers = getattr(args, 'workers', 8)

    mode = "setup-only (no trace generation)" if setup_only else "full"
    print(f"Generating {len(experiments)} weak scaling experiments for backend: {backend} (mode={mode}, workers={workers})")
    if snapshot_dir:
        print(f"  Snapshot: {snapshot_dir}")

    work_items = [
        (idx, exp_cfg, pbs_template_content, run_exp_script, backend, null_compute, setup_only, clear_results)
        for idx, exp_cfg in enumerate(experiments)
    ]
    submit_cmds = [None] * len(experiments)
    missing_traces = []
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_setup_one_weak_scaling_experiment, item): item[0] for item in work_items}
        for future in as_completed(futures):
            try:
                idx, submit_cmd, missing_trace = future.result()
                submit_cmds[idx] = submit_cmd
                if missing_trace:
                    missing_traces.append(missing_trace)
            except Exception as e:
                print(f"!!! ERROR: Experiment generation failed: {e}")
                raise
    submit_cmds = [c for c in submit_cmds if c is not None]

    if missing_traces:
        print(f"\n!!! WARNING: {len(missing_traces)} trace file(s) not found:")
        for t in missing_traces:
            print(f"    - {t}")
        print("    Jobs using missing traces will FAIL. Run without --setup-only to generate them.")

    if submit_cmds:
        first_dir = experiments[0].pbs_working_dir
        # Parent of "<N_nodes>/config" — go up two levels to reach the experiment dir
        parent_dir = os.path.abspath(os.path.dirname(os.path.dirname(first_dir)))
        if snapshot_dir:
            submit_src = os.path.join(snapshot_dir, "eval", "submit_all.py")
            if not os.path.exists(submit_src):
                submit_src = args.submit_script
        else:
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
        help="Backend to use: 'ray' for Ray orchestrator or 'mpi' for MPI API server (default: ray)",
    )
    parser.add_argument(
        "-e", "--experiment",
        default=None,
        help=(
            "Experiment name from the registry to generate. "
            f"Known names: {', '.join(EXPERIMENT_REGISTRY)}. "
            "Pass 'list' to print all available names and exit."
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of parallel workers for experiment generation (default: 8)",
    )
    parser.add_argument(
        "--setup-only",
        action="store_true",
        help="Generate configs and PBS scripts without trace generation. "
             "Warns if expected trace files are missing.",
    )
    parser.add_argument(
        "--snapshot-dir",
        default="/home/wenyiw/agpt/data/snapshots",
        help="Base directory for code snapshots (default: /home/wenyiw/agpt/data/snapshots)",
    )
    parser.add_argument(
        "--no-snapshot",
        action="store_true",
        help="Skip git snapshot; PBS jobs will reference the development repo directly",
    )
    parser.add_argument(
        "--clear-results",
        action="store_true",
        help="Delete and recreate each experiment's results directory before generation",
    )

    args = parser.parse_args()

    if args.experiment == "list" or args.experiment is None:
        print("Available experiments:")
        for name, params in EXPERIMENT_REGISTRY.items():
            print(f"  {name:40s}  nodes={params.num_nodes_list}")
        exit(0 if args.experiment == "list" else 1)

    if args.experiment not in EXPERIMENT_REGISTRY:
        print(f"!!! ERROR: Unknown experiment '{args.experiment}'. Known: {list(EXPERIMENT_REGISTRY)}")
        exit(1)

    if args.backend not in VALID_BACKENDS:
        print(f"!!! ERROR: Invalid backend '{args.backend}'. Must be one of: {VALID_BACKENDS}")
        exit(1)

    params = EXPERIMENT_REGISTRY[args.experiment]
    print(f">>> Generating experiment '{args.experiment}' for backend: {args.backend}")

    snapshot_dir = None
    if not args.no_snapshot:
        snapshot_dir, commit_hash = create_snapshot(REPO_ROOT, args.snapshot_dir)

    experiments = build_weak_scaling_configs(backend=args.backend, params=params)
    setup_weak_scaling(
        args, experiments,
        backend=args.backend,
        null_compute=params.null_compute,
        setup_only=args.setup_only,
        snapshot_dir=snapshot_dir,
        clear_results=args.clear_results,
    )
