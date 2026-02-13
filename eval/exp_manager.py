import argparse
import yaml
import os
import shutil
import stat
from aurora_rayserver.eval.exp_configs import *
from aurora_rayserver.eval.trace_generator import TraceGenerator

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
VALID_BACKENDS = ["ray", "mpi"]

def load_file(path):
    with open(path, 'r') as f:
        return f.read()

def save_yaml(data, path):
    with open(path, 'w') as f:
        yaml.dump(data, f, default_flow_style=False)

def setup_weak_scaling(args, experiments, backend="ray"):
    """
    Setup weak scaling experiments for the specified backend.
    
    Args:
        args: Command line arguments
        experiments: List of ExpConfig objects
        backend: Either "ray" or "mpi"
    """
    # Prepare Submission List
    submit_cmds = []
    
    # Ensure run_exp.sh path
    run_exp_script = os.path.join(SCRIPT_DIR, "run_exp.sh")
    
    if not os.path.exists(args.pbs_template):
        print(f"!!! ERROR: PBS template not found: {args.pbs_template}")
        return

    pbs_template_content = load_file(args.pbs_template)

    print(f"Generating {len(experiments)} weak scaling experiments for backend: {backend}")

    for exp_cfg in experiments:
        # Create Dirs
        os.makedirs(exp_cfg.working_dir, exist_ok=True)
        # stdout/stderr dir
        os.makedirs(os.path.join(exp_cfg.pbs_output_dir, "stdout"), exist_ok=True)
        os.makedirs(os.path.join(exp_cfg.pbs_output_dir, "stderr"), exist_ok=True)
        # Result dir: replay_client writes result0.json, result1.json, ... here
        os.makedirs(exp_cfg.result_dir, exist_ok=True)
        
        # 1. Generate Trace
        print(f"  [{exp_cfg.num_nodes} Nodes] Generating trace...")
        
        # Initialize TraceGenerator with ExpConfig
        trace_gen = TraceGenerator(exp_cfg)
        
        # Generate trace based on config type
        if isinstance(exp_cfg.trace_config, WeakScalingConfig):
            trace_gen.generate_weak_scaling(exp_cfg)
        elif isinstance(exp_cfg.trace_config, TraceGeneratorConfig):
            trace_gen.generate_trace(exp_cfg)
        else:
            print(f"!!! ERROR: Unknown trace_config type: {type(exp_cfg.trace_config)}")
            continue
        
        # 2. Scale num_replicas for models based on num_nodes
        # For weak scaling, we scale replica count linearly with nodes
        gpus_per_node = exp_cfg.gpu_topology.num_gpus_per_node
        
        # Update model configs with proper replica counts
        for model_cfg in exp_cfg.model_configs:
            tp_size = model_cfg.tensor_parallel_size
            replicas_per_node = gpus_per_node // tp_size
            total_replicas = replicas_per_node * exp_cfg.num_nodes
            model_cfg.num_replicas = total_replicas
            print(f"      -> Scaling {model_cfg.model_id}: {total_replicas} replicas (Nodes={exp_cfg.num_nodes}, TP={tp_size})")
        
        # 3. Generate Experiment Config (config.yaml) from ExpConfig
        # Use the config_path specified in ExpConfig (already set to working_dir/config.yaml)
        exp_cfg.save_yaml(exp_cfg.config_path)
        print(f"      -> Config: {exp_cfg.config_path}")
        
        # 4. Generate PBS
        pbs_content = pbs_template_content \
            .replace("{{JOB_NAME}}", exp_cfg.job_name) \
            .replace("{{NUM_NODES}}", str(exp_cfg.num_nodes)) \
            .replace("{{WALLTIME}}", exp_cfg.walltime) \
            .replace("{{QUEUE}}", exp_cfg.queue_name) \
            .replace("{{PBS_OUT_DIR}}", exp_cfg.pbs_output_dir) \
            .replace("{{RUN_DIR}}", os.path.abspath(exp_cfg.working_dir)) \
            .replace("{{CONFIG_FILE}}", os.path.abspath(exp_cfg.config_path)) \
            .replace("{{RUN_EXP_SCRIPT}}", run_exp_script) \
            .replace("{{BACKEND}}", backend) \
            .replace("{{NO_WARMUP}}", "--no-warmup" if exp_cfg.no_warmup else "") \
            .replace("{{NUM_RUNS}}", str(exp_cfg.num_runs))
            
        pbs_path = os.path.join(exp_cfg.working_dir, "job.pbs")
        with open(pbs_path, 'w') as f:
            f.write(pbs_content)
        print(f"      -> PBS: {pbs_path}")
            
        submit_cmds.append(f"qsub {os.path.abspath(pbs_path)}")
        
    # Generate Submit Script (template-based)
    if submit_cmds:
        first_dir = experiments[0].working_dir
        # Parent of "1_nodes" etc.
        parent_dir = os.path.abspath(os.path.dirname(first_dir))
        submit_template_path = args.submit_template
        submit_script_path = os.path.join(parent_dir, "submit_all.py")

        if not os.path.exists(submit_template_path):
            print(f"!!! ERROR: Submit template not found: {submit_template_path}")
            return

        job_dirs = [os.path.basename(exp.working_dir) for exp in experiments]
        template_content = load_file(submit_template_path)
        template_content = template_content.replace("{{PARENT_DIR}}", parent_dir)
        template_content = template_content.replace(
            "DEFAULT_JOBS = []  # {{DEFAULT_JOBS}}",
            f"DEFAULT_JOBS = {repr(job_dirs)}",
        )

        with open(submit_script_path, "w") as f:
            f.write(template_content)

        st = os.stat(submit_script_path)
        os.chmod(submit_script_path, st.st_mode | stat.S_IEXEC)

        print(f"\nGeneration complete. Run:\n  {submit_script_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate weak scaling experiment configurations"
    )
    parser.add_argument(
        "--pbs-template",
        default=os.path.join(SCRIPT_DIR, "job.pbs.tmpl"),
    )
    parser.add_argument(
        "--submit-template",
        default=os.path.join(SCRIPT_DIR, "submit_all.py.tmpl"),
        help="Submit script template (default: submit_all.py.tmpl)",
    )
    parser.add_argument(
        "--backend", 
        choices=VALID_BACKENDS,
        default="ray",
        help="Backend to use: 'ray' for orchestrator.py or 'mpi' for MPI API server (default: ray)"
    )
    
    args = parser.parse_args()
    
    # Validate backend
    if args.backend not in VALID_BACKENDS:
        print(f"!!! ERROR: Invalid backend '{args.backend}'. Must be one of: {VALID_BACKENDS}")
        exit(1)
    
    print(f">>> Generating experiments for backend: {args.backend}")
    # experiments = get_weak_scaling_configs(backend=args.backend)
    experiments = get_weak_scaling_configs_with_num_runs(backend=args.backend, num_runs=5)
    setup_weak_scaling(args, experiments, backend=args.backend)
