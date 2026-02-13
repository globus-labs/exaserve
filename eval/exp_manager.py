import argparse
import yaml
import os
import shutil
import stat
from copy import deepcopy
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
    # Load template
    print(f"Loading config template from {args.config_template}...")
    with open(args.config_template, 'r') as f:
        template_config = yaml.safe_load(f)
    
    # Init Generator
    print("Initializing TraceGenerator...")
    trace_gen = TraceGenerator(args.config_template)
    
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
        # Since get_weak_scaling_configs returns ExpConfigs with WeakScalingConfig in trace_config
        trace_gen.generate_weak_scaling(exp_cfg)
        
        # 2. Generate Experiment Config (config.yaml)
        # Deepcopy template
        conf = deepcopy(template_config)
        
        # Update Topology
        if "gpu_topology" not in conf: conf["gpu_topology"] = {}
        conf["gpu_topology"]["num_nodes"] = exp_cfg.num_nodes
        
        # [NEW] Scale num_replicas for models
        # For weak scaling, we assume the provided template has a single model entry
        # and we scale its replica count linearly with nodes.
        # Assuming 1 node = 8 GPUs (or whatever topology says), and standard TP=1
        
        # We assume the user provides a template with settings for 1 node (or 2 nodes),
        # but for weak scaling, we want to fully utilize the requested nodes.
        # If the template says "num_replicas: 8" for 1 node, then for 2 nodes it should be 16.
        # Logic: 
        #   target_replicas = num_nodes * (gpus_per_node / tp_size)
        # OR simpler:
        #   target_replicas = base_replicas_per_node * num_nodes
        
        # Let's inspect the models section
        if "models" in conf and conf["models"]:
            gpus_per_node = conf.get("gpu_topology", {}).get("num_gpus_per_node", 4)
            for model_entry in conf["models"]:
                model_name = list(model_entry.keys())[0]
                model_spec = model_entry[model_name]
                
                tp_size = model_spec.get("tensor_parallel_size", 1)
                
                # Calculate max possible replicas per node
                # e.g. 4 GPUs / TP 1 = 4 replicas per node
                # e.g. 4 GPUs / TP 2 = 2 replicas per node
                replicas_per_node = gpus_per_node // tp_size
                
                # Scale total replicas
                total_replicas = replicas_per_node * exp_cfg.num_nodes
                
                model_spec["num_replicas"] = total_replicas
                print(f"      -> Scaling {model_name}: {total_replicas} replicas (Nodes={exp_cfg.num_nodes}, TP={tp_size})")

        # Update Benchmark
        if "benchmark" not in conf: conf["benchmark"] = {}
        
        # Extract trace path from the config object
        ws_cfg = exp_cfg.trace_config
        if isinstance(ws_cfg, WeakScalingConfig):
             conf["benchmark"]["output_trace_path"] = ws_cfg.output_path
             
             # Also persist other weak scaling params for reference
             conf["weak_scaling"] = {
                "requests_per_node_per_sec": ws_cfg.rpn,
                "duration": ws_cfg.duration,
                "input_len": ws_cfg.input_len,
                "output_len": ws_cfg.output_len
             }
        
        # Result directory (replay_client writes result0.json, result1.json, ...)
        conf["benchmark"]["output_result_dir"] = exp_cfg.result_dir

        # [NEW] Ensure trace path is NOT in the experiment directory but in the centralized location
        # This was already handled by get_weak_scaling_configs setting output_path to DEFAULT_OUTPUT_TRACE_DIR/...
        # But we ensure the config.yaml reflects it (which it does via 'output_trace_path' below).
        # We assume exp_cfg.trace_config.output_path is already pointing to the shared location.
        
        config_path = os.path.join(exp_cfg.working_dir, "config.yaml")
        save_yaml(conf, config_path)
        print(f"      -> Config: {config_path}")
        
        # 3. Generate PBS
        pbs_content = pbs_template_content \
            .replace("{{JOB_NAME}}", exp_cfg.job_name) \
            .replace("{{NUM_NODES}}", str(exp_cfg.num_nodes)) \
            .replace("{{WALLTIME}}", exp_cfg.walltime) \
            .replace("{{QUEUE}}", exp_cfg.queue_name) \
            .replace("{{PBS_OUT_DIR}}", exp_cfg.pbs_output_dir) \
            .replace("{{RUN_DIR}}", os.path.abspath(exp_cfg.working_dir)) \
            .replace("{{CONFIG_FILE}}", os.path.abspath(config_path)) \
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
        "--config-template",
        default=os.path.join(SCRIPT_DIR, "config-nodes.yaml"),
        help="Base config template (default: config-nodes.yaml)",
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
