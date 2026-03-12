# AGENTS.md

## Purpose

This repository is developed and tested on ALCF Aurora HPC Cluster.

All experiment execution must follow the cluster workflow described below. Do not run long, resource-intensive, or GPU workloads directly on the login node. For this project, experiments are typically run in an **interactive job** so the agent can actively monitor logs, inspect failures, and iterate while the job is live.

---

## Operating Rules

### 1) Never run heavy workloads on the login node
Treat the login node as a place for:
- editing code
- inspecting files
- lightweight checks
- submitting or starting interactive jobs
- reviewing logs and results

Do **not** use the login node for:
- evaluations
- distributed jobs
- GPU execution
- long preprocessing pipelines
- any command likely to consume significant CPU, RAM, disk I/O, or GPU

If asked to run an experiment, assume cluster resources are required unless the command is clearly trivial.
(Generating experiment configs can be done at login nodes.)

### 2) Interactive allocation is the default
When experiments need to be run, first obtain an interactive PBS allocation using the project-approved method. Only after the interactive job starts should environment setup and experiment execution occur.

Use the following wrapper scripts located in `~/script/`:

| Command | What it does |
|---------|-------------|
| `srundbg` | 1 node, 1 hour, `debug` queue |
| `srundsc $N` | N nodes (default 8), 1 hour, `debug-scaling` queue |

Both use `qsub -I` (interactive), project `AuroraGPT`, filesystems `home:flare`.

Default behavior:
1. Start or confirm an interactive PBS job using the wrapper scripts above.
2. Wait until the job is active and a compute-node shell is available.
3. Set up the environment inside that interactive session.
4. Run the experiment there.
5. Monitor outputs, resource usage, and failures while the job is active.

Do not skip the allocation step.

### 3) Environment setup must happen inside the interactive job
After entering the allocated compute environment, source the appropriate environment script before running code:

| Command | When to use |
|---------|-------------|
| `source ~/script/env_aurora` | Ray-only work (loads `frameworks` module, sets proxies) |
| `source ~/script/env_litellm` | LiteLLM work (loads `frameworks` module, sets proxies, activates `~/agpt/venv/litellm` virtualenv) |

These scripts handle module loading, proxy configuration, and virtualenv activation. Always source the correct one for the task at hand.

After sourcing, confirm:
- the correct virtual environment is active (if applicable)
- required executables are on `PATH`
- relevant runtime settings (CUDA visibility, etc.) are correct

Do not assume the environment from a previous session is still valid.

### 4) Verify before launching
Before starting a run, check:
- current working directory is correct
- required files and configs exist
- output/log directories exist or can be created safely
- the selected environment is the intended one
- required executables are on `PATH`
- any necessary datasets, checkpoints, or credentials are available

If something is missing, stop and report the specific issue.

### 5) Prefer active monitoring
Because interactive jobs are used specifically for live supervision, monitor the run after launch:
- watch stdout/stderr
- inspect generated logs
- check for early crashes, hangs, OOMs, missing files, or environment issues
- report problems promptly and suggest the smallest safe next step
- do not wait for a program to finish after seeing error logs, program can hang and error without exiting.

Do not start a long experiment and immediately abandon observation unless explicitly told to do so.

### 6) Multi-node experiments and SSH
When running multi-node experiments (e.g., `bench_internode.sh`):
- The list of allocated nodes is in `$PBS_NODEFILE` (one hostname per line).
- The agent may SSH from the head compute node to other allocated nodes to launch processes (e.g., stub servers).
- Always verify `$PBS_NODEFILE` exists and contains the expected number of nodes before launching multi-node workloads.
- Do not SSH to nodes that are not in the current allocation.

### 7) Walltime awareness
Interactive PBS jobs have a fixed walltime (typically 1 hour with the default wrapper scripts). Be aware of remaining walltime:
- Before starting a long experiment, estimate whether it will complete within the remaining walltime.
- If walltime is running low, warn the user and suggest saving state or requesting a new allocation.
- If a job is killed due to walltime expiration, report it clearly — do not treat it as an unknown crash.
- Check remaining walltime with `qstat -f $PBS_JOBID | grep Walltime` if uncertain.

### 8) Be conservative with destructive actions
Do not delete checkpoints, logs, outputs, caches, or generated data unless explicitly asked.

Do not overwrite prior experiment outputs unless the command or workflow clearly intends to do so.

When possible, use unique output directories or timestamped run directories.

---

## Required Workflow for Any Experiment Run

When the user asks to "run", "launch", "train", "evaluate", or otherwise execute nontrivial experiment code, interpret that request using this workflow:

1. Confirm whether an interactive PBS allocation is already active.
   Treat the current shell as a valid interactive PBS compute-node session only if all of the following are true:
   - `$PBS_JOBID` is set
   - `$PBS_NODEFILE` exists and is readable
   - `hostname` matches one of the hosts listed in `$PBS_NODEFILE`
   - the shell is already attached to the compute-node session entered via `qsub -I`
2. If not active, start one using `srundbg` (1 node) or `srundsc $N` (N nodes).
3. Once inside the interactive job, source the correct environment script (`source ~/script/env_aurora` or `source ~/script/env_litellm`).
4. Perform lightweight preflight checks.
5. Run the requested command.
6. Monitor the run and summarize status, failures, and next actions.

If the user gives a command that would bypass the interactive allocation step, do **not** execute it directly on the login node. Instead, adapt it to the required workflow.

---

## Session Detection and Assumptions

Before running any nontrivial experiment command, determine whether execution is happening:
- on the login node
- inside an active interactive PBS allocation
- in the correct project directory
- with the correct environment active

For session validation, do not rely on a single signal such as hostname alone or `$PBS_JOBID` alone. Require all of the following:
- `$PBS_JOBID` is set
- `$PBS_NODEFILE` exists and is readable
- `hostname` appears in `$PBS_NODEFILE`
- the shell is the active compute-node shell entered from `qsub -I`

If any of these are uncertain, do not guess. Check first.

Do not assume that because a prior prompt mentioned allocation or setup, the current shell is still in the same valid state.

---

## Approved Execution Pattern

Use this mental model for all experiment work:

- **login node**: prepare, inspect, allocate
- **interactive PBS job**: set up environment, execute, monitor, debug

Any deviation from this should be treated as exceptional and called out explicitly.

---

## What To Do When the User Says "Run This"

Unless the user explicitly says otherwise, interpret "run this" as:

1. use an interactive PBS allocation if not already inside one (`srundbg` for 1 node, `srundsc $N` for N nodes)
2. initialize the project runtime environment (`source ~/script/env_aurora` for Ray, `source ~/script/env_litellm` for LiteLLM)
3. run the experiment on the allocated compute node
4. monitor the results for failures or suspicious behavior
5. report concise status updates

Do not interpret "run this" as permission to execute directly on the login node.

Tiny local checks that are clearly non-experimental, such as reading files, generating configs, linting a small file, or running a very small smoke test, may still be done on the login node if they are lightweight and do not violate the login-node resource rules above.

---

## Failure Handling

If a run fails, do the following:
1. capture the exact failing command
2. capture the relevant error output
3. identify the most likely cause
4. propose the smallest safe fix
5. retry only if the retry is low-risk and justified

Common categories to check:
- missing allocation / wrong node
- missing modules or inactive environment
- path issues
- missing input files
- permissions problems
- out-of-memory or resource mismatch
- CUDA/device visibility problems
- incompatible package versions
- bad config values

Do not repeatedly retry the same failing command without a clear change.

---

## Resource Awareness

Be mindful of cluster etiquette and resource usage:
- request only the resources needed
- avoid wasteful reruns
- do not spawn duplicate jobs accidentally
- confirm device and resource assumptions before large runs
- terminate clearly broken processes when safe and appropriate

If resource requirements are unclear, prefer asking for clarification or using the smallest reasonable test first.

---

## Logging and Reporting

When running experiments, report:
- whether an interactive PBS allocation was active or newly started
- whether environment setup was completed
- the exact command being run
- where logs and outputs are going
- whether the run appears healthy in its early stage
- any errors, anomalies, or recommended next steps

Be concise but precise.

---

## Safe Defaults

Unless project documentation says otherwise, default to:
- interactive PBS allocation before execution
- environment setup in each fresh interactive session
- lightweight validation before expensive runs
- active monitoring after launch
- non-destructive behavior toward outputs and checkpoints

---

## Prohibited Behavior

Do not:
- run heavy jobs on the login node
- skip interactive allocation for experiment execution
- skip environment setup in a fresh session
- assume prior shell state is still valid
- overwrite outputs carelessly
- ignore early signs of failure
- hide errors or silently continue after a suspicious failure

---

## Repository-Specific Overrides

This project uses helper scripts in `~/script/`:

| Script | Purpose |
|--------|---------|
| `~/script/srundbg` | Interactive 1-node debug allocation |
| `~/script/srundsc $N` | Interactive N-node debug-scaling allocation |
| `~/script/env_aurora` | Environment setup for Ray-only work |
| `~/script/env_litellm` | Environment setup for LiteLLM work |

Additionally, batch job infrastructure exists under `eval/`:
- `eval/templates/job.pbs.tmpl` — PBS job template
- `eval/submit_all.py` — batch job submission orchestrator

When repository-specific instructions conflict with generic behavior, prefer the repository-specific instructions, while still preserving the core rule: **do not run experiments directly on the login node**.

---

## Decision Rule

If there is any doubt whether a command needs cluster resources, assume it does.

If there is any doubt whether the current session is already a valid interactive PBS job, verify before running.

If there is any doubt whether the environment has been prepared in the current session, prepare it again or explicitly check it.

This project values caution, reproducibility, and live monitoring over speed.
