# AGENTS.md

## Purpose

This repository is developed and tested on ALCF Aurora HPC Cluster.

For the production-hardening migration, this file is authoritative for safety,
permissions, allocation/session handling, and where commands may run. The sole
architecture and implementation specification is
`doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md`; audit, feasibility, Known Issues,
and TODO documents are evidence/context and cannot override that plan.

All experiment execution must follow the cluster workflow described below. Do not run long, resource-intensive, MPI, or GPU workloads directly on the login node. For this project, experiments run in a monitored **compute session**, preferably a `subjob` lease from the user's keepalive allocation, so the agent can inspect failures and iterate while the session is live.

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

For brief login-node Python or Go checks, first run `module load frameworks && module load go`; system Python is too old for this repository.

### 2) A leased compute session is the default
When experiments need to run, first reuse or obtain a project-approved compute session. Only after the session is validated should environment setup and execution occur.

Preferred order:

1. Reuse a valid existing compute session.
2. Use `subjob N` (or `subjob N -- <command>`) to lease exactly the needed nodes from the user's long-lived keepalive allocation. The agent does not start or manage keepalive.
3. If no keepalive/debug source is available to `subjob`, use `~/script/srundbg` for one node or `~/script/srundsc N` for an interactive PBS fallback. Do not improvise scheduler parameters.

`subjob` preserves the real PBS/PALS environment, replaces `$PBS_NODEFILE` with the leased subset, marks the session with `$AURORA_SUBJOB=1`, and releases the lease on exit/TTL. Do not bypass it with a plain SSH shell, which loses required PBS/PALS state.

Do not skip the compute-session step.

### 3) Environment setup must happen inside the compute session
After entering the allocated compute environment, source the appropriate environment script before running code:

| Command | When to use |
|---------|-------------|
| `source ~/script/env_aurora` | Ray-only work (loads `frameworks` module, sets proxies) |
| `source ~/script/env_litellm` | LiteLLM work (loads `frameworks` module, sets proxies, activates `~/agpt/venv/litellm` virtualenv) |

These scripts handle module loading, proxy configuration, and virtualenv activation. Always source the correct one for the task at hand.

After sourcing, confirm:
- the correct virtual environment is active (if applicable)
- required executables are on `PATH`
- relevant runtime settings are correct; on Aurora use `ZE_AFFINITY_MASK` and never introduce `ONEAPI_DEVICE_SELECTOR`

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
Because compute sessions are used specifically for live supervision, monitor the run after launch:
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
Compute sessions have a fixed lease TTL or PBS walltime. Be aware of both the lease and underlying job deadline:
- Before starting a long experiment, estimate whether it will complete within the remaining walltime.
- If walltime is running low, warn the user and suggest saving state or requesting a new allocation.
- If a job is killed due to walltime expiration, report it clearly — do not treat it as an unknown crash.
- For a lease, inspect `subjob status`; inspect the underlying PBS walltime with `qstat -f $PBS_JOBID | grep Walltime` if uncertain.

### 8) Be conservative with destructive actions
Do not delete checkpoints, logs, outputs, caches, or generated data unless explicitly asked.

Do not overwrite prior experiment outputs unless the command or workflow clearly intends to do so.

When possible, use unique output directories or timestamped run directories.

---

## Required Workflow for Any Experiment Run

When the user asks to "run", "launch", "train", "evaluate", or otherwise execute nontrivial experiment code, interpret that request using this workflow:

1. Confirm whether a valid compute session is already active.
   Treat the current shell as valid only if all of the following are true:
   - `$PBS_JOBID` is set
   - `$PBS_NODEFILE` exists and is readable
   - `hostname` matches one of the hosts listed in `$PBS_NODEFILE`
   - the shell is either marked `$AURORA_SUBJOB=1` or verified as the active compute-node shell entered from interactive PBS
2. If not active, prefer `subjob N`; use `srundbg` or `srundsc N` only when no lease source is available.
3. Once inside the compute session, source the correct environment script (`source ~/script/env_aurora` or `source ~/script/env_litellm`).
4. Perform lightweight preflight checks.
5. Run the requested command.
6. Monitor the run and summarize status, failures, and next actions.

If the user gives a command that would bypass the compute-session step, do **not** execute it directly on the login node. Instead, adapt it to the required workflow.

---

## Session Detection and Assumptions

Before running any nontrivial experiment command, determine whether execution is happening:
- on the login node
- inside an active `subjob` lease or interactive PBS allocation
- in the correct project directory
- with the correct environment active

For session validation, do not rely on a single signal such as hostname alone or `$PBS_JOBID` alone. Require all of the following:
- `$PBS_JOBID` is set
- `$PBS_NODEFILE` exists and is readable
- `hostname` appears in `$PBS_NODEFILE`
- the shell is marked `$AURORA_SUBJOB=1` or is the verified interactive PBS compute shell

If any of these are uncertain, do not guess. Check first.

Do not assume that because a prior prompt mentioned allocation or setup, the current shell is still in the same valid state.

---

## Approved Execution Pattern

Use this mental model for all experiment work:

- **login node**: prepare, inspect, allocate
- **leased/interactive compute session**: set up environment, execute, monitor, debug

Any deviation from this should be treated as exceptional and called out explicitly.

---

## What To Do When the User Says "Run This"

Unless the user explicitly says otherwise, interpret "run this" as:

1. use a validated compute session if not already inside one (`subjob N` preferred; `srundbg`/`srundsc N` fallback)
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
- whether a validated compute session was reused or newly obtained, and whether it is a `subjob` lease or interactive PBS fallback
- whether environment setup was completed
- the exact command being run
- where logs and outputs are going
- whether the run appears healthy in its early stage
- any errors, anomalies, or recommended next steps

Be concise but precise.

---

## Safe Defaults

Unless project documentation says otherwise, default to:
- a validated compute session before execution, preferring `subjob N`
- environment setup in each fresh compute session
- lightweight validation before expensive runs
- active monitoring after launch
- non-destructive behavior toward outputs and checkpoints

---

## Prohibited Behavior

Do not:
- run heavy jobs on the login node
- skip the validated compute-session step for experiment execution
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
| `~/script/subjob N` | Preferred N-node lease from the user-managed keepalive/debug allocation |
| `~/script/srundbg` | Interactive 1-node debug allocation |
| `~/script/srundsc $N` | Interactive N-node debug-scaling allocation |
| `~/script/env_aurora` | Environment setup for Ray-only work |
| `~/script/env_litellm` | Environment setup for LiteLLM work |

Additionally, batch jobs are materialized and submitted through the shared
Python control plane:
- `python3 -m eval.cli run materialize <spec>` — compile immutable run bundles
- `python3 -m eval.cli run submit <run.yaml>` — submit one exact run identity
- `python3 -m eval.cli run submit-all <spec>` — bounded, idempotent batch submission

Job bodies are rendered by `src/exaserve/schedulers/`; no shell template owns
deployment lifecycle, readiness, or cleanup.

When repository-specific instructions conflict with generic behavior, prefer the repository-specific instructions, while still preserving the core rule: **do not run experiments directly on the login node**.

---

## Decision Rule

If there is any doubt whether a command needs cluster resources, assume it does.

If there is any doubt whether the current session is a valid `subjob` lease or interactive PBS compute shell, verify before running.

If there is any doubt whether the environment has been prepared in the current session, prepare it again or explicitly check it.

This project values caution, reproducibility, and live monitoring over speed.
