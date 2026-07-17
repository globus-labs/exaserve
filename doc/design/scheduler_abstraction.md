# Design Sketch: Scheduler Abstraction (PBS → pluggable, Slurm-ready)

Status: **IMPLEMENTED (PBS validated, Slurm untested)** · Step (3) of the
generic-HPC refactor. Sibling of `doc/design/pluggable_interfaces.md`.

> **Implementation status:** built on branch `feature/slurm-amd-support`.
> - **Package submission**: `src/exaserve/schedulers/` — `SchedulerBackend` ABC +
>   `get_scheduler()` registry, `PBSScheduler` (byte-identical to the old
>   `submit.py`) + `SlurmScheduler` (sbatch/squeue). `submit.py` uses it.
> - **Runtime seam**: `launch_cluster.sh` / `distribute_to_nodes.sh` /
>   `model_bcast.py` detect the scheduler and use `EXASERVE_NODEFILE` +
>   `EXASERVE_MPILAUNCH` (mpiexec on PBS, **srun on Slurm**); `driver.py`
>   `get_rank()` reads `SLURM_PROCID`. Selected by `EXASERVE_SCHEDULER` (default
>   `pbs`). See `doc/deploy_slurm_amd.md`.
> - **Not done**: the eval/benchmark harness (`eval/lib/schedulers/`) is still
>   PBS-only. Slurm end-to-end is unproven (no Slurm system on Aurora).

## Goal

Make the batch scheduler pluggable so ExaServe runs on a **Slurm** site (and
later others) without editing the serving core. Today everything assumes **PBS
Pro + PALS** (Aurora/ALCF). The `scheduler.type` field already exists in specs
(`SchedulerSpec.type = "pbs"`, `eval/lib/models.py:195`) but only PBS is wired.

## Where PBS is coupled today (three layers)

### Layer A — eval control plane (submit + poll)
- `eval/lib/schedulers/pbs.py`
  - `render_pbs_job(...)` → emits the `#PBS` header + job body (`cd snapshot`,
    PYTHONPATH, `source env_script`, `python3 -m eval.cli run execute`).
  - `default_queue_and_walltime(num_nodes)` → PBS/Aurora queue policy.
- `eval/lib/run_executor.py`
  - `submit_run` / `_try_qsub` → `qsub <job.pbs>`.
  - `submit_all` → `qstat -u <user>` to count running+queued, throttled by
    `_QUEUE_SLOT_LIMITS = {debug:2, debug-scaling:2, prod:None}` (PBS queues),
    `_POLL_INTERVAL_S=300`.
- `SchedulerSpec` fields are PBS-shaped: `queue`, `filesystems`, `keep_output`,
  `mail_events` (PBS `-q/-l filesystems/-k/-m`).

### Layer B — package one-shot submit
- `src/exaserve/submit.py`: `_render_job_pbs()` + `submit_serve()` → `qsub` a
  job.pbs that calls `exaserve-launch-cluster`. Standalone of the eval plane.

### Layer C — runtime (inside the allocation)
- `src/exaserve/resources/launch_cluster.sh`
  - Hard-requires `$PBS_NODEFILE` (line 6-7), reads `$PBS_JOBID` (line 143).
  - Node-parallel work via **`mpiexec -n N -ppn 1 --cpu-bind none`** (PALS) for
    model bcast/gather + overlay distribution (lines 209, 322, 463).
- `src/exaserve/resources/distribute_to_nodes.sh`: same `mpiexec -ppn 1` pattern.

**PBS provides three things the code consumes:** (1) a job-submission CLI
(`qsub`/`qstat`/`qdel`), (2) a **nodefile** (`$PBS_NODEFILE`), (3) a **run-once-
per-node launcher** (`mpiexec -ppn 1` via PALS). Slurm provides all three
differently. The abstraction must cover all three, not just `qsub`.

## Proposed design

### 3a. `SchedulerBackend` interface (control plane)

New `eval/lib/schedulers/base.py`, mirroring `ProxyBackend`/`EngineBackend`:

```python
class SchedulerBackend(ABC):
    name: str                                    # "pbs", "slurm"

    def default_queue_and_walltime(self, num_nodes: int) -> tuple[str, str]: ...
    def render_job(self, spec: JobRenderSpec) -> str: ...     # returns script text
    def submit(self, job_path: str) -> str: ...               # -> job_id
    def poll(self, user: str) -> QueueState: ...              # running/queued counts
    def cancel(self, job_id: str) -> None: ...
    def slot_limits(self) -> dict[str, int | None]: ...       # per-queue caps
```

`JobRenderSpec` is the scheduler-neutral render input (job_name, num_nodes,
queue/partition, walltime, account, filesystems-or-None, stdout/stderr dirs,
code_root, env_script, run_yaml_path, job_exports). PBS uses `filesystems`;
Slurm ignores it. Fields not meaningful to a backend are simply unused.

### 3b. Registry keyed on the existing `scheduler.type`

`eval/lib/schedulers/__init__.py::get_scheduler(type) -> SchedulerBackend`,
same lazy-registry shape as `get_proxy`/`get_engine`. `run_executor` and
`run_planner` stop importing `pbs.render_pbs_job` directly and go through the
registry using `SchedulerSpec.type`. `PBSScheduler` wraps today's `pbs.py`
verbatim (zero behavior change); `SlurmScheduler` is the new second impl.

### 3c. Runtime seam (Layer C) — the harder half

The launcher must stop reading `$PBS_NODEFILE` / hard-calling `mpiexec` directly.
Introduce a thin, scheduler-neutral contract the job body sets **before**
`launch_cluster.sh` runs:

- **`EXASERVE_NODEFILE`** — path to a one-host-per-line nodefile. A small shim
  resolves it per scheduler:
  - PBS: `EXASERVE_NODEFILE="$PBS_NODEFILE"`
  - Slurm: `scontrol show hostnames "$SLURM_JOB_NODELIST" > $tmp; EXASERVE_NODEFILE=$tmp`
  `launch_cluster.sh` reads `EXASERVE_NODEFILE` (fallback to `$PBS_NODEFILE` for
  back-compat).
- **`EXASERVE_MPILAUNCH`** — the run-once-per-node launch prefix, so the bcast/
  overlay steps call `$EXASERVE_MPILAUNCH <cmd>` instead of a literal `mpiexec`:
  - PBS/PALS: `mpiexec -n {N} -ppn 1 --cpu-bind none`
  - Slurm: `srun --nodes={N} --ntasks-per-node=1`
  N is derived from the nodefile line count in both cases.
- **`EXASERVE_JOBID`** — generalize `$PBS_JOBID` (→ `$SLURM_JOB_ID`).

This keeps `mpiexec`/PALS as the PBS default while making the primitive swappable.
The MPI-broadcast staging itself (bcast.c/gather.c) is MPI-generic and works under
`srun` too — only the launch prefix differs.

### PBS → Slurm mapping (reference)

| Concern | PBS Pro + PALS (today) | Slurm |
|---|---|---|
| Header | `#PBS -N/-A/-q/-l select=/-l walltime=/-l filesystems=/-k` | `#SBATCH -J/-A/-p/-N/-t/(no fs)/--open-mode` |
| Submit | `qsub` | `sbatch` |
| Queue poll | `qstat -u $USER` | `squeue -u $USER` |
| Cancel | `qdel` | `scancel` |
| Nodefile | `$PBS_NODEFILE` | `scontrol show hostnames $SLURM_JOB_NODELIST` |
| Job id | `$PBS_JOBID` | `$SLURM_JOB_ID` |
| Per-node launch | `mpiexec -ppn 1` (PALS) | `srun --ntasks-per-node=1` |
| "queue" | queue (`-q`) | partition (`-p`) |

### 3d. `SchedulerSpec` generalization

Keep `type`; make PBS-only fields optional/typed per backend. Rename `queue` in
docs to "queue/partition"; `filesystems`/`keep_output` become PBS-scoped extras
(a `scheduler.extra: {}` passthrough, or a per-type sub-schema). Defaults
(`project=AuroraGPT`, `filesystems=home:flare`) move into the **site** config
(step 4), since they are site facts, not scheduler facts.

## What stays PBS/Aurora-specific (out of scope for (3))

- `subjob`/`keepalive.sh` dev tooling (`references` in the aurora-hpc skill) is an
  Aurora convenience for interactive leases; it stays PBS-side and is not part of
  the portable path.
- Site facts (account, filesystems, `env_aurora`, module loads) belong to **(4)
  vendor/site**, not the scheduler. The scheduler backend should receive them as
  inputs, not hardcode them.

## Migration & validation

1. Add `schedulers/base.py` + registry; wrap current PBS logic as `PBSScheduler`
   (behavior-identical). Route `run_executor`/`run_planner` through the registry.
   **Gate: PBS smokes still pass unchanged** (same set as the engine refactor).
2. Add `EXASERVE_NODEFILE`/`EXASERVE_MPILAUNCH`/`EXASERVE_JOBID` shims to the job
   body + `launch_cluster.sh` (PBS path sets them from `PBS_*`; back-compat
   fallback retained). **Gate: PBS smokes still pass.**
3. Add `SlurmScheduler` + Slurm branch of the runtime shim.
   **Validation is offsite** — no Slurm cluster on Aurora, so this can only be
   unit-tested (render/submit-command shape) here; end-to-end Slurm validation is
   owed on an actual Slurm system and must be flagged as unproven until then.

## Open questions
- Does `submit.py` (Layer B) get folded into the same registry, or stay a thin
  PBS-only convenience? (Lean: fold it, share `render_job`.)
- Per-queue slot limits are scheduler+site policy — live on the backend, or in
  site config? (Lean: backend default, site override.)
- Interactive-lease equivalent for Slurm (`salloc`/`srun --pty`) — needed for a
  Slurm analog of `subjob`, or leave dev-tooling site-specific?
