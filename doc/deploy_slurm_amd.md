# Deploying ExaServe on Slurm + AMD/NVIDIA (bring-up guide)

**Status: implemented, not yet run on a Slurm/AMD system.** Aurora (PBS + Intel
XPU) remains the only validated path. Everything below is built and unit-checked;
this guide is for the first real bring-up (e.g. NCSA Delta MI100). Items marked
**⚠ VERIFY** need confirming on the node.

## The three knobs

ExaServe selects scheduler and accelerator at runtime via env vars — no code
changes, no YAML changes:

| Env var | Values | Default | Effect |
|---|---|---|---|
| `EXASERVE_SCHEDULER` | `psij` (alias `exawork`) \| `pbs` \| `slurm` | `psij` | job submit + status (`exaserve-serve-submit`). The default is the ExaWorks PSI/J portable layer (executor auto-detected; see [exawork_psij_notes.md](exawork_psij_notes.md)); `pbs`/`slurm` are the hand-rolled native fallbacks |
| `EXASERVE_VENDOR` | `xpu` \| `cuda` \| `rocm` | `xpu` | device isolation + engine device string |
| `EXASERVE_ENV_SETUP` | shell snippet | Aurora `env_aurora`/frameworks | how the job puts Ray + engine + toolchain on PATH |

Two more, rarely needed:
- `EXASERVE_MPILAUNCH` — override the per-node launch prefix entirely (default:
  `mpiexec -n N -ppn 1 --cpu-bind none` for PBS, `srun --nodes=N
  --ntasks-per-node=1 --cpu-bind=none` for Slurm).
- `EXASERVE_ROCM_VISIBLE_VAR` — `ROCR_VISIBLE_DEVICES` (default) or
  `HIP_VISIBLE_DEVICES` for AMD device isolation.

## What the scheduler seam does

The launcher (`exaserve-launch-cluster`) is scheduler-agnostic. At startup it:
1. detects the allocation — PBS `$PBS_NODEFILE`, or Slurm `$SLURM_JOB_NODELIST`
   materialized into a nodefile via `scontrol show hostnames`;
2. picks the per-node launcher — `mpiexec` on PBS/PALS, **`srun` on Slurm**
   (Cray/Slurm sites have no `mpiexec`);
3. runs one driver per node; rank 0 (from `PMI_RANK`/`SLURM_PROCID`) is the head.

So the MPI weight broadcast, log gather, and driver all work under `srun`.

## Path A — batch submit (`exaserve-serve-submit`)

```bash
export EXASERVE_SCHEDULER=slurm   # optional: default 'psij' auto-detects Slurm; set this to force the native backend
export EXASERVE_VENDOR=rocm
export EXASERVE_ENV_SETUP='module load gcc python
export PATH=/opt/rocm/bin:$PATH
source $WORK/exaserve-venv/bin/activate'

exaserve-serve-submit examples/config.delta.mi100.yaml \
    --project-account <gpu-account> --queue gpuMI100x8 --walltime 01:00:00 --wait
# --queue is the Slurm partition; --wait prints http://<head>:4001 once running.
```

This renders an `#SBATCH` script (partition, `--nodes`, `--ntasks-per-node=1`,
`--gpus-per-node` from `num_gpus_per_node`), `sbatch`s it, and resolves the head
node via `squeue`/`scontrol`.

## Path B — interactive (`salloc` + `exaserve-launch-cluster`)

```bash
salloc -A <acct> --partition=gpuMI100x8-interactive --nodes=1 \
       --gpus-per-node=8 --time=01:00:00
# inside the allocation:
export EXASERVE_VENDOR=rocm
module load gcc python; export PATH=/opt/rocm/bin:$PATH
source $WORK/exaserve-venv/bin/activate
exaserve-launch-cluster examples/config.delta.mi100.yaml
```

`exaserve-launch-cluster` reads `$SLURM_JOB_NODELIST`, so no scheduler var is
needed here; only `EXASERVE_VENDOR=rocm` (the scheduler is auto-detected).

## Building the ROCm engine environment (Delta MI100)

Delta has **no ROCm PyTorch module** — build a venv (or use an AMD Infinity Hub /
Apptainer container). Sketch:
```bash
module load gcc python
python -m venv $WORK/exaserve-venv && source $WORK/exaserve-venv/bin/activate
pip install --index-url https://download.pytorch.org/whl/rocm6.x torch   # ⚠ match /opt/rocm version
pip install vllm                                                          # ⚠ ROCm vLLM build
pip install ray[serve] fastapi
pip install /path/to/exaserve                                            # this package
```
**⚠ VERIFY on gpud01:** ROCm version (`cat /opt/rocm/.info/version`), that the
torch/vLLM wheels match it, and that vLLM's ROCm build imports (`python -c "import
vllm"`). SGLang on ROCm is not covered — vLLM is the supported AMD engine.

## NVIDIA (CUDA) sites (e.g. Delta A100, most Slurm clusters)

Same as above with `EXASERVE_VENDOR=cuda` and a CUDA torch/vLLM venv (or
`module load pytorch-conda` on Delta). CUDA is the vendor default inside vLLM, so
no ROCm caveats apply. `num_gpus_per_node: 4` for `gpuA100x4`.

## Known-unverified (owed on first bring-up)

- **AMD device isolation** — we set `ROCR_VISIBLE_DEVICES`; confirm a replica only
  sees its GPU (`rocm-smi` inside the replica), else set
  `EXASERVE_ROCM_VISIBLE_VAR=HIP_VISIBLE_DEVICES`. On Delta also confirm the MI210
  doesn't shift MI100 device indices.
- **Multi-node on Slurm** — the srun seam is implemented but only Delta's *single*
  MI100 node is available for AMD; the multi-node srun path (NVIDIA partitions,
  other sites) is unproven end-to-end.
- **`num_gpus_per_node`** must match the partition (8 for MI100/A100x8, 4 for
  A100x4). ExaServe places one replica per GPU from this number.
- **Proxy** — HAProxy isn't preinstalled off-Aurora; `bash scripts/build_haproxy.sh`
  or use `proxy_config.type: none` (per-node endpoints) / `litellm`.
- **PP > 1 on CUDA/ROCm** — the XPU compiled-DAG workaround is correctly *not*
  applied off-XPU, but multi-node pipeline parallelism on CUDA/ROCm is untested.

## What is NOT portable yet

Aurora-specific dev tooling (`subjob`, `keepalive`, `env_aurora`), the Copper
staging path (`EXASERVE_AURORA_USE_COPPER`), and the 256-node Ray/vLLM scale
patches remain Aurora/PBS-scoped. They don't block single-/few-node Slurm+AMD
serving.
