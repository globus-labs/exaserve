# ExaWorks PSI/J backend — Aurora validation & friction log

ExaWorks **PSI/J** (`psij-python`) is now the **default** scheduler backend
(`EXASERVE_SCHEDULER=psij`, alias `exawork`); the hand-rolled `pbs`/`slurm`
backends remain selectable alternatives. This file records what we validated on
Aurora, every friction point found, and how each was reduced — so PSI/J runs
smoothly on the next platform without rediscovering these.

## Why PSI/J as default

- One submit layer for PBS Pro, Slurm, LSF, Flux, Cobalt, NQSV — new sites need
  **no hand-written backend**, just `EXASERVE_PSIJ_EXECUTOR` (auto-detected from
  PATH when unset) and our site defaults (queue/walltime/account), which ride
  along unchanged.
- Library-only, login-node-only dependency (`pip install --user psij-python`);
  job scripts and compute nodes never import it.
- Health note (2026-08): maintained but in maintenance mode (last release
  0.9.11, May 2025; bus factor ≈ 1 at ANL). This is why the native backends are
  kept as first-class alternatives rather than deleted.

## Division of labor

| Concern | Owner |
|---|---|
| Submit-script directives + submission (qsub/sbatch/bsub quirks) | **PSI/J** |
| Job **body** (env setup, PYTHONPATH, launch) | ours — byte-identical to the native backends |
| Site defaults (queue, walltime, account, filesystems) | ours (site config/env), passed into PSI/J |
| Job state + head-node resolution | ours — one-shot `qstat`/`squeue` (PSI/J has no client-side nodelist and an async poller) |
| Per-queue slot limits / queue counting (`submit_all`) | ours (PSI/J only tracks its own jobs) |
| Runtime seam inside the job (`EXASERVE_NODEFILE`/`EXASERVE_MPILAUNCH`) | ours (scheduler auto-detected from the allocation env) |

Mechanism: `render_job` emits a self-describing script — `# PSIJ-SPEC: {json}`
header (executor, nodes, queue, walltime, account, custom attributes, output
paths) + the standard body — so render (materialize) and submit can happen in
different processes, and the artifact stays inspectable.

## Validated on Aurora (real submissions)

- `pip install --user psij-python` (0.9.11) on frameworks Python: clean.
- Raw PSI/J round-trip: submit → `QUEUED` → cancel via the `pbs` executor
  (job `8731000`), with Aurora specials as custom attributes
  (`{"pbs.l": "filesystems=home:flare", "pbs.k": "doe"}`) — qsub accepted the
  generated script, which also settles PSI/J's quoted-directive
  (`#PBS -l "filesystems=..."`) and `#PBS -N="name"` syntax questions on Aurora.
- End-to-end serve path (`exaserve-serve-submit`, default = psij, 1-node
  null-compute deployment) — three attempts, each finding/killing a real bug:
  - `8731095`: died instantly — relative `stdout_path` (friction #3). Fixed.
  - `8731123`: died instantly — relative *script* path, `/bin/bash: ... No
    such file or directory` (friction #3). Fixed, plus `bash -l` (friction #3b).
  - `8731184`: **PASS** — submit 0.8 s → `Q` → `R` (+219 s queue) → head node +
    URL via delegated qstat → **`ALL SERVICES READY` 51 s after R** → qdel.
  The passing run exercises the whole chain: PSI/J submit → PBS → login-shell
  env setup → launcher (scheduler auto-detect, mpiexec seam) → Ray + 12
  replicas + HAProxy → readiness → teardown.

## Friction found → reduction applied

1. **`EXASERVE_SCHEDULER` selector collision.** The same env var selects the
   *submit backend* (`psij`) and, at runtime, names the *launch mechanism*
   (`pbs`/`slurm` → mpiexec/srun). A leaked `psij` value would break
   `EXASERVE_MPILAUNCH` on Slurm. Reduced two ways: the launcher normalizes any
   value other than `pbs`/`slurm` and re-detects from the allocation env, and
   the PSI/J backend submits with `inherit_environment=False` (matching the
   native backends, which never used `-V`).
2. **10-minute default walltime.** PSI/J defaults `duration` to 10 min if
   unset. We always set it from our walltime.
3. **Every path is resolved against the JOB's cwd — relative paths are FATAL.**
   Native `#PBS -o` and `qsub script` resolve relative paths against the
   *submit* cwd; PSI/J resolves both its `stdout_path`/`stderr_path` **and the
   payload arguments** against the *job's* cwd (`$HOME` on PBS). Measured on
   Aurora, twice: job `8731095` (relative stdout path → wrapper redirect
   failed, payload never ran, no output anywhere) and job `8731123` (relative
   script path → `/bin/bash: tmp/.../x.psij.sh: No such file or directory`).
   PSI/J also forces `#PBS -o/-e /dev/null`, so nothing surfaces in the usual
   places. Fixed: absolutize the script path and stdout/stderr at render/submit.
3b. **The payload is invoked as `/bin/bash <script>` — the shebang is ignored.**
   Our scripts declare `#!/bin/bash -l` (login shell → `/etc/profile` → the
   `module` function for env setup); qsub honors that, PSI/J's launcher does
   not. Fixed: submit with `arguments=["-l", script]` so env_setup keeps its
   login-shell semantics on every scheduler.
4. **Asynchronous, fragile status poller.** PSI/J polls in a background thread
   (30 s interval) and marks **all** registered jobs FAILED after 2 consecutive
   poll errors — dangerous with Aurora's slow/flaky qstat. Reduced by not using
   it: for PBS/Slurm we answer `job_state`/`head_node` with our one-shot
   native helpers; PSI/J `attach` is only the fallback for other executors.
5. **No client-side nodelist.** PSI/J never reports allocated hosts, so
   `serve-url` head-node resolution stays native (`qstat -f exec_host` /
   `squeue %N`). On executors without a native helper, resolve the URL from the
   job's run logs (TODO: have the launcher write a head-node marker file).
6. **Opinionated PBS select line.** PSI/J emits
   `-l select=N:ncpus=P:mpiprocs=P` + `-l place=scatter:{exclhost|shared}`
   (vs our bare `-l select=N`). Accepted by Aurora's qsub (proven by real
   submits); we set `exclusive_node_use=True` by default
   (`EXASERVE_PSIJ_EXCLUSIVE=0` to opt out). Watch placement semantics on
   shared-node sites.
7. **GPU requests.** PSI/J's PBS template ignores its GPU field entirely, and
   Slurm only maps `--gpus-per-task`. For Slurm sites we inject
   `slurm.gpus-per-node` as a custom attribute from the deployment's
   `num_gpus_per_node` (the Delta idiom).
8. **No PALS `mpiexec` launcher** in PSI/J. Irrelevant to us: PSI/J launches
   only the job body (`launcher=single`); all per-node fan-out inside the job
   uses our `EXASERVE_MPILAUNCH` seam.
9. **Scheduler-specific extras have no first-class fields.** Aurora's
   `-l filesystems` / `-k` ride as namespaced `custom_attributes`
   (`pbs.l`, `pbs.k`); arbitrary site extras can be added via
   `EXASERVE_PSIJ_ATTRS` (JSON dict, e.g. `{"slurm.constraint": "scratch"}`).
10. **Post-mortem debugging needs `PSIJ_BATCH_KEEP_FILES=1`.** PSI/J deletes
    its per-job work files (`~/.psij/work/.../<jobid>.out`, the generated
    submit script) once a job completes, so a failed job leaves nothing to
    inspect by default. Export `PSIJ_BATCH_KEEP_FILES=1` when diagnosing.

## Comparison vs the native PBS backend (Aurora)

| | native `pbs` | `psij` (default) |
|---|---|---|
| Submit latency | ~1 qsub | ~1 qsub + psij overhead (< 1 s observed) |
| Script | exact historical `#PBS` header | PSI/J-generated header, same body |
| Dependencies | none | psij-python on the submit host |
| Extra schedulers | none | LSF/Flux/Cobalt/NQSV for free |
| Status/head-node | one-shot qstat | same code (delegated) |
| Risk | none (validated for months) | upstream maintenance-mode; mitigated by keeping native backends |

## Using it

```bash
# default — no env needed on a PBS or Slurm machine (executor auto-detected):
exaserve-serve-submit my_config.yaml --wait

# pin the executor on an exotic site, add site extras:
EXASERVE_PSIJ_EXECUTOR=lsf EXASERVE_PSIJ_ATTRS='{"lsf.core_isolation": "1"}' \
  exaserve-serve-submit my_config.yaml --wait

# native fallbacks:
EXASERVE_SCHEDULER=pbs   exaserve-serve-submit ...   # Aurora hand-rolled
EXASERVE_SCHEDULER=slurm exaserve-serve-submit ...
```

Eval harness: specs select it with `scheduler.type: psij` (specs that omit the
type now default to psij; existing specs pinning `type: pbs` keep the native
path).
