# Agent guide

- Follow [CONTRIBUTING.md](CONTRIBUTING.md) for development and review.
  [Getting started](docs/getting_started.md) owns environment preparation;
  do not assume maintainer-private scripts or home-directory layouts exist.
- The [production-hardening plan](doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md)
  is the architecture and implementation authority. Audits, TODOs and design
  drafts are context, not overrides. This file governs agent execution safety.
- Check [hardening status](doc/hardening/STATUS.md) before making support claims.
  Tests and historical runs do not qualify a changed artifact, platform or scale.
- Preserve unrelated changes and historical evidence. Do not delete or overwrite
  logs, checkpoints or results without approval; use unique run directories.
- Disclose AI assistance in the PR or commit description. Do not add agent
  author/co-author attribution by default or rewrite history for attribution.

## Execution safety

- On shared HPC systems, login nodes are for editing, inspection, submission
  and brief lightweight checks only. Full suites, intensive builds, evaluations,
  GPU and MPI work require allocated compute resources. When unsure, use compute.
- Use the site's supported scheduler/allocation workflow and approved environment.
  Verify job identity, node membership, requested resources and remaining
  walltime before execution. Plain SSH or a job ID alone does not prove allocation.
  Never fabricate scheduler state or access nodes outside the allocation.
- Check inputs, executables and output paths before launch. Avoid duplicate jobs.
  Monitor runs, diagnose early failures, stop clearly broken work safely and
  report exact commands, evidence paths and results without hiding errors.
- Use the [canonical submission and evaluation interfaces](docs/usage.md);
  native batch jobs execute in scheduler allocations, not on the login node.
  Scheduler job bodies provide environment setup and invoke the canonical
  executor; shell wrappers must not own readiness, lifecycle or cleanup.
- Allocation campaigns require the current controller/runtime-bound WP12
  isolation proof before production use. An unqualified controller may submit
  only its bounded qualification campaign. Report physical and logical node
  counts separately.
