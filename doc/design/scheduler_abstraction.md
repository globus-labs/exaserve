# Shared scheduler boundary

Status: implemented; the default Aurora release profile supports native PBS
only.

## Contract

Serving and eval use `exaserve.schedulers.SchedulerBackend`. Callers construct a
typed `JobSpec`; the backend owns rendering, submission, observation,
cancellation, allocation metadata, and exact reconciliation after ambiguous
submission.

The only raw shell field is `JobSpec.bootstrap_script`, which is trusted
site-administrator setup. Paths, environment values, and command arguments are
validated and quoted. The rendered job body ends in one `exec` of an argument
vector. There is no intermediate lifecycle shell.

Scheduler subprocesses run through a finite process-group helper with bounded
timeouts and cleanup. A timeout during submission is classified as ambiguous,
because the scheduler may already own the job. Retry is forbidden until exact
`run_identity` reconciliation proves whether a job exists.

## Backends and support

The registry contains PBS, Slurm, and PSI/J (`exawork` is an alias). Registry
presence describes code availability, not release support. The authoritative
`SiteProfile.scheduler_types` controls what can compile or submit:

- PBS: accepted by the Aurora release profile.
- Slurm: implemented adapter, rejected by the Aurora release profile.
- PSI/J: implemented optional adapter, rejected by the Aurora release profile.

Both package submission and eval compilation enforce that boundary before a
job or log artifact is created.

## Queue defaults

Aurora defaults are centralized in `default_queue_and_walltime`:

- 1–16 nodes: `capacity`, one hour;
- 17–255 nodes: `debug-scaling`, one hour;
- 256 or more: `prod`, two hours.

These scheduler defaults do not expand the scale envelope. The deployment must
also be within an approved or explicit validation-mode `ScaleEnvelope`.

## Qualification requirements

A non-PBS site needs a separately named `SiteProfile`, exact scheduler
capabilities, render/quote tests, accepted/rejected/ambiguous submission tests,
observation and cancellation tests, allocation binding evidence, and real-site
smoke/scale evidence. Until then, setting `EXASERVE_SCHEDULER` cannot bypass the
profile and fails closed.
