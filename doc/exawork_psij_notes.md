# PSI/J adapter status

Status: implemented optional adapter; unsupported by the default Aurora release
profile.

PSI/J remains in the scheduler registry because it may be useful for a future
qualified site. It is not the default scheduler and `exawork` is only an alias
for the `psij` registry key. Installing `exaserve[scheduler]` makes the adapter
importable; it does not add it to `SiteProfile.scheduler_types` or qualify an
executor.

Historical experiments showed that PSI/J could submit and cancel an Aurora PBS
job, but those experiments predate the final architecture and do not prove the
current plan, reconciliation, status, cleanup, or scale contracts. They are
context only.

Before a release profile can name PSI/J, it needs:

- a pinned PSI/J/executor compatibility identity;
- exact render and custom-attribute tests for the target scheduler;
- accepted, rejected, and post-acceptance-timeout behavior;
- exact `run_identity` reconciliation without fuzzy job-name matching;
- terminal-state, head-node, and cancellation evidence;
- clean installation in the submitting environment;
- real-site one/two-node and approved scale-ladder evidence.

Until then, package submission and eval fail closed when the Aurora profile is
asked to use `psij` or `exawork`.
