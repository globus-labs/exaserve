# Eval control plane

The eval system is a frontend to the same immutable deployment architecture as
the serving CLI. It does not own a second serving schema, readiness protocol,
scheduler implementation, or process supervisor.

## Flow

```text
experiment YAML
  -> strict ExperimentSpec validation and matrix expansion
  -> canonical DeploymentPlan + RunPlan
  -> immutable trace and run materialization
  -> shared SchedulerBackend job
  -> RuntimeSupervisor deployment
  -> generation-bound DeploymentStatus READY
  -> supervised replay client
  -> atomic ResultManifest
```

`DeploymentPlan` contains serving topology and support-envelope identity.
`RunPlan` adds workload, trace, client, scheduler, backend, and artifact policy.
`EvalManifest` is the job-local presentation object: it references the exact
deployment plan path and hash rather than copying model, proxy, or placement
configuration.

Workload chat/completion/mixed and streaming/non-streaming choices are compiled
into the deployment's scale-envelope identity. A hash-valid manifest is still
strictly validated for numeric bounds, paths, replay topology, and plan hash.

## Commands

```bash
python3 -m eval.cli spec validate <spec>
python3 -m eval.cli trace materialize <spec>
python3 -m eval.cli run materialize <spec>
python3 -m eval.cli run submit <bundle-or-run.yaml> [--dry-run]
python3 -m eval.cli run submit-all <spec-name> [--dry-run]
python3 -m eval.cli run execute <run.yaml> [--dry-run]
```

Materialized scheduler scripts invoke `run execute` directly. Old
`run_exp.sh`, sequential submission shells, keepalive scripts, and duplicated
eval scheduler classes have been removed.

## Runtime rules

- Scheduler support is checked against the `SiteProfile` before artifacts or
  submissions are created.
- Scheduler subprocesses and replay clients have finite deadlines, process-group
  ownership, descendant cleanup, and causal errors.
- READY comes only from the canonical status store for the exact deployment,
  generation, plan hash, and allocation binding. Logs are evidence only.
- Client startup uses a nonce-bound readiness handshake; an exited or silent
  process fails the run.
- Results are published atomically with required-artifact hashes. Partial or
  failed runs cannot masquerade as complete results.
- All checked-in specs are loaded by a regression test.

Nontrivial runs must use a validated compute session and the workflow in
`AGENTS.md`. Client capacity and synthetic-target studies belong in ClientLab.
