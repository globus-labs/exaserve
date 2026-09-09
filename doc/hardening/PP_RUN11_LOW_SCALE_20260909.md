# Final-snapshot PP405B low-scale completion — 2026-09-09

Status: monitoring; no newly accepted paper point is claimed here yet.

This execution record covers the existing immutable `run11` n4, n8 and n16
cells of `pp405b_pp2_haproxy_nostream_v040`. It is evidence/context; the
production-hardening execution plan and `AGENTS.md` remain authoritative.

All three cells bind commit `0a835470421bfabba744688061ad6ef70ed6752c`, source
snapshot
`5d85794f26a54596dc4ddb237996652171b9cd78d21751c8235bf0e25f8d365f`, and the
`bounded_hash_v2` identity scheme. This matches accepted `run11/n256`, PBS
`8811810`; its accepted result is preserved unchanged.

Run-group root:
`/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs/sc26workshop/full/pp405b_pp2_haproxy_nostream_v040/run11`.

| Cell | PBS job | Queue / walltime | Observed status | Expected requests per replay |
|---|---|---|---|---:|
| n4 | 8814589 | capacity / 02:00:00 | Submitted; scheduler Q at 2026-09-09 20:03 UTC | 96 |
| n8 | none | capacity / 02:00:00 | PLANNED; gated on accepted n4 | 192 |
| n16 | none | capacity / 02:00:00 | PLANNED; gated on accepted n8 | 384 |

## WP12 execution budgets

No eligible keepalive allocation is available for these exact-size jobs;
acquisition is the canonical PBS batch fallback. Each gate uses FINAL-lane
recordkeeping for this paper campaign, not a complete production release claim.

| Gate ID | Lane | Logical / physical nodes | Acquisition / queue | Walltime | Expected runtime | Requested node-hours | Attempt limit | Clean state / output |
|---|---|---:|---|---|---|---:|---:|---|
| PP405B-RUN11-N4 | FINAL | 4 / 4 | canonical batch / capacity | 2h | about 75m | 8 | 1 | `clean_stage=true`; unique `run11/n4` bundle |
| PP405B-RUN11-N8 | FINAL | 8 / 8 | canonical batch / capacity | 2h | about 75m | 16 | 1 | `clean_stage=true`; unique `run11/n8` bundle |
| PP405B-RUN11-N16 | FINAL | 16 / 16 | canonical batch / capacity | 2h | about 75m | 32 | 1 | `clean_stage=true`; unique `run11/n16` bundle |

The attempt limit applies to each immutable identity. A retry requires a new
identity after a diagnosed change; no failed bundle is reset or resubmitted.
All previous results, logs, caches and negative evidence are preserved.

## Execution and acceptance contract

The n4 job was submitted once at `2026-09-09 20:02:22 UTC`, scheduler identity
`es-43d2461f6ba1`; its durable state is `SUBMITTED`, phase `submitted`, and
`submit_attempt=1`. No duplicate submission was made by this monitor.

The exact immutable job body sources `/home/wenyiw/script/env_aurora`, selects
the sealed snapshot, and invokes `python3 -m eval.cli run execute <cell>/run.yaml`
inside its exact-size PBS compute allocation. Monitoring and small evidence
reads occur on the login node; model, Ray, MPI and replay execution do not.
Logs are under `<cell>/logs/pbs/{stdout,stderr}` and outputs under
`<cell>/results`.

The provided Aurora `wait_for_job.sh` is monitoring PBS `8814589` with the
explicit capacity start-wait bound of 604800 seconds. Queued time is not an
execution failure. Once running, stdout/stderr, canonical state and remaining
walltime are inspected for early failures.

Each next submission is permitted only after the preceding cell passes
`require_accepted_paper_run`, replay validation, exact readiness/topology and
clean terminal shutdown checks. The protocol is one deployment, replay 0 as
warmup and replay 1 as the reported measurement, both complete with zero
errors. n8 and n16 will use canonical `eval.cli run submit` after verifying
that their existing identities remain unsubmitted and queue capacity exists.

No n32-or-larger submission is authorized by this record. Their one-hour
walltime is insufficient under current staging observations, and no compliant
long-allocation acquisition/subset-proof route is presently available; the
root task is requesting user direction for that distinct blocker.
