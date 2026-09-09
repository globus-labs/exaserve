# Final-snapshot PP405B low-scale completion — 2026-09-09

Status: n4 accepted on 2026-09-09 after independent strict validation; n8
running as PBS 8814766, with n16 still gated on accepted n8.

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
| n4 | 8814589 | capacity / 02:00:00 | Accepted; PBS finished 21:31:50 UTC, exit 0 | 96 |
| n8 | 8814766 | capacity / 02:00:00 | Running since 22:05:53 UTC; startup under observation | 192 |
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
`es-43d2461f6ba1`; its durable state is now `SUCCEEDED`, phase `succeeded`, and
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

## n4 live execution

PBS `8814589` entered `R` at `2026-09-09 20:29:18 UTC`, with exactly four
nodes and a two-hour allocation. The exact bound nodes are `x4312c4s4b0n0`,
`x4312c6s1b0n0`, `x4312c6s5b0n0` and `x4312c7s3b0n0`. Generation is
`1788985796180450420`; the logged allocation-binding prefix is `528fb16f0c6e`.

By `20:30:01 UTC`, head-rooted source distribution and verification passed on
all 4/4 ranks: each published the same 415-file, 22,829,930-byte source capsule,
verified two model-info cache seeds, node-local `tmpfs` runtime/state, and the
qualified framework Python on `squashfs`. Capsule manifest:
`f40883b0602db3790d4ca1e4dab75eaf6fad9fedb3222a8d5db2297fc3f31d32`.
This is early healthy staging evidence, not READY or an accepted replay result.

## n4 acceptance — 2026-09-09 21:50 UTC

The resumed monitor independently ran `require_accepted_paper_run` from the
clean `/tmp/exaserve-pp-canary.sSUkVe/repo` worktree at the frozen commit above,
requiring `replay/default`, `deployment_ready_evidence`,
`compatibility_receipts`, and `run_provenance`. Every entry was authenticated
with `load_authenticated_result_json`, and `_validate_replay_results` returned
no incomplete reasons. The result manifest is complete, hash
`2a7bc84e78e4268a7baa48b6098454a59d705fac54e48bf77c91704ceeff65bb`;
canonical RunStatus is `SUCCEEDED`, revision 5, `submit_attempt=1`.

| Replay | Role | Scheduled / completed | Errors | Duration (s) | RPS | p50 (s) | p99 (s) |
|---|---|---:|---:|---:|---:|---:|---:|
| 0 | Warmup | 96 / 96 | 0 | 135.572890 | 0.708106 | 14.733524 | 24.715352 |
| 1 | Reported measurement | 96 / 96 | 0 | 134.225298 | 0.715215 | 15.131095 | 17.840994 |

Both replays have complete typed gathers for exact ranks `[0,1,2,3]`, no
missing ranks, and `meta.completed_runs=2`; `overall` exactly mirrors replay 1.
Authenticated READY evidence binds the same generation and allocation and has
no blockers, missing identities, or unhealthy identities. It proves four
exact Ray nodes, four healthy proxies, six exact applications, 46/46 receipt
slots, and 2/2 real Llama-3.1-405B replicas at TP=8, PP=2 with disjoint planned
rank pairs `(0,1)` and `(2,3)`. The observed node set exactly equals the
allocation binding.

Terminal deployment state is `STOPPED`, reason `DRAINED_AND_REAPED`, with
published shutdown evidence `clean=true`, `errors=[]`, and
`deadline_exhausted=false`. The service log records DRAIN and GOODBYE from all
4/4 ranks. Shutdown-related SIGTERM/rank-launcher 143 messages follow the
requested drain; they are preserved and are not hidden or misclassified as
an experiment failure. PBS historical status is `F`, `Exit_status=0`, with
reported runtime `01:01:21` and `obittime=2026-09-09 21:31:50 UTC`.

Verdict: **PASS** for this n4 paper-campaign cell. This is not a complete
production release qualification claim.

## n8 submission preflight

Before submission, n8 remained `PLANNED`, revision 0, phase `materialized`,
without a scheduler job ID or prior submit attempt. Its semantic hash is
`1de8115a2b3b7b47c0efa4fbd1b5292cc6019c61bcbe06aa3328fa2ab9718341`;
its source snapshot matches accepted n4. Exact deployment/scheduler size is
8 nodes; the sealed job requests capacity and `02:00:00`, sources
`env_aurora`, then unsets `ONEAPI_DEVICE_SELECTOR` before compute execution.
Required bundle, trace, runtime, plan, profile and output-directory checks
passed. `qstat -u wenyiw` showed no active user jobs; `subjob status` showed no
active leases and keepalive metadata reported no usable source.

Submission command, from the clean frozen worktree after loading `frameworks`
and `go` and unsetting `ONEAPI_DEVICE_SELECTOR`:

```bash
PYTHONPATH=src python3 -m eval.cli run submit /lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs/sc26workshop/full/pp405b_pp2_haproxy_nostream_v040/run11/n8/run.yaml
```

The canonical command submitted PBS
`8814766.aurora-pbs-0001.hostmgmt.cm.aurora.alcf.anl.gov` once at
`2026-09-09 21:50:53 UTC`; the scheduler name is `es-d6c3ff1f758b`.
Post-submit canonical state is `SUBMITTED`, revision 2, `submit_attempt=1`.
At `21:51:42 UTC`, PBS was `Q` with exactly eight requested nodes and two
hours. The skill's `wait_for_job.sh 8814766 604800` monitor is live and printed
`WAITING|job=8814766|queue=capacity|max_wait=7d|started=2026-09-09T21:51:45+00:00`.
Queued time is expected; runtime/setup evidence will be checked after start.

## n8 live execution

PBS entered `R` at `2026-09-09 22:05:53 UTC`, with exactly eight nodes and
two hours (nominal allocation deadline `2026-09-10 00:05:53 UTC`). The approved
wait helper returned `JOB_READY|8814766|x4115c3s4b0n0`. The first direct check at
`22:06:20 UTC` found PBS stdout/stderr files newly created but empty; canonical
state was still `SUBMITTED` during initial shell startup. A live stdout/stderr
tail is now following the sealed PBS job. No READY claim is made from PBS
running state alone.

By `22:06:58 UTC`, canonical RunStatus was `RUNNING`, revision 3, and
DeploymentStatus was `STAGING`. Source distribution passed all 8/8 ranks in
5.5 seconds, with 415 files / 22,865,346 bytes, verified node-local `tmpfs`
runtime/state, qualified framework Python on `squashfs`, and 2/2 model-info
seeds on every rank. Source-capsule manifest:
`ff9b26a861db2bce75923988bc47ab51eb7eb9abff7aa22a82ea7822b9206a4b`.
The n4/n8 runtime-capsule hashes differ; both immutable RunPlans retain the
same source snapshot hash. These are separate recorded identity fields.
Model staging has begun; no execution error was observed at this checkpoint.

Generation is `1788991595757467635`; allocation-binding hash is
`b26e32844e872a9d6ce0c8cf72b7260f802625b403eb616b92e4a1ac4f03bd7e`.
Exact rank-order nodes are `x4115c3s4b0n0`, `x4209c5s7b0n0`,
`x4302c6s7b0n0`, `x4312c0s5b0n0`, `x4401c0s3b0n0`, `x4401c1s6b0n0`,
`x4201c3s2b0n0`, and `x4315c2s7b0n0`.
