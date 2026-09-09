# Final-snapshot null-compute startup curve — 2026-09-09

Status: monitoring; both 32-node trials are accepted. Six trials remain.

This record covers two independent startup-only deployment lifecycles per
32, 64, 128 and 256 nodes for `nullcompute_haproxy_scale_to256_v040`.
The immutable groups are `run12` and `run13`, materialized from commit
`0a835470421bfabba744688061ad6ef70ed6752c`, source snapshot
`5d85794f26a54596dc4ddb237996652171b9cd78d21751c8235bf0e25f8d365f`.
This is campaign evidence/context; `AGENTS.md` and the production-hardening
execution plan remain authoritative.

Run-store parent:
`/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs/sc26workshop/full/nullcompute_haproxy_scale_to256_v040`.

## WP12 execution budgets

No eligible user-managed keepalive allocation is available. Acquisition is
the approved canonical PBS batch fallback, with exact logical and physical
node counts. FINAL denotes campaign gate recordkeeping, not a complete
production release claim. Budgets below are recorded before the monitor's
next submission; the root submitted the first cell before this record.

| Gate ID | Lane | Logical / physical nodes | Acquisition / queue | Walltime | Expected runtime | Requested node-hours | Attempt limit | Clean state / output |
|---|---|---:|---|---|---|---:|---:|---|
| NULL-RUN12-N32 | FINAL | 32 / 32 | canonical batch / debug-scaling | 1h | about 15m | 32 | 1 | `clean_stage=true`; fresh `run12/n32` bundle |
| NULL-RUN13-N32 | FINAL | 32 / 32 | canonical batch / debug-scaling | 1h | about 15m | 32 | 1 | `clean_stage=true`; fresh `run13/n32` bundle |
| NULL-RUN12-N64 | FINAL | 64 / 64 | canonical batch / debug-scaling | 1h | about 15m | 64 | 1 | `clean_stage=true`; fresh `run12/n64` bundle |
| NULL-RUN13-N64 | FINAL | 64 / 64 | canonical batch / debug-scaling | 1h | about 15m | 64 | 1 | `clean_stage=true`; fresh `run13/n64` bundle |
| NULL-RUN12-N128 | FINAL | 128 / 128 | canonical batch / debug-scaling | 1h | about 15m | 128 | 1 | `clean_stage=true`; fresh `run12/n128` bundle |
| NULL-RUN13-N128 | FINAL | 128 / 128 | canonical batch / debug-scaling | 1h | about 15m | 128 | 1 | `clean_stage=true`; fresh `run13/n128` bundle |
| NULL-RUN12-N256 | FINAL | 256 / 256 | canonical batch / prod | 3h | about 30m | 768 | 1 | `clean_stage=true`; fresh `run12/n256` bundle |
| NULL-RUN13-N256 | FINAL | 256 / 256 | canonical batch / prod | 3h | about 30m | 768 | 1 | `clean_stage=true`; fresh `run13/n256` bundle |

Total requested maximum: 1,984 node-hours; expected useful runtime budget:
368 node-hours. Each attempt limit applies to one immutable identity. Failure
halts the ladder; a retry needs a diagnosed change, review by the coordinating
agent, and a fresh identity. Failed bundles are never reset or resubmitted. Existing artifacts,
caches, logs and negative evidence are preserved.

## Execution and acceptance contract

Submission order is the table order below. A next cell is submitted only after
the preceding cell passes strict acceptance and PBS exits zero. Each cell has
one startup-only lifecycle; `client.num_runs=1` does not create a second
deployment. Independent trials require distinct generations and provenance.

Canonical submission is performed after `module load frameworks && module
load go`, with `ONEAPI_DEVICE_SELECTOR` unset, from the clean frozen-commit
checkout `/tmp/exaserve-pp-canary.sSUkVe/repo`:

```text
python3 -m eval.cli run submit <run-store-parent>/<group>/n<nodes>/run.yaml
```

The sealed job body selects its immutable source snapshot, sources
`/home/wenyiw/script/env_aurora`, and executes the exact materialized run in
its PBS compute allocation. The login node is used only for submission,
monitoring and small evidence reads. Logs are at `<cell>/logs/pbs/{stdout,stderr}`
and backend logs at `<cell>/logs/backend`; accepted output is under
`<cell>/results`. No queue, walltime, source, node-count or frozen-bundle
override is permitted.

Queued starts use the Aurora skill's `wait_for_job.sh` with explicit limits:
10,800 seconds for debug-scaling and 604,800 seconds for prod. While running,
inspect stdout/stderr and canonical state for early failure and walltime
margin; queued time itself is not failure.

Acceptance invokes `eval.plot.nullcompute_startup_table._load_trial` with the
explicit `run12` or `run13` group, not the consumer's historical `RUN_GROUPS`.
It must authenticate all seven result-manifest entries, exact READY and
STOPPED identities, `12*N` measured replicas, `2*N` Serve applications and
`14*N+2` required receipt slots. Additional gates require clean shutdown,
no exhausted cleanup deadline, exact allocation-bound source/model staging,
matching final source hash and PBS terminal state F with exit zero. Only then
are startup measurements accepted. Once both trials at a scale exist, their
generation and run-provenance hashes must differ.

## Cell status

| Cell | PBS job | Status | READY / trace / deploy / stage3 seconds | Generation |
|---|---|---|---|---|
| run12/n32 | 8814592 | ACCEPTED; PBS F, exit 0, used 00:01:32 | 28.961413 / 23.5403 / 19.8505 / 19.8576 | 1788985796176521479 |
| run13/n32 | 8814635 | ACCEPTED; PBS F, exit 0, used 00:01:33 | 29.888999 / 23.4858 / 19.7354 / 19.7419 | 1788987869969436504 |
| run12/n64 | none | PLANNED; gated on accepted run13/n32 | pending | pending |
| run13/n64 | none | PLANNED; gated on accepted run12/n64 | pending | pending |
| run12/n128 | none | PLANNED; gated on accepted run13/n64 | pending | pending |
| run13/n128 | none | PLANNED; gated on accepted run12/n128 | pending | pending |
| run12/n256 | none | PLANNED; gated on accepted run13/n128 | pending | pending |
| run13/n256 | none | PLANNED; gated on accepted run12/n256 | pending | pending |

`run12/n32` scheduler identity is `es-64991680f6a4`. Its durable submit time
is `2026-09-09 20:04:19 UTC`; the monitor did not submit it again. The queued
start helper began at `2026-09-09T20:05:41+00:00`, with a three-hour bound.

The first job started at `2026-09-09 20:29:18 UTC`, allocation head
`x4711c1s6b0n0`, requesting exactly 32 nodes for one hour. Canonical state
became RUNNING at 20:29:56 UTC. Its allocation-binding hash is
`821ed9ef2f204ace681657dd415a26b02b66d34611326cda6e4dabeb3ba05b69`.
Source staging produced 32/32 rank receipts in 4.019767 seconds for 415 files
and 23,407,374 bytes; the source capsule manifest is
`635408b9f30611a8ed208548b7d294ee99fee408bbbfbf3c51df98980a1c7669`.
Early logs verify exact 32-node Ray membership and 384/384 registered GPUs;
the 384 node-grouped null replicas are being deployed. Model-weight staging
is deliberately skipped in null-compute mode. This is startup progress,
not acceptance evidence.

### Accepted run12/n32

Strict `_load_trial(root, "run12", 32)` and the additional allocation-bound
source-staging and shutdown checks passed. The final source hash matches the
campaign identity. READY and STOPPED bind generation `1788985796176521479`;
384 replica measurements, 64 Serve applications and 450 receipt slots match
the exact 32-node plan. All seven result entries passed byte-length and
SHA-256 checks. ResultManifest semantic hash:
`8debbbc798ea146dd1ef8a442d284d4e3a9aa0bc32afd03dee03b572b935c5cd`.
Run-provenance hash:
`6446f429af36a245eef4d8fc7fb0bf1bc7a0d35ebfb02acb30509b8b9066a802`.

The job published clean STOPPED with 32/32 DRAIN and GOODBYE, no shutdown
errors and no exhausted deadline. RunStatus became SUCCEEDED at
`2026-09-09 20:31:10 UTC`; PBS finished at `20:31:43 UTC`, terminal F,
exit 0, reported walltime `00:01:32`. Metrics-exporter connection warnings
were observed; the site profile disables Ray metrics collection. They did
not prevent complete hash-bound startup evidence or clean shutdown. Launcher
143 and HAProxy -15 are controlled shutdown outcomes, not the PBS exit status.

After the preceding point passed every acceptance gate, canonical submission
of fresh `run13/n32` succeeded as PBS `8814635`, scheduler identity
`es-e0046ddec140`. Its preflight confirmed PLANNED revision 0, no scheduler
job ID, no prior ResultManifest and the exact final source hash. A dedicated
Aurora start monitor uses the explicit 10,800-second debug-scaling bound.

### Accepted run13/n32 and monitor resumption

The resumed monitor independently revalidated both 32-node trials through
`_load_trial(root, group, 32)` with explicit `run12` and `run13` groups. The
second trial started at `2026-09-09 21:03:40 UTC` and finished at
`21:06:19 UTC`, PBS F / exit 0 / run_count 1 / walltime `00:01:33`.
Its exact 32 physical PBS hosts match the immutable allocation binding,
`b61a5a361a64a267d66cd5cefc82e1da1d409b7ea37bcaefbd7d007618a603db`.

All seven manifest entries authenticate; the ResultManifest semantic hash is
`2c48f0342e30a00bf4bb70020f8cbddb4b42b660ba79c2190e03a33375fe619f`.
Source staging passes the current-layout and VC-01 evidence validator,
including all 32 allocation-bound rank receipts; it took 3.915002 seconds.
READY and STOPPED use generation `1788987869969436504`, with 384 measured
replicas, 64 applications and 450 required receipt slots. Shutdown was clean,
without errors or deadline exhaustion, with 32/32 DRAIN and GOODBYE.
Run-provenance hash:
`cd7308c7fd8545ce356dd39543a3cb2cd65cdc6bb530e35b994e6115569bb102`.
Both 32-node trials retain the exact final source and have distinct generations
and provenance hashes. Metrics-exporter warnings again did not prevent the
hash-bound acceptance or clean shutdown.

Before the next submission, `subjob status` reports no active leases,
`qstat -u wenyiw` reports no jobs, and the user-managed keepalive record remains
stopped with no source allocation. Canonical batch fallback therefore remains
the acquisition path. The clean frozen checkout remains at `0a83547`.
