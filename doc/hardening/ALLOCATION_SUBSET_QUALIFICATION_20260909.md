# Finite allocation subset qualification — 2026-09-09

Status: **proof C passed the bounded physical-four/logical-two qualification**
with the fully regression-tested controller and unchanged frozen child runtime.
The first two requests were withdrawn while queued. This proof is not a paper
throughput sample or a general multi-tenant reservation qualification.

This evidence record accompanies the full-scaling continuation. The authoritative
design is WP12 in `doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md`; this file does
not waive its safety, ownership, staging or evidence requirements.

## Why this work is needed

Aurora's exact-size 32/64/128-node debug-scaling queue permits at most one hour.
The final-runtime four-node 405B run, PBS `8814589`, used 1:01:21; the previous
hardened four-node run used 1:08:50, and accepted n256 used 1:14:28. One-hour
middle-scale submissions have insufficient demonstrated margin. Capacity is
limited to 16 nodes; production begins at 256. No eligible keepalive allocation
was available when this continuation resumed.

The proposed adapter acquires one finite physical PBS allocation and executes
exact logical subsets sequentially on its original head. It does not edit the
existing PP `run11` children, weaken exact membership or hash checks, shorten
their workload, or replace their canonical lifecycle. A native parent job is
not a keepalive and is not advertised as a subjob lease.

Parent controller source identity is separate from child runtime identity.
The target child source remains commit `0a835470421bfabba744688061ad6ef70ed6752c`,
snapshot `5d85794f26a54596dc4ddb237996652171b9cd78d21751c8235bf0e25f8d365f`.
The three intended children are PP `run11/n32`, `run11/n64`, and `run11/n128`;
their original standalone SchedulerPlans are **not submitted** by this route.
Accepted PP `run11/n256` remains unchanged.

## Pre-submission budgets and gates

| Gate | Lane | Logical / physical nodes | Acquisition | Walltime | Expected runtime | Requested node-hours | Attempt limit |
|---|---|---|---|---|---|---:|---:|
| Controller regression suite | WP0_EARLY | 1 / 1 | Approved `srundbg` interactive fallback | 1h | About 3–5m | 1 | 1 per reviewed candidate |
| Subset isolation proof | WP0_EARLY | 2 / 4, two independent null lifecycles | Native finite parent; capacity | 1h | 10–15m | 4 | 1 per immutable campaign |
| Middle PP curve | FINAL | 32, then 64, then 128 / 256 | Native finite parent; prod | 6h | About 3–4h total | 1,536 | 1 per immutable campaign |

The proof includes a normal full-replay lifecycle and a distinct deliberate
cancel-after-READY lifecycle. Only the first is expected to pass ordinary paper
acceptance; the second must prove bounded cancellation and cleanup and is never
a paper result. A native MPI over-launch attempt must fail without starting
extra ranks. An owned sentinel outside the logical subset must survive child
cleanup and then be explicitly reaped by its parent owner.

The fresh proof children have been materialized, but not submitted or executed:

- Happy path:
  `/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs/sc26workshop/smokes/nullcompute_haproxy_mpi_large_raw_2node/run1/n2/run.yaml`.
  Two replays each schedule 3,072 requests, exercising multi-chunk MPI result
  aggregation on the exact two-node subset.
- Cancellation:
  `/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs/nullcompute_haproxy_subset_cancel_2node/run0/n2/run.yaml`.
  Its 60-second replay supplies an unambiguous cancellation window. The spec was
  passed as an external input to the clean `0a83547` materializer, so the child
  runtime source remains unchanged; only this qualification workload differs.

Production admission additionally requires authenticated proof of exact subset
membership, all source-stage receipts, no extra Ray members, clean rank and
component teardown, independent generations/artifact namespaces, and unchanged
sealed child inputs. Qualification must bind the exact controller and child
source identities; missing or mismatched proof blocks materialization/submission.

Each PP child has a 90-minute execution bound with its full 900-second watchdog
cleanup window plus bounded forced-reap margin reserved. The parent must check
remaining budget before every child. A failure stops the sequence; no subsequent
child runs against possibly dirty resources. A retry requires diagnosis and a
fresh failed-attempt identity, never resetting failed evidence.

All children retain `clean_stage=true` and unique existing bundle outputs.
Parent acquisition, physical/subset inventories, immutable-input hashes, exact
commands, source identities, result references, timings and cleanup verdicts
will be stored in a fresh campaign output directory recorded here before submit.
No worker reads the parent manifest or writes a shared-filesystem receipt.

The first proof campaign has been materialized at
`/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/allocation_campaigns/subset-proof-20260909-a/campaign.json`.
It requests exactly four capacity nodes for one hour, with an eight-minute
execution deadline per child and separately reserved canonical cleanup budgets.
Campaign hash:
`67dfb2946f44f917a14e38f6bbfccf1909729d14124744817f728571cd5fb447`.
Controller commit `8c59f5130e642cfc3d23619e8933b548e6aa2bae`, controller source
hash `18603db557a3b663282a1c696603566f11857097f0a17f6c8f0e4a591da8f9a5`.
Only the unrelated design drafts were explicitly excluded from its committed
snapshot. Scheduler identity is `ac-67dfb2946f44`; native stdout/stderr are
directed to `/home/wenyiw/aurora_rayserver/tmp/`. Per-child executor logs and
parent proof sidecars live in the campaign directory; canonical child evidence
remains in the two original child bundle directories above.

### First request withdrawn before execution

Campaign A was submitted as PBS `8814852`, then verified still in Q and
cancelled before allocation on 2026-09-09. No child started: both retain
PLANNED revision 0. The campaign and its scheduler history are preserved;
it is not qualification evidence and must not be resubmitted.

A final inspection found that canonical READY can be visible to the parent
before the frozen executor's two-second observer has acknowledged it. Injecting
SIGINT in that interval can cause the executor to record a cleanup-contract
mismatch. The replacement waits for identity-bound RunStatus RUNNING with
phase `replaying`, as well as canonical READY, before cancellation. That phase
proves the executor's readiness wait returned; it does not claim the replay
subprocess has already started. Cancellation acceptance now rejects any
`cleanup_error` field and requires the exact RunStatus identity. This is a
controller-only correction; child runtime and all child input hashes stay fixed.

The updated controller passed all **72 targeted tests in 1.02 seconds** and
correctness lint. An independent review confirmed the new checkpoint ordering
against the frozen executor. The 1,768-test full-suite result below belongs to
the initial `8c59f51` candidate; it is not relabeled as a full-suite run of this
later correction.

### Replacement proof candidate

Campaign B is materialized at
`/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/allocation_campaigns/subset-proof-20260909-b/campaign.json`,
hash `aafcfe1b37d839361ef4a4d7213b6d1b0773b06428226016371889dc88371607`.
It retains the four-physical/two-logical-node, one-hour, eight-minute-per-child
budgets and the two still-unexecuted frozen child bundles. Its controller is
commit `6d0e1e536e8bf5995334b75014b548218e4cb84b`, source snapshot
`20ddd971afc3887a4f03d8db031105cdfcd3f8558a03670294bc8c2bb17ec86c`,
with scheduler identity `ac-aafcfe1b37d8`. A fresh one-node interactive
regression session, PBS `8814870`, has also been requested to test the complete
corrected candidate; the same per-candidate regression budget above applies.

Campaign B was submitted as PBS `8814875`, then also withdrawn while still Q.
A final cold-start review found that the parent asked the filesystem validator
to inspect `/tmp/exaserve` before any child had created that directory. The
parent now validates the existing declared `/tmp` mount and creates its own
private temporary directory there. This removes dependence on an earlier job
having warmed the node. The failure-gating test now asserts this bootstrap path.
No B child ran; both frozen child bundles still have PLANNED revision 0 and may
be referenced by the replacement campaign. No old campaign or output was reset
or deleted, and neither A nor B is qualification evidence.

### Corrected-candidate regression and cold bootstrap

On the validated one-node interactive session PBS `8814870`, head
`x4217c7s0b0n0`, `/tmp/exaserve` was actually absent. The corrected validator
accepted the declared `/tmp` mount and created/removed a private temporary
directory successfully, without requiring an existing ExaServe tree.

The complete corrected working-tree suite then passed: **1,773 tests,
27 warnings, 166.40 seconds, exit 0**, including all 72 controller tests and
the cold-bootstrap assertion. Environment setup was repeated inside the
allocation, and the session was released after testing. Log:
`artifacts/diagnostics/allocation_subset_20260909/pbs8814870/pytest.log`;
SHA-256 `8d8433751f38406a03360e4b3944ba49fd5257b3ab150da7c812cf8eea809dd3`.
The next immutable controller snapshot will include exactly this code. Native
subset qualification remains pending; neither withdrawn request is a substitute.

### Fully regression-tested proof candidate C

The corrected code is committed as `97e82838a7b0454b75b5a0abe500dbebbaa60855`.
Campaign C:
`/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/allocation_campaigns/subset-proof-20260909-c/campaign.json`;
campaign hash `dfc946575981150631f2e5bb059cfe84be1a960a06f3f40dbb4362a4bcc5c7bd`;
controller source hash
`7f7f864bcebcf5780f56a39cec8f9b12fd3437100207c60f0cd48cac915f6a08`.
Its scheduler identity is `ac-dfc946575981`. The physical/logical sizes,
deadlines and two still-fresh frozen children are unchanged. The 1,773-test
result and cold-bootstrap check above cover this controller's code.

### Proof C native execution

PBS `8814899` was submitted at 23:08 UTC on 2026-09-09 and entered R at
23:21:33 UTC. The approved start waiter confirmed native head
`x4711c1s0b0n0`; PBS reports exactly four physical capacity nodes:
`x4711c1s0b0n0`, `x4711c1s1b0n0`, `x4711c1s2b0n0`, and
`x4711c1s4b0n0`, with `AuroraGPT` and one-hour walltime. The two children
remain logical two-node runs, not exact-size physical two-node jobs.

The batch body sources `~/script/env_aurora` and invokes the sealed controller:
`python3 -m eval.cli allocation execute /lus/flare/projects/AuroraGPT/wenyiw/data/experiments/allocation_campaigns/subset-proof-20260909-c/campaign.json`.
Native output is directed to `/home/wenyiw/aurora_rayserver/tmp/`; campaign
state, `child-0/executor.log`, and `child-1/executor.log` were actively monitored
under the campaign directory through the terminal result.

The parent published SUCCEEDED at 23:26:22.603645 UTC. Native PBS subsequently
reached **F with explicit `Exit_status = 0`**, job name `ac-dfc946575981`,
`obittime = 23:26:55 UTC`, and `resources_used.walltime = 00:04:59` (about
0.332 physical node-hours; four node-hours were reserved). The sealed
`97e82838...` controller's `_validate_qualification` was then rerun read-only
against the exact `7f7f864b...` controller and `5d85794f...` child source
identities and returned **PASS**. `_require_pbs_zero_exit` independently
reconfirmed the exact native identity, terminal F, and zero exit.

Canonical report: `subset-proof-20260909-c/qualification.json` under the
allocation-campaign root above; SHA-256
`fe233da1cd9c5278c92d9da043deb66d2e572ac23eca937bf23b431da96eb34d`.
All sixteen referenced immutable input files (eight per child), both child
source snapshots, the controller snapshot, and the complete acceptance/evidence
bindings were revalidated without modifying them.

| Scenario | Canonical outcome | Generation | Elapsed | Replay evidence |
|---|---|---:|---:|---|
| Independent happy-path child | SUCCEEDED | 1788996154633383398 | 156.146s | Two complete replays; each 3,072 scheduled, 3,072 completed, zero errors |
| Independent cancellation child | CANCELLED_AFTER_READY | 1788996312031078850 | 70.254s | Identity-bound RUNNING/replaying plus canonical READY preceded the deliberate SIGINT; clean CANCELLED RunStatus, no `cleanup_error` key |

Both bindings use exactly `x4711c1s0b0n0` and `x4711c1s1b0n0`, preserving the
native parent head and PBS job ID. The original child SchedulerPlans were not
submitted. Exact binding hashes are
`36ddc43f5d208ae61dcf433aee707c91fb08d227829f06a2f3f4cc851a6ee292`
(happy path) and
`6a42e73269fdb906173f3b7c880e98a9b4419b9f8506bcd018b4dba9b415b737`
(cancellation). The happy-path ResultManifest hash is
`2f559060b528673b2ead09eee4a036c03a4012fff876b32d331866b1c64c66e4`.

For each child, both source-stage rank receipts verified node-local tmpfs
runtime/state placement, the exact generation and binding, and the frozen
source. Source manifest hashes are
`72e36e330256bd0df4df7f47c350494ac778d17247d4ef17444e959b85f1c7a4`
and
`8fe3963186b70ecb01e4d85848cd386ab3fc4893797e457a6763a35ca1a6487a`.
Both READY captures authenticate all **30/30 exact compatibility receipts**,
two-node Ray membership, four applications, two proxies, and 24 null replicas.
ReceiptManifest hashes are
`c53c8669173f018dafa36a9c008c9b8b2a0bd9323185a5be57f8586a95200600`
and
`2fa7ba2a1694888458da6f559edd9e52267231a646396676e497fe20de037e8b`.

Native MPI placement returned exactly the two logical hosts with exit zero.
The three-rank over-launch returned 127, empty stdout, and
`Cannot place all ranks on node list`; no extra hostname was emitted.
The excluded-node sentinel on `x4711c1s4b0n0`, PID `19242`, was witnessed
before child startup at sequence 0 and after each cleanup at sequences 158
and 230. All witnesses retain the same campaign token/host/PID, fresh timing,
and authenticated transcript prefixes. The sentinel survived both cleanups,
was then explicitly reaped, and its final transcript SHA-256 is
`93cbc4a876067df3d8c2a46ca14591a9c92732160b4b7c45a5cac780d6eaf20a`.

Both canonical shutdown reports record STOPPED, `clean=true`, no errors, and
`deadline_exhausted=false`. `audit-0/process_audit.json`,
`audit-1/process_audit.json`, and `sentinel-reaped/process_audit.json` each
record all four physical hosts, exit zero, and no remaining owned PIDs; the
last audit includes both independent generation namespaces and the sentinel
token. Thus process-group exit was not used as a substitute for remote cleanup
evidence. Ray metrics-exporter connection warnings and expected shutdown
signals appeared in diagnostics; neither produced a canonical failure. The
cancellation traceback is the deliberate KeyboardInterrupt, not a hidden pass.

This proof qualifies only this controller/child-source acquisition boundary.
It does not qualify a general reservation service, establish large-node serving
results, or convert the null-compute runs into paper throughput samples.

## Current evidence

### Qualified production acquisition intent

The finite middle-scale campaign is now materialized at
`/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/allocation_campaigns/pp405b-middle-20260909-a/campaign.json`.
Campaign hash `524195253ba049155eca5614560f7efd24469e842f5798f3518b9af049cd0296`;
scheduler identity `ac-524195253ba0`. It requests 256 physical production
nodes for six hours and executes the existing `run11/n32`, `run11/n64`, and
`run11/n128` children sequentially, with a 90-minute child deadline and separate
cleanup reserves. All three were still PLANNED revision 0 at preflight.

Materialization explicitly selected the sealed, qualified `97e8283` controller
snapshot rather than current documentation HEAD. Controller source remains
`7f7f864bcebcf5780f56a39cec8f9b12fd3437100207c60f0cd48cac915f6a08`; child
runtime source remains `5d85794f...`. The immutable qualification reference binds
proof C's full report SHA-256 `fe233da1cd9c5278c92d9da043deb66d2e572ac23eca937bf23b431da96eb34d`.
Each child keeps its original source, workload, deployment and exact logical
node count; its standalone debug-scaling SchedulerPlan is not submitted.
Physical allocation size and actual queue/walltime are captured separately.

Canonical submission succeeded as PBS **`8814939`** on 2026-09-09, using
`python3 -m eval.cli allocation submit <campaign>/campaign.json`. A dedicated
Aurora start monitor uses the seven-day production wait bound, followed by
active parent/child-state and log monitoring. A queued parent is not an accepted
paper measurement. No child standalone PBS request is submitted alongside it.

- Frozen launcher and eval executor support direct in-allocation execution and
  exact explicit nodefiles; native PBS/PALS identity can remain unchanged.
- Existing exact-count AllocationBinding validation remains mandatory.
- Shared scheduler rendering/submission and exclusive leases are available;
  no alternate shell lifecycle is needed.
- The reviewed controller and CLI passed correctness lint and 67 targeted
  hermetic tests (0.75 seconds). Tests cover exact subsets, environment isolation,
  ambiguous/duplicate submissions, failure gating, cancellation/lease guards,
  strict native PBS completion, sentinel identity/freshness, and contract budgets.
- Full regression passed on interactive PBS `8814817`, exact node
  `x4311c4s3b0n0`, after the approved `srundbg` fallback and fresh `env_aurora`
  setup: **1,768 passed, 27 warnings, 165.99 seconds**, exit 0. The session was
  validated against its actual PBS job/nodefile before execution and released
  after the test. Pytest used a fresh short `/tmp/xs-regression.*` base directory.
  Log: `artifacts/diagnostics/allocation_subset_20260909/pbs8814817/pytest.log`;
  SHA-256 `3cfe1c01763878b8950aeac9aa751a2bb8a2830a59f2c13218132dca2c9818f2`.
- Read-only validation of the already accepted large-message two-node canary
  passed through the new controller's exact child acceptance boundary. This
  checks integration with real artifact schemas; it does not qualify subsets.
- Review corrected an evidence-field mismatch, ambiguous-node acceptance,
  insufficient qualification evidence binding, native missing-exit-status
  ambiguity, submission/publication and ownership races, late cancellation,
  incomplete cleanup budgeting, and sentinel startup/survival proof gaps before
  any parent compute experiment was submitted.
- Proof C's native isolation gate and sealed-source qualification validation
  passed as recorded above. Production acquisition and paper measurements
  remain separate, independently recorded work.
