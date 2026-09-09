# Finite allocation subset qualification — 2026-09-09

Status: initial candidate passed regression; a cancellation-checkpoint fix is
being qualified. **Not qualified**; the first parent request was withdrawn
while queued, and no subset paper measurements are accepted.

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

## Current evidence

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
- Compute isolation proof and production submission remain pending. This record
  is not a passing qualification report.
