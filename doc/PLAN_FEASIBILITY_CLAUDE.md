# Advisory Feasibility Review of the Production Hardening Plan

**Date:** 2026-08-05

**Reviewed document:** `doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md`

**Authority:** Advisory only. This file sizes and challenges the program; it is
not an implementation specification. `AGENTS.md` governs execution safety and
site workflow. `doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md` is the sole
canonical architecture, ordering, acceptance, fallback, and completion spec.
If this memo conflicts with either, the higher-authority document wins.

## Method and limits

This review checked plan consistency against the corrected audit, historical
Claude cross-verification, repository layout, selected source/test counts, and
current Aurora workflow instructions. It did not re-prove every defect, execute
Ray/MPI/GPU workloads, verify offsite access, or establish allocation lead time.
Code regions and dependency versions must be relocated and discovered again at
WP0; audit line numbers and historical environment versions are not timeless.

## Grounding facts checked on 2026-08-05

| Fact | Checked value and interpretation |
|---|---|
| `src/exaserve` Python | 21,485 selected lines, including 10,945 lines under the vendored Ray Serve overlay; a sizing count, not all first-party logic |
| `eval` Python | 13,919 lines across all Python files, not solely the “control plane” |
| ClientLab Python | 2,627 lines across all Python files |
| Tests | 10 `test_*.py` files and 36 collected tests in the adjudicated baseline; 24–25 passed depending on an undeclared host `rg`, with other recorded failures |
| CI | No `.github` workflow files in the checked tree. A GitHub remote exists, but that does not prove GitHub Actions is enabled, authorized, or the selected CI provider |
| Aurora acquisition | Reuse a valid compute session, then prefer `subjob` only when an eligible source allocation already contains enough free nodes; otherwise use the approved interactive fallback |
| Aurora queues | `capacity`: 1–16 nodes; `debug-scaling`: 2–256 nodes and at most one hour; `prod`: 256+ nodes with longer runs. Historical queue delay/job-count observations are not policy |

The source counts are only rough sizing inputs. They do not justify estimating
architecture work by lines changed, and they do not include all shell, C,
configuration, packaging, operational, or external-validation work.

## Corrected verdict

The target architecture is technically plausible for a **bounded first-release
envelope**, but feasibility is conditional on the mandatory WP0 spikes and
early one-/two-node proofs. The plan is a controlled closure program, not a
short patch and not necessarily one branch or PR. Its end state is one
architecture; its milestones are safety checkpoints rather than permission to
ship a partially migrated system as production.

Three matters remain deliberately unresolved until evidence exists:

1. the supported single-Ray-cluster scale ceiling or replacement topology;
2. the exact compatibility delivery mechanism for each patch/role; and
3. which external scheduler/vendor/engine combinations can actually be tested.

If a capability is marked `UNSUPPORTED`, that closes only the corresponding
production claim. It does not prove that capability, and it cannot close a
defect in a configuration that remains advertised as supported.

## 1. What is genuinely feasibility-positive

1. Stable contracts isolate evidence-selected fallbacks. A less elegant
   internal adapter need not leak a second lifecycle, readiness, or patch policy.
2. The plan permits `ACCEPTED_LIMIT`, `UNSUPPORTED`, `EXTERNAL_BLOCKER`, and
   `OUT_OF_PRODUCTION_SCOPE` outcomes with support impact, evidence, and revisit
   criteria. The worker cannot self-approve an accepted limit, and out-of-scope
   cannot be used for a production defect or still-advertised capability.
3. Existing staging, atomic-publication, scheduler, proxy, and empirical
   findings work provides useful implementation material, although each part
   still has to pass the new contracts.
4. Early compute proofs now precede schema freeze, so process, readiness, and
   compatibility assumptions are not deferred until final qualification.
5. The final release gate removes migration switches and duplicate control
   paths, directly addressing the largest program risk: stopping with two
   architectures.

## 2. Effort and calendar model

Claude's prior **20–32 focused engineer-week** total did not match the upper
bounds in its own table (approximately 20–33.5) and did not price the S00–S03
spikes, early compute, authenticated distributed channel, per-rank watchdogs,
or a possible scale-topology change. Therefore that number is an optimistic
lower-order estimate, not a commitment.

Use the following only to prioritize discovery; re-estimate after P00:

| Area | Preliminary effort character | Principal uncertainty |
|---|---|---|
| WP0 + S00–S03 | Non-mechanical, potentially several focused weeks plus queue time | Scale envelope, direct deployment lifecycle, transport, actual versions, spawned-engine reach |
| WP1–WP2 | Medium | Three consumers, compatibility adapters, filesystem-specific crash semantics |
| WP3 | High | Complete patch inventory and role/lifecycle delivery; no fallback is selected yet |
| WP4–WP5 | High | Outer supervisor, rank agents, authenticated transport, failure propagation, linear readiness |
| WP6–WP8 | Medium-to-high | Distributed publication, gateway capability/security, scheduler idempotency |
| WP9–WP11 | High/broad | Live evaluation compatibility, operational policy, packaging, hermetic and fault tests, CI access |
| WP12 | Calendar-dominant and TBD | Source allocations, queue/walltime, node-hours, retries, offsite access |
| WP13 | Medium if migration discipline holds; high otherwise | Consumer sweep, legacy deletion, full clean-artifact qualification |

No credible calendar estimate exists until WP0 records owners/availability and
WP12 records a resource row for each gate. For planning only, if the original
20–33.5-week lower-order estimate survived WP0 unchanged, one maintainer at
roughly 50% availability would already imply about 9–16 months before allocation
delays; one dedicated maintainer would imply roughly 5–8 months. Distributed
rework or a new scale topology increases those ranges. Multiple contributors
do not make whole WPs parallel: only ledger slices with frozen shared contracts
may overlap.

## 3. Corrections to Claude's original feasibility recommendations

### a. “No minimum cut” was the wrong completion framing

The program may use milestones, but only section 9 of the canonical plan defines
production completion. A short safety milestone can yield a more truthful
research artifact; it cannot be relabeled a production release while legacy
control paths, unsupported security, partial validation, or open claimed-
configuration defects remain.

### b. Branch/PR and paper-freeze assumptions were unauthorized

No SC26 data-freeze date or permission to create branches, commits, PRs, or write
to `main` was established. The canonical plan correctly keeps the existing eval
path and result schema as the migration default until the user confirms the
campaign boundary. WP1, WP4, WP8, and WP9 still affect the live evaluation
instrument, so adapters, two-state tests, parity evidence, and a registered
cutover gate are mandatory. Source-control operations remain a separate user
authorization decision.

### c. WP3 cannot be pre-decided as an overlay

It was unsupported to predict that Aurora must land on “fallback 3” or a
“matrix of one.” WP0 must inventory every patch and actual runtime role/version,
then test public configuration/upstream behavior, immutable exact-version
wheel/environment, generated exact-hash overlay, and narrow guarded runtime
adapter in order. A generated full-file overlay can be a valid evidence-selected
mechanism; editing the installed package in place cannot. One
`CompatibilityActivator` and profile/receipt contract unifies consumer behavior
even if different declared patches require different delivery internals.

### d. Readiness scalability is necessary but not sufficient

ExaServe's observation/coordinator path must be linear in planned components and
events, using push-on-change, bounded heartbeat, indexed counts, per-node fan-in,
replica registrations, and per-model/route canaries. That prevents ExaServe from
adding another `nodes * replicas` polling cliff. It does **not** eliminate Ray
Serve's documented internal control-plane traffic. S00 must separately bound or
replace the single-control-plane topology before a high-scale claim is made.

### e. WP12 logical gates cannot be collapsed into “256-node batteries”

Scenario sharing may reduce allocation overhead, but 16/64/128/256 remain
independent logical gates with fresh generations, reset boundaries, artifacts,
and verdicts. A larger physical allocation may host a smaller logical gate only
after subset isolation is proven and both sizes are recorded. `subjob` cannot
create nodes outside its source allocation. Queue, walltime/lease TTL,
node-hours, expected duration, retry policy, and output path must be budgeted per
gate before a final campaign; calendar cost remains TBD until then.

### f. External combinations are access-dependent, not predetermined failures

Delta MI100/A100 or other candidates require a bounded access/account/runtime
probe. An unavailable Slurm+ROCm combination may be marked unsupported with
evidence, but it says nothing about Slurm+CUDA, another ROCm site, or another
engine axis. External work is removed from the release critical path only by
removing the corresponding production claim, not by assuming the result.

### g. Evidence artifacts are not optional program-office overhead

The baseline, ledger, four decision ADRs, migration log, compatibility matrix,
receipts, experiment manifests, and final audit make a multi-month architectural
migration reviewable and prevent silent fallback. Automate generation and link
large raw logs rather than duplicating them, but do not discard the artifacts.
ADRs are mandatory for S00–S03 and for later architectural/fallback decisions,
including a preferred option that succeeds. CI artifacts may supplement, but do
not replace, a stable verdict and provenance record.

### h. CI must be provider-neutral until access is verified

WP11 needs portable clean-wheel unit/component/static gates. Aurora and offsite
compute remain WP12 gates. GitHub hosting alone does not authorize or configure
GitHub Actions; WP0 must verify the available CI provider, permissions, secrets,
and runner constraints before choosing workflow files. Tests must not depend on
undeclared host tools, live services, plugin order, or unrecorded random seeds.

### i. Unsupported dependency-maintenance claims were removed

The previous memo's “maintenance-mode,” bus-factor, and related dependency
claims were not established by the repository evidence used for this review.
The valid concern is ordinary version drift: profiles and receipts make it fail
loudly, and upgrade tests gate new versions. Specific upstream health claims
require current primary-source verification before influencing architecture.

## 4. Planning progress views, not an execution sequence

The canonical P00–P06 packets alone sequence the work. These milestones are
reporting views across packet boundaries; they neither reorder dependencies nor
create optional stopping scopes. Only Milestone 3 may satisfy the production
definition of done.

| Milestone | Outcome | What it does **not** claim |
|---|---|---|
| 1 — Decision foundation | P00 complete: portable baseline/ledger, S00–S03 coherent ADRs, and all mandatory WP0-early compute evidence | Does not claim that production implementation, packaging, or scale qualification is complete |
| 2 — Migrated implementation | P01–P05 complete in their canonical order: shared contracts and all WP1–WP11 implementation/release gates pass, with registered switches and parity evidence | Does not establish the claimed support matrix or permit permanent dual architecture/cutover before P06 |
| 3 — Production closure | P06 complete: final WP12 clean-artifact qualification, WP13 cutover/legacy deletion, support matrix, and closed ledger | Nothing less is the canonical production completion state |

Milestone status belongs in `FINDINGS.yaml`; it must not weaken an acceptance
test or silently move a production defect to `OUT_OF_PRODUCTION_SCOPE`.

## 5. Definition-of-done feasibility

The canonical definition of done is internally consistent, but its achievability
is conditional:

- S00 may propose a lower first-release scale envelope, subject to the canonical
  user/product-owner approval rule, or establish that a new topology must be
  separately designed and estimated.
- S01–S03 and early compute may reject an otherwise attractive library, default
  TCP/framing choice, readiness adapter, or patch delivery choice; an
  authenticated structured cross-rank channel remains mandatory.
- Aurora scale gates depend on available source allocations/queues and explicit
  node-hour budgets.
- External platform claims depend on access; absent evidence means the claim is
  blocked, not validated.
- The repository-wide marker-consumer inventory in WP0/WP13 is the required
  scope. No unnamed external “babysitter” consumer is assumed; if one is later
  identified, it must receive an owner and disposition.

These conditions use the roadblock protocol; they do not justify silently
relaxing release criteria.

## 6. Worker instruction

Claude Code must use this file only to understand feasibility risk. For all
implementation decisions, it must follow, in order:

1. `AGENTS.md` for site safety, permissions, and compute execution;
2. `doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md` for architecture, packets,
   mandatory spikes, acceptance tests, fallback adjudication, and completion;
3. `doc/hardening/FINDINGS.yaml` after WP0 for current item ownership/status and
   evidence, without allowing it to weaken the plan.

Claude must not preselect a fallback, delete a logical validation gate, write to
`main`, create commits/PRs, launch jobs, or cut over the live evaluation path
unless the canonical plan's gate is satisfied and the active user authorization
covers the operation. Rebaseline effort and calendar after P00 rather than
treating this memo's historical estimate as a schedule.
