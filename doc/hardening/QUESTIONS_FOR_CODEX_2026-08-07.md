# Resolved cutover questions for Claude Code

**Status:** Architecture questions resolved on 2026-08-07. One product-scope
approval remains open: a named product owner must durably approve the proposed
64-node first-release ceiling before ADR-000/support-ledger scope freezes.
“Resolved architecture” does not mean the strengthened P00 proof gates have
passed; the older spikes are partial evidence and must be completed below.

**Authority:** The binding specification is
`doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md`, especially §3.2.1, “Binding
cutover resolutions.” This document is a non-normative worker index and
historical answer to Claude Code's questions. It does not define a competing
work order. If wording ever differs, the execution plan wins.

Claude Code follows the corresponding canonical plan packets and uses the
summaries below only to locate the resolved decisions. It must not select one of
the old A/B alternatives, retain a legacy path because it is smaller, wait for
a 256-node allocation before lower-scale correctness work, guess production
timeout values, or fabricate product-owner approval.

## Q1 — IMP-H03: shell preflight versus lifecycle

### Decision

Use the thin site-adapter end state, with one correction to Claude's option A:
the shell may prepare Copper-related modules/environment, but it may not start
and later stop a Copper service.

The shell may only:

1. select/load the site interpreter and unavoidable modules;
2. discover and validate the existing allocation/nodefile;
3. sanitize inherited environment and resolve the clean packaged ExaServe
   artifact; and
4. end in one final `exec` of the import-light `exaserve.launcher` composition
   root.

Move plan creation out of the shell into the canonical pre-submission compiler
or, only for a direct/manual legacy launch, the one-way composition-root
adapter. The outer `RuntimeSupervisor` consumes the resulting typed plan; it
must not create, reload, or reinterpret that plan. Move
model/source/runtime/overlay staging, MPI/native distribution orchestration,
persistent Copper lifecycle, component logs, result collection, readiness,
terminal status, and cleanup into the outer supervisor's ownership.

Do not port working MPI/native algorithms line by line into Python. Keep them as
finite supervised subprocesses behind argv, deadline, exit-status, and
validated result-manifest contracts.

### Bootstrap

Run the import-light composition root from a clean immutable wheel/package on
the shared filesystem or site environment. The normal scheduler/eval path
compiles and persists RunPlan/DeploymentPlan before submission; the
in-allocation root loads and verifies that exact artifact/hash. A direct/manual
legacy launch may instead invoke the one-way compiler once. It activates
compatibility, constructs `RuntimeSupervisor(plan)` in the same process, and
passes only typed plan objects. The supervisor never reloads, recompiles, or
reinterprets YAML. It then stages
content-addressed artifacts for ranks and launches ranks from those verified
artifacts. It must not import itself from the node-local directory it is about
to create; no second staged supervisor copy is required.

### Required proof

- The shell has no lifecycle child or cleanup path after its final Python exec,
  staging decision, or readiness decision.
- It reaches Python through exactly one final `exec`.
- Native success requires zero exit and all planned per-rank result receipts.
- Any failure yields a typed first cause, bounded cleanup, and nonzero exit.

## Q2 — IMP-B04: receipt cardinality and transport

### Decision

Keep one compatibility receipt per exact planned process/actor instance. A
per-rank batch is allowed only as transport packaging; one aggregate assertion
or a matching count is not valid evidence.

The head verifies exact planned receipt-requirement equality, including role,
logical component/replica slot, ownership or placement constraint, and current
instance. The semantic DeploymentPlan may pin a stable rank but never an
allocation hostname. After nodefile resolution, a generation-scoped immutable
`AllocationBinding` binds rank to node without changing the plan hash; dynamic
component registrations bind allowed actor/process instances to one unfilled
planned slot. Matching only the total count is invalid. Duplicates, stale
instances, one receipt representing multiple slots, or one missing requirement
fail readiness. Managed Python roles self-report. A supervisor may attest only
an individually identified unmodified daemon that it owns.

Implement the exact `CompatibilityReceipt` version-2 field names, types,
hashing rule, `SELF|SUPERVISOR` restriction, and
`APPLIED|NOT_REQUIRED|FAILED` semantics in canonical plan §3.2.1. Do not invent
an alternate role-only or permissive compatibility form.

All remote/rank-owned authoritative receipt payloads flow through the
authenticated §3.2 control channel. A rank session may submit only `RANK`
receipts matching its authenticated rank/node binding. `GLOBAL` receipts enter
only from the local in-process `RuntimeSupervisor`/composition-root authority,
with `owner_rank: null` and the allocation-head `node_id`, through the same
validator/global writer; this covers the supervisor's SELF receipt and its
attestation of an owned global daemon. Detached Ray actors, stdout, shared
files, and node-local files cannot be readiness authorities. A bounded local
IPC handoff may deliver the exact rank-owned receipt to a `NodeSupervisor`, but
that supervisor must forward it over the channel unchanged.

Use:

- `OBSERVATION` with an optional exact receipt for incremental state; and
- a bounded `SNAPSHOT` containing observations and individual receipts for
  initial registration and reconnect.

If a complete snapshot exceeds the frame limit, use the exact bounded chunking
contract in canonical plan §3.2.1: payload version exactly 1; zero-based
contiguous chunks; canonical complete-set SHA-256; one in-flight snapshot;
maximum chunks/bytes/items; one latest observation and one receipt per exact
item key; byte-identical duplicate collapse; and fatal conflicting duplicates.
On acceptance the snapshot replaces all prior rank-owned projection state,
tombstoning absent items. The head then sends the normal version-1
`COMMAND(operation=SNAPSHOT_ACCEPTED)` with stable `command_id`, snapshot
ID/hash, and requires its matching idempotent successful `COMMAND_RESULT`.
REGISTER/START, readiness, and reconnect incrementals remain blocked until that
round trip completes.

### Scale correction

The technically selected first-release candidate is 64 nodes, with a final
ladder of 1 → 2 → 4 → 16 → 64. Existing 1/2/16/64 results are early or
feasibility evidence, not final target-architecture qualification. Because
prior material discussed 256-node production behavior, excluding 128/256 from
the release claim still requires explicit, durably linked product-owner scope
approval. Claude cannot infer or author it. This pending approval does not
weaken exact receipting or block lower-scale implementation; synthetic tests
must exercise 256-equivalent or otherwise high-cardinality receipt sets without
requiring hardware.

Do not reclassify a 128/256 ledger record merely by assuming that approval. Once
the 64 scope is approved, behavior claimed at ≤64 is proven on the final
architecture at the required lower tiers; a record concerning only an excluded
larger/streaming combination is compile-time rejected and adjudicated as
`OUT_OF_PRODUCTION_SCOPE` when genuinely optional/never advertised or
`UNSUPPORTED` when rejected because of a demonstrated defect. Preserve
historical evidence, support impact, and S00-expansion revisit condition; never
relabel the mechanism `FIXED` merely by narrowing the envelope.

### Exact P00/scope-gate semantics

The pending product decision is not the only current P00 gap. Treat the old
“P00 GATE CLOSED” migration-log entry as superseded: S01 used a placeholder
instead of a real Ray child and the old immediate-kill watchdog; S02 probed
direct Serve endpoints without the global HAProxy advertised path and complete
negative matrix; S03 has not executed the per-patch wheel → generated-overlay →
guarded-runtime ladder or the full two-node exact-receipt failure proof. Close
those technical gates and correct ADR-001/002/003 evidence before calling P00
technically passed.

If every technical P00 gate passes and only the product-owner scale decision is
missing, record `TECHNICAL_PASS_SCOPE_PENDING`. That status permits P01-P05 to
freeze only ceiling-independent mechanics: generic schemas/interfaces,
explicit scope fields/validators, shared plan production, lifecycle/readiness,
and lower-scale correctness. Candidate-64 test fixtures must say `candidate`.
Record this marker in ADR-000 and the migration log; it is a P00 program-gate
marker, not a legal `FINDINGS.yaml` status.
It does **not** permit Claude to default/publish a production
`qualification_target`, freeze a value-bearing release SiteProfile, close or
reclassify 128/256 findings, publish support, or start P06. Those require the
durable approval block and a full P00 `PASS`. If the owner selects the larger
scope, reopen S00 and affected assumptions without discarding generic work.

### Required proof

- Exact-set reconciliation detects duplicate-plus-missing identities even when
  the total count matches.
- Old role-only receipts fail closed after the schema-version bump.
- Chunked snapshots are atomic and bounded; incomplete/mixed snapshots fail.
- Reconnect accepts no incrementals before its complete snapshot.
- Coordinator work and message count are linear in plan/event cardinality.

## Q3 — IMP-B02: gateway startup and readiness

### Decision

Neither old option A nor B is the target:

- Rank zero must not own the gateway.
- Do not add a public deployment-level `SERVE_READY` state.
- The allocation-head `RuntimeSupervisor` owns the gateway as a `GLOBAL`
  component.

The sequence is:

1. deploy Serve and collect typed current-generation route/target/replica
   evidence;
2. enter `VALIDATING` and establish the compiled advertised endpoint: start
   global HAProxy for production, or publish the typed Serve endpoint only for
   an explicit validation-direct plan with no gateway process;
3. verify the declared endpoint, HAProxy process/health/routes when applicable,
   exact receipts, and per-model inference canaries through that endpoint;
4. atomically persist the one deployment `READY` transition.

Internal Serve health is only an input to `VALIDATING`. After READY, an
unexpected gateway-process exit revokes readiness and transitions immediately
to terminal `FAILED`, followed by bounded cleanup and a nonzero
scheduler-visible result. If the gateway process is still alive but its health,
route, or canary check fails, transition `READY -> VALIDATING`, revoke the
persisted READY record, and attempt bounded recovery. Successful revalidation
may atomically persist READY again for the same generation; expiry of the
recovery deadline transitions to `FAILED`, followed by bounded cleanup and a
nonzero result. Do not invent `DEGRADED` behavior in this pass.

### Direct mode

HAProxy is the only first-release production gateway. Legacy
`proxy_config.type: none` is validation/benchmark direct exposure, not a
production default. Keep the schemas separate: `GatewayPlan.kind` names a real
managed gateway and has no `none/direct`; `ExposurePlan.mode` carries
`PROXIED_INTERNAL` or `DIRECT_VALIDATION`. Production requires HAProxy +
PROXIED_INTERNAL. DIRECT_VALIDATION requires `gateway: null` and
`validation_mode: true`; every other combination fails compilation. A
validation direct plan must explicitly declare the Serve
endpoint as its canonical advertised endpoint and still prove routes, canaries,
security/bind policy, request limits, and revocation. Eval `dest=direct` changes
client routing only and cannot redefine deployment exposure.

Production direct exposure requires a future S00/ADR-000 envelope expansion and
separate WP7/WP12 evidence. Until then, the production compiler must reject it
outside explicit validation mode.

### Required proof

- Rank-owned observations cannot claim `GLOBAL` gateway ownership.
- No internal health response, marker, or eval routing flag advances READY.
- Canary traffic uses exactly the compiled advertised endpoint.
- Missing/unhealthy/dead gateway blocks or revokes production READY.

## Q4 — IMP-B06: fail-closed registration and reconnect

### Decision

Separate three phases:

1. **Listener bind:** bounded transient retry is allowed before rank launch;
   ultimate failure is immediately terminal and launches no rank.
2. **Initial registration:** all planned ranks must authenticate and publish
   their supervisor receipt within a bounded deadline. A rank may run enough of
   `NodeSupervisor` to register, but no rank starts Ray or another long-lived
   child before every registration is accepted and the head sends START.
3. **Established-session reconnect:** readiness is revoked immediately on
   connection/lease loss. The same planned identity may recover within a
   bounded grace period only after a complete current-generation snapshot.

Missing initial registration at its deadline or reconnect-grace expiry makes
the generation terminal, terminates the launcher, and returns nonzero. A later
registration/reconnect cannot resurrect that generation. Bad-MAC or otherwise
unauthenticated pre-registration traffic closes/audits only that session while
the expected-rank deadline continues; it cannot trivially abort the allocation.
After an expected rank authenticates, its deterministic identity/schema/
sequence violation is rank-fatal and therefore generation-fatal. REGISTER alone
does not count: the initial complete snapshot and supervisor receipt must be
accepted before `all_registered` or START.

EOF/reset sets `loss_time` immediately; silent loss sets it at last accepted
heartbeat arrival plus `lease_timeout_s`. Readiness is revoked at `loss_time`,
reconnect grace ends at `loss_time + reconnect_grace_s`, and node-local cleanup
starts only after grace expiry and completes within
`watchdog_cleanup_deadline_s`. Do not double-count lease plus grace. A GOODBYE
from the long-lived `NodeSupervisor` is normal only after acknowledged
DRAIN/STOP; completion of one finite child does not authorize the rank session
to end. An unsolicited GOODBYE from an active required rank is terminal loss.

### Deadline fields

Add these positive resolved plan/SiteProfile fields:

```text
registration_deadline_s
reconnect_grace_s
heartbeat_interval_s
lease_timeout_s
snapshot_assembly_deadline_s
watchdog_cleanup_deadline_s
max_frame_bytes
max_snapshot_chunks
max_snapshot_bytes
max_snapshot_items
```

Require `lease_timeout_s >= 3 * heartbeat_interval_s`. Environment variables
must not silently override the compiled values.

Do not invent a 256-node default. Implement the contract now with injectable
short test values. Measure and record production Aurora values during S01/WP12
at required 1/2/4/16/64 tiers; a SiteProfile is not production-qualified until
its values have evidence.

### Required proof

- no rank launch after listener failure;
- bad unauthenticated traffic is session-local, while an authenticated-rank
  deterministic violation is generation-fatal;
- delayed registration within deadline succeeds;
- one missing rank is terminal;
- no `all_registered`, START, or Ray child before every initial snapshot and
  supervisor receipt is accepted;
- disconnect immediately removes readiness;
- reconnect snapshot inside grace recovers;
- incremental-before-snapshot and reconnect-after-grace fail; and
- expected and unsolicited GOODBYE have distinct outcomes; and
- the exact loss/grace anchors trigger bounded node-local watchdog cleanup.

## Q5 — IMP-H01: canonical compiled plan

### Decision

Extend/refactor `src/exaserve/plan/` as the sole shared public contract. Do not
promote the eval-shaped `eval/lib/models.py::RunPlan` and do not create another
parallel contract.

Use this composition:

- Separate immutable, versioned, content-addressed `SiteProfile` artifact with
  a stable ID and canonical content hash. It contains site capabilities and
  defaults, not a particular allocation request.
- Immutable `SchedulerPlan` containing the per-run queue/account/node/walltime
  and reservation request, validated against SiteProfile capabilities.
- Immutable `DeploymentPlan` containing the SiteProfile ID/hash,
  compatibility/manifest hashes, models, gateway/exposure, topology/resources,
  receipt slots, and resolved control deadlines, but no allocation hostnames or
  queue/account request.
- Shared immutable `RunPlan` only for eval/ClientLab, containing that exact
  `DeploymentPlan`, `SchedulerPlan`, and semantic workload/trace/client/artifact
  policy. Core uses DeploymentPlan directly; serving-only launch does not build
  an empty-workload RunPlan.
- Separate generation-scoped `AllocationBinding` and live component-instance
  bindings for rank→hostname and process/actor identity.

Hashes are distinct: `site_profile_hash`, `deployment_plan_hash`,
`run_semantic_hash`, `allocation_binding_hash`, and `run_provenance_hash`.
Paths, timestamps, commands, allocation nodes, and output locations affect only
binding/provenance identity. A restart increments generation and creates a new
binding without silently changing semantic plan hashes.

Legacy core/eval schemas become one-way compile adapters during migration and
are removed at WP13. Every accepted runtime field must be represented or
rejected explicitly; unknown fields cannot disappear.

### Required proof

- CLI/core/eval/ClientLab derive byte-identical DeploymentPlan hashes from one
  serving input.
- Serving changes alter `deployment_plan_hash`; workload/scheduler semantic
  changes alter `run_semantic_hash`.
- Paths/timestamps/allocation changes alter only binding/provenance hashes.
- SiteProfile drift changes the bound DeploymentPlan hash.
- Direct YAML interpretation and mutable parallel plan construction are absent
  after cutover.

## ACCEPTED_LIMIT approval

Use exactly:

```yaml
approval:
  approver_id: <actual user/product-owner identity>
  approved_at: <RFC3339 UTC timestamp>
  evidence_ref: <stable repository path, issue/PR URL, or durable message ID>
  scope: <exact accepted limit and production envelope>
```

`owner` is not approval. Claude Code may propose an approval block but may not
fill or infer it. Name/date without linked affirmative evidence is invalid.
Changing the scope, support impact, or production envelope requires renewed
approval. Approval cannot waive a mandatory execution-plan invariant.

No valid block currently records approval of the proposed 64-node ceiling.
Claude must leave that scope gate pending and ask the actual product owner for
an explicit decision; it must not derive approval from a username, this file,
or its own prior prose.

## Plan §8 ledger fields

The exact full-record YAML types and conditional rules are in canonical plan
§3.2.1; the §8 semantic list is authoritative. Normalize every record to that
schema and run the required repository validator, then independently adjudicate
status:

- retain `FIXED`/`REPLACED` only with stable linked evidence proving the whole
  invariant on the production path;
- use `OPEN` when no implementation/proof is underway;
- use `IN_PROGRESS` when work or proof is underway; and
- reopen an unapproved `ACCEPTED_LIMIT` until approval or another valid
  disposition exists.

Do not reopen a genuinely proven fix merely because metadata was absent, but do
not add fabricated or unexplained `N/A` values to preserve a closure count.
`TD-CONSTS` must reopen because calling SiteProfile cosmetic contradicts the
mandatory plan; it cannot be preserved as an accepted limit by metadata alone.

## Known current-tree deltas — these are work, not design choices

Before claiming any resolution complete, relocate and close these current
behaviors on every reachable production path:

| Region | Current contradiction to the binding design |
|---|---|
| `src/exaserve/plan/schemas.py` | `none` is still a production gateway and the implicit default; the contract lacks the complete SiteProfile/SchedulerPlan/DeploymentPlan/RunPlan, hash-boundary, AllocationBinding, and control-limit family. |
| `src/exaserve/driver.py`, `src/exaserve/server.py`, `eval/lib/models.py` and eval planners | Production and eval still load/reconstruct parallel legacy/eval-shaped configuration rather than consuming one compiled plan identity. |
| `src/exaserve/compat/receipt.py` and `src/exaserve/compat/collector.py` | Receipts are role-level/incomplete and the detached Ray actor remains an authority. |
| `src/exaserve/control/transport.py` and `channel_runtime.py` | Snapshot payload/reassembly, mandatory reconnect snapshot, production heartbeat/lease behavior, and fail-closed registration are incomplete or not wired. |
| `src/exaserve/supervisor_main.py::run` | Listener failure still degrades to launcher-only operation instead of failing before launch. |
| `src/exaserve/launcher.py` | The supposed Python composition root still starts the lifecycle-owning shell as a child and does not compile/pass one typed plan. |
| `src/exaserve/rank_main.py` | Rank connection success is not a START gate; ranks immediately start Ray, rank zero owns deployment, and the outer supervisor owns no gateway. |
| `src/exaserve/resources/launch_cluster.sh` | The shell still owns staging/distribution, Copper lifecycle, collection, cleanup, and code after supervisor invocation. |
| readiness/gateway consumers in core/eval | Legacy marker/direct-Serve paths can still bypass the one global advertised-endpoint readiness predicate. |
| `doc/hardening/FINDINGS.yaml` | Records still require truthful §8 completion and evidence-based status readjudication; unapproved `ACCEPTED_LIMIT` is not closure. |

Do not treat an existing helper, schema class, or unit test as closure if the
default production path does not use it or uses it with weaker predicates.

## Claude Code: map the work to the canonical packet order

This is an index, not another sequence. Follow plan §4.3 P00 → P06 and all
packet exit gates:

1. **P00:** rebaseline; normalize/validate/re-adjudicate the ledger; bind the
   affected slices to IMP-H01, IMP-B04, IMP-B06, IMP-H03, IMP-B02, and IMP-B10;
   mark the historical P00 closure superseded; complete the real-Ray S01,
   global-HAProxy/negative-matrix S02, and per-patch-ladder/full-receipt S03
   proofs; retain the pending 64-node product-scope approval explicitly. Use
   `TECHNICAL_PASS_SCOPE_PENDING` only under the rule above.
2. **P01 (WP1+WP2):** implement SiteProfile/SchedulerPlan/DeploymentPlan/RunPlan,
   semantic hashes, AllocationBinding/runtime identity, adapters, and atomic
   persistence. Migrate every plan-producing/configuration path—including
   core, eval, and ClientLab planners/adapters—to import and construct the
   shared contracts now, and prove byte-identical DeploymentPlan hashes. Do not
   defer this compiler cutover or leave a second plan interpretation for P05.
3. **P02:** implement CompatibilityProfile plus exact v2 receipt schemas,
   managed SELF producers, owned-daemon SUPERVISOR producers, the local
   outer-supervisor ingress for GLOBAL receipts, and bounded node-local IPC
   ingress for rank-owned receipts. Hermetic/spawn proofs may pass, but do not
   close authenticated-delivery/readiness findings yet.
4. **P03 vertical gate:** implement the protocol/session state machine,
   bounded replacement snapshots, REGISTER/START/reconnect/GOODBYE semantics,
   and outer/node supervisor wiring; then carry rank-owned P02 receipts over the
   authenticated channel, inject GLOBAL receipts only through the in-process
   outer-supervisor authority, disable the Ray collector on the new path,
   reconcile exact instances, and drive revocable readiness. There is no
   closure checkpoint between “transport exists” and “production
   receipts/readiness consume it.”
5. **P04 (WP6-8):** move finite staging/log/result/Copper ownership under the
   supervisor, reduce the shell to the final adapter, implement the global
   HAProxy/advertised-endpoint sequence and security contract, and complete the
   shared scheduler backend.
6. **P05 (WP9-11):** migrate eval and ClientLab runtime
   status/supervision/scheduler/operations consumers onto the already-shared
   P01 plan identities, remove their temporary compile adapters, complete
   operations/metrics/error policy, and make packaged hermetic CI the gate.
7. **P06 (WP12-13):** only after lower gates, the required scope approval, and
   full P00 `PASS`, remove legacy marker/Ray-actor/shared-file/duplicate-plan
   paths and execute the clean packaged qualification ladder required by the
   approved envelope.

In every slice, update tests, ADRs, migration log, ledger decision/evidence,
fallback, residual risk, support impact, and revisit condition. Never mark a
slice closed merely because a primitive exists or a unit test bypasses the
production path.

The worker stops only under the execution plan's roadblock protocol: a
preferred option has a reproduced failure and all specified fallbacks are
tested, external authority/resources are missing, or a safety constraint
prevents progress. Numerical production deadlines and the pending product-
owner scale decision are explicit evidence/scope gates, not reasons to postpone
the current lower-scale architecture implementation.
