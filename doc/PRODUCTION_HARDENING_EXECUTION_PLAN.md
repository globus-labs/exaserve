# ExaServe Production Hardening: One-Pass Execution Plan

**Draft date:** 2026-08-04

**Last reconciled:** 2026-08-05 after Claude round-2 and feasibility-plan
adjudication

**Companion audit:** `doc/PRODUCTION_READINESS_AUDIT.md`

**Scope:** Core serving, cluster launch, readiness, compatibility patches,
model staging, gateways, schedulers, evaluation, ClientLab, observability,
packaging, tests, documentation, and supported-platform claims.

## 0. Authority, precedence, and worker contract

This document is the **canonical implementation specification** for the
production-hardening program. Claude Code and any other implementation worker
must follow it faithfully. The active user request controls authorized scope and
whether operational actions such as commits, external writes, or compute runs
may occur; it does not make an unstated architecture choice. Within that scope,
the documents have the following precedence:

1. `AGENTS.md` and the current site/HPC workflow instructions govern safety,
   permissions, allocation acquisition, and where commands may run.
2. This execution plan governs architecture, work ordering, acceptance gates,
   fallback selection, evidence, and the definition of completion.
3. After WP0 creates it, `doc/hardening/FINDINGS.yaml` is the authoritative
   status/evidence ledger. It may track disposition but may not weaken an
   invariant or gate in this plan.
4. `doc/PRODUCTION_READINESS_AUDIT.md` is the corrected finding/evidence
   register. `doc/PRODUCTION_READINESS_CLAUDE_AUDIT.md` is historical
   adjudication; `doc/KNOWN_ISSUES.md`, `doc/TODO.md`, and `findings/` are
   empirical/backlog inputs, not implementation-order instructions.
5. `doc/PLAN_FEASIBILITY_CLAUDE.md` is advisory sizing and program-risk
   commentary only. It cannot waive, reorder, narrow, or replace a requirement
   in this plan.

If two documents disagree, the higher-precedence document wins. The worker
must record the conflict in `doc/hardening/MIGRATION_LOG.md`; it must not
silently choose the easier interpretation. A safety rule always wins over an
implementation rule.
Audit line numbers are revision-local evidence hints: before editing, relocate
the named symbol or behavior in the current tree and record the resolved region.

“One pass” means one controlled closure program ending in one maintainable
production architecture. It does **not** mean one prompt, one giant diff, one
unreviewed branch, or permission to skip gates. Intermediate safety and data-
integrity milestones are checkpoints, not alternative definitions of
production completion. The pass is complete only under section 9.

The worker may implement and test local slices without asking for routine
design choices already resolved here. It must stop and use the roadblock
protocol when a mandatory spike fails, when the requested production envelope
would require an unplanned control-plane topology, or when external access or
authority is missing. It may not preselect a fallback merely because that
fallback appears easier or more likely.

## 1. Executive decision

Execute this as one controlled migration program, not as one giant code change.
It ends with one production architecture and is divided into ordered gates so
that each new contract is proven before the legacy path is removed. Whether the
checkpoints live in one authorized hardening branch, several authorized short-
lived branches/PRs, or an uncommitted worktree is an authorization/workflow
choice, not an architecture requirement.

The intended architecture is:

1. Keep subprocesses only at genuine operating-system boundaries: Ray daemons,
   external gateways, MPI/native staging tools, and scheduler launchers.
2. Replace log parsing as a control protocol with typed component state and a
   deployment readiness coordinator owned by ExaServe.
3. Replace ad hoc runtime patch injection with a versioned compatibility
   environment and a verified patch manifest. Runtime monkey patches remain a
   last-resort, explicitly bounded compatibility mechanism.
4. Compile user configuration into immutable, validated plans consumed by core
   serving, evaluation, and ClientLab.
5. Make state transitions and artifacts atomic, idempotent, observable, and
   safe to resume.

“Nothing unresolved” means every audit, Known Issue, and TODO entry must receive
one of these dispositions:

- **Fixed** — corrected and verified.
- **Replaced** — the affected design was removed by the new architecture.
- **Accepted limit** — bounded, documented, monitored, and explicitly approved
  by the named user/product owner for a stated production envelope. The worker
  may propose this disposition but may not approve it itself.
- **Unsupported** — rejected as a production claim and blocked by validation.
- **External blocker** — isolated with reproducer, evidence, owner, and a safe
  fallback. It cannot remain a silent open item.
- **Out of production scope** — a non-production experiment, paper-only task, or
  optional feature, recorded with reason, backlog owner, and revisit condition.
  This disposition cannot be used for a production-readiness defect or a
  capability still advertised by the release.

An item marked merely “TODO,” “partially tested,” or “works on Aurora” is not
closed.

Reducing a support envelope the user has already stated as required likewise
needs explicit user/product-owner approval. Until then, failed feasibility is a
release blocker for that envelope, not permission for the worker to redefine
the release silently.

## 2. Re-audit snapshot

The re-audit retains all 35 findings in the companion audit as the formal
closure register. It also brings the repository's Known Issues and TODO records
into the same program because several have drifted from the implementation.

The principal risk concentrations are:

- `src/exaserve/driver.py` and `src/exaserve/server.py` combine orchestration,
  subprocess management, deployment, readiness, persistence, and error policy.
- Core serving and `eval/lib/backends/ray.py` independently parse readiness log
  markers. `src/exaserve/proxy/litellm_proxy.py` has another marker-based
  readiness implementation.
- Compatibility behavior is split among the Ray Serve source overlay,
  `exaserve._sitecustomize`, a Ray worker setup hook, and a generated
  `sitecustomize.py` shim for spawned vLLM processes.
- Core, evaluation, and ClientLab each implement overlapping process, state,
  configuration, and artifact behavior.
- Scheduler logic is duplicated between `src/exaserve/schedulers/` and
  `eval/lib/schedulers/`.
- Model staging and result generation contain non-atomic writes, weak cache or
  completeness checks, and failure paths that can continue after partial work.
- Security and public exposure are not expressed as an explicit product
  boundary.
- At commit `005891e`, the suite collected 36 tests but had no portable baseline:
  25 passed / 11 failed with a real `rg` executable, while 24 passed / 12 failed
  without one. The delta is an undeclared host-tool dependency, not test-order
  or network flakiness. The common failures include live Hugging Face access, a
  src-layout subprocess import failure, stale proxy and scheduler expectations,
  and a ClientLab configuration failure. Closure is tracked by pytest node ID
  and root cause.
- There is no repository CI workflow, dependency lock, lint configuration, or
  type-checking gate sufficient for a production release.
- Aurora is the only credible current platform. Slurm, AMD/ROCm, SGLang, and
  very large scale behavior must not be advertised as production-supported
  until their own validation gates pass.

## 3. Target control architecture

```text
User config / API
       |
       v
Validated Plan Compiler ----> immutable DeploymentPlan / RunPlan / SiteProfile
       |                                      |
       v                                      v
SchedulerBackend                RuntimeSupervisor
                                (allocation-head process; owns the
                                 control listener, DeploymentManager,
                                 gateway, and one MPI/srun RankLauncher)
                                             |
                        one NodeSupervisor rank per planned node
                           /                                  \
                   rank 0 / head                         worker ranks
                   Ray head child                       Ray worker child
                   + local receipts                     + local receipts
                           \                                  /
                            typed ComponentObservation events
                                             |
                         RuntimeSupervisor / ReadinessCoordinator
                                             |
                            atomic DeploymentStatus + event log
                                             |
                              CLI / eval / ClientLab / operators
```

Before any Ray or engine import, a `CompatibilityActivator` selects and verifies
an immutable compatibility profile. All process roles publish a compatibility
receipt containing package versions, profile identity, patch hashes, and active
capabilities.

Process ownership is explicit and distributed. `RuntimeSupervisor` is the
canonical name of the one allocation-head coordinator: it binds the structured
control listener before rank launch, owns one `RankLauncher` managed component
for the MPI/srun process group, and owns the `DeploymentManager`, external
gateway, `ReadinessCoordinator`, and durable
global status writer. Every planned-node rank runs a `NodeSupervisor` that owns
only its node-local children; the rank-zero `NodeSupervisor` owns only the Ray
head child and other ranks own their Ray worker child. `RankLauncher` is an OS-
boundary component, not a second coordinator. The global supervisor never
pretends that it can signal or reap an unowned remote PID. Rank/process failure
reaches it through typed observations and must also propagate through the
MPI/srun exit contract.

The supervisors own cancellation, deadlines, child exit propagation, cleanup,
and durable state. Logs remain operational evidence; they are not the source of
truth for lifecycle decisions.

### 3.1 Normative control contracts

These are design constraints, not illustrative pseudocode. The module ownership
below is the default implementation layout. It may change only through the
governing ADR (ADR-001 for process/control ownership or ADR-003 for
compatibility ownership), with equivalent dependency direction and acceptance
evidence; consumers must see one versioned contract.

`ManagedComponent` has a stable identity and explicit `ComponentOwner`. The
owner is either `GLOBAL` with no rank (the outer supervisor's components) or
`RANK` with exactly one non-negative MPI rank (node-local components). Its
interface is `start(plan)`, `observe()`, `wait(deadline)`, and
`stop(reason, deadline)`.
`start` returns launch metadata rather than readiness; `stop` is idempotent; an
unexpected long-lived-component exit is fatal unless the resolved plan declares
a bounded degraded mode. Component states are exactly `NEW`, `STARTING`,
`RUNNING`, `READY`, `STOPPING`, `STOPPED`, `FAILED`, or `UNKNOWN`; component
`READY` is only an input to, never a synonym for, deployment READY. Deadlines
and lease expiry use a monotonic clock; wall-clock timestamps are evidence only.

Every lifecycle source emits a `ComponentObservation` containing at least:

```text
schema_version, deployment_id, plan_hash, generation, component_id,
instance_id, sequence, owner_scope, owner_rank?, role, node_id,
model_id?, replica_id?, state, observed_at, reason_code?, detail?,
compatibility_receipt_hash?
```

`owner_rank` must be absent/null for `GLOBAL` and present for `RANK`; the schema
rejects every other combination. A global component uses the allocation-head
node ID, while node-local identities must agree with their authenticated rank
registration. `component_id` is the stable planned logical identity;
`instance_id` is unique per start/restart. A new explicitly accepted instance
supersedes the old instance for that logical identity, and later observations
from the old instance are rejected.

Delivery is at least once. The deduplication key is `(deployment_id, generation,
component_id, instance_id, sequence)`. Unknown schema versions, wrong plan
hashes, stale generations/instances, and sequence regressions are rejected.
States are typed; arbitrary log strings are not converted into state. Each
component pushes on a state change and, only where liveness requires it, on a
bounded heartbeat. The receiver's monotonic clock determines arrival and lease
expiry; sender wall-clock time remains diagnostic evidence.

The `ReadinessCoordinator` maintains indexed counts and predicates from those
events. Its own ExaServe-added work must be linear in the number of planned
components and observations: no nested head polling of every replica through
every node/proxy is allowed. Node supervisors aggregate node-local process and
route observations; replicas register engine readiness once per state change;
external canaries run per model/route, while generation-scoped replica
registrations prove the full replica set. This does not solve Ray Serve's
internal quadratic control-plane behavior, which is handled by S00's supported-
scale decision.

`CompatibilityActivator` is the sole consumer-facing activation API. It
resolves one immutable `CompatibilityProfile`, prepares the child environment
before any Ray/engine import, and verifies `CompatibilityReceipt`s. Managed
Python roles self-report their runtime versions and capability/hash set.
Unmodified external daemons are attested by their owning supervisor (outer or
node-local) using the executable, arguments, prepared-environment hash, process
identity, and a version probe; they are not falsely described as self-reporting
ExaServe code.
Affected Serve actors, replicas, and spawned engine processes must self-report.
READY requires every role demanded by the resolved profile to have a matching
receipt.

### 3.2 Normative process topology and transport

The allocation-head `RuntimeSupervisor` is a normal Python process outside the
MPI/srun rank set. It is the only owner of global deployment state. Before
launching ranks, it binds an allocation-reachable TCP control listener on an
ephemeral port, creates a 256-bit per-deployment authentication secret, and then
starts one MPI/srun process group containing exactly one `NodeSupervisor` rank
per planned node for the first-release topology. Rank count must equal planned
node count and registered hostnames must be unique; a different mapping requires
a new topology ADR and scale proof. The endpoint, deployment/generation
identity, and secret are passed through a redacted inherited environment; they
must never appear in
commands, logs, receipts, status files, or committed artifacts. S01 must prove
that the selected launcher propagates this environment on Aurora. A
permission-restricted bootstrap file is an acceptable site-specific **secret-
delivery mechanism only** if the ADR proves cleanup, non-persistence in release
artifacts, and the same redaction invariant. It never replaces the live
structured control channel.

The control transport is independent of Ray and readiness-marker text. Its
default wire format is a versioned, length-prefixed canonical JSON envelope with
a strict maximum message size and an HMAC over the envelope. It carries only
these protocol families: `REGISTER`, `OBSERVATION`, `COMMAND`,
`COMMAND_RESULT`, `SNAPSHOT`, `HEARTBEAT`, and `GOODBYE`. Every command has a
stable `command_id` and idempotent result; every sender has a monotonic sequence.
On a bounded reconnect, a node registers again and sends a complete current-
generation snapshot before incremental observations resume. Unknown versions,
bad authentication, wrong deployment/plan hash/generation, rank/node disagreement,
oversized messages, and sequence regression fail closed and are audited.

This structured channel is mandatory even when MPI/srun propagates rank exits
well: it carries component state, receipts, readiness blockers, commands, and
the first causal failure. MPI/srun exit aggregation is a separate redundant
contract that makes failure visible to the scheduler and reaps the rank set. It
must not be the readiness or diagnostic protocol. Shared stdout, shared-file
polling, and Ray actors are forbidden as substitutes for this channel. If the
default TCP/framing choice fails the S01 proof because of a demonstrated site
constraint, ADR-001 may select another authenticated structured transport while
preserving the message contract and failure semantics.

The `RuntimeSupervisor` sends bounded start, drain, and stop commands. A
`NodeSupervisor` starts and reaps only its local children, publishes state on
change, and sends bounded heartbeats. Loss of a rank/control lease immediately
removes its components from readiness and becomes terminal after the declared
grace period. Loss of the global supervisor causes each node supervisor's
watchdog to drain/terminate local children by a fixed deadline, so an orphaned
allocation cannot continue serving indefinitely. Stdout/stderr are drained
asynchronously into component-scoped logs for operators; no parser of those
streams may advance lifecycle state. The implementation should use one event
loop and async subprocess/transport tasks; threads are allowed only for a
library boundary that cannot integrate with that loop and never for parsing a
control signal.

The startup sequence is fixed:

1. Compile and persist the immutable plan; activate the compatibility profile
   before importing Ray or an engine.
2. Bind the control listener and persist `CLUSTER_STARTING` without the secret.
3. Launch ranks and require an authenticated registration, node/rank identity,
   and supervisor receipt from every planned rank.
4. Start the rank-zero Ray head child with explicit endpoints, then start worker
   Ray children after the typed head endpoint is available. Process liveness is
   not Ray readiness.
5. Verify exact current-generation Ray membership/resources through the
   S02-selected adapter, then let the outer `DeploymentManager` connect and
   deploy Serve applications directly through its typed API.
6. Start the selected external gateway as a globally owned component, collect
   actor/replica/engine receipts and registrations, and evaluate the section 5
   readiness predicate.
7. Persist READY atomically before rendering any compatibility marker. On any
   failure, preserve the first cause, stop in reverse ownership order, and exit
   nonzero.

### 3.3 Normative code ownership

Claude Code must converge on these ownership boundaries rather than adding a
second orchestration layer:

| Module/region | Sole responsibility after cutover |
|---|---|
| `src/exaserve/control/contracts.py` | Lifecycle enums, observations, commands, errors, serialization, and schema-version validation; no Ray imports |
| `src/exaserve/control/transport.py` | Authenticated framing, connection/session handling, replay/snapshot, bounds, and redaction; no readiness policy |
| `src/exaserve/control/supervisor.py` | Allocation-head `RuntimeSupervisor`, global component registry, MPI/srun ownership, cancellation, first-cause and terminal-state policy |
| `src/exaserve/control/rank_launcher.py` | The one supervised MPI/srun OS-boundary component: rank command/environment construction, process group, exit aggregation, and bounded termination; no readiness policy |
| `src/exaserve/control/node_supervisor.py` | Rank entry point, local child ownership, local watchdog, receipts, observations, and command execution |
| `src/exaserve/control/readiness.py` | Pure indexed readiness projection/predicate over plan plus observations; no process launch or log reads |
| `src/exaserve/deployment.py` | Callable `DeploymentManager` for Serve prepare/deploy/observe/drain/stop; typed exceptions; lazy Ray/engine import only after activation; no CLI or scheduler submission |
| `src/exaserve/compat/{profile,activator,receipt}.py` | Compatibility selection, pre-import environment construction, manifest/capability verification, and receipt schemas |
| `src/exaserve/driver.py` | Thin adapter from compiled plan to the outer supervisor and final exit code; no marker parsing or duplicate child registry |
| `src/exaserve/server.py` | Serving/application/replica definitions consumed by `DeploymentManager`; no cluster launch, global persistence, or gateway ownership |
| `src/exaserve/resources/launch_cluster.sh` | Temporary allocation-head environment/preflight adapter that ends with one `exec` of the outer Python supervisor; the supervisor launches the rank entry point through MPI/srun; remove the shell at WP13 if it adds no required site boundary |

Core, eval, and ClientLab consume the same plan/status APIs and may not import
transport internals. Shell and Python compatibility-marker consumers are
removed in WP13; a marker producer may remain only as a rendering of already
persisted typed state.

The process-boundary decisions are likewise explicit:

| Boundary | Required production form | Lifecycle/readiness evidence |
|---|---|---|
| ExaServe CLI -> supervisor | In-process typed plan call | Return/typed exception and durable terminal status |
| Supervisor -> Ray head/worker daemons | One node-owned supervised subprocess per daemon; a public CLI is acceptable when no stable equivalent library lifecycle exists; argument vector, never shell text | PID/exit plus S02-selected Ray membership/resource observation; stdout is diagnostic only |
| Supervisor -> Serve deployment | Direct, lazy-imported `DeploymentManager` API by default | Typed deployment/replica/route observations; exactly one local IPC child is the proven fault-isolation fallback |
| Supervisor -> external gateway | Globally owned supervised subprocess | PID/exit, official health or real canary, route/capability check |
| Supervisor/node -> MPI/native staging tool | Finite supervised subprocess at the native boundary | Zero exit **and** validated rank receipts/result manifest |
| Core/eval -> scheduler | Shared `SchedulerBackend`; a scheduler CLI subprocess may exist only inside a backend boundary | Parsed typed job identity/state with fail-closed ambiguity |
| Eval/ClientLab -> deployment | Shared `DeploymentStatus`/event API | Generation-specific terminal/readiness state; no private process monitor or log grep |

Thus the migration does not try to eliminate subprocesses. It eliminates
subprocesses as an untyped internal control architecture and centralizes the
remaining real OS boundaries under one supervisor.

## 4. Working method and roadblock protocol

Every design uncertainty uses the same decision gate:

1. State the invariant that must hold, independently of an implementation.
2. Build the smallest isolated reproducer for the current failure or limitation.
3. Attempt the most elegant option against that reproducer.
4. Record results, including version, command, environment, logs, and artifacts.
5. If it fails, identify whether the cause is architectural, upstream, platform,
   packaging, or test-environment specific.
6. Test the next fallback through the same acceptance test; do not substitute
   opinion for evidence.
7. Select the simplest option that satisfies the invariant and operational
   constraints.
8. Record the choice as an ADR with rejected alternatives, compromise, scope,
   owner, and removal/revisit condition.

The fallback order is always **public/stable interface -> direct library
integration -> versioned build-time adaptation -> strictly verified runtime
adapter -> explicitly unsupported**. Silent catch-and-continue behavior is not
an allowed fallback.

Each work package must leave the main branch-equivalent state testable. The old
path may coexist temporarily behind a migration switch, but it is removed in the
cutover package. It cannot survive as an undocumented second architecture.

### 4.1 Claude Code execution contract

Claude Code should treat this section and each work-package exit gate as
execution instructions, not optional advice:

1. Read `AGENTS.md`, the production audit, the Claude cross-verification, Known
   Issues, TODO, and this plan before changing code.
2. Record `git status`, revision, environment, and every pre-existing dirty file.
   Preserve user changes; never reset, overwrite, delete, commit, push, or submit
   jobs unless the active request authorizes that operation.
3. Work on one ledger-owned slice at a time. Before editing, state its finding
   IDs, invariant, affected regions, acceptance tests, elegant target, and
   fallback ladder.
4. Write or isolate the acceptance test first. A test may initially reproduce a
   defect, but unrelated known failures must remain identified by node ID.
5. Where architecture is uncertain, run the smallest elegant-first spike and
   record its verdict before selecting a fallback.
6. Implement the selected option behind the stable contract. A fallback may be
   less elegant internally, but it must not leak a second lifecycle, readiness,
   configuration, or error protocol to consumers.
7. Run targeted tests and the cumulative login-node-safe suite after every
   slice. Test work starts in WP0; WP11 completes the global packaging/CI gate.
8. Update `doc/hardening/FINDINGS.yaml`, the relevant ADR,
   `doc/hardening/MIGRATION_LOG.md`, and evidence links in the same slice. Do
   not defer documentation until the end.
9. Advance only when the slice and work-package gates pass. A failed preferred
   option means continue down the recorded fallback ladder, not silently retain
   the legacy path.
10. Stop only for missing authority, unavailable external resources, or a safety
    restriction. Record an `EXTERNAL_BLOCKER` with reproducer and support impact.
    An external blocker cannot preserve a production claim: the affected
    combination is release-blocking or explicitly `UNSUPPORTED` and rejected by
    validation.

Every temporary migration switch is registered in
`doc/hardening/MIGRATION_LOG.md` with its owner, old and new paths, default
state, two-state tests, introduction slice, cutover gate, and mandatory WP13
removal. Until the user confirms an evaluation
campaign/data-freeze boundary, existing eval result schemas and the legacy path
remain the default while adapters and parity tests are introduced; WP1, WP4,
WP8, and WP9 must not silently cut the live paper instrument over early. This compatibility
rule does not permit marker parsing or unsafe behavior in the final release.

This plan authorizes neither git commits/branches/pushes/PRs nor writes to
external services. Those operations still require the active user's permission.
Logical checkpoints apply equally to an uncommitted worktree, an authorized
hardening branch, or separately authorized PRs.

For every slice, leave a compact handoff record:

```text
Finding IDs / invariant / owner WP
Files and contracts changed
Acceptance tests added or selected
Commands run and exact results
Preferred option and fallback attempts
Decision and ADR link
Ledger disposition and residual risk
Next unblocked slice
```

### 4.2 Mandatory design spikes before WP1 freezes contracts

These spikes are part of WP0. They use fakes or null-compute locally first.
When a verdict depends on real Ray, MPI, process spawning, or an accelerator,
the smallest one- or two-node proof runs during **WP0-Early Compute** using the
acquisition and safety procedure in WP12. It is not deferred until final
qualification. Large-scale and release-matrix runs remain in WP12.

If the required early allocation is unavailable, record an external blocker and
stop the dependent contract from freezing. Do not turn an untested hypothesis
into an architecture decision merely to keep coding.

P00 is an iterative co-design loop, not four independent spikes in filename
order. First draft S00's provisional envelope; then perform S03's full inventory
and build only the import-light minimal activator harness needed for experiments;
then prove S01's rank channel/topology; then run S02 and the full cross-role S03
proofs; finally reconcile all four ADRs and repeat any affected proof. None of
the four contracts freezes until their plan/profile hashes, role set, transport,
readiness inputs, and support envelope agree.

#### S00 — Initial production envelope and scale topology

Resolve the first release's scheduler/site, vendor, engine, gateway, exposure,
request mode, node count, and replica count as independent support dimensions.
Unless the user explicitly expands it, the provisional candidate is Aurora
PBS/Intel XPU with vLLM, a trusted allocation/internal-network boundary, and
HAProxy as the production gateway candidate. SGLang, public Internet exposure,
Slurm, CUDA, and ROCm remain gated claims. Streaming and every scale tier remain
outside the claimed support matrix pending their own capability/scale gates;
`UNSUPPORTED` is assigned only after a failed proof or documented blocker.

Decide whether 256-or-more-node service is a release requirement. A bounded
readiness coordinator does not remove Ray Serve's measured internal `R*N^2`
actor-handle traffic. If the required envelope exceeds the proven single-Ray-
cluster topology, test and select among a maintained upstream/patch-series fix,
sharded Ray clusters/control planes, another topology, or an explicit lower
supported ceiling. A Ray fork or sharded-control-plane program is new scope and
must be re-estimated before WP1; it is not hidden inside WP5.

Deliverable: `doc/hardening/decisions/ADR-000-production-envelope.md`.

#### S01 — Process boundary and supervisor

Evaluate direct in-process `DeploymentManager` use after compatibility
activation. Exercise cancellation, deadlines, SIGINT/SIGTERM, child failure,
stdout/stderr saturation, idempotent stop, and partial-start cleanup. The
decision ladder is: stable in-process library call; exactly one isolated
deployment child with versioned local structured IPC; unsupported combination.
Threads may drain logs, but lifecycle decisions cannot depend on parsed output.

Also resolve distributed ownership explicitly. The preferred topology is the
outer `RuntimeSupervisor` owning one MPI/srun `RankLauncher` plus one
`NodeSupervisor` per planned-node rank. Each rank owns its local Ray child; the outer
supervisor, not rank zero, owns `DeploymentManager`, the gateway, readiness, and
global state. Implement the mandatory section 3.2 authenticated control channel,
then independently test whether MPI/srun propagates a worker-rank nonzero exit,
preserves a useful first failure, and cancels the remaining ranks. If launcher
aggregation is insufficient, the supervisor still has the channel's causal
event but must explicitly terminate the launcher group and return nonzero. The
two-node proof must kill a worker Ray child, a worker supervisor, and the control
connection in turn, then verify nonzero terminal status, bounded cleanup,
watchdog behavior, and causal evidence.

Deliverable: `doc/hardening/decisions/ADR-001-process-boundaries.md`.

#### S02 — Readiness authority

Against the pinned Ray version, evaluate public cluster/resource observations,
public Serve status, ExaServe generation-scoped replica registration, required
node proxy/route observations, gateway health, per-model external canaries, and
compatibility receipts. Reproduce stale generation, proxy-before-deployment,
missing replica, one-replica-only canary success, broken route with live process,
and component death after READY. The fallback order is public APIs, a pinned
internal adapter, targeted registration/probes, then unsupported. Log text is
diagnostic only.

The spike must measure the ExaServe observation path's event count and coordinator
work at a small synthetic high-cardinality plan. Select push-on-change plus
bounded heartbeat and per-node aggregation unless evidence requires another
implementation. Reject any design that polls `nodes * replicas`, performs one
external canary per replica through load balancing, or lets a replica-local
`/health` response certify the fleet.

Deliverable: `doc/hardening/decisions/ADR-002-readiness-authority.md`.

#### S03 — Compatibility delivery across process lifecycles

Build a role matrix for supervisor, Ray head/worker, Serve controller/proxy,
replica, and spawned/forkserver engine processes. For every patch, try public
configuration/upstream behavior, an immutable exact-version patched
wheel/environment, an exact-hash generated overlay, then a narrow
version-guarded runtime adapter. Every affected role must publish the selected
profile/version/hash receipt. Never alter the shared installed package in place.

The inventory includes `_sitecustomize.py`, every overlay hunk, `ray_start.py`,
worker setup hooks and constant mutation in `server.py`, private Serve APIs,
engine spawn shims, shell environment/`PYTHONPATH` activation, staged venvs, and
instrumentation hooks. Discover actual runtime versions; do not assume that the
frameworks version in README or patch-module prose is current. Timebox the
immutable patched-wheel/environment probe, but select the generated-overlay
fallback only when the probe's acceptance test fails. One logical activation API
may use different verified delivery internals for managed Python roles and
unmodified daemons; it may not expose two patch policies to consumers.

The one-node proof must show the selected profile reaching a spawned EngineCore
process. The two-node proof must show matching supervisor, Ray head/worker,
affected Serve actor, and replica receipts, plus fail-closed behavior for a
missing or mismatched receipt.

Deliverable: `doc/hardening/decisions/ADR-003-compatibility-delivery.md`.

The security boundary and initial supported compatibility envelope are WP0
decisions because WP1 must encode them in the immutable plan schema.

### 4.3 Ordered task packets

| Packet | Work | May start when | Required checkpoint |
|---|---|---|---|
| P00 | WP0 baseline, ledger, hermetic test-environment repair, S00-S03, and required one-/two-node early compute proofs | Immediately | Reproducible environment receipt, every current failure identified, four ADR verdicts backed by the required proofs |
| P01 | WP1 contracts plus WP2 atomic state/artifacts | P00 gates pass | Canonical immutable plans and crash-safe state tests |
| P02 | WP3 compatibility profiles and process receipts | P01 contract stable; S03 early proof already passed in P00 | Every process role proves one profile or fails closed; the early spawned-engine/two-node proof is retained as a regression |
| P03 | WP4 supervisor plus WP5 readiness state machine | P02 activation proven; S01/S02 early proofs already passed in P00 | No log-controlled success; fake and leased two-node lifecycle/failure/readiness regressions pass |
| P04 | WP6 staging, WP7 gateways/security, WP8 schedulers | P03 component contract stable | Transaction, capability, port, submission, and recovery tests pass |
| P05 | WP9 eval/ClientLab, WP10 operations, WP11 release gates | P04 status/plan APIs stable | Consumers share contracts; packaged hermetic suite and CI are green |
| P06 | WP12 platform proof and WP13 cutover | All lower gates pass | Supported matrix evidence, legacy deletion, closed ledger, final audit |

Packets are dependency groups, not permission to make a giant diff. Only slices
inside the same packet may overlap after their shared interfaces are frozen;
later packets do not “parallelize cleanly after WP0.” Use the logical checkpoints
in section 10 and keep the tree testable after each slice.

### 4.4 Mandatory acceptance catalog for the final adjudicated items

These tests are minimum closure evidence; Claude may add stronger tests but may
not replace them with prose:

| ID | Required acceptance evidence | Owner |
|---|---|---|
| AC-TST-01 | The legacy-import policy test passes with and without an executable `rg`; its implementation uses no undeclared host binary. `eval/tests`, its individual control-plane module, and child subprocess tests collect from the documented installed/source contract. Plugin set and fixed/random seed are recorded. | WP0/WP11 |
| AC-SUP-01 | A fake launcher and a leased two-node run prove explicit per-rank ownership. Worker Ray-child death, worker-supervisor death, rank nonzero, head-child death, cancellation, and partial start each preserve the first cause, produce one terminal state and nonzero scheduler-visible status, and finish bounded idempotent cleanup. No head process signals or reaps an unowned remote PID. | WP0/WP4/WP12 |
| AC-CTL-01 | The authenticated rank channel rejects wrong deployment/plan hash/generation, rank/node mismatch, bad MAC, oversized/unknown messages, stale sequence, and duplicate non-idempotent commands. Disconnect removes readiness; reconnect requires a full snapshot; supervisor loss triggers bounded node-local cleanup. MPI exit and the typed event independently produce a nonzero global failure, and stdout text cannot change state. | WP0/WP4/WP12 |
| AC-COMP-01 | A profile manifest inventories every patch and affected role. One-node spawn and two-node tests prove matching profile/version/hash/capability receipts from every required managed role and supervisor attestation for allowed external daemons; missing, stale, or mismatched receipts prevent READY. No test edits an installed package in place. | WP0/WP3/WP12 |
| AC-RDY-01 | For each READY conjunct, a test withholds or stales only that observation and proves the deployment remains non-ready with a named blocker. A gateway/process health response alone, a one-replica canary, a log marker, or prior-generation status can never satisfy the predicate. Post-READY loss performs the declared state transition. | WP0/WP5/WP7/WP12 |
| AC-RDY-02 | Synthetic high-cardinality runs prove O(K) retained state and full reconciliation for K planned identities, O(1) indexed update per ordinary observation, at-least-once deduplication, stale-instance/generation rejection, bounded heartbeat traffic, exact blocker reporting, atomic READY persistence before marker rendering, and READY revocation after required lease expiry. No producer performs fleet-wide polling. | WP5/WP12 |
| AC-PLAN-01 | Omitted scheduler size inherits deployment size; an explicit no-matrix mismatch and explicit targeted/derived matrix mismatch are rejected; an untargeted deployment matrix synchronizes; zero remains invalid; rendered allocation and advertised deployment agree; every dispatch arm is validated. | WP1/WP9 |
| AC-TEL-01 | Two deployments in one Ray cluster cannot share telemetry state; stale-generation writes are rejected; reporting is bounded/observable; cleanup runs after success, timeout, and failure; placement is declared rather than assumed. | WP4/WP10 |
| AC-STAT-01 | A typed producer/consumer test covers the active push schema; expected-vs-received replica counts are enforced; incomplete required stats cannot yield success; the broken legacy pull API is either removed/deprecated explicitly or fixed without `KeyError`. | WP9/WP10 |
| AC-OBS-01 | A supplied transport request ID is preserved/echoed and linked to a distinct completion ID through an engine spy and structured logs; an absent ID is generated exactly once; aggregate status reports planned/observed replicas; replica-local health tests do not claim fleet-wide proof. | WP5/WP7/WP10 |
| AC-DIST-01 | Active distribution scripts contain no SSH fan-out; a stubbed MPI launcher observes broadcast and overlay-assembly commands; native failure or missing rank receipt exits nonzero; a later leased multi-node run proves every allocated rank's version/profile/hash. | WP3/WP6/WP12 |
| AC-PP-01 | PP defaults to one replica, retains a feasible explicit count, and proves disjoint node-pinned shard-aware bundles. The non-shard multi-replica path is explicitly gated/warned, and the private multi-app call is removed or capability/version guarded. | WP1/WP3/WP12 |
| AC-PROXY-01 | Proxy evidence classifies `healthy`, `degraded`, and `process_dead` separately. A death captures exit code/signal/log before causal assignment. Bounded-client no-delay on/off runs record retransmits, connections, CPU, process state, and unique run IDs. | WP4/WP7/WP12 |
| AC-INST-01 | Static call search proves the legacy proxy profiling hook is absent or unreachable; instrumentation has exactly one documented output owner and emits an atomic activation receipt. | WP3/WP10 |
| AC-SCALE-01 | ADR-000 distinguishes the user-approved qualification target from the evidence-derived supported maximum for each site/vendor/engine/gateway/request-mode/node/replica envelope. ExaServe readiness-event processing is linear in planned components/events in a synthetic high-cardinality test, with event/message/canary counts reported against plan size. Logical 16/64/128/256 gates remain distinct; any larger-allocation subset proves isolation and records both sizes. Scale above the accepted single-control-plane envelope is rejected unless its separate topology gate passed. | WP0/WP5/WP12 |

The test-policy, fake-supervisor, scheduler, telemetry, stats, observability,
synthetic scale, and mocked distribution checks are login-node-safe when kept
brief. Real MPI, PP, proxy, engine, and scale proofs use the appropriate early or
final WP12 lane.

## 5. Ordered implementation plan

### WP0 — Freeze the baseline and create the closure ledger

**Purpose:** Make regressions, decisions, and completion measurable.

Actions:

1. Snapshot the exact repository revision, dirty-worktree state, Python and
   package versions, `PYTHONPATH`, executable resolution, loaded pytest plugins
   and seed, scheduler/site configuration, and failed pytest node IDs.
2. Convert PR-001 through PR-035; every baseline Known Issue (currently A1-A7,
   B1-B3, C1-C6, D1-D4, and resolved record E1); and every TODO/backlog entry
   into one machine-readable finding ledger. Historical/resolved entries still
   receive a disposition and evidence rather than silently disappearing.
3. Assign each item an invariant, work package, validation test, disposition,
   and evidence link.
4. Capture every baseline failure by pytest node ID and root cause, with a
   minimal reproducer, before changing it.
5. Define the supported deployment envelopes to validate: Aurora XPU first;
   CPU/null-compute for hermetic tests; CUDA/ROCm, Slurm, and SGLang as gated
   claims rather than assumptions.
6. Remove baseline-environment accidents before architectural work: replace the
   test's `rg` subprocess with an in-process scan, make every test subtree
   independently collectible, prevent unit tests from reaching live Hugging Face
   services across forkserver workers, and define canonical plugin/seed policy.
7. Run S00-S03, including their mandatory one-/two-node proofs, and record the
   production-envelope, distributed-process-boundary, readiness-authority, and
   compatibility-delivery decisions. Define exact node/replica tiers rather than
   an ambiguous “medium scale” tier.
8. Inventory every repository readiness-marker control consumer before replacing
   it: `src/exaserve/driver.py`, `eval/lib/backends/base.py`,
   `eval/lib/backends/ray.py`, `eval/scripts/debug_128n.pbs`,
   `eval/scripts/profile_init_scaling.sh`, and
   `src/exaserve/proxy/litellm_proxy.py`, plus tests encoding the legacy behavior.
   Producers may retain a human-readable compatibility line only after typed
   state is persisted. No external marker consumer is currently identified; any
   later one must be named, assigned an owner, and dispositioned. Also update
   marker-based operator/pass-criterion prose and comments currently present in
   `README.md`, `eval/lib/models.py`, `eval/specs/`, and
   `src/exaserve/proxy/ray_serve_proxy.py`; these are not control consumers but
   must not teach the removed protocol.

Exit gate: every issue has exactly one owner work package and no item exists only
in prose or a stale TODO file; baseline environment-dependent failures have a
portable reproducer/fix; S00-S03 have recorded ADR verdicts backed by every
required early proof.

### WP1 — Establish immutable configuration and plan contracts

**Purpose:** Stop configuration interpretation from drifting across the CLI,
driver, server, evaluation, and ClientLab.

Actions:

1. Define versioned schemas for `DeploymentPlan`, `ModelPlan`, `GatewayPlan`,
   `SchedulerPlan`, `RunPlan`, `SiteProfile`, and `ScaleEnvelope`. The envelope
   independently names scheduler/site, vendor/accelerator, engine/profile,
   gateway/exposure, request/streaming mode, node range, replica/model limits,
   and the validation tier required for each combination; it is not a single
   boolean such as `production_supported`. It distinguishes the user-approved
   `qualification_target` from the evidence-derived `supported_max`; only a
   validation-mode plan may exceed the latter, and a passing lower tier never
   implies support for a higher tier.
2. Separate user intent from derived values. The compiler produces a new,
   immutable plan; it never rewrites the source YAML.
3. Make coercion strict: booleans, numbers, enums, ports, paths, node counts,
   model identities, and environment variables fail with path-specific errors.
4. Give models an explicit stable identity distinct from display name and model
   repository path. Detect collisions before submission.
5. Represent secrets by references, not serialized values, and redact them from
   plans, logs, commands, and status artifacts.
6. Add a legacy-config adapter so existing inputs can be compiled once into the
   new contract during migration.
7. Make core, eval, and ClientLab import the shared schemas instead of creating
   parallel interpretations.
8. Validate `scheduler.nodes` against `deployment.num_nodes` after defaults and
   matrix derivation. Permit divergence only through a named, typed reservation
   topology whose resource and rate semantics are explicit.

Fallback: if a schema library creates an unacceptable deployment dependency,
retain the typed contract and use a small internal validator. Do not fall back
to permissive dictionary access.

Exit gate: the same input yields the same canonical plan everywhere; invalid or
ambiguous configuration is rejected before allocation or process launch.

### WP2 — Introduce atomic state, artifact, and provenance services

**Purpose:** Create one reliable persistence contract before orchestration is
rewired.

Actions:

1. Implement shared atomic write primitives using temp-file, flush/fsync where
   required, rename, and directory sync where the filesystem supports it.
2. Add per-run locking and compare-and-set state transitions to prevent duplicate
   submission and concurrent writers.
3. Define a versioned `DeploymentStatus` and `RunStatus` with explicit lifecycle,
   timestamps, reason codes, component observations, and provenance.
4. Record the source snapshot policy explicitly: working tree, committed HEAD,
   or packaged artifact. Never silently substitute committed HEAD.
5. Make result completeness manifest-based: expected shards/files/checksums must
   be satisfied before a run is marked successful.
6. Use content identity for traces and generated inputs; write them atomically
   and make reuse safe under concurrent runs.

Fallback: on filesystems whose rename or locking semantics prove inadequate,
use one designated writer plus append-only events and rebuild materialized state
from those events. Document the filesystem-specific choice in the SiteProfile.

Exit gate: interruption at every write boundary leaves either the previous valid
state or the complete new state, never a successful-looking partial artifact.

### WP3 — Unify the compatibility and patch system

**Purpose:** Make every process role run one auditable version combination,
without editing installed packages in place.

Actions:

1. Inventory every overlay hunk and runtime patch, then classify it as:
   configuration, vendor compatibility, functional upstream fix, performance
   instrumentation, or obsolete workaround.
2. Delete patches replaceable through stable Ray/vLLM configuration or public
   interfaces.
3. Separate instrumentation from correctness changes so profiling is opt-in and
   cannot alter production semantics accidentally.
4. Define a compatibility profile keyed by Python, Ray, vLLM/SGLang, vendor,
   accelerator, and ExaServe versions.
5. Preferred implementation: build/cache an immutable environment from the
   exact site distribution plus a reviewable patch series. Stage that same
   environment to every node and use it for the supervisor, Ray workers,
   replicas, and spawned engine processes.
6. Emit and verify a signed-or-hashed manifest and a runtime receipt from every
   process role. A mismatch fails readiness.
7. Add import-order tests, spawn tests, and worker tests proving that required
   behavior reaches all lifecycle boundaries.
8. Convert version-sensitive private API use into explicit capability checks.
9. Enumerate which roles are patched and which remain unmodified external
   daemons; receipts are required from the supervisor, Ray head/workers, affected
   Serve actors, replicas, and spawned engine processes.
10. Give every manifest entry a stable patch ID, target distribution and exact
    version/source hash, affected role and symbols/files, delivery mechanism,
    patch artifact hash, required import timing, capability produced, semantic
    probe, and upstream/removal reference. The profile ID is a hash over the
    canonical normalized manifest plus base-environment identity (SHA-256 in
    schema version 1).
11. Build generated overlays in a content-addressed run cache from a clean,
    verified source artifact and publish them atomically. Never copy a hand-
    edited replacement over a shared installed file. Activate exactly one
    profile through `CompatibilityActivator`; per-role internals may differ only
    as declared by that profile.
12. Separate profile materialization from activation. Materialization may build
    an immutable wheel/environment/overlay; activation is read-only, verifies
    the base and patch artifacts, constructs the role environment, and then
    imports/execs the role. Add a clean-interpreter test proving `ray`,
    `ray.serve`, `vllm`, and `sglang` are not imported before verification.
13. Define `CompatibilityReceipt` with schema version, deployment/generation,
    plan/profile/manifest hashes, role and logical/instance identity, owner/node,
    PID or actor identity, executable hash, observed package/source hashes,
    per-patch activation/postcondition results, capabilities, attestation type,
    and timestamp. It contains no secret. Receipts travel through the structured
    observation channel and the global writer materializes them; processes do
    not race to write independent shared-filesystem receipt files.

Fallback ladder:

1. Upstream/public API or supported configuration.
2. Rebuilt exact-version wheel/environment with patch series.
3. Generated exact-version overlay produced from verified upstream file hashes.
4. Narrow runtime monkey patch behind a strict version/capability guard.
5. Mark the version/platform combination unsupported.

`strict=False`, broad `except Exception`, and unverified source replacement are
not acceptable production fallbacks. If Aurora's custom distributions cannot be
reproduced, option 3 may be selected, but it must be generated, hashed, role-
verified, and fail closed. A full-file overlay is not automatically forbidden:
it is acceptable only when generated against an exact verified source hash,
diffed/reviewed as a patch artifact, installed outside the shared environment,
activated before import, and covered by role receipts plus semantic probes. A
runtime monkey patch is never a parallel default tier; it is a per-patch last
resort with an exact version/symbol guard and fail-closed self-test.

Exit gate: one activation path reaches all process types; unknown versions and
partial activation cannot reach READY.

### WP4 — Replace subprocess sprawl with a runtime supervisor

**Purpose:** Retain necessary process isolation while removing shell/log parsing
as ExaServe's internal architecture.

Actions:

1. Extract deployment logic from `server.py` into a callable
   `DeploymentManager` with `prepare`, `deploy`, `observe`, `drain`, and `stop`
   operations and typed exceptions.
2. Add a `RuntimeSupervisor` that owns a registry of `ManagedComponent`s. Each
   component has typed `start`, `observe`, `stop`, and `wait` behavior.
3. Implement the S01 ownership topology: `RuntimeSupervisor` owns one MPI/srun
   `RankLauncher`; each launched rank is a `NodeSupervisor` owning its local Ray
   child. The outer allocation-head supervisor owns `DeploymentManager`, the
   gateway, readiness, persistence, and global termination policy. Every
   observation includes a valid `ComponentOwner`; remote child PIDs are never
   managed as if they were local.
4. Implement the mandatory section 3.2 authenticated structured channel for
   observations, receipts, commands, snapshots, and watchdog leases. Independently
   use launcher exit aggregation for scheduler-visible rank failure when it is
   causal and bounded; otherwise terminate the launcher group explicitly after
   the typed failure. A node/rank loss must reach both canonical deployment state
   and scheduler-visible exit status.
5. Classify every current subprocess as a long-lived component, finite
   component, scheduler boundary, native/MPI boundary, test/tool, or removable
   shell wrapper. Keep Ray head/workers, external gateways, scheduler/native
   tools, and MPI collectives as subprocesses only where their boundary is real;
   invoke them with argument arrays, sanitized environments, process groups,
   deadlines, and captured structured metadata.
6. Run `DeploymentManager` in the outer supervisor when import/lifecycle tests
   permit. If fault isolation requires a child process, use a narrow structured
   local IPC protocol, not stdout parsing; this does not move global ownership
   into rank zero.
7. Treat an unexpected exit of a declared long-lived component as fatal. A
   finite component succeeds only after zero exit and validation of its expected
   result or artifact. Any nonzero exit, timeout, signal, missing result, or
   invalid result becomes a supervisor failure event.
8. Implement idempotent cleanup, signal handling, graceful drain, and forced
   termination deadlines.
9. Replace broad exception catches with typed handling at the boundary; every
   ignored exception requires an explicit reason and metric.
10. Remove duplicated `ProcessMonitor` behavior from eval and make eval consume
   the shared supervisor/status contract.

Fallback: if in-process deployment conflicts with patch/import ordering, first
fix lazy import plus compatibility activation. If process isolation is still
required, retain exactly one deployment child with a versioned local socket or
pipe protocol. This local fault-isolation IPC is subordinate to and never
replaces the mandatory cross-rank control channel. The current log-marker
protocol is permitted only as a temporary migration oracle and is removed at
cutover.

Exit gate: no success path depends on matching stdout; a local child, remote
worker rank, head component, timeout, or signal produces one deterministic
terminal state and nonzero scheduler-visible status, with bounded cluster-wide
cleanup and the first causal failure preserved.

### WP5 — Build authoritative readiness and lifecycle state

**Purpose:** Define READY as an ExaServe deployment invariant, not a convenient
message from one component.

Actions:

1. Implement an explicit deployment state machine:
   `PLANNED -> STAGING -> CLUSTER_STARTING -> DEPLOYING -> VALIDATING -> READY ->
   DRAINING -> STOPPED`. `FAILED` and `CANCELLED` are distinct terminal outcomes
   reachable from every active state. Add `DEGRADED` only if it is an explicit
   product mode with allowed operations and recovery rules. Specify post-READY
   transitions for replica/gateway loss back to `VALIDATING`, `DEGRADED`,
   `DRAINING`, or `FAILED`.
2. Create a named `ReadinessCoordinator` that tracks the plan generation and
   the versioned `ComponentObservation` contract in section 3.1. Reject stale
   generations, duplicate/out-of-order sequences, and unknown schemas.
3. Keep coordinator work linear in planned components and received events. Use
   push-on-change, bounded heartbeat where necessary, indexed counts, and
   per-node fan-in. Do not add head polling whose work is `nodes * replicas` or
   whose calls trigger the dependency's existing all-to-all behavior.
4. Define READY as one pure predicate over the resolved plan and current
   observations: exact required membership/resources; every required long-lived
   component healthy; expected model and replica counts; current-generation
   engine registrations; required routes/proxies; matching compatibility
   receipts; selected gateway healthy; and every required per-model external
   canary successful. Every required observation must belong to the current plan
   hash/generation/instance and remain inside its typed SiteProfile freshness or
   lease bound. Persist both satisfied and blocking predicates.
5. Require exact planned-versus-observed node membership and required resources,
   with an explicit policy for tolerated excess resources.
6. Require each model deployment to reach its expected replica count and each
   replica to register engine-level readiness for the current generation.
7. Require Ray Serve route/proxy health on every required node, not just actor
   creation or mailbox ordering.
8. Require the selected external gateway to pass its own health check and route
   discovery.
9. Send a per-model canary through the externally advertised endpoint and verify
   the response contract. Do not let a canary for one replica certify all
   replicas; use replica registrations plus externally routed canaries.
10. Compare-and-set one atomic readiness snapshot containing plan hash,
    generation, exact satisfied/missing/unhealthy identity sets, receipt hashes,
    canonical endpoint, model map, and capability map for clients and
    evaluation. Observation expiry or required-component loss after READY
    revokes the predicate and triggers the declared post-READY transition; READY
    is not a permanent latch.
11. Persist the READY transition before rendering `CLUSTER FULLY READY` as a
   human-readable compatibility line. Add a repository-wide test proving no
   Python or shell control path parses readiness markers.
12. Fail closed on deadline. A degraded mode must be explicitly requested and
    list the unavailable capabilities/models.

Fallbacks are component-specific:

- Ray: stable status API -> internal adapter pinned by the compatibility profile
  -> unsupported Ray version.
- Replica: explicit registration -> targeted per-replica probe if addressable ->
  unsupported backend. Actor creation alone is insufficient.
- Gateway: official health/readiness endpoint -> HTTP/TCP canary plus process
  observation. A startup log line is only diagnostic evidence.

Exit gate: readiness cannot be produced by a false-positive proxy, a stale
generation, a partial model deployment, or a matching log line.

### WP6 — Make model and runtime staging transactional

**Purpose:** Prevent partial or stale models, environments, and native tools from
appearing valid.

Actions:

1. Replace shallow cache tests with manifests that cover required files,
   metadata, sizes, and checksums appropriate to the cost envelope.
2. Stage into a unique temporary directory, validate, then atomically publish.
3. Propagate broadcast/extraction failures from every rank and aggregate them
   before publishing success.
4. Detect trailing-byte, truncation, incomplete shard, and concurrent-writer
   cases explicitly.
5. Unify model, environment, proxy binary, and patch-manifest staging receipts.
6. Cap concurrency and memory use; report per-stage timing and byte counts.
7. Validate tokenizer and pipeline-parallel workarounds as compatibility
   capabilities rather than unconditional behavior.

Fallback: where full checksums are too expensive at scale, use a versioned
manifest with immutable source identity, required-file inventory, sizes, and
sampled checksums, and document the residual risk. Existence-only validation is
not acceptable.

Exit gate: no node can start a replica from a partially published model or an
unverified runtime environment.

### WP7 — Harden gateways, ports, API, and the security boundary

**Purpose:** Turn proxy selection and exposure into explicit production
capabilities.

Actions:

1. Decide and document the initial security product boundary. Recommended first
   release: trusted allocation/internal network, management endpoint bound
   locally, externally exposed inference endpoint explicitly configured.
2. Validate authentication, TLS, bind addresses, allowed models, request/body
   limits, timeouts, and secret references before launch.
3. Replace probe-then-bind port selection. Prefer owning-process bind-to-zero and
   structured port discovery. A separate process cannot safely “reserve” a port
   unless socket activation or file-descriptor passing is actually supported;
   otherwise prove the handoff or document and bound the residual race.
4. Model every gateway as a `ManagedComponent` with configuration preflight,
   capabilities, health, metrics, process supervision, and drain semantics.
5. Test streaming, cancellation, backpressure, retries, error translation,
   request limits, and overload for each production gateway.
6. Mark benchmark-only gateways as such. In particular, fake streaming must not
   be reported as streaming correctness or streaming performance.
7. Establish a supported scale envelope. The Envoy failure chain, centralized
   no-coalescing streaming/network concentration, and per-node proxy topology
   are architectural decisions, not tuning footnotes. Preserve the corrected
   non-streaming HAProxy result as a regression test rather than treating the
   former harness-confounded plateau as a current ceiling. Classify the common
   degraded-but-stable 256-node streaming regime separately from rare unexplained
   HAProxy process death; capture exit/signal evidence before assigning the death
   a network root cause.

Fallback: if a gateway lacks a dependable readiness API, use a real inference
canary plus process observation. If it lacks required streaming/backpressure or
security capabilities, restrict its supported use instead of emulating success.

Exit gate: advertised URLs are the bound, verified URLs; gateway failure changes
deployment state; each supported gateway has a tested capability contract.

### WP8 — Consolidate scheduler and submission control

**Purpose:** Make submission safe, idempotent, portable, and consistent between
serving and evaluation.

Actions:

1. Define one shared `SchedulerBackend` contract for render, submit, observe,
   cancel, attach/reuse, and allocation metadata.
2. Migrate both core and eval to the shared implementation; remove duplicate
   submission paths.
3. Treat PSI/J as a backend capability, not an assumed universal default. Declare
   it as a dependency when selected and fail preflight if unavailable.
4. Keep native PBS/Slurm backends where they expose required site behavior not
   available through PSI/J, but make their outputs conform to the same contract.
5. Render job scripts without interpolating untrusted shell. Use safe quoting,
   explicit environment files, or structured launcher arguments.
6. Make repeated submission idempotent using the run lock and scheduler job
   identity. Define recovery after submit-success/state-write-failure.
7. Remove legacy profile races and make SiteProfile selection explicit.
8. Persist the real workspace/source provenance selected in WP2.

Fallback: a site-native backend is an acceptable final choice if PSI/J cannot
express or reliably observe required behavior. The compromise is documented per
site; duplicate higher-level run logic is not reintroduced.

Exit gate: one invocation creates at most one job; all backends render safely,
report canonical states, and have hermetic contract tests.

### WP9 — Rebuild evaluation and ClientLab on shared contracts

**Purpose:** Ensure experiments measure the deployment that actually became
ready and cannot silently accept partial or mislabeled results.

Actions:

1. Make eval wait on canonical `DeploymentStatus` generation and endpoint, not a
   driver log marker.
2. Use shared plans, IDs, lifecycle events, scheduler backends, atomic artifacts,
   and cancellation behavior.
3. Replace unsafe expression evaluation with an AST-restricted expression
   language or explicit operators.
4. Validate matrices, traces, rates, durations, clients, models, and startup-only
   behavior before scheduling.
5. Give generated traces content-derived identity and race-safe storage.
6. Require result manifests and expected-client/shard completeness before
   success; distinguish partial, cancelled, invalid, and failed results.
7. Define one versioned statistics schema and propagate logging/stats flags end
   to end. Remove or repair the dead pull collector. If stats are required by the
   resolved plan, missing, stale-generation, or incomplete push output makes the
   result non-successful; otherwise record an explicit telemetry-degraded state.
8. Fix ClientLab's missing/default `faults` behavior and remove unsafe shell
   construction.
9. Make streaming interpretation explicit and prevent fake-streaming results
   from entering real-streaming comparisons.
10. Preserve deployment and compatibility receipts in every experiment result.

Exit gate: eval and ClientLab contain no independent serving readiness or
scheduler control plane, and no partial result can be reported as complete.

### WP10 — Add operational observability and failure policy

**Purpose:** Make production behavior diagnosable without reconstructing it from
mixed stdout.

Actions:

1. Emit structured events with run, deployment, generation, component, node,
   model, replica, request, and scheduler identifiers.
2. Preserve readable console output as a rendering of structured events.
3. Add metrics for lifecycle duration, readiness blockers, child exits, replica
   health, gateway errors, queueing, staging, patch activation, retries, and
   dropped/ignored errors.
4. Make telemetry actors supervised or explicitly optional. Detached actors must
   have ownership, cleanup, naming, and version policies.
5. Define error classes and whether each is retryable, fatal, degradable, or
   operator-actionable.
6. Put bounded retention and size policy around logs and traces, including the
   cluster-wide replica-init and serving collectors. Give each collector
   deployment-scoped ownership, explicit placement, cleanup, stale-generation
   rejection, and observable backpressure/drop policy.
7. Produce an operator-facing status command that explains why a deployment is
   not ready and which action is safe.

Exit gate: every terminal failure has a reason code and correlated evidence;
ignored errors are intentional, counted, and bounded.

### WP11 — Make packaging and the test suite release-gating

**Purpose:** Establish a reproducible, hermetic correctness floor.

Actions:

1. Pin and lock runtime/test dependencies and pytest plugins per supported
   compatibility profile. Define whether plugin autoload is disabled and how
   fixed and randomized seeds are recorded.
2. Declare optional backend dependencies accurately, including PSI/J and gateway
   tooling.
3. Add formatting, lint, type, unit, package-build/install, and security/static
   checks to CI.
4. Make unit tests hermetic: no live Hugging Face access, scheduler, cluster,
   gateway binary, site filesystem, or undeclared host executable unless marked
   integration. Replace the `rg` source-policy subprocess with Python.
5. Define one source-layout/package-install contract that works for full-suite,
   targeted-subtree, and child-process tests. Prove the release suite from an
   installed wheel rather than relying on `conftest.py` collection order.
6. Repair every recorded baseline failure by correcting contracts or stale
   tests, not by weakening assertions or targeting a hardcoded failure count.
7. Add contract tests for every component, scheduler, gateway, engine, vendor,
   and state transition.
8. Add fault-injection tests for process crash, signal, deadline, partial write,
   port collision, stale readiness, corrupt model, missing patch, scheduler
   ambiguity, and lost gateway.
9. Add upgrade tests proving unsupported dependency versions fail before launch.
10. Run one fixed-order reproducible CI job and a separate randomized-order job
    that prints a replayable seed.

Exit gate: clean checkout and packaged-wheel test suites are green; CI blocks
merges when the production contract regresses.

### WP12 — Validate progressively on Aurora and gate other platforms

**Purpose:** Prove the architecture under real lifecycle and scale behavior
without wasting allocation time or masking platform gaps.

WP12 defines one execution procedure used by two distinct lanes:

- **WP0-Early Compute:** the smallest one-/two-node Ray/MPI/engine proofs needed
  to resolve S00-S03 before WP1 freezes contracts. These runs are owned by P00.
- **Final qualification:** the clean-artifact engine, failure, scale, and support-
  matrix gates after lower implementation packets pass. These runs are owned by
  P06.

Using the WP12 procedure early does not waive the final qualification run.

No GPU, distributed, MPI, or experiment workload runs on the login node. Before
any cluster action, load the current Aurora workflow instructions. Login-node
work is limited to brief unit/static/package checks after loading `frameworks`
and `go` as needed.

Compute-session acquisition and validation:

1. Reuse a valid existing compute session when available.
2. Otherwise prefer `subjob` to lease the required nodes from the user's
   keepalive allocation. The agent does not start or manage keepalive.
   `subjob` can lease only nodes already present and free in an eligible source;
   never assume that `subjob 64`, `subjob 128`, or `subjob 256` is available.
3. If no lease source exists, use the repository-approved `srundbg` or
   `srundsc N` interactive fallback; do not improvise scheduler commands.
4. A session is valid only when `PBS_JOBID` is set, `PBS_NODEFILE` exists and is
   readable, the current hostname is in that file, and the shell is either marked
   `AURORA_SUBJOB=1` or verified as the interactive PBS compute shell.
5. In every fresh compute session, source `~/script/env_aurora` for Ray work or
   `~/script/env_litellm` for LiteLLM work. Verify node count, working directory,
   Python/environment, executables, input models/data, unique output directory,
   remaining walltime, and XPU visibility before launch. Use
   `ZE_AFFINITY_MASK`; never introduce `ONEAPI_DEVICE_SELECTOR`.
6. Monitor stdout/stderr and process health actively. Preserve evidence and stop
   clearly broken work instead of waiting for a hung allocation.

Current Aurora queue bounds are planning inputs, not substitutes for the current
site instructions: `capacity` is 1-16 nodes with up to seven days;
`debug-scaling` is 2-256 nodes with at most one hour; `prod` starts at 256 nodes
and permits longer runs. Campaign-era queue delays and the observed roughly five
queued-job workflow are not site-policy guarantees. Capacity jobs, not
debug-scaling generally, have the documented maintenance-preemption caveat.

Before submitting any compute gate in either lane, add a row to the experiment
plan containing:

```text
gate_id, lane(WP0_EARLY|FINAL), logical_nodes, physical_allocation_nodes, acquisition_source,
queue, walltime_or_lease_ttl, expected_runtime, node_hours,
attempt_limit, retry_reason_policy, clean_state/reset_method, output_path
```

An early proof may use a smaller attempt/node-hour budget than final
qualification, but it requires the same command/environment/provenance fields
and an explicit pass/fail verdict. Calendar duration remains allocation-
dependent/TBD until these rows exist. A logical 64- or 128-node gate may run
inside an authorized larger physical
allocation only after a small proof shows that the substituted nodefile,
MPI/srun launcher, network membership, process cleanup, and artifact namespace
are isolated to the logical subset. Record logical and physical sizes separately.

Validation order:

1. Login-node-safe checks: schema/unit tests, static checks, package build,
   config rendering, and null mocks only.
2. WP0-Early Compute one-node process/import proof: in-process versus isolated
   `DeploymentManager`, spawned EngineCore compatibility receipt, typed state,
   cancellation, and bounded cleanup.
3. WP0-Early Compute two-node distributed proof: head and worker ownership,
   worker-child/rank failure propagation, MPI/srun cancellation, matching
   compatibility receipts, partial readiness, and first-cause preservation.
4. Final one-node null-compute lifecycle from the clean packaged artifact: start,
   ready, canary, drain, restart,
   cancellation, child failure, and artifact recovery.
5. One-node real engine/model smoke for each claimed engine/vendor pair.
6. Two-node test: staging, membership, per-node proxy, worker loss, gateway loss,
   duplicate ports, and partial readiness.
7. Four-node pipeline/tensor-parallel test including spawned EngineCore patch
   verification and replica failure.
8. 16-node and then 64-node topology tests validating controller/proxy load,
   staging fanout, network behavior, and readiness deadlines.
9. 128-node and then 256-node tests only after every lower gate passes. Capture GCS
   behavior, proxy topology, controller ticks, network throughput, and failure
   recovery rather than treating READY alone as success.
10. Separate offsite validation by support dimension: scheduler (for example
    Slurm), accelerator (CUDA or ROCm), engine, site profile, and scale. WP0 first
    performs a bounded account/access/environment probe for documented candidates
    such as Delta MI100 and Delta A100. A combination becomes `UNSUPPORTED` only
    after its access or validation blocker is recorded; one unavailable
    Slurm/ROCm combination does not reject Slurm/CUDA or another independent axis.

Each run has a preflight, exact command, environment receipt, output directory,
expected observations, active monitoring, and explicit pass/fail decision. Stop
on an early error rather than waiting through a hung allocation. If the required
resources are unavailable, record the external blocker and suppress the affected
scale/platform production claim; lack of a run is never implicit validation.
Scenario batteries may share an expensive allocation, but each scenario requires
a fresh generation, declared reset boundary, unique artifact directory, and an
independent verdict. Bundling never removes the 16/64/128/256 logical gates.

Exit gate: the published support matrix contains only combinations that passed
their required tier; scale limits have a measured envelope or an explicit
accepted-limit disposition.

### WP13 — Cut over, remove legacy paths, and perform the final audit

**Purpose:** End with one understandable system and a closed finding register.

Actions:

1. Switch CLI, eval, and ClientLab to the new plan, supervisor, readiness, state,
   and scheduler contracts.
2. Run parity tests against the old path for accepted behavior, then remove the
   migration switch. Production rollback after cutover uses the previous
   immutable release artifact; it does not preserve a hidden legacy architecture
   in the new release.
3. Delete log-marker consumers, duplicate process monitors/schedulers, stale
   full-file overlays, generated patch shims, unsafe evaluators, and deprecated
   config mutations.
4. Update examples, design documents, operational runbooks, failure recovery,
   support matrix, security assumptions, and upgrade instructions.
5. Re-run repository searches for broad catches, silent pass, shell execution,
   direct non-atomic writes, readiness strings, duplicated launch logic, private
   APIs, and stale TODO/known-issue statements.
6. Review every PR-001..PR-035, every baseline Known Issue/TODO, and every item
   discovered later with direct test or ADR evidence and assign a final
   disposition.
7. Run the complete hermetic and Aurora release matrix from a clean packaged
   artifact.
8. Produce a final audit report containing remaining accepted limits and
   unsupported combinations. There must be no unclassified residue.

Exit gate: no legacy control path remains reachable; documentation matches the
shipped behavior; the closure ledger has no `OPEN`, `PARTIAL`, or
`VALIDATION_OWED` entries for a claimed production configuration.

## 6. Validation matrix

| Layer | Required evidence | Main failures isolated |
|---|---|---|
| Pure unit | Hermetic schema, state-machine, planner, ID, quoting, manifest, and capability tests | Config coercion, collision, unsafe eval/shell, transition bugs |
| Package | Build/install in clean environment; compatibility-version rejection | Undeclared dependencies, import order, source-tree-only behavior |
| Component | Fake child processes, fake schedulers, local HTTP gateways, injected timeouts/errors | Silent exits, cleanup, deadline, readiness, gateway supervision |
| Persistence | Crash at each write/lock boundary; concurrent writers; filesystem-specific tests | Partial state, duplicate submit, trace/result races |
| One-node compute | Full lifecycle and real engine smoke | Environment, vendor, patch activation, model and canary readiness |
| Multi-node compute | Membership, staging, replicas, proxies, node/process loss | False readiness, partial staging, port/topology, recovery |
| Scale | 128/256-node measured topology and failure behavior | GCS/controller scaling, proxy cliff, network saturation |
| Offsite | Native Slurm and CUDA/ROCm runs | Portability claims that Aurora cannot prove |

Every test result is linked to the finding ledger and compatibility profile. A
test is not evidence for a different dependency version, backend, vendor, or
scale tier unless the profile declares that equivalence.

## 7. Finding-to-work-package closure map

| Work package | Audit findings | Known issues / debt covered |
|---|---|---|
| WP1 Contracts | PR-003, PR-006, PR-007, PR-011, PR-024 | Configuration drift, invalid inputs, model identity |
| WP2 State/artifacts | PR-018, PR-035 | C2, trace/result publication and source provenance |
| WP3 Compatibility | PR-002, PR-022, PR-026 | A5, resolved-D3 dead hook, D4, version drift, worker/spawn patch reach |
| WP4 Supervisor | PR-001, PR-009, PR-012, PR-028, PR-029 | A2, duplicate process control, lifecycle leaks |
| WP5 Readiness | PR-008, PR-023, PR-033 | A3, A4, D1, false/scale-dependent readiness |
| WP6 Staging | PR-004, PR-005 | A7 scale revalidation, C3, C4, partial/cache corruption |
| WP7 Gateway/security | PR-010, PR-011, PR-024, PR-025, PR-033 | A1, former-B1 regression guard, B2, B3, port/exposure/capability limits |
| WP8 Scheduler | PR-013, PR-014, PR-015, PR-027, PR-034 | Duplicated scheduler and unsafe submission |
| WP9 Eval/ClientLab | PR-016, PR-017, PR-019, PR-020, PR-021, PR-030 | C1, C5, C6, partial/mislabeled experiments |
| WP10 Operations | PR-029, PR-032 | Superseded-A6 collector debt, silent exceptions, telemetry lifecycle |
| WP11 Testing | PR-031 | Failed/stale tests, CI, dependency reproducibility |
| WP12 Platform proof | PR-002, PR-025, PR-026, PR-033 | Slurm/AMD/SGLang and scale validation debt |
| WP13 Cutover/docs | All findings | D2 MPI-path revalidation and all stale TODO/Known Issue claims |

Overlapping entries are intentional where correction and production proof are
different gates. The machine-readable ledger should retain one primary owner and
zero or more validation packages.

## 8. Required evidence and decision artifacts

Maintain these artifacts throughout the pass:

```text
doc/hardening/
  BASELINE.md
  FINDINGS.yaml
  COMPATIBILITY_MATRIX.md
  MIGRATION_LOG.md
  FINAL_AUDIT.md
  decisions/ADR-*.md
artifacts/hardening/<experiment-id>/
  manifest.yaml
  command.txt
  environment.txt
  stdout.log
  stderr.log
  verdict.md
```

`BASELINE.md` records revision and dirty state; Python/package/plugin versions;
`PYTHONPATH` and executable resolution; exact commands and seeds; collected and
failed pytest node IDs; scheduler/site profile; and current behavior. Large/raw
Aurora logs remain in unique run storage and are linked by path and checksum
rather than committed.

Each `FINDINGS.yaml` record requires:

```text
id, source, severity, invariant, primary_work_package, affected_regions,
acceptance_tests, status, decision, evidence, fallback, residual_risk,
support_impact, owner, approval?, revisit_condition
```

`status` uses only `OPEN`, `IN_PROGRESS`, `FIXED`, `REPLACED`,
`ACCEPTED_LIMIT`, `UNSUPPORTED`, `EXTERNAL_BLOCKER`, or
`OUT_OF_PRODUCTION_SCOPE`. The last four require a support impact and revisit
condition. `OUT_OF_PRODUCTION_SCOPE` is invalid for a defect in a claimed
production configuration or for a still-advertised capability. An
`ACCEPTED_LIMIT` record also requires approval identity, timestamp, and linked
approval evidence; a worker-authored proposal is not approval.

Each ADR records:

- problem and invariant;
- elegant target;
- alternatives tested;
- exact evidence and failure mode;
- selected implementation;
- operational trade-off;
- scope and support envelope;
- removal or revisit condition;
- linked findings and tests.

## 9. Release definition of done

The pass is complete only when all of the following are true:

1. Core serving, eval, and ClientLab share configuration, state, scheduler,
   process supervision, and readiness contracts.
2. No control decision parses `CLUSTER FULLY READY`, `ALL SERVICES READY`, or a
   gateway startup log string.
3. Subprocesses are restricted to documented OS boundaries and are supervised.
4. The selected compatibility mechanism is immutable, versioned, verified in
   every process role, and never edits installed packages in place.
5. READY proves the current plan generation, resources, deployments, replicas,
   routes, gateway, compatibility receipt, and external canaries.
6. Fatal errors cannot result in a successful terminal state or zero exit status.
7. Configuration is immutable and strict; writes and results are transactional.
8. Security and gateway capabilities match the documented exposure model.
9. Hermetic tests and the claimed Aurora validation tiers are green from a clean
   packaged artifact.
10. Slurm, CUDA/ROCm, SGLang, and untested scale tiers are either separately
    proven or explicitly blocked from production claims.
11. Every audit/Known Issue/TODO entry has a final disposition and evidence.
12. The legacy orchestration, readiness, scheduler, and patch paths are removed;
    there is one production architecture to maintain.

## 10. Recommended commit structure for the one pass

Use the following small, reviewable logical checkpoints even though this is one
migration. Claude Code creates commits only when explicitly authorized; otherwise
it leaves a reviewed working-tree checkpoint with the same tests/evidence.

1. Baseline and finding ledger.
2. Contracts and legacy adapters.
3. Atomic state/artifact layer.
4. Compatibility profiles and receipts.
5. Supervisor and component interfaces.
6. Readiness coordinator and status publication.
7. Transactional staging.
8. Gateways, ports, and security boundary.
9. Unified scheduler/submission.
10. Eval and ClientLab migration.
11. Observability and failure policy.
12. Test/packaging/CI gates.
13. Platform validation evidence.
14. Legacy deletion, documentation, and final audit.

This ordering makes the elegant architecture the default attempt while keeping
fallbacks localized behind stable contracts. If one component must use a less
elegant implementation, the compromise does not force log parsing, mutable
configuration, or runtime patch sprawl back into the rest of the system.
