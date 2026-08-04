# ExaServe Production Hardening: One-Pass Execution Plan

**Draft date:** 2026-08-04  
**Companion audit:** `doc/PRODUCTION_READINESS_AUDIT.md`  
**Scope:** Core serving, cluster launch, readiness, compatibility patches,
model staging, gateways, schedulers, evaluation, ClientLab, observability,
packaging, tests, documentation, and supported-platform claims.

## 1. Executive decision

Execute this as one controlled migration program, not as one giant code change.
The work stays on one hardening branch and ends with one production architecture,
but it is divided into ordered gates so that each new contract is proven before
the legacy path is removed.

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

“Nothing unresolved” means every audit and known-issue entry must finish in one
of these states:

- **Fixed** — corrected and verified.
- **Replaced** — the affected design was removed by the new architecture.
- **Accepted limit** — bounded, documented, monitored, and approved for a
  stated production envelope.
- **Unsupported** — rejected as a production claim and blocked by validation.
- **External blocker** — isolated with reproducer, evidence, owner, and a safe
  fallback. It cannot remain a silent open item.

An item marked merely “TODO,” “partially tested,” or “works on Aurora” is not
closed.

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
- The current test baseline is not green: 25 of 36 tests pass and 11 fail. The
  failures include accidental network access, stale proxy and scheduler
  expectations, and a ClientLab configuration failure.
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
SchedulerBackend                      Runtime Supervisor
                                             |
                   +-------------------------+-------------------------+
                   |                         |                         |
             Ray Component          DeploymentManager          Gateway Component
             (subprocess)              (callable)               (subprocess)
                   |                         |                         |
                   +-------------------------+-------------------------+
                                             |
                                      ReadinessCoordinator
                                             |
                                  atomic DeploymentStatus + events
                                             |
                              CLI / eval / ClientLab / operators
```

Before any Ray or engine import, a `CompatibilityActivator` selects and verifies
an immutable compatibility profile. All process roles publish a compatibility
receipt containing package versions, profile identity, patch hashes, and active
capabilities.

The supervisor owns cancellation, deadlines, child process exit propagation,
cleanup, and durable state. Logs remain operational evidence; they are not the
source of truth for lifecycle decisions.

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

## 5. Ordered implementation plan

### WP0 — Freeze the baseline and create the closure ledger

**Purpose:** Make regressions, decisions, and completion measurable.

Actions:

1. Snapshot the exact repository revision, dirty-worktree state, environment,
   package versions, scheduler/site configuration, and existing test result.
2. Convert PR-001 through PR-035, Known Issues A1-D4, and active TODOs into one
   machine-readable finding ledger.
3. Assign each item an invariant, work package, validation test, disposition,
   and evidence link.
4. Capture minimal reproducers for the 11 failing tests before changing them.
5. Define the supported deployment envelopes to validate: Aurora XPU first;
   CPU/null-compute for hermetic tests; CUDA/ROCm, Slurm, and SGLang as gated
   claims rather than assumptions.

Exit gate: every issue has exactly one owner work package and no item exists only
in prose or a stale TODO file.

### WP1 — Establish immutable configuration and plan contracts

**Purpose:** Stop configuration interpretation from drifting across the CLI,
driver, server, evaluation, and ClientLab.

Actions:

1. Define versioned schemas for `DeploymentPlan`, `ModelPlan`, `GatewayPlan`,
   `SchedulerPlan`, `RunPlan`, and `SiteProfile`.
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

Fallback ladder:

1. Upstream/public API or supported configuration.
2. Rebuilt exact-version wheel/environment with patch series.
3. Generated exact-version overlay produced from verified upstream file hashes.
4. Narrow runtime monkey patch behind a strict version/capability guard.
5. Mark the version/platform combination unsupported.

`strict=False`, broad `except Exception`, and unverified source replacement are
not acceptable production fallbacks. If Aurora's custom distributions cannot be
reproduced, option 3 may be selected, but it must be generated, hashed, role-
verified, and fail closed.

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
3. Keep Ray head/workers, external gateways, scheduler/native tools, and MPI
   collectives as subprocesses. Invoke them with argument arrays, sanitized
   environments, process groups, deadlines, and captured structured metadata.
4. Run `DeploymentManager` in the head supervisor when import/lifecycle tests
   permit. If fault isolation requires a child process, use a narrow structured
   IPC protocol, not stdout parsing.
5. Make any child exit before shutdown a supervisor event. Propagate failure,
   cancel outstanding work, update status, and exit nonzero.
6. Implement idempotent cleanup, signal handling, graceful drain, and forced
   termination deadlines.
7. Replace broad exception catches with typed handling at the boundary; every
   ignored exception requires an explicit reason and metric.
8. Remove duplicated `ProcessMonitor` behavior from eval and make eval consume
   the shared supervisor/status contract.

Fallback: if in-process deployment conflicts with patch/import ordering, first
fix lazy import plus compatibility activation. If process isolation is still
required, retain exactly one deployment child with a versioned local socket or
pipe protocol. The current log-marker protocol is permitted only as a temporary
migration oracle and is removed at cutover.

Exit gate: no success path depends on matching stdout; a child crash, timeout,
or signal produces one deterministic terminal state and nonzero status.

### WP5 — Build authoritative readiness and lifecycle state

**Purpose:** Define READY as an ExaServe deployment invariant, not a convenient
message from one component.

Actions:

1. Implement an explicit deployment state machine:
   `PLANNED -> STAGING -> CLUSTER_STARTING -> DEPLOYING -> VALIDATING -> READY ->
   DRAINING -> STOPPED`, with `FAILED` reachable from every active state.
2. Create a named `ReadinessCoordinator` that tracks the plan generation and
   typed observations from components and replicas.
3. Require exact planned-versus-observed node membership and required resources,
   with an explicit policy for tolerated excess resources.
4. Require each model deployment to reach its expected replica count and each
   replica to register engine-level readiness for the current generation.
5. Require Ray Serve route/proxy health on every required node, not just actor
   creation or mailbox ordering.
6. Require the selected external gateway to pass its own health check and route
   discovery.
7. Send a per-model canary through the externally advertised endpoint and verify
   the response contract. Do not let a canary for one replica certify all
   replicas; use replica registrations plus externally routed canaries.
8. Publish the canonical endpoint, model map, capability map, generation, and
   status atomically for clients and evaluation.
9. Keep `CLUSTER FULLY READY` only as a human-readable compatibility log emitted
   after the state transition; nothing may parse it for control.
10. Fail closed on deadline. A degraded mode must be explicitly requested and
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
3. Replace probe-then-bind port selection. Let the owning process bind port zero
   where supported, or hold a lease/reservation until handoff.
4. Model every gateway as a `ManagedComponent` with configuration preflight,
   capabilities, health, metrics, process supervision, and drain semantics.
5. Test streaming, cancellation, backpressure, retries, error translation,
   request limits, and overload for each production gateway.
6. Mark benchmark-only gateways as such. In particular, fake streaming must not
   be reported as streaming correctness or streaming performance.
7. Establish a supported scale envelope. Envoy collapse, HAProxy single-process
   limits, network saturation, and per-node proxy topology are architectural
   decisions, not tuning footnotes.

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
   to end. A missing stats producer is an error when stats were requested.
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
6. Put bounded retention and size policy around logs and traces, especially
   per-replica scaling traces.
7. Produce an operator-facing status command that explains why a deployment is
   not ready and which action is safe.

Exit gate: every terminal failure has a reason code and correlated evidence;
ignored errors are intentional, counted, and bounded.

### WP11 — Make packaging and the test suite release-gating

**Purpose:** Establish a reproducible, hermetic correctness floor.

Actions:

1. Pin and lock runtime/test dependencies per supported compatibility profile.
2. Declare optional backend dependencies accurately, including PSI/J and gateway
   tooling.
3. Add formatting, lint, type, unit, package-build/install, and security/static
   checks to CI.
4. Make unit tests hermetic: no live Hugging Face access, scheduler, cluster,
   gateway binary, or site filesystem unless marked integration.
5. Repair the 11 current failures by correcting contracts or stale tests, not by
   weakening assertions.
6. Add contract tests for every component, scheduler, gateway, engine, vendor,
   and state transition.
7. Add fault-injection tests for process crash, signal, deadline, partial write,
   port collision, stale readiness, corrupt model, missing patch, scheduler
   ambiguity, and lost gateway.
8. Add upgrade tests proving unsupported dependency versions fail before launch.

Exit gate: clean checkout and packaged-wheel test suites are green; CI blocks
merges when the production contract regresses.

### WP12 — Validate progressively on Aurora and gate other platforms

**Purpose:** Prove the architecture under real lifecycle and scale behavior
without wasting allocation time or masking platform gaps.

No GPU, distributed, MPI, or experiment workload runs on the login node. Aurora
execution must use the project keepalive/interactive compute workflow and source
`~/script/env_aurora` in each fresh compute session. Reuse a valid allocation
when available and actively monitor logs and failures.

Validation order:

1. Login-node-safe checks: schema/unit tests, static checks, package build,
   config rendering, and null mocks only.
2. One-node null-compute lifecycle: start, ready, canary, drain, restart,
   cancellation, child failure, and artifact recovery.
3. One-node real engine/model smoke for each claimed engine/vendor pair.
4. Two-node test: staging, membership, per-node proxy, worker loss, gateway loss,
   duplicate ports, and partial readiness.
5. Four-node pipeline/tensor-parallel test including spawned EngineCore patch
   verification and replica failure.
6. Medium-scale topology test validating controller/proxy load, staging fanout,
   network behavior, and readiness deadlines.
7. 128-node and then 256-node tests only after lower gates pass. Capture GCS
   behavior, proxy topology, controller ticks, network throughput, and failure
   recovery rather than treating READY alone as success.
8. Separate offsite validation for Slurm, CUDA, and ROCm. Until executed, those
   combinations remain experimental/unsupported regardless of unit coverage.

Each run has a preflight, exact command, environment receipt, output directory,
expected observations, active monitoring, and explicit pass/fail decision. Stop
on an early error rather than waiting through a hung allocation.

Exit gate: the published support matrix contains only combinations that passed
their required tier; scale limits have a measured envelope or an explicit
accepted-limit disposition.

### WP13 — Cut over, remove legacy paths, and perform the final audit

**Purpose:** End with one understandable system and a closed finding register.

Actions:

1. Switch CLI, eval, and ClientLab to the new plan, supervisor, readiness, state,
   and scheduler contracts.
2. Run parity tests against the old path for accepted behavior, then remove the
   migration switch.
3. Delete log-marker consumers, duplicate process monitors/schedulers, stale
   full-file overlays, generated patch shims, unsafe evaluators, and deprecated
   config mutations.
4. Update examples, design documents, operational runbooks, failure recovery,
   support matrix, security assumptions, and upgrade instructions.
5. Re-run repository searches for broad catches, silent pass, shell execution,
   direct non-atomic writes, readiness strings, duplicated launch logic, private
   APIs, and stale TODO/known-issue statements.
6. Review every PR-001..PR-035 and A1-D4 ledger entry with direct test or ADR
   evidence and assign a final disposition.
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
| WP3 Compatibility | PR-002, PR-022, PR-026 | A5, D4, version drift, worker/spawn patch reach |
| WP4 Supervisor | PR-001, PR-009, PR-012, PR-028, PR-029 | A2, duplicate process control, lifecycle leaks |
| WP5 Readiness | PR-008, PR-023, PR-033 | A3, A4, D1, false/scale-dependent readiness |
| WP6 Staging | PR-004, PR-005 | A7, C3, C4, partial/cache corruption |
| WP7 Gateway/security | PR-010, PR-011, PR-024, PR-025, PR-033 | A1, B1, B2, B3, port/exposure/capability limits |
| WP8 Scheduler | PR-013, PR-014, PR-015, PR-027, PR-034 | D3, duplicated scheduler and unsafe submission |
| WP9 Eval/ClientLab | PR-016, PR-017, PR-019, PR-020, PR-021, PR-030 | C1, C5, C6, partial/mislabeled experiments |
| WP10 Operations | PR-032 | A6, silent exceptions and unbounded telemetry |
| WP11 Testing | PR-031 | Failed/stale tests, CI, dependency reproducibility |
| WP12 Platform proof | PR-002, PR-025, PR-026, PR-033 | Slurm/AMD/SGLang and scale validation debt |
| WP13 Cutover/docs | All findings | D2 and all stale TODO/Known Issue claims |

Overlapping entries are intentional where correction and production proof are
different gates. The machine-readable ledger should retain one primary owner and
zero or more validation packages.

## 8. Required evidence and decision artifacts

Maintain these artifacts throughout the pass:

- `BASELINE.md` — revision, environment, test baseline, current behavior.
- `FINDINGS.yaml` — authoritative issue, owner, state, invariant, evidence.
- `DECISIONS/ADR-*.md` — architectural choices and rejected alternatives.
- `EXPERIMENTS/<id>/` — reproducer, exact command, receipt, logs, result, verdict.
- `COMPATIBILITY_MATRIX.md` — exact supported version/platform combinations.
- `MIGRATION_LOG.md` — old-to-new behavior changes and intentional breaks.
- `FINAL_AUDIT.md` — closure evidence, accepted limits, unsupported claims.

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

Use small, reviewable commits even though this is one migration:

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
