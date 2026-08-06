# Production-Hardening Implementation Audit

**Audit date:** 2026-08-06  
**Implementation audited:** `005891e..e73f3eb` (`e73f3eb9ac899e5efed3669b7afb9ab24ecec462`)  
**Change size:** 97 files, 8,753 insertions, 2,249 deletions  
**Overall verdict:** **NOT PRODUCTION READY**

This document is review evidence, not an alternative implementation
specification. `doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md` remains the sole
architecture and implementation authority. Where the implementation, status
ledger, ADR prose, or this audit disagree with that plan, the plan wins.

## 1. Executive conclusion

The commit contains real, useful hardening work. In particular, it introduces
good foundations for authenticated control messages, atomic single-file
publication, lifecycle state machines, stricter configuration compilation,
safer HAProxy administration, and better native staging error propagation.
The existing hermetic suite also passes: **102 passed in 29.63 seconds**.

It does **not**, however, implement the production architecture described by
the canonical plan. The new plan, control, and status packages are mostly
isolated substrate used by tests. The shipped CLI, serving path, eval path,
and ClientLab still use the legacy Bash/MPI/driver/server topology, parse
stdout markers as lifecycle state, apply compatibility changes without typed
receipts, and retain multiple unsupervised or fail-open boundaries.

The commit message and `doc/hardening/STATUS.md` claim that 34 of 35 audit
findings are fixed. That is not supported by reachable code or acceptance
evidence. `STATUS.md` itself acknowledges that the WP4/WP5 supervisor and
readiness rewrite and the WP13 cutover remain unfinished. Those items are not
optional cleanup: they own the blocker invariants that motivated the
production-hardening migration.

The correct characterization of `e73f3eb` is:

- valuable P00/P01-style foundations and incremental repairs;
- useful exploratory 2/16/64-node feasibility evidence;
- no production control-plane cutover;
- no release-gate closure for WP1-WP13 as a whole;
- not safe to deploy as the claimed production-ready revision.

No new cluster workload was launched for this audit. The source-level blockers
are decisive, and the existing local compute artifacts were inspected rather
than consuming another allocation.

## 2. Release-gate summary

| Area | Audit disposition | Main reason |
|---|---|---|
| WP1 plans | **FAIL / partial substrate** | Incomplete, mutable-through-nested-values plan; path-dependent hash; not used by production consumers |
| WP2 state/artifacts | **FAIL / partial substrate** | Lease takeover is unfenced; release can delete another owner; status CAS has ABA holes; many direct writers remain |
| WP3 compatibility | **FAIL** | No profile/activator/receipt system; activation remains fail-open; READY is not receipt-gated |
| WP4 supervision | **FAIL** | No `RuntimeSupervisor`; essential-child, signal, process-group, and first-cause invariants remain incomplete |
| WP5 readiness | **FAIL** | Stdout remains authoritative; false-positive paths and permanent READY latch remain |
| WP6 staging/distribution | **FAIL / improved** | Blind completion marker and in-place extraction; no generation-isolated, per-rank receipts |
| WP7 gateway/security | **FAIL / improved** | Production boundary is ambient/optional; TCP-only health, port races, incomplete schema and request validation |
| WP8 scheduler/submission | **FAIL** | Two scheduler stacks; submit-success/state-write crash window; unsafe job-body quoting |
| WP9 eval/ClientLab | **FAIL** | Partial results can succeed; legacy lifecycle/launcher remains; ClientLab not migrated |
| WP10 observability | **FAIL / partial** | Telemetry can reuse stale detached actors; cleanup and request-ID propagation incomplete |
| WP11 tests/CI | **FAIL / improved** | Green local tests do not cover acceptance gates; clean CI dependency and wheel/type/format/security gaps remain |
| WP12 qualification | **NOT QUALIFYING** | Useful direct smoke, but missing required provenance, production gateway, same-workload baseline, and target architecture |
| WP13 cutover | **FAIL** | 533-line Bash orchestrator and marker consumers remain the active paths |

## 3. Release blockers

### IMP-B01 — The target architecture is not wired into production

**Evidence**

- `src/exaserve/control/__init__.py` explicitly leaves `supervisor`,
  `rank_launcher`, `node_supervisor`, and `readiness` for future work.
- Required modules such as `control/supervisor.py`,
  `control/rank_launcher.py`, `control/node_supervisor.py`,
  `control/readiness.py`, `deployment.py`, and
  `compat/{profile,activator,receipt}.py` do not exist.
- `src/exaserve/plan/__init__.py` says serving, eval, and ClientLab migration is
  later work and keeps legacy loaders as the default.
- `compile_deployment_plan`, `ControlListener`, and `StatusStore` have no
  production serving consumers. Their consumers are principally tests/spikes.
- `src/exaserve/resources/launch_cluster.sh:527-528` still starts one legacy
  `exaserve.driver` process per rank.
- `src/exaserve/driver.py:568-724` still makes rank 0 own the Ray head, Serve,
  and gateway while worker ranks directly own Ray workers.
- `src/exaserve/cli.py:28-34` still replaces itself with Bash.
- `eval/lib/backends/ray.py:230-287` still launches that Bash script.
- ClientLab scripts and `clientlab/runner/runtime.py` still independently own
  shell/SSH launch and kill behavior.

**Impact**

There is no production `RuntimeSupervisor`, `RankLauncher`, `NodeSupervisor`,
`DeploymentManager`, `ReadinessCoordinator`, single first-cause owner, or
shared lifecycle used by core/eval/ClientLab. The new modules prove useful
techniques; they do not change the deployed topology.

**Required disposition**

WP4, WP5, WP9, and WP13 remain open. Do not call the extraction/cutover
“clean-architecture follow-up”; it owns mandatory correctness gates.

### IMP-B02 — Stdout remains the authoritative readiness protocol

**Evidence**

- `src/exaserve/driver.py:54-81` scans child stdout for a substring and sets a
  readiness event.
- `src/exaserve/driver.py:630-639` advances or fails startup based on
  `CLUSTER FULLY READY`.
- `src/exaserve/server.py:2314-2321` prints that marker without first
  publishing an authoritative typed readiness snapshot.
- `eval/lib/backends/base.py:40-84` tails stdout in a daemon thread and changes
  readiness state when it sees a marker.
- `eval/lib/backends/ray.py:90-93` waits for
  `[Driver] ALL SERVICES READY`.
- `tests/test_driver.py:32-49` positively tests the forbidden marker protocol
  instead of asserting that stdout cannot alter lifecycle state.

The legacy predicate also retains false-positive paths:

- `server.py:1888-1915` uses aggregate GPU count rather than exact planned
  node/resource identities.
- `server.py:2251-2266` accepts a nonempty all-healthy proxy set without
  matching it to the exact planned proxy identities.
- Planner and multi-model branches can reach the common ready marker without
  the single-model proxy check.
- `server.py:2197-2203` treats a `ray.wait()`-completed reference as success
  without `ray.get()`; a completed exception can count as ready.
- `EXASERVE_ALLOW_DEGRADED_GPUS=1` and
  `EXASERVE_ALLOW_DEGRADED_PROXIES=1` can bypass failed predicates, after
  which the code still prints **FULLY READY** rather than a typed DEGRADED
  state.
- Driver post-marker checks are static health requests, not one inference
  canary per model through the externally advertised gateway.

After READY, `server.py:2337-2341` only waits for shutdown. It does not
reconcile replica, Ray membership, engine lease, compatibility receipt,
route, proxy, or canary state. READY therefore cannot be revoked after a
component loss.

**Impact**

Log text, partial membership, failed proxy RPCs, missing compatibility
receipts, or stale resources can still produce a successful-looking
deployment. AC-CTL-01, AC-RDY-01, AC-RDY-02, PR-008, and KI-D1 are not closed.

### IMP-B03 — Essential-child supervision and shutdown remain incomplete

**Evidence**

- The Ray head is started at `driver.py:573-575` but is not polled in the
  service-lifetime loop at `driver.py:674-690`.
- An unexpected Serve exit with code zero breaks that loop; only a nonzero
  code is later converted to failure.
- A worker Ray process that exits unexpectedly with code zero is treated as
  successful completion at `driver.py:717-724`.
- If Serve exits zero in the same poll interval that the proxy exits nonzero,
  the Serve check runs first and can hide the proxy failure.
- Ray and Serve children are not launched into explicitly owned process
  groups; cleanup signals only direct child PIDs.
- Server SIGTERM/SIGINT handlers are installed only after READY, so partial
  startup uses default abrupt termination.
- `serve.shutdown()` failure is logged and swallowed.
- `launch_cluster.sh` installs only an `EXIT` trap, with no TERM/INT forwarding
  contract or process-group cleanup.
- A failing final MPI launch can skip the ordinary Copper cleanup due to
  `set -e`.

**Impact**

An essential long-lived child can disappear while the job survives, some
unexpected exits can remain scheduler-visible success, descendants can
escape cleanup, and secondary cleanup errors can obscure the first cause.
PR-001, PR-009, PR-028, and AC-SUP-01 remain incomplete.

### IMP-B04 — Compatibility activation is fail-open and has no receipts

**Evidence**

- The canonical `compat/profile.py`, `compat/activator.py`, and
  `compat/receipt.py` components do not exist.
- `src/exaserve/patches/__init__.py:103-147` defaults `apply_all()` to
  `strict=False`, suppresses mismatch output unless verbose, catches import
  failure, and still sets `_apply_all_done = True`.
- `src/exaserve/server.py:35-36` calls that fail-open default.
- Individual `_sitecustomize.py` patch routines catch import/API failures and
  return, so even an outer `strict=True` cannot establish that every required
  patch activated and passed a postcondition.
- The spawned vLLM engine shim catches activation exceptions and continues.
- `ray_start.py` imports private Ray APIs before a compatibility activator can
  attest them.
- Overlay setup verifies Ray version and only a subset of files, permits an
  ambient override, and does not verify a complete source/patch/profile hash.
- `pyproject.toml` permits arbitrary newer Ray/vLLM releases.
- `compatibility_receipt_hash` is only a field in a prototype observation;
  there is no receipt producer, per-role receipt collection, external-daemon
  attestation, or READY receipt predicate.

The active vLLM patches also contain correctness-risk fallbacks:

- `_find_backend_fallback_layer()` and `_resolve_mapping_value()` can choose
  the first mapping entry when no exact match exists.
- Layer lookup can install a representative layer when a true match is absent.
- KV-cache binding logs and skips unmatched layers rather than failing.

**Impact**

A required compatibility change can fail or bind the wrong internal object in
one process while the deployment continues to READY. PR-026, KI-A5, the
STATUS “receipts” claim, and AC-COMP-01 are not closed.

### IMP-B05 — Model completeness and distribution can report false success

**Evidence**

- `model_staging.py:75-77` returns complete whenever
  `.exaserve_complete.json` exists; the marker contents and current files are
  not checked.
- The marker stores only top-level names and sizes, not content hashes, source
  revision, or a recursively complete inventory, and is never reconciled.
- A tokenizer-only download skips full-model validation but writes the same
  completion marker.
- Downloads use an unpinned repository revision.
- Publishing a replacement removes the old final directory before rename,
  so a previous valid generation is not preserved throughout publication.
- MPI broadcast extracts directly into the final node-local target. It does
  not extract to a unique per-rank generation and atomically publish it.
- The completion marker can be extracted before the archive finishes or can
  survive interrupted extraction. The post-broadcast probe then trusts it.
- `run_cache_probe()` counts JSON-looking stdout lines; it does not validate
  exact unique rank/host/path/content/profile identities.

**Direct reproduction**

A valid model was checked once to create its marker, its only weight file was
deleted, and `check_model_exists()` still returned `True`. An empty marker by
itself is also sufficient.

**Impact**

An incomplete or corrupt model cache can be reused as complete on one or more
ranks. PR-005 and the WP6/AC-DIST completion guarantees are not closed.

### IMP-B06 — The control-channel prototype is unsafe and incomplete

The framing foundation is useful, but the listener does not yet implement the
canonical trust and lifecycle contract.

**Evidence**

- `transport.py:224-248` validates observation deployment/hash/generation but
  does not bind `owner_rank`, `node_id`, scope, role, or component identity to
  the authenticated session.
- An authenticated rank can therefore send another rank's observation or a
  GLOBAL observation.
- Malformed or wrong-scope observations are audited and skipped with
  `continue`, rather than terminating the offending session as the module's
  fail-closed contract states.
- `_register()` creates a new `SessionInfo` on reconnect, resets sequence and
  dedup state, and does not require a complete snapshot before incrementals.
- HEARTBEAT is discarded; there is no receiver-side lease timestamp, expiry,
  or watchdog deadline.
- COMMAND/COMMAND_RESULT dispatch and idempotency are explicitly deferred.
- `_all_registered` is set once and never cleared on disconnect.
- A second REGISTER on an already registered connection can leave a ghost
  registered rank because the local session reference is overwritten before
  `finally` emits disconnect.
- `stop()` closes only the listening socket, not active session writers, so a
  node watchdog cannot reliably infer supervisor loss.
- `seen_obs` and the listener audit list grow with event count, rather than
  remaining O(planned components).
- Component sequence regressions and superseded instance IDs are not rejected.
- Contract validation is shallow: malformed field types, empty identities,
  non-finite timestamps, and booleans-as-integers are not consistently
  rejected as typed `ContractError`s.

**Direct reproduction**

- Rank 0 successfully published a GLOBAL `READY` observation owned by `head`.
- After the only rank disconnected, `wait_all_registered()` still returned
  `True`.

**Impact**

The prototype cannot be promoted into the readiness authority without fixing
identity binding, reconnection, snapshot replacement, command, watchdog,
bounded-state, and fail-closed semantics. AC-CTL-01 and AC-RDY-02 remain open.

### IMP-B07 — Lease and status primitives do not provide safe concurrency

**Exclusive lease defects**

- Two takeover contenders can both observe expiry and both `os.replace()`
  their takeover file onto the lease. Rename does not elect exactly one
  winner.
- Lease identity has no unique fencing token.
- `release()` unconditionally unlinks the lease path without confirming that
  the current on-disk lease still belongs to that instance.
- Leases do not renew. Long model downloads, trace generation, and submission
  loops can outlive their TTL while still active.

**Status-store defects**

- `transition()` compares only expected state, not expected revision,
  generation, or an ownership token. An ABA-stale writer is accepted if the
  state cycles back to the same enum value.
- `initialize()` permits any enum state, including direct initialization as
  READY.
- Loaded records validate only schema version, not kind, record identity,
  state, revision, provenance, or typed component data.
- History grows without a retention policy.
- The store is not connected to the production serving lifecycle.

**Direct reproduction**

- An expired holder was replaced by a live successor; calling
  `old.release()` deleted the successor's lease while the successor still
  believed it held it.
- A deployment status record initialized directly in READY.
- A writer that observed READY at revision 5 successfully transitioned a
  later READY at revision 7 to DRAINING; the stale write became revision 8.

**Impact**

The concurrency foundation underneath model staging, trace generation,
run-group allocation, submission idempotency, and status publication is not
safe under expiry/takeover. WP2 and all closures relying on these leases need
revalidation.

### IMP-B08 — Eval can label partial or telemetry-invalid work successful

**Evidence**

- On replay exit code zero, stats collection failures are warnings and the run
  becomes `succeeded` (`eval/lib/run_executor.py:96-114`).
- `_validate_replay_results()` prints partial-completion failures but returns
  normally (`run_executor.py:318-345`).
- Distributed gather accepts missing ranks by default. Missing-rank failure is
  opt-in via `EXASERVE_EVAL_STRICT_COMPLETE=1`
  (`eval/lib/replay_engine.py:151-172`).
- `result_is_complete()` retains a legacy path in which missing gather metadata
  can be treated as complete.

**Impact**

Missing ranks, missing required stats, or partial replay can still produce a
successful run and contaminate analysis. PR-019, PR-021, KI-C4, and the WP9
result-state contract are not closed.

### IMP-B09 — Scheduler consolidation and durable submission are incomplete

**Evidence**

- `eval/lib/schedulers` explicitly describes itself as parallel to
  `src/exaserve/schedulers`; each defines a separate scheduler abstraction.
- Package default, eval default, and scheduler documentation disagree between
  PSI/J and PBS.
- `eval/lib/run_executor.py:578-590` submits first, then catches and ignores a
  failure to persist the scheduler job ID. A crash or rerun can submit the
  same logical run again.
- Existing idempotency tests cover an already-persisted submitted state, not
  the submit-success/state-write crash boundary.
- Core submission has the same submit-then-ordinary-job-ID-write window.
- Scheduler-count observation failure holds submissions, but loops forever
  without an overall deadline or terminal state.
- `_is_completed()` skips any submitted/running/replaying record without
  reconciling its job identity and live scheduler state, allowing stale runs
  to remain stranded forever.
- `EvalScheduler._body()` shell-quotes `code_root`, then embeds the resulting
  quoted text inside a double-quoted `PYTHONPATH` assignment. Single quote
  characters have no quoting effect inside double quotes, so `$()` or `$` in
  a path is expanded by the job shell.

**Direct reproduction**

For `code_root=/tmp/$(touch /tmp/SHOULD_NOT_EXIST)`, the generated line was:

```bash
export PYTHONPATH="'/tmp/$(touch /tmp/SHOULD_NOT_EXIST)':'/tmp/$(touch /tmp/SHOULD_NOT_EXIST)'/src${PYTHONPATH:+:$PYTHONPATH}"
```

The audit rendered this line only; it did not execute it.

**Impact**

PR-013, PR-015, PR-027, and WP8 are not closed. Submissions can duplicate at
the most important crash boundary, and untrusted path data can enter an
executable shell context.

### IMP-B10 — Closure records and release claims are not trustworthy

**Evidence**

- `doc/hardening/STATUS.md` says 34/35 audit findings are fixed and that no
  open item is a production blocker, then admits the normative WP4/WP5 rewrite
  and WP13 deletion remain.
- A direct ledger count is 82 records: 53 FIXED, 23 OPEN, 4 IN_PROGRESS, and
  2 OUT_OF_PRODUCTION_SCOPE. The status headline instead reports 50/6/19/1.
- FIXED entries PR-017, PR-035, KI-A7, and KI-B1 have evidence text saying
  required work is still owed.
- PR-012 is FIXED while KI-A2/TD-PORTS retain the same unresolved port-race
  invariant.
- PR-026 is FIXED while TD-SITECUST retains the same compatibility invariant
  as IN_PROGRESS.
- PR-019 is FIXED while KI-C4 retains silent partial gather as OPEN.
- ADR-001 says reconnect-with-snapshot was unit-tested, but the test sends a
  snapshot only on an initial connection; no reconnect occurs.
- ADR-000 remains PROVISIONAL while the migration log calls it finalized.
- STATUS says this is an uncommitted working-tree checkpoint, but the changes
  are committed as `e73f3eb`.
- The commit claims full scale evidence while the raw `artifacts/` tree is
  ignored and not present in the commit.
- Ledger records do not consistently carry the decision, evidence, fallback,
  residual-risk, support-impact, and revisit fields required by plan section
  8.

**Impact**

The ledger currently records labels rather than demonstrated gate closure.
Release decisions based on its headline would be unsafe. Reconcile the ledger
against reachable production code and actual acceptance evidence before using
it as a release artifact.

## 4. High-priority remaining defects

### IMP-H01 — The plan compiler is incomplete, noncanonical, and not deeply immutable

**Evidence and direct checks**

- `source_path` is included in canonical hash input. Identical intent stored at
  `/a/config.yaml` and `/b/config.yaml` produced different hashes.
- Frozen `GatewayPlan.options` retains caller-owned nested dict/list values.
  Mutating the input after compilation changed plan content while
  `plan_hash` remained unchanged.
- `ray_cluster_config`, `deployment_name`, and
  `replica_max_ongoing_requests` are accepted but discarded; changing
  runtime-significant intent can leave the hash unchanged.
- The top-level `envelope` key is accepted but ignored unless an out-of-band
  Python argument is supplied.
- Proxy options and scheduler keys are opaque and not strictly validated.
- `reservation_topology=False` bypassed an explicit scheduler/deployment node
  mismatch even though the field is declared as a named string.
- `strict_float()` accepted `"nan"`; non-finite values bypass bounds and can
  produce nonstandard canonical JSON.
- Arbitrary proxy values, including plaintext secrets, can enter the hashed
  plan; there is no `SecretRef`.
- Required `RunPlan` and `SiteProfile` schemas are absent. Eval defines a
  separate mutable `RunPlan`.
- `ScaleEnvelope` covers only node count and defaults support to 2 nodes while
  the support matrix claims 64.
- Active driver/server/model-broadcast paths still load the permissive legacy
  schema. Unknown keys and weak coercions therefore remain in production;
  for example `bool("false")` becomes `True` in some legacy fields.
- Matrix expansion can overwrite explicit scheduler/client node intent and
  does not revalidate every expanded allocation/deployment combination.

**Impact**

The hash is neither a stable identity of semantic intent nor guaranteed to
match runtime behavior. AC-PLAN-01 and the WP1 exit gate remain open.

### IMP-H02 — Distribution is not generation-isolated or receipt-based

**Evidence**

- Source, venv, and overlay trees use stable shared paths such as
  `/tmp/exaserve_src`, `/tmp/exaserve_venv`, `/tmp/overlay_patches`, and
  `/tmp/exaserve_overlay` across deployments.
- Extraction overlays existing trees; files removed in a newer source/profile
  can remain stale and importable.
- MPI launcher commands are stored and expanded as shell text rather than a
  validated argv vector.
- Clean-stage failure is explicitly ignored by the launcher.
- `bcast.c` now quotes paths and aggregates failures, but still constructs a
  tar shell command and invokes it with `popen()`.
- There are no exact per-rank source/profile/model receipts, missing/duplicate
  receipt rejection, rank-specific extraction-failure injection, or atomic
  generation publish.

**Impact**

A run can mix stale source, overlay, environment, and model generations while
appearing successful. PR-004, KI-A7, KI-D2, and AC-DIST-01 need narrower
status or reopening. The no-SSH distribution improvement is valid.

### IMP-H03 — The Bash launcher remains a fragile second control plane

`src/exaserve/resources/launch_cluster.sh` is still 533 lines and owns:

- allocation discovery and scheduler-specific behavior;
- environment and interpreter selection;
- runtime-config derivation;
- staging and cleanup policy;
- compatibility overlay activation;
- Copper lifecycle;
- MPI command construction;
- launch, logging, and finalization.

Additional defects include:

- `EXASERVE_MPILAUNCH` is command text expanded unquoted in multiple places;
- clean-stage failure is ignored;
- Copper startup failure can be ignored while active state is set;
- no explicit TERM/INT forwarding ownership;
- global mutable `/tmp` destinations;
- the control process can use an engine-specific interpreter rather than a
  stable ExaServe supervisor environment.

This script is still necessary only because the target Python supervisor and
scheduler adapter were not connected. Under the canonical design it should
become a thin environment adapter or disappear; it should not remain a
coequal lifecycle authority.

### IMP-H04 — Gateway, API, and port contracts remain incomplete

**Evidence**

- Production-boundary enforcement is opt-in through
  `EXASERVE_PRODUCTION_BOUNDARY=1`; default behavior only warns.
- External inference binds are hard-coded to wildcard interfaces.
- `GatewayPlan` does not type TLS, authentication, bind address, allowed
  models, body limits, timeout policy, or secret references.
- HAProxy health is a TCP-connect check; a listening proxy with no viable
  backend can pass.
- HAProxy uses requested-port probing rather than bind-to-zero plus descriptor
  ownership/handoff, retaining a check-then-bind race.
- `http_no_delay` and `abortonclose` use `bool(value)` rather than the available
  strict boolean helper, so YAML string `"false"` becomes true.
- The CLI `serve_url()` looks for `proxy_out/proxy_port` beside the source
  config, while the launcher writes it beside the run-scoped runtime config.
  It will commonly miss the actual port and fall back to the requested port.
- Request handlers call `validate_model_field()` before confirming the JSON
  body is an object. A list body raised `AttributeError`; a list-valued model
  raised `TypeError`, escaping the typed 400 path.
- `stream`, `ignore_eos`, `add_generation_prompt`, and
  `continue_final_message` use truthiness in places, so string `"false"` can
  become true.
- Request/body/concurrency limits and production authentication are absent.

**Impact**

PR-010, PR-011, PR-012, and the full WP7 capability/readiness gate remain
incomplete, even though HAProxy admin lockdown and native config validation
are genuine improvements.

### IMP-H05 — Telemetry and request correlation are incomplete

**Evidence**

- Telemetry scope falls back to scheduler job ID rather than a unique
  deployment ID.
- Detached collectors use `get_if_exists=True`, allowing stale state reuse.
- Serving push loops are unbounded/best-effort and serving collector cleanup
  is absent.
- Init-collector exception paths can return without killing the detached
  actor.
- Chat streaming and non-streaming responses do not consistently echo
  `X-Request-ID`; completion streaming also omits it.
- The engine receives the generated completion ID, not the caller's
  correlation ID.
- No engine-spy/structured-log acceptance test establishes end-to-end
  gateway-to-engine identity.

**Impact**

PR-029, PR-032, AC-TEL-01, and AC-OBS-01 are not closed.

### IMP-H06 — Structured writes are only partially atomic

**Evidence**

- PR-035's own ledger evidence says ClientLab, scaling-trace, and replay
  writers remain owed.
- ClientLab utilities still use truncate-and-write JSON/YAML output.
- Eval replay and server-stats paths still directly write results.
- Core job scripts and scheduler job IDs are non-atomic.
- Driver proxy-port and merged-trace files are non-atomic.
- Proxy configs are generated and rewritten in place.
- Model-broadcast timing metadata is directly written.

**Impact**

Interruption can leave successful-looking partial metadata or lose previous
valid state. PR-035 must be reopened or narrowed to only the helpers actually
migrated.

### IMP-H07 — CI is not yet a clean release gate

**Evidence**

- The randomized CI job invokes `pytest -p randomly`, but `pytest-randomly`
  is absent from `[project.optional-dependencies].dev`. A clean runner does
  not receive that plugin from `pytest`.
- Test jobs run from an editable checkout rather than from the built wheel.
- The wheel job imports a few modules but does not run the test suite against
  the installed artifact.
- The job named “format + lint + types” runs no formatter and no type checker;
  broader lint is non-blocking.
- There is no dependency lock/pin gate, security/static analysis gate, or
  packaged-resource execution test.
- The 102 green tests omit the core AC-SUP/CTL/RDY/COMP/DIST high-cardinality,
  crash-boundary, revocation, and role-receipt cases. Some tests positively
  assert marker-based behavior that the production plan prohibits.

**Impact**

PR-031 has implementation gaps beyond repository-admin enablement. The green
suite is a useful regression baseline, not a production acceptance suite.

### IMP-H08 — The 64-node result is useful smoke evidence, not qualification

The local artifacts report credible narrow results:

- 16 nodes: 10,821 successes, 0 errors, 344.4 aggregate RPS,
  21.53 RPS/node, p99 1.59 s;
- 64 nodes: 43,207 successes, 0 errors, 1,373.9 aggregate RPS,
  21.47 RPS/node, p99 1.585 s, 768/768 GPUs.

The broader “production-qualified and regression-free” claim is unsupported:

- `artifacts/` is gitignored and no raw artifact is tracked in the audited
  commit.
- The local experiment plan says a 64-node row will be added when P06 opens;
  the run happened without that required pre-run row.
- Required per-run `manifest.yaml`, `command.txt`, `environment.txt`, and
  `verdict.md` files are absent.
- The script intentionally tests the live mutable tree, launches the legacy
  Bash path, and waits on a marker.
- The 64-node config uses `proxy_config.type: none`, not the production HAProxy
  envelope.
- The harness is not consistently fail-closed and uses broad cleanup commands.
- Throughput aggregation accepts any nonzero shard count rather than the exact
  expected rank set.
- Logs contain recovered `EADDRINUSE` failures, consistent with still-open
  port-race KI-A2.
- The comparison baseline used a different workload/concurrency, so this is
  not same-workload A/B proof that hardening added no penalty.
- Most importantly, the run exercises the legacy architecture, not the target
  supervisor/readiness/compatibility architecture.

Preserve these results as **direct-mode weak-scaling feasibility smoke**.
Do not use them to close WP12, AC-SCALE-01, production gateway qualification,
or target-architecture no-regression gates.

## 5. Verification performed

### Existing suite

```text
python -m pytest -q
102 passed in 29.63s
```

The test was a lightweight login-node check after loading the required Aurora
frameworks and Go modules. No GPU, MPI, Ray, or long-running service workload
was launched.

### Focused hermetic checks missing from the suite

| Check | Observed result |
|---|---|
| Mutate nested gateway option after compiling a frozen plan | Plan content changed; hash stayed unchanged |
| Compile identical intent from two source paths | Hashes differed |
| Use `reservation_topology: false` to permit node mismatch | Accepted |
| Use `gpu_memory_utilization: nan` | Accepted |
| Delete model weight after completion marker creation | Model still reported complete |
| Replace expired lease, then release old holder | Old holder deleted successor's live lease |
| Send list request body / list-valued model | Escaped as `AttributeError` / `TypeError` |
| Render scheduler body with `$()` in code root | Command substitution remained active inside double quotes |
| Rank 0 send GLOBAL READY observation | Accepted by listener |
| Disconnect the only registered rank | `wait_all_registered()` remained true |
| Initialize deployment status directly as READY | Accepted |
| Use a stale READY writer after READY→VALIDATING→READY | Stale transition accepted |

These checks used temporary directories and loopback sockets only.

## 6. Ledger corrections required before further implementation claims

At minimum, the following records must be reopened or narrowed to the exact
incremental behavior that is genuinely complete:

- **IN_PROGRESS / reopen:** PR-001, PR-004, PR-005, PR-006, PR-008, PR-009,
  PR-010, PR-011, PR-012, PR-013, PR-015, PR-017, PR-018, PR-019, PR-021,
  PR-026, PR-027, PR-028, PR-029, PR-031, PR-032, PR-033, PR-035.
- **Reconcile corresponding Known Issues/TODOs:** KI-A2, KI-A5, KI-A7,
  KI-C4, KI-D1, KI-D2, TD-PORTS, TD-SITECUST, and TD-TESTS.
- **Retain narrow FIXED evidence where appropriate:** source-config copy
  (PR-003 slice), derived model identity collision checks (PR-007), AST
  expression restriction (PR-016), required-model default placement failure
  (PR-023), HAProxy admin lockdown/config preflight (parts of PR-010/PR-024),
  and the narrow ClientLab defaults/argv repair in PR-030.

Do not use a `FIXED` status when the record's own evidence says work remains.
If an improvement closes only one subcase, split or narrow the record rather
than closing the parent invariant.

## 7. Improvements worth retaining

The following changes should survive the corrective implementation:

- source configuration is copied before runtime head-IP mutation;
- `atomic_write_*` correctly performs same-directory temporary publication,
  flush/fsync, and `os.replace` for a single writer;
- HMAC framing, canonical envelope encoding, and message-size bounds are a
  sound control-transport foundation;
- explicit lifecycle enums and transition tables are useful foundations once
  revision/generation CAS and production wiring are added;
- strict boolean parsing, derived storage/route collision checks, and a subset
  of model-placement checks are useful plan-compiler foundations;
- indexed model-shard completeness is checked before first marker creation;
- native broadcast now bounds buffers, quotes tar arguments more safely,
  checks producer/extractor status, and aggregates rank failure;
- active source distribution no longer relies on SSH fan-out;
- several driver/proxy nonzero exits and readiness timeouts now fail more
  loudly;
- HAProxy stats default to loopback/read-only, admin requires authentication,
  and `haproxy -c` preflight is present;
- AST-restricted eval expressions, trace content hashes, and numeric result
  ordering are meaningful incremental repairs;
- the 16/64-node direct smoke results are useful feasibility evidence when
  described narrowly and preserved with complete provenance.

## 8. Correct next step

Claude Code should continue to follow
`doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md` faithfully. This audit should be
used only to correct false closures and add missing regression/acceptance
cases.

The safest order remains the canonical packet order:

1. Correct the ledger and status claims; restore unresolved invariants to
   IN_PROGRESS.
2. Repair plan identity/deep immutability and fenced lease/status primitives
   before building more consumers on them.
3. Implement the single compatibility profile, fail-closed activator, typed
   per-role receipts, and receipt-gated readiness.
4. Implement and wire the outer supervisor, rank/node supervisors, deployment
   manager, authenticated command channel, and readiness coordinator.
5. Make model/source/profile staging generation-isolated, transactional, and
   receipt-based.
6. Move scheduler submission, eval, and ClientLab onto the same plan and
   lifecycle contracts.
7. Reduce Bash to a thin site adapter, remove marker consumers, and delete the
   legacy orchestration path only after parity gates pass.
8. Run the canonical acceptance suites and only then repeat qualifying Aurora
   scale/gateway evidence with complete predeclared provenance.

Do not patch the legacy path until it superficially resembles the target and
then call the architectural work optional. The production acceptance gates
must execute through the new path, and the old path must stop being an
independent authority.
