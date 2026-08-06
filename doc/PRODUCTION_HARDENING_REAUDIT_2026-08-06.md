# Production-Hardening Live Re-audit

**Audit date:** 2026-08-06  
**Frozen committed revision:** `a6f04bb498e6914c8a9cb401a68f27d3085206fd`  
**Dirty-tree observation time:** `2026-08-06T17:51:36Z`  
**Overall verdict:** **NOT PRODUCTION READY**

This is a point-in-time re-audit while another implementation agent is still
working. It is evidence, not an alternative specification. The sole
architecture and implementation authority remains
`doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md`.

The dirty files observed at the freeze were:

- `src/exaserve/cli.py`
- `src/exaserve/resources/launch_cluster.sh`
- `src/exaserve/control/deployment.py` (untracked)
- `src/exaserve/control/node_supervisor.py` (untracked)
- `src/exaserve/control/rank_launcher.py` (untracked)
- `src/exaserve/supervisor_main.py` (untracked)
- `tests/test_rank_topology.py` (untracked)

Uncommitted work was inspected, but it is not credited as a closed production
contract. Its partial progress is reflected below.

**Post-freeze observation:** at `2026-08-06T17:52:46Z`, `server.py` also
became dirty and began instantiating `DeploymentManager`. That does not change
the count: it labels already-completed staging/deploy operations after the
fact, mutates private callback fields, and still installs no post-READY
`observe()`/reconciliation loop or unified ownership/control path. It is
additional IMP-B01/B02 substrate, not closure.

## 1. Count

Using the same 18 grouped findings from
`doc/PRODUCTION_HARDENING_IMPLEMENTATION_AUDIT.md`:

| Disposition | Count |
|---|---:|
| Fully resolved | **0** |
| Partially addressed, still release-blocking | **17** |
| Open, still release-blocking | **1** |
| **Total still remaining** | **18 of 18** |

This count is intentionally stricter than counting repaired unit-level bugs.
Of the previous audit's 12 narrow reproductions, 11 now behave as intended;
the status-store ABA case remains possible when callers omit
`expected_revision`. New negative reproductions also uncovered contracts not
covered by those original 12 cases. A finding closes only when its complete
canonical invariant is implemented on the reachable production path and has
acceptance evidence.

## 2. Finding-by-finding disposition

| Finding | Current disposition | Decisive remaining gap |
|---|---|---|
| IMP-B01 target architecture wiring | **PARTIAL** | The in-flight allocation supervisor still launches `exaserve.driver`; `NodeSupervisor`, `DeploymentManager`, the listener, readiness coordinator, durable status, and gateway are not wired into one production ownership tree. |
| IMP-B02 readiness authority | **PARTIAL** | Expected membership is inferred from observed live Ray/Serve state, startup is checked once, READY is not reconciled/revoked, snapshot persistence is best-effort, and driver/eval still accept stdout markers. |
| IMP-B03 essential-child supervision | **PARTIAL** | The generic supervisor is useful, but production ranks still use the legacy driver. Rank-local supervisors do not own Ray/deployment children, and a zero-exit aggregate can still be accepted. |
| IMP-B04 compatibility activation | **PARTIAL** | Receipts are role-presence checks rather than exact planned-instance checks; `not_applicable` satisfies required patches; owner fallback can replace missing engine self-attestation; daemon/replica coverage is incomplete. |
| IMP-B05 model completeness | **PARTIAL** | The marker now notices missing listed files, but validation is top-level and size-only. Same-size corruption, incomplete inventories, unpinned source revisions, staging races, and unsafe final replacement remain. |
| IMP-B06 control channel | **PARTIAL** | Authentication/dedup improved, but the channel has no production consumer, no reconnect full-snapshot rule, no command/result path, no heartbeat expiry watchdog, and incomplete active-connection shutdown. |
| IMP-B07 lease and durable status | **PARTIAL** | Sequential takeover improved, but renew/release remain check-then-mutate races; status revision fencing is optional and not integrated into the production lifecycle. |
| IMP-B08 eval correctness | **PARTIAL** | Partial state exists, but missing/stale gather data can pass, only one topology arm is validated, partial can return scheduler exit 0, and required stats can silently fail. |
| IMP-B09 scheduler/submission | **PARTIAL** | Pre-submit intent reduces duplication, but state-persist failure has no reconciliation, `submitting` without a job ID can be skipped forever, and two scheduler stacks remain. |
| IMP-B10 ledger truthfulness | **PARTIAL** | Arithmetic is now consistent, but several records are falsely closed and the claim that all 82 records contain the plan section 8 fields is false: direct parsing found 0 of 82 complete. |
| IMP-H01 immutable typed plans | **PARTIAL** | Important runtime fields are accepted then discarded from the plan/hash; direct `ScaleEnvelope` construction bypasses validation; production/eval still lack one shared RunPlan/SiteProfile contract. |
| IMP-H02 generation-isolated distribution | **PARTIAL** | Source publication improved, but venv/overlay/model targets remain stable in-place paths and no exact per-rank content/profile/model receipt aggregation exists. |
| IMP-H03 Bash cutover | **OPEN** | The 557-line script still owns orchestration, staging, model distribution, config mutation, Copper, MPI helpers, cleanup, logging, and finalization; it does not reduce to a site adapter ending in one `exec`. |
| IMP-H04 gateway/API/security | **PARTIAL** | Boundary hardening is ambient opt-in; wildcard binding, port TOCTOU, TCP-only health, incomplete typed request limits/security, and cross-deployment port discovery remain. |
| IMP-H05 observability/request IDs | **PARTIAL** | Scoped producer and unscoped stats consumer disagree; chat/stream responses omit request-ID echo; caller IDs do not reach the engine; detached telemetry cleanup and bounded failure behavior remain. |
| IMP-H06 atomic artifacts | **PARTIAL** | Atomic helpers exist, but readiness and numerous production/eval artifacts still use best-effort or truncate/write publication. READY may proceed after readiness snapshot persistence fails. |
| IMP-H07 CI/release gates | **PARTIAL** | Randomization and tests improved, but CI still lacks installed-wheel execution, actual format/type/security gates, dependency locking, and acceptance-level failure injection. |
| IMP-H08 scale qualification | **PARTIAL** | New 2/16-node runs are useful smoke evidence, but 64-node target-path evidence and required manifests/provenance/repetitions are absent; artifacts are not durable qualification records. |

## 3. Assessment of Claude Code's final two work areas

### Engine self-attestation

Commit `a6f04bb` is real progress, but it does not close IMP-B04:

- absence of an engine self-receipt still falls back to owner attestation in
  `server.py`;
- one receipt can satisfy the entire `engine` role, regardless of planned
  replica or EngineCore cardinality;
- all required patches, including `EN-01`, may be reported as
  `not_applicable`, and `ReceiptStore` treats that as complete;
- receipt identity is not bound to exact plan, executable, manifest, replica,
  and engine instance identities.

The correct next step is not another role-level receipt. It is exact
plan-derived receipt cardinality with fail-closed self-attestation for every
process boundary that can import affected code.

### Python rank topology / Bash reduction

The in-flight topology files establish useful types and ownership vocabulary,
but do not yet implement the topology described in their docstrings:

- `supervisor_main.py` launches `python -m exaserve.driver`, not a per-rank
  `NodeSupervisor` entry point;
- the production `rank_result_check` is wired to `lambda: None`, so the typed
  second failure signal does not exist;
- `NodeSupervisor`, `DeploymentManager`, and `ControlListener` have no
  production consumers;
- the custom `NodeSupervisor.supervise()` loop does not observe the nested
  supervisor's signal/shutdown flag;
- each rank reports the same logical component ID `ray`, which can collide in
  readiness indexing;
- the shell invokes `supervisor_main` normally rather than replacing itself
  with the one required `exec`, and still owns most lifecycle work.

This work should therefore be described as topology substrate, not the WP13
cutover.

## 4. Highest-priority newly confirmed defects

1. **Serving stats can fail silently.** The server creates
   `ServingStatsCollector:<scope>`, while eval looks up
   `ServingStatsCollector`. The collector returns an error dictionary rather
   than raising, and the executor ignores the returned error, so a run may be
   labelled successful without required stats.
2. **Published ports are not deployment-bound.** `serve_url()` selects the
   newest recursive port artifact rather than an artifact tied to the
   requested scheduler job/deployment, so it can advertise another concurrent
   deployment's port.
3. **Readiness can omit missing planned members.** The gate derives expected
   nodes, applications, replicas, and routes from what is currently observed.
   A completely absent planned member can therefore shrink the expected set
   instead of blocking READY.
4. **A stale lease owner can delete its successor.** Token validation and
   unlink/write are separate operations in renew/release, leaving a takeover
   race despite the sequential stale-release repair.
5. **Model corruption with unchanged size is accepted.** Marker validation has
   no content hashes or pinned source revision and can also accept an
   incomplete hand-written inventory.
6. **Result validation is not generation-bound.** Stale result files and
   missing gather metadata can pass without a result manifest/freshness
   contract.
7. **Ledger completeness is overstated.** The status file's section 8 field
   claim is objectively inconsistent with the YAML records and several FIXED
   dispositions, notably readiness, request IDs, and eval correctness.

## 5. Validation performed

- The full lightweight suite on the immediately preceding live snapshot
  (`b3c0857`) passed: **196 passed in 47.26 seconds**.
- The current in-flight topology test file passed: **22 passed in 2.78
  seconds**.
- Two focused re-audit selections passed **44/44** and **46/46** respectively;
  these counts overlap and are not summed.
- Direct negative reproductions demonstrated the remaining plan-hash,
  same-size model corruption, lease takeover, optional-revision ABA,
  compatibility cardinality, eval completeness, and cross-deployment port
  defects described above.
- No MPI, GPU, or cluster experiment was launched. This was a source and
  lightweight hermetic audit on the login node.

Green unit tests are not evidence that the 18 groups are closed: most current
failures are missing end-to-end contracts or unwired production paths.

## 6. Release decision

Do not promote the current tree as production-ready. Continue to follow
`doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md` faithfully, and require, for each
finding, all three of:

1. the invariant is implemented on every reachable production consumer;
2. negative/failure-injection acceptance tests demonstrate fail-closed
   behavior; and
3. durable evidence is recorded before changing the ledger disposition to
   `FIXED`.

The immediate implementation order should be: finish the real ownership-tree
cutover and authoritative plan-derived readiness; make compatibility receipts
exact and fail-closed; then repair eval/submission/telemetry identity and
artifact contracts before running final 2/16/64-node qualification.
