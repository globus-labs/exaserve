# Final audit — final43 evidence and release/v0.4.0 successor

**Audit date:** 2026-08-09; successor reconciliation 2026-08-30

**Verdict:** final43 implementation pass at one and two Aurora nodes;
release/v0.4.0 source and clean package gates pass, exact-candidate scale
qualification pending.

This audit covers code reached by a production deployment. Earlier audit,
feasibility, Known Issues, and TODO documents remain context; the architecture
authority is `doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md` and the evidence-bound
dispositions are in `doc/hardening/FINDINGS.yaml`.

## Successor reconciliation

The clean preview lineage through `ccccb82` contains the runtime corrections
exercised by the 4/16/64-node paper preview, including native HeadOnly,
isolated LiteLLM, exact readiness/recovery, immutable staging, and bounded
scale cleanup. The release branch preserves that lineage and incorporates the
previously separate scale qualification/adjudication tooling and preview
ledger.

The final release audit additionally closed four locally actionable gaps:

1. oversized requests are rejected with a correlated HTTP 400 before JSON or
   SSE response commitment;
2. only explicitly classified incremental SSE may report TTFT/TBT, while
   LiteLLM buffered and non-streaming results retain non-token timing evidence;
3. exposure fields are either implemented by HAProxy or rejected; and
4. READY leases use monotonic time fenced by Linux boot identity, while mixed
   scheduler groups fail before submission.

These source changes intentionally invalidate old plan/result identities for
release qualification. Final43 receipts and the multi-snapshot paper preview
are retained evidence, not proof for the successor wheel. A new clean package
gate and owner-approved 4/16/64 ladder are required before changing the
production support maximum.

The reconciled successor source gate in validated PBS job `8792581` passes
1,323 tests with 19 skips, repository-wide Ruff and compileall, the canonical
findings validator, the scale adjudicator/harness tests, and Go formatting,
vet, and unit tests.

The final clean package built from source commit `a920d7e3` also passes in
validated PBS job `8792605`: 1,302 tests pass with 31 optional-plugin skips,
packaged resources are present, and the four-module typed core is mypy clean.
The wheel SHA-256 is
`75bab1c97948d29237000d5fa087f3ed1f1d18e5ef3233fe2c759b4faf0e32f8`;
the sdist SHA-256 is
`8e8b0a6af44ad75bc48004dba48b66fc382e451d8e44449b55ec36dd8cd6bcb3`.

## 1. Architecture outcome

The migration implements the selected design:

- one allocation-head Python composition root owns all global lifecycle state;
- one node supervisor per planned rank owns only its local Ray daemon;
- subprocesses remain at genuine OS/native fault boundaries and are supervised
  by argv, exact PID/start-time receipts, process groups, deadlines, and typed
  control messages;
- Ray startup output is never parsed for READY or failure decisions;
- serving, eval, ClientLab, scheduler rendering, and result identity share one
  strict immutable plan family;
- READY is generation-bound, exact-set based, continuously evaluated, and
  revoked after component or canary loss;
- compatibility uses an exact-hash generated overlay plus the narrowly scoped
  spawned-engine bootstrap; installed framework files are untouched;
- source/model staging, telemetry, result publication, status history, and
  cleanup are bounded and fail closed; and
- scheduler shell is only an environment/setup wrapper around a Python entry
  point, never a lifecycle authority.

The intentionally isolated deployment child remains necessary for the pinned
Ray/Serve stack: its public synchronous lifecycle calls have no cancellation
token and cannot isolate a native fatal exit. It is one typed fault boundary,
not a return to a subprocess-driven control architecture.

## 2. Candidate and reproducibility

| Artifact | SHA-256 |
|---|---|
| wheel | `1041be53eb5b5875d198d5ee6c6664718b4085775dcba99107873dd3d1fcdff2` |
| sdist | `94d9b10a0f47603a3c847ec55cdd74f154d42b08bb9124fef1d730da4defcee6` |
| artifact manifest | `0c8fcf3025fb3dff9b6ea4aa22c325ddef33bf5ddc06068379fee04456033cc8` |
| candidate review | `763dfdf04e72ef6b42a35277d362c275e64dcd66b477457e87c5235045638c6d` |
| site profile | `4814429547fd4397014819a0f8b5c6ec8f7d77c889eaf844d27935b39a0a6e26` |
| compatibility profile | `c17e684fe485261a9cfa82248bd24a9209b66a7c66bae8b889b24ca878d335d3` |
| compatibility manifest | `cd85123822f4b936216282ed43346223a4b68f1a7cb152a85715a36fdab24259` |

The immutable release is `artifacts/hardening/release-20260809-final43`.
`artifacts/hardening/final43-candidate-review.json` binds the exact artifact,
package receipt/log hashes, campaign plan hashes, result and cleanup hashes,
expected scenarios, and unresolved dispositions. The verifier also compares
every installed bootstrap package member byte-for-byte with the wheel.

The generic adjudicator independently checks those declarations rather than
compiling a candidate name into code. It refuses changed plan/result bytes,
candidate drift, incomplete toggle/cell matrices, false READY, wrong first
cause, malformed proxy metrics/workload identities, or cleanup survivors.

## 3. Verification results

### Package and source

- final working-tree source suite: **1224 passed**;
- exact installed-wheel pytest: **1215 passed, 9 skipped**;
- exact installed-wheel mypy over the typed contract core
  (`plan/contracts.py`, `control/contracts.py`, `telemetry.py`, and
  `state/results.py`, with imports skipped): **no issues found**;
- Ruff formatting, lint, and security rules: pass;
- Python compileall: pass;
- Go formatting, vet, and tests: pass; and
- installed package resource/import checks from outside the source tree: pass.

Evidence:

- `artifacts/hardening/final43-final-source-gate-20260809-a1/pytest.log`;
- `artifacts/hardening/final43-packaged-gate-20260809-a1/`;
- `artifacts/hardening/final43-final-static-gate-20260809-a1/`; and
- `artifacts/hardening/release-20260809-final43/artifact_manifest.json`.

The final43 package gate ran from a clean installation of the exact wheel and
its receipt is included in the candidate review. The final replay also includes
the generic adjudicator's bootstrap, repository-bound manifest, and immutable
cleanup-evidence checks.

### Aurora campaigns

Every campaign below used `validation_mode=true`: the selected SiteProfile is
not allowed to cross the production execution boundary until its limits are
evidence-backed and the release envelope is approved. These were still
HAProxy/`PROXIED_INTERNAL` runs over the same serving, readiness, receipt, fault,
and cleanup code. Validation mode only permits a non-native explicit allocation
identity and records `production_qualified=false` in gateway evidence; the
positive production-qualification branch remains unexercised on hardware.

| Nodes | Engine/topology | Gate | Result |
|---:|---|---|---|
| 1 | null | `FQ-FINAL43-1N-NULL-XPU-20260809` | PASS |
| 1 | real vLLM/XPU TP=1/PP=1 | `FQ-FINAL43-1N-REAL-XPU-20260809` | PASS |
| 2 | null, two planned replicas | `FQ-FINAL43-2N-NULL-XPU-20260809` | PASS |
| 2 | real vLLM/XPU PP=2 across two hosts | `FQ-FINAL43-2N-REAL-XPU-20260809` | PASS |
| 1 | real, HAProxy no-delay on | `FQ-FINAL43-PROXY-NODELAY-ON-1N-20260809` | PASS |
| 1 | real, HAProxy no-delay off | `FQ-FINAL43-PROXY-NODELAY-OFF-1N-20260809` | PASS |
| 2 | strict supervisor/watchdog faults | `FQ-FINAL43-SUPERVISOR-WATCHDOG-V3Q2-2N-20260809` | PASS |

The two-node null campaign proves normal drain, gateway death, authenticated
worker-Ray loss, duplicate gateway-port fail-before-READY, and partial worker
proxy non-readiness. The real PP=2 campaign records one EngineCore and two
engine-worker instances on two physical hosts. The paired proxy arms execute a
bounded real-engine client workload and derive process/TCP/connection
diagnostics from exact samples.

The supervisor campaign kills the SELF-attested rank-zero `ray_head` child and
the SELF-attested rank-one node supervisor. It preserves respectively:

- `rank 0 component ray: exit=137`; and
- `authenticated rank control session disappeared without GOODBYE for rank(s) [1]`.

Both return nonzero (not operator-drain 143), publish exactly one FAILED
terminal record, perform clean bounded shutdown, and produce two-node cleanup
reports with no matched processes, signals, or survivors.

## 4. Production bugs corrected in this final pass

The last source audit found classes not covered by the earlier completion
claim and fixed them before final43:

- readiness/control identities now reject booleans, coercion, unplanned routes,
  stale generations, conflicting duplicates, unissued command waits, and
  instance-fencing drift;
- high-cardinality readiness projections are indexed and bounded rather than
  repeatedly copying full histories;
- status, deployment, telemetry, diagnostics, and pending-command histories
  have explicit retention limits and strict durable-load validation;
- listener/reader startup is transactional and shutdown is idempotent;
- process cleanup kills only older processes for the same deployment identity,
  preserving equal/newer generations and other deployments;
- explicit deployment IDs are byte-exact through observability contracts;
- empty or malformed exception messages retain a deterministic first cause;
- supervisor and deployment cleanup preserve primary and secondary failures;
- persisted plans rederive gateway/exposure and exact compatibility-receipt
  invariants on load rather than trusting compiler-only checks;
- readiness-monitor failures preserve their typed first cause while owned
  gateway death remains classified by the process supervisor; and
- the candidate adjudicator now verifies the current candidate and strict
  supervisor evidence instead of silently remaining hardcoded to final35.

## 5. Residuals and non-claims

The pinned Ray driver can continue retrying failed GCS/task notifications until
its 120-second reconnect timeout after injected head/worker loss. ExaServe
classified and persisted the owner-level cause before that dependency delay;
the campaign stayed inside its deadline and cleanup proved zero survivors. This
is bounded shutdown latency, not a claim of immediate recovery.

Aurora also emitted optional Ray metrics-exporter warnings. Exporter health is
not a READY input, and owned readiness, canary, status, cleanup, and ExaServe
metrics contracts passed.

Unproven dimensions are:

1. any final43 run above two nodes;
2. the unapproved proposed 64-node release ceiling and its 4/16/64 ladder;
3. native Slurm plus CUDA/ROCm;
4. streaming, public exposure, and non-HAProxy production gateways; and
5. SGLang under the selected Aurora profile.

Historical 4/16/64/128/256-node runs and earlier candidates are regression
context only and cannot qualify final43.

## 6. Ledger and release decision

The canonical ledger contains **89 FIXED / 8 IN_PROGRESS / 1
EXTERNAL_BLOCKER / 3 OUT_OF_PRODUCTION_SCOPE** records. There are no implicit
waivers, self-approved limits, or unresolved locally actionable code findings
within the qualified dimensions.

Do not label final43 generally production-ready. It is the immutable,
technically qualified one/two-node candidate. A product owner must approve the
release envelope, authorize the corresponding immutable scale campaign, and
accept its receipts before a broader production release verdict can be issued.
