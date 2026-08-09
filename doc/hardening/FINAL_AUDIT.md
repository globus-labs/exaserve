# Final audit — final42 production-hardening candidate

**Audit date:** 2026-08-09

**Verdict:** implementation pass at one and two Aurora nodes; release scope and
larger-scale qualification pending.

This audit covers code reached by a production deployment. Earlier audit,
feasibility, Known Issues, and TODO documents remain context; the architecture
authority is `doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md` and the evidence-bound
dispositions are in `doc/hardening/FINDINGS.yaml`.

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
| wheel | `5346c7ab858b056448702b207b76350ac2ee134a65fa45ea67779039d41362e3` |
| sdist | `af1f9d75a6a6168806ef128df55bbfb1f4f29db84e4594146c6f0fa35d64eb7b` |
| artifact manifest | `0bc6f132a167bd5e0e8df2d6651b68217cee0d27a6359358bb5bec09a1d987e6` |
| site profile | `4814429547fd4397014819a0f8b5c6ec8f7d77c889eaf844d27935b39a0a6e26` |
| compatibility profile | `c17e684fe485261a9cfa82248bd24a9209b66a7c66bae8b889b24ca878d335d3` |
| compatibility manifest | `cd85123822f4b936216282ed43346223a4b68f1a7cb152a85715a36fdab24259` |

The immutable release is `artifacts/hardening/release-20260809-final42`.
`artifacts/hardening/final42-candidate-review.json` binds the exact artifact,
package receipt/log hashes, campaign plan hashes, result and cleanup hashes,
expected scenarios, and unresolved dispositions. The verifier also compares
every installed bootstrap package member byte-for-byte with the wheel.

The generic adjudicator independently checks those declarations rather than
compiling a candidate name into code. It refuses changed plan/result bytes,
candidate drift, incomplete toggle/cell matrices, false READY, wrong first
cause, malformed proxy metrics/workload identities, or cleanup survivors.

## 3. Verification results

### Package and source

- final working-tree source suite: **1216 passed**;
- exact installed-wheel pytest: **1207 passed, 9 skipped**;
- exact installed-wheel mypy: **no issues found**;
- Ruff formatting, lint, and security rules: pass;
- Python compileall: pass;
- Go formatting, vet, and tests: pass; and
- installed package resource/import checks from outside the source tree: pass.

Evidence:

- `artifacts/hardening/final42-final-source-gate-20260809-a3/pytest.log`;
- `artifacts/hardening/final42-packaged-gate-20260809-a4/`;
- `artifacts/hardening/final42-final-static-gate-20260809-a2/`; and
- `artifacts/hardening/release-20260809-final42/artifact_manifest.json`.

The first packaged-gate attempt (`a1`) exposed a real qualification-harness
bug: derived paths depended on the caller's current directory. That evidence is
retained as failed/partial history. The harness was anchored to the repository,
the supervisor campaign was re-predeclared with the new harness hash, and only
the current passing `a4` package receipt and q2 supervisor result are
adjudicated. The final replay includes the generic adjudicator's additional
bootstrap, repository-bound manifest, and immutable cleanup-evidence tests.

### Aurora campaigns

| Nodes | Engine/topology | Gate | Result |
|---:|---|---|---|
| 1 | null | `FQ-FINAL42-1N-NULL-XPU-20260809` | PASS |
| 1 | real vLLM/XPU TP=1/PP=1 | `FQ-FINAL42-1N-REAL-XPU-20260809` | PASS |
| 2 | null, two planned replicas | `FQ-FINAL42-2N-NULL-XPU-20260809` | PASS |
| 2 | real vLLM/XPU PP=2 across two hosts | `FQ-FINAL42-2N-REAL-XPU-20260809` | PASS |
| 1 | real, HAProxy no-delay on | `FQ-FINAL42-PROXY-NODELAY-ON-1N-20260809` | PASS |
| 1 | real, HAProxy no-delay off | `FQ-FINAL42-PROXY-NODELAY-OFF-1N-20260809` | PASS |
| 2 | strict supervisor/watchdog faults | `FQ-FINAL42-SUPERVISOR-WATCHDOG-V3Q2-2N-20260809` | PASS |

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
claim and fixed them before final42:

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
  and
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

1. any final42 run above two nodes;
2. the unapproved proposed 64-node release ceiling and its 4/16/64 ladder;
3. native Slurm plus CUDA/ROCm;
4. streaming, public exposure, and non-HAProxy production gateways; and
5. SGLang under the selected Aurora profile.

Historical 4/16/64/128/256-node runs and earlier candidates are regression
context only and cannot qualify final42.

## 6. Ledger and release decision

The canonical ledger contains **80 FIXED / 8 IN_PROGRESS / 1
EXTERNAL_BLOCKER / 3 OUT_OF_PRODUCTION_SCOPE** records. There are no implicit
waivers, self-approved limits, or unresolved locally actionable code findings
within the qualified dimensions.

Do not label final42 generally production-ready. It is the immutable,
technically qualified one/two-node candidate. A product owner must approve the
release envelope, authorize the corresponding immutable scale campaign, and
accept its receipts before a broader production release verdict can be issued.
