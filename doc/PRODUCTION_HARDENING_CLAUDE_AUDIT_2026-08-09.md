# Independent audit of the final42 completion claim

**Audit date:** 2026-08-09

**Auditor:** Claude Code, at user request, independently of the implementation
worker that produced final42.

**Subject:** branch `feature/slurm-amd-support` at `e6958df`
("Complete ExaServe production hardening"), clean working tree.

**Reference authority:** `doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md`.
Claims under audit are those in `doc/hardening/STATUS.md`,
`doc/hardening/FINAL_AUDIT.md`, and `doc/hardening/FINDINGS.yaml`.

**Role of this document:** non-normative. Per plan §0 precedence it is an
evidence/context input. It cannot close, waive, weaken, or create a gate, and
it does not modify `FINDINGS.yaml`. Every finding below is recorded as a
*proposal* with `approval: null`; whether any `CA-*` item becomes a ledger
record is the reviewer's decision.

---

## 1. Verdict

The completion claim is substantively sound. I could not find a defect inside
the qualified one/two-node envelope, and the central architectural claims are
true of the code rather than only of the prose. The immutable candidate is
real: its declared digests reproduce, and the wheel is byte-identical to the
source tree at `HEAD`.

Eight findings follow. None invalidates the `TECHNICAL_PASS_SCOPE_PENDING`
verdict. One (`CA-01`) affects the *citability* of the final gate receipts
rather than the correctness of the system. One (`CA-03`) is a real
defense-in-depth gap in contract enforcement that currently fails closed. The
remainder are documentation drift, claim-scope precision, and residue.

---

## 2. Method and environment receipt

All checks ran on the Aurora login node and are login-node-safe (brief static,
unit, and in-process contract checks only; no allocation, no GPU, no MPI, no
network egress). No compute session was used and no hardware campaign was
re-run — see §5 for what that leaves uncovered.

| Item | Value |
|---|---|
| Interpreter | `/opt/aurora/default/frameworks/aurora_frameworks-2025.3.1/bin/python` (CPython 3.12.12) |
| Env | `PYTHONUSERBASE=/home/wenyiw/.local/aurora` |
| pytest | 8.3.5 |
| ruff | 0.11.6 |
| Revision | `e6958df`, `git status --porcelain --untracked-files=all` empty |
| Tracked files | 588 |

Note on the interpreter: the repository requires Python ≥3.10 and the system
`/usr/bin/python3` on the login node is 3.6.15, so the `frameworks` interpreter
is mandatory for any local check. This is already documented in `AGENTS.md`.

---

## 3. Claims independently reverified

These were re-executed, not read. This section exists so the reviewer can trust
the negative space in §4: the absence of a finding about these areas means they
were checked and held, not that they were skipped.

| # | Claim under audit | Command | Observed |
|---|---|---|---|
| V1 | Final source suite passes | `pytest -q -p no:randomly --ignore=tmp` | **1219 passed** in 160.99s |
| V2 | Ruff formatting is release-gating clean | `ruff format --check .` | 273 files already formatted |
| V3 | Ruff correctness floor clean | `ruff check .` | All checks passed |
| V4 | Runtime lint clean | `ruff check --select E4,E7,F src/exaserve eval/lib eval/cli.py clientlab tests conftest.py` | All checks passed |
| V5 | Security subset clean | `ruff check --select S102,S307,S602,S608,S609 src/exaserve eval/lib clientlab` | All checks passed |
| V6 | Ledger matches the canonical §3.2.1 schema | `python scripts/hardening/validate_findings.py` | 92 records; `{FIXED:80, IN_PROGRESS:8, EXTERNAL_BLOCKER:1, OUT_OF_PRODUCTION_SCOPE:3}`; valid |
| V7 | Ledger counts match `STATUS.md` §Ledger | direct count | exact match, including the named eight `IN_PROGRESS` IDs |
| V8 | Candidate evidence adjudicates | `python scripts/hardening/adjudicate.py --verify-candidate artifacts/hardening/final42-candidate-review.json` | "verified exact final42 package and campaign evidence", exit 0 |
| V9 | Declared candidate digests are real | `sha256sum` over wheel, sdist, artifact manifest, candidate review | all four match `STATUS.md` / `FINDINGS.yaml:meta` byte-for-byte |
| V10 | The immutable wheel is this source tree | unzip + per-member SHA-256 vs `src/` | 107 members: 0 different, 0 in-wheel-only, 0 in-src-only |
| V11 | Receipt v2 schema is exactly the plan's field list | `dataclasses.fields(CompatibilityReceiptV2)` vs plan §3.2.1 | 28/28 fields, no omissions, no extras |
| V12 | No log marker can advance control state | grep for `CLUSTER FULLY READY` / `ALL SERVICES READY` / `CLUSTER READY` across `src`, `eval`, `clientlab`, `scripts`, `tools` | only docstrings and negative tests; no consumer |
| V13 | Legacy lifecycle paths are deleted (WP13) | inspection | `driver.py` is a 22-line one-way alias; `resources/launch_cluster.sh` absent and CI asserts it is not in the wheel; `eval/lib/schedulers/` empty |
| V14 | `GatewayKind` admits no `none`/`direct` | `plan/contracts.py:166` | confirmed; compiler additionally rejects the legacy spellings with a typed error |
| V15 | `lease_timeout_s >= 3 × heartbeat_interval_s` is enforced | `ControlLimits.__post_init__` | confirmed |
| V16 | Production execution fails closed on unmeasured limits | `require_execution_qualification` on all shipped examples | both production examples blocked; see `CA-04` |
| V17 | Every shipped example still compiles | `compile_deployment_plan` over `examples/*.yaml` | 4/4 OK |
| V18 | Default SiteProfile rejects Slurm / CUDA / ROCm / SGLang | `site.py:150-166` | `scheduler_types=("pbs",)`, `vendors=("xpu",)`, `engines=("vllm",)`; `supported_max_nodes=2`, `qualification_target_approved=False`, and the approval flag is enforced at `site.py:306` |
| V19 | Referenced evidence files exist | link + path sweep over `README.md`, `doc/**` | no broken markdown links; all cited `findings/` and `eval/specs/` evidence present |

Two gates in `FINAL_AUDIT.md` §3 were **not** reverified locally and are
carried forward on the original receipts: mypy (not installed on the frameworks
interpreter) and the Go client suite (`go` not on `PATH` without `module load
go`). See §5.

---

## 4. Findings

Severity uses the plan §3.2.1 vocabulary. Each item is independently
reviewable; adjudicate them separately.

---

### CA-01 — The final source and static gate receipts predate the last changes to the tree

**Severity:** medium-high
**Proposed disposition:** OPEN

**Claim.** `STATUS.md` cites
`final42-final-source-gate-20260809-a3/pytest.log` ("PASS — 1216 passed") and
`final42-final-static-gate-20260809-a2/` (Ruff, security, compileall, Go,
ledger, adjudicator) as the durable evidence for the shipped candidate.
`FINAL_AUDIT.md` §2 states: "Do not rebuild, relabel, or broaden final42
without creating a new candidate review."

**Observed.** Both receipts were produced before the final edits to the tree:

```
artifacts/.../final42-final-static-gate-20260809-a2/   20:22
artifacts/.../final42-final-source-gate-20260809-a3/pytest.log   20:30:59
--- files modified after both ---
tests/test_findings_validator.py                       20:50:41
scripts/hardening/validate_findings.py                 20:50:58
tests/test_adjudicator_evidence.py                     (post-gate)
tests/test_supervisor_fault_qualification_harness.py   (post-gate)
tests/test_supervisor_fault_qualification_v2.py        (post-gate)
tests/test_supervisor_fault_qualification_v3.py        (post-gate)
tests/test_proxy_qualification_harness.py              (post-gate)
commit e6958df                                         20:55:09
```

The tree today collects **1219** tests against the **1216** recorded in the
cited log, and the ledger validator that the static gate exercised is not the
validator in the tree.

**Impact.** No behavioral risk: `src/` is unchanged (V10 proves the wheel is
byte-identical to the current source tree), and I re-ran both surfaces
successfully (V1, V6). The defect is evidentiary — the receipts as cited do not
cover the shipped bytes of the test and validator surface, which is precisely
the discipline the plan imposes on itself.

**Proposed remedy.** Re-run the two gates against `e6958df` and record them as
`-a4` / `-a3`, or state the exclusion explicitly in `STATUS.md` and
`FINAL_AUDIT.md`. Regenerating the candidate review is not required, since the
package identity is unchanged.

---

### CA-02 — `KNOWN_ISSUES.md` and `TODO.md` are still reconciled against the superseded final35 candidate

**Severity:** medium
**Proposed disposition:** OPEN

**Claim.** `MIGRATION_LOG.md` records final35 as superseded, and
`FINAL_AUDIT.md` §4 lists as a corrected bug that "the candidate adjudicator now
verifies the current candidate ... instead of silently remaining hardcoded to
final35."

**Observed.** Two of the four documents the plan names as program inputs were
not carried forward with the adjudicator:

- `doc/KNOWN_ISSUES.md:3` — "**Reconciled:** 2026-08-09 against final35"
- `doc/KNOWN_ISSUES.md:9` — "The final35 candidate is qualified at one and two
  Aurora nodes"
- `doc/TODO.md:3` — "**Reconciled:** 2026-08-09 against final35"
- `doc/TODO.md:10` — "Product owner: authorize one additional four-node
  **final35** attempt"
- `doc/TODO.md:13` — "predeclare and run only the matching **final35** ladder"

`final35` occurs 8× in `KNOWN_ISSUES.md` and 6× in `TODO.md`. Both files
correctly disclaim that they cannot close a gate, but they still make
candidate-specific *qualification* assertions about a candidate that no longer
exists, and `TODO.md` directs the product owner to authorize a run against it.

**Impact.** Documentation only, but these are the two files a reviewer reads to
decide what is still owed. A reader following `TODO.md` item 1 would authorize
the wrong ladder.

**Proposed remedy.** Mechanical: reconcile both headers and bodies to final42,
or add a one-line supersession banner pointing at `hardening/STATUS.md`.

---

### CA-03 — The gateway/exposure cross-invariant is enforced only in the compiler, not on the load path production uses

**Severity:** medium
**Proposed disposition:** OPEN

**Claim.** Plan §3.2.1, *Gateway ownership, readiness ordering, and direct
exposure*: "Production requires `GatewayPlan(kind=HAPROXY)` with
`PROXIED_INTERNAL`. `DIRECT_VALIDATION` requires `gateway: null` and
`ScaleEnvelope.validation_mode = true`; **every other null/mode combination is a
compile error**." The same section forbids letting a null gateway "silently make
the gateway predicate vacuous."

**Observed.** `plan/compiler.py:562-577` and `:661-664` implement this
correctly. Neither `DeploymentPlan.__post_init__` nor
`plan/io.load_deployment_plan` re-checks it — and per plan §3.2.1 the
in-allocation composition root *loads and verifies* the persisted artifact
rather than recompiling, so the load path is the production entry point.
Reproduction:

```
compiled ok; gateway=haproxy mode=PROXIED_INTERNAL validation_mode=False
DIRECT CONSTRUCTION ACCEPTED gateway=None + PROXIED_INTERNAL: PROXIED_INTERNAL False
LOAD ACCEPTED tampered plan: None PROXIED_INTERNAL False
```

(`dataclasses.replace(plan, gateway=None).finalize()`, written and reloaded
through `plan/io`. The self-recomputed `deployment_plan_hash` matches, so the
integrity check does not catch it.)

**Impact — bounded.** The system still fails closed. `plan_readiness.py:419`
keys the gateway conjunct off `plan.is_production_exposure()` (the exposure
mode) rather than `plan.gateway is not None`, so a tampered plan yields a
permanently missing `gateway/process-observation` blocker and never reaches
READY. This is a hardening gap in contract enforcement, **not** a READY bypass.
The correct reading is that the invariant is stated as universal but implemented
at one of two entry points.

**Proposed remedy.** Move the cross-field rule into
`DeploymentPlan.__post_init__`, where the sibling cross-field rules
(`validation_mode` vs `scale_envelope.validation_mode`, `pp_shard_aware` vs
multi-replica PP, topology double-booking) already live. That closes both the
direct-construction and the load path in one place, and the compiler's
path-specific error messages can remain as the friendlier surface.

---

### CA-04 — Every Aurora qualification campaign ran in validation mode, and this is not stated

**Severity:** medium
**Proposed disposition:** OPEN (disclosure, not defect)

**Observed.** All six campaign configs set `validation_mode: true`:

```
scripts/hardening/config.final-null.haproxy.yaml
scripts/hardening/config.final-null.haproxy.2n.yaml
scripts/hardening/config.final-real.haproxy.1n.yaml
scripts/hardening/config.final-real.haproxy.2n.yaml
scripts/hardening/config.final-real.haproxy.4n.yaml
scripts/hardening/config.proxy-nodelay-off.real.1n.yaml
```

This is forced, not sloppy: the shipped SiteProfile has
`control.evidence_backed=False`, so no production-mode plan can cross the
execution boundary at all. Confirmed:

```
examples/config.haproxy.yaml:   BLOCKED -> production execution is not qualified:
                                SiteProfile alcf-aurora carries unmeasured control/readiness limits
examples/config.reference.yaml: BLOCKED -> (same)
examples/config.direct.yaml:    qualification OK, production=False
```

**Impact.** Small and defensible. I traced every runtime consumer of
`validation_mode` outside the plan package; there is exactly one behavioral
divergence — `launcher.py:168`, which permits `EXASERVE_JOBID` without a native
scheduler id only under validation mode. The serving, readiness, gateway,
receipt, and cleanup paths are identical. So the campaigns do exercise the
production code.

Two things are nevertheless unstated. First, neither `STATUS.md` nor
`FINAL_AUDIT.md` says the passing campaigns were validation-mode runs, which a
reader would reasonably assume otherwise. Second, the
`require_execution_qualification` → `True` branch has never executed on
hardware, because no evidence-backed SiteProfile exists yet; it will first run
in production on the day the measured limits land.

**Proposed remedy.** One sentence in `STATUS.md` §"Support boundary and
residuals" recording that the qualified evidence is validation-mode, that the
only divergence is the job-id rule, and that the production-qualified branch is
first exercised when WP12 measurements set `evidence_backed`.

---

### CA-05 — "mypy clean" covers four modules out of 104

**Severity:** low
**Proposed disposition:** OPEN

**Claim.** `FINAL_AUDIT.md:67` — "exact installed-wheel mypy: **no issues
found**", listed among package/source gates without qualification.
`STATUS.md:32` — "1207 passed, 9 skipped; mypy clean".

**Observed.** `.github/workflows/ci.yml:166-170` runs:

```
mypy --ignore-missing-imports --follow-imports=skip
  src/exaserve/plan/contracts.py src/exaserve/control/contracts.py
  src/exaserve/telemetry.py src/exaserve/state/results.py
```

Four files of 104 in `src/exaserve`, with imports not followed. The job is named
"Typed contract core", which is accurate; the audit prose is what generalizes.

**Impact.** Claim-scope precision. Plan §6 requires that "a test is not evidence
for a different ... scale tier unless the profile declares that equivalence" —
the same discipline should apply to a type gate's file set.

**Proposed remedy.** Either qualify both sentences as "typed contract core
(4 modules)", or widen the gate. I did not attempt to widen it and have no
evidence about how much would fail; mypy is not installed on the frameworks
interpreter.

---

### CA-06 — A scratch tree under the repository root breaks the release-gate pytest invocation

**Severity:** low
**Proposed disposition:** OPEN

**Observed.** My first run of the exact CI command failed:

```
$ pytest -q -p no:randomly
ERROR tmp/staged-checkout.YG9dhd/tests/test_vllm_support.py
... !!! Interrupted: 80 errors during collection !!!
80 errors in 18.44s
```

`tmp/` is gitignored and `tmp/staged-checkout.YG9dhd/` is operator residue — I
confirmed no script in the repository creates it. But `pyproject.toml` sets
`addopts` and nothing else: there is no `testpaths` and no `norecursedirs`, so
pytest recurses into any nested tree under the root regardless of `.gitignore`.
Both release-gate invocations run from the root:
`.github/workflows/ci.yml:82` and `scripts/hardening/run_packaged_gate.sh:67`
(`pytest -q -p no:randomly "$repo_root"`), and `tmp/` is the scratch area the
hardening scripts themselves use.

**Impact.** Situational and non-silent — it fails loudly rather than
under-collecting. But it makes a green gate dependent on operator hygiene in a
directory the harness writes to.

**Proposed remedy.** Add `testpaths = ["tests", "eval/tests", "clientlab"]` (or
`norecursedirs = ["tmp", "build", "dist", "artifacts", ".git"]`) to
`[tool.pytest.ini_options]`. Separately, delete the stale
`tmp/staged-checkout.YG9dhd/` — I left it in place rather than remove state I
did not create.

---

### CA-07 — Two small residues the WP13 sweep did not catch

**Severity:** low
**Proposed disposition:** OPEN

**7a. `CompositionRoot.observe_gateway` has no production caller.**
`composition.py:1819`. I checked every method on the class mechanically: it is
the only one with zero production call sites and a nonzero test call site
(`tests/test_composition_root.py:818`). Its logic — a dead gateway is terminal,
revoke and fail — duplicates the live path in `monitor_readiness:1854-1861`.
The consequence is that a test asserts behavior production never invokes, so
the two copies can silently diverge. WP13 action 5 ("re-run repository searches
for ... duplicated launch logic") was meant to catch this. Remedy: delete the
method and point the test at `monitor_readiness`, or call it from the
supervision loop and delete the duplicate.

**7b. Static reason codes are assigned per branch, not per cause.**
`launcher.py:365-380`. `deployment_done_or_failed` labels anything returned by
`monitor_readiness` as `READINESS_RECOVERY_EXPIRED` and any pre-existing
`first_cause` as `GATEWAY_FAILURE`. A gateway process death after READY returns
through `monitor_readiness`, so it is durably recorded with reason code
`READINESS_RECOVERY_EXPIRED`. The `detail` string and `first_cause` remain
accurate, so no diagnosis is lost — but plan WP10 makes reason codes the
operator-facing classification, and this one is wrong for a case the campaign
explicitly exercises. Remedy: have `monitor_readiness` return a
`(reason_code, detail)` pair.

---

### CA-08 — Ledger observations

**Severity:** info
**Proposed disposition:** reviewer's call; no action proposed

Two observations about `FINDINGS.yaml` that are not schema violations — the
validator passes (V6) — but that a reviewer using the ledger as an audit trail
should know.

**8a. All 80 `FIXED` records carry a mechanically templated `decision`.**
Verified exactly: for all 80, `decision == "Verified on the final42 production
path: " + invariant`. Plan §3.2.1 specifies `decision: <non-empty adjudication
and missing proof, if any>`. A verbatim restatement of the invariant satisfies
the schema but records no adjudication — it cannot distinguish a record closed
by a two-node hardware receipt from one closed by a unit test. The `evidence`
lists do carry that distinction and are populated per record, so the
information exists; it is the human-readable adjudication that is uniform. The
eight `IN_PROGRESS` records, by contrast, have specific decisions naming the
missing proof.

**8b. Documentation drift in README claims.**
`README.md:160` refers to "LiteLLM, NGINX, Envoy, and Pingora examples" —
`examples/` contains only `config.litellm.yaml` of those four.
`README.md:80` says "256+ maps to `prod`", which is unreachable under the
default SiteProfile's 64-node `qualification_target_nodes` ceiling.
Additionally, the quickstart at `README.md:62-71` instructs the reader to copy
`examples/config.haproxy.yaml` and run `exaserve-serve-submit`, which per
`CA-04` cannot succeed today. The block at `README.md:57-60` warns that
production submission fails closed, so this is inconsistency rather than a
false claim, but the primary quickstart is currently a non-runnable path.

---

## 5. Scope and limits of this audit

Stated explicitly so no reader treats absence of a finding as proof.

1. **No hardware was used.** I ran nothing in a compute session and re-ran no
   Aurora campaign. Every one/two-node, real-engine, PP=2, proxy, and
   supervisor-fault result in `STATUS.md` is accepted on its recorded receipt
   plus the adjudicator's verification of those receipts (V8), not
   re-executed. Findings about hardware behavior are therefore out of reach of
   this audit.
2. **Two gates were not reverified**: mypy (module not installed on the
   frameworks interpreter) and the Go replay suite (`go` not on `PATH`;
   `module load go` was not performed). Both are carried on their original
   receipts, and `CA-05` concerns only how the mypy result is described.
3. **The packaged-wheel gate was not re-run.** It builds and installs into a
   clean venv, which exceeds what is appropriate on a login node. V10 (wheel
   ≡ source, per member) is the substitute and is strictly about identity, not
   about installed-package behavior.
4. **This is not a line-by-line review of 38k lines.** I read the composition
   root, launcher, plan contracts and compiler, plan I/O, readiness projection,
   site profile, and the compatibility overlay entry points in full or near
   full, and sampled transport, supervisor, and status. The control-channel
   internals (`transport.py`, `channel_runtime.py`, ~3.4k lines) and
   `_sitecustomize.py` (1118 lines, excluded from ruff by design) were read
   only at their interfaces.
5. **Adversarial review of the adjudicator was shallow.** I confirmed it is not
   candidate-hardcoded and that it passes, but I did not attempt to construct
   evidence that it would wrongly accept.
6. **Scope decisions are untouched.** The 64-node ceiling, the pending
   product-owner approval, and the eight `IN_PROGRESS` scale records are
   external decisions; I neither validated nor challenged them.

---

## 6. Reviewer adjudication table

| ID | Severity | Area | One-line | Disposition |
|---|---|---|---|---|
| CA-01 | medium-high | evidence | Final source/static receipts predate the last tree changes (1216 vs 1219) | |
| CA-02 | medium | docs | `KNOWN_ISSUES.md` / `TODO.md` still reconciled against final35 | |
| CA-03 | medium | contracts | Gateway/exposure cross-invariant missing on the load path; fails closed | |
| CA-04 | medium | claims | All campaigns ran validation-mode; unstated in the status documents | |
| CA-05 | low | claims | "mypy clean" is 4 modules of 104 | |
| CA-06 | low | gates | No `testpaths`; a scratch tree under the root breaks the gate command | |
| CA-07 | low | residue | Test-only `observe_gateway`; miscategorized reason codes | |
| CA-08 | info | ledger/docs | Templated `FIXED` decisions; three README inaccuracies | |

Recommended order if any are actioned: `CA-01` first (it is two commands and
determines whether the other receipts are citable), then `CA-02` and `CA-04`
(documentation reconciliation), then `CA-03` as the only change with a
correctness argument behind it.

No `FINDINGS.yaml` record was created for any `CA-*` item, and no file outside
this document was modified by this audit.

---

## 7. Codex reviewer adjudication (2026-08-09)

The audit is useful and mostly correct, but its individual claims need separate
dispositions. The accepted changes are recorded in the canonical ledger and
bound to a new candidate, final43; final42 remains historical evidence.

| Finding | Adjudication | Reviewer evidence and action |
|---|---|---|
| CA-01 | **Accepted, remedy corrected** | The receipts were stale. Because accepted findings changed packaged source, retaining final42 and merely rerunning two commands would have been wrong. A new wheel/sdist, candidate review, installed-wheel gate, full Aurora replay, source gate, and static gate were created as final43. |
| CA-02 | **Accepted** | Current Known Issues, TODO, status, and final audit now identify final43. The related README inaccuracies from CA-08b are closed by the same documentation record. Historical migration/audit text deliberately retains its original candidate names. |
| CA-03 | **Accepted, impact narrowed** | Construction/load enforcement belonged in the canonical dataclasses and is now there. The audit correctly notes that its exact null-gateway reproduction failed readiness closed; it was not a READY bypass. Negative direct-construction and rehashed-load tests cover the fix. |
| CA-04 | **Accepted as claim-scope disclosure** | Every final43 hardware row still uses HAProxy/`PROXIED_INTERNAL` and the production serving/readiness/receipt/cleanup path, but `validation_mode=true`. Status and final audit now explicitly leave the positive production-qualification branch unclaimed. |
| CA-05 | **Accepted** | The mypy claim now says exactly what the gate proves: four typed contract modules, missing imports ignored, imports not followed. This is not repository-wide typing evidence. |
| CA-06 | **Accepted** | Both `testpaths` and `norecursedirs` are configured. A regression locks that configuration, and the explicit-root final gate passed with Claude's nested `tmp/staged-checkout.YG9dhd` still present. |
| CA-07a | **Accepted** | The duplicate test-only `observe_gateway` method was removed; the test uses the production monitor path. |
| CA-07b | **Narrowed; concrete gateway claim rejected** | The broad point—that monitor branches should return typed causes instead of being relabeled by the launcher—was valid and is fixed. But an owned HAProxy death does **not** normally reach that branch: `RuntimeSupervisor.poll_once()` observes it first. The final43 one-node fault result records `gateway/haproxy: UNEXPECTED_EXIT`, `terminal_reason_code=FIRST_CAUSE`, not `READINESS_RECOVERY_EXPIRED`. |
| CA-08a | **Rejected as a production finding** | The uniform decisions are weak audit prose, but the exact-schema ledger is valid and each record's specific evidence list carries the actual adjudication boundary. Rewriting 80 valid decisions would add review noise without changing behavior, proof, or disposition. New records use specific decisions. |
| CA-08b | **Accepted; folded into CA-02** | README now distinguishes the runnable direct-validation path from blocked production submission, stops the default queue mapping at 64 nodes, and describes only the shipped LiteLLM alternate example while naming the other gateways as adapters. |

### Additional reviewer finding

**CA-09 — persisted receipt requirements could be weakened after rehashing
(high, FIXED).** While testing CA-03, the reviewer found a more consequential
variant: a persisted plan could remove or weaken `receipt_requirements`,
recompute its top-level hash, and make the exact receipt ledger satisfy a
vacuous `0/0` requirement. `DeploymentPlan.__post_init__` now rederives the
canonical receipt tuple from topology, models, gateway, and engine mode and
rejects any mismatch. `tests/test_plan_io.py` includes the rehashed-load
regression.

### Final43 evidence binding

- Candidate review:
  `artifacts/hardening/final43-candidate-review.json`
  (`763dfdf04e72ef6b42a35277d362c275e64dcd66b477457e87c5235045638c6d`).
- Wheel:
  `1041be53eb5b5875d198d5ee6c6664718b4085775dcba99107873dd3d1fcdff2`.
- Installed-wheel gate:
  `artifacts/hardening/final43-packaged-gate-20260809-a1/`.
- Gateway-death counterexample to CA-07b:
  `artifacts/hardening/final43-null-1n-20260809-a1/qualification/result.json`.
- Full one/two-node lifecycle, real-engine PP=2, proxy-toggle, and strict
  supervisor campaigns are the final43 results enumerated by the candidate
  review; the generic adjudicator verifies every declared hash and scenario.

The resulting disposition is still `TECHNICAL_PASS_SCOPE_PENDING`: no accepted
Claude finding remains locally actionable, but release-envelope approval,
measurements above two nodes, and offsite Slurm/CUDA/ROCm evidence remain open
exactly as recorded in `FINDINGS.yaml`.

---

## 8. Auditor verification of the closure pass (2026-08-09)

The reviewer's remediation was re-verified independently, by re-executing the
checks rather than reading the adjudication. Same login-node constraints as §2;
same interpreter and tool versions. Working tree at the time of this pass:
`e6958df` plus the uncommitted remediation (25 modified files).

### 8.1 Candidate replacement is real, not a relabel

The remedy for `CA-01` was a full candidate rebuild. Because relabeling is
exactly what the plan forbids, the substitution was checked directly:

| Check | Result |
|---|---|
| final43 wheel / sdist / artifact-manifest / candidate-review SHA-256 | all four reproduce and match `STATUS.md` |
| `review_manifest_sha256` in `FINDINGS.yaml:meta` vs the file | match |
| final43 wheel vs current `src/` | 107 members, 0 different, 0 wheel-only, 0 src-only |
| final42 vs final43 campaign `result.json` (7 cells) | all 7 distinct files |
| wheel hash recorded *inside* each final43 receipt | `1041be53eb5b` in all 7 — the new wheel, not the old |
| campaign start times | final42 ≈ `1786304154–1786305012`; final43 ≈ `1786311948–1786312807` (~2h10m later, sequential over ~14 min) |
| two-node real receipt physical hosts | `x4303c4s1b0`, `x4310c4s0b0`; verdict `true` |
| experiment plan predeclared before first result | `21:45:05` vs `21:47:02` |

These are new hardware runs against the new wheel, not renamed final42
evidence.

### 8.2 CA-01 gate ordering now holds

Source gate `22:20:03`, static gate `22:20:23`; newest tracked file `22:17:51`
(`doc/KNOWN_ISSUES.md`, `doc/hardening/FINDINGS.yaml`). No tracked `.py` file
postdates either gate. The ordering defect is closed by construction, not by
assertion.

### 8.3 Re-executed gates

| Check | Result |
|---|---|
| `pytest -q -p no:randomly` (bare, from the repository root) | **1224 passed** — matches the gate log exactly |
| `ruff format --check .` / `ruff check .` / runtime lint / security subset | all pass |
| `validate_findings.py` | 101 records, `{FIXED:89, IN_PROGRESS:8, EXTERNAL_BLOCKER:1, OUT_OF_PRODUCTION_SCOPE:3}`; valid |
| `adjudicate.py --verify-candidate .../final43-candidate-review.json` | "verified exact final43 package and campaign evidence", exit 0 |
| packaged-gate receipt | clean venv, `wheel_sha256=1041be53eb5b…`, 1215 passed / 9 skipped, mypy "no issues found in 4 source files" |
| static-gate `mypy-receipt.log` hash vs packaged-gate `mypy.log` | match — the static gate references the artifact instead of re-asserting the claim |

`CA-06` was verified under real conditions rather than by its regression test:
the bare root invocation collected 1224 with `tmp/staged-checkout.YG9dhd` still
present on disk. Before the fix the identical command died with 80 collection
errors.

### 8.4 CA-03 and CA-09 re-probed adversarially

The original reproduction no longer succeeds, and three further tamper cases
were constructed against the **load** path, which is what production uses:

```
direct construction (gateway=None + PROXIED_INTERNAL)
  -> rejected: deployment without a gateway requires DIRECT_VALIDATION exposure
t1 gateway stripped on disk
  -> rejected: compiled plan artifact invalid: ... requires DIRECT_VALIDATION exposure
t2 gateway stripped + self-hash forged
  -> rejected: compiled plan artifact lacks a valid deployment_plan_hash
t3 one receipt requirement deleted (engine/core), everything else valid
  -> rejected: receipt_requirements disagrees with derived runtime topology
t0 unmodified round trip
  -> loads, hash 7196b204c24e
```

`CA-09` is confirmed both as a real pre-existing defect and as fixed. Before the
change, `DeploymentPlan.__post_init__` checked only for *duplicate* requirement
slots, so an empty or truncated `receipt_requirements` tuple passed
(`0 == 0`) and READY's exact-set proof could be satisfied vacuously. This is a
genuine finding of the reviewer's own pass that the original audit missed: §4
probed the gateway/exposure matrix but never tried shrinking the evidence set.
Its `high` severity is appropriate — it weakens the receipt contract itself
rather than one predicate — even though reaching it requires write access to
the run directory the composition root owns.

The derivation now lives in `contracts.build_receipt_requirements` and the
compiler imports it; it was moved, not duplicated, so there is still one
implementation.

### 8.5 Correction to CA-07b

**The reviewer is right and the original finding's example was wrong.**
`CA-07b` claimed a post-READY HAProxy death would be durably recorded as
`READINESS_RECOVERY_EXPIRED`. Checking the cited final43 one-node fault
receipt:

```
terminal_detail: "gateway/haproxy: UNEXPECTED_EXIT (exit=-15) gateway process_dead;
                  signal=15; evidence=gateway_failure.json"
terminal_reason_code: "FIRST_CAUSE"
READINESS_RECOVERY_EXPIRED: 0 occurrences
GATEWAY_FAILURE: 0 occurrences
```

`RuntimeSupervisor.poll_once()` classifies an owned child's death before the
readiness poll reaches its gateway branch, so the mislabeling path is not the
one a real gateway death takes. The general defect — the launcher stamping a
fixed reason code onto whatever `monitor_readiness` returned — was real, and
the typed `FirstCause` return closes it. The question of which path wins the
race is now moot, since both classify correctly.

### 8.6 Remaining observations

Neither is a defect; both are recorded for traceability.

1. **`CA-08a` is declined, and the decline is recorded only here.** The ledger
   contains no `CA-08` record, so `FINDINGS.yaml` reads `CA-07B` → `CA-09` with
   an unexplained gap; the README half was closed under `CA-02` without
   attribution. The substance of the decline is reasonable — the evidence lists
   carry the real adjudication boundary. Note, though, that the 80 pre-existing
   decisions were updated by substituting `final42` → `final43` in place: all 80
   still match `"Verified on the final43 production path: " + invariant`
   verbatim. Since final43 did receive a complete hardware replay, the claim is
   now substantively backed; it remains uniform prose. A one-line pointer in the
   ledger or `MIGRATION_LOG.md` would close the numbering gap.
2. **The `CA-06` regression asserts configuration text, not behavior.**
   `test_pytest_collection_excludes_operator_and_release_scratch_trees` matches
   two literal strings in `pyproject.toml`. It will not catch a future change
   that keeps the strings but breaks collection. The behavioral proof exists
   (§8.3) but is not captured as a test.

### 8.7 Verification verdict

All eight original findings are resolved: seven fixed and verified, one
(`CA-08a`) declined with a stated rationale, its README half fixed under
`CA-02`. One original finding (`CA-07b`) was correctly narrowed against
hardware evidence. One additional defect of higher severity than anything in
this audit (`CA-09`) was found and fixed during remediation.

I found no new defect in the remediation and no regression from it. The
disposition remains `TECHNICAL_PASS_SCOPE_PENDING` for the reasons already in
`STATUS.md`: release-envelope approval, evidence above two nodes, and offsite
Slurm/CUDA/ROCm remain external. Nothing in this verification pass changes that
boundary in either direction.

This section modified no file other than this document.
