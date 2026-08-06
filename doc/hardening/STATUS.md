# ExaServe Production-Hardening — Status

**Updated:** 2026-08-06, after the implementation audit
(`doc/PRODUCTION_HARDENING_IMPLEMENTATION_AUDIT.md`) of commit `e73f3eb`.
Companion to `MIGRATION_LOG.md` (chronological) and `FINDINGS.yaml`
(authoritative ledger, 82 records).

## Headline — read this first

**NOT production ready. The target architecture is not yet wired into the
production path.**

An earlier version of this file claimed "34 of 35 audit findings fixed" and
that no open item was a production blocker. **That claim was wrong** and has
been withdrawn. The work in `e73f3eb` is genuine and useful, but it is
P00/P01-class foundations plus incremental repairs to the *legacy* path — not
the WP4/WP5/WP13 cutover that owns the blocker invariants.

Current ledger (YAML-parsed, not regex-counted — the previous count was also
wrong):

| Status | All records (82) | Audit findings (35) |
|---|---|---|
| FIXED | 27 | 12 |
| IN_PROGRESS | 31 | 23 |
| OPEN | 22 | 0 |
| OUT_OF_PRODUCTION_SCOPE | 2 | 0 |

A record is `FIXED` only when its invariant holds **on the path a production
deployment actually takes**. Anything owned by the un-cut-over architecture is
`IN_PROGRESS` with the narrow completed slice named in its `evidence` field.

## What is genuinely complete (narrow, verified)

- Source config copied before runtime head-IP mutation (PR-003 slice).
- Derived model-identity collision checks (PR-007).
- AST-restricted matrix expressions (PR-016), with escape-attempt tests.
- Required-model default placement failure (PR-023).
- HAProxy admin lockdown + `haproxy -c` / `nginx -t` preflight (PR-024 slice).
- ClientLab `faults` default + argv-vector SSH (PR-030).
- Spec enum/bound validation (PR-020); benchmark-gateway marking (PR-025).
- Scheduler/eval job-body shell quoting **after** the IMP-B09 fix below.
- Atomic single-writer publication helpers; lifecycle enums/transition tables.

## Defects found by the audit and now fixed (this pass)

Each has a regression test in `tests/test_audit_regressions.py` (17 tests):

| ID | Defect | Fix |
|---|---|---|
| IMP-B09 | **Self-inflicted shell injection**: my PR-015 "fix" pasted `shlex.quote` output *inside* double quotes, where single quotes lose their power — `$(cmd)` in a path executed | assign to a shell var (single-quoted), reference as `"$var"`; regression test renders `$()`/backtick payloads |
| IMP-B05 | completion marker trusted by existence; deleting weights still read "complete"; tokenizer-only download wrote a full-model marker | marker inventory verified against disk (name+size); `kind` recorded; tokenizer-only never certifies a model |
| IMP-B07 | lease takeover unfenced (stale holder's `release()` deleted the successor's live lease); two stealers could both win; status CAS had an ABA hole; a record could initialize directly as READY | per-acquisition fencing token + `holds_lease()`/`renew()`; O_EXCL arbitration for takeover; `expected_revision` CAS; initial states restricted to PLANNED |
| IMP-B06 | an authenticated rank could forge another rank's or a **GLOBAL** observation; malformed observations were skipped not fail-closed; `all_registered` stayed true after disconnect; unbounded listener state | observation identity bound to the authenticated session (scope/rank/node); fail-closed termination; registration cleared on disconnect; bounded dedup/audit; per-component sequence enforcement; heartbeat recorded as a lease timestamp |
| IMP-H01 | `plan_hash` included `source_path` (same intent → different identity); frozen plan held caller-owned mutable dicts (content changed, hash didn't); `nan` accepted; `reservation_topology: false` bypassed node-agreement; top-level `envelope` ignored | hash covers semantic intent only; deep-freeze of nested options; non-finite rejected; reservation topology must be a non-empty string; `envelope` block honored |
| IMP-H04 | list/scalar bodies escaped as `AttributeError`/`TypeError` (500 not 400); `"false"` was truthy for `stream`/`ignore_eos`/HAProxy options | `require_object_body()` + typed model check + `strict_flag()` wired into both handlers and HAProxy options |
| IMP-B08 | partial replay / missing rank shards / failed required stats still wrote `succeeded` | incomplete runs are written as `partial` with reasons; `_is_completed` treats partial as needing human triage, not resubmission |
| IMP-H04b | `serve_url` looked for `proxy_out/proxy_port` beside the **source** config while the launcher writes it beside the run-scoped runtime config | search run-log tree (newest first), then legacy location |
| IMP-H07 | randomized CI job used `pytest -p randomly` without declaring `pytest-randomly` | added to `[dev]` extra |
| IMP-B10 | ledger recorded labels, not demonstrated closure; record count itself was wrong | 23 audit findings reopened as IN_PROGRESS; all 82 records now carry required plan §8 fields; counts YAML-parsed |

**Suite: 123 passed / 0 failed.**

## What remains — the real work (unchanged by this pass)

These are the audit's release blockers and they are **not** optional cleanup:

- **IMP-B01 / WP4+WP5+WP13:** no `RuntimeSupervisor`, `RankLauncher`,
  `NodeSupervisor`, `DeploymentManager`, or `ReadinessCoordinator` in the
  production path. `launch_cluster.sh` (533 lines) + per-rank
  `exaserve.driver` remain the active topology.
- **IMP-B02:** stdout markers (`CLUSTER FULLY READY` / `ALL SERVICES READY`)
  are still the authoritative readiness protocol in driver, server, and eval;
  READY cannot be revoked after component loss.
- **IMP-B04:** no `compat/{profile,activator,receipt}` system; `apply_all()`
  is fail-open (`strict=False`); READY is not receipt-gated.
- **IMP-B03:** essential-child supervision gaps (Ray head unpolled;
  exit-code-0 unexpected exits read as success; no process groups).
- **IMP-B09 residue / WP8:** two scheduler stacks; submit-then-persist crash
  window remains.
- **IMP-H02/H03:** distribution is not generation-isolated or receipt-based;
  Bash remains a coequal control plane.
- **IMP-H08:** the 16/64-node results are **direct-mode weak-scaling
  feasibility smoke** (legacy path, `proxy_config: none`, no predeclared
  provenance) — they do **not** close WP12/AC-SCALE-01. They are retained and
  relabelled as such in `COMPATIBILITY_MATRIX.md`.

## Correct next step

Follow the canonical packet order in
`doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md` §8 of the audit: ledger
correction (done), then primitives (done for lease/status/plan), then the
compatibility profile + receipts, then the supervisor/readiness coordinator,
then staging generation-isolation, then eval/ClientLab onto shared contracts,
then reduce Bash to a site adapter and delete marker consumers — and only
then re-run qualifying Aurora evidence through the **new** path.
