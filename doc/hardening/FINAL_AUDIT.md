# Final audit — production hardening cutover (P00–P06)

Status date: 2026-08-07. Branch `feature/slurm-amd-support`, baseline `2a7726f`.

This document states what the cutover changed, what is proven and by what
evidence, and what is still owed. It is written to be falsifiable: every claim
names the artifact or test that supports it, and every unproven thing is listed
as unproven rather than omitted.

## 1. What the audits actually found, and what changed

The completion-claim audit's charge was not "the code is wrong". It was that
components existed, were unit-tested, and **were not what production used**.
That pattern recurred four times in this pass, and each time it was found by
running the production path rather than by reading it:

| Component | Existed and tested | What production did instead |
|---|---|---|
| `NodeSupervisor` | yes | `RankLauncher` started `exaserve.driver`; the documented ownership tree described a design, not a process tree |
| `PlanReadiness` | yes | `server.py` drove the in-child `serve_readiness` gate against an internal endpoint no client uses |
| `ExactReceiptLedger` | yes | nothing produced a receipt for any planned slot; arriving receipts landed in a list nothing adjudicated |
| `StatusStore` | yes | nobody wrote to it; consumers had a private file shape or a log line |

Each is now on the reachable path, and each has a test that fails if it is
detached again — not a test that the component works, but a test that the
production entry point *calls* it.

## 2. Architecture as it now stands

```
scheduler
  └── launch_cluster.sh          SITE ADAPTER: env, allocation, package. ONE exec.
        └── exaserve.launcher    COMPOSITION ROOT (Python owns the lifecycle)
              ├── compile/verify DeploymentPlan          (one canonical identity)
              ├── AllocationBinding                       (generation identity)
              ├── control listener                        FAIL-CLOSED: no listener, no ranks
              ├── DeploymentStatusPublisher               shared §3.4 boundary
              ├── staging steps                           finite, result-validated
              ├── RankLauncher ── NodeSupervisor × N
              │                     ├── receipt ingress   bounded node-local hop
              │                     ├── ray daemon        SELF-attests its slot
              │                     └── deployment (r0)   publishes EVIDENCE only
              ├── gateway                                 GLOBAL owned component
              ├── PlanReadiness                           decides, commits ONE READY
              └── bounded reverse-order shutdown          exit 143 ≠ exit 1
```

Five hash boundaries stay distinct: `site_profile_hash`,
`deployment_plan_hash`, `run_semantic_hash`, `allocation_binding_hash`,
`run_provenance_hash`.

## 3. Evidence

### Hermetic
- 470 tests pass (`tests/`, `eval/tests/`, `clientlab/tests/`); ruff clean.
- Each subtree collects independently (428 / 36 / 4).
- CI has a `packaged` job that installs the wheel and imports from **outside**
  the source tree, so a source-layout assumption fails there rather than in a
  job; and a `ledger` job that runs the §8 validator.

### On hardware (2 nodes, Aurora, `artifacts/hardening/supervisor-smoke`)
- Composition root owns the lifecycle end to end; three earlier clean runs plus
  the cutover runs recorded in `MIGRATION_LOG.md`.
- `supervisor_exit=143` for a requested shutdown, distinct from a fault; named
  processes 15 → 0 after the drain deadline.
- The receipt chain transports and adjudicates: receipts crossed the local hop,
  the authenticated channel, and reached the ledger, which rejected them on a
  genuine identity mismatch before the fix and accepted them after.

### Identity
- Core and eval derive **byte-identical** `deployment_plan_hash` for one input
  (`eval/tests/test_shared_plan_identity.py`), so a serving change cannot alter
  one side silently.

## 4. Defects this pass found by running the production path

Listed because each was invisible to the test suite and to reading:

1. `supervise()` ignored its own shutdown flag — SIGTERM left 15 orphans.
2. Four consecutive environment-chain breaks, each hidden by the previous one:
   run dir fell back to `cwd`; head IP was passed by **mutating a config file**;
   `EXASERVE_RUN_LOG_DIR` never reached the ranks; `get_ray_env()` dropped the
   deployment identity before the server child.
3. `SessionCoordinator` was never fed by the listener, so `all_registered()`
   could not become true.
4. START was computed and undeliverable — no COMMAND dispatch existed.
5. Node identity: the scheduler's node file is fully qualified, a process
   reports `socket.gethostname()`, and the literal comparison rejected **every
   receipt from every correctly-placed rank**.
6. Inherited node state, twice: a prior generation's Ray session name, and
   orphaned `EngineCore` processes still holding device memory (`ray stop` does
   not reap them), which surfaced as `XPU out of memory`.
7. `EXASERVE_LEGACY_SHELL_LIFECYCLE` was an infinite exec loop, not the
   run-to-run comparison its comment advertised.

## 5. What is NOT proven

Stated plainly, because the release decision depends on it.

- **Scale.** Everything above is proven at 2 nodes. Nothing in this pass is
  qualified at 16, 64, 128 or 256 nodes on the new architecture. The 256-node
  re-run is held at the user's explicit instruction.
- **The production envelope is unapproved.** `ADR-000` has no durable approval
  for a 64-node ceiling. Per plan §S00 that means scale records may **not** be
  closed or reclassified by assuming the narrower scope, and Claude Code may
  propose but never fill an `ACCEPTED_LIMIT` approval block. Seven records are
  held on this basis and say so in their evidence field.
- **`PROXIED_INTERNAL` on hardware.** The root now compiles the gateway argv,
  starts it as a GLOBAL owned component, attests it, and canaries the compiled
  advertised endpoint — but the runs recorded here are `DIRECT_VALIDATION`
  plans. The production exposure path is implemented and unit-tested, not yet
  demonstrated end to end on hardware.
- **Gateways other than HAProxy.** `gateway_argv` raises for nginx, envoy and
  pingora: a named refusal rather than a silent gap, but a gap.
- **Non-Aurora vendors and schedulers.** Slurm/ROCm/CUDA remain declared and
  unvalidated (`TD-SLURM-AMD`), as does SGLang (`TD-SGLANG`).

## 6. Ledger

`doc/hardening/FINDINGS.yaml` is re-adjudicated by
`scripts/hardening/adjudicate.py` — a script rather than hand edits, so the
rule applied to each record is explicit and the set is reproducible. Closure
still requires linked evidence and acceptance tests, enforced separately by
`scripts/hardening/validate_findings.py`, which CI runs.

Records held open name their reason in the evidence field rather than leaving
it to inference. The largest group is scale, gated on the unapproved envelope.

## 7. Recommended next steps, in order

1. Obtain the S00/ADR-000 production-envelope approval. Until it exists, the
   scale records cannot be adjudicated in either direction, and roughly a third
   of the remaining ledger is blocked behind that one decision.
2. Demonstrate a `PROXIED_INTERNAL` plan end to end on hardware — the last
   architectural contract implemented but not exercised.
3. Re-qualify the ladder on the new architecture at the tiers the approved
   envelope actually claims.
