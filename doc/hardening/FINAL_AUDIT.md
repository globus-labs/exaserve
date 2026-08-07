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
- 476 tests pass (`tests/`, `eval/tests/`, `clientlab/tests/`); ruff clean.
- Each subtree collects independently (428 / 36 / 4).
- CI has a `packaged` job that installs the wheel and imports from **outside**
  the source tree, so a source-layout assumption fails there rather than in a
  job; and a `ledger` job that runs the §8 validator.

### On hardware (2 nodes, Aurora, `artifacts/hardening/supervisor-smoke`)

```
[Composition] READY via http://10.112.170.134:8000 —
  ['sessions: 2 planned ranks established',
   'receipts: 5/5 exact slots',
   'model meta-llama/Meta-Llama-3-8B-Instruct: 24/24',
   'canaries: 1 model(s) answered']
```

Nine checks, all PASS:

| check | result |
|---|---|
| `gate_ready` | PASS — the root's own verdict, not the child's |
| `shared_status_ready` | PASS — read through the §3.4 API, not a log |
| `marker_never_precedes_gate` | PASS |
| `receipt_slots_exact` | PASS (5/5 exact set equality) |
| `evidence_separate_from_verdict` | PASS |
| `ray_receipt_actor_retired` | PASS |
| `engine_self_attested` | PASS (48 engine self-receipts) |
| `canary` | PASS — a real completion through the compiled endpoint |
| `tree_reaped` | PASS — `supervisor_exit=143`, group 1→0, named 15→0 |

Every clause of that READY line was decorative or absent a day earlier. The
receipt slots are exact set equality against the compiled plan, fed by real
producers; the replica count comes from the plan, not from the survivors; the
canary is a real completion through the compiled advertised endpoint. 72
further evidence receipts (24 replica, 48 engine) arrived over the same path.

The run directory carries the four artifacts the architecture keeps distinct:
`deployment_status.json` (shared record), `readiness.json` (the root's
verdict), `deployment_evidence.json` (the child's witness statement), and
`allocation_binding.json` (this generation's identity).

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
8. The root's own binding hash was exported only into the ranks' environment,
   so the root could not attest itself and readiness blocked on
   `global/supervisor`.
9. Serve does not name applications after model ids — a single-model
   deployment is just `default` — so every model resolved to a zero replica
   target: "the deployment is empty" for a deployment that was fully up.
10. An orderly SIGTERM published `FAILED` on the shared status record, throwing
    away the 143-vs-fault distinction the exit code already made. Visible only
    because the state history now exists to be read.

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

Current state: **14 FIXED / 66 IN_PROGRESS / 2 OUT_OF_PRODUCTION_SCOPE**,
validator passing. Records held open name their reason in the evidence field
rather than leaving it to inference; the largest group is scale, gated on the
unapproved envelope.

The count is deliberately not impressive. A record closes when its invariant is
proven on the path production takes, and most of the remaining ledger belongs
to work packages this cutover did not touch (WP6–WP12) or to scale evidence
nobody can gather until the envelope decision exists.

## 7. Recommended next steps, in order

1. Obtain the S00/ADR-000 production-envelope approval. Until it exists, the
   scale records cannot be adjudicated in either direction, and roughly a third
   of the remaining ledger is blocked behind that one decision.
2. Demonstrate a `PROXIED_INTERNAL` plan end to end on hardware — the last
   architectural contract implemented but not exercised.
3. Re-qualify the ladder on the new architecture at the tiers the approved
   envelope actually claims.
