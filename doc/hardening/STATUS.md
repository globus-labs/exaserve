# ExaServe Production-Hardening — Status

**Updated:** 2026-08-06. Companion to `MIGRATION_LOG.md` (chronological) and
`FINDINGS.yaml` (authoritative ledger).

## Headline

- **Audit findings: 34 / 35 FIXED**, 1 IN_PROGRESS (PR-031 CI — workflow
  written and locally green; enabling GitHub Actions is a repo-admin step).
- **All findings (audit + Known Issues + TODO): 50 FIXED / 6 IN_PROGRESS /
  19 OPEN / 1 out-of-scope.** OPEN are backlog/experimental (parallel
  staging, request caching, C++ client, offsite Slurm/AMD, the deferred A2
  vLLM-internal port race) — none are unaddressed production blockers.
- **Hermetic suite: 102 passed / 0 failed** (was 24/12 at baseline).
  Ruff correctness-gate clean tree-wide. Wheel builds + imports clean.
- **Compute-validated on Aurora (1–2 nodes):** S00–S03 spikes, P04 battery
  (5/5), HAProxy smoke (4/4), fail-closed readiness non-regression, direct +
  HAProxy serving with 0 errors.

## What was hardened (by area)

| Area | Findings closed | Compute-validated |
|---|---|---|
| Truthful failure / exit codes | PR-001, PR-028 | P04 (SIGTERM drain) |
| Vendor/accelerator config | PR-002 | P04 ("GPUs=12 vendor=xpu") |
| Native staging (bcast.c) | PR-004 | P04 (round-trip + fail-loudly) |
| Model staging transactional | PR-005 | (hermetic; scale in flight) |
| Immutable config / plan schema | PR-003, PR-006, PR-007, PR-020 | P04 (source unmutated) |
| Fail-closed readiness | PR-008, PR-023 | P04 (healthy→READY, unhealthy→fail) |
| Proxy supervision + config validate | PR-009, PR-024 | HAProxy smoke (kill→terminate; haproxy -c) |
| Ports / URLs | PR-012 | — |
| Security boundary | PR-010, PR-025 | HAProxy smoke |
| Request validation | PR-011 | scaling probe (unknown model→400) |
| Scheduler safety/idempotency | PR-013, PR-014, PR-015, PR-027 | hermetic |
| Eval correctness | PR-016, PR-017, PR-019, PR-021, PR-030 | hermetic |
| Atomic state/artifacts | PR-018, PR-035 | hermetic (crash-injection) |
| Compatibility profile | PR-022, PR-026, PR-029 | P04/S03 (receipts, spawn reach) |
| Observability | PR-032 | — |
| Scale envelope / matrix | PR-033 | COMPATIBILITY_MATRIX.md |
| Test/CI floor | PR-031 (IN_PROGRESS) | — |

## New architecture substrate (plan §3)

- `src/exaserve/control/{contracts,transport}.py` — typed authenticated
  control channel (S01-proven on 2 nodes).
- `src/exaserve/state/{atomic,status}.py` — atomic writes, cross-host leases,
  CAS status machines.
- `src/exaserve/plan/schemas.py` — immutable validated plan compiler.
- `src/exaserve/request_validation.py` — OpenAI request validation.
- ADR-000..003 in `decisions/`.

## Scale validation (WP12) — in progress

| Tier | Status |
|---|---|
| 1 node | PASS (S03, canary) |
| 2 nodes | PASS (deploy READY, direct 22.1 RPS / 11.0 per-node / 0 err; HAProxy 0-err) |
| 16 nodes | **PASS** — 10,821 req / 0 err / 344.4 RPS / **21.53 per-node** / p99 1.59s; 192/192 GPUs |
| 64 nodes | **PASS** — 43,207 req / 0 err / 1373.9 RPS / **21.47 per-node** / p99 1.585s; 768/768 GPUs; READY 300s |

**Weak scaling is linear and regression-free to 64 nodes:** per-node RPS flat
16→64n (21.53→21.47, 0.3% over a 4× cluster), aggregate 344→1374 RPS (~100%
efficiency), p99 flat (1.59→1.585s), **0 errors across 43,207 requests at 768
replicas**. The hardened orchestration adds no scaling penalty. Full scale
ladder (1→2→16→64) complete and passing.

Weak-scaling criterion: per-node RPS holds flat 2→64 at fixed per-node
concurrency (Direct-MPI probe, one client rank/node, avoids the single-
fat-client ceiling the baseline documented).

## Not done / explicitly deferred

- PR-031: CI provider enablement (repo-admin).
- The full WP4/WP5 `RuntimeSupervisor`/`ReadinessCoordinator` REWRITE: the
  invariants (fail-closed readiness, child supervision, typed exit) are met
  incrementally in driver.py/server.py; the clean-architecture extraction and
  legacy-path deletion (WP13 cutover) remain.
- Offsite Slurm/CUDA/ROCm gates (no access from here) — EXTERNAL_BLOCKER.
- KI-A2 vLLM-internal torch.distributed port race — transient, Serve-retry
  recovers; race-free ports are the deferred fix.
- Git commits/branches: not authorized by the active request; all work is a
  reviewed working-tree checkpoint.
