# ADR-002: Readiness authority

**Status:** SELECTED AND PROVEN FOR FINAL42 AT ONE AND TWO NODES (2026-08-09).
Scale qualification and the product envelope remain separate in ADR-000.

## Decision

Only the allocation-head `ReadinessCoordinator`, owned by the composition root,
may publish or revoke canonical READY. It evaluates one indexed,
generation-bound projection over immutable planned identities and current typed
observations. No producer may publish its own global verdict.

| Conjunct | Authoritative input | Why it is sufficient |
|---|---|---|
| Allocation identity | immutable `AllocationBinding` derived from the scheduler node file | exact rank/node set, allocation ID, plan/site identity, generation |
| Rank liveness | authenticated session snapshot, heartbeat lease, reconnect state, and exact node-supervisor receipt | session is bound to plan/binding/generation/rank/node and is continuously freshness checked |
| Ray membership/resources | bounded public Ray cluster probe plus exact head/worker receipts | public resource data is a witness; receipts prove owned planned daemon instances |
| Serve applications/replicas | typed deployment-child observations matched to exact planned application/replica identities | absolute sets replace survivor counting; app RUNNING alone is not enough |
| Engine readiness/compatibility | exact self receipts from every required EngineCore/worker instance | each real process proves its own resolved role, patches, hashes, and semantic postconditions |
| Gateway | globally owned process identity, native config validation, listener/health observation, and failure evidence | an open unrelated socket or healthy unmanaged proxy cannot count |
| Route liveness | one typed completion per planned model/route through the compiled advertised endpoint | proves the externally advertised route, not an internal health endpoint |
| Persistence | atomic generation-bound status revision | consumers observe the same committed verdict; marker rendering happens only afterward |

Every observation has a bounded freshness lease. Missing, stale, unexpected,
duplicated, prior-generation, or identity-mismatched evidence produces a named
blocker. After READY, loss applies the plan's declared transition and terminal
policy rather than leaving a latched success.

## Evidence

- Unit/contract suites withhold each conjunct, exercise stale generations and
  exact blocker reporting, and prove high-cardinality indexed state, bounded
  retention, deduplication, atomic publication, and revocation. Principal tests:
  `tests/test_plan_readiness.py`, `tests/test_readiness_coordinator.py`,
  `tests/test_serve_readiness.py`, and `tests/test_no_readiness_marker_consumers.py`.
- Final42 one-node null and real-engine cells reach READY only after an HAProxy
  completion canary and exact receipt equality, then classify owned gateway
  death and exit nonzero.
- Final42 two-node null deliberately withholds a worker proxy and proves the
  deployment never reaches READY. It also kills an owned worker after READY and
  preserves the typed first cause through cleanup.
- Final42 two-node real PP=2 records the EngineCore and both stage-worker self
  receipts from two physical hosts before the HAProxy canary can satisfy READY.

Hardware evidence:

- `artifacts/hardening/final42-null-1n-20260809-a1/qualification/result.json`;
- `artifacts/hardening/final42-real-1n-20260809-a1/qualification/result.json`;
- `artifacts/hardening/final42-null-2n-20260809-a1/qualification/result.json`;
- `artifacts/hardening/final42-real-2n-20260809-a1/qualification/result.json`;
- `artifacts/hardening/final42-supervisor-watchdog-v3q2-2n-20260809-a1/qualification/result.json`.

All four name wheel
`5346c7ab858b056448702b207b76350ac2ee134a65fa45ea67779039d41362e3`.

## Dependency limitations retained from the feasibility ladder

- Serve's application/replica status is useful but lagging and cannot be the
  replica-health authority. Earlier probes observed an app-RUNNING-to-serving
  gap and long replica-death blindness.
- `ray.util.state` depends on dashboard services absent from the pinned Aurora
  environment and is not a readiness dependency.
- Ray metrics-exporter availability is optional and does not affect the owned
  readiness/status/metrics contracts.
- A canary HTTP client ignores ambient corporate proxy variables, but it never
  bypasses the plan's HAProxy route unless the plan is explicitly
  `DIRECT_VALIDATION`.

## Rejected alternatives

- stdout/log markers, including `CLUSTER READY`;
- PBS `RUNNING` or process exit aggregation;
- one proxy health endpoint or one surviving replica;
- public Serve RUNNING state as the global verdict;
- shared files without descriptor/generation binding;
- fleet-wide polling by producers; and
- a Ray actor or deployment child publishing global READY.

## Revisit condition

Any readiness-source/schema change, frameworks upgrade, supported gateway or
engine change, or new exposure boundary reruns the relevant negative and
hardware gates. Scale tiers must additionally pass the exact-candidate ladder
selected by an approved ADR-000 envelope.
