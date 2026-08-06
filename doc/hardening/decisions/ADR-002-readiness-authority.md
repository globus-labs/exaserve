# ADR-002: Readiness authority

**Status:** DECIDED for the ≤64-node envelope (S02 two-node proofs 2026-08-05,
gate S02-2N, allocation 8734762; scale re-measurement owed at the 16/64
tiers). Evidence: `artifacts/hardening/s02-2n/`, spike
`scripts/hardening/spike_s02.py` (4 runs; runs 1/3/4 evidentiary).

## Decision (per readiness conjunct)

| Conjunct | Selected source | Ladder rung | Evidence |
|---|---|---|---|
| Node membership/resources | public `ray.nodes()` / `ray.cluster_resources()` | 1 | Q1 exact 2-node membership, correct hosts/CPUs |
| App/deployment/replica states | public `serve.status()` — **as a lagging witness only, never the readiness authority** | 1 (bounded role) | Q2 OK, but see the 120 s blindness below |
| Replica health | **ExaServe generation-scoped engine registration** over the control channel (plan §3.1), NOT Serve replica states | ExaServe-owned | run 3/4: after SIGKILL of a replica worker, controller logged `ActorDiedError` within ~6 s yet public status held `RUNNING:2`; the controller marked it unhealthy only **~120 s** after the kill (07:14:55 kill → 07:16:55 "marking it unhealthy immediately"). A dead replica is publicly "RUNNING" for ~2 minutes at DEFAULT constants |
| Per-node proxy health | public `serve.status().proxies` (per-node keyed) polled with deadline | 1 | Q3: healthy at 5.6 s — 2.0 s AFTER app RUNNING (3.6 s); a point-in-time read at app-RUNNING sees STARTING (run 1) |
| Route liveness | **external per-model HTTP canary through every required node**, deadline-bounded | ExaServe-owned | Q4: routes 200 at 5.6 s; at the app-RUNNING instant both nodes 502 (run 1). Canaries MUST bypass proxy env (`http_proxy` turns node-to-node canaries into corporate-proxy 502s — harness-proven) |
| Generation identity | **ExaServe plan-hash/generation in observations**; public `last_deployed_time_s` is a timestamp, not a generation | ExaServe-owned | Q6 |
| `ray.util.state` observability API | **REJECTED as a dependency** | — | requires the dashboard agent (:8265), absent on this stack (run 2 `ServerUnavailable`); metrics exporter agent also absent (log spam in every run) |

## Consequences

1. READY = the plan §5 predicate; Serve's app RUNNING is necessary but far
   from sufficient: at 2 nodes there is already a 2.0 s RUNNING→serving gap,
   and replica death is publicly invisible for ~120 s. The legacy overlay
   constants (health period 120 s / timeout 600 s / threshold 100 —
   OV-const-02/03/06/07, SV-01) stretch that blindness to **hours** at the
   extreme; those values are load-shedding for 256-node control-plane storms
   and must become scale-gated profile capabilities, not defaults (feeds
   PR-008/PR-033 and the WP3 profile).
2. Replica-level truth comes from ExaServe engine registrations pushed on
   state change over the §3.2 channel plus post-READY lease expiry — exactly
   the plan §5 design; no fleet-wide polling (S02 measured none needed at
   2 nodes; AC-RDY-02 synthetic test covers cardinality).
3. External canaries are mandatory per model/route and must use
   direct (proxy-env-free) connections.
4. D1's incident mechanism is now reproduced and understood at minimal
   scale: "app RUNNING + one proxy HEALTHY" can coexist with 0% servable
   routes (run 1) and with dead replicas (runs 3/4).

## Rejected alternatives

- Serve replica states as the replica-health authority (120 s blind window).
- `ray.util.state` (dashboard-dependent; unavailable).
- Log markers (plan-forbidden; S01 already showed exit codes/stdout lose
  causality).

## Revisit condition

Re-measure the RUNNING→serving gap, proxy-health lag, and replica-death
detection latency at the 16- and 64-node gates; if Serve internals change the
status semantics on a frameworks upgrade, the compatibility profile pins the
adapter (WP3).
