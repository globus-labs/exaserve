# ADR-002: Readiness authority

**Status:** PROVISIONAL SOURCE MAPPING; P00/S02 REOPENED on 2026-08-07. The
2026-08-05 two-node direct-Serve experiments establish useful dependency
limitations, but they do not prove the strengthened production readiness gate
through the globally owned HAProxy advertised endpoint. Product scope approval
also remains pending in ADR-000. Evidence:
`artifacts/hardening/s02-2n/`, `scripts/hardening/spike_s02.py` (4 runs; runs
1/3/4 are partial evidence).

## Target source mapping (binding design; final proof open)

| Conjunct | Selected source | Ladder rung | Evidence |
|---|---|---|---|
| Node membership/resources | public `ray.nodes()` / `ray.cluster_resources()` | 1 | Q1 exact 2-node membership, correct hosts/CPUs |
| App/deployment/replica states | public `serve.status()` — **as a lagging witness only, never the readiness authority** | 1 (bounded role) | Q2 OK, but see the 120 s blindness below |
| Replica health | **ExaServe generation-scoped engine registration** over the control channel (plan §3.1), NOT Serve replica states | ExaServe-owned | run 3/4: after SIGKILL of a replica worker, controller logged `ActorDiedError` within ~6 s yet public status held `RUNNING:2`; the controller marked it unhealthy only **~120 s** after the kill (07:14:55 kill → 07:16:55 "marking it unhealthy immediately"). A dead replica is publicly "RUNNING" for ~2 minutes at DEFAULT constants |
| Per-node proxy health | public `serve.status().proxies` (per-node keyed) polled with deadline | 1 | Q3: healthy at 5.6 s — 2.0 s AFTER app RUNNING (3.6 s); a point-in-time read at app-RUNNING sees STARTING (run 1) |
| Route liveness | **per-model/route inference canary through the one compiled canonical advertised endpoint**, deadline-bounded; separate typed proxy observations cover every required node | ExaServe-owned | Q4's historical probe reached direct per-node Serve proxy endpoints: routes were 200 at 5.6 s, while both were 502 at the app-RUNNING instant. This proves the readiness gap, not the target HAProxy path. The final canary client must ignore ambient corporate proxy variables while still targeting the compiled HAProxy endpoint; it must not bypass that gateway unless the plan is explicitly validation-only direct exposure. |
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
2. Replica-level truth must come from ExaServe engine registrations pushed on
   state change over the §3.2 channel plus post-READY lease expiry, as required
   by plan §5. The old experiment supports avoiding fleet-wide polling but does
   not prove the final authenticated registration/lease path; AC-RDY-02 still
   owes the synthetic cardinality proof.
3. Advertised-endpoint canaries are mandatory per model/route. “Proxy-env-free”
   means the canary HTTP client ignores ambient `http_proxy`/`https_proxy`; it
   does not mean bypassing the compiled HAProxy gateway. Per-node proxy/route
   observations prove fleet membership without one external canary per replica
   or per node. Validation-only direct exposure canaries its explicitly
   advertised Serve endpoint as specified by plan §3.2.1.
4. D1's direct-Serve incident mechanism is reproduced at minimal
   scale: "app RUNNING + one proxy HEALTHY" can coexist with 0% servable
   direct routes (run 1) and with dead replicas (runs 3/4). That does not prove
   global-gateway ownership, advertised-endpoint readiness, or revocation.

## Reopened S02 proof owed before P00 technical pass

- Run the production target with the allocation-head `RuntimeSupervisor`
  owning HAProxy and send every per-model canary through the one compiled
  advertised endpoint; separately prove explicit validation-direct mode.
- Exercise stale generation, missing rank/replica/receipt, proxy-before-deploy,
  one-replica-only canary success, broken route with a live process, live
  gateway validation failure/recovery, unexpected gateway exit, and component
  death after READY. Verify the exact `READY -> VALIDATING -> {READY, FAILED}`
  and direct-to-FAILED transitions.
- Prove exact current-generation engine/receipt registration over the final
  control path, atomic READY persistence/revocation, and the synthetic
  high-cardinality linear-work/message bounds.

Until these pass, this ADR records selected target sources and rejected
dependency authorities, not a closed S02 gate or a support claim.

## Rejected alternatives

- Serve replica states as the replica-health authority (120 s blind window).
- `ray.util.state` (dashboard-dependent; unavailable).
- Log markers (plan-forbidden; S01 already showed exit codes/stdout lose
  causality).

## Revisit condition

First close the reopened one-/two-node S02 proof. Then re-measure the
RUNNING→serving gap, proxy-health lag, and replica-death detection latency at
the tiers selected by the approved envelope; if Serve internals change the
status semantics on a frameworks upgrade, the compatibility profile pins the
adapter (WP3).
