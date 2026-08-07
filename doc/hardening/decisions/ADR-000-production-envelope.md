# ADR-000: Initial production envelope and scale topology

**Status:** TECHNICALLY SELECTED, AWAITING EXPLICIT PRODUCT-OWNER SCOPE
APPROVAL; production support also remains unqualified until the final-
architecture WP12 gates pass. Drafted 2026-08-05; reconciled with plan §3.2.1
on 2026-08-07. A worker may not fill the missing approval identity/evidence.
The older P00 closure is superseded: S01-S03 technical gates are also reopened.
Only after those pass may plan §4.2's `TECHNICAL_PASS_SCOPE_PENDING` status
authorize generic P01-P05 mechanics while this approval remains open.

## Decision

First-release support dimensions (each independent, per plan S00):

| Dimension | Proposed first-release target (qualification pending) | Gated / not proposed |
|---|---|---|
| Scheduler/site | Aurora PBS (backend selection finalized in WP8) | Slurm, other sites |
| Vendor/accelerator | Intel XPU (PVC, 12 GPUs/node) | CUDA, ROCm |
| Engine | vLLM (frameworks 2025.3.1 profile; exact versions discovered in S03) | SGLang |
| Gateway implementation | HAProxy | LiteLLM/Envoy/NGINX/Pingora = validation/benchmark only; N/A only for an explicit validation-direct exposure |
| Exposure | HAProxy-proxied inference on the trusted allocation/internal network; management endpoints local | direct Serve endpoint = validation only; any public exposure |
| Request mode | Non-streaming | Streaming (capability/scale gates owed) |
| Nodes | proposed `qualification_target = 64` (1→2→4→16→64) | 128/256 tiers not proposed for this pass; explicit approved envelope expansion required |
| Replicas | ≤ 12 × nodes (one GPU-replica granularity; PP per AC-PP-01) | multi-replica non-shard PP |

## Scale-topology question (plan S00 ¶2)

**Proposed scope decision:** 256-or-more-node service is not a release
requirement for this pass; final qualification would be
1 → 2 → 4 → 16 → 64 nodes. This reduction is not frozen until the required
product-owner approval identity, timestamp, evidence reference, and exact scope
are recorded. Ray Serve's measured internal `R*N²`
actor-handle behavior (KI-A3) and the wait_proxies cliff (KI-A4) therefore do
not force a Ray fork/sharded-control-plane program into this pass; existing
mitigations (`RAY_gcs_server_num_threads`, UNHEALTHY_THRESHOLD patch) remain
compatibility-profile capabilities at ≤64 nodes, where the recorded evidence
(cliff onset ~64n is the boundary tier) must be re-measured at the 64-node
gate. Scale above the accepted envelope is rejected by plan validation unless
a separate topology gate passes later (AC-SCALE-01). 128/256 evidence already
in `findings/` remains recorded but confers no support claim.

`proxy_config.type: none` is not a production gateway or an implicit supported
exposure mode. During migration it may be accepted only as an explicitly
validation/benchmark direct-exposure plan. That plan advertises and canaries
the allocation-reachable Serve endpoint but cannot establish a production
support claim. An eval-side `dest=direct` choice changes client routing only; it
does not change the deployment's compiled gateway/exposure contract. Adding
direct exposure to production requires reopening this ADR and passing its own
security, readiness, failure, and scale gates.

**Scalability acceptance for the proposed final 64-node qualification:** run
the final architecture through HAProxy with the declared client topology and
compare it to a matched HAProxy baseline. The historical direct result
(approximately linear weak scaling and ~6.8k aggregate RPS at 64 nodes in
`findings/weakscaling_short_v3_progress.md`) is contextual regression evidence
only; it cannot qualify a different gateway/exposure dimension. Preserve exact
replication/client topology per KI-C5.

## Rationale

- Aurora XPU/vLLM/HAProxy/non-streaming is the only combination with useful
  historical end-to-end evidence (audit §2; KNOWN_ISSUES B1-corrected 27.1k
  @256n non-stream). That is candidate-selection context, not clean packaged
  target-architecture qualification or a 256-node support claim.
- The 64-node ladder is the smallest technically coherent candidate matching
  the hardening campaign, but it still needs the explicit scope approval above.
- Streaming at scale has a live unexplained failure mode (KI-B2 rare HAProxy
  process death) — claiming it would violate plan §1 ("works on Aurora" ≠
  closed).

## Rejected alternatives

- Claiming 256n now: requires the unresolved single-control-plane topology
  decision plus prod-queue qualification budget; it is not the selected
  technical candidate and remains pending the owner's scope decision.
- Choosing LiteLLM as gateway: fake-streaming semantics (KI-B3) disqualify it
  as a production gateway candidate.

## Revisit condition

First obtain the product-owner decision selecting 64 versus a larger release
target. After a 64 decision is durably recorded, any later expansion (for
example 128/256, streaming, SGLang, or Slurm/ROCm) reopens S00 and requires a
new estimate before affected schema/support fields freeze; `ScaleEnvelope`
encodes qualification target versus supported maximum explicitly.
