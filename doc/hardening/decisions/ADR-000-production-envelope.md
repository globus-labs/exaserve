# ADR-000: Initial production envelope and scale topology

**Status:** PROVISIONAL (P00 co-design loop; freezes only when S01–S03 proofs
agree per plan §4.2). Drafted 2026-08-05.

## Decision

First-release support dimensions (each independent, per plan S00):

| Dimension | Claimed for first release | Gated (not claimed) |
|---|---|---|
| Scheduler/site | Aurora PBS (backend selection finalized in WP8) | Slurm, other sites |
| Vendor/accelerator | Intel XPU (PVC, 12 GPUs/node) | CUDA, ROCm |
| Engine | vLLM (frameworks 2025.3.1 profile; exact versions discovered in S03) | SGLang |
| Gateway | HAProxy (production candidate) | LiteLLM/Envoy/NGINX/Pingora = benchmark/experimental |
| Exposure | Trusted allocation/internal network; management endpoints local | Any public exposure |
| Request mode | Non-streaming | Streaming (capability/scale gates owed) |
| Nodes | `qualification_target = 64` (this program qualifies 1→2→16→64) | 128/256 tiers: VALIDATION_OWED, outside the claimed envelope |
| Replicas | ≤ 12 × nodes (one GPU-replica granularity; PP per AC-PP-01) | multi-replica non-shard PP |

## Scale-topology question (plan S00 ¶2)

**256-or-more-node service is NOT a release requirement for this program.**
The active user request sets the qualification ladder at one node → two nodes
→ smoke scalability up to 64 nodes. Ray Serve's measured internal `R*N²`
actor-handle behavior (KI-A3) and the wait_proxies cliff (KI-A4) therefore do
not force a Ray fork/sharded-control-plane program into this pass; existing
mitigations (`RAY_gcs_server_num_threads`, UNHEALTHY_THRESHOLD patch) remain
compatibility-profile capabilities at ≤64 nodes, where the recorded evidence
(cliff onset ~64n is the boundary tier) must be re-measured at the 64-node
gate. Scale above the accepted envelope is rejected by plan validation unless
a separate topology gate passes later (AC-SCALE-01). 128/256 evidence already
in `findings/` remains recorded but confers no support claim.

**Scalability acceptance for the 64-node smoke (user requirement):** measured
throughput/efficiency at the 64-node tier must be consistent with or better
than the recorded pre-hardening baselines for the same workload shape
(reference points: direct ≈ linear weak-scaling ~6.8k RPS aggregate at 64n in
`findings/weakscaling_short_v3_progress.md`; exact replication spec chosen at
the gate with identical client topology per KI-C5).

## Rationale

- Aurora XPU/vLLM/HAProxy/non-streaming is the only combination with existing
  end-to-end evidence (audit §2; KNOWN_ISSUES B1-corrected 27.1k @256n
  non-stream, though that tier stays unclaimed here).
- The user's active request fixes the qualification ladder (1 → 2 → ≤64).
- Streaming at scale has a live unexplained failure mode (KI-B2 rare HAProxy
  process death) — claiming it would violate plan §1 ("works on Aurora" ≠
  closed).

## Rejected alternatives

- Claiming 256n now: requires the unresolved single-control-plane topology
  decision plus prod-queue qualification budget; out of the requested scope.
- Choosing LiteLLM as gateway: fake-streaming semantics (KI-B3) disqualify it
  as a production gateway candidate.

## Revisit condition

User/product owner expands the envelope (e.g., 128/256 tier, streaming,
SGLang, Slurm/ROCm) → re-open S00 and re-estimate before WP1 freezes affected
schema fields; the `ScaleEnvelope` schema (WP1) encodes qualification_target
vs supported_max explicitly.
