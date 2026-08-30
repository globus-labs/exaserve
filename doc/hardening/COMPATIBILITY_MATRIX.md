# ExaServe configuration and scale matrix

**Status date:** 2026-08-30

**Last packaged qualified candidate:** final43

**Successor source:** `release-v0.4.0`; clean package passes, scale gate pending

**Formal release state:** `SUCCESSOR_SOURCE_READY_SCALE_PENDING`

This is the current-facing matrix. “Qualified candidate” means the exact
immutable artifact passed that cell; it is not a published support promise
until ADR-000 receives product-owner scope approval. A lower tier never implies
a higher tier or a different dependency/profile dimension.

## Exact software profile

| Component | Selected identity | Qualification note |
|---|---|---|
| ExaServe | 0.4.0 wheel SHA `1041be53eb5b5875d198d5ee6c6664718b4085775dcba99107873dd3d1fcdff2` | final43 immutable candidate |
| ExaServe successor | 0.4.0 wheel SHA `75bab1c97948d29237000d5fa087f3ed1f1d18e5ef3233fe2c759b4faf0e32f8` | release-v0.4.0 package gate passes; production scale unqualified |
| Aurora frameworks | 2025.3.1 | qualification environment |
| Python | 3.12.12 | packaged and hardware gates |
| Ray | 2.53.0, commit `0de2118` | exact base hashes; generated overlay; installed files untouched |
| vLLM | 0.15.0+xpu | real EngineCore/worker receipts at one and two nodes |
| transformers | 4.57.6 | selected environment |
| torch | 2.10.0a0 XPU | selected environment |
| HAProxy | executable identity recorded per run | owned and native-config validated |

Candidate-bound hashes:

- site profile: `4814429547fd4397014819a0f8b5c6ec8f7d77c889eaf844d27935b39a0a6e26`;
- compatibility profile: `c17e684fe485261a9cfa82248bd24a9209b66a7c66bae8b889b24ca878d335d3`;
- compatibility manifest: `cd85123822f4b936216282ed43346223a4b68f1a7cb152a85715a36fdab24259`.

Any dependency or identity change is a new profile and reopens affected cells.

## Dimension matrix

| Dimension | Qualified candidate | Validation-only / unqualified | Rejected or unsupported |
|---|---|---|---|
| Site/scheduler | ALCF Aurora with native PBS | PSI/J rendering/interface tests | Native Slurm production use |
| Accelerator | Intel PVC XPU, 12 tiles/node | — | CUDA and ROCm until offsite qualification |
| Engine | vLLM | null engine for lifecycle faults | SGLang in the selected profile |
| Gateway | HAProxy | LiteLLM, Envoy, NGINX, Pingora adapters | Unowned/unprofiled gateways |
| Exposure | `PROXIED_INTERNAL` on trusted allocation network | `DIRECT_VALIDATION` | public Internet/direct Serve production exposure |
| API/mode | OpenAI-compatible non-streaming completion | streaming/chat/mixed experiments; LiteLLM buffered throughput/error evidence | buffered, coarse, or unclassified timing represented as incremental TTFT/TBT |
| Model topology | TP=1/PP=1 at one node; one PP=2 replica across two nodes | explicitly gated combinations | topology outside the canonical capability set |
| Scale | one and two physical nodes | proposed 4/16/64 ladder | >2 as a current support claim; 128/256 historical only |

The selected production envelope is
`aurora-xpu-vllm-haproxy-completion-non_streaming-candidate64`. It records
`supported_max_nodes=2`, `qualification_target_nodes=64`,
`qualification_target_approved=false`, and `validation_mode=false`. Normal
production plan compilation rejects unmatched dimensions or more than two
nodes. Explicit validation mode may create experiment-only plans but cannot
satisfy production qualification.

## Scale tiers

| Physical nodes | production status | Evidence / next requirement |
|---:|---|---|
| 1 | **QUALIFIED CANDIDATE** — null and real XPU plus proxy toggle pair | `final43-null-1n-20260809-a1`, `final43-real-1n-20260809-a1`, proxy on/off results |
| 2 | **QUALIFIED CANDIDATE** — null fault matrix, real PP=2, supervisor faults | `final43-null-2n-20260809-a1`, `final43-real-2n-20260809-a1`, supervisor q2 result |
| 4 | **NOT RUN FOR THE SUCCESSOR WHEEL** | owner authorization and a new predeclared gate are required |
| 16 | **NOT QUALIFIED** | only after the approved envelope includes this tier |
| 64 | **PROPOSED, UNAPPROVED, NOT QUALIFIED** | owner approval plus exact-candidate 4/16/64 ladder |
| 128 / 256 | **NO SUPPORT CLAIM** | historical research context; explicit envelope expansion required |

## Qualification semantics

One/two-node qualification proves:

- exact scheduler membership and authenticated per-rank sessions;
- exact source-staging and compatibility receipt sets;
- planned Ray resources, Serve applications/replicas, and engine instances;
- owned HAProxy identity, native configuration, route canary, and toggle arms;
- false-READY prevention for port collision and partial proxy state;
- revocation and exact first-cause classification after owned process loss; and
- bounded reverse-order cleanup with zero exact-generation survivors.

The real two-node cell binds PP stages to two physical hosts. The supervisor
campaign kills the attested Ray head child and worker supervisor without name
matching and validates one FAILED terminal record for each.

## Residual constraints

- Ray's driver may emit GCS/task retry errors for up to its configured
  120-second reconnect timeout after catastrophic head/worker loss. Cleanup is
  bounded, but recovery is not advertised as instantaneous.
- Optional Ray metrics-exporter availability is not a readiness input.
- Historical controller-pressure, actor-handle, proxy-cliff, shared-filesystem,
  and streaming-failure data remain reasons to require the unrun scale ladder.
- The no-delay on/off pair is a correctness/confound-control gate, not a
  throughput equivalence or large-scale performance claim.

## Revisit condition

First record an owner decision selecting the release ceiling. Then declare and
execute the exact release-v0.4.0 scale cells authorized by that decision. Any later
expansion—streaming, public exposure, another gateway/engine/vendor/scheduler,
or 128/256 nodes—requires a new profile, evidence plan, and candidate review.
