# ExaServe Supported Configuration & Scale Matrix

**Status:** provisional. Scale tiers below record FEASIBILITY SMOKE on the
legacy path, not WP12 qualification — see audit IMP-H08. This file is
the single place that states what is *claimed as supported* versus *validated*
versus *explicitly not a production claim* (PR-033 / AC-SCALE-01). It is
derived from `decisions/ADR-000-production-envelope.md` and updated as WP12
gates pass.

A configuration is **supported** only if it has passed its required
validation tier. A passing lower tier never implies a higher tier.
P00 is currently reopened under the strengthened S01-S03 proofs; none of the
rows below converts that partial evidence into a closed architecture gate.

## Software versions (measured, WP3)

| Component | Version | Notes |
|---|---|---|
| Python | 3.12.12 | Aurora frameworks 2025.3.1 |
| Ray | 2.53.0 (commit 0de2118) | overlay + patches pinned to this; `setup_overlay.sh` refuses a mismatch |
| vLLM | 0.15.0 | as bundled in frameworks 2025.3.1 |
| transformers | 4.57.6 | |
| torch | 2.10.0a0 (XPU) | |
| SGLang | venv-only | not in the base frameworks python |

## Support dimensions

| Dimension | First-release target (not yet a support claim) | Gated / experimental | Explicitly not proposed |
|---|---|---|---|
| Scheduler/site | Aurora PBS | Slurm (offsite gate owed) | — |
| Accelerator | Intel XPU (PVC, 12 tiles/node) | CUDA, ROCm (offsite gates owed) | — |
| Engine | vLLM | SGLang (smoke gate owed) | — |
| Gateway implementation | HAProxy | LiteLLM/Envoy/NGINX/Pingora = **validation/benchmark only**; N/A only with explicit validation-direct exposure | — |
| Exposure | HAProxy-proxied inference on trusted allocation/internal network; management endpoints local | direct Serve endpoint = **validation only** | public Internet or direct production exposure |
| Request mode | non-streaming | streaming (capability + scale gate owed) | — |

## Scale tiers (node count)

The technically selected candidate is `qualification_target = 64`, pending the
explicit product-owner scope approval required by ADR-000 and the plan. The
plan's
`ScaleEnvelope` enforces this: a plan may not exceed `supported_max` without a
validation-mode plan, and may never exceed `qualification_target` without
user/product-owner approval.

| Tier | Status | Evidence |
|---|---|---|
| 1 node | early-spike evidence only; clean packaged final-architecture qualification owed | P00 S03; P04 battery |
| 2 nodes | early-spike evidence only; final ownership/gateway/control negative matrix owed | P00 S01/S02/S03; P04 battery (2026-08-06) |
| 4 nodes | qualification owed | no final-architecture gate yet |
| 16 nodes | **feasibility smoke only** (0 err, 21.53 rps/node, 344 agg) — NOT a WP12 qualification: legacy path, `proxy_config: none`, no predeclared provenance (audit IMP-H08) | scaling-smoke n16 |
| 64 nodes | **feasibility smoke only** (0 err, 21.47 rps/node flat vs 16n, 1374 agg) — NOT a WP12 qualification (audit IMP-H08); production-gateway + target-architecture qualification still owed | scaling-smoke n64 (job 8737093) |
| 128 / 256 nodes | outside the current 64-node technical proposal; final release disposition pending explicit product-owner scope approval, with no current support claim | historical `findings/` data only |

## Known scale constraints (architectural, not tuning)

- Ray GCS / ServeController is single-threaded; measured O(N²) proxy/actor
  handle traffic drives the `wait_proxies` cliff at ≥64n (KI-A3/A4). Mitigated
  by compatibility-profile knobs (`RAY_gcs_server_num_threads`, health
  thresholds) at the ≤64n envelope; a sharded control plane is out of scope
  for this program and would be re-scoped (ADR-000).
- Single head-node gateway is the ingress ceiling at large N; non-streaming
  HAProxy measured healthy at 27.1k RPS/256n (corrected, KI-B1), streaming has
  a distinct network-concentration limit + a rare unexplained process death
  (KI-B2) — streaming stays unclaimed.
- Static engine ports are best-effort (KI-A2); race-safe port ownership is the
  WP7 target.

## Readiness semantics (ADR-002)

The cutover target fails closed using immutable compiled plan/SiteProfile
deadlines and the complete §3.2.1 predicate. Environment variables such as
`EXASERVE_ALLOW_DEGRADED_GPUS`, `EXASERVE_ALLOW_DEGRADED_PROXIES`, and
`EXASERVE_PROXY_READY_DEADLINE_S` are legacy migration controls, are not part of
the supported contract, and must be removed at WP13; they cannot override final
READY. Any future degraded mode must be a typed plan mode with explicit
capabilities and support evidence. Serve app-RUNNING remains necessary but not
sufficient (the early proof measured a 2.0s RUNNING→serving gap and roughly
120s replica-death blindness at two nodes).

## Evidence classes for compatibility receipts (added 2026-08-06)

A receipt is not just "present" — it carries an evidence class, and the two are
not interchangeable:

| Attestation | Who signs | Evidence | Used for |
|---|---|---|---|
| `self` | managed code in the target process/actor | required manifest entries proved by in-process semantic postconditions | supervisor and affected Serve actors/replicas/spawned engines |
| `supervisor` | the owning supervisor, for one individually identified unmodified external daemon | executable/argv/prepared-environment hashes, process identity, version probe, and only manifest entries explicitly not targeted to that role | unmodified Ray or gateway daemons when the resolved profile requires no in-process patch there |

An owner cannot claim that an in-process patch took effect inside an unmodified
daemon. If the resolved profile targets a patch or capability to that role,
`not provable` cannot satisfy readiness: use a version/profile-specific semantic
probe or make the combination unsupported. `NOT_REQUIRED` is valid only when
the canonical manifest explicitly excludes that patch from the role. The
readiness snapshot records each attestation type and exact receipt identity.

**EN-01 closed (2026-08-06).** The vLLM `EngineCore` now self-reports: the
generated `sitecustomize` shim writes a receipt from inside the engine process
and the owning replica forwards it, so `engine` is a `self` attestation
whenever the shim runs. Owner attestation is not a fallback for a required
engine self-report; a missing engine receipt blocks readiness.

The receipt describes what that process actually received. PP engines prove
each required patch with in-process sentinels. Non-PP engines deliberately
never receive the vLLM/PP patches: the compiler resolves the PP gate off, and
the manifest makes those entries not required for that process. Their v2 result
is `NOT_REQUIRED`, never `APPLIED`. A PP engine targeted by those entries but
missing the import/postcondition reports `FAILED` and is rejected.

A patch is only required where its gate requests it: `PatchSpec.env_gate`
scopes the required set to what `EXASERVE_VLLM_PATCH_PP_LAYER_FILTER` (and
future typed plan gates) resolve at compile time. Merely failing to import a
target module does not make a targeted patch optional; it is `FAILED` unless the
resolved manifest had already classified it `NOT_REQUIRED` for that role.
