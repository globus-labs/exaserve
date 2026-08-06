# ExaServe Supported Configuration & Scale Matrix

**Status:** provisional. Scale tiers below record FEASIBILITY SMOKE on the
legacy path, not WP12 qualification — see audit IMP-H08. This file is
the single place that states what is *claimed as supported* versus *validated*
versus *explicitly not a production claim* (PR-033 / AC-SCALE-01). It is
derived from `decisions/ADR-000-production-envelope.md` and updated as WP12
gates pass.

A configuration is **supported** only if it has passed its required
validation tier. A passing lower tier never implies a higher tier.

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

| Dimension | Supported (claimed) | Gated / experimental | Explicitly NOT claimed |
|---|---|---|---|
| Scheduler/site | Aurora PBS | Slurm (offsite gate owed) | — |
| Accelerator | Intel XPU (PVC, 12 tiles/node) | CUDA, ROCm (offsite gates owed) | — |
| Engine | vLLM | SGLang (smoke gate owed) | — |
| Gateway | HAProxy; `none` (direct) | LiteLLM/Envoy/NGINX/Pingora = **benchmark only** | — |
| Exposure | trusted allocation / internal network; mgmt endpoints local | — | public Internet exposure |
| Request mode | non-streaming | streaming (capability + scale gate owed) | — |

## Scale tiers (node count)

`qualification_target = 64` nodes for this program (ADR-000). The plan's
`ScaleEnvelope` enforces this: a plan may not exceed `supported_max` without a
validation-mode plan, and may never exceed `qualification_target` without
user/product-owner approval.

| Tier | Status | Evidence |
|---|---|---|
| 1 node | validated (lifecycle, engine, canary) | P00 S03; P04 battery |
| 2 nodes | validated (ownership, staging, readiness, pp=2 canary, SIGTERM drain) | P00 S01/S02/S03; P04 battery (2026-08-06) |
| 16 nodes | **feasibility smoke only** (0 err, 21.53 rps/node, 344 agg) — NOT a WP12 qualification: legacy path, `proxy_config: none`, no predeclared provenance (audit IMP-H08) | scaling-smoke n16 |
| 64 nodes | **feasibility smoke only** (0 err, 21.47 rps/node flat vs 16n, 1374 agg) — NOT a WP12 qualification (audit IMP-H08); production-gateway + target-architecture qualification still owed | scaling-smoke n64 (job 8737093) |
| 128 / 256 nodes | NOT a production claim for this program | historical `findings/` data only |

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

READY fails closed on: GPU registration shortfall
(`EXASERVE_ALLOW_DEGRADED_GPUS=1` to override), proxy-serving deadline
(`EXASERVE_PROXY_READY_DEADLINE_S`), and any unhealthy proxy
(`EXASERVE_ALLOW_DEGRADED_PROXIES=1` to override). Serve app-RUNNING is
necessary but not sufficient (measured 2.0s RUNNING→serving gap and ~120s
replica-death blindness at 2 nodes with default constants).

## Evidence classes for compatibility receipts (added 2026-08-06)

A receipt is not just "present" — it carries an evidence class, and the two are
not interchangeable:

| Attestation | Who signs | Evidence | Used for |
|---|---|---|---|
| `self` | our own code, in-process | each required patch proved by an `_exaserve_*` sentinel on the patched symbol | `replica` |
| `supervisor` | the owning process | executable + prepared environment + version probe; every required patch declared **not provable here** | `ray_head`, `ray_worker`, `engine`, `supervisor` |

An external attestation cannot reach inside an unmodified daemon for a
sentinel, so it must **declare** the required set as unprovable rather than
report it applied; a `supervisor` receipt that claims applied patches is
rejected. The readiness snapshot records `externally_attested_roles`, so a
reader can always see which roles rest on the weaker evidence.

**Known limitation.** The vLLM `EngineCore` subprocess is currently attested by
its owning replica. Patches reach it through the EN-01 generated
`sitecustomize` shim, but the engine does not yet self-report, so `EN-01` and
the `SC-*` set are not proved *inside* the engine process. Closing this means
having the shim publish a receipt from the engine process itself; until then
the engine's row above is the honest description of what is known.

A patch is only required where its gate requests it: `PatchSpec.env_gate`
scopes the required set to what `EXASERVE_VLLM_PATCH_PP_LAYER_FILTER` (and
future gates) actually enable, and a patch whose target module is not imported
in a process is recorded as `not_applicable`, never as applied.
