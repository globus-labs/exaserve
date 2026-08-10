# Production compatibility inventory

**Updated:** 2026-08-09. This is the current selected manifest inventory.
Historical SC/OV/SH/SV records and the eliminated alternatives are preserved
in repository history and summarized in ADR-003; they are not active policy.

The machine authority is `src/exaserve/compat/profile.py`. Full hashes,
affected symbols, import timing, semantic probes, removal references, patch
implementation hashes, and delivery-code hashes are included in the profile
and therefore its SHA-256 identity. Prefixes below are for review only.

## Pinned base

| Component | Exact version | Distribution observation |
|---|---|---|
| Python | 3.12.12 | Aurora frameworks 2025.3.1 |
| Ray | 2.53.0 | 4,829 recorded files; non-pure cp312 wheel; no runtime `direct_url` provenance |
| vLLM | 0.15.0+xpu | 3,014 recorded files; pure Python wheel; original build URI recorded but absent at runtime |
| ExaServe | 0.4.0 | exact release wheel/source identity supplied by the run bundle |

## Active entries

| ID | Distribution/version | Pinned base file | Base SHA-256 prefix | Required roles | Delivery | Capability |
|---|---|---|---|---|---|---|
| SC-01 | vLLM 0.15.0+xpu | `vllm/config/vllm.py` | `940f2c526bbd` | replica, engine core, engine worker | generated overlay | PP layer alias |
| SC-02 | vLLM 0.15.0+xpu | `vllm/v1/worker/utils.py` | `38ad72193886` | replica, engine worker | generated overlay | PP KV binding |
| SC-03 | vLLM 0.15.0+xpu | `vllm/forward_context.py` | `2d6d7d5bc92f` | replica, engine worker | generated overlay | PP forward context |
| SC-04 | vLLM 0.15.0+xpu | `vllm/attention/layer.py` | `74f359628f76` | replica, engine worker | generated overlay | PP attention context |
| SC-05 | vLLM 0.15.0+xpu | `vllm/v1/worker/gpu_model_runner.py` | `e8a73e1ccbff` | replica, engine worker | generated overlay | PP backend lookup |
| SC-09 | vLLM 0.15.0+xpu | `vllm/v1/executor/ray_executor.py` | `7576bec19a3e` | replica, engine core | generated overlay | XPU channel selection |
| SC-10 | vLLM 0.15.0+xpu | `vllm/v1/executor/ray_executor.py` | `7576bec19a3e` | replica, engine core | generated overlay | uncompiled XPU PP fallback |
| EW-01 | vLLM 0.15.0+xpu | `vllm/v1/executor/ray_executor.py` | `7576bec19a3e` | engine core | generated overlay | worker pre-interpreter environment |
| EW-02 | vLLM 0.15.0+xpu | `vllm/v1/executor/multiproc_executor.py` | `ea3d0115c1af` | engine core | generated overlay | multiprocessing worker identity |
| EW-03 | vLLM 0.15.0+xpu | `vllm/v1/executor/ray_utils.py` | `04c2b93ef48b` | engine core | generated overlay | Ray worker logical identity |
| SC-11 | Ray 2.53.0 | `ray/_private/accelerators/intel_gpu.py` | `dfcba05544e8` | replica, engine worker | generated overlay | XPU visibility mapping |
| SC-12 | Ray 2.53.0 | `ray/experimental/channel/accelerator_context.py` | `c92c9c9b3b63` | replica, engine worker | generated overlay | XPU accelerator context |
| RS-01 | Ray 2.53.0 | `ray/serve/_private/constants.py` | `84c39b95bd32` | deployment | generated overlay | Serve startup proxy timeout |
| RS-02 | Ray 2.53.0 | `ray/_private/services.py` | `ae09f861986f` | Ray head/worker | generated overlay | Raylet startup fanout |
| EN-01 | ExaServe 0.4.0 | `exaserve/compat/engine_shim.py` | profile-pinned | engine core/worker | generated shim | spawned-interpreter reach/self-attestation |

The profile currently contains 15 entries and 12 generated target modules.
SC-09, SC-10, and EW-01 share one complete generated `ray_executor.py`;
all other duplicate base files are similarly grouped into one output whose
manifest lists the exact patch-ID set.

## Public configuration retained outside the patch set

The following are plan/site configuration rather than compatibility patches:

- Ray/Serve proxy health and ready timeouts exposed by supported environment
  controls;
- per-deployment replica health period/timeout through public Serve options;
- Ray worker environment propagation through supported `runtime_env.env_vars`;
- XPU/ZE/CCL and bounded thread/fanout values projected from the hash-bearing
  `SiteProfile`/`DeploymentPlan`;
- instrumentation emitted by ExaServe-owned telemetry collectors rather than
  modified Ray Serve source.

## Removed entries and mechanisms

- SC-06: unnecessary once SC-05 is activated by the generated target module.
- SC-07/SC-08: obsolete vendored executor paths.
- SC-D1/D2/D3: dead or superseded.
- All OV-* Ray Serve full-file replacement/instrumentation files: deleted.
- `setup_overlay.sh`, `/tmp/exaserve_overlay`, implicit symlink farms, and
  installed-file replacement: deleted.
- `builtins.__import__` and import-time application of every patch: deleted.
- stdout/log markers as compatibility evidence: forbidden.

## Evidence

- Ladder and process feasibility:
  `artifacts/hardening/architecture-feasibility-20260809-r2/result.json`
  (`6916762f…`).
- Selected-mechanism feasibility proof:
  `artifacts/hardening/compatibility-overlay-20260809-r5/result.json`
  (`64c1fdec…`), PASS for deployment, Ray head, replica, EngineCore, and
  engine worker, including a generic-worker-to-replica role transition.
- Final43 package gate:
  `artifacts/hardening/final43-packaged-gate-20260809-a1/`, **1215 passed, 9
  skipped**, four-module typed contract core mypy clean with imports skipped,
  exact wheel
  `1041be53eb5b5875d198d5ee6c6664718b4085775dcba99107873dd3d1fcdff2`.
- Final43 real pinned-stack hardware receipts:
  `artifacts/hardening/final43-real-1n-20260809-a1/qualification/result.json`
  and
  `artifacts/hardening/final43-real-2n-20260809-a1/qualification/result.json`.
  The two-node PP=2 cell records one EngineCore and two engine workers across
  the two planned physical hosts.

The final profile hash is
`c17e684fe485261a9cfa82248bd24a9209b66a7c66bae8b889b24ca878d335d3`;
the final compatibility manifest hash is
`cd85123822f4b936216282ed43346223a4b68f1a7cb152a85715a36fdab24259`.

Any profile/source change supersedes those semantic identities and requires a
fresh final-candidate proof; see ADR-003.
