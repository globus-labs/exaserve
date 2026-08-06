# S03 Compatibility-Patch Inventory (manifest seed)

**Recorded:** 2026-08-05 (P00, spike S03 inventory phase; AC-COMP-01 input).
Diffed against the *actually installed* upstream — not inferred from markers.

## 0. Measured runtime versions

| package | measured | pinned/claimed in repo |
|---|---|---|
| ray | **2.53.0** | `patches/__init__.py:52` pins **2.49.1** (+commit) — STALE |
| vllm | **0.15.0** | "as bundled in frameworks 2025.2.0" — STALE (2025.3.1) |
| transformers | 4.57.6 | — |
| torch | 2.10.0a0+git449b176 | — |
| sglang | NOT installed in frameworks python (venv-only path) | — |

Live hazards from the stale pin: `_check_ray_version()` warns on every run
(suppressed unless `EXASERVE_PATCH_VERBOSE=1`) and `apply_all(strict=True)`
would hard-fail today; `setup_overlay.sh` resolves system Ray with **no
version assertion** (a frameworks bump silently yields a mixed-version
`ray.serve._private`); `pyproject` has floor-only `ray[serve]>=2.49`.
Also stale: `patches/__init__.py` says "16 patch functions" (actual 15: 12
live + 3 dead), "five patched files" (actual 8 shipped: 6 differ + 2
byte-identical), and references nonexistent `plan/production_packaging.md`.

## 1. `_sitecustomize.py` (SC-*) — runtime monkey-patches

Delivery: sitecustomize import hook via `exaserve.patches.apply_all()`;
reaches driver + replicas, and spawned EngineCore via the generated shim
EN-01. Master gate: `EXASERVE_VLLM_PATCH_PP_LAYER_FILTER=1` (SH-08). Lines
12–242 are a shared PP-layer-aliasing helper library (one non-optional
dependency block for SC-01..SC-05).

| id | loc | target | class | ladder |
|---|---|---|---|---|
| SC-00 | :7-9 | `_patch_log` (verbose gate) | instr | 1 |
| SC-01 | :245-294 | `vllm.config.vllm.get_layers_from_vllm_config` | upstream-fix (PP stage-local layer aliasing) | 4 |
| SC-02 | :297-358 | `vllm.v1.worker.utils.bind_kv_cache` (+ is_xpu widen) | upstream-fix | 4 |
| SC-03 | :361-401 | `vllm.forward_context.create_forward_context` | upstream-fix | 4 |
| SC-04 | :404-495 | `vllm.attention.layer.get_attention_context` + `unified_kv_cache_update` | upstream-fix | 4 |
| SC-05 | :498-616 | `GPUModelRunner.initialize_attn_backend` (full reimpl, 3-tier fallback) | upstream-fix | 4 |
| SC-06 | :619-655 | **`builtins.__import__` global hook** (re-applies SC-05) | vendor-compat; highest blast radius — replace with MetaPathFinder | 4 |
| SC-07 | :658-852 | `vllm...ray_utils.initialize_ray_cluster`/`_verify_bundles` (~195 vendored lines) | **OBSOLETE** (superseded by per-GPU bundles, commit 17b88be) | 1=delete after PP re-test |
| SC-08 | :855-1105 | `RayDistributedExecutor._init_workers_ray` (~250 vendored lines) | **OBSOLETE** (self-documented) | 1=delete after PP re-test |
| SC-09 | :1108-1162 | `_init_executor` XPU compiled-DAG channel-type force (`shm` fallback) | vendor-compat | 2/4 |
| SC-10 | :1165-1224 | `_execute_dag` → explicit per-stage chain when `EXASERVE_XPU_VLLM_DISABLE_RAY_COMPILED_DAG=1` | vendor-compat | 4 |
| SC-11 | :1227-1286 | `IntelGPUAcceleratorManager.get_current_process_visible_accelerator_ids` | vendor-compat (generic ONEAPI selector → ordinals); upstream PR candidate | 4 |
| SC-12 | :1289-1411 | `AcceleratorContext.get_accelerator_devices` (3-tier fallback; pairs SC-11) | vendor-compat; upstream PR candidate | 4 |
| SC-D1 | :1414-1475 | `proxy_state.wrap_as_future` | **DEAD** (call site commented) | delete |
| SC-D2 | :1478-1511 | Serve constants (values DISAGREE with live 3600/300/100) | **DEAD**, superseded 3× (OV-const, SV-02/03) | delete |
| SC-D3 | :1514-1629 | ProxyActor init/ready profiler via 2nd `__import__` hook | **DEAD**, superseded by OV-proxy-01/02 | delete |

## 2. Ray Serve overlay (OV-*) — diffed vs upstream 2.53.0

Delivery: `/tmp/exaserve_overlay` PYTHONPATH symlink farm; gate
`EXASERVE_INSTRUMENTATION=1` (**default 0 — entire overlay off by default**).
Diff lines: client 0, common 77, constants 54, controller 76,
deployment_state 114, proxy 155, proxy_state 0, router 38.

| id | target | class | ladder |
|---|---|---|---|
| OV-const-01 | `HTTP_PROXY_TIMEOUT` 60→3600 | config | 2 (env exists via SV-01) |
| OV-const-02 | `DEFAULT_HEALTH_CHECK_PERIOD_S` 10→120 | config | 1 (public per-deployment param) |
| OV-const-03 | `DEFAULT_HEALTH_CHECK_TIMEOUT_S` 30→600 | config | 1 |
| OV-const-04 | `PROXY_HEALTH_CHECK_TIMEOUT_S` def 10→300 | config | 1 (upstream env-readable) |
| OV-const-05 | `PROXY_READY_CHECK_TIMEOUT_S` def 5→60 | config | 1 |
| OV-const-06 | `PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD` 3→100 | config | 2 (no upstream env) |
| OV-const-07 | `REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD` 3→100 | config | 2 |
| OV-common-01/02 | buffered CSV probe writer + `get_actor_handle` timing | instr | 3/4 |
| OV-controller-01/02 | control-loop sub-phase timers (dupes upstream gauges) + tick JSONL | instr | 1/3 |
| OV-ds-01..03 | DSM update()-step timers (+O(deployments) count per tick) + JSONL | instr | 3/4 |
| OV-proxy-01..03 | ProxyActor init/ready timers + `/tmp/exaserve_inst/proxy_init_*.json` | instr (supersedes SC-D3) | 3 |
| OV-router-01 | `update_deployment_targets` timing JSONL | instr | 3 |
| OV-DEAD-1 | `client.py` 626 lines **byte-identical to upstream** | dead weight; pins file to 2.53.0 | delete |
| OV-DEAD-2 | `proxy_state.py` 823 lines **byte-identical** | dead weight | delete |

**Overlay roll-up: 20 modifications / 6 files; 7 config + 11 instrumentation +
0 functional fixes.** Nothing in the overlay is correctness-load-bearing.

## 3. `ray_start.py` (RS-*)

RS-01 private imports (`ray._private.{ray_constants,services}`); RS-02
monkey-patch of `services.start_ray_process` (raylet-only argv injection);
RS-03 `--maximum_startup_concurrency`/`--num_prestart_python_workers` caps;
RS-04 in-process `ray.scripts.scripts.cli.main` (keeps RS-02 live); RS-05
driver passes caps + hardcoded `--num-gpus=12` (dupes `XPUVendor
.default_gpus_per_node()`; PR-002). Ladder: 2 if Ray grows CLI flags, else 4.

## 4. `server.py`/`driver.py` direct code (SV-*)

- SV-01 `_ray_serve_timeout_patches()` — canonical 7-constant dict
  (**single source of truth**; matches OV-const exactly).
- SV-02/SV-04 `worker_process_setup_hook=_patch_ray_serve_proxy_constants`
  (public API) + `sys.modules` re-bind sweep → all workers. Ladder 1/2.
- SV-03 same constants in driver pre-`serve.start()`.
- SV-05 `build_actor_runtime_env()` forwards 14 env vars incl. `PYTHONPATH`
  (**load-bearing for OV-*/EN-01 reach into replicas**).
- SV-06 `apply_all()` import-order constraint before `ray.serve`/vllm.
- SV-07 `_verify_core_env()` real-remote-worker probe of 4 GCS vars
  (**generalize as the AC-COMP-01 assertion pattern**).
- SV-08 `_run_many` private batch deploy (public serve.run serializes: ~3h@32,
  ~12h@128 replicas). Ladder 1 = upstream public batch API needed.
- SV-09 `os._exit(1)` fail-fast after deploy failure.
- SV-10..SV-14, SV-16 deploy-phase instrumentation via private
  APIs/protobufs (`serve_start`, `deploy_utils.get_deploy_args`,
  `serve_pb2.*`, 5 private client methods, `SERVE_CONTROLLER_ACTOR` name,
  `client._controller.get_proxies`) — **deepest private coupling, purely for
  timing; highest-value removal** (ladder 1).
- SV-15 per-deployment health params (30/120) **contradict OV-const-02/03
  (120/600)**; public API wins for replicas.
- SV-17 `get_ray_env()` hardcodes XPU env unconditionally (PR-002; vendor
  gating exists only in shell SH-03).
- SV-18 `RAY_SERVE_QUEUE_LENGTH_RESPONSE_DEADLINE_S=300` (upstream 0.1);
  underlying NoneType crash is an upstream bug to file.
- SV-19 `prepend_pythonpath(SRC_DIR)`.

## 5. Shell activation (SH-*) — 28 records

Highlights (full detail in agent transcript, artifacts/hardening/s03-inventory/):
- SH-03 XPU env block **vendor-gated** (`EXASERVE_VENDOR=xpu`): ZE_*,
  CCL hydra, NOSET_ONEAPI + unset selector (pairs SC-11/12).
- SH-04 `ulimit -s 8192`; SH-05 Ray gRPC/callback thread clamps; SH-06 GCS
  hardening (`RAY_gcs_server_num_threads=8` etc.; verified by SV-07);
  SH-13 `RAYON_NUM_THREADS=1`+`TOKENIZERS_PARALLELISM=false` (KI-A5);
  SH-17 `RAY_SERVE_MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT=200`.
- SH-08 `EXASERVE_VLLM_PATCH_PP_LAYER_FILTER=1` default — master SC gate.
- SH-11 `EXASERVE_INSTRUMENTATION=0` default — master OV gate.
- SH-18/19 PYTHONPATH: `/tmp/exaserve_src` always; `/tmp/exaserve_overlay`
  iff instrumentation on.
- SH-21..SH-23 symlink-farm build: patched-file set is **implicit** (whatever
  is in the bcast dir), sanity check covers only 2 of 8 files, no version
  assertion (AC-COMP-01 gap).
- SH-24..SH-26 MPI bcast of src (excl. overlay), overlay patches, optional
  venv + injected triton copy.
- SH-27 head-IP helper runs with patches disabled; SH-28 `cleanup_run.sh`
  wipes `/tmp/exaserve_pp_shim` (interacts with EN-01's exists-cache).

## 6. Engine shims (EN-*)

- **EN-01** generated `sitecustomize.py` at `/tmp/exaserve_pp_shim`
  (`import exaserve._sitecustomize` wrapped in try/except) + PYTHONPATH
  prepend; **only route by which SC-* reaches spawned EngineCore**; fires
  when pp>1. The archetype for WP3's generated-shim step (ladder 3).
- EN-02 `hasattr(engine_args, "enable_log_requests")` fallback (template-
  quality shim; note it *forces* True — PR-022). EN-03 capability preflight
  (good pattern). EN-04 port bases (23000+dev*100). EN-05 in-process XPU gate
  setting (`FORCE_RAY_CHANNEL_TYPE=auto` here vs SC-09 fallback `shm` — two
  values, two paths). EN-06 dup of SH-13. **EN-07 SGLang `__main__`
  neutralization** (spawn re-import SIGSEGV; fragile, ladder 4). EN-08 nccl
  port base 25100. EN-09 `torch_native` attention (correctness on PVC).
  EN-10 hardcoded sglang args. EN-11 engine-divergent ONEAPI selector
  (SGLang needs it set, vLLM needs it popped). EN-12 `12` GPUs/node const
  (duplicated literal in driver — PR-002).

## 7. runtime_env / setup-hook sweep (exhaustive)

Only three sites: `server.py:622-624` (SV-04, the sole
`worker_process_setup_hook`), `server.py:1419` (SV-05 per-deployment
runtime_env), `_sitecustomize.py:769` (pass-through inside vendored SC-07).

## 8. Roll-up and WP3 consequences

By classification: config 33 · vendor-compat 22 · functional-upstream-fix 8
(SC-01..05, RS-02/03, SV-08, SV-09) · instrumentation 18 · obsolete 5.

1. **Delete-first opportunity (~1,900 lines, no-risk except PP re-test):**
   OV-DEAD-1/2 (1,449 identical lines), SC-07/SC-08 (~445 vendored lines,
   self-documented obsolete), SC-D1/D2/D3 (dead call sites).
2. **The overlay can stop being an architecture:** zero functional content;
   constants already flow through public `worker_process_setup_hook`
   (SV-02/04). Instrumentation → ladder-3 generated overlay or a public
   tracing sink; then SH-19/21-25 farm machinery retires.
3. **Version-drift guard is mandatory and cheap:** correct pin to 2.53.0,
   version assertion in overlay assembly, explicit declared file set,
   role receipts (profile hash over manifest per plan WP3.10).
4. Real vendor-compat core that must survive in the profile: SC-01..06
   (PP aliasing), SC-09..12 (XPU DAG/selector), EN-01/05/07/09/11, SH-03/04/
   05/06/13, RS-02/03.
