# Design Sketch: Vendor & Site Abstraction (XPU → NVIDIA/AMD, Aurora → pluggable site)

Status: **sketch (design only — no code shipped)** · Branch:
`refactor/pluggable-interfaces` · Step (4) of the generic-HPC refactor. Builds on
(2) engine and (3) scheduler.

> **Implementation status:** nothing here is built yet. Device isolation
> (`ZE_AFFINITY_MASK`/`ONEAPI_DEVICE_SELECTOR`) still lives inline in the
> `server.py` worker classes, and site facts are still spread across
> `eval/site_config.py` + `SchedulerSpec` defaults. `VendorBackend`, `SiteConfig`,
> and the CUDA/ROCm backends are all **proposed**. Best sequenced *after* the (2)
> engine extraction, since the device code lives in the worker `__init__` that (2)
> refactors.

## Goal & the two distinct axes

The original stack is Intel-XPU + Aurora-specific. Two *separate* axes hide here
and must not be conflated:

- **Vendor** = the accelerator/runtime: device isolation, target-device flags,
  vendor env quirks. Intel **XPU** today → **NVIDIA (CUDA)** / **AMD (ROCm)**.
- **Site** = facility facts: scheduler account, filesystems, environment script,
  module loads, model-storage path, GPUs-per-node, fabric hostname scheme. Aurora
  today → any HPC facility.

A site *has* a vendor (Aurora → XPU) but they vary independently: a CUDA site and
an XPU site can share scheduler/engine code; only the vendor + site layers differ.

## Where vendor (XPU) is coupled today

All inside the worker classes' `__init__` (server.py) — will move behind the
(2) `EngineBackend`/vendor layer:

- **Device isolation**: `os.environ["ZE_AFFINITY_MASK"] = affinity_mask` +
  `ONEAPI_DEVICE_SELECTOR` handling (unset for vLLM `server.py:928-929`; set to
  `"opencl:gpu;level_zero:gpu"` for SGLang `server.py:1403`). Intel level-zero.
- **Target/runtime flags** propagated to actors (`server.py:537-543`):
  `VLLM_TARGET_DEVICE`, `ZE_FLAT_DEVICE_HIERARCHY`,
  `RAY_EXPERIMENTAL_NOSET_ONEAPI_DEVICE_SELECTOR`, and the `EXASERVE_XPU_*`
  compiled-DAG/channel workarounds.
- **Hardware fact**: `config.num_gpus_per_node` (12 tiles on Aurora PVC;
  `server.py:399`).
- **PVC/Triton-SYCL quirks** documented across `server.py:10-12`, `driver.py`.

## Where site (Aurora) is coupled today

- `eval/site_config.py` + `EXASERVE_SITE_CONFIG_LOCAL`: project_root, model
  storage, account defaults, sglang python path.
- Scheduler defaults baked into `SchedulerSpec` (`project=AuroraGPT`,
  `filesystems=home:flare`) — see (3), these are *site* facts.
- Runtime: `source ~/script/env_aurora`, `module load frameworks`, `/opt/aurora`
  paths, HSN hostname resolution (`get_hsn_ip`, `server.py:253`).
- `num_gpus_per_node` (site *hardware* fact, feeds the vendor tile math).

## Proposed design

### 4a. `VendorBackend` interface (plugs into the (2) engine layer)

New `src/exaserve/vendors/base.py`:

```python
class VendorBackend(ABC):
    name: str                                  # "xpu", "cuda", "rocm"

    def isolate_devices(self, device_ids: list[int]) -> None: ...
        # set the vendor's device-visibility env for this replica process:
        #   xpu:  ZE_AFFINITY_MASK=...; manage ONEAPI_DEVICE_SELECTOR
        #   cuda: CUDA_VISIBLE_DEVICES=...
        #   rocm: ROCR_VISIBLE_DEVICES=... / HIP_VISIBLE_DEVICES=...

    def engine_env(self) -> dict[str, str]: ...
        # vendor flags to export into engine actors (VLLM_TARGET_DEVICE=xpu/cuda/rocm,
        # ZE_* only for xpu, the EXASERVE_XPU_* workarounds only for xpu, ...)

    def default_gpus_per_node(self) -> int: ...   # 12 (XPU tiles) / 8 (typical CUDA)
    def preflight(self) -> None: ...              # optional vendor sanity checks
```

`get_vendor(name)` registry (default `xpu`), same lazy-registry shape as
proxy/engine/scheduler; selected by **`EXASERVE_VENDOR`** or the site config.
`EngineWorker.__init__` (from (2)) calls `vendor.isolate_devices(ids)` before
`engine.create(spec)` — so both the `ZE_AFFINITY_MASK` block and the SGLang
`ONEAPI_DEVICE_SELECTOR` special-case move into `XPUVendor`, out of the engines.

### 4b. `SiteConfig` — one home for facility facts

Extend `eval/site_config.py` (already the `EXASERVE_SITE_CONFIG_LOCAL` hook) into
a first-class `SiteConfig` consumed by scheduler + launcher + vendor selection:

```
SiteConfig:
    name: str                      # "aurora"
    vendor: str                    # "xpu"  -> selects VendorBackend
    scheduler: str                 # "pbs"  -> selects SchedulerBackend (3)
    account / partitions / filesystems
    env_script: str                # "~/script/env_aurora"
    module_loads: list[str]        # ["frameworks", "go"]
    model_storage_path / project_root
    gpus_per_node: int             # 12
    fabric_suffix: str             # ".hsn.cm.aurora.alcf.anl.gov"  (HSN naming)
```

The scheduler defaults (`project`, `filesystems`) move here out of `SchedulerSpec`
(closing the (3) open question). `launch_cluster.sh` sources `env_script` and does
`module load` from the site config instead of hardcoding Aurora's.

### Vendor mapping (reference)

| Concern | Intel XPU (today) | NVIDIA CUDA | AMD ROCm |
|---|---|---|---|
| Device isolation | `ZE_AFFINITY_MASK` (+ONEAPI_DEVICE_SELECTOR mgmt) | `CUDA_VISIBLE_DEVICES` | `ROCR_VISIBLE_DEVICES` |
| vLLM target | `VLLM_TARGET_DEVICE=xpu` (+ZE_FLAT_DEVICE_HIERARCHY) | `cuda` (default) | `rocm` |
| Extra quirks | `EXASERVE_XPU_*` compiled-DAG/channel workarounds, SGLang OneAPI selector | none known | none known |
| GPUs/node (site) | 12 tiles (6 PVC ×2) | site-dependent (e.g. 8) | site-dependent |

## Composition with (2) and (3)

```
SiteConfig ─┬─ vendor  → get_vendor()  → device isolation + engine env (into (2))
            ├─ scheduler → get_scheduler() (3) + supplies account/fs/partitions
            └─ env_script/module_loads/storage → launcher + staging
Engine (2) inference is already vendor-neutral; only VendorBackend.isolate_devices
+ engine_env change per accelerator, so the CUDA/ROCm swap is contained.
```

## Migration & validation

1. Add `vendors/` (ABC + registry + `XPUVendor` wrapping today's exact
   ZE_AFFINITY_MASK / ONEAPI logic). **Gate: XPU smokes unchanged.** Best done
   *with* or *after* the (2) engine extraction, since the device code lives in the
   worker `__init__` that (2) is refactoring anyway.
2. Promote `SiteConfig`; move scheduler/site defaults into it; wire launcher.
   **Gate: Aurora smokes unchanged.**
3. Add `CUDAVendor` / `ROCmVendor` + a second `SiteConfig`. **Validation is
   offsite** — Aurora is XPU-only, so NVIDIA/AMD can only be unit-tested here
   (env-dict shape); real validation is owed on CUDA/ROCm hardware and must be
   flagged unproven until then.

## Open questions
- `num_gpus_per_node`: site fact vs vendor default? (Lean: vendor supplies a
  default, site overrides — Aurora's 12 is both.)
- Does `EXASERVE_XPU_*` stay XPU-namespaced (already is, from the rename) and get
  emitted only by `XPUVendor.engine_env()`? (Lean: yes — clean.)
- The Ray-timeout `sitecustomize.py` is vendor/site-agnostic (pure Ray Serve
  constant patch) → stays in the launcher, not the vendor layer.
