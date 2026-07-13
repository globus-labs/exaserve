# Design: Pluggable Interfaces (Engine, Proxy) — and the seams for Scheduler & Vendor

Status: **partially implemented** · Branch: `refactor/pluggable-interfaces` ·
Depends on: the `aurora_rayserver → exaserve` rename (commit `6a2faa9`).

## Implementation status (what actually shipped vs. proposed)

| Piece | State |
|---|---|
| `src/exaserve/engines/` — `EngineBackend` ABC, neutral dataclasses, `NullEngine`, `get_engine()` registry | **Shipped** (`1e58aa0`), unit-exercised on login node (registry + NullEngine stream/non-stream). **Not yet wired into `server.py`.** |
| Proxy `BackendEndpoint` docstring de-Aurora-ification | **Shipped** (`1e58aa0`). |
| Proxy entry-point plugin registration | Proposed only. |
| `engines/vllm.py`, `engines/sglang.py`, the shared `EngineWorker` host, deletion of `VLLMWorker`/`SGLangWorker` | **Not started** — the load-bearing `server.py` extraction. Deferred pending real-compute smoke validation. |

The sections below are the full design; treat anything not in the table above as
**proposed, not built**.

## Goal

Turn ExaServe from an Aurora/vLLM/PBS-specific stack into a generic HPC serving
platform: **Ray + engine-of-choice + proxy-of-choice**, with clean seams so
later work can add other schedulers (PBS → Slurm) and other vendors (Intel XPU →
NVIDIA/AMD) without touching the serving core.

This document covers step **(2) — formalize the engine and proxy interfaces** —
and sketches how (2) sets up steps **(3) scheduler** and **(4) vendor/site**.

## Current state (as of `6a2faa9`)

| Axis | Shape today | Verdict |
|---|---|---|
| **Proxy** | `proxy/base.py::ProxyBackend` ABC (generate_config/start/health_check/stop) + `proxy/__init__.py::get_proxy()` registry; `driver.py` is the only caller and touches only the interface. Backends: haproxy, litellm, envoy, nginx, pingora, ray_serve. | **Already formalized.** Needs only light polish. |
| **Engine** | `server.py:1962` reads `EXASERVE_ENGINE`; `server.py:1963` `worker_cls = SGLangWorker if engine=="sglang" else VLLMWorker`. `VLLMWorker` (~500 LOC, `server.py:872`) and `SGLangWorker` (~350 LOC, `server.py:1372`) are parallel Ray Serve deployment classes. | **Not formalized.** Binary branch; duplicated HTTP/placement/stats scaffolding. |

### What the two worker classes duplicate (the shared "host")

Both expose the same OpenAI HTTP surface and lifecycle scaffolding:

- Ingress routes: `/health`, `/stats`, `/v1/models`, `/v1/chat/completions`,
  `/v1/completions`
- Per-tile placement + device assignment (`ZE_AFFINITY_MASK`, Ray GPU ids)
- Serving-stats collection (`CollectingStatLogger` / `_ServingStatsCollectorImpl`)
- Null-compute mode (`EXASERVE_NULL_COMPUTE` → `sleep(latency)`)
- Chat-message → plain-prompt conversion, sampling-param parsing

### What actually differs (the engine-specific core)

- **Engine creation**: `AsyncLLMEngine.from_engine_args(...)` vs `sgl.Engine(...)`
- **Inference**: `_generate` / `_generate_stream` (vLLM `AsyncLLMEngine.generate`
  vs SGLang `engine.async_generate`)
- **Sampling-param mapping**, finish-reason extraction, request-abort

## Proposed design for (2)

Mirror the proxy pattern: **one shared host, many pluggable backends.**

### 2a. `EngineBackend` interface (`src/exaserve/engines/base.py`)

```python
class EngineBackend(ABC):
    name: str                       # "vllm", "sglang", ...

    @abstractmethod
    def create(self, spec: EngineSpec) -> None: ...
        # instantiate the underlying engine on the assigned tile(s).
        # spec carries: model_id, local_path, tp, pp, max_model_len,
        # gpu_memory_utilization, enforce_eager, max_num_seqs, device_ids,
        # distributed master addr/port, extra engine kwargs.

    @abstractmethod
    async def generate(self, prompt: str, params: SamplingParams) -> GenResult: ...

    @abstractmethod
    async def generate_stream(self, prompt, params) -> AsyncIterator[GenDelta]: ...

    def capabilities(self) -> EngineCaps: ...   # streaming?, logprobs?, stats?
    def collect_stats(self) -> dict: ...        # default: {}
    async def shutdown(self) -> None: ...        # default: no-op
```

Plus `EngineSpec`, `SamplingParams`, `GenResult`, `GenDelta` dataclasses (engine-
neutral), and a shared **`NullEngine`** implementing the null-compute path once
(deletes the duplicated `_null_generate*` methods).

### 2b. Registry (`src/exaserve/engines/__init__.py`)

`get_engine(name) -> EngineBackend`, lazy-import per backend (preserves the
"only import the chosen engine" property that avoids the vLLM/SGLang transformers-
pin clash — see `server.py:46-49`). Same shape as `proxy/get_proxy`.

### 2c. One `EngineWorker` Ray Serve deployment (replaces both worker classes)

A single `@serve.deployment @serve.ingress(app)` class that owns the shared HTTP
surface, tile placement, stats, and null-compute, and delegates inference to
`self.backend = get_engine(EXASERVE_ENGINE)` (or `NullEngine` when null-compute).
`deploy_model` loses the `worker_cls` branch (`server.py:1962-1991`) — it always
binds `EngineWorker`; the engine choice moves inside.

**Non-negotiable invariant:** placement/PP logic (`build_pp_replica_bundles`,
`ordered_pp_nodes`, shard-aware path, `build_actor_runtime_env`) is **unchanged**.
This refactor only relocates the HTTP+inference split behind an interface; it must
not alter how replicas are placed or how the overlay/PYTHONPATH is wired.

## (1.5) Proxy polish (small)

- De-Aurora-ify docstrings/examples in `proxy/base.py` (HSN hostname example).
- Optional: allow out-of-tree backends via `importlib.metadata` entry points, so
  a site can register a proxy without editing `_register()`.

## How this sets up (3) and (4)

- **(4) Vendor/HW** plugs into `EngineBackend.create` through a tiny
  `DevicePlacement` helper: today device isolation is `ZE_AFFINITY_MASK` (Intel);
  NVIDIA/AMD need `CUDA_VISIBLE_DEVICES`/`ROCR_VISIBLE_DEVICES`. Hoist the
  `EXASERVE_XPU_*` knobs and level-zero specifics behind a `vendor/` module
  selected by e.g. `EXASERVE_VENDOR` (default `xpu`). Because inference already
  goes through `EngineBackend`, the vendor swap is contained.
- **(3) Scheduler** is orthogonal to serving: it lives in `submit.py`,
  `launch_cluster.sh`, and `eval/lib/schedulers/`. Formalizing the engine first
  keeps the serving core stable while the scheduler seam is cut. A
  `SchedulerBackend` (render_job / submit / poll / cancel) will mirror this same
  registry pattern; `eval/lib/schedulers/pbs.py` already exists as the first impl.

## Migration & validation plan

Incremental, each step green before the next:

1. Land `engines/` package (ABC + registry + NullEngine) — **additive, no wiring**.
2. Port `VLLMWorker`'s engine bits into `engines/vllm.py::VLLMEngine`; build
   `EngineWorker`; switch `deploy_model` to bind it for `EXASERVE_ENGINE=vllm`.
3. Port SGLang into `engines/sglang.py::SGLangEngine`.
4. Delete the old worker classes.

**Regression gate = the exact smokes already validated on this rename:**
`nullcompute_smoke_1node`, `refcard_smoke_1node`, the 3 `smoke_slo_stream_*`,
`serverstats_ttft_*`, `serverstats_ttft_ondelay_2node` (24-replica proxy/internode),
and the 405B PP runs (`pp405b_verify_2node`, `pp405b_pp2_proxyfix`). Numbers must
match the pre-refactor baselines (e.g. 1-node refcard ≈ 28.6 rps / 0 err;
2-node serverstats ≈ 39 rps / 0 err).

## Risks

- `server.py` is load-bearing and subtle (Ray Serve decorators, PP placement,
  overlay import-timing, stats push threads). The engine extraction must be
  behavior-preserving; validate on real compute, not just unit tests.
- Streaming cadence is easy to regress (TBT distributions). The streaming smokes
  are the guard.
- Keep lazy per-engine imports — eager import reintroduces the transformers-pin clash.
