## Optimizations

### [03/04/2026] Client side unnecessary environment setup cleanup
- The client now uses `litellm` venv which needs to specifically set in the environment - currently using python to inject, we can simply move the setup before the command in the bash scripts.
- The prints are all dumped in the same stdout/stderr. Need to categorize them into different stream and save them for further analysis, or we have our own log dedup strategy globally, which only prints necessary error output and collapse similar INFO output with singleline by several "x"
- A static port assignment may be a potential issue if the port is gone. A robust way is we capture the port and capture the code.
- experiments folder should be moved to somewhere else, pbs_output and results should be in the same folder so we don't need to do manul indexing every time we check the results.


## Experiments Ideas
- Raw latency including the LiteLLM proxy hop
- Proxy overhead measured in isolation
- Error rate before vs. after retries (?)


## Limitations & Improvements [03/23/2026]

### Critical

- **Single replica enforced for pipeline parallelism** (schemas.py:200-206)
  - When `pipeline_parallel_size > 1`, `num_replicas` is forced to 1. Blocks throughput scaling for large PP models. Fix requires redesigning placement group scheduling for multiple PP replica groups.

- **Ray GCS scalability ceiling**
  - GCS is single-threaded, caps ~1000 nodes. ServeController also single-threaded. For exascale, consider alternative orchestration (Kubernetes + custom scheduler) or sharded Ray topology.

- **No unit or integration tests**
  - Zero formal test coverage. `schemas`, `model_staging`, `model_paths`, and proxy config generation are all testable in isolation. Add CI pipeline.

### Moderate

- **Null-compute token counting is inaccurate** (exaserve_serve.py:~607)
  - Uses `len(prompt.split())` instead of actual tokenization. Off by 1.5-2x, skews null-compute benchmark results. Use `tiktoken` or the model's tokenizer.

- **Port allocation is fragile** (exaserve_serve.py:64-77)
  - Best-effort scan over a port range with no registry. Can collide under multi-tenant nodes. Use OS-assigned ports (port 0) or a proper lease/registry.

- **Sequential model staging** (model_bcast.py)
  - Models are downloaded and broadcast one at a time. Parallelize across models for multi-model deployments to reduce startup time.

- **Silent chat template fallback** (exaserve_serve.py:~462-474)
  - Falls back to plain-text concatenation silently when tokenizer lacks a chat template. Should log a warning — produces garbage for chat-tuned models.

- **No request tracing or correlation IDs**
  - Request IDs are worker-local. Add OpenTelemetry or `X-Request-ID` header propagation for end-to-end tracing through proxy → Ray Serve → vLLM.

### Architectural Gaps

- **No observability exports**
  - No Prometheus/OpenMetrics endpoint. Add `/metrics` on serve and proxy layers for dashboarding and alerting.

- **No request caching or deduplication**
  - Identical requests all go through full inference. A cache at the proxy layer could save significant compute.

- **Hardcoded Aurora-specific constants scattered throughout**
  - `num_gpus_per_node=12`, master port base `23000`, Lustre paths, retry counts. Centralize into a config/constants module for portability to other HPC systems.

- **sitecustomize.py is a 35KB monkeypatch**
  - Patches vLLM internals at import time. Brittle across vLLM version bumps. Add version compatibility checks, or contribute patches upstream.

### Priority Table

| Priority | Improvement | Effort | Impact |
|----------|------------|--------|--------|
| P0 | Unit tests for schemas, model_staging, model_paths | Low | Catches regressions |
| P0 | Fix null-compute tokenization | Low | Accurate benchmarks |
| P1 | Multi-replica PP (redesign placement groups) | High | Unlocks large model scaling |
| P1 | Parallelize multi-model staging | Medium | Faster startup |
| P1 | Add /metrics Prometheus endpoint | Medium | Production observability |
| P2 | Robust port allocation | Low | Eliminates collisions |
| P2 | Request correlation IDs | Medium | Debuggability |
| P2 | Centralize hardcoded constants | Low | Portability |
| P3 | Request caching at proxy layer | Medium | Compute savings |
| P3 | Version-check sitecustomize patches | Low | Prevents silent breakage |

### Wenyi's Note
[] Make a main branch with clean-up code so people can deploy it with one click. - can work on stable branch.
[] Performance instrumentation on ray side.
[] Now need to broadcast all used files. (Our exaserve_serve code, ray overlay(or conda env), model data).
[] Legacy `EXASERVE_PROXY_PROFILE` monkey-patches at exaserve_serve.py:264-297, :1547-1605, :1929-1930 — superseded by overlay [proxy.py](~/.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray/serve/_private/proxy.py) probe. Still default-on via launch_cluster.sh:372; both write `/tmp/exaserve_inst/proxy_init_*.json` → overlay and legacy race/overwrite. Writes are tmpfs (no Lustre impact) but redundant. Decide: disable via `EXASERVE_PROXY_PROFILE=0` or delete the three blocks.
[] Extend Copper broadcast to cover `$PROJECT_ROOT/src` — right now Copper only broadcasts the overlay (launch_cluster.sh:473-477). Every Ray process still does Lustre imports of our exaserve Python modules, causing MDS stampede at scale. At 256n with ~3k Python processes × ~20 imports = ~60k concurrent Lustre opens during Stage 3.

### Paper related TODOs
[] The client could be written with C++, Boost.io, verify if that is a better choice, need clear justification
[] 

### Generic-HPC refactor — deferred (2026-07-13, v0.3.0)
Rename + pluggable-engine refactor landed and smoke-validated; the following are owed follow-ups (design docs under doc/design/):
[] Fix stale HAProxy unit tests — `tests/test_haproxy_proxy.py` asserts the old per-model `/<route>/health` but the code emits `/-/healthz` (commit 3d130c8). 2 failures, PRE-EXISTING (red on main too), not from the refactor.
[] Implement (3) scheduler abstraction — `SchedulerBackend` ABC + `get_scheduler()` registry (PBS wrapper first, then Slurm) + runtime seam `EXASERVE_NODEFILE` / `EXASERVE_MPILAUNCH` / `EXASERVE_JOBID` so `launch_cluster.sh` stops hard-coding `$PBS_NODEFILE`/`mpiexec`. Spec: doc/design/scheduler_abstraction.md. Slurm e2e only validatable off-Aurora.
[] Implement (4) vendor/site abstraction — `VendorBackend` (XPU/CUDA/ROCm device isolation) + `SiteConfig`; hoist `ZE_AFFINITY_MASK`/`ONEAPI_DEVICE_SELECTOR` out of the engines into the vendor layer. Spec: doc/design/vendor_site_abstraction.md. NVIDIA/AMD e2e only validatable off-Aurora.
[] SGLang engine path is NOT smoke-validated (no sglang smoke spec exists; refactor only exercised vLLM). Add a 1-node sglang smoke or validate `SGLangEngine` end-to-end before trusting the sglang path post-refactor.
[] Document `EXASERVE_ENGINE` + pluggable engine/proxy selection in README (currently only in doc/exaserve.md + the reference-card docx).
[] doc/figures/*.png are gitignored (`*.png`) so the reference-card docx is not regenerable from a clean clone (the docx embeds them, so it renders fine). Either track the two figures + tmp/ref_card/ template, or document the regen prerequisites. Sources are in ~/aurora_rayserver.
[] Update the SC26 paper (sc26workshop/) to the generic scheduler/vendor framing (roadmap step 5) once (3)/(4) land.
[] Validate the refactor + rename at 128/256 nodes — this round covered components+correctness (1/2/4-node), NOT the large-scale overlay/GCS timing.

### Slurm + AMD support — status & follow-ups (2026-07-17, feature/slurm-amd-support)
DONE (deployment/serving path): vendor abstraction (XPU/CUDA/ROCm, src/exaserve/vendors/), package scheduler (PBS+Slurm, src/exaserve/schedulers/), runtime seam (EXASERVE_NODEFILE/MPILAUNCH/JOBID + srun in launch_cluster.sh/distribute_to_nodes.sh/model_bcast.py), driver SLURM_PROCID rank, Delta MI100 example + doc/deploy_slurm_amd.md. Aurora (PBS+XPU) unchanged & validated.
[x] Eval/benchmark harness Slurm support DONE (eval/lib/schedulers/ EvalScheduler PBS+Slurm; run_planner/run_executor/backends.ray wired). Slurm e2e unproven (offsite).
[] First AMD bring-up verifications (doc/deploy_slurm_amd.md "Known-unverified"): ROCm device isolation var (ROCR_ vs HIP_VISIBLE_DEVICES), ROCm/torch/vLLM wheel versions vs /opt/rocm on Delta gpud01, MI210-vs-MI100 device indexing, haproxy availability off-Aurora, multi-node srun path, PP>1 on CUDA/ROCm.
[] TACC has NO Slurm HPC system with AMD Instinct GPUs (only MI100 on Chameleon Cloud, which is OpenStack/Blazar not Slurm). Confirm the intended "TACC AMD machine" with the user — likely Chameleon (needs a non-Slurm provisioning path) or an off-TACC system (Frontier/LUMI MI250X, El Capitan MI300A).
[] SiteConfig object still deferred — site facts flow via env vars (EXASERVE_ENV_SETUP/EXASERVE_VENDOR/num_gpus_per_node). Promote to a first-class SiteConfig per doc/design/vendor_site_abstraction.md §4b if site count grows.

### ExaWorks PSI/J default scheduler — follow-ups (2026-08-03)
DONE: PSI/J (psij-python) is the DEFAULT submit backend (alias exawork), validated end-to-end on Aurora (submit->R->ALL SERVICES READY->qdel, job 8731184); native pbs/slurm kept as alternatives; friction log doc/exawork_psij_notes.md (10 items found+reduced, 2 fatal path bugs measured).
[] head_node() for psij executors WITHOUT a native helper (lsf/flux/cobalt): have launch_cluster.sh write a head-node marker file into the run dir and teach serve-url to read it.
[] Exercise the eval-harness psij path on a real campaign cell (scheduler.type: psij in a smoke spec) — package path is validated; eval path is render/parse-validated only.
[] On Delta bring-up, A/B psij vs native slurm backend and record friction items in doc/exawork_psij_notes.md.
[] psij-python upstream is maintenance-mode (bus factor ~1): revisit yearly; if it goes dormant, the native backends are the escape hatch (or vendor the pbs/slurm mustache templates).
