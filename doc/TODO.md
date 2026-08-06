# ExaServe Backlog and Research TODOs

> **Document role:** Non-normative backlog. The canonical implementation order
> and production definition of done are in
> `doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md`. WP0 must classify every
> entry in `doc/hardening/FINDINGS.yaml` as production work, accepted/unsupported
> scope, external blocker, or genuinely out-of-production-scope work with an
> owner and revisit condition. This file cannot waive a production gate.

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

- **Multi-replica pipeline parallelism is topology- and private-API-sensitive**
  - PP defaults to one replica, but an explicit `num_replicas` is retained. The
    ordinary planner uses location-agnostic bundles for multi-replica PP and
    warns that a TP stage can straddle nodes. With
    `EXASERVE_PP_SHARD_AWARE=1`, ExaServe instead creates separate node-pinned
    applications and submits them through private Ray Serve `_run_many`.
  - Promote the topology choice into typed configuration, remove or tightly
    version-gate the private API, validate placement/staging receipts, and retain
    one-replica, explicit multi-replica, and shard-aware PP smoke/scale tests.
    Regions: `src/exaserve/server.py:310-317,491-517,1287-1366,1455-1631`.

- **Ray GCS scalability ceiling**
  - GCS is single-threaded, caps ~1000 nodes. ServeController also single-threaded. For exascale, consider alternative orchestration (Kubernetes + custom scheduler) or sharded Ray topology.

- **Tests and quality gates are not release-ready**
  - At commit `005891e`, 36 tests collect, but the result is environment-sensitive:
    25 passed / 11 failed with a real `rg` executable and 24 passed / 12 failed
    without one. The delta is an undeclared ripgrep dependency.
  - Replace that subprocess with an in-process Python scan; make each test subtree
    runnable without unrelated `conftest.py` side effects; eliminate live Hugging
    Face access from unit tests; and fix failures by pytest node ID/root cause.
  - Pin test dependencies and plugins, define plugin-autoload and random-seed
    policy, and add CI, typing, linting, packaging, security, and failure-path
    coverage.

### Moderate

- **Null-compute token counting is inaccurate** (`src/exaserve/engines/base.py:161-183`)
  - Uses `len(prompt.split())` instead of actual tokenization. Off by 1.5-2x, skews null-compute benchmark results. Use `tiktoken` or the model's tokenizer.

- **Port allocation is fragile** (`src/exaserve/server.py:278-291`, engine/proxy bind paths)
  - Best-effort scan over a port range with no registry. Can collide under multi-tenant nodes. Use OS-assigned ports (port 0) or a proper lease/registry.

- **Sequential model staging** (model_bcast.py)
  - Models are downloaded and broadcast one at a time. Parallelize across models for multi-model deployments to reduce startup time.

- **SGLang chat-template fallback is overly broad and silent**
  - `src/exaserve/engines/sglang.py:143-155` catches every exception and silently
    falls back to a plain prompt. Catch only the expected missing-template case,
    emit a structured warning, and propagate unrelated tokenizer/configuration
    errors. The vLLM path already narrows and logs this fallback.

- **No end-to-end tracing and correlation contract**
  - Ray supplies request-context support and EngineWorker creates OpenAI
    completion IDs, but ExaServe does not accept/preserve one transport
    correlation ID and link it to the distinct completion ID across proxy → Ray
    Serve → engine → logs. Define that contract and add OpenTelemetry-compatible
    propagation.

### Architectural Gaps

- **No observability exports**
  - No Prometheus/OpenMetrics endpoint. Add `/metrics` on serve and proxy layers for dashboarding and alerting.

- **No request caching or deduplication**
  - Identical requests all go through full inference. A cache at the proxy layer could save significant compute.

- **Hardcoded Aurora-specific constants scattered throughout**
  - `num_gpus_per_node=12`, master port base `23000`, Lustre paths, retry counts. Centralize into a config/constants module for portability to other HPC systems.

- **Compatibility patching is broad and version-sensitive**
  - A 1,629-line import-time monkeypatch, a 10,945-line Ray Serve private
    overlay, worker setup hooks, and a spawned-process shim depend on private
    Ray/vLLM behavior. Pin and verify compatibility profiles, minimize patches,
    and prefer upstream fixes or immutable patched environments.

### Priority Table

| Priority | Improvement | Effort | Impact |
|----------|------------|--------|--------|
| P0 | Hermetic failure-path tests for schemas, staging, proxy, and state | Medium | Catches regressions |
| P0 | Fix null-compute tokenization | Low | Accurate benchmarks |
| P1 | Harden multi-replica PP topology and remove private `_run_many` dependency | High | Makes existing scaling support maintainable |
| P1 | Parallelize multi-model staging | Medium | Faster startup |
| P1 | Add /metrics Prometheus endpoint | Medium | Production observability |
| P2 | Robust port allocation | Low | Eliminates collisions |
| P2 | Request correlation IDs | Medium | Debuggability |
| P2 | Centralize hardcoded constants | Low | Portability |
| P3 | Request caching at proxy layer | Medium | Compute savings |
| P3 | Verified compatibility profiles; minimize/remove runtime patches | High | Prevents silent cross-process drift |

### Wenyi's Note
[] Make a main branch with clean-up code so people can deploy it with one click. - can work on stable branch.
[] Performance instrumentation on ray side.
[x] Broadcast application source, optional Ray overlay, and model data; optional
node-local venv/Triton staging is also implemented. Large-scale revalidation is
still owed.
[x] Legacy `EXASERVE_PROXY_PROFILE` import-hook installation is disabled in
`src/exaserve/_sitecustomize.py`; the optional overlay owns proxy profiling.
Remove the dead hook in the compatibility cleanup and retain one documented,
atomic output owner plus an activation receipt test.
[x] Application-source import stampede addressed in code:
`distribute_to_nodes.sh` performs one MPI broadcast and execution uses
`/tmp/exaserve_src`; optional shared-filesystem venv/Triton content can also be
staged node-local. Re-measure residual third-party imports at 128/256 nodes.

### Paper related TODOs
[] The client could be written with C++, Boost.io, verify if that is a better choice, need clear justification
[] 

### Generic-HPC refactor — deferred (2026-07-13, v0.3.0)
Rename + pluggable-engine refactor landed and smoke-validated; the following are owed follow-ups (design docs under doc/design/):
[] Fix stale HAProxy unit tests — `tests/test_haproxy_proxy.py` asserts the old per-model `/<route>/health` but the code emits `/-/healthz` (commit 3d130c8). 2 failures, PRE-EXISTING (red on main too), not from the refactor.
[x] Implement (3) scheduler abstraction — package PBS/Slurm/PSI-J backends and
runtime `EXASERVE_NODEFILE` / `EXASERVE_MPILAUNCH` / `EXASERVE_JOBID` seam are
present. Eval still has a second scheduler stack to consolidate; Slurm e2e
remains an off-Aurora validation gate.
[x] Implement the vendor portion of (4) — XPU/CUDA/ROCm backends exist and
device isolation moved behind them. A first-class `SiteConfig` and offsite
NVIDIA/AMD e2e validation remain open.
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
