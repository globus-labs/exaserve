# Ray Launch Stage Walkthrough

Reference for the full startup sequence from PBS job to serving readiness.
Built from code walkthrough and experiment logs (v3 weak-scaling runs).

## Stage 0 — Shell Setup (`launch_cluster.sh`)

**What happens:** Shell script runs on the head node inside the PBS job.

1. Resolves Aurora frameworks Python, sanitizes PYTHONPATH
2. Detects head node HSN IP via `socket.connect(("10.255.255.255", 1))`, writes it into the manifest YAML
3. Stages models to node-local `/tmp/hf_home/` via `mpiexec model_bcast.py` (all nodes)
4. Sets Ray tuning env vars:
   - `RAY_enable_metrics_collection=0` — intended to disable metrics, but the C++ metrics agent still tries to connect and times out (~30s, see Stage 2 — Serve Init)
   - `RAY_num_server_call_thread=4`, gRPC thread clamping — prevents thread exhaustion at scale
   - `RAY_gcs_server_num_threads=8` — helps GCS handle 256+ node registration storms
   - `RAYON_NUM_THREADS=1`, `TOKENIZERS_PARALLELISM=false` — prevents HF tokenizer thread pool panic at 128+ nodes
   - `AURORA_SCALING_TRACE=0` — disables per-replica Lustre trace I/O (saves 10+ min at 128n)
5. Installs `sitecustomize.py` in user site-packages to monkey-patch Ray Serve proxy timeouts in ALL Python processes (including the ServeController actor which runs as a separate process)
6. Launches `mpiexec -n $NODE_COUNT -ppn 1 python src/driver.py`

## Stage 1 — MPI Fork + Ray Start (`driver.py`)

**All ranks run in parallel via MPI.** Rank 0 = head node, rank 1+ = workers.

### Rank 0 (Head Node)

1. **`start_ray_head()`** — `subprocess.Popen("ray start --head --block ...")`.
   Async — returns immediately while Ray head starts in background.
   `--block` keeps the subprocess alive (loops sleeping 1s monitoring child processes),
   but does NOT wait for GCS readiness or any nodes to join.

2. **Launch aurora_serve.py** — `subprocess.Popen("python src/aurora_serve.py --config ...")`.
   Non-blocking. Output is relayed through `ProcessOutputRelay` which scans for the
   readiness marker `"CLUSTER FULLY READY"`.

3. **Wait for readiness marker** — blocks until aurora_serve prints `CLUSTER FULLY READY`
   (timeout: `AURORA_SERVE_READY_TIMEOUT_S`, default 3600s = 1 hour).

4. **HTTP health check** — polls `GET /health` on localhost:8000 to confirm Ray Serve
   HTTP routes are live. Timeout: `RAY_SERVE_HEALTH_TIMEOUT_S` (1800s = 30 min).

5. **Start proxy** (if configured) — generates HAProxy config from discovered backends
   (reads PBS_NODEFILE, creates backend entry per node), starts HAProxy on port 4001.

6. **Print `ALL SERVICES READY`** — run_executor watches for this to start the replay client.

7. **Block** — `serve_process.wait()` keeps the head node alive until aurora_serve exits.

### Rank 1+ (Worker Nodes)

1. **`start_ray_worker()`** — `subprocess.Popen("ray start --address=<head_ip>:6379 --block ...")`.
   Like the head, `--block` just loops sleeping — it does NOT signal when the worker
   has successfully registered with GCS. Registration happens asynchronously inside
   the Ray C++ runtime.

2. **Block** — `ray_process.wait()` keeps the worker alive until Ray dies or is killed.

**Key insight:** There is no explicit barrier between "all workers started" and
"aurora_serve begins". Workers register with GCS asynchronously, and aurora_serve
polls `ray.cluster_resources()` to detect them (see Stage 3 — GPU Poll).

## Stage 2 — Serve Init (`aurora_serve.py`)

Runs as a subprocess on the head node, launched by driver.py rank 0.
**Observed: ~47s constant regardless of cluster size (1 to 256 nodes).**

| Step | Time | What |
|------|------|------|
| `ray.init()` | 0.2–1.5s | Connect to GCS (head node only) |
| Timeout patches | instant | Monkey-patch Ray Serve constants |
| `serve.start()` | ~45s | Create ServeController + schedule ProxyActors |
| **Total** | **~47s** | **Constant across 1–256 nodes** |

### `ray.init(address=...)` — Connect to GCS

- `init_ray_cluster()` calls `ray.init(address="<head_ip>:6379")` with retry logic:
  12 attempts, 5s delay between retries (60s total budget).
- `ray.init()` only needs GCS on the head node — succeeds as soon as the head's
  GCS server is accepting connections. Does NOT wait for any workers.
- Observed: **<2s** at all scales.

### Monkey-patch Ray Serve timeouts

- Patches `HTTP_PROXY_TIMEOUT` from 60s → 3600s (prevents ProxyActor kill cascade at 128n)
- Patches `PROXY_HEALTH_CHECK_TIMEOUT_S` → 300s, `UNHEALTHY_THRESHOLD` → 100
- Also patches any ray.serve module that already imported the constant by name

### `serve.start()` — Start Ray Serve controller + ProxyActors

- **Blocking call.** Returns only after the ServeController is ready.
- Creates ServeController actor (single actor, always on head node)
- With `ProxyLocation.EveryNode`, schedules ProxyActor on every node (port 8000)
- ProxyActors are spawned asynchronously — `serve.start()` returns before they're all healthy

**Why ~45s constant?** The bottleneck is the **Ray metrics agent timeout**, not
ProxyActor spawning. Every new Ray process (GCS server, raylet, ServeController,
core workers) tries to connect to a metrics exporter gRPC service via
`MetricsAgentClientImpl::WaitForServerReadyWithRetry`. The retry parameters are
`constexpr` in `ray/rpc/metrics_agent_client.h`:
`kMetricAgentInitMaxRetries=30`, `kMetricAgentInitRetryDelayMs=1000` (= 30s).
Not configurable at runtime. Things we verified do NOT help:
- `RAY_enable_metrics_collection=0` — does not prevent C++ connection attempt
- `RAY_agent_register_timeout_ms` — controls a different timeout (dashboard ↔ GCS)
- `--disable-metrics-collection` on dashboard agent — only affects Python Prometheus export

**Status: Accepted as fixed ~30s overhead. Constant regardless of cluster scale.**

**Proxy readiness vs metrics timeout interaction:** Removing the GCS sleep
exposed a race: `serve.start()` spawns ProxyActors while the metrics agent is
still in its 30s retry loop. ProxyActors can't respond to the controller's
`.ready()` check during this time. The controller checks all proxies in parallel
(non-blocking async futures in a loop), but `PROXY_READY_CHECK_TIMEOUT_S`
(default 5.0s) is the per-check timeout. After 3 timeouts (15s < 30s), the
controller kills the proxy as unhealthy.

Fix: `RAY_SERVE_PROXY_READY_CHECK_TIMEOUT_S=60` env var in `launch_cluster.sh`.
This is read at module load time by `constants.py` via `get_env_float_positive()`,
so it takes effect in all processes including the ServeController actor. The
monkey-patch / sitecustomize approach does NOT work for this constant because
`proxy_state.py` does `from constants import PROXY_READY_CHECK_TIMEOUT_S` which
copies the value before our hook fires. The env var is the only reliable mechanism.
60s gives 2× headroom over the 30s metrics timeout, and is constant regardless of
node count since all proxy readiness checks run in parallel.

## Stage 3 — GPU Poll (`aurora_serve.py`)

- Polls `ray.cluster_resources()` + `ray.nodes()` in a loop (15s sleep between polls)
- Waits until `total_gpus >= expected_gpus` (100% — changed from 95%)
- 10-minute deadline (increased from 5 min)
- This is the real logical barrier that ensures workers have joined before deploying models

**Note:** The 95% threshold means up to 5% of nodes can be missing. At 256 nodes,
that's 12 nodes. This was presumably pragmatic for straggler tolerance but could
mask real failures.

## Stage 4 — Model Deployment (`aurora_serve.py`)

### 4a. Resolve staged models

- Looks up model paths in node-local `/tmp/hf_home/` (staged in Stage 0)

### 4b. Build node inventory + compute replica plan

- Inventories all Ray nodes and available GPU resources
- Plans replica placement: which GPUs on which nodes get which model replicas
- Default: 12 replicas per node (1 per GPU tile), TP=1, PP=1

### 4c. Deploy models via `serve.run()`

- For each model, creates `VLLMWorker` deployment with computed replica count
- Each replica's `__init__`:
  - Sets `ZE_AFFINITY_MASK` for GPU isolation
  - Constructs vLLM `AsyncEngineArgs`
  - Creates vLLM engine (loads weights, initializes GPU, allocates KV cache)
  - Each replica parses its own Ray worker log for EngineCore sub-phases
    (`weight_load_s`, `kv_cache_init_s`) and reports via Ray actor
- All replicas initialize in parallel across the cluster

### 4d. Collect replica stats

- Stats collected via `ReplicaStatsCollector` Ray actor (zero Lustre I/O)
- Each replica reports: `engine_create_s`, `weight_load_s`, `kv_cache_init_s`,
  plus placement info (hostname, device_id, gpu_ids)
- `engine_create_s` ≈ `weight_load_s` + `kv_cache_init_s` + framework overhead

**engine_create_s breakdown** (observed on 2 nodes, Llama-3-8B):

| Sub-phase | Time | Source |
|-----------|------|--------|
| weight_load_s | ~18s | Safetensors from /tmp → GPU |
| kv_cache_init_s | ~1.7s | Memory profiling + KV cache alloc |
| framework overhead | ~13s | Subprocess spawn, Python imports, device init |
| **engine_create_s** | **~33s** | Total |

**Per-replica sub-phase collection:** Each VLLMWorker parses its own Ray worker
log at `/tmp/ray/session_latest/logs/worker-*-{pid}.out` after `from_engine_args()`
returns (which blocks ~33s until model loading completes). The EngineCore subprocess
output (tagged with `(EngineCore_DP0 pid=...)`) appears in that log file.

**Why not monkey-patch:** vLLM forces `multiprocessing.spawn` (not fork) when
running inside a Ray actor with XPU initialized. The spawned EngineCore subprocess
starts a fresh Python interpreter that does NOT load user site-packages. Verified:
debug marker files in sitecustomize.py appeared for parent processes but not for
any EngineCore PIDs. Neither monkey-patching nor sitecustomize can reach inside
the spawned subprocess.

**Future:** For deeper sub-phase instrumentation, copy the conda environment,
patch vLLM's `EngineCore.__init__` directly, and distribute via MPI bcast before
launching any Python program.

### 4e. Print `CLUSTER FULLY READY`

- driver.py rank 0 detects this marker and proceeds to start the proxy

## Stage 5 — Deploy Drain (`serve.run()` tail)

After all replicas finish `__init__`, `serve.run()` doesn't return immediately.
The ServeController must confirm each replica via `initialize_and_get_metadata`
RPC (runs `reconfigure()` + `check_health()` per replica), then transition each
from STARTING → RUNNING state. This is processed in the controller's reconcile
loop (`CONTROL_LOOP_INTERVAL_S = 0.1s`).

**Observed scaling:**

| Nodes | Replicas | Drain time | Per-replica overhead |
|------:|---------:|-----------:|---------------------:|
| 8     | 96       | 0.3s       | ~3ms                 |
| 32    | 384      | 2.6s       | ~7ms                 |
| 64    | 768      | 66.8s      | ~87ms                |

**Root cause confirmed via instrumentation (run2):** `serve.run()` was decomposed
into `deploy_applications(wait=True)` + `wait_for_proxies_serving()`:

| Phase                        | 2 nodes | 64 nodes |
|------------------------------|--------:|---------:|
| `serve.run.deploy_apps`     |  75.3s  |   78.5s  |
| `serve.run.wait_proxies`    |  0.004s | **69.3s** |
| **Total**                   |  75.3s  |  147.9s  |

`deploy_applications` is constant (~75-78s). The entire scaling overhead is from
`wait_for_proxies_serving()`, which calls `.serving.remote()` on every ProxyActor
and does `ray.wait()` for all responses.

The `.serving()` method on the ProxyActor is a **no-op** (`return` immediately,
proxy.py:1328). Yet collecting 64 no-op remote call results takes 69s. This is
pure Ray RPC overhead: issuing 64 `.remote()` calls to actors across 64 nodes
and resolving them through the gRPC layer + object store.

**Partial finding at 64 nodes:** Running the same `wait_for_proxies_serving`
code on a live 64-node cluster AFTER full startup completes in **0.028s** (64
no-op RPCs, 0.2ms each). This confirms RPCs are fast when proxies are ready.
The 69.3s during startup is spent waiting for ProxyActors still initializing.

**But the scaling is much worse than 30s at larger scales:**

| Nodes | Deploy time (haproxy) | Deploy time (direct) |
|------:|----------------------:|---------------------:|
|     1 |                  74s  |                 75s  |
|    32 |                  78s  |                 79s  |
|    64 |                 148s  |                149s  |
|   128 |                 442s  |                443s  |
|   256 |                1665s  |                349s  |

128 nodes takes 442s (7 min), 256 nodes takes 1665s (27 min) in haproxy mode.
This grows much faster than the 30s metrics timeout can explain. The root cause
at 128+ nodes is **not yet conclusively identified** — it requires instrumented
runs with `deploy_apps` vs `wait_proxies` decomposition at 128-256 nodes.

Possible explanations (unverified):
- ServeController becomes CPU-bound processing 1536-3072 replica state
  transitions, causing `deploy_applications` itself to scale poorly
- ProxyActor restarts cascade at scale (port collisions, node failures),
  multiplying the 30s timeout
- Ray's GCS or scheduler contention under high concurrent actor count
- The haproxy 256n outlier (1665s vs 349s direct) suggests proxy-specific
  issues at that scale

**Next step:** Run 128-256 node experiments with the decomposed instrumentation
(`serve.run.deploy_apps` vs `serve.run.wait_proxies`) using the `startup_only`
flag to minimize queue time.

## Known Issues & Action Items

- **GCS sleep was redundant** — aurora_serve's retry loop handles GCS readiness.
  **Fixed:** Removed `time.sleep()` in driver.py.
- **Metrics agent timeout wastes ~30s** — hardcoded `constexpr` in C++, not
  configurable at runtime. Official Ray position: *"doing no monitoring at all
  is unfortunately not possible now"*
  ([discuss.ray.io](https://discuss.ray.io/t/ray-command-line-parameter-to-turn-off-monitoring-completely/13135)).
  **Status: Accept ~30s overhead. Constant regardless of scale.**
- **Scaling trace I/O is O(n) on Lustre** — per-replica file writes/reads/deletes
  hit MDS contention. Redesign needed: collect via Ray object store or MPI gather,
  not filesystem.
- **95% GPU threshold** — could silently proceed with missing nodes. Consider
  making this configurable or at least logging a prominent warning.
- **`serve.start()` returns before ProxyActors are healthy** — the HTTP health
  check in driver.py (Stage 1, step 4) is the real readiness gate, not
  `serve.start()` itself.
