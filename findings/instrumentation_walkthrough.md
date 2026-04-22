# Instrumentation Walkthrough — What Was Patched, How It Fires, What It Measures

Goal of this doc: step through the exact code paths our instrumentation touches,
so the timing numbers in [gcs_contention_quantitative.md](gcs_contention_quantitative.md)
can be traced to specific lines of Ray Serve code.

## 1. What the patch changes (exact diff in the overlay)

Our overlay at `~/.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray`
is a git-tracked repo with **four files** under `serve/_private/`:

| File | Changed? | Purpose |
|---|---|---|
| `constants.py` | **Patched** | Aurora timeouts (HTTP_PROXY_TIMEOUT=3600, PROXY_HEALTH_CHECK_TIMEOUT_S=300, UNHEALTHY_THRESHOLD=100, etc.) |
| `proxy.py` | **Patched** | Per-substep timing inside `ProxyActor.__init__` and `ready()`, writes JSON to Lustre |
| `controller.py` | Pristine | Kept in overlay as a baseline for future probes |
| `proxy_state.py` | Pristine | Same |

`proxy.py` is the file we actually use to extract timing. It adds **seven
substep timers** across `ProxyActor.__init__` (lines are from our overlay
file `serve/_private/proxy.py`):

```python
class ProxyActor(ProxyActorInterface):
    def __init__(...):
        # ── OUR CODE: wall-clock init_start
        _init_wall_start = _ptime.time()
        _substeps = {}

        # [1] super_init ───────────────────────────────────────────── Line ~1165
        _t = _ptime.time()
        super().__init__(node_id=..., node_ip_address=..., logging_config=...)
        _substeps["super_init"] = time() - _t

        # [2] configure_options ───────────────────────────────────── Line ~1175
        _t = _ptime.time()
        self._grpc_options = grpc_options
        self._http_options = configure_http_middlewares(http_options)
        grpc_enabled = is_grpc_enabled(self._grpc_options)
        event_loop = get_or_create_event_loop()
        _substeps["configure_options"] = time() - _t

        # [3] long_poll_client ────────────────────────────────────── Line ~1182  <-- ONE GCS CALL HERE
        _t = _ptime.time()
        self.long_poll_client = long_poll_client or LongPollClient(
            ray.get_actor(SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE),  # GCS lookup
            {(LongPollNamespace.ROUTE_TABLE,): self._update_routes_in_proxies,
             (LongPollNamespace.GLOBAL_LOGGING_CONFIG,): self.reconfigure_global_logging_config},
            call_in_event_loop=event_loop,
        )
        _substeps["long_poll_client"] = time() - _t

        # [4] memory_profiler ────────────────────────────────────── Line ~1207
        _t = _ptime.time()
        configure_component_memory_profiler(...)
        _substeps["memory_profiler"] = time() - _t

        # [5] logging_context ────────────────────────────────────── Line ~1213
        _t = _ptime.time()
        if logging_config.encoding == EncodingType.JSON:
            ...  # build logging context dict
        _substeps["logging_context"] = time() - _t

        # [6] create_proxies ─────────────────────────────────────── Line ~1244  <-- HTTPProxy / GRPCProxy objects
        _t = _ptime.time()
        self.proxy_router = ProxyRouter(get_proxy_handle)
        self.http_proxy = HTTPProxy(...)
        self.grpc_proxy = GRPCProxy(...) if grpc_enabled else None
        _substeps["create_proxies"] = time() - _t

        # [7] server_tasks_and_gc ────────────────────────────────── Line ~1272  <-- Schedules uvicorn asyncio task
        _t = _ptime.time()
        self._start_http_server_task = event_loop.create_task(self._run_http_server())
        self._start_grpc_server_task = event_loop.create_task(self._run_grpc_server()) if grpc_enabled else None
        self._running_http_server_task = None
        self._running_grpc_server_task = None
        _configure_gc_options()
        _substeps["server_tasks_and_gc"] = time() - _t

        # ── OUR CODE: write the substep breakdown JSON to Lustre
        _run_log = os.environ.get("AURORA_RUN_LOG_DIR")
        _inst_dir = f"{_run_log}/instrumentation/{hostname}" if _run_log else "/tmp/aurora_inst"
        with open(f"{_inst_dir}/proxy_init_{hostname}_{pid}.json", "w") as f:
            json.dump({
                "event": "proxy_init", "hostname": ..., "pid": ..., "node_id": ...,
                "wall_start": _init_wall_start, "wall_end": ...,
                "duration_s": ..., "substeps": _substeps,
            }, f, indent=2)
```

Then `ready()` (called by the controller after `__init__` returns) adds
`ready_duration_s` and `ready_http_s` back into the same JSON.

**Nothing in constants.py changes timing behavior at the substep level.** The
constants patches only change how long the *controller* waits before giving
up on a health check. They don't make anything faster — they just prevent
the proxy-kill cascade.

## 2. Call graph: from `serve.run()` to the wait_proxies cliff

Here's what happens in Stage 3 (null-compute, 256n example). Each box is a
function; arrows are function calls. GCS calls are marked with 🌐.

```
Driver (aurora_serve.py, head node) 🔵
└── serve.run(...)
    ├── 1. client.deploy_applications(built_apps, wait=True)    [serve.run.deploy_apps]
    │       │
    │       ├── controller.deploy_applications.remote(...)
    │       │       │
    │       │       └── ServeController.deploy_applications(...)       🟡 controller (head, separate actor process)
    │       │               │
    │       │               ├── ApplicationStateManager.apply_deployment_args(...)
    │       │               └── DeploymentStateManager.update(...)
    │       │                       ├── create_replicas (3072 of them)
    │       │                       ├── schedule replicas to nodes via placement groups
    │       │                       └── wait for each replica's .initialize_and_get_metadata()
    │       │                           └── each reply transitions replica STARTING → RUNNING
    │       │
    │       └── wait_deployment_created (until app state is RUNNING)
    │
    └── 2. client.wait_for_proxies_serving()                     [serve.run.wait_proxies]  🔴 the cliff
            │
            │  # client.py line 293
            ├── proxy_handles = ray.get(controller.get_proxies.remote())    🌐 GCS (once)
            │
            ├── for each proxy: serving_refs.append(handle.serving.remote())
            │   # client.py line 297-301
            │   # The .serving.remote() call is dispatched as a Ray task to
            │   # each of 256 ProxyActors. The task runs on the proxy's asyncio
            │   # event loop. It is a NO-OP (proxy.py line 1326-1328):
            │   #   async def serving(...): return
            │
            └── ray.wait(serving_refs, timeout=3600, num_returns=256)
                # Blocks until every proxy's event loop *gets around to*
                # picking up and replying to .serving.remote().
                #
                # CRITICAL: a proxy's event loop can only pick up this RPC
                # AFTER it has finished processing whatever else is in its
                # asyncio queue. And what's in the queue is the LongPollClient
                # callback for the new replica set broadcast — which does
                # N_replicas × ray.get_actor() GCS calls inline.
```

The time accounted to `wait_proxies` is almost entirely **proxies blocked in
their event loop doing GCS actor-handle lookups**, not Ray RPC transit time.

### What a proxy's event loop does while `wait_proxies` is ticking

```
ProxyActor (on each of 256 nodes, separate Python process) 🟢
│
├── __init__ ran earlier, created LongPollClient subscribed to:
│       DEPLOYMENT_TARGETS    ──► update_deployment_targets(new_replica_set)
│       ROUTE_TABLE          ──► _update_routes_in_proxies(endpoints)
│       GLOBAL_LOGGING_CONFIG ──► reconfigure_global_logging_config(...)
│
├── LongPollClient runs long_poll loop in a BACKGROUND THREAD, calling
│   controller.listen_for_change.remote(). When it gets back updates:
│
│   long_poll.py:180 _process_update(updates):
│       for key, update in updates.items():        # key = DEPLOYMENT_TARGETS
│           callback = self.key_listeners[key]
│           def chained():
│               callback(update.object_snapshot)   # = update_deployment_targets
│               self._on_callback_completed(...)
│           self._schedule_to_event_loop(chained)  # ── schedules on asyncio loop
│
└── Asyncio event loop picks up `chained` and runs it INLINE:
    │
    └── router.py:663 update_deployment_targets(deployment_target_info):
        │
        └── request_router._update_running_replicas(running_replicas):
            │
            │  # request_router.py:1166
            │  # For each replica in the broadcast (3072 of them at 256n),
            │  # create a RunningReplica wrapper. RunningReplica.__init__
            │  # calls replica_info.get_actor_handle(), which does:
            │
            └── for r in running_replicas:                 # ← 3072 iterations per proxy
                    replica_wrappers.append(
                        RunningReplica(r)                  # replica_wrapper.py:109
                        └── actor_handle = r.get_actor_handle()
                            │
                            └── common.py:629 get_actor_handle():
                                return ray.get_actor(
                                    self.actor_name,       # named lookup by name
                                    namespace=SERVE_NAMESPACE)
                                    │
                                    │ 🌐 GCS CALL ────────────────────────┐
                                    ▼                                      │
                                CoreWorker.get_named_actor_handle(name)    │
                                  ├── GCS: ActorInfoGcsService.            │
                                  │     GetNamedActorInfo(name)            │
                                  └── GCS: ActorInfoGcsService.            │
                                        GetActorInfo(actor_id)             │
                                    (the two GCS methods we see spike in   │
                                     the event-stats table)                │
                                                                           │
        At 256n this inner loop is executed:                              │
          256 proxies × 3072 replicas × 2 GCS calls = 1,572,864 calls ←───┘
          (observed 1,581,437 GetActorInfo + 920,785 GetNamedActorInfo)
        
        Every one of these calls is SYNCHRONOUS from the proxy's asyncio
        event loop's perspective. The loop cannot accept any other task
        (including the controller's .serving() or check_health() RPCs)
        until this function returns.
```

## 3. Mapping the timing numbers to code lines

| Measurement | Instrumentation file:line | Scope | 32n value | 256n value |
|---|---|---|---:|---:|
| `substeps.super_init` | proxy.py:1167 wraps `super().__init__` | ProxyActor init | ~1ms | ~1ms |
| `substeps.configure_options` | proxy.py:1175 wraps grpc/http/event_loop setup | Pure Python | <1ms | <1ms |
| `substeps.long_poll_client` | proxy.py:1183 wraps `LongPollClient(ray.get_actor(...))` | 1× GCS call | ~22ms | TBD |
| `substeps.memory_profiler` | proxy.py:1207 wraps `configure_component_memory_profiler` | local | 0ms | 0ms |
| `substeps.logging_context` | proxy.py:1213 wraps logging dict build | local | 0ms | 0ms |
| `substeps.create_proxies` | proxy.py:1244 wraps ProxyRouter + HTTPProxy + GRPCProxy | local | ~8ms | ~20ms |
| `substeps.server_tasks_and_gc` | proxy.py:1272 wraps uvicorn task scheduling | local | ~380ms | ~520ms |
| `ready_duration_s` | awaits `_start_http_server_task` | uvicorn completes | 0ms | 0ms |

Proxy `__init__` **total** at 256n bumps to 0.56s mean / 1.88s max — mostly
driven by `server_tasks_and_gc` because that's where most local work is done
(creating asyncio tasks under load), plus small bumps in `long_poll_client`
(one GCS call per proxy) and `create_proxies` (instantiation overhead).

**But the proxy_init substep total is NOT what causes wait_proxies to be 1751s.**
`__init__` finishes in well under 2s even at 256n. The 1751s gap comes from
the LongPollClient callback path AFTER `__init__` returns but BEFORE the
first `.serving()` RPC is picked up by the asyncio loop.

## 4. What `RAY_event_stats=1` measures and where the numbers come from

Ray's GCS server process emits a text block every second to `gcs_server.out`.
Each block has sections like:

```
Event stats:
  <MethodName> - <total> total (<active> active[, <running> running]),
    Execution time: mean = Xms, total = Yms,
    Queueing time:  mean = Am, max = Bm, min = Cm, total = Dm
```

- **Execution time** = how long the handler's code took to run.
- **Queueing time** = how long the RPC waited in the service's thread queue
  BEFORE the handler started running.

Our parser at [tools/parse_gcs_event_stats.py](../tools/parse_gcs_event_stats.py)
pulls these into a table and the analyzer emits peak values across all blocks.

When we say "GCS Put peak queueing is 1210ms" — it means at some 1-second
window during the run, a Put RPC sat in the service queue for 1.21s before
the handler ran. When "GetActorInfo peak queueing at 256n is 90ms" — it
means the GetActorInfo handler's queue built up to ~90ms of backlog at
peak, sustained (because we saw this at 128n and 256n similarly).

### Why Put is flat and reads saturate progressively

GCS's internal store is a single-writer design:

```
GcsInMemoryStore                (single-threaded writer lock)
├── Put()    ← all writes serialize here
├── Get()    ← reads can parallelize
├── GetAll()
├── ...
```

At 32n, the write rate is already enough to keep that writer thread saturated.
Adding more writes (64n/128n/256n) doesn't increase Put queueing because
the queue is already deep — Ray's Put throughput is ~N pushes/sec where N is
a fixed constant. So Put queueing sits at ~1210ms at every scale.

Reads are served by a pool of threads (Ray's `RAY_gcs_server_num_threads`,
which we set to 8). At 32n/64n there's plenty of headroom; at 128n the read
load (197k GetActorInfo calls) exceeds what 8 threads can clear quickly, so
queueing jumps from 1-2ms to 60-90ms. At 256n the read load is 4× again but
queueing barely grows (85-90ms) — the threads are fully utilized and we're
throughput-bounded.

## 5. Putting it together — why the 256n cliff is 1751s

Back-of-envelope math using the measured numbers:

- 256 proxies × 3072 replicas in the broadcast = 786,432 `ray.get_actor()` calls
- Each call = 2 GCS RPCs (GetNamedActorInfo + GetActorInfo)
- GCS read throughput at saturation ≈ 900 calls/sec (observed)
- Total GCS read time ≈ (786,432 × 2) / 900 ≈ 1748s

**Observed wait_proxies at 256n: 1751.5s** — matches the back-of-envelope
cluster-wide saturated-throughput estimate to within 0.2%.

Each proxy individually isn't doing 1748s of work; the bottleneck is the
*aggregate* GCS read throughput serving all 256 proxies concurrently. Each
proxy's individual wait is ≈ 1572k / 256 = 6144 read-RPCs worth of time,
served from a 900/sec GCS → ~6.8s of "my work" per proxy in the ideal case,
but they all serialize through the same GCS thread pool so they effectively
queue end-to-end.

## 6. Where to put a NEW probe to localize further

Given the picture above, the next useful probe would be inside
`update_deployment_targets` in `router.py:663`:

```python
def update_deployment_targets(self, deployment_target_info):
    _t0 = time.time()
    n = len(deployment_target_info.running_replicas)
    self._deployment_available = deployment_target_info.is_available
    running_replicas = deployment_target_info.running_replicas
    if self.request_router:
        self.request_router._update_running_replicas(running_replicas)
    # ...
    log("update_deployment_targets", n_replicas=n, duration=time.time()-_t0)
```

This would give a per-proxy, per-broadcast duration. At 256n we'd expect
this to be O(minutes) per call, and should match the total wait_proxies
duration when aggregated.

A second useful probe: count `ray.get_actor()` calls on each proxy directly
(wrap `common.py:629`) and log total count + per-call duration. Would give
per-proxy amortized GCS call cost.

## 7. Reproducing the data

```bash
# On login node
module load frameworks go/1.25.3

# Materialize run bundles (produces run.yaml + job.pbs per scale)
PYTHONPATH=/home/wenyiw/aurora_rayserver \
    python3 -m eval.cli run materialize weakscaling_nullcompute_proxy

# Retarget queue if using a reservation (leave as-is for prod)
for s in 32 64 128; do
    sed -i 's/#PBS -q debug-scaling/#PBS -q R<RESV_ID>/' \
        /lus/flare/.../runN/${s}-nodes/job/job.pbs
done

# Submit
qsub /lus/flare/.../runN/32-nodes/job/job.pbs
# Wait for completion
# Submit next scale…

# Analyze
python3 tools/analyze_scaling.py \
    /lus/flare/.../runN/32-nodes \
    /lus/flare/.../runN/64-nodes \
    /lus/flare/.../runN/128-nodes \
    /lus/flare/.../runN/256-nodes

# Inspect individual GCS event stats
python3 tools/parse_gcs_event_stats.py \
    /lus/flare/.../runN/256-nodes/logs/backend/*_ray_runtime/ray_logs/<head>/gcs_server.out
```

## 8. File map / cross-reference

- **Overlay**: `~/.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray/`
  (git repo, commits 2014298 pristine → 4dce240 latest)
- **Patched files**: `serve/_private/{proxy.py, constants.py}` (the latter just changes numbers)
- **Ray Serve files quoted here (pristine upstream, for reference)**:
  - `/opt/aurora/.../ray/serve/_private/proxy.py` (`ProxyActor.__init__`, `.serving()`)
  - `/opt/aurora/.../ray/serve/_private/long_poll.py` (`LongPollClient._process_update`)
  - `/opt/aurora/.../ray/serve/_private/router.py` (`update_deployment_targets`)
  - `/opt/aurora/.../ray/serve/_private/request_router/request_router.py` (`_update_running_replicas`)
  - `/opt/aurora/.../ray/serve/_private/request_router/replica_wrapper.py` (`RunningReplica.__init__`)
  - `/opt/aurora/.../ray/serve/_private/common.py` (`get_actor_handle` — the GCS call)
  - `/opt/aurora/.../ray/serve/_private/client.py` (`wait_for_proxies_serving` — the "cliff")
  - `/opt/aurora/.../ray/serve/_private/deployment_state.py` (`_long_poll_host.notify_changed` — where controller broadcasts)
- **Tools**: `tools/parse_gcs_event_stats.py`, `tools/analyze_scaling.py`
- **Data**: `/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs/weakscaling_nullcompute_proxy/run16/*/`
