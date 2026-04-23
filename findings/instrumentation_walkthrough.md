# Instrumentation Walkthrough — What's Patched, How It Fires, What It Measures

Goal: step through the exact code paths our instrumentation touches so the
numbers in [corrected_scaling_measurements.md](corrected_scaling_measurements.md),
[section6_probes_evidence.md](section6_probes_evidence.md),
[section6_1_controller_ticks.md](section6_1_controller_ticks.md), and
[section6_2_dsm_breakdown.md](section6_2_dsm_breakdown.md) can be traced to
specific lines of Ray Serve code.

## 1. Overlay files and what each one does

Our overlay at `~/.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray`
is a git-tracked repo. It currently tracks seven files under
`serve/_private/`: six patched files plus one pristine baseline copy
(`proxy_state.py`).

| File | Patched? | Purpose |
|---|---|---|
| `constants.py` | ✅ timeouts | `HTTP_PROXY_TIMEOUT=3600`, `PROXY_HEALTH_CHECK_TIMEOUT_S=300`, `PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD=100` (prevents the ProxyActor kill cascade). |
| `proxy.py` | ✅ timing probe | 7 substep timers in `ProxyActor.__init__` and wall-time around `ready()`. Writes `proxy_init_<host>_<pid>.json` once per proxy. |
| `common.py` | ✅ §6 probe | Every `RunningReplicaInfo.get_actor_handle()` call records `(t, dur_ms)` into a buffered in-memory list; flushed every 500 calls (tunable via `AURORA_PROBE_FLUSH_N`) and again at `atexit`. |
| `router.py` | ✅ §6 probe | Wraps `AsyncioRouter.update_deployment_targets()` to append one JSONL line per call with `n_replicas + duration_s`. |
| `controller.py` | ✅ §6.1 probe | Per-tick JSONL in `ServeController.run_control_loop_step`, capturing sub-phase durations (`cluster_node_info_update`, `dsm_update`, `asm_update`, `node_update`, `proxy_state_update`). |
| `deployment_state.py` | ✅ §6.2 probe | Per-call JSONL in `DeploymentStateManager.update()` with 7 step-level timings (`s1_check_and_update_replicas` … `s7_broadcast`). |
| `proxy_state.py` | pristine | In the overlay for future probe work. |

## 2. Where each probe writes — the /tmp → gather architecture

**During the run**, every probe writes to `/tmp/aurora_inst/` on the node
where the process is running:

```
Proxy worker node N:
  /tmp/aurora_inst/
    proxy_init_<host>_<pid>.json           # one per ProxyActor (proxy.py)
    get_actor_calls_<pid>.csv              # one per ProxyActor process (common.py, buffered)
    router_updates_<pid>.jsonl             # one per ProxyActor process (router.py)

Head (controller) node:
  /tmp/aurora_inst/
    controller_ticks_<pid>.jsonl           # one per ServeController (controller.py)
    dsm_updates_<pid>.jsonl                # one per ServeController (deployment_state.py)
```

Writing to node-local tmpfs eliminates per-call Lustre MDS contention
(which previously added +634s at 256n in run17).

**At end of Stage 3** (right after `wait_for_proxies_serving` returns),
`aurora_serve._collect_instrumentation_all` runs one Ray remote task per
alive node with NodeAffinity scheduling:

```python
@ray.remote(num_cpus=0)
def _read_node_inst():
    out = {"hostname": socket.gethostname(), "files": {}}
    for path in glob.glob("/tmp/aurora_inst/*"):
        if os.path.isfile(path):
            out["files"][os.path.basename(path)] = open(path, "rb").read()
    return out
```

Each task reads its node's `/tmp/aurora_inst/` contents into memory and
returns `{filename: bytes}`. The head process receives these objects via
`ray.get(refs)` and writes one file per (host, filename) to
`$AURORA_RUN_LOG_DIR/instrumentation/<host>/` on Lustre. **One Lustre
write per file per node — no per-call I/O.**

Total Lustre load: ~1,200 files × ~1 MiB at 32n, ~5,000 files × ~5 MiB at
128n. Negligible.

## 3. The 7 substep timers in `ProxyActor.__init__`

```python
class ProxyActor(ProxyActorInterface):
    def __init__(...):
        _init_wall_start = _ptime.time()
        _substeps = {}

        # [1] super_init — ProxyActorInterface.__init__
        _t = _ptime.time()
        super().__init__(node_id=..., node_ip_address=..., logging_config=...)
        _substeps["super_init"] = time() - _t

        # [2] configure_options — HTTP/gRPC options + event loop
        _t = _ptime.time()
        self._grpc_options = grpc_options
        self._http_options = configure_http_middlewares(http_options)
        grpc_enabled = is_grpc_enabled(self._grpc_options)
        event_loop = get_or_create_event_loop()
        _substeps["configure_options"] = time() - _t

        # [3] long_poll_client — ONE ray.get_actor() call to controller
        _t = _ptime.time()
        self.long_poll_client = long_poll_client or LongPollClient(
            ray.get_actor(SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE),  # GCS
            {(LongPollNamespace.ROUTE_TABLE,): self._update_routes_in_proxies, ...},
            call_in_event_loop=event_loop,
        )
        _substeps["long_poll_client"] = time() - _t

        # [4] memory_profiler
        _substeps["memory_profiler"] = ...

        # [5] logging_context
        _substeps["logging_context"] = ...

        # [6] create_proxies — ProxyRouter + HTTPProxy + GRPCProxy
        _substeps["create_proxies"] = ...

        # [7] server_tasks_and_gc — event_loop.create_task(run_http_server)
        _substeps["server_tasks_and_gc"] = ...

        # Write proxy_init JSON to /tmp/aurora_inst
        with open(f"/tmp/aurora_inst/proxy_init_{host}_{pid}.json", "w") as f:
            json.dump({
                "event": "proxy_init", "hostname": host, "pid": pid,
                "wall_start": _init_wall_start, "wall_end": time(),
                "duration_s": _init_dur, "substeps": _substeps,
            }, f, indent=2)
```

`ready()` (awaiting `_start_http_server_task`) adds `ready_duration_s` +
`ready_http_s` into the same JSON.

### Observed substep times (128n, overhead-free run21)

| Substep | Typical (ms) |
|---|---:|
| super_init | ~1 |
| configure_options | <1 |
| long_poll_client | ~50 (1 GCS call) |
| memory_profiler | 0 |
| logging_context | 0 |
| create_proxies | ~20 |
| server_tasks_and_gc | ~500 |
| **total `__init__`** | **~360 mean, 570 max** |

## 4. Call graph — from `serve.run()` to the wait_proxies cliff

```
Driver (aurora_serve.py, head node) 🔵
└── serve.run(...)
    ├── 1. client.deploy_applications(built_apps, wait=True)     [serve.run.deploy_apps]
    │       └── controller.deploy_applications.remote(...)
    │               └── ServeController.deploy_applications(...) 🟡 (head, separate actor process)
    │                       └── DeploymentStateManager.update()  ◄── §6.2 probe
    │                           runs 8 sequential steps (s1..s8), broadcast in s7
    │                           ┌── s1_check_and_update_replicas  (per-replica state transition)
    │                           ├── s3_drain_nodes                (linear O(nodes))
    │                           ├── s6_schedule_and_stop          (one-time burst: ~1ms/replica)
    │                           └── s7_broadcast                  ► notify_changed fans out
    │                                                              to all subscribed proxies
    │
    └── 2. client.wait_for_proxies_serving()                      [serve.run.wait_proxies]  🔴 the cliff
            │
            ├── proxy_handles = ray.get(controller.get_proxies.remote())
            │
            ├── for each proxy: serving_refs.append(handle.serving.remote())
            │   # proxy.py line 1326-1328:
            │   #   async def serving(...): return   # no-op
            │
            └── ray.wait(serving_refs, timeout=HTTP_PROXY_TIMEOUT, num_returns=N)
                # Waits until every proxy's event loop picks up and replies.
                # In this repo's Serve config, proxies can only reply AFTER
                # the handle-router LongPollClient callback processes the
                # replica set and does N_replicas ray.get_actor() lookups
                # (below).
```

### Important repo-specific assumption for the event-loop story

This repo exports `RAY_SERVE_THROUGHPUT_OPTIMIZED=1`
(`src/driver.py`, `scripts/launch_cluster.sh`). In upstream Ray Serve,
that flips `RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP` from its default `1`
to `0`.

That matters because proxy-created handles then use
`CurrentLoopRouter` rather than `SingletonThreadRouter`. So in **this**
repo's runs, the handle router and the proxy actor share the same
asyncio loop; the event-loop-blocking explanation below depends on that
configuration.

### How a proxy becomes subscribed to `DEPLOYMENT_TARGETS`

`ProxyActor.__init__` itself only creates a `LongPollClient` for
`ROUTE_TABLE` and `GLOBAL_LOGGING_CONFIG`. The `DEPLOYMENT_TARGETS`
subscription is reached later, through the route-table update path:

```
ProxyActor.__init__()
└── LongPollClient subscribes to:
    ├── ROUTE_TABLE
    └── GLOBAL_LOGGING_CONFIG

ROUTE_TABLE update arrives
└── ProxyActor._update_routes_in_proxies(endpoints)
    └── ProxyRouter.update_routes(endpoints)
        └── for each new endpoint:
            self._get_handle(endpoint, info)            # = get_proxy_handle(...)
            └── client.get_handle(...)
                └── if not handle.is_initialized:
                    handle._init(
                        _run_router_in_separate_loop=RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP,
                        ...
                    )
                    └── create_router(...)
                        └── CurrentLoopRouter(...)      # in this repo's config
                            └── AsyncioRouter(...)
                                ├── dedicated LongPollClient for fast initial update
                                └── SharedRouterLongPollClient registration
                                    subscribes to DEPLOYMENT_TARGETS / DEPLOYMENT_CONFIG
```

So the startup chain is:

`ROUTE_TABLE` broadcast
→ `ProxyRouter.update_routes()`
→ `get_proxy_handle()`
→ `handle._init()`
→ router creation
→ router subscribes to `DEPLOYMENT_TARGETS`
→ later `DEPLOYMENT_TARGETS` broadcast calls `update_deployment_targets()`.

### In this repo's config, what shares the proxy's event loop during `wait_proxies`

```
ProxyActor (on each of N nodes, its own Python process) 🟢
│
├── ProxyActor main loop also hosts handle routers
│   because this repo sets RAY_SERVE_THROUGHPUT_OPTIMIZED=1
│   → RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP=0
│   → proxy-created handles use CurrentLoopRouter
│
├── Handle-router LongPollClient (running long-poll loop in background thread):
│   ◄── controller ServeController.listen_for_change returns updates
│   long_poll.py:180 _process_update(updates):
│       self._schedule_to_event_loop(chained_callback)
│
└── Asyncio event loop runs `chained_callback`:
    ◄── §6 probe (router.py) records n_replicas + duration
    router.py:663 update_deployment_targets(DeploymentTargetInfo):
        │
        └── request_router._update_running_replicas(running_replicas):
            │
            │  # request_router.py:1166
            │  # For each of N_replicas (3072 at 256n), create RunningReplica wrapper.
            │
            └── for r in running_replicas:                     # N_replicas iterations
                    replica_wrappers.append(RunningReplica(r))
                    └── common.py:628 get_actor_handle():
                        ◄── §6 probe (common.py) records (t, dur_ms)
                        return ray.get_actor(
                            self.actor_name,
                            namespace=SERVE_NAMESPACE)
                            │
                            │ 🌐 TWO GCS CALLS per ray.get_actor():
                            ▼
                        CoreWorker.get_named_actor_handle(name)
                          ├── GCS: GetNamedActorInfo(name)
                          └── GCS: GetActorInfo(actor_id)
```

At 256n, one full-broadcast back-of-envelope is:

- 256 proxies × 3072 replicas = **786,432 logical `ray.get_actor()` calls**
- If every lookup takes the cold path, that implies up to
  **~1.57M underlying GCS RPCs** (`GetNamedActorInfo` + `GetActorInfo`)

The totals quoted in §5 and §6 are different measurement streams and
should not be merged into one unit:

- §5 `RAY_event_stats`: per-RPC counters by GCS method
  (`GetActorInfo`, `GetNamedActorInfo`, ...)
- §6 direct probe: per-call timings of logical `ray.get_actor()` calls

### Interlude: how the `ServeController` actor is spawned, and why its event loop is everything

The `.remote()` method calls scattered throughout the call graph above (and
our §6.1 tick probe, and §6.2 dsm probe, and the whole 128n cliff story)
only make sense if you know how the `ServeController` actor gets started
and what runs inside it. The short version: **`ServeController` is a Ray
actor whose single asyncio event loop hosts the control loop, every
inbound RPC handler, and the LongPoll fan-out — all three compete for
the same thread.**

#### Actor spawn chain

```
aurora_serve.py driver process (head node, Python process #A)
  serve.run(deployment)                                  [serve/api.py:686]
    _run(...)                                            [serve/api.py:614]
      client = _private_api.serve_start(...)             [serve/api.py:593]
        ▼
      serve_start_async(...)                             [_private/api.py:85-111]
        controller_impl = get_controller_impl()          ← applies
                                                           @ray.remote(...)
                                                           to ServeController
                                                           [default_impl.py:225]
        controller = controller_impl.remote(...)         ← SPAWN ACTOR
                                                           [api.py:94]
        # returned handle points at the new controller actor
```

`controller_impl.remote(...)` tells the raylet on the head node to:

1. Allocate a new Python process (honoring `num_cpus=0` +
   `head_node_resource` from the `@ray.remote(...)` decorator —
   [default_impl.py:225-235](../../.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray/serve/_private/default_impl.py#L225-L235)).
2. Start that process.
3. Instantiate `ServeController(*args)` inside it. Because `__init__` is
   declared `async def`, Ray creates an asyncio event loop and runs
   `__init__` as a coroutine on it.
4. Once `__init__` returns, the actor is "alive" and accepts `.remote()`
   RPC calls on its methods; those RPCs are dispatched as coroutines onto
   the same event loop.

#### What `ServeController.__init__` does

Most of it wires up long-lived state: kv_store, `LongPollHost`,
`DeploymentStateManager`, `ApplicationStateManager`, `ProxyStateManager`,
`EndpointState`, metrics. Then at the end:

```python
# controller.py:229-230
self._create_control_loop_metrics()
run_background_task(self.run_control_loop())     # ← kickoff
```

`run_background_task(coro)`
([ray/_common/utils.py:103](../../.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray/_common/utils.py#L103))
is a thin wrapper around:

```python
task = asyncio.get_event_loop().create_task(coroutine)
_BACKGROUND_TASKS.add(task)   # strong reference so GC doesn't kill it
```

So `run_control_loop` becomes a **background `asyncio.Task` on the same
event loop** the controller uses for everything else. It runs
concurrently with inbound RPC handlers.

#### What the control loop does — [controller.py:416](../../.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray/serve/_private/controller.py#L416)

```python
async def run_control_loop(self) -> None:
    while True:
        try:
            await self.run_control_loop_step(...)        # one tick
        except Exception:
            await asyncio.sleep(1)

        loop_duration = time.time() - loop_start_time
        if loop_duration > 10:
            logger.warning(f"The last control loop was slow (took {loop_duration}s). ...")
        num_loops += 1
        await asyncio.sleep(CONTROL_LOOP_INTERVAL_S)     # default 0.1s
```

Inside each tick (where our §6.1 probe records sub-phase timings):

```
run_control_loop_step()                                  [controller.py:452]
  ├── self.cluster_node_info_cache.update()              [:460]
  ├── self.deployment_state_manager.update()             [:486]  ← §6.2 dsm probe
  ├── self.application_state_manager.update()            [:504]
  └── self.proxy_state_manager.update(...)               [:511+]
```

#### The single-event-loop picture

```
┌───────────────────────────────────────────────────────────────────────┐
│ aurora_serve.py driver process (head node, Python process #A)         │
│                                                                       │
│   serve.run(deployment)                                               │
│     └── serve_start_async()                                           │
│           └── controller_impl.remote(...)      ──────┐                │
│                 (fire RPC to raylet; returns handle) │                │
└──────────────────────────────────────────────────────┼────────────────┘
                                                       │ spawn
                                                       ▼
┌───────────────────────────────────────────────────────────────────────┐
│ ServeController actor (head node, Python process #B)                  │
│                                                                       │
│   asyncio event loop (single thread)                                  │
│                                                                       │
│   ┌──────────────┐   ┌──────────────────┐   ┌─────────────────────┐   │
│   │ RPC handler  │   │ run_control_loop │   │ LongPollHost        │   │
│   │ for          │   │ (background task │   │ (async task for     │   │
│   │ deploy_apps  │   │  created in      │   │  listen_for_change  │   │
│   │ get_proxies  │   │  __init__ at     │   │  broadcasts)        │   │
│   │ list_services│   │  line 230)       │   │                     │   │
│   └───────▲──────┘   └────────▲─────────┘   └──────────▲──────────┘   │
│           │                   │                         │             │
│           │ Ray dispatches    │ awaits                  │ fires on    │
│           │ inbound RPCs      │ run_control_loop_step() │ notify_     │
│           │ onto this loop    │ every ~0.1s             │ changed     │
│                                                                       │
│   All three coroutines share the SAME event loop — they cooperatively │
│   yield at `await` points. If any one holds the loop (e.g. a slow     │
│   synchronous stretch inside dsm.update()), the others are starved.   │
└───────────────────────────────────────────────────────────────────────┘
```

#### Why this matters for the 128n wait_proxies cliff

Everything we probe in §6.1 and §6.2 comes back to *this* architecture:

- **Controller tick max 4.3 s at 128n (§6.1).** A single `dsm.update()`
  iteration that takes 3–4 s is blocking the asyncio loop for that whole
  window. During that blockage, no inbound RPCs get served, no LongPoll
  subscribers get pulled, the next tick can't start.
- **300 s proxy healthcheck timeout warnings at 256n.** The controller
  serves `check_health` RPCs from proxies on the **same** loop that is
  spending seconds inside `dsm.update()` per tick. The RPC queues up.
  The proxy, meanwhile, is itself stuck in its own asyncio loop doing
  `update_deployment_targets` → `get_actor_handle × N_replicas`. Both
  ends of the RPC are pathologically single-threaded.
- **Per-call `update_deployment_targets` max 110.9 s at 128n (§6).** The
  proxy's asyncio loop also hosts the handle-router LongPollClient
  callback (because this repo exports `RAY_SERVE_THROUGHPUT_OPTIMIZED=1`,
  which flips `RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP=0` — see §4.) So
  the N_replicas `get_actor_handle` lookups happen on the same loop that
  answers `serving()` and `check_health()`. Until that full iteration
  completes, the proxy *cannot* ack readiness.
- **`s7_broadcast` (dsm's notify_changed fan-out) scaling from 3 ms
  (32n) to 514 ms (128n) (§6.2).** That's the controller trying to push
  `DEPLOYMENT_TARGETS` to N LongPoll subscribers via RPCs that must all
  serialize through its single event loop.

The controller's single event loop is a shared resource; the cliff is
what happens when the demand for that resource scales
superlinearly (N × N_replicas × per-call GCS latency) while the supply
stays at one cooperating coroutine.

#### Who "spawns" the control loop process

Nobody spawns a process — `run_control_loop` is **just an `asyncio.Task`**
living inside the controller actor's existing Python process. `__init__`
creates the task via `run_background_task(...)` on line 230; Python's
asyncio scheduler keeps it alive. It dies only when the actor dies
(controller crash, `serve.shutdown`, raylet SIGTERM). There's no
separate thread or process.

The closest thing to a "spawning event" is:

1. `controller_impl.remote(...)` at [api.py:94](../../.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray/serve/_private/api.py#L94) — spawns the controller **process** (Ray raylet side).
2. `run_background_task(self.run_control_loop())` at [controller.py:230](../../.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray/serve/_private/controller.py#L230) — schedules the control loop **coroutine** onto the process's existing event loop.

## 5. Mapping the timing numbers to code lines (clean, run21)

| Measurement | Probe file:line | Scope | 32n | 64n | 128n |
|---|---|---:|---:|---:|---:|
| **proxy.py / init substeps** | | | | | |
| super_init | proxy.py:1172 | local (`super().__init__`) | ~1ms | ~1ms | ~1ms |
| long_poll_client | proxy.py:1187 | 1 GCS call (to controller) | ~22ms | ~30ms | ~50ms |
| server_tasks_and_gc | proxy.py:1274 | event_loop.create_task | ~380ms | ~420ms | ~520ms |
| total `__init__` | — | everything above | 0.34s | 0.34s | 0.36s |
| **common.py / per-call GCS** | | | | | |
| median | common.py:681 | fast path (actor cache hit) | 0.044 ms | 0.044 ms | 0.047 ms |
| mean | common.py:681 | includes slow path | 13.4 ms | 30.7 ms | 82.0 ms |
| p99 | common.py:681 | high-tail GCS queueing | 137 ms | 227 ms | 295 ms |
| max | common.py:681 | worst-case | 363 ms | 918 ms | 2,349 ms |
| **router.py / update_deployment_targets** | | | | | |
| mean | router.py:663 | per-call (iterates N_replicas) | 0.33s | 0.67s | 5.3s |
| max | router.py:663 | worst broadcast | 6.2s | 7.7s | **110.9s** |
| per-proxy max total time | — | all broadcasts for one proxy | 6.3s | 7.8s | **112s** |
| **controller.py / tick** | | | | | |
| tick mean | controller.py:452 | per-tick reconcile | 28ms | 60ms | 59ms |
| tick max | controller.py:452 | worst tick | 1018ms | 2124ms | **4282ms** |
| total time in ticks | — | cumulative controller CPU | 17.5s | 54.6s | 197.7s |
| **deployment_state.py / dsm.update()** | | | | | |
| total mean | deployment_state.py:3635 | per-dsm-update | 32ms | 57ms | 105ms |
| **s1_check_and_update_replicas mean** | ditto | per-replica state probe | 20.5ms | 32.5ms | 57.3ms |
| **s6_schedule_and_stop max** | ditto | **one-time startup burst** | **1014ms** | **2048ms** | **3915ms** |
| **s7_broadcast max** | ditto | notify_changed fan-out | 3ms | **477ms** | **514ms** |

### Why `update_deployment_targets` max cliffs from 7.7 s (64n) to 110.9 s (128n)

The function iterates every replica in the broadcast snapshot and calls
`ray.get_actor` via `get_actor_handle` for each one. Both the iteration
count and the per-call latency degrade between 64n and 128n, and their
product crosses a regime boundary.

**Factor 1 — replicas per broadcast roughly doubles per scale** (~640 →
~1280 from 64n → 128n).

**Factor 2 — per-call `get_actor_handle` latency ~triples**:

| | 32n | 64n | 128n |
|---|---:|---:|---:|
| median (cache hit) | 0.044 ms | 0.044 ms | 0.047 ms **(flat)** |
| **mean** | 13.4 ms | 30.7 ms | **82.0 ms** (~2.7×) |
| max | 363 ms | 918 ms | 2,349 ms |

Median is flat 44 μs because that is the actor-handle cache fast path.
Mean tracks GCS slow-path fraction — the share of lookups that miss the
cache and round-trip to the GCS `GetNamedActorInfo`+`GetActorInfo` pair
(read-pool pegged at ~85–90 ms peak queueing from 128n onward, per §5).

**Multiplying the two factors against "every replica hits the slow
path":**

| Scale | N_replicas × mean | Observed max | Slow-path fraction observed |
|---|---:|---:|---:|
| 64n | 640 × 30.7 ms = **19.6 s** | **7.7 s** | ~40% (cache still absorbing) |
| 128n | 1280 × 82 ms = **105 s** | **110.9 s** | **~100%** (cache fully saturated) |

At 64n, the observed max is well below the every-call-slow bound — the
actor-handle cache is still catching most of the 640 lookups. At 128n
the observed max **matches** the bound: essentially every lookup in the
worst broadcast went through GCS.

**Why the cache saturates at 128n specifically.** Ray's `CoreWorker`
actor-handle cache is bounded and gets evicted under three pressures
that all worsen together:

1. **Replica churn** — more replicas starting, more `(name, namespace)`
   entries being added, more evictions of older entries.
2. **Concurrent broadcasts fan-out** — 128 proxies each processing a
   fresh `DEPLOYMENT_TARGETS` update in parallel. Cache entries don't
   get a chance to warm up before the next broadcast overwrites them.
3. **GCS read-pool saturation** — ~2,100 req/s cluster-wide ceiling.
   Once queue depth is non-trivial, even cache-adjacent paths that
   require a GCS metadata check slow down.

Compound factor between 64n and 128n:

- **2×** (replicas) ×
- **2.7×** (mean per-call) ×
- **~2.5×** (slow-path fraction rising from ~0.4 to ~1.0)
- ≈ **~14× cliff** → 7.7 s → 110.9 s, exactly the measurement.

This is the signature of a linear scan with per-call latency that is
not O(1). The architectural fix — ship pre-resolved actor handles in
the broadcast payload — zeros out both factors: no iteration, no
per-replica GCS round-trip.

## 6. Per-proxy aggregate ≈ wait_proxies (the clean causal chain)

This is the headline finding from [corrected_scaling_measurements.md](corrected_scaling_measurements.md):

| Scale | wait_proxies (observed) | Per-proxy **max** total GCS time (measured) | Ratio |
|---|---:|---:|---:|
| 32n | 20.58s | 23.4s | 1.14× |
| 64n | 91.86s | 95.6s | 1.04× |
| **128n** | **393.87s** | **395.6s** | **1.004×** |

**`wait_proxies` ≈ time the slowest proxy spent in `ray.get_actor()` calls inside `update_deployment_targets`.** The probe directly measures this; it matches the observed cliff to within 0.4% at 128n.

## 7. `RAY_event_stats=1` — the GCS-side view

Ray's built-in server-side telemetry (enabled with
`RAY_event_stats=1 RAY_event_stats_print_interval_ms=1000`) emits a
text block every 1 second to `gcs_server.out` on the head node:

```
Event stats:
    GcsInMemoryStore.Put - 9 total (6 active), Execution time: mean = 157ms,
        Queueing time: mean = 160ms, max = 1440ms, total = 1440ms
    ActorInfoGcsService.grpc_server.GetActorInfo.HandleRequestImpl - 25441 total,
        Execution time: mean = 0.1ms, Queueing time: mean = 0.3ms, max = 30ms
    ...
```

- **Execution time** = handler code duration.
- **Queueing time** = how long the RPC waited in the dispatch queue before the handler started.

Our [parse_gcs_event_stats.py](../tools/parse_gcs_event_stats.py) parses
these into a time-series CSV and peak summary. Because the stats are
collected by Ray itself (not by us), this view has **zero instrumentation
overhead** and is available in every run including run16 baseline.

Key observations from §5:
- `GcsInMemoryStore.Put` peak queueing is flat at ~1210ms across all scales — single-writer lock already saturated at 32n.
- GCS **read** paths (`GetActorInfo`, `GetNamedActorInfo`, `HealthCheck`) plateau at ~85-90ms peak queueing from 128n onward — read-pool saturated at ~2100 calls/sec cluster-wide.
- `GetActorInfo` counts scale **~4× per node-doubling** (25k → 100k → 397k → 1.58M), while `GetNamedActorInfo` also grows superlinearly (14k → 52k → 206k → 921k) — consistent with the O(N²) startup/update pattern.

## 8. What each probe adds to the picture

| Probe | Measures | Answers question |
|---|---|---|
| proxy.py substeps (§orig) | Proxy `__init__` time broken into 7 sub-phases | Where does proxy init spend time? (answer: mostly `server_tasks_and_gc` ~500ms) |
| common.py get_actor_handle (§6) | Every individual GCS lookup's duration | What does the per-call GCS distribution look like? |
| router.py update_deployment_targets (§6) | Every `AsyncioRouter.update_deployment_targets()` call's duration + replica count | How long does processing one broadcast take? |
| controller.py tick (§6.1) | Each reconcile loop iteration's total + per-sub-phase time | Is the controller's asyncio loop saturated? If so, in which step? |
| deployment_state.py dsm.update() (§6.2) | 7 step-level timings inside `dsm.update()` | Which dsm step (s1..s7) dominates a controller stall? |
| RAY_event_stats (§5, built-in) | Per-RPC execution + queueing time at GCS | Is GCS the bottleneck, and in which methods? |

Layered together, these give the complete Stage 3 decomposition:

```
    ┌─────────────────────────────────────────────────────────────┐
    │ Section 5: RAY_event_stats                                  │
    │   What GCS sees: per-RPC rate, queue depth, which methods   │
    └─────────────────────────────────────────────────────────────┘
                                  ▲
                                  │ triggered by
    ┌─────────────────────────────┴───────────────────────────────┐
    │ Section 6.1: controller tick probe                          │
    │   Controller's asyncio loop activity + where each tick      │
    │   spends time                                               │
    └─────────────────────────────────────────────────────────────┘
                                  ▲
                                  │ inside each tick's dsm_update:
    ┌─────────────────────────────┴───────────────────────────────┐
    │ Section 6.2: dsm.update() step breakdown                    │
    │   Which of the 7 dsm steps stalls the controller            │
    │   (answer: s6 at startup, s7_broadcast at steady state)     │
    └─────────────────────────────────────────────────────────────┘
                                  ▼
                            broadcast event
                                  ▼
    ┌─────────────────────────────────────────────────────────────┐
    │ Section 6: router.py update_deployment_targets probe        │
    │   How long each proxy's broadcast-processing call takes     │
    └─────────────────────────────────────────────────────────────┘
                                  ▲
                                  │ inside each update_deployment_targets:
    ┌─────────────────────────────┴───────────────────────────────┐
    │ Section 6: common.py get_actor_handle probe                 │
    │   Per-call GCS lookup latency distribution                  │
    │   (median 44us cached, mean 82ms at 128n)                   │
    └─────────────────────────────────────────────────────────────┘
```

## 9. Reproducibility

### Submit a run

```bash
module load frameworks go/1.25.3
PYTHONPATH=/home/wenyiw/aurora_rayserver python3 -m eval.cli run materialize weakscaling_nullcompute_proxy
# retarget queue if using a reservation (leave as-is for prod at 256n)
for s in 32 64 128; do
    sed -i 's/#PBS -q debug-scaling/#PBS -q R8443082/' \
        /lus/flare/.../runN/${s}-nodes/job/job.pbs
done
qsub /lus/flare/.../runN/32-nodes/job/job.pbs
```

### Analyze

```bash
# Cross-scale summary (wait_proxies, proxy init, controller events, GCS stats)
python3 tools/analyze_scaling.py runN/{32,64,128,256}-nodes

# §6 probes — per-proxy GCS calls + update_deployment_targets breakdown
python3 tools/analyze_probes.py runN/{32,64,128,256}-nodes

# §6.1 controller tick timing
python3 tools/analyze_controller_ticks.py runN/{32,64,128,256}-nodes

# §6.2 dsm.update() step breakdown
python3 tools/analyze_dsm.py runN/{32,64,128,256}-nodes

# Raw GCS event stats
python3 tools/parse_gcs_event_stats.py runN/256-nodes/.../gcs_server.out
```

## 10. File map and references

### Overlay (`~/.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray`)
- `serve/_private/proxy.py` — init substeps + ready timing
- `serve/_private/common.py` — buffered get_actor_handle probe + module-level `_aurora_probe_record` helpers
- `serve/_private/router.py` — update_deployment_targets JSONL
- `serve/_private/controller.py` — run_control_loop_step JSONL
- `serve/_private/deployment_state.py` — dsm.update() JSONL
- `serve/_private/constants.py` — timeout patches
- `.gitignore`

### Main repo (`perf-inst-dev`)
- `scripts/launch_cluster.sh` — overlay build + symlink tree + env vars
- `src/aurora_serve.py` — `_collect_instrumentation_all` Ray-remote gather + `_collect_proxy_profiles` legacy summary
- `tools/parse_gcs_event_stats.py` — GCS server-side event parser
- `tools/analyze_scaling.py` — cross-scale table
- `tools/analyze_probes.py` — §6 probes (get_actor + router)
- `tools/analyze_controller_ticks.py` — §6.1 ticks
- `tools/analyze_dsm.py` — §6.2 dsm sub-phases
- `findings/` — all analysis docs

### Pristine upstream (`/opt/aurora/.../ray/serve/_private/`)
Reference-only; every overlay patch can be recovered via
`git -C <overlay> diff 2014298 HEAD -- <file>`.

### Data
`/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs/weakscaling_nullcompute_proxy/runN/*` for runs 16 through 21.
- run16: baseline (no probes)
- run17: §6 probes (per-call Lustre writes — **overhead artifact, superseded**)
- run18: adds §6.1 (partial, with Lustre-per-call overhead)
- run19: adds §6.2 (with Lustre-per-call overhead)
- run20: buffered common.py only
- run21: **clean architecture** (all probes to /tmp + ray-gather) — this is the reference run
