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

At launch, `src/aurora_rayserver/resources/launch_cluster.sh` copies every `*.py` currently
present under the overlay's `serve/_private/` into `/tmp/ray_overlay`.
So the tracked files below are the intended patch set, but any untracked
Python file left in that directory is also active for that run.

Source links in this walkthrough use the overlay path for patched
`serve/_private` files. For unpatched Ray files that are not present in
the sparse overlay, links point at the Aurora framework install under
`/opt/aurora/26.26.0/frameworks/aurora_frameworks-2025.3.1/`.

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

**During the run**, the core probes write to `/tmp/aurora_inst/` on the node
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

Other diagnostics can also appear in the same directory depending on
launcher settings, for example `uc_load_*.txt`, `proxy_wrap_debug_*.txt`,
or legacy proxy-profile files. The files above are the ones this
walkthrough uses for the Stage 3 decomposition.

Writing to node-local `/tmp` eliminates per-call Lustre MDS contention
(which previously added +634s at 256n in run17).

**At end of Stage 3** (right after `aurora_serve.py`'s decomposed
`wait_proxies` loop returns; this loop is equivalent to
`wait_for_proxies_serving`), `aurora_serve._collect_instrumentation_all`
runs one Ray remote task per alive node with NodeAffinity scheduling:

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

Measured run21 gather size: 1,267 files / 1.46 MiB total at 32n, and
4,997 files / 15.99 MiB total at 128n. Negligible compared with the old
per-call Lustre path.

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
| long_poll_client | ~50 (one `ray.get_actor` named-actor lookup) |
| memory_profiler | 0 |
| logging_context | 0 |
| create_proxies | ~57 mean / 45 median |
| server_tasks_and_gc | ~332 mean / 360 median |
| **total `__init__`** | **~440 mean, 653 max** |

## 4. Call graph — from Serve deployment to the wait_proxies cliff

Ray Serve uses a **declarative reconcile loop**: the controller-side
`deploy_applications` RPC writes target application state, while actual
deployment reconciliation, replica spawning, healthchecks, and long-poll
snapshot updates happen asynchronously in the controller's control loop
(see the Interlude below for how that loop is spawned and why it
matters).

Both upstream `serve.run(target)` and this repo's decomposed
`aurora_serve.py` path wait for ingress deployment creation and
application RUNNING by default. That waiting is controlled by the
internal `_blocking: bool = True` parameter on
[`_run(...)`](/opt/aurora/26.26.0/frameworks/aurora_frameworks-2025.3.1/lib/python3.12/site-packages/ray/serve/api.py#L614),
which `_run` passes through as `wait_for_applications_running=_blocking`
to `client.deploy_applications`. The public `blocking: bool = False`
parameter on `serve.run(...)` controls something **different** — whether,
after the application is RUNNING, the call should `wait_for_interrupt()`
and loop logging status until Ctrl-C'd ([api.py:732-733](/opt/aurora/26.26.0/frameworks/aurora_frameworks-2025.3.1/lib/python3.12/site-packages/ray/serve/api.py#L732-L733)).
So the readiness wait is **independent of the public `blocking`
parameter**.

What's specific to this repo's `aurora_serve.py` is **decomposition**,
not different waiting semantics: each step is wrapped in its own
`tracer.phase(...)` block so the deploy RPC, the deployment-creation
poll, the application-RUNNING poll, and the wait_proxies step can each
be timed independently. The `serve.run.deploy_apps` phase is therefore a
mix of one fast intent RPC plus client-side polling waits for the
control loop to catch up.

```
Driver (aurora_serve.py, head node) 🔵
└── serve.run-equivalent path
    # aurora_serve.py decomposes serve.run so it can time each phase.
    ├── 1. client.deploy_applications(... wait_for_* = True)     [serve.run.deploy_apps]
    │   # Step 1a: fast intent RPC. This RPC returns as soon as the
    │   # controller stores target application state; no replicas are
    │   # spawned inside this RPC.
    │       └── controller.deploy_applications.remote(...)
    │               └── ServeController.deploy_applications(...) 🟡 (controller actor)
    │                       └── application_state_manager.deploy_apps(...)
    │                               └── ApplicationState._set_target_state(...)
    │                                   # stores _target_state, flips app status
    │                                   # to DEPLOYING, returns.
    │
    │   # Step 1b/1c: client-side waits.
    │       ├── _wait_for_deployment_created(...)
    │       │   # polls controller.get_deployment_status until the deployment
    │       │   # has been registered by the control loop.
    │       └── _wait_for_application_running(...)
    │           # polls controller.get_serve_status until the app status is RUNNING.
    │
    │   Meanwhile, already running on the controller's asyncio loop:
    │   ┌───────────────────────────────────────────────────────────┐
    │   │ run_control_loop (background task, see Interlude below)   │
    │   │   while True:                                             │
    │   │     await run_control_loop_step(...)  ◄── §6.1 probe      │
    │   │       ├── cluster_node_info_cache.update()                │
    │   │       ├── deployment_state_manager.update() ◄── §6.2 probe│
    │   │       │   runs 7 measured steps plus cleanup              │
    │   │       │     ├── s1_check_and_update_replicas              │
    │   │       │     ├── s3_drain_nodes                            │
    │   │       │     ├── s6_schedule_and_stop (startup burst)      │
    │   │       │     └── s7_broadcast — notify_changed updates     │
    │   │       │                        LongPollHost snapshots and │
    │   │       │                        wakes current waiters for  │
    │   │       │                        DEPLOYMENT_TARGETS/CONFIG  │
    │   │       ├── application_state_manager.update()              │
    │   │       │   reads _target_state, tells dsm target replicas, │
    │   │       │   and for routed deployments calls                │
    │   │       │   endpoint_state.update_endpoint(...) which       │
    │   │       │   publishes ROUTE_TABLE to proxies. This route    │
    │   │       │   update is not gated on all replicas being       │
    │   │       │   healthy.                                       │
    │   │       └── proxy_state_manager.update(...)                 │
    │   │     await asyncio.sleep(CONTROL_LOOP_INTERVAL_S ≈ 0.1s)   │
    │   └───────────────────────────────────────────────────────────┘
    │
    └── 2. wait_proxies loop                                      [serve.run.wait_proxies]  🔴 the cliff
            # Same operations as client.wait_for_proxies_serving(), with
            # extra per-proxy progress logging in aurora_serve.py.
            │
            ├── proxy_handles = ray.get(controller.get_proxies.remote())
            │
            ├── for each proxy: serving_refs.append(handle.serving.remote())
            │   # proxy.py line 1404-1406:
            │   #   async def serving(...): return   # no-op on the proxy side;
            │   #                                      completion blocks until
            │   #                                      the proxy's event loop
            │   #                                      picks it up
            │
            └── ray.wait(serving_refs, timeout=HTTP_PROXY_TIMEOUT, num_returns=N)
                # Waits until every proxy's event loop picks up and replies.
                # serving() has no internal readiness check; the delay comes
                # when the proxy event loop is busy running router long-poll
                # callbacks such as update_deployment_targets() below.
```

Key asymmetry to keep in mind when reading the probe data: the
controller RPC submission is fast, but the `serve.run.deploy_apps`
phase also includes polling waits for deployment creation and application
RUNNING. The **control-loop path** is where §6.1 and §6.2 probes fire,
and where the long-poll snapshots originate that trigger the §6
router-side cliff.

### Important repo-specific assumption for the event-loop story

This repo exports `RAY_SERVE_THROUGHPUT_OPTIMIZED=1`
(`src/driver.py`, `src/aurora_rayserver/resources/launch_cluster.sh`). In upstream Ray Serve,
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
→ the initial snapshot, or a later `DEPLOYMENT_TARGETS` broadcast, calls
`update_deployment_targets()`.

### In this repo's config, what shares the proxy's event loop during `wait_proxies`

```
ProxyActor (on each of N nodes, its own Python process) 🟢
│
├── ProxyActor main loop also hosts handle routers
│   because this repo sets RAY_SERVE_THROUGHPUT_OPTIMIZED=1
│   → RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP=0
│   → proxy-created handles use CurrentLoopRouter
│
├── Handle-router LongPollClient (polling asynchronously; completion
│   callbacks hand off to the event loop via `call_soon_threadsafe`):
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
                    └── common.py:681 get_actor_handle():
                        ◄── §6 probe (common.py) records (t, dur_ms)
                        return ray.get_actor(
                            self.actor_name,
                            namespace=SERVE_NAMESPACE)
                            │
                            │ 🌐 Cold path can issue two GCS calls:
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

Keep these units separate:

- **Logical lookup**: one Python `ray.get_actor()` call measured by the
  `common.py` probe.
- **Underlying GCS RPC**: one server-side GCS method invocation counted by
  `RAY_event_stats`. A cold logical lookup can issue both
  `GetNamedActorInfo` and `GetActorInfo`.
- **Router update**: one `AsyncioRouter.update_deployment_targets()`
  callback on one proxy.
- **Per-proxy aggregate**: the sum of all measured logical lookup time
  inside router updates for one proxy process.

### Interlude: how the `ServeController` actor is spawned, and why its event loop is everything

The `.remote()` method calls scattered throughout the call graph above (and
our §6.1 tick probe, and §6.2 dsm probe, and the whole 128n cliff story)
only make sense if you know how the `ServeController` actor gets started
and what runs inside it. The short version: **`ServeController` is a Ray
actor whose single asyncio event loop hosts the control loop and every
inbound RPC handler, including `listen_for_change()`. The LongPollHost
snapshot-update and waiter-wakeup logic runs inside those
controller-side code paths, so all of that work competes for the same
thread.**

#### Actor spawn chain

```
aurora_serve.py driver process (head node, Python process #A)
  serve.run(deployment)                                  [serve/api.py:686]
    _run(...)                                            [serve/api.py:614]
      client = _private_api.serve_start(...)             [serve/api.py:593]
        ▼
      serve_start(...)                                   [_private/api.py:158]
        _start_controller(...)                           [_private/api.py:65-111]
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
   [default_impl.py:225-235](/opt/aurora/26.26.0/frameworks/aurora_frameworks-2025.3.1/lib/python3.12/site-packages/ray/serve/_private/default_impl.py#L225-L235)).
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
([ray/_common/utils.py:103](/opt/aurora/26.26.0/frameworks/aurora_frameworks-2025.3.1/lib/python3.12/site-packages/ray/_common/utils.py#L103))
is a thin wrapper around:

```python
task = get_or_create_event_loop().create_task(coroutine)
_BACKGROUND_TASKS.add(task)   # strong reference so GC doesn't kill it
task.add_done_callback(_BACKGROUND_TASKS.discard)
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
│     └── serve_start() / _start_controller()                           │
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
│   │ RPC handler  │   │ run_control_loop │   │ listen_for_change() │   │
│   │ for          │   │ (background task │   │ / notify_changed()  │   │
│   │ deploy_apps  │   │  created in      │   │ paths that touch    │   │
│   │ get_proxies  │   │  __init__ at     │   │ LongPollHost state  │   │
│   │ list_services│   │  line 230)       │   │                     │   │
│   └───────▲──────┘   └────────▲─────────┘   └──────────▲──────────┘   │
│           │                   │                         │             │
│           │ Ray dispatches    │ awaits                  │ run as      │
│           │ inbound RPCs      │ run_control_loop_step() │ controller  │
│           │ onto this loop    │ every ~0.1s             │ code paths  │
│                                                                       │
│   All of this work shares the SAME event loop. If any one path holds  │
│   the loop (e.g. a slow synchronous stretch inside dsm.update()), the │
│   others are starved.                                                 │
└───────────────────────────────────────────────────────────────────────┘
```

#### Long poll — how state crosses process boundaries

Controller-side state (route table, replica memberships, per-deployment
config, global logging config) reaches the proxies and handle routers
running in other processes through Ray Serve's own pub-sub mechanism
called **long poll**. Every `notify_changed`, every proxy callback, and
every `update_deployment_targets` invocation you see in the §6/§6.2
probes is mediated by this one mechanism.

**Two classes:**

- **`LongPollHost`** ([long_poll.py:228](/opt/aurora/26.26.0/frameworks/aurora_frameworks-2025.3.1/lib/python3.12/site-packages/ray/serve/_private/long_poll.py#L228)) — the publisher. Single instance inside the controller actor (created in [controller.py:147](../../.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray/serve/_private/controller.py#L147)). Holds `object_snapshots: Dict[key, Any]`, `snapshot_ids: Dict[key, int]`, and a set of waiters per key. Methods: `notify_changed({key: new_value})` bumps the snapshot id and wakes waiters; `listen_for_change(snapshot_ids)` is the async RPC subscribers block on.
- **`LongPollClient`** ([long_poll.py:71](/opt/aurora/26.26.0/frameworks/aurora_frameworks-2025.3.1/lib/python3.12/site-packages/ray/serve/_private/long_poll.py#L71)) — the subscriber. Each instance takes a dict of `{key: callback}`, calls `host.listen_for_change.remote(my_snapshot_ids)` in a loop, and invokes the registered callback whenever the corresponding key updates. A proxy process can have more than one instance: the `ProxyActor` creates one for `ROUTE_TABLE` / `GLOBAL_LOGGING_CONFIG`, and router creation adds dedicated/shared clients for `DEPLOYMENT_TARGETS` / `DEPLOYMENT_CONFIG`.

"*A proxy subscribes to `DEPLOYMENT_TARGETS`*" means: its `LongPollClient`
has an entry like `{(DEPLOYMENT_TARGETS, deployment_id): some_callback}`
in its key listeners, so its outstanding `listen_for_change` RPC will
return whenever the controller publishes a new `DeploymentTargetInfo`
for that deployment.

**The four namespaces** ([long_poll.py:42-49](/opt/aurora/26.26.0/frameworks/aurora_frameworks-2025.3.1/lib/python3.12/site-packages/ray/serve/_private/long_poll.py#L42-L49)) are just enum keys:

| Namespace | Payload | Who owns/publishes | Who subscribes |
|---|---|---|---|
| `ROUTE_TABLE` | `Dict[DeploymentID, EndpointInfo]` — ingress deployment → HTTP route | `EndpointState` inside the controller ([endpoint_state.py:48](/opt/aurora/26.26.0/frameworks/aurora_frameworks-2025.3.1/lib/python3.12/site-packages/ray/serve/_private/endpoint_state.py#L48)) | `ProxyActor` ([proxy.py:1178](../../.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray/serve/_private/proxy.py#L1178)) |
| `DEPLOYMENT_TARGETS` | `DeploymentTargetInfo(is_available, running_replicas)` per deployment; key is compound `(DEPLOYMENT_TARGETS, deployment_id)` | `DeploymentState` inside the controller ([deployment_state.py:2252](../../.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray/serve/_private/deployment_state.py#L2252), fired from `dsm.update()`'s s7_broadcast step) | `AsyncioRouter` instances per handle ([router.py:600, 1116](../../.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray/serve/_private/router.py#L600)) |
| `DEPLOYMENT_CONFIG` | `DeploymentConfig` (autoscaling, timeouts) per deployment | same as DEPLOYMENT_TARGETS ([deployment_state.py:2282](../../.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray/serve/_private/deployment_state.py#L2282)) | same `AsyncioRouter` instances ([router.py:604](../../.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray/serve/_private/router.py#L604)) |
| `GLOBAL_LOGGING_CONFIG` | single cluster-wide `LoggingConfig` | `ServeController` directly ([controller.py:253](../../.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray/serve/_private/controller.py#L253)) | `ProxyActor` ([proxy.py:1177](../../.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray/serve/_private/proxy.py#L1177)) |

The long-poll snapshots live **inside the controller actor process**.
Checkpointed target state, endpoint routes, and controller config are
persisted through Ray's GCS-backed `InternalKV` store where those managers
use `kv_store`. The `DEPLOYMENT_TARGETS` payload is different: it is live
running-replica membership derived by `DeploymentStateManager`, then
published through `LongPollHost`. The long-poll layer itself is an
in-memory coordination mechanism.

**Where long-poll state lives physically:**

```
┌────────────────────────────────────────────────────────────────────┐
│ Controller actor (head node, single Python process)                │
│                                                                    │
│   LongPollHost.object_snapshots = {                                │
│     ROUTE_TABLE:                 {d_id: EndpointInfo},             │
│     GLOBAL_LOGGING_CONFIG:       LoggingConfig(...),               │
│     (DEPLOYMENT_TARGETS, d_id):  DeploymentTargetInfo(...),        │
│     (DEPLOYMENT_TARGETS, d_id2): DeploymentTargetInfo(...),        │
│     (DEPLOYMENT_CONFIG,  d_id):  DeploymentConfig(...),            │
│     ...                                                            │
│   }                                                                │
│                                                                    │
│   notify_changed() call sites:                                     │
│   ├── EndpointState        (ROUTE_TABLE)           endpoint_state.py:48 │
│   ├── ServeController      (GLOBAL_LOGGING_CONFIG) controller.py:253    │
│   └── DeploymentState      (DEPLOYMENT_TARGETS,    deployment_state.py:2252 │
│                             DEPLOYMENT_CONFIG)                          │
└────────────────────────────────────────────────────────────────────┘
                           │
                           │ listen_for_change.remote(snapshot_ids)
                           │ async RPC, blocks until a key's snapshot_id bumps
                           ▼
┌────────────────────────────────────────────────────────────────────┐
│ Subscribers (LongPollClient instances in proxy/router processes)   │
│                                                                    │
│ ProxyActor (× N nodes)                                             │
│   ├── ROUTE_TABLE           → _update_routes_in_proxies()          │
│   └── GLOBAL_LOGGING_CONFIG → _update_logging_config()             │
│                                                                    │
│ AsyncioRouter (per DeploymentHandle, created lazily)               │
│   ├── (DEPLOYMENT_TARGETS, d_id) → update_deployment_targets()     │
│   │                                ◄── §6 router probe             │
│   └── (DEPLOYMENT_CONFIG,  d_id) → update_deployment_config()      │
└────────────────────────────────────────────────────────────────────┘
```

**Causal chain into the wait_proxies cliff.** This is the missing piece
that closes the story in §4 "How a proxy becomes subscribed to
DEPLOYMENT_TARGETS" and the `update_deployment_targets` cliff analysis
in §5:

1. A controller tick runs `application_state_manager.update()`, which
   reconciles `_target_state` into deployment state and, for routed
   deployments, calls `endpoint_state.update_endpoint(...)` →
   `notify_changed({ROUTE_TABLE: ...})`. This route-table update is not
   gated on all replicas being healthy.
2. Proxy's `LongPollClient` was blocked on `listen_for_change` with
   `ROUTE_TABLE` in its keys — wakes up → invokes
   `_update_routes_in_proxies(endpoints)`.
3. Inside `_update_routes_in_proxies`, the proxy realizes it has a new
   deployment endpoint → constructs a `DeploymentHandle` → lazy init
   → creates an `AsyncioRouter` (on the proxy's own event loop because
   of `RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP=0`).
4. That new `AsyncioRouter` instantiates its own `LongPollClient`
   subscribed to `(DEPLOYMENT_TARGETS, d_id)`. The initial snapshot id
   on the client side is `-1`; if the host already has a snapshot for
   that deployment, the first `listen_for_change` returns **immediately**
   with the current `DeploymentTargetInfo`. If not, it waits for the
   next `s7_broadcast` update from `dsm.update()`.
5. Callback: `update_deployment_targets(DeploymentTargetInfo)`
   (wrapped by our §6 router probe) → iterates `N_replicas`
   `get_actor_handle` calls → this is the scan that blows up to 110.9s
   max at 128n.

So the DEPLOYMENT_TARGETS subscription is what **triggers** the
event-loop-blocking scan on each proxy. The cliff is not a separate
phenomenon — it's the expected consequence of a newly-initialized router
receiving a replica snapshot through this pub-sub channel.

#### Why this matters for the 128n wait_proxies cliff

Everything we probe in §6.1 and §6.2 comes back to *this* architecture:

- **Controller tick max 4.3 s at 128n (§6.1).** A single `dsm.update()`
  iteration that takes 3–4 s is blocking the asyncio loop for that whole
  window. During that blockage, no inbound controller RPCs get served,
  `listen_for_change()` calls cannot make progress, and the next tick
  can't start.
- **300 s proxy healthcheck timeout warnings at 256n.** The controller
  issues proxy-health checks from `proxy_state_manager.update()` on the
  same controller loop that is spending seconds inside `dsm.update()`
  per tick. The proxy actor, meanwhile, answers `check_health()` on the
  same asyncio loop that is stuck in
  `update_deployment_targets` → `get_actor_handle × N_replicas`. So the
  health-check path spans two stressed single-threaded event loops.
- **Per-call `update_deployment_targets` max 110.9 s at 128n (§6).** The
  proxy's asyncio loop also hosts the handle-router LongPollClient
  callback (because this repo exports `RAY_SERVE_THROUGHPUT_OPTIMIZED=1`,
  which flips `RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP=0` — see §4.) So
  the N_replicas `get_actor_handle` lookups happen on the same loop that
  answers `serving()` and `check_health()`. If `serving()` is queued
  behind that callback, the proxy cannot ack readiness until the full
  iteration completes.
- **`s7_broadcast` (dsm's notify_changed snapshot update) scaling from 3 ms
  (32n) to 514 ms (128n) (§6.2).** That's the controller updating
  `DEPLOYMENT_TARGETS` / `DEPLOYMENT_CONFIG` snapshots and waking current
  long-poll waiters through code that serializes on its single event loop.

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

1. `controller_impl.remote(...)` at [api.py:94](/opt/aurora/26.26.0/frameworks/aurora_frameworks-2025.3.1/lib/python3.12/site-packages/ray/serve/_private/api.py#L94) — spawns the controller **process** (Ray raylet side).
2. `run_background_task(self.run_control_loop())` at [controller.py:230](../../.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray/serve/_private/controller.py#L230) — schedules the control loop **coroutine** onto the process's existing event loop.

## 5. Mapping the timing numbers to code lines (clean, run21)

| Measurement | Probe file:line | Scope | 32n | 64n | 128n |
|---|---|---:|---:|---:|---:|
| **proxy.py / init substeps** | | | | | |
| super_init | proxy.py:1172 | local (`super().__init__`) | ~1ms | ~1ms | ~1ms |
| long_poll_client | proxy.py:1187 | one `ray.get_actor` named-actor lookup to the controller | ~22ms | ~25ms | ~49ms |
| create_proxies | proxy.py:1244 | ProxyRouter + HTTPProxy + GRPCProxy construction | ~13ms | ~21ms | ~57ms |
| server_tasks_and_gc | proxy.py:1274 | event_loop.create_task + GC config | ~329ms | ~332ms | ~332ms |
| total `__init__` | — | everything above | 0.37s | 0.38s | 0.44s |
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
| **s7_broadcast max** | ditto | LongPollHost snapshot update / waiter wakeup | 3ms | **477ms** | **514ms** |

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

Median is flat 44 μs because many lookups still hit the actor-handle
cache fast path. The mean is much larger because it includes a heavier
tail of calls that round-trip through GCS (`GetNamedActorInfo` +
`GetActorInfo`, with the read pool pegged at ~85–90 ms peak queueing
from 128n onward, per §5).

**Multiplying the two factors against the measured global mean:**

| Scale | N_replicas × global mean | Observed max | What this implies |
|---|---:|---:|---:|
| 64n | 640 × 30.7 ms = **19.6 s** | **7.7 s** | worst update below the average-cost bound |
| 128n | 1280 × 82 ms = **105 s** | **110.9 s** | worst update close to average-cost bound |

At 64n, the observed max is well below `N_replicas × global mean`. At
128n, the observed max is close to that product: the worst update had
aggregate lookup cost similar to applying the global per-call mean across
the whole replica scan.

**Interpretation, not directly proven from Python source.** The
measurements are consistent with much lower effective actor-handle cache
reuse, or much worse slow-path queuing, in the worst 128n update than in
the worst 64n update. A plausible explanation is that more replicas, more
concurrent broadcasts, and more startup churn reduce temporal locality
enough that many more lookups fall through to GCS. But the exact internal
cache behavior is an inference from timings and counters here, not
something this walkthrough proves from inspected Ray source alone.

Compound factor between 64n and 128n:

- **2×** (replicas) ×
- **2.7×** (mean per-call) ×
- **~2.5×** (worst-update aggregate cost moving from below the global
  average-cost bound to roughly matching it)
- ≈ **~14× cliff** → 7.7 s → 110.9 s, matching the measurement.

This is the signature of a linear scan with per-call latency that is
not O(1). The architectural fix — ship pre-resolved actor handles in
the broadcast payload — would keep the router's replica-state rebuild but
remove the per-replica named-actor/GCS lookup.

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

Our [parse_gcs_event_stats.py](../eval/tools/parse_gcs_event_stats.py) parses
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
| proxy.py substeps (§orig) | Proxy `__init__` time broken into 7 sub-phases | Where does proxy init spend time? (answer: mostly `server_tasks_and_gc`, ~332ms mean / 360ms median at 128n run21) |
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
                            long-poll update event
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
python3 eval/tools/analyze_scaling.py runN/{32,64,128,256}-nodes

# §6 probes — per-proxy GCS calls + update_deployment_targets breakdown
python3 eval/tools/analyze_probes.py runN/{32,64,128,256}-nodes

# §6.1 controller tick timing
python3 eval/tools/analyze_controller_ticks.py runN/{32,64,128,256}-nodes

# §6.2 dsm.update() step breakdown
python3 eval/tools/analyze_dsm.py runN/{32,64,128,256}-nodes

# Raw GCS event stats
python3 eval/tools/parse_gcs_event_stats.py runN/256-nodes/.../gcs_server.out
```

## 10. File map and references

### Overlay (`~/.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray`)
- `serve/_private/proxy.py` — init substeps + ready timing
- `serve/_private/common.py` — buffered get_actor_handle probe + module-level `_aurora_probe_record` helpers
- `serve/_private/router.py` — update_deployment_targets JSONL
- `serve/_private/controller.py` — run_control_loop_step JSONL
- `serve/_private/deployment_state.py` — dsm.update() JSONL
- `serve/_private/constants.py` — timeout patches
- `serve/_private/client.py` — currently present in the overlay directory and therefore copied by `launch_cluster.sh` if left there, even though it is not part of the tracked seven-file patch set
- `.gitignore`

### Main repo (`perf-inst-dev`)
- `src/aurora_rayserver/resources/launch_cluster.sh` — overlay build + symlink tree + env vars
- `src/aurora_serve.py` — `_collect_instrumentation_all` Ray-remote gather + `_collect_proxy_profiles` legacy summary
- `eval/tools/parse_gcs_event_stats.py` — GCS server-side event parser
- `eval/tools/analyze_scaling.py` — cross-scale table
- `eval/tools/analyze_probes.py` — §6 probes (get_actor + router)
- `eval/tools/analyze_controller_ticks.py` — §6.1 ticks
- `eval/tools/analyze_dsm.py` — §6.2 dsm sub-phases
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
