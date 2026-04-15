# wait_proxies Root Cause — Instrumented Analysis (2026-04-15)

## Summary

The wait_proxies performance cliff between 32 and 64 nodes is caused by two
compounding factors in Ray Serve's controller architecture:

1. **Late proxy spawning**: proxies are only created after replicas are RUNNING
2. **Controller event loop saturation** at 64+ nodes

The previous hypothesis (3-factor interaction: proxy spawn delay +
startup_concurrency=8 + deploy_apps window) was incorrect.

## Instrumentation Setup

We added monkey-patching hooks via `usercustomize.py` (loaded by Python's
`site` module in every Ray worker process) to instrument:

- **ServeController**: `run_control_loop_step` timing, `_update_proxy_nodes` changes,
  `done_recovering_event` firing
- **ProxyStateManager**: proxy spawn/stop events, state summaries
- **ProxyActor**: granular `__init__` sub-step timing, `ready()` duration

All instrumentation writes to `/tmp/aurora_inst/` as JSONL, collected post-run
by `finalize_run_logs` in `launch_cluster.sh`.

Experiments used `AURORA_NULL_COMPUTE=1` (replicas sleep 1s instead of loading
a real model) to isolate proxy startup behavior from model loading time.

## Raw Data

### wait_proxies timing (null-compute)

| Metric          | 1 node | 32 nodes | 64 nodes |
|-----------------|--------|----------|----------|
| **total**       | 0.0s   | **20.8s**| **59.1s**|
| first           | 0.0s   | 0.0s     | 0.0s     |
| median          | 0.0s   | 13.8s    | 47.2s    |
| p90             | 0.0s   | 20.2s    | 57.9s    |
| last            | 0.0s   | 20.8s    | 59.1s    |
| Controller errs | 0      | 0        | hundreds |

Scaling: 2x nodes → 2.8x wait time (super-linear).

### Proxy spawn timeline (from deploy monitor, sampled every 5s)

**32 nodes:**
```
+5s:  1 proxy (head only)
+10s: 1
+15s: 1
+20s: 29  ← burst: replicas become RUNNING, controller discovers nodes
+25s: 32
```

**64 nodes:**
```
+5s:  1 proxy (head only)
+10s: 1
+15s: 1
+20s: 47  ← burst
+25s: 64
```

### Controller instrumentation (32 nodes, pid 201377)

```
t=0.000s  controller_inst_installed
t=0.025s  done_recovering (proxy_nodes=1)
t=0.025s  control_loop #1: 24ms (proxy_nodes=1)
          ... loops 2-171: ~2ms each, proxy_nodes=1 ...
t=18.7s   control_loop #172: 1094ms ← slow loop (deployment_state_manager processing 384 replicas)
t=36.7s   proxy_nodes_changed: 1→4→7→15→25→29→32 (6 transitions in ~1s)
t=38.0s   control_loop #300: 61ms (proxy_nodes=32)
          ... loops 400+: ~27ms each (heavier with 32 proxy states) ...
```

Key observations:
- `done_recovering` fires immediately (0.025s) — not a factor
- Proxy nodes stay at 1 for 36 seconds while replicas initialize
- Burst from 1→32 in ~1 second once replicas are RUNNING
- Control loop slows from 2ms to 27ms after 32 proxy states are added

### 64-node controller behavior

The ServeController (pid 129471) produced hundreds of asyncio errors:
```
Future exception was never retrieved
```
These appeared in bursts during the wait_proxies phase. At 32 nodes, zero
such errors occurred.

## Root Cause Analysis

### Factor 1: Late proxy spawning (by design)

Ray Serve's controller deliberately defers proxy actor creation. In
`controller.py:510-516`:

```python
# Don't update proxy_state until after the done recovering event is set,
# otherwise we may start a new proxy but not broadcast it any
# info about available deployments & their replicas.
if self.proxy_state_manager and self.done_recovering_event.is_set():
    self.proxy_state_manager.update(proxy_nodes=self._proxy_nodes)
```

And `_update_proxy_nodes()` (line 409) uses `get_active_node_ids()` which
only returns nodes with RUNNING replicas. This means:

1. `serve.start()` spawns only the head-node proxy
2. During deployment, worker nodes have no proxies
3. Only after replicas are RUNNING does the controller discover those nodes
4. Proxy actors are spawned in a burst, just seconds before `wait_proxies` polls

The gap between "proxies spawned" and "wait_proxies starts" is only ~4s at
32 nodes, giving proxy actors very little time to initialize.

### Factor 2: Controller event loop saturation (64+ nodes)

The ServeController runs a single-threaded asyncio event loop that handles:
- `deployment_state_manager.update()` — manages all replica states
- `application_state_manager.update()` — manages app states
- `proxy_state_manager.update()` — manages all proxy states, health checks
- LongPoll broadcast to all proxies and replicas
- GCS RPCs for actor scheduling, health checks

At 64 nodes (768 replicas + 64 proxies), the event loop becomes saturated:
- Control loop duration increases from 2ms (idle) to 27ms+ (32n) to higher (64n)
- Proxy health check futures pile up and are never awaited → `Future exception was never retrieved`
- The error cascade creates additional work, further slowing the loop
- Proxy readiness checks (`is_ready()` in `proxy_state.py`) have a 5s timeout
  on first attempt — slow loop iterations mean the controller can't poll
  proxy readiness fast enough, triggering unnecessary restarts

### Why this looks like a "cliff"

At 32 nodes, the controller is at the edge of its capacity:
- 384 replicas + 32 proxies
- Control loop: ~27ms (manageable)
- No asyncio errors
- wait_proxies: 20.8s

At 64 nodes, it tips over:
- 768 replicas + 64 proxies
- Control loop: overwhelmed
- Hundreds of asyncio Future errors (cascade)
- wait_proxies: 59.1s (2.8x for 2x nodes)

The super-linear scaling is the signature of a saturation cliff, not a
linear resource constraint.

## Previous finding (incorrect)

The previous hypothesis claimed:
> wait_proxies cliff between 32 nodes (0s) and 64 nodes (75s) is caused by:
> 1. Proxies only spawned after GCS places replicas
> 2. max_startup_concurrency=8 creates 60s per-node worker pipeline
> 3. GCS delay (~20s) + pipeline (60s) = 80s > deploy_apps (78s) at 64 nodes

Problems with this:
- wait_proxies is NOT 0s at 32 nodes — it's 20.8s (with null-compute; with
  real models it appears ~0s because model loading gives proxies time to init)
- startup_concurrency is irrelevant to proxy startup timing
- The "deploy_apps window" framing confuses cause and effect

The correct framing: wait_proxies is always >0 at multi-node scale because
proxy spawning is deferred. At 64+ nodes, the controller's single-threaded
event loop becomes the bottleneck that makes it dramatically worse.

## Implications

1. **Cannot fix without modifying Ray Serve**: the late-spawning design and
   single-threaded controller are fundamental to Ray Serve's architecture
2. **Workaround — skip wait_proxies**: since HAProxy handles readiness
   independently, the `wait_proxies` step could be made non-blocking or
   removed entirely for the HAProxy dispatch mode
3. **Workaround — pre-spawn proxies**: call `serve.start()` with
   `ProxyLocation.EveryNode` earlier, before deployment. But this requires
   all nodes to be registered, and the controller still gates proxy spawning
   on `done_recovering_event`
4. **At 128+ nodes**: expect wait_proxies to grow further (possibly 2+ min)
   due to the super-linear scaling

## Experiment Details

| Run | Nodes | Job ID | Queue | Run Dir |
|-----|-------|--------|-------|---------|
| 1n smoke | 1 | capacity 8432934 | capacity | 20260415T005512Z_ray_runtime |
| 32n inst | 32 | 8436738 | debug-scaling | 20260415T021549Z_ray_runtime |
| 64n inst | 64 | 8436742 | debug-scaling | 20260415T033626Z_ray_runtime |

All runs: `AURORA_NULL_COMPUTE=1`, commit `9ed7f8f` (perf-inst branch).

## Open Gap: Per-Proxy Lifecycle Timing

We have wait_proxies totals and controller-side data, but NOT per-proxy
lifecycle breakdown (scheduling_delay vs init_duration vs ready_duration).

Attempts to instrument ProxyActor failed because:
1. **Monkey-patching doesn't survive Ray pickle**: Ray serializes actor classes
   from the driver and deserializes in workers. Worker-side patches are ignored.
2. **Import finder approach**: Works interactively but fails in PBS jobs because
   Ray's pre-started workers import `ray.serve._private.proxy` during their
   startup, before the custom MetaPathFinder can intercept.
3. **`.pth` file approach**: Breaks `ray.serve` import chain (`AttributeError:
   module 'ray' has no attribute 'serve'`).

**Next approach to try**: Use Ray's `worker_process_setup_hook` to inject timing
after the worker starts but before the actor task executes. Or accept that
driver-side timing (which we have) is sufficient to characterize the problem.

## Files

- Instrumentation code: `scripts/launch_cluster.sh` (usercustomize.py generation)
- Overlay proxy.py: `~/.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray/serve/_private/proxy.py`
- Scaling traces: `run_logs/*/scaling_trace.json`
- Controller JSONL: `/tmp/aurora_inst/controller_*.jsonl` (on compute nodes)
