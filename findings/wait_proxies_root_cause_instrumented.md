# wait_proxies Root Cause — Instrumented Analysis (2026-04-15, updated 2026-04-16)

## Summary

The wait_proxies performance cliff between 32 and 64 nodes is caused by two
compounding factors:

1. **Late proxy spawning** (~39s delay): proxies are only created after replicas
   are RUNNING, adding a fixed ~39s scheduling delay at any multi-node scale
2. **Proxy restarts at 64+ nodes**: the controller's saturated event loop fails
   to poll proxy health in time, marking healthy proxies as unhealthy and
   restarting them — adding another ~70-80s per restart cycle

From 64 through 256 nodes, wait_proxies plateaus at ~59.5s. The restart cycle
adds a fixed ~70-80s penalty regardless of node count, suggesting the bottleneck
is the controller's health check timeout (a constant), not a function of cluster
size. At 256 nodes, `deploy_apps` grows to 51.8s and proxy `__init__` slows
(up to 0.96s due to GCS contention in `create_proxies` and `long_poll_client`
substeps), but wait_proxies itself does not change.

Previously observed 300s wait_proxies at 256 nodes was not reproduced under
null-compute. That longer time may require real model loading to trigger
(different deploy_apps timing, Lustre contention, or multiple restart cycles).

The previous hypothesis (3-factor interaction: proxy spawn delay +
startup_concurrency=8 + deploy_apps window) was incorrect.

## Evidence Chain

### Per-proxy lifecycle data (PYTHONPATH overlay instrumentation)

Instrumented `ProxyActor.__init__` and `ready()` directly in a copy of
`ray/serve/_private/proxy.py`, loaded via PYTHONPATH symlink overlay
(`/tmp/ray_overlay`). Each proxy writes a JSON profile to
`/tmp/aurora_inst/proxy_init_{hostname}_{pid}.json`.

**32 nodes** (7 sampled proxies, 0 restarts):

| Host | Init dur | Ready dur | Scheduling delay |
|------|----------|-----------|------------------|
| head (x4220c0s3b0n0) | 0.225s | 0.0s | 0.0s |
| worker min | 0.270s | 0.0s | 38.1s |
| worker max | 0.509s | 0.0s | 39.2s |

**64 nodes** (14 proxy_init files, 8 unique hosts, **6 hosts restarted**):

| Host | Attempt | Init dur | Ready dur | Scheduling delay |
|------|---------|----------|-----------|------------------|
| head (x4203c3s5b0n0) | 1 | 0.384s | 0.0s | 0.0s |
| workers (first batch) | 1 | 0.27-0.50s | 0.0s | 38.9-40.0s |
| **workers (restarts)** | **2** | 0.42-0.48s | 0.0s | **107.9-119.7s** |

**128 nodes** (14 proxy_init files, 7 unique hosts, **7 hosts restarted** — all sampled):

| Host | Attempt | Init dur | Ready dur | Scheduling delay |
|------|---------|----------|-----------|------------------|
| head (x4614c1s0b0n0) | 1 | 0.364s | 0.0s | 0.0s |
| workers (first batch) | 1 | 0.35-0.63s | 0.0s | 39.2-40.8s |
| **workers (restarts)** | **2** | 0.27-0.59s | 0.0s | **109.2-121.7s** |

**256 nodes** (13 proxy_init files, 7 unique hosts, **6 hosts restarted**):

| Host | Attempt | Init dur | Ready dur | Scheduling delay |
|------|---------|----------|-----------|------------------|
| head (x4518c7s2b0n0) | 1 | 0.423s | 0.0s | 0.0s |
| workers (first batch) | 1 | 0.37-0.96s | 0.0s | 40.8-43.4s |
| **workers (restarts)** | **2** | 0.41-0.73s | 0.0s | **125.5-130.6s** |

Key observations:
- `__init__` is fast at all scales: 0.2-0.6s at 32-128n, up to 0.96s at 256n
- `ready()` is instant: 0.0s — HTTP server task completes before first poll
- The ~39-43s scheduling delay is consistent across all scales (slightly longer at 256n)
- At 64n, 6/7 restarted; at 128n, 7/7; at 256n, 6/7
- Restart gap: ~70-80s between first and second attempt (consistent at all scales)
- GCS contention visible at 256n: `long_poll_client` up to 0.14s, `create_proxies` up to 0.45s

### Where wait_proxies time is spent

**32 nodes (wait_proxies = 21.6s):**
```
  0s        38s    39s                   60s
  |---------|------|                      |
  ^head     ^workers spawned              ^wait_proxies
  proxy     (replicas RUNNING)            ends
            init: 0.3-0.5s
            ready: 0.0s

  wait_proxies starts at ~39s after deployment begins.
  All 32 proxies complete init within ~1s of spawning.
  wait_proxies = time from "start polling" to "last proxy ready"
               = ~21.6s (mostly the gap between head proxy and worker batch)
```

**64 nodes (wait_proxies = 59.4s):**
```
  0s        39s    40s         108s   120s
  |---------|------|-----------|------|
  ^head     ^workers spawned   ^restarts
  proxy     init: 0.3-0.5s    init: 0.4-0.5s
            (health check     (new PIDs, same hosts)
             timeout+restart)

  wait_proxies = 59.4s because proxy restarts add ~80s to the timeline.
  6 of 7 sampled worker proxies restarted. The last restart completes at ~120s.
```

### Controller error cascade (64+ nodes)

| Metric | 32 nodes | 64 nodes | 128 nodes | 256 nodes |
|--------|----------|----------|-----------|-----------|
| `Future exception was never retrieved` | 0 | 396 | 381 | 765 |
| `ActorDiedError` in futures | 0 | present | present | present |
| Proxy restarts (sampled) | 0/7 | 6/7 | 7/7 | 6/7 |
| Proxy restarts (from logs) | 0 | ~30+ | ~112 | ~172 |

The `ActorDiedError` futures indicate the controller lost track of proxy actors.
This is caused by the controller's single-threaded asyncio event loop being
unable to process health check responses fast enough at 64+ node scale.

Future error count scales roughly with proxy count (381→765 from 128→256n),
but wait_proxies remains constant — the errors don't cause additional restart
cycles within the measurement window.

### wait_proxies timing (null-compute, reproduced)

| Metric          | 1 node | 32 nodes | 64 nodes | 128 nodes | 256 nodes |
|-----------------|--------|----------|----------|-----------|-----------|
| **total**       | 0.0s   | **21.6s**| **59.4s**| **59.7s** | **59.5s** |
| first           | 0.0s   | 0.0s     | 0.0s     | 0.0s      | 0.0s      |
| median          | 0.0s   | 13.8s    | 47.8s    | 58.4s     | 57.0s     |
| p90             | 0.0s   | 20.5s    | 59.4s    | 59.2s     | 58.2s     |
| last            | 0.0s   | 21.6s    | 59.4s    | 59.7s     | 59.5s     |

### Scaling trace comparison

| Phase | 32 nodes | 64 nodes | 128 nodes | 256 nodes |
|-------|----------|----------|-----------|-----------|
| ray.init | 25.8s | 23.5s | 25.1s | 25.2s |
| serve.start | 34.7s | 34.3s | 35.8s | 36.1s |
| deploy_apps | 40.6s | 42.5s | 44.1s | 51.8s |
| **wait_proxies** | **21.6s** | **59.4s** | **59.7s** | **59.5s** |
| Total | 141.7s | 181.4s | 186.4s | 195.3s |

`deploy_apps` scales roughly linearly (40.6→42.5→44.1→51.8s). `wait_proxies`
has a cliff at 64 nodes (21.6→59.4s) then **plateaus at ~59.5s** through 256
nodes. The plateau happens because the restart penalty is dominated by a fixed
health check timeout constant (~70s), not by the number of proxies.

## Root Cause Analysis

### Factor 1: Late proxy spawning (by design, ~38s delay)

Ray Serve's controller deliberately defers proxy actor creation. In
`controller.py:510-516`:

```python
# Don't update proxy_state until after the done recovering event is set,
# otherwise we may start a new proxy but not broadcast it any
# info about available deployments & their replicas.
if self.proxy_state_manager and self.done_recovering_event.is_set():
    self.proxy_state_manager.update(proxy_nodes=self._proxy_nodes)
```

`_update_proxy_nodes()` (line 409) uses `get_active_node_ids()` which only
returns nodes with RUNNING replicas. This creates a fixed ~38s delay between
deployment start and worker proxy spawning (time for null-compute replicas to
become RUNNING). This delay is consistent across 32 and 64 nodes.

### Factor 2: Proxy restarts from health check timeout (64+ nodes only)

The controller runs a single-threaded asyncio event loop. At 64 nodes
(768 replicas + 64 proxies), the event loop is saturated:

1. Controller spawns 64 proxy actors in a burst at ~39s
2. Proxies init successfully (0.3-0.5s) and are ready
3. Controller's event loop can't poll `is_ready()` fast enough
4. Health check futures pile up → `Future exception was never retrieved` (396x)
5. Proxy health check timeout (5s on first attempt) fires
6. Controller marks proxies as unhealthy → `ActorDiedError`
7. Controller kills and respawns proxy actors
8. Second attempt succeeds (controller has fewer pending futures by then)

The restart cycle adds ~70-80s. Combined with the 38s spawn delay, the total
wait_proxies at 64 nodes is ~120s from deployment start.

At 32 nodes (384 replicas + 32 proxies), the event loop handles the load
without errors: zero Future exceptions, zero restarts, 21.6s wait_proxies.

### Why this looks like a "cliff"

At 32 nodes, the controller is at the edge of capacity but can still complete
all health checks within the timeout. At 64 nodes, the event loop tips over
into a cascade: missed health checks → restart → more work → more missed
checks. The transition is sharp because the health check timeout is a
binary pass/fail threshold.

## Implications

1. **Cannot fix without modifying Ray Serve**: the late-spawning design and
   single-threaded controller are fundamental to Ray Serve's architecture
2. **Workaround — skip wait_proxies**: since HAProxy handles readiness
   independently, the `wait_proxies` step could be made non-blocking or
   removed entirely for the HAProxy dispatch mode
3. **Workaround — increase health check timeouts**: our patched
   `PROXY_HEALTH_CHECK_TIMEOUT_S=300` and `UNHEALTHY_THRESHOLD=100` should
   prevent restarts, but the controller's event loop saturation may still
   cause delays in recognizing proxy readiness
4. **128-node behavior**: wait_proxies plateaus at ~60s (same as 64n). The
   cliff is a one-time step from "no restarts" (32n) to "one restart cycle"
   (64n+), not a continuous degradation. However, at 128n the median proxy
   completion time (58.4s) is much closer to the tail (59.7s) compared to
   64n (median=47.8s, tail=59.4s) — meaning the straggler effect worsens

## Instrumentation Method

### PYTHONPATH symlink overlay

Since the system Ray installation is read-only, we use a symlink overlay:

1. Copy `ray/serve/_private/proxy.py` to `~/.local/aurora/.../ray/serve/_private/proxy.py`
2. Add inline instrumentation to `ProxyActor.__init__` and `ready()`
3. At launch, `scripts/launch_cluster.sh` creates `/tmp/ray_overlay` on each
   node with symlinks to the system ray package, replacing only proxy.py
4. `PYTHONPATH=/tmp/ray_overlay:$PYTHONPATH` makes Python find our version first

### usercustomize.py hooks

Controller and ProxyStateManager instrumentation via `usercustomize.py`
(loaded by Python's `site` module in every worker process):
- Controller: `run_control_loop_step` timing, `_update_proxy_nodes` changes
- ProxyStateManager: proxy spawn/stop events, state summaries

### Data collection

`finalize_run_logs` in `launch_cluster.sh` collects `/tmp/aurora_inst/` from
all nodes via SCP into the run log directory.

## Previous Finding (incorrect)

The 3-factor hypothesis was disproven:
- `startup_concurrency` is irrelevant to proxy timing
- wait_proxies is NOT 0s at 32 nodes (it's 21.6s with null-compute)
- The "deploy_apps window" framing confused cause and effect

## Experiment Details

| Run | Nodes | Job ID | Queue | Run Dir |
|-----|-------|--------|-------|---------|
| 1n smoke | 1 | capacity 8432934 | capacity | 20260415T210345Z_inst_2node_manifest |
| 32n inst | 32 | 8437845 | debug-scaling | weakscaling_nullcompute_proxy/run2/32-nodes |
| 64n inst | 64 | 8437850 | debug-scaling | weakscaling_nullcompute_proxy/run2/64-nodes |
| 128n inst | 128 | 8438789 | debug-scaling | weakscaling_nullcompute_proxy/run3/128-nodes |
| 256n inst | 256 | 8438882 | small (prod) | weakscaling_nullcompute_proxy/run4/256-nodes |

All runs: `AURORA_NULL_COMPUTE=1`, PYTHONPATH overlay with instrumented proxy.py.
All use commit `36c9066` (perf-inst branch) via snapshot.

## Files

- Overlay proxy.py: `~/.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray/serve/_private/proxy.py`
- Overlay setup: `scripts/launch_cluster.sh` (Ray overlay section + usercustomize.py)
- 32n proxy data: `weakscaling_nullcompute_proxy/run2/32-nodes/logs/backend/20260415T211330Z_ray_runtime/instrumentation/*/proxy_init_*.json`
- 64n proxy data: `weakscaling_nullcompute_proxy/run2/64-nodes/logs/backend/20260415T212219Z_ray_runtime/instrumentation/*/proxy_init_*.json`
- 128n proxy data: `weakscaling_nullcompute_proxy/run3/128-nodes/logs/backend/20260416T172344Z_ray_runtime/instrumentation/*/proxy_init_*.json`
- 256n proxy data: `weakscaling_nullcompute_proxy/run4/256-nodes/logs/backend/20260416T185228Z_ray_runtime/instrumentation/*/proxy_init_*.json`
- Scaling traces: `*/scaling_trace.json`
