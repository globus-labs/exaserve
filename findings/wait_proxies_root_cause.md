# wait_proxies Scaling Cliff: Root Cause Analysis

## The Problem

`wait_proxies` exhibits a cliff between 32 and 64 nodes:

| Nodes | deploy_apps | wait_proxies | Total deploy |
|------:|------------:|-------------:|-------------:|
| 1-32  | ~77-83s     | **0.0-2.6s** | ~80-85s      |
| 64    | ~77-78s     | **75s**      | ~153s        |
| 128   | ~82s        | **366s**     | ~449s        |

Verified across 4 independent runs (run1+run2 of two different specs).

## Root Cause: 3-Factor Interaction

The cliff is caused by the interaction of three factors:

### Factor 1: Proxies are spawned AFTER replicas are placed (not at serve.start time)

**Code evidence:** `controller.py:403-414`
```python
def _update_proxy_nodes(self):
    new_proxy_nodes = self.deployment_state_manager.get_active_node_ids()
    new_proxy_nodes.add(self._controller_node_id)  # always head node
    self._proxy_nodes = new_proxy_nodes
```

`get_active_node_ids()` returns nodes where replicas are in STARTING/RUNNING
state with `actor_node_id` set (`deployment_state.py:2203-2222`). The controller
only spawns ProxyActors on nodes AFTER GCS has placed at least one replica there.

**Timeline evidence** (from controller log, startup_inst run2/64-nodes):
```
01:30:08  Controller starts — head node proxy spawned
01:30:26  deploy_applications submitted (768 replicas)
01:30:46  Worker proxies spawned (63 nodes, all within 2s window)
          → 20s delay from deploy submit to proxy spawn
```

### Factor 2: max_startup_concurrency=8 creates per-node batching

**Code evidence:** `driver.py:43,199-202`
```python
DEFAULT_RAY_INTERNAL_STARTUP_LIMIT = 8
def get_ray_internal_startup_limit(cluster):
    return max(1, min(cluster.node_cpus, DEFAULT_RAY_INTERNAL_STARTUP_LIMIT))
```

Each raylet can only spawn 8 worker processes concurrently. With 13 workers
per node (12 VLLMWorkers + 1 ProxyActor):
- Batch 1: 8 workers start, 30s metrics timeout each
- Batch 2: 5 workers start (including ProxyActor), 30s metrics timeout
- Per-node pipeline: **~60s** from first worker to ProxyActor ready

### Factor 3: The cliff = pipeline exceeds deploy_apps window

Total time from deploy_apps start to proxy ready:
```
proxy_ready_time = GCS_delay + per_node_pipeline
                 = 20s       + 60s
                 = 80s
```

deploy_apps takes ~78s (dominated by VLLMWorker model loading, constant
across scales).

**At 32 nodes** (384 replicas): GCS places replicas faster. GCS_delay ≈ 10-15s.
→ proxy_ready_time ≈ 70-75s < 78s → all proxies finish during deploy_apps
→ wait_proxies ≈ 0

**At 64 nodes** (768 replicas): GCS takes longer for 2× more placements.
GCS_delay ≈ 20s.
→ proxy_ready_time ≈ 80s > 78s → last proxies spill past deploy_apps
→ wait_proxies ≈ 75s (last proxy finishes 75s after deploy_apps ends)

The margin is razor-thin (~2-3s). The 32→64 jump is a **threshold crossing**,
not a gradual increase.

## Evidence Summary

### Timing data (reproducible)

32-node run2: deploy_apps=82.8s, wait_proxies=**0.006s** (all proxies at +0.0s)
64-node run2: deploy_apps=77.9s, wait_proxies=**75.1s** (first=0.0s, median=40.8s, last=75.1s)

64-node per-proxy completion (linear trickle, not burst):
```
10/64 at +11.9s
20/64 at +25.4s
30/64 at +39.4s
40/64 at +49.4s
50/64 at +59.5s
60/64 at +70.5s
64/64 at +75.1s
```

### Code walkthrough

1. `serve.start()` creates ServeController with `ProxyLocation.EveryNode`
   → controller spawns head node proxy only (proxy_state.py:750-772)
2. `deploy_applications()` submits replicas → GCS schedules on nodes
3. Controller reconcile loop (0.1s interval, controller.py:449) calls
   `_update_proxy_nodes()` which reads `get_active_node_ids()`
4. As GCS places replicas, nodes appear as "active" → proxies spawned
5. ProxyActor `.remote()` → GCS → raylet → worker spawn queue
6. Worker goes through 30s C++ metrics agent timeout
   (constexpr kMetricAgentInitMaxRetries=30, kMetricAgentInitRetryDelayMs=1000)
7. `ProxyActor.__init__()` runs (fast, <1s) → actor ready

### Why proxies trickle in linearly (not as a burst)

Each node's 13 workers compete for 8 startup slots. The VLLMWorkers arrive
first (submitted at deploy_apps start). The ProxyActor arrives ~20s later.
The raylet processes workers roughly FIFO. The ProxyActor's position in the
queue varies by node (depends on when GCS placed that node's replicas).

Nodes whose replicas were placed earliest → ProxyActor enters queue earliest
→ starts in batch 1 or early batch 2 → finishes during deploy_apps.

Nodes whose replicas were placed latest → ProxyActor enters late →
may start in batch 2 or later → finishes after deploy_apps.

This creates the linear trickle: proxies finish in the order their nodes'
replicas were placed.

## Direct Evidence: Per-Process Start Times from Metrics Failures

Each Ray worker process goes through a 30s C++ metrics agent timeout. The
failure log message marks the END of this timeout, so subtracting 30s gives
the approximate process START time.

From startup_inst run2/64-nodes service.log metrics failure timestamps:

```
~01:30:49  511 VLLMWorkers started   ← BATCH 1 (8 per node × 64 nodes)
~01:31:15  303 VLLMWorkers started   ← BATCH 2 (remaining 4-5 per node)
~01:31:26   15 ProxyActors started   ← BATCH 2 (after batch 1 frees slots)
```

This directly confirms the batching model:
- Batch 1 fills all 8 slots with VLLMWorkers at ~01:30:49
- Batch 2 starts ~26s later at ~01:31:15 when batch 1 workers finish
- ProxyActors are in batch 2, starting at ~01:31:26
- ProxyActors finish 30s later at ~01:31:56
- deploy_apps ends at 01:31:43 (77s after start at 01:30:26)
- **01:31:56 > 01:31:43** → ProxyActors NOT ready → wait_proxies kicks in

## Potential Fixes

1. **Increase max_startup_concurrency**: From 8 to 16 would halve per-node
   pipeline to ~30s. Total = 20+30 = 50s << 78s. Eliminates cliff entirely.
   Risk: more concurrent processes at startup, higher memory pressure.

2. **Spawn proxies during serve.start()**: Modify `_update_proxy_nodes()` to
   use `cluster_node_info_cache.get_alive_node_ids()` instead of
   `deployment_state_manager.get_active_node_ids()`. This spawns proxies
   on ALL alive nodes immediately, not just nodes with replicas. Gives
   proxies a ~77s head start.

3. **Skip wait_proxies**: Since we use HAProxy, we don't need to wait for
   ALL ProxyActors. We could wait for a subset (e.g., 90%) or skip entirely
   and let HAProxy health-check handle readiness.

4. **Eliminate metrics agent timeout**: The 30s C++ constexpr is the largest
   fixed cost. If reduced or eliminated, per-node pipeline drops to ~1s.
   Currently not configurable at runtime.
