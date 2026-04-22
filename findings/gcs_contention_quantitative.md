# GCS Contention at Scale — Quantitative Measurement (32/64/128/256 nodes)

**Branch:** perf-inst-dev (commit 757809e + overlay 4dce240)
**Status:** 32n/64n/128n complete; 256n running.

## Goal

Quantify the GCS-contention hypothesis from
[proxyactor_death_cascade_256n.md](proxyactor_death_cascade_256n.md) using
Ray's built-in `RAY_event_stats=1` telemetry. Specifically answer:

1. **To what extent** is GCS the bottleneck for the wait_proxies cliff
   (21.8s → 93.6s at 32n→64n)?
2. Does GCS queueing time scale with node count?
3. Which specific GCS RPC methods dominate queueing time, and how do their
   call counts scale?

## Method

Clean instrumentation overlay (perf-inst-dev). Each ProxyActor writes
its init profile to `$AURORA_RUN_LOG_DIR/instrumentation/<host>/` on Lustre
(direct write, no SCP). `RAY_event_stats=1` emits per-RPC stats blocks
every 1s into `gcs_server.out` on the head node.

All runs: null-compute mode, `startup_only=True` (no replay client), null
replicas = 12/node. Uses reservation R8443082 for 32/64/128n, prod queue
for 256n.

## Stage timing (seconds, from `scaling_trace.json`)

| Phase | 32n | 64n | 128n | 256n |
|---|---:|---:|---:|---:|
| `serve.start` | 40.2 | 44.6 | 37.6 | TBD |
| `serve.run.deploy_apps` | 43.8 | 43.2 | 45.9 | TBD |
| **`serve.run.wait_proxies`** | **21.8** | **93.6** | **393.8** | **TBD** |
| `stage3.total` | 86.9 | 156.7 | 463.5 | TBD |
| Total CLUSTER FULLY READY | ~145 | ~217 | ~500 | TBD |

**Key observations:**
- `serve.start` and `deploy_apps` are essentially **flat** across 32→128n
- **`wait_proxies` scales super-linearly**: 21.8s → 93.6s → 393.8s. Doubling nodes → 4× wait_proxies.
- Stage 3 total dominated by wait_proxies at higher scales.

## Proxy init duration (from per-proxy JSON on Lustre)

| Stat | 32n | 64n | 128n | 256n |
|---|---:|---:|---:|---:|
| mean (s) | 0.345 | 0.344 | 0.359 | TBD |
| p95 | 0.416 | 0.449 | 0.471 | TBD |
| p99 | 0.428 | 0.471 | 0.536 | TBD |
| max | 0.432 | 0.498 | 0.574 | TBD |
| proxies collected | 32/32 | 64/64 | 128/128 | TBD |

**Proxy init itself is flat.** ProxyActor `__init__` takes ~0.35s at all
scales. The wait_proxies cliff is NOT in the proxy's own work.

## Controller health-check events

| Event | 32n | 64n | 128n | 256n |
|---|---:|---:|---:|---:|
| Proxy `failed the health check` (× kill) | 0 | 0 | 0 | TBD |
| Proxy `Didn't receive ... response` | 0 | 0 | **126** | TBD |
| Replica `Didn't receive ... response` | 0 | 0 | 0 | TBD |
| Replicas `started successfully` | 384 | 768 | 1536 | TBD |

**First signal of proxy-side trouble appears at 128n**: 126 cases where
the controller dispatched a health check to a proxy but didn't get a
response back in time (patched timeout 300s). None of these reached the
100-consecutive-failure threshold, so no proxies were actually killed
(our patch worked — without it, 3-consecutive-failures at 30s/each
would have killed proxies by 128n).

## GCS event stats — peak queueing time and call counts

Format: `peak_q_max_ms / total_calls`. Peak queueing = maximum observed
queueing time (time from RPC dispatch to handler start) across all 1-second
event-stats blocks.

| Method | 32n | 64n | 128n | 256n |
|---|---:|---:|---:|---:|
| GcsInMemoryStore.Put | **1225**/3169 | **1241**/6072 | **1209**/12282 | TBD |
| GcsInMemoryStore.Get | 1.4/3466 | 3.9/6804 | **59.2**/13360 | TBD |
| ActorInfoGcsService.GetActorInfo | 1.0/25442 | 2.3/100034 | **86.6**/396674 | TBD |
| ActorInfoGcsService.GetNamedActorInfo | 19.3/13566 | 19.7/51660 | **86.2**/206360 | TBD |
| NodeInfoGcsService.GetAllNodeAddressAndLiveness | 63.4/522 | 18.5/1034 | 37.7/2058 | TBD |
| NodeManagerService.grpc_client.GetResourceLoad | 10.6/5041 | 26.4/14685 | **63.6**/66940 | TBD |
| PeriodicalRunner.RunFnPeriodically | 1226/5 | 1241/5 | 1209/5 | TBD |
| GcsHealthCheckManager::MarkNodeHealthy | 10.1/529 | 16.4/1276 | **85.8**/2480 | TBD |
| HealthCheck | 0.3/1553 | 2.9/4592 | **71.1**/21713 | TBD |

**The picture at 128n:**

1. **`GcsInMemoryStore.Put` queueing is flat (~1200ms)** across 32→128n despite
   calls doubling each step. This is a single serialization hotspot (only
   one thread can Put at a time) that's already saturated at 32n; it has
   a fixed worst-case queue depth that doesn't grow further.
   `PeriodicalRunner` shows the same profile because it queues behind Puts.

2. **GCS READ methods become contended at 128n.** GetActorInfo,
   GetNamedActorInfo, Get, HealthCheck, MarkNodeHealthy all jump from
   sub-30ms peak queueing at 32/64n to **60-90ms at 128n**. This is GCS
   read contention emerging as the cluster scales.

3. **Call counts scale super-linearly** for actor lookups:
   GetActorInfo: 25k (32n) → 100k (64n) → **397k (128n)** — 4× per step.
   Each proxy calls `ray.get_actor()` per replica; at 128n that's
   128 proxies × 1536 replicas = ~197k name lookups, fanned out as
   handle lookups.

## Interpretation (so far)

### Why is `wait_proxies` the dominant Stage 3 cost at scale?

`wait_proxies` in our code is `ray.wait()` on `.serving.remote()` calls
dispatched to every proxy. Each `.serving()` is a no-op on the proxy side,
but for the proxy to *respond*, its `LongPollClient` must have received
the updated replica set from the controller AND resolved each replica's
actor handle via `ray.get_actor()`. This last step is 128 × 1536 ≈ 197k
GCS queries.

The elapsed time before a proxy can respond to `.serving()` is dominated
by this long-poll + handle-resolution pipeline, not by the proxy's own
init. That's why `wait_proxies` grows super-linearly while `__init__`
stays flat.

### GCS writes aren't the bottleneck

The Put peak queueing is *identical* across scales (1225ms flat). This
is not a "not yet at capacity" signal — it's a saturated single-thread
serialization already at 32n. But crucially, Put's queueing doesn't
translate directly to user-visible latency. Writes happen in background
(node registration, actor state updates) and don't gate wait_proxies.

### GCS reads ARE the emerging bottleneck

At 128n, read paths (GetActorInfo, GetNamedActorInfo, HealthCheck) start
seeing 60-90ms queueing. Each proxy does hundreds of actor lookups
sequentially (or nearly so) — at 86ms/lookup × ~1.5k lookups = 130s just
for one proxy's long-poll refresh. 128 proxies doing this concurrently
saturates GCS reads.

### The `proxy didn't receive response` emergence at 128n

This controller log line (126 occurrences at 128n) appears when the
controller's health-check RPC to a proxy exceeds its timeout. It's the
precursor to the 256n proxy-death cascade documented in
[proxyactor_death_cascade_256n.md](proxyactor_death_cascade_256n.md).

At 128n our patched `PROXY_HEALTH_CHECK_UNHEALTHY=100` prevents actual
kills (100 consecutive failures never accumulate within the short
Stage 3 window). At 256n the same signal at higher rate historically
reached the threshold and killed proxies.

## 256n data — to be filled

(awaiting run16/256-nodes job completion)

## Headline conclusion (current)

The wait_proxies cliff is **not caused by GCS write contention** — Put
queueing is constant from 32n to 128n. The actual scaling problem is:

1. **GCS actor-handle lookups scale super-linearly** because every
   proxy must resolve every replica's actor on each long-poll update,
   producing O(proxies × replicas) = O(N²) lookups per cluster event.
2. At 128n this pushes GCS read queueing into the 60-90ms range; at
   256n it likely pushes past the controller's health-check timeout
   often enough to trigger the documented proxy-death cascade.
3. **Raising UNHEALTHY_THRESHOLD to 100 successfully prevents the
   proxy-kill cascade** at 128n (126 timeouts observed, 0 kills).

The theoretical fix would be: proxies should receive PRE-RESOLVED actor
handles from the controller's broadcast, not names that each proxy
re-resolves. This moves the O(N²) GCS lookup into a single O(N) operation
in the controller.

## Data
All runs stored under `/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs/weakscaling_nullcompute_proxy/run16/{32,64,128,256}-nodes/`.

## Tools
- `tools/parse_gcs_event_stats.py` — parse `gcs_server.out` event-stats blocks
- `tools/analyze_scaling.py` — cross-scale comparison table generator
