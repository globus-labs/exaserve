# GCS Contention at Scale — Quantitative Measurement (32/64/128/256 nodes)

**Branch:** perf-inst-dev (commit 757809e + overlay 4dce240)
**Date:** 2026-04-22
**Jobs:** 8445314 (32n), 8445324 (64n), 8445336 (128n), 8445349 (256n)

## Headline

The "wait_proxies cliff" scales super-linearly (21.8s → 93.6s → 393.8s →
**1751.5s**) across 32/64/128/256 nodes. It is NOT driven by GCS write
contention (`GcsInMemoryStore.Put` queueing is constant at ~1210ms
across all four scales — a fully saturated single-writer path). It IS
driven by an **O(N²) pattern of GCS actor-handle lookups**: each proxy
calls `ray.get_actor()` for every replica on every LongPollClient
broadcast, producing millions of `GetActorInfo` / `GetNamedActorInfo`
calls per deployment. At 256n this reaches **1.58 million** GetActorInfo
calls with peak GCS read queueing of 90ms.

**Patched `UNHEALTHY_THRESHOLD=100` successfully prevents the
ProxyActor kill cascade at 256n** (0 kills despite 1187 "Didn't receive
health check response" events — the precursor signal).

## Goal

Quantify the GCS-contention hypothesis from
[proxyactor_death_cascade_256n.md](proxyactor_death_cascade_256n.md):

1. Is GCS the bottleneck for wait_proxies at scale? **Partially.** GCS
   writes are saturated but not growing; GCS reads are the real scaling
   problem.
2. Which specific methods dominate queueing and call counts? **Actor-handle
   lookups.**
3. Where's the architectural waste? **Quadratic re-resolution of actor
   handles by every proxy.**

## Method

- **Clean instrumentation overlay** (perf-inst-dev) replaces
  sitecustomize/usercustomize. Four files patched at
  `~/.local/aurora/.../ray`: `constants.py` (Aurora timeouts),
  `proxy.py` (ProxyActor `__init__`/`ready()` instrumentation with
  per-substep timing), plus pristine `controller.py` and `proxy_state.py`
  as baselines.
- **Direct-to-Lustre output**: ProxyActors write
  `$AURORA_RUN_LOG_DIR/instrumentation/<host>/proxy_init_<pid>.json`
  instead of `/tmp/aurora_inst/` (which was being cleaned by PBS epilogue
  before the head-node finalize trap could ssh to collect).
- **`RAY_event_stats=1 RAY_event_stats_print_interval_ms=1000`** — Ray's
  built-in per-RPC telemetry, dumped every 1s to the head's
  `gcs_server.out`.
- **All runs**: null-compute mode, `startup_only=True` (no replay client),
  12 replicas/node. 32/64/128n on reservation R8443082; 256n on `prod`
  queue.

## Results

### Stage timing (from `scaling_trace.json`)

| Phase | 32n | 64n | 128n | 256n |
|---|---:|---:|---:|---:|
| `ray.init` | 25.8 | ~24 | — | 24.9 |
| `serve.start` | 40.2 | 44.6 | 37.6 | 38.7 |
| `serve.run.deploy_apps` | 43.8 | 43.2 | 45.9 | **104.6** |
| **`serve.run.wait_proxies`** | **21.8** | **93.6** | **393.8** | **1751.5** |
| `stage3.total` | 86.9 | 156.7 | 463.5 | **1880.6** |
| CLUSTER FULLY READY | ~145 | ~217 | ~500 | ~1940 |

Doubling nodes → **~4.5× wait_proxies**. Classic O(N²) scaling.

`deploy_apps` is flat up to 128n and finally jumps 2.3× at 256n.
This jump mirrors replica count scaling (3074 replicas at 256n).

### Proxy init distribution (per-proxy JSON)

| Stat | 32n | 64n | 128n | 256n |
|---|---:|---:|---:|---:|
| mean (s) | 0.345 | 0.344 | 0.359 | **0.558** |
| p95 | 0.416 | 0.449 | 0.471 | **1.030** |
| p99 | 0.428 | 0.471 | 0.536 | **1.255** |
| max | 0.432 | 0.498 | 0.574 | **1.876** |
| collected | 32/32 | 64/64 | 128/128 | 256/256 |

Proxy `__init__` is flat through 128n (~0.35s) and bumps to 0.56s mean
at 256n with a tail out to 1.88s — the `long_poll_client` and
`create_proxies` substeps slow down because they're hitting the saturated
GCS read path themselves.

### Controller health-check events (from `controller_*.log`)

| Event | 32n | 64n | 128n | 256n |
|---|---:|---:|---:|---:|
| Proxy `failed health check` (→ kill) | 0 | 0 | 0 | **0** |
| Proxy `Didn't receive ... response` | 0 | 0 | 126 | **1187** |
| Replica `Didn't receive ... response` | 0 | 0 | 0 | 0 |
| Replicas `started successfully` | 384 | 768 | 1536 | 3074 |

**"Didn't receive response" events scale ~10× per node-doubling**
(0→0→126→1187). This is the controller failing to reach a proxy within
the patched 300s timeout. It's the precursor to the proxy-kill cascade
documented in the prior findings note.

**Our patches prevented the kill cascade at 256n** (0 kills). With the
default `UNHEALTHY_THRESHOLD=3`, the 1187 timeout events would have
easily crossed the threshold per-proxy (4.6 avg per proxy → some
proxies hit ≥3 consecutive), producing kills matching the historical
74-proxy cascade at 256n.

### GCS event stats — peak queueing / total calls

Format: `peak_q_max_ms / total_calls`. Peak queueing = max observed
over all 1-second event-stats blocks.

| Method | 32n | 64n | 128n | 256n | Call scaling |
|---|---:|---:|---:|---:|:---:|
| `GcsInMemoryStore.Put` | 1225/3.2k | 1241/6.1k | 1209/12.3k | 1211/**27k** | 2× per step |
| `GcsInMemoryStore.Get` | 1.4/3.5k | 3.9/6.8k | 59.2/13k | 70.8/**27k** | 2× |
| **`ActorInfoGcsService.GetActorInfo`** | 1.0/**25k** | 2.3/**100k** | 86.6/**397k** | 90.0/**1,581k** | **~4×/step (N²)** |
| **`ActorInfoGcsService.GetNamedActorInfo`** | 19.3/14k | 19.7/52k | 86.2/**206k** | 89.0/**921k** | **~4×/step (N²)** |
| `NodeManagerService.GetResourceLoad` | 10.6/5k | 26.4/15k | 63.6/67k | 83.0/**494k** | 3-7× |
| `HealthCheck` | 0.3/1.5k | 2.9/4.6k | 71.1/22k | 85.0/**163k** | 3-7× |
| `GcsHealthCheckManager::MarkNodeHealthy` | 10.1/0.5k | 16.4/1.3k | 85.8/2.5k | 87.1/5.5k | ~2× (linear) |
| `PeriodicalRunner.RunFnPeriodically` | 1226/5 | 1241/5 | 1209/5 | 1212/5 | flat |

**The O(N²) methods are the story.** GetActorInfo and GetNamedActorInfo
grow ~4× per node-doubling — matching the N² pattern where each of N
proxies resolves each of N×R replicas. At 256n this is **1.58 million
GetActorInfo calls** over a 1751s wait_proxies window = 900 calls/sec
sustained to GCS, with 90ms peak queueing per call.

**GCS write path (Put) is saturated but static.** ~1200ms peak queueing
across all scales; call count doubles per step but queueing never grows.
This is Ray's single-writer InMemoryStore thread already at capacity at
32n. It doesn't gate wait_proxies because writes are background
(node/actor registration).

**GCS read path plateaus at ~85-90ms peak queueing** between 128n and
256n. Throughput is the bottleneck, not latency-per-call.

## Interpretation

### Why is `wait_proxies` the dominant Stage 3 cost?

`wait_proxies` is our code's `ray.wait()` on `.serving.remote()` calls
dispatched to every proxy. The proxy-side `.serving()` method is a
no-op (it just returns). But for the proxy to *reach* the state where
its event loop can pick up and respond to the RPC, its LongPollClient
must have:

1. Received the controller's broadcast of the new replica set
2. Resolved **every replica's actor handle** via
   `ray.get_actor(name, namespace)` (see `replica_wrapper.py:109`)
3. Updated its internal request router

Step 2 is the O(N²) cost. The controller's broadcast is O(N_proxies)
network-wise, but the proxy-side handle resolution is O(N_proxies ×
N_replicas) GCS queries across the cluster. N_replicas = 12 ×
N_proxies, so total is O(N²) × 12.

At 256n: 256 proxies × 3072 replicas = 786k lookups per broadcast.
Each logical ray.get_actor involves ~2 GCS round-trips (name +
handle) = 1.57M GCS calls — matches observed 1.58M.

### Why GCS reads but not writes?

Writes are serialized through `GcsInMemoryStore.Put` (~1200ms peak
queue, constant at all scales). But writes are driven by structural
events (node register, actor register, job create) — O(N), not O(N²).

Reads hit the same store but can parallelize more inside the GCS
process. They still hit a ceiling: ~900 calls/sec throughput, ~85ms
peak queueing. Beyond that, calls stack up in the queue.

### Why the "Didn't receive response" cascade at 128n+?

The controller polls each proxy's health every
`PROXY_HEALTH_CHECK_PERIOD_S=10s`. The health-check RPC is:

```
controller.core_worker → GCS: resolve proxy actor handle
                      → gRPC to proxy's raylet
                      → proxy's asyncio loop → check_health()
                      → return path
```

When the proxy's asyncio loop is blocked on 3k ray.get_actor()
resolutions at 85ms each, it can't service incoming health-check RPCs.
The controller's 300s timeout (our patched value) fires — recorded as
"Didn't receive response".

Our patched `UNHEALTHY_THRESHOLD=100` prevents the kill: even at 256n
with 1187 timeouts over 1751s, no single proxy accumulates 100
consecutive failures within the window.

### Architectural implication

The actor-handle re-resolution on every long-poll update is the
scaling-fundamental problem. The controller already knows the full
replica→actor mapping — it just sends names over long-poll and expects
every proxy to re-resolve. A fix would be: **ship actor handles in the
long-poll payload**, making the proxy's update O(1) GCS calls instead
of O(N_replicas). This turns the total from O(N² × R) to O(N × R) =
O(N), a huge scaling win.

## Takeaways

1. **Wait_proxies is our dominant Stage 3 cost** and scales ~4.5× per
   node-doubling up through 256n (21.8s → 1751.5s).
2. **GCS contention IS real but it's READ-side** (actor lookup storm),
   not the write-side path that the initial hypothesis focused on.
3. **Ray Serve's LongPollClient replica-refresh is O(N²)** in GCS
   lookups. This is the scaling-fundamental architectural issue.
4. **Our timeout patches are a successful workaround**: they prevent
   the proxy-kill cascade that historically was observed at 256n
   (74 kills in prior runs). With patches applied, the system is
   *slow* at 256n but *not broken*.
5. **GCS writes are saturated at 32n already**, but don't further
   bottleneck because write rate is O(N), not O(N²).

## Future work

- **Eliminate the per-proxy re-resolution** by including actor handles
  in the long-poll payload (Ray Serve change).
- **Measure the controller asyncio loop** directly — the "Didn't receive
  response" signals include proxy-side delays AND controller
  dispatch delays, and we haven't separated them.
- **Test with `RAY_gcs_server_num_threads` increased** to see if
  read-side parallelism lifts the 900/sec ceiling. (Current: 8.)

## Data

All runs stored under:
`/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs/weakscaling_nullcompute_proxy/run16/{32,64,128,256}-nodes/`

Key artifacts per run:
- `logs/backend/*_ray_runtime/scaling_trace.json` — phase timing
- `logs/backend/*_ray_runtime/instrumentation/<host>/proxy_init_<pid>.json` — per-proxy init breakdown with substeps
- `logs/backend/*_ray_runtime/ray_logs/<head>/gcs_server.out` — Ray event stats (2-19k blocks)
- `logs/backend/*_ray_runtime/ray_logs/<head>/serve/controller_*.log` — controller events

## Tools

- `tools/parse_gcs_event_stats.py` — parse gcs_server.out event-stats
  blocks into CSV
- `tools/analyze_scaling.py` — cross-scale comparison table generator
  (takes N run_dirs, produces side-by-side table)

## Rollback

Main-repo commit 2f32633 restores the pre-perf-inst-dev state.
Overlay repo commit 2014298 restores pristine upstream Ray files.
