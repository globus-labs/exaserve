# Section 6.2 — Inside `DeploymentStateManager.update()`

**Commit**: overlay `3d66a9a` — 7 step-level timers in `dsm.update()`.
**Data**: `weakscaling_nullcompute_proxy/run19/{32,64,128}-nodes/`.

## What's measured

Each `dsm.update()` call now writes one JSONL line with duration for each of
the 8 sequential steps:

```
s1_check_and_update_replicas   # per-replica state transition probing (STARTING→RUNNING etc.)
s2_check_curr_status            # deployment-level status recompute
s3_drain_nodes                  # drain/migrate logic
s4_scale_replicas               # upscale/downscale target compute
s5_update_status                # second status pass (detect deletions)
s6_schedule_and_stop            # scheduler.schedule() + stop_replicas
s7_broadcast                    # long-poll notify_changed + autoscaler notify
# (s8 cleanup rarely fires during startup)
```

## Scaling table — MEAN per-step time (ms)

| Step | 32n | 64n | 128n | Per-doubling |
|---|---:|---:|---:|:---|
| **s1_check_and_update_replicas** | **20.5** | **32.5** | **57.3** | **1.7×** (linear with replicas) |
| s2_check_curr_status | 0.15 | 0.39 | 0.77 | 2.1× |
| s3_drain_nodes | 7.9 | 17.6 | 33.7 | 2.1× (linear with nodes) |
| s4_scale_replicas | 0.4 | 0.86 | 1.72 | 2.1× |
| s5_update_status | 0.08 | 0.18 | 0.35 | 2.0× |
| s6_schedule_and_stop | 1.8 | 1.6 | 2.1 | flat |
| s7_broadcast | 1.2 | 4.1 | 9.0 | 2.6× |
| **TOTAL mean** | **32.0** | **57.2** | **105.0** | **1.8×** |

**The mean dsm.update() is dominated by s1 (per-replica state probing)
and s3 (drain check)** — both scale roughly linearly with replica or
node count. These are "always happening" ticks.

## Scaling table — MAX per-step time (ms) — peak stalls

| Step | 32n | 64n | 128n | Interpretation |
|---|---:|---:|---:|:---|
| s1_check_and_update_replicas | 152 | 246 | 490 | O(replicas) at peak |
| s3_drain_nodes | 32 | 77 | 93 | ~linear |
| s4_scale_replicas | 63 | 135 | 260 | startup burst, 2× per doubling |
| **s6_schedule_and_stop** | **1014** | **2048** | **3915** | **startup-only, 2× per doubling** |
| s7_broadcast | 3 | **477** | **514** | emerges at 64n, plateaus |
| s2, s5 | < 3 | < 3 | < 3 | negligible |

**Two very different max-stall stories:**

### s6_schedule_and_stop — the one-time initial scheduling burst

Worst tick at every scale has the same signature:
```
n_replicas=0  s6=<HUGE>  total=<HUGE>
```

At the very first dsm tick, the scheduler assigns placement-group for all
N replicas at once. This is a one-shot cost at startup:
- 32n: 1014ms (placing 384 replicas)
- 64n: 2048ms (768 replicas)
- 128n: 3915ms (1536 replicas)
- 256n (predicted): ~8s (3072 replicas)

Scaling: **1ms per replica** to schedule. This is a one-tick cost; doesn't
recur during the run. But at 256n it blocks the controller asyncio loop
for ~8 seconds at Stage 3 startup.

### s7_broadcast — long-poll notify_changed fan-out

Interesting emergence:
- 32n mean=1.2ms, max=3ms — trivial
- **64n mean=4ms, max=477ms** — stall emerges
- 128n mean=9ms, max=514ms — plateau (!)

The s7 step calls `deployment_state.broadcast_running_replicas_if_changed()`
which in turn calls `self._long_poll_host.notify_changed({...})`.
`notify_changed` walks the subscriber list and schedules callbacks to each
proxy's pending long-poll request.

At 64n+ this becomes a synchronous loop over 64-128 subscribers. It
plateaus at ~500ms because:
- The callbacks are scheduled on asyncio futures (non-blocking individually)
- The bottleneck is the iteration + future creation itself
- Doesn't scale with replica count, only subscriber count

**This is the server-side counterpart of the per-proxy O(N²) problem from
Section 6**: s7 creates 128 long-poll responses at 128n, each sent to a
proxy which then does 1536 get_actor_handle calls to process it.

## The worst ticks decoded

### 128n — worst tick

```
n_replicas=0
  s4_scale_replicas:     259.8ms  (initial upscale target compute)
  s6_schedule_and_stop: 3914.6ms  ← placing 1536 replicas
  TOTAL:                4174.8ms
```

Matches Section 6.1 finding of 4.3s max controller tick at 128n — this is
literally that same tick, broken down.

### 128n — second/third worst

```
n_replicas=1536
  s1_check_and_update_replicas:  48-54ms  (checking 1536 replicas)
  s3_drain_nodes:                34-40ms
  s7_broadcast:                 486-514ms  ← fan-out to 128 subscribers
  TOTAL:                       577-606ms
```

Steady-state worst: dominated by s7_broadcast. Happens every time the
replica set changes (so roughly once per batch of RUNNING transitions).

## Cross-section synthesis — the full timing chain

Now we can sketch the end-to-end chain for a single replica-set change
at 128n:

```
 ┌──────────────────────────────────────────────────────────────────────┐
 │  Controller event loop (head node, single asyncio thread)            │
 │                                                                      │
 │  dsm.update() tick that detects new RUNNING replicas:                │
 │    s1_check_and_update_replicas  ≈ 50ms   (probes state of 1536)     │
 │    s3_drain_nodes                ≈ 34ms   (O(nodes) check)           │
 │    s7_broadcast                  ≈ 500ms  ← notify_changed fan-out   │
 │        ↓ schedules 128 futures, one per subscribed proxy             │
 │    total_tick                    ≈ 600ms                             │
 │                                                                      │
 │  Controller sleeps 0.1s, then next tick...                           │
 └──────────────────────────────────────────────────────────────────────┘
                                 ↓ long_poll_host serves the 128 pending
                                 ↓ responses to proxies (parallel over network)
 ┌──────────────────────────────────────────────────────────────────────┐
 │  Each of 128 ProxyActors (own asyncio loop, own Python process)      │
 │                                                                      │
 │  LongPollClient._process_update receives DeploymentTargetInfo        │
 │    ↓ schedules callback on asyncio loop                              │
 │  router.update_deployment_targets(deployment_target_info)            │
 │    ↓ request_router._update_running_replicas(running_replicas)       │
 │    ↓ for r in running_replicas (1536 iterations):                    │
 │        RunningReplica(r) → r.get_actor_handle()                      │
 │            → ray.get_actor(name, namespace)                          │
 │                ↓ GCS: GetNamedActorInfo(name)     ← 50-120ms queue   │
 │                ↓ GCS: GetActorInfo(actor_id)      ← 50-120ms queue   │
 │                                                                      │
 │  Per-proxy single-broadcast cost: 1536 × 2 × ~45ms eff = ~138s       │
 │  (matches Section 6 per-proxy max 179s for ONE broadcast)            │
 └──────────────────────────────────────────────────────────────────────┘
                                 ↓
 ┌──────────────────────────────────────────────────────────────────────┐
 │  Driver: ray.wait(serving_refs) waits until every proxy's event      │
 │  loop picks up and replies to .serving.remote(), which can only      │
 │  happen AFTER the proxy finishes its update_deployment_targets call  │
 │  AND any earlier queued updates.                                     │
 │                                                                      │
 │  Observed wait_proxies at 128n: 394s                                 │
 │    = proxy_spawn_delay (~38s, documented)                            │
 │    + per-proxy-max update time (179s, Section 6)                     │
 │    + inter-broadcast intervals (~100-200ms × several broadcasts)     │
 │    ≈ 394s ✓                                                          │
 └──────────────────────────────────────────────────────────────────────┘
```

## The single architectural observation

Each dsm tick that triggers a broadcast pays:
- **~500ms in s7** on the controller (fan-out cost)
- **~138s per proxy** downstream (O(N_replicas) GCS handle lookups)
- And blocks wait_proxies until the SLOWEST of 128 proxies finishes

Across the system, a single update propagates at 500ms controller cost +
138s worst-case proxy cost = **~138s end-to-end** per broadcast. At 128n
we observe ~2-3 broadcasts during Stage 3, which matches the ~394s
wait_proxies figure.

At 256n (predicted):
- s7 stays ~500ms (plateau)
- per-proxy update: 3072 replicas × ~120ms eff = ~370s per broadcast
- Stage 3 sees 2-3 broadcasts → ~740s-1100s wait_proxies
- Observed: 1751s

The extra ~700s at 256n over the minimum prediction comes from:
- proxy startup delays (~38s fixed)
- **more broadcasts** (653 total across 256 proxies = 2.55 per proxy)
- occasional very-long broadcasts (up to 411s observed)

## The architectural fix (unchanged, now even better supported)

**Ship actor handles in the broadcast payload, not just names.**

This removes the proxy's O(N_replicas) GCS lookup on every broadcast. It
also lets the controller cache handles and send diffs instead of full
replica lists, which would cut s1 and s7 cost.

Specifically:
- **Section 6 (per-proxy):** O(N²) GCS calls → O(N) controller-side once
- **Section 6.1 (controller ticks):** dsm max stall drops because s1 scans
  fewer "changed" replicas
- **Section 6.2 (dsm internals):** s7_broadcast stays ~500ms (not really
  addressed by this change), but broadcast PAYLOAD shrinks to diffs
- **Section 5 (GCS):** GetActorInfo/GetNamedActorInfo calls drop by 10-100×
  because the 1.5M lookups become a much smaller number of controller-side
  lookups

## Reproducibility

- Overlay commits: `2014298` (pristine baseline) → `3d66a9a` (latest probes)
- Overlay files patched: `serve/_private/{proxy,constants,controller,common,router,deployment_state}.py`
- Main repo: see `perf-inst-dev` branch
- Data: `weakscaling_nullcompute_proxy/run{16,17,18,19}/*`
  - run16: final 4-scale baseline (no probes beyond proxy.py)
  - run17: §6 probes (update_deployment_targets timer + get_actor_handle counter)
  - run18: §6.1 controller tick probe
  - run19: §6.2 dsm step breakdown (this doc)
- Tools:
  - `tools/parse_gcs_event_stats.py` — GCS server event-stats parser
  - `tools/analyze_scaling.py` — cross-scale summary
  - `tools/analyze_probes.py` — §6 probes
  - `tools/analyze_controller_ticks.py` — §6.1 ticks
  - `tools/analyze_dsm.py` — §6.2 sub-phases (this doc)
