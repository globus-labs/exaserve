# ProxyActor Death Cascade at 256n — Stage 3 Bottleneck Investigation

## Summary

At 256 nodes × 12 replicas = 3072 replicas, the HAProxy-mode `serve.run()`
call (Stage 3) took **1665s** to complete vs **349s** for the direct-MPI run
of the same day's snapshot. The 1316s gap is caused by a **ProxyActor death
cascade** triggered ~7 minutes after bulk replica init completes. The
controller explicitly kills 74 ProxyActors via `ray.kill()` after each
accumulates 100 consecutive health-check failures.

Whether the cascade is HAProxy-mode-specific or a non-deterministic roll is
not yet established — we have only one run in each mode. Known variability
flag: [weakscaling_short_v3_progress.md:69](weakscaling_short_v3_progress.md#L69)
already notes "Stage 3 highly variable, first attempt timed out at 1800s,
succeeded at 1787s on retry."

## Kill Mechanism — Definitive Evidence

From [controller_10619.log:9603-9605](../agpt/data/experiments/runs/weakscaling_haproxy_short_v3/run1/256-nodes/logs/backend/20260410T093039Z_ray_runtime/ray_logs/x4304c0s1b0n0/serve/controller_10619.log#L9603-L9605):

```
WARNING 2026-04-10 09:44:23,222 controller 10619 --
  Proxy SERVE_PROXY_ACTOR-<id> failed the health check 100 times in a row,
  marking it unhealthy.
INFO 2026-04-10 09:44:23,222 controller 10619 --
  Proxy on node '<id>' is unhealthy. Shutting down the unhealthy proxy
  and starting a new one.
```

The controller reaches the patched `PROXY_HEALTH_CHECK_UNHEALTHY=100` threshold
and calls `ray.kill(proxy_actor)`. The `ActorDiedError` messages in the driver
log (with `The actor is dead because it was killed by ray.kill.`) are the
downstream effect.

**Direct 256n has zero such kills** — confirming the cascade is not a
universal 256n phenomenon.

## Per-Failure Mechanism — 300s Timeout Per Check

From [controller_10619.log:9240-9303](../agpt/data/experiments/runs/weakscaling_haproxy_short_v3/run1/256-nodes/logs/backend/20260410T093039Z_ray_runtime/ray_logs/x4304c0s1b0n0/serve/controller_10619.log#L9240):

```
09:40:02 -- Didn't receive health check response for proxy on <node> after 300.0s.
09:40:11 -- (x4 more proxies) after 300.0s.
09:40:12 -- ...
09:41:35 -- ...
09:41:50 -- ...
```

Each individual failed health check hit our patched
`PROXY_HEALTH_CHECK_TIMEOUT_S=300`. The controller fires checks on
`PROXY_HEALTH_CHECK_PERIOD_S=10s` intervals; multiple checks are in flight
concurrently. 100 consecutive failures accumulate in ~10 minutes because of
this concurrent-check behavior, not serial 100 × 300s.

## Timeline

Deployment started 09:33:24 (Stage 3 begin).

| Time | Δ from start | Event |
|------|------|------|
| 09:33:24 | +0s | Stage 3 start (`serve.run` dispatched) |
| 09:34:47 | +83s | First replica starts loading weights |
| 09:35:30 | +126s | Bulk of 3072 replicas finished `engine_create` |
| 09:37:37 | +253s | **First replica health-check timeouts** (120.0s patched per-deployment) |
| 09:39:55 | +391s | First proxy hits `GetTimeoutError: actor_manager.cc:98: GCS server is dead or under high load` in `LongPollClient` |
| 09:40:02 | +398s | **First proxy 300s health-check timeout** recorded |
| 09:44:23 | +659s | **First proxy killed** — 100 consecutive failures reached |
| ~09:44–09:50 | +660–1000s | Cascade continues; 74 proxies killed total |
| 09:50:38 | +1034s | One late-starting replica does fresh `__init__` |
| 09:50:47 | +1043s | Stage 3 completes at **1665s** wall clock |

Replica health-check timeouts appear 3 minutes **before** proxy timeouts —
this is the earliest signal that the controller is falling behind.

## Why a "network call" takes minutes

A Ray actor RPC is not a TCP ping. The path for `proxy.check_health.remote()`:

```
controller.core_worker
    → GCS: resolve named-actor handle (if not cached)
    → gRPC to proxy's raylet
    → proxy's core_worker inbox
    → proxy's asyncio loop → check_health()
    → return path reverse
    → controller's asyncio loop picks up future
```

At 256n scale, both ends contend:

- **Controller asyncio loop saturation** — single-threaded loop dispatching
  3072× `initialize_and_get_metadata` + 256× proxy `check_health` at
  `CONTROL_LOOP_INTERVAL_S=0.1s`. Completed future callbacks queue behind
  reconcile work.
- **Proxy LongPollClient stall** — each of 256 proxies calls `ray.get_actor()`
  for every new running replica. 256 × 3072 = ~786K actor-handle lookups
  against a single GCS process. `GetTimeoutError` at 09:39:55 is direct
  evidence of GCS saturation.

TCP RTT remains milliseconds. The stall is application-level rendezvous
(GCS) + event-loop scheduling on both sides.

## What Is Not Yet Proven

The "GCS contention" hypothesis is **qualitatively supported** (one
`GetTimeoutError` log line, expected fan-in arithmetic) but **not
quantitatively measured**. We do not have:

- GCS server CPU/IO utilization over time
- GCS RPC rate, queue depth, or per-method latency
- Controller asyncio loop iteration duration over time
- Per-call breakdown of where `check_health` time is spent (network vs
  controller dispatch vs proxy response)

Without these, we cannot say GCS contention vs. controller event-loop
saturation is the dominant cost. Both are plausible; they may reinforce
each other.

## Open Questions

1. Is the cascade HAProxy-mode-specific or just one unlucky run? Need a
   second 256n HAProxy run + a second 256n direct run to distinguish
   systematic from non-deterministic.
2. To what extent is GCS contending? Need RPC rate and latency distribution.
3. Why does the 100-failure threshold even matter if each replica
   `initialize_and_get_metadata` also independently takes >120s? The
   controller is stuck on both fronts simultaneously.

## Cross-Reference

- HAProxy 256n driver log: [20260410T093039Z_ray_runtime/launch.log](../agpt/data/experiments/runs/weakscaling_haproxy_short_v3/run1/256-nodes/logs/backend/20260410T093039Z_ray_runtime/launch.log)
- HAProxy 256n controller log: [ray_logs/x4304c0s1b0n0/serve/controller_10619.log](../agpt/data/experiments/runs/weakscaling_haproxy_short_v3/run1/256-nodes/logs/backend/20260410T093039Z_ray_runtime/ray_logs/x4304c0s1b0n0/serve/controller_10619.log)
- Direct 256n driver log: [20260411T160850Z_ray_runtime/launch.log](../agpt/data/experiments/runs/weakscaling_direct_short_v3/run1/256-nodes/logs/backend/20260411T160850Z_ray_runtime/launch.log)
- Prior wait_proxies investigation (null-compute): [wait_proxies_root_cause_instrumented.md](wait_proxies_root_cause_instrumented.md)
- Stage walkthrough: [ray_launch_stages.md](ray_launch_stages.md)
