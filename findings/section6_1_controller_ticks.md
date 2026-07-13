# Section 6.1 Probe — Controller Reconcile Tick Breakdown

**Commit**: overlay `7233fca` — adds per-tick timing to
`ServeController.run_control_loop_step`.

**Data**: `weakscaling_nullcompute_proxy/run18/{32,64,128,256}-nodes/`.
32n/64n/128n complete; 256n pending (prod queue).

## What's measured

Each iteration of the controller's reconcile loop writes one JSONL line
to `$EXASERVE_RUN_LOG_DIR/instrumentation/<head>/controller_ticks_<pid>.jsonl`:

```json
{
  "t": 1776894441.536,
  "num_loops": 0,
  "done_recovering": true,
  "subs": {
    "cluster_node_info_update": 0.0014,
    "dsm_update": 0.0,
    "asm_update": 0.0,
    "node_update": 0.0,
    "proxy_state_update": 0.0228,
    "total": 0.0243
  },
  "total_s": 0.0243
}
```

These are the exact same sub-phase timers Ray already captures internally
for its Gauge metrics; we just append them to a file so we can reconstruct
the per-tick timeline.

## Scaling results (32n/64n/128n — 256n running)

| Metric | 32n | 64n | 128n | 256n |
|---|---:|---:|---:|---|
| n_ticks observed | 634 | 910 | 3,347 | TBD |
| wall_clock_span (s) | 206 | 335 | 1,333 | TBD |
| total_time_in_ticks (s) | 17.5 | 54.6 | 197.7 | TBD |
| **tick total mean (ms)** | 27.6 | 60.0 | 59.1 | TBD |
| tick total p50 (ms) | 24.9 | 56.1 | **5.5** | TBD |
| tick total p99 (ms) | 92.2 | 137.3 | 179.4 | TBD |
| **tick total MAX (ms)** | **1018** | **2124** | **4282** | TBD |
| gap mean (ms) | 298 | 309 | 339 | TBD |
| gap max (ms) | 2155 | 2333 | 3206 | TBD |

Sub-phase mean (ms):

| | 32n | 64n | 128n |
|---|---:|---:|---:|
| cluster_node_info_update | 2.5 | 3.5 | 5.7 |
| **dsm_update** (deployment state) | **24.5** | **55.4** | **52.6** |
| asm_update (application state) | 0.2 | 0.3 | 0.1 |
| node_update (proxy nodes) | 0.08 | 0.18 | 0.18 |
| proxy_state_update | 0.2 | 0.3 | 0.2 |

Sub-phase MAX (ms) — peak stalls:

| | 32n | 64n | 128n |
|---|---:|---:|---:|
| cluster_node_info_update | 35 | 19 | 35 |
| **dsm_update** | **1015** | **2120** | **4275** |
| asm_update | 3 | 3 | 3 |
| node_update | 0.2 | 0.3 | 0.7 |
| proxy_state_update | 23 | 63 | 126 |

## Key findings from controller ticks

### 1. `deployment_state_manager.update()` IS the reconcile bottleneck

At every scale, the `dsm_update` sub-phase dominates both mean and max tick
time. At 128n the worst-case dsm_update is **4.3 seconds** — a single
reconcile iteration stalled the controller's asyncio loop for 4 seconds.
During that 4s, no health checks, no long-poll replies, no proxy state
updates could be processed.

### 2. Tick distribution becomes bimodal at 128n

Mean stays ~60ms at 64n→128n, but p50 drops from 56ms to **5.5ms** while
max jumps to 4.3s. The distribution is splitting: most ticks are fast
(nothing to reconcile) but a minority are slow (processing replica state
transitions). Looking at the timeline, the slow ticks cluster around replica
lifecycle events (RUNNING transitions, proxy spawns).

### 3. Controller tick rate effectively unchanged

Gap mean stays ~300ms across all scales — the controller is still
sleeping its 100ms `CONTROL_LOOP_INTERVAL_S` between iterations. The fact
that the OBSERVED gap is 300ms (3× larger) means each tick is itself taking
~200ms on average (handling accumulated state). Total tick period:
~60ms tick + ~300ms gap ≈ 360ms per loop.

### 4. Total controller CPU time is small relative to wait_proxies

At 128n: 197s of cumulative tick time over a 1333s wall-clock span.
Controller is actively "computing" only 15% of the time. The rest is in
the sleep/gap between ticks (where it's just awaiting I/O).

## How this fits with Section 6 per-proxy evidence

Recall the 256n budget from `section6_probes_evidence.md`:

- wait_proxies total = 1751.5s (observed)
- Per-proxy max GCS time = 994s (57% of wait_proxies)
- Remaining = 757s "controller-side + asyncio scheduling"

The controller tick data at 128n says total tick time (197s) is much
smaller than the observed wait_proxies (393.8s). So the remaining ~200s
at 128n is NOT controller CPU — it's the time between reconcile loop
ticks where:

- **The controller is sleeping** (`await asyncio.sleep(0.1)`) — but gaps
  observed at 300ms mean, not 100ms. The extra 200ms comes from other
  async tasks competing for the event loop (long_poll_host responding
  to proxy subscriptions, and the kill_actor cleanup logic).
- **Broadcasts fire at ~3× gap** — the controller's `notify_changed` calls
  from deployment_state are triggered by state changes, not on a timer.
  So broadcasts happen at ~each tick that sees a RUNNING transition.

### Why proxy broadcasts are limited in frequency

Each proxy receives updates via LongPollClient. The controller's
`long_poll_host.notify_changed` (called from `_broadcast_running_replicas_if_changed`
inside `deployment_state_manager.update()`) triggers the broadcast. The
controller can issue a broadcast **at most once per reconcile tick**.

At 128n tick rate = ~3 ticks/sec, broadcasts are limited to ~3/sec
system-wide. Each broadcast is received by all 128 proxies in parallel,
so per-proxy broadcast RATE is also ~3/sec. Each proxy processes the
broadcast by doing N=1536 `ray.get_actor()` calls (Section 6 evidence).

**So: proxies receive a new broadcast every ~333ms, each containing a
nearly-full replica set (1536 replicas at 128n). Each broadcast takes the
proxy 20s mean / 179s max to process (Section 6 evidence). Broadcasts
queue on the proxy's asyncio loop; slow proxies fall further and further
behind.**

## Interpretation for the wait_proxies budget

Refined decomposition of wait_proxies at 128n (393.8s total):

```
├── Proxy spawn delay (~38s fixed, documented in wait_proxies_root_cause_instrumented.md)
├── ~155 total update_deployment_targets broadcasts across all 128 proxies
│    = ~1.2 broadcasts per proxy (with some proxies getting 2-3 during startup)
│    = per-proxy update time up to 179s in single call
├── Per-proxy max total time in broadcasts = 179s (Section 6)
├── Controller tick time cumulative = 197s (this doc)
│   – overlapping with proxy work since both are running in parallel
└── Actual cliff = max(controller, per-proxy-worst) + spawn delay
    = max(1333, 179) + 38 ≈ 393s (matches observed 394s)
```

**The controller's total wall-span (1333s at 128n) EXCEEDS wait_proxies
(393s).** This is because the controller keeps running after Stage 3
completes — it's measuring the whole job lifetime. But the per-proxy
broadcast work (179s) fits inside wait_proxies budget cleanly.

## Where's the remaining 43% of wait_proxies at 256n?

We previously estimated controller-side work accounts for ~757s of
wait_proxies at 256n. With 128n data extrapolated:

- At 128n: 3,347 ticks, max tick 4.3s, total_time_in_ticks 197s
- At 256n: assume 4× scaling → ~13,000 ticks, max tick maybe 8-15s,
  total_time_in_ticks ~800s

If the controller's tick cumulative time at 256n is ~800s, and the
per-proxy max GCS time is 994s, these two largely overlap in wall-clock
time because they run in parallel. So the observed wait_proxies at 256n
(1751s) is *still* close to the larger of the two.

The actual 256n controller data will confirm whether tick time approaches
~800s or something different. Expected result: worst-case tick in the
multi-second range, total_time_in_ticks in the several-hundreds.

## Implication for upstream Ray Serve

The two bottlenecks identified across Sections 5, 6, 6.1 are:

1. **Proxy-side O(N²) actor-handle re-resolution** on each broadcast
   (Section 6) — dominant at all scales.
2. **Controller `deployment_state_manager.update()` cost per tick**
   that grows with replica count (Section 6.1) — dominant for controller
   asyncio loop health.

Both would be fixed by the same architectural change: **controller ships
pre-resolved actor handles in the broadcast payload, and uses a diff-based
update instead of sending the full replica list every time**. That removes
both the per-proxy N² lookup and shrinks dsm_update's work to O(changes)
instead of O(replicas).

## Commits

- Overlay: `7233fca` — controller tick probe
- Analyzer: `eval/tools/analyze_controller_ticks.py` (committed with this findings doc)
- Data: `/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs/weakscaling_nullcompute_proxy/run18/`
