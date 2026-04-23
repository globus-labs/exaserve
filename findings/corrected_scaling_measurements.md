# Corrected Scaling Measurements (Overhead-Free)

**Branch**: perf-inst-dev
**Architecture**: probes write to node-local `/tmp/aurora_inst`; aurora_serve
runs one Ray remote task per node to gather all files in-memory; head
writes once per file per node to Lustre. Single MDS pass at the end of
Stage 3, zero per-call Lustre load during the critical path.

## Problem these measurements correct

Earlier runs (run17/18/19) had probes that wrote per-call to Lustre. At
256n this added +634s to wait_proxies — so the measured 2385s was really
~1751s baseline + ~634s probe overhead.

Run20 reduced overhead with buffered writes (still on Lustre).
Run21 moved writes to node-local /tmp and added a single-shot gather.
Both fixes together bring overhead below the run-to-run noise floor.

## Corrected wait_proxies

| Scale | Baseline (run16, no probes) | run21 (all probes, clean arch) | Overhead |
|---|---:|---:|---:|
| 32n | 21.81s | 20.58s | noise |
| 64n | 93.60s | 91.86s | noise |
| 128n | 393.84s | **393.87s** | +0.01% |
| 256n | 1751.52s | TBD (prod queue) | TBD |

## Clean per-proxy scaling (run21, overhead < 1%)

| Metric | 32n | 64n | 128n | Pattern |
|---|---:|---:|---:|---|
| Total GCS lookups (cluster) | 42,000 | 146,000 | **589,500** | ~3.5× per doubling |
| Total GCS time (cluster, s) | 561 | 4,477 | **48,339** | ~10× per doubling |
| Per-call mean (ms) | 13.4 | 30.7 | **82.0** | ~2.5× |
| Per-call median (ms) | 0.044 | 0.044 | 0.047 | **FLAT (cached path)** |
| Per-call p95 (ms) | 68 | 209 | 251 | ~3× |
| Per-call max (ms) | 363 | 918 | **2,349** | ~3× |
| Per-proxy mean calls | 1,313 | 2,281 | **4,606** | ~2× |
| Per-proxy max calls | 11,500 | 20,000 | **25,000** | ~2× |
| **Per-proxy mean total GCS time (s)** | 17.5 | 69.9 | **377.7** | ~4-5× per doubling |
| **Per-proxy max total GCS time (s)** | 23.4 | 95.6 | **395.6** | ~4× per doubling |
| update_deployment_targets total (s) | 47.5 | 151.7 | **1,997.8** | ~4× per doubling |
| Per-proxy max update time (s) | 6.3 | 7.8 | **112.0** | stairstep |

## The clean picture: per-proxy max GCS time ≈ wait_proxies

At 128n: per-proxy max total GCS time = **395.6s**, observed
wait_proxies = **393.87s**. Match to within 0.4%.

**The GCS actor-handle lookups account for essentially 100% of
wait_proxies at 128n.** Earlier measurements that suggested a
~43% "unaccounted" remainder were actually measurement artifacts from
per-call probe overhead inflating the denominator.

## Scaling signature revealed cleanly

Per-call median is **44 microseconds at every scale** — this is Ray's
actor-handle cache fast-path. When the proxy's LongPollClient receives
an update for a replica it already knows about, the lookup is essentially
free.

Per-call mean climbs 13.4 → 30.7 → 82.0 ms because the slow-path (cache
miss) gets more contended as scale grows. The max jumps even more:
363 → 918 → 2349 ms. At 128n, a single worst-case lookup blocks the
proxy's asyncio loop for 2.3 seconds.

Per-proxy call count **doubles per node-doubling** (linear with replicas
the proxy needs to track). But per-proxy TOTAL GCS time scales
**~4-5× per doubling** because both the count AND the per-call time
grow.

## How `update_deployment_targets` decomposes the per-proxy budget at 128n

- Total `update_deployment_targets` calls = 378 across 128 proxies (≈3 per proxy)
- Per-call mean = 5.3s, max = 110.9s
- Per-proxy max total time in updates = **111s**

The per-proxy max time in updates (111s) is much less than per-proxy
max GCS time (395s). This means each proxy got multiple broadcasts during
the run, with GCS time accumulating across them. The worst proxy spent
~395s in GCS over ~3 broadcasts, each doing ~1500 lookups.

## Architectural fix (unchanged)

**Ship pre-resolved actor handles in the broadcast payload.** That change
would:

1. Eliminate the O(N_replicas) per-proxy GCS lookup (biggest win)
2. Shrink update_deployment_targets per-call time by ~100× at 128n
3. Wait_proxies at 128n would drop from 394s to the non-GCS floor (~38s
   spawn delay + small constant for broadcast fan-out + first-time handle
   resolution)

## Instrumentation architecture (the clean version)

```
During run:
  ProxyActor.__init__             → /tmp/aurora_inst/proxy_init_<host>_<pid>.json (1 write/proxy)
  ProxyActor.ready()              → appends to same file
  RunningReplicaInfo.get_actor_handle  → buffered CSV (flush every 500 calls)
  RequestRouter.update_deployment_targets → /tmp/aurora_inst/router_updates_<pid>.jsonl
  ServeController.run_control_loop_step  → /tmp/aurora_inst/controller_ticks_<pid>.jsonl (head only)
  DeploymentStateManager.update() → /tmp/aurora_inst/dsm_updates_<pid>.jsonl (head only)

End of Stage 3 (after wait_for_proxies_serving returns):
  aurora_serve._collect_instrumentation_all runs Ray remote task per node
  → each task reads /tmp/aurora_inst/*, returns bytes in-memory to head
  → head writes each file ONCE to $AURORA_RUN_LOG_DIR/instrumentation/<host>/<file>

Total Lustre ops: ~1238 files across 32 nodes = ~1.4 MB total per scale run.
One open/write/close per file.
```

## Commits

- Overlay:
  - `a7fc2b7` — common.py buffered writes
  - `1504cda` — all probes write to /tmp (was Lustre)
- Main:
  - `a1befd1` — aurora_serve._collect_instrumentation_all

## Reproducibility

```bash
module load frameworks go/1.25.3
PYTHONPATH=/home/wenyiw/aurora_rayserver python3 -m eval.cli run materialize weakscaling_nullcompute_proxy
# edit job.pbs to use reservation queue if needed
qsub /lus/flare/.../runN/{scale}-nodes/job/job.pbs

# analyze
python3 tools/analyze_scaling.py runN/{32,64,128,256}-nodes
python3 tools/analyze_probes.py runN/{32,64,128,256}-nodes
python3 tools/analyze_controller_ticks.py runN/{32,64,128,256}-nodes
python3 tools/analyze_dsm.py runN/{32,64,128,256}-nodes
```
