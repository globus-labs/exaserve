# Weak-scaling Short v3 — Progress Log

## Root Causes Discovered

### 1. Instrumentation overhead (solved in v3 commit 8cbc66b)
`_collect_replica_traces()` read/deleted per-replica trace files on Lustre,
adding ~615s at 128 nodes, ~1000s at 256 nodes. Fix: `EXASERVE_SCALING_TRACE=0`
default in `launch_cluster.sh`; all tracer I/O short-circuits when disabled.
Propagated via Ray actor `runtime_env`.

### 2. Rayon thread pool exhaustion (solved in commit 05b7bc3)
**Symptom**: At 128+ nodes, replay phase hangs with replay.log=0 bytes and
repeated `ThreadPoolBuildError { kind: IOError(Os { code: 11, kind: WouldBlock }) }`
from rayon-core in replica processes. Subsequent panics cascade with
`GlobalPoolAlreadyInitialized`.

**Root cause**: At 128 nodes × 12 replicas = 1536 Ray actors, each replica
maintains ~1500 gRPC connections. When the HF Rust tokenizer lazily creates
its rayon thread pool on first request, it tries to spawn ~nproc (208 on
Aurora) threads and hits EAGAIN because the process is already near the
thread/nproc limit.

**Fix**:
- `RAYON_NUM_THREADS=1` (default in launch_cluster.sh)
- `TOKENIZERS_PARALLELISM=false` (default in launch_cluster.sh)
- Both propagated into Ray actor `runtime_env` via `build_actor_runtime_env`
- HAProxy short v3 spec uses `num_go_procs=4` (was 8) to reduce head-node
  thread pressure from client-side Go procs

### 3. SSH-based testing breaks MPI (workflow lesson)
Running `mpiexec` from an SSH session into a PBS job's head node fails with
`VNI request failed [409]: Failed to find specified job`. Aurora's Slingshot
fabric allocates VNI per-PBS-job and it's not accessible via SSH. **Must
use real qsub for multi-node tests**, not sleep+SSH workflow.

### 4. Lustre trailing garbage in result files (solved in commit 680adb7)
On Lustre, `open("w")` sometimes leaves stale bytes past the new file
boundary when the new content is smaller than the old. `_validate_replay_results`
now uses `json.JSONDecoder.raw_decode` to tolerate trailing garbage and
rewrites clean content in place.

## Tests Completed

| Test | Spec | Snapshot | Result |
|---|---|---|---|
| 1-node v3 (capacity) | haproxy_short_v3 | df48571 | ✓ 107 RPS, 4/4 runs (with trailing-garbage fix) |
| 128-node v3 run0 | haproxy_short_v3 | df48571 | ✗ rayon panic (pre-rayon-fix) |
| 128-node v3 run1 | haproxy_short_v3 | 05b7bc3 | ✓ 844800 reqs, 0 errors, setup 487s |
| 256-node v3 run1 | haproxy_short_v3 | 05b7bc3 | in progress (8431216) |

## Baseline Comparison (HAProxy short)

| Metric | v0 (f2753ab) | v3 run1 (05b7bc3) | Δ |
|---|---|---|---|
| 128n Stage 1 | 50.16s | 44.76s | -5.4s |
| 128n Stage 3 | 445.51s | 442.25s | -3.3s |
| 128n Total | 496.62s | 487.02s | **-9.6s (1.9% faster)** |

Setup time regression from instrumentation is fully eliminated.

## 256-Node Ray Init Timing

Deploying 3072 replicas (256 nodes × 12 replicas/node) at 256 nodes:

| Stage | Duration | Notes |
|---|---|---|
| Stage 1 (Ray cluster init) | ~45s | Consistent across all runs |
| Stage 2 (Model resolution) | ~0s | Models pre-staged to /tmp/hf_home |
| Stage 3 (Deploy 3072 replicas via serve.run) | **1787s (~30 min)** | Highly variable; first attempt timed out at 1800s, succeeded at 1787s on retry |
| **Total init** | **~30.5 min** | |

The original `EXASERVE_SERVE_READY_TIMEOUT_S=1800` was too tight — the successful
run completed Stage 3 with only 13s to spare. Timeout is now configurable via
env var, defaulting to 3600s.

## Three-Dispatch Comparison (v3 final results)

Three dispatch modes tested across 1–256 nodes, all with identical server
config (Llama-3-8B, 12 replicas/node, 64in/64out, target 110 RPS/node):

| Nodes | HAProxy RPS | Direct-MPI RPS | Direct-Fat RPS | Ideal RPS |
|---|---|---|---|---|
| 1 | 107.1 | 105.9 | 107.1 | 107 |
| 2 | 214.2 | 214.1 | 214.2 | 214 |
| 4 | 428.1 | 427.6 | 428.0 | 428 |
| 8 | 854.6 | 855.8 | 856.1 | 856 |
| 16 | 1,711 | 1,681 | 1,712 | 1,712 |
| 32 | 3,366 | 3,365 | 3,388 | 3,424 |
| 64 | 6,712 | 6,785 | 6,830 | 6,848 |
| 128 | 12,142 | 13,426 | 12,105 | 13,696 |
| 256 | 13,730 | 26,920 | 13,855 | 27,392 |

**Dispatch modes:**
- **HAProxy**: single HAProxy on head node, leastconn balancer, 4 go-procs
- **Direct-MPI**: 1 MPI rank per node, each rank runs 8 go-procs, hash-shard
- **Direct-Fat**: single python on head node, 4 go-procs, hash-shard directly to all backends (no HAProxy, no MPI)

**Key findings:**
1. All three modes are indistinguishable up to 64 nodes (~6800 RPS)
2. At 128n, Direct-MPI (13,426) pulls 10% ahead of the head-node modes
3. At 256n, Direct-MPI scales linearly (26,920 RPS, 99% efficiency) while
   both head-node modes plateau at ~13,800 RPS (51% efficiency)
4. **Direct-Fat and HAProxy converge to the same ceiling** — the bottleneck
   is the single head node running a fat client process, not HAProxy itself
5. The "lower" p50 latency at 256n for HAProxy/Direct-Fat (1609–1615ms vs
   1853ms for Direct-MPI) is a queueing artifact: backends are under-loaded
   at half the target rate, so requests queue less

Plot: `findings/weakscaling_three_dispatch_v3.png`

## 512-Node Attempt — Ray GCS Failure

**Attempted**: Direct-MPI 512n (job 8433728, prod queue, 4hr walltime, snapshot e5b1569b)

**Result**: Stage 3 failed — `serve.run()` raised `RuntimeError: Deploying application default failed`.

**Root cause**: Ray GCS (Global Control Service) on the head node was overwhelmed by 512 nodes
(6144 replicas). Multiple raylets reported `Failed to get cluster ID from GCS server: TimedOut:
RPC error: Deadline Exceeded` (observed on 17+ nodes). When enough workers fail GCS
connectivity, replica deployment fails outright — not a timeout, but an active rejection.

**Key log lines**:
```
gcs_client.cc:205: Failed to get cluster ID from GCS server: TimedOut: RPC error: Deadline Exceeded
  [repeated 17x across cluster]
ray.exceptions.RaySystemError: System error: Failed to connect to GCS.
RuntimeError: Deploying application default failed: Failed to update the deployments [...]
```

**Init timeline (512n)**: Stage 1 = 45s, Stage 2 = 0s, Stage 3 = crashed ~55 min in.

**Conclusion**: 256 nodes (3072 replicas) is the practical ceiling for single-head-node
Ray Serve on Aurora with the current Ray version and GCS configuration. 512 nodes
(6144 replicas) exceeds the GCS connection capacity. Scaling beyond 256n would
require either multi-head Ray clusters, GCS sharding, or a fundamentally different
serving architecture.
