# Weak-scaling Short v3 — Progress Log

## Root Causes Discovered

### 1. Instrumentation overhead (solved in v3 commit 8cbc66b)
`_collect_replica_traces()` read/deleted per-replica trace files on Lustre,
adding ~615s at 128 nodes, ~1000s at 256 nodes. Fix: `AURORA_SCALING_TRACE=0`
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

## Next Steps

1. Wait for 256n v3 run1 to complete
2. Submit remaining node counts (1-64) for haproxy v3 run1 via qsub loop
3. Run direct_short_v3 run1 (num_go_procs=8 since per-node client load)
4. Plot results when all runs complete
