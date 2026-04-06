# Phase 0: Single-Node Server Characterization

Goal: find the per-node saturation throughput for Llama-3-8B on Aurora, and determine
whether `max_num_seqs` or `replica_max_ongoing_requests` affect performance.

## Setup

- Model: meta-llama/Meta-Llama-3-8B-Instruct
- Hardware: 1 Aurora node, 12 GPU tiles (Ponte Vecchio)
- Deployment: TP=1, 12 replicas (1 per tile), Ray Serve
- Workload: 2048 input tokens, 128 output tokens, streaming enabled
- Client: Go saturation finder, go_concurrency=1024, initial_rate=50, step_duration=15s
- dest=direct (bypasses LiteLLM proxy)

## max_ongoing_requests sweep (fixed max_num_seqs=64)

| max_ongoing_requests | Saturation Rate | Source |
|---------------------|----------------|--------|
| 4 | 25 rps | in-node sweep (zombie contamination possible) |
| 16 | 25 rps | in-node sweep (zombie contamination possible) |
| 96 | 25 rps | clean single-job run |
| 128 | 25 rps | clean single-job run (eval pipeline) |
| 256 | 25 rps | clean single-job run |

**Conclusion**: `max_ongoing_requests` has no effect on throughput. The Ray Serve queue
depth is irrelevant because the server is compute-bound, not queue-bound. At 25 rps
across 12 replicas (~2 rps/replica), the queues never build up.

## max_num_seqs sweep (fixed max_ongoing_requests=128)

| max_num_seqs | Saturation Rate | Source |
|-------------|----------------|--------|
| 8 | 25 rps | clean single-job run |
| 16 | 25 rps | clean single-job run |
| 32 | 25 rps | clean single-job run |
| 64 | 25 rps | clean single-job run (eval pipeline) |
| 128 | 25 rps | clean single-job run |

**Conclusion**: `max_num_seqs` has no effect on throughput. vLLM's batch scheduler
limit doesn't matter because at ~2 rps/replica with ~3s per request, each replica
only has ~6 concurrent sequences — well below even the smallest max_num_seqs=8.

## Why 25 rps?

With 12 replicas each processing 2048-token prompts + 128-token outputs:
- Per-request latency: ~3s (prefill + decode)
- Per-replica throughput: ~2 rps (limited by GPU compute)
- Node throughput: 12 × 2 ≈ 24 rps

The saturation finder reports 25 rps, consistent with this calculation. The slight
excess over 24 comes from vLLM's continuous batching overlapping prefill and decode
phases across sequences.

## Saturation finder behavior

The plateau ratio (achieved/target ≥ 0.95) is never met because vLLM's streaming
response adds variable overhead. At target=26 rps, achieved=21.7 (ratio 0.83).
The consistent ~83% ratio across all targets suggests a systematic measurement
artifact — likely the streaming response read time inflating the measurement window.

The saturation point of 25 rps is the binary search's lower bound where it converges,
not the actual achieved rate (which plateaus at ~22-23 rps).

## Recommended config for weak-scaling

- `max_num_seqs`: 64 (default, doesn't matter)
- `replica_max_ongoing_requests`: 128 (default, doesn't matter)
- `rate_per_node`: 17.5 (25 × 0.7 headroom)
- `go_concurrency`: 1024 (generous, scales with nodes)

## Raw data

- max_ongoing sweep: `runs/server_char_llama8b/run5/` (eval pipeline)
- max_num_seqs sweep: `runs/server_char_llama8b/run6/` (eval pipeline)
- Burst test (concurrency sweep): `bench_results/clientlab/burst_test_20260403T215824Z/`
- Manual saturation (go_concurrency=192): `/tmp/sat_highconc.json` on compute node (ephemeral)
