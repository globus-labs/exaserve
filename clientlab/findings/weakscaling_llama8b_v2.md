# Weak-Scaling: Llama-3-8B on Aurora

## Setup

- Model: meta-llama/Meta-Llama-3-8B-Instruct, TP=1, 12 replicas/node
- Workload: 2048 input tokens, 128 output tokens, non-streaming
- Rate: 17.5 rps/node (70% of 25 rps saturation from Phase 0)
- Duration: 60s per run
- Client: 1 Go process, go_concurrency=1024, dest=proxy (LiteLLM, 2 workers)
- Nodes swept: 1, 2, 4, 8, 16, 32 (64 failed — exceeded debug-scaling queue limit)

## Results

| Nodes | Target RPS | Achieved RPS | Efficiency | Errors | P50 (ms) | P99 (ms) | Dispatch (s) |
|-------|-----------|-------------|------------|--------|----------|----------|-------------|
| 1 | 17.5 | 16.4 | 100% | 0 | 6504 | 13388 | 60.0 |
| 2 | 35.0 | 32.9 | 100% | 0 | 6406 | 14750 | 60.0 |
| 4 | 70.0 | 65.1 | 99% | 0 | 6392 | 14521 | 60.0 |
| 8 | 140.0 | 131.0 | 100% | 2 | 6607 | 11592 | 60.0 |
| 16 | 280.0 | 186.5 | 71% | 4 | 4888 | 10384 | 84.6 |
| 32 | 560.0 | 221.6 | 42% | 24 | 4417 | 8057 | 148.0 |

## Analysis

### Linear scaling up to 8 nodes

From 1 to 8 nodes, throughput scales nearly perfectly:
- 1 node: 16.4 rps → 8 nodes: 131.0 rps (8.0x speedup, 100% efficiency)
- Dispatch completes in 60s (exactly the trace duration) — no backpressure
- P50 latency stable at ~6.5s, errors negligible (0-2)
- The system is compute-bound: each node adds proportional throughput

### LiteLLM proxy bottleneck at 16+ nodes

At 16 nodes (280 rps target), efficiency drops to 71%:
- Dispatch takes 84.6s instead of 60s — the client can't push 280 rps through the proxy
- Achieved 186.5 rps, leaving 94 rps on the table
- LiteLLM with 2 workers saturates around 200 rps

At 32 nodes (560 rps target), efficiency collapses to 42%:
- Dispatch takes 148s (2.5x the trace) — severely proxy-bottlenecked
- Achieved only 221.6 rps — nearly the same as 16 nodes
- 24 errors (likely timeouts from queue buildup)

### Latency improves under proxy saturation

Counter-intuitively, P50 and P99 **decrease** at 16-32 nodes:
- 8 nodes: P50=6607ms, P99=11592ms
- 32 nodes: P50=4417ms, P99=8057ms

This is because the proxy throttles the request rate, reducing the per-replica
queue depth. Fewer concurrent requests per replica → less queueing → lower latency.
The latency improvement is artificial — it comes from the proxy starving the
servers, not from the servers being faster.

### Proxy capacity estimate

The proxy achieves ~221 rps at 32 nodes (saturated). With 2 LiteLLM workers,
that's ~110 rps/worker. To maintain linear scaling at 32 nodes (560 rps target),
we'd need ~6 workers. For 64 nodes (1120 rps), ~11 workers.

## Follow-up: Concurrency starvation diagnosis

The initial 16-node results (186.5 rps via proxy, run2) used `go_concurrency=1024`.
Follow-up profiling revealed this was insufficient:

- With 16 remote backends and ~5s avg latency, the client needs
  `target_rps × avg_latency ≈ 280 × 5 = 1400` concurrent slots.
- At `go_concurrency=1024`, slots fill up and the dispatcher stalls (78-85s for
  a 60s trace).
- At `go_concurrency=2048` with `dest=direct`, the centralized client achieves
  **255 rps (91% efficiency)** at 16 nodes — matching distributed MPI.

| Mode | go_concurrency | 16-node RPS | Efficiency |
|------|---------------|-------------|-----------|
| Via proxy (2w) | 1024 | 186.5 | 67% |
| Centralized direct | 1024 | 204.6 | 73% |
| Centralized direct | 2048 | **255.0** | **91%** |
| Distributed MPI | 1024 | 256.1 | 91% |

The proxy path has not been re-tested with go_concurrency=2048.
See `clientlab/findings/bottleneck_profiling.md` for full analysis.

## Trace generation fix

The original traces (run2) had a tokenizer round-trip bug: `_truncate_prompt`
truncated to N tokens, decoded to text, but vLLM re-tokenized the text to a
different (often larger) count. This caused 6% of requests to exceed
`max_model_len=4096` in the distributed direct test (run8/run11).

Fixed by re-encoding after decode and iteratively trimming until the verified
token count fits within the hard limit. All subsequent runs (run10+) use the
fixed trace generator and show 0 errors.

## Conclusions

1. **Server-side scaling is linear.** Each Aurora node adds ~16.4 rps of
   Llama-3-8B throughput (TP=1, 2048in/128out).

2. **go_concurrency must be sized for the workload.** At N nodes with latency L,
   need `go_concurrency >= rate_per_node × N × L`. Undersizing causes the
   client to become the bottleneck, mimicking a proxy or server issue.

3. **A single centralized client can drive 16 nodes at 91% efficiency** with
   sufficient concurrency (2048), bypassing the proxy via `dest=direct`.

4. **The proxy path (LiteLLM) achieved 67% at go_concurrency=1024** but has not
   been re-tested at 2048. True proxy overhead is unknown.

5. **64-node test failed** — exceeded the debug-scaling queue's node limit.

6. **Streaming disabled** due to LiteLLM's MidStreamFallbackError on repetitive
   model output.

## Raw Data

- Results: `runs/weakscaling_llama8b_v2/run2/`
- Plot: `weakscaling_v2_results.png`
- Phase 0: `clientlab/findings/phase0_server_characterization.md`
