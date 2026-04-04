# Bottleneck Profiling: Why Weak-Scaling Degrades at 16+ Nodes

## Question

Weak-scaling achieves 100% efficiency up to 8 nodes (131 rps) but drops to 71%
at 16 nodes (186 rps vs 280 target) and 42% at 32 nodes (222 rps vs 560 target).
Is this the LiteLLM proxy, the Go client, or the serving layer?

## 1-Node Profiling (proxy vs direct)

Ran phase traces (100% sampling, httptrace enabled) on 1 node at various rates:

### Per-request latency (17.5 rps, within capacity)

| Path | TTH p50 | TTH p99 | Dispatch lag p99 |
|------|---------|---------|-----------------|
| Via proxy | 3074.6ms | 3264.7ms | 0.6ms |
| Direct | 3082.5ms | 3280.3ms | 0.6ms |

**Proxy overhead: ~0ms.** Both paths are identical at low rate.

### Per-request latency (200 rps, overloaded)

| Path | TTH p50 | TTH p99 | Dispatch lag p99 |
|------|---------|---------|-----------------|
| Via proxy | 9358ms | 12671ms | 10252ms |
| Direct | 9321ms | 12206ms | 9841ms |

**Proxy overhead: ~40ms p50, ~465ms p99.** Still negligible vs 9s inference.

### Throughput comparison at increasing rates

| Target Rate | Proxy Achieved | Direct Achieved |
|-------------|---------------|-----------------|
| 25 rps | 20.8 | 20.8 |
| 50 rps | 40.7 | 40.6 |
| 100 rps | 75.1 | 77.2 |
| 200 rps | 92.1 | 92.5 |

**Proxy and direct are identical.** The ceiling at ~92 rps is vLLM server capacity,
not proxy capacity. On 1 node, the proxy is not a bottleneck at any rate.

## LiteLLM worker count (4 vs 2 workers)

| Config | 16-node achieved | 32-node achieved |
|--------|-----------------|-----------------|
| 2 workers | 186.5 rps | 221.6 rps |
| 4 workers | 192.2 rps | 198.3 rps |

4 workers performed **worse** than 2, ruling out worker count as the bottleneck.

## What we ruled out

1. **LiteLLM per-request overhead**: ~0ms on 1 node at any rate
2. **LiteLLM throughput on 1 node**: matches direct path exactly
3. **LiteLLM worker count**: 4 workers ≈ 2 workers (or worse)
4. **Go client concurrency**: 1024 slots, only ~186 used at 16 nodes
5. **vLLM compute**: 25 rps/node capacity is correct (verified in Phase 0)

## What's left to test

The bottleneck is specific to **multi-node configurations** — it doesn't appear
on 1 node. Possible causes:

1. **Network latency to remote backends**: Proxy connects to backends on other
   nodes over HSN. If each remote connection adds 5-10ms of setup/routing,
   and the proxy can only maintain N concurrent backend connections, this could
   cap throughput.

2. **Proxy backend connection pool**: LiteLLM may use a limited connection pool
   per backend. With 16 backends and a small pool, requests queue waiting for
   connections.

3. **Ray Serve routing at scale**: With 192 replicas (16 × 12), the
   power-of-two-choices router's queue_len cache may become stale, causing
   imbalanced routing.

4. **Single client process**: One Go process dispatching 280 rps through one
   proxy host. Network/socket buffers may saturate.

## Next step

Run profiling on a 16-node cluster with phase traces to measure per-request
TTH through the proxy. Compare with `dest=direct` (Go client distributes
directly to all backend nodes). This isolates whether the bottleneck is
proxy→backend or client→proxy.

## Raw data

- 1-node profiles: `bench_results/clientlab/profile_1n_proxy/`, `profile_1n_direct/`
- Throughput comparison: `bench_results/clientlab/profile_1n_proxy_200rps/`, `profile_1n_direct_200rps/`
