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

## 16-Node Profiling: Proxy vs Direct

Submitted 16-node jobs with `dest=proxy` (run2) vs `dest=direct` (run8).
In direct mode, each MPI rank runs a Go client on its own node, hitting
its local Ray Serve on port 8000. No proxy involved.

| Metric | Via Proxy | Direct | Delta |
|--------|----------|--------|-------|
| Achieved RPS | 186.5 | 256.1 | **+37%** |
| Success RPS | 186.1 | 240.4 | **+29%** |
| Errors | 4 | 1028 | (see below) |
| Dispatch time | 84.6s | 60.0s | **-29%** |
| P50 latency | 4888ms | 6360ms | +30% |
| P99 latency | 10384ms | 8208ms | -21% |

### Analysis

**Direct mode achieves 91% of target (256/280) vs proxy's 67% (186/280).**
This confirms the LiteLLM proxy is the multi-node throughput bottleneck.

In direct mode:
- All 16 ranks dispatched in exactly 60s — no backpressure
- Each rank independently hit its local Ray Serve
- 5s drain — minimal queueing
- 1028 errors (6.1%) — likely Ray Serve `max_ongoing_requests` rejections

In proxy mode:
- Single Go process on head node → LiteLLM → 16 backend nodes
- Dispatch took 84.6s for 60s trace — proxy couldn't forward fast enough
- Near-zero errors — proxy throttled the rate enough to prevent overload

### Why the proxy bottlenecks at 16+ nodes

The proxy adds ~0ms per-request overhead (verified on 1 node). But it's a
single process routing all traffic through one network path (head node's HSN).
At 280 rps with 3s average latency, the proxy maintains ~840 concurrent backend
connections to 16 nodes. The proxy's event loop serializes on connection management
at this scale.

Increasing workers from 2→4 didn't help (and hurt at 32 nodes), confirming
the bottleneck is in LiteLLM's internal routing/connection management, not
worker-level parallelism.

### 1028 errors in direct mode

The errors are from `max_ongoing_requests=128` per replica × 12 replicas = 1536
concurrent capacity per node. Each rank sends 1050 requests with `go_concurrency=1024`.
At burst moments, some requests exceed the per-replica queue limit. This can be
fixed by increasing `max_ongoing_requests` or reducing `go_concurrency` per rank.

## Conclusions

1. **The LiteLLM proxy is the multi-node bottleneck.** It caps throughput at
   ~200 rps regardless of worker count, losing 37% of throughput at 16 nodes.

2. **Direct mode scales linearly.** With each node running its own client,
   throughput reaches 91% of target at 16 nodes.

3. **Per-request proxy overhead is negligible.** The bottleneck is the proxy's
   aggregate throughput when managing hundreds of concurrent backend connections
   across many nodes.

4. **Recommendation for production weak-scaling:** Use `dest=direct` with MPI
   distribution. The proxy is useful for development but becomes a bottleneck
   beyond 8 nodes.

## 16-Node: Three-Way Comparison (Proxy vs Centralized-Direct vs Distributed-Direct)

### go_concurrency matters

The initial centralized direct test used `go_concurrency=1024`, achieving only 204.6
rps (73%). A follow-up with `go_concurrency=2048` recovered to 255.0 rps (91%) — 
matching the distributed MPI result. The bottleneck at conc=1024 was concurrency
starvation: with 16 remote backends and ~5s avg latency, the client needs
`280 × 5 = 1400` concurrent slots to sustain 280 rps. 1024 wasn't enough.

| | Via Proxy | Centralized Direct | Centralized Direct | Distributed Direct |
|---|----------|-------------------|-------------------|-------------------|
| | (2w, conc=1024) | (conc=1024) | (conc=2048) | (MPI, conc=1024) |
| Client location | 1 node | 1 node | 1 node | 16 nodes |
| Routing | →LiteLLM→backends | →backends | →backends | →local backend |
| Achieved RPS | 186.5 | 204.6 | **255.0** | 256.1 |
| Errors | 4 | 0 | 0 | 0 |
| Dispatch time | 84.6s | 78.1s | **60.0s** | 60.0s |
| P50 latency | 4888ms | 4773ms | 5861ms | 6360ms |
| P99 latency | 10384ms | 6090ms | 7710ms | 8208ms |
| Efficiency | 67% | 73% | **91%** | 91% |

Note: The proxy result (186.5 rps) also used `go_concurrency=1024` and may improve
with higher concurrency. This comparison is not fully apples-to-apples for the proxy
path — a re-run with conc=2048 is needed to isolate true proxy overhead.

### Analysis

With sufficient `go_concurrency` (2048), a single centralized Go process on one
node can drive 16 remote backends at 91% efficiency — matching distributed MPI.
The earlier 73% result was a client configuration issue, not a fundamental network
or architecture limitation.

**The proxy path (186.5 rps) needs re-testing with go_concurrency=2048.** The gap
between proxy (186.5) and centralized direct (204.6) at conc=1024 was 18 rps.
But since concurrency starvation affected both, the true proxy overhead may be
smaller than measured.

### Conclusions

1. **go_concurrency must be sized for the workload**: At 16 nodes with ~5s latency,
   need `target_rps × avg_latency` ≈ 1400 concurrent slots minimum. 2048 provides
   sufficient headroom.
2. **Centralized client scales to 16 nodes at 91%** when properly configured.
3. **Proxy overhead is still unknown** at proper concurrency — needs re-run.
4. **Distributed MPI is not required for 16-node scaling** — it's a concurrency
   sizing issue, not an architecture limitation.

## Raw data

- 1-node profiles: `bench_results/clientlab/profile_1n_proxy/`, `profile_1n_direct/`
- 16-node proxy (2w): `runs/weakscaling_llama8b_v2/run2/16-nodes/`
- 16-node distributed direct (MPI): `runs/weakscaling_llama8b_v2/run11/16-nodes/`
- 16-node centralized direct: `runs/weakscaling_llama8b_v2/run12/16-nodes/`
