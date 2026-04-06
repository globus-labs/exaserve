# Centralized Client Scaling Limits

Theoretical max nodes a single-node HTTP/1.1 client can drive, given the
two independent ceilings discovered in our characterization work.

## Formula

```
max_nodes = min(
    floor(0.8 × port_range / (rps_per_node × avg_latency)),
    floor(dispatch_ceiling / rps_per_node)
)
```

Where:
- `port_range` = ephemeral port count (28232 on Aurora, from `/proc/sys/net/ipv4/ip_local_port_range`)
- `rps_per_node` = sustained requests/sec the server delivers per node (from Phase 0 saturation)
- `avg_latency` = average per-request latency in seconds
- `dispatch_ceiling` ≈ 300K rps (empirical, 12 Go procs, kernel TCP bound on Aurora)

## Ceiling 1: Connection limit (port exhaustion)

Each in-flight request holds one TCP connection (HTTP/1.1). Max concurrent
connections = `0.8 × port_range = 22586` (20% headroom for TIME_WAIT safety).

```
max_rps_connection = 22586 / avg_latency
max_nodes_connection = max_rps_connection / rps_per_node
```

This is the binding constraint for real inference workloads (latency ≥ 100ms).

## Ceiling 2: Dispatch throughput (kernel TCP)

Even with unlimited connections, the single-node kernel TCP stack saturates
at ~300K rps for 12 Go procs on Aurora (measured in dispatch ceiling tests).

```
max_rps_dispatch ≈ 300K
max_nodes_dispatch = 300K / rps_per_node
```

This is the binding constraint for fast servers (latency < 1ms).

## Predictions for Aurora

| Model | Latency | RPS/node | Conn limit | Dispatch limit | Max nodes |
|-------|---------|----------|-----------|---------------|-----------|
| Llama-3-8B (2048in/128out) | 3s | 25 | 301 | 12000 | **301** |
| Llama-3-8B (512in/32out) | 0.5s | 150 | 300 | 2000 | **300** |
| Llama-3-70B (2048in/128out) | 15s | 5 | 301 | 60000 | **301** |
| Fast model (100ms latency) | 0.1s | 1000 | 225 | 300 | **225** |
| Very fast model (10ms) | 0.01s | 10000 | 2258 | 30 | **30** |

For typical LLM inference (latency ≥ 0.5s), the connection limit caps at
~300 nodes regardless of model speed. This is because rps_per_node and
avg_latency are related via Little's Law: `rps_per_node ≈ concurrency / avg_latency`,
so the connection limit simplifies to `port_range / concurrency_per_node`.

For very fast models (latency < 10ms), the dispatch ceiling becomes the bottleneck.

## Beyond these limits

- **Distributed MPI client** (`dest=direct`): each MPI rank on its own node
  drives local backends. Eliminates both ceilings. Scales to any node count.
- **HTTP/2**: multiplexes streams over one TCP connection, removing the port
  exhaustion ceiling. Not implemented in current stack (separate project).
- **More client nodes**: multiple centralized clients behind a load balancer,
  each driving a subset of backends.

## Empirical validation

- 16-node centralized direct at go_concurrency=2048: **255 rps** (91% of 280 target)
  — consistent with formula: `22586 / 5s_latency = 4517 rps >> 280 target`
  (connection limit not reached, throughput limited by server capacity)
- 16-node via LiteLLM proxy: **186 rps** (67%) — proxy adds ~10% overhead on top
- Dispatch ceiling validated at 12 procs: **300K rps** with stub server

## Data sources

- Connection characterization: `clientlab/findings/client_dispatch_ceiling.md`
- Multi-node profiling: `clientlab/findings/bottleneck_profiling.md`
- Phase 0 server characterization: `clientlab/findings/phase0_server_characterization.md`
- Weak-scaling results: `clientlab/findings/weakscaling_llama8b_v2.md`
