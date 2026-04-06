# Anyscale Ray Serve Production Architecture: Findings

Date: 2026-04-06

## Summary

Anyscale's recommended production serving stack is **HAProxy + Ray Serve + vLLM**, where HAProxy
**replaces** the Ray Serve Python HTTP proxy entirely (not fronts it). This was announced in
March 2026 with Ray 2.55+.

## Architecture

### Default Ray Serve (what our baseline currently uses)

```
Client --> Ray Serve Python Proxy (port 8000, Uvicorn/Starlette)
       --> Ray actor RPC (pickle serialization)
       --> VLLMWorker ingress deployment
       --> vLLM engine (GPU)
```

The Python proxy on port 8000 is a Uvicorn/Starlette server that terminates HTTP, deserializes
the request, and forwards it to the ingress deployment via Ray Core actor RPC. This proxy is
the bottleneck at high concurrency.

### Anyscale's recommended setup (HAProxy bypass, Ray 2.55+)

```
Client --> HAProxy
       --> direct HTTP to ingress replica internal servers (port ~30000 range)
       --> gRPC (not Ray Core RPC)
       --> LLMServer deployment (vLLM engine, GPU)
```

Key differences:
1. HAProxy routes **directly to internal HTTP servers** on ingress replicas (~30000 ports),
   completely bypassing the Python proxy on port 8000
2. Inter-deployment communication uses **gRPC** instead of Ray Core actor RPC
3. The Python proxy is not used at all in the request path

### Two-Deployment Pattern (`ray.serve.llm`)

Anyscale's `ray.serve.llm` API uses a two-deployment architecture:

- **OpenAiIngress** (CPU-only): FastAPI-based, handles OpenAI protocol, model routing, LoRA
  multiplexing. Recommended 2:1 ratio to LLMServer replicas.
- **LLMServer** (GPU): Wraps one vLLM engine instance per replica.

Source: https://docs.ray.io/en/latest/serve/llm/architecture/overview.html

> "When the ingress makes an RPC call to LLMServer through the deployment handle,
> it can reach any replica across any node. However, the default request router
> prioritizes replicas on the same node to minimize cross-node RPC overhead."

## Evidence: Python Proxy is the Bottleneck

### Anyscale's own measurements

Source: https://www.anyscale.com/blog/ray-serve-inference-lower-latency-higher-throughput-haproxy

> "When HAProxy is enabled, each ingress deployment replica starts an HTTP server
> internally. HAProxy is notified of these servers by the ServeController, and can
> directly route requests to them."

> "Ingress deployments now contain HTTP servers with ports in the :30000 range,
> to which HAProxy routes incoming requests."

Performance gains with HAProxy (vs Python proxy):
- Unary workloads: **up to 2.0x throughput**
- Streaming workloads: **1.4x throughput**
- Combined with gRPC transport: **up to 11.1x throughput** (unary), **8.9x** (streaming)
- P99 latency reduction: **75% to 88%**

The environment variable `RAY_SERVE_USE_GRPC_BY_DEFAULT` enables gRPC transport between
deployments.

### Legacy proxy capacity

Source: https://docs.ray.io/en/releases-1.12.0/serve/performance.html (Ray 1.12 docs)

The head node proxy was documented as handling ~3,000 QPS. With EveryNode mode, this
distributes across N nodes, but a single-entry-point constraint funnels all traffic through
one proxy.

## Ray Serve's EveryNode Proxy: What It Actually Does

Each EveryNode proxy is a **cluster-wide load balancer** (not local-only):
- A request hitting node A's proxy CAN route to replicas on node B
- Routing priority: same node > same AZ > any replica (fallback)
- Controlled by `RAY_SERVE_PROXY_PREFER_LOCAL_NODE_ROUTING` (default=1)
  and `RAY_SERVE_PROXY_PREFER_LOCAL_AZ_ROUTING` (default=1)

Source: https://docs.ray.io/en/latest/serve/advanced-guides/performance.html

> "If replicas at a higher priority level are busy or unavailable, the system
> automatically falls back to the next level."

The purpose of EveryNode is to distribute the proxy bottleneck across N nodes. The intended
usage is an external LB (HAProxy/nginx/ALB) fanning out to multiple EveryNode proxies.

Source: https://docs.ray.io/en/latest/serve/architecture.html

**Caveat**: EveryNode only runs proxies on nodes with at least one replica. Nodes without
replicas get no proxy, which causes issues with Kubernetes Services that round-robin across
all pods.

Source: https://github.com/ray-project/ray/issues/42701

## What Ray Serve Still Does (with HAProxy)

HAProxy replaces the HTTP ingress, but Ray Serve remains for:
- Replica lifecycle management (scaling, health checks, restarts)
- ServeController: notifies HAProxy of replica addresses as they change
- Deployment handles for inter-deployment routing
- Autoscaling policy
- Multi-model routing

Ray Serve is NOT removed — its weakest component (Python HTTP proxy) is replaced while
keeping the orchestration layer.

## Controller Scalability Limits

Source: https://github.com/ray-project/ray/issues/60680

At 2048 replicas: controller loop duration = 2.9 seconds, replica metrics delay = 10.7
seconds. The bottleneck is autoscaling metrics aggregation. Recommendation: >2000 replicas
needs architectural improvements (hierarchical aggregation).

## RayLLM / Aviary: Archived

Source: https://github.com/ray-project/ray-llm

RayLLM (formerly Aviary) was archived March 2025. Replaced by `ray.serve.llm` in core Ray.
The evolution: Aviary (2023) -> RayLLM (2023-2025) -> `ray.serve.llm` (2025+).

## Implications for Our Baseline

### Current baseline is two generations behind
Our baseline uses LiteLLM -> Ray Serve Python proxy (port 8000) -> vLLM replicas.
Anyscale's recommended setup bypasses the Python proxy entirely.

### To make the baseline state-of-the-art:
1. Replace LiteLLM with HAProxy routing to internal replica ports (~30000)
2. Enable gRPC transport between deployments (`RAY_SERVE_USE_GRPC_BY_DEFAULT`)
3. Use the two-deployment pattern (OpenAiIngress + LLMServer) via `ray.serve.llm`
4. **Caveat**: Some optimizations may be Anyscale-proprietary or require Ray 2.55+.
   Need to verify which features are available in upstream open-source Ray.

### Revised framing for paper
Even Anyscale acknowledges the Python proxy is a bottleneck and replaces it with HAProxy +
gRPC. Our MPI system avoids this multi-layer workaround entirely: rank-0 scatter distributes
requests at the network substrate level without a userspace proxy hop. This is a stronger
argument than "Ray has no built-in LB" (which is inaccurate).

## Open-Source Availability (verified 2026-04-06)

All components needed to replicate Anyscale's setup are open-source. No proprietary packages
required.

### Available in stable Ray (2.54.1, PyPI)

- `ray.serve.llm` (OpenAiIngress + LLMServer two-deployment pattern)
  - Install: `pip install "ray[llm]"`
  - Available since Ray 2.44
  - Canonical import: `ray.llm.*` (old `ray.serve.llm.*` still works, deprecated)
  - Source: `python/ray/llm/` in ray-project/ray repo
  - No `anyscale` package dependency

### Merged to ray-project/ray master, NOT in any stable release

- HAProxy bypass (direct routing to internal replica HTTP servers on ~30000 ports)
  - Env var: `RAY_SERVE_ENABLE_HA_PROXY`
  - HAProxy binary bundled in Ray Docker image (compiled from source)
  - ServeController notifies HAProxy of replica addresses dynamically
- gRPC inter-deployment transport (replaces Ray Core actor RPC with protobuf + point-to-point)
  - Env var: `RAY_SERVE_USE_GRPC_BY_DEFAULT`
- Combined throughput optimization bundle
  - Env var: `RAY_SERVE_THROUGHPUT_OPTIMIZED`
- Additional tuning knobs
  - `RAY_SERVE_HAPROXY_TCP_NODELAY` (PR #61468)
  - `RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD`
  - `RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP`
- Documented in: GitHub issue [#61212](https://github.com/ray-project/ray/issues/61212)
  (upcoming 2.55 changes), [#59618](https://github.com/ray-project/ray/issues/59618) (env var catalog)

### Anyscale-proprietary (NOT needed for our baseline)

- Fast model loading and startup optimizations (5.1x faster scaling)
- Multi-AZ scheduling for redundancy
- Per-deployment container images
- Zero-downtime incremental rollouts
- Custom metric dashboards, log search, tracing, alerting
- Managed infrastructure

### How to install pre-release Ray

```bash
# Option 1: Nightly wheels
pip install -U "ray[serve,llm]" --pre --extra-index-url https://s3-us-west-2.amazonaws.com/ray-wheels/latest/

# Option 2: Build from source
git clone https://github.com/ray-project/ray.git
cd ray && python setup.py install
```

## References

1. Anyscale HAProxy blog (March 2026):
   https://www.anyscale.com/blog/ray-serve-inference-lower-latency-higher-throughput-haproxy

2. Anyscale production best practices:
   https://docs.anyscale.com/platform/services/production-best-practices/

3. Ray Serve LLM architecture overview:
   https://docs.ray.io/en/latest/serve/llm/architecture/overview.html

4. Ray Serve architecture docs:
   https://docs.ray.io/en/latest/serve/architecture.html

5. Ray Serve performance tuning:
   https://docs.ray.io/en/latest/serve/advanced-guides/performance.html

6. GitHub issue - proxy locations:
   https://github.com/ray-project/ray/issues/42701

7. GitHub issue - controller scalability:
   https://github.com/ray-project/ray/issues/60680

8. RayLLM (archived):
   https://github.com/ray-project/ray-llm

9. Anyscale Wide-EP and disaggregated serving blog:
   https://www.anyscale.com/blog/ray-serve-llm-anyscale-apis-wide-ep-disaggregated-serving-vllm

10. Ray Serve LLM serving on Anyscale:
    https://docs.anyscale.com/llm/serving

11. GitHub issue - upcoming Ray Serve 2.55+ changes:
    https://github.com/ray-project/ray/issues/61212

12. GitHub issue - Ray Serve env var catalog:
    https://github.com/ray-project/ray/issues/59618

13. Ray on PyPI (latest 2.54.1, 2026-03-25):
    https://pypi.org/project/ray/

14. vLLM Router (Rust-based alternative LB):
    https://blog.vllm.ai/2025/12/13/vllm-router-release.html
