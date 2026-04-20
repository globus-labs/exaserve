# Next Experiments — Ray Serve Limitation Study

## Context

Weak-scaling (1–256 nodes, 3 dispatch modes) is complete. Results show:
- Head-node modes (HAProxy, Direct-Fat) plateau at ~13.8k RPS / 51% efficiency at 256n
- Direct-MPI scales linearly: 26.9k RPS / 99% efficiency at 256n
- Ray GCS crashes at 512n (6144 replicas): `Failed to get cluster ID from GCS server: TimedOut`
- Deployment time at 256n: **30 minutes** for 3072 replicas (Stage 3)

The question: what additional experiments make the strongest case that Ray's centralized
architecture is fundamentally limited, and that MPI-based serving eliminates those limits?

---

## Experiment Candidates

### E1. GCS O(N²) Sync Overhead (deployment latency vs. node count)

**What**: Plot time-to-first-request (Stage 1+2+3) vs. node count for Ray Serve.
Compare against an MPI-based baseline (mpiexec + local model load).

**Why novel**: RFC ray-project/ray#57640 documents that GCS resource updates trigger
O(N²) broadcast amplification — one placement group creation per node on a 1000-node
cluster can take 10+ minutes. Our 256n Stage 3 = 30 min is direct evidence. No one has
published deployment latency scaling curves for Ray Serve at this scale.

**Data needed**: We already have Stage 1/2/3 times for 1–256 nodes. Need to extract and
plot them. For the MPI comparison, time `mpiexec` model loading at matching node counts.

**Effort**: Low (data already exists for Ray side; MPI baseline needs one run per scale).

**Sources**:
- https://github.com/ray-project/ray/issues/57640 (RFC: batched resource sync)
- https://github.com/ray-project/ray/issues/60680 (Serve Controller scalability tracking)

---

### E2. Multi-Proxy Ray Serve (proxy_location="EveryNode")

**What**: Run Ray Serve with `proxy_location="EveryNode"` so every node runs its own
HTTP proxy. Test at 128n and 256n. Compare throughput against our existing HAProxy
(single head node) and Direct-MPI results.

**Why novel**: If EveryNode mode approaches Direct-MPI throughput, it proves the
bottleneck is purely the single-proxy architecture. If it doesn't (likely due to GCS
coordination overhead for proxy health checks), it shows Ray's overhead persists even
when the proxy is distributed — the MPI approach avoids this because it has no
centralized coordination at all.

**Data needed**: New runs at 128n and 256n with EveryNode proxy config.

**Effort**: Medium (need to modify proxy config, may need different client dispatch to
hit all proxies).

**Sources**:
- https://docs.ray.io/en/latest/serve/advanced-guides/performance.html
- https://www.anyscale.com/blog/ray-serve-inference-lower-latency-higher-throughput-haproxy

---

### E3. Realistic Workload (Variable Request Lengths)

**What**: Replace the fixed 64in/64out workload with a ShareGPT-like trace having
variable prompt and generation lengths. Measure throughput, latency distribution,
and head-of-line blocking effects.

**Why novel**: Fixed-length requests hide queueing effects. With variable lengths,
leastconn routing becomes critical, and the single-proxy bottleneck manifests as
tail latency spikes (long requests block short ones in the proxy queue). MPI direct
mode avoids this by distributing the routing decision.

**Data needed**: New runs across dispatch modes with a realistic trace.

**Effort**: Medium (need a replay trace with variable lengths; modify Go client to
support variable input/output).

---

### E4. Fault Recovery Under Load

**What**: At 64n or 128n, kill a worker node mid-serving. Measure:
(a) time until the first failed request,
(b) time until routing avoids the dead node,
(c) total error count during recovery.

**Why novel**: Ray's actor restart goes through GCS. At 128n where GCS is already
stressed, recovery should be slow. MPI-based systems can detect failures via direct
health checks and reroute in seconds without centralized coordination.

**Data needed**: New experiment with node-kill injection.

**Effort**: Medium-high (need fault injection harness, careful PBS job management).

**Sources**:
- https://github.com/ray-project/ray/issues/57173 (Serve Controller crash on slow workers)
- https://github.com/ray-project/ray/issues/59327 (24h idle recovery failure)

---

### E5. HAProxy Multi-Thread / HTTP2 (close the gap question)

**What**: Add `nbthread 16` and/or `proto h2` to HAProxy config. Re-run at 128n, 256n.

**Why novel**: Low novelty — mostly confirms what Direct-Fat already showed (head-node
is the ceiling). But closes the "did you try tuning HAProxy?" reviewer question.

**Data needed**: Modified HAProxy config, 2 runs.

**Effort**: Low.

**Sources**:
- https://www.haproxy.com/blog/multithreading-in-haproxy (sublinear scaling beyond 2-4 threads)
- https://www.loadbalancer.org/blog/how-to-get-more-out-of-and-in-to-haproxy/

---

### E6. Autoscaling Latency

**What**: Trigger a load spike that requires Ray Serve to scale from N to N+12 replicas.
Measure time from load increase to replica availability.

**Why novel**: Demonstrates GCS coordination cost for dynamic scaling. MPI-based approach
can pre-provision or scale via mpiexec with known startup time.

**Data needed**: New experiment with stepped load profile.

**Effort**: Medium.

---

### E7. Long-Idle Recovery

**What**: Deploy at moderate scale (32-64n), leave idle for 1-24 hours, then send a
burst of requests. Measure first-request failure rate and recovery time.

**Why novel**: GitHub issue ray-project/ray#59327 reports consistent first-batch failures
after 24h+ idle. Easy to reproduce, dramatic for reviewers.

**Data needed**: Long-running PBS job with idle period.

**Effort**: Low (but needs walltime).

---

## Additional Context

### HAProxy Capabilities (for reference)
- Multi-threading: `nbthread N` in global section. Sublinear scaling beyond 2-4 threads.
- HTTP/2: frontend `alpn h2`, backend `proto h2` per server line.
- Algorithms: roundrobin, leastconn, source, uri, hdr(). No LLM-aware routing.
- No KV-cache-aware routing (projects like llm-d add this at the Kubernetes layer).

### Known Ray Serve Issues (verified from web)
- GCS O(N²) broadcast: ray-project/ray#57640
- Serve Controller scalability: ray-project/ray#60680
- Autoscaler scale-down failure: ray-project/ray#60546
- Controller crash on slow workers: ray-project/ray#57173
- Long-idle first-request failure: ray-project/ray#59327
- vLLM TP>1 placement group failure: vllm-project/vllm#30016
- vLLM RayWorker 300% CPU: vllm-project/vllm#21231

### MPI Comparison Gap
- No published paper benchmarks Ray Serve vs MPI for LLM inference serving.
- DeepSpeed-MII (MPI backend) claims 2.5x over vLLM but vLLM rebuts this as
  workload-dependent. DeepSpeed-MII is less actively maintained.
- Our direct-MPI data (26.9k RPS / 99% linear at 256n) would be novel evidence.
