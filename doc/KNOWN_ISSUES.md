# Known Issues & Failure Log

> **Document role:** Empirical incident/evidence input. Status labels here do
> not close production findings and workaround prose is not an implementation
> instruction. The canonical plan and, after WP0, `doc/hardening/FINDINGS.yaml`
> govern ownership, acceptance evidence, and final disposition.

A running list of issues, failure modes, and gotchas we've actually hit while
running the ExaServe eval — kept so we can revisit them instead of
re-discovering them. This is the **empirical** companion to:

- [TODO.md](TODO.md) — forward-looking improvement backlog (P0–P3, feature gaps).
- [findings/](../findings/) — deep-dive root-cause writeups (GCS contention,
  wait_proxies, death cascade, launch stages, etc.).
- Agent memory under `~/.claude/projects/-home-wenyiw-exaserve/memory/`
  — one fact per file; slugs referenced below as `[[memory_slug]]`.

When something here is fixed, move it to **§E Resolved** with the fix rather
than deleting it. Entries whose A/B/C/D identifiers are referenced by audit
ledgers may remain in place when clearly marked resolved or superseded.

**Status legend:** `OPEN` needs work · `WORKAROUND` mitigated, living with it ·
`NEEDS-RERUN` transient infra, just resubmit · `RESOLVED-IN-CODE / NEEDS-RERUN`
implementation exists but its scale evidence must be refreshed · `WATCH`
intermittent, monitor · `PRESENTATION-PENDING` data is fine, how-to-show is
undecided · `RESOLVED`.

---

## A. Scale-out startup & orchestration failures (≥128 nodes)

### A1. Envoy 256n streaming deploy collapse — `NEEDS-RERUN`
All 3 envoy 256n streaming attempts (`proxycmp_envoy_scale` run0/run1,
`proxycmp_envoy_256retry` run0) recorded **0% success**. Not Envoy's fault —
Envoy started and was confirmed healthy on `:4001`. Chain: vLLM EngineCore
**EADDRINUSE** (static port 49717/39479 collides across the cluster) → unhealthy
replicas; Ray ServeController asyncio crash-loop (`AssertionError` in
`_copy_future_state`); readiness gate false-positive (`Failed to collect proxy
statuses: 'ProxyStatus' object has no attribute 'status'` but still printed
`CLUSTER FULLY READY`); node loss in cabinet c7; proxy torn down before the
**measured** replay dispatched → every request `connection refused`.
**Action:** re-run (transient). If frequent, needs race-free/retry-on-bind port
assignment. Envoy non-stream 256n succeeds — streaming-only.
Refs: `[[project_envoy_256n_deploy_failure]]`.

### A2. Static port assignment is not race-safe — `WORKAROUND`
EngineCore / Ray ports are assigned by a best-effort range scan with no
registry/lease, deliberately not bullet-proof (most runs are fine). Under a
chaotic ≥128n deploy (reused nodes, co-located DP replicas, node loss) it can
collide → EADDRINUSE → A1. **Action:** re-run on collision; eventual fix is
OS-assigned (port 0) or a lease/retry-on-bind scheme.
Refs: TODO.md "Port allocation is fragile";
`src/exaserve/server.py:278-291`, `src/exaserve/engines/vllm.py:112-131`,
`src/exaserve/engines/sglang.py:74-76`,
`src/exaserve/proxy/litellm_proxy.py:127-164`.

### A3. GCS O(N²) actor-handle contention — `WORKAROUND`
At 256n, `wait_proxies` reaches ~1751s (27 min): each of N proxies re-resolves
each of N replicas' actor handle via `ray.get_actor()` on every LongPoll
broadcast (~1.58M GetActorInfo calls). GCS is single-threaded (caps ~1000 nodes).
**Workaround:** `RAY_gcs_server_num_threads=8`, request-timeout=60, and patch
`UNHEALTHY_THRESHOLD=100` to stop the proxy-kill cascade. Real fix is a Ray Serve
change (ship handles in the long-poll payload).
Refs: findings/gcs_contention_quantitative.md, findings/proxyactor_death_cascade_256n.md, `[[project_128n_proxy_debug]]`.

### A4. wait_proxies cliff (~59s plateau at 64n+) — `WORKAROUND`
Three-factor: Ray Serve defers proxy spawn until replicas RUNNING (~38s fixed) +
`startup_concurrency=8` + deploy window; at 64n+ the single-threaded controller
saturates, misses health checks, and restarts proxies (+70–80s). **Workaround:**
skip wait_proxies for HAProxy dispatch (HAProxy handles readiness itself).
Refs: findings/wait_proxies_root_cause_instrumented.md, `[[project_wait_proxies_root_cause]]`.

### A5. HF tokenizer RAYON panic at 128n+ — `WORKAROUND` (default-on)
Without guards the HF Rust tokenizer's lazy rayon pool tries to spawn ~nproc
threads on first request and panics `ThreadPoolBuildError WouldBlock` (Aurora
nproc exhausted by Ray gRPC threads); the replay client then produces no output.
**Workaround (baked into launch_cluster.sh):** `RAYON_NUM_THREADS=1` +
`TOKENIZERS_PARALLELISM=false`.
Refs: `src/exaserve/resources/launch_cluster.sh:426-448`,
`src/exaserve/server.py:539-551`.

### A6. Former per-replica scaling-trace JSON cost — `RESOLVED / SUPERSEDED`
The old description is no longer current. Scaling tracing now defaults to
enabled, and replica-init records are pushed to one named Ray actor and folded
into the head serving process's aggregate trace instead of writing one Lustre
file per replica. `ScalingTracer.save_replica_trace()` remains in the tree but has no
repository caller. This resolves the former metadata-server write storm; the
single-actor ownership, stale-state, cleanup, and backpressure questions are
separate operational debt under PR-029/WP10. **Action:** remove or explicitly
deprecate the dead per-replica writer and validate actor load and cleanup at the
claimed scale.
Refs: `src/exaserve/resources/launch_cluster.sh:401-405`,
`src/exaserve/scaling_trace.py:312-327,346-423`,
`src/exaserve/server.py:942-956,1772-1775,2179-2200`.

### A7. Former application-source Lustre import stampede — `RESOLVED-IN-CODE / NEEDS-RERUN`
The old description is no longer current. `launch_cluster.sh` now invokes
`distribute_to_nodes.sh`, which performs one MPI broadcast of the ExaServe
package and runs it from `/tmp/exaserve_src` on every node. It can also stage a
shared-filesystem engine venv and Triton tree into node-local storage. This
removes the original per-process application-source open storm by construction.
**Action:** revalidate import traffic at 128/256 nodes and separately measure
any residual imports from system packages outside the staged environment.
Refs: `src/exaserve/resources/launch_cluster.sh:446-486`,
`src/exaserve/resources/distribute_to_nodes.sh:1-15,84-157`.

---

## B. Proxy behavior & bottlenecks

### B1. Former general HAProxy throughput ceiling — `RESOLVED / SUPERSEDED BY B2`
The ~13.7k RPS non-streaming plateau was confounded by the benchmark harness and
head-node client topology, not an architectural HAProxy throughput ceiling.
With the client fleet bounded correctly, current evidence records 27.1k RPS at
256 nodes with 0.04% errors, matching the earlier healthy result. Direct-Fat and
HAProxy converging at ~13.8k in the older comparison implicated the single
head-node client process rather than HAProxy itself. The remaining demonstrated
centralized-proxy constraint is the no-coalescing **streaming** network path in
B2. **Action:** keep client topology explicit and do not generalize B2 to
non-streaming or to HAProxy independent of topology.
Refs: `eval/specs/sc26workshop/FINDINGS_haproxy_256n.md:9-29`,
`findings/weakscaling_short_v3_progress.md:98-107`.

### B2. Centralized 256n streaming congestion; rare proxy death — `KNOWN / WATCH`
The common bounded-client result is degraded-but-stable streaming: approximately
100% request success but poor latency/SLO attainment. Surviving degraded runs
measured a retransmission storm (up to ~6.75M retransmits and ~195k established
connections) on the centralized, no-coalescing streaming path; CPU,
accept-queue, and TIME_WAIT exhaustion were falsified. A distinct rare outcome
is total HAProxy process death and `ECONNREFUSED` (about 1/12 production ramps in
the current evidence). Its cause is still unresolved and must not be attributed
to congestion or resource exhaustion without a captured death signal.
Non-streaming reaches 27.1k RPS and direct streaming reaches 19.4k RPS in the
cited runs. **Action:** report the common congestion regime and rare process
death separately; retain self-recording process diagnostics until the death is
explained.
Refs: `eval/specs/sc26workshop/FINDINGS_haproxy_256n.md:9-80,100-120`,
`[[project_haproxy_streaming_256n_network]]`.

### B3. litellm "fake streaming" → degenerate P99-TBT — `PRESENTATION-PENDING`
litellm buffers the full response and flushes all tokens in a sub-ms end-burst:
~44.6% of successful 64-tok requests show P99-TBT ≤ 1ms while TTFT ≈ full latency
(~43s). The TBT CDF therefore looks deceptively good; the cost is entirely in
TTFT. The CDF already excludes failures — this is **not** a filtering bug.
**Action (not applied):** consider excluding non-streamed requests
(`latency − ttft` ≈ 0, >1 token) from the TBT CDF with a per-proxy footnote; user
to decide how to present.
Refs: `[[project_litellm_fake_streaming_tbt]]`.

---

## C. Measurement & data-correctness gotchas

### C1. Former `collect_stats=true` silent no-op — `RESOLVED / SUPERSEDED`
The categorical no-output statement is stale. The active path now has replicas
push bounded summaries to a named `ServingStatsCollector`; `run_executor` reads
that actor before teardown and writes aggregate `server_stats.json`. Repository
evidence records a successful 12/12-replica collection. Residual contract debt
remains: collection failures are warnings and do not necessarily fail a run,
and the unused `EngineWorker.collect_stats()` / `VLLMEngine.collect_stats()`
path still has an incompatible data schema. **Action:** decide whether requested
stats are required or best-effort, enforce that policy, contract-test the active
producer/consumer, and delete or repair the dead legacy API.
Refs: `eval/lib/run_executor.py:96-103`, `eval/lib/server_stats.py:41-109`,
`src/exaserve/server.py:794-865,979-984`,
`src/exaserve/engines/vllm.py:279-297`,
`eval/specs/sc26workshop/OVERNIGHT_LOG.md:71-80,92-99`,
`[[project_server_stats_noop]]`.

### C2. Run uses committed HEAD, not the working tree — `KNOWN` (gotcha)
`materialize` snapshots the repo at **committed HEAD**; uncommitted changes are
ignored (it warns "snapshot will use committed HEAD only"). **Action:** commit
code/spec changes before materialize+submit, or the run silently uses stale code.
Refs: `[[project_paper_eval_infra]]`, eval/lib/run_planner.py `_ensure_repo_snapshot`.

### C3. Lustre trailing-bytes on rewritten result JSON — `WORKAROUND` (don't revert)
Lustre sometimes leaves trailing bytes past EOF when `open("w")` writes a smaller
file over a larger one. `_validate_replay_results` uses `JSONDecoder.raw_decode()`
(not `json.load()`) and rewrites in place. **Do not** replace with plain
`json.load()`.
Refs: `eval/lib/run_executor.py:286-308`,
`findings/weakscaling_short_v3_progress.md:36-40`.

### C4. dest=direct result gather is shard-based, not MPI collective — `WORKAROUND` (don't revert)
`replay_engine._gather_results_via_shards` has each rank write one shard to Lustre
(atomic temp+rename) that root polls/reads. The old single-root `comm.gather` hung
and lost ALL results at 64n on a node drop. **Do not** switch back to a collective
or to gather.c (nesting under PALS; replay client has no Ray handle). Watch the
"collected N/M rank shards" warning for silent partials.
Refs: `eval/lib/replay_engine.py:79-159`, commit 570d727.

### C5. client.num_nodes footgun (1 client/node hammering 1 proxy) — `KNOWN` (fixed in specs)
A spec that sets `client.num_nodes = num_nodes` spawns N clients all hammering the
single head-node proxy, fabricating a false "regression" (e.g. HAProxy 256n). With
`client.num_nodes=4` HAProxy 256n returns 27.1k (matches prior). **Action:** keep
`client.num_nodes = min(num_nodes, 4)` for proxy dispatch (the proxycmp specs now
derive this). Re-check any large-N proxy finding for this confound.
Refs: `[[project_haproxy_httpnodelay_regression]]`.

### C6. n64 SLO collapse is streaming-path dominated — `KNOWN` (interpretation)
The n64 paper-SLO drop is dominated by the STREAMING token-delivery path, not GPU:
non-stream E2E ≈ 1.85s / 0 err, while the HAProxy/streaming cost is streaming-only.
TTFT and TBT components are additive (HAProxy term + server-side decode). **Action:**
separate TTFT vs TBT attainment when presenting (fig1 already does).
Refs: `[[project_n64_slo_two_causes]]`.

---

## D. Harness robustness / known workarounds

### D1. Readiness gate false-positive at scale — `RESOLVED-IN-CODE / NEEDS-SCALE-RERUN`
At 256n the driver printed `CLUSTER FULLY READY` despite a crashed ServeController
and `Failed to collect proxy statuses` — the health check only probed one
`/health` endpoint (0.025s), not all N. So the client ran against an unhealthy
cluster. Contributing factor to A1.

**Resolved in code (2026-08-06).** `CLUSTER FULLY READY` is no longer a state:
`control.serve_readiness.enforce_readiness()` gates it on a predicate — exact
node membership, a healthy proxy on **every** node, `target_num_replicas`
running per application, application RUNNING, and a real completion through the
external route (a `/health` probe explicitly does not count as a canary) — plus
a compatibility receipt from every required role. Failure is fail-closed with
each blocker named, and the verdict is written to `readiness.json`, which the
eval harness treats as authoritative over the stdout marker.

Readiness is also **revocable**: replica sets are absolute and refreshed every
poll, so a replica lost after the fact drops the count below target instead of
leaving a latched marker.

Validated at 2 and 16 nodes (16n: 192/192 replicas, 16 healthy proxies, canary
answered, all roles attested). **Still needs a re-run at 256n** before this is
closed outright — the original symptom was scale-dependent.
Refs: `[[project_envoy_256n_deploy_failure]]`, `doc/hardening/STATUS.md`.

### D2. Former SSH fan-out overlay distribution race — `RESOLVED-IN-CODE / NEEDS-RERUN`
The SSH fan-out description is stale. Package source and optional overlay files
are now distributed through MPI broadcast, and overlay assembly is launched
through the allocation's MPI/srun seam. This removes the O(N) SSH handshake and
staged-script deletion race. Residual risks are the native broadcast error
contract and verifying that every process activated the expected overlay; those
are tracked by PR-004 and PR-026 rather than by the former SSH race.
**Action:** revalidate the MPI distribution and activation receipts at scale.
Refs: `src/exaserve/resources/distribute_to_nodes.sh:1-15,159-180`,
`src/exaserve/resources/launch_cluster.sh:446-486`.

### D3. Former legacy/overlay proxy-profile race — `RESOLVED-IN-CODE`
The legacy `_install_proxy_actor_profiling_hook()` still exists but its only call
is commented out; no active launch path sets `EXASERVE_PROXY_PROFILE`. The old
hook also writes under `/tmp/exaserve_proxy_profile`, not the overlay's
`/tmp/exaserve_inst/proxy_init_*.json`, so the prior current-tense race/default-on
description is unsupported. The optional overlay is the active instrumentation
owner. **Action:** remove the dead hook during compatibility cleanup and add an
atomic single-owner output/activation-receipt test.
Refs: `src/exaserve/_sitecustomize.py:1514-1629`,
`src/exaserve/patches/ray_serve_overlay/ray/serve/_private/proxy.py:1316-1381`,
TODO.md "Wenyi's Note".

### D4. Multi-replica PP has topology/private-API constraints — `KNOWN`
PP defaults to one replica, but an explicit multi-replica request is retained.
The ordinary planner then uses location-agnostic placement bundles and warns a
stage's TP group may straddle nodes. The shard-aware mode instead stages by
node group and deploys one node-pinned application per replica through private
Ray Serve `_run_many`; it has measured scale evidence but remains version- and
topology-sensitive. On XPU, PP also selects the uncompiled Ray executor through
`EXASERVE_XPU_VLLM_DISABLE_RAY_COMPILED_DAG=1`. **Action:** make PP topology a
typed capability, replace or strictly guard the private API, and validate
default, explicit, and shard-aware layouts per platform.
Refs: `src/exaserve/server.py:310-317,491-517,1287-1366,1455-1631`,
`src/exaserve/resources/launch_cluster.sh:394-399`, `doc/exaserve.md:112-115`.

---

## E. Resolved (kept for record)

### E1. HAProxy 256n "http-no-delay regression" — `RESOLVED`
The apparent HAProxy 256n streaming regression was a harness bug, not HAProxy:
`client.num_nodes` was set to `num_nodes`, so 256 clients hammered one proxy.
Fixed by `client.num_nodes=4` → 27.1k RPS (matches April). See C5 for the
remaining footgun to guard against.
Refs: `[[project_haproxy_httpnodelay_regression]]`.

---

## Pointers / where to look next

- **Improvement backlog & priorities:** [TODO.md](TODO.md) (P0–P3 table, feature gaps:
  tests, /metrics, tracing, caching, constants centralization, sitecustomize fragility).
- **Deep-dive root causes:** [findings/](../findings/) — esp. `gcs_contention_quantitative.md`,
  `wait_proxies_root_cause_instrumented.md`, `proxyactor_death_cascade_256n.md`,
  `ray_launch_stages.md`, `overlay_distribution_design.md`.
- **SC26 eval status & data coverage:** eval/specs/sc26workshop/README.md, DATA_LEDGER.md.
- **Operational facts (one per file):** agent memory dir (`MEMORY.md` index).
