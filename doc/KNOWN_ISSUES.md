# Known Issues & Failure Log

A running list of issues, failure modes, and gotchas we've actually hit while
running the Aurora Ray Serve eval — kept so we can revisit them instead of
re-discovering them. This is the **empirical** companion to:

- [TODO.md](TODO.md) — forward-looking improvement backlog (P0–P3, feature gaps).
- [findings/](findings/) — deep-dive root-cause writeups (GCS contention,
  wait_proxies, death cascade, launch stages, etc.).
- Agent memory under `~/.claude/projects/-home-wenyiw-aurora-rayserver/memory/`
  — one fact per file; slugs referenced below as `[[memory_slug]]`.

When something here is fixed, move it to **§E Resolved** with the fix, don't delete it.

**Status legend:** `OPEN` needs work · `WORKAROUND` mitigated, living with it ·
`NEEDS-RERUN` transient infra, just resubmit · `WATCH` intermittent, monitor ·
`PRESENTATION-PENDING` data is fine, how-to-show is undecided · `RESOLVED`.

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
Refs: TODO.md "Port allocation is fragile"; `aurora_serve.py` port scan.

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
Refs: CLAUDE.md "Project-Specific Knobs", launch_cluster.sh.

### A6. Per-replica scaling-trace JSON cost on Lustre — `WORKAROUND` (default-off)
`AURORA_SCALING_TRACE=1` writes per-replica trace JSONs that cost ~5s/file under
MDS contention → +10 min setup at 128n. **Default `=0`**; only enable for short
Ray-startup debugging.
Refs: CLAUDE.md, `aurora_serve.py:_collect_replica_traces`.

### A7. Lustre import stampede at scale — `OPEN`
~3k Ray processes × ~20 imports = ~60k concurrent Lustre opens during launch
Stage 3 (Copper only broadcasts the overlay, not `$PROJECT_ROOT/src`). MDS
stampede. **Action:** extend Copper broadcast to cover src.
Refs: TODO.md "Wenyi's Note", findings/ray_launch_stages.md.

---

## B. Proxy behavior & bottlenecks

### B1. Single HAProxy is the throughput ceiling at ≥256n — `KNOWN`
One HAProxy on the head node plateaus ~13.7k RPS / ~50% efficiency; direct (MPI)
hits 26.9k / ~99% linear at 256n. **Action:** default `dest=direct` for large
weak-scaling unless specifically testing the proxy.
Refs: CLAUDE.md "HAProxy is the bottleneck".

### B2. HAProxy streaming 256n head-node network saturation — `KNOWN`
HAProxy **streaming** at 256n collapses on head-node NETWORK saturation (measured:
6.75M TCP retransmits, ~195k conns) — not CPU/accept-queue/TIME_WAIT. Non-stream
hits 27k; direct streaming 19.4k. **Action:** report as a network-bound result,
not a proxy-CPU limit.
Refs: `[[project_haproxy_streaming_256n_network]]`.

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

### C1. `collect_stats=true` writes NO server_stats.json — `KNOWN` (silent no-op)
Setting `collect_stats=true` silently produces no `server_stats.json`. **Action:**
use client-side per-request fields only; don't depend on server-side stats.
Refs: `[[project_server_stats_noop]]`.

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
Refs: CLAUDE.md "Result file robustness", eval/lib/run_executor.py.

### C4. dest=direct result gather is shard-based, not MPI collective — `WORKAROUND` (don't revert)
`replay_engine._gather_results_via_shards` has each rank write one shard to Lustre
(atomic temp+rename) that root polls/reads. The old single-root `comm.gather` hung
and lost ALL results at 64n on a node drop. **Do not** switch back to a collective
or to gather.c (nesting under PALS; replay client has no Ray handle). Watch the
"collected N/M rank shards" warning for silent partials.
Refs: CLAUDE.md "Result file robustness", commit 570d727.

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

### D1. Readiness gate false-positive at scale — `OPEN`
At 256n the driver printed `CLUSTER FULLY READY` despite a crashed ServeController
and `Failed to collect proxy statuses` — the health check only probed one
`/health` endpoint (0.025s), not all N. So the client ran against an unhealthy
cluster. **Action:** harden readiness to verify all proxies/replicas (or fail
loudly) before launching the client. Contributing factor to A1.
Refs: `[[project_envoy_256n_deploy_failure]]`.

### D2. SSH fan-out overlay distribution race — `WORKAROUND`
Ray overlay patches are pushed via O(N) serial `ssh -f`; no return-code check (a
failed worker silently falls back to system Ray), a `sleep 5` guards a race on
deleting the staged script, and O(N) handshakes (~25s at 256n) don't scale past
~512n. **Action:** add rc checks / migrate to Copper.
Refs: findings/overlay_distribution_design.md.

### D3. Legacy AURORA_PROXY_PROFILE races with overlay probe — `OPEN` (low-impact)
Legacy `AURORA_PROXY_PROFILE` monkey-patches and the overlay `proxy.py` probe both
write `/tmp/aurora_inst/proxy_init_*.json` and race/overwrite (tmpfs, no Lustre
impact, but redundant). Still default-on. **Action:** `AURORA_PROXY_PROFILE=0` or
delete the three aurora_serve.py blocks.
Refs: TODO.md "Wenyi's Note", launch_cluster.sh.

### D4. PP>1 forces single replica / uncompiled DAG — `KNOWN` (limitation + workaround)
`pipeline_parallel_size > 1` forces `num_replicas=1` (blocks PP throughput
scaling), and Ray compiled-DAG channels crash on XPU so PP>1 must use the
uncompiled executor (`AURORA_VLLM_DISABLE_RAY_COMPILED_DAG=1`, set in
launch_cluster.sh). **Action:** redesign placement groups for multi-replica PP.
Refs: TODO.md "Single replica enforced for PP", server.py.

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
- **Deep-dive root causes:** [findings/](findings/) — esp. `gcs_contention_quantitative.md`,
  `wait_proxies_root_cause_instrumented.md`, `proxyactor_death_cascade_256n.md`,
  `ray_launch_stages.md`, `overlay_distribution_design.md`.
- **SC26 eval status & data coverage:** eval/specs/sc26workshop/README.md, DATA_LEDGER.md.
- **Operational facts (one per file):** agent memory dir (`MEMORY.md` index).
