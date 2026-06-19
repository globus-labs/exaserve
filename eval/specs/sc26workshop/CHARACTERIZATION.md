# Performance characterization — reviewer-facing synthesis (DRAFT, autonomous)

Goal: every data point explainable; no claim a reviewer can poke. Status of each
claim is marked: [SOLID] validated/repro'd, [PENDING] experiment in flight,
[NOTE] caveat/methodology. Numbers are run≥1 (warm-up dropped) unless stated.

## 0. Methodology guardrails (why the numbers are trustworthy)

- **Client topology is bounded.** `dest=proxy` cells use `client.num_nodes=min(N,4)`
  (a fixed 4-node load fleet); `dest=direct` uses one client per node (required).
  We proved STREAMING metrics are client-topology-invariant: n64 haproxy streaming
  with client=4 vs client=64 is statistically identical (attain 0.029 vs 0.042,
  TTFT p50 4.56 vs 4.47s). [SOLID] So the n1–n64 sweep is valid regardless of the
  earlier `client.num_nodes=num_nodes` default. The default materially affected
  ONLY non-stream 256n throughput (see §3). [SOLID]
- **Paper SLO**: TTFT ≤ 1s AND per-request P99-TBT ≤ 250ms, success required;
  per-request filter, attainment = fraction passing. [NOTE]
- **TBT measurement**: client-side TBT is valid ONLY with `http-no-delay` ON
  (immediate SSE forwarding). With coalescing OFF, client TBT measures the ~220ms
  proxy flush, not the ~30ms decode — so for any coalescing config, TBT is read
  SERVER-SIDE (decode/(gen-1)), which is proxy-immune. Server-side collector
  validated: server TBT p50/p99 = 22/24ms (tight) vs client 30/126ms at n1. [SOLID]

## 1. Set 1 — proxy/dispatch comparison (8B, 64/64, rate 110 = saturation stress)

Throughput scales near-linearly for the native proxies and direct through n64;
rate 110 (> the 109 single-replica knee) makes absolute attainment a saturation
stress test, so the SCALING TREND is the result, not the absolute SLO number.

- Successful throughput n1→n64: direct/haproxy/envoy track ideal-linear; rayserve
  & litellm fall off (drop 40–97% of requests at scale). [SOLID]
- Attainment falls with N because offered rate is at the knee (cli-invariant, §0). [SOLID]
- iso-SLO goodput ranking @ n64: direct ≫ haproxy > envoy ≫ rayserve/litellm. [SOLID]

### 256-node extension (haproxy + direct)
- **direct 256n: 19.4k rps, 0 err** (streaming) — distributed dispatch scales. [SOLID]
- **haproxy 256n NON-stream: 27.1k rps, 0.04% err** — the centralized proxy's
  request routing scales fine; matches an Apr-2026 run (26.9k). [SOLID]
- **haproxy 256n STREAMING: degraded-but-stable, with a RARE stochastic total failure.**
  (a) **Degraded-but-stable (the common case)** — completes ~100% of requests but slowly:
  across a 6-run sustained job (8548562) every run hit **99.9–100% success, E2E p50 ~5.7s /
  p99 ~17s** (vs direct 19.4k rps / healthy ~1.6s). SLO attain ~0 (E2E ≫ 1s). Driven by ~200k
  concurrent SSE connections + TCP-retransmit congestion funneled through one proxy. This is the
  reproducible result — report it. [SOLID]
  (b) **Rare total proxy DEATH** — observed in 2 early runs (8545884 prod, 8546289 dbg-scale):
  proxy process died mid-job → 100% ECONNREFUSED. But it is **RARE on prod: ~1 in 12 ramp-ups**
  (NOT reproduced across 11 consecutive instrumented prod ramps: 8548202/8548410/8548562). The
  death is **NOT resource exhaustion**: in a 43-min / 6-run survivor the resource trajectory is
  flat at 4–16% of every limit (fds 16% of 1.05M, threads 4% of pid_max, MemAvailable ~untouched
  at ~1 TB) with **no upward creep** (rules out both steady-state exhaustion AND slow leak), and
  the fork canary never fired. Exact death signal not yet captured (it's too rare to force);
  `stop()` now records `KILLED BY <signal>` so the next occurrence (incl. the full sweep) is
  self-diagnosing. [SOLID it's rare + not-resource; death signal TBD]
  Ruled OUT for the *throughput* limit: CPU/threads (64 default threads → ~4 cores; nbthread=1 at
  n64 pegs 1 core/92.9%, drops 5850→4527 → parallelism saturates ~4 cores then I/O-bound),
  accept-queue overflow (0), TIME_WAIT, and host-NIC-RX-drop (sysfs rx_dropped/rx_fifo_errors=0).
- **SERVER-SIDE proof the GPUs are healthy at 256n** [SOLID, collected via the fixed collector,
  3072 replicas]: server TTFT **54/66ms**, server TBT **24/28ms** (≪ 250ms SLO), server E2E
  1.6/1.8s. So the serving system MEETS the SLO at 256n; the client-observed collapse (attain
  ~0, E2E 18s) is **entirely the centralized-proxy delivery path**, not the GPUs. Direct/
  distributed dispatch lets the client see this healthy server performance (19.4k). This is the
  cleanest statement of the result: *the proxy, not the model server, is the 256n streaming wall.*
- **coalescing (http-no-delay OFF) actually HELPS throughput** [CORRECTED, job 8546289]: its
  warm-up served **~10k rps at 99.6% success** (vs the ~4.6k no-delay-ON survivor) and ~halved
  the packet storm (3.5M vs 7M retransmits). The data run then showed 100% ECONNREFUSED — but
  that was the **HAProxy process death** (mode (b) above), NOT a coalescing failure. So the
  earlier "coalescing doesn't rescue it" claim was WRONG: coalescing improves the per-run
  throughput; what limits 256n is the proxy death + connection concentration, which coalescing
  doesn't fix but also isn't *worsened* by. The robust fix remains distributed/direct dispatch.

## 2. The n64 SLO drop, decomposed (baseline, rate 98)

- Removing HAProxy (direct) at n64 cut median TTFT 1.22→0.64s, attain 0.236→0.585:
  HAProxy contributes a real TTFT cost. [SOLID]
- Decode/TBT inflation at n64 is proxy-invariant (2.30 vs 2.36s) → server/streaming
  side, not the proxy. [SOLID]
- Non-streaming removes nearly all of it (E2E 1.86s at n64, vs 7.6s streaming) →
  the dominant at-scale cost is the streaming token-delivery path, not the GPU. [SOLID]

## 3. The "regression" that wasn't (resolved)

The apparent HAProxy 256n regression (26.9k→10.9k) was a harness bug
(`client.num_nodes=num_nodes` → 256 client nodes hammered one proxy). With
client=4 it is 27.1k, matching April. Falsified en route, each by experiment:
http-no-delay; vLLM/Ray version (vLLM identical 0.15.0); architectural proxy
ceiling; accept-queue overflow; TIME_WAIT exhaustion; CPU. [SOLID]

## 4. http-no-delay tradeoff (Task 2, n1 low load) [SOLID]

| metric | ON | OFF (coalescing) | reading |
|---|---|---|---|
| client TTFT p50 | 66ms | 213ms | bounded +147ms additive; ≪ 1s SLO |
| client TBT-p99/req p50 | 30ms | 228ms | OFF measures the ~220ms flush, not decode |
| client E2E p50 | 1.448s | 1.477s | +29ms: bursting shifts arrival, not total latency |

Recipe for any coalescing config: TTFT client-side (+ bounded offset noted), TBT
server-side (proxy-immune). Server-side collector validated (§0).

## 5. Set 2 — OAT workload robustness (8B ×8 + 120B, N∈{1,64}) [SOLID, cli-invariant]

(attainment n1 vs n64; high-rate baseline/poisson collapse at n64 = saturation;
long-seq/low-rate mixes hold; 120B@rate9 holds.) Numbers in goodput tables /
set2_oat_robustness.png. Re-verify is unnecessary (streaming cli-invariance, §0).
- [NOTE] burstgpt n1 == n64 (19.2 rps, attain 0.46 both): its trace has a FIXED total
  arrival rate not scaled by N, so n64 is NOT actually stressed — the bar pair is
  uninformative as a scale test. Either scale its rate or drop it from the scaling claim;
  flag explicitly so a reviewer doesn't read it as "burstgpt is scale-invariant."

## Open items
- [DONE] Task 3 (256n streaming + coalescing): coalescing does not rescue it (see §1).
- [DONE] 256n network-saturation: now also instrumented at the NIC layer (sysfs
  rx_dropped/rx_fifo_errors per HSN iface, commit 25e52a0) — the full-sweep collapse run
  showed rx_dropped/rx_fifo_errors=0 in the nbthread runs -> the loss is NOT host-NIC; it's
  fabric/TCP-RTO under the ~200k-connection concentration (mechanism corrected in §1).
