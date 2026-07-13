# HAProxy @ 256 nodes — investigation findings

Scope: 8B, 64-in/64-out, rate 110 rps/node (offered ≈ 28.2k rps aggregate),
single centralized HAProxy on the head node fanning out to 256 replicas.
Comparison reference: an April run that achieved 26.9k rps.

## TL;DR

1. **There was no regression.** The apparent "HAProxy 256n collapse (26.9k → 10.9k)"
   was a benchmark-harness bug: the sc26workshop specs derived
   `client.num_nodes = num_nodes`, so at 256n they launched **256 client nodes
   (1024 dispatcher procs) against one HAProxy** — measuring the proxy's
   connection-handling limit, not its throughput. The old specs used a bounded
   `min(num_nodes, 4)` = 4-node client fleet. Fixed in 29 specs.
2. **Non-streaming HAProxy 256n is healthy: 27.1k rps, 0.04% err, ~2s drain** —
   matches April. (Confirmed with `client.num_nodes=4`, current code.)
3. **Streaming HAProxy 256n: degraded-but-stable, plus a RARE stochastic death.**
   (a) Common case = degraded-but-stable: ~100% success, **E2E p50 ~5.7s** (6-run job 8548562,
   every run consistent) — slow, SLO attain ~0, but completes. (b) Rare total proxy death
   (100% `ECONNREFUSED`), observed in 2 early runs but **~1/12 prod ramps** — NOT reproduced in
   11 consecutive instrumented prod ramps. The death is **NOT resource exhaustion or leak**: a
   43-min/6-run survivor held resources flat at 4–16% of every limit with no creep; canary never
   fired. Death signal now self-recording in `stop()` for the next occurrence. Direct/distributed
   streams fine (19.4k). (Earlier "~2/3 die" and "coalescing doesn't rescue" were both WRONG.)

Falsified along the way (each by experiment, not argument):
`http-no-delay` as the regression cause; a vLLM 0.15 / Ray version regression
(vLLM is identical 0.15.0 both runs); an "architectural HAProxy throughput
ceiling"; accept-queue overflow; TIME_WAIT/port exhaustion; CPU saturation.

> **NOTE (correction in progress):** the sections below analyze the *surviving degraded*
> streaming runs as congestion collapse — that still holds. But the **100%-ECONNREFUSED runs
> are HAProxy PROCESS DEATHS**, not "stochastic congestion variance" (see TL;DR #3). The death
> cause is under instrumented investigation (job 8548202, prod). The n1 baseline already rules
> out HAProxy's own fd limit (compute-node `Max open files`=1,048,575 ≫ ~200k conns). This
> body will be rewritten with the confirmed death mechanism once 8548202 lands; treat §"client
> count variance" below as SUPERSEDED for the 100% runs.

## Measured root cause of the streaming-256n collapse

Instrumented with a head-node sampler (commit adds `_start_diag_sampler` in
`src/exaserve/proxy/haproxy_proxy.py`; writes `proxy_out/haproxy_diag.log`
every 3s; disable with `EXASERVE_HAPROXY_DIAG=0`). From the run that collapsed
(`proxycmp_haproxy_256_val/run2`, streaming, client=4):

| signal | measured | verdict |
|---|---|---|
| `TcpRetransSegs` | 331k → **7.08M** (+6.75M) | **congestion collapse** |
| peak established TCP conns (head node) | **195,205** | connection storm |
| `TCPAbortOnData` | 241 → **182,244** | connections dying |
| accept-queue (`ListenOverflows/Drops/Syncookies/ReqQFull`) | **0, never** | not accept overflow |
| TIME_WAIT (`tw`) | peak 24.5k (not exhaustion) | not http-server-close churn |
| HAProxy `%cpu` | peak 427% on ~208 threads (~2% of node) | not CPU-bound |

Retransmissions grow **in lockstep** with concurrent-connection count across both
the warm-up and data run waves, and plateau whenever connections drain. With ~98%
of head-node CPU idle at collapse, the bottleneck is network I/O.

**Mechanism (measured):** `option http-no-delay` makes every generated token its own
packet (no coalescing). Streaming 28.2k rps × 64 tokens concentrates **~1.8M+ small
packets/s and ~200k long-lived SSE connections on one head node**, producing a TCP
retransmission storm (6.4–6.75M) → congestion collapse. Non-streaming is fine (27k):
~1 buffered response/request = far fewer packets and connections. Direct dispatch is
fine (19.4k): traffic never funnels through one node.

What it is NOT (each ruled out by measurement):
- NOT CPU/threads: HAProxy ran 64 threads by default (nlwp=64; 32 when pinned via nbthread)
  but used only ~4 cores of CPU (fluctuating, not pegged) → threads are I/O/network-bound and
  idle, not CPU-starved. More threads can't help; nbthread is the right knob but not the limit.
- NOT accept-queue overflow (ListenOverflows = 0), NOT TIME_WAIT exhaustion.
- **NOT host-NIC-RX-drop**: `/sys/class/net/hsn*/statistics/rx_dropped` & `rx_fifo_errors`
  stayed **0**. So the loss is not the head NIC ring — it's at the fabric/switch or TCP-RTO
  level under the ~200k-connection concentration. (Corrects an earlier "the NIC is the wall"
  phrasing — the host NIC counters refute it.)

**Server-side proof the model server is healthy at 256n** (collected via the fixed
collector, 3072 replicas, data-run-only): server TTFT 54/66ms, server TBT **24/28ms**
(≪250ms SLO), server E2E 1.6/1.8s. The GPUs meet the SLO; the collapse is entirely the
centralized-proxy streaming/delivery path. Direct dispatch surfaces this healthy server
performance to the client (19.4k); the centralized proxy does not.

## Client count at 256n streaming: run-to-run variance, not a robust effect

Raw observations (256n streaming): client=256 → 3.2k rps / 34% err (one run);
client=4 → 100% ECONNREFUSED (one run) AND 4.6k rps / 0.06% err (another run).

CORRECTION / reconciliation: do NOT over-read this as "more clients = more stable."
A clean controlled test at **n64** showed streaming is **client-topology-INVARIANT**
(cli=4 vs cli=64: attain 0.029 vs 0.042, TTFT 4.56 vs 4.47s — statistically identical).
So at 256n the proxy is sitting right at the congestion-collapse edge, and the spread
(limp at ~4.6k vs total ECONNREFUSED) is **stochastic congestion-collapse variance**,
not a reproducible client-count effect. The client topology materially changed only the
NON-streaming 256n throughput (10.9k at cli=256 vs 27.1k at cli=4), where the proxy is
not network-saturated and the connection storm from 1024 dispatchers is the binding limit.
Burstiness/statistical-multiplexing may modulate the *severity* of the streaming collapse
but is not its cause (the per-token packet storm is). Treat the 256n streaming result as
"congestion-collapsed, high variance," and report a representative run (4.6k, attain 0.003)
with the variance noted — not a client-count trend.

## Is SSE a dead-end for serving? Why do people use it? How to bypass the per-token packets?

Not a dead-end. The packet storm is **not inherent to SSE** — it's the conjunction
of (a) `http-no-delay` (coalescing OFF, added here only for accurate per-token TBT
measurement), (b) a *single centralized* proxy, and (c) extreme scale (256n / 28k
rps through one node). SSE is the standard for incremental LLM token delivery
(OpenAI-style APIs) and is required for interactive TTFT/TBT; at normal scale and
with normal proxy topologies the per-token packet rate is a non-issue. Funneling
256 nodes through one no-coalescing proxy is a benchmark worst case.

Knobs / techniques that bypass the per-token-packet problem:
1. **Drop `http-no-delay` (re-enable coalescing).** It exists only for TBT-measurement
   fidelity. With coalescing (Nagle / buffered flush), multiple tokens share a packet
   → packet rate falls by the tokens-per-flush factor. Production should not run
   no-delay. (The `http_no_delay: false` knob already added enables this — worth
   testing whether streaming 256n survives with it off.)
2. **Application-level token batching:** emit N tokens per SSE event, or flush every
   few ms, instead of per token — fewer events and packets, small bounded TBT cost.
3. **Don't centralize:** distributed/hierarchical proxies (per-rack), L4 load
   balancing (route, don't terminate SSE), or direct dispatch. Direct already
   scales here (19.4k).
4. **HTTP/2/3 multiplexing:** many SSE streams over few connections → cuts the ~195k
   connection count (one of the two pathologies).
5. **Proxy-node network tuning:** GRO/GSO offload, larger NIC RX rings, RSS across
   more queues, bigger `net.core` buffers → raises the packet-rate ceiling.
6. **TCP_CORK + timed flush:** coalesce tokens into fewer packets with bounded added
   latency.



## Measured: http-no-delay ON vs OFF (n1, low load, client-side) — Task 2

| metric | ON (no-delay) | OFF (coalescing) | note |
|---|---|---|---|
| client TTFT p50 | 66 ms | 213 ms | bounded +147 ms additive (= flush interval ~220 ms); still ≪ 1 s SLO |
| client TBT-p99/req p50 | 30 ms (true decode) | 228 ms (proxy flush) | client TBT INVALID with no-delay off |
| client TBT-p99/req p99 | 126 ms | 287 ms | off-TBT FALSELY fails the 250 ms SLO vs true 30 ms |
| client E2E p50 | 1.448 s | 1.477 s | +29 ms — bursting changes arrival timing, not total latency |

Recipe with no-delay off: TTFT client-side (note the bounded ~150 ms offset), TBT server-side
(proxy-immune; client-side TBT measures the ~220 ms coalescing flush, not the ~30 ms decode).

## Reproduction pointers

- Spec knob fixed: `client.num_nodes: min(num_nodes, 4)` for `dest=proxy` serving
  specs (proxycmp_{haproxy,envoy,litellm,rayserve}, oat_8b_*, oat_120b). `direct`
  specs keep `num_nodes` (one client per local replica). `nullcompute` left as-is.
- Jobs: non-stream client=4 → 8545811 (27.1k); stream client=4 → 8545884 (100%
  fail), 8545993 (diag, ~5k, the table above).
- Branch `haproxy-no-delay-knob`: `http_no_delay` option + diag sampler (both
  default-safe).
