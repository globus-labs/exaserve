# Client Dispatch Ceiling Analysis

Goal: find the maximum req/s a single Go process can dispatch with zero server delay,
and explain why that rate is the hard ceiling.

## Experiment Setup

- Spec: `clientlab/specs/client_dispatch_ceiling.yaml`
- `service_time=0` (C++ server responds instantly, ~6 us)
- `rate=1,000,000` (rate limiter never the bottleneck)
- `num_go_procs=1`, `num_go_workers=4` (4 dispatcher goroutines)
- Sweep `max_active_requests`: 40, 80, 160, 320, 640, 1280, 2560, 5120, 10240
- 5M requests per point, 5s configured duration (actual ~31-36s due to dispatch saturation)
- Node: Aurora x4220c4s7b0n0 (208 cores)

## Results

| max_active | achieved rps | server max_active | server p50 | server p99 | new_conns |
|-----------|-------------|-------------------|------------|------------|-----------|
| 40 | 157,699 | 27 | 6.2 us | 11.0 us | 37 |
| 80 | 158,016 | 25 | 6.5 us | 12.1 us | 59 |
| 160 | 142,069 | 22 | 6.7 us | 12.7 us | 57 |
| 320 | 157,760 | 26 | 6.8 us | 12.7 us | 78 |
| 640 | 148,338 | 28 | 6.6 us | 12.6 us | 139 |
| 1,280 | 150,918 | 32 | 6.8 us | 12.8 us | 505 |
| 2,560 | 159,729 | 23 | 6.7 us | 13.2 us | 740 |
| 5,120 | 139,276 | 43 | 7.8 us | 17.8 us | 3,110 |
| 10,240 | 149,615 | 40 | 6.6 us | 12.2 us | 6,487 |

**Ceiling: ~150-160K rps, flat across all concurrency levels.**

Phase trace latency breakdown (sampled at 5%):

| max_active | slot_hold mean | time_to_headers mean | queue_wait mean | body_read mean |
|-----------|---------------|---------------------|----------------|---------------|
| 40 | 0.176 ms | 0.137 ms | 0.016 ms | 0.018 ms |
| 2,560 | 0.504 ms | 0.367 ms | 0.067 ms | 0.059 ms |
| 10,240 | 1.316 ms | 0.789 ms | 0.410 ms | 0.094 ms |

## Why ~155K rps is the Hard Ceiling

### 1. The server is not the bottleneck

Server-side latency is 6-7 us p50 / 11-13 us p99. With max_active=40 and 7 us
latency, the theoretical server throughput via Little's Law is 40/0.000007 = 5.7M rps.
The server only sees 22-43 concurrent requests regardless of client config. It could
handle 30x more.

### 2. The bottleneck is the dispatch loop

The Go client architecture (eval/go_client/main.go):

```
4 dispatcher goroutines --> [outstandingSlots channel] --> [workCh channel] --> N worker goroutines --> HTTP round-trip
```

Each dispatcher goroutine does this per request (main.go:458-524):
1. Timing spin-wait (lines 489-493) -- busy-loops until targetTime
2. Slot acquisition (line 501) -- channel send, blocks if concurrency full
3. Work item construction (lines 510-518) -- struct alloc, rand.Float64(), nowSeconds()
4. Channel send (line 521) -- workCh <- item

### 3. Where the CPU time goes

The slot_hold at max_active=40 is 176 us mean, but the server takes only 6-7 us. The
remaining ~170 us per request is Go overhead:
- bytes.NewReader() to wrap the request body
- http.NewRequestWithContext() -- allocates an http.Request
- client.Do() -- connection pool lookup, write HTTP headers, flush, read response
- io.ReadAll() -- read and allocate response body
- json.Unmarshal() -- parse response JSON
- Channel operations, goroutine scheduling, garbage collection

### 4. The math

4 dispatchers each pinned to an OS thread via runtime.LockOSThread() (line 462).
With rate=1M and 4 dispatchers, each targets 250K rps = 4 us budget per request.
But each dispatcher spin-waits (tight `for time.Now().Before(targetTime)` loop),
burning a full core. With 4 cores consumed by spin-wait alone, the actual CPU
available for useful work is reduced.

On the worker side, each goroutine runs the full doRequest() function (~170 us
round-trip to localhost). At ~155K rps, you need ~26 concurrent workers active --
which matches server_max_active=22-27 in the data.

### 5. Why adding concurrency doesn't help

The flat curve proves the bottleneck is upstream of the worker pool. Dispatchers
can only push ~155K items/sec through the workCh channel regardless of how many
workers are listening. Workers drain workCh fast enough that the channel never
backs up (max_queue_depth=0 for all points). The constraint is the production
rate, not the consumption rate.

## Conclusion

~155K rps is the raw throughput of 4 Go goroutines marshaling HTTP requests through
Go's channel + net/http transport machinery on this Aurora compute node. The ceiling
is determined by:

1. Channel send/receive overhead (~100-200 ns per operation)
2. http.Transport connection pool locking
3. HTTP request/response serialization and allocation
4. GC pressure from per-request allocations (GOGC=200)
5. time.Now() syscalls in the spin-wait loop

This is an intrinsic property of the Go HTTP client on this hardware. No amount of
concurrency tuning can push past it from a single process. To exceed this ceiling,
scale horizontally with num_go_procs > 1.

---

# Multi-Process Scaling Analysis

Goal: does dispatch throughput scale linearly with num_go_procs, and if not, where
does it plateau and why?

## Experiment Setup

- Spec: `clientlab/specs/client_dispatch_scaling.yaml`
- Same echo-server setup: `service_time=0`, `rate=1M`, `max_active=80`
- `num_go_workers=4` per process (4 dispatcher goroutines per process)
- Sweep `num_go_procs`: 1, 2, 4, 8, 12, 16
- 10M requests per point, 10s configured duration
- Node: Aurora x4400c3s5b0n0 (208 cores)

## Results

| procs | achieved rps | speedup | efficiency | svr_max_active | svr_p50 | svr_p99 | new_conns |
|------:|------------:|---------:|-----------:|---------------:|--------:|--------:|----------:|
| 1 | 157,968 | 1.00x | 100.0% | 25 | 6.5 us | 11.9 us | 59 |
| 2 | 355,085 | 2.25x | 112.4% | 107 | 6.7 us | 12.8 us | 106 |
| 4 | 464,787 | 2.94x | 73.6% | 117 | 6.9 us | 15.9 us | 182 |
| 8 | 550,029 | 3.48x | 43.5% | 151 | 7.6 us | 45.9 us | 433 |
| 12 | 625,174 | 3.96x | 33.0% | 153 | 7.0 us | 87.0 us | 531 |
| 16 | 569,907 | 3.61x | 22.5% | 155 | 9.0 us | 143.8 us | 948 |

**Scaling is sub-linear from 4 procs onward. Peak throughput is ~625K rps at 12 procs.
At 16 procs, throughput actually regresses.**

Phase trace latency breakdown (sampled at 1%):

| procs | slot_hold mean | tth mean | tth p99 | queue_wait mean | body_read mean |
|------:|---------------:|---------:|--------:|----------------:|---------------:|
| 1 | 0.227 ms | 0.181 ms | 0.819 ms | 0.022 ms | 0.019 ms |
| 2 | 0.299 ms | 0.261 ms | 1.364 ms | 0.015 ms | 0.016 ms |
| 4 | 0.438 ms | 0.378 ms | 2.110 ms | 0.029 ms | 0.023 ms |
| 8 | 1.034 ms | 0.969 ms | 3.659 ms | 0.038 ms | 0.020 ms |
| 12 | 1.363 ms | 1.284 ms | 4.642 ms | 0.050 ms | 0.021 ms |
| 16 | 2.111 ms | 2.038 ms | 6.089 ms | 0.045 ms | 0.020 ms |

## Why Scaling Breaks Down

### 1. The shared bottleneck is the kernel TCP stack + server accept path

All Go processes connect to the same `127.0.0.1:18500`. Every HTTP request flows
through:
- Client: kernel TCP send buffer -> loopback interface -> kernel TCP recv buffer
- Server: epoll notification -> read -> process -> write -> kernel TCP send buffer
- Client: kernel TCP recv buffer -> Go userspace read

The loopback interface and kernel TCP stack are shared OS resources. At 1 proc, the
kernel handles ~158K loopback round-trips/sec with 6.5 us p50 server latency. As
processes multiply, kernel contention shows up in server-side tail latency:

- 1 proc: p99 = 11.9 us (no contention)
- 8 procs: p99 = 45.9 us (3.9x degradation)
- 16 procs: p99 = 143.8 us (12x degradation)

Server p50 stays nearly flat (6.5-9.0 us) — the fast path is unchanged. But tail
latency blows up because epoll wakeup, socket buffer management, and loopback
packet scheduling all contend under load.

### 2. Per-request latency inflates, canceling the parallelism gains

At 1 proc, each request takes 0.227 ms (slot_hold mean). At 16 procs, each request
takes 2.111 ms — 9.3x slower. By Little's Law:

  throughput = concurrency / latency

With 16 procs * 80 max_active = 1280 total concurrent slots, but latency inflated
9.3x: expected throughput = 1280 / 0.002111 = 606K rps. The actual 570K rps is close,
confirming that the throughput is latency-bound, not concurrency-bound.

### 3. Where the latency inflation comes from

time_to_headers (tth) dominates slot_hold in all cases, meaning the inflation is in
the HTTP round-trip itself, not in queue_wait or body_read:

- tth grows from 0.181 ms (1 proc) to 2.038 ms (16 procs) = 11.3x
- queue_wait stays flat at 0.02-0.05 ms
- body_read stays flat at 0.02 ms

tth measures from `client.Do()` call to first response header byte. This includes:
1. Go http.Transport acquiring a connection from the pool
2. Kernel TCP send (request bytes through loopback)
3. Server epoll wakeup + read + process + write
4. Kernel TCP recv (response bytes through loopback)
5. Go reading response headers

With 16 procs * 4 dispatch workers = 64 OS threads pinned via LockOSThread() doing
spin-wait, plus up to 1280 worker goroutines doing I/O, the kernel is handling
thousands of concurrent socket operations on the loopback path. CPU cache thrashing,
socket lock contention in the kernel, and scheduler pressure all contribute to the
latency inflation.

### 4. Server max_active plateaus at ~155

Across all points from 4+ procs, the server sees at most 117-155 concurrent requests.
Even though the clients offer 320-1280 concurrent slots, the server's reactor never
has more than ~155 requests in flight simultaneously. This confirms the bottleneck is
in getting requests to the server (TCP stack throughput), not in the server processing
them.

### 5. Regression at 16 procs

At 16 procs, throughput drops from 625K (12 procs) to 570K (16 procs). This is the
classic over-subscription cliff:
- 16 procs * 4 dispatch workers = 64 threads doing LockOSThread() spin-wait
- 64 cores burned purely on busy-wait timing loops, out of 208 total
- Plus ~1280 goroutines doing I/O across 16 Go runtimes
- Kernel CPU time for TCP processing competes with the same cores
- Result: more contention, less useful work

## Scaling Model

The data fits a model where each added process contributes diminishing throughput
due to shared-resource contention:

    procs=1:   158K (base)
    procs=2:   +197K (incremental: 197K per proc — super-linear from warm caches)
    procs=4:   +110K (incremental: 55K per proc)
    procs=8:   +85K  (incremental: 21K per proc)
    procs=12:  +75K  (incremental: 19K per proc)
    procs=16:  -55K  (negative — over-subscription)

The practical sweet spot on this hardware is **4-8 procs** for a single-node echo
workload, yielding 465-550K rps at 44-74% efficiency.

## Conclusion

Multi-process dispatch does NOT scale linearly. The ceiling is ~625K rps at 12 procs
(3.96x speedup) on this Aurora compute node. The bottleneck shifts from Go-internal
overhead (single process) to kernel TCP stack contention (multi-process):

1. **Loopback TCP contention**: all procs share one loopback path, kernel socket locks
   serialize packet processing at high rates
2. **CPU over-subscription**: 4 dispatcher threads per proc do LockOSThread() spin-wait,
   consuming cores that the kernel needs for TCP processing
3. **Latency inflation**: per-request tth grows ~11x from 1 to 16 procs, canceling
   parallelism gains via Little's Law

For real deployments with non-zero service_time (100ms+), the dispatch rate needed per
process is far lower (~10K rps at most), so this ceiling is unlikely to be a practical
concern. The scaling question matters more for understanding system limits than for
operational planning.

---

# Profiling: What Causes the Plateau

The scaling analysis above hypothesized two possible causes: (1) kernel TCP stack
contention on the loopback path, or (2) CPU over-subscription from dispatcher
spin-wait threads. A controlled experiment disambiguates them.

## Experiment Design

Three configs on the same node (x4304c6s6b0n0, 204 cores), all with service_time=0,
15M requests, rate=1M, max_active=80:

| Config | Procs | Workers/proc | Spin-wait threads | Purpose |
|--------|------:|------------:|-----------------:|---------|
| A | 1 | 4 | 4 | Baseline |
| B | 12 | 4 | 48 | Plateau (reproduces regression) |
| C | 12 | 1 | 12 | Same parallelism, fewer spin-wait |

Profiling: `mpstat -P ALL 1` (CPU breakdown), `vmstat 1` (context switches), phase
trace sampling at 1%.

**Key comparison:** if C >> B, spin-wait is the bottleneck. If B ~ C, kernel TCP is.

## Results

| Config | rps | %usr | %sys | %idle | ctx_sw/s | tth mean | tth p99 |
|--------|-------:|-----:|-----:|------:|---------:|---------:|--------:|
| A (1p/4w) | 131,972 | 5.6 | 1.5 | 92.9 | 711K | 0.293 ms | 4.702 ms |
| B (12p/4w) | 538,893 | 25.7 | 44.0 | 30.3 | 2,020K | 1.329 ms | 4.764 ms |
| C (12p/1w) | 555,774 | 25.2 | 43.7 | 31.2 | 1,733K | 1.000 ms | 3.810 ms |

## Analysis

### 1. The bottleneck is the kernel, not spin-wait

B and C perform nearly identically: 539K vs 556K rps (3% difference). Reducing
spin-wait threads from 48 to 12 barely helps. **CPU over-subscription from spin-wait
is NOT the primary cause.**

### 2. Kernel syscall overhead dominates

At 12 procs, %sys=44% — the kernel consumes nearly twice as much CPU as userspace
(%usr=26%). At 1 proc, kernel is negligible (%sys=1.5%). The jump from 1.5% to 44%
as we go from 1 to 12 procs is the smoking gun.

The kernel CPU time comes from:
- `sendmsg()`/`recvmsg()` syscalls for every HTTP request/response on loopback
- TCP connection management (keep-alive pool, socket buffer allocation)
- `epoll_wait()` / `epoll_ctl()` on both client (Go runtime) and server sides
- Socket lock contention in the kernel TCP stack (all procs hit the same listener)
- Context switches: 711K/s → 2M/s (3x increase)

### 3. Loopback TCP is the specific bottleneck

At 539K rps, each request involves at minimum:
- Client: `connect()` or connection reuse check, `write()` (request), `read()` (response)
- Kernel: loopback packet copy, TCP state machine, socket buffer management
- Server: `epoll_wait()` wakeup, `read()`, `write()`

That's ~6+ syscalls per request = ~3.2M syscalls/s. On 204 cores, 44% sys = ~90
core-equivalents consumed by kernel. Each syscall averages ~28us of kernel time
(90 cores / 3.2M syscalls × 1e6), which is consistent with known overhead for
TCP socket operations under contention.

### 4. tth inflation confirms kernel path

time_to_headers grows from 0.293ms (1 proc) to 1.329ms (12 procs, 4 workers) and
1.000ms (12 procs, 1 worker). Since tth spans the full syscall path (Go client.Do →
kernel send → server recv/process/send → kernel recv → Go read), the 4.5x inflation
maps directly to the kernel being saturated.

Config C's tth is 25% lower than B (1.000 vs 1.329ms) despite similar throughput,
suggesting the extra spin-wait threads add modest scheduling pressure but are not
the primary constraint.

### 5. Why ~550K rps is the wall

At 550K rps with 44% sys, the kernel is processing ~3.2M syscalls/s using 90 of 204
cores for kernel work. Adding more Go processes would push more syscalls into an
already-saturated kernel, increasing per-syscall latency and yielding diminishing
(then negative) returns — exactly the regression seen at 16 procs in the scaling
sweep.

## Conclusion

The multi-process dispatch plateau at ~550-625K rps is caused by **kernel TCP stack
saturation on the loopback path**, not by Go-side CPU over-subscription. The kernel
consumes 44% of total CPU (90 core-equivalents) servicing socket syscalls at this
rate. This is a fundamental limit of running high-frequency HTTP over localhost TCP
on this hardware.

Potential bypass paths (not needed for real deployments):
- Unix domain sockets (skip TCP stack entirely)
- io_uring (batch syscalls, reduce context switches)
- Shared-memory IPC (eliminate kernel networking entirely)

None of these are worth pursuing since real inference workloads with 100ms+ service
times need <10K rps per process, well within the 150K/proc ceiling.

## Auto-Derived Concurrency Validation (2026-04-05)

After removing `go_concurrency` as a required user parameter, validated that
auto-derived settings (`max_active_requests=0` → derives from ephemeral port range,
capped at 10240) produce no regression.

### Single-proc: no regression

Client safe zone (8 points, 2D sweep of concurrency × service_time):

| Config | Previous | Auto-derived | Delta |
|--------|----------|-------------|-------|
| active=2048, svc=0ms | 10000.0 | 10000.0 | 0% |
| active=2048, svc=100ms | 9804.0 | 9803.8 | 0% |
| active=8192, svc=1000ms | 6891.6 | 6990.8 | +1.4% |
| active=8192, svc=2000ms | 3532.9 | 3534.6 | 0% |

Phase 0 saturation (Llama-3-8B, 1 node): 25 rps (exact match).

Dispatch ceiling (auto-derived, service_time=0): ~105K rps on test node.
With explicit max_active=10240 on same node: ~102K rps. With max_active=80: ~88K rps.
All consistent — node was ~57% of the original ceiling node's capacity.

### Multi-proc: port exhaustion at high auto-derived concurrency

With auto-derived `max_active=10240` per proc, multi-proc fails:
- 4 procs × 10240 = 40960 total connections → exceeds ephemeral port range (28232)
- TIME_WAIT socket accumulation compounds the issue

With explicit `max_active=80` per proc (the known-good config):

| Procs | Achieved RPS | Errors | Per-proc active |
|-------|-------------|--------|----------------|
| 1 | 89,611 | 0 | 80 |
| 2 | 227,805 | 0 | 80 |
| 4 | 394,782 | 0 | 80 |
| 8 | 445,408 | 0 | 80 |
| 12 | 483,449 | 0 | 80 |

With `10240 // nprocs` per proc (auto-derived, reduced):

| Procs | Achieved RPS | Errors | Per-proc active |
|-------|-------------|--------|----------------|
| 1 | 98,260 | 0 | 10240 |
| 2 | 230,580 | 0 | 5120 |
| 4 | 254,977 | 0 | 2560 |
| 8 | 274,402 | 0 | 1280 |
| 12 | 312,181 | 0 | 853 |

Higher per-proc concurrency **hurts** multi-proc throughput with fast servers:
connection churn overhead outweighs parallelism gains. At 4+ procs, max_active=80
outperforms max_active=2560 by 35%.

### Resolution

- **Single-proc**: auto-derive (10240 cap) works for all workloads.
- **Multi-proc**: spec must set `max_active_requests` explicitly. For fast servers
  (clientlab), 80 per proc is optimal. For real inference, use
  `ceil(target_rps × avg_latency / num_go_procs)`.

### Deep investigation: why high max_active degrades multi-proc (2026-04-05)

Ran 1M requests at 100K rps (service_time=0) with full metrics (httptrace, phase
traces at 1% sampling). Results in `conc_investigation_20260405T163321Z/`.

**Single-proc: connections are reused, tail latency is the issue**

| max_active | reuse% | tth p50 | tth p99 | tth mean |
|-----------|--------|---------|---------|----------|
| 80 | 100% | 96μs | 4.4ms | 300μs |
| 320 | 100% | 70μs | 5.7ms | 420μs |
| 1280 | 99.98% | 73μs | 20.7ms | 1.8ms |
| 5120 | 99.6% | 88μs | 160ms | 12.1ms |
| 10240 | 99.4% | 100μs | 376ms | 32.3ms |

Connection reuse is 99%+ at all levels — **connection churn is NOT the cause**.
The p50 is flat (~80-100μs). The p99 grows exponentially — this is goroutine
scheduling contention in the Go runtime. With 10240 goroutines competing for
CPU, some goroutines wait 100-300ms before processing their HTTP response.

**Multi-proc: TIME_WAIT from rare new connections exhausts ports**

| per_proc | total | port_exhaustion errors | error |
|----------|-------|----------------------|-------|
| 80 | 960 | 0 | — |
| 320 | 3840 | 0 | — |
| 1280 | 15360 | 60,042 | EADDRNOTAVAIL |
| 2267 | 27204 | 112,951 | EADDRNOTAVAIL |

Even though 99.4% of requests reuse connections, the 0.6% that create new ones
close them after use → TIME_WAIT (60s hold). With 12 procs × 0.6% × high request
rate, TIME_WAIT sockets accumulate and exhaust the ephemeral port range (28232).

**Root cause chain:**
1. High max_active → many goroutines → Go scheduling contention
2. Scheduling delays → goroutines slow to return connections to idle pool
3. Idle pool miss → transport creates new connection (0.6% of requests)
4. New connection closes → TIME_WAIT socket (60s hold)
5. Multi-proc: 12 procs × small TIME_WAIT rate → port exhaustion

### Connection management design

`go_concurrency` controls three things simultaneously:
- `outstandingSlots` channel capacity (goroutine concurrency)
- `MaxConnsPerHost` on http.Transport (TCP connection cap per host)
- `MaxIdleConnsPerHost` (idle connection pool size)

When `MaxConnsPerHost` limit is reached, goroutines block in a FIFO queue inside
the transport until a connection frees up. Setting `MaxConnsPerHost = max_active`
ensures goroutines never wait for connections — each active goroutine has its own.

Port exhaustion (`EADDRNOTAVAIL`, `EMFILE`) is now detected and reported as
error class `"port_exhaustion"` in per-request results.

## Raw Data

- Single-proc sweep: `/home/wenyiw/agpt/data/bench_results/clientlab/client-dispatch-ceiling_20260401T235004Z/`
- Multi-proc sweep: `/home/wenyiw/agpt/data/bench_results/clientlab/client-dispatch-scaling_20260402T013700Z/`
- Profiling experiment: `/home/wenyiw/agpt/data/bench_results/clientlab/dispatch_profiling_20260402T024400Z/`
- Safe zone re-run: `/home/wenyiw/agpt/data/bench_results/clientlab/client-safe-zone_20260405T000211Z/`
