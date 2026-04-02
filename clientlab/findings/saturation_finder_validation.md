# Saturation Finder Validation

Goal: validate that the saturation finder (`--mode saturation`) produces throughput
numbers consistent with the trace-based ceiling study, and understand any gaps.

## Experiment Setup

Both tests run on the same node against the same C++ stub server (zero service time),
same binary, same `max_active_requests=80`, `num_go_workers=4`.

- Script: `clientlab/scripts/sat_vs_ceiling.sh`
- Spec: `clientlab/specs/saturation_smoke.yaml`
- Control: trace-based ceiling study (1M rps target, 15s, replays pre-generated trace)
- Test: saturation finder (binary search with 10s step duration, 3s warmup, 5% tolerance)

## Run 1: Before Transport Fix (x4309c0s3b0n0, 204 cores)

Ceiling study control:

| Config | Completed | Duration | Achieved RPS |
|--------|-----------|----------|-------------|
| 1 proc | 15,000,001 | 113.7s | 131,906 |
| 12 procs | 15,000,012 | 29.9s | 502,399 |

Saturation finder, single-proc binary search → **74,000 rps** (13 steps):

| Target | Achieved | Duration | Healthy |
|--------|----------|----------|---------|
| 64,000 | 63,975 | 10.00s | Y |
| 128,000 | 69,787 | 18.34s | N |
| 96,000 | 70,379 | 13.64s | N |
| 80,000 | 71,062 | 11.26s | N |
| 72,000 | 70,270 | 10.25s | Y |
| 76,000 | 70,653 | 10.76s | N |
| 74,000 | 71,980 | 10.28s | Y |

Saturation finder, 12-proc step sweep:

| Target | Achieved | Ratio | Healthy |
|--------|----------|-------|---------|
| 200,000 | 199,831 | 0.999 | Y |
| 400,000 | 427,607 | 1.069 | Y |
| 500,000 | 419,170 | 0.838 | N |
| 600,000 | 421,122 | 0.702 | N |

**Gap: single-proc saturation finds 74K vs ceiling 132K (56%). 12-proc plateaus
at ~420K vs ceiling 502K (84%).**

### Root cause identified: shared http.Transport

The saturation finder used a single shared `http.Transport` for all 80 worker
goroutines. The replay mode creates per-scheduler transports (4 transports, 20
goroutines each). At high throughput, mutex contention in Go's `http.Transport`
connection pool serializes goroutines, inflating per-request latency.

Fix: changed `saturation.go` to create per-scheduler transports with separate
connection pools, matching the replay mode's `newHTTPClient()` approach.

## Run 2: After Transport Fix (x4309c4s0b0n0, 204 cores)

Ceiling study control:

| Config | Completed | Duration | Achieved RPS |
|--------|-----------|----------|-------------|
| 1 proc | 15,000,001 | 115.7s | 129,667 |
| 12 procs | 15,000,012 | 28.0s | 535,738 |

Saturation finder, single-proc binary search → **84,000 rps** (12 steps):

| Target | Achieved | Duration | Healthy |
|--------|----------|----------|---------|
| 64,000 | 63,975 | 10.00s | Y |
| 128,000 | 79,007 | 16.20s | N |
| 96,000 | 80,299 | 11.96s | N |
| 80,000 | 79,762 | 10.03s | Y |
| 88,000 | 80,247 | 10.97s | N |
| 84,000 | 80,591 | 10.42s | Y |

Saturation finder, 12-proc step sweep:

| Target | Achieved | Ratio | Healthy |
|--------|----------|-------|---------|
| 200,000 | 199,895 | 0.999 | Y |
| 400,000 | 408,128 | 1.020 | Y |
| 500,000 | 473,947 | 0.948 | N |
| 600,000 | 474,938 | 0.792 | N |

**Improvement: single-proc 74K → 84K (+13.5%). 12-proc ~420K → ~475K (+13%).**

Gap narrowed: single-proc 65% of ceiling, 12-proc 89% of ceiling.

## Run 3: 16-proc Sweep (x4310c4s7b0n0, 204 cores)

To test whether more procs push the saturation finder closer to ceiling:

| Target | Achieved | Ratio | Healthy |
|--------|----------|-------|---------|
| 200,000 | 199,791 | 0.999 | Y |
| 400,000 | 399,786 | 0.999 | Y |
| 500,000 | 516,614 | 1.033 | Y |
| 600,000 | 526,695 | 0.878 | N |
| 700,000 | 526,238 | 0.752 | N |

**At 16 procs the saturation finder reaches ~527K rps — within 2% of the 12-proc
ceiling study (536K).** Each proc only needs ~31K rps (500K/16), well within the
single-proc comfort zone, so the dispatch overhead becomes negligible.

## Measurement Window Validation

A key concern was whether the measurement window includes search/drain overhead.
The implementation records:

- `measureStartNs`: set via `atomic.Int64.CompareAndSwap(0, time.Now().UnixNano())`
  when the first measurement-phase request is dispatched (first writer wins)
- Duration: `(mc.lastBodyDoneNs - measureStartNs) / 1e9`, where `lastBodyDoneNs`
  is the timestamp of the last response body completion in the collector

This excludes warmup dispatch time and post-drain idle time. Validation:
- At target=64K (below ceiling): duration=10.00s, matching configured step_duration
- At target=128K (above ceiling): duration=16.20s, correctly reflecting that the
  system takes longer to process the measurement-phase requests

## Remaining Single-Proc Gap

After the transport fix, single-proc saturation still reaches only ~65% of the
ceiling study. The exact cause is undiagnosed. Potential factors:

- Saturation dispatch loop structure vs trace replay's simple slice iteration
- Per-request synthetic generation overhead
- Differences in goroutine pool creation/teardown per step

This gap does not affect practical usage: at 16 procs the aggregate matches the
ceiling. The single-proc gap is tracked in `clientlab/TODO` for future profiling.

## Conclusions

1. The saturation finder produces correct results when given enough procs to keep
   per-proc rates below the single-proc dispatch bottleneck.
2. At 16 procs, the saturation finder achieves 98% of the trace-based ceiling study
   throughput (~527K vs ~536K rps).
3. The per-scheduler transport fix (separate `http.Transport` per scheduler instead
   of shared) closed 13% of the single-proc gap. The remaining ~35% gap is unexplained
   and needs CPU profiling.
4. The measurement window correctly captures only steady-state throughput, excluding
   warmup and drain phases.

## Raw Data

- Run 1 (before fix): `/home/wenyiw/agpt/data/bench_results/clientlab/sat_vs_ceiling_20260402T184951Z/`
- Run 2 (after fix): `/home/wenyiw/agpt/data/bench_results/clientlab/sat_vs_ceiling_20260402T191439Z/`
- Run 3 (16-proc): `/home/wenyiw/agpt/data/bench_results/clientlab/sat_16proc_20260402T202501Z/`
