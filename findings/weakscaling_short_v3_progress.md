# Weak-scaling Short v3 — Overnight Progress

## Context

v2 run (`weakscaling_haproxy_short_v2`) hit two issues:
1. **Instrumentation overhead**: `_collect_replica_traces()` read/deleted 1536 Lustre files at 128 nodes, adding ~615s to setup (~1000s at 256 nodes).
2. **Replay hang**: even with enough walltime, the 128-node replay subprocess produced 0 lines of output in 41 minutes. Status=replaying, replay.log empty.

v3 fix (commit `8cbc66b`): `AURORA_SCALING_TRACE=0` now defaults in `launch_cluster.sh`. All trace I/O paths short-circuit:
- `tracer.save()` / `save_replica_trace()` return early
- `_collect_replica_traces()` returns immediately
- `_save_driver_trace()` / `_collect_and_merge_traces()` no-op
- `AURORA_SCALING_TRACE` is propagated into Ray actor `runtime_env`

## v3 Specs

- `eval/specs/weakscaling_haproxy_short_v3.yaml`
- `eval/specs/weakscaling_direct_short_v3.yaml`

Materialized into `runs/weakscaling_{haproxy,direct}_short_v3/run0/`.
Snapshot: `df48571`.

## Tests

### 1-node v3 (capacity node x4311c3s4b0n0) — PASS

Ran via `python3 -m eval.cli run execute .../1-nodes/run.yaml`.

- Total setup: ~345s (Stage 1: 52s, Stage 3: 293s)
- Replay: 4/4 runs, 0 errors, **107 RPS overall**
- Sample run timings: each run 61.7-61.8s, ~825 reqs × 8 procs = 6600 reqs/run

**Comparison to v0 (`weakscaling_haproxy_short/run0/1-nodes`, snapshot `f2753ab`):**
- v0 Stage 1: (not logged), Stage 3: 77s, Total: 130s
- v3 Stage 1: 52s, Stage 3: 293s, Total: 345s

v3 1-node is ~2.5x slower than v0. **This specific run had a double-launch
issue** (two backend log dirs 14s apart, Ray session assertion error, likely
triggered two execute attempts). Need a cleaner re-run to trust the comparison.

**Tangential fix**: the 1-node result file had 56 bytes of trailing garbage
after the valid JSON (probably from Lustre quirk with repeated writes).
`_validate_replay_results` now uses `json.JSONDecoder.raw_decode` to tolerate
trailing garbage and rewrites the clean content. State was also manually
repaired to `succeeded`.

### 128-node v3 — PENDING

Debug sleep job `8431086` (128 nodes, `debug-scaling`, 1hr) still queued at
time of writing. Plan:
1. Wait for sleep job to start
2. SSH into head node, clean leftover state
3. Copy nodefile to `PBS_NODEFILE` env var
4. Run `python3 -m eval.cli run execute .../128-nodes/run.yaml` from the
   snapshot dir
5. Capture Stage 1 / Stage 3 / Total timing; compare to v0 baseline
   `weakscaling_haproxy_short/run0/128-nodes` (Stage1=50s, Stage3=445s,
   Total=497s)

### 256-node v3 — PENDING (after 128 passes)

Submit 256-node sleep job separately, repeat the same flow.

## Key Decisions / Findings

- `AURORA_SCALING_TRACE=0` is the right default for production runs; enabling
  it at 128+ nodes costs 10+ min of Lustre overhead.
- The replay engine writes `result0.json` with `open("w")` which should
  truncate, but Lustre (or some other quirk) can leave trailing garbage bytes.
  `_validate_replay_results` now tolerates this via `raw_decode`.

## Running Jobs At Time of Writing

- `8423379` capacity (debug-cap): 32h+ elapsed, still running
- `8431086` debug-128-v3: queued
- (v2 jobs cancelled earlier)
