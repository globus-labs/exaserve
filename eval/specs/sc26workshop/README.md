# SC26 workshop — experiment suite

The complete spec set for the paper's Performance Characterization section.
Cross-references `~/sc26workshop/plan/plan_exp.md` §9 (the experiment checklist);
every checklist run-item has exactly one spec here.

## Layout

| Folder | What | `num_runs` |
|---|---|---|
| `calibration/` | Single-node(/replica) saturation finders. Run once per (model, workload); their 90 %-rates are pasted into the OAT specs. | n/a (binary search) |
| `validation/` | **Pilot sweep** — the complete experiment set, but each cell runs only `2` (run 0 = warm-up, run 1 = the data point). Use it to shake out every cell end-to-end and to measure real per-cell cost before committing to the full sweep. Spec names/filenames carry a `_val` suffix so results land in separate run groups. | 2 |
| `full/` | The real sweep. Identical specs to `validation/` minus the suffix, with `6` runs (run 0 warm-up + 5 data points → mean/var per the v2 protocol). 6 is tentative — revisit after the validation sweep timing. | 6 |
| `smokes/` | 1-node/2-node infrastructure validation + the PP demonstration (`pp405b_verify_2node`). Already exercised; keep for re-validation after stack changes. | 1 |

Spec names must be unique across the whole `eval/specs/` tree (the catalog
resolves by filename stem), hence the `_val` suffix convention.

**Results mirror this layout**: run groups land under
`<experiments_root>/runs/<spec-folder>/<spec-name>/runN/` (e.g.
`runs/sc26workshop/full/oat_8b_baseline/run0/`), derived automatically from the
spec's location (`spec_runs_dir` in `eval/lib/run_planner.py`). Pre-existing
flat result dirs were relocated with symlinks left at the old paths, and the
resolver falls back to flat `runs/<name>/` for anything never relocated.

Invariant: `runs/sc26workshop/` contains only folders that mirror this spec
tree, and a folder there means real data from that suite stage.

All `validation/` and `full/` specs set `launch.clean_stage: true`: node-local
artifacts (staged weights, instrumentation, scratch) are wiped before staging,
so the bring-up/lifecycle numbers (Phase 2 included) are cold-start regardless
of whether cells run as fresh PBS jobs or back-to-back on a reused allocation.
Serving metrics are unaffected (staging precedes the replay; the in-group
warm-up protocol is unchanged). `calibration/` and `smokes/` leave it off for
iteration speed.

## Suite contents (mirror of plan_exp.md §9)

- **Set 1 — proxy comparison:** `proxycmp_{haproxy,envoy,litellm,rayserve,direct}`,
  N ∈ {1,4,16,64} (+ a 256 extension for haproxy/direct, run from the same specs by
  widening the matrix on capacity).
- **Set 2 — OAT robustness:** `oat_8b_{baseline,poisson,2kx2k,4kx4k,code,chat,summary,burstgpt}`
  + `oat_120b` (rate 17.1 from `calibration/sat_120b_64x64`),
  N ∈ {1,64}.
- **Set 3 — control-plane null-compute:** `nullcompute_scaling`
  (validation = the single `n256_r12` cell; full = the (N,R) matrix to 1024).
- **Multi-node PP:** demonstration row = `smokes/pp405b_verify_2node` (verified);
  optional OAT-style rate via `calibration/sat_405b_pp2`.

## Run checklist (per folder)

### calibration/ — 9 of 9 done
- [x] `sat_8b_64x64` — 109 rps/node → OAT rate **98**
- [x] `sat_8b_2kx2k` — 4 → **3.6**
- [x] `sat_8b_4kx4k` — 2 → **1.8**
- [x] `sat_8b_code` — 6 → **5.4**
- [x] `sat_8b_chat` — 12 → **10.8**
- [x] `sat_8b_summary` — 12 → **10.8**
- [x] `sat_120b_64x64` — throughput knee 19 rps/node → 17.1, but SUPERSEDED: 17.1 busted
      the paper SLO at sustained load (attain 0.14). See `sat_120b_paper_recal`.
- [x] `sat_120b_paper_recal` — fixed-rate sweep vs paper SLO: r9 attain **1.000** (TBT p99
      175ms), r12 0.94 (382ms), r15 0.93. → **120B OAT rate = 9** (committed `0d62a97`).
- [x] `sat_405b_pp2` — replica ceiling BELOW the finder's 1 rps floor: offered 1 -> achieved
      0.31 rps, p99 TTFT 25 s (verify run: ~0.5 rps with growing queue). Conclusion: treat the
      405B row as a demonstration (raw TTFT/TBT); an OAT-style rate would be ~0.25-0.3 rps/replica
      and needs longer windows for stable P99 stats.

### validation/ — 32 of 39 cells done (N=1 complete; N>1 mostly done)
Counted by CELL. Attainment = paper SLO (TTFT≤1s ∧ P99-TBT≤250ms), warm-up dropped.

**N=1 (14/14 done):** 8B fixed-shape rows ≥0.99 (baseline 0.994, 2kx2k 0.991, 4kx4k/code/
summary/burstgpt 1.000, chat 0.995); poisson 0.761 (burst sensitivity, intended); proxies
0.19–0.63 (rate 110 ≈ knee → TTFT-bound; Set 1 scored by RPS/η, not n1 attainment).

**N=64 OAT (8/9 done) — KEY FINDING: high-rate `dest=proxy` rows collapse at scale while
throughput stays ~linear:**
- [x] `oat_8b_baseline_val` n64 — rps **5776** (η≈0.92) but attain **0.236** (TTFT p99 2.66s, TBT p99 940ms)
- [x] `oat_8b_poisson_val` n64 — rps 5857, attain **0.375**
- [x] `oat_8b_2kx2k_val` 0.943 · [x] `4kx4k` 0.995 · [x] `code` 1.000 · [x] `chat` 0.996 ·
      [x] `summary` 1.000 · [x] `burstgpt` (low-rate rows hold — don't stress the proxy funnel)
- [ ] `oat_120b_val` n1+n64 @ **rate 9** — re-materialized run1; submitting now (old 17.1 run0 superseded)

**Set 1 proxy N∈{4,16,64} (12/15 done) — throughput η ~0.9 but n64 attainment craters:**
- [x] `proxycmp_haproxy_val` n4/16/64 — η .98/.97/.93, n64 attain **0.04**
- [x] `proxycmp_envoy_val` n4/16/64 — η 1.0/.97/.85, n64 attain **0.04**
- [x] `proxycmp_rayserve_val` n4/16 · [ ] n64 (`8541790`, draining)
- [x] `proxycmp_direct_val` n4/16 · [ ] **n64 (`8541577`, draining) ← decisive proxy-funnel test**
- [x] `proxycmp_litellm_val` n16 · [ ] **n4, n64 — GAP:** cleanup submit-all timed out twice
      (debug-scaling backlog); needs another submit pass

**HELD (prod queue):**
- [ ] `nullcompute_scaling_val` — the single `n256_r12` cell

### full/ — 0 of 15 done (gated on the validation sweep + budget)
- [ ] `oat_8b_*` ×8 + `oat_120b` (rate **9**, paper-SLO recal) — N ∈ {1,64}; baseline also N=256
- [ ] `proxycmp_*` ×5 — N ∈ {1,4,16,64}; haproxy+direct extended to 256 (capacity)
- [ ] `nullcompute_scaling` — full (N,R) matrix to 1024; approve per-cell (cost!)

### smokes/ — 7 of 9 exercised, all passing
- [x] `smoke_slo_stream_1node` — HAProxy + stream + TBT + goodput (run repeatedly)
- [x] `smoke_slo_stream_rayserve_1node` — Ray-native proxy; the TBT-buffering diagnostic
- [x] `smoke_slo_stream_direct_1node` — dest=direct, verified post-fix (`ea2c5e9`)
- [x] `dataset_replay_smoke_humaneval` — dataset_replay + Poisson end-to-end
- [x] `nullcompute_smoke_1node` — null-compute + instrumentation probes
- [x] `pp405b_verify_2node` — **the multi-node-PP demonstration: 30/30 streaming** ✓
- [x] `pp2_verify_2node` — diagnostic; documents the small-TP auto-pack/over-density failure
- [ ] `pp2_serve_2node` — small-TP multi-replica PP; known-failing (tile co-location OOM);
      revisit only with the whole-node-rounding guard or ≥4 nodes
- [ ] `clean_stage_check_1node` — clean-stage flag check (added with `884e1a0`; not run in
      the eval sessions — run after any staging-path change)

## Machine-time estimate (coarse, queue wait excluded)

Per-cell model from measured runs: bring-up ≈ 4 min (8B) / 7 min (120B) /
22 min (405B PP2) / 35 min (null-compute @256n); each run ≈ 2.25 min
(60 s trace + 75 s cooldown). Validation cell ≈ bring-up + 4.5 min; full
cell ≈ bring-up + 13.5 min. Each cell is its own PBS job (no cluster reuse).

| Block | validation (2 runs) | full (6 runs) |
|---|---|---|
| calibration (7 + 405B finder) | ~4 node·h (one-time) | — |
| Set 2 OAT, N=1 (9 specs) | ~1.5 node·h | ~3 node·h |
| Set 2 OAT, N=64 (9 cells) | ~80 node·h | ~170 node·h |
| Set 2 baseline N=256 | — | ~210 node·h |
| Set 1 (5 proxies × N∈{1,4,16,64}) | ~60 node·h | ~125 node·h |
| Set 1 256-extension (haproxy+direct) | — | ~430 node·h |
| Set 3 (`n256_r12` only / full matrix) | ~150 node·h | ~2–4 k node·h (≥384n cells dominate; approve per-cell) |
| **Total** | **~300 node·h** | **~1 k node·h ex-Set 3; 3–5 k with Set 3** |

Debug/debug-scaling fits: N=1 and N≤64 validation cells (≤1 h each).
Anything ≥256n must be submitted to the **prod** queue (the keepalive
"capacity" job is small interactive headroom, not 256 nodes) — held until the
sub-256n sweep is done.

## How to run

```bash
# one cell interactively:            subjob --nodes N -- bash tmp/run_smoke.sh <spec>
# whole spec via PBS:                python -m eval.cli run materialize <spec> && \
#                                    python -m eval.cli run submit-all <spec>
# score:                             python -m eval.plot.goodput -e <spec> --preset paper
```

Order: `calibration/` first (fills the two placeholder rates) → `validation/`
sweep → revisit `full/` run count → `full/` sweep on capacity.
