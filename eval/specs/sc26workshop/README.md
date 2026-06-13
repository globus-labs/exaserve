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

### calibration/ — 7 of 8 done
- [x] `sat_8b_64x64` — 109 rps/node → OAT rate **98**
- [x] `sat_8b_2kx2k` — 4 → **3.6**
- [x] `sat_8b_4kx4k` — 2 → **1.8**
- [x] `sat_8b_code` — 6 → **5.4**
- [x] `sat_8b_chat` — 12 → **10.8**
- [x] `sat_8b_summary` — 12 → **10.8**
- [x] `sat_120b_64x64` — 19 rps/node → OAT rate **17.1** (READY in 203 s)
- [ ] `sat_405b_pp2` — 2 nodes, ≥90 min window; only needed if the PP row joins the OAT table

### validation/ — 0 of 15 done (the pilot sweep has not started)
- [ ] `oat_8b_{baseline,poisson,2kx2k,4kx4k,code,chat,summary,burstgpt}_val` (9 incl. 120b below)
      — N=1 cells fit debug; N=64 cells fit debug-scaling (≤1 h each)
- [ ] `oat_120b_val` — rate filled (17.1)
- [ ] `proxycmp_{haproxy,envoy,litellm,rayserve,direct}_val` — N ∈ {1,4,16,64}
- [ ] `nullcompute_scaling_val` — the single `n256_r12` cell; **capacity only**

### full/ — 0 of 15 done (gated on the validation sweep + budget)
- [ ] `oat_8b_*` ×8 + `oat_120b` (rate 17.1) — N ∈ {1,64}; baseline also N=256
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
Capacity needed for: anything ≥256n, and the full sweep's volume.

## How to run

```bash
# one cell interactively:            subjob --nodes N -- bash tmp/run_smoke.sh <spec>
# whole spec via PBS:                python -m eval.cli run materialize <spec> && \
#                                    python -m eval.cli run submit-all <spec>
# score:                             python -m eval.plot.goodput -e <spec> --preset paper
```

Order: `calibration/` first (fills the two placeholder rates) → `validation/`
sweep → revisit `full/` run count → `full/` sweep on capacity.
