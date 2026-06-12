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

## Suite contents (mirror of plan_exp.md §9)

- **Set 1 — proxy comparison:** `proxycmp_{haproxy,envoy,litellm,rayserve,direct}`,
  N ∈ {1,4,16,64} (+ a 256 extension for haproxy/direct, run from the same specs by
  widening the matrix on capacity).
- **Set 2 — OAT robustness:** `oat_8b_{baseline,poisson,2kx2k,4kx4k,code,chat,summary,burstgpt}`
  + `oat_120b` (⚠ `rate_per_node` is a PLACEHOLDER until `calibration/sat_120b_64x64` runs),
  N ∈ {1,64}.
- **Set 3 — control-plane null-compute:** `nullcompute_scaling`
  (validation = the single `n256_r12` cell; full = the (N,R) matrix to 1024).
- **Multi-node PP:** demonstration row = `smokes/pp405b_verify_2node` (verified);
  optional OAT-style rate via `calibration/sat_405b_pp2`.

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
