# SC26 workshop — experiment suite

The complete spec set for the paper's Performance Characterization section.
Cross-references `~/sc26workshop/plan/plan_exp.md` §9 (the experiment checklist);
every checklist run-item has exactly one spec here.

## Layout

| Folder | What | `num_runs` |
|---|---|---|
| `calibration/` | Single-node(/replica) saturation finders. Run once per (model, workload); their 90 %-rates are pasted into the OAT specs. | n/a (binary search) |
| `validation/` | **Pilot sweep** — the complete experiment set, but each cell runs only `2` (run 0 = warm-up, run 1 = the data point). Use it to shake out every cell end-to-end and to measure real per-cell cost before committing to the full sweep. Spec names/filenames carry a `_val` suffix so results land in separate run groups. | 2 |
| `full/` | The real sweep (**complete**). Specs mirror `validation/` minus the suffix. `6` runs for N≤64 (warm-up + 5 data), `3` for the 128/256 scale cells (warm-up + 2; large-run economy). Both SSE (`stream:true`) and E2E (`_nostream`) variants. | 6 / 3 |
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
  N ∈ {1,4,16,64} (full-sweep specs) **extended to {128,256} for all five** via
  `proxycmp_<proxy>_scale` (128→debug-scaling, 256→prod). Each also has a `_nostream`
  E2E variant at N≤64.
- **Set 2 — OAT robustness:** `oat_8b_{baseline,poisson,2kx2k,4kx4k,code,chat,summary,burstgpt}`
  + `oat_120b` (rate 17.1 from `calibration/sat_120b_64x64`),
  N ∈ {1,64}.
- **Set 3 — control-plane null-compute:** `nullcompute_scaling`
  (validation = the single `n256_r12` cell; full = the (N,R) matrix to 1024).
- **Multi-node PP:** demonstration row = `smokes/pp405b_verify_2node` (verified);
  optional OAT-style rate via `calibration/sat_405b_pp2`.

## Status tables (per subdir)

Results are data-runs-only (warm-up dropped). SLO attainment = paper SLO
(per-request TTFT≤1s ∧ P99-TBT≤250ms); only meaningful for streaming.

### calibration/ — done (rates feed the OAT specs)
| spec | swept | config | result |
|---|---|---|---|
| `sat_8b_64x64` | 1 replica | 64in/64out, binary search | 90%-rate → OAT **98** |
| `sat_8b_2kx2k` | 1 | 2k/2k | → **3.6** |
| `sat_8b_4kx4k` | 1 | 4k/2k | → **1.8** |
| `sat_8b_code` | 1 | 256/512 | → **5.4** |
| `sat_8b_chat` | 1 | 256/256 | → **10.8** |
| `sat_8b_summary` | 1 | 768/256 | → **10.8** |
| `sat_120b_paper_recal` | 1 | 120B, fixed-rate vs paper SLO | **rate 9** (r9 attain 1.000, TBT 175ms); 17.1 busted SLO |
| `sat_405b_pp2` | 2 (PP2) | 405B | ~0.25–0.3 rps/replica; demo-only (offered 1→0.31 achieved) |

### full/ — COMPLETE (the real sweep). num_runs=6 for N≤64, 3 for {128,256}. Both SSE + E2E modes.

**Set 1 — proxy comparison** · 8B, 64in/64out, 110 rps/node, `client.num_nodes=4`, stream+nostream
| spec (+`_nostream`,`_scale`) | nodes swept | config | result (streaming) |
|---|---|---|---|
| `proxycmp_direct` | 1,4,16,64,128,256 | `dest=direct` (per-node, distributed) | **scales linearly → 18.1k rps / 100% @256** |
| `proxycmp_haproxy` | 1…256 | `dest=proxy` haproxy | **plateaus ~4.7k / 100%** from 128n (centralized cap) |
| `proxycmp_envoy` | 1…256 | `dest=proxy` envoy | clean to 128n; **degrades @256 (3.0k / 44%)**; SSE TBT-heavy |
| `proxycmp_rayserve` | 1…256 | `dest=proxy` rayserve | **collapses** — success 100%→15%(n16)→3%(n256), proxy resets |
| `proxycmp_litellm` | 1…256 | `dest=proxy` litellm (uvicorn) | **collapses** — success 89%(n1)→6.5%(n64+); accept-bound |
| `..._nostream` (×5) | 1,4,16,64 | `stream:false` (E2E only) | all fast (~2s p99, ~6.8k rps); no per-token SLO |

Mode contrast: non-stream routing scales fine (≈27k-class at 256n in earlier runs); the
**streaming token-delivery path is the wall** for centralized proxies. litellm/rayserve are
saturation (proxy alive, can't accept), not crashes.

**Set 2 — OAT robustness** · 8B unless noted, stream+nostream, N∈{1,64}
| spec (+`_nostream`) | in/out | rate/node | SLO attain n1→n64 | note |
|---|---|---|---|---|
| `oat_8b_baseline` | 64/64 | 98 | 0.91 → **0.43** | short/high-rate → TBT-bound at scale |
| `oat_8b_poisson` | 64/64 | 98 | 0.89 → **0.35** | burst sensitivity (intended) |
| `oat_8b_2kx2k` | 2048/2048 | 3.6 | 1.00 → 0.90 | E2E p99 ~75s (long ctx) |
| `oat_8b_4kx4k` | 4096/2048 | 1.8 | 1.00 → **1.00** | long-context holds |
| `oat_8b_code` | 256/512 | 5.4 | 1.00 → 1.00 | |
| `oat_8b_chat` | 256/256 | 10.8 | 0.99 → 0.99 | sharegpt dataset_replay |
| `oat_8b_summary` | 768/256 | 10.8 | 1.00 → 1.00 | |
| `oat_8b_burstgpt` | 512/64 | 5.0 | 0.46 → 0.46 | azure trace, bursty (low aggregate rps) |
| `oat_120b` | 64/64 | 9 | 0.98 → 0.97 | gpt-oss-120b; 1 node-failure run re-ran clean |

**Set 3 — null-compute control plane**
| spec | nodes swept | config | result |
|---|---|---|---|
| `nullcompute_scaling` | 256,384,512,768,1024 | no GPU compute, proxy-routing stress | **NOT RUN** — held (prod, approve per-cell; ≥384n dominates cost) |

### validation/ — pilot complete (superseded by full/)
38 sub-256 cells + n256, `num_runs=2`, all families. Shook out every cell end-to-end and
measured per-cell cost before the full sweep. Kept for provenance; the figures read `full/`.

### smokes/ — passing (infra + diagnostics; re-run after stack changes)
| spec | nodes | what | status |
|---|---|---|---|
| `smoke_slo_stream_1node` | 1 | haproxy + stream + TBT + goodput | ✓ |
| `smoke_slo_stream_rayserve_1node` | 1 | Ray-native proxy TBT-buffering diagnostic | ✓ |
| `smoke_slo_stream_direct_1node` | 1 | `dest=direct` post gather-fix | ✓ |
| `dataset_replay_smoke_humaneval` | 1 | dataset_replay + Poisson e2e | ✓ |
| `nullcompute_smoke_1node` | 1 | null-compute + instrumentation probes | ✓ |
| `pp405b_verify_2node` | 2 (PP) | multi-node-PP demo: 30/30 streaming | ✓ |
| `pp2_verify_2node` / `pp2_serve_2node` | 2 | small-TP multi-replica PP (serves post per-GPU-bundle fix `17b88be`) | ✓ |
| `clean_stage_check_1node` | 1 | clean-stage flag (wipe→fresh broadcast→READY) | ✓ |
| `serverstats_ttft_{on,off}delay_1node`, `_ondelay_2node` | 1,2 | `http-no-delay` TBT + server-stats collector | ✓ (added for the no-delay study) |

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

Queue routing actually used (per-user limits: capacity max 5 queued / 2 run,
debug & debug-scaling max 1 run each, **global ~5 queued cap across queues**):
**N≤16 → capacity** (2 parallel), **N=64,128 → debug-scaling** (1h, serialized),
**N≥256 → prod**. 256n streaming with `num_runs=3` ≈ 50–60 min, so **2:30 walltime**
acquires far faster than 6h on a congested prod. NOTE: 256n runs are
node-failure-prone (~1h × 256 nodes) — re-run; the failures are diagnosable
(ping/death-signal instrumentation), not mysterious.

## How to run

```bash
# one cell interactively:            subjob --nodes N -- bash tmp/run_smoke.sh <spec>
# whole spec via PBS:                python -m eval.cli run materialize <spec> && \
#                                    python -m eval.cli run submit-all <spec>
# score:                             python -m eval.plot.goodput -e <spec> --preset paper
```

Order: `calibration/` first (fills the two placeholder rates) → `validation/`
sweep → revisit `full/` run count → `full/` sweep on capacity.


Figures: `eval/plot/sc26_full_figures.py` → `eval/plot/output/sc26_full/`
(fig1 proxy scaling [1→256], fig2 SSE-vs-E2E, fig3 workload sweep, fig4 n64 latency).

TODO:
* `nullcompute_scaling` (Set 3) — still unrun; approve per-cell (prod, ≥384n costly)
* fine-grained CDF of inter-token latency; server-side TTFT/TBT panel at 128/256
* envoy 256n: swap in the clean retry (`proxycmp_envoy_256retry`) if it lands

TODO:
* update ttft to 3s
* consistency
* add error bars
* putting in a rcfile for the consistent fontsize
* table for the e2e latency one (condense)
* cdf include errors to make litellm look worse
