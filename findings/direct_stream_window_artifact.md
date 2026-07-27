# Direct-streaming "shortfall" = fixed-window accounting (RESOLVED 2026-07-27)

Question: why does direct streaming measure 18.1k QPS at 256 nodes (71% of
the single-node rate) when nothing is shared between nodes?

Answer: it is the measurement denominator, not capacity.

## Code
`eval/lib/replay_engine.py:1142-1145` — a run's duration is measured from
run start to the LAST completion across ALL ranks
(`end_time = max(item[4] ...)`); `rps = completed / duration`. The
denominator therefore includes the fleet-maximum drain.

## Data (direct_stream_window_analysis.py over
`full/proxycmp_direct{,_scale}` per-request records; start = first_token_at
− ttft, end = start + latency)

| N | n (= 110·60·N) | drain range (s) | frac after send | T/(T+d) | measured/offered |
|---|---|---|---|---|---|
| 1 | 6,600 | 2.2–10.7 | 6–24% | 0.85–0.96 | matches ≤0.5% |
| 64 | 422,400 | 15.2–18.2 | ~18% | 0.77–0.80 | matches |
| 128 | 844,800 | 18.4–20.8 | ~23% | 0.74–0.77 | matches |
| 256 | 1,689,600 | 30.4–34.8 | ~28% | 0.63–0.66 | matches |

- Every scheduled request completes with zero errors at every N; rps over
  the 60 s send window = 110.0/node everywhere. No capacity is lost.
- 18–28% of requests finish after injection stops = the standing queue at
  the deliberately saturating rate (110 > ~106 sustainable) — present at
  n1 too.
- Fleet drain = max over N per-node drains → extreme-value growth
  (n1 spread 2–11 s is the underlying distribution; 256 nodes sample its
  tail: ~33 s).
- T/(T+d) reproduces measured/offered to <0.5% at all 15 run cells.
  n256 run2: 60/92.2 = 0.651 → 71.6 QPS/node (the paper's "71").

## Same mechanism as PP
Identical to the 405B finding (pp_efficiency_drop_explained): eff =
T/(T+overhang) exact; durtest T=120/300/600 → 82/92/95% delivered.

## Gotchas
- n1 latency varies hugely run-to-run at saturation (p50 3.3–10.2 s);
  comparing one lucky n1 run to a large-N aggregate manufactures a fake
  "latency grows with N" trend.
- `overall` in result JSONs uses the LAST run's duration.
