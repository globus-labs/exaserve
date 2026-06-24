# Data provenance ledger (auto-generated 2026-06-16 23:36 CDT)

Audit of every materialized cell: spec, run-group, node count, **client.num_nodes
(cli)**, stream (S/-), achieved rps (run>=1), err%. The `cli` column is the
reviewer-critical one — the old specs derived client.num_nodes=num_nodes, which
overwhelms a single proxy at large N (the "regression" that wasn't).

## Validity rule
- `direct` cells: cli=num_nodes is CORRECT (one client per local replica) → VALID.
- proxy cells n1 (cli=1), n4 (cli=4): == the bounded fix min(N,4) → VALID.
- proxy cells n16 (cli=16), n64 (cli=64): 4x / 16x the bounded client fleet → SUSPECT
  (pending the n64 cli=4 check, job 8546128). At n64 they did NOT collapse on throughput
  (0% err, good rps) so the confound, if any, is in TTFT/attain, not throughput.
- proxy cells n256 (cli=256): CONFOUNDED (proven; see FINDINGS_haproxy_256n.md).
- OAT n64 (cli=64): SUSPECT (same as Set-1 n64).

```
spec                                     rg    node   cli  st      rps  err%
oat_120b_val                             run0  n1     1    S        14   0.0
oat_120b_val                             run1  n1     1    S         9   0.0
oat_120b_val                             run1  n64    64   S       544   0.0
oat_8b_2kx2k_val                         run0  n1     1    S         2   0.0
oat_8b_2kx2k_val                         run0  n64    64   S       107   0.0
oat_8b_4kx4k_val                         run0  n1     1    S         1   0.0
oat_8b_4kx4k_val                         run0  n64    64   S        55   0.0
oat_8b_baseline_direct_val               run0  n1     1    S        95   0.0
oat_8b_baseline_direct_val               run0  n64    64   S      5584   0.0
oat_8b_baseline_val                      run0  n1     1    S        95   0.0
oat_8b_baseline_val                      run0  n64    64   S      5776   0.0
oat_8b_burstgpt_val                      run0  n1     1    S        19   0.0
oat_8b_burstgpt_val                      run0  n64    64   S        19   0.0
oat_8b_chat_val                          run0  n1     1    S         9   0.5
oat_8b_chat_val                          run0  n64    64   S        91   0.2
oat_8b_code_val                          run0  n1     1    S         5   0.0
oat_8b_code_val                          run0  n64    64   S       288   0.0
oat_8b_poisson_val                       run0  n1     1    S        92   0.0
oat_8b_poisson_val                       run0  n64    64   S      5857   0.0
oat_8b_summary_val                       run0  n1     1    S        10   0.0
oat_8b_summary_val                       run0  n64    64   S       624   0.0
proxycmp_direct_256_val                  run0  n256   256  S     19361   0.0
proxycmp_direct_nostream_val             run0  n64    64   -      6765   0.0
proxycmp_direct_val                      run0  n1     1    S       104   0.0
proxycmp_direct_val                      run0  n16    16   S      1475   0.0
proxycmp_direct_val                      run0  n4     4    S       392   0.0
proxycmp_direct_val                      run1  n1     1    S        91   0.0
proxycmp_direct_val                      run1  n16    16   S      1495   0.0
proxycmp_direct_val                      run1  n4     4    S       372   0.0
proxycmp_direct_val                      run1  n64    64   S      5323   0.0
proxycmp_envoy_val                       run0  n1     1    S        98   0.0
proxycmp_envoy_val                       run0  n16    16   S      1522   0.0
proxycmp_envoy_val                       run0  n4     4    S       391   0.0
proxycmp_envoy_val                       run0  n64    64   S      5288   0.6
proxycmp_haproxy_256_val                 run0  n256   256  S      4787  33.9
proxycmp_haproxy_256_val                 run1  n256   4    S     28160 100.0
proxycmp_haproxy_256_val                 run2  n256   4    S      4578   0.1
proxycmp_haproxy_nond_256_val            run0  n256   256  -     10950   3.0
proxycmp_haproxy_nostream_c4_256_val     run0  n256   4    -     27124   0.0
proxycmp_haproxy_nostream_val            run0  n256   256  -     10976   3.3
proxycmp_haproxy_nostream_val            run0  n64    64   -      6845   0.0
proxycmp_haproxy_val                     run0  n1     1    S        98   0.0
proxycmp_haproxy_val                     run0  n16    16   S      1517   0.0
proxycmp_haproxy_val                     run0  n4     4    S       384   0.0
proxycmp_haproxy_val                     run0  n64    64   S      5850   0.0
proxycmp_litellm_val                     run0  n1     1    S       102   0.1
proxycmp_litellm_val                     run0  n16    16   S       712  80.2
proxycmp_litellm_val                     run0  n64    64   S      2795  95.3
proxycmp_litellm_val                     run2  n4     4    S       180  30.3
proxycmp_litellm_val                     run3  n4     4    S       177  25.5
proxycmp_rayserve_val                    run0  n1     1    S        95   0.0
proxycmp_rayserve_val                    run0  n16    16   S       674  87.6
proxycmp_rayserve_val                    run0  n4     4    S       163  41.9
proxycmp_rayserve_val                    run0  n64    64   S      2633  96.7
serverstats_ttft_offdelay_1node          run0  n1     1    S        20   0.0
serverstats_ttft_ondelay_1node           run0  n1     1    S        20   0.0
serverstats_ttft_ondelay_1node           run1  n1     1    S        20   0.0
```

## Reviewer-proof status (as of audit)
- SOLID: all direct cells; all proxy n1/n4; non-stream 256n (cli=4, 27.1k); the n1
  on/off-delay TTFT/TBT (Task 2); server-side collector validated.
- RESOLVED n64 cli=4 (8546128): cli=4 ~= cli=64 (attain 0.029 vs 0.042, TTFT 4.56 vs 4.47s)
  → STREAMING cells are cli-INVARIANT. n16/n64 proxy + OAT-n64 data VALID, no re-run. The
  cli confound materially hit ONLY non-stream 256n throughput (fixed: 27k).
- KNOWN-CONFOUNDED, re-run with cli=4: all proxy n256 streaming.
- litellm/rayserve n16/n64: high err (80-97%); escalates with N — need cli=4 to separate
  proxy failure from client-topology overload (lower priority; they're the "bad proxy" arm).
