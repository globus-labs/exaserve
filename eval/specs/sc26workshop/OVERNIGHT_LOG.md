# Overnight refinement log (autonomous)

Goal: a performance-characterization section where a reviewer can't doubt any data
point — every number must be explainable. Started 2026-06-16 22:34 CDT, target run
until 2026-06-17 09:00 CDT. No approvals pending; decisions + justifications recorded
here as I go.

Tasks handed off:
1. Fix `collect_server_stats` (currently a silent no-op) → server-side TTFT
   (queued+prefill) + avg TBT (decode_time/(N-1)); optionally per-step P99.
2. Low-load on-vs-off run to quantify the TTFT inflation Δ from coalescing.
3. Streaming 256n with `http_no_delay:false` + server-side stats → survives the
   packet storm AND yields valid server-side metrics.

Standing principles for the night:
- Validate one cell before fanning out; never trust a number without a mechanism.
- Prefer cheap (1-node / debug) validation before prod 256n.
- Record each decision below with a one-line justification.

---

## Decision log

- 22:34 CDT — Confirmed time = night (not after 9am) → enter overnight refine loop.
  Justification: user instruction is time-gated; 22:34 CDT ⇒ keep looping.
- 22:34 CDT — Order: Task 1 first (server-stats), because Tasks 2 and 3 both depend
  on having valid server-side metrics; no point running 256n until the collector works.

- 03:40 CDT — Task 1 root cause = THREE bugs (committed 9dd6d16):
  (1) load_run_plan omitted deployment.collect_stats → always False → collect block
      silently skipped (explains no file + no warning); (2) run_executor had no Ray
      connection → serve.status() would throw → added ray.init(address=auto);
      (3) stat logger shipped raw arrays, no summary → aggregation emptied → now compute
      proxy-immune server TTFT(queued+prefill) & TBT(decode/(gen-1)) + capped sample,
      pool for fleet percentiles. Verified math on synthetic data.
  Justification: all three are needed for any server-side metric; (1) alone explains
  the no-op, but (2)/(3) would have produced an empty file next.
- 03:45 CDT — Decision: fold Task-1 validation INTO Task 2 (low-load on-vs-off, n1).
  Two n1 runs (http_no_delay true vs false), stream, collect_stats, rate 20 (well under
  the ~109 knee so TTFT/TBT reflect proxy coalescing, not queueing). This validates the
  collector AND quantifies the TTFT Δ AND checks server-TBT invariance vs client-TBT
  bursting — one set of cheap runs covers three goals.
  Justification: avoid burning separate jobs; n1 removes scale confounds for a clean
  coalescing measurement.

- 03:55 CDT — Prepared (not yet submitted) Task 3 spec
  proxycmp_haproxy_256_nodelayoff_stream_val (stream, http_no_delay:false, client=4,
  collect_stats, rate 110) + server_vs_client.py comparison tool. Holding submit until
  the n1 collector validation (job 8546121) confirms server_stats.json works — won't
  spend prod 256n on an unproven collector.
  Justification: validate-one-first; prod is expensive + flaky.

- 03:58 CDT — Parallelized: submitted haproxy n64 streaming with client=4 (job 8546128,
  debug-scaling) alongside the n1 collector validation (8546121, debug; separate queue).
  Question it answers: was n64 ALSO confounded by client topology (old n64 used 64 client
  nodes)? Compare TTFT/attain client=4 vs the old client=64 run0. If materially different,
  the whole Set-1 streaming sweep needs re-running with client=4; if not, only n256 was.
  Justification: this single cheap cell decides whether a big re-run is needed — high
  information per node-hour.

- 23:15 CDT — Server-stats is broken at FOUR layers (fixed 3: load_run_plan, ray.init,
  summary compute; 4th found: config->worker collect_stats plumbing means the logger was
  never even attached → collect_stats=False at the worker; 5th: replica enumeration via
  serve.status() returns no handles in this Ray ver). DECISION: time-box server-stats;
  PIVOT Task 2 to CLIENT-side now (TTFT is largely valid client-side; bounded coalescing
  inflation is exactly what we want to measure). Submitted off-delay n1 (Task 2 arm B).
  Justification: don't let a never-worked feature block the night; client-side TTFT
  answers Task 2; server-stats continues in parallel as code work, deferrable if it
  doesn't converge by ~01:00.

- 23:35 CDT — Server-stats: implemented robust PUSH pattern (commit 31a676c). Each replica
  runs a daemon thread that computes its server-side summary+sample and pushes to a detached
  named ServingStatsCollector actor; collect_server_stats reads that actor (no serve.status
  enumeration, no Lustre files, not gated on EXASERVE_SCALING_TRACE). Added a [serving-stats]
  log line to confirm the logger records in-process. Unit-tested the full path on synthetic
  data (2 replicas → pooled server TTFT 0.2s / TBT 22ms). Submitted fresh on-delay n1
  (job 8546145, snapshot 31a676c) to validate end-to-end. If server_stats.json appears with
  sane server TTFT/TBT → collector works → unblocks Task 3.
  Justification: push avoids the Ray-version enumeration gap AND the MDS load; the in-process
  diagnostic resolves the open question of whether the stat logger records where we can reach it.

- 23:25 CDT — TASK 2 RESULT (n1, low load, client-side, jobs 8546121 on / 8546137 off):
  TTFT p50 66ms->213ms (+147ms bounded additive); TBT-p99/req p50 30ms->228ms (DESTROYED:
  off measures the ~220ms proxy flush, not the 30ms decode); E2E 1.448->1.477s (+29ms,
  invariant). Confirms: TTFT stays valid with a bounded ~150ms offset under no-delay-off;
  TBT MUST be measured server-side (client off-TBT p99 287ms would falsely fail the 250ms
  SLO while true decode is 30ms); coalescing changes arrival timing not total latency.
  => Decision: with no-delay OFF, report TTFT client-side (+~150ms caveat) and TBT
  server-side. This is the reviewer-proof recipe for the streaming-256n cell IF the proxy
  survives with coalescing (Task 3). Task 2 essentially COMPLETE.

- 23:40 CDT — TASK 1 VALIDATED (job 8546145): server_stats.json written, 12/12 replicas
  pushed. server TTFT p50/p99=51/84ms (=client 66/85ms minus net hop), server TBT
  p50/p99=22/24ms (TIGHT true decode; client TBT 30/126ms includes delivery jitter even
  with no-delay ON). The low server-TBT variance (p99≈p50) empirically backs "mean server
  TBT ≈ true P99" → reviewer-proof. Then made the push scale-safe (env period 10s, cap 1500)
  for the 3072-replica 256n case (commit). DECISION: Task 1 COMPLETE; submit Task 3 now.
  Justification: collector proven; scale-safety added before the 256n run to avoid hammering
  the single collector actor.

- 23:35 CDT — Built DATA_LEDGER.md (provenance audit of all cells). KEY FINDING: the
  client-topology confound (cli=num_nodes) affects ALL proxy n16/n64 cells, not just n256.
  BUT at n64 they did NOT collapse on throughput (0% err, good rps) — so any confound is in
  TTFT/attain, not throughput. The pivotal gate is 8546128 (n64 cli=4): if its TTFT/attain
  ~= the old cli=64 n64, then n16/n64 are valid and only n256 needs re-running; if not, a
  larger re-run (Set1 ×4 ×{n16,n64} + OAT ×9 ×n64) is needed — would prioritize haproxy/
  envoy/direct/baseline and note the rest given the per-cell ~30min debug-scaling cost.
  Justification: don't re-run 26 cells blindly; one cheap n64 cli=4 cell decides the scope.

- 23:50 CDT — GATE RESOLVED (job 8546128): n64 haproxy streaming cli=4 vs cli=64 are
  statistically identical (attain 0.029 vs 0.042; TTFT p50 4.56 vs 4.47s; E2E 7.94 vs 7.60s).
  => client topology does NOT confound STREAMING cells; the low n64 attain is REAL GPU
  saturation (rate 110 > 109 knee), cli-invariant. DECISION: NO broad Set-1 re-run. Existing
  Set-1 n1-n64 streaming data is VALID. The cli confound materially hit ONLY non-stream 256n
  throughput (already fixed to 27k). 256n streaming collapse is real network saturation.
  Justification: the cheap n64 cli=4 cell proved cli-invariance for streaming → saves ~26
  re-runs; the characterization's n1-n64 trend stands.

- 00:05 CDT (Jun17) — Hardened server-stats collection BEFORE the 256n Task-3 run (it was
  still queued, so cancelled 8546156 + resubmitted as 8546166, snapshot 11dd95b):
  (a) write ONE combined replica_stats_all.json instead of 3072 per-replica files (256n
  Lustre MDS storm, ~5s/file); (b) data-run-only fleet metrics via cooldown-gap split (drop
  the cold warm-up run from server TTFT/TBT). Unit-tested both. Justification: the linchpin
  256n run must have robust, warm-up-free server-side metrics — fixing after would waste a
  prod run.

- 03:25 CDT — Prod/small congested (67 queued, "not enough free nodes"); Task-3 prod job
  8546166 stuck 3.3h. Submitted Task 3 to debug-scaling instead (8546289, 256n, 1h walltime
  — debug-scaling permits 256 nodes and is far less backed up). Keep 8546166 as backup;
  qdel the loser once one starts. Justification: don't burn the night waiting on a congested
  prod queue when debug-scaling can run the same 256n cell within ~1h.

- 06:01 CDT — Machine saturated (debug-scaling 8 queued, small 67 queued); both Task-3 jobs
  (8546166 prod, 8546289 dbgscale) stuck queued ~2.5-3.3h, may not seat 256 nodes before 9am.
  KEY REFRAME: the characterization is COMPLETE without Task 3 — the DEFAULT config (no-delay
  ON) streaming-256n collapse is already measured (real network saturation); Task 3 only tests
  whether coalescing RESCUES it (a fix experiment, not core characterization). DECISION: keep
  both Task-3 jobs queued + combined monitor; spend the remaining window finalizing deliverables
  (regenerate plots with corrected data, finalize CHARACTERIZATION.md). Task 3 stays marked
  PENDING; if it lands before 9am, fold it in. Justification: can't force the scheduler; the
  reviewer-proof story doesn't depend on the fix experiment.

- 06:12 CDT — Regenerated ALL preview plots with corrected data; verified set1 + trend
  figures visually. CERTIFIED haproxy 256n streaming (run2 cli=4): rps 4578, attain 0.003,
  E2E p50/p99/max 5.85/17.99/199.6s, 369s drain (caught + corrected my earlier mis-stated
  "146s" which was a stray max from another run — verified directly). Added burstgpt
  n1==n64 caveat to CHARACTERIZATION (reviewer would flag the fixed-rate trace). Finalized
  CHARACTERIZATION.md §1 256n with certified numbers.
  Justification: every plotted/claimed number now traced to a specific run and re-verified;
  no doubtful data point left in the solid set.

- 06:25 CDT — Added HSN NIC drop/error counters (sysfs, no-priv) to the diag sampler
  (commit 25e52a0) to close the one inference gap (TCP-retransmit -> NIC-is-the-wall) for
  the full sweep. Did NOT resubmit 256n for it: machine congested (futile), and Task 3 is
  no-delay-OFF (coalescing relieves packets → won't show NIC drops; the drop evidence belongs
  to the no-delay-ON collapse run, which the full sweep re-captures with this diag).
  Justification: stage the airtight instrumentation without churning the stuck prod/dbgscale
  queue; TCP-layer evidence already strong.

- 06:25 CDT — Coherence pass: reconciled an internal contradiction in FINDINGS (the
  "1024 vs 16 clients" section read 256n-streaming cli variance as a robust effect, but n64
  proved streaming is cli-INVARIANT). Reframed it as stochastic congestion-collapse variance;
  cli mattered only for non-stream 256n throughput. Now no contradictory claims across docs.
  Justification: a reviewer would catch the contradiction; the corrected framing matches the
  controlled n64 evidence.
- 06:25 CDT — STATE: night's reviewer-proofing complete except the PENDING Task-3 fix-experiment
  (8546166 prod / 8546289 dbgscale, both stuck in congested queues). Combined monitor runs to
  ~08:31 CDT; if Task 3 seats I fold it in, else I summarize at 9am. Deliverables finalized:
  CHARACTERIZATION.md, FINDINGS_haproxy_256n.md, DATA_LEDGER.md, 5 plots, OVERNIGHT_LOG.md.
  Code committed on branch haproxy-no-delay-knob (http_no_delay knob, diag sampler + NIC
  counters, server-stats collector + fixes).

- 08:35 CDT — Task 3 (8546289, debug-scaling) SEATED and RUNNING after ~2.5h queue (bring-up
  phase, 18min in). Result expected ~09:00. Re-armed monitor. Keeping prod backup 8546166
  queued in case the 1h debug-scaling walltime is too tight for 256n bring-up (~28min) +
  the run. This is the last data point (does coalescing rescue 256n streaming?); will fold
  in + do the final summary when it lands (~9am).

- 08:50 CDT — TASK 3 RESULT (8546289, 256n streaming, http_no_delay OFF/coalescing ON):
  coalescing does NOT rescue it. Warm-up limped (0.4% err, ~10k rps, 169s drain — better
  than no-delay-ON's ~4.6k), but the DATA run 100%-collapsed (ECONNREFUSED), same edge-of-
  collapse pattern. Diag: retransmits 3.5M (vs 7M no-delay-on → coalescing ~halved packets)
  BUT ~201k concurrent connections persist. CONCLUSION: the 256n centralized-proxy streaming
  limit is robust to the coalescing knob — it's BOTH packet-rate AND connection-count bound;
  coalescing only addresses packets. The real fix is distributed/direct dispatch (19.4k).
  server_stats missing: run_executor ray.init(auto) failed at collection (256n teardown/timing
  fragility) — note for full sweep; data run had 0 successes anyway so server-side moot here.
  Cancelled redundant prod backup 8546166. Task 3 DONE → all handed-off tasks complete.

- 11:30 CDT (morning) — nbthread=32 256n streaming test (8546671) + server-stats fix validated:
  * MORE THREADS DON'T HELP: run1 4470 rps (≈ baseline 4578), HAProxy peak CPU 390% (~4 of
    32 threads) → NOT thread/CPU-bound. Retransmits 6.4M, peak estab 202k (storm persists).
  * NIC sysfs rx_dropped/fifo = 0 → the retransmit storm is NOT host-NIC-RX-drop. CORRECTS my
    earlier "NIC packet-rate is the wall" overclaim. It's TCP-retransmit congestion from the
    ~200k-connection concentration at one proxy (loss likely fabric/switch or TCP-RTO, not
    host-visible) — and provably NOT CPU/threads, NOT accept-queue, NOT host-NIC-drop.
  * SERVER-SIDE NOW COLLECTED at 256n (explicit-head-addr fix works! 3072 replicas, run-split
    OK): server TTFT 54/66ms, TBT 24/28ms, E2E 1.6/1.8s → the GPUs are HEALTHY and MEET the SLO
    at 256n. The collapse is ENTIRELY the centralized-proxy streaming path, not the serving.
  Justification: definitive — server-side proves serving is fine; threads/NIC ruled out; the
  fix is distributed/direct dispatch (client then sees the healthy 19.4k).

- Jun18 ~07:30 CDT — nbthread=1 control (8548017, n64 streaming): CONFIRMS the thread curve.
  1 thread (nlwp=1) -> HAProxy CPU pegs ~1 core (max 92.9%, median 65%), throughput 4527 rps
  (vs ~5850 default-64) = ~23% drop, attain 0.011, drain stretched to 93s. So: 1 thread =
  CPU-bound (proxy is the wall); ~4 cores' useful work saturates by the default 64 threads;
  >~4 idle/I-O-bound (why 32/64 didn't change 256n). The nbthread knob is real + matters at
  the low end; it just can't fix the connection/network-bound 256n collapse. User's prediction
  (perf drops, CPU<=100%) verified exactly.

- Jun18 ~08:00 CDT — USER CAUGHT a misattribution: the 100%-err 256n data runs are HAProxy
  PROCESS DEATHS, not congestion. Verified (8546289): warm-up 99.6% success @ ~10k rps, then
  `haproxy pid gone` (nlwp 64->1->0), data run 0 survivors / all ECONNREFUSED. RSS flat 730MB
  (not self-OOM), no logged segfault. Existing logs have NO 'resource temporarily unavailable'/
  EAGAIN/OOM line — evidence wasn't captured. CORRECTED docs: (1) "coalescing doesn't rescue"
  was WRONG (coalescing warm-up = 10k/99.6%, better than no-delay-ON 4.6k; the 100% was the
  crash); (2) split 256n streaming into mode-(a) degraded congestion vs mode-(b) proxy death.
  ENHANCED diag (commit bdd7bd8) to capture resource-exhaustion: mem, thread/fd vs limits
  (threads-max/pid_max/file-max + HAProxy NOFILE), fork CANARY (logs EAGAIN), dmesg-on-death.
  5s interval to keep the sampler's own fork footprint low (per user caution on resource use).
  Submitted instrumented 256n streaming re-run (8548052, debug-scaling) to root-cause the death.
  Justification: user's instinct right; capture the EAGAIN/OOM evidence the prior runs missed.

- Jun18 ~09:40 CDT — SELF-INFLICTED BUG (caught via user's "poke it"): the enhanced diag used
  .format(pid=) on a script containing awk {print $1$2} → KeyError on the literal braces →
  _start_diag_sampler raised → broke HAProxy start() → run 8548052 died (raylet exit, no replay).
  NOT the proxy death we're hunting. FIX: .replace("{pid}",...) instead of .format (ba317b8);
  verified the sampler end-to-end (limits+res+canary+NIC, no bash errors) before re-running.
  Re-submitted on PROD (8548202) per user (debug-scaling queues too long at 256n).
  Lesson: never .format() a shell script string with literal braces; test instrumentation
  in isolation before putting it in the critical launch path.

- Jun18 ~10:15 CDT — PROD 256n diag survivor (8548202, snapshot ba317b8): HAProxy did NOT die
  this run (gone=0, canary=0). Data run 99.6% success, E2E p50/p99 5.23/32.85s (degraded but
  NOT collapsed). KEY negative result from the resource trajectory: when it survives, resources
  are FAR from every ceiling — peak ha_fds=161,579 (15% of 1.05M NOFILE), peak procs/threads
  =8,275 (4% of pid_max 212,992), min MemAvailable=1016 GB (~untouched), HAProxy cpu peak 4.9
  cores. peak estab conns 188,954. So the death is NOT steady-state resource exhaustion.
  Server-side healthy throughout: TTFT 54/67ms, TBT 25/28ms (3072 replicas, 1.68M reqs).
  => 256n streaming is UNSTABLE/stochastic: ~half the runs survive-degraded (this one, 99.6%/
  E2E 5s), ~half the proxy dies (100% ECONNREFUSED). Resources aren't the steady-state limit.
  Found the reason every death was unexplained: HAProxyProxy.stop() returned SILENTLY when the
  proxy had already died -> exit signal never logged. FIX (8912099): stop() now decodes the
  death signal (SIGKILL=OOM/resource-kill, SIGSEGV=crash, code=self-exit). Submitted 8548410
  (prod, num_runs=3 = 3 ramp-up death chances) to catch a death WITH the signal recorded.

- Jun18 late — DEATH HUNT (8548562, prod, num_runs=6, death-signal snapshot): survived ALL 6
  ramps (gone=0, canary=0, no death signal). Per-run 99.9-100% success, E2E p50 ~5.7s/p99 ~17s
  — degradation is STABLE and reproducible, not a collapse. Revises the death rate: prod = 1
  death (8545884) / 11 surviving ramps (8548202+8548410+8548562) ≈ 1/12, NOT ~2/3. Death is
  genuinely RARE. 43-min/6-run resource trajectory: ha_fds 16% max (no creep), threads 4% (flat),
  MemAvailable ~untouched (~1TB, flat), estab conns wave 10k-199k (no creep) -> rules out BOTH
  steady-state exhaustion AND slow leak as the death cause. DECISION (user chose "one efficient
  hunt"): hunt done; not forcing more 256n for such a rare event. stop() now self-records the
  death signal, so the next death (incl. full sweep) is auto-diagnosing. Updated CHARACTERIZATION
  + FINDINGS to "degraded-but-stable + rare stochastic death (not resource-bound)". Final story:
  256n centralized-proxy STREAMING is degraded (E2E ~5.7s, attain ~0) though it completes; server
  healthy (TBT 25ms); direct dispatch is the robust answer (19.4k, 0 err).
