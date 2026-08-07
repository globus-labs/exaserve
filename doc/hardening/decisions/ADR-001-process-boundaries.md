# ADR-001: Process boundaries, distributed ownership, and control transport

**Status:** PROVISIONAL TARGET; P00/S01 REOPENED on 2026-08-07. The 2026-08-05
two-node placeholder harness proves useful PALS/TCP feasibility facts, but it
does not pass the strengthened S01 gate. The architecture below is binding
because the canonical plan selects it, not because the cited harness fully
proved it.

## Decision

1. **Target topology (selected; production proof open):** allocation-head
   `RuntimeSupervisor` (outside the rank set) owns the control listener,
   `RankLauncher` (one supervised mpiexec), `DeploymentManager`, gateway,
   readiness, and global state; one `NodeSupervisor` rank per planned node owns
   only its node-local children. The placeholder harness proves this shape can
   communicate over PALS, not that the real Ray/Serve lifecycle satisfies it.
2. **Transport primitive (early feasibility proven; target semantics open):**
   length-prefixed canonical JSON with HMAC-SHA256 under a 256-bit
   per-deployment secret delivered via redacted inherited environment through
   mpiexec worked across two nodes. The version-2 envelope, binding, initial
   snapshot/acknowledgment/START barrier, reconnect, and complete negative
   matrix still require the plan's S01 proof.
3. **Launcher behavior (observed partial evidence):** PALS reports the tested
   placeholder-rank failures only as job abort (exit 143), losing the rank's
   own exit code/cause, while the prototype channel delivered a typed
   `CHILD_EXIT` first. This motivates redundant channel plus launcher evidence;
   it does not prove both signals on the final real-Ray path.
4. **Watchdog target (not proven by the old harness):** readiness is revoked at
   `loss_time`; reconnection remains possible through
   `loss_time + reconnect_grace_s`; only after grace expiry may node-local
   cleanup begin, and it must then finish within
   `watchdog_cleanup_deadline_s`. The historical conn-drop harness killed its
   placeholder child immediately on disconnect, so it proves only eventual
   reaping/nonzero exit and must not be cited for this timing contract.
5. **DeploymentManager placement target:** the outer supervisor owns it and invokes it
   in-process after compatibility activation by default. If an isolated proof
   demonstrates that this cannot satisfy import/fault-isolation requirements,
   retain exactly one outer-supervisor-owned local child behind versioned
   structured IPC. Rank zero never owns deployment/global state. The
   in-process/import-order/fault-isolation proof remains part of reopened S01.
6. **Registration/reconnect target:** listener bind is mandatory before rank
   launch; every rank registers before START permits Ray children; reconnect
   inside the compiled grace requires a complete snapshot. The exact deadline
   fields, snapshot acknowledgment, GOODBYE, and acceptance cases are binding
   in plan §3.2.1 and are not proven by the old harness.

## Historical partial evidence (allocation 8734762, nodes x4117c0s7b0n0 + x4117c4s3b0n0)

Harness: `scripts/hardening/spike_s01.py` via
`scripts/hardening/run_s01_battery.sh`; verdicts in
`artifacts/hardening/s01-2n/verdict_*.json`.

| scenario | registered | typed evidence | launcher exit | residue |
|---|---|---|---|---|
| clean | 2/2 in 0.38s | RUNNING obs from both ranks; legacy unsolicited GOODBYEs (not target acceptance evidence) | 0 | 0 procs |
| placeholder-child-death | 2/2 in 0.28s | FAILED obs, reason `CHILD_EXIT`, first cause preserved | 143 (job abort) | 0 procs |
| sup-death | 2/2 in 0.26s | session drop of acting rank (no GOODBYE) | 143 | 0 procs |
| conn-drop | 2/2 in 0.25s | session drop; rank watchdog killed child, exited 24 | 143 | 0 procs |

Additional facts proven: mpiexec/PALS propagates the inherited environment
(registration MACs verify ⇒ the secret crossed intact); the head can bind an
ephemeral port reachable from the worker node; PALS aborts the remaining
ranks on any rank's nonzero exit (bounded cluster-wide cleanup came from the
launcher in ≤ seconds in all failure scenarios).

Earlier local validation (1-rank, self-reap mode) had found and fixed two
harness defects: orphaned placeholder children can wedge a pipe-holding
parent (rank output now goes to files) and failure injection needed a
configurable acting rank.

## Reopened S01 proof owed before P00 technical pass

- Replace the placeholder child with a real worker Ray child and exercise its
  death, a worker `NodeSupervisor` death, control loss, partial start,
  cancellation, stdout/stderr saturation, and SIGINT/SIGTERM under the final
  supervisor wiring.
- Prove listener-before-launch, authenticated binding, initial
  SNAPSHOT/`SNAPSHOT_ACCEPTED`/START, expected DRAIN/STOP GOODBYE, reconnect
  snapshot inside grace, rejection after grace, immediate readiness revocation,
  and cleanup only after grace expiry within the cleanup deadline.
- Prove the preferred in-process `DeploymentManager` after compatibility
  activation, or record the preferred failure before selecting the one-child
  structured-IPC fallback.
- Verify worker-node residue directly and prove both typed first-cause and
  scheduler-visible nonzero behavior on the real path.

## Caveats / residual evidence limits

- Rank-side residue was asserted on the head node only; worker-node residue
  after sup-death relies on PALS's job-wide cleanup (indicated by the 143
  abort). The WP4 fake-launcher tests plus the final WP12 lane re-verify
  worker-side reaping explicitly (AC-SUP-01).
- Reconnect-with-snapshot was exercised only against the older unit contract;
  it does not cover the new replacement snapshot/acknowledgment and clock
  anchors across nodes.

## Rejected alternatives

- Rank-zero-owned deployment/global state: rejected by plan §3.2; the old
  harness establishes only that an external head can reach two ranks.
- Shared stdout / file polling / Ray actors as control transport: forbidden
  by plan; the measured PALS exit-code loss (143 for every distinct failure)
  independently justifies it — stdout/exit codes cannot carry cause.

## Revisit condition

First close the reopened one-/two-node S01 proof above. Then re-run at whatever
16/64 or larger tiers the approved envelope requires (registration-storm
behavior, heartbeat load); any launcher other than PALS mpiexec (srun) re-runs
the environment-propagation and abort-semantics proofs.
