# ADR-001: Process boundaries, distributed ownership, and control transport

**Status:** PROVISIONAL→largely proven (S01 two-node proof passed 2026-08-05;
in-process DeploymentManager question remains open until the S03 activation
probe, per the P00 co-design loop). 

## Decision

1. **Topology (proven):** allocation-head `RuntimeSupervisor` (outside the
   rank set) owns the control listener, `RankLauncher` (one supervised
   mpiexec), `DeploymentManager`, gateway, readiness, and global state; one
   `NodeSupervisor` rank per planned node owns only its node-local children.
2. **Transport (proven):** the §3.2 authenticated channel as implemented in
   `exaserve/control/transport.py` — length-prefixed canonical JSON with
   HMAC-SHA256 under a 256-bit per-deployment secret delivered via redacted
   inherited environment through mpiexec.
3. **Exit contract (proven):** launcher exit aggregation is redundant with,
   never a substitute for, the typed channel. Evidence: PALS reports rank
   failure only as job abort (exit 143), losing the rank's own exit code and
   cause; the channel delivered the typed first cause (`CHILD_EXIT`) before
   the abort in every run.
4. **Watchdog (proven):** on control-connection loss a NodeSupervisor kills
   its local children within the grace bound and exits nonzero
   (conn-drop scenario, exit 24 path).
5. **DeploymentManager placement (open):** default remains in-process in the
   outer supervisor; decided after the S03 clean-interpreter activation test
   (fallback: exactly one local structured-IPC child).

## Evidence (gate S01-2N, allocation 8734762, nodes x4117c0s7b0n0 + x4117c4s3b0n0)

Harness: `scripts/hardening/spike_s01.py` via
`scripts/hardening/run_s01_battery.sh`; verdicts in
`artifacts/hardening/s01-2n/verdict_*.json`.

| scenario | registered | typed evidence | launcher exit | residue |
|---|---|---|---|---|
| clean | 2/2 in 0.38s | RUNNING obs from both ranks; GOODBYEs | 0 | 0 procs |
| child-death | 2/2 in 0.28s | FAILED obs, reason `CHILD_EXIT`, first cause preserved | 143 (job abort) | 0 procs |
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

## Caveats / residual

- Rank-side residue was asserted on the head node only; worker-node residue
  after sup-death relies on PALS's job-wide cleanup (indicated by the 143
  abort). The WP4 fake-launcher tests plus the final WP12 lane re-verify
  worker-side reaping explicitly (AC-SUP-01).
- Reconnect-with-snapshot was exercised only in unit tests
  (`tests/test_control_channel.py`), not yet across nodes.

## Rejected alternatives

- Rank-zero-owned deployment/global state: rejected by plan §3.2 (head
  supervisor sits outside the rank set; proven workable).
- Shared stdout / file polling / Ray actors as control transport: forbidden
  by plan; the measured PALS exit-code loss (143 for every distinct failure)
  independently justifies it — stdout/exit codes cannot carry cause.

## Revisit condition

S01 re-runs at the 16/64-node tiers (registration-storm behavior, heartbeat
load) before those gates claim support; any launcher other than PALS mpiexec
(srun) re-runs the environment-propagation and abort-semantics proofs.
