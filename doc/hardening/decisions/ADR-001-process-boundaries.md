# ADR-001: Process boundaries, ownership, and control transport

**Status:** SELECTED AND PROVEN FOR FINAL42 AT ONE AND TWO NODES (2026-08-09).
The four-node candidate run awaits explicit authorization; the product decision
and larger scale ladder remain in ADR-000.

## Decision

1. One allocation-head `RuntimeSupervisor` owns global status, the
   authenticated control listener, one `RankLauncher`, the gateway, readiness,
   and exactly one isolated deployment child.
2. One `NodeSupervisor` rank per planned node owns only the processes it
   creates on that node. Rank zero owns no global lifecycle or readiness state.
3. The head/rank transport is length-prefixed canonical JSON authenticated
   with HMAC-SHA256 and bound to deployment, plan, generation, rank, node,
   session, sequence, and message kind. Registration is not accepted until a
   complete snapshot, supervisor receipt, and `SNAPSHOT_ACCEPTED` exchange
   finish. START, heartbeat, reconnect, DRAIN, STOP, GOODBYE, and grace-expiry
   are typed messages; stdout is diagnostic only.
4. Rank control work runs on one private asyncio event loop. Heartbeats,
   reconnect, command receipt, snapshot replacement, acknowledgements, and
   backoff are coroutines on that loop. There is no second maintenance thread
   making lifecycle decisions and no thread parses output for control state.
5. Ray head/worker daemons, the MPI/PALS launcher, native/MPI staging tools,
   the external gateway, and the deployment fault boundary remain supervised
   subprocesses because they are genuine operating-system boundaries. Every
   boundary uses an argument vector, its own process group where applicable,
   a deadline, typed result/exit evidence, and bounded escalation.
6. Serve deployment uses exactly one allocation-head-owned isolated child
   behind version-2 Unix structured IPC. The receiver validates the kernel
   peer PID against the exact child it created and validates the complete plan,
   site, allocation, deployment, and generation identity. The child owns no
   global status or readiness authority.

## Why the deployment is isolated

The canonical ladder prefers a stable in-process `DeploymentManager`. The
pinned Ray 2.53 public lifecycle does provide callable APIs, but all relevant
entry points (`ray.init`, `serve.start`, `serve.run`, `serve.delete`, and
`serve.shutdown`) are synchronous and expose neither a timeout nor a
cancellation token. A native-extension fatal exit or an indefinitely blocked
call in the same process therefore removes or wedges the global authority; a
Python exception boundary cannot catch `os._exit`, a segfault, or a stuck C
call.

The reproducible S01 probe injected both failure classes. In-process, exit 86
removed the authority and the hang required killing the whole process. With
one child, the authority survived, observed exit 86, and terminated a hung
child within the deadline while remaining able to persist the cause. The
probe selected `isolated-deployment-child` and recorded the actual Ray/Serve
signatures. Evidence:
`artifacts/hardening/architecture-feasibility-20260809-r2/result.json`, SHA-256
`6916762f1f091c991f9d1869aed484c3a98cf35baae0ed1f48ce66ebb0801a30`.

This is the plan's permitted second rung, not a general subprocess control
architecture. The child protocol carries observations and terminal causes;
no readiness or lifecycle decision depends on log text.

## Acceptance evidence

- The final42 clean installed-package gate ran outside the source tree:
  **1207 passed, 9 skipped**, with mypy clean. Evidence:
  `artifacts/hardening/final42-packaged-gate-20260809-a4/`.
- The final42 two-node null gate proves real rank registration/START, exact
  two-rank sessions/receipts, typed worker failure and nonzero global exit,
  duplicate-port fail-before-launch, partial-worker non-readiness, operator
  drain, gateway death, first-cause preservation, and bounded exact cleanup.
  Evidence:
  `artifacts/hardening/final42-null-2n-20260809-a1/qualification/result.json`.
- The final42 two-node real vLLM/XPU PP=2 gate proves the same ownership tree
  with an EngineCore and two workers across two physical hosts, a real canary,
  drain, gateway death, and cleanup. Evidence:
  `artifacts/hardening/final42-real-2n-20260809-a1/qualification/result.json`.
- The strict supervisor gate kills the exact rank-zero Ray child and rank-one
  supervisor, preserves authenticated first cause, and proves zero survivors:
  `artifacts/hardening/final42-supervisor-watchdog-v3q2-2n-20260809-a1/qualification/result.json`.

All three artifacts name wheel
`5346c7ab858b056448702b207b76350ac2ee134a65fa45ea67779039d41362e3`;
no historical candidate is reused as its qualification.

## Rejected alternatives

- In-process deployment lifecycle: rejected for the pinned stack because it
  cannot meet bounded cancellation or fault-isolation requirements.
- More than one deployment child or rank-zero global ownership: unnecessary
  and violates the selected ownership topology.
- Shared files, stdout markers, parsed CLI text, Ray actors, or scheduler exit
  aggregation as the control transport: they cannot preserve authenticated
  first cause and reconnect semantics.
- Plain SSH to workers: loses the scheduler/PALS allocation context and PID
  ownership model.

## Revisit condition

Re-evaluate the child only if a pinned Ray/Serve release supplies a stable
cancelable lifecycle API whose native failures can be isolated without taking
down global control and status. A different launcher must re-run environment
propagation, worker-exit aggregation, and process-tree cleanup proofs. Scale
tiers are qualified only after the ADR-000 product scope decision.
