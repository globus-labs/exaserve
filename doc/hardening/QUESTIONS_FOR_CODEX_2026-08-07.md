# Questions for Codex before executing the §9 cutover

Context: the completion-claim audit (2026-08-06) is accepted. Its §9 gives an
ordered 8-step plan and I intend to follow it in that order. These five points
are the ones where the plan and audit leave a genuine fork, and where guessing
wrong costs a rewrite rather than an edit. Everything not listed here I intend
to implement as the audit specifies without further input.

---

## Q1 — IMP-H03: where exactly is the line between "preflight" and "lifecycle"?

The execution plan calls `launch_cluster.sh` a *"temporary allocation-head
environment/preflight adapter that ends with one `exec` of the outer Python
supervisor"*. The audit says the script still owns "scheduler setup, runtime
config, staging, distribution, Copper, MPI helpers, cleanup, logs, and final
launch".

Those two statements are compatible with two very different end states:

- **(A) Thin adapter** — the shell keeps module loading, PYTHONPATH/env
  sanitation, scheduler env detection, and Copper, then `exec`s Python. Model
  staging, MPI source distribution, runtime-config writing, log routing and
  cleanup all move into Python. (~1500 lines of behaviour to port.)
- **(B) Preflight adapter** — the shell additionally keeps staging and the MPI
  source distribution as *preflight* (they must happen before any Python that
  imports from the staged tree), and `exec`s Python immediately after. (~200
  lines to move.)

**Which is the required end state?** (B) has a bootstrapping argument in its
favour: the Python supervisor imports from the tree that distribution creates.
If (A) is required, what bootstraps the supervisor — a second staged copy, or
running the supervisor from the shared filesystem and only distributing for the
ranks?

## Q2 — IMP-B04 #17: per-instance receipt cardinality at 256 nodes

The audit requires "one per planned node/rank/daemon/replica/engine instance"
rather than one per role. At the qualification scale that is roughly
256 ranks + 256 ray daemons + ~3,072 replicas + ~3,072 engines ≈ **6,600
receipts**, each carrying per-patch results.

1. Is per-instance receipting intended at that cardinality, or is per-rank
   aggregation acceptable provided the rank enumerates its own instances and
   the head verifies the count against the plan?
2. The audit also flags that receipts bypass the authenticated channel (they
   travel via a detached Ray actor and node-local files). Must all ~6,600 flow
   over the §3.2 channel? If so, is a per-rank batched `SNAPSHOT` the intended
   carrier rather than one frame per receipt?

## Q3 — IMP-B02 #9: ordering of gateway startup vs readiness

Today `exaserve.driver` starts the selected external proxy **after** the server
reports ready, so a gateway canary cannot exist as currently sequenced. Two
ways to fix it:

- **(A)** the rank-0 node supervisor owns the gateway and starts it *before*
  the readiness gate runs, so the gate canaries the external gateway directly;
- **(B)** readiness becomes two-phase — `SERVE_READY` (internal) → start
  gateway → `READY` (external canary through the gateway) — with only the
  second phase publishing READY.

**Which is intended?** (A) is simpler; (B) matches the plan's state machine
language more closely. Also: for `dest=direct` deployments with no external
gateway, is the internal Serve endpoint the legitimate canary target, or must
direct mode be treated as a distinct, explicitly-declared exposure mode?

## Q4 — IMP-B06 #4: fail-closed control channel on a large allocation

The plan requires the canonical control path to fail closed; the audit confirms
it is currently fail-open on both listener bind and rank registration.

Fail-closed unconditionally means a single bind failure or one rank that cannot
register aborts a 256-node allocation. Is that the intent, or should it be:

- fail-closed **after a bounded registration window** (all planned ranks must
  register within N seconds, else fatal), with transient send failures
  triggering reconnect-with-snapshot rather than immediate abort?

If the bounded-window reading is right, what is the intended window, and is a
rank that reconnects *after* the window a fatal condition or a recoverable one?

## Q5 — IMP-H01: which artifact becomes the one compiled plan?

§9.1 requires "one compiled plan and generation identity authoritative in core,
eval, and ClientLab". Three candidates exist today:

- `src/exaserve/plan/schemas.py` — the compiler I wrote; the audit says it is
  unreachable from production and "discards runtime-significant accepted
  fields";
- `eval/lib/models.py::RunPlan` — reachable and in use, but eval-shaped;
- a new shared contract that both adopt.

**Should I extend the existing `plan/` compiler to cover the runtime-significant
fields and make eval/ClientLab consume it, or converge on the eval RunPlan, or
build the shared contract fresh?** Related: is `SiteProfile` expected to be part
of that same artifact, or a separate one referenced by hash?

---

## Two small ones

- **ACCEPTED_LIMIT approval (§5)**: 0 of 5 are validly approved. What
  constitutes approval — a named approver and date in the record's fields? Is
  there a required field name? This blocks the IMP-B10 ledger correction.
- **Plan §8 fields**: the audit reports 0 of 82 records carry every required
  field. Is the authoritative field list the one in plan §8 verbatim, and
  should records missing a field be filled in-place or reopened?

---

## Not asking (proceeding as specified)

For the record, I do **not** need input on: making unexpected zero exits fatal,
removing the marker consumers, the `GOODBYE`-as-lost-lease fix, command
dispatch and reconnect snapshots, content-addressed model completeness,
lease/port race repair, per-response-mode request-ID propagation, calling the
metrics hooks from production request paths, packaged-wheel CI with locked
dependencies, or the final 4/16/64 qualification matrix. Those are specified
clearly enough to implement directly.
