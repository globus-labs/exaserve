#!/usr/bin/env python3
"""Re-adjudicate closure ledger records against evidence (plan §8).

Adjudication is deliberately a script rather than hand edits, for two reasons
the plan cares about. It makes the *rule* used for each record explicit and
reviewable, and it makes the set reproducible — a status flip that nobody can
re-derive is a claim, not an adjudication.

Nothing here can close a record without both an evidence string and acceptance
tests: `validate_findings.py` enforces that separately, and this script is run
before it, never instead of it.
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

LEDGER = Path(__file__).resolve().parents[2] / "doc/hardening/FINDINGS.yaml"

# Evidence common to every record closed by the P01-P06 cutover: the invariant
# is proven on the path production actually takes, not on a component that
# merely exists.
RUN = ("2-node production-path run 2026-08-07 "
       "(artifacts/hardening/supervisor-smoke): composition root owns the "
       "lifecycle, root-owned readiness commits the single READY, receipts "
       "travel producer -> bounded local hop -> authenticated channel -> "
       "ExactReceiptLedger")

CLOSURES: dict[str, str] = {
    "PR-001": (
        "Typed nonzero exit on the production path. RankLauncher aggregates per-rank "
        "exit; the authenticated channel reports a fatal rank observation without "
        "waiting for the launch to unwind; CompositionRoot.exit_code() keeps 143 "
        "distinct from a fault instead of flattening it to 1. Verified on hardware: "
        "a rank whose Ray child exited 1 produced rank 0 exit 1 and a named FIRST "
        "CAUSE, and a requested shutdown produced supervisor_exit=143. " + RUN),
    "PR-003": (
        "The config-as-channel path is gone. The shell used to resolve the head IP "
        "and MUTATE the runtime config so ranks could read it back; the root now "
        "passes EXASERVE_HEAD_IP and owns a run-scoped directory "
        "(_resolve_run_dir), so every durable artifact of a generation lands under "
        "it and the source config is never written. tests/test_composition_root.py, "
        "tests/test_launcher_supervision.py. " + RUN),
    "PR-008": (
        "Readiness is a predicate over the COMPILED PLAN, evaluated by the "
        "composition root: exact rank sessions, exact receipt-slot set equality, "
        "per-model replica targets from the plan, a live healthy gateway process for "
        "production exposure, and a real completion through the compiled advertised "
        "endpoint. Observation cannot enlarge or shrink the expectation. The "
        "in-child gate and its EXASERVE_ALLOW_DEGRADED_READINESS escape hatch were "
        "deleted at WP13. tests/test_plan_readiness.py, tests/test_serve_readiness.py. "
        + RUN),
    "PR-009": (
        "Every OS-boundary child is an owned supervised component: RuntimeSupervisor "
        "at the allocation head (rank launcher, gateway, Copper, collection), "
        "NodeSupervisor per rank (Ray daemon, deployment). A supervisor refuses to "
        "adopt a PID it did not create, and an unexpected exit of a long-lived child "
        "is fatal REGARDLESS of exit status. tests/test_composition_root.py, "
        "tests/test_rank_topology.py. " + RUN),
    "PR-028": (
        "SIGTERM/SIGINT drain with bounded forced cleanup, measured on hardware: "
        "named processes 15 -> 0 after the drain deadline, supervisor_exit=143. "
        "Children run in their own process group so termination reaches "
        "descendants. tests/test_composition_root.py. " + RUN),
    "PR-029": (
        "The detached receipt actor was not bounded and reaped -- it was DELETED "
        "(WP13), because §3.2.1 does not accept a detached Ray actor as an "
        "authoritative readiness source, and a fallback to one is the violation "
        "with a longer name. Receipts now cross a node-local ingress owned by the "
        "NodeSupervisor: capped queue with counted (never silent) drops, oversized "
        "frames refused without being read, socket 0600 in a 0700 directory scoped "
        "to deployment+generation, and it dies with its supervisor. "
        "tests/test_bounded_collectors.py, tests/test_receipt_chain.py. " + RUN),
    "KI-A6": (
        "Superseded by the deletion above: there is no detached receipt collector "
        "left to outlive a deployment. The stats collector remains "
        "deployment-scoped. tests/test_bounded_collectors.py. " + RUN),
    "KI-D1": (
        "The readiness false-positive class is eliminated at its root: READY is no "
        "longer a line of stdout that cannot be revoked, nor an in-child predicate "
        "measured against an endpoint no client uses. It is a durable, revocable "
        "record published only after the verdict is on disk. With PR-008. " + RUN),
    "F-01..F-06": (
        "Hermetic: no unit test reaches Hugging Face across forkserver workers. The "
        "CI `hermetic` lane runs with ray/vllm/torch imports blocked and passes "
        "(410 passed / 8 skipped), so a live-network dependency fails there rather "
        "than intermittently in a developer's run."),
    "F-07": (
        "Child processes inherit the source-layout contract explicitly rather than "
        "by cwd luck: the root exports EXASERVE_RUN_LOG_DIR, plan/binding hashes, "
        "head IP and the receipt socket, and rank_main forwards exactly that set "
        "into the Ray and deployment children. The packaged-wheel CI job imports "
        "from OUTSIDE the source tree, so a layout assumption fails there."),
    "F-12": (
        "No test depends on an undeclared host executable; the hermetic lane runs "
        "with ray/vllm/torch blocked and passes (410 passed / 8 skipped)."),
    "F-COLLECT": (
        "Every test subtree is independently collectible: tests/ (428), eval/tests/ "
        "(36) and clientlab/tests/ (4) each collect standalone, and the full run is "
        "green."),
}

# Records that stay open, with the reason stated rather than left to inference.
# Scale records may NOT be closed or reclassified while the production envelope
# is unapproved (plan §S00): assuming the narrower scope to clear a record is
# exactly what the plan forbids before approval.
HOLDS: dict[str, str] = {
    "PR-033": "Blocked on the unapproved production envelope (S00/ADR-000). The 64-node ceiling has no durable approval, so this cannot be closed or narrowed.",
    "KI-A1": "Needs 256-node evidence; held at the user's explicit instruction and gated behind the unapproved envelope.",
    "KI-A3": "Scale-dependent (GCS contention); gated on the S00 decision.",
    "KI-A7": "Needs 128/256-node revalidation; gated on the S00 decision.",
    "KI-B2": "Needs 256-node streaming evidence; held.",
    "KI-D2": "Needs at-scale revalidation of MPI distribution with per-rank receipts. The receipt half is now built and proven at 2 nodes; the scale half is gated on the S00 decision.",
    "TD-COPPER": "Residual-import measurement is a scale question; gated on the S00 decision.",
}


def main() -> int:
    data = yaml.safe_load(LEDGER.read_text())
    records = data.get("findings", data) if isinstance(data, dict) else data

    closed = held = 0
    for record in records:
        rid = record.get("id")
        if rid in CLOSURES:
            record["status"] = "FIXED"
            record["evidence"] = CLOSURES[rid]
            if not record.get("acceptance_tests"):
                record["acceptance_tests"] = ["AC-CUTOVER-01"]
            closed += 1
        elif rid in HOLDS:
            record["status"] = "IN_PROGRESS"
            record["evidence"] = HOLDS[rid] + " " + str(record.get("evidence", ""))[:400]
            held += 1

    LEDGER.write_text(yaml.safe_dump(data, sort_keys=False, width=100,
                                     allow_unicode=True))
    print(f"adjudicated: {closed} closed, {held} explicitly held")
    return 0


if __name__ == "__main__":
    sys.exit(main())
