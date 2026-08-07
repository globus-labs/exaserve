"""Ledger validator (plan §8, packet P00, IMP-B10).

The audit found 0 of 82 records carrying every required field, 0 of 5
ACCEPTED_LIMIT records validly approved, and several FIXED dispositions that
the production path did not support. A ledger that cannot be validated is a
ledger nobody can trust, so this is the gate.

It refuses to invent values. A record missing `evidence` is reported, not
filled with "N/A" to preserve a closure count.
"""

from __future__ import annotations

import re
import sys

REQUIRED = ("id", "source", "severity", "invariant", "primary_work_package",
            "status", "owner")
# Required only for a record claiming closure.
REQUIRED_FOR_CLOSED = ("evidence", "acceptance_tests")
VALID_STATUS = {"OPEN", "IN_PROGRESS", "FIXED", "REPLACED", "ACCEPTED_LIMIT",
                "UNSUPPORTED", "EXTERNAL_BLOCKER", "OUT_OF_PRODUCTION_SCOPE"}
APPROVAL_FIELDS = ("approver_id", "approved_at", "evidence_ref", "scope")
RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")


def validate(records: list) -> list:
    problems: list = []
    seen: set = set()
    for record in records:
        rid = record.get("id", "<no id>")
        if rid in seen:
            problems.append(f"{rid}: duplicate record id")
        seen.add(rid)

        for field in REQUIRED:
            if not record.get(field):
                problems.append(f"{rid}: missing required field {field!r}")

        status = record.get("status")
        if status not in VALID_STATUS:
            problems.append(f"{rid}: status {status!r} is not a legal disposition")

        if status in ("FIXED", "REPLACED"):
            for field in REQUIRED_FOR_CLOSED:
                if not record.get(field):
                    problems.append(
                        f"{rid}: {status} requires {field!r} (linked evidence "
                        "proving the invariant on the production path)")

        if status == "ACCEPTED_LIMIT":
            approval = record.get("approval")
            if not isinstance(approval, dict):
                problems.append(
                    f"{rid}: ACCEPTED_LIMIT without an approval block is not "
                    "closure; reopen it or obtain approval")
            else:
                for field in APPROVAL_FIELDS:
                    if not approval.get(field):
                        problems.append(f"{rid}: approval missing {field!r}")
                stamp = str(approval.get("approved_at", ""))
                if stamp and not RFC3339.match(stamp):
                    problems.append(
                        f"{rid}: approved_at {stamp!r} is not RFC3339 UTC")
                if str(approval.get("approver_id", "")).lower() in (
                        "claude", "claude code", "assistant", "owner"):
                    problems.append(
                        f"{rid}: approver_id {approval.get('approver_id')!r} is "
                        "not a product-owner identity; approval cannot be "
                        "self-issued")
    return problems


def main(path: str = "doc/hardening/FINDINGS.yaml") -> int:
    import yaml

    with open(path, encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    records = data.get("findings", [])
    problems = validate(records)

    from collections import Counter

    counts = Counter(r.get("status") for r in records)
    print(f"records: {len(records)}  {dict(sorted(counts.items()))}")
    if problems:
        print(f"\n{len(problems)} validation problem(s):")
        for problem in problems[:60]:
            print(f"  - {problem}")
        if len(problems) > 60:
            print(f"  ... and {len(problems) - 60} more")
        return 1
    print("ledger valid: every record carries its required §8 fields")
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
