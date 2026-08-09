"""Fail-closed validator for the canonical hardening findings ledger.

The record contract is defined by ``PRODUCTION_HARDENING_EXECUTION_PLAN.md``
section 3.2.1.  This module deliberately validates the exact contract instead
of accepting a convenient subset: the ledger is a release gate, so an empty
ledger, an invented acceptance ID, or prose presented as durable evidence must
not produce a green build.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Mapping
import re
import sys


RECORD_FIELDS = (
    "id",
    "source",
    "severity",
    "invariant",
    "primary_work_package",
    "affected_regions",
    "acceptance_tests",
    "status",
    "decision",
    "evidence",
    "fallback",
    "residual_risk",
    "support_impact",
    "owner",
    "approval",
    "revisit_condition",
)
RECORD_FIELD_SET = frozenset(RECORD_FIELDS)
LIST_FIELDS = ("affected_regions", "acceptance_tests", "evidence")
TEXT_FIELDS = (
    "id",
    "source",
    "invariant",
    "decision",
    "fallback",
    "residual_risk",
    "support_impact",
    "owner",
    "revisit_condition",
)

VALID_SEVERITY = {"blocker", "high", "medium-high", "medium", "low", "info"}
VALID_STATUS = {
    "OPEN",
    "IN_PROGRESS",
    "FIXED",
    "REPLACED",
    "ACCEPTED_LIMIT",
    "UNSUPPORTED",
    "EXTERNAL_BLOCKER",
    "OUT_OF_PRODUCTION_SCOPE",
}
CLOSED_STATUS = {"FIXED", "REPLACED"}
EXCEPTIONAL_STATUS = {
    "ACCEPTED_LIMIT",
    "UNSUPPORTED",
    "EXTERNAL_BLOCKER",
    "OUT_OF_PRODUCTION_SCOPE",
}

ACCEPTANCE_CATALOG = {
    "AC-TST-01",
    "AC-SUP-01",
    "AC-CTL-01",
    "AC-COMP-01",
    "AC-RDY-01",
    "AC-RDY-02",
    "AC-PLAN-01",
    "AC-TEL-01",
    "AC-STAT-01",
    "AC-OBS-01",
    "AC-DIST-01",
    "AC-PP-01",
    "AC-PROXY-01",
    "AC-INST-01",
    "AC-SCALE-01",
}
APPROVAL_FIELDS = ("approver_id", "approved_at", "evidence_ref", "scope")
APPROVAL_FIELD_SET = frozenset(APPROVAL_FIELDS)
RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")
WORK_PACKAGE = re.compile(r"^WP(?:[0-9]|1[0-3])$")
SHA_OR_URL = re.compile(r"^(?:https?://|[0-9a-f]{7,64}$)")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
MISSING_PROOF_WORDS = (
    "missing",
    "pending",
    "in progress",
    "not yet",
    "owed",
    "requires",
    "blocked",
    "unimplemented",
    "unproven",
)
LOCAL_PREFIXES = (
    ".github/",
    "artifacts/",
    "clientlab/",
    "doc/",
    "eval/",
    "examples/",
    "findings/",
    "scripts/",
    "src/",
    "tests/",
    "tools/",
)
LOCAL_FILES = {"AGENTS.md", "README.md", "pyproject.toml"}
EXTERNAL_ARTIFACT_PREFIX = "artifacts/hardening/"
EXTERNAL_ARCHIVE_META_FIELDS = (
    "candidate",
    "wheel_sha256",
    "scope_state",
    "review_manifest",
    "review_manifest_sha256",
)


def _nonempty_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _local_reference_path(reference: str) -> str | None:
    """Return the repository-relative path portion of a durable reference.

    References may append a pytest node id (``::test``), a line number
    (``:123``), or an anchor (``#section``).  Free-form prose is not treated as
    a path and is rejected separately when it is used as closure evidence.
    """

    value = reference.strip()
    if value in LOCAL_FILES:
        return value
    if not value.startswith(LOCAL_PREFIXES):
        return None
    value = value.split("::", 1)[0].split("#", 1)[0]
    value = re.sub(r":\d+(?::\d+)?$", "", value)
    return value


def _validate_approval(record: Mapping[str, Any], rid: str, problems: list[str]) -> None:
    approval = record.get("approval")
    if record.get("status") != "ACCEPTED_LIMIT":
        if approval is not None:
            problems.append(f"{rid}: approval must be null unless status is ACCEPTED_LIMIT")
        return

    if not isinstance(approval, Mapping):
        problems.append(f"{rid}: ACCEPTED_LIMIT requires the exact product-owner approval block")
        return
    missing = sorted(APPROVAL_FIELD_SET - set(approval))
    unknown = sorted(set(approval) - APPROVAL_FIELD_SET)
    if missing:
        problems.append(f"{rid}: approval missing field(s) {missing}")
    if unknown:
        problems.append(f"{rid}: approval has unknown field(s) {unknown}")
    for field in APPROVAL_FIELDS:
        if field in approval and not _nonempty_text(approval[field]):
            problems.append(f"{rid}: approval.{field} must be a non-empty string")
    stamp = approval.get("approved_at")
    if _nonempty_text(stamp) and not RFC3339.fullmatch(stamp):
        problems.append(f"{rid}: approval.approved_at {stamp!r} is not RFC3339 UTC")
    approver = str(approval.get("approver_id", "")).strip().lower()
    if approver in {"claude", "claude code", "assistant", "codex", "owner", "worker"}:
        problems.append(f"{rid}: approval cannot be self-issued by {approver!r}")


def _validate_reference(
    reference: Any,
    *,
    rid: str,
    field: str,
    repo_root: Path,
    require_durable: bool,
    allow_missing_external_artifacts: bool,
    problems: list[str],
) -> None:
    if not _nonempty_text(reference):
        problems.append(f"{rid}: {field} entries must be non-empty strings")
        return
    value = reference.strip()
    local_path = _local_reference_path(value)
    if local_path is not None:
        if not (repo_root / local_path).exists():
            externally_retained = allow_missing_external_artifacts and local_path.startswith(
                EXTERNAL_ARTIFACT_PREFIX
            )
            if not externally_retained:
                problems.append(f"{rid}: {field} local reference does not exist: {local_path}")
        return
    if require_durable and not SHA_OR_URL.match(value):
        problems.append(
            f"{rid}: {field} entry is prose, not a stable repository/artifact/test/ADR reference: "
            f"{value[:100]!r}"
        )


def validate(
    records: list[Any],
    *,
    repo_root: Path | None = None,
    allow_missing_external_artifacts: bool = False,
) -> list[str]:
    """Validate ledger records against the exact canonical record contract."""

    root = (repo_root or Path(__file__).resolve().parents[2]).resolve()
    problems: list[str] = []
    if not isinstance(records, list):
        return ["findings must be a list"]
    if not records:
        return ["findings must contain at least one record"]

    seen: set[str] = set()
    for index, raw_record in enumerate(records):
        if not isinstance(raw_record, Mapping):
            problems.append(f"record[{index}] must be a mapping")
            continue
        record = raw_record
        rid_value = record.get("id")
        rid = rid_value.strip() if _nonempty_text(rid_value) else f"record[{index}]"

        missing = sorted(RECORD_FIELD_SET - set(record))
        unknown = sorted(set(record) - RECORD_FIELD_SET)
        if missing:
            problems.append(f"{rid}: missing exact-schema field(s) {missing}")
        if unknown:
            problems.append(f"{rid}: unknown exact-schema field(s) {unknown}")

        for field in TEXT_FIELDS:
            if field in record and not _nonempty_text(record[field]):
                problems.append(f"{rid}: {field} must be a non-empty string")
        if _nonempty_text(rid_value):
            if rid in seen:
                problems.append(f"{rid}: duplicate record id")
            seen.add(rid)

        severity = record.get("severity")
        if severity not in VALID_SEVERITY:
            problems.append(f"{rid}: severity {severity!r} is not canonical")
        status = record.get("status")
        if status not in VALID_STATUS:
            problems.append(f"{rid}: status {status!r} is not a legal disposition")
        work_package = record.get("primary_work_package")
        if not _nonempty_text(work_package) or not WORK_PACKAGE.fullmatch(work_package):
            problems.append(f"{rid}: primary_work_package {work_package!r} must be WP0..WP13")

        for field in LIST_FIELDS:
            value = record.get(field)
            if not isinstance(value, list):
                problems.append(f"{rid}: {field} must be a YAML list")
                continue
            if field == "affected_regions" and not value:
                problems.append(f"{rid}: affected_regions must not be empty")
            for entry in value:
                _validate_reference(
                    entry,
                    rid=rid,
                    field=field,
                    repo_root=root,
                    require_durable=(field == "evidence" and status in CLOSED_STATUS),
                    allow_missing_external_artifacts=allow_missing_external_artifacts,
                    problems=problems,
                )

        acceptance = record.get("acceptance_tests")
        if isinstance(acceptance, list):
            for acceptance_id in acceptance:
                if acceptance_id not in ACCEPTANCE_CATALOG:
                    problems.append(
                        f"{rid}: acceptance_tests contains unknown ID {acceptance_id!r}"
                    )

        evidence = record.get("evidence")
        if status in CLOSED_STATUS:
            if not isinstance(acceptance, list) or not acceptance:
                problems.append(f"{rid}: {status} requires linked acceptance_tests")
            if not isinstance(evidence, list) or not evidence:
                problems.append(f"{rid}: {status} requires linked durable evidence")
        elif status in {"OPEN", "IN_PROGRESS"}:
            missing_proof = (
                not isinstance(acceptance, list)
                or not acceptance
                or not isinstance(evidence, list)
                or not evidence
            )
            decision = str(record.get("decision", "")).lower()
            if missing_proof and not any(word in decision for word in MISSING_PROOF_WORDS):
                problems.append(
                    f"{rid}: {status} with empty acceptance/evidence must explicitly name "
                    "the missing proof in decision"
                )

        if status in EXCEPTIONAL_STATUS:
            for field in ("support_impact", "revisit_condition"):
                if not _nonempty_text(record.get(field)):
                    problems.append(f"{rid}: {status} requires non-empty {field}")

        _validate_approval(record, rid, problems)

    return problems


def _external_archive_policy(
    data: Mapping[str, Any], records: Any, root: Path, problems: list[str]
) -> bool:
    """Allow absent raw evidence only through a hash-bound external archive.

    Large Aurora logs are intentionally not source-controlled.  A clean clone
    can therefore validate their stable references but cannot inspect their
    bytes.  This exception is enabled only when the entire hardening archive is
    absent and the ledger carries the exact candidate review pointer and
    cryptographic identities needed to retrieve and adjudicate it separately.
    A present (even partial) archive remains subject to strict existence checks.
    """

    if not isinstance(records, list):
        return False
    artifact_paths = []
    for record in records:
        if not isinstance(record, Mapping):
            continue
        for field in LIST_FIELDS:
            entries = record.get(field)
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if _nonempty_text(entry):
                    local_path = _local_reference_path(entry)
                    if local_path is not None and local_path.startswith("artifacts/"):
                        artifact_paths.append(local_path)

    if not artifact_paths or (root / "artifacts" / "hardening").exists():
        return False
    if any(not path.startswith(EXTERNAL_ARTIFACT_PREFIX) for path in artifact_paths):
        problems.append("external evidence references must remain under artifacts/hardening/")
        return False

    meta = data.get("meta")
    if not isinstance(meta, Mapping):
        problems.append("missing external evidence archive requires a ledger meta mapping")
        return False
    missing = [
        field for field in EXTERNAL_ARCHIVE_META_FIELDS if not _nonempty_text(meta.get(field))
    ]
    if missing:
        problems.append(
            f"missing external evidence archive requires non-empty meta field(s) {missing}"
        )
        return False

    candidate = str(meta["candidate"]).strip()
    expected_review = f"{EXTERNAL_ARTIFACT_PREFIX}{candidate}-candidate-review.json"
    if meta["review_manifest"] != expected_review:
        problems.append(
            "meta.review_manifest must identify the candidate review under artifacts/hardening/"
        )
    for field in ("wheel_sha256", "review_manifest_sha256"):
        if not SHA256.fullmatch(str(meta[field]).strip()):
            problems.append(f"meta.{field} must be a lowercase SHA-256 digest")
    if meta["scope_state"] != "TECHNICAL_PASS_SCOPE_PENDING":
        problems.append(
            "externally retained candidate evidence requires "
            "scope_state TECHNICAL_PASS_SCOPE_PENDING"
        )
    return not problems


def validate_document(data: Any, *, repo_root: Path | None = None) -> tuple[list[Any], list[str]]:
    """Validate the top-level document and return ``(records, problems)``."""

    if not isinstance(data, Mapping):
        return [], ["ledger document must be a mapping"]
    if "findings" not in data:
        return [], ["ledger document is missing top-level 'findings'"]
    records = data["findings"]
    root = (repo_root or Path(__file__).resolve().parents[2]).resolve()
    policy_problems: list[str] = []
    allow_missing_external_artifacts = _external_archive_policy(
        data, records, root, policy_problems
    )
    validation_problems = validate(
        records,
        repo_root=root,
        allow_missing_external_artifacts=allow_missing_external_artifacts,
    )
    return (
        records if isinstance(records, list) else [],
        [*policy_problems, *validation_problems],
    )


def load_yaml_unique(stream) -> Any:
    """Load YAML while rejecting duplicate mapping keys at every depth."""
    import yaml

    class UniqueKeyLoader(yaml.SafeLoader):
        pass

    def construct_mapping(loader, node, deep=False):
        loader.flatten_mapping(node)
        mapping = {}
        for key_node, value_node in node.value:
            key = loader.construct_object(key_node, deep=deep)
            if key in mapping:
                raise yaml.constructor.ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    f"found duplicate key {key!r}",
                    key_node.start_mark,
                )
            mapping[key] = loader.construct_object(value_node, deep=deep)
        return mapping

    UniqueKeyLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        construct_mapping,
    )
    return yaml.load(stream, Loader=UniqueKeyLoader)


def main(path: str = "doc/hardening/FINDINGS.yaml") -> int:
    import yaml

    ledger_path = Path(path)
    try:
        with ledger_path.open(encoding="utf-8") as handle:
            data = load_yaml_unique(handle)
    except (OSError, yaml.YAMLError) as exc:
        print(f"ledger unreadable: {exc}")
        return 1

    repo_root = Path(__file__).resolve().parents[2]
    records, problems = validate_document(data, repo_root=repo_root)
    counts = Counter(record.get("status") for record in records if isinstance(record, Mapping))
    print(f"records: {len(records)}  {dict(sorted(counts.items()))}")
    if problems:
        print(f"\n{len(problems)} validation problem(s):")
        for problem in problems[:100]:
            print(f"  - {problem}")
        if len(problems) > 100:
            print(f"  ... and {len(problems) - 100} more")
        return 1
    if not (repo_root / "artifacts" / "hardening").exists():
        print(
            "external artifact archive absent: validated its hash-bound candidate pointer; "
            "evidence bytes require the retained archive"
        )
    print("ledger valid: every record matches the exact canonical §3.2.1 schema")
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
