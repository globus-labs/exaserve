"""Allocation-wide pipeline-parallel model distribution.

Every stage uses the complete allocation communicator. Global rank zero is
therefore the only process that can open the head-local view whose symlinks
resolve to shared model storage. A recipient-rank mask makes each worker write
only the pipeline stage it owns; non-recipients participate in transport and
the bounded result gather without touching the candidate or shared source.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import subprocess
import tempfile
import uuid
from collections.abc import Mapping
from pathlib import Path

from . import shard_prune


def _validate_pp_receipt(value: object) -> dict:
    from .model_bcast import _MODEL_RECEIPT_FIELDS, _validate_model_receipt

    if not isinstance(value, dict) or set(value) != _MODEL_RECEIPT_FIELDS | {"pp_stage"}:
        raise RuntimeError("PP publication receipt fields are invalid")
    if type(value["pp_stage"]) is not int or value["pp_stage"] < 0:
        raise RuntimeError("PP publication receipt stage is invalid")
    _validate_model_receipt({key: item for key, item in value.items() if key != "pp_stage"})
    return value


def assign_pp_nodes(ordered_nodes, pp_size: int, num_replicas: int):
    need = num_replicas * pp_size
    if len(ordered_nodes) != need:
        raise ValueError(
            f"PP staging requires exactly {need} nodes for {num_replicas} "
            f"replicas x PP{pp_size}, got {len(ordered_nodes)}"
        )
    return [
        (replica, stage, ordered_nodes[replica * pp_size + stage])
        for replica in range(num_replicas)
        for stage in range(pp_size)
    ]


def stage_node_groups(ordered_nodes, pp_size: int, num_replicas: int):
    groups = {stage: [] for stage in range(pp_size)}
    for _replica, stage, node in assign_pp_nodes(ordered_nodes, pp_size, num_replicas):
        groups[stage].append(node)
    return groups


def bcast_cmd(
    bcast_bin,
    src,
    dest,
    *,
    num_nodes: int,
    recipient_ranks: list[int],
    root_host: str,
    application_cwd: str,
    application_environment: Mapping[str, str] | None = None,
    scheduler: str = "pbs",
    transfer_executable: bool = False,
) -> list[str]:
    from .model_bcast import mpi_launch_prefix

    if (
        not recipient_ranks
        or recipient_ranks != sorted(set(recipient_ranks))
        or recipient_ranks[0] < 0
        or recipient_ranks[-1] >= num_nodes
    ):
        raise ValueError("PP recipient ranks must be a nonempty sorted allocation subset")
    return [
        *mpi_launch_prefix(
            num_nodes,
            scheduler=scheduler,
            application_cwd=application_cwd,
            application_environment=application_environment,
            transfer_executable=transfer_executable,
        ),
        str(bcast_bin),
        "--expected-root-host",
        root_host,
        "--expected-world-size",
        str(num_nodes),
        "--recipients",
        ",".join(str(rank) for rank in recipient_ranks),
        str(src),
        str(dest),
    ]


def _run(argv: list[str], *, timeout_s: float) -> subprocess.CompletedProcess[str]:
    from .control.finite_process import FiniteProcessError, run_finite

    try:
        result = run_finite(argv, timeout_s=timeout_s)
    except (OSError, FiniteProcessError) as exc:
        raise RuntimeError(f"PP staging command failed: {exc}") from exc
    if result.stdout:
        print(result.stdout, end="" if result.stdout.endswith("\n") else "\n", flush=True)
    if result.stderr:
        print(
            result.stderr,
            end="" if result.stderr.endswith("\n") else "\n",
            file=os.sys.stderr,
            flush=True,
        )
    if result.returncode:
        raise RuntimeError(f"PP staging command exited {result.returncode}: {argv[0]}")
    return result


def verify_and_publish(
    candidate: Path,
    target: Path,
    *,
    local_root: Path,
    model_id: str,
    expected_manifest_hash: str,
    cache_key_hash: str,
    generation: int,
    stage: int,
) -> dict:
    from .model_bcast import verify_and_publish_model

    receipt = verify_and_publish_model(
        candidate,
        target,
        local_root=local_root,
        model_id=model_id,
        expected_manifest_hash=expected_manifest_hash,
        generation=generation,
        cache_key_hash=cache_key_hash,
    )
    receipt["pp_stage"] = stage
    return receipt


def _skip_payload(*, stage: int) -> dict:
    from .model_bcast import _runtime_rank

    return {
        "rank": _runtime_rank(),
        "node": socket.gethostname(),
        "participating": False,
        "pp_stage": stage,
    }


def _selected_collective_receipts(
    payloads: list[dict],
    *,
    stage: int,
    recipient_ranks: list[int],
    rank_to_node: tuple[tuple[int, str], ...],
) -> list[dict]:
    from .plan.contracts import same_node

    expected_nodes = dict(rank_to_node)
    selected: list[dict] = []
    for rank, payload in enumerate(payloads):
        if rank in recipient_ranks:
            receipt = _validate_pp_receipt(payload)
            if receipt["rank"] != rank or not same_node(receipt["node"], expected_nodes[rank]):
                raise RuntimeError(f"PP stage {stage} rank {rank} has wrong allocation identity")
            selected.append(receipt)
            continue
        if (
            not isinstance(payload, dict)
            or set(payload)
            != {
                "schema_version",
                "attempt_id",
                "result_id",
                "rank",
                "node",
                "participating",
                "pp_stage",
            }
            or payload["schema_version"] != 1
            or not isinstance(payload["attempt_id"], str)
            or not isinstance(payload["result_id"], str)
            or payload["rank"] != rank
            or payload["participating"] is not False
            or payload["pp_stage"] != stage
            or not isinstance(payload["node"], str)
            or not same_node(payload["node"], expected_nodes[rank])
        ):
            raise RuntimeError(f"PP stage {stage} non-recipient rank {rank} result is invalid")
    if [item["rank"] for item in selected] != recipient_ranks:
        raise RuntimeError(f"PP stage {stage} receipt set is incomplete")
    return selected


def stage_pp_sharded(
    model_dir,
    safe_name,
    shared_stage_base,
    local_path,
    pp_size,
    ordered_nodes,
    num_replicas,
    bcast_bin,
    *,
    dry_run=False,
    partition_env=None,
    generation=0,
    scheduler="pbs",
    operation_timeout_s=1800.0,
    model_id=None,
    binding=None,
    application_environment=None,
    source_manifest=None,
):
    """Stage PP subsets with one global-rank-zero shared reader."""

    del shared_stage_base  # shared PP staging directories are prohibited
    if binding is None:
        raise RuntimeError("PP staging requires the exact AllocationBinding")
    if not dry_run and source_manifest is None:
        raise RuntimeError("PP staging requires the full-hash source model manifest")
    rank_to_node = tuple(binding.rank_to_node)
    num_nodes = len(rank_to_node)
    if [rank for rank, _node in rank_to_node] != list(range(num_nodes)):
        raise RuntimeError("PP staging allocation binding rank set is invalid")
    if len({node for _rank, node in rank_to_node}) != num_nodes:
        raise RuntimeError("PP staging requires one uniquely bound rank per node")
    node_to_rank = {node: rank for rank, node in rank_to_node}
    try:
        ordered_ranks = [node_to_rank[node] for node in ordered_nodes]
    except KeyError as exc:
        raise RuntimeError(f"PP planned node is absent from AllocationBinding: {exc}") from exc
    if len(ordered_ranks) != len(set(ordered_ranks)):
        raise RuntimeError("PP staging planned ranks must be unique")
    rank_groups = {
        stage: sorted(ranks)
        for stage, ranks in stage_node_groups(ordered_ranks, pp_size, num_replicas).items()
    }
    model_id = str(model_id or model_dir)
    attempt = uuid.uuid4().hex
    from .model_staging import (
        content_addressed_model_path,
        ensure_node_local_directory,
    )

    try:
        local_root = ensure_node_local_directory(Path(local_path))
    except (RuntimeError, ValueError) as exc:
        raise RuntimeError(f"PP local stage root is not proven node-local: {exc}") from exc
    root_host = dict(rank_to_node)[0]
    runtime_root = os.environ.get("EXASERVE_LOCAL_RUNTIME_ROOT", "").strip()
    if not dry_run and not runtime_root:
        raise RuntimeError("PP staging requires EXASERVE_LOCAL_RUNTIME_ROOT")
    application_cwd = str(Path(runtime_root) / "python") if runtime_root else str(local_root)
    plan: list[dict] = []
    with tempfile.TemporaryDirectory(prefix=f"pp-input-{safe_name}.", dir=local_root) as tmp:
        input_root = Path(tmp)
        for stage in range(pp_size):
            stage_dir = input_root / f"stage{stage}" / safe_name
            if dry_run:
                shards, keep, total = shard_prune.plan_stage_shards(
                    model_dir, pp_size, stage, partition_env
                )
                summary = {
                    "stage": stage,
                    "n_shards": len(shards),
                    "n_weights": len(keep),
                    "bytes": total,
                }
                manifest_hash = ""
            else:
                summary = shard_prune.build_stage_dir(
                    model_dir, pp_size, stage, stage_dir, partition_env
                )
                manifest = shard_prune.write_stage_manifest(
                    Path(model_dir),
                    stage_dir,
                    source_manifest,
                    source_identity=f"{Path(model_dir).resolve()}#pp{pp_size}/stage{stage}",
                )
                manifest_hash = manifest["manifest_hash"]
            plan.append(
                {
                    "stage": stage,
                    "recipient_ranks": rank_groups[stage],
                    "hosts": [dict(rank_to_node)[rank] for rank in rank_groups[stage]],
                    "summary": summary,
                    "manifest_hash": manifest_hash,
                    "receipts": [],
                    "source": stage_dir,
                }
            )

        if dry_run:
            for item in plan:
                item["command"] = bcast_cmd(
                    bcast_bin,
                    item["source"],
                    local_root / ".dry-run-candidate",
                    num_nodes=num_nodes,
                    recipient_ranks=item["recipient_ranks"],
                    root_host=root_host,
                    application_cwd=application_cwd,
                    application_environment=application_environment,
                    scheduler=scheduler,
                )
                item.pop("source")
            return plan

        aggregate_hash = hashlib.sha256(
            json.dumps([item["manifest_hash"] for item in plan], separators=(",", ":")).encode()
        ).hexdigest()
        target = content_addressed_model_path(model_id, local_root, aggregate_hash)
        from .model_bcast import _run_python_collective

        for item in plan:
            stage = item["stage"]
            candidate_root = local_root / (
                f".exaserve_pp_candidate.{safe_name}.{generation}.{attempt}.stage{stage}"
            )
            candidate = candidate_root / safe_name
            command = bcast_cmd(
                bcast_bin,
                item.pop("source"),
                candidate_root,
                num_nodes=num_nodes,
                recipient_ranks=item["recipient_ranks"],
                root_host=root_host,
                application_cwd=application_cwd,
                application_environment=application_environment,
                scheduler=scheduler,
            )
            item["command"] = command
            print(
                f"[pp_stage] candidate stage {stage} "
                f"({item['summary']['n_shards']} shards, "
                f"{item['summary']['bytes'] / 1024**3:.1f} GiB) -> ranks "
                f"{item['recipient_ranks']}",
                flush=True,
            )
            try:
                _run(command, timeout_s=operation_timeout_s)
            except BaseException as exc:
                from .model_bcast import cleanup_native_candidates

                for cleanup_path, ranks in ((candidate_root, item["recipient_ranks"]),):
                    try:
                        cleanup_native_candidates(
                            Path(bcast_bin),
                            cleanup_path,
                            local_root=local_root,
                            num_nodes=num_nodes,
                            binding=binding,
                            scheduler=scheduler,
                            application_cwd=Path(application_cwd),
                            application_environment=application_environment or {},
                            recipient_ranks=ranks,
                            timeout_s=min(300.0, operation_timeout_s),
                        )
                    except BaseException as cleanup_exc:
                        from .exception_notes import add_exception_note

                        add_exception_note(
                            exc,
                            f"PP native cleanup for {cleanup_path} also failed: {cleanup_exc}",
                        )
                raise
            stage_attempt = f"{attempt}-stage{stage}"
            recipient_text = ",".join(str(rank) for rank in item["recipient_ranks"])
            try:
                payloads = _run_python_collective(
                    [
                        "--verify-and-publish",
                        str(candidate),
                        "--publish-target",
                        str(target),
                        "--local-root",
                        str(local_root),
                        "--model-id",
                        model_id,
                        "--expected-manifest-hash",
                        item["manifest_hash"],
                        "--cache-key-hash",
                        aggregate_hash,
                        "--generation",
                        str(generation),
                        "--stage",
                        str(stage),
                        "--recipient-ranks",
                        recipient_text,
                    ],
                    module="exaserve.pp_stage",
                    attempt_id=stage_attempt,
                    num_nodes=num_nodes,
                    binding=binding,
                    scheduler=scheduler,
                    timeout_s=operation_timeout_s,
                )
            except BaseException as exc:
                from .model_bcast import cleanup_native_candidates

                for cleanup_path, ranks in ((candidate_root, item["recipient_ranks"]),):
                    try:
                        cleanup_native_candidates(
                            Path(bcast_bin),
                            cleanup_path,
                            local_root=local_root,
                            num_nodes=num_nodes,
                            binding=binding,
                            scheduler=scheduler,
                            application_cwd=Path(application_cwd),
                            application_environment=application_environment or {},
                            recipient_ranks=ranks,
                            timeout_s=min(300.0, operation_timeout_s),
                        )
                    except BaseException as cleanup_exc:
                        from .exception_notes import add_exception_note

                        add_exception_note(
                            exc,
                            f"PP verifier cleanup for {cleanup_path} also failed: {cleanup_exc}",
                        )
                raise
            receipts = _selected_collective_receipts(
                payloads,
                stage=stage,
                recipient_ranks=item["recipient_ranks"],
                rank_to_node=rank_to_node,
            )
            if any(
                receipt["generation"] != generation
                or receipt["pp_stage"] != stage
                or receipt["manifest_hash"] != item["manifest_hash"]
                or receipt["target"] != str(target)
                for receipt in receipts
            ):
                raise RuntimeError(f"PP stage {stage} receipt has wrong content identity")
            item["receipts"] = receipts
        return plan


def _parse_recipient_ranks(value: str, *, world_size: int) -> list[int]:
    try:
        ranks = [int(item) for item in value.split(",")]
    except ValueError as exc:
        raise ValueError("recipient ranks must be comma-separated integers") from exc
    if not ranks or ranks != sorted(set(ranks)) or ranks[0] < 0 or ranks[-1] >= world_size:
        raise ValueError("recipient ranks must be a sorted allocation subset")
    return ranks


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("model_dir", nargs="?")
    parser.add_argument("--pp", type=int)
    parser.add_argument("--replicas", type=int)
    parser.add_argument("--nodes")
    parser.add_argument("--local-path", default="/tmp/exaserve_stage")
    parser.add_argument("--stage-base", default="/tmp/exaserve_pp_stage")
    parser.add_argument("--safe-name", default="MODEL")
    parser.add_argument("--bcast-bin", default="bcast")
    parser.add_argument("--verify-and-publish")
    parser.add_argument("--publish-target")
    parser.add_argument("--local-root")
    parser.add_argument("--model-id")
    parser.add_argument("--expected-manifest-hash")
    parser.add_argument("--cache-key-hash")
    parser.add_argument("--generation", type=int)
    parser.add_argument("--stage", type=int)
    parser.add_argument("--recipient-ranks")
    parser.add_argument("--attempt-id")
    parser.add_argument("--expected-world-size", type=int)
    parser.add_argument("--expected-root-node")
    args = parser.parse_args(argv)
    if args.verify_and_publish:
        required = (
            args.publish_target,
            args.local_root,
            args.model_id,
            args.expected_manifest_hash,
            args.cache_key_hash,
            args.generation,
            args.stage,
            args.recipient_ranks,
            args.attempt_id,
            args.expected_world_size,
            args.expected_root_node,
        )
        if any(value is None for value in required):
            parser.error("verification mode requires target/model/hash/stage/collective identity")
        recipients = _parse_recipient_ranks(
            args.recipient_ranks, world_size=args.expected_world_size
        )
        from .model_bcast import _runtime_rank
        from .staging_results import run_collective_operation

        rank = _runtime_rank()
        success = run_collective_operation(
            lambda: (
                verify_and_publish(
                    Path(args.verify_and_publish),
                    Path(args.publish_target),
                    local_root=Path(args.local_root),
                    model_id=args.model_id,
                    expected_manifest_hash=args.expected_manifest_hash,
                    cache_key_hash=args.cache_key_hash,
                    generation=args.generation,
                    stage=args.stage,
                )
                if rank in recipients
                else _skip_payload(stage=args.stage)
            ),
            attempt_id=args.attempt_id,
            expected_world_size=args.expected_world_size,
            expected_root_node=args.expected_root_node,
        )
        if not success and rank in recipients:
            from .model_bcast import _rollback_model_candidate

            try:
                _rollback_model_candidate(
                    Path(args.verify_and_publish),
                    local_root=Path(args.local_root),
                )
            except BaseException as cleanup_exc:
                print(
                    f"[pp_stage] rollback cleanup failed: {cleanup_exc}",
                    file=os.sys.stderr,
                    flush=True,
                )
        return 0 if success else 1
    if not args.model_dir or not args.pp or not args.replicas or not args.nodes:
        parser.error("planning mode requires model_dir, --pp, --replicas, --nodes")
    nodes = args.nodes.split(",")
    # Planning mode has no AllocationBinding artifact. Build the exact synthetic
    # rank map solely to render the allocation-wide command.
    from types import SimpleNamespace

    binding = SimpleNamespace(rank_to_node=tuple(enumerate(nodes)))
    plan = stage_pp_sharded(
        args.model_dir,
        args.safe_name,
        None,
        args.local_path,
        args.pp,
        nodes,
        args.replicas,
        args.bcast_bin,
        dry_run=True,
        binding=binding,
    )
    for item in plan:
        print(f"stage {item['stage']}: ranks={item['recipient_ranks']} -> {item['command']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
