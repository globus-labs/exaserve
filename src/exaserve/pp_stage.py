"""Transactional shard-aware pipeline-parallel model staging.

Each stage is first built on shared storage, broadcast into a unique
node-local candidate on the exact planned node subset, verified by every
participant, and only then atomically published to the stable model path.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

from . import shard_prune
from .exception_notes import add_exception_note


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


def subset_launch_prefix(hosts: list[str], scheduler: str) -> list[str]:
    if not hosts or len(hosts) != len(set(hosts)):
        raise ValueError("PP stage host subset must be non-empty and unique")
    if scheduler.lower() == "slurm":
        return [
            "srun",
            f"--nodes={len(hosts)}",
            f"--ntasks={len(hosts)}",
            "--ntasks-per-node=1",
            f"--nodelist={','.join(hosts)}",
            "--cpu-bind=none",
        ]
    return [
        "mpiexec",
        "-n",
        str(len(hosts)),
        "-ppn",
        "1",
        "--cpu-bind",
        "none",
        "--hosts",
        ",".join(hosts),
    ]


def bcast_cmd(bcast_bin, src, dest, hosts, *, scheduler="pbs"):
    return [*subset_launch_prefix(list(hosts), scheduler), str(bcast_bin), str(src), str(dest)]


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
            file=sys.stderr,
            flush=True,
        )
    if result.returncode:
        raise RuntimeError(f"PP staging command exited {result.returncode}: {argv[0]}")
    return result


def verify_and_publish(
    candidate: Path,
    target: Path,
    *,
    model_id: str,
    expected_manifest_hash: str,
    generation: int,
    stage: int,
) -> dict:
    from .model_bcast import verify_and_publish_model

    receipt = verify_and_publish_model(
        candidate,
        target,
        model_id=model_id,
        expected_manifest_hash=expected_manifest_hash,
        generation=generation,
    )
    receipt["pp_stage"] = stage
    return receipt


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
):
    """Stage all PP subsets and return content-verified per-node receipts."""
    groups = stage_node_groups(list(ordered_nodes), pp_size, num_replicas)
    model_id = str(model_id or model_dir)
    attempt = uuid.uuid4().hex
    shared_attempt = Path(shared_stage_base) / f".attempt.{generation}.{attempt}"
    target = Path(local_path) / safe_name
    plan = []
    configured_result_root = os.environ.get("EXASERVE_RUN_LOG_DIR", "").strip()
    result_root = (
        Path(configured_result_root)
        if configured_result_root and os.path.isabs(configured_result_root)
        else Path(shared_stage_base).resolve()
    )
    try:
        for stage in range(pp_size):
            stage_dir = shared_attempt / f"stage{stage}" / safe_name
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
                from .model_staging import (
                    COMPLETION_MARKER,
                    write_completion_marker,
                )

                write_completion_marker(
                    stage_dir,
                    source_identity=f"{Path(model_dir).resolve()}#pp{pp_size}/stage{stage}",
                )
                from .state.atomic import strict_json_load_path

                manifest = strict_json_load_path(stage_dir / COMPLETION_MARKER)
                manifest_hash = manifest["manifest_hash"]
            hosts = groups[stage]
            candidate_root = Path(local_path) / (
                f".exaserve_pp_candidate.{safe_name}.{generation}.{attempt}.stage{stage}"
            )
            candidate = candidate_root / safe_name
            command = bcast_cmd(bcast_bin, stage_dir, candidate_root, hosts, scheduler=scheduler)
            item = {
                "stage": stage,
                "hosts": list(hosts),
                "command": command,
                "summary": summary,
                "manifest_hash": manifest_hash,
                "receipts": [],
            }
            plan.append(item)
            if dry_run:
                continue
            print(
                f"[pp_stage] candidate stage {stage} "
                f"({summary['n_shards']} shards, "
                f"{summary['bytes'] / 1024**3:.1f} GiB) -> {hosts}",
                flush=True,
            )
            _run(command, timeout_s=operation_timeout_s)
            from .staging_results import create_result_dir, load_rank_results

            stage_attempt = f"{attempt}-stage{stage}"
            category = "pp-publish-" + hashlib.sha256(model_id.encode()).hexdigest()[:12]
            result_dir = create_result_dir(result_root, category, stage_attempt)
            _run(
                [
                    *subset_launch_prefix(hosts, scheduler),
                    sys.executable,
                    "-m",
                    "exaserve.pp_stage",
                    "--verify-and-publish",
                    str(candidate),
                    "--publish-target",
                    str(target),
                    "--model-id",
                    model_id,
                    "--expected-manifest-hash",
                    manifest_hash,
                    "--generation",
                    str(generation),
                    "--stage",
                    str(stage),
                    "--result-dir",
                    str(result_dir),
                    "--attempt-id",
                    stage_attempt,
                ],
                timeout_s=operation_timeout_s,
            )
            receipts = [
                _validate_pp_receipt(receipt)
                for receipt in load_rank_results(result_dir, attempt_id=stage_attempt)
            ]
            by_node = {receipt["node"]: receipt for receipt in receipts}
            if len(receipts) != len(hosts) or len(by_node) != len(hosts):
                raise RuntimeError(
                    f"PP stage {stage} returned {len(receipts)} receipts for "
                    f"{len(hosts)} unique planned hosts"
                )
            from .plan.contracts import same_node

            ordered_receipts = []
            for host in hosts:
                matches = [receipt for receipt in receipts if same_node(receipt["node"], host)]
                if len(matches) != 1:
                    raise RuntimeError(
                        f"PP stage {stage} host {host!r} has {len(matches)} receipts"
                    )
                receipt = matches[0]
                if (
                    receipt.get("generation") != generation
                    or receipt.get("pp_stage") != stage
                    or receipt.get("manifest_hash") != manifest_hash
                ):
                    raise RuntimeError(f"PP stage {stage} receipt from {host!r} has wrong identity")
                ordered_receipts.append(receipt)
            item["receipts"] = ordered_receipts
        return plan
    finally:
        if not dry_run:
            active_error = sys.exc_info()[1]
            try:
                shutil.rmtree(shared_attempt)
            except FileNotFoundError:
                pass
            except OSError as cleanup_exc:
                if active_error is None:
                    raise RuntimeError(
                        f"PP shared staging cleanup failed: {cleanup_exc}"
                    ) from cleanup_exc
                add_exception_note(
                    active_error, f"PP shared staging cleanup also failed: {cleanup_exc}"
                )


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
    parser.add_argument("--model-id")
    parser.add_argument("--expected-manifest-hash")
    parser.add_argument("--generation", type=int)
    parser.add_argument("--stage", type=int)
    parser.add_argument("--result-dir")
    parser.add_argument("--attempt-id")
    args = parser.parse_args(argv)
    if args.verify_and_publish:
        required = (
            args.publish_target,
            args.model_id,
            args.expected_manifest_hash,
            args.generation,
            args.stage,
            args.result_dir,
            args.attempt_id,
        )
        if any(value is None for value in required):
            parser.error("verification mode requires target/model/hash/generation/stage")
        receipt = verify_and_publish(
            Path(args.verify_and_publish),
            Path(args.publish_target),
            model_id=args.model_id,
            expected_manifest_hash=args.expected_manifest_hash,
            generation=args.generation,
            stage=args.stage,
        )
        from .staging_results import write_rank_result

        write_rank_result(args.result_dir, attempt_id=args.attempt_id, payload=receipt)
        return 0
    if not args.model_dir or not args.pp or not args.replicas or not args.nodes:
        parser.error("planning mode requires model_dir, --pp, --replicas, --nodes")
    nodes = args.nodes.split(",")
    plan = stage_pp_sharded(
        args.model_dir,
        args.safe_name,
        args.stage_base,
        args.local_path,
        args.pp,
        nodes,
        args.replicas,
        args.bcast_bin,
        dry_run=True,
    )
    for item in plan:
        print(f"stage {item['stage']}: {item['hosts']} -> {item['command']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
