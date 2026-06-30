"""Shard-aware pipeline-parallel staging orchestration.

Deterministic node<->(replica, stage) mapping + per-PP-stage ("two-group") bcast,
so each node stages ONLY its pipeline stage's shards (~model_size / PP), small
enough for node-local tmpfs, with only the PP seed nodes reading the shared store.

The mapping `assign_pp_nodes` is the contract shared with the (node-pinned)
deployment: the deploy MUST place replica r's stage s on the SAME node this
staging put stage s's shards on, or a worker would find the wrong shards.

  replica r  -> nodes[r*PP : (r+1)*PP]
  stage  s   -> node[r*PP + s]

Staging then runs bcast once per PP stage on that stage's node group (a disjoint
subset across replicas): the group's seed reads the stage's pruned dir (a symlink
farm on the shared store; bcast's `tar -ch` dereferences it) and broadcasts to
the rest of the group. No change to tools/bcast.c — just a host subset per call.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

from . import shard_prune


def assign_pp_nodes(ordered_nodes, pp_size: int, num_replicas: int):
    """[(replica, stage, node)] for the canonical contiguous pinning. `ordered_nodes`
    must be a STABLE ordering shared by staging and deploy."""
    need = num_replicas * pp_size
    if len(ordered_nodes) < need:
        raise ValueError(
            f"need {need} nodes for {num_replicas} replicas x PP{pp_size}, "
            f"got {len(ordered_nodes)}")
    return [(r, s, ordered_nodes[r * pp_size + s])
            for r in range(num_replicas) for s in range(pp_size)]


def stage_node_groups(ordered_nodes, pp_size: int, num_replicas: int):
    """stage -> [node, ...] hosting that stage across all replicas (the bcast group)."""
    groups = {s: [] for s in range(pp_size)}
    for _r, s, node in assign_pp_nodes(ordered_nodes, pp_size, num_replicas):
        groups[s].append(node)
    return groups


def bcast_cmd(bcast_bin, src, dest, hosts):
    """mpiexec invocation broadcasting `src` to exactly `hosts` (rank 0 = hosts[0])."""
    return ["mpiexec", "-n", str(len(hosts)), "-ppn", "1", "--cpu-bind", "none",
            "--hosts", ",".join(hosts), str(bcast_bin), str(src), str(dest)]


def stage_pp_sharded(model_dir, safe_name, shared_stage_base, local_path, pp_size,
                     ordered_nodes, num_replicas, bcast_bin, *, dry_run=False,
                     partition_env=None):
    """Build per-stage pruned dirs under <shared_stage_base>/stage{s}/<safe_name>
    (each a symlink farm), then bcast each to its stage's node group so every node
    ends up with local_path/<safe_name> holding ONLY its stage's shards.

    The pruned dir's leaf is <safe_name> so bcast lands it at local_path/<safe_name>
    on every node (matching the harness's get_model_storage_path layout).

    Returns the plan: [(stage, hosts, cmd, summary)].
    """
    groups = stage_node_groups(ordered_nodes, pp_size, num_replicas)
    plan = []
    for s in range(pp_size):
        stage_dir = Path(shared_stage_base) / f"stage{s}" / safe_name
        summary = None
        if not dry_run:
            summary = shard_prune.build_stage_dir(model_dir, pp_size, s, stage_dir, partition_env)
        else:
            shards, keep, total = shard_prune.plan_stage_shards(model_dir, pp_size, s, partition_env)
            summary = {"stage": s, "n_shards": len(shards), "n_weights": len(keep), "bytes": total}
        hosts = groups[s]
        cmd = bcast_cmd(bcast_bin, stage_dir, local_path, hosts)
        plan.append((s, hosts, cmd, summary))
        if not dry_run:
            print(f"[pp_stage] bcast stage {s} ({summary['n_shards']} shards, "
                  f"{summary['bytes'] / 1024**3:.1f} GiB) -> {len(hosts)} node(s): {hosts}",
                  flush=True)
            subprocess.run(cmd, check=True)
    return plan


def main(argv=None):
    """Dry-run planner: print the node<->(replica,stage) map and the bcast plan."""
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("model_dir")
    ap.add_argument("--pp", type=int, required=True)
    ap.add_argument("--replicas", type=int, required=True)
    ap.add_argument("--nodes", required=True, help="comma-separated ordered node ids/hosts")
    ap.add_argument("--local-path", default="/tmp/aurora_stage")
    ap.add_argument("--stage-base", default="/lus/flare/.../pp_stage")
    ap.add_argument("--safe-name", default="MODEL")
    ap.add_argument("--bcast-bin", default="bcast")
    args = ap.parse_args(argv)
    nodes = args.nodes.split(",")
    print("node <-> (replica, stage):")
    for r, s, node in assign_pp_nodes(nodes, args.pp, args.replicas):
        print(f"  replica {r} stage {s} -> {node}")
    print("\nbcast plan (dry-run):")
    plan = stage_pp_sharded(args.model_dir, args.safe_name, args.stage_base, args.local_path,
                            args.pp, nodes, args.replicas, args.bcast_bin, dry_run=True)
    for s, hosts, cmd, summ in plan:
        print(f"  stage {s}: {summ['n_shards']} shards / {summ['bytes']/1024**3:.1f} GiB "
              f"-> {hosts}\n    $ {' '.join(cmd)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
