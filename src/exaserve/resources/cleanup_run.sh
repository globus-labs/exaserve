#!/bin/bash
# Small MPI cleanup script: wipe node-local artifacts a run touches.
#
# Run one rank per node, e.g.:
#   mpiexec -n <N> -ppn 1 --cpu-bind none bash cleanup_run.sh [extra_path ...]
#
# Purpose: leave each node's tmpfs clean so the NEXT run stages weights fresh
# (Phase-2 MPI staging is exercised + timed every run) and starts from a known
# state (also clears stale caches/instrumentation left by a crashed run).
# Idempotent; never touches Lustre or $HOME. Extra paths (e.g. a custom
# local_stage_path) may be passed as arguments.
set -u
host="$(hostname)"

# Standard per-run node-local artifacts (see launch_cluster.sh / model_bcast.py
# / distribute_to_nodes.sh). Globs are expanded below.
patterns=(
  /tmp/hf_home            # staged model weights (default local_stage_path)
  /tmp/hf_home_*          # custom/variant stage paths
  /tmp/exaserve_src         # bcast'd exaserve source (symlink to the generation)
  /tmp/exaserve_src.*       # per-generation source trees (IMP-H02)
  /tmp/exaserve_inst        # instrumentation probe outputs
  /tmp/exaserve_overlay     # Ray Serve overlay (instrumentation builds)
  /tmp/ray/session_*      # Ray session scratch (Ray usually clears on stop)
  /tmp/replay_rank*       # replay client per-run Go JSONL scratch (mkdtemp, never self-removed)
  /tmp/exaserve_pp_shim     # PP sitecustomize shim dir written by VLLMWorker.__init__
)
patterns+=("$@")

removed=0
for pat in "${patterns[@]}"; do
  for path in $pat; do            # word-split + glob-expand intentionally
    # -e is false for a DANGLING symlink; /tmp/exaserve_src is a symlink to a
    # generation tree that a prior pattern may already have removed, and it
    # must still be cleaned up rather than left pointing at nothing.
    [ -e "$path" ] || [ -L "$path" ] || continue
    if rm -rf -- "$path" 2>/dev/null; then
      echo "[cleanup $host] removed $path"
      removed=$((removed + 1))
    else
      echo "[cleanup $host] WARN: could not remove $path"
    fi
  done
done
echo "[cleanup $host] done (${removed} path(s) removed)"
