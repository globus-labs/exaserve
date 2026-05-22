#!/bin/bash
# Distribute aurora_rayserver package code and, optionally, a Ray Serve
# overlay to every PBS node's local /tmp via MPI primitives.
#
# Replaces the prior parallel-ssh fan-out: the head node opened one ssh
# per worker and bash-forked them with &+wait, which:
#   - serialized on head-node CPU at large scale (256+ TCP sessions),
#   - made a non-trivial paper-claim weak (control plane is ssh, not MPI),
#   - duplicated reads from Lustre because each worker `cp -r`'d from
#     shared FS independently.
#
# The new path: one bcast for the package tree (1 Lustre read + tree
# broadcast over HSN, written into rank-local tmpfs), and if
# AURORA_INSTRUMENTATION=1, a second bcast for the small set of patched
# Ray Serve files plus an mpiexec'd setup_overlay.sh on every rank.
#
# Outputs (unchanged):
#   /tmp/aurora_src/aurora_rayserver       (always)
#   /tmp/aurora_overlay/ray/...            (when AURORA_INSTRUMENTATION=1)
#
# Required env:
#   AURORA_RAYSERVER_PACKAGE_ROOT          absolute path to package dir
#   PYTHON_EXEC                            Aurora frameworks python3
#   UNIQUE_NODES_FILE                      one PBS hostname per line
#   HOSTNAME_SHORT                         short hostname of the head node
#
# Optional env:
#   AURORA_INSTRUMENTATION                 1 to build overlay, 0 to skip
#   AURORA_BCAST_BUILD_DIR                 override bcast build dir
set -euo pipefail

if [ -z "${PYTHON_EXEC:-}" ] || [ -z "${UNIQUE_NODES_FILE:-}" ] || [ -z "${HOSTNAME_SHORT:-}" ]; then
    echo "[distribute_to_nodes] ERROR: PYTHON_EXEC/UNIQUE_NODES_FILE/HOSTNAME_SHORT must be set"
    exit 1
fi

if [ -z "${AURORA_RAYSERVER_PACKAGE_ROOT:-}" ]; then
    if [ -n "${PROJECT_ROOT:-}" ] && [ -d "$PROJECT_ROOT/src/aurora_rayserver" ]; then
        AURORA_RAYSERVER_PACKAGE_ROOT="$PROJECT_ROOT/src/aurora_rayserver"
    else
        echo "[distribute_to_nodes] ERROR: AURORA_RAYSERVER_PACKAGE_ROOT is not set"
        exit 1
    fi
fi

PACKAGE_ROOT="$AURORA_RAYSERVER_PACKAGE_ROOT"
INSTRUMENTATION="${AURORA_INSTRUMENTATION:-0}"
OVERLAY_SRC="$PACKAGE_ROOT/patches/ray_serve_overlay/ray"

if [ ! -d "$PACKAGE_ROOT" ]; then
    echo "[distribute_to_nodes] ERROR: package root does not exist: $PACKAGE_ROOT"
    exit 1
fi
if [ ! -f "$PACKAGE_ROOT/driver.py" ] || [ ! -f "$PACKAGE_ROOT/server.py" ]; then
    echo "[distribute_to_nodes] ERROR: invalid package root: $PACKAGE_ROOT"
    exit 1
fi
if [ "$INSTRUMENTATION" = "1" ] && [ ! -d "$OVERLAY_SRC/serve/_private" ]; then
    echo "[distribute_to_nodes] ERROR: AURORA_INSTRUMENTATION=1 but $OVERLAY_SRC/serve/_private not found"
    exit 1
fi

LOCAL_SRC="/tmp/aurora_src"
LOCAL_OVERLAY="/tmp/aurora_overlay"
NODE_COUNT="$(wc -l < "$UNIQUE_NODES_FILE")"

# Compile bcast (+ gather, harmless) on demand. We share the same build
# dir Python-side uses, so a single mtime-keyed make handles both.
BUILD_DIR="${AURORA_BCAST_BUILD_DIR:-${AURORA_RUN_LOG_DIR:-/tmp}/bcast_build}"
mkdir -p "$BUILD_DIR"
"$PYTHON_EXEC" - <<PY
from aurora_rayserver.model_bcast import compile_bcast, compile_gather
from pathlib import Path
tools = Path("$BUILD_DIR")
compile_bcast(tools)
compile_gather(tools)
PY
BCAST_BIN="$BUILD_DIR/bcast"
if [ ! -x "$BCAST_BIN" ]; then
    echo "[distribute_to_nodes] ERROR: bcast binary not built at $BCAST_BIN"
    exit 1
fi

# Excluded subtrees / files. The bcast'd tree is what every rank loads on
# startup, so __pycache__ and the (large) overlay source dir are not needed.
# tar handles these via --exclude before piping into MPI_Bcast.
#
# We don't have a "tar --exclude" hook into bcast.c, so we stage a clean
# copy on rank 0's tmpfs first, then bcast that. This is one extra local
# copy on the head — negligible vs. the saved 256× Lustre reads.
TMP_CLEAN="${BUILD_DIR}/aurora_rayserver_clean"
rm -rf "$TMP_CLEAN"
mkdir -p "$TMP_CLEAN/aurora_rayserver"
if command -v rsync >/dev/null 2>&1; then
    rsync -a \
        --exclude='patches/ray_serve_overlay/' \
        --exclude='__pycache__/' --exclude='*.pyc' \
        "$PACKAGE_ROOT/" "$TMP_CLEAN/aurora_rayserver/"
else
    cp -a "$PACKAGE_ROOT/." "$TMP_CLEAN/aurora_rayserver/"
    rm -rf "$TMP_CLEAN/aurora_rayserver/patches/ray_serve_overlay"
    find "$TMP_CLEAN" -name __pycache__ -type d -prune -exec rm -rf {} +
    find "$TMP_CLEAN" -name '*.pyc' -type f -delete
fi

echo "[distribute_to_nodes] bcast source ($(du -sh "$TMP_CLEAN" | awk '{print $1}')) to $NODE_COUNT node(s) -> $LOCAL_SRC"
mpiexec -n "$NODE_COUNT" -ppn 1 --cpu-bind none \
    "$BCAST_BIN" "$TMP_CLEAN/aurora_rayserver" "$LOCAL_SRC"

# Optional: overlay. bcast the patched files (tiny — five .py files),
# then run setup_overlay.sh on every rank to assemble the symlink farm
# locally. mpiexec replaces the previous `for node in ...; do ssh & done`.
if [ "$INSTRUMENTATION" = "1" ]; then
    # bcast extracts <dest>/<basename(src)>/. We want every node to end up
    # with /tmp/overlay_patches/serve/_private/*.py, so the src basename
    # must be exactly "overlay_patches" and we bcast to dest=/tmp.
    TMP_PATCHES="${BUILD_DIR}/overlay_patches"
    rm -rf "$TMP_PATCHES"
    mkdir -p "$TMP_PATCHES/serve/_private"
    cp "$OVERLAY_SRC"/serve/_private/*.py "$TMP_PATCHES/serve/_private/"

    echo "[distribute_to_nodes] bcast overlay patches to $NODE_COUNT node(s) -> /tmp/overlay_patches"
    mpiexec -n "$NODE_COUNT" -ppn 1 --cpu-bind none \
        "$BCAST_BIN" "$TMP_PATCHES" "/tmp"

    SETUP_SCRIPT="$PACKAGE_ROOT/resources/setup_overlay.sh"
    if [ ! -x "$SETUP_SCRIPT" ]; then chmod +x "$SETUP_SCRIPT" || true; fi
    echo "[distribute_to_nodes] building per-node /tmp/aurora_overlay symlink farm"
    PYTHON_EXEC="$PYTHON_EXEC" AURORA_OVERLAY_PATCHES_DIR="/tmp/overlay_patches" \
    mpiexec -n "$NODE_COUNT" -ppn 1 --cpu-bind none \
        bash "$SETUP_SCRIPT"
fi

if [ "$INSTRUMENTATION" = "1" ]; then
    echo "[distribute_to_nodes] aurora_src -> $LOCAL_SRC; overlay -> $LOCAL_OVERLAY (${NODE_COUNT} ranks via MPI)"
else
    echo "[distribute_to_nodes] aurora_src -> $LOCAL_SRC (${NODE_COUNT} ranks via MPI); overlay disabled"
fi
