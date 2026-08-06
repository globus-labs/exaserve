#!/bin/bash
# Distribute exaserve package code and, optionally, a Ray Serve
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
# EXASERVE_INSTRUMENTATION=1, a second bcast for the small set of patched
# Ray Serve files plus an mpiexec'd setup_overlay.sh on every rank.
#
# Outputs (unchanged):
#   /tmp/exaserve_src/exaserve       (always)
#   /tmp/exaserve_overlay/ray/...            (when EXASERVE_INSTRUMENTATION=1)
#
# Required env:
#   EXASERVE_PACKAGE_ROOT          absolute path to package dir
#   PYTHON_EXEC                            Aurora frameworks python3
#   UNIQUE_NODES_FILE                      one PBS hostname per line
#   HOSTNAME_SHORT                         short hostname of the head node
#
# Optional env:
#   EXASERVE_INSTRUMENTATION                 1 to build overlay, 0 to skip
#   EXASERVE_BCAST_BUILD_DIR                 override bcast build dir
set -euo pipefail

if [ -z "${PYTHON_EXEC:-}" ] || [ -z "${UNIQUE_NODES_FILE:-}" ] || [ -z "${HOSTNAME_SHORT:-}" ]; then
    echo "[distribute_to_nodes] ERROR: PYTHON_EXEC/UNIQUE_NODES_FILE/HOSTNAME_SHORT must be set"
    exit 1
fi

if [ -z "${EXASERVE_PACKAGE_ROOT:-}" ]; then
    if [ -n "${PROJECT_ROOT:-}" ] && [ -d "$PROJECT_ROOT/src/exaserve" ]; then
        EXASERVE_PACKAGE_ROOT="$PROJECT_ROOT/src/exaserve"
    else
        echo "[distribute_to_nodes] ERROR: EXASERVE_PACKAGE_ROOT is not set"
        exit 1
    fi
fi

PACKAGE_ROOT="$EXASERVE_PACKAGE_ROOT"
INSTRUMENTATION="${EXASERVE_INSTRUMENTATION:-0}"
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
    echo "[distribute_to_nodes] ERROR: EXASERVE_INSTRUMENTATION=1 but $OVERLAY_SRC/serve/_private not found"
    exit 1
fi

# IMP-H02: stage into a GENERATION-ISOLATED directory. A stable shared path
# lets a new run import files left behind by an older source tree (deleted
# modules stay importable). The generation id comes from the run-log stamp /
# job id; `exaserve_src` remains as a symlink so existing PYTHONPATH entries
# and docs keep working, but it always points at THIS run's tree.
EXASERVE_GENERATION="${EXASERVE_GENERATION:-${EXASERVE_JOBID:-$$}}"
EXASERVE_GENERATION="${EXASERVE_GENERATION%%.*}"
EXASERVE_GENERATION="$(printf '%s' "$EXASERVE_GENERATION" | tr -c 'A-Za-z0-9_-' '_' | cut -c1-32)"
LOCAL_SRC_GEN="/tmp/exaserve_src.${EXASERVE_GENERATION}"
LOCAL_SRC="/tmp/exaserve_src"
LOCAL_OVERLAY="/tmp/exaserve_overlay"
NODE_COUNT="$(wc -l < "$UNIQUE_NODES_FILE")"

# Compile bcast (+ gather, harmless) on demand. We share the same build
# dir Python-side uses, so a single mtime-keyed make handles both.
BUILD_DIR="${EXASERVE_BCAST_BUILD_DIR:-${EXASERVE_RUN_LOG_DIR:-/tmp}/bcast_build}"
mkdir -p "$BUILD_DIR"
"$PYTHON_EXEC" - <<PY
from exaserve.model_bcast import compile_bcast, compile_gather
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
TMP_CLEAN="${BUILD_DIR}/exaserve_clean"
rm -rf "$TMP_CLEAN"
mkdir -p "$TMP_CLEAN/exaserve"
if command -v rsync >/dev/null 2>&1; then
    rsync -a \
        --exclude='patches/ray_serve_overlay/' \
        --exclude='__pycache__/' --exclude='*.pyc' \
        "$PACKAGE_ROOT/" "$TMP_CLEAN/exaserve/"
else
    cp -a "$PACKAGE_ROOT/." "$TMP_CLEAN/exaserve/"
    rm -rf "$TMP_CLEAN/exaserve/patches/ray_serve_overlay"
    find "$TMP_CLEAN" -name __pycache__ -type d -prune -exec rm -rf {} +
    find "$TMP_CLEAN" -name '*.pyc' -type f -delete
fi

echo "[distribute_to_nodes] bcast source ($(du -sh "$TMP_CLEAN" | awk '{print $1}')) to $NODE_COUNT node(s) -> $LOCAL_SRC_GEN (gen $EXASERVE_GENERATION)"
${EXASERVE_MPILAUNCH} \
    "$BCAST_BIN" "$TMP_CLEAN/exaserve" "$LOCAL_SRC_GEN"
# Publish the stable name atomically on every rank. `ln -sfn` + `mv -T` is a
# rename(2), so a reader sees either the old tree or the new one, never a mix.
# A pre-existing REAL directory at the stable path is a leftover from before
# generation isolation; rename cannot replace a directory with a symlink, so
# retire it first. Older generations are collected here too — /tmp is node
# tmpfs and a long-lived allocation would otherwise accumulate source trees.
${EXASERVE_MPILAUNCH} bash -c "
    set -e
    if [ -d '$LOCAL_SRC' ] && [ ! -L '$LOCAL_SRC' ]; then rm -rf '$LOCAL_SRC'; fi
    ln -sfn '$LOCAL_SRC_GEN' '${LOCAL_SRC}.new.\$\$'
    mv -Tf '${LOCAL_SRC}.new.\$\$' '$LOCAL_SRC'
    find /tmp -maxdepth 1 -name 'exaserve_src.*' -type d -mmin +360 \
        -exec rm -rf {} + 2>/dev/null || true
    exit 0"

# --- Optional: node-local engine venv (sglang) ---
# PYTHON_EXEC may point at a venv on shared $HOME (gecko), and Triton lives in
# the frameworks install on /opt/aurora, which is NFS from hawk.lb. Every
# engine process imports the venv's site-packages, and Triton's JIT
# (triton_key) hashes the entire ~1GB triton package per process on first
# compile. At hundreds of concurrent engines this is a multi-minute
# shared-FS read storm (sglang_haproxy_full run4: every stream wedged for
# 330s). Stage the venv once via MPI bcast — with a triton copy injected
# into its site-packages so it shadows the NFS copy — exactly like the
# exaserve_src tree above. launch_cluster.sh repoints PYTHON_EXEC afterwards.
LOCAL_VENV="/tmp/exaserve_venv"
if [ "${EXASERVE_STAGE_VENV:-0}" = "1" ] && [ -n "${EXASERVE_VENV_ROOT:-}" ]; then
    if [ ! -f "$EXASERVE_VENV_ROOT/pyvenv.cfg" ]; then
        echo "[distribute_to_nodes] ERROR: EXASERVE_VENV_ROOT=$EXASERVE_VENV_ROOT is not a venv (no pyvenv.cfg)"
        exit 1
    fi
    # Stage a dereferenced clean copy on the head node's tmpfs (not Lustre:
    # this tree is ~7GB and tmpfs->HSN bcast avoids a shared-FS round trip).
    TMP_VENV_STAGE="/tmp/exaserve_venv_stage"
    rm -rf "$TMP_VENV_STAGE"
    mkdir -p "$TMP_VENV_STAGE/exaserve_venv"
    if command -v rsync >/dev/null 2>&1; then
        rsync -aL --exclude='__pycache__/' --exclude='*.pyc' \
            "$EXASERVE_VENV_ROOT/" "$TMP_VENV_STAGE/exaserve_venv/"
    else
        cp -rL "$EXASERVE_VENV_ROOT/." "$TMP_VENV_STAGE/exaserve_venv/"
        find "$TMP_VENV_STAGE" -name __pycache__ -type d -prune -exec rm -rf {} +
    fi
    # Shadow the NFS triton with a node-local copy: venv site-packages
    # precede system site-packages, so this wins over /opt/aurora.
    VENV_SITE="$(ls -d "$TMP_VENV_STAGE/exaserve_venv/lib/python"*/site-packages | head -1)"
    if [ ! -d "$VENV_SITE/triton" ]; then
        TRITON_SRC="$("$PYTHON_EXEC" -c 'import triton, os; print(os.path.dirname(triton.__file__))' 2>/dev/null || true)"
        if [ -n "$TRITON_SRC" ] && [ -d "$TRITON_SRC" ]; then
            cp -r "$TRITON_SRC" "$VENV_SITE/triton"
        else
            echo "[distribute_to_nodes] WARNING: could not locate triton to inject into staged venv"
        fi
    fi
    echo "[distribute_to_nodes] bcast venv ($(du -sh "$TMP_VENV_STAGE/exaserve_venv" | awk '{print $1}')) to $NODE_COUNT node(s) -> $LOCAL_VENV"
    ${EXASERVE_MPILAUNCH} \
        "$BCAST_BIN" "$TMP_VENV_STAGE/exaserve_venv" "/tmp"
    rm -rf "$TMP_VENV_STAGE"
    if [ ! -x "$LOCAL_VENV/bin/python" ]; then
        echo "[distribute_to_nodes] ERROR: staged venv missing $LOCAL_VENV/bin/python"
        exit 1
    fi
fi

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
    ${EXASERVE_MPILAUNCH} \
        "$BCAST_BIN" "$TMP_PATCHES" "/tmp"

    SETUP_SCRIPT="$PACKAGE_ROOT/resources/setup_overlay.sh"
    if [ ! -x "$SETUP_SCRIPT" ]; then chmod +x "$SETUP_SCRIPT" || true; fi
    echo "[distribute_to_nodes] building per-node /tmp/exaserve_overlay symlink farm"
    PYTHON_EXEC="$PYTHON_EXEC" EXASERVE_OVERLAY_PATCHES_DIR="/tmp/overlay_patches" \
    ${EXASERVE_MPILAUNCH} \
        bash "$SETUP_SCRIPT"
fi

if [ "$INSTRUMENTATION" = "1" ]; then
    echo "[distribute_to_nodes] exaserve_src -> $LOCAL_SRC; overlay -> $LOCAL_OVERLAY (${NODE_COUNT} ranks via MPI)"
else
    echo "[distribute_to_nodes] exaserve_src -> $LOCAL_SRC (${NODE_COUNT} ranks via MPI); overlay disabled"
fi
