#!/bin/bash
# Distribute aurora_rayserver sources (and optionally a Ray Serve overlay) to
# every PBS node's local /tmp. Always runs.
#
# Outputs:
#   /tmp/aurora_src                 — copy of $PROJECT_ROOT/src (minus the
#                                     patches/ subtree). Prepended to PYTHONPATH
#                                     so user code is loaded from node-local
#                                     tmpfs instead of Lustre.
#   /tmp/aurora_overlay/ray/...     — symlink farm pointing at the system Ray
#                                     install, with a few patched files
#                                     replaced from
#                                     $PROJECT_ROOT/src/patches/ray_serve_overlay/.
#                                     Only created when AURORA_INSTRUMENTATION=1.
#
# Caller is responsible for prepending the resulting paths to PYTHONPATH.
#
# Required env:
#   PROJECT_ROOT          — absolute path to aurora_rayserver checkout
#   PYTHON_EXEC           — path to the Aurora frameworks python3 (used to
#                           locate site-packages/ray/)
#   UNIQUE_NODES_FILE     — file with one short hostname per line (PBS nodes)
#   HOSTNAME_SHORT        — short hostname of the head node (skip in fan-out)
#
# Optional env:
#   AURORA_INSTRUMENTATION  — 1 to build the overlay, 0 (default) to skip
set -e

if [ -z "$PROJECT_ROOT" ] || [ -z "$PYTHON_EXEC" ] || [ -z "$UNIQUE_NODES_FILE" ] || [ -z "$HOSTNAME_SHORT" ]; then
    echo "[distribute_to_nodes] ERROR: PROJECT_ROOT/PYTHON_EXEC/UNIQUE_NODES_FILE/HOSTNAME_SHORT must be set"
    exit 1
fi

INSTRUMENTATION="${AURORA_INSTRUMENTATION:-0}"
SRC_DIR="$PROJECT_ROOT/src"
OVERLAY_SRC="$PROJECT_ROOT/src/aurora_rayserver/patches/ray_serve_overlay/ray"
SYSRAY="$(dirname "$(dirname "$PYTHON_EXEC")")/lib/python3.12/site-packages/ray"

if [ ! -d "$SRC_DIR" ]; then
    echo "[distribute_to_nodes] ERROR: $SRC_DIR does not exist"
    exit 1
fi
if [ "$INSTRUMENTATION" = "1" ] && [ ! -d "$OVERLAY_SRC/serve/_private" ]; then
    echo "[distribute_to_nodes] ERROR: AURORA_INSTRUMENTATION=1 but $OVERLAY_SRC/serve/_private not found"
    exit 1
fi
if [ "$INSTRUMENTATION" = "1" ] && [ ! -d "$SYSRAY/serve/_private" ]; then
    echo "[distribute_to_nodes] ERROR: system Ray not found at $SYSRAY"
    exit 1
fi

LOCAL_SRC="/tmp/aurora_src"
LOCAL_OVERLAY="/tmp/aurora_overlay"

# List of patched filenames (computed once on the head, used on every node).
PATCHED_FILES=""
if [ "$INSTRUMENTATION" = "1" ]; then
    PATCHED_FILES=$(cd "$OVERLAY_SRC/serve/_private" && ls *.py 2>/dev/null | tr '\n' ' ')
fi

# Stage the per-node setup script on Lustre so SSHed workers can read it.
_STAGED_SCRIPT=$(mktemp "$HOME/.aurora_distribute_XXXX.sh")
trap 'rm -f "$_STAGED_SCRIPT"' EXIT

cat > "$_STAGED_SCRIPT" <<NODEEOF
#!/bin/bash
set -e

SRC_DIR="$SRC_DIR"
OVERLAY_SRC="$OVERLAY_SRC"
SYSRAY="$SYSRAY"
LOCAL_SRC="$LOCAL_SRC"
LOCAL_OVERLAY="$LOCAL_OVERLAY"
INSTRUMENTATION="$INSTRUMENTATION"
PATCHED_FILES="$PATCHED_FILES"

# 1. aurora_serve sources -> /tmp/aurora_src (excluding the patches overlay
#    subtree, which is staged separately at /tmp/aurora_overlay below).
rm -rf "\$LOCAL_SRC"
mkdir -p "\$LOCAL_SRC"
# rsync is ubiquitous on Aurora compute nodes; falls back to cp if missing.
if command -v rsync >/dev/null 2>&1; then
    rsync -a --exclude='aurora_rayserver/patches/ray_serve_overlay/' \\
        --exclude='__pycache__/' --exclude='*.pyc' \\
        "\$SRC_DIR/" "\$LOCAL_SRC/"
else
    cp -a "\$SRC_DIR/." "\$LOCAL_SRC/"
    rm -rf "\$LOCAL_SRC/aurora_rayserver/patches/ray_serve_overlay"
    find "\$LOCAL_SRC" -name __pycache__ -type d -prune -exec rm -rf {} +
fi

# 2. Ray overlay -> /tmp/aurora_overlay/ray (symlink farm + patched files).
if [ "\$INSTRUMENTATION" = "1" ]; then
    rm -rf "\$LOCAL_OVERLAY"
    mkdir -p "\$LOCAL_OVERLAY/ray/serve/_private"
    # Symlink ray/* (except serve)
    for f in "\$SYSRAY"/*; do
        n=\$(basename "\$f")
        [ "\$n" = serve ] && continue
        ln -s "\$f" "\$LOCAL_OVERLAY/ray/\$n" 2>/dev/null
    done
    # Symlink ray/serve/* (except _private)
    for f in "\$SYSRAY/serve"/*; do
        n=\$(basename "\$f")
        [ "\$n" = _private ] && continue
        ln -s "\$f" "\$LOCAL_OVERLAY/ray/serve/\$n" 2>/dev/null
    done
    # Symlink ray/serve/_private/* except patched files and __pycache__
    for f in "\$SYSRAY/serve/_private"/*; do
        n=\$(basename "\$f")
        [ "\$n" = __pycache__ ] && continue
        skip=0
        for p in \$PATCHED_FILES; do
            [ "\$n" = "\$p" ] && skip=1 && break
        done
        [ \$skip -eq 1 ] && continue
        ln -s "\$f" "\$LOCAL_OVERLAY/ray/serve/_private/\$n" 2>/dev/null
    done
    # Real copies of patched files
    for p in \$PATCHED_FILES; do
        cp "\$OVERLAY_SRC/serve/_private/\$p" "\$LOCAL_OVERLAY/ray/serve/_private/\$p"
    done
fi

# Verify the files that the runtime now depends on are present on this node.
test -f "\$LOCAL_SRC/aurora_rayserver/driver.py"
test -f "\$LOCAL_SRC/aurora_rayserver/server.py"
test -f "\$LOCAL_SRC/aurora_rayserver/ray_start.py"
if [ "\$INSTRUMENTATION" = "1" ]; then
    test -f "\$LOCAL_OVERLAY/ray/serve/_private/constants.py"
    test -f "\$LOCAL_OVERLAY/ray/serve/_private/deployment_state.py"
fi
NODEEOF

chmod +x "$_STAGED_SCRIPT"

# Run on head first (synchronous so failures surface immediately).
bash "$_STAGED_SCRIPT"

# Fan out to workers in parallel and wait for completion so missing /tmp
# staging fails before mpiexec starts the runtime.
DIST_LOG_DIR="${AURORA_RUN_LOG_DIR:-/tmp}"
mkdir -p "$DIST_LOG_DIR" 2>/dev/null || DIST_LOG_DIR="/tmp"
worker_count=0
pids=()
nodes=()
while read -r node; do
    [ -z "$node" ] && continue
    short="${node%%.*}"
    [ "$short" = "$HOSTNAME_SHORT" ] && continue
    timeout 180 ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "$node" \
        "bash $_STAGED_SCRIPT" >"$DIST_LOG_DIR/distribute_${short}.log" 2>&1 &
    pids+=($!)
    nodes+=("$node")
    worker_count=$((worker_count + 1))
done < "$UNIQUE_NODES_FILE"

failed=0
for i in "${!pids[@]}"; do
    if ! wait "${pids[$i]}"; then
        echo "[distribute_to_nodes] ERROR: staging failed on ${nodes[$i]} (see $DIST_LOG_DIR/distribute_${nodes[$i]%%.*}.log)"
        failed=1
    fi
done
if [ "$failed" -ne 0 ]; then
    exit 1
fi

if [ "$INSTRUMENTATION" = "1" ]; then
    echo "[distribute_to_nodes] aurora_src -> $LOCAL_SRC; ray overlay -> $LOCAL_OVERLAY ($worker_count workers + head)"
else
    echo "[distribute_to_nodes] aurora_src -> $LOCAL_SRC ($worker_count workers + head); overlay disabled"
fi
