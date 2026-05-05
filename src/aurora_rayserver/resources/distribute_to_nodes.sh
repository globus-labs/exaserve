#!/bin/bash
# Distribute aurora_rayserver package code and, optionally, a Ray Serve overlay
# to every PBS node's local /tmp. Always runs.
#
# Outputs:
#   /tmp/aurora_src/aurora_rayserver
#       Copy of the installed aurora_rayserver package, excluding the large
#       Ray overlay subtree. /tmp/aurora_src is prepended to PYTHONPATH so
#       runtime code is loaded from node-local tmpfs instead of Lustre.
#
#   /tmp/aurora_overlay/ray/...
#       Symlink farm pointing at the system Ray install, with patched files
#       replaced from aurora_rayserver/patches/ray_serve_overlay/. Only created
#       when AURORA_INSTRUMENTATION=1.
#
# Required env:
#   AURORA_RAYSERVER_PACKAGE_ROOT    absolute path to aurora_rayserver package
#   PYTHON_EXEC                      Aurora frameworks python3
#   UNIQUE_NODES_FILE                one PBS hostname per line
#   HOSTNAME_SHORT                   short hostname of the head node
#
# Optional env:
#   AURORA_INSTRUMENTATION           1 to build overlay, 0 to skip
set -e

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
SYSRAY="$(dirname "$(dirname "$PYTHON_EXEC")")/lib/python3.12/site-packages/ray"

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
if [ "$INSTRUMENTATION" = "1" ] && [ ! -d "$SYSRAY/serve/_private" ]; then
    echo "[distribute_to_nodes] ERROR: system Ray not found at $SYSRAY"
    exit 1
fi

LOCAL_SRC="/tmp/aurora_src"
LOCAL_OVERLAY="/tmp/aurora_overlay"

# List of patched filenames, computed once on the head and used on every node.
PATCHED_FILES=""
if [ "$INSTRUMENTATION" = "1" ]; then
    PATCHED_FILES=$(cd "$OVERLAY_SRC/serve/_private" && ls *.py 2>/dev/null | tr '\n' ' ')
fi

# Stage the per-node setup script on shared storage so SSHed workers can read it.
_STAGED_SCRIPT=$(mktemp "$HOME/.aurora_distribute_XXXX.sh")
trap 'rm -f "$_STAGED_SCRIPT"' EXIT

cat > "$_STAGED_SCRIPT" <<NODEEOF
#!/bin/bash
set -e

PACKAGE_ROOT="$PACKAGE_ROOT"
OVERLAY_SRC="$OVERLAY_SRC"
SYSRAY="$SYSRAY"
LOCAL_SRC="$LOCAL_SRC"
LOCAL_OVERLAY="$LOCAL_OVERLAY"
INSTRUMENTATION="$INSTRUMENTATION"
PATCHED_FILES="$PATCHED_FILES"

rm -rf "\$LOCAL_SRC"
mkdir -p "\$LOCAL_SRC/aurora_rayserver"
if command -v rsync >/dev/null 2>&1; then
    rsync -a --exclude='patches/ray_serve_overlay/' \\
        --exclude='__pycache__/' --exclude='*.pyc' \\
        "\$PACKAGE_ROOT/" "\$LOCAL_SRC/aurora_rayserver/"
else
    cp -a "\$PACKAGE_ROOT/." "\$LOCAL_SRC/aurora_rayserver/"
    rm -rf "\$LOCAL_SRC/aurora_rayserver/patches/ray_serve_overlay"
    find "\$LOCAL_SRC" -name __pycache__ -type d -prune -exec rm -rf {} +
    find "\$LOCAL_SRC" -name '*.pyc' -type f -delete
fi

if [ "\$INSTRUMENTATION" = "1" ]; then
    rm -rf "\$LOCAL_OVERLAY"
    mkdir -p "\$LOCAL_OVERLAY/ray/serve/_private"
    for f in "\$SYSRAY"/*; do
        n=\$(basename "\$f")
        [ "\$n" = serve ] && continue
        ln -s "\$f" "\$LOCAL_OVERLAY/ray/\$n" 2>/dev/null
    done
    for f in "\$SYSRAY/serve"/*; do
        n=\$(basename "\$f")
        [ "\$n" = _private ] && continue
        ln -s "\$f" "\$LOCAL_OVERLAY/ray/serve/\$n" 2>/dev/null
    done
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
    for p in \$PATCHED_FILES; do
        cp "\$OVERLAY_SRC/serve/_private/\$p" "\$LOCAL_OVERLAY/ray/serve/_private/\$p"
    done
fi

test -f "\$LOCAL_SRC/aurora_rayserver/driver.py"
test -f "\$LOCAL_SRC/aurora_rayserver/server.py"
test -f "\$LOCAL_SRC/aurora_rayserver/ray_start.py"
if [ "\$INSTRUMENTATION" = "1" ]; then
    test -f "\$LOCAL_OVERLAY/ray/serve/_private/constants.py"
    test -f "\$LOCAL_OVERLAY/ray/serve/_private/deployment_state.py"
fi
NODEEOF

chmod +x "$_STAGED_SCRIPT"

# Run on the head first so failures surface immediately.
bash "$_STAGED_SCRIPT"

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
