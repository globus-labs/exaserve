#!/bin/bash
# Per-node script: build /tmp/aurora_overlay/ray symlink farm.
#
# Called from distribute_to_nodes.sh as the second step (after bcast'ing
# the patched files into /tmp/overlay_patches/). Each PBS-allocated rank
# runs one copy via `mpiexec -n N -ppn 1`, so there is no ssh fan-out.
#
# Required env:
#   AURORA_OVERLAY_PATCHES_DIR  /tmp/overlay_patches (the bcast'd subset)
#   PYTHON_EXEC                 Aurora frameworks python (used to locate the
#                               system Ray package via `import ray`)
#
# Result:
#   /tmp/aurora_overlay/ray/...
#       Symlink farm pointing at the system Ray install, with five patched
#       files in ray/serve/_private/ replaced by copies from the bcast'd
#       /tmp/overlay_patches/serve/_private/.

set -euo pipefail

PATCHES_DIR="${AURORA_OVERLAY_PATCHES_DIR:-/tmp/overlay_patches}"
PYTHON_EXEC="${PYTHON_EXEC:?PYTHON_EXEC must be set}"

if [ ! -d "$PATCHES_DIR/serve/_private" ]; then
    echo "[setup_overlay $(hostname -s)] ERROR: $PATCHES_DIR/serve/_private not found"
    exit 1
fi

SYSRAY=$("$PYTHON_EXEC" -c 'import ray, os; print(os.path.dirname(ray.__file__))')
if [ -z "$SYSRAY" ] || [ ! -d "$SYSRAY/serve/_private" ]; then
    echo "[setup_overlay $(hostname -s)] ERROR: cannot resolve system ray dir ($SYSRAY)"
    exit 1
fi

LOCAL_OVERLAY="/tmp/aurora_overlay"
rm -rf "$LOCAL_OVERLAY"
mkdir -p "$LOCAL_OVERLAY/ray/serve/_private"

# Copy patched files list. Source of truth = whatever is in the bcast dir
# under serve/_private/. This keeps the overlay layer thin: just patches,
# everything else falls through to system Ray.
PATCHED_FILES=()
for f in "$PATCHES_DIR/serve/_private"/*.py; do
    PATCHED_FILES+=("$(basename "$f")")
done

# Top of ray/ : everything except serve/ is symlinked through.
for f in "$SYSRAY"/*; do
    n="$(basename "$f")"
    [ "$n" = serve ] && continue
    ln -s "$f" "$LOCAL_OVERLAY/ray/$n" 2>/dev/null
done

# ray/serve/ : everything except _private/ is symlinked through.
for f in "$SYSRAY/serve"/*; do
    n="$(basename "$f")"
    [ "$n" = _private ] && continue
    ln -s "$f" "$LOCAL_OVERLAY/ray/serve/$n" 2>/dev/null
done

# ray/serve/_private/ : unpatched files are symlinked, patched files are
# copied from /tmp/overlay_patches/.
for f in "$SYSRAY/serve/_private"/*; do
    n="$(basename "$f")"
    [ "$n" = __pycache__ ] && continue
    skip=0
    for p in "${PATCHED_FILES[@]}"; do
        if [ "$n" = "$p" ]; then skip=1; break; fi
    done
    if [ "$skip" -eq 1 ]; then continue; fi
    ln -s "$f" "$LOCAL_OVERLAY/ray/serve/_private/$n" 2>/dev/null
done
for p in "${PATCHED_FILES[@]}"; do
    cp "$PATCHES_DIR/serve/_private/$p" "$LOCAL_OVERLAY/ray/serve/_private/$p"
done

# Sanity: at least the two we care most about must exist.
test -f "$LOCAL_OVERLAY/ray/serve/_private/constants.py"
test -f "$LOCAL_OVERLAY/ray/serve/_private/deployment_state.py"

echo "[setup_overlay $(hostname -s)] overlay built at $LOCAL_OVERLAY (${#PATCHED_FILES[@]} patched files)"
