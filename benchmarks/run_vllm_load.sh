#!/bin/bash
# Run inside an active PBS allocation (or via subjob).
#   - stages Llama-3-8B-Instruct to /tmp (DDR-backed tmpfs)
#   - launches K parallel vLLM AsyncLLMEngine instances (one per PVC)
#   - records per-replica timing JSONs
# Usage:
#   bash run_vllm_load.sh <KLIST> <OUT_DIR> [--profile-k1]

set -eo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
cd "$REPO_DIR"

KLIST="${1:-1 2 4 6}"
OUT_DIR="${2:-$REPO_DIR/run_logs/vllm_load_$(date +%Y%m%dT%H%M%S)}"
PROFILE_K1=""
for arg in "$@"; do
    [[ "$arg" == "--profile-k1" ]] && PROFILE_K1="--profile"
done

LUSTRE_MODEL="/lus/flare/projects/AuroraGPT/wenyiw/models/meta-llama--Meta-Llama-3-8B-Instruct"
TMP_MODEL="/tmp/llama-3-8b"

mkdir -p "$OUT_DIR"
echo "[$(date +%H:%M:%S)] OUT_DIR=$OUT_DIR  HOST=$(hostname)"

# Stage if not present (idempotent).
if [[ ! -f "$TMP_MODEL/config.json" ]]; then
    echo "[$(date +%H:%M:%S)] Staging model: $LUSTRE_MODEL -> $TMP_MODEL"
    t0=$EPOCHREALTIME
    mkdir -p "$TMP_MODEL"
    # Use rsync for fast bulk copy with progress.
    cp -r "$LUSTRE_MODEL"/. "$TMP_MODEL"/
    t1=$EPOCHREALTIME
    echo "[$(date +%H:%M:%S)] Stage done in $(awk "BEGIN{printf \"%.1f\", $t1 - $t0}")s. Size:"
    du -sh "$TMP_MODEL"
else
    echo "[$(date +%H:%M:%S)] Model already staged at $TMP_MODEL"
fi

for K in $KLIST; do
    K_OUT="$OUT_DIR/k${K}"
    mkdir -p "$K_OUT"
    echo "[$(date +%H:%M:%S)] === K=$K replicas ==="
    PROFILE_FLAG=""
    [[ -n "$PROFILE_K1" && "$K" == "1" ]] && PROFILE_FLAG="--profile"
    python -m benchmarks.vllm_load launcher \
        --model-path "$TMP_MODEL" \
        --replicas "$K" \
        --out-dir "$K_OUT" \
        $PROFILE_FLAG \
        2>&1 | tee "$K_OUT/launcher.log"
done

echo "[$(date +%H:%M:%S)] All done. Results under $OUT_DIR"
