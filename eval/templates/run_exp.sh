#!/bin/bash
set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 <path_to_run.yaml> [env_script]" >&2
    exit 1
fi

RUN_YAML="$1"
ENV_SCRIPT="${2:-$HOME/script/env_aurora}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$PROJECT_ROOT"
source "$ENV_SCRIPT"
python -m eval.cli run execute "$RUN_YAML"
