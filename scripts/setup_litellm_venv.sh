#!/bin/bash
# Create a standalone venv for the LiteLLM proxy.
#
# LiteLLM pins FastAPI / pydantic / openai versions that conflict with
# the Aurora frameworks stack (Ray + vLLM), so it needs its own venv.
# The core cluster launcher keeps using the frameworks python; only
# the proxy subprocess uses this venv.
#
# Usage:
#   bash scripts/setup_litellm_venv.sh [venv_path]
#
# Default venv path: $HOME/litellm_venv
# After setup, point proxy_config.python_path at <venv_path>/bin/python3
# in your config.yaml (see examples/config.litellm.yaml).

set -euo pipefail

VENV_PATH="${1:-$HOME/litellm_venv}"
LITELLM_VERSION="${LITELLM_VERSION:-1.52.0}"

if [ -d "$VENV_PATH" ]; then
    echo "[setup_litellm_venv] $VENV_PATH already exists; upgrading in place."
else
    echo "[setup_litellm_venv] Creating venv at $VENV_PATH"
    # Use system python3 (not frameworks) so litellm doesn't fight the
    # heavy Ray/vLLM stack on sys.path.
    python3 -m venv "$VENV_PATH"
fi

# shellcheck disable=SC1091
source "$VENV_PATH/bin/activate"

pip install --upgrade pip wheel
pip install "litellm[proxy]==$LITELLM_VERSION"

echo ""
echo "[setup_litellm_venv] Installed: $("$VENV_PATH/bin/litellm" --version 2>&1 | head -1)"
echo "[setup_litellm_venv] Use this in config.yaml:"
echo "  proxy_config:"
echo "    type: litellm"
echo "    python_path: $VENV_PATH/bin/python3"
