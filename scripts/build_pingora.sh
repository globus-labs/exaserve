#!/bin/bash
# Install rustup (if missing) and build the minimal pingora_lb binary.
#
# Aurora doesn't ship rustc/cargo. We use rustup to install the toolchain
# under $HOME/.cargo so the build is fully user-local.
#
# Usage: bash scripts/build_pingora.sh
#
# The binary is installed at $HOME/bin/pingora_lb. The launcher
# (launch_cluster.sh) adds $HOME/bin to PATH automatically.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CRATE_DIR="$REPO_ROOT/scripts/pingora_lb"
BIN_DIR="${PINGORA_BIN_DIR:-$HOME/bin}"
CARGO_HOME="${CARGO_HOME:-$HOME/.cargo}"
RUSTUP_HOME="${RUSTUP_HOME:-$HOME/.rustup}"

# Aurora login nodes need the ALCF proxy for outbound HTTPS (rustup + crates.io).
# Use the existing proxy env if available; this matches env_aurora's setup.
if [ -z "${HTTPS_PROXY:-}" ] && [ -z "${https_proxy:-}" ]; then
    echo "[build_pingora] WARNING: HTTPS_PROXY not set. If this is an Aurora login node," \
         "you likely need 'source ~/script/env_aurora' first." >&2
fi

if ! command -v cargo >/dev/null 2>&1; then
    if [ -x "$CARGO_HOME/bin/cargo" ]; then
        export PATH="$CARGO_HOME/bin:$PATH"
    else
        echo "[build_pingora] Installing rustup under $CARGO_HOME"
        export CARGO_HOME RUSTUP_HOME
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
            | sh -s -- --default-toolchain stable --profile minimal -y
        export PATH="$CARGO_HOME/bin:$PATH"
    fi
fi

cargo --version
rustc --version

echo "[build_pingora] Building $CRATE_DIR (release)"
cd "$CRATE_DIR"
cargo build --release

mkdir -p "$BIN_DIR"
ln -sf "$CRATE_DIR/target/release/pingora_lb" "$BIN_DIR/pingora_lb"

echo "[build_pingora] Installed: $BIN_DIR/pingora_lb -> $CRATE_DIR/target/release/pingora_lb"
"$BIN_DIR/pingora_lb" --version 2>/dev/null || echo "[build_pingora] (no --version output; binary present)"
echo "[build_pingora] Add \$HOME/bin to PATH (launch_cluster.sh does this automatically)."
