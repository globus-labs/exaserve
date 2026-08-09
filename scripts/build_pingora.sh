#!/bin/bash
# Build the validation-only minimal pingora_lb binary with an existing toolchain.
#
# This helper never downloads or executes a toolchain installer. Provision an
# audited Rust toolchain separately and commit Cargo.lock before qualification.
#
# Usage: bash scripts/build_pingora.sh
#
# The binary is installed at $HOME/bin/pingora_lb. Add that directory to the
# validated site environment used for Pingora runs.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CRATE_DIR="$REPO_ROOT/scripts/pingora_lb"
BIN_DIR="${PINGORA_BIN_DIR:-$HOME/bin}"
if ! command -v cargo >/dev/null 2>&1 || ! command -v rustc >/dev/null 2>&1; then
    echo "[build_pingora] An audited cargo/rustc toolchain must already be on PATH." >&2
    exit 2
fi
if [ ! -f "$CRATE_DIR/Cargo.lock" ]; then
    echo "[build_pingora] Cargo.lock is missing; refusing an unpinned dependency build." >&2
    exit 2
fi

cargo --version
rustc --version

echo "[build_pingora] Building $CRATE_DIR (release)"
cd "$CRATE_DIR"
cargo build --release --locked

mkdir -p "$BIN_DIR"
ln -sf "$CRATE_DIR/target/release/pingora_lb" "$BIN_DIR/pingora_lb"

echo "[build_pingora] Installed: $BIN_DIR/pingora_lb -> $CRATE_DIR/target/release/pingora_lb"
"$BIN_DIR/pingora_lb" --version 2>/dev/null || echo "[build_pingora] (no --version output; binary present)"
echo "[build_pingora] Add \$HOME/bin to the validated runtime PATH."
