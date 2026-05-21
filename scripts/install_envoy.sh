#!/bin/bash
# Download a prebuilt Envoy static binary and install to ~/bin/envoy.
#
# Envoy is normally built with Bazel which is impractical on Aurora. We use
# the static x86_64 Linux release from envoyproxy/envoy on GitHub.
#
# Usage: bash scripts/install_envoy.sh [version]
#   version defaults to v1.32.3.

set -euo pipefail

VERSION="${1:-v1.32.3}"
ARCH="x86_64"
PREFIX="${ENVOY_PREFIX:-$HOME/.local/envoy-$VERSION}"
BIN_DIR="${ENVOY_BIN_DIR:-$HOME/bin}"

# Upstream releases ship a tar.xz with a single envoy binary inside.
ASSET="envoy-${VERSION#v}-linux-${ARCH}"
URL="https://github.com/envoyproxy/envoy/releases/download/${VERSION}/${ASSET}"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
cd "$WORK"

echo "[install_envoy] Downloading $URL"
curl -fsSL -L -o envoy "$URL"
chmod +x envoy

mkdir -p "$PREFIX/bin" "$BIN_DIR"
mv envoy "$PREFIX/bin/envoy"
ln -sf "$PREFIX/bin/envoy" "$BIN_DIR/envoy"

echo "[install_envoy] Installed: $BIN_DIR/envoy -> $PREFIX/bin/envoy"
"$BIN_DIR/envoy" --version | head -1
echo "[install_envoy] Add \$HOME/bin to PATH (launch_cluster.sh does this automatically)."
