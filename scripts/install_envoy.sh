#!/bin/bash
# Download a prebuilt Envoy static binary and install to ~/bin/envoy.
#
# Envoy is normally built with Bazel which is impractical on Aurora. We use
# the static x86_64 Linux release from envoyproxy/envoy on GitHub.
#
# Usage: ENVOY_BINARY_SHA256=<sha256> bash scripts/install_envoy.sh [version]
#   version defaults to v1.32.3.

set -euo pipefail

VERSION="${1:-v1.32.3}"
EXPECTED_SHA256="${ENVOY_BINARY_SHA256:-}"
ARCH="x86_64"
PREFIX="${ENVOY_PREFIX:-$HOME/.local/envoy-$VERSION}"
BIN_DIR="${ENVOY_BIN_DIR:-$HOME/bin}"

if [[ ! "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "[install_envoy] Invalid version: $VERSION" >&2
    exit 2
fi
if [[ ! "$EXPECTED_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
    echo "[install_envoy] ENVOY_BINARY_SHA256 must be the audited lowercase SHA-256." >&2
    exit 2
fi

# Upstream releases ship a tar.xz with a single envoy binary inside.
ASSET="envoy-${VERSION#v}-linux-${ARCH}"
URL="https://github.com/envoyproxy/envoy/releases/download/${VERSION}/${ASSET}"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
cd "$WORK"

echo "[install_envoy] Downloading $URL"
curl --proto '=https' --tlsv1.2 -fsSL -L -o envoy "$URL"
ACTUAL_SHA256="$(sha256sum envoy | awk '{print $1}')"
if [ "$ACTUAL_SHA256" != "$EXPECTED_SHA256" ]; then
    echo "[install_envoy] Binary SHA-256 mismatch: $ACTUAL_SHA256" >&2
    exit 1
fi
chmod +x envoy

mkdir -p "$PREFIX/bin" "$BIN_DIR"
mv envoy "$PREFIX/bin/envoy"
ln -sf "$PREFIX/bin/envoy" "$BIN_DIR/envoy"

echo "[install_envoy] Installed: $BIN_DIR/envoy -> $PREFIX/bin/envoy"
"$BIN_DIR/envoy" --version | head -1
echo "[install_envoy] Add \$HOME/bin to the validated runtime PATH."
