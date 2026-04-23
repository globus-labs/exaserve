#!/bin/bash
# Build HAProxy from source and install to ~/bin/haproxy.
#
# Aurora login nodes don't ship haproxy, so users of the haproxy proxy
# mode need to build it once. The launcher adds $HOME/bin to PATH.
#
# Usage: bash scripts/build_haproxy.sh [version]
#   version defaults to 3.1.6 (what this project was tested against).

set -euo pipefail

VERSION="${1:-3.1.6}"
PREFIX="${HAPROXY_PREFIX:-$HOME/.local/haproxy-$VERSION}"
BIN_DIR="${HAPROXY_BIN_DIR:-$HOME/bin}"

TARBALL="haproxy-$VERSION.tar.gz"
URL="https://www.haproxy.org/download/${VERSION%.*}/src/$TARBALL"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
cd "$WORK"

echo "[build_haproxy] Downloading $URL"
curl -fsSL -o "$TARBALL" "$URL"
tar xf "$TARBALL"
cd "haproxy-$VERSION"

# TARGET=linux-glibc covers Aurora login/compute nodes. USE_OPENSSL=1
# pulls in TLS support from the system libs.
echo "[build_haproxy] Compiling (TARGET=linux-glibc, PREFIX=$PREFIX)"
make -j"$(nproc)" TARGET=linux-glibc USE_OPENSSL=1 PREFIX="$PREFIX"
make install PREFIX="$PREFIX"

mkdir -p "$BIN_DIR"
ln -sf "$PREFIX/sbin/haproxy" "$BIN_DIR/haproxy"

echo "[build_haproxy] Installed: $BIN_DIR/haproxy -> $PREFIX/sbin/haproxy"
"$BIN_DIR/haproxy" -v | head -1
echo "[build_haproxy] Add \$HOME/bin to PATH (scripts/launch_cluster.sh does this automatically)."
