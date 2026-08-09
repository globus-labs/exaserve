#!/bin/bash
# Build HAProxy from source and install to ~/bin/haproxy.
#
# Aurora login nodes don't ship haproxy, so users of the haproxy proxy
# mode need to build it once. The validated runtime PATH must include $HOME/bin.
#
# Usage: HAPROXY_SOURCE_SHA256=<sha256> bash scripts/build_haproxy.sh [version]
#   version defaults to 3.1.6 (what this project was tested against).

set -euo pipefail

VERSION="${1:-3.1.6}"
EXPECTED_SHA256="${HAPROXY_SOURCE_SHA256:-}"
PREFIX="${HAPROXY_PREFIX:-$HOME/.local/haproxy-$VERSION}"
BIN_DIR="${HAPROXY_BIN_DIR:-$HOME/bin}"

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "[build_haproxy] Invalid version: $VERSION" >&2
    exit 2
fi
if [[ ! "$EXPECTED_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
    echo "[build_haproxy] HAPROXY_SOURCE_SHA256 must be the audited lowercase SHA-256." >&2
    exit 2
fi

TARBALL="haproxy-$VERSION.tar.gz"
URL="https://www.haproxy.org/download/${VERSION%.*}/src/$TARBALL"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
cd "$WORK"

echo "[build_haproxy] Downloading $URL"
curl --proto '=https' --tlsv1.2 -fsSL -o "$TARBALL" "$URL"
ACTUAL_SHA256="$(sha256sum "$TARBALL" | awk '{print $1}')"
if [ "$ACTUAL_SHA256" != "$EXPECTED_SHA256" ]; then
    echo "[build_haproxy] Source SHA-256 mismatch: $ACTUAL_SHA256" >&2
    exit 1
fi
tar --extract --gzip --file "$TARBALL" --no-same-owner --no-same-permissions
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
echo "[build_haproxy] Add \$HOME/bin to the validated runtime PATH."
