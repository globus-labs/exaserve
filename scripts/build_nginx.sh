#!/bin/bash
# Build NGINX from source and install to ~/bin/nginx.
#
# Aurora login nodes don't ship nginx, so users of the nginx proxy
# mode need to build it once. The validated runtime PATH must include $HOME/bin.
#
# Usage: NGINX_SOURCE_SHA256=<sha256> bash scripts/build_nginx.sh [version]
#   version defaults to 1.27.3 (mainline at time of writing).

set -euo pipefail

VERSION="${1:-1.27.3}"
EXPECTED_SHA256="${NGINX_SOURCE_SHA256:-}"
PREFIX="${NGINX_PREFIX:-$HOME/.local/nginx-$VERSION}"
BIN_DIR="${NGINX_BIN_DIR:-$HOME/bin}"

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "[build_nginx] Invalid version: $VERSION" >&2
    exit 2
fi
if [[ ! "$EXPECTED_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
    echo "[build_nginx] NGINX_SOURCE_SHA256 must be the audited lowercase SHA-256." >&2
    exit 2
fi

TARBALL="nginx-$VERSION.tar.gz"
URL="https://nginx.org/download/$TARBALL"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
cd "$WORK"

echo "[build_nginx] Downloading $URL"
curl --proto '=https' --tlsv1.2 -fsSL -o "$TARBALL" "$URL"
ACTUAL_SHA256="$(sha256sum "$TARBALL" | awk '{print $1}')"
if [ "$ACTUAL_SHA256" != "$EXPECTED_SHA256" ]; then
    echo "[build_nginx] Source SHA-256 mismatch: $ACTUAL_SHA256" >&2
    exit 1
fi
tar --extract --gzip --file "$TARBALL" --no-same-owner --no-same-permissions
cd "nginx-$VERSION"

# Minimal build: HTTP only (no mail/stream needed for our load-balancer use),
# system OpenSSL, system PCRE, no module surface beyond core HTTP load
# balancing. Disable mail, gzip is included by default which is fine.
echo "[build_nginx] Configuring (PREFIX=$PREFIX)"
./configure \
    --prefix="$PREFIX" \
    --sbin-path="$PREFIX/sbin/nginx" \
    --conf-path="$PREFIX/conf/nginx.conf" \
    --error-log-path="$PREFIX/logs/error.log" \
    --http-log-path="$PREFIX/logs/access.log" \
    --pid-path="$PREFIX/logs/nginx.pid" \
    --lock-path="$PREFIX/logs/nginx.lock" \
    --with-http_stub_status_module \
    --with-pcre \
    --without-mail_pop3_module \
    --without-mail_imap_module \
    --without-mail_smtp_module \
    --without-http_uwsgi_module \
    --without-http_scgi_module \
    --without-http_fastcgi_module

echo "[build_nginx] Compiling"
make -j"$(nproc)"
make install

mkdir -p "$BIN_DIR"
ln -sf "$PREFIX/sbin/nginx" "$BIN_DIR/nginx"

echo "[build_nginx] Installed: $BIN_DIR/nginx -> $PREFIX/sbin/nginx"
"$BIN_DIR/nginx" -v 2>&1 | head -1
echo "[build_nginx] Add \$HOME/bin to the validated runtime PATH."
