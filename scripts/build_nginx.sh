#!/bin/bash
# Build NGINX from source and install to ~/bin/nginx.
#
# Aurora login nodes don't ship nginx, so users of the nginx proxy
# mode need to build it once. The launcher adds $HOME/bin to PATH.
#
# Usage: bash scripts/build_nginx.sh [version]
#   version defaults to 1.27.3 (mainline at time of writing).

set -euo pipefail

VERSION="${1:-1.27.3}"
PREFIX="${NGINX_PREFIX:-$HOME/.local/nginx-$VERSION}"
BIN_DIR="${NGINX_BIN_DIR:-$HOME/bin}"

TARBALL="nginx-$VERSION.tar.gz"
URL="https://nginx.org/download/$TARBALL"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
cd "$WORK"

echo "[build_nginx] Downloading $URL"
curl -fsSL -o "$TARBALL" "$URL"
tar xf "$TARBALL"
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
echo "[build_nginx] Add \$HOME/bin to PATH (launch_cluster.sh does this automatically)."
