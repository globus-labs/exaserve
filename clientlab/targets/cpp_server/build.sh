#!/bin/bash
# Build the synthetic_server binary.
# Usage: ./build.sh
#
# Outputs: clientlab/targets/cpp_server/bin/synthetic_server

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINARY="$SCRIPT_DIR/bin/synthetic_server"
mkdir -p "$SCRIPT_DIR/bin"

CXX="${CXX:-g++}"
CXXFLAGS="-std=c++20 -O2 -Wall -Wextra -pthread"

SOURCES=(
    main.cpp
    server.cpp
    handler.cpp
    faults.cpp
    metrics.cpp
    config.cpp
    vendor/yyjson.c
)

echo "[cpp_server] Using $($CXX --version | head -1)"
echo "[cpp_server] Building synthetic_server..."
cd "$SCRIPT_DIR"
$CXX $CXXFLAGS -o "$BINARY" "${SOURCES[@]}"
echo "[cpp_server] Built: $BINARY"
