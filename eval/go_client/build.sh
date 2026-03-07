#!/bin/bash
# Build the go_dispatch binary.
# Auto-loads the Go module on Aurora/HPC systems if Go is not in PATH.
# Usage: ./build.sh
#
# Outputs: eval/go_client/bin/go_dispatch (Linux/amd64 static binary)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINARY="$SCRIPT_DIR/bin/go_dispatch"
mkdir -p "$SCRIPT_DIR/bin"

# Always rebuild to ensure binary matches latest source

# Ensure Go is available
if ! command -v go &>/dev/null; then
    # Try module system (Aurora / Argonne HPC)
    if command -v module &>/dev/null; then
        # Source module init if needed
        if [ -f /etc/profile.d/modules.sh ]; then
            source /etc/profile.d/modules.sh
        elif [ -f /usr/share/lmod/lmod/init/bash ]; then
            source /usr/share/lmod/lmod/init/bash
        fi
        module load go 2>/dev/null || module load go/1.23.9 2>/dev/null || true
    fi
    # Fall back to known spack install paths
    for candidate in \
        /opt/aurora/25.190.0/spack/unified/0.10.1/install/linux-sles15-x86_64/gcc-13.3.0/go-1.23.9-qpofi5g/go/bin \
        /opt/aurora/24.347.0/spack/unified/0.9.2/install/linux-sles15-x86_64/gcc-13.3.0/go-1.23.5-s45w7fy/go/bin \
        /opt/aurora/24.180.3/spack/unified/0.8.0/install/linux-sles15-x86_64/gcc-12.2.0/go-1.22.2-a2h7cly/bin \
        "$HOME/go/bin"; do
        if [ -x "$candidate/go" ]; then
            export PATH="$candidate:$PATH"
            break
        fi
    done
fi

if ! command -v go &>/dev/null; then
    echo "!!! ERROR: Go not found. Install Go >= 1.21 or run: module load go" >&2
    exit 1
fi

echo "[go_client] Using $(go version)"
echo "[go_client] Building go_dispatch..."
cd "$SCRIPT_DIR"
go build -trimpath -o "$BINARY" .
echo "[go_client] Built: $BINARY"
