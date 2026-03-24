#!/bin/bash
# ==============================================================================
# MULTI-STUB SERVER SCALING BENCHMARK
# ==============================================================================
# Measures how max sustainable RPS scales with the number of independent stub
# server groups.  Each group runs on its own port with SO_REUSEPORT workers.
# The replay_client round-robins across all groups.
#
# USAGE:
#   bash benchmarks/bench_multi_stub.sh [OPTIONS]
#
# EXAMPLES:
#   # Quick smoke test
#   bash benchmarks/bench_multi_stub.sh --num-stubs-list 1,2 --stub-workers 4
#
#   # Full scaling sweep
#   bash benchmarks/bench_multi_stub.sh \
#       --num-stubs-list 1,2,4,8 --stub-workers 32 \
#       --num-go-procs 2 --num-go-workers 4 --go-concurrency 4000
#
# OPTIONS:
#   --num-stubs-list   Comma-separated list of N values to sweep (default: 1,2,4,8)
#   --stub-workers     SO_REUSEPORT workers per stub group (default: 32)
#   --stub-port-base   First port number (default: 8000)
#   --stub-latency     Stub server latency in ms (default: 0)
#   --python           Python interpreter (default: python3)
#   --output-dir       Results directory (default: auto-timestamped)
#
#   All other flags are forwarded to bench_client.py --find-max-rps:
#   --num-go-workers, --num-go-procs, --go-concurrency, --sum-only,
#   --probe-duration, --rps-start, --max-rps-ceiling, --precision,
#   --duration, --point-timeout, --no-port-monitor, --sweep-workers,
#   --sweep-payloads, --payload
# ==============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
NUM_STUBS_LIST="1,2,4,8"
STUB_WORKERS=32
STUB_PORT_BASE=8000
STUB_LATENCY=0
PYTHON="python3"
OUTPUT_DIR=""

# bench_client.py find-max-rps defaults
NUM_GO_WORKERS=2
NUM_GO_PROCS=8
GO_CONCURRENCY=40
SUM_ONLY=true
PROBE_DURATION=3
RPS_START=64000
MAX_RPS_CEILING=32000000
PRECISION=0.1
DURATION=20
POINT_TIMEOUT=240
NO_PORT_MONITOR=false
PAYLOAD="medium"
SWEEP_WORKERS_VAL=""
SWEEP_PAYLOADS_VAL=""
CPUPROFILE=false

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --num-stubs-list|--num-stub-list)   NUM_STUBS_LIST="$2";    shift 2 ;;
        --stub-workers)     STUB_WORKERS="$2";      shift 2 ;;
        --stub-port-base)   STUB_PORT_BASE="$2";    shift 2 ;;
        --stub-latency)     STUB_LATENCY="$2";      shift 2 ;;
        --python)           PYTHON="$2";            shift 2 ;;
        --output-dir)       OUTPUT_DIR="$2";        shift 2 ;;
        --num-go-workers)   NUM_GO_WORKERS="$2";    shift 2 ;;
        --num-go-procs)     NUM_GO_PROCS="$2";      shift 2 ;;
        --go-concurrency)   GO_CONCURRENCY="$2";    shift 2 ;;
        --sum-only)         SUM_ONLY=true;          shift   ;;
        --probe-duration)   PROBE_DURATION="$2";    shift 2 ;;
        --rps-start)        RPS_START="$2";         shift 2 ;;
        --max-rps-ceiling)  MAX_RPS_CEILING="$2";   shift 2 ;;
        --precision)        PRECISION="$2";         shift 2 ;;
        --duration)         DURATION="$2";          shift 2 ;;
        --point-timeout)    POINT_TIMEOUT="$2";     shift 2 ;;
        --no-port-monitor)  NO_PORT_MONITOR=true;   shift   ;;
        --payload)          PAYLOAD="$2";           shift 2 ;;
        --sweep-workers)    SWEEP_WORKERS_VAL="$2"; shift 2 ;;
        --sweep-payloads)   SWEEP_PAYLOADS_VAL="$2"; shift 2 ;;
        --cpuprofile)       CPUPROFILE=true;         shift   ;;
        *)
            echo "!!! ERROR: Unknown argument '$1'"
            exit 1
            ;;
    esac
done

# ---------------------------------------------------------------------------
# Derived paths
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"

resolve_site_config_field() {
    local field="$1"
    PYTHONPATH="$PROJECT_ROOT/src${PYTHONPATH:+:$PYTHONPATH}" python3 -S -m site_config get "$field" 2>/dev/null || true
}

SITE_BENCH_ROOT="$(resolve_site_config_field bench_results_dir)"

if [ -z "$OUTPUT_DIR" ]; then
    OUTPUT_BASE="${SITE_BENCH_ROOT:-$HOME/agpt/data/bench_results}"
    OUTPUT_DIR="${OUTPUT_BASE}/multi_stub_${TIMESTAMP}"
fi
mkdir -p "$OUTPUT_DIR"

STUB_SERVER="$SCRIPT_DIR/stub_server.py"
BENCH_CLIENT="$SCRIPT_DIR/bench_client.py"
LOG_FILE="$OUTPUT_DIR/multi_stub_${TIMESTAMP}.log"
SUMMARY_FILE="$OUTPUT_DIR/multi_stub_summary_${TIMESTAMP}.json"

# Validate scripts exist
for f in "$STUB_SERVER" "$BENCH_CLIENT"; do
    if [ ! -f "$f" ]; then
        echo "!!! ERROR: Script not found: $f"
        exit 1
    fi
done

# ---------------------------------------------------------------------------
# Cleanup handler
# ---------------------------------------------------------------------------
ALL_STUB_PIDS=""
CLEANUP_DONE=false

cleanup() {
    if [ "$CLEANUP_DONE" = true ]; then return; fi
    CLEANUP_DONE=true
    echo ""
    echo ">>> [MULTI-STUB] Cleaning up all stub servers..."
    if [ -n "$ALL_STUB_PIDS" ]; then
        for pid in $ALL_STUB_PIDS; do
            kill -TERM "$pid" 2>/dev/null || true
        done
    fi
    echo ">>> [MULTI-STUB] Cleanup complete."
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Helper: start N stub groups on sequential ports
# ---------------------------------------------------------------------------
start_stub_groups() {
    local n_groups="$1"
    local port_base="$2"
    local workers="$3"

    ALL_STUB_PIDS=""
    for i in $(seq 0 $((n_groups - 1))); do
        local port=$((port_base + i))
        for w in $(seq 1 "$workers"); do
            "$PYTHON" "$STUB_SERVER" \
                --port "$port" \
                --host 127.0.0.1 \
                --latency-ms "$STUB_LATENCY" \
                --response-tokens 10 \
                --reuse-port \
                --log-level error \
                >> "$OUTPUT_DIR/stub_${port}.log" 2>&1 &
            ALL_STUB_PIDS="$ALL_STUB_PIDS $!"
        done
    done

    # Wait for all ports to be ready
    for i in $(seq 0 $((n_groups - 1))); do
        local port=$((port_base + i))
        local deadline=$((SECONDS + 15))
        while [ $SECONDS -lt $deadline ]; do
            if "$PYTHON" -c \
                "import socket, sys; s=socket.socket(); s.settimeout(1); \
                 sys.exit(0 if s.connect_ex(('127.0.0.1', $port))==0 else 1)" \
                 2>/dev/null; then
                break
            fi
            sleep 0.5
        done
        if [ $SECONDS -ge $deadline ]; then
            echo "!!! ERROR: Stub server on port $port did not start within 15s"
            exit 1
        fi
    done
}

# ---------------------------------------------------------------------------
# Helper: kill all stub servers
# ---------------------------------------------------------------------------
kill_stubs() {
    if [ -n "$ALL_STUB_PIDS" ]; then
        for pid in $ALL_STUB_PIDS; do
            kill -TERM "$pid" 2>/dev/null || true
        done
        # Wait briefly for processes to exit
        sleep 1
        for pid in $ALL_STUB_PIDS; do
            kill -9 "$pid" 2>/dev/null || true
        done
        ALL_STUB_PIDS=""
    fi
}

# ---------------------------------------------------------------------------
# Build base URLs string
# ---------------------------------------------------------------------------
build_urls() {
    local n_groups="$1"
    local port_base="$2"
    local urls=""
    for i in $(seq 0 $((n_groups - 1))); do
        local port=$((port_base + i))
        if [ -z "$urls" ]; then
            urls="http://0.0.0.0:$port"
        else
            urls="$urls,http://0.0.0.0:$port"
        fi
    done
    echo "$urls"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
echo "=================================================="
echo " MULTI-STUB SERVER SCALING BENCHMARK"
echo "=================================================="
echo " num_stubs_list:  $NUM_STUBS_LIST"
echo " stub_workers:    $STUB_WORKERS (SO_REUSEPORT per group)"
echo " stub_port_base:  $STUB_PORT_BASE"
echo " stub_latency:    ${STUB_LATENCY}ms"
echo " num_go_workers:  $NUM_GO_WORKERS"
echo " num_go_procs:    $NUM_GO_PROCS"
echo " go_concurrency:  $GO_CONCURRENCY"
echo " duration:        ${DURATION}s"
echo " output_dir:      $OUTPUT_DIR"
echo " python:          $PYTHON"
echo "=================================================="
echo ""

# Build common bench_client args
CLIENT_ARGS=(
    "--find-max-rps"
    "--probe-duration"  "$PROBE_DURATION"
    "--rps-start"       "$RPS_START"
    "--max-rps-ceiling" "$MAX_RPS_CEILING"
    "--precision"       "$PRECISION"
    "--duration"        "$DURATION"
    "--num-go-workers"  "$NUM_GO_WORKERS"
    "--num-go-procs"    "$NUM_GO_PROCS"
    "--go-concurrency"  "$GO_CONCURRENCY"
    "--stub-workers"    "$STUB_WORKERS"
    "--payload"         "$PAYLOAD"
    "--python"          "$PYTHON"
    "--work-dir"        "$OUTPUT_DIR/client_work"
)
if [ "$POINT_TIMEOUT" -gt 0 ] 2>/dev/null; then
    CLIENT_ARGS+=("--point-timeout" "$POINT_TIMEOUT")
fi
if [ "$SUM_ONLY" = true ]; then
    CLIENT_ARGS+=("--sum-only")
fi
if [ "$NO_PORT_MONITOR" = true ]; then
    CLIENT_ARGS+=("--no-port-monitor")
fi
if [ -n "$SWEEP_WORKERS_VAL" ]; then
    CLIENT_ARGS+=("--sweep-workers" "$SWEEP_WORKERS_VAL")
fi
if [ -n "$SWEEP_PAYLOADS_VAL" ]; then
    CLIENT_ARGS+=("--sweep-payloads" "$SWEEP_PAYLOADS_VAL")
fi
if [ "$CPUPROFILE" = true ]; then
    CLIENT_ARGS+=("--cpuprofile")
fi

# Initialize summary JSON
echo "[" > "$SUMMARY_FILE"
FIRST_RESULT=true

OVERALL_EXIT=0

for N in ${NUM_STUBS_LIST//,/ }; do
    echo ""
    echo "=================================================="
    echo " N=$N stub groups (${STUB_WORKERS} workers each)"
    echo "=================================================="

    # Start stub servers
    echo ">>> [MULTI-STUB] Starting $N stub groups on ports ${STUB_PORT_BASE}..$(( STUB_PORT_BASE + N - 1 ))..."
    start_stub_groups "$N" "$STUB_PORT_BASE" "$STUB_WORKERS"

    # Count alive stubs
    local_alive=0
    for pid in $ALL_STUB_PIDS; do
        if kill -0 "$pid" 2>/dev/null; then
            local_alive=$((local_alive + 1))
        fi
    done
    echo "[✓] $local_alive/$((N * STUB_WORKERS)) stub processes alive"

    # Build URLs
    URLS=$(build_urls "$N" "$STUB_PORT_BASE")
    echo "    URLs: $URLS"

    # Run bench_client
    RESULT_FILE="$OUTPUT_DIR/max_rps_N${N}_${TIMESTAMP}.json"
    echo ">>> [MULTI-STUB] Running find-max-rps with N=$N..."
    echo "    Args: ${CLIENT_ARGS[*]} --base-urls $URLS"

    "$PYTHON" "$BENCH_CLIENT" \
        "${CLIENT_ARGS[@]}" \
        "--base-urls"   "$URLS" \
        "--output"      "$RESULT_FILE" \
        "--stub-port"   "$STUB_PORT_BASE" \
        2>&1 | tee -a "$LOG_FILE"
    local exit_code=${PIPESTATUS[0]}

    # Kill stubs
    echo ">>> [MULTI-STUB] Stopping stub servers..."
    kill_stubs

    if [ $exit_code -ne 0 ]; then
        echo "!!! ERROR: bench_client.py exited with code $exit_code for N=$N"
        OVERALL_EXIT=$exit_code
    fi

    # Extract max_rps from result file
    MAX_RPS="null"
    if [ -f "$RESULT_FILE" ]; then
        MAX_RPS=$("$PYTHON" -c "
import json, sys
try:
    data = json.load(open('$RESULT_FILE'))
    results = data.get('results', [])
    if results:
        print(results[0].get('max_rps', 'null'))
    else:
        print('null')
except:
    print('null')
" 2>/dev/null || echo "null")
    fi

    echo "[✓] N=$N: max_rps=$MAX_RPS"

    # Append to summary JSON
    if [ "$FIRST_RESULT" = true ]; then
        FIRST_RESULT=false
    else
        echo "," >> "$SUMMARY_FILE"
    fi
    cat >> "$SUMMARY_FILE" <<EOF
  {
    "num_stub_groups": $N,
    "stub_workers_per_group": $STUB_WORKERS,
    "total_stub_processes": $((N * STUB_WORKERS)),
    "max_rps": $MAX_RPS,
    "result_file": "$RESULT_FILE"
  }
EOF
done

echo "]" >> "$SUMMARY_FILE"

# ---------------------------------------------------------------------------
# Print scaling table
# ---------------------------------------------------------------------------
echo ""
echo "=================================================="
echo " MULTI-STUB SCALING RESULTS"
echo "=================================================="
printf "%10s %12s %12s %12s\n" "N_groups" "workers" "total_procs" "max_rps"
printf "%10s %12s %12s %12s\n" "--------" "--------" "-----------" "--------"

"$PYTHON" -c "
import json, sys
try:
    data = json.load(open('$SUMMARY_FILE'))
    for r in data:
        n = r['num_stub_groups']
        w = r['stub_workers_per_group']
        t = r['total_stub_processes']
        rps = r['max_rps']
        rps_str = f'{rps:.1f}' if rps is not None and rps != 'null' else 'N/A'
        print(f'{n:>10} {w:>12} {t:>12} {rps_str:>12}')
except Exception as e:
    print(f'Error reading summary: {e}', file=sys.stderr)
"

echo "=================================================="
echo " Summary:  $SUMMARY_FILE"
echo " Log:      $LOG_FILE"
echo " Exit:     $OVERALL_EXIT"
echo "=================================================="

exit $OVERALL_EXIT
