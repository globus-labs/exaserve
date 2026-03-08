#!/bin/bash
# ==============================================================================
# INTER-NODE SCALING BENCHMARK
# ==============================================================================
# Measures how max sustainable RPS scales when stub servers are distributed
# across multiple remote nodes.  Traffic pattern: one-to-all — client on node 0
# sends requests to stub servers on nodes 1..N-1, each listening on port 8000.
#
# USAGE:
#   bash benchmarks/bench_internode.sh --node-list 2,4,8 [OPTIONS]
#
# EXAMPLES:
#   # 2-node test (1 client + 1 stub node)
#   bash benchmarks/bench_internode.sh --node-list 2 --stub-workers 32
#
#   # Scaling sweep with fast-fail
#   bash benchmarks/bench_internode.sh --node-list 2,4,8 --fast-fail \
#       --num-go-procs 8 --go-concurrency 4000
#
# REQUIRES:
#   - Running inside a PBS job (PBS_NODEFILE must be set)
#   - --node-list is required (no default)
#
# OPTIONS:
#   --node-list        Comma-separated N values to sweep (REQUIRED)
#   --stub-workers     SO_REUSEPORT workers per node (default: 32)
#   --stub-port        Port for all stubs (default: 8000)
#   --stub-latency     Stub server latency in ms (default: 0)
#   --fast-fail        Stop sweep on first regression
#   --no-netstats      Disable network stats collection
#   --interfaces       Network interfaces to monitor (default: all)
#   --python           Python interpreter (default: python3)
#   --output-dir       Results directory (default: auto-timestamped)
#   --env-setup        Remote env setup command (default: source ~/script/env_local)
#
#   Forwarded to bench_client.py --find-max-rps:
#   --num-go-workers, --num-go-procs, --go-concurrency, --sum-only,
#   --probe-duration, --rps-start, --max-rps-ceiling, --precision,
#   --duration, --point-timeout, --payload
# ==============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
NODE_LIST=""
STUB_WORKERS=32
STUB_PORT=8000
STUB_LATENCY=0
FAST_FAIL=false
NO_NETSTATS=false
INTERFACES=""
PYTHON="python3"
OUTPUT_DIR=""
ENV_SETUP="source ~/script/env_local"

# bench_client.py find-max-rps defaults
NUM_GO_WORKERS=2
NUM_GO_PROCS=8
GO_CONCURRENCY=40
PROBE_DURATION=3
RPS_START=64000
MAX_RPS_CEILING=32000000
PRECISION=0.1
DURATION=20
POINT_TIMEOUT=240
PAYLOAD="medium"
CPUPROFILE=false
NO_PORT_MONITOR=false
SWEEP_WORKERS_VAL=""
SWEEP_PAYLOADS_VAL=""

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --node-list)        NODE_LIST="$2";         shift 2 ;;
        --stub-workers)     STUB_WORKERS="$2";      shift 2 ;;
        --stub-port)        STUB_PORT="$2";         shift 2 ;;
        --stub-latency)     STUB_LATENCY="$2";      shift 2 ;;
        --fast-fail)        FAST_FAIL=true;          shift   ;;
        --no-netstats)      NO_NETSTATS=true;        shift   ;;
        --interfaces)       INTERFACES="$2";         shift 2 ;;
        --python)           PYTHON="$2";            shift 2 ;;
        --output-dir)       OUTPUT_DIR="$2";        shift 2 ;;
        --env-setup)        ENV_SETUP="$2";         shift 2 ;;
        --num-go-workers)   NUM_GO_WORKERS="$2";    shift 2 ;;
        --num-go-procs)     NUM_GO_PROCS="$2";      shift 2 ;;
        --go-concurrency)   GO_CONCURRENCY="$2";    shift 2 ;;
        --probe-duration)   PROBE_DURATION="$2";    shift 2 ;;
        --rps-start)        RPS_START="$2";         shift 2 ;;
        --max-rps-ceiling)  MAX_RPS_CEILING="$2";   shift 2 ;;
        --precision)        PRECISION="$2";         shift 2 ;;
        --duration)         DURATION="$2";          shift 2 ;;
        --point-timeout)    POINT_TIMEOUT="$2";     shift 2 ;;
        --payload)          PAYLOAD="$2";           shift 2 ;;
        --cpuprofile)       CPUPROFILE=true;         shift   ;;
        --no-port-monitor)  NO_PORT_MONITOR=true;    shift   ;;
        --sweep-workers)    SWEEP_WORKERS_VAL="$2"; shift 2 ;;
        --sweep-payloads)   SWEEP_PAYLOADS_VAL="$2"; shift 2 ;;
        *)
            echo "!!! ERROR: Unknown argument '$1'"
            exit 1
            ;;
    esac
done

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
if [ -z "$NODE_LIST" ]; then
    echo "!!! ERROR: --node-list is required (e.g. --node-list 2,4,8)"
    exit 1
fi

if [ -z "${PBS_NODEFILE:-}" ]; then
    echo "!!! ERROR: \$PBS_NODEFILE not set. Are you inside a PBS job?"
    exit 1
fi

if [ ! -f "$PBS_NODEFILE" ]; then
    echo "!!! ERROR: PBS_NODEFILE=$PBS_NODEFILE does not exist"
    exit 1
fi

# ---------------------------------------------------------------------------
# Discover nodes
# ---------------------------------------------------------------------------
mapfile -t ALL_NODES < <(sort -u "$PBS_NODEFILE")
TOTAL_NODES=${#ALL_NODES[@]}
CLIENT_NODE="${ALL_NODES[0]}"
MY_HOSTNAME=$(hostname)

# Verify we are running on node 0
SHORT_CLIENT=$(echo "$CLIENT_NODE" | cut -d. -f1)
SHORT_SELF=$(echo "$MY_HOSTNAME" | cut -d. -f1)
if [ "$SHORT_CLIENT" != "$SHORT_SELF" ]; then
    echo "!!! ERROR: Must run on the first node in PBS_NODEFILE."
    echo "    Expected: $CLIENT_NODE (short: $SHORT_CLIENT)"
    echo "    Got:      $MY_HOSTNAME (short: $SHORT_SELF)"
    exit 1
fi

# Validate we have enough nodes for the largest N
MAX_N=0
for N in ${NODE_LIST//,/ }; do
    if [ "$N" -gt "$MAX_N" ]; then
        MAX_N=$N
    fi
done

if [ "$MAX_N" -gt "$TOTAL_NODES" ]; then
    echo "!!! ERROR: --node-list requires up to $MAX_N nodes, but only $TOTAL_NODES available."
    echo "    Nodes: ${ALL_NODES[*]}"
    exit 1
fi

# ---------------------------------------------------------------------------
# Derived paths
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"

if [ -z "$OUTPUT_DIR" ]; then
    OUTPUT_DIR="/home/wenyiw/agpt/data/bench_results/internode_${TIMESTAMP}"
fi
mkdir -p "$OUTPUT_DIR"

STUB_SERVER="$SCRIPT_DIR/stub_server.py"
BENCH_CLIENT="$SCRIPT_DIR/bench_client.py"
NETSTATS="$SCRIPT_DIR/netstats.py"
PLOT_SCRIPT="$SCRIPT_DIR/plot_internode.py"
LOG_FILE="$OUTPUT_DIR/internode.log"
SUMMARY_FILE="$OUTPUT_DIR/internode_summary.json"

# Validate scripts exist
for f in "$STUB_SERVER" "$BENCH_CLIENT" "$NETSTATS"; do
    if [ ! -f "$f" ]; then
        echo "!!! ERROR: Script not found: $f"
        exit 1
    fi
done

# ---------------------------------------------------------------------------
# Resolve HSN hostname for a node
# ---------------------------------------------------------------------------
resolve_hsn() {
    local node="$1"
    local hsn_host
    hsn_host=$(getent hosts "${node}.hsn.cm.aurora.alcf.anl.gov" 2>/dev/null \
               | awk '{ print $1 }' | head -n 1)
    if [ -n "$hsn_host" ]; then
        echo "$hsn_host"
    else
        # Fallback: use the node hostname directly
        echo "$node"
    fi
}

# ---------------------------------------------------------------------------
# Summary JSON management (incremental, always valid JSON)
# ---------------------------------------------------------------------------
SUMMARY_RESULTS=()

finalize_summary() {
    "$PYTHON" -c "
import json, sys
results = []
for arg in sys.argv[1:]:
    results.append(json.loads(arg))
with open('$SUMMARY_FILE', 'w') as f:
    json.dump(results, f, indent=2)
    f.write('\n')
" "${SUMMARY_RESULTS[@]}" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Cleanup handler
# ---------------------------------------------------------------------------
ACTIVE_STUB_NODES=()
ACTIVE_NETSTATS_NODES=()
CLEANUP_DONE=false

cleanup() {
    if [ "$CLEANUP_DONE" = true ]; then return; fi
    CLEANUP_DONE=true
    echo ""
    echo ">>> [INTERNODE] Cleaning up..."

    # Kill netstats collectors
    for node in "${ACTIVE_NETSTATS_NODES[@]}"; do
        ssh "$node" "pkill -f 'netstats.py'" 2>/dev/null || true
    done

    # Kill stub servers
    for node in "${ACTIVE_STUB_NODES[@]}"; do
        ssh "$node" "pkill -f 'stub_server.py'" 2>/dev/null || true
    done

    # Finalize summary with whatever we have
    finalize_summary

    # Try to generate plots from partial data
    if [ -f "$PLOT_SCRIPT" ]; then
        echo ">>> [INTERNODE] Generating plots from collected data..."
        "$PYTHON" "$PLOT_SCRIPT" --input-dir "$OUTPUT_DIR" 2>/dev/null || true
    fi

    echo ">>> [INTERNODE] Results saved to: $OUTPUT_DIR"
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Helper: start stub servers on a remote node
# ---------------------------------------------------------------------------
start_remote_stubs() {
    local node="$1"
    local workers="$2"
    local port="$3"

    for w in $(seq 1 "$workers"); do
        ssh "$node" "bash -lc '${ENV_SETUP} && $PYTHON $STUB_SERVER \
            --port $port \
            --host 0.0.0.0 \
            --latency-ms $STUB_LATENCY \
            --response-tokens 10 \
            --reuse-port \
            --log-level error'" \
            >> "$NDIR/stub_${node}.log" 2>&1 &
    done
}

# ---------------------------------------------------------------------------
# Helper: wait for a remote stub to be reachable via HTTP /health
# ---------------------------------------------------------------------------
wait_for_remote_stub() {
    local hsn_addr="$1"
    local port="$2"
    local deadline=$((SECONDS + 30))
    while [ $SECONDS -lt $deadline ]; do
        if "$PYTHON" -c "
import urllib.request, sys
try:
    urllib.request.urlopen('http://${hsn_addr}:${port}/health', timeout=2)
    sys.exit(0)
except:
    sys.exit(1)
" 2>/dev/null; then
            return 0
        fi
        sleep 1
    done
    return 1
}

# ---------------------------------------------------------------------------
# Helper: start netstats collector on a node
# ---------------------------------------------------------------------------
start_netstats() {
    local node="$1"
    local output_file="$2"
    local iface_arg=""
    if [ -n "$INTERFACES" ]; then
        iface_arg="--interfaces $INTERFACES"
    fi
    ssh "$node" "bash -lc '${ENV_SETUP} && $PYTHON $NETSTATS \
        --output $output_file \
        $iface_arg'" \
        >> "$NDIR/netstats_${node}_stderr.log" 2>&1 &
}

# ---------------------------------------------------------------------------
# Helper: kill stubs and netstats for current N
# ---------------------------------------------------------------------------
kill_current() {
    for node in "${ACTIVE_NETSTATS_NODES[@]}"; do
        ssh "$node" "pkill -f 'netstats.py'" 2>/dev/null || true
    done
    for node in "${ACTIVE_STUB_NODES[@]}"; do
        ssh "$node" "pkill -f 'stub_server.py'" 2>/dev/null || true
    done
    sleep 1
    # Force kill
    for node in "${ACTIVE_STUB_NODES[@]}"; do
        ssh "$node" "pkill -9 -f 'stub_server.py'" 2>/dev/null || true
    done
    ACTIVE_STUB_NODES=()
    ACTIVE_NETSTATS_NODES=()
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
echo "=================================================="
echo " INTER-NODE SCALING BENCHMARK"
echo "=================================================="
echo " node_list:        $NODE_LIST"
echo " total_nodes:      $TOTAL_NODES"
echo " client_node:      $CLIENT_NODE"
echo " stub_workers:     $STUB_WORKERS (SO_REUSEPORT per node)"
echo " stub_port:        $STUB_PORT"
echo " stub_latency:     ${STUB_LATENCY}ms"
echo " num_go_workers:   $NUM_GO_WORKERS"
echo " num_go_procs:     $NUM_GO_PROCS"
echo " go_concurrency:   $GO_CONCURRENCY"
echo " duration:         ${DURATION}s"
echo " fast_fail:        $FAST_FAIL"
echo " netstats:         $([ "$NO_NETSTATS" = true ] && echo "disabled" || echo "enabled")"
echo " env_setup:        $ENV_SETUP"
echo " output_dir:       $OUTPUT_DIR"
echo " python:           $PYTHON"
echo "=================================================="
echo ""

# Print node list
echo "Allocated nodes:"
for i in "${!ALL_NODES[@]}"; do
    local_hsn=$(resolve_hsn "${ALL_NODES[$i]}")
    echo "  [$i] ${ALL_NODES[$i]}  (HSN: $local_hsn)"
done
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
    "--sum-only"
)
if [ "$POINT_TIMEOUT" -gt 0 ] 2>/dev/null; then
    CLIENT_ARGS+=("--point-timeout" "$POINT_TIMEOUT")
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

OVERALL_EXIT=0
PREV_MAX_RPS=""

for N in ${NODE_LIST//,/ }; do
    echo ""
    echo "=================================================="
    echo " N=$N nodes (1 client + $((N-1)) stub node(s))"
    echo "=================================================="

    if [ "$N" -lt 2 ]; then
        echo "!!! ERROR: N must be >= 2 (1 client + at least 1 stub node)"
        OVERALL_EXIT=1
        continue
    fi

    # Create per-N output directory
    NDIR="$OUTPUT_DIR/N${N}"
    mkdir -p "$NDIR"

    # Select stub nodes and resolve HSN addresses
    STUB_NODES=()
    HSN_ADDRS=()
    for i in $(seq 1 $((N-1))); do
        node="${ALL_NODES[$i]}"
        hsn=$(resolve_hsn "$node")
        STUB_NODES+=("$node")
        HSN_ADDRS+=("$hsn")
    done

    echo ">>> [INTERNODE] Stub nodes: ${STUB_NODES[*]}"
    echo ">>> [INTERNODE] HSN addresses: ${HSN_ADDRS[*]}"

    # Start stub servers on each stub node
    echo ">>> [INTERNODE] Starting $STUB_WORKERS stub workers on each of ${#STUB_NODES[@]} node(s)..."
    ACTIVE_STUB_NODES=("${STUB_NODES[@]}")
    for node in "${STUB_NODES[@]}"; do
        start_remote_stubs "$node" "$STUB_WORKERS" "$STUB_PORT"
    done

    # Start netstats collectors on all nodes (client + stubs)
    ACTIVE_NETSTATS_NODES=()
    if [ "$NO_NETSTATS" != true ]; then
        echo ">>> [INTERNODE] Starting netstats collectors..."
        ALL_CURRENT_NODES=("$CLIENT_NODE" "${STUB_NODES[@]}")
        for node in "${ALL_CURRENT_NODES[@]}"; do
            netstats_out="$NDIR/netstats_${node}.jsonl"
            start_netstats "$node" "$netstats_out"
            ACTIVE_NETSTATS_NODES+=("$node")
        done
    fi

    # Wait for all stubs to be reachable
    echo ">>> [INTERNODE] Waiting for stub servers to be reachable..."
    ALL_READY=true
    for i in "${!STUB_NODES[@]}"; do
        hsn="${HSN_ADDRS[$i]}"
        node="${STUB_NODES[$i]}"
        if wait_for_remote_stub "$hsn" "$STUB_PORT"; then
            echo "    [OK] $node ($hsn:$STUB_PORT)"
        else
            echo "    [FAIL] $node ($hsn:$STUB_PORT) — not reachable within 30s"
            ALL_READY=false
        fi
    done

    if [ "$ALL_READY" != true ]; then
        echo "!!! ERROR: Not all stub servers became reachable for N=$N"
        kill_current
        OVERALL_EXIT=1
        continue
    fi

    # Build URL list
    URLS=""
    for hsn in "${HSN_ADDRS[@]}"; do
        if [ -z "$URLS" ]; then
            URLS="http://${hsn}:${STUB_PORT}"
        else
            URLS="${URLS},http://${hsn}:${STUB_PORT}"
        fi
    done
    echo "    URLs: $URLS"

    # Run bench_client
    RESULT_FILE="$NDIR/max_rps_N${N}.json"
    echo ">>> [INTERNODE] Running find-max-rps with N=$N..."

    CLIENT_ARGS_N=("${CLIENT_ARGS[@]}")
    CLIENT_ARGS_N+=("--base-urls" "$URLS")
    CLIENT_ARGS_N+=("--output" "$RESULT_FILE")
    CLIENT_ARGS_N+=("--work-dir" "$NDIR/client_work")

    "$PYTHON" "$BENCH_CLIENT" "${CLIENT_ARGS_N[@]}" 2>&1 | tee -a "$LOG_FILE"
    local_exit=${PIPESTATUS[0]}

    # Kill stubs and netstats
    echo ">>> [INTERNODE] Stopping stub servers and netstats..."
    kill_current

    if [ $local_exit -ne 0 ]; then
        echo "!!! ERROR: bench_client.py exited with code $local_exit for N=$N"
        OVERALL_EXIT=$local_exit
    fi

    # Extract max_rps and below_floor from result file
    MAX_RPS="null"
    BELOW_FLOOR="false"
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
        BELOW_FLOOR=$("$PYTHON" -c "
import json, sys
try:
    data = json.load(open('$RESULT_FILE'))
    results = data.get('results', [])
    if results:
        print('true' if results[0].get('below_floor', False) else 'false')
    else:
        print('false')
except:
    print('false')
" 2>/dev/null || echo "false")
    fi

    echo "[RESULT] N=$N: max_rps=$MAX_RPS  below_floor=$BELOW_FLOOR"

    # Append to summary
    RESULT_JSON=$(cat <<JSONEOF
{"num_nodes": $N, "num_stub_nodes": $((N-1)), "stub_workers_per_node": $STUB_WORKERS, "total_stub_processes": $(( (N-1) * STUB_WORKERS )), "max_rps": $MAX_RPS, "below_floor": $BELOW_FLOOR, "result_file": "$RESULT_FILE"}
JSONEOF
    )
    SUMMARY_RESULTS+=("$RESULT_JSON")
    finalize_summary

    # Fast-fail check
    if [ "$FAST_FAIL" = true ]; then
        if [ "$BELOW_FLOOR" = "true" ]; then
            echo ">>> [FAST-FAIL] below_floor=true at N=$N. Stopping sweep."
            break
        fi
        if [ -n "$PREV_MAX_RPS" ] && [ "$MAX_RPS" != "null" ] && [ "$PREV_MAX_RPS" != "null" ]; then
            REGRESSED=$("$PYTHON" -c "
prev = $PREV_MAX_RPS
curr = $MAX_RPS
print('true' if curr < prev * 0.9 else 'false')
" 2>/dev/null || echo "false")
            if [ "$REGRESSED" = "true" ]; then
                echo ">>> [FAST-FAIL] max_rps regressed from $PREV_MAX_RPS to $MAX_RPS at N=$N. Stopping sweep."
                break
            fi
        fi
    fi

    if [ "$MAX_RPS" != "null" ]; then
        PREV_MAX_RPS="$MAX_RPS"
    fi
done

# ---------------------------------------------------------------------------
# Print scaling table
# ---------------------------------------------------------------------------
echo ""
echo "=================================================="
echo " INTER-NODE SCALING RESULTS"
echo "=================================================="
printf "%8s %12s %12s %12s %12s\n" "N_nodes" "stub_nodes" "workers/node" "total_procs" "max_rps"
printf "%8s %12s %12s %12s %12s\n" "-------" "----------" "------------" "-----------" "--------"

"$PYTHON" -c "
import json, sys
try:
    data = json.load(open('$SUMMARY_FILE'))
    for r in data:
        n = r['num_nodes']
        sn = r['num_stub_nodes']
        w = r['stub_workers_per_node']
        t = r['total_stub_processes']
        rps = r['max_rps']
        rps_str = f'{rps:.1f}' if rps is not None and rps != 'null' else 'N/A'
        print(f'{n:>8} {sn:>12} {w:>12} {t:>12} {rps_str:>12}')
except Exception as e:
    print(f'Error reading summary: {e}', file=sys.stderr)
"

# Generate plots
if [ -f "$PLOT_SCRIPT" ]; then
    echo ""
    echo ">>> [INTERNODE] Generating plots..."
    "$PYTHON" "$PLOT_SCRIPT" --input-dir "$OUTPUT_DIR" 2>&1 || true
fi

echo "=================================================="
echo " Summary:  $SUMMARY_FILE"
echo " Log:      $LOG_FILE"
echo " Output:   $OUTPUT_DIR"
echo " Exit:     $OVERALL_EXIT"
echo "=================================================="

exit $OVERALL_EXIT
