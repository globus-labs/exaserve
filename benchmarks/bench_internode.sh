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
RPS_START=256000
MAX_RPS_CEILING=32000000
PRECISION=0.1
DURATION=20
POINT_TIMEOUT=240
PAYLOAD="medium"
PROBE_COOLDOWN=30
CPUPROFILE=false
NO_PORT_MONITOR=false
SWEEP_WORKERS_VAL=""
SWEEP_PAYLOADS_VAL=""
NUM_CLI=1

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
        --probe-cooldown)   PROBE_COOLDOWN="$2";    shift 2 ;;
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
        --num-cli)          NUM_CLI="$2";           shift 2 ;;
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

# Validate each N has at least 1 stub node
for N in ${NODE_LIST//,/ }; do
    if [ "$N" -le "$NUM_CLI" ]; then
        echo "!!! ERROR: --node-list value $N must be > --num-cli $NUM_CLI (need at least 1 stub node)"
        exit 1
    fi
done

# ---------------------------------------------------------------------------
# Derived paths
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"

resolve_site_config_field() {
    local field="$1"
    PYTHONPATH="$PROJECT_ROOT:$PROJECT_ROOT/src${PYTHONPATH:+:$PYTHONPATH}" python3 -S -m eval.site_config get "$field" 2>/dev/null || true
}

SITE_BENCH_ROOT="$(resolve_site_config_field bench_results_dir)"

if [ -z "$OUTPUT_DIR" ]; then
    OUTPUT_BASE="${SITE_BENCH_ROOT:-$HOME/agpt/data/bench_results}"
    OUTPUT_DIR="${OUTPUT_BASE}/internode_${TIMESTAMP}"
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
    local hsn_fqdn="${node}.hsn.cm.aurora.alcf.anl.gov"
    # Verify the FQDN resolves, then return it as-is (not the IP).
    # Using the FQDN in URLs ensures no_proxy=*.alcf.anl.gov matches,
    # so requests bypass HTTP_PROXY and connect directly over HSN.
    if getent hosts "$hsn_fqdn" >/dev/null 2>&1; then
        echo "$hsn_fqdn"
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
        ssh "$node" "bash -lc '${ENV_SETUP} && \
            unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ftp_proxy && \
            $PYTHON $STUB_SERVER \
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
    local attempt=0
    while [ $SECONDS -lt $deadline ]; do
        attempt=$((attempt + 1))
        if "$PYTHON" -c "
import urllib.request, sys, os
for k in ['HTTP_PROXY','HTTPS_PROXY','http_proxy','https_proxy']:
    os.environ.pop(k, None)
try:
    urllib.request.urlopen('http://${hsn_addr}:${port}/health', timeout=2)
    sys.exit(0)
except Exception as e:
    # Print error on every 5th attempt for visibility
    if ${attempt} % 5 == 0:
        print(f'  [health-check] attempt ${attempt}: {e}', flush=True)
    sys.exit(1)
" 2>&1; then
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
# Unset proxy for intra-cluster traffic
# ---------------------------------------------------------------------------
# env_local sets HTTP_PROXY for internet access, but benchmark traffic is
# entirely intra-cluster and must NOT go through the ALCF proxy.
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ftp_proxy
echo "[proxy] Unset HTTP_PROXY/HTTPS_PROXY for intra-cluster benchmark traffic"

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
echo "=================================================="
echo " INTER-NODE SCALING BENCHMARK"
echo "=================================================="
echo " node_list:        $NODE_LIST"
echo " total_nodes:      $TOTAL_NODES"
echo " num_cli:          $NUM_CLI"
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
    "--probe-cooldown"  "$PROBE_COOLDOWN"
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
if [ "$NUM_CLI" -gt 1 ]; then
    CLIENT_ARGS+=("--num-cli-nodes" "$NUM_CLI")
fi

OVERALL_EXIT=0
PREV_MAX_RPS=""

for N in ${NODE_LIST//,/ }; do
    echo ""
    echo "=================================================="
    echo " N=$N nodes ($NUM_CLI client(s) + $((N-NUM_CLI)) stub node(s))"
    echo "=================================================="

    if [ "$N" -le "$NUM_CLI" ]; then
        echo "!!! ERROR: N=$N must be > num_cli=$NUM_CLI (need at least 1 stub node)"
        OVERALL_EXIT=1
        continue
    fi

    # Create per-N output directory
    NDIR="$OUTPUT_DIR/N${N}"
    mkdir -p "$NDIR"

    # Select client nodes (indices 0..NUM_CLI-1) and stub nodes (indices NUM_CLI..N-1)
    CLI_NODES=()
    for ((i = 0; i < NUM_CLI; i++)); do
        CLI_NODES+=("${ALL_NODES[$i]}")
    done

    STUB_NODES=()
    HSN_ADDRS=()
    for ((i = NUM_CLI; i < N; i++)); do
        node="${ALL_NODES[$i]}"
        hsn=$(resolve_hsn "$node")
        STUB_NODES+=("$node")
        HSN_ADDRS+=("$hsn")
    done

    echo ">>> [INTERNODE] Client nodes: ${CLI_NODES[*]}"
    echo ">>> [INTERNODE] Stub nodes: ${STUB_NODES[*]}"
    echo ">>> [INTERNODE] HSN addresses: ${HSN_ADDRS[*]}"

    # Start stub servers on each stub node
    echo ">>> [INTERNODE] Starting $STUB_WORKERS stub workers on each of ${#STUB_NODES[@]} node(s)..."
    ACTIVE_STUB_NODES=("${STUB_NODES[@]}")
    for node in "${STUB_NODES[@]}"; do
        start_remote_stubs "$node" "$STUB_WORKERS" "$STUB_PORT"
    done

    # Start netstats collectors on all nodes (clients + stubs)
    ACTIVE_NETSTATS_NODES=()
    if [ "$NO_NETSTATS" != true ]; then
        echo ">>> [INTERNODE] Starting netstats collectors..."
        ALL_CURRENT_NODES=("${CLI_NODES[@]}" "${STUB_NODES[@]}")
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
        echo ""
        echo "=== DIAGNOSTICS ==="
        for i in "${!STUB_NODES[@]}"; do
            node="${STUB_NODES[$i]}"
            hsn="${HSN_ADDRS[$i]}"
            echo "--- Node: $node  HSN: $hsn ---"

            # Check if stub processes are running on remote node
            echo "  [procs] stub_server.py processes:"
            ssh "$node" "ps aux | grep stub_server.py | grep -v grep" 2>&1 | sed 's/^/    /' || echo "    (none)"

            # Check what is listening on the stub port
            echo "  [port] Listeners on port $STUB_PORT:"
            ssh "$node" "ss -tlnp 2>/dev/null | grep ':${STUB_PORT} '" 2>&1 | sed 's/^/    /' || echo "    (none)"

            # Check remote proxy env
            echo "  [proxy] HTTP_PROXY on remote:"
            ssh "$node" "bash -lc '${ENV_SETUP} && echo HTTP_PROXY=\$HTTP_PROXY'" 2>&1 | sed 's/^/    /'

            # Check remote network interfaces
            echo "  [net] Interfaces with IPs:"
            ssh "$node" "ip -brief addr show" 2>&1 | sed 's/^/    /'

            # Try a raw TCP connect from client node
            echo "  [tcp] TCP connect test from client ($(hostname)) to $hsn:$STUB_PORT:"
            "$PYTHON" -c "
import socket, sys
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(3)
try:
    s.connect(('${hsn}', ${STUB_PORT}))
    print('    TCP connect: OK')
    s.close()
except Exception as e:
    print(f'    TCP connect: FAILED — {e}')
" 2>&1

            # Try HTTP health check with detailed error
            echo "  [http] HTTP /health test from client:"
            "$PYTHON" -c "
import urllib.request, sys, os
# Ensure no proxy
for k in ['HTTP_PROXY','HTTPS_PROXY','http_proxy','https_proxy']:
    os.environ.pop(k, None)
try:
    resp = urllib.request.urlopen('http://${hsn}:${STUB_PORT}/health', timeout=3)
    print(f'    HTTP /health: {resp.status} {resp.read().decode()[:100]}')
except Exception as e:
    print(f'    HTTP /health: FAILED — {e}')
" 2>&1

            # Show last 5 lines of stub log
            echo "  [log] Last 5 lines of stub_${node}.log:"
            tail -5 "$NDIR/stub_${node}.log" 2>/dev/null | sed 's/^/    /' || echo "    (no log file)"
            echo ""
        done
        echo "=== END DIAGNOSTICS ==="
        kill_current
        OVERALL_EXIT=1
        continue
    fi

    # Create MPI hostfile for multi-client (raw PBS hostnames — MPI routes over HSN internally)
    if [ "$NUM_CLI" -gt 1 ]; then
        CLI_HOSTFILE="$NDIR/client_hostfile"
        printf '%s\n' "${CLI_NODES[@]}" > "$CLI_HOSTFILE"
        echo ">>> [INTERNODE] MPI hostfile ($CLI_HOSTFILE):"
        cat "$CLI_HOSTFILE" | sed 's/^/    /'
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
    if [ "$NUM_CLI" -gt 1 ]; then
        CLIENT_ARGS_N+=("--mpi-hostfile" "$CLI_HOSTFILE")
    fi

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
{"num_nodes": $N, "num_cli_nodes": $NUM_CLI, "num_stub_nodes": $((N-NUM_CLI)), "stub_workers_per_node": $STUB_WORKERS, "total_stub_processes": $(( (N-NUM_CLI) * STUB_WORKERS )), "max_rps": $MAX_RPS, "below_floor": $BELOW_FLOOR, "result_file": "$RESULT_FILE"}
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
