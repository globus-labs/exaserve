#!/bin/bash
# ==============================================================================
# BENCHMARK ORCHESTRATOR
# ==============================================================================
# Mirrors the structure of eval/templates/run_exp.sh but for isolated
# benchmarking (no Ray, no GPU, no PBS required).
#
# USAGE:
#   bash benchmarks/run_bench.sh [OPTIONS]
#
# MODE EXAMPLES:
#   # Client throughput sweep (stub backend, no proxy)
#   bash benchmarks/run_bench.sh --mode client --sweep workers,rps
#
#   # Client + proxy sweep together
#   bash benchmarks/run_bench.sh --mode all --litellm-python /path/to/venv/bin/python3
#
#   # Quick single-point test
#   bash benchmarks/run_bench.sh --mode client --workers 8 --rps 500
#
#   # Ramp test on LiteLLM to find breaking point
#   bash benchmarks/run_bench.sh --mode proxy --ramp \
#       --litellm-python /path/to/venv/bin/python3
#
# OPTIONS:
#   --mode           client | proxy | all   (default: client)
#   --sweep          Dimensions to sweep for client, e.g. workers,rps,payload,pool,http
#   --workers        Fixed num_workers (default: 4)
#   --rps            Fixed target RPS (default: 200)
#   --payload        Fixed payload size: small|medium|large|xl (default: medium)
#   --pool           Fixed connection pool size per worker (default: 100)
#   --http2          Use HTTP/2 (default: true)
#   --http11         Use HTTP/1.1 instead of HTTP/2
#   --duration       Seconds per sweep point (default: 20)
#   --warmup         Warmup seconds to discard (default: 5)
#   --stub-port      Port for stub server (default: 8000)
#   --stub-latency   Stub server artificial latency in ms (default: 0)
#   --stub-workers   Number of uvicorn worker processes for the stub server (default: 1)
#   --num-stubs      Number of stub server instances (default: 1, for proxy mode)
#   --litellm-python Path to the litellm venv Python (default: site_config.litellm_python_path)
#   --litellm-workers Fixed LiteLLM num_workers (default: 4)
#   --routing        Fixed LiteLLM routing strategy (default: least-busy)
#   --num-backends   Fixed number of stub backends for proxy (default: 4)
#   --ramp           Run a ramp test instead of fixed-rate sweep (proxy mode)
#   --ramp-start     Starting RPS for ramp (default: 10)
#   --ramp-max       Maximum RPS for ramp (default: 2000)
#   --ramp-step      RPS multiplier per step (default: 1.3)
#   --ramp-window    Seconds per ramp step (default: 10)
#   --plots          Plot types for analyze.py (default: all)
#   --no-analyze     Skip analyze.py after benchmark
#   --no-port-monitor Skip live port monitoring
#   --output-dir     Where to save results (default: site_config.bench_results_dir/run_<timestamp>)
#   --python         Python interpreter to use (default: python3)
#   --find-max-rps   Auto-discover max sustainable RPS per (workers, payload) via
#                    exponential probe + binary search + validation
#   --probe-duration Seconds per probe in find-max-rps mode (default: 8)
#   --rps-start      Starting RPS for exponential probe phase (default: 50)
#   --max-rps-ceiling Upper RPS bound for probing (default: 20000)
#   --precision      Binary-search precision as fraction (default: 0.02 = 2%)
# ==============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
MODE="client"
SWEEP=""
WORKERS=4
RPS=200
PAYLOAD="medium"
POOL=100
HTTP_FLAG="--http2"
DURATION=20
WARMUP=5
POINT_TIMEOUT=0
STUB_PORT=8000
STUB_LATENCY=0
STUB_WORKERS=1
NUM_STUBS=1
GO_CONCURRENCY=2000
NUM_GO_WORKERS=2
NUM_GO_PROCS=1
LITELLM_PYTHON=""
LITELLM_WORKERS=4
ROUTING="least-busy"
NUM_BACKENDS=4
RAMP=false
RAMP_START=10
RAMP_MAX=2000
RAMP_STEP=1.3
RAMP_WINDOW=10
PLOTS="all"
NO_ANALYZE=false
NO_PORT_MONITOR=false
OUTPUT_DIR=""
PYTHON="python3"
# Fine-grained sweep range overrides (empty = use bench_client.py defaults)
SWEEP_WORKERS_VAL=""
SWEEP_RPS_VAL=""
SWEEP_PAYLOADS_VAL=""
SWEEP_POOLS_VAL=""
# Auto max-RPS search
SUM_ONLY=true
FIND_MAX_RPS=false
PROBE_DURATION=8
RPS_START=50
MAX_RPS_CEILING=3200000
PRECISION=0.1

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)            MODE="$2";             shift 2 ;;
        --sweep)           SWEEP="$2";            shift 2 ;;
        --workers)         WORKERS="$2";          shift 2 ;;
        --rps)             RPS="$2";              shift 2 ;;
        --payload)         PAYLOAD="$2";          shift 2 ;;
        --pool)            POOL="$2";             shift 2 ;;
        --http2)           HTTP_FLAG="--http2";   shift   ;;
        --http11)          HTTP_FLAG="--http11";  shift   ;;
        --duration)        DURATION="$2";              shift 2 ;;
        --warmup)          WARMUP="$2";               shift 2 ;;
        --point-timeout)   POINT_TIMEOUT="$2";        shift 2 ;;
        --sweep-workers)   SWEEP_WORKERS_VAL="$2";    shift 2 ;;
        --sweep-rps)       SWEEP_RPS_VAL="$2";        shift 2 ;;
        --sweep-payloads)  SWEEP_PAYLOADS_VAL="$2";   shift 2 ;;
        --sweep-pools)     SWEEP_POOLS_VAL="$2";      shift 2 ;;
        --stub-port)       STUB_PORT="$2";         shift 2 ;;
        --stub-latency)    STUB_LATENCY="$2";     shift 2 ;;
        --stub-workers)    STUB_WORKERS="$2";     shift 2 ;;
        --num-stubs)       NUM_STUBS="$2";        shift 2 ;;
        --go-concurrency)    GO_CONCURRENCY="$2";    shift 2 ;;
        --num-go-workers)    NUM_GO_WORKERS="$2";    shift 2 ;;
        --num-go-procs)      NUM_GO_PROCS="$2";     shift 2 ;;
        --litellm-python)  LITELLM_PYTHON="$2";  shift 2 ;;
        --litellm-workers) LITELLM_WORKERS="$2"; shift 2 ;;
        --routing)         ROUTING="$2";          shift 2 ;;
        --num-backends)    NUM_BACKENDS="$2";     shift 2 ;;
        --ramp)            RAMP=true;             shift   ;;
        --ramp-start)      RAMP_START="$2";       shift 2 ;;
        --ramp-max)        RAMP_MAX="$2";         shift 2 ;;
        --ramp-step)       RAMP_STEP="$2";        shift 2 ;;
        --ramp-window)     RAMP_WINDOW="$2";      shift 2 ;;
        --plots)           PLOTS="$2";            shift 2 ;;
        --no-analyze)      NO_ANALYZE=true;       shift   ;;
        --no-port-monitor) NO_PORT_MONITOR=true;   shift   ;;
        --output-dir)      OUTPUT_DIR="$2";        shift 2 ;;
        --python)          PYTHON="$2";            shift 2 ;;
        --sum-only)        SUM_ONLY=true;           shift   ;;
        --find-max-rps)    FIND_MAX_RPS=true;      shift   ;;
        --probe-duration)  PROBE_DURATION="$2";    shift 2 ;;
        --rps-start)       RPS_START="$2";         shift 2 ;;
        --max-rps-ceiling) MAX_RPS_CEILING="$2";   shift 2 ;;
        --precision)       PRECISION="$2";         shift 2 ;;
        *)
            echo "!!! ERROR: Unknown argument '$1'"
            echo "    Run: bash benchmarks/run_bench.sh --help  (or read this script header)"
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
    PYTHONPATH="$PROJECT_ROOT:$PROJECT_ROOT/src${PYTHONPATH:+:$PYTHONPATH}" python3 -S -m eval.site_config get "$field" 2>/dev/null || true
}

SITE_BENCH_ROOT="$(resolve_site_config_field bench_results_dir)"
SITE_LITELLM_PYTHON="$(resolve_site_config_field litellm_python_path)"

if [ -z "$OUTPUT_DIR" ]; then
    OUTPUT_BASE="${SITE_BENCH_ROOT:-$HOME/agpt/data/bench_results}"
    OUTPUT_DIR="${OUTPUT_BASE}/run_${TIMESTAMP}"
fi
mkdir -p "$OUTPUT_DIR"

if [ "$FIND_MAX_RPS" = true ]; then
    CLIENT_RESULT="$OUTPUT_DIR/max_rps_${TIMESTAMP}.json"
else
    CLIENT_RESULT="$OUTPUT_DIR/client_sweep_${TIMESTAMP}.json"
fi
PROXY_RESULT="$OUTPUT_DIR/proxy_sweep_${TIMESTAMP}.json"
LOG_FILE="$OUTPUT_DIR/run_bench_${TIMESTAMP}.log"

STUB_SERVER="$SCRIPT_DIR/stub_server.py"
BENCH_CLIENT="$SCRIPT_DIR/bench_client.py"
BENCH_PROXY="$SCRIPT_DIR/bench_proxy.py"
ANALYZE="$SCRIPT_DIR/analyze.py"

PORT_MONITOR_FLAG=""
if [ "$NO_PORT_MONITOR" = true ]; then
    PORT_MONITOR_FLAG="--no-port-monitor"
fi

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
echo "=================================================="
echo " BENCHMARK ORCHESTRATOR"
echo "=================================================="
echo " Mode:        $MODE"
echo " Output dir:  $OUTPUT_DIR"
echo " Log file:    $LOG_FILE"
echo " Python:      $PYTHON"
echo "=================================================="

if [ "$MODE" = "proxy" ] || [ "$MODE" = "all" ]; then
    if [ -z "$LITELLM_PYTHON" ]; then
        LITELLM_PYTHON="$SITE_LITELLM_PYTHON"
    fi
    if [ -z "$LITELLM_PYTHON" ]; then
        echo "!!! ERROR: No LiteLLM Python configured for proxy/all mode."
        echo "    Pass --litellm-python or set site_config.litellm_python_path."
        exit 1
    fi
    if [ ! -x "$LITELLM_PYTHON" ]; then
        echo "!!! ERROR: litellm python not found or not executable: $LITELLM_PYTHON"
        exit 1
    fi
    echo "[✓] LiteLLM python: $LITELLM_PYTHON"
fi

for f in "$STUB_SERVER" "$BENCH_CLIENT" "$BENCH_PROXY" "$ANALYZE"; do
    if [ ! -f "$f" ]; then
        echo "!!! ERROR: Script not found: $f"
        exit 1
    fi
done
echo "[✓] All benchmark scripts found."
echo ""

# ---------------------------------------------------------------------------
# Cleanup handler
# ---------------------------------------------------------------------------
STUB_PIDS=""
CLEANUP_DONE=false

cleanup() {
    if [ "$CLEANUP_DONE" = true ]; then return; fi
    CLEANUP_DONE=true
    echo ""
    echo ">>> [BENCH] Caught EXIT/Signal. Cleaning up..."
    if [ -n "$STUB_PIDS" ]; then
        for pid in $STUB_PIDS; do
            if kill -0 "$pid" 2>/dev/null; then
                echo "    Stopping stub server (PID $pid)..."
                kill -TERM "$pid" 2>/dev/null || true
            fi
        done
    fi
    echo ">>> [BENCH] Cleanup complete."
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Helper: start one stub server and wait for it to be ready
# ---------------------------------------------------------------------------
start_stub() {
    local port="$1"
    local n_procs="${STUB_WORKERS:-1}"

    if [ "$n_procs" -gt 1 ]; then
        echo ">>> [BENCH] Starting $n_procs stub processes on port $port (SO_REUSEPORT)..."
        for i in $(seq 1 "$n_procs"); do
            "$PYTHON" "$STUB_SERVER" \
                --port "$port" \
                --host 127.0.0.1 \
                --latency-ms "$STUB_LATENCY" \
                --response-tokens 10 \
                --reuse-port \
                --log-level error \
                >> "$OUTPUT_DIR/stub_${port}.log" 2>&1 &
            local pid=$!
            STUB_PIDS="$STUB_PIDS $pid"
            echo "    [stub $i/$n_procs] launched PID $pid"
        done
    else
        echo ">>> [BENCH] Starting stub server on port $port..."
        "$PYTHON" "$STUB_SERVER" \
            --port "$port" \
            --host 127.0.0.1 \
            --latency-ms "$STUB_LATENCY" \
            --response-tokens 10 \
            --log-level error \
            >> "$OUTPUT_DIR/stub_${port}.log" 2>&1 &
        local pid=$!
        STUB_PIDS="$STUB_PIDS $pid"
        echo "    [stub 1/1] launched PID $pid"
    fi

    # Wait up to 15 s for the stub to accept connections
    local deadline=$((SECONDS + 15))
    while [ $SECONDS -lt $deadline ]; do
        if "$PYTHON" -c \
            "import socket, sys; s=socket.socket(); s.settimeout(1); \
             sys.exit(0 if s.connect_ex(('127.0.0.1', $port))==0 else 1)" \
             2>/dev/null; then
            # Verify how many stub processes are actually alive
            local alive=0
            for p in $STUB_PIDS; do
                if kill -0 "$p" 2>/dev/null; then
                    alive=$((alive + 1))
                fi
            done
            echo "[✓] Stub server ready on port $port ($alive/$n_procs process(es) alive)"
            if [ "$alive" -lt "$n_procs" ]; then
                echo "!!! WARNING: Only $alive of $n_procs stub processes survived!"
                echo "    Check log: $OUTPUT_DIR/stub_${port}.log"
            fi
            return 0
        fi
        sleep 0.5
    done
    echo "!!! ERROR: Stub server on port $port did not start within 15s"
    echo "    Check log: $OUTPUT_DIR/stub_${port}.log"
    exit 1
}

# ==============================================================================
# CLIENT BENCHMARK
# ==============================================================================
run_client_bench() {
    echo ""
    echo "=================================================="
    echo " CLIENT BENCHMARK"
    echo "=================================================="

    # Build client sweep args
    CLIENT_ARGS=(
        "--target"        "stub_server"
        "--stub-host"     "127.0.0.1"
        "--stub-port"     "$STUB_PORT"
        "--workers"       "$WORKERS"
        "--rps"           "$RPS"
        "--payload"       "$PAYLOAD"
        "--pool"          "$POOL"
        "$HTTP_FLAG"
        "--duration"      "$DURATION"
        "--point-timeout" "$POINT_TIMEOUT"
        "--output"        "$CLIENT_RESULT"
        "--work-dir"      "$OUTPUT_DIR/client_work"
        "--python"        "$PYTHON"
    )
    if [ -n "$SWEEP" ]; then
        CLIENT_ARGS+=("--sweep" "$SWEEP")
    fi
    if [ -n "$SWEEP_WORKERS_VAL" ]; then
        CLIENT_ARGS+=("--sweep-workers" "$SWEEP_WORKERS_VAL")
    fi
    if [ -n "$SWEEP_RPS_VAL" ]; then
        CLIENT_ARGS+=("--sweep-rps" "$SWEEP_RPS_VAL")
    fi
    if [ -n "$SWEEP_PAYLOADS_VAL" ]; then
        CLIENT_ARGS+=("--sweep-payloads" "$SWEEP_PAYLOADS_VAL")
    fi
    if [ -n "$SWEEP_POOLS_VAL" ]; then
        CLIENT_ARGS+=("--sweep-pools" "$SWEEP_POOLS_VAL")
    fi
    if [ "$NO_PORT_MONITOR" = true ]; then
        CLIENT_ARGS+=("--no-port-monitor")
    fi
    if [ "${STUB_WORKERS:-1}" -gt 1 ]; then
        CLIENT_ARGS+=("--stub-workers" "$STUB_WORKERS")
    fi
    if [ "$FIND_MAX_RPS" = true ]; then
        CLIENT_ARGS+=(
            "--find-max-rps"
            "--probe-duration" "$PROBE_DURATION"
            "--rps-start"      "$RPS_START"
            "--max-rps-ceiling" "$MAX_RPS_CEILING"
            "--precision"      "$PRECISION"
        )
    fi
    CLIENT_ARGS+=("--go-concurrency"   "$GO_CONCURRENCY")
    CLIENT_ARGS+=("--num-go-workers"   "$NUM_GO_WORKERS")
    CLIENT_ARGS+=("--num-go-procs"     "$NUM_GO_PROCS")
    if [ "$SUM_ONLY" = true ]; then
        CLIENT_ARGS+=("--sum-only")
    fi

    echo ">>> [BENCH] Starting stub server..."
    start_stub "$STUB_PORT"

    echo ">>> [BENCH] Running bench_client.py..."
    echo "    Args: ${CLIENT_ARGS[*]}"
    "$PYTHON" "$BENCH_CLIENT" "${CLIENT_ARGS[@]}" 2>&1 | tee -a "$LOG_FILE"
    local exit_code=${PIPESTATUS[0]}

    # Kill stub server now that client is done
    if [ -n "$STUB_PIDS" ]; then
        for pid in $STUB_PIDS; do
            kill -TERM "$pid" 2>/dev/null || true
        done
        STUB_PIDS=""
    fi

    if [ $exit_code -ne 0 ]; then
        echo "!!! ERROR: bench_client.py exited with code $exit_code"
        return $exit_code
    fi

    echo "[✓] Client benchmark complete. Results: $CLIENT_RESULT"
}

# ==============================================================================
# PROXY BENCHMARK
# ==============================================================================
run_proxy_bench() {
    echo ""
    echo "=================================================="
    echo " PROXY (LiteLLM) BENCHMARK"
    echo "=================================================="

    # Note: bench_proxy.py manages its own stub servers internally,
    # so we don't start stubs here.

    PROXY_ARGS=(
        "--litellm-python"  "$LITELLM_PYTHON"
        "--litellm-workers" "$LITELLM_WORKERS"
        "--routing"         "$ROUTING"
        "--num-backends"    "$NUM_BACKENDS"
        "--rps"             "$RPS"
        "--duration"        "$DURATION"
        "--work-dir"        "$OUTPUT_DIR/proxy_work"
        "--output"          "$PROXY_RESULT"
    )
    if [ -n "$SWEEP" ]; then
        PROXY_ARGS+=("--sweep" "$SWEEP")
    fi
    if [ "$RAMP" = true ]; then
        PROXY_ARGS+=("--ramp"
                     "--ramp-start"  "$RAMP_START"
                     "--ramp-max"    "$RAMP_MAX"
                     "--ramp-step"   "$RAMP_STEP"
                     "--ramp-window" "$RAMP_WINDOW")
    fi
    if [ "$NO_PORT_MONITOR" = true ]; then
        PROXY_ARGS+=("--no-port-monitor")
    fi

    echo ">>> [BENCH] Running bench_proxy.py..."
    echo "    Args: ${PROXY_ARGS[*]}"
    "$PYTHON" "$BENCH_PROXY" "${PROXY_ARGS[@]}" 2>&1 | tee -a "$LOG_FILE"
    local exit_code=${PIPESTATUS[0]}

    if [ $exit_code -ne 0 ]; then
        echo "!!! ERROR: bench_proxy.py exited with code $exit_code"
        return $exit_code
    fi

    echo "[✓] Proxy benchmark complete. Results: $PROXY_RESULT"
}

# ==============================================================================
# ANALYSIS
# ==============================================================================
run_analysis() {
    local result_file="$1"
    if [ ! -f "$result_file" ]; then
        echo "!!! WARNING: Result file not found, skipping analysis: $result_file"
        return 0
    fi

    echo ""
    echo ">>> [BENCH] Running analyze.py on $result_file..."
    "$PYTHON" "$ANALYZE" \
        --input "$result_file" \
        --plots "$PLOTS" \
        --out-dir "$OUTPUT_DIR" \
        2>&1 | tee -a "$LOG_FILE"
    echo "[✓] Plots saved to $OUTPUT_DIR"
}

# ==============================================================================
# MAIN FLOW
# ==============================================================================
echo ""
echo ">>> [BENCH] Starting at $(date)"
echo ">>> [BENCH] Logging to $LOG_FILE"
echo ""

OVERALL_EXIT=0

case "$MODE" in
    client)
        run_client_bench || OVERALL_EXIT=$?
        if [ "$NO_ANALYZE" = false ] && [ $OVERALL_EXIT -eq 0 ]; then
            run_analysis "$CLIENT_RESULT"
        fi
        ;;
    proxy)
        run_proxy_bench || OVERALL_EXIT=$?
        if [ "$NO_ANALYZE" = false ] && [ $OVERALL_EXIT -eq 0 ]; then
            run_analysis "$PROXY_RESULT"
        fi
        ;;
    all)
        run_client_bench || OVERALL_EXIT=$?
        if [ $OVERALL_EXIT -eq 0 ] || true; then   # run proxy even if client had issues
            run_proxy_bench || OVERALL_EXIT=$?
        fi
        if [ "$NO_ANALYZE" = false ]; then
            run_analysis "$CLIENT_RESULT"
            run_analysis "$PROXY_RESULT"
        fi
        ;;
    *)
        echo "!!! ERROR: Unknown mode '$MODE'. Use: client | proxy | all"
        exit 1
        ;;
esac

echo ""
echo "=================================================="
echo " BENCHMARK ORCHESTRATOR - FINISHED"
echo " Mode:       $MODE"
echo " Exit code:  $OVERALL_EXIT"
echo " Output dir: $OUTPUT_DIR"
echo " Date:       $(date)"
echo "=================================================="

exit $OVERALL_EXIT
