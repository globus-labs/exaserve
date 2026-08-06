#!/bin/bash
# PR-024/PR-009 proxy-mode smoke. Run inside a >=2-node lease.
# Deploys tp=1 8B across all nodes fronted by HAProxy, confirms:
#   - haproxy -c preflight ran (PR-024),
#   - a request routes through the proxy and returns tokens,
#   - killing HAProxy makes the driver terminate the deployment (PR-009).
set -o pipefail
source ~/script/env_aurora
cd ~/exaserve || exit 1
export PATH="$HOME/bin:$PATH"   # user-built haproxy
OUT=$PWD/artifacts/hardening/haproxy-smoke
mkdir -p "$OUT"
NODES=$(sort -u "$PBS_NODEFILE" | wc -l)
export EXASERVE_RUN_LOG_ROOT=$OUT/run_logs EXASERVE_VENDOR=xpu
export no_proxy="localhost,127.0.0.1,$(hostname)"
cp scripts/hardening/config.haproxy.8b.yaml "$OUT/config.yaml"
# match num_nodes to the lease
sed -i "s/^  num_nodes: .*/  num_nodes: $NODES/" "$OUT/config.yaml"
echo "=== HAProxy smoke on $NODES nodes ($(hostname)) ==="
command -v haproxy >/dev/null 2>&1 && echo "haproxy: $(haproxy -v 2>&1 | head -1)" || echo "HAPROXY_MISSING"

bash src/exaserve/resources/launch_cluster.sh "$OUT/config.yaml" > "$OUT/launch.log" 2>&1 &
LPID=$!
ready=0
for i in $(seq 1 150); do
  grep -q "ALL SERVICES READY" "$OUT/launch.log" && { ready=1; break; }
  grep -qiE "FATAL|Critical Error|Traceback|failed .haproxy -c." "$OUT/launch.log" && break
  kill -0 $LPID 2>/dev/null || break
  sleep 10
done
echo "ready=$ready after $((i*10))s"

# PR-024 evidence: the haproxy -c validation line must appear.
pr024=$(grep -c "config validated (haproxy -c)" "$OUT/launch.log" 2>/dev/null || echo 0)
echo "pr024_haproxy_check=$pr024 (want >=1)"

canary_ok=0
if [ "$ready" = "1" ]; then
  PORT=$(cat "$(ls -t "$OUT"/run_logs/*/proxy_out/proxy_port 2>/dev/null | head -1)" 2>/dev/null)
  PORT=${PORT:-4001}
  echo "proxy_port=$PORT"
  RESP=$(curl -s --noproxy '*' -m 60 "http://localhost:$PORT/v1/completions" \
    -H 'Content-Type: application/json' \
    -d '{"model":"meta-llama/Meta-Llama-3-8B-Instruct","prompt":"The capital of France is","max_tokens":8}')
  echo "proxy_canary -> $RESP" | tee "$OUT/canary.json"
  echo "$RESP" | grep -q '"text"' && canary_ok=1

  # PR-009: kill HAProxy; the driver must notice and terminate (exit within ~30s).
  echo "--- killing HAProxy to test supervision (PR-009) ---"
  pkill -f "haproxy -f" 2>/dev/null
  supervised=0
  for i in $(seq 1 12); do
    grep -qE "Proxy .* exited with code .* while serving|terminating deployment" "$OUT/launch.log" && { supervised=1; break; }
    kill -0 $LPID 2>/dev/null || { supervised=1; break; }
    sleep 5
  done
  echo "pr009_proxy_supervised=$supervised"
fi

kill $LPID 2>/dev/null; pkill -f exaserve.driver 2>/dev/null; ray stop --force >/dev/null 2>&1
echo "=== HAPROXY SMOKE VERDICT ==="
echo "deploy_ready=$([ "$ready" = 1 ] && echo PASS || echo FAIL)"
echo "pr024_config_validated=$([ "${pr024:-0}" -ge 1 ] && echo PASS || echo FAIL)"
echo "proxy_canary=$([ "$canary_ok" = 1 ] && echo PASS || echo FAIL)"
echo "pr009_proxy_supervision=$([ "${supervised:-0}" = 1 ] && echo PASS || echo FAIL)"
echo "HAPROXY_SMOKE_DONE"
