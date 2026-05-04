#!/bin/bash
# Check Ray Serve HTTP proxy health on all nodes in the cluster.
# Usage: bash scripts/debug_proxy_health.sh [nodefile]
# Outputs: per-node HTTP status (200=healthy, other=problem)

NODEFILE="${1:-$PBS_NODEFILE}"
if [ -z "$NODEFILE" ] || [ ! -f "$NODEFILE" ]; then
    echo "ERROR: No nodefile. Usage: $0 <nodefile>"
    exit 1
fi

UNIQUE_NODES=$(sort -u "$NODEFILE")
TOTAL=$(echo "$UNIQUE_NODES" | wc -l)
HEALTHY=0
UNHEALTHY=0
DOWN=0

echo "[ProxyHealth] Checking $TOTAL nodes on port 8000..."
echo "---"

for node in $UNIQUE_NODES; do
    # Try both HSN hostname and short hostname
    hsn="${node}.hsn.cm.aurora.alcf.anl.gov"
    status=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 3 --max-time 5 "http://${hsn}:8000/health" 2>/dev/null || echo "000")
    if [ "$status" = "200" ]; then
        HEALTHY=$((HEALTHY + 1))
    elif [ "$status" = "000" ]; then
        DOWN=$((DOWN + 1))
        echo "  DOWN: $node (connection failed)"
    else
        UNHEALTHY=$((UNHEALTHY + 1))
        echo "  UNHEALTHY: $node (HTTP $status)"
    fi
done

echo "---"
echo "[ProxyHealth] Total=$TOTAL Healthy=$HEALTHY Unhealthy=$UNHEALTHY Down=$DOWN"
