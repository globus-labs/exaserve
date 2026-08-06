#!/bin/bash
# Start a tiny local Ray and exercise the compatibility receipt channel.
set -o pipefail
source ~/script/env_aurora
cd ~/exaserve || exit 1
export EXASERVE_DEPLOYMENT_ID="probe$$" EXASERVE_GENERATION=1
export EXASERVE_VLLM_PATCH_PP_LAYER_FILTER=1
export no_proxy="localhost,127.0.0.1,$(hostname)"
# Keep Ray small: this probe needs a control plane, not GPUs.
ulimit -s 8192
export RAYON_NUM_THREADS=1 RAY_num_server_call_thread=1
ray stop --force >/dev/null 2>&1
ray start --head --num-cpus=2 --num-gpus=0 --disable-usage-stats \
    --port 6399 --dashboard-port 8299 >/dev/null 2>&1
export RAY_ADDRESS="127.0.0.1:6399"
PYTHONPATH=$PWD/src python scripts/hardening/probe_receipt_channel.py
rc=$?
ray stop --force >/dev/null 2>&1
echo "RECEIPT_PROBE_DONE rc=$rc"
exit $rc
