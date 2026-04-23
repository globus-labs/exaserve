#!/bin/bash
# Aurora environment setup for Ray-only work.
# Source this (don't exec) from a login-node shell or PBS job.

# ALCF Squid proxy — needed for HuggingFace downloads and any outbound
# HTTP from compute nodes. Scripts that do intra-cluster health checks
# should unset these after sourcing (see examples/launch.pbs).
export HTTP_PROXY="http://proxy.alcf.anl.gov:3128"
export HTTPS_PROXY="http://proxy.alcf.anl.gov:3128"
export http_proxy="http://proxy.alcf.anl.gov:3128"
export https_proxy="http://proxy.alcf.anl.gov:3128"
export ftp_proxy="http://proxy.alcf.anl.gov:3128"
export no_proxy="admin,polaris-adminvm-01,localhost,*.cm.polaris.alcf.anl.gov,polaris-*,*.polaris.alcf.anl.gov,*.alcf.anl.gov,127.0.0.1,0.0.0.0"

# Aurora modules providing the Python + Ray + vLLM stack, and Go (used
# by internal tooling).
module load frameworks
module load go
