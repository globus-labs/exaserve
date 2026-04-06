# Evaluation Objectives

Items to address before publishing the Ray Serve baseline.

## Tail Latency Analysis
- Report p99/p999 from existing Go client histogram data
- Compare tail latency between MPI and Ray Serve baselines
- Investigate whether MPI trades tail latency for throughput

## Strong Scaling Results
- Fixed workload, increasing node count
- Show single-request latency reduction across nodes
- This is where MPI's low-latency scatter should be most visible

## Fault Tolerance Discussion
- Acknowledge MPI's weakness: single rank failure kills the job
- Ray Serve restarts actors automatically (health_check_period_s=30)
- Discuss trade-off: performance vs reliability in HPC context
