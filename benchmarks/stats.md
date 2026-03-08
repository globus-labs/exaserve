# Network Statistics Reference

## Metrics Collected (from `/proc/net/dev`)

| Field | Description |
|-------|-------------|
| `rx_bytes` / `tx_bytes` | Total bytes received/transmitted (cumulative counter). Deltas between samples give instantaneous throughput. |
| `rx_packets` / `tx_packets` | Total packets received/transmitted. Useful for computing packets/second and average packet size. |
| `rx_errors` / `tx_errors` | Receive/transmit errors (CRC, framing, etc.). Non-zero values indicate hardware or driver issues. |
| `rx_drops` / `tx_drops` | Packets dropped due to buffer overflows. Non-zero drops mean the interface or kernel cannot keep up. |

## Derived Metrics

| Metric | Formula | What it tells you |
|--------|---------|-------------------|
| **Bandwidth (GB/s)** | `delta_bytes / delta_time / 1e9` | Primary throughput metric. Compare against NIC limit. |
| **Packets/s (PPS)** | `delta_packets / delta_time` | Per-packet overhead indicator. High PPS with low bandwidth = small packets. |
| **Avg packet size (bytes)** | `delta_bytes / delta_packets` | If < 1 KB, the workload is likely packet-rate limited, not bandwidth limited. |

## Aurora Slingshot-11 Hardware Specs

| Spec | Value |
|------|-------|
| NICs per node | 2 (`hsn0`, `hsn1`) |
| Per-NIC bandwidth | ~25 GB/s per direction (200 Gbps) |
| Per-node aggregate | ~50 GB/s per direction (400 Gbps) |
| Per-NIC PPS | ~30M packets/s |
| Topology | Dragonfly |
| MTU | 9000 (jumbo frames on HSN) |

## Interpreting "Is the Interface Overwhelmed?"

The plots include horizontal dashed lines at the per-NIC hardware limits. Use these zones:

| Zone | Per-NIC utilization | What it means |
|------|---------------------|---------------|
| **Green** | < 50% (< 12.5 GB/s) | Network is not a bottleneck. |
| **Yellow** | 50–80% (12.5–20 GB/s) | Approaching limits. May see increased latency under bursty traffic. |
| **Red** | > 80% (> 20 GB/s) | Network is likely the bottleneck. Expect increased latency and potential drops. |

### Key signals to look for

1. **Client TX vs Stub RX symmetry** — Total client TX should approximately equal total stub RX. Large discrepancies indicate packet loss or retransmission.

2. **Per-node balance** — If one stub node shows much higher RX than others, the Go client's round-robin may not be balanced, or network paths have different latencies.

3. **Drops > 0** — Any non-zero drop count means the kernel is dropping packets. This is a hard signal that the interface is overwhelmed.

4. **Small packet size** — If average packet size is 200–500 bytes (HTTP headers only), the bottleneck is packet rate, not bandwidth. Slingshot can handle ~30M PPS/NIC but HTTP request overhead adds up.

5. **Loopback (`lo`) traffic** — If significant traffic appears on `lo`, some requests may be routing locally instead of to remote nodes, indicating a configuration issue.

## Equivalence to `sar -n DEV 1`

The metrics collected are equivalent to `sar -n DEV 1` output:

| `sar` field | Our equivalent |
|-------------|----------------|
| `rxkB/s` | `bandwidth_rx_gbs * 1e6` |
| `txkB/s` | `bandwidth_tx_gbs * 1e6` |
| `rxpck/s` | `pps_rx` |
| `txpck/s` | `pps_tx` |

We use `/proc/net/dev` directly because it does not require the `sysstat` package and provides finer control over which interfaces and polling intervals to use.
