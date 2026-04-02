# ClientLab Report: client-safe-zone

- Suite: `client_safe_zone`
- Point count: `8`
- Execution mode: `local`

## Operating Envelope

- Max stable RPS: `9803.98`
- Safe active budget estimate: `1205`
- Selected point with diagnosis=healthy and queue_fraction=0.007.

## Point Summaries

| Point | Requested RPS | Achieved RPS | Diagnosis | Queue Fraction | Safe Active Budget |
|---|---:|---:|---|---:|---:|
| max_active_requests=2048_service_time_ms=0_ff7e91 | 10000.00 | 10000.04 | healthy | 0.442 | 2 |
| max_active_requests=2048_service_time_ms=100_8b5d3f | 10000.00 | 9803.98 | healthy | 0.007 | 1205 |
| max_active_requests=2048_service_time_ms=1000_2faed8 | 10000.00 | 1989.94 | concurrency_saturated | 0.000 | 2391 |
| max_active_requests=2048_service_time_ms=2000_b41ccb | 10000.00 | 997.31 | concurrency_saturated | 0.000 | 2397 |
| max_active_requests=8192_service_time_ms=0_dc9d53 | 10000.00 | 10000.06 | healthy | 0.469 | 4 |
| max_active_requests=8192_service_time_ms=100_cfc39b | 10000.00 | 9803.91 | healthy | 0.022 | 1248 |
| max_active_requests=8192_service_time_ms=1000_6b6c6d | 10000.00 | 6891.58 | transport_conn_bound | 0.001 | 8336 |
| max_active_requests=8192_service_time_ms=2000_df26c7 | 10000.00 | 3532.89 | concurrency_saturated | 0.001 | 8519 |

## Key Questions

- What changed when concurrency increased? Achieved RPS changed by `-6467.15` between active budgets `2048` and `8192`, while queue fraction moved from `0.442` to `0.001`.
- Was the client stalled by its own queue, transport, the server, or the network? The strongest observed diagnosis in the completed study was `concurrency_saturated`.
- What active-request and connection budget is safe for this regime? A conservative estimate from the completed points is `8519` active requests.
