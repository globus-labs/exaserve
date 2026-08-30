# Deployment call graph

An annotated, diagrammed walkthrough of every function between
`exaserve-serve-submit` and a durable `READY`, worked against
[`examples/config.haproxy.yaml`](../../examples/config.haproxy.yaml) at two
nodes with one Llama-3-8B model.

Originally traced at commit `7b20715`, then reconciled on 2026-08-30 against
the `release/v0.4.0` source candidate descended from `ccccb82`. This checked-in
document is the durable authority; no private rendering or external artifact is
required to read it. If a future refactor moves any boundary below, update this
document with the corresponding contract tests.

This note is descriptive. The normative architecture remains
[`../PRODUCTION_HARDENING_EXECUTION_PLAN.md`](../PRODUCTION_HARDENING_EXECUTION_PLAN.md)
and the live verdict remains [`../hardening/STATUS.md`](../hardening/STATUS.md).

## Worked configuration

| Input | Value |
|---|---|
| Nodes | 2 (rank 0 is the allocation head) |
| Accelerators | 12 Intel PVC tiles per node |
| Model | `meta-llama/Meta-Llama-3-8B-Instruct`, TP=1, PP=1 |
| Gateway | HAProxy `:4001` → Ray Serve `:8000` on each node |

Derived by `plan.compiler`: **24 logical replicas** (12 per rank),
**26 Ray Serve applications** (24 model applications at `/<route>_r0…_r23` plus
two route-less proxy anchors), and **54 receipt requirements**
(1 supervisor + 1 gateway + 2 Ray daemons + 2 node supervisors + 24 replicas +
24 engine cores). A different TP, PP, or model count changes all four numbers.

## Ownership

The site adapter `exec`s one Python process and nothing else. Over a real-engine
startup that root launches two sequential finite staging children, followed by
three long-lived direct children: the rank launcher, deployment witness, and
gateway. The tree distinguishes OS launch (`├─`) from a control/API relationship
(`⇢`); Ray, not the deployment witness, creates the Serve actor processes.

```text
PBS job (2 nodes)
└─ exaserve.launcher · CompositionRoot                     [rank 0 only]
   ├─ source staging         finite, deadline-bounded, result-validated
   ├─ model staging          finite, starts after source staging completes
   ├─ mpiexec -n 2 -ppn 1   → exaserve.rank_main · NodeSupervisor  [each node]
   │                            └─ ray start --head | --address    [each node]
   │                                 └─ Ray-owned Serve proxy and actor workers
   ├─ exaserve.server_bootstrap · same process calls exaserve.server.main
   │     serve.start(EveryNode) + serve.run_many(26 apps)
   │        ⇢ asks Ray to create Serve HTTP proxy :8000 + EngineWorker ×12/node
   └─ haproxy -f … -db      inherits the listening fd the root already holds
```

## Phase order

The order is load-bearing; each entry names the owning call site.

| # | Phase | Entry point |
|---|---|---|
| 0 | Compile YAML once into an immutable plan, then submit | `submit.submit_serve` → `plan.compiler.compile_deployment_plan` |
| 1 | Boot the root; activate compatibility before any Ray/vLLM import | `launcher.run` → `CompatibilityActivator.activate("supervisor")` |
| 2 | Bind the allocation; open the shared status record | `CompositionRoot.bind_allocation` |
| 3 | Preflight the gateway config and **hold** `:4001`; bind the control listener | `CompositionRoot.gateway_argv`, `.bind_control_listener` |
| 4 | Stage source and models as owned finite subprocesses | `CompositionRoot.run_staging` |
| 5 | Launch both ranks; hold them at the START gate | `CompositionRoot.launch_ranks`, `.await_all_registered` |
| 6 | Start Ray head, then workers, then prove exact membership | `CompositionRoot.start_ray_cluster` |
| 7 | Start the isolated deployment child; bring up Ray Serve | `CompositionRoot.start_deployment` → `server_bootstrap.main` ⇢ same-process `server.main` |
| 8 | Establish the endpoint, start the gateway, converge readiness, commit `READY` | `launcher._drive_readiness` |
| 9 | Supervise the same predicate; unwind in reverse order | `RuntimeSupervisor.supervise`, `CompositionRoot.shutdown` |

Four orderings are deliberate and easy to break by accident:

- Compatibility activation precedes every `ray` and `vllm` import in every
  process role (`supervisor`, `node_supervisor`, `ray_head`/`ray_worker`,
  `deployment`, `replica`, `engine_core`).
- The gateway listener is bound in phase 3, before staging, so a port conflict
  fails in seconds instead of after a 30-minute model load. The root keeps that
  socket and passes the descriptor to HAProxy — there is no close-then-rebind
  window on the production path.
- The control listener is bound before any rank is launched. Three failed bind
  attempts launch nothing at all.
- The receipt ledger is constructed before the listener, or a receipt arriving
  during registration lands where nothing adjudicates it.

## Authority

- The deployment child (`server.py`) is a **witness**. It reports whether its
  own applications reached target and publishes that over a peer-PID-checked
  Unix socket. It owns no readiness or status decision.
- Each `NodeSupervisor` witnesses its own node's components.
- Only `CompositionRoot` sees the gateway, every rank session, the exact
  receipt set, and the advertised endpoint together, so only it commits
  `READY` — as one CAS-guarded write carrying the evidence that proves it.
- `READY` is continuously re-evaluated by the same predicate that granted it.
  Loss revokes the durable record and drives the declared terminal policy.

## Request path at steady state

```text
client → HAProxy :4001
           http-request set-var(txn.ridx) rand(24)
           set-path /<route>_r{ridx}<orig-path>
           balance leastconn across 2 servers, check GET /-/healthz
       → Ray Serve HTTP proxy :8000 (node 0 or node 1)
       → EngineWorker application _r{ridx}, node-pinned by the compiler
       → vLLM AsyncLLM on one PVC tile
```

Two independent decisions: HAProxy picks the *replica* by path rewrite, then
spreads TCP across node proxies. Health checks hit each proxy's own
`/-/healthz` and never a model route — funnelling every server's check through
one replica is what produced the 405B/256-node backend flap.

## Scope

This describes the code path, not a support claim. The evidence-backed maximum
for the current candidate is two nodes; see
[`../hardening/STATUS.md`](../hardening/STATUS.md) and
[`../hardening/decisions/ADR-000-production-envelope.md`](../hardening/decisions/ADR-000-production-envelope.md).
