# ADR-000: Initial production envelope and scale topology

**Status:** TECHNICALLY SELECTED, AWAITING EXPLICIT PRODUCT-OWNER SCOPE
APPROVAL. Last reconciled 2026-08-30. Final43 remains the packaged
one-/two-node hardware-evidence artifact. The `release/v0.4.0` successor clean
package gate passes, but its scale ladder cannot inherit final43 receipts. The
evidence-derived production maximum remains two nodes.

No worker or agent may fill the missing approval identity/evidence, and no
historical scale result may qualify this exact candidate.

## Proposed decision

| Dimension | Proposed first-release target | Gated / not proposed |
|---|---|---|
| Scheduler/site | Aurora PBS | Slurm, other sites |
| Accelerator | Intel XPU/PVC, 12 tiles per node | CUDA, ROCm |
| Engine | vLLM under frameworks 2025.3.1 | SGLang |
| Gateway | HAProxy | alternate gateways are validation/benchmark only |
| Exposure | proxied inference on trusted allocation network | direct Serve validation only; any public exposure |
| Request mode | non-streaming completion | streaming requires independent gates |
| Nodes | proposed target 64 through 1→2→4→16→64 | 128/256 require explicit expansion |
| Topology | TP=1/PP=1 and capability-gated PP | unsupported canonical combinations |

The 64-node target is a proposal, not an approval.

The multi-snapshot paper preview through candidate `ccccb82` demonstrates that
the successor architecture can reach exact READY, account requests, and clean
64 nodes across Envoy, LiteLLM, and HeadOnly validation modes. It does not
replace the proposed production proof: one immutable wheel must pass the
HAProxy/non-streaming 4/16/64 ladder declared by this ADR.

## Exact final43 evidence

Wheel
`1041be53eb5b5875d198d5ee6c6664718b4085775dcba99107873dd3d1fcdff2`
passed the clean installed-package gate and these Aurora cells:

| Tier | Exact-candidate result |
|---:|---|
| 1 node | null lifecycle/failure PASS; real XPU PASS; proxy no-delay on/off PASS |
| 2 nodes | null negative/fault matrix PASS; real PP=2 PASS; supervisor head/worker fault PASS |
| 4 nodes | not run for final43; authorization and a new predeclared row required |
| 16 / 64 nodes | not qualified; run only after an owner selects this envelope |

The compiled envelope records `supported_max_nodes=2`,
`qualification_target_nodes=64`, `qualification_target_approved=false`, and
`validation_mode=false`. That is the current evidence boundary, not an implicit
two-node product decision.

The site profile declares no parallel production envelopes for other gateways,
request modes, or streaming. Unmatched dimensions fail normal compilation.
Validation mode may create an experiment-only plan but cannot establish a
production support claim.

## Scale-topology rationale

The proposed pass excludes 128/256-node service and would qualify
1→2→4→16→64. This reduction is not frozen until an owner, timestamp, evidence
reference, and exact scope are recorded.

Historical Ray Serve/GCS actor-handle, controller-pressure, proxy-cliff,
shared-filesystem, and streaming-failure evidence remains relevant risk
context. It does not require a Ray fork in the qualified one/two-node path, nor
does it prove that the upstream limits are solved at 64 nodes. Those behaviors
must be remeasured at the approved boundary.

The final43 supervisor campaign also shows that, after catastrophic head or
worker loss, the pinned Ray driver may retry failed GCS/task notifications until
its 120-second reconnect timeout. ExaServe preserves first cause and bounded
cleanup, but does not claim instantaneous recovery.

`proxy_config.type: none` is validation-direct exposure only. An eval client
choosing a direct destination does not alter the deployment's compiled
gateway/exposure contract. Production direct exposure requires a new security,
readiness, failure, and scale decision.

## Qualification after approval

If 64 nodes is approved:

1. predeclare immutable final43 gates for 4, 16, and 64 nodes;
2. use the same HAProxy/non-streaming profile and declared client topology;
3. compare the 64-node result to a matched HAProxy baseline, not historical
   direct-routing data;
4. require exact membership, READY, canary, fault, terminal, and cleanup
   receipts at each tier; and
5. update the review manifest and ledger only after all selected cells pass.

A different ceiling changes the required ladder. Earlier candidates and old
research runs cannot substitute.

## Rejected alternatives

- Claiming 256 nodes from historical evidence: no exact-candidate qualification
  or approved topology exists.
- Selecting LiteLLM as the production gateway: its historical fake-streaming
  semantics do not satisfy the selected contract.
- Self-approving two nodes merely because it is measured: technical evidence
  cannot replace product scope authority.

## Revisit condition

Record the product-owner decision selecting 64 or another release target. Any
later expansion—128/256 nodes, streaming, public exposure, another engine,
gateway, scheduler, or accelerator—reopens this ADR and requires a new estimate,
profile, campaign, and candidate review.
