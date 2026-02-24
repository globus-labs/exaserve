import random
from typing import List, Optional

from ray.serve._private.request_router import PowerOfTwoChoicesRequestRouter
from ray.serve._private.request_router.request_router import (
    LocalityScope,
    PendingRequest,
    RunningReplica,
)


class LocalNodeRequestRouter(PowerOfTwoChoicesRequestRouter):
    """
    Routes each request using power-of-two-choices, but only among replicas
    colocated on the same node as the ProxyActor.

    The default PowerOfTwoChoicesRequestRouter uses prefer_local_node_routing
    as a soft preference: it tries local first, then falls back to all replicas
    cluster-wide. At 64+ nodes (768+ replicas), that global fallback means the
    proxy polls every replica with a short deadline, causing a timeout cascade
    that triggers a NoneType bug in _fulfill_pending_requests.

    This router hard-restricts candidate replicas to the local node, making
    polling overhead O(12) instead of O(N_replicas) regardless of cluster size.
    It falls back to global only if no local replicas are known yet (transient
    state during cluster startup).
    """

    async def choose_replicas(
        self,
        candidate_replicas: List[RunningReplica],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[List[RunningReplica]]:
        local_ids = self._colocated_replica_ids[LocalityScope.NODE]

        if local_ids:
            local_candidates = [
                r for r in candidate_replicas if r.replica_id in local_ids
            ]
            if local_candidates:
                chosen = random.sample(local_candidates, k=min(2, len(local_candidates)))
                return [chosen]

        # Startup fallback: local replicas not yet known, use global pool.
        if not candidate_replicas:
            return []
        chosen = random.sample(candidate_replicas, k=min(2, len(candidate_replicas)))
        return [chosen]
