"""Isolate the IMP-B04 receipt channel: head creates, remote worker publishes.

Run inside a lease with a local Ray. Prints a one-line verdict per stage so a
failure names the stage rather than disappearing into a swallowed exception.
"""

from __future__ import annotations

import os
import sys

os.environ.setdefault("EXASERVE_DEPLOYMENT_ID", "probe")
os.environ.setdefault("EXASERVE_GENERATION", "1")

import ray  # noqa: E402

from exaserve.compat.activator import CompatibilityActivator  # noqa: E402
from exaserve.compat.collector import (  # noqa: E402
    _get_collector,
    collector_name,
    create_receipt_collector,
    drain_receipts,
    publish_receipt,
)


@ray.remote(num_cpus=0)
def _worker_publish() -> dict:
    """Stand in for a Serve replica: activate + publish from another process."""
    out = {"pid": os.getpid(), "deployment_id": os.environ.get("EXASERVE_DEPLOYMENT_ID"),
           "collector_name": collector_name()}
    try:
        out["found_actor"] = _get_collector() is not None
    except Exception as exc:
        out["found_actor"] = f"raised {exc!r}"
    try:
        activator = CompatibilityActivator()
        receipt = activator.activate("replica", apply_fn=lambda: None)
        out["activated"] = True
        out["patch_results"] = receipt.patch_results
        out["not_applicable"] = list(receipt.not_applicable)
        out["published"] = publish_receipt(receipt)
    except Exception as exc:
        out["activated"] = f"{type(exc).__name__}: {exc}"
        out["published"] = False
    return out


def main() -> int:
    ray.init(address=os.environ.get("RAY_ADDRESS", "auto"), namespace="serve")
    print(f"head: collector_name={collector_name()}")
    actor = create_receipt_collector()
    print(f"head: create_receipt_collector -> {actor!r}")
    if actor is None:
        print("VERDICT: FAIL (head could not create the collector)")
        return 1
    head_activator = CompatibilityActivator()
    print(f"head: profile={head_activator.profile.name} "
          f"id={head_activator.profile.profile_id[:12]} "
          f"required(replica)={head_activator.profile.required_patch_ids('replica')}")

    result = ray.get(_worker_publish.remote())
    print(f"worker: {result}")

    drained = drain_receipts()
    print(f"head: drained {len(drained)} receipt(s)")
    for r in drained:
        print(f"  role={r.get('role')} profile={str(r.get('profile_id'))[:12]} "
              f"applied={r.get('patch_results')} n/a={r.get('not_applicable')}")

    from exaserve.compat.collector import receipt_from_dict
    from exaserve.compat.receipt import ReceiptStore

    store = ReceiptStore(head_activator.profile, os.environ["EXASERVE_DEPLOYMENT_ID"],
                         int(os.environ["EXASERVE_GENERATION"]))
    for payload in drained:
        receipt = receipt_from_dict(payload)
        ok, why = store.add(receipt) if receipt else (False, "unparseable")
        print(f"  store.add({payload.get('role')}) -> {ok} {why}")
    ok = bool(drained) and result.get("published") is True
    print(f"VERDICT: {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
