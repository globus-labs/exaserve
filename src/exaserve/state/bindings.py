"""Append-authoritative live component-instance binding store (WP2.4).

Each accepted receipt binds one current process/actor instance to one exact
planned slot.  Transitions are immutable, content-verified event artifacts;
``current.json`` is an atomically replaceable materialization that can always
be rebuilt from those events.  This avoids a two-file transaction pretending
to be atomic on a shared filesystem while still making the current projection
cheap for operators and readiness diagnostics.
"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import asdict, replace

from ..plan.contracts import (
    SCHEMA_VERSION,
    ComponentInstanceBinding,
    PlanError,
)
from ..plan.io import component_instance_binding_from_dict
from .atomic import ExclusiveLease, atomic_create_json, atomic_write_json, strict_json_load_path


class BindingStoreError(RuntimeError):
    pass


class ComponentBindingStore:
    """The composition root's sole durable writer for live slot bindings."""

    def __init__(
        self, run_dir: str, *, plan, binding, current_publish_batch_size: int = 1
    ) -> None:
        if (
            isinstance(current_publish_batch_size, bool)
            or not isinstance(current_publish_batch_size, int)
            or current_publish_batch_size < 1
        ):
            raise ValueError("current_publish_batch_size must be a positive integer")
        self.plan = plan
        self.binding = binding
        self.current_publish_batch_size = current_publish_batch_size
        self.directory = os.path.join(run_dir, "component_bindings")
        self.events_dir = os.path.join(self.directory, "events")
        self.current_path = os.path.join(self.directory, "current.json")
        self.lease_path = os.path.join(self.directory, "writer.lease")
        os.makedirs(self.events_dir, mode=0o700, exist_ok=True)
        self._thread_lock = threading.RLock()
        self._current: dict[str, ComponentInstanceBinding] = {}
        self._sequence = 0
        self._published_sequence = 0
        self._recover()

    def _identity(self) -> dict:
        return {
            "deployment_id": self.plan.deployment_id,
            "generation": self.binding.generation,
            "deployment_plan_hash": self.plan.deployment_plan_hash,
            "site_profile_hash": self.plan.site_profile_hash,
            "allocation_binding_hash": self.binding.allocation_binding_hash,
        }

    def _recover(self) -> None:
        for name in sorted(os.listdir(self.events_dir)):
            if not name.endswith(".json"):
                continue
            path = os.path.join(self.events_dir, name)
            try:
                event = component_instance_binding_from_dict(strict_json_load_path(path))
            except (OSError, json.JSONDecodeError, ValueError, PlanError) as exc:
                raise BindingStoreError(
                    f"component binding event {path!r} is invalid: {exc}"
                ) from exc
            self._verify_identity(event)
            if event.binding_sequence <= self._sequence:
                raise BindingStoreError(f"component binding sequence is not monotonic at {path!r}")
            self._sequence = event.binding_sequence
            if event.state == "ACTIVE":
                self._current[event.key()] = event
            else:
                current = self._current.get(event.key())
                if current is not None and current.instance_id == event.instance_id:
                    self._current.pop(event.key(), None)
        self._publish_current()

    def _verify_identity(self, event: ComponentInstanceBinding) -> None:
        expected = self._identity()
        mismatches = {
            key: (value, getattr(event, key))
            for key, value in expected.items()
            if getattr(event, key) != value
        }
        if mismatches:
            raise BindingStoreError(f"component binding identity mismatch: {mismatches}")

    def _publish_current(self) -> None:
        atomic_write_json(
            self.current_path,
            {
                "schema_version": SCHEMA_VERSION,
                **self._identity(),
                "revision": self._sequence,
                "bindings": [asdict(self._current[key]) for key in sorted(self._current)],
            },
        )
        self._published_sequence = self._sequence

    def _append(self, event: ComponentInstanceBinding) -> None:
        self._verify_identity(event)
        if event.component_instance_binding_hash != event.compute_hash():
            raise BindingStoreError("component binding event is not finalized")
        path = os.path.join(
            self.events_dir,
            f"{event.binding_sequence:012d}-{event.component_instance_binding_hash}.json",
        )
        try:
            atomic_create_json(path, asdict(event))
        except FileExistsError as exc:
            raise BindingStoreError(f"component binding event already exists: {path}") from exc
        if event.state == "ACTIVE":
            self._current[event.key()] = event
        else:
            current = self._current.get(event.key())
            if current is not None and current.instance_id == event.instance_id:
                self._current.pop(event.key(), None)
        self._sequence = event.binding_sequence
        if self._sequence - self._published_sequence >= self.current_publish_batch_size:
            self._publish_current()

    def flush(self) -> None:
        """Publish the exact in-memory projection after durable event appends.

        Immutable events are authoritative and individually durable.  The
        replaceable ``current.json`` projection is batched in production so a
        synchronized receipt burst does not rewrite an ever-growing Lustre
        file once per receipt and starve the control listener.  READY and
        shutdown call this barrier explicitly.
        """
        with (
            self._thread_lock,
            ExclusiveLease(
                self.lease_path, ttl_s=60, owner_note="component-instance-binding-writer"
            ),
        ):
            if self._published_sequence != self._sequence:
                self._publish_current()

    def bind_receipt(self, receipt) -> ComponentInstanceBinding:
        """Durably bind an already validated receipt to its planned slot."""
        with (
            self._thread_lock,
            ExclusiveLease(
                self.lease_path, ttl_s=60, owner_note="component-instance-binding-writer"
            ),
        ):
            current = self._current.get(receipt.receipt_requirement_id)
            if current is not None and current.instance_id == receipt.instance_id:
                return current
            supersedes = current.instance_id if current is not None else None
            if current is not None:
                self._append(
                    replace(
                        current,
                        state="SUPERSEDED",
                        binding_sequence=self._sequence + 1,
                        component_instance_binding_hash="",
                    ).finalize()
                )
            event = ComponentInstanceBinding(
                schema_version=SCHEMA_VERSION,
                **self._identity(),
                receipt_requirement_id=receipt.receipt_requirement_id,
                component_id=receipt.component_id,
                instance_id=receipt.instance_id,
                owner_scope=receipt.owner_scope,
                owner_rank=receipt.owner_rank,
                node_id=receipt.node_id,
                bound_at=time.time(),
                state="ACTIVE",
                binding_sequence=self._sequence + 1,
                supersedes_instance_id=supersedes,
            ).finalize()
            self._append(event)
            return event

    def revoke_slot(self, slot: str, *, reason: str = "lease lost") -> bool:
        """Append a revocation and remove a current instance fail-closed."""
        del reason  # reason belongs to lifecycle status; binding state is typed.
        with (
            self._thread_lock,
            ExclusiveLease(
                self.lease_path, ttl_s=60, owner_note="component-instance-binding-writer"
            ),
        ):
            current = self._current.get(slot)
            if current is None:
                return False
            self._append(
                replace(
                    current,
                    state="REVOKED",
                    binding_sequence=self._sequence + 1,
                    component_instance_binding_hash="",
                ).finalize()
            )
            return True

    def current(self) -> dict[str, ComponentInstanceBinding]:
        with self._thread_lock:
            return dict(self._current)
