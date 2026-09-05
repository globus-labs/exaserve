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
    canonical_hash,
)
from ..plan.io import component_instance_binding_from_dict
from .atomic import ExclusiveLease, atomic_create_json, atomic_write_json, strict_json_load_path


class BindingStoreError(RuntimeError):
    pass


class ComponentBindingStore:
    """The composition root's sole durable writer for live slot bindings."""

    def __init__(
        self,
        run_dir: str,
        *,
        plan,
        binding,
        current_publish_batch_size: int = 1,
        event_publish_batch_size: int | None = None,
    ) -> None:
        event_publish_batch_size = (
            current_publish_batch_size
            if event_publish_batch_size is None
            else event_publish_batch_size
        )
        for name, value in (
            ("current_publish_batch_size", current_publish_batch_size),
            ("event_publish_batch_size", event_publish_batch_size),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        self.plan = plan
        self.binding = binding
        self.current_publish_batch_size = current_publish_batch_size
        self.event_publish_batch_size = event_publish_batch_size
        self.directory = os.path.join(run_dir, "component_bindings")
        self.events_dir = os.path.join(self.directory, "events")
        self.current_path = os.path.join(self.directory, "current.json")
        self.lease_path = os.path.join(self.directory, "writer.lease")
        os.makedirs(self.events_dir, mode=0o700, exist_ok=True)
        self._thread_lock = threading.RLock()
        self._current: dict[str, ComponentInstanceBinding] = {}
        self._sequence = 0
        self._published_sequence = 0
        self._pending_events: list[ComponentInstanceBinding] = []
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
                payload = strict_json_load_path(path)
            except (OSError, json.JSONDecodeError, ValueError, PlanError) as exc:
                raise BindingStoreError(
                    f"component binding event {path!r} is invalid: {exc}"
                ) from exc
            if isinstance(payload, dict) and payload.get("kind") == "component_binding_batch":
                events = self._decode_batch(payload, path)
            else:
                # Schema-v3 compatibility: existing immutable runs stored one
                # ComponentInstanceBinding per file.
                try:
                    events = (component_instance_binding_from_dict(payload),)
                except (TypeError, ValueError, PlanError) as exc:
                    raise BindingStoreError(
                        f"component binding event {path!r} is invalid: {exc}"
                    ) from exc
            for event in events:
                self._apply_recovered_event(event, path)
        self._publish_current()

    def _decode_batch(self, payload: dict, path: str) -> tuple[ComponentInstanceBinding, ...]:
        expected = {
            "schema_version",
            "kind",
            *self._identity(),
            "start_sequence",
            "end_sequence",
            "events",
            "batch_hash",
        }
        if set(payload) != expected or payload.get("schema_version") != SCHEMA_VERSION:
            raise BindingStoreError(f"component binding batch {path!r} has invalid fields")
        if any(payload.get(key) != value for key, value in self._identity().items()):
            raise BindingStoreError(f"component binding batch {path!r} has wrong identity")
        declared = payload.get("batch_hash")
        unhashed = {key: value for key, value in payload.items() if key != "batch_hash"}
        if not isinstance(declared, str) or declared != canonical_hash(unhashed):
            raise BindingStoreError(f"component binding batch {path!r} hash mismatch")
        raw_events = payload.get("events")
        if not isinstance(raw_events, list) or not raw_events:
            raise BindingStoreError(f"component binding batch {path!r} is empty")
        try:
            events = tuple(component_instance_binding_from_dict(item) for item in raw_events)
        except (TypeError, ValueError, PlanError) as exc:
            raise BindingStoreError(
                f"component binding batch {path!r} contains an invalid event: {exc}"
            ) from exc
        sequences = [event.binding_sequence for event in events]
        if (
            sequences != list(range(sequences[0], sequences[-1] + 1))
            or payload.get("start_sequence") != sequences[0]
            or payload.get("end_sequence") != sequences[-1]
        ):
            raise BindingStoreError(f"component binding batch {path!r} sequence range is invalid")
        return events

    def _apply_recovered_event(self, event: ComponentInstanceBinding, path: str) -> None:
        self._verify_identity(event)
        if event.binding_sequence != self._sequence + 1:
            raise BindingStoreError(f"component binding sequence is not contiguous at {path!r}")
        self._sequence = event.binding_sequence
        if event.state == "ACTIVE":
            self._current[event.key()] = event
        else:
            current = self._current.get(event.key())
            if current is not None and current.instance_id == event.instance_id:
                self._current.pop(event.key(), None)

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

    def _publish_pending_events(self) -> None:
        if not self._pending_events:
            return
        events = tuple(self._pending_events)
        payload = {
            "schema_version": SCHEMA_VERSION,
            "kind": "component_binding_batch",
            **self._identity(),
            "start_sequence": events[0].binding_sequence,
            "end_sequence": events[-1].binding_sequence,
            "events": [asdict(event) for event in events],
        }
        payload["batch_hash"] = canonical_hash(payload)
        path = os.path.join(
            self.events_dir,
            f"{events[0].binding_sequence:012d}-{events[-1].binding_sequence:012d}-"
            f"{payload['batch_hash']}.batch.json",
        )
        try:
            atomic_create_json(path, payload)
        except FileExistsError as exc:
            raise BindingStoreError(f"component binding batch already exists: {path}") from exc
        del self._pending_events[:]

    def _publish_barrier(self, *, publish_current: bool) -> None:
        with ExclusiveLease(
            self.lease_path, ttl_s=60, owner_note="component-instance-binding-writer"
        ):
            self._publish_pending_events()
            if publish_current and self._published_sequence != self._sequence:
                self._publish_current()

    def _append(self, event: ComponentInstanceBinding) -> None:
        self._verify_identity(event)
        if event.component_instance_binding_hash != event.compute_hash():
            raise BindingStoreError("component binding event is not finalized")
        if event.state == "ACTIVE":
            self._current[event.key()] = event
        else:
            current = self._current.get(event.key())
            if current is not None and current.instance_id == event.instance_id:
                self._current.pop(event.key(), None)
        self._sequence = event.binding_sequence
        self._pending_events.append(event)
        publish_current = (
            self._sequence - self._published_sequence >= self.current_publish_batch_size
        )
        if len(self._pending_events) >= self.event_publish_batch_size or publish_current:
            self._publish_barrier(publish_current=publish_current)

    def flush(self) -> None:
        """Group-commit pending events and publish the exact projection.

        Before this barrier, accepted in-memory receipts cannot authorize a
        durable READY record. Immutable batch segments are authoritative after
        the barrier; ``current.json`` remains a disposable projection. This
        turns an N-receipt burst into bounded group commits instead of N file
        and directory fsyncs. READY, pre-START, and shutdown call the barrier.
        """
        with self._thread_lock:
            self._publish_barrier(publish_current=True)

    def bind_receipt(self, receipt) -> ComponentInstanceBinding:
        """Bind a receipt; the explicit barrier group-commits the event."""
        with self._thread_lock:
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
        with self._thread_lock:
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
