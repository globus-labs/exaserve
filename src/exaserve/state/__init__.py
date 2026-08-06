"""Shared persistence contracts (plan WP2): atomic writes, leases, status."""

from .atomic import (  # noqa: F401
    ExclusiveLease,
    LeaseHeldError,
    atomic_write_bytes,
    atomic_write_json,
    atomic_write_text,
    atomic_write_yaml,
)
