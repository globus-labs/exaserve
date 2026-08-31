"""Process-independent clock identity helpers.

Wall time is useful operator evidence, but only monotonic timestamps from the
same kernel boot may be subtracted to form durations. Keeping boot identity in
one small module prevents status and scaling evidence from inventing subtly
different clock contracts.
"""

from __future__ import annotations


def system_boot_id() -> str:
    """Return the Linux boot identity, or an empty string when unavailable."""
    try:
        with open("/proc/sys/kernel/random/boot_id", encoding="ascii") as handle:
            value = handle.read().strip()
    except OSError:
        return ""
    return value if 0 < len(value) <= 128 else ""
