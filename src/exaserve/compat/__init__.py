"""Compatibility profile, activation, and receipts (plan WP3, audit IMP-B04).

One immutable ``CompatibilityProfile`` describes the exact version/patch
combination a deployment is allowed to run. ``CompatibilityActivator`` is the
sole activation API: it verifies the base environment, applies the profile's
patches, and produces a typed ``CompatibilityReceipt`` per process role.

Fail-closed is the contract: an unknown version, a missing patch, or a failed
post-condition raises. READY requires a matching receipt from every required
role (see ``control.readiness``).
"""

from .profile import (  # noqa: F401
    CompatibilityProfile,
    PatchSpec,
    ProfileMismatch,
    default_profile,
)
from .receipt import CompatibilityReceipt, ReceiptStore  # noqa: F401
from .activator import ActivationError, CompatibilityActivator  # noqa: F401
