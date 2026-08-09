"""Compatibility profile, activation, and receipts (plan WP3, audit IMP-B04).

One immutable ``CompatibilityProfile`` describes the exact version/patch
combination a deployment is allowed to run. ``CompatibilityActivator`` is the
sole activation API; exact readiness evidence uses only receipt schema v2.

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
from .activator import (  # noqa: F401
    ActivationError,
    ActivationReport,
    CompatibilityActivator,
)
from .receipt_v2 import (  # noqa: F401
    CompatibilityReceiptV2,
    ExactReceiptLedger,
    ReceiptError,
)
