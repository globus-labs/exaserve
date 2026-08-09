"""Deprecated command alias into the sole ExaServe composition root.

The former driver independently launched Ray, parsed a stdout readiness
marker, started gateways, and cleaned up a second process tree. WP13 removes
that architecture. Imports of Ray argv helpers use
``exaserve.control.ray_runtime``; invoking ``exaserve-driver`` is a one-way
alias to the canonical launcher and cannot select a legacy lifecycle.
"""

from __future__ import annotations

from typing import Optional, Sequence


def main(argv: Optional[Sequence[str]] = None) -> None:
    from .launcher import main as launcher_main

    launcher_main(argv)


if __name__ == "__main__":
    main()
