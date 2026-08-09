"""Installed command entry points.

Serving starts only through ``exaserve-launch-cluster`` (or the deprecated
one-way ``exaserve-driver`` alias). There is deliberately no public command
that starts the isolated deployment child outside its owning supervisor.
"""

from __future__ import annotations


def launch_cluster() -> None:
    from .launcher import main

    main()


def driver() -> None:
    from .driver import main

    main()


def model_bcast() -> None:
    from .model_bcast import main

    raise SystemExit(main())


def ray_start() -> None:
    from .ray_start import main

    raise SystemExit(main())


def serve_submit() -> None:
    from .submit import serve_submit_main

    raise SystemExit(serve_submit_main())


def serve_url() -> None:
    from .submit import serve_url_main

    raise SystemExit(serve_url_main())


def status() -> None:
    from .status_cli import main

    raise SystemExit(main())
