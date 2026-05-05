"""Console-script entry points for aurora-rayserver.

Each entry point is wired in ``pyproject.toml`` under ``[project.scripts]``.

For pure-Python modules with a ``main()`` function (driver, model_bcast,
ray_start), the entry point delegates by invoking that ``main()`` so
``sys.argv`` parsing and exit-code propagation flow through unchanged.

The ``server`` module currently exposes its CLI via a ``if __name__ ==
"__main__":`` block rather than a ``main()`` function. Rather than
restructure 450 lines of script-style code in this commit, ``server()``
re-execs into the module via ``python -m`` so the ``__main__`` block runs
exactly as it does when launched by the driver.

For shell-script entries (``aurora-launch-cluster``) we resolve the
script's path inside the installed package's ``resources/`` data dir
via ``importlib.resources`` and ``os.execvp`` into bash. The .sh stays
the source of truth; the Python wrapper just locates it.
"""

from __future__ import annotations

import os
import sys
from importlib import resources


def launch_cluster() -> None:
    package_root = resources.files("aurora_rayserver")
    package_parent = package_root.parent
    script = package_root / "resources" / "launch_cluster.sh"
    os.environ.setdefault("AURORA_RAYSERVER_PACKAGE_ROOT", str(package_root))
    os.environ.setdefault("AURORA_RAYSERVER_PACKAGE_PARENT", str(package_parent))
    os.execvp("bash", ["bash", str(script), *sys.argv[1:]])


def driver() -> None:
    from . import driver as _driver
    _driver.main()


def server() -> None:
    # server.py has its CLI in a __main__ block; re-exec via -m so the
    # block fires exactly as the launcher invokes it.
    os.execvp(
        sys.executable,
        [sys.executable, "-m", "aurora_rayserver.server", *sys.argv[1:]],
    )


def model_bcast() -> None:
    from . import model_bcast as _mb
    raise SystemExit(_mb.main())


def ray_start() -> None:
    from . import ray_start as _rs
    raise SystemExit(_rs.main())


def serve_submit() -> None:
    from . import submit as _submit
    raise SystemExit(_submit.serve_submit_main())


def serve_url() -> None:
    from . import submit as _submit
    raise SystemExit(_submit.serve_url_main())
