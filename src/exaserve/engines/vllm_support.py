"""Import-light lifecycle helpers owned by the vLLM backend.

This module deliberately has no Ray or vLLM import at module load.  Backend
contract tests, configuration tools, and alternate-engine environments can
therefore import it without loading the Ray Serve application.
"""

from __future__ import annotations

import inspect
from pathlib import Path
import re
import socket
from typing import Any


def async_engine_arg_supported(arg_name: str) -> bool:
    """Return whether the installed vLLM ``AsyncEngineArgs`` accepts a field."""
    from vllm.engine.arg_utils import AsyncEngineArgs

    try:
        signature = inspect.signature(AsyncEngineArgs.__init__)
    except (TypeError, ValueError):
        return hasattr(AsyncEngineArgs, arg_name)
    if any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    ):
        return True
    return arg_name in signature.parameters


def ray_node_ip() -> str | None:
    """Best-effort lookup of the IP Ray associates with this process's node."""
    try:
        import ray

        return str(ray.util.get_node_ip_address())
    except (ImportError, AttributeError, RuntimeError, ValueError):
        return None


def routed_node_ip() -> str:
    """Ask the kernel which local IPv4 address owns the default route."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            # UDP connect sends no packet but selects the route/interface.
            probe.connect(("10.255.255.255", 1))
            return str(probe.getsockname()[0])
    except OSError:
        # The hostname lookup is the explicit fallback and is allowed to fail;
        # an invented loopback address would make distributed startup hang.
        return socket.gethostbyname(socket.gethostname())


_WEIGHT_PATTERN = re.compile(r"Loading weights took ([\d.]+) seconds")
_KV_PATTERN = re.compile(
    r"init engine \(profile, create kv cache, warmup model\) took ([\d.]+) seconds"
)


def _current_ray_session_dir() -> Path:
    """Resolve this worker's exact Ray session, never the mutable latest link."""
    from ray._private.worker import global_worker

    node = getattr(global_worker, "node", None)
    if node is None or not hasattr(node, "get_session_dir_path"):
        raise RuntimeError("Ray worker exposes no current session directory")
    raw_path = node.get_session_dir_path()
    if not isinstance(raw_path, str) or not raw_path or not Path(raw_path).is_absolute():
        raise RuntimeError(f"Ray returned an invalid session directory: {raw_path!r}")
    session_dir = Path(raw_path).resolve(strict=True)
    if not session_dir.is_dir():
        raise RuntimeError(f"Ray session directory is not a directory: {session_dir}")
    return session_dir


def parse_engine_log(pid: int, *, session_dir: str | Path | None = None) -> dict[str, Any]:
    """Read optional phase timings from this exact Ray session and worker PID."""
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        raise ValueError("engine log pid must be a positive integer")
    try:
        if session_dir is None:
            exact_session = _current_ray_session_dir()
        else:
            if not isinstance(session_dir, (str, Path)):
                raise TypeError("session_dir must be a path")
            exact_session = Path(session_dir).resolve(strict=True)
            if not exact_session.is_dir():
                raise RuntimeError(f"Ray session directory is not a directory: {exact_session}")
        logs_dir = (exact_session / "logs").resolve(strict=True)
        if not logs_dir.is_dir() or logs_dir.parent != exact_session:
            raise RuntimeError(f"Ray logs directory is invalid: {logs_dir}")
    except (AttributeError, ImportError, OSError, RuntimeError, TypeError) as exc:
        return {"engine_log_error": f"{type(exc).__name__}: {exc}"}

    paths = sorted(
        path
        for path in logs_dir.glob(f"worker-*-{pid}.out")
        if path.is_file() and path.resolve().parent == logs_dir
    )
    if not paths:
        return {}
    if len(paths) != 1:
        return {
            "engine_log_error": (
                f"ambiguous Ray worker log for pid {pid} in exact session {exact_session}: "
                f"{[path.name for path in paths]}"
            )
        }
    result: dict[str, Any] = {}
    try:
        with paths[0].open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                match = _WEIGHT_PATTERN.search(line)
                if match:
                    result["weight_load_s"] = round(float(match.group(1)), 4)
                match = _KV_PATTERN.search(line)
                if match:
                    result["kv_cache_init_s"] = round(float(match.group(1)), 4)
    except OSError as exc:
        # These fields are optional diagnostics, but their loss must remain
        # visible rather than becoming a silent empty success.
        result["engine_log_error"] = f"{type(exc).__name__}: {exc}"
    return result
