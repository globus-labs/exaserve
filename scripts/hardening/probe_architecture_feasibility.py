#!/usr/bin/env python3
"""Record reproducible evidence for the S01/S03 architecture decisions.

This probe is intentionally read-only.  It records the public Ray Serve
lifecycle surface, the provenance/rebuild properties of the installed Ray and
vLLM distributions, and two fault-injection observations that distinguish an
in-process deployment manager from the one-child isolation fallback.

It does not start Ray or touch accelerators, so it is safe to run as a brief
compute-node preflight in the pinned frameworks environment.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import inspect
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from typing import Any
from urllib.parse import unquote, urlparse


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _distribution(name: str) -> dict[str, Any]:
    distribution = importlib.metadata.distribution(name)
    files = tuple(distribution.files or ())
    root = Path(distribution.locate_file("")).resolve()
    metadata_files = {
        kind: [str(item) for item in files if str(item).endswith(suffix)]
        for kind, suffix in {
            "record": "RECORD",
            "wheel": "WHEEL",
            "direct_url": "direct_url.json",
        }.items()
    }
    native_suffixes = (".so", ".a", ".o")
    native_files = [str(item) for item in files if str(item).endswith(native_suffixes)]
    total_size = 0
    missing = []
    for item in files:
        path = Path(distribution.locate_file(item))
        try:
            total_size += path.stat().st_size
        except OSError:
            missing.append(str(item))
    direct_url = None
    direct_url_local_artifact = None
    if metadata_files["direct_url"]:
        path = Path(distribution.locate_file(metadata_files["direct_url"][0]))
        try:
            direct_url = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            direct_url = {"unreadable": str(exc)}
    if isinstance(direct_url, dict) and isinstance(direct_url.get("url"), str):
        parsed = urlparse(direct_url["url"])
        if parsed.scheme == "file":
            local_path = Path(unquote(parsed.path))
            expected = (
                direct_url.get("archive_info", {}).get("hashes", {}).get("sha256")
                if isinstance(direct_url.get("archive_info"), dict)
                else None
            )
            exists = local_path.is_file()
            readable = exists and os.access(local_path, os.R_OK)
            actual = _sha256(local_path) if readable else None
            direct_url_local_artifact = {
                "path": str(local_path),
                "exists": exists,
                "readable": readable,
                "expected_sha256": expected,
                "actual_sha256": actual,
                "hash_matches": bool(expected and actual == expected),
            }
    wheel_metadata = None
    if metadata_files["wheel"]:
        path = Path(distribution.locate_file(metadata_files["wheel"][0]))
        try:
            wheel_metadata = path.read_text(encoding="utf-8")
        except OSError as exc:
            wheel_metadata = f"unreadable: {exc}"
    return {
        "name": distribution.metadata["Name"],
        "version": distribution.version,
        "root": str(root),
        "file_count": len(files),
        "installed_size_bytes": total_size,
        "native_file_count": len(native_files),
        "missing_recorded_files": missing,
        **metadata_files,
        "direct_url_content": direct_url,
        "direct_url_local_artifact": direct_url_local_artifact,
        "wheel_metadata": wheel_metadata,
    }


def _lifecycle_surface() -> dict[str, dict[str, Any]]:
    # The package/profile check happens before this probe is accepted.  Imports
    # are local so importing the script itself remains lightweight.
    import ray
    from ray import serve

    result = {}
    for name, function in (
        ("ray.init", ray.init),
        ("serve.start", serve.start),
        ("serve.run", serve.run),
        ("serve.delete", serve.delete),
        ("serve.shutdown", serve.shutdown),
    ):
        signature = inspect.signature(function)
        parameters = set(signature.parameters)
        result[name] = {
            "signature": str(signature),
            "has_timeout_parameter": bool(parameters & {"timeout", "timeout_s"}),
            "has_cancellation_parameter": bool(
                parameters & {"cancel", "cancel_event", "cancellation_token"}
            ),
            "is_coroutine_function": inspect.iscoroutinefunction(function),
            "module": function.__module__,
        }
    return result


def _run_python(source: str, *, timeout_s: float) -> dict[str, Any]:
    started = time.monotonic()
    try:
        completed = subprocess.run(
            [sys.executable, "-c", source],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "timed_out": True,
            "elapsed_s": time.monotonic() - started,
            "stdout": (exc.stdout or b"").decode(errors="replace")
            if isinstance(exc.stdout, bytes)
            else (exc.stdout or ""),
            "stderr": (exc.stderr or b"").decode(errors="replace")
            if isinstance(exc.stderr, bytes)
            else (exc.stderr or ""),
        }
    return {
        "timed_out": False,
        "elapsed_s": time.monotonic() - started,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def _fault_isolation() -> dict[str, Any]:
    # In the preferred topology, deployment code and the global authority share
    # a process.  A representative native/extension fatal exit therefore
    # removes both; no Python exception boundary can intercept os._exit().
    in_process = _run_python(
        "import os; print('global-authority-live', flush=True); os._exit(86)",
        timeout_s=5.0,
    )

    # In the selected fallback, the authority survives the same deployment
    # failure, observes the exact exit, and remains able to persist a cause.
    isolated_source = """
import json, os, subprocess, sys, time
started = time.monotonic()
child = subprocess.Popen([sys.executable, '-c', 'import os; os._exit(86)'])
returncode = child.wait(timeout=2.0)
print(json.dumps({'authority_survived': True, 'child_returncode': returncode,
                  'elapsed_s': time.monotonic() - started}, sort_keys=True))
"""
    isolated = _run_python(isolated_source, timeout_s=5.0)

    # A synchronous in-process lifecycle call offers no killable unit.  This
    # injected hang demonstrates that the only OS-enforceable deadline is the
    # entire process.  The counterpart child can be terminated while its owner
    # survives and observes the signal-derived status.
    in_process_hang = _run_python(
        "import time; print('global-authority-entered-call', flush=True); time.sleep(60)",
        timeout_s=0.5,
    )
    isolated_hang_source = """
import json, subprocess, sys, time
child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
time.sleep(0.1)
child.terminate()
returncode = child.wait(timeout=2.0)
print(json.dumps({'authority_survived': True, 'child_returncode': returncode}, sort_keys=True))
"""
    isolated_hang = _run_python(isolated_hang_source, timeout_s=5.0)
    return {
        "fatal_exit_same_process": in_process,
        "fatal_exit_isolated_child": isolated,
        "hang_same_process": in_process_hang,
        "hang_isolated_child": isolated_hang,
    }


def _verdict(result: dict[str, Any]) -> dict[str, Any]:
    lifecycle = result["ray_serve_lifecycle_surface"]
    lifecycle_is_bounded = all(
        info["has_timeout_parameter"]
        or info["has_cancellation_parameter"]
        or info["is_coroutine_function"]
        for info in lifecycle.values()
    )
    fault = result["fault_isolation"]
    fatal_same = fault["fatal_exit_same_process"]
    fatal_child = fault["fatal_exit_isolated_child"]
    hang_same = fault["hang_same_process"]
    hang_child = fault["hang_isolated_child"]
    isolated_fatal_ok = (
        fatal_child.get("returncode") == 0
        and '"authority_survived": true' in fatal_child.get("stdout", "")
        and '"child_returncode": 86' in fatal_child.get("stdout", "")
    )
    isolated_hang_ok = hang_child.get(
        "returncode"
    ) == 0 and '"authority_survived": true' in hang_child.get("stdout", "")
    select_isolated = (
        not lifecycle_is_bounded
        and fatal_same.get("returncode") == 86
        and hang_same.get("timed_out") is True
        and isolated_fatal_ok
        and isolated_hang_ok
    )
    return {
        "stable_public_in_process_lifecycle_has_bounded_cancellation": lifecycle_is_bounded,
        "same_process_fatal_exit_removes_global_authority": fatal_same.get("returncode") == 86,
        "same_process_hang_requires_whole_process_deadline": hang_same.get("timed_out") is True,
        "isolated_child_preserves_authority_on_fatal_exit": isolated_fatal_ok,
        "isolated_child_preserves_authority_on_bounded_termination": isolated_hang_ok,
        "selected_boundary": "isolated-deployment-child" if select_isolated else "UNRESOLVED",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)

    result: dict[str, Any] = {
        "schema_version": 1,
        "python": sys.version,
        "executable": sys.executable,
        "hostname": os.uname().nodename,
        "distributions": {name: _distribution(name) for name in ("ray", "vllm")},
        "ray_serve_lifecycle_surface": _lifecycle_surface(),
        "fault_isolation": _fault_isolation(),
    }
    result["verdict"] = _verdict(result)
    serialized = json.dumps(result, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=output.parent, delete=False
    ) as stream:
        stream.write(serialized)
        stream.flush()
        os.fsync(stream.fileno())
        temporary = Path(stream.name)
    os.replace(temporary, output)
    print(
        json.dumps(
            {
                "output": str(output),
                "sha256": _sha256(output),
                "verdict": result["verdict"],
            },
            sort_keys=True,
        )
    )
    return 0 if result["verdict"]["selected_boundary"] != "UNRESOLVED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
