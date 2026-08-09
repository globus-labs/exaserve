#!/usr/bin/env python3
"""Exercise the selected generated overlay against the pinned Aurora stack."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _child_code(role: str) -> str:
    return f"""
import json, os, sys
from exaserve.compat.activator import CompatibilityActivator
report = CompatibilityActivator().activate({role!r})
files = {{}}
base_files = {{}}
overlay_root = os.path.realpath(os.environ['EXASERVE_COMPAT_OVERLAY_ROOT']) + os.sep
for name, module in sorted(sys.modules.items()):
    path = getattr(module, '__exaserve_overlay_source__', None)
    if isinstance(path, str):
        base_files[name] = getattr(module, '__file__', None)
    if not isinstance(path, str):
        candidate = getattr(module, '__file__', None)
        if isinstance(candidate, str) and os.path.realpath(candidate).startswith(overlay_root):
            path = candidate
    if isinstance(path, str) and os.path.realpath(path).startswith(overlay_root):
        files[name] = path
print(json.dumps({{
    'role': report.role,
    'profile_id': report.profile_id,
    'patch_results': dict(report.patch_results),
    'not_applicable': list(report.not_applicable),
    'overlay_modules': files,
    'base_files': base_files,
    'ray_services': ({{
        'ray_path': sys.modules['ray._private.services'].RAY_PATH,
        'ray_path_exists': os.path.isdir(sys.modules['ray._private.services'].RAY_PATH),
        'gcs_server': sys.modules['ray._private.services'].GCS_SERVER_EXECUTABLE,
        'gcs_server_exists': os.path.isfile(
            sys.modules['ray._private.services'].GCS_SERVER_EXECUTABLE
        ),
    }} if 'ray._private.services' in sys.modules else None),
}}, sort_keys=True))
"""


def _role_transition_code() -> str:
    return """
import json, os, sys
from exaserve.compat.activator import CompatibilityActivator
initial_role = os.environ['EXASERVE_COMPAT_ROLE']
os.environ['EXASERVE_COMPAT_ROLE'] = 'replica'
report = CompatibilityActivator().activate('replica')
overlay_root = os.path.realpath(os.environ['EXASERVE_COMPAT_OVERLAY_ROOT']) + os.sep
files = {}
base_files = {}
for name, module in sorted(sys.modules.items()):
    path = getattr(module, '__exaserve_overlay_source__', None)
    if isinstance(path, str):
        base_files[name] = getattr(module, '__file__', None)
    if not isinstance(path, str):
        candidate = getattr(module, '__file__', None)
        if isinstance(candidate, str) and os.path.realpath(candidate).startswith(overlay_root):
            path = candidate
    if isinstance(path, str) and os.path.realpath(path).startswith(overlay_root):
        files[name] = path
print(json.dumps({
    'initial_role': initial_role,
    'role': report.role,
    'profile_id': report.profile_id,
    'patch_results': dict(report.patch_results),
    'overlay_modules': files,
    'base_files': base_files,
}, sort_keys=True))
"""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    from exaserve.compat.generated_overlay import ROOT_ENV, materialize
    from exaserve.compat.producers import manifest_hash
    from exaserve.compat.profile import default_profile

    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    profile = default_profile("xpu")
    with tempfile.TemporaryDirectory(prefix="exaserve-overlay-probe-") as temporary:
        overlay = Path(temporary) / "overlay"
        overlay_manifest = materialize(profile, overlay)
        base_env = dict(os.environ)
        base_env.update(
            {
                ROOT_ENV: str(overlay),
                "EXASERVE_COMPAT_PROFILE_ID": profile.profile_id,
                "EXASERVE_COMPAT_MANIFEST_HASH": manifest_hash(profile),
                "EXASERVE_VENDOR": "xpu",
                "EXASERVE_VLLM_PATCH_PP_LAYER_FILTER": "1",
                "EXASERVE_XPU_VLLM_DISABLE_RAY_COMPILED_DAG": "1",
                "EXASERVE_XPU_VLLM_FORCE_RAY_CHANNEL_TYPE": "auto",
                "EXASERVE_RAY_SERVE_START_PROXY_TIMEOUT_S": "600",
                "EXASERVE_RAYLET_MAX_STARTUP_CONCURRENCY": "8",
                "EXASERVE_RAYLET_NUM_PRESTART_PYTHON_WORKERS": "8",
            }
        )
        python_path = [str(overlay), str(Path(__file__).resolve().parents[2] / "src")]
        if base_env.get("PYTHONPATH"):
            python_path.append(base_env["PYTHONPATH"])
        base_env["PYTHONPATH"] = os.pathsep.join(python_path)
        observations = []
        for role in ("deployment", "ray_head", "replica", "engine_core", "engine_worker"):
            role_env = dict(base_env)
            role_env["EXASERVE_COMPAT_ROLE"] = role
            started = time.monotonic()
            completed = subprocess.run(
                [sys.executable, "-c", _child_code(role)],
                check=False,
                capture_output=True,
                text=True,
                timeout=180.0,
                env=role_env,
            )
            parsed = None
            if completed.returncode == 0:
                lines = [line for line in completed.stdout.splitlines() if line.strip()]
                if lines:
                    try:
                        parsed = json.loads(lines[-1])
                    except json.JSONDecodeError:
                        parsed = None
            observations.append(
                {
                    "role": role,
                    "returncode": completed.returncode,
                    "duration_s": round(time.monotonic() - started, 6),
                    "stdout": completed.stdout,
                    "stderr": completed.stderr,
                    "report": parsed,
                }
            )
        transition_env = dict(base_env)
        transition_env["EXASERVE_COMPAT_ROLE"] = "ray_head"
        started = time.monotonic()
        completed = subprocess.run(
            [sys.executable, "-c", _role_transition_code()],
            check=False,
            capture_output=True,
            text=True,
            timeout=180.0,
            env=transition_env,
        )
        transition_report = None
        if completed.returncode == 0:
            lines = [line for line in completed.stdout.splitlines() if line.strip()]
            if lines:
                try:
                    transition_report = json.loads(lines[-1])
                except json.JSONDecodeError:
                    transition_report = None
        role_transition = {
            "from_role": "ray_head",
            "to_role": "replica",
            "returncode": completed.returncode,
            "duration_s": round(time.monotonic() - started, 6),
            "stdout": completed.stdout,
            "stderr": completed.stderr,
            "report": transition_report,
        }
        passed = all(
            item["returncode"] == 0
            and isinstance(item["report"], dict)
            and item["report"]["patch_results"]
            and item["report"]["overlay_modules"]
            and item["report"]["base_files"]
            and all(
                isinstance(path, str)
                and os.path.isfile(path)
                and not os.path.realpath(path).startswith(os.path.realpath(str(overlay)))
                for path in item["report"]["base_files"].values()
            )
            for item in observations
        ) and (
            role_transition["returncode"] == 0
            and isinstance(role_transition["report"], dict)
            and role_transition["report"]["initial_role"] == "ray_head"
            and role_transition["report"]["role"] == "replica"
            and role_transition["report"]["patch_results"]
            and role_transition["report"]["overlay_modules"]
            and role_transition["report"]["base_files"]
            and all(
                isinstance(path, str)
                and os.path.isfile(path)
                and not os.path.realpath(path).startswith(os.path.realpath(str(overlay)))
                for path in role_transition["report"]["base_files"].values()
            )
            and next(
                item["report"]["ray_services"]
                for item in observations
                if item["role"] == "ray_head"
            )["ray_path_exists"]
            and next(
                item["report"]["ray_services"]
                for item in observations
                if item["role"] == "ray_head"
            )["gcs_server_exists"]
        )
        result = {
            "schema_version": 1,
            "profile_id": profile.profile_id,
            "compatibility_manifest_hash": manifest_hash(profile),
            "overlay_manifest_hash": overlay_manifest["manifest_hash"],
            "overlay_entries": overlay_manifest["entries"],
            "observations": observations,
            "role_transition": role_transition,
            "verdict": "PASS" if passed else "FAIL",
        }
    serialized = json.dumps(result, indent=2, sort_keys=True) + "\n"
    output.write_text(serialized, encoding="utf-8")
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
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
