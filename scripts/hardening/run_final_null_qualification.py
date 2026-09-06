#!/usr/bin/env python3
"""Final packaged-artifact lifecycle qualification on Aurora.

The site shell is only responsible for sourcing ``env_aurora`` and invoking
this program.  This harness then starts the installed ExaServe composition
root directly, observes only the canonical status/receipt artifacts, and
records a self-contained evidence bundle for fresh generations.  The default
lifecycle profile covers:

* READY -> advertised HAProxy canary -> operator drain -> STOPPED; and
* READY -> owned HAProxy death -> FAILED with nonzero process exit.

The explicit two-node profile additionally covers exact worker loss, gateway
port exclusion, and a worker-proxy port conflict that must withhold READY. The
four-node real profile proves disjoint PP/TP bundles, every spawned EngineCore
and engine-worker patch receipt, and fail-closed behavior after one exact
replica process is killed.

It supports an explicit null-compute control-plane cell and real-vLLM cells;
the selected mode must agree with the immutable plan. It is intentionally
standard-library-only outside the packaged ExaServe artifact supplied with
``--bootstrap``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import select
import shlex
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import urllib.error
import urllib.request
import zipfile


_TERMINAL = {"FAILED", "STOPPED", "CANCELLED"}
_HEX = frozenset("0123456789abcdef")
_PLAN_FIELDS = {
    "schema_version",
    "created_at",
    "candidate",
    "harness",
    "gates",
}
_CANDIDATE_FIELDS = {
    "release_path",
    "artifact_manifest_path",
    "artifact_manifest_sha256",
    "wheel_path",
    "wheel_sha256",
    "sdist_path",
    "sdist_sha256",
    "bootstrap_path",
    "site_profile_hash",
    "compatibility_profile_hash",
    "compatibility_manifest_hash",
}
_HARNESS_FIELDS = {"path", "sha256"}
_GATE_FIELDS = {
    "gate_id",
    "lane",
    "logical_nodes",
    "physical_allocation_nodes",
    "acquisition_source",
    "queue",
    "lease_ttl",
    "expected_runtime",
    "node_hours",
    "attempt_limit",
    "attempt",
    "output_path",
    "engine_mode",
    "scenario_profile",
    "ready_timeout_s",
    "partial_observation_s",
    "config_path",
    "config_sha256",
    "deployment_plan_path",
    "deployment_plan_sha256",
    "site_profile_path",
    "site_profile_sha256",
    "clean_state_reset_method",
    "retry_reason_policy",
    "expected_observations",
}


def _add_note(error: BaseException, message: str) -> None:
    add_note = getattr(error, "add_note", None)
    if callable(add_note):
        add_note(message)
    else:  # Python 3.10 compatibility for the supported portable floor.
        notes = list(getattr(error, "__notes__", ()))
        notes.append(message)
        error.__notes__ = notes


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except BaseException as exc:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        except OSError as cleanup_exc:
            _add_note(exc, f"atomic temporary cleanup also failed: {cleanup_exc}")
        raise


def _atomic_json(path: Path, payload: object) -> None:
    _atomic_text(path, json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n")


def _read_json(path: Path) -> dict:
    from exaserve.state.atomic import strict_json_load

    with path.open(encoding="utf-8") as handle:
        payload = strict_json_load(handle)
    if not isinstance(payload, dict):
        raise RuntimeError(f"expected an object at {path}")
    return payload


def _require_exact_shape(value: object, fields: set[str], context: str) -> dict:
    if not isinstance(value, dict):
        raise RuntimeError(f"{context} must be an object")
    unknown = sorted(set(value) - fields)
    missing = sorted(fields - set(value))
    if unknown or missing:
        raise RuntimeError(f"{context} shape mismatch: unknown={unknown}, missing={missing}")
    return value


def _require_sha256(value: object, context: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in _HEX for char in value):
        raise RuntimeError(f"{context} must be a lowercase SHA-256 digest")
    return value


def _declared_path(
    repo_root: Path,
    value: object,
    context: str,
    *,
    kind: str,
) -> Path:
    if not isinstance(value, str) or not value or Path(value).is_absolute():
        raise RuntimeError(f"{context} must be a non-empty repository-relative path")
    path = (repo_root / value).resolve()
    try:
        path.relative_to(repo_root)
    except ValueError as exc:
        raise RuntimeError(f"{context} escapes the repository: {value!r}") from exc
    if kind == "file" and not path.is_file():
        raise RuntimeError(f"{context} is not a file: {path}")
    if kind == "directory" and not path.is_dir():
        raise RuntimeError(f"{context} is not a directory: {path}")
    if kind not in {"file", "directory", "output"}:
        raise AssertionError(f"unsupported declared path kind {kind!r}")
    return path


def _expected_observations(engine_mode: str, scenario_profile: str) -> list[str]:
    common = [
        "fresh generation reaches canonical READY",
        f"{engine_mode} engine receipts satisfy the exact planned slot set",
        "advertised HAProxy endpoint returns a typed completion",
        "exact receipt slots and per-rank source staging are complete",
    ]
    if scenario_profile == "two_node":
        return common + [
            "SIGTERM drains and publishes STOPPED with exit 143",
            "a fresh restart reaches READY",
            "owned gateway death publishes process_dead evidence and FAILED with nonzero exit",
            "exact rank-1 Ray worker death publishes FAILED with nonzero non-143 exit",
            "an already-owned gateway port fails closed before READY",
            "exact two-node Ray membership cannot publish READY while the worker Serve port is held",
            "the bounded partial-readiness observation cancels cleanly if no typed failure wins first",
        ]
    if scenario_profile == "four_node_real":
        return common + [
            "SIGTERM drains and publishes STOPPED with exit 143",
            "a fresh restart reaches READY",
            "compiled TP/PP replica bundles are disjoint across exactly four ranks",
            "every spawned EngineCore and engine worker self-attests EN-01 and all resolved patches",
            "one identity-fenced replica SIGKILL durably revokes READY",
            "replica loss either re-attests a new exact instance within recovery policy or fails typed and cleanly",
        ]
    if scenario_profile == "lifecycle":
        return common + [
            "SIGTERM drains and publishes STOPPED with exit 143",
            "a fresh restart reaches READY",
            "owned gateway death publishes process_dead evidence and FAILED with nonzero exit",
        ]
    raise RuntimeError(f"unsupported scenario profile {scenario_profile!r}")


def _load_declared_gate(
    experiment_plan_path: Path,
    gate_id: str,
    *,
    repo_root: Path | None = None,
    harness_path: Path | None = None,
) -> tuple[dict, dict, dict[str, Path]]:
    """Load one exact predeclared gate and verify every immutable input byte."""

    root = (repo_root or Path(__file__).resolve().parents[2]).resolve()
    current_harness = (harness_path or Path(__file__)).resolve()
    plan_path = experiment_plan_path.resolve()
    try:
        plan_path.relative_to(root)
    except ValueError as exc:
        raise RuntimeError("experiment plan must be inside the repository") from exc
    document = _require_exact_shape(_read_json(plan_path), _PLAN_FIELDS, "experiment plan")
    if document["schema_version"] != 2:
        raise RuntimeError("experiment plan schema_version must be 2")
    if not isinstance(document["created_at"], str) or not document["created_at"].strip():
        raise RuntimeError("experiment plan created_at must be non-empty")

    candidate = _require_exact_shape(
        document["candidate"], _CANDIDATE_FIELDS, "experiment plan candidate"
    )
    harness = _require_exact_shape(document["harness"], _HARNESS_FIELDS, "experiment plan harness")
    harness_file = _declared_path(root, harness["path"], "harness.path", kind="file")
    if harness_file != current_harness:
        raise RuntimeError(
            f"declared harness {harness_file} is not running harness {current_harness}"
        )
    if _sha256_file(harness_file) != _require_sha256(harness["sha256"], "harness.sha256"):
        raise RuntimeError("running qualification harness bytes differ from the declaration")

    paths = {
        "release": _declared_path(
            root, candidate["release_path"], "candidate.release_path", kind="directory"
        ),
        "artifact_manifest": _declared_path(
            root,
            candidate["artifact_manifest_path"],
            "candidate.artifact_manifest_path",
            kind="file",
        ),
        "wheel": _declared_path(root, candidate["wheel_path"], "candidate.wheel_path", kind="file"),
        "sdist": _declared_path(root, candidate["sdist_path"], "candidate.sdist_path", kind="file"),
        "bootstrap": _declared_path(
            root, candidate["bootstrap_path"], "candidate.bootstrap_path", kind="directory"
        ),
    }
    for name in (
        "artifact_manifest_sha256",
        "wheel_sha256",
        "sdist_sha256",
        "site_profile_hash",
        "compatibility_profile_hash",
        "compatibility_manifest_hash",
    ):
        _require_sha256(candidate[name], f"candidate.{name}")
    for path_name, digest_name in (
        ("artifact_manifest", "artifact_manifest_sha256"),
        ("wheel", "wheel_sha256"),
        ("sdist", "sdist_sha256"),
    ):
        if _sha256_file(paths[path_name]) != candidate[digest_name]:
            raise RuntimeError(f"candidate {path_name} bytes differ from the declaration")

    gates = document["gates"]
    if not isinstance(gates, list) or not gates:
        raise RuntimeError("experiment plan gates must be a non-empty list")
    normalized: list[dict] = []
    ids: list[str] = []
    for index, raw in enumerate(gates):
        gate = _require_exact_shape(raw, _GATE_FIELDS, f"experiment plan gates[{index}]")
        normalized.append(gate)
        ids.append(str(gate["gate_id"]))
    if len(ids) != len(set(ids)):
        raise RuntimeError("experiment plan gate_id values must be unique")
    matches = [gate for gate in normalized if gate["gate_id"] == gate_id]
    if len(matches) != 1:
        raise RuntimeError(f"gate_id {gate_id!r} is not declared exactly once")
    gate = matches[0]

    if gate["lane"] != "FINAL":
        raise RuntimeError("qualification gate lane must be FINAL")
    for name in ("logical_nodes", "physical_allocation_nodes", "node_hours"):
        if isinstance(gate[name], bool) or not isinstance(gate[name], int) or gate[name] <= 0:
            raise RuntimeError(f"gate {name} must be a positive integer")
    if gate["logical_nodes"] != gate["physical_allocation_nodes"]:
        raise RuntimeError("lower-tier gate cannot silently subset a larger allocation")
    if gate["acquisition_source"] not in {"subjob", "interactive_pbs"}:
        raise RuntimeError("gate acquisition_source is unsupported")
    if gate["queue"] not in {"capacity", "debug"}:
        raise RuntimeError("gate queue is unsupported")
    for name in (
        "lease_ttl",
        "expected_runtime",
        "clean_state_reset_method",
        "retry_reason_policy",
    ):
        if not isinstance(gate[name], str) or not gate[name].strip():
            raise RuntimeError(f"gate {name} must be a non-empty string")
    for name in ("attempt_limit", "attempt"):
        if isinstance(gate[name], bool) or not isinstance(gate[name], int) or gate[name] <= 0:
            raise RuntimeError(f"gate {name} must be a positive integer")
    if gate["attempt"] > gate["attempt_limit"]:
        raise RuntimeError("gate attempt exceeds its predeclared attempt_limit")
    if gate["engine_mode"] not in {"null", "real"}:
        raise RuntimeError("gate engine_mode is unsupported")
    if gate["scenario_profile"] not in {"lifecycle", "two_node", "four_node_real"}:
        raise RuntimeError("gate scenario_profile is unsupported")
    for name in ("ready_timeout_s", "partial_observation_s"):
        value = gate[name]
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value <= 0
        ):
            raise RuntimeError(f"gate {name} must be finite and positive")
    expected = _expected_observations(gate["engine_mode"], gate["scenario_profile"])
    if gate["expected_observations"] != expected:
        raise RuntimeError(
            "gate expected_observations differs from the executable scenario contract"
        )

    paths.update(
        {
            "output": _declared_path(root, gate["output_path"], "gate.output_path", kind="output"),
            "config": _declared_path(root, gate["config_path"], "gate.config_path", kind="file"),
            "deployment_plan": _declared_path(
                root,
                gate["deployment_plan_path"],
                "gate.deployment_plan_path",
                kind="file",
            ),
            "site_profile": _declared_path(
                root, gate["site_profile_path"], "gate.site_profile_path", kind="file"
            ),
        }
    )
    for path_name, digest_name in (
        ("config", "config_sha256"),
        ("deployment_plan", "deployment_plan_sha256"),
        ("site_profile", "site_profile_sha256"),
    ):
        digest = _require_sha256(gate[digest_name], f"gate.{digest_name}")
        if _sha256_file(paths[path_name]) != digest:
            raise RuntimeError(f"gate {path_name} bytes differ from the declaration")
    return document, gate, paths


def _pin_bootstrap_environment(bootstrap: Path) -> str:
    """Make every cwd-changing child import the exact declared wheel tree."""

    absolute = str(bootstrap.resolve())
    os.environ["PYTHONPATH"] = absolute
    return absolute


def _validated_nodes(expected: int, *, acquisition_source: str) -> tuple[str, ...]:
    job_id = os.environ.get("PBS_JOBID", "").strip()
    nodefile = os.environ.get("PBS_NODEFILE", "").strip()
    if not job_id or not nodefile or not os.path.isfile(nodefile):
        raise RuntimeError("a readable PBS_NODEFILE and PBS_JOBID are required")
    if acquisition_source == "subjob":
        if os.environ.get("AURORA_SUBJOB") != "1":
            raise RuntimeError("subjob qualification requires AURORA_SUBJOB=1")
    elif acquisition_source == "interactive_pbs":
        if os.environ.get("AURORA_SUBJOB") == "1":
            raise RuntimeError("interactive_pbs was declared inside a subjob lease")
        if os.environ.get("PBS_ENVIRONMENT") != "PBS_INTERACTIVE":
            raise RuntimeError(
                "interactive_pbs qualification requires PBS_ENVIRONMENT=PBS_INTERACTIVE"
            )
    else:
        raise ValueError(f"unsupported acquisition source {acquisition_source!r}")
    with open(nodefile, encoding="utf-8") as handle:
        nodes = tuple(dict.fromkeys(line.strip() for line in handle if line.strip()))
    if len(nodes) != expected:
        raise RuntimeError(f"expected exactly {expected} leased node(s), observed {nodes}")
    host = socket.gethostname().split(".", 1)[0]
    if host not in {node.split(".", 1)[0] for node in nodes}:
        raise RuntimeError(f"current host {host!r} is not in PBS_NODEFILE {nodes}")
    if "ONEAPI_DEVICE_SELECTOR" in os.environ:
        raise RuntimeError("ONEAPI_DEVICE_SELECTOR must be absent on Aurora")
    return nodes


def _command_identity(argv: list[str]) -> dict[str, object]:
    executable = shutil.which(argv[0]) if not os.path.isabs(argv[0]) else argv[0]
    if not executable or not os.path.isfile(executable):
        raise RuntimeError(f"required executable is unavailable: {argv[0]}")
    return {
        "argv": argv,
        "resolved_executable": os.path.realpath(executable),
        "executable_sha256": _sha256_file(Path(executable)),
    }


def _verify_bootstrap(wheel: Path, bootstrap: Path) -> dict[str, object]:
    """Prove the imported package tree is byte-identical to the named wheel."""
    with zipfile.ZipFile(wheel) as archive:
        expected = {
            name: hashlib.sha256(archive.read(name)).hexdigest()
            for name in archive.namelist()
            if name.startswith("exaserve/") and not name.endswith("/")
        }
    package_root = bootstrap / "exaserve"
    if not package_root.is_dir() or not expected:
        raise RuntimeError("wheel/bootstrap lacks the ExaServe package tree")
    observed: dict[str, str] = {}
    for path in package_root.rglob("*"):
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode):
            raise RuntimeError(f"bootstrap package member must not be a symlink: {path}")
        if stat.S_ISDIR(metadata.st_mode) or "__pycache__" in path.parts or path.suffix == ".pyc":
            continue
        if not stat.S_ISREG(metadata.st_mode):
            raise RuntimeError(f"bootstrap package member is not a regular file: {path}")
        relative = f"exaserve/{path.relative_to(package_root).as_posix()}"
        observed[relative] = _sha256_file(path)
    if observed != expected:
        missing = sorted(set(expected) - set(observed))
        unexpected = sorted(set(observed) - set(expected))
        changed = sorted(
            name for name in set(expected) & set(observed) if expected[name] != observed[name]
        )
        raise RuntimeError(
            "bootstrap is not byte-identical to the named wheel: "
            f"missing={missing}, unexpected={unexpected}, changed={changed}"
        )
    canonical = json.dumps(observed, sort_keys=True, separators=(",", ":")).encode()
    return {
        "package_members": len(observed),
        "package_tree_sha256": hashlib.sha256(canonical).hexdigest(),
    }


def _process_group_exists(process_group: int) -> bool:
    try:
        os.killpg(process_group, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _pals_argv(
    nodes: tuple[str, ...],
    executable: str,
    *application_argv: str,
    application_environment: dict[str, str] | None = None,
    working_directory: str = "/tmp",
) -> list[str]:
    if not nodes or any(not node or any(char.isspace() for char in node) for node in nodes):
        raise ValueError("PALS qualification nodes are invalid")
    if not os.path.isabs(executable) or not os.path.isabs(working_directory):
        raise ValueError("PALS qualification executable/cwd must be absolute")
    environment = dict(application_environment or {})
    # PMIx consults HOME/.pmix before the application bootstrap can clear its
    # own environment. Keep that pre-exec lookup on node-local scratch even
    # for capsule helpers whose Python code later installs a narrower HOME.
    environment.setdefault("HOME", "/tmp")
    environment.setdefault("TMPDIR", "/tmp")
    from exaserve.site import AURORA_PMIX_PREPARED_ENVIRONMENT

    for name, expected in AURORA_PMIX_PREPARED_ENVIRONMENT:
        if name in environment and environment[name] != expected:
            raise ValueError(f"PALS qualification {name} is not Aurora-qualified")
        environment[name] = expected
    prefix = ["mpiexec", "--genvnone", "--envnone", "--shared"]
    for name, value in sorted(environment.items()):
        if (
            not name
            or not name.replace("_", "a").isalnum()
            or not isinstance(value, str)
            or "\x00" in value
        ):
            raise ValueError("PALS qualification environment is invalid")
        prefix.extend(("--genv", f"{name}={value}"))
    return [
        *prefix,
        "-n",
        str(len(nodes)),
        "--ppn",
        "1",
        "--cpu-bind",
        "none",
        "--hosts",
        ",".join(nodes),
        "--wdir",
        working_directory,
        executable,
        *application_argv,
    ]


def _signal_local_generation_pid(
    pid: int,
    signal_name: str,
    *,
    deployment_id: str,
    generation: int,
    plan_hash: str,
    run_dir: Path,
) -> dict[str, object]:
    """Signal one PID only after a same-process exact-generation check.

    A self-attested receipt selects the intended process, but the PID may have
    exited before fault injection. Re-reading the kernel-published generation
    identity and Linux start ticks in the helper that performs ``kill`` fences
    both an unrelated process and PID reuse.
    """
    if signal_name not in {"TERM", "KILL"}:
        raise ValueError(f"unsupported generation signal {signal_name!r}")
    identity = _generation_process_identity(
        pid,
        deployment_id=deployment_id,
        generation=generation,
        plan_hash=plan_hash,
        run_dir=run_dir,
    )
    if identity is None:
        raise RuntimeError(
            f"PID {pid} no longer carries the exact generation identity; refusing to signal"
        )
    os.kill(pid, getattr(signal, f"SIG{signal_name}"))
    return {
        "schema_version": 1,
        "hostname": socket.gethostname(),
        "deployment_id": deployment_id,
        "generation": generation,
        "deployment_plan_hash": plan_hash,
        "run_dir": str(run_dir.resolve()),
        "process": identity,
        "signal": signal_name,
    }


_REMOTE_CAPSULE_BOOTSTRAP = r"""
import os, runpy, sys
python_root, state_root = sys.argv[1:3]
os.chdir(python_root)
os.environ.clear()
os.environ.update({
    "PATH": "/usr/bin:/bin",
    "PYTHONPATH": python_root,
    "PYTHONNOUSERSITE": "1",
    "PYTHONSAFEPATH": "1",
    "PYTHONDONTWRITEBYTECODE": "1",
    "HOME": state_root + "/home",
    "TMPDIR": state_root + "/tmp",
    "XDG_CACHE_HOME": state_root + "/cache",
    "EXASERVE_LOCAL_RUNTIME_ROOT": os.path.dirname(python_root),
    "EXASERVE_LOCAL_STATE_ROOT": state_root,
})
sys.path.insert(0, python_root)
sys.argv = ["exaserve.state.qualification_process", *sys.argv[3:]]
runpy.run_module("exaserve.state.qualification_process", run_name="__main__")
""".strip()


def _runtime_capsule_context(
    run_dir: Path, *, deployment_id: str, generation: int, plan_hash: str
) -> dict[str, str]:
    from exaserve.source_staging import validate_source_staging_result

    manifest_path = run_dir / "source_staging_manifest.json"
    result = validate_source_staging_result(
        _read_json(manifest_path),
        expected_deployment_id=deployment_id,
        expected_generation=generation,
        expected_plan_hash=plan_hash,
        require_current_state_layout=True,
    )
    context = {
        "python_root": result["local_python_root"],
        "state_root": result["local_state_root"],
        "qualified_python": result["qualified_python"],
    }
    for name, value in context.items():
        if not isinstance(value, str) or not os.path.isabs(value):
            raise RuntimeError(f"runtime capsule {name} is not absolute")
        resolved = os.path.realpath(value)
        if (
            resolved == "/home"
            or resolved.startswith("/home/")
            or resolved == "/lus"
            or resolved.startswith("/lus/")
        ):
            raise RuntimeError(f"runtime capsule {name} resolves on shared storage")
        context[name] = resolved
    return context


def _remote_capsule_argv(node: str, context: dict[str, str], *arguments: str) -> list[str]:
    return _pals_argv(
        (node,),
        context["qualified_python"],
        "-I",
        "-s",
        "-c",
        _REMOTE_CAPSULE_BOOTSTRAP,
        context["python_root"],
        context["state_root"],
        *arguments,
        application_environment={
            "HOME": context["state_root"] + "/home",
            "TMPDIR": context["state_root"] + "/tmp",
        },
        working_directory="/tmp",
    )


def _run_remote_json(
    argv: list[str], *, node: str, activity: str, timeout: float
) -> dict[str, object]:
    completed = subprocess.run(
        argv,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"{activity} failed on {node}: returncode={completed.returncode}, "
            f"stderr={completed.stderr[-2000:]}"
        )
    try:
        report = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"{activity} returned invalid evidence on {node}: {completed.stdout[-2000:]}"
        ) from exc
    if not isinstance(report, dict):
        raise RuntimeError(f"{activity} evidence is not an object on {node}")
    return report


def _run_remote_generation_signal(
    node: str,
    pid: int,
    signal_name: str,
    *,
    deployment_id: str,
    generation: int,
    plan_hash: str,
    run_dir: Path,
    owner_rank: int,
    requirement_id: str,
    role: str,
) -> dict[str, object]:
    """Run one pidfd-fenced signal helper from the node-local runtime capsule."""
    context = _runtime_capsule_context(
        run_dir,
        deployment_id=deployment_id,
        generation=generation,
        plan_hash=plan_hash,
    )
    argv = _remote_capsule_argv(
        node,
        context,
        "signal",
        "--pid",
        str(pid),
        "--signal",
        signal_name,
        "--owner-rank",
        str(owner_rank),
        "--requirement-id",
        requirement_id,
        "--role",
        role,
        "--deployment-id",
        deployment_id,
        "--generation",
        str(generation),
        "--plan-hash",
        plan_hash,
    )
    report = _run_remote_json(argv, node=node, activity="exact generation signal", timeout=30.0)
    from exaserve.plan.contracts import same_node

    if (
        not same_node(str(report.get("hostname", "")), node)
        or report.get("deployment_id") != deployment_id
        or report.get("generation") != generation
        or report.get("deployment_plan_hash") != plan_hash
        or report.get("signal") != signal_name
        or report.get("owner_rank") != owner_rank
        or report.get("requirement_id") != requirement_id
        or int(report.get("process", {}).get("pid", 0)) != pid
    ):
        raise RuntimeError(f"exact generation signal evidence is invalid on {node}: {report}")
    report["argv"] = argv
    return report


def _process_start_ticks(pid: int) -> int:
    raw = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    return int(raw.rsplit(") ", 1)[1].split()[19])


def _generation_process_identity(
    pid: int,
    *,
    deployment_id: str,
    generation: int,
    plan_hash: str,
    run_dir: Path,
) -> dict[str, object] | None:
    """Return an exact local process identity, or null when unrelated.

    Qualification cleanup never uses process-name matching. Every selected
    process must carry the exact immutable generation identity in its own
    kernel-published environment, belong to this uid, and retain the same Linux
    start ticks when it is later signalled. This fences unrelated jobs and PID
    reuse.
    """
    if pid <= 1 or pid == os.getpid():
        return None
    process_dir = Path("/proc") / str(pid)
    try:
        status = (process_dir / "status").read_text(encoding="utf-8")
        uid_line = next(line for line in status.splitlines() if line.startswith("Uid:"))
        if int(uid_line.split()[1]) != os.getuid():
            return None
        raw_environment = (process_dir / "environ").read_bytes()
        if len(raw_environment) > 1 << 20:
            raise RuntimeError(f"process {pid} environment exceeds the cleanup evidence bound")
        environment: dict[str, str] = {}
        for entry in (item for item in raw_environment.split(b"\0") if item):
            key, separator, value = entry.partition(b"=")
            if not separator:
                continue
            text_key = key.decode("utf-8", errors="strict")
            if text_key in environment:
                raise RuntimeError(f"process {pid} has duplicate environment key {text_key!r}")
            environment[text_key] = value.decode("utf-8", errors="strict")
        expected = {
            "EXASERVE_DEPLOYMENT_ID": deployment_id,
            "EXASERVE_GENERATION": str(generation),
            "EXASERVE_PLAN_HASH": plan_hash,
            "EXASERVE_RUN_LOG_DIR": str(run_dir),
        }
        if any(environment.get(key) != value for key, value in expected.items()):
            return None
        command = [
            item.decode("utf-8", errors="replace")
            for item in (process_dir / "cmdline").read_bytes().split(b"\0")
            if item
        ]
        return {
            "pid": pid,
            "process_start_ticks": _process_start_ticks(pid),
            "pgid": os.getpgid(pid),
            "argv": command,
        }
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        # Shared-user systems can expose status ownership while denying
        # environ for an unrelated non-dumpable process. Such a PID is not an
        # exact match and, critically, is never signalled.
        return None
    except (OSError, StopIteration, UnicodeDecodeError, ValueError) as exc:
        raise RuntimeError(f"could not verify cleanup identity for process {pid}: {exc}") from exc


def _same_generation_process(
    identity: dict[str, object],
    *,
    deployment_id: str,
    generation: int,
    plan_hash: str,
    run_dir: Path,
) -> bool:
    observed = _generation_process_identity(
        int(identity["pid"]),
        deployment_id=deployment_id,
        generation=generation,
        plan_hash=plan_hash,
        run_dir=run_dir,
    )
    return (
        observed is not None and observed["process_start_ticks"] == identity["process_start_ticks"]
    )


def _cleanup_local_generation_processes(
    *,
    deployment_id: str,
    generation: int,
    plan_hash: str,
    run_dir: Path,
) -> dict[str, object]:
    """Reap only exact generation processes on the current allocated node."""
    if not deployment_id or "\0" in deployment_id:
        raise ValueError("cleanup deployment identity must be non-empty non-NUL text")
    if isinstance(generation, bool) or not isinstance(generation, int) or generation <= 0:
        raise ValueError("cleanup generation must be a positive integer")
    if len(plan_hash) != 64 or any(character not in "0123456789abcdef" for character in plan_hash):
        raise ValueError("cleanup plan hash must be lowercase SHA-256 text")
    run_dir = run_dir.resolve()

    matched = []
    for process_dir in sorted(Path("/proc").glob("[0-9]*"), key=lambda item: int(item.name)):
        identity = _generation_process_identity(
            int(process_dir.name),
            deployment_id=deployment_id,
            generation=generation,
            plan_hash=plan_hash,
            run_dir=run_dir,
        )
        if identity is not None:
            matched.append(identity)

    signals: list[dict[str, object]] = []
    for identity in reversed(matched):
        if _same_generation_process(
            identity,
            deployment_id=deployment_id,
            generation=generation,
            plan_hash=plan_hash,
            run_dir=run_dir,
        ):
            try:
                os.kill(int(identity["pid"]), signal.SIGTERM)
                signals.append({"pid": identity["pid"], "signal": "TERM"})
            except ProcessLookupError:
                pass
    term_deadline = time.monotonic() + 3.0
    while time.monotonic() < term_deadline:
        if not any(
            _same_generation_process(
                identity,
                deployment_id=deployment_id,
                generation=generation,
                plan_hash=plan_hash,
                run_dir=run_dir,
            )
            for identity in matched
        ):
            break
        time.sleep(0.05)
    for identity in reversed(matched):
        if _same_generation_process(
            identity,
            deployment_id=deployment_id,
            generation=generation,
            plan_hash=plan_hash,
            run_dir=run_dir,
        ):
            try:
                os.kill(int(identity["pid"]), signal.SIGKILL)
                signals.append({"pid": identity["pid"], "signal": "KILL"})
            except ProcessLookupError:
                pass
    kill_deadline = time.monotonic() + 3.0
    survivors = []
    while time.monotonic() < kill_deadline:
        survivors = [
            identity
            for identity in matched
            if _same_generation_process(
                identity,
                deployment_id=deployment_id,
                generation=generation,
                plan_hash=plan_hash,
                run_dir=run_dir,
            )
        ]
        if not survivors:
            break
        time.sleep(0.05)
    if survivors:
        raise RuntimeError(
            f"exact generation processes survived qualification cleanup: {survivors}"
        )
    return {
        "schema_version": 1,
        "hostname": socket.gethostname(),
        "deployment_id": deployment_id,
        "generation": generation,
        "deployment_plan_hash": plan_hash,
        "run_dir": str(run_dir),
        "matched": matched,
        "signals": signals,
        "survivors": [],
    }


def _cleanup_generation_on_nodes(
    nodes: tuple[str, ...],
    *,
    deployment_id: str,
    generation: int,
    plan_hash: str,
    run_dir: Path,
) -> list[dict[str, object]]:
    """Run capsule-local exact-generation cleanup on every allocated node."""
    context = _runtime_capsule_context(
        run_dir,
        deployment_id=deployment_id,
        generation=generation,
        plan_hash=plan_hash,
    )
    argv = _pals_argv(
        nodes,
        context["qualified_python"],
        "-I",
        "-s",
        "-c",
        _REMOTE_CAPSULE_BOOTSTRAP,
        context["python_root"],
        context["state_root"],
        "cleanup",
        "--timeout",
        "8",
        "--deployment-id",
        deployment_id,
        "--generation",
        str(generation),
        "--plan-hash",
        plan_hash,
        application_environment={
            "HOME": context["state_root"] + "/home",
            "TMPDIR": context["state_root"] + "/tmp",
        },
        working_directory="/tmp",
    )
    process = subprocess.Popen(
        argv,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=20.0)
    except subprocess.TimeoutExpired as exc:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.communicate(timeout=5.0)
        raise RuntimeError("allocation-wide exact generation cleanup timed out") from exc
    if process.returncode != 0:
        raise RuntimeError(
            "allocation-wide exact generation cleanup failed: "
            f"returncode={process.returncode}, stderr={stderr[-4000:]}"
        )
    lines = [line for line in stdout.splitlines() if line.strip()]
    if len(lines) != len(nodes):
        raise RuntimeError(
            f"exact generation cleanup returned {len(lines)}/{len(nodes)} report lines"
        )
    try:
        reports = [json.loads(line) for line in lines]
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"exact generation cleanup returned invalid evidence: {stdout[-4000:]}"
        ) from exc
    from exaserve.plan.contracts import same_node

    ordered = []
    used = set()
    for node in nodes:
        matches = [
            (index, report)
            for index, report in enumerate(reports)
            if index not in used
            and isinstance(report, dict)
            and same_node(str(report.get("hostname", "")), node)
        ]
        if len(matches) != 1:
            raise RuntimeError(f"exact generation cleanup has no unique report for {node}")
        index, report = matches[0]
        used.add(index)
        if (
            report.get("deployment_id") != deployment_id
            or report.get("generation") != generation
            or report.get("deployment_plan_hash") != plan_hash
            or report.get("survivors") != []
        ):
            raise RuntimeError(f"exact generation cleanup evidence is invalid on {node}: {report}")
        report["argv"] = argv
        ordered.append(report)
    return ordered


_REMOTE_PORT_HOLDER_CODE = r"""
import json, os, signal, socket, sys
host = sys.argv[1]
port = int(sys.argv[2])
stopping = False
def stop(_signal, _frame):
    global stopping
    stopping = True
for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
    signal.signal(signum, stop)
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
    listener.bind((host, port))
    listener.listen(8)
    listener.settimeout(0.5)
    print(json.dumps({"schema_version": 1, "state": "READY", "hostname": socket.gethostname(), "bind_host": host, "port": port, "pid": os.getpid()}, sort_keys=True), flush=True)
    while not stopping:
        try:
            connection, _peer = listener.accept()
        except TimeoutError:
            continue
        connection.close()
"""


def _qualified_remote_python(site_profile) -> str:
    """Bind the pre-capsule bootstrap to the declared immutable site image."""
    from exaserve.site import qualify_site_local_bootstrap

    return qualify_site_local_bootstrap(os.path.realpath(sys.executable), site_profile)


class _RemotePortHolder:
    """Own and later reap one exact listener process on an allocated node."""

    def __init__(
        self,
        *,
        node: str,
        port: int,
        pid: int,
        process: subprocess.Popen,
        command: list[str],
        handshake: dict,
        evidence_path: Path,
    ) -> None:
        self.node = node
        self.port = port
        self.pid = pid
        self.process = process
        self.command = command
        self.handshake = handshake
        self.evidence_path = evidence_path
        self.signals: list[dict[str, object]] = []

    @classmethod
    def start(
        cls, *, node: str, port: int, evidence_path: Path, site_profile
    ) -> "_RemotePortHolder":
        from exaserve.plan.contracts import same_node
        from exaserve.state.atomic import strict_json_loads

        if not 1 <= port <= 65535:
            raise ValueError(f"remote port must be in 1..65535, observed {port!r}")
        argv = _pals_argv(
            (node,),
            _qualified_remote_python(site_profile),
            "-I",
            "-u",
            "-c",
            _REMOTE_PORT_HOLDER_CODE,
            "0.0.0.0",
            str(port),
            application_environment={
                "HOME": "/tmp",
                "TMPDIR": "/tmp",
                "PYTHONNOUSERSITE": "1",
                "PYTHONSAFEPATH": "1",
            },
            working_directory="/tmp",
        )
        process = subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            start_new_session=True,
        )
        assert process.stdout is not None and process.stderr is not None
        try:
            deadline = time.monotonic() + 30.0
            line = ""
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    break
                readable, _, _ = select.select(
                    [process.stdout], [], [], max(0.0, min(0.5, deadline - time.monotonic()))
                )
                if readable:
                    line = process.stdout.readline()
                    break
            if not line:
                try:
                    _remaining_out, error = process.communicate(timeout=1.0)
                except subprocess.TimeoutExpired:
                    error = ""
                raise RuntimeError(
                    f"remote port holder on {node}:{port} did not publish READY: "
                    f"returncode={process.poll()}, stderr={error[-2000:]}"
                )
            payload = strict_json_loads(line)
            expected_keys = {
                "schema_version",
                "state",
                "hostname",
                "bind_host",
                "port",
                "pid",
            }
            if (
                not isinstance(payload, dict)
                or set(payload) != expected_keys
                or payload.get("schema_version") != 1
                or payload.get("state") != "READY"
                or payload.get("bind_host") != "0.0.0.0"
                or payload.get("port") != port
                or not same_node(str(payload.get("hostname", "")), node)
                or isinstance(payload.get("pid"), bool)
                or not isinstance(payload.get("pid"), int)
                or payload["pid"] <= 0
            ):
                raise RuntimeError(f"invalid remote port-holder handshake: {payload!r}")
            holder = cls(
                node=node,
                port=port,
                pid=payload["pid"],
                process=process,
                command=argv,
                handshake=payload,
                evidence_path=evidence_path,
            )
            holder._publish(stopped=False)
            return holder
        except BaseException:
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
            try:
                process.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait(timeout=5.0)
            raise

    def _publish(self, *, stopped: bool) -> None:
        _atomic_json(
            self.evidence_path,
            {
                "schema_version": 1,
                "kind": "remote_port_holder",
                "node": self.node,
                "port": self.port,
                "remote_pid": self.pid,
                "command": self.command,
                "handshake": self.handshake,
                "signals": self.signals,
                "stopped": stopped,
                "pals_returncode": self.process.poll(),
            },
        )

    def stop(self) -> None:
        if self.process.poll() is None:
            try:
                os.killpg(self.process.pid, signal.SIGTERM)
                self.signals.append(
                    {"owner_pid": self.process.pid, "signal": "TERM", "transport": "pals"}
                )
            except ProcessLookupError:
                pass
            try:
                self.process.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(self.process.pid, signal.SIGKILL)
                    self.signals.append(
                        {"owner_pid": self.process.pid, "signal": "KILL", "transport": "pals"}
                    )
                except ProcessLookupError:
                    pass
                self.process.wait(timeout=10.0)
        if self.process.poll() is None:
            raise RuntimeError("PALS port-holder process group survived bounded cleanup")
        self._publish(stopped=True)


def _environment_receipt(
    nodes: tuple[str, ...], bootstrap: Path, wheel: Path, *, gate_id: str
) -> dict:
    commands = {}
    for name in ("python3", "mpiexec", "mpicc", "ray", "haproxy"):
        commands[name] = _command_identity([name])
    import exaserve
    import ray

    package_path = Path(exaserve.__file__).resolve()
    if os.path.commonpath((str(bootstrap), str(package_path))) != str(bootstrap):
        raise RuntimeError(
            f"ExaServe imported from {package_path}, outside immutable bootstrap {bootstrap}"
        )
    bootstrap_receipt = _verify_bootstrap(wheel, bootstrap)
    return {
        "schema_version": 1,
        "gate_id": gate_id,
        "captured_at": time.time(),
        "hostname": socket.gethostname(),
        "nodes": list(nodes),
        "pbs_job_id": os.environ["PBS_JOBID"],
        "pbs_nodefile": os.path.realpath(os.environ["PBS_NODEFILE"]),
        "pbs_queue": os.environ.get("PBS_QUEUE") or os.environ.get("PBS_O_QUEUE") or "unknown",
        "aurora_subjob": os.environ.get("AURORA_SUBJOB", ""),
        "pbs_environment": os.environ.get("PBS_ENVIRONMENT", ""),
        "python": sys.version,
        "exaserve_import": str(package_path),
        "ray_version": getattr(ray, "__version__", "unknown"),
        "bootstrap": str(bootstrap),
        "wheel": str(wheel),
        "wheel_sha256": _sha256_file(wheel),
        "bootstrap_receipt": bootstrap_receipt,
        "ze_affinity_mask": os.environ.get("ZE_AFFINITY_MASK", ""),
        "commands": commands,
    }


class _Tee:
    def __init__(self, stream, path: Path, label: str) -> None:
        self.stream = stream
        self.path = path
        self.label = label
        self.thread = threading.Thread(target=self._run, name=f"tee-{label}", daemon=False)
        self.error: BaseException | None = None

    def start(self) -> None:
        self.thread.start()

    def _run(self) -> None:
        try:
            with self.stream, self.path.open("w", encoding="utf-8") as handle:
                for line in self.stream:
                    handle.write(line)
                    handle.flush()
                    print(f"[{self.label}] {line}", end="", flush=True)
        except BaseException as exc:
            self.error = exc

    def join(self, *, deadline: float) -> None:
        self.thread.join(max(0.0, deadline - time.monotonic()))
        if self.thread.is_alive():
            raise RuntimeError(f"{self.label} capture thread did not terminate")
        if self.error is not None:
            raise RuntimeError(f"{self.label} capture failed: {self.error}")


def _wait_status(
    run_dir: Path,
    generation: int,
    plan_hash: str,
    timeout_s: float,
    *,
    process,
):
    from exaserve.status_api import read_deployment_status

    deadline = time.monotonic() + timeout_s
    last_state = None
    while time.monotonic() < deadline:
        status = read_deployment_status(str(run_dir))
        if status is not None:
            if status.generation != generation or status.deployment_plan_hash != plan_hash:
                raise RuntimeError(
                    "canonical status identity disagrees with the launched generation"
                )
            if status.state != last_state:
                print(
                    f"QUALIFICATION_STATUS|generation={generation}|state={status.state}", flush=True
                )
                last_state = status.state
            if status.ready or status.terminal:
                return status
        returncode = process.poll()
        if returncode is not None:
            raise RuntimeError(
                "launcher exited before publishing canonical READY/terminal status "
                f"(returncode={returncode})"
            )
        time.sleep(1.0)
    raise TimeoutError(f"canonical deployment status did not become ready/terminal in {timeout_s}s")


def _wait_replica_failure_outcome(
    run_dir: Path,
    *,
    generation: int,
    plan,
    baseline_revision: int,
    baseline_manifest_hash: str,
):
    """Require durable READY revocation, then bounded recovery or failure."""
    from exaserve.status_api import read_deployment_status

    timeout_s = (
        float(plan.readiness.recovery_deadline_s)
        + 3.0 * float(plan.readiness.validation_interval_s)
        + 30.0
    )
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        status = read_deployment_status(str(run_dir))
        if status is None:
            time.sleep(0.25)
            continue
        if (
            status.generation != generation
            or status.deployment_plan_hash != plan.deployment_plan_hash
        ):
            raise RuntimeError("replica-failure status identity changed generation")
        if status.state == "FAILED":
            outcome = status
            break
        if (
            status.ready
            and status.revision > baseline_revision
            and status.receipt_manifest_hash
            and status.receipt_manifest_hash != baseline_manifest_hash
        ):
            outcome = status
            break
        if status.terminal:
            raise RuntimeError(
                f"replica failure produced unexpected terminal state {status.state}: "
                f"{status.reason_code}: {status.detail}"
            )
        time.sleep(0.25)
    else:
        raise TimeoutError(f"replica failure neither re-attested nor failed within {timeout_s:g}s")

    record = _read_json(run_dir / "deployment_status.json")
    history = record.get("history")
    if not isinstance(history, list) or len(history) != outcome.revision + 1:
        raise RuntimeError("replica-failure status history is incomplete")
    post_fault = [
        {"revision": revision, **event}
        for revision, event in enumerate(history)
        if revision > baseline_revision
    ]
    revoked = [
        event
        for event in post_fault
        if event.get("state") == "VALIDATING" and event.get("reason_code") == "READINESS_REVOKED"
    ]
    if not revoked:
        raise RuntimeError(
            f"exact replica death did not durably revoke READY before its outcome: {post_fault}"
        )
    return outcome, post_fault


def _canary(endpoint: str, model_id: str) -> dict:
    body = json.dumps(
        {"model": model_id, "prompt": "production lifecycle probe", "max_tokens": 4}
    ).encode()
    request = urllib.request.Request(
        endpoint.rstrip("/") + "/v1/completions",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=30.0) as response:
            raw = response.read()
            status_code = response.status
    except urllib.error.URLError as exc:
        raise RuntimeError(f"advertised-endpoint canary failed: {exc}") from exc
    from exaserve.state.atomic import strict_json_loads

    payload = strict_json_loads(raw.decode("utf-8"))
    if status_code != 200 or not isinstance(payload, dict):
        raise RuntimeError(f"canary returned status={status_code}, payload={payload!r}")
    choices = payload.get("choices")
    if (
        not isinstance(choices, list)
        or not choices
        or not isinstance(choices[0], dict)
        or not isinstance(choices[0].get("text"), str)
    ):
        raise RuntimeError(f"canary response lacks an OpenAI completion choice: {payload!r}")
    return {"model_id": model_id, "status_code": status_code, "response": payload}


def _validate_ready_evidence(status, plan, run_dir: Path) -> dict:
    from exaserve.compat.profile import default_profile
    from exaserve.source_staging import validate_source_staging_result
    from exaserve.state.receipts import load_receipt_manifest
    from exaserve.status_api import load_status_allocation_binding
    from exaserve.vllm_modelinfo_seed import expected_source_seed_evidence

    manifest = load_receipt_manifest(status.receipt_manifest_path)
    expected_slots = sorted(item.receipt_requirement_id for item in plan.receipt_requirements)
    observed_slots = sorted(item["receipt_requirement_id"] for item in manifest.receipts)
    if observed_slots != expected_slots:
        raise RuntimeError(
            f"receipt slot set mismatch: expected={expected_slots}, observed={observed_slots}"
        )
    binding = load_status_allocation_binding(str(run_dir), status)
    compatibility = default_profile(plan.vendor)
    seed_evidence = expected_source_seed_evidence(
        compatibility,
        install_required=plan.engine == "vllm" and not plan.runtime.null_compute,
    )
    source = validate_source_staging_result(
        _read_json(run_dir / "source_staging_manifest.json"),
        expected_deployment_id=plan.deployment_id,
        expected_generation=status.generation,
        expected_plan_hash=plan.deployment_plan_hash,
        expected_site_profile_hash=plan.site_profile_hash,
        expected_binding_hash=status.allocation_binding_hash,
        expected_compatibility_profile_id=plan.compatibility_profile_hash,
        expected_compatibility_manifest_hash=plan.manifest_hash,
        expected_rank_to_node=binding.rank_to_node,
        expected_run_dir=run_dir,
        require_seed_evidence=True,
        expected_seed_evidence=seed_evidence,
        require_current_state_layout=True,
    )
    receipts = source["rank_receipts"]
    engine_evidence = (
        _engine_patch_evidence({"receipts": list(manifest.receipts)}, plan)
        if any(item.role == "engine_core" for item in plan.receipt_requirements)
        else None
    )
    return {
        "receipt_manifest_hash": manifest.manifest_hash,
        "receipt_slots": observed_slots,
        "source_manifest_hash": source.get("source_manifest_hash"),
        "source_file_count": source.get("file_count"),
        "source_rank_receipts": len(receipts),
        "engine_patch_evidence": engine_evidence,
    }


def _owned_gateway_pid(receipt_manifest: dict, plan) -> int:
    if plan.gateway is None:
        raise RuntimeError("gateway fault injection requires a planned gateway")
    requirement_id = f"global/gateway/{plan.gateway.kind}"
    matches = [
        item
        for item in receipt_manifest.get("receipts", [])
        if item.get("receipt_requirement_id") == requirement_id
        and item.get("role") == "gateway"
        and item.get("component_id") == f"gateway/{plan.gateway.kind}"
        and item.get("owner_scope") == "GLOBAL"
        and item.get("attestation_type") == "SUPERVISOR"
    ]
    pids = [item.get("pid") for item in matches]
    if (
        len(matches) != 1
        or len(pids) != 1
        or isinstance(pids[0], bool)
        or not isinstance(pids[0], int)
        or pids[0] <= 0
    ):
        raise RuntimeError(
            f"READY did not publish one exact owned gateway PID for {requirement_id}: {matches}"
        )
    return pids[0]


def _owned_worker_target(receipt_manifest: dict, binding, *, worker_rank: int) -> dict:
    """Select one exact SELF-attested Ray worker in the verified binding."""
    from exaserve.plan.contracts import same_node

    if isinstance(worker_rank, bool) or not isinstance(worker_rank, int) or worker_rank <= 0:
        raise ValueError("worker fault injection requires a positive non-head rank")
    expected_nodes = dict(binding.rank_to_node)
    expected_node = expected_nodes.get(worker_rank)
    requirement_id = f"rank{worker_rank}/ray_worker"
    matches = [
        item
        for item in receipt_manifest.get("receipts", [])
        if item.get("receipt_requirement_id") == requirement_id
        and item.get("role") == "ray_worker"
        and item.get("component_id") == "ray"
        and item.get("owner_scope") == "RANK"
        and item.get("owner_rank") == worker_rank
        and item.get("attestation_type") == "SELF"
        and expected_node is not None
        and same_node(str(item.get("node_id", "")), expected_node)
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"READY did not publish one exact owned Ray worker for {requirement_id}: {matches}"
        )
    pid = matches[0].get("pid")
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        raise RuntimeError(f"exact Ray worker receipt lacks a positive PID: {matches[0]}")
    return {
        "receipt_requirement_id": requirement_id,
        "rank": worker_rank,
        "node": expected_node,
        "pid": pid,
        "receipt_hash": matches[0].get("receipt_hash"),
    }


def _owned_replica_target(receipt_manifest: dict, binding, *, model, replica) -> dict:
    """Select one exact SELF-attested Serve replica from its compiled slot."""
    from exaserve.plan.contracts import same_node

    owner_rank = replica.planned_ranks[0]
    expected_node = dict(binding.rank_to_node).get(owner_rank)
    requirement_id = f"model/{model.route_name}/replica/{replica.replica_index}"
    matches = [
        item
        for item in receipt_manifest.get("receipts", [])
        if item.get("receipt_requirement_id") == requirement_id
        and item.get("role") == "replica"
        and item.get("component_id") == replica.replica_id
        and item.get("owner_scope") == "RANK"
        and item.get("owner_rank") == owner_rank
        and item.get("attestation_type") == "SELF"
        and expected_node is not None
        and same_node(str(item.get("node_id", "")), expected_node)
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"READY did not publish one exact owned replica for {requirement_id}: {matches}"
        )
    receipt = matches[0]
    pid = receipt.get("pid")
    instance_id = receipt.get("instance_id")
    if (
        isinstance(pid, bool)
        or not isinstance(pid, int)
        or pid <= 0
        or not isinstance(instance_id, str)
        or not instance_id
    ):
        raise RuntimeError(f"exact replica receipt lacks a process identity: {receipt}")
    return {
        "receipt_requirement_id": requirement_id,
        "model_id": model.model_id,
        "replica_index": replica.replica_index,
        "rank": owner_rank,
        "node": expected_node,
        "pid": pid,
        "instance_id": instance_id,
        "receipt_hash": receipt.get("receipt_hash"),
    }


def _engine_patch_evidence(receipt_manifest: dict, plan) -> dict[str, object]:
    """Re-prove every planned engine-process receipt and its resolved patches."""
    from exaserve.compat.producers import patch_requirements_for_plan
    from exaserve.compat.profile import default_profile
    from exaserve.compat.receipt_v2 import receipt_from_dict, validate_receipt

    profile = default_profile(plan.vendor)
    if profile.profile_id != plan.compatibility_profile_hash:
        raise RuntimeError("qualification compatibility profile disagrees with the plan")
    receipts = receipt_manifest.get("receipts")
    if not isinstance(receipts, list):
        raise RuntimeError("READY receipt manifest lacks a receipts list")
    planned = [
        item for item in plan.receipt_requirements if item.role in {"engine_core", "engine_worker"}
    ]
    if not planned or not any(item.role == "engine_core" for item in planned):
        raise RuntimeError("real-engine qualification plan has no spawned EngineCore slot")

    instances = []
    for requirement in planned:
        matches = [
            item
            for item in receipts
            if item.get("receipt_requirement_id") == requirement.receipt_requirement_id
            and item.get("role") == requirement.role
            and item.get("component_id") == requirement.component_slot
            and item.get("owner_scope") == "RANK"
            and item.get("owner_rank") == requirement.planned_rank
            and item.get("attestation_type") == "SELF"
        ]
        if len(matches) != 1:
            raise RuntimeError(
                "READY did not publish one exact SELF engine receipt for "
                f"{requirement.receipt_requirement_id}: {matches}"
            )
        receipt = receipt_from_dict(matches[0])
        required, resolved_not_required = patch_requirements_for_plan(
            plan, requirement.receipt_requirement_id, profile
        )
        validate_receipt(
            receipt,
            required_patch_ids=required,
            resolved_not_required=resolved_not_required,
        )
        if "EN-01" not in required:
            raise RuntimeError(
                f"spawned process {requirement.receipt_requirement_id} does not require EN-01"
            )
        en01 = receipt.patch_results.get("EN-01")
        if en01 is None or en01.status != "APPLIED" or not en01.postcondition_passed:
            raise RuntimeError(
                f"spawn reach was not applied in {requirement.receipt_requirement_id}: {en01}"
            )
        instances.append(
            {
                "receipt_requirement_id": requirement.receipt_requirement_id,
                "role": requirement.role,
                "planned_rank": requirement.planned_rank,
                "placement": requirement.placement,
                "node_id": receipt.node_id,
                "pid": receipt.pid,
                "instance_id": receipt.instance_id,
                "receipt_hash": receipt.receipt_hash,
                "required_patch_ids": list(required),
                "resolved_not_required_patch_ids": list(resolved_not_required),
                "patch_results": {
                    patch_id: {
                        "status": result.status,
                        "postcondition_passed": result.postcondition_passed,
                        "evidence_hash": result.evidence_hash,
                    }
                    for patch_id, result in sorted(receipt.patch_results.items())
                },
            }
        )
    if len({item["instance_id"] for item in instances}) != len(instances):
        raise RuntimeError("two planned engine-process slots share one process instance")
    return {
        "compatibility_profile_hash": profile.profile_id,
        "engine_core_count": sum(item["role"] == "engine_core" for item in instances),
        "engine_worker_count": sum(item["role"] == "engine_worker" for item in instances),
        "instances": instances,
    }


def _validate_cluster_snapshot(
    payload: dict,
    *,
    generation: int,
    plan,
    expected_nodes: tuple[str, ...],
) -> dict:
    from exaserve.plan.contracts import same_node

    observed = payload.get("nodes")
    if (
        payload.get("schema_version") != 1
        or payload.get("deployment_id") != plan.deployment_id
        or payload.get("generation") != generation
        or payload.get("deployment_plan_hash") != plan.deployment_plan_hash
        or payload.get("site_profile_hash") != plan.site_profile_hash
        or payload.get("ready") is not True
        or payload.get("blockers") != []
        or not isinstance(observed, list)
        or len(observed) != len(expected_nodes)
    ):
        raise RuntimeError(
            f"Ray cluster snapshot is not an exact ready membership proof: {payload}"
        )
    unmatched = list(expected_nodes)
    for item in observed:
        if not isinstance(item, dict) or item.get("alive") is not True:
            raise RuntimeError(f"Ray cluster snapshot contains a non-live node: {item!r}")
        matches = [node for node in unmatched if same_node(str(item.get("node_name", "")), node)]
        if len(matches) != 1:
            raise RuntimeError(
                f"Ray cluster node {item.get('node_name')!r} does not map exactly to {unmatched}"
            )
        unmatched.remove(matches[0])
    if unmatched:
        raise RuntimeError(f"Ray cluster snapshot omitted allocated nodes {unmatched}")
    return {
        "ready": True,
        "node_count": len(observed),
        "node_names": sorted(str(item["node_name"]) for item in observed),
        "observed_at": payload.get("observed_at"),
    }


def _observe_partial_readiness(
    run_dir: Path,
    *,
    generation: int,
    plan,
    expected_nodes: tuple[str, ...],
    timeout_s: float,
    observation_s: float,
    process,
):
    """Prove membership completed while one required proxy withheld READY."""
    from exaserve.status_api import read_deployment_status

    deadline = time.monotonic() + timeout_s
    history: list[dict[str, object]] = []
    seen_revisions: set[int] = set()
    membership = None
    membership_seen_at = None
    status = None
    while time.monotonic() < deadline:
        status = read_deployment_status(str(run_dir))
        if status is not None:
            if (
                status.generation != generation
                or status.deployment_plan_hash != plan.deployment_plan_hash
            ):
                raise RuntimeError("canonical status identity disagrees with partial-readiness run")
            if status.revision not in seen_revisions:
                seen_revisions.add(status.revision)
                history.append(
                    {
                        "observed_at": time.time(),
                        "revision": status.revision,
                        "state": status.state,
                        "reason_code": status.reason_code,
                        "detail": status.detail,
                    }
                )
            if status.ready:
                raise RuntimeError(
                    "deployment published READY while the worker proxy port was held"
                )
        snapshot_path = run_dir / "ray_cluster.snapshot.json"
        if membership is None and snapshot_path.is_file():
            membership = _validate_cluster_snapshot(
                _read_json(snapshot_path),
                generation=generation,
                plan=plan,
                expected_nodes=expected_nodes,
            )
            membership_seen_at = time.monotonic()
        if membership is not None:
            if status is not None and status.terminal:
                return status, history, membership
            if time.monotonic() - membership_seen_at >= observation_s:
                if status is None:
                    raise RuntimeError("membership completed without a canonical deployment status")
                return status, history, membership
        elif status is not None and status.terminal:
            raise RuntimeError(
                f"partial-readiness run terminated before exact membership: "
                f"{status.state} {status.reason_code}: {status.detail}"
            )
        returncode = process.poll()
        if returncode is not None:
            raise RuntimeError(
                "launcher exited before partial-readiness evidence completed "
                f"(returncode={returncode})"
            )
        time.sleep(1.0)
    raise TimeoutError(
        f"partial-readiness scenario did not prove exact membership within {timeout_s}s"
    )


def _terminal_record(
    run_dir: Path,
    expected_state: str,
    *,
    require_graceful_deployment: bool = False,
):
    from exaserve.status_api import read_deployment_status

    status = read_deployment_status(str(run_dir))
    if status is None or status.state != expected_state:
        observed = None if status is None else status.state
        raise RuntimeError(f"expected terminal status {expected_state}, observed {observed}")
    report = _read_json(run_dir / "shutdown_report.json")
    _validate_shutdown_report(
        report,
        expected_state,
        require_graceful_deployment=require_graceful_deployment,
    )
    return status, report


def _validate_shutdown_report(
    report: dict,
    expected_state: str,
    *,
    require_graceful_deployment: bool = False,
) -> None:
    """Validate cleanup and the phase that owned terminal publication.

    Graceful stops become terminal during shutdown, so the shutdown phase must
    publish them. Fatal status is published at first-cause assignment before
    cleanup starts; shutdown must preserve it and report that a second terminal
    publication was not required.
    """
    expected_publication = "not_required" if expected_state == "FAILED" else "published"
    if (
        report.get("clean") is not True
        or report.get("terminal_publication") != expected_publication
        or report.get("observed_terminal_state") != expected_state
    ):
        raise RuntimeError(
            "shutdown report does not match the terminal ownership contract: "
            f"expected state={expected_state!r}, publication={expected_publication!r}; "
            f"observed {report}"
        )
    components = report.get("components")
    if not isinstance(components, dict) or any(
        item.get("state") != "STOPPED" for item in components.values() if isinstance(item, dict)
    ):
        raise RuntimeError(f"shutdown report retains a non-stopped component: {components}")
    if require_graceful_deployment:
        deployment = components.get("deployment")
        if not isinstance(deployment, dict) or deployment.get("returncode") != 0:
            raise RuntimeError(
                f"healthy rank sessions did not permit a graceful deployment drain: {deployment}"
            )


def _owner_exit_timeout_s(plan) -> float:
    """Runtime cleanup budget plus a bounded process/result handoff margin."""
    return float(plan.control.watchdog_cleanup_deadline_s) + 10.0


def _await_premature_terminal_cleanup(process, run_dir: Path, status, plan) -> dict:
    """Let a failed launcher finish its own bounded cleanup before verdicting."""
    try:
        returncode = process.wait(timeout=_owner_exit_timeout_s(plan))
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"launcher did not complete cleanup after premature {status.state}"
        ) from exc
    terminal, report = _terminal_record(run_dir, status.state)
    return {
        "returncode": returncode,
        "terminal_revision": terminal.revision,
        "shutdown_report": report,
    }


def _launch_scenario(
    *,
    name: str,
    root: Path,
    plan_path: Path,
    site_path: Path,
    site_profile,
    generation: int,
    fault: str,
    ready_timeout_s: float,
    nodes: tuple[str, ...],
    partial_observation_s: float,
) -> dict:
    from exaserve.plan.io import load_deployment_plan
    from exaserve.status_api import require_ready_endpoint

    plan = load_deployment_plan(str(plan_path))
    run_dir = root / name / "deployment"
    run_dir.mkdir(parents=True)
    stdout_path = root / name / "stdout.log"
    stderr_path = root / name / "stderr.log"
    fault_evidence_path = root / name / "fault_injection.json"
    local_listener: socket.socket | None = None
    remote_holder: _RemotePortHolder | None = None
    precondition: dict[str, object] | None = None
    if fault == "duplicate_gateway_port":
        if plan.gateway is None:
            raise RuntimeError("duplicate gateway-port scenario requires a planned gateway")
        local_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            local_listener.bind(("0.0.0.0", plan.gateway.port))
            local_listener.listen(8)
        except BaseException:
            local_listener.close()
            raise
        precondition = {
            "schema_version": 1,
            "kind": "local_gateway_port_holder",
            "node": socket.gethostname(),
            "port": plan.gateway.port,
            "pid": os.getpid(),
            "stopped": False,
        }
        _atomic_json(fault_evidence_path, precondition)
    elif fault == "partial_proxy_readiness":
        if plan.gateway is None or len(nodes) != 2:
            raise RuntimeError("partial proxy-readiness scenario requires exactly two nodes")
        remote_holder = _RemotePortHolder.start(
            node=nodes[1],
            port=plan.gateway.backend_port,
            evidence_path=fault_evidence_path,
            site_profile=site_profile,
        )
        precondition = {
            "kind": "remote_worker_proxy_port_holder",
            "node": remote_holder.node,
            "port": remote_holder.port,
            "remote_pid": remote_holder.pid,
        }
    argv = [sys.executable, "-u", "-m", "exaserve.launcher", str(plan_path)]
    _atomic_text(root / name / "command.txt", shlex.join(argv) + "\n")
    env = os.environ.copy()
    env.update(
        {
            "EXASERVE_GENERATION": str(generation),
            "EXASERVE_DEPLOYMENT_ID": plan.deployment_id,
            "EXASERVE_RUN_LOG_DIR": str(run_dir),
            "EXASERVE_SITE_PROFILE_PATH": str(site_path),
            "EXASERVE_NODEFILE": os.environ["PBS_NODEFILE"],
            "EXASERVE_SCHEDULER": "pbs",
            "EXASERVE_VENDOR": "xpu",
        }
    )
    started = time.time()
    try:
        process = subprocess.Popen(
            argv,
            cwd=root,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            start_new_session=True,
        )
    except BaseException as exc:
        if local_listener is not None:
            local_listener.close()
        if remote_holder is not None:
            try:
                remote_holder.stop()
            except BaseException as cleanup_exc:
                _add_note(exc, f"remote fault precondition cleanup also failed: {cleanup_exc}")
        raise
    assert process.stdout is not None and process.stderr is not None
    out_tee = _Tee(process.stdout, stdout_path, f"{name}:stdout")
    err_tee = _Tee(process.stderr, stderr_path, f"{name}:stderr")
    out_tee.start()
    err_tee.start()
    try:
        if fault == "duplicate_gateway_port":
            status = _wait_status(
                run_dir,
                generation,
                plan.deployment_plan_hash,
                ready_timeout_s,
                process=process,
            )
            if status.ready or status.state != "FAILED":
                raise RuntimeError(
                    f"occupied gateway port did not fail closed before READY: {status.state}"
                )
            try:
                returncode = process.wait(timeout=_owner_exit_timeout_s(plan))
            except subprocess.TimeoutExpired as exc:
                raise RuntimeError(
                    "launcher did not terminate after gateway port conflict"
                ) from exc
            if returncode == 0 or returncode == 143:
                raise RuntimeError(
                    f"gateway port conflict returned invalid launcher code {returncode}"
                )
            terminal, report = _terminal_record(run_dir, "FAILED")
            detail = f"{terminal.reason_code}: {terminal.detail}".lower()
            if "listener" not in detail or "bind" not in detail:
                raise RuntimeError(
                    f"gateway port conflict lacks typed bind/listener evidence: {detail}"
                )
            return {
                "scenario": name,
                "passed": True,
                "fault": fault,
                "generation": generation,
                "deployment_plan_hash": plan.deployment_plan_hash,
                "advertised_endpoint": None,
                "ready_revision": None,
                "terminal_revision": terminal.revision,
                "terminal_state": terminal.state,
                "terminal_reason_code": terminal.reason_code,
                "terminal_detail": terminal.detail,
                "returncode": returncode,
                "duration_s": round(time.time() - started, 3),
                "fault_precondition": precondition,
                "shutdown_report": report,
            }

        if fault == "partial_proxy_readiness":
            observed, history, membership = _observe_partial_readiness(
                run_dir,
                generation=generation,
                plan=plan,
                expected_nodes=nodes,
                timeout_s=ready_timeout_s,
                observation_s=partial_observation_s,
                process=process,
            )
            _atomic_json(root / name / "status_history.json", history)
            if observed.terminal:
                if observed.state != "FAILED":
                    raise RuntimeError(
                        f"partial readiness ended in unexpected state {observed.state}"
                    )
                expected_state = "FAILED"
                expected_codes = set(range(1, 256)) - {143}
                cancelled_after_observation = False
            else:
                process.send_signal(signal.SIGTERM)
                expected_state = "CANCELLED"
                expected_codes = {143}
                cancelled_after_observation = True
            try:
                returncode = process.wait(timeout=_owner_exit_timeout_s(plan))
            except subprocess.TimeoutExpired as exc:
                raise RuntimeError("launcher did not terminate after partial readiness") from exc
            if returncode not in expected_codes:
                raise RuntimeError(
                    f"partial-readiness launcher code {returncode} is invalid for "
                    f"terminal {expected_state}"
                )
            terminal, report = _terminal_record(
                run_dir,
                expected_state,
                require_graceful_deployment=cancelled_after_observation,
            )
            if terminal.ready:
                raise RuntimeError("partial-readiness terminal record incorrectly remains ready")
            return {
                "scenario": name,
                "passed": True,
                "fault": fault,
                "generation": generation,
                "deployment_plan_hash": plan.deployment_plan_hash,
                "advertised_endpoint": None,
                "ready_revision": None,
                "terminal_revision": terminal.revision,
                "terminal_state": terminal.state,
                "terminal_reason_code": terminal.reason_code,
                "terminal_detail": terminal.detail,
                "returncode": returncode,
                "duration_s": round(time.time() - started, 3),
                "fault_precondition": precondition,
                "membership_evidence": membership,
                "status_history": history,
                "observation_s": partial_observation_s,
                "cancelled_after_observation": cancelled_after_observation,
                "shutdown_report": report,
            }

        status = _wait_status(
            run_dir,
            generation,
            plan.deployment_plan_hash,
            ready_timeout_s,
            process=process,
        )
        if not status.ready:
            cleanup = _await_premature_terminal_cleanup(process, run_dir, status, plan)
            raise RuntimeError(
                f"deployment became terminal before READY: {status.state} "
                f"{status.reason_code}: {status.detail}; cleanup={cleanup}"
            )
        endpoint = require_ready_endpoint(
            str(run_dir),
            expected_generation=generation,
            expected_plan_hash=plan.deployment_plan_hash,
        )
        canary = [_canary(endpoint, model.model_id) for model in plan.models]
        evidence = _validate_ready_evidence(status, plan, run_dir)
        manifest = _read_json(Path(status.receipt_manifest_path))
        _atomic_json(root / name / "canary.json", canary)
        fault_injection = None
        replica_failure_evidence = None

        if fault == "operator_drain":
            process.send_signal(signal.SIGTERM)
            expected_state = "STOPPED"
            expected_codes = {143}
        elif fault == "gateway_death":
            gateway_pid = _owned_gateway_pid(manifest, plan)
            exact_signal = _signal_local_generation_pid(
                gateway_pid,
                "TERM",
                deployment_id=plan.deployment_id,
                generation=generation,
                plan_hash=plan.deployment_plan_hash,
                run_dir=run_dir,
            )
            fault_injection = {
                "kind": "owned_gateway_death",
                "node": nodes[0],
                "pid": gateway_pid,
                "signal": exact_signal,
            }
            _atomic_json(fault_evidence_path, fault_injection)
            expected_state = "FAILED"
            expected_codes = set(range(1, 256)) - {143}
        elif fault == "worker_death":
            from exaserve.plan.contracts import same_node
            from exaserve.status_api import load_status_allocation_binding

            binding = load_status_allocation_binding(str(run_dir), status)
            target = _owned_worker_target(manifest, binding, worker_rank=1)
            if len(nodes) != 2 or not same_node(target["node"], nodes[1]):
                raise RuntimeError(
                    f"exact rank-1 worker {target} is outside the leased worker {nodes[1:]}"
                )
            remote_signal = _run_remote_generation_signal(
                target["node"],
                target["pid"],
                "TERM",
                deployment_id=plan.deployment_id,
                generation=generation,
                plan_hash=plan.deployment_plan_hash,
                run_dir=run_dir,
                owner_rank=target["rank"],
                requirement_id=target["receipt_requirement_id"],
                role="ray_worker",
            )
            fault_injection = {
                "kind": "owned_ray_worker_death",
                "target": target,
                "signal": remote_signal,
            }
            _atomic_json(fault_evidence_path, fault_injection)
            expected_state = "FAILED"
            expected_codes = set(range(1, 256)) - {143}
        elif fault == "replica_death":
            from exaserve.plan.contracts import same_node
            from exaserve.status_api import load_status_allocation_binding

            if len(plan.models) != 1 or len(plan.models[0].replicas) < 2:
                raise RuntimeError("replica-death gate requires one multi-replica model")
            model = plan.models[0]
            replica = model.replicas[-1]
            binding = load_status_allocation_binding(str(run_dir), status)
            target = _owned_replica_target(manifest, binding, model=model, replica=replica)
            expected_node = dict(binding.rank_to_node)[replica.planned_ranks[0]]
            if not same_node(target["node"], expected_node):
                raise RuntimeError(
                    f"exact replica target {target} disagrees with compiled owner {expected_node}"
                )
            remote_signal = _run_remote_generation_signal(
                target["node"],
                target["pid"],
                "KILL",
                deployment_id=plan.deployment_id,
                generation=generation,
                plan_hash=plan.deployment_plan_hash,
                run_dir=run_dir,
                owner_rank=target["rank"],
                requirement_id=target["receipt_requirement_id"],
                role="replica",
            )
            fault_injection = {
                "kind": "owned_serve_replica_death",
                "target": target,
                "signal": remote_signal,
            }
            _atomic_json(fault_evidence_path, fault_injection)
            outcome, status_history = _wait_replica_failure_outcome(
                run_dir,
                generation=generation,
                plan=plan,
                baseline_revision=status.revision,
                baseline_manifest_hash=status.receipt_manifest_hash,
            )
            _atomic_json(root / name / "status_history.json", status_history)
            if outcome.ready:
                recovered_manifest = _read_json(Path(outcome.receipt_manifest_path))
                recovered_target = _owned_replica_target(
                    recovered_manifest, binding, model=model, replica=replica
                )
                if (
                    recovered_target["instance_id"] == target["instance_id"]
                    or recovered_target["receipt_hash"] == target["receipt_hash"]
                ):
                    raise RuntimeError(
                        "replica recovery reused the killed instance's compatibility evidence"
                    )
                recovery_ready_evidence = _validate_ready_evidence(outcome, plan, run_dir)
                recovery_canary = [_canary(endpoint, item.model_id) for item in plan.models]
                _atomic_json(root / name / "recovery_canary.json", recovery_canary)
                process.send_signal(signal.SIGTERM)
                expected_state = "STOPPED"
                expected_codes = {143}
                outcome_kind = "recovered_and_re_attested"
            else:
                recovered_target = None
                recovery_ready_evidence = None
                recovery_canary = None
                expected_state = "FAILED"
                expected_codes = set(range(1, 256)) - {143}
                outcome_kind = "failed_after_recovery_deadline"
            replica_failure_evidence = {
                "outcome": outcome_kind,
                "baseline_revision": status.revision,
                "baseline_manifest_hash": status.receipt_manifest_hash,
                "status_history": status_history,
                "outcome_revision": outcome.revision,
                "outcome_state": outcome.state,
                "outcome_reason_code": outcome.reason_code,
                "outcome_detail": outcome.detail,
                "recovered_target": recovered_target,
                "recovery_ready_evidence": recovery_ready_evidence,
                "recovery_canary": recovery_canary,
            }
            _atomic_json(root / name / "replica_failure_outcome.json", replica_failure_evidence)
        else:
            raise RuntimeError(f"unknown fault scenario {fault!r}")

        try:
            returncode = process.wait(timeout=_owner_exit_timeout_s(plan))
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(f"launcher did not terminate after {fault}") from exc
        if returncode not in expected_codes:
            raise RuntimeError(
                f"launcher return code {returncode} is invalid for {fault}; "
                f"expected one of {sorted(expected_codes)}"
            )
        terminal, report = _terminal_record(
            run_dir,
            expected_state,
            require_graceful_deployment=fault in {"operator_drain", "gateway_death"},
        )
        if fault == "gateway_death":
            gateway_evidence = _read_json(run_dir / "gateway_failure.json")
            if gateway_evidence.get("classification") != "process_dead":
                raise RuntimeError(f"gateway death was not classified exactly: {gateway_evidence}")
        else:
            gateway_evidence = None
        if fault == "worker_death":
            causal_text = f"{terminal.reason_code}: {terminal.detail}".lower()
            if not any(word in causal_text for word in ("worker", "ray", "rank")):
                raise RuntimeError(
                    f"worker death lacks a typed worker/ray/rank terminal cause: {causal_text}"
                )
        if fault == "replica_death" and expected_state == "FAILED":
            causal_text = f"{terminal.reason_code}: {terminal.detail}".lower()
            if not any(word in causal_text for word in ("readiness", "replica", "serve", "route")):
                raise RuntimeError(
                    f"replica death lacks a typed readiness/replica/Serve cause: {causal_text}"
                )
        return {
            "scenario": name,
            "passed": True,
            "fault": fault,
            "generation": generation,
            "deployment_plan_hash": plan.deployment_plan_hash,
            "advertised_endpoint": endpoint,
            "ready_revision": status.revision,
            "terminal_revision": terminal.revision,
            "terminal_state": terminal.state,
            "terminal_reason_code": terminal.reason_code,
            "terminal_detail": terminal.detail,
            "returncode": returncode,
            "duration_s": round(time.time() - started, 3),
            "ready_evidence": evidence,
            "fault_injection": fault_injection,
            "shutdown_report": report,
            "gateway_failure": gateway_evidence,
            "replica_failure": replica_failure_evidence,
        }
    finally:
        active_error = sys.exc_info()[1]
        cleanup_errors: list[BaseException] = []
        owner_cleanup_s = float(plan.control.watchdog_cleanup_deadline_s)
        cleanup_deadline = time.monotonic() + max(60.0, owner_cleanup_s + 60.0)
        # Ask the composition root to drain the tree it owns. Signalling the
        # entire process group first races children against their IPC/control
        # receivers and destroys the shutdown evidence this gate is meant to
        # validate. Group TERM/KILL remains the bounded last resort only.
        if process.poll() is None:
            try:
                process.send_signal(signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                if process.poll() is None:
                    process.wait(
                        timeout=max(
                            0.0,
                            min(owner_cleanup_s + 10.0, cleanup_deadline - time.monotonic()),
                        )
                    )
            except subprocess.TimeoutExpired:
                pass
            except BaseException as cleanup_exc:
                cleanup_errors.append(cleanup_exc)
        if _process_group_exists(process.pid):
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            term_deadline = min(cleanup_deadline, time.monotonic() + 10.0)
            while _process_group_exists(process.pid) and time.monotonic() < term_deadline:
                time.sleep(0.1)
            if _process_group_exists(process.pid):
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                kill_deadline = min(cleanup_deadline, time.monotonic() + 10.0)
                while _process_group_exists(process.pid) and time.monotonic() < kill_deadline:
                    time.sleep(0.1)
        if process.poll() is None:
            try:
                process.wait(timeout=max(0.0, cleanup_deadline - time.monotonic()))
            except BaseException as cleanup_exc:
                cleanup_errors.append(cleanup_exc)
        try:
            generation_cleanup = _cleanup_generation_on_nodes(
                nodes,
                deployment_id=plan.deployment_id,
                generation=generation,
                plan_hash=plan.deployment_plan_hash,
                run_dir=run_dir,
            )
            _atomic_json(
                root / name / "exact_generation_cleanup.json",
                {
                    "schema_version": 1,
                    "reports": generation_cleanup,
                },
            )
            reaped = [
                {
                    "hostname": report["hostname"],
                    "process": process_identity,
                }
                for report in generation_cleanup
                for process_identity in report["matched"]
            ]
            if reaped:
                cleanup_errors.append(
                    RuntimeError(
                        "qualification fallback had to reap exact generation "
                        f"processes left by the owner: {reaped}"
                    )
                )
        except BaseException as cleanup_exc:
            cleanup_errors.append(cleanup_exc)
        for tee in (out_tee, err_tee):
            try:
                tee.join(deadline=cleanup_deadline)
            except BaseException as cleanup_exc:
                cleanup_errors.append(cleanup_exc)
        if _process_group_exists(process.pid):
            cleanup_errors.append(
                RuntimeError(f"qualification launcher process group {process.pid} survived cleanup")
            )
        if local_listener is not None:
            try:
                local_listener.close()
                assert precondition is not None
                precondition["stopped"] = True
                _atomic_json(fault_evidence_path, precondition)
            except BaseException as cleanup_exc:
                cleanup_errors.append(cleanup_exc)
        if remote_holder is not None:
            try:
                remote_holder.stop()
            except BaseException as cleanup_exc:
                cleanup_errors.append(cleanup_exc)
        if cleanup_errors:
            if active_error is not None:
                for cleanup_error in cleanup_errors:
                    _add_note(
                        active_error,
                        f"qualification scenario cleanup also failed: {cleanup_error}",
                    )
            else:
                primary_cleanup = cleanup_errors[0]
                for cleanup_error in cleanup_errors[1:]:
                    _add_note(primary_cleanup, f"additional cleanup failure: {cleanup_error}")
                raise primary_cleanup


def _verdict_text(
    passed: bool,
    scenarios: list[dict],
    *,
    gate_id: str,
    logical_nodes: int,
    engine_mode: str = "null",
    error: str = "",
) -> str:
    engine_label = "null-compute" if engine_mode == "null" else "real-engine"
    lines = [f"# {gate_id} verdict", "", f"Verdict: **{'PASS' if passed else 'FAIL'}**", ""]
    for item in scenarios:
        lines.append(
            f"- `{item['scenario']}`: terminal `{item['terminal_state']}`, "
            f"exit `{item['returncode']}`, {item['duration_s']} s"
        )
    if error:
        lines.extend(["", "Failure:", "", "```text", error.rstrip(), "```"])
    lines.extend(
        [
            "",
            f"This verdict qualifies only the {logical_nodes}-node "
            f"Aurora/XPU/vLLM-{engine_label}/HAProxy",
            "lifecycle cell identified by the adjacent immutable plan, wheel hash, and PBS receipt.",
            "It does not qualify another engine mode, a higher scale, or product support scope.",
            "",
        ]
    )
    return "\n".join(lines)


def _scenario_matrix(profile: str) -> tuple[tuple[str, str], ...]:
    lifecycle = (
        ("normal-drain", "operator_drain"),
        ("gateway-death", "gateway_death"),
    )
    if profile == "lifecycle":
        return lifecycle
    if profile == "two_node":
        return lifecycle + (
            ("worker-death", "worker_death"),
            ("duplicate-gateway-port", "duplicate_gateway_port"),
            ("partial-worker-proxy", "partial_proxy_readiness"),
        )
    if profile == "four_node_real":
        return (
            ("normal-drain", "operator_drain"),
            ("replica-death", "replica_death"),
        )
    raise ValueError(f"unknown qualification scenario profile {profile!r}")


def _cleanup_generation_main(argv: list[str]) -> int:
    """Allocated-node helper for the harness's exact fallback cleanup."""
    parser = argparse.ArgumentParser(prog="run_final_null_qualification.py --cleanup-generation")
    parser.add_argument("--deployment-id", required=True)
    parser.add_argument("--generation", required=True, type=int)
    parser.add_argument("--plan-hash", required=True)
    parser.add_argument("--run-dir", required=True)
    args = parser.parse_args(argv)
    report = _cleanup_local_generation_processes(
        deployment_id=args.deployment_id,
        generation=args.generation,
        plan_hash=args.plan_hash,
        run_dir=Path(args.run_dir),
    )
    print(json.dumps(report, sort_keys=True, allow_nan=False), flush=True)
    return 0


def _signal_generation_main(argv: list[str]) -> int:
    """Allocated-node helper for one identity-fenced fault injection."""
    parser = argparse.ArgumentParser(prog="run_final_null_qualification.py --signal-generation-pid")
    parser.add_argument("--deployment-id", required=True)
    parser.add_argument("--generation", required=True, type=int)
    parser.add_argument("--plan-hash", required=True)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--pid", required=True, type=int)
    parser.add_argument("--signal", required=True, choices=("TERM", "KILL"))
    args = parser.parse_args(argv)
    report = _signal_local_generation_pid(
        args.pid,
        args.signal,
        deployment_id=args.deployment_id,
        generation=args.generation,
        plan_hash=args.plan_hash,
        run_dir=Path(args.run_dir),
    )
    print(json.dumps(report, sort_keys=True, allow_nan=False), flush=True)
    return 0


def main() -> int:
    if sys.argv[1:2] == ["--cleanup-generation"]:
        return _cleanup_generation_main(sys.argv[2:])
    if sys.argv[1:2] == ["--signal-generation-pid"]:
        return _signal_generation_main(sys.argv[2:])
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment-plan", required=True)
    parser.add_argument("--gate-id", required=True)
    args = parser.parse_args()

    gate_id = args.gate_id.strip()
    if not gate_id or any(
        character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for character in gate_id
    ):
        parser.error("--gate-id must contain only uppercase ASCII letters, digits, '-' and '_'")
    experiment_plan_path = Path(args.experiment_plan).resolve()
    try:
        experiment_plan, declared_gate, declared_paths = _load_declared_gate(
            experiment_plan_path,
            gate_id,
        )
    except RuntimeError as exc:
        parser.error(str(exc))
    candidate = experiment_plan["candidate"]
    output = declared_paths["output"]
    bootstrap = declared_paths["bootstrap"]
    wheel = declared_paths["wheel"]
    plan_path = declared_paths["deployment_plan"]
    site_path = declared_paths["site_profile"]
    args.acquisition_source = declared_gate["acquisition_source"]
    args.engine_mode = declared_gate["engine_mode"]
    args.scenario_profile = declared_gate["scenario_profile"]
    args.ready_timeout = float(declared_gate["ready_timeout_s"])
    args.partial_observation = float(declared_gate["partial_observation_s"])
    args.lease_ttl = declared_gate["lease_ttl"]
    args.expected_runtime = declared_gate["expected_runtime"]
    args.attempt = declared_gate["attempt"]
    try:
        output.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        parser.error(f"output must be a new directory so evidence cannot be overwritten: {output}")

    # The caller supplies exactly one immutable bootstrap path. Never fall back
    # to the checkout or an ambient editable install.
    current = [entry for entry in sys.path if entry]
    if str(bootstrap) not in current:
        raise SystemExit(f"PYTHONPATH must include immutable bootstrap {bootstrap}")
    _pin_bootstrap_environment(bootstrap)

    scenarios: list[dict] = []
    started = time.time()
    logical_nodes = 0
    try:
        from exaserve.plan.io import load_deployment_plan, load_site_profile

        plan = load_deployment_plan(str(plan_path))
        profile = load_site_profile(str(site_path))
        logical_nodes = plan.num_nodes
        if plan.site_profile_hash != profile.site_profile_hash:
            raise RuntimeError("plan/site identity is invalid for this gate")
        if profile.site_profile_hash != candidate["site_profile_hash"]:
            raise RuntimeError("gate SiteProfile differs from the declared candidate")
        if plan.compatibility_profile_hash != candidate["compatibility_profile_hash"]:
            raise RuntimeError("gate compatibility profile differs from the declared candidate")
        if plan.manifest_hash != candidate["compatibility_manifest_hash"]:
            raise RuntimeError("gate compatibility manifest differs from the declared candidate")
        if plan.deployment_id != gate_id.lower():
            raise RuntimeError("compiled deployment_id is not bound to the declared gate_id")
        if plan.num_nodes != declared_gate["logical_nodes"]:
            raise RuntimeError("compiled node count differs from the declared logical gate size")
        expected_null_compute = args.engine_mode == "null"
        if plan.runtime.null_compute is not expected_null_compute:
            raise RuntimeError(
                f"--engine-mode={args.engine_mode} disagrees with "
                f"plan.runtime.null_compute={plan.runtime.null_compute}"
            )
        if plan.gateway is None or plan.gateway.kind != "haproxy":
            raise RuntimeError("gate requires the exact HAProxy exposure plan")
        if args.scenario_profile == "two_node" and (
            plan.num_nodes != 2 or args.engine_mode != "null"
        ):
            raise RuntimeError(
                "two_node scenario profile is the exact two-node null-compute fault battery"
            )
        if args.scenario_profile == "four_node_real":
            exact_models = len(plan.models) == 1
            model = plan.models[0] if exact_models else None
            disjoint_ranks = (
                [rank for replica in model.replicas for rank in replica.planned_ranks]
                if model is not None
                else []
            )
            if (
                plan.num_nodes != 4
                or args.engine_mode != "real"
                or model is None
                or model.pipeline_parallel_size < 2
                or model.tensor_parallel_size < 2
                or model.num_replicas < 2
                or len(disjoint_ranks) != len(set(disjoint_ranks))
                or set(disjoint_ranks) != set(range(4))
            ):
                raise RuntimeError(
                    "four_node_real requires one real model with TP>=2, PP>=2, and "
                    "two or more disjoint replica bundles covering exactly four ranks"
                )
        nodes = _validated_nodes(logical_nodes, acquisition_source=args.acquisition_source)
        environment = _environment_receipt(nodes, bootstrap, wheel, gate_id=gate_id)
        if len(nodes) != declared_gate["physical_allocation_nodes"]:
            raise RuntimeError("physical allocation size differs from the declared gate")
        if environment["pbs_queue"] != declared_gate["queue"]:
            raise RuntimeError(
                f"PBS queue {environment['pbs_queue']!r} differs from declared "
                f"queue {declared_gate['queue']!r}"
            )
        _atomic_json(output / "environment.json", environment)
        manifest = {
            "schema_version": 1,
            "gate_id": gate_id,
            "lane": declared_gate["lane"],
            "experiment_plan_path": str(experiment_plan_path),
            "experiment_plan_sha256": _sha256_file(experiment_plan_path),
            "declared_gate": declared_gate,
            "started_at": started,
            "logical_nodes": logical_nodes,
            "physical_allocation_nodes": len(nodes),
            "acquisition_source": args.acquisition_source,
            "pbs_job_id": environment["pbs_job_id"],
            "queue": environment["pbs_queue"],
            "lease_ttl": args.lease_ttl.strip(),
            "expected_runtime": args.expected_runtime.strip(),
            "engine_mode": args.engine_mode,
            "scenario_profile": args.scenario_profile,
            "ready_timeout_s": args.ready_timeout,
            "partial_observation_s": args.partial_observation,
            "attempt_limit": declared_gate["attempt_limit"],
            "attempt": args.attempt,
            "deployment_plan_path": str(plan_path),
            "deployment_plan_hash": plan.deployment_plan_hash,
            "site_profile_path": str(site_path),
            "site_profile_hash": profile.site_profile_hash,
            "wheel": str(wheel),
            "wheel_sha256": environment["wheel_sha256"],
            "bootstrap": str(bootstrap),
            "harness": str(Path(__file__).resolve()),
            "harness_sha256": _sha256_file(Path(__file__).resolve()),
            "port_holder_helper": (
                "inline-stdlib-python" if args.scenario_profile == "two_node" else None
            ),
            "port_holder_helper_sha256": (
                hashlib.sha256(_REMOTE_PORT_HOLDER_CODE.encode()).hexdigest()
                if args.scenario_profile == "two_node"
                else None
            ),
            "expected_observations": list(declared_gate["expected_observations"]),
        }
        _atomic_json(output / "manifest.json", manifest)

        base_generation = time.time_ns()
        for offset, (name, fault) in enumerate(_scenario_matrix(args.scenario_profile)):
            scenarios.append(
                _launch_scenario(
                    name=name,
                    root=output,
                    plan_path=plan_path,
                    site_path=site_path,
                    site_profile=profile,
                    generation=base_generation + offset,
                    fault=fault,
                    ready_timeout_s=args.ready_timeout,
                    nodes=nodes,
                    partial_observation_s=args.partial_observation,
                )
            )
        result = {
            **manifest,
            "passed": True,
            "completed_at": time.time(),
            "duration_s": round(time.time() - started, 3),
            "scenarios": scenarios,
        }
        _atomic_json(output / "result.json", result)
        _atomic_text(
            output / "verdict.md",
            _verdict_text(
                True,
                scenarios,
                gate_id=gate_id,
                logical_nodes=logical_nodes,
                engine_mode=args.engine_mode,
            ),
        )
        print(f"QUALIFICATION_VERDICT|gate={gate_id}|PASS", flush=True)
        return 0
    except BaseException as exc:
        error = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        result = {
            "schema_version": 1,
            "gate_id": gate_id,
            "lane": declared_gate["lane"],
            "experiment_plan_path": str(experiment_plan_path),
            "experiment_plan_sha256": _sha256_file(experiment_plan_path),
            "declared_gate": declared_gate,
            "attempt_limit": declared_gate["attempt_limit"],
            "attempt": declared_gate["attempt"],
            "passed": False,
            "completed_at": time.time(),
            "duration_s": round(time.time() - started, 3),
            "scenarios": scenarios,
            "error": error,
        }
        _atomic_json(output / "result.json", result)
        _atomic_text(
            output / "verdict.md",
            _verdict_text(
                False,
                scenarios,
                gate_id=gate_id,
                logical_nodes=logical_nodes,
                engine_mode=args.engine_mode,
                error=error,
            ),
        )
        print(
            f"QUALIFICATION_VERDICT|gate={gate_id}|FAIL|{type(exc).__name__}: {exc}",
            flush=True,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
