import argparse
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from importlib import resources
from pathlib import Path
from typing import Dict, List, Tuple

from .model_paths import get_model_storage_name, get_model_storage_path, iter_unique_model_ids
from .model_staging import (
    get_model_dir_state,
    print_red,
    stage_models,
    validate_tensor_parallel_compatibility,
)
from .schemas import load_deployment_config


def _resource_bytes(name: str) -> bytes:
    return (resources.files("aurora_rayserver.resources") / name).read_bytes()


def _write_if_changed(path: Path, data: bytes) -> None:
    if path.exists() and path.read_bytes() == data:
        return
    path.write_bytes(data)


def _default_bcast_build_dir() -> Path:
    raw = os.environ.get("AURORA_BCAST_BUILD_DIR", "").strip()
    if raw:
        return Path(raw)

    run_log_dir = os.environ.get("AURORA_RUN_LOG_DIR", "").strip()
    if run_log_dir:
        return Path(run_log_dir) / "bcast_build"

    return Path.cwd() / ".aurora_rayserver_build" / "bcast"


def prepare_bcast_tools(build_dir: Path | None = None) -> Path:
    """Materialize packaged bcast sources into a writable shared build dir."""
    tools_dir = build_dir or _default_bcast_build_dir()
    tools_dir.mkdir(parents=True, exist_ok=True)
    _write_if_changed(tools_dir / "bcast.c", _resource_bytes("bcast.c"))
    _write_if_changed(tools_dir / "Makefile", _resource_bytes("bcast.Makefile"))
    return tools_dir


def compile_bcast(tools_dir: Path | None = None) -> Path:
    """
    Build the packaged MPI broadcast helper if the binary is missing or stale.
    """
    tools_dir = prepare_bcast_tools(tools_dir)
    binary_path = tools_dir / "bcast"
    source_path = tools_dir / "bcast.c"
    makefile_path = tools_dir / "Makefile"

    if not source_path.is_file():
        raise FileNotFoundError(f"Missing bcast source: {source_path}")
    if not makefile_path.is_file():
        raise FileNotFoundError(f"Missing bcast Makefile: {makefile_path}")

    binary_mtime = binary_path.stat().st_mtime if binary_path.exists() else -1
    source_mtime = max(source_path.stat().st_mtime, makefile_path.stat().st_mtime)
    if binary_mtime < source_mtime:
        print(f"[ModelBcast] Building {binary_path}...", flush=True)
        subprocess.run(
            ["make", "-C", str(tools_dir), "bcast"],
            check=True,
        )

    return binary_path


def probe_cache_locally(path: Path) -> None:
    """
    Probe one model directory on the current host and print a JSON result.
    """
    payload = {
        "host": socket.gethostname(),
        "path": str(path),
        "state": get_model_dir_state(path),
    }
    print(json.dumps(payload), flush=True)


def run_cache_probe(path: Path, num_nodes: int) -> List[Dict[str, str]]:
    """
    Probe the cache state on every allocated node via MPI.
    """
    # Invoke the module via -m (not by absolute path) so the package's
    # relative imports (`from .schemas import ...`) resolve. `python <abspath>`
    # would set __package__ to None and break the imports.
    cmd = [
        "mpiexec",
        "-n",
        str(num_nodes),
        "-ppn",
        "1",
        "--cpu-bind",
        "none",
        sys.executable,
        "-m",
        "aurora_rayserver.model_bcast",
        "--probe-cache",
        str(path),
    ]
    result = subprocess.run(
        cmd,
        check=False,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"[ModelBcast] Cache probe failed for {path}\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )

    entries: List[Dict[str, str]] = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    if len(entries) != num_nodes:
        raise RuntimeError(
            f"[ModelBcast] Expected {num_nodes} cache probe result(s) for {path}, "
            f"got {len(entries)}.\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return entries


def check_cache_state(
    model_id: str,
    local_stage_path: str,
    num_nodes: int,
) -> str:
    """
    Return the aggregate cache state for a model across all nodes.
    """
    target_path = get_model_storage_path(model_id, local_stage_path)
    entries = run_cache_probe(target_path, num_nodes)
    states = {entry["state"] for entry in entries}

    if states == {"complete"}:
        return "complete"
    if states == {"missing"}:
        return "missing"

    detail = ", ".join(f"{entry['host']}={entry['state']}" for entry in entries)
    raise RuntimeError(
        f"[ModelBcast] Refusing to reuse staged cache for {model_id}. "
        f"Cache state across nodes is inconsistent or partial: {detail}"
    )


def bcast_models(
    model_configs,
    lustre_path: str,
    local_path: str,
    num_nodes: int,
) -> Tuple[Dict[str, str], list[dict]]:
    """
    Ensure every model exists on Lustre, then broadcast it to local storage.
    """
    binary_path = compile_bcast()

    lustre_model_paths = stage_models(model_configs, lustre_path)
    local_model_paths: Dict[str, str] = {}

    per_model_timings: list[dict] = []

    for model_id in iter_unique_model_ids(model_configs):
        model_t0 = time.monotonic()
        model_config = next(cfg for cfg in model_configs if cfg.model_id == model_id)
        validate_tensor_parallel_compatibility(
            model_id,
            Path(lustre_model_paths[model_id]),
            model_config.tensor_parallel_size,
        )
        target_path = get_model_storage_path(model_id, local_path)
        cache_state = check_cache_state(model_id, local_path, num_nodes)

        if cache_state == "complete":
            print(
                f"[ModelBcast] ✓ Reusing staged cache for {model_id} at {target_path}",
                flush=True,
            )
            local_model_paths[model_id] = str(target_path)
            per_model_timings.append({
                "model_id": model_id, "cache_reused": True,
                "duration_s": round(time.monotonic() - model_t0, 4),
            })
            continue

        source_path = Path(lustre_model_paths[model_id])
        safe_name = get_model_storage_name(model_id)

        # HF cache snapshots use a revision hash as the directory name. Create a
        # temporary symlink with the stable model cache name so the extracted
        # node-local directory always lands at <local_stage_path>/<safe_name>.
        with tempfile.TemporaryDirectory(prefix=f"model-bcast-{safe_name}-") as tmpdir:
            bcast_source = source_path
            if source_path.name != safe_name:
                symlink_path = Path(tmpdir) / safe_name
                symlink_path.symlink_to(source_path, target_is_directory=True)
                bcast_source = symlink_path

            print(
                f"[ModelBcast] Broadcasting {model_id} from {source_path} to {target_path} "
                f"across {num_nodes} node(s)...",
                flush=True,
            )
            subprocess.run(
                [
                    "mpiexec",
                    "-n",
                    str(num_nodes),
                    "-ppn",
                    "1",
                    "--cpu-bind",
                    "none",
                    str(binary_path),
                    str(bcast_source),
                    str(local_path),
                ],
                check=True,
            )

        final_state = check_cache_state(model_id, local_path, num_nodes)
        if final_state != "complete":
            raise RuntimeError(
                f"[ModelBcast] Expected a complete staged cache for {model_id} after broadcast, "
                f"found state={final_state}"
            )
        print_red(f"[ModelBcast] ✓ Broadcast complete for {model_id}")
        local_model_paths[model_id] = str(target_path)
        per_model_timings.append({
            "model_id": model_id, "cache_reused": False,
            "duration_s": round(time.monotonic() - model_t0, 4),
        })

    return local_model_paths, per_model_timings


def main() -> int:
    parser = argparse.ArgumentParser(description="MPI model staging helper for Aurora")
    parser.add_argument("--config", help="Deployment or experiment YAML to load")
    parser.add_argument("--num-nodes", type=int, help="Allocated node count")
    parser.add_argument(
        "--probe-cache",
        help="Internal mode: print local cache state for one model path as JSON",
    )
    args = parser.parse_args()

    if args.probe_cache:
        probe_cache_locally(Path(args.probe_cache))
        return 0

    if not args.config:
        raise SystemExit("--config is required")
    if args.num_nodes is None or args.num_nodes < 1:
        raise SystemExit("--num-nodes must be >= 1")

    config = load_deployment_config(args.config)
    if args.num_nodes != config.num_nodes:
        raise SystemExit(
            f"--num-nodes ({args.num_nodes}) does not match config.num_nodes ({config.num_nodes})"
        )

    print(
        f"[ModelBcast] Preparing {len(list(iter_unique_model_ids(config.model_configs)))} unique "
        f"model(s) for {args.num_nodes} node(s)",
        flush=True,
    )
    overall_start = time.monotonic()
    _, per_model_timings = bcast_models(
        config.model_configs,
        config.model_storage_path,
        config.local_stage_path,
        args.num_nodes,
    )
    overall_s = round(time.monotonic() - overall_start, 4)

    # Write timing JSON to a well-known path for launch_cluster.sh to pick up
    timing = {"model_bcast_total_s": overall_s, "models": per_model_timings}
    timing_path = os.path.join(
        os.environ.get("AURORA_RUN_LOG_DIR", "/tmp"), "model_bcast_timing.json"
    )
    os.makedirs(os.path.dirname(timing_path), exist_ok=True)
    with open(timing_path, "w") as f:
        json.dump(timing, f)
    print(f"[ModelBcast] Timing: {overall_s:.1f}s total, {len(per_model_timings)} model(s)", flush=True)
    print_red("[ModelBcast] ✓ All models are ready in node-local storage")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
