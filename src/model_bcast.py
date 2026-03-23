import argparse
import json
import socket
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict, List

from model_paths import get_model_storage_name, get_model_storage_path, iter_unique_model_ids
from model_staging import (
    get_model_dir_state,
    print_red,
    stage_models,
    validate_tensor_parallel_compatibility,
)
from schemas import load_deployment_config


def compile_bcast(tools_dir: Path) -> Path:
    """
    Build the MPI broadcast helper if the binary is missing or stale.
    """
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


def run_cache_probe(script_path: Path, path: Path, num_nodes: int) -> List[Dict[str, str]]:
    """
    Probe the cache state on every allocated node via MPI.
    """
    cmd = [
        "mpiexec",
        "-n",
        str(num_nodes),
        "-ppn",
        "1",
        "--cpu-bind",
        "none",
        sys.executable,
        str(script_path),
        "--probe-cache",
        str(path),
    ]
    result = subprocess.run(
        cmd,
        check=False,
        text=True,
        capture_output=True,
        cwd=str(script_path.parent.parent),
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
    script_path: Path,
) -> str:
    """
    Return the aggregate cache state for a model across all nodes.
    """
    target_path = get_model_storage_path(model_id, local_stage_path)
    entries = run_cache_probe(script_path, target_path, num_nodes)
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
) -> Dict[str, str]:
    """
    Ensure every model exists on Lustre, then broadcast it to local storage.
    """
    project_root = Path(__file__).resolve().parent.parent
    tools_dir = project_root / "tools"
    script_path = Path(__file__).resolve()
    binary_path = compile_bcast(tools_dir)

    lustre_model_paths = stage_models(model_configs, lustre_path)
    local_model_paths: Dict[str, str] = {}

    for model_id in iter_unique_model_ids(model_configs):
        model_config = next(cfg for cfg in model_configs if cfg.model_id == model_id)
        validate_tensor_parallel_compatibility(
            model_id,
            Path(lustre_model_paths[model_id]),
            model_config.tensor_parallel_size,
        )
        target_path = get_model_storage_path(model_id, local_path)
        cache_state = check_cache_state(model_id, local_path, num_nodes, script_path)

        if cache_state == "complete":
            print(
                f"[ModelBcast] ✓ Reusing staged cache for {model_id} at {target_path}",
                flush=True,
            )
            local_model_paths[model_id] = str(target_path)
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
                cwd=str(project_root),
            )

        final_state = check_cache_state(model_id, local_path, num_nodes, script_path)
        if final_state != "complete":
            raise RuntimeError(
                f"[ModelBcast] Expected a complete staged cache for {model_id} after broadcast, "
                f"found state={final_state}"
            )
        print_red(f"[ModelBcast] ✓ Broadcast complete for {model_id}")
        local_model_paths[model_id] = str(target_path)

    return local_model_paths


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
    bcast_models(
        config.model_configs,
        config.model_storage_path,
        config.local_stage_path,
        args.num_nodes,
    )
    print_red("[ModelBcast] ✓ All models are ready in node-local storage")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
