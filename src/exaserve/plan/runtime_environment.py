"""Closed, node-local runtime environment derived from a canonical plan.

The allocation head is the only process allowed to retain paths to the shared
run bundle. Once the distribution transaction has published a runtime capsule,
every rank and descendant receives paths from :class:`RuntimePaths`; no ambient
Python, home, cache, log, plan, or binding path crosses that boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import re
from typing import Mapping


LOCAL_RUNTIME_ROOT_ENV = "EXASERVE_LOCAL_RUNTIME_ROOT"
LOCAL_STATE_ROOT_ENV = "EXASERVE_LOCAL_STATE_ROOT"
LOCAL_GO_DISPATCH_ENV = "EXASERVE_LOCAL_GO_DISPATCH"
QUALIFIED_PYTHON_ENV = "EXASERVE_QUALIFIED_PYTHON"
QUALIFIED_PYTHON_HASH_ENV = "EXASERVE_QUALIFIED_PYTHON_SHA256"
QUALIFIED_PYTHON_PROFILE_ENV = "EXASERVE_QUALIFIED_PYTHON_SITE_PROFILE_HASH"
COMPAT_SOURCE_PROFILE_ENV = "EXASERVE_COMPAT_SOURCES_NODE_PROFILE"
COMPAT_SOURCE_MANIFEST_ENV = "EXASERVE_COMPAT_SOURCES_NODE_MANIFEST"
SHARED_ROOTS_ENV = "EXASERVE_SHARED_ROOTS"
LOCAL_EVAL_MANIFEST_ENV = "EXASERVE_LOCAL_EVAL_MANIFEST"
LOCAL_RUN_PLAN_ENV = "EXASERVE_LOCAL_RUN_PLAN_PATH"
LOCAL_PLAN_ENV = "EXASERVE_LOCAL_PLAN_PATH"

_SAFE_ID = re.compile(r"[^A-Za-z0-9_.-]+")
_SEARCH_PATH_KEYS = (
    "PATH",
    "LD_LIBRARY_PATH",
    "LIBRARY_PATH",
    "CPATH",
    "CMAKE_PREFIX_PATH",
    "PKG_CONFIG_PATH",
)
_SHARED_ONLY_KEYS = {
    "EXASERVE_RUN_PLAN_PATH",
    "EXASERVE_EVAL_MANIFEST_PATH",
    "EXASERVE_OUTPUT_LOCATIONS",
    "EXASERVE_SOURCE_SNAPSHOT_PATH",
    "EXASERVE_MODEL_BCAST_RESULT",
    "PYTHONUSERBASE",
    "PYTHONSTARTUP",
    "VIRTUAL_ENV",
    "PYTHONHOME",
    "CONDA_PREFIX",
    "CONDA_DEFAULT_ENV",
}
_AMBIENT_ALLOWED_KEYS = {
    "LANG",
    "LANGUAGE",
    "LC_ALL",
    "TZ",
    "USER",
    "LOGNAME",
    "HOSTNAME",
    "TERM",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "REQUESTS_CA_BUNDLE",
    "CURL_CA_BUNDLE",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
    "RANK",
    "LOCAL_RANK",
    "WORLD_SIZE",
    "MASTER_ADDR",
    "MASTER_PORT",
    "CCL_PROCESS_LAUNCHER",
    "EXASERVE_DEPLOYMENT_ID",
    "EXASERVE_GENERATION",
    "EXASERVE_VENDOR",
    "EXASERVE_ENGINE",
    "EXASERVE_PLAN_HASH",
    "EXASERVE_SITE_PROFILE_HASH",
    "EXASERVE_ALLOCATION_BINDING_HASH",
    "EXASERVE_HEAD_IP",
    "EXASERVE_NUM_GPUS_PER_NODE",
    "EXASERVE_JOBID",
    "EXASERVE_SCHEDULER",
    "EXASERVE_RUN_ID",
    "EXASERVE_RUN_SEMANTIC_HASH",
    "EXASERVE_SOURCE_SNAPSHOT_HASH",
    "EXASERVE_COMPAT_PROFILE_ID",
    "EXASERVE_COMPAT_MANIFEST_HASH",
    "EXASERVE_COMPAT_DELIVERY",
    "EXASERVE_COMPAT_ROLE",
    "EXASERVE_ROOT_OWNS_READINESS",
    "EXASERVE_SCALING_TRACE",
    "EXASERVE_SCALING_TRACE_TOKEN",
    "EXASERVE_CONTROL_HOST",
    "EXASERVE_CONTROL_PORT",
    "EXASERVE_CONTROL_SECRET",
    "EXASERVE_RECEIPT_SOCKET",
    "EXASERVE_RECEIPT_RANK",
    "EXASERVE_RECEIPT_SLOT",
    "EXASERVE_RECEIPT_ROLE",
    "EXASERVE_RECEIPT_REQUIREMENT_ID_ENGINE",
    "EXASERVE_RECEIPT_COMPONENT_ID_ENGINE",
    "EXASERVE_RECEIPT_MODEL_ID_ENGINE",
    "EXASERVE_RECEIPT_DEVICE_IDS_ENGINE",
    "EXASERVE_RECEIPT_REPLICA_INDEX",
    "EXASERVE_ENGINE_WORKER_GLOBAL_RANK",
    "EXASERVE_ENGINE_WORKER_KIND",
    "EXASERVE_DEPLOYMENT_OBSERVATION_SOCKET",
    "EXASERVE_PROCESS_OWNERSHIP_ROOT",
    "EXASERVE_RUNTIME_OWNERSHIP_ROOT",
    "EXASERVE_MULTIPROC_SPAWN_LOCK",
    "EXASERVE_VLLM_PATCH_VERBOSE",
    "EXASERVE_QUALIFIED_PYTHON",
    "EXASERVE_QUALIFIED_PYTHON_SHA256",
    "EXASERVE_QUALIFIED_PYTHON_SITE_PROFILE_HASH",
    "EXASERVE_COMPAT_SOURCES_NODE_PROFILE",
    "EXASERVE_COMPAT_SOURCES_NODE_MANIFEST",
}
_AMBIENT_ALLOWED_PREFIXES = (
    "FI_",
    "GLOO_",
    "RAY_",
    "VLLM_",
    "ZE_",
    "SYCL_",
    "ONEAPI_",
    "CCL_",
    "OMP_",
    "KMP_",
    "MKL_",
    "DNNL_",
    "OPENBLAS_",
    "VECLIB_",
    "NUMEXPR_",
    "RAYON_",
    "TOKENIZERS_",
    "CUDA_",
    "HIP_",
    "ROCR_",
    "EXASERVE_VERSION_",
)
_AMBIENT_DISCARD_PREFIXES = (
    "PBS_",
    "SLURM_",
    "PALS_",
    "PMI_",
    "PMIX_",
    "OMPI_",
    # Module initialization is a head-side concern. Lmod encodes its loaded
    # module table and reference-counted search paths in ambient variables;
    # those values can retain the login-node HOME even after PATH itself has
    # been projected. They are bookkeeping, not application inputs, so never
    # expose them to ranks or treat their intentionally discarded values as a
    # child-environment leak.
    "LMOD_",
    "__LMOD_",
    "_ModuleTable",
    "MODULEPATH",
    "MODULESHOME",
    # Exported shell functions are neither data nor a supported child-runtime
    # extension point. In particular, Lmod exports ``module``/``ml`` this way.
    "BASH_FUNC_",
)
_AMBIENT_DISCARD_KEYS = {"LOADEDMODULES", "_LMFILES_"}
_OVERWRITTEN_PATH_KEYS = {
    "PWD",
    "OLDPWD",
    "HOME",
    "TMPDIR",
    "TMP",
    "TEMP",
    "PYTHONPATH",
    "PYTHONPYCACHEPREFIX",
    "XDG_CACHE_HOME",
    "XDG_CONFIG_HOME",
    "XDG_DATA_HOME",
    "XDG_RUNTIME_DIR",
    "IPYTHONDIR",
    "JUPYTER_CONFIG_DIR",
    "NUMBA_CACHE_DIR",
    "TORCH_EXTENSIONS_DIR",
    "MPLCONFIGDIR",
    "HF_HOME",
    "HF_HUB_CACHE",
    "HUGGINGFACE_HUB_CACHE",
    "TRANSFORMERS_CACHE",
    "TORCH_HOME",
    "TRITON_CACHE_DIR",
    "VLLM_CACHE_ROOT",
    "RAY_TMPDIR",
    "EXASERVE_RUN_LOG_DIR",
    "EXASERVE_RUN_LOG_ROOT",
    "EXASERVE_PLAN_PATH",
    "EXASERVE_SITE_PROFILE_PATH",
    "EXASERVE_ALLOCATION_BINDING_PATH",
    LOCAL_RUNTIME_ROOT_ENV,
    LOCAL_STATE_ROOT_ENV,
    LOCAL_GO_DISPATCH_ENV,
    LOCAL_EVAL_MANIFEST_ENV,
    LOCAL_RUN_PLAN_ENV,
    LOCAL_PLAN_ENV,
    SHARED_ROOTS_ENV,
}


class RuntimePathError(RuntimeError):
    """A managed child would retain or escape to shared storage."""


@dataclass(frozen=True)
class FilesystemExpectation:
    kind: str
    fstype: str = ""
    readonly: bool | None = None


@dataclass(frozen=True)
class FilesystemIdentity:
    mount_point: Path
    fstype: str
    readonly: bool
    device: int
    mount_id: int = 0


@dataclass(frozen=True)
class _MountInfo:
    mount_id: int
    mount_point: Path
    fstype: str
    readonly: bool


def parse_filesystem_expectation(value: str) -> FilesystemExpectation:
    if not isinstance(value, str) or not value:
        raise RuntimePathError("filesystem expectation must be non-empty text")
    parts = value.split(";")
    kind = parts[0]
    if kind == "lustre":  # legacy SiteProfile spelling
        return FilesystemExpectation("shared", fstype="lustre")
    if kind not in {"shared", "node_local", "immutable_read_only_site"}:
        raise RuntimePathError(f"unknown filesystem expectation kind: {kind!r}")
    options = {}
    for item in parts[1:]:
        if "=" not in item:
            raise RuntimePathError(f"malformed filesystem expectation option: {item!r}")
        key, option = item.split("=", 1)
        if key not in {"fstype", "readonly"} or not option or key in options:
            raise RuntimePathError(f"invalid filesystem expectation option: {item!r}")
        options[key] = option
    readonly = None
    if "readonly" in options:
        if options["readonly"] not in {"true", "false"}:
            raise RuntimePathError("filesystem readonly expectation must be true or false")
        readonly = options["readonly"] == "true"
    return FilesystemExpectation(kind, fstype=options.get("fstype", ""), readonly=readonly)


def _mountinfo_unescape(value: str) -> str:
    for encoded, decoded in (("\\040", " "), ("\\011", "\t"), ("\\012", "\n"), ("\\134", "\\")):
        value = value.replace(encoded, decoded)
    return value


def filesystem_identity(
    path: str | os.PathLike[str], *, mountinfo_path: str = "/proc/self/mountinfo"
) -> FilesystemIdentity:
    """Resolve the longest Linux mount identity for an existing local path."""

    return _filesystem_identity_from_mounts(path, _read_mountinfo(mountinfo_path))


def _read_mountinfo(mountinfo_path: str = "/proc/self/mountinfo") -> tuple[_MountInfo, ...]:
    try:
        lines = Path(mountinfo_path).read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise RuntimePathError(f"could not read Linux mount identity: {exc}") from exc
    mounts = []
    for line in lines:
        try:
            left, right = line.split(" - ", 1)
            left_fields = left.split()
            right_fields = right.split()
            mount_options = set(left_fields[5].split(","))
            super_options = set(right_fields[2].split(",")) if len(right_fields) > 2 else set()
            mounts.append(
                _MountInfo(
                    mount_id=int(left_fields[0]),
                    mount_point=Path(_mountinfo_unescape(left_fields[4])),
                    fstype=right_fields[0],
                    readonly="ro" in mount_options or "ro" in super_options,
                )
            )
        except (IndexError, ValueError):
            continue
    if not mounts:
        raise RuntimePathError("Linux mountinfo contains no usable entries")
    return tuple(mounts)


def _filesystem_identity_from_mounts(
    path: str | os.PathLike[str], mounts: tuple[_MountInfo, ...]
) -> FilesystemIdentity:
    candidate = _absolute(path, "filesystem identity path")
    probe = candidate
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    try:
        resolved = probe.resolve(strict=True)
        device = os.stat(resolved).st_dev
    except OSError as exc:
        raise RuntimePathError(
            f"could not inspect filesystem identity for {candidate}: {exc}"
        ) from exc
    matches = [
        mount
        for mount in mounts
        if resolved == mount.mount_point or mount.mount_point in resolved.parents
    ]
    if not matches:
        raise RuntimePathError(f"no mountinfo entry covers filesystem path: {resolved}")
    selected = max(matches, key=lambda item: len(item.mount_point.parts))
    return FilesystemIdentity(
        mount_point=selected.mount_point,
        fstype=selected.fstype,
        readonly=selected.readonly,
        device=device,
        mount_id=selected.mount_id,
    )


def validate_declared_filesystem(
    path: str | os.PathLike[str],
    *,
    policy,
    root_kind: str,
) -> FilesystemIdentity | None:
    """Validate a path against stable hash-bearing type/RO expectations.

    Per-node device numbers remain runtime evidence.  The SiteProfile hashes
    only stable root, filesystem-type, and read-only semantics.
    """

    if root_kind not in {"local_root", "shared_root", "site_root"}:
        raise RuntimePathError(f"unsupported filesystem root kind: {root_kind}")
    candidate = _absolute(path, "filesystem policy path")
    matches = []
    for key, value in getattr(policy, "filesystem_semantics", ()):
        prefix = f"{root_kind}:"
        if not key.startswith(prefix):
            continue
        root = _absolute(key.removeprefix(prefix), "filesystem policy root")
        if candidate == root or root in candidate.parents:
            matches.append((root, parse_filesystem_expectation(value)))
    if not matches:
        return None  # legacy profiles remain readable but are not type-qualified
    root, expectation = max(matches, key=lambda item: len(item[0].parts))
    expected_kind = {
        "local_root": "node_local",
        "shared_root": "shared",
        "site_root": "immutable_read_only_site",
    }[root_kind]
    if expectation.kind != expected_kind:
        raise RuntimePathError(
            f"filesystem policy for {root} declares {expectation.kind!r}, "
            f"expected {expected_kind!r}"
        )
    identity = filesystem_identity(candidate)
    if expectation.fstype and identity.fstype != expectation.fstype:
        raise RuntimePathError(
            f"filesystem type for {candidate} is {identity.fstype!r}, "
            f"expected {expectation.fstype!r}"
        )
    if expectation.readonly is not None and identity.readonly != expectation.readonly:
        raise RuntimePathError(
            f"filesystem read-only state for {candidate} is {identity.readonly}, "
            f"expected {expectation.readonly}"
        )
    return identity


def _absolute(path: str | os.PathLike[str], name: str) -> Path:
    if not isinstance(path, (str, os.PathLike)) or not str(path):
        raise RuntimePathError(f"{name} must be a non-empty absolute path")
    value = Path(path)
    if not value.is_absolute():
        raise RuntimePathError(f"{name} must be an absolute path: {value}")
    if "\x00" in str(value) or ".." in value.parts:
        raise RuntimePathError(f"{name} must be normalized and contain no NUL: {value}")
    return value


def _contained(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _reject_existing_symlink_components(path: Path, name: str) -> None:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        if not os.path.lexists(current):
            return
        if current.is_symlink():
            raise RuntimePathError(f"{name} contains a symlink: {current}")


def shared_roots(policy=None) -> tuple[Path, ...]:
    """Return declared shared roots, retaining Aurora legacy compatibility.

    New profiles may encode roots as ``shared_root:/absolute/path`` keys in
    ``filesystem_semantics``. Older Aurora profiles predate that spelling;
    their stable site identity still implies the two documented Lustre roots.
    This keeps historical artifacts readable while new child launches fail
    closed.
    """

    roots: set[Path] = set()
    for value in os.environ.get(SHARED_ROOTS_ENV, "").split(os.pathsep):
        if value:
            roots.add(_absolute(value, "shared filesystem root").resolve())
    semantics = getattr(policy, "filesystem_semantics", ())
    for key, _value in semantics:
        prefix = "shared_root:"
        if isinstance(key, str) and key.startswith(prefix):
            roots.add(_absolute(key[len(prefix) :], "shared filesystem root").resolve())
    site_id = getattr(policy, "site_id", getattr(policy, "site_profile_id", ""))
    if policy is None or site_id == "alcf-aurora":
        roots.update((Path("/home"), Path("/lus/flare")))
    model_root = getattr(policy, "model_storage_path", "")
    if model_root:
        candidate = Path(model_root)
        # Model input is shared by definition. Prefer the mount root rather
        # than treating one model directory as the whole boundary.
        if _contained(candidate, Path("/lus/flare")):
            roots.add(Path("/lus/flare"))
        elif _contained(candidate, Path("/home")):
            roots.add(Path("/home"))
    return tuple(sorted(roots, key=lambda item: (len(item.parts), str(item))))


def path_is_shared(path: str | os.PathLike[str], policy=None) -> bool:
    candidate = _absolute(path, "runtime path")
    # Resolve the existing prefix as well as the lexical path. A symlink below
    # a seemingly local directory must not make a shared path acceptable.
    variants = {candidate, Path(os.path.realpath(candidate))}
    return any(_contained(variant, root) for variant in variants for root in shared_roots(policy))


def require_non_shared_path(
    path: str | os.PathLike[str],
    *,
    policy=None,
    name: str = "runtime path",
) -> str:
    candidate = _absolute(path, name)
    if path_is_shared(candidate, policy):
        raise RuntimePathError(f"{name} resolves beneath a declared shared root: {candidate}")
    return str(candidate)


def require_contained_local_path(
    path: str | os.PathLike[str],
    root: str | os.PathLike[str],
    *,
    policy=None,
    name: str = "runtime path",
    require_exists: bool = False,
) -> str:
    """Prove lexical/real containment and reject symlink or mount escapes."""

    candidate = _absolute(path, name)
    root_path = _absolute(root, f"{name} root")
    require_non_shared_path(candidate, policy=policy, name=name)
    require_non_shared_path(root_path, policy=policy, name=f"{name} root")
    lexical_root = Path(os.path.normpath(root_path))
    lexical = Path(os.path.normpath(candidate))
    _reject_existing_symlink_components(lexical_root, f"{name} root")
    if not _contained(lexical, lexical_root):
        raise RuntimePathError(f"{name} escapes its approved local root: {candidate}")
    if not root_path.exists():
        if require_exists:
            raise RuntimePathError(f"{name} root does not exist: {root_path}")
        ancestor = root_path
        while not ancestor.exists() and ancestor != ancestor.parent:
            ancestor = ancestor.parent
        require_non_shared_path(ancestor, policy=policy, name=f"{name} existing ancestor")
        return str(candidate)
    resolved_root = root_path.resolve(strict=True)
    require_non_shared_path(resolved_root, policy=policy, name=f"{name} root")
    if root_path.is_symlink():
        raise RuntimePathError(f"{name} root must not be a symlink: {root_path}")
    current = root_path
    relative = lexical.relative_to(lexical_root)
    from ..state.mounts import MountBoundaryError, nested_mount_points

    try:
        nested_mounts = nested_mount_points(resolved_root)
    except MountBoundaryError as exc:
        raise RuntimePathError(str(exc)) from exc
    for part in relative.parts:
        current = current / part
        if not current.exists() and not current.is_symlink():
            if require_exists:
                raise RuntimePathError(f"{name} does not exist: {candidate}")
            break
        if current.is_symlink():
            raise RuntimePathError(f"{name} contains a symlink: {current}")
        resolved = current.resolve(strict=True)
        if not _contained(resolved, resolved_root):
            raise RuntimePathError(f"{name} resolves outside its local root: {current}")
        if resolved in nested_mounts:
            raise RuntimePathError(f"{name} crosses a filesystem boundary: {current}")
    return str(candidate)


@dataclass(frozen=True)
class RuntimePaths:
    """The fixed layout published by the allocation-wide transaction."""

    root: Path
    python_root: Path
    run_root: Path
    bin_root: Path
    plan_path: Path
    site_profile_path: Path
    binding_path: Path
    bcast_path: Path
    go_dispatch_path: Path
    eval_manifest_path: Path
    run_plan_path: Path
    state_root: Path
    home: Path
    tmp: Path
    cache: Path
    logs: Path
    diagnostics: Path
    ray: Path

    @classmethod
    def from_roots(
        cls,
        runtime_root: str | os.PathLike[str],
        state_root: str | os.PathLike[str],
        *,
        policy=None,
        require_runtime: bool = False,
    ) -> "RuntimePaths":
        runtime = Path(
            require_non_shared_path(runtime_root, policy=policy, name="local runtime root")
        )
        state = Path(require_non_shared_path(state_root, policy=policy, name="local state root"))
        if policy is not None and hasattr(policy, "filesystem_semantics"):
            from ..site import require_complete_filesystem_policy

            require_complete_filesystem_policy(policy)
            for local_path in (runtime, state):
                if (
                    validate_declared_filesystem(local_path, policy=policy, root_kind="local_root")
                    is None
                ):
                    raise RuntimePathError(
                        f"local path has no declared filesystem identity: {local_path}"
                    )
        if runtime.exists():
            require_contained_local_path(
                runtime,
                runtime,
                policy=policy,
                name="local runtime root",
                require_exists=True,
            )
            if not runtime.is_dir():
                raise RuntimePathError(f"local runtime root is not a directory: {runtime}")
        elif require_runtime:
            raise RuntimePathError(f"local runtime root does not exist: {runtime}")
        return cls(
            root=runtime,
            python_root=runtime / "python",
            run_root=runtime / "run",
            bin_root=runtime / "bin",
            plan_path=runtime / "run" / "deployment.plan.json",
            site_profile_path=runtime / "run" / "site.profile.json",
            binding_path=runtime / "run" / "allocation_binding.json",
            bcast_path=runtime / "bin" / "bcast",
            go_dispatch_path=runtime / "bin" / "go_dispatch",
            eval_manifest_path=runtime / "run" / "eval_manifest.yaml",
            run_plan_path=runtime / "run" / "run.plan.json",
            state_root=state,
            home=state / "home",
            tmp=state / "tmp",
            cache=state / "cache",
            logs=state / "logs",
            diagnostics=state / "diagnostics",
            ray=state / "ray",
        )

    def prepare_state(self, *, policy=None) -> None:
        from ..model_staging import ensure_node_local_directory

        declared_shared = tuple(shared_roots(policy))
        if policy is not None and hasattr(policy, "filesystem_semantics"):
            from ..site import require_complete_filesystem_policy

            require_complete_filesystem_policy(policy)
            if (
                validate_declared_filesystem(
                    self.state_root,
                    policy=policy,
                    root_kind="local_root",
                )
                is None
            ):
                raise RuntimePathError(
                    f"local state root has no declared filesystem identity: {self.state_root}"
                )
        for path in (
            self.state_root,
            self.home,
            self.tmp,
            self.cache,
            self.logs,
            self.diagnostics,
            self.ray,
            self.cache / "pycache",
            self.cache / "huggingface" / "hub",
            self.cache / "torch",
            self.cache / "triton",
            self.cache / "vllm",
            self.cache / "numba",
            self.cache / "torch_extensions",
            self.cache / "matplotlib",
            self.cache / "ipython",
            self.state_root / "config" / "jupyter",
            self.state_root / "data",
            self.state_root / "runtime",
        ):
            require_contained_local_path(
                path,
                self.state_root,
                policy=policy,
                name="local state path",
                require_exists=False,
            )
            try:
                ensure_node_local_directory(
                    path,
                    mode=0o700,
                    enforce_mode=True,
                    shared_roots=declared_shared,
                )
            except (RuntimeError, ValueError) as exc:
                raise RuntimePathError(
                    f"local state path could not be created safely: {path}: {exc}"
                ) from exc
            if os.stat(path, follow_symlinks=False).st_uid != os.getuid():
                raise RuntimePathError(f"local state path is not owned by this uid: {path}")
            require_contained_local_path(
                path,
                self.state_root,
                policy=policy,
                name="local state path",
                require_exists=True,
            )

    def verify_capsule(self, *, policy=None) -> None:
        for entry in (self.root, *self.root.rglob("*")):
            try:
                metadata = os.lstat(entry)
            except OSError as exc:
                raise RuntimePathError(
                    f"runtime capsule entry is unreadable: {entry}: {exc}"
                ) from exc
            if entry.is_symlink():
                raise RuntimePathError(f"runtime capsule contains a symlink: {entry}")
            if metadata.st_uid != os.getuid():
                raise RuntimePathError(f"runtime capsule entry has the wrong owner: {entry}")
            if metadata.st_mode & 0o222:
                raise RuntimePathError(f"runtime capsule entry is writable: {entry}")
        for name, path in (
            ("capsule Python root", self.python_root),
            ("capsule run root", self.run_root),
            ("capsule bin root", self.bin_root),
            ("capsule DeploymentPlan", self.plan_path),
            ("capsule SiteProfile", self.site_profile_path),
            ("capsule AllocationBinding", self.binding_path),
        ):
            require_contained_local_path(
                path,
                self.root,
                policy=policy,
                name=name,
                require_exists=True,
            )
        if (
            not self.python_root.is_dir()
            or not self.run_root.is_dir()
            or not self.bin_root.is_dir()
        ):
            raise RuntimePathError("runtime capsule is missing python/run/bin directories")
        for path in (self.plan_path, self.site_profile_path, self.binding_path):
            if not path.is_file():
                raise RuntimePathError(f"runtime capsule artifact is not a regular file: {path}")
        for path in (self.bcast_path, self.go_dispatch_path):
            if path.exists():
                require_contained_local_path(
                    path,
                    self.root,
                    policy=policy,
                    name="capsule executable",
                    require_exists=True,
                )
                if not path.is_file() or not os.access(path, os.X_OK):
                    raise RuntimePathError(f"capsule helper is not executable: {path}")
        optional_eval = (self.eval_manifest_path.exists(), self.run_plan_path.exists())
        if any(optional_eval) and not all(optional_eval):
            raise RuntimePathError(
                "runtime capsule must contain eval manifest and RunPlan together"
            )
        if all(optional_eval):
            for path in (self.eval_manifest_path, self.run_plan_path):
                require_contained_local_path(
                    path,
                    self.root,
                    policy=policy,
                    name="capsule evaluation artifact",
                    require_exists=True,
                )


def default_local_state_root(plan, generation: int, rank: int = 0) -> str:
    if isinstance(generation, bool) or not isinstance(generation, int) or generation < 0:
        raise RuntimePathError("generation must be a non-negative integer")
    if isinstance(rank, bool) or not isinstance(rank, int) or rank < 0:
        raise RuntimePathError("rank must be a non-negative integer")
    raw = str(plan.deployment_id)
    safe = _SAFE_ID.sub("_", raw).strip("._")[:24] or "deployment"
    identity = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    # The supported launcher contract is exactly one rank per node. Identical
    # spelling lets the distribution transaction create this directory before
    # the first Python rank process starts, while the physical directories are
    # naturally distinct on each node-local filesystem.
    return f"/tmp/exaserve/state/{safe}.{identity}/g{generation}"


def runtime_paths(
    plan,
    *,
    generation: int,
    rank: int = 0,
    runtime_root: str | None = None,
    state_root: str | None = None,
    policy=None,
    require_runtime: bool = True,
) -> RuntimePaths:
    root = runtime_root or os.environ.get(LOCAL_RUNTIME_ROOT_ENV, "")
    if not root:
        raise RuntimePathError(f"{LOCAL_RUNTIME_ROOT_ENV} is required after distribution")
    state = state_root or os.environ.get(LOCAL_STATE_ROOT_ENV, "")
    if not state:
        state = default_local_state_root(plan, generation, rank)
    return RuntimePaths.from_roots(
        root, state, policy=policy or plan, require_runtime=require_runtime
    )


def staged_pythonpath(plan, inherited: str = "", *, runtime_root: str | None = None) -> str:
    """Return the closed post-staging Python search path.

    ``inherited`` remains in the signature for source compatibility but is
    deliberately ignored. Carrying it forward was the shared-repository and
    user-site escape that this boundary exists to remove.
    """

    del inherited
    from ..compat.generated_overlay import overlay_root_for

    root = runtime_root or os.environ.get(LOCAL_RUNTIME_ROOT_ENV, "")
    if not root:
        raise RuntimePathError(f"{LOCAL_RUNTIME_ROOT_ENV} is required for staged PYTHONPATH")
    python_root = Path(root) / "python"
    overlay = overlay_root_for(plan.compatibility_profile_hash, runtime_root=root)
    return os.pathsep.join((overlay, str(python_root)))


def _clean_search_path(value: str, *, policy=None) -> str:
    kept = []
    for item in value.split(os.pathsep):
        if not item or not os.path.isabs(item):
            continue
        if path_is_shared(item, policy):
            continue
        kept.append(item)
    return os.pathsep.join(dict.fromkeys(kept))


def shared_path_tokens(value: str, *, policy=None) -> tuple[str, ...]:
    """Find absolute shared-root tokens even in nonstandard environment keys."""

    roots = shared_roots(policy)
    if not roots:
        return ()
    # Environment path values are normally colon-separated, but arguments and
    # JSON-ish values can place paths after whitespace, commas, quotes, or '='.
    # Extract absolute tokens without treating URL ``//`` components as paths.
    candidates = re.findall(r"(?:(?<=^)|(?<=[\s=,:;'\"\[(]))(/[^\s,;:'\"\])}]*)", value)
    return tuple(
        candidate
        for candidate in candidates
        if any(_contained(Path(os.path.normpath(candidate)), root) for root in roots)
    )


def _ambient_allowed(key: str, prepared: frozenset[str]) -> bool:
    return (
        key in _SEARCH_PATH_KEYS
        or key in _AMBIENT_ALLOWED_KEYS
        or key in prepared
        or key.startswith(_AMBIENT_ALLOWED_PREFIXES)
    )


def _ambient_discarded(key: str) -> bool:
    return key in _AMBIENT_DISCARD_KEYS or key.startswith(_AMBIENT_DISCARD_PREFIXES)


def closed_runtime_environment(
    plan,
    *,
    paths: RuntimePaths,
    base_environment: Mapping[str, str] | None = None,
    policy=None,
) -> dict[str, str]:
    """Project one closed environment for ranks, Ray, actors, and engines."""

    source = os.environ if base_environment is None else base_environment
    if not isinstance(source, Mapping) or any(
        not isinstance(key, str) or not isinstance(value, str) for key, value in source.items()
    ):
        raise RuntimePathError("base runtime environment must be a string mapping")
    exempt = _SHARED_ONLY_KEYS | _OVERWRITTEN_PATH_KEYS | set(_SEARCH_PATH_KEYS)
    leaked = {}
    for key, value in source.items():
        if key in exempt or _ambient_discarded(key):
            continue
        paths_found = shared_path_tokens(value, policy=policy or plan)
        if paths_found:
            leaked[key] = paths_found
    if leaked:
        raise RuntimePathError(
            "ambient child environment contains shared-storage path(s): "
            + "; ".join(f"{key}={list(paths)!r}" for key, paths in sorted(leaked.items()))
        )
    prepared_environment = dict(getattr(policy or plan, "prepared_environment", ()))
    prepared_names = frozenset(prepared_environment)
    selected = {
        key: value
        for key, value in source.items()
        if key not in _SHARED_ONLY_KEYS
        and not _ambient_discarded(key)
        and _ambient_allowed(key, prepared_names)
    }
    for key in _SEARCH_PATH_KEYS:
        if key in selected:
            cleaned = _clean_search_path(selected[key], policy=policy or plan)
            if cleaned:
                selected[key] = cleaned
            else:
                selected.pop(key, None)
    for key in ("VIRTUAL_ENV", "PYTHONHOME", "CONDA_PREFIX", "CONDA_DEFAULT_ENV"):
        selected.pop(key, None)
    for key in getattr(policy or plan, "environment_unset", ()):
        selected.pop(key, None)
    invalid_prepared = {}
    for key, value in prepared_environment.items():
        shared = shared_path_tokens(value, policy=policy or plan)
        if shared:
            invalid_prepared[key] = shared
    if invalid_prepared:
        raise RuntimePathError(
            "SiteProfile prepared environment contains shared-storage path(s): "
            + "; ".join(
                f"{key}={list(values)!r}" for key, values in sorted(invalid_prepared.items())
            )
        )
    selected.update(prepared_environment)
    selected.update(runtime_environment(plan))
    from ..compat.generated_overlay import ROOT_ENV, overlay_root_for

    selected[ROOT_ENV] = overlay_root_for(
        plan.compatibility_profile_hash,
        runtime_root=str(paths.root),
    )
    qualified_python = selected.get(QUALIFIED_PYTHON_ENV, "")
    if not qualified_python:
        raise RuntimePathError(
            f"{QUALIFIED_PYTHON_ENV} is required for managed runtime descendants"
        )
    require_non_shared_path(
        qualified_python,
        policy=policy or plan,
        name="qualified Python executable",
    )
    try:
        resolved_python = Path(qualified_python).resolve(strict=True)
    except OSError as exc:
        raise RuntimePathError(
            f"qualified Python is unavailable: {qualified_python}: {exc}"
        ) from exc
    if not resolved_python.is_file() or not os.access(resolved_python, os.X_OK):
        raise RuntimePathError(
            f"qualified Python is not an executable regular file: {qualified_python}"
        )
    selected[QUALIFIED_PYTHON_ENV] = str(resolved_python)
    qualified_hash = selected.get(QUALIFIED_PYTHON_HASH_ENV, "")
    if re.fullmatch(r"[0-9a-f]{64}", qualified_hash) is None:
        raise RuntimePathError(
            f"{QUALIFIED_PYTHON_HASH_ENV} must carry the distributed per-node proof"
        )
    if selected.get(QUALIFIED_PYTHON_PROFILE_ENV) != plan.site_profile_hash:
        raise RuntimePathError(f"{QUALIFIED_PYTHON_PROFILE_ENV} does not match DeploymentPlan")
    if selected.get(COMPAT_SOURCE_PROFILE_ENV) != plan.compatibility_profile_hash:
        raise RuntimePathError(f"{COMPAT_SOURCE_PROFILE_ENV} does not match DeploymentPlan")
    if selected.get(COMPAT_SOURCE_MANIFEST_ENV) != plan.manifest_hash:
        raise RuntimePathError(f"{COMPAT_SOURCE_MANIFEST_ENV} does not match DeploymentPlan")
    selected.update(
        {
            LOCAL_RUNTIME_ROOT_ENV: str(paths.root),
            LOCAL_STATE_ROOT_ENV: str(paths.state_root),
            "PYTHONPATH": staged_pythonpath(plan, runtime_root=str(paths.root)),
            "PYTHONNOUSERSITE": "1",
            "PYTHONSAFEPATH": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPYCACHEPREFIX": str(paths.cache / "pycache"),
            "HOME": str(paths.home),
            "TMPDIR": str(paths.tmp),
            "TMP": str(paths.tmp),
            "TEMP": str(paths.tmp),
            "XDG_CACHE_HOME": str(paths.cache),
            "XDG_CONFIG_HOME": str(paths.state_root / "config"),
            "XDG_DATA_HOME": str(paths.state_root / "data"),
            "XDG_RUNTIME_DIR": str(paths.state_root / "runtime"),
            "IPYTHONDIR": str(paths.cache / "ipython"),
            "JUPYTER_CONFIG_DIR": str(paths.state_root / "config" / "jupyter"),
            "NUMBA_CACHE_DIR": str(paths.cache / "numba"),
            "TORCH_EXTENSIONS_DIR": str(paths.cache / "torch_extensions"),
            "MPLCONFIGDIR": str(paths.cache / "matplotlib"),
            "HF_HOME": str(paths.cache / "huggingface"),
            "HF_HUB_OFFLINE": "1",
            "HF_DATASETS_OFFLINE": "1",
            "TRANSFORMERS_OFFLINE": "1",
            "HF_HUB_CACHE": str(paths.cache / "huggingface" / "hub"),
            "HUGGINGFACE_HUB_CACHE": str(paths.cache / "huggingface" / "hub"),
            "TRANSFORMERS_CACHE": str(paths.cache / "huggingface" / "hub"),
            "TORCH_HOME": str(paths.cache / "torch"),
            "TRITON_CACHE_DIR": str(paths.cache / "triton"),
            "VLLM_CACHE_ROOT": str(paths.cache / "vllm"),
            "RAY_TMPDIR": str(paths.ray),
            "EXASERVE_RUN_LOG_DIR": str(paths.logs),
            "EXASERVE_PLAN_PATH": str(paths.plan_path),
            LOCAL_PLAN_ENV: str(paths.plan_path),
            "EXASERVE_SITE_PROFILE_PATH": str(paths.site_profile_path),
            "EXASERVE_ALLOCATION_BINDING_PATH": str(paths.binding_path),
        }
    )
    selected.pop("EXASERVE_RUN_LOG_ROOT", None)
    selected.pop("ONEAPI_DEVICE_SELECTOR", None)
    if paths.go_dispatch_path.is_file() and os.access(paths.go_dispatch_path, os.X_OK):
        selected[LOCAL_GO_DISPATCH_ENV] = str(paths.go_dispatch_path)
    else:
        selected.pop(LOCAL_GO_DISPATCH_ENV, None)
    if paths.eval_manifest_path.is_file():
        selected[LOCAL_EVAL_MANIFEST_ENV] = str(paths.eval_manifest_path)
    else:
        selected.pop(LOCAL_EVAL_MANIFEST_ENV, None)
    if paths.run_plan_path.is_file():
        selected[LOCAL_RUN_PLAN_ENV] = str(paths.run_plan_path)
    else:
        selected.pop(LOCAL_RUN_PLAN_ENV, None)
    return selected


def assert_worker_launch_is_local(
    *,
    argv,
    cwd: str | os.PathLike[str],
    environment: Mapping[str, str],
    policy=None,
) -> None:
    """Reject a non-head descriptor containing a declared shared path."""

    if isinstance(argv, (str, bytes)) or not argv:
        raise RuntimePathError("worker argv must be a non-empty argument vector")
    from ..site import AURORA_PMIX_PREPARED_ENVIRONMENT

    prepared = dict(getattr(policy, "prepared_environment", ()))
    qualified_pmix = {
        name: expected
        for name, expected in AURORA_PMIX_PREPARED_ENVIRONMENT
        if getattr(policy, "site_id", "") == "alcf-aurora" and prepared.get(name) == expected
    }
    forbidden_transport = sorted(
        key
        for key, value in environment.items()
        if key == SHARED_ROOTS_ENV
        or (
            key.startswith(("PBS_", "SLURM_", "PALS_", "PMI_", "PMIX_", "OMPI_"))
            and qualified_pmix.get(key) != value
        )
    )
    if forbidden_transport:
        raise RuntimePathError(
            f"worker environment retains launcher-only scheduler state: {forbidden_transport}"
        )
    require_non_shared_path(cwd, policy=policy, name="worker cwd")
    for index, item in enumerate(argv):
        if not isinstance(item, str) or not item:
            raise RuntimePathError("worker argv must contain non-empty strings")
        candidates = [item]
        if "=" in item:
            candidates.append(item.split("=", 1)[1])
        if any(
            candidate.startswith("/") and path_is_shared(candidate, policy)
            for candidate in candidates
        ):
            raise RuntimePathError(f"worker argv[{index}] names shared storage: {item}")
    path_keys = {
        "HOME",
        "TMPDIR",
        "TMP",
        "TEMP",
        "PYTHONPYCACHEPREFIX",
        "XDG_CACHE_HOME",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "XDG_RUNTIME_DIR",
        "IPYTHONDIR",
        "JUPYTER_CONFIG_DIR",
        "NUMBA_CACHE_DIR",
        "TORCH_EXTENSIONS_DIR",
        "MPLCONFIGDIR",
        "HF_HOME",
        "HF_HUB_CACHE",
        "HUGGINGFACE_HUB_CACHE",
        "TRANSFORMERS_CACHE",
        "TORCH_HOME",
        "TRITON_CACHE_DIR",
        "VLLM_CACHE_ROOT",
        "RAY_TMPDIR",
        "EXASERVE_RUN_LOG_DIR",
        "EXASERVE_PLAN_PATH",
        "EXASERVE_SITE_PROFILE_PATH",
        "EXASERVE_ALLOCATION_BINDING_PATH",
        LOCAL_RUNTIME_ROOT_ENV,
        LOCAL_STATE_ROOT_ENV,
        LOCAL_GO_DISPATCH_ENV,
        LOCAL_EVAL_MANIFEST_ENV,
        LOCAL_RUN_PLAN_ENV,
        LOCAL_PLAN_ENV,
        QUALIFIED_PYTHON_ENV,
        QUALIFIED_PYTHON_HASH_ENV,
        QUALIFIED_PYTHON_PROFILE_ENV,
    }
    for key, value in environment.items():
        values = (
            value.split(os.pathsep) if key in _SEARCH_PATH_KEYS or key == "PYTHONPATH" else [value]
        )
        if key not in path_keys and key not in _SEARCH_PATH_KEYS and key != "PYTHONPATH":
            continue
        for item in values:
            if item.startswith("/") and path_is_shared(item, policy):
                raise RuntimePathError(f"worker environment {key} names shared storage: {item}")
    if environment.get("PYTHONNOUSERSITE") != "1":
        raise RuntimePathError("worker environment must set PYTHONNOUSERSITE=1")
    if environment.get("PYTHONSAFEPATH") != "1":
        raise RuntimePathError("worker environment must set PYTHONSAFEPATH=1")


def runtime_environment(plan) -> dict[str, str]:
    """Return immutable plan-derived values independent of path placement."""

    policy = plan.runtime
    pp_enabled = any(model.pipeline_parallel_size > 1 for model in plan.models)
    multiproc_enabled = any(
        model.pipeline_parallel_size == 1 and model.tensor_parallel_size > 1
        for model in plan.models
    )
    diagnostics = bool(policy.instrumentation or plan.collect_stats)
    from ..compat.generated_overlay import ROOT_ENV, overlay_root_for
    from ..compat.profile import (
        MULTIPROC_WORKER_PATCH_GATE,
        PP_PATCH_GATE,
        RAY_WORKER_PATCH_GATE,
    )

    values = {
        "EXASERVE_NULL_COMPUTE": "1" if policy.null_compute else "0",
        "EXASERVE_NULL_COMPUTE_LATENCY": str(policy.null_compute_latency_s),
        "EXASERVE_PP_SHARD_AWARE": "1" if policy.pp_shard_aware else "0",
        "EXASERVE_CLEAN_STAGE": "1" if policy.clean_stage else "0",
        "EXASERVE_SCALING_TRACE": "1" if diagnostics else "0",
        "RAY_event_stats": "1" if diagnostics else "0",
        "RAY_event_stats_print_interval_ms": "1000",
        "RAYON_NUM_THREADS": "1",
        "TOKENIZERS_PARALLELISM": "false",
        "RAY_SERVE_PROXY_HEALTH_CHECK_TIMEOUT_S": str(
            plan.readiness.serve_proxy_health_check_timeout_s
        ),
        "RAY_SERVE_PROXY_READY_CHECK_TIMEOUT_S": str(
            plan.readiness.serve_proxy_ready_check_timeout_s
        ),
        "EXASERVE_RAY_SERVE_START_PROXY_TIMEOUT_S": str(plan.readiness.serve_start_proxy_timeout_s),
        "EXASERVE_ENGINE": plan.engine,
        "EXASERVE_VENDOR": plan.vendor,
        "EXASERVE_COMPAT_PROFILE_ID": plan.compatibility_profile_hash,
        "EXASERVE_COMPAT_MANIFEST_HASH": plan.manifest_hash,
        "EXASERVE_COMPAT_DELIVERY": "generated-overlay",
        PP_PATCH_GATE: "1" if pp_enabled else "0",
        RAY_WORKER_PATCH_GATE: "1" if pp_enabled else "0",
        MULTIPROC_WORKER_PATCH_GATE: "1" if multiproc_enabled else "0",
        "EXASERVE_XPU_VLLM_DISABLE_RAY_COMPILED_DAG": (
            "1" if pp_enabled and plan.vendor == "xpu" else "0"
        ),
        "EXASERVE_XPU_VLLM_FORCE_RAY_CHANNEL_TYPE": (
            "auto" if pp_enabled and plan.vendor == "xpu" else ""
        ),
    }
    runtime_root = os.environ.get(LOCAL_RUNTIME_ROOT_ENV, "")
    if runtime_root:
        values[ROOT_ENV] = overlay_root_for(
            plan.compatibility_profile_hash, runtime_root=runtime_root
        )
    return values


__all__ = [
    "LOCAL_GO_DISPATCH_ENV",
    "LOCAL_EVAL_MANIFEST_ENV",
    "LOCAL_RUN_PLAN_ENV",
    "LOCAL_PLAN_ENV",
    "LOCAL_RUNTIME_ROOT_ENV",
    "LOCAL_STATE_ROOT_ENV",
    "QUALIFIED_PYTHON_ENV",
    "QUALIFIED_PYTHON_HASH_ENV",
    "QUALIFIED_PYTHON_PROFILE_ENV",
    "COMPAT_SOURCE_PROFILE_ENV",
    "COMPAT_SOURCE_MANIFEST_ENV",
    "SHARED_ROOTS_ENV",
    "RuntimePathError",
    "RuntimePaths",
    "assert_worker_launch_is_local",
    "closed_runtime_environment",
    "default_local_state_root",
    "path_is_shared",
    "require_contained_local_path",
    "require_non_shared_path",
    "runtime_environment",
    "runtime_paths",
    "shared_roots",
    "shared_path_tokens",
    "staged_pythonpath",
]
