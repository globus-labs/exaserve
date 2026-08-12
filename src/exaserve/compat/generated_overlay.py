"""Immutable exact-hash compatibility overlay (ADR-003 ladder rung 3).

The pinned Aurora framework wheels are deployment inputs, not files ExaServe
may edit.  This module verifies those input bytes and materializes only the
Python modules named by ``CompatibilityProfile`` into the staged release tree.
A narrow meta-path finder then loads those complete, hash-verified modules in
place of the base copies.  No installed package is modified, no implicit
symlink farm is built, and importing this module has no mutation side effect.
"""

from __future__ import annotations

import hashlib
import importlib.abc
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import re
import sys
from typing import Iterable

from .profile import CompatibilityProfile, ProfileMismatch, default_profile

SCHEMA_VERSION = 2
ROOT_ENV = "EXASERVE_COMPAT_OVERLAY_ROOT"
_SHA256 = re.compile(r"[0-9a-f]{64}")

# Each generated target module invokes only the adapter functions declared for
# that exact manifest entry.  The call occurs after the target's own definitions
# exist, so the semantic sentinel is established during the module import, not
# by an unrelated interpreter-wide import sweep.
PATCH_CALLS = {
    "SC-01": ("exaserve._sitecustomize", "_patch_vllm_layer_lookup"),
    "SC-02": ("exaserve._sitecustomize", "_patch_vllm_bind_kv_cache"),
    "SC-03": ("exaserve._sitecustomize", "_patch_vllm_forward_context_aliases"),
    "SC-04": ("exaserve._sitecustomize", "_patch_vllm_attention_context"),
    "SC-05": ("exaserve._sitecustomize", "_patch_vllm_gpu_model_runner_attn_backend"),
    "SC-09": ("exaserve._sitecustomize", "_patch_vllm_ray_executor_channel_type"),
    "SC-10": ("exaserve._sitecustomize", "_patch_vllm_ray_executor_uncompiled_pp"),
    "SC-11": ("exaserve._sitecustomize", "_patch_ray_oneapi_selector"),
    "SC-12": ("exaserve._sitecustomize", "_patch_ray_accelerator_context"),
    "EW-01": ("exaserve._sitecustomize", "_patch_vllm_ray_worker_runtime_env"),
    "EW-02": ("exaserve._sitecustomize", "_patch_vllm_multiproc_worker_identity"),
    "EW-03": ("exaserve._sitecustomize", "_patch_vllm_ray_worker_identity"),
    "RS-01": ("exaserve._sitecustomize", "_patch_ray_serve_start_timeout"),
    "RS-02": ("exaserve.ray_start", "_patch_raylet_launch_from_environment"),
    "RS-03": ("exaserve._sitecustomize", "_patch_ray_serve_proxy_future_timeout"),
}


class GeneratedOverlayError(RuntimeError):
    """The selected overlay cannot be materialized or verified."""


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read_regular_bytes(path: Path) -> bytes:
    from ..state.atomic import regular_file_reader

    with regular_file_reader(path, binary=True) as handle:
        return handle.read()


def _module_name(relative: str) -> str:
    path = PurePosixPath(relative)
    if path.suffix != ".py" or path.is_absolute() or ".." in path.parts:
        raise GeneratedOverlayError(f"overlay target path is not a safe Python module: {relative}")
    parts = list(path.with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    if not parts:
        raise GeneratedOverlayError(f"overlay target has no module name: {relative}")
    return ".".join(parts)


def _selected(profile: CompatibilityProfile):
    return tuple(patch for patch in profile.patches if patch.delivery == "generated-overlay")


def _suffix(profile_id: str, patches: Iterable) -> bytes:
    lines = [
        "",
        f"# ExaServe generated compatibility overlay: {profile_id}",
        "import os as _exaserve_overlay_os",
    ]
    for index, patch in enumerate(patches):
        patch_id = patch.patch_id
        try:
            module, function = PATCH_CALLS[patch_id]
        except KeyError as exc:
            raise GeneratedOverlayError(
                f"generated-overlay patch {patch_id!r} has no declared source activation"
            ) from exc
        alias = f"_exaserve_overlay_adapter_{index}"
        roles = set(patch.roles)
        if roles & {"engine_core", "engine_worker"}:
            roles.add("engine_bootstrap")
        lines.extend(
            (
                f"if _exaserve_overlay_os.environ.get('EXASERVE_COMPAT_ROLE') in "
                f"{tuple(sorted(roles))!r}:",
                f"    from {module} import {function} as {alias}",
                f"    {alias}()",
                f"    del {alias}",
            )
        )
    lines.append("del _exaserve_overlay_os")
    lines.append("")
    return "\n".join(lines).encode("utf-8")


def materialize(profile: CompatibilityProfile, root: str | os.PathLike) -> dict:
    """Create one complete deterministic overlay under a fresh staging root."""
    root_path = Path(root).resolve()
    if root_path.exists():
        raise GeneratedOverlayError(f"overlay destination already exists: {root_path}")
    root_path.mkdir(parents=True, mode=0o755)

    grouped = {}
    for patch in _selected(profile):
        key = (
            patch.target_distribution,
            patch.target_version,
            patch.target_file,
            patch.target_source_hash,
        )
        grouped.setdefault(key, []).append(patch)
    entries = []
    try:
        from importlib import metadata

        for (distribution_name, version, relative, base_hash), patches in sorted(grouped.items()):
            distribution = metadata.distribution(distribution_name)
            if distribution.version != version:
                raise GeneratedOverlayError(
                    f"{distribution_name} version {distribution.version!r} != {version!r}"
                )
            source = Path(distribution.locate_file(relative)).resolve()
            distribution_root = Path(distribution.locate_file("")).resolve()
            try:
                source.relative_to(distribution_root)
            except ValueError as exc:
                raise GeneratedOverlayError(f"overlay source escapes {distribution_name}") from exc
            data = _read_regular_bytes(source)
            if _sha256(data) != base_hash:
                raise GeneratedOverlayError(
                    f"overlay base source hash mismatch for {distribution_name}:{relative}"
                )
            normalized_patches = tuple(sorted(patches, key=lambda item: item.patch_id))
            normalized_ids = tuple(patch.patch_id for patch in normalized_patches)
            output = data + _suffix(profile.profile_id, normalized_patches)
            module = _module_name(relative)
            destination = root_path / "modules" / PurePosixPath(relative)
            destination.parent.mkdir(parents=True, exist_ok=True)
            from ..state.atomic import atomic_create_bytes

            atomic_create_bytes(destination, output)
            entries.append(
                {
                    "module": module,
                    "relative_path": f"modules/{relative}",
                    "distribution": distribution_name,
                    "version": version,
                    "base_source_hash": base_hash,
                    "output_source_hash": _sha256(output),
                    "patch_ids": list(normalized_ids),
                }
            )
        from .engine_shim import shim_source

        bootstrap_source = shim_source()
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "profile_id": profile.profile_id,
            "entries": entries,
            "bootstrap_source_hash": _sha256(bootstrap_source.encode("utf-8")),
        }
        canonical = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        manifest["manifest_hash"] = _sha256(canonical)
        # One immutable bootstrap serves ordinary managed interpreters and the
        # spawned EngineCore/Ray-worker EN-01 path.  In particular, never
        # create an additional shim in one node's /tmp: Ray PP actors execute
        # on other nodes where that path has different contents.
        from ..state.atomic import atomic_create_json, atomic_create_text

        atomic_create_text(root_path / "sitecustomize.py", bootstrap_source)
        # The manifest is the completion marker and is therefore published last.
        atomic_create_json(root_path / "manifest.json", manifest)
        return manifest
    except BaseException:
        # This destination is a fresh transaction-owned directory. A partial
        # overlay must never be mistaken for a publishable compatibility root.
        import shutil

        shutil.rmtree(root_path, ignore_errors=True)
        raise


def load_manifest(profile: CompatibilityProfile, root: str | os.PathLike) -> dict:
    root_path = Path(root).resolve()
    try:
        from ..state.atomic import strict_json_load_path

        manifest = strict_json_load_path(root_path / "manifest.json")
    except (OSError, ValueError) as exc:
        raise GeneratedOverlayError(f"overlay manifest is unreadable: {exc}") from exc
    if not isinstance(manifest, dict) or set(manifest) != {
        "schema_version",
        "profile_id",
        "entries",
        "bootstrap_source_hash",
        "manifest_hash",
    }:
        raise GeneratedOverlayError("overlay manifest fields are invalid")
    if (
        type(manifest["schema_version"]) is not int
        or manifest["schema_version"] != SCHEMA_VERSION
        or manifest["profile_id"] != profile.profile_id
        or not isinstance(manifest["bootstrap_source_hash"], str)
        or not _SHA256.fullmatch(manifest["bootstrap_source_hash"])
        or not isinstance(manifest["manifest_hash"], str)
        or not _SHA256.fullmatch(manifest["manifest_hash"])
    ):
        raise GeneratedOverlayError("overlay manifest identity mismatch")
    canonical = json.dumps(
        {key: value for key, value in manifest.items() if key != "manifest_hash"},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    if manifest["manifest_hash"] != _sha256(canonical):
        raise GeneratedOverlayError("overlay manifest hash mismatch")
    try:
        bootstrap_source = _read_regular_bytes(root_path / "sitecustomize.py")
    except (OSError, ValueError) as exc:
        raise GeneratedOverlayError(f"overlay bootstrap is unreadable: {exc}") from exc
    if _sha256(bootstrap_source) != manifest["bootstrap_source_hash"]:
        raise GeneratedOverlayError("overlay bootstrap source hash mismatch")
    entries = manifest["entries"]
    if not isinstance(entries, list) or not entries:
        raise GeneratedOverlayError("overlay manifest entries are invalid")
    expected_groups: dict[str, dict] = {}
    for patch in _selected(profile):
        module = _module_name(patch.target_file)
        expected = expected_groups.setdefault(
            module,
            {
                "module": module,
                "relative_path": f"modules/{patch.target_file}",
                "distribution": patch.target_distribution,
                "version": patch.target_version,
                "base_source_hash": patch.target_source_hash,
                "patch_ids": [],
            },
        )
        for name, value in (
            ("relative_path", f"modules/{patch.target_file}"),
            ("distribution", patch.target_distribution),
            ("version", patch.target_version),
            ("base_source_hash", patch.target_source_hash),
        ):
            if expected[name] != value:
                raise GeneratedOverlayError(
                    f"profile maps module {module!r} to conflicting {name} values"
                )
        expected["patch_ids"].append(patch.patch_id)
    for expected in expected_groups.values():
        expected["patch_ids"] = sorted(expected["patch_ids"])

    expected_ids = {patch.patch_id for patch in _selected(profile)}
    observed_ids = set()
    modules = set()
    for entry in entries:
        fields = {
            "module",
            "relative_path",
            "distribution",
            "version",
            "base_source_hash",
            "output_source_hash",
            "patch_ids",
        }
        if not isinstance(entry, dict) or set(entry) != fields:
            raise GeneratedOverlayError("overlay entry fields are invalid")
        if (
            not isinstance(entry["module"], str)
            or not entry["module"]
            or entry["module"] in modules
            or not isinstance(entry["relative_path"], str)
            or not isinstance(entry["distribution"], str)
            or not entry["distribution"]
            or not isinstance(entry["version"], str)
            or not entry["version"]
            or not isinstance(entry["patch_ids"], list)
            or not entry["patch_ids"]
            or any(not isinstance(item, str) or not item for item in entry["patch_ids"])
            or entry["patch_ids"] != sorted(set(entry["patch_ids"]))
            or any(
                not isinstance(entry[name], str) or not _SHA256.fullmatch(entry[name])
                for name in ("base_source_hash", "output_source_hash")
            )
        ):
            raise GeneratedOverlayError("overlay entry values are invalid")
        expected = expected_groups.get(entry["module"])
        if expected is None or any(entry[name] != expected[name] for name in expected):
            raise GeneratedOverlayError(
                f"overlay entry does not match profile target mapping: {entry['module']!r}"
            )
        relative = PurePosixPath(entry["relative_path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise GeneratedOverlayError("overlay entry path is unsafe")
        path = (root_path / relative).resolve()
        try:
            path.relative_to(root_path)
        except ValueError as exc:
            raise GeneratedOverlayError("overlay entry escapes its root") from exc
        try:
            data = _read_regular_bytes(path)
        except (OSError, ValueError) as exc:
            raise GeneratedOverlayError(f"overlay entry is unreadable: {exc}") from exc
        if _sha256(data) != entry["output_source_hash"]:
            raise GeneratedOverlayError(f"overlay output hash mismatch for {entry['module']}")
        modules.add(entry["module"])
        observed_ids.update(entry["patch_ids"])
    if observed_ids != expected_ids:
        raise GeneratedOverlayError(
            f"overlay patch set mismatch: expected={sorted(expected_ids)}, "
            f"observed={sorted(observed_ids)}"
        )
    if modules != set(expected_groups):
        raise GeneratedOverlayError(
            f"overlay module set mismatch: expected={sorted(expected_groups)}, "
            f"observed={sorted(modules)}"
        )
    return manifest


def _required_patch_ids(profile: CompatibilityProfile, role: str) -> set[str]:
    if role == "engine_bootstrap":
        required = set(profile.required_patch_ids("engine_core"))
        required.update(profile.required_patch_ids("engine_worker"))
        return required
    return set(profile.required_patch_ids(role))


def _verified_base_sources(entries: list[dict]) -> dict[str, Path]:
    """Resolve and reverify every immutable distribution source at install."""
    from importlib import metadata

    sources: dict[str, Path] = {}
    for entry in entries:
        relative = PurePosixPath(entry["relative_path"])
        if len(relative.parts) < 2 or relative.parts[0] != "modules":
            raise GeneratedOverlayError(
                f"overlay entry has no canonical module source path: {entry['relative_path']!r}"
            )
        target_relative = PurePosixPath(*relative.parts[1:])
        try:
            distribution = metadata.distribution(entry["distribution"])
        except metadata.PackageNotFoundError as exc:
            raise GeneratedOverlayError(
                f"overlay base distribution is unavailable: {entry['distribution']}"
            ) from exc
        if distribution.version != entry["version"]:
            raise GeneratedOverlayError(
                f"{entry['distribution']} version {distribution.version!r} != {entry['version']!r}"
            )
        source = Path(distribution.locate_file(str(target_relative))).resolve()
        distribution_root = Path(distribution.locate_file("")).resolve()
        try:
            source.relative_to(distribution_root)
        except ValueError as exc:
            raise GeneratedOverlayError(
                f"overlay base source escapes {entry['distribution']}"
            ) from exc
        try:
            base_data = _read_regular_bytes(source)
        except (OSError, ValueError) as exc:
            raise GeneratedOverlayError(f"overlay base source is unreadable: {source}") from exc
        if _sha256(base_data) != entry["base_source_hash"]:
            raise GeneratedOverlayError(
                f"overlay base source hash mismatch for {entry['distribution']}:{target_relative}"
            )
        sources[entry["module"]] = source
    return sources


class _OverlayLoader(importlib.abc.Loader):
    """Execute generated bytes without changing dependency resource identity."""

    def __init__(
        self,
        *,
        source: Path,
        base_source: Path,
        profile_id: str,
        output_hash: str,
    ) -> None:
        self.source = source
        self.base_source = base_source
        self.profile_id = profile_id
        self.output_hash = output_hash

    def create_module(self, spec):
        del spec
        return None

    def exec_module(self, module) -> None:
        try:
            data = _read_regular_bytes(self.source)
        except (OSError, ValueError) as exc:
            raise GeneratedOverlayError(
                f"generated overlay source became unreadable: {self.source}"
            ) from exc
        if _sha256(data) != self.output_hash:
            raise GeneratedOverlayError(
                f"generated overlay source changed before import: {self.source}"
            )
        # Ray and vLLM modules may locate native binaries, templates, or other
        # package data relative to __file__. Preserve the verified installed
        # distribution identity while compiling the generated bytes. Separate
        # markers retain exact overlay provenance for receipts and re-entry.
        module.__file__ = str(self.base_source)
        module.__cached__ = None
        module.__exaserve_overlay_source__ = str(self.source)
        module.__exaserve_overlay_profile_id__ = self.profile_id
        module.__exaserve_overlay_output_hash__ = self.output_hash
        code = compile(data, str(self.base_source), "exec", dont_inherit=True)
        # This is the import loader boundary, not evaluation of operator input:
        # both the base and generated source bytes were SHA-256 verified above.
        exec(code, module.__dict__)  # noqa: S102
        module.__file__ = str(self.base_source)
        module.__cached__ = None
        module.__exaserve_overlay_source__ = str(self.source)
        module.__exaserve_overlay_profile_id__ = self.profile_id
        module.__exaserve_overlay_output_hash__ = self.output_hash


class _OverlayFinder(importlib.abc.MetaPathFinder):
    def __init__(
        self,
        profile: CompatibilityProfile,
        root: Path,
        entries: list[dict],
        base_sources: dict[str, Path],
    ) -> None:
        self._exaserve_overlay_profile_id = profile.profile_id
        self.profile = profile
        self.root = root
        self.entries = {entry["module"]: entry for entry in entries}
        self.base_sources = base_sources

    def find_spec(self, fullname, path=None, target=None):
        del path, target
        entry = self.entries.get(fullname)
        if entry is None:
            return None
        # A Ray worker interpreter can be spawned with its parent ray_head or
        # ray_worker environment before Ray applies an actor's runtime_env.
        # Resolve the role at the actual target import boundary rather than
        # freezing it when sitecustomize installed this finder.  A later
        # install() call rejects any affected target that was loaded from the
        # base distribution before the role was rebound.
        role = os.environ.get("EXASERVE_COMPAT_ROLE", "")
        required = _required_patch_ids(self.profile, role)
        if not required.intersection(entry["patch_ids"]):
            return None
        source = self.root / entry["relative_path"]
        base_source = self.base_sources[fullname]
        loader = _OverlayLoader(
            source=source,
            base_source=base_source,
            profile_id=self.profile.profile_id,
            output_hash=entry["output_source_hash"],
        )
        package_locations = [str(base_source.parent)] if base_source.name == "__init__.py" else None
        return importlib.util.spec_from_file_location(
            fullname,
            base_source,
            loader=loader,
            submodule_search_locations=package_locations,
        )


def install(
    profile: CompatibilityProfile, root: str | os.PathLike, *, role: str | None = None
) -> dict:
    """Verify and install one overlay finder; never replace a loaded target."""
    root_path = Path(root).resolve()
    manifest = load_manifest(profile, root_path)
    base_sources = _verified_base_sources(manifest["entries"])
    role = role or os.environ.get("EXASERVE_COMPAT_ROLE", "")
    if not role:
        raise GeneratedOverlayError("EXASERVE_COMPAT_ROLE is required to install the overlay")
    relevant_ids = _required_patch_ids(profile, role)
    entries = [entry for entry in manifest["entries"] if relevant_ids & set(entry["patch_ids"])]
    existing = [
        finder
        for finder in sys.meta_path
        if getattr(finder, "_exaserve_overlay_profile_id", None) is not None
    ]
    if existing:
        if (
            len(existing) != 1
            or existing[0]._exaserve_overlay_profile_id != profile.profile_id
            or existing[0].root != root_path
        ):
            raise GeneratedOverlayError("a different compatibility overlay is already installed")
        wrong_source = []
        expected_paths = {
            entry["module"]: (root_path / entry["relative_path"]).resolve() for entry in entries
        }
        expected_hashes = {entry["module"]: entry["output_source_hash"] for entry in entries}
        for module_name, expected_path in expected_paths.items():
            module = sys.modules.get(module_name)
            if module is None:
                continue
            module_path = getattr(module, "__exaserve_overlay_source__", None)
            try:
                observed_path = Path(module_path).resolve() if module_path else None
            except (OSError, RuntimeError):
                observed_path = None
            if (
                observed_path != expected_path
                or getattr(module, "__exaserve_overlay_profile_id__", None) != profile.profile_id
                or getattr(module, "__exaserve_overlay_output_hash__", None)
                != expected_hashes[module_name]
            ):
                wrong_source.append(module_name)
        if wrong_source:
            raise GeneratedOverlayError(
                "compatibility role was bound after target import from the base "
                f"distribution: {sorted(wrong_source)}"
            )
        return manifest
    target_modules = {entry["module"] for entry in entries}
    already_loaded = sorted(target_modules & set(sys.modules))
    if already_loaded:
        raise GeneratedOverlayError(
            f"compatibility overlay was installed after target import: {already_loaded}"
        )
    # Install the complete manifest. The finder filters against the live role
    # at each import so Ray may safely bind an actor runtime_env after spawning
    # a generic worker interpreter, provided no affected target was imported.
    sys.meta_path.insert(
        0,
        _OverlayFinder(profile, root_path, manifest["entries"], base_sources),
    )
    return manifest


def install_from_environment() -> dict | None:
    """Site bootstrap: verify the exact profile/base bytes, then install."""
    root = os.environ.get(ROOT_ENV, "")
    expected = os.environ.get("EXASERVE_COMPAT_PROFILE_ID", "")
    if not root and not expected:
        return None
    if not root or not expected:
        raise GeneratedOverlayError("compatibility overlay environment is incomplete")
    profile = default_profile(os.environ.get("EXASERVE_VENDOR", "xpu"))
    if profile.profile_id != expected:
        raise GeneratedOverlayError("compatibility overlay profile identity mismatch")
    try:
        profile.verify_environment(
            {
                "python": __import__("platform").python_version(),
                "ray": __import__("importlib.metadata", fromlist=["version"]).version("ray"),
                "vllm": __import__("importlib.metadata", fromlist=["version"]).version("vllm"),
            }
        )
        profile.verify_installed_sources()
    except ProfileMismatch as exc:
        raise GeneratedOverlayError(str(exc)) from exc
    return install(profile, root, role=os.environ.get("EXASERVE_COMPAT_ROLE", ""))


def activate_patch_ids(profile: CompatibilityProfile, patch_ids: Iterable[str]) -> None:
    """Load each selected overlay target and thereby prove its sentinel."""
    import importlib

    required = set(patch_ids)
    generated_ids = {
        patch.patch_id for patch in profile.patches if patch.delivery == "generated-overlay"
    }
    selected = required & generated_ids
    if not selected:
        return
    root = os.environ.get(ROOT_ENV, "")
    if not root:
        raise GeneratedOverlayError(f"{ROOT_ENV} is required for {sorted(selected)}")
    manifest = install(profile, root, role=os.environ.get("EXASERVE_COMPAT_ROLE", ""))
    modules = {
        entry["module"] for entry in manifest["entries"] if selected & set(entry["patch_ids"])
    }
    for module in sorted(modules):
        importlib.import_module(module)


def overlay_root_for(profile_id: str) -> str:
    if not isinstance(profile_id, str) or not _SHA256.fullmatch(profile_id):
        raise GeneratedOverlayError("profile_id must be SHA-256")
    return f"/tmp/exaserve_src/exaserve/_compat_runtime/{profile_id}"


__all__ = [
    "GeneratedOverlayError",
    "PATCH_CALLS",
    "ROOT_ENV",
    "activate_patch_ids",
    "install",
    "install_from_environment",
    "load_manifest",
    "materialize",
    "overlay_root_for",
]
