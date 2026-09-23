#!/usr/bin/env python3
"""Build and audit release artifacts without reusing checkout build products.

Setuptools' in-tree ``build/lib`` and ignored ``egg-info/SOURCES.txt`` caches
can retain files that were deleted or never intended for release. This tool
copies only declared release inputs into a fresh tree, audits the resulting
sdist, extracts it, builds the wheel there, and requires the wheel's
``exaserve/`` members to equal the current declared source/package-data surface
exactly.
"""

from __future__ import annotations

import argparse
from email.parser import BytesParser
from email.policy import default as email_policy
import hashlib
from importlib import metadata
import json
import os
from pathlib import Path
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import zipfile


_LOCAL_BUILD_TOOL_VERSIONS = {
    "build": "1.4.0",
    "setuptools": "78.1.1",
    "wheel": "0.46.3",
}
_BUILD_BACKEND_ENTRY_POINT_GROUPS = (
    "distutils.commands",
    "distutils.setup_keywords",
    "setuptools.finalize_distribution_options",
)
_RELEASE_ROOT_FILES = (
    "MANIFEST.in",
    "README.md",
    "pyproject.toml",
    "LICENSE",
    "NOTICE",
    "uv.lock",
    "CHANGELOG.md",
    "CONTRIBUTING.md",
    "CODE_OF_CONDUCT.md",
    "SECURITY.md",
)
_GENERATED_SDIST_FILES = {
    "PKG-INFO",
    "setup.cfg",
    "src/exaserve.egg-info/PKG-INFO",
    "src/exaserve.egg-info/SOURCES.txt",
    "src/exaserve.egg-info/dependency_links.txt",
    "src/exaserve.egg-info/entry_points.txt",
    "src/exaserve.egg-info/requires.txt",
    "src/exaserve.egg-info/top_level.txt",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _expected_package_members(repo_root: Path) -> dict[str, Path]:
    source = repo_root / "src" / "exaserve"
    expected: dict[str, Path] = {}
    for path in source.rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(source)
        if path.suffix == ".py" or relative.as_posix() == "py.typed":
            expected[f"exaserve/{relative.as_posix()}"] = path
        elif relative.parent.as_posix() == "resources" and (
            path.suffix == ".c" or path.name.endswith(".Makefile")
        ):
            expected[f"exaserve/{relative.as_posix()}"] = path
        elif (
            relative.parts[:2] == ("resources", "vllm_modelinfo")
            and len(relative.parts) == 3
            and path.suffix == ".json"
        ):
            expected[f"exaserve/{relative.as_posix()}"] = path
    return expected


def _expected_sdist_inputs(repo_root: Path) -> dict[str, Path]:
    expected = {name: repo_root / name for name in _RELEASE_ROOT_FILES}
    for name in ("README.md", "aurora-frameworks-2025.3.1.lock", "ci.lock"):
        expected[f"requirements/{name}"] = repo_root / "requirements" / name
    for wheel_name, source in _expected_package_members(repo_root).items():
        expected[f"src/{wheel_name}"] = source
    missing = sorted(name for name, path in expected.items() if not path.is_file())
    if missing:
        raise RuntimeError(f"release input is missing declared file(s): {missing}")
    return expected


def _copy_release_input(repo_root: Path, destination: Path) -> dict[str, Path]:
    """Stage only the declared sdist inputs, never checkout caches/manifests."""
    if destination.exists():
        raise RuntimeError(f"release-input destination already exists: {destination}")
    destination.mkdir(parents=True, mode=0o755)
    expected = _expected_sdist_inputs(repo_root)
    for relative, source in sorted(expected.items()):
        source_metadata = source.lstat()
        if not stat.S_ISREG(source_metadata.st_mode):
            raise RuntimeError(f"release input must be a regular file: {source}")
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
        shutil.copy2(source, target, follow_symlinks=False)
    return expected


def _audit_sdist(sdist: Path, repo_root: Path) -> list[str]:
    expected = _expected_sdist_inputs(repo_root)
    with tarfile.open(sdist, "r:gz") as archive:
        file_members = [member for member in archive.getmembers() if member.isfile()]
        roots = {Path(member.name).parts[0] for member in file_members if Path(member.name).parts}
        if len(roots) != 1:
            raise RuntimeError(f"sdist must contain one root, observed {sorted(roots)}")
        root = next(iter(roots))
        observed = {}
        for member in file_members:
            path = Path(member.name)
            if len(path.parts) < 2 or path.parts[0] != root:
                raise RuntimeError(f"sdist member is outside its release root: {member.name!r}")
            relative = Path(*path.parts[1:]).as_posix()
            if relative in observed:
                raise RuntimeError(f"sdist contains a duplicate member: {relative!r}")
            observed[relative] = member
        allowed = set(expected) | _GENERATED_SDIST_FILES
        missing = sorted(set(expected) - set(observed))
        unexpected = sorted(set(observed) - allowed)
        changed = []
        for relative, source in expected.items():
            member = observed.get(relative)
            if member is None:
                continue
            handle = archive.extractfile(member)
            if handle is None or hashlib.sha256(handle.read()).hexdigest() != _sha256(source):
                changed.append(relative)
        sources_member = observed.get("src/exaserve.egg-info/SOURCES.txt")
        stale_sources = []
        if sources_member is not None:
            handle = archive.extractfile(sources_member)
            if handle is None:
                stale_sources = ["<unreadable>"]
            else:
                declared = {
                    line.strip()
                    for line in handle.read().decode("utf-8").splitlines()
                    if line.strip()
                }
                stale_sources = sorted(declared - (set(expected) | _GENERATED_SDIST_FILES))
    if missing or unexpected or changed or stale_sources:
        detail = []
        if missing:
            detail.append(f"missing sdist inputs: {missing}")
        if unexpected:
            detail.append(f"unexpected/stale sdist members: {unexpected}")
        if changed:
            detail.append(f"sdist members differ from source bytes: {changed}")
        if stale_sources:
            detail.append(f"sdist SOURCES.txt contains stale members: {stale_sources}")
        raise RuntimeError("; ".join(detail))
    return sorted(observed)


def _audit_wheel(wheel: Path, repo_root: Path) -> list[str]:
    expected = _expected_package_members(repo_root)
    with zipfile.ZipFile(wheel) as archive:
        metadata_members = [
            name
            for name in archive.namelist()
            if name.endswith(".dist-info/METADATA") and len(Path(name).parts) == 2
        ]
        if len(metadata_members) != 1:
            raise RuntimeError("wheel must contain exactly one distribution metadata file")
        metadata_name = metadata_members[0]
        metadata = BytesParser(policy=email_policy).parsebytes(archive.read(metadata_name))
        if metadata.get("Name") != "exaserve" or metadata.get("License-Expression") != "Apache-2.0":
            raise RuntimeError("wheel lacks the declared ExaServe Apache-2.0 license metadata")
        if set(metadata.get_all("License-File", [])) != {"LICENSE", "NOTICE"}:
            raise RuntimeError("wheel metadata must declare LICENSE and NOTICE")
        distribution_root = metadata_name.rsplit("/", 1)[0]
        for license_name in ("LICENSE", "NOTICE"):
            member = f"{distribution_root}/licenses/{license_name}"
            if archive.namelist().count(member) != 1:
                raise RuntimeError(f"wheel must contain one exact license artifact: {license_name}")
            if archive.read(member) != (repo_root / license_name).read_bytes():
                raise RuntimeError(f"wheel license artifact differs from source: {license_name}")
        names = {
            name
            for name in archive.namelist()
            if name.startswith("exaserve/") and not name.endswith("/")
        }
        changed = sorted(
            name
            for name, source_path in expected.items()
            if name in names
            and hashlib.sha256(archive.read(name)).hexdigest() != _sha256(source_path)
        )
    missing = sorted(set(expected) - names)
    unexpected = sorted(names - set(expected))
    if missing or unexpected or changed:
        detail = []
        if missing:
            detail.append(f"missing package members: {missing}")
        if unexpected:
            detail.append(f"unexpected/stale package members: {unexpected}")
        if changed:
            detail.append(f"package members differ from source bytes: {changed}")
        raise RuntimeError("; ".join(detail))
    return sorted(names)


def _atomic_copy(source: Path, destination: Path) -> None:
    """Publish one immutable artifact only after its bytes reach storage."""
    fd, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=destination.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as output_handle:
            with source.open("rb") as input_handle:
                shutil.copyfileobj(input_handle, output_handle)
            output_handle.flush()
            os.fsync(output_handle.fileno())
        os.chmod(temporary, source.stat().st_mode & 0o777)
        os.link(temporary, destination, follow_symlinks=False)
        temporary.unlink()
        directory_fd = os.open(destination.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def _extract_sdist(archive_path: Path, destination: Path) -> None:
    """Extract only regular sdist members with Python-version-stable rules."""
    root = destination.resolve()
    with tarfile.open(archive_path, "r:gz") as archive:
        members = archive.getmembers()
        for member in members:
            relative = Path(member.name)
            if (
                not member.name
                or relative.is_absolute()
                or ".." in relative.parts
                or not (member.isdir() or member.isfile())
            ):
                raise RuntimeError(f"sdist contains an unsafe entry: {member.name!r}")
            target = (root / relative).resolve()
            if os.path.commonpath((str(root), str(target))) != str(root):
                raise RuntimeError(f"sdist entry escapes extraction root: {member.name!r}")
        for member in members:
            target = root / member.name
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True, mode=0o755)
                continue
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
            source = archive.extractfile(member)
            if source is None:
                raise RuntimeError(f"sdist file has no payload: {member.name!r}")
            flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            with source, os.fdopen(os.open(target, flags, 0o600), "wb") as output:
                shutil.copyfileobj(source, output)
                output.flush()
                os.fsync(output.fileno())
            os.chmod(target, 0o755 if member.mode & 0o111 else 0o644)


def _run(argv: list[str], *, cwd: Path, timeout_s: float = 900.0) -> None:
    """Run one owned build process group under a finite deadline."""
    process = subprocess.Popen(argv, cwd=cwd, start_new_session=True)
    try:
        returncode = process.wait(timeout=timeout_s)
    except subprocess.TimeoutExpired as exc:
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
        raise TimeoutError(f"release build command exceeded {timeout_s:g}s: {argv}") from exc
    if returncode != 0:
        raise subprocess.CalledProcessError(returncode, argv)


def _verified_local_build_environment(repo_root: Path) -> dict[str, object]:
    """Fail unless the current interpreter has the exact pinned build tools."""
    try:
        import tomllib
    except ImportError:  # pragma: no cover - builder normally runs on Python 3.12
        import tomli as tomllib

    pyproject_path = repo_root / "pyproject.toml"
    with pyproject_path.open("rb") as handle:
        pyproject = tomllib.load(handle)
    build_system = pyproject.get("build-system")
    expected_requirements = {
        "setuptools==78.1.1",
        "wheel==0.46.3",
    }
    if (
        not isinstance(build_system, dict)
        or set(build_system.get("requires", ())) != expected_requirements
        or build_system.get("build-backend") != "setuptools.build_meta"
    ):
        raise RuntimeError("pyproject build-system is not the audited exact tool set")
    observed = {}
    for distribution, expected in sorted(_LOCAL_BUILD_TOOL_VERSIONS.items()):
        try:
            version = metadata.version(distribution)
        except metadata.PackageNotFoundError as exc:
            raise RuntimeError(f"verified local build tool is absent: {distribution}") from exc
        if version != expected:
            raise RuntimeError(
                f"verified local build tool {distribution} {version!r} != {expected!r}"
            )
        observed[distribution] = version
    plugins = []
    allowed_plugin_distributions = {"setuptools", "wheel"}
    for group in _BUILD_BACKEND_ENTRY_POINT_GROUPS:
        for entry_point in metadata.entry_points(group=group):
            distribution = getattr(entry_point, "dist", None)
            distribution_name = (
                distribution.name.lower().replace("_", "-")
                if distribution is not None and isinstance(distribution.name, str)
                else ""
            )
            if distribution_name not in allowed_plugin_distributions:
                raise RuntimeError(
                    "verified local build environment has an undeclared backend plugin: "
                    f"group={group!r}, name={entry_point.name!r}, "
                    f"distribution={distribution_name!r}"
                )
            plugins.append(
                {
                    "group": group,
                    "name": entry_point.name,
                    "value": entry_point.value,
                    "distribution": distribution_name,
                    "version": distribution.version,
                }
            )
    executable = Path(sys.executable).resolve()
    return {
        "mode": "verified_local_tools",
        "python_version": sys.version.split()[0],
        "python_executable": str(executable),
        "python_executable_sha256": _sha256(executable),
        "tools": observed,
        "build_backend_entry_points": sorted(
            plugins,
            key=lambda item: (item["group"], item["name"], item["distribution"]),
        ),
        "pyproject_sha256": _sha256(pyproject_path),
    }


def build(
    repo_root: Path,
    output_dir: Path,
    *,
    verified_local_tools: bool = False,
) -> dict[str, object]:
    repo_root = repo_root.resolve()
    output_dir = output_dir.resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        raise RuntimeError(f"output directory must be new or empty: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    if verified_local_tools:
        build_environment = _verified_local_build_environment(repo_root)
        isolation_args = ["--no-isolation"]
        build_method = "sdist_then_wheel_with_verified_local_tools"
    else:
        build_environment = {
            "mode": "isolated",
            "build_system_requires": sorted(
                requirement.split("==", 1)[0] + "==" + requirement.split("==", 1)[1]
                for requirement in ("setuptools==78.1.1", "wheel==0.46.3")
            ),
        }
        isolation_args = []
        build_method = "sdist_then_isolated_wheel"

    with tempfile.TemporaryDirectory(prefix="exaserve-release-") as temporary:
        scratch = Path(temporary)
        build_input = scratch / "input"
        _copy_release_input(repo_root, build_input)
        sdist_output = scratch / "sdist"
        wheel_output = scratch / "wheel"
        sdist_output.mkdir()
        wheel_output.mkdir()
        _run(
            [
                sys.executable,
                "-m",
                "build",
                *isolation_args,
                "--sdist",
                "--outdir",
                str(sdist_output),
                str(build_input),
            ],
            cwd=build_input,
        )
        sdists = sorted(sdist_output.glob("*.tar.gz"))
        if len(sdists) != 1:
            raise RuntimeError(f"expected one sdist, found {sdists}")
        sdist_members = _audit_sdist(sdists[0], repo_root)
        extracted = scratch / "source"
        extracted.mkdir()
        _extract_sdist(sdists[0], extracted)
        roots = [path for path in extracted.iterdir() if path.is_dir()]
        if len(roots) != 1:
            raise RuntimeError(f"sdist must contain exactly one source root, found {roots}")
        _run(
            [
                sys.executable,
                "-m",
                "build",
                *isolation_args,
                "--wheel",
                "--outdir",
                str(wheel_output),
                str(roots[0]),
            ],
            cwd=roots[0],
        )
        wheels = sorted(wheel_output.glob("*.whl"))
        if len(wheels) != 1:
            raise RuntimeError(f"expected one wheel, found {wheels}")
        members = _audit_wheel(wheels[0], repo_root)
        final_sdist = output_dir / sdists[0].name
        final_wheel = output_dir / wheels[0].name
        _atomic_copy(sdists[0], final_sdist)
        _atomic_copy(wheels[0], final_wheel)

    manifest = {
        "schema_version": 1,
        "build_method": build_method,
        "build_environment": build_environment,
        "sdist": final_sdist.name,
        "sdist_sha256": _sha256(final_sdist),
        "sdist_members": sdist_members,
        "wheel": final_wheel.name,
        "wheel_sha256": _sha256(final_wheel),
        "package_members": members,
    }
    manifest_path = output_dir / "artifact_manifest.json"
    fd, temporary = tempfile.mkstemp(prefix=".artifact-manifest.", dir=output_dir)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(manifest, indent=2, sort_keys=True, allow_nan=False) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, manifest_path, follow_symlinks=False)
        os.unlink(temporary)
        directory_fd = os.open(output_dir, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[2]))
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--verified-local-tools",
        action="store_true",
        help=(
            "build without dependency downloads only after verifying the exact "
            "pinned local build-tool versions"
        ),
    )
    args = parser.parse_args()
    manifest = build(
        Path(args.repo_root),
        Path(args.output_dir),
        verified_local_tools=args.verified_local_tools,
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
