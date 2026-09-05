"""One-snapshot Linux mount-boundary checks for node-local trees.

Aurora's overlay filesystem can report different ``st_dev`` values for a
directory and a regular file visible through the same mount.  Consequently,
``entry.st_dev == root.st_dev`` is not a valid mount-containment proof.  The
kernel mount table is authoritative: parse it once per tree validation, record
the mount points strictly beneath the validated root, then reject those paths
in O(1) while walking the tree.
"""

from __future__ import annotations

import os
from pathlib import Path


class MountBoundaryError(ValueError):
    """The current mount namespace could not prove a tree boundary."""


def _unescape_mountinfo(value: str) -> str:
    # proc(5): space, tab, newline, and backslash are octal escaped.
    return (
        value.replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
    )


def _contained(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def nested_mount_points(
    root: str | os.PathLike,
    *,
    mountinfo_path: str | os.PathLike = "/proc/self/mountinfo",
) -> frozenset[Path]:
    """Return all kernel mount points strictly beneath an existing tree root.

    The result is a set so callers pay one mount-table scan and constant-time
    checks per walked entry. The root's own covering mount (including when the
    root is itself a mount point) is allowed; any mount introduced below it is
    a tree-boundary escape regardless of its filesystem type.
    """

    requested = Path(root)
    if not requested.is_absolute() or ".." in requested.parts:
        raise MountBoundaryError(f"mount boundary root is not absolute/normalized: {root}")
    try:
        resolved = Path(os.path.realpath(requested))
        lines = Path(mountinfo_path).read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise MountBoundaryError(f"could not read mount identity for {requested}: {exc}") from exc

    mount_points: list[Path] = []
    for line in lines:
        fields = line.split()
        if len(fields) < 6 or "-" not in fields:
            continue
        raw_mount = _unescape_mountinfo(fields[4])
        mount = Path(raw_mount)
        if mount.is_absolute():
            mount_points.append(mount)
    if not any(resolved == mount or _contained(resolved, mount) for mount in mount_points):
        raise MountBoundaryError(f"no kernel mount covers tree root: {resolved}")
    return frozenset(
        mount for mount in mount_points if mount != resolved and _contained(mount, resolved)
    )


__all__ = ["MountBoundaryError", "nested_mount_points"]
