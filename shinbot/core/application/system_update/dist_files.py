"""Filesystem operations for deployed dashboard dist assets."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from pathlib import Path

from .common import DASHBOARD_DIST_MANIFEST, SystemUpdateError


def replace_dist(
    *,
    source_dist: Path,
    target_dist: Path,
    source_commit: str,
    package_sha256: str = "",
) -> None:
    """Atomically replace a deployed dashboard dist directory with a new one.

    Copies the source dist to a temporary directory, writes a manifest,
    validates the result, and swaps it into place. A backup of the existing
    target is kept during the swap and restored on failure.

    Args:
        source_dist: Path to the validated source dist directory.
        target_dist: Path where the deployed dist should live.
        source_commit: Git commit SHA that produced the source dist.
        package_sha256: Optional SHA-256 hex digest of the source zip package.

    Raises:
        SystemUpdateError: If validation fails or the swap operation encounters
            an unrecoverable error.
    """
    source_error = validate_dist(source_dist)
    if source_error:
        raise SystemUpdateError(
            code="UPDATE_NOT_ALLOWED",
            message=source_error,
            status_code=409,
        )

    target_parent = target_dist.parent
    target_parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = target_parent / f".{target_dist.name}.tmp-{os.getpid()}"
    backup_dir = target_parent / f".{target_dist.name}.backup-{os.getpid()}"

    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    if backup_dir.exists():
        shutil.rmtree(backup_dir)

    try:
        shutil.copytree(source_dist, tmp_dir)
        write_manifest(tmp_dir, source_commit, package_sha256=package_sha256)
        tmp_error = validate_dist(tmp_dir)
        if tmp_error:
            raise SystemUpdateError(
                code="UPDATE_NOT_ALLOWED",
                message=tmp_error,
                status_code=409,
            )

        if target_dist.exists():
            target_dist.rename(backup_dir)
        tmp_dir.rename(target_dist)
        if backup_dir.exists():
            shutil.rmtree(backup_dir)
    except Exception:
        if target_dist.exists() and backup_dir.exists():
            shutil.rmtree(target_dist)
        if backup_dir.exists():
            backup_dir.rename(target_dist)
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        raise


def validate_dist(dist_dir: Path) -> str:
    """Validate that a dist directory is suitable for deployment.

    Args:
        dist_dir: Path to the dist directory to validate.

    Returns:
        An empty string if the dist is valid, or a human-readable error
        message describing the first validation failure encountered.
    """
    if not dist_dir.is_dir():
        return "WebUI dist source directory does not exist"
    if not (dist_dir / "index.html").is_file():
        return "WebUI dist source must contain index.html"
    for path in dist_dir.rglob("*"):
        if path.is_symlink():
            return "WebUI dist source must not contain symlinks"
    return ""


def read_deployed_source_commit(target_dist: Path) -> str:
    """Read the source commit SHA from a deployed dist manifest.

    Args:
        target_dist: Path to the deployed dist directory.

    Returns:
        The source commit string from the manifest, or an empty string if
        the manifest is missing or does not contain the field.
    """
    payload = _read_manifest(target_dist)
    commit = payload.get("sourceCommit")
    return commit if isinstance(commit, str) else ""


def read_deployed_package_sha256(target_dist: Path) -> str:
    """Read the package SHA-256 hash from a deployed dist manifest.

    Args:
        target_dist: Path to the deployed dist directory.

    Returns:
        The package SHA-256 hex digest string from the manifest, or an empty
        string if the manifest is missing or does not contain the field.
    """
    payload = _read_manifest(target_dist)
    package_sha = payload.get("packageSha256")
    return package_sha if isinstance(package_sha, str) else ""


def write_manifest(dist_dir: Path, source_commit: str, *, package_sha256: str = "") -> None:
    """Write a deployment manifest into a dist directory.

    Args:
        dist_dir: Path to the dist directory where the manifest will be
            written.
        source_commit: Git commit SHA that produced the dist.
        package_sha256: Optional SHA-256 hex digest of the source zip package.
    """
    payload = {"sourceCommit": source_commit}
    if package_sha256:
        payload["packageSha256"] = package_sha256
    (dist_dir / DASHBOARD_DIST_MANIFEST).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        "utf-8",
    )


def sha256_file(path: Path) -> str:
    """Compute the SHA-256 hex digest of a file.

    Args:
        path: Path to the file to hash.

    Returns:
        The lowercase hex-encoded SHA-256 digest of the file contents.
    """
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ensure_no_symlinks(root: Path) -> None:
    """Raise an error if any symlinks exist under the given directory tree.

    Args:
        root: Root path to scan recursively for symlinks.

    Raises:
        SystemUpdateError: If any symlink is found within the directory tree.
    """
    for path in root.rglob("*"):
        if path.is_symlink():
            raise SystemUpdateError(
                code="UPDATE_NOT_ALLOWED",
                message="WebUI dist package must not contain symlinks",
                status_code=409,
            )


def same_path(left: Path, right: Path) -> bool:
    """Check whether two paths resolve to the same filesystem location.

    Args:
        left: First path to compare.
        right: Second path to compare.

    Returns:
        ``True`` if both paths resolve identically, ``False`` otherwise or
        if resolution raises an ``OSError``.
    """
    try:
        return left.resolve() == right.resolve()
    except OSError:
        return False


def _read_manifest(target_dist: Path) -> dict[str, object]:
    manifest_path = target_dist / DASHBOARD_DIST_MANIFEST
    if not manifest_path.is_file():
        return {}
    try:
        payload = json.loads(manifest_path.read_text("utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}

