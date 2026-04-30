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
    if not dist_dir.is_dir():
        return "WebUI dist source directory does not exist"
    if not (dist_dir / "index.html").is_file():
        return "WebUI dist source must contain index.html"
    for path in dist_dir.rglob("*"):
        if path.is_symlink():
            return "WebUI dist source must not contain symlinks"
    return ""


def read_deployed_source_commit(target_dist: Path) -> str:
    payload = _read_manifest(target_dist)
    commit = payload.get("sourceCommit")
    return commit if isinstance(commit, str) else ""


def read_deployed_package_sha256(target_dist: Path) -> str:
    payload = _read_manifest(target_dist)
    package_sha = payload.get("packageSha256")
    return package_sha if isinstance(package_sha, str) else ""


def write_manifest(dist_dir: Path, source_commit: str, *, package_sha256: str = "") -> None:
    payload = {"sourceCommit": source_commit}
    if package_sha256:
        payload["packageSha256"] = package_sha256
    (dist_dir / DASHBOARD_DIST_MANIFEST).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        "utf-8",
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ensure_no_symlinks(root: Path) -> None:
    for path in root.rglob("*"):
        if path.is_symlink():
            raise SystemUpdateError(
                code="UPDATE_NOT_ALLOWED",
                message="WebUI dist package must not contain symlinks",
                status_code=409,
            )


def same_path(left: Path, right: Path) -> bool:
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

