"""Zip package download, integrity, and extraction helpers for dashboard dist updates."""

from __future__ import annotations

import os
import shutil
import stat
import tempfile
import urllib.parse
import urllib.request
import zipfile
from collections.abc import Callable
from pathlib import Path

from .common import SystemUpdateError
from .dist_files import ensure_no_symlinks


def is_url(raw: str) -> bool:
    return urllib.parse.urlparse(raw).scheme in {"http", "https"}


def normalize_sha256(raw: str) -> str:
    token = raw.strip().split()[0] if raw.strip() else ""
    if len(token) != 64 or any(ch not in "0123456789abcdefABCDEF" for ch in token):
        raise SystemUpdateError(
            code="UPDATE_NOT_ALLOWED",
            message="WebUI dist SHA256 must be a 64-character hex digest",
            status_code=409,
        )
    return token.lower()


def resolve_expected_package_sha256(
    *,
    expected_sha256: str,
    expected_sha256_source: str,
    allow_insecure_http: bool,
    resolve_path: Callable[[object], Path],
) -> str:
    if expected_sha256:
        return normalize_sha256(expected_sha256)
    if not expected_sha256_source:
        return ""

    if is_url(expected_sha256_source):
        parsed = urllib.parse.urlparse(expected_sha256_source)
        if parsed.scheme == "http" and not allow_insecure_http:
            raise SystemUpdateError(
                code="UPDATE_NOT_ALLOWED",
                message="WebUI dist SHA256 URL must use HTTPS",
                status_code=409,
            )
        try:
            with urllib.request.urlopen(expected_sha256_source, timeout=10) as response:
                raw = response.read(4096).decode("utf-8", errors="replace")
        except Exception as exc:
            raise SystemUpdateError(
                code="UPDATE_FAILED",
                message=f"Failed to read WebUI dist SHA256: {exc}",
                status_code=502,
            ) from exc
        return normalize_sha256(raw)

    sha_path = resolve_path(expected_sha256_source)
    if not sha_path.is_file():
        raise SystemUpdateError(
            code="UPDATE_NOT_ALLOWED",
            message="Configured WebUI dist SHA256 file does not exist",
            status_code=409,
        )
    return normalize_sha256(sha_path.read_text("utf-8"))


def stage_package_zip(
    *,
    package_source: str,
    target_parent: Path,
    package_max_bytes: int,
    allow_insecure_http: bool,
    resolve_path: Callable[[object], Path],
) -> Path:
    fd, tmp_name = tempfile.mkstemp(prefix=".webui-dist-package-", suffix=".zip", dir=target_parent)
    os.close(fd)
    tmp_zip = Path(tmp_name)

    try:
        if is_url(package_source):
            download_package(
                url=package_source,
                target=tmp_zip,
                package_max_bytes=package_max_bytes,
                allow_insecure_http=allow_insecure_http,
            )
        else:
            package_path = resolve_path(package_source)
            if not package_path.is_file():
                raise SystemUpdateError(
                    code="UPDATE_NOT_ALLOWED",
                    message="Configured WebUI dist zip package does not exist",
                    status_code=409,
                )
            if package_path.stat().st_size > package_max_bytes:
                raise SystemUpdateError(
                    code="UPDATE_NOT_ALLOWED",
                    message="Configured WebUI dist zip package exceeds the size limit",
                    status_code=409,
                )
            shutil.copyfile(package_path, tmp_zip)

        validate_zip_package(tmp_zip)
    except Exception:
        if tmp_zip.exists():
            tmp_zip.unlink()
        raise
    return tmp_zip


def download_package(
    *,
    url: str,
    target: Path,
    package_max_bytes: int,
    allow_insecure_http: bool,
) -> None:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise SystemUpdateError(
            code="UPDATE_NOT_ALLOWED",
            message="WebUI dist zip package URL must use HTTP or HTTPS",
            status_code=409,
        )
    if parsed.scheme == "http" and not allow_insecure_http:
        raise SystemUpdateError(
            code="UPDATE_NOT_ALLOWED",
            message="WebUI dist zip package URL must use HTTPS",
            status_code=409,
        )

    request = urllib.request.Request(url, headers={"User-Agent": "ShinBot-WebUI-Updater"})
    downloaded = 0
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            with target.open("wb") as out:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    downloaded += len(chunk)
                    if downloaded > package_max_bytes:
                        raise SystemUpdateError(
                            code="UPDATE_NOT_ALLOWED",
                            message="Downloaded WebUI dist zip package exceeds the size limit",
                            status_code=409,
                        )
                    out.write(chunk)
    except SystemUpdateError:
        raise
    except Exception as exc:
        raise SystemUpdateError(
            code="UPDATE_FAILED",
            message=f"Failed to download WebUI dist zip package: {exc}",
            status_code=502,
        ) from exc


def validate_zip_package(package_path: Path) -> None:
    try:
        with zipfile.ZipFile(package_path) as archive:
            entries = archive.infolist()
            if not entries:
                raise SystemUpdateError(
                    code="UPDATE_NOT_ALLOWED",
                    message="WebUI dist zip package is empty",
                    status_code=409,
                )
            for info in entries:
                validate_zip_entry(info)
            if not zip_contains_dist_index(entries):
                raise SystemUpdateError(
                    code="UPDATE_NOT_ALLOWED",
                    message=(
                        "WebUI dist zip package must contain index.html at root "
                        "or in one top-level directory"
                    ),
                    status_code=409,
                )
    except zipfile.BadZipFile as exc:
        raise SystemUpdateError(
            code="UPDATE_NOT_ALLOWED",
            message="WebUI dist package is not a valid zip file",
            status_code=409,
        ) from exc


def zip_contains_dist_index(entries: list[zipfile.ZipInfo]) -> bool:
    names = [info.filename.strip("/") for info in entries if not info.is_dir()]
    names = [name for name in names if name and not name.startswith("__MACOSX/")]
    if "index.html" in names:
        return True

    top_levels = {name.split("/", 1)[0] for name in names if "/" in name}
    return len(top_levels) == 1 and f"{next(iter(top_levels))}/index.html" in names


def extract_zip_package(package_path: Path, extract_root: Path) -> None:
    with zipfile.ZipFile(package_path) as archive:
        for info in archive.infolist():
            validate_zip_entry(info)
            destination = (extract_root / info.filename).resolve()
            if not destination.is_relative_to(extract_root):
                raise SystemUpdateError(
                    code="UPDATE_NOT_ALLOWED",
                    message="WebUI dist zip package contains unsafe paths",
                    status_code=409,
                )
        archive.extractall(extract_root)


def validate_zip_entry(info: zipfile.ZipInfo) -> None:
    name = info.filename
    if not name or name.startswith(("/", "\\")):
        raise SystemUpdateError(
            code="UPDATE_NOT_ALLOWED",
            message="WebUI dist zip package contains absolute paths",
            status_code=409,
        )
    normalized = Path(name)
    if ".." in normalized.parts:
        raise SystemUpdateError(
            code="UPDATE_NOT_ALLOWED",
            message="WebUI dist zip package contains parent-directory paths",
            status_code=409,
        )

    file_type = (info.external_attr >> 16) & 0o170000
    if stat.S_ISLNK(file_type):
        raise SystemUpdateError(
            code="UPDATE_NOT_ALLOWED",
            message="WebUI dist zip package must not contain symlinks",
            status_code=409,
        )


def resolve_extracted_dist(extract_root: Path) -> Path:
    if (extract_root / "index.html").is_file():
        ensure_no_symlinks(extract_root)
        return extract_root

    children = [path for path in extract_root.iterdir() if path.name != "__MACOSX"]
    dirs = [path for path in children if path.is_dir()]
    if len(dirs) == 1 and (dirs[0] / "index.html").is_file():
        ensure_no_symlinks(dirs[0])
        return dirs[0]

    raise SystemUpdateError(
        code="UPDATE_NOT_ALLOWED",
        message="WebUI dist zip package must contain index.html at root or in one top-level directory",
        status_code=409,
    )
