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
    """Check whether a string is an HTTP or HTTPS URL.

    Args:
        raw: The string to test.

    Returns:
        ``True`` if the string parses as an HTTP or HTTPS URL.
    """
    return urllib.parse.urlparse(raw).scheme in {"http", "https"}


def normalize_sha256(raw: str) -> str:
    """Normalize and validate a SHA-256 hex digest string.

    Accepts a string that may contain leading/trailing whitespace or extra
    text after the hex token (e.g. from a ``sha256sum`` output).

    Args:
        raw: The raw string containing a SHA-256 hex digest.

    Returns:
        The lowercase 64-character hex digest.

    Raises:
        SystemUpdateError: If the input does not contain a valid 64-character
            hex token.
    """
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
    """Resolve the expected SHA-256 digest for a dist package.

    If an explicit digest is provided it is returned directly. Otherwise the
    function fetches the digest from a remote URL or local file path.

    Args:
        expected_sha256: An explicit 64-character hex digest, or empty.
        expected_sha256_source: A URL or file path to fetch the digest from
            when *expected_sha256* is empty.
        allow_insecure_http: Whether plain HTTP is permitted for remote
            SHA-256 sources.
        resolve_path: Callable that resolves a relative path to an absolute
            ``Path``.

    Returns:
        The normalized lowercase SHA-256 hex digest, or an empty string if
        no source is configured.

    Raises:
        SystemUpdateError: If the source is unreachable, uses insecure HTTP,
            or contains an invalid digest.
    """
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
    """Stage a zip package for deployment by downloading or copying it.

    Downloads the package from a remote URL or copies it from a local path
    into a temporary zip file under *target_parent*, then validates it.

    Args:
        package_source: A URL or file path to the zip package.
        target_parent: Directory where the temporary zip will be created.
        package_max_bytes: Maximum allowed size of the zip in bytes.
        allow_insecure_http: Whether plain HTTP is permitted for remote
            package sources.
        resolve_path: Callable that resolves a relative path to an absolute
            ``Path``.

    Returns:
        Path to the staged temporary zip file.

    Raises:
        SystemUpdateError: If the source is missing, exceeds the size limit,
            or the zip fails validation.
    """
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
    """Download a zip package from a remote URL to a local file.

    Args:
        url: HTTP or HTTPS URL of the zip package.
        target: Local file path to write the downloaded content to.
        package_max_bytes: Maximum allowed download size in bytes. The
            download is aborted if the response exceeds this limit.
        allow_insecure_http: Whether plain HTTP is permitted.

    Raises:
        SystemUpdateError: If the URL scheme is unsupported, insecure HTTP is
            not allowed, the download exceeds the size limit, or the request
            fails.
    """
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
    """Validate the structure and safety of a zip package for deployment.

    Checks that the archive is not empty, contains no unsafe entries, and
    includes an ``index.html`` at the root or inside a single top-level
    directory.

    Args:
        package_path: Path to the zip file to validate.

    Raises:
        SystemUpdateError: If the zip is invalid, empty, contains unsafe
            paths, or is missing a ``dist/index.html``.
    """
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
    """Check whether a zip archive contains a ``dist/index.html`` entry.

    The ``index.html`` may be at the archive root or inside exactly one
    top-level directory. ``__MACOSX`` entries are ignored.

    Args:
        entries: List of ``ZipInfo`` entries from the archive.

    Returns:
        ``True`` if a valid ``index.html`` location is found.
    """
    names = [info.filename.strip("/") for info in entries if not info.is_dir()]
    names = [name for name in names if name and not name.startswith("__MACOSX/")]
    if "index.html" in names:
        return True

    top_levels = {name.split("/", 1)[0] for name in names if "/" in name}
    return len(top_levels) == 1 and f"{next(iter(top_levels))}/index.html" in names


def extract_zip_package(package_path: Path, extract_root: Path) -> None:
    """Extract a validated zip package to a target directory.

    Each entry is validated for safety before extraction. Path traversal
    and symlink attacks are rejected.

    Args:
        package_path: Path to the zip file to extract.
        extract_root: Directory to extract the archive contents into.

    Raises:
        SystemUpdateError: If any entry contains unsafe paths (absolute
            paths, parent-directory traversal, or symlinks).
    """
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
    """Validate that a single zip entry is safe for extraction.

    Rejects absolute paths, parent-directory traversal, and symlinks.

    Args:
        info: The ``ZipInfo`` entry to validate.

    Raises:
        SystemUpdateError: If the entry is an absolute path, contains
            ``..`` traversal, or is a symlink.
    """
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
    """Locate the actual dist directory inside an extracted zip package.

    Handles two layouts: ``index.html`` at the extraction root, or
    ``index.html`` inside exactly one non-``__MACOSX`` subdirectory.

    Args:
        extract_root: Root directory of the extracted zip contents.

    Returns:
        The resolved path to the directory containing ``index.html``.

    Raises:
        SystemUpdateError: If no valid ``index.html`` location is found or
            the resolved directory contains symlinks.
    """
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
