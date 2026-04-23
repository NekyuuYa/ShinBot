"""Resource ingress helpers for media pre-download and AST rewriting."""

from __future__ import annotations

import hashlib
import mimetypes
from pathlib import Path
from typing import Any

import httpx

from shinbot.schema.elements import MessageElement

DEFAULT_MAX_RESOURCE_BYTES = 15 * 1024 * 1024
MEDIA_RESOURCE_TYPES = frozenset({"img", "video"})
FILE_RESOURCE_TYPES = frozenset({"file"})


def _hash_resource_url(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def _guess_suffix(url: str, content_type: str | None = None) -> str:
    suffix = Path(url.split("?", 1)[0]).suffix
    if suffix:
        return suffix
    if content_type:
        guessed = mimetypes.guess_extension(content_type.split(";", 1)[0].strip())
        if guessed:
            return guessed
    return ".bin"


async def download_resource_elements(
    elements: list[MessageElement],
    cache_dir: Path,
    *,
    timeout: float = 5.0,
    download_media: bool = True,
    download_files: bool = False,
    max_bytes: int = DEFAULT_MAX_RESOURCE_BYTES,
) -> list[MessageElement]:
    """Download remote resource elements to a local cache directory.

    Images/videos and files are controlled independently. Other elements are
    preserved as-is.
    """
    max_bytes = _normalize_max_bytes(max_bytes)
    cache_dir.mkdir(parents=True, exist_ok=True)
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(timeout, connect=timeout, read=timeout, write=timeout, pool=timeout),
        follow_redirects=True,
    ) as client:
        return await _download_elements(
            elements,
            cache_dir,
            client,
            download_media=download_media,
            download_files=download_files,
            max_bytes=max_bytes,
        )


async def _download_elements(
    elements: list[MessageElement],
    cache_dir: Path,
    client: httpx.AsyncClient,
    download_media: bool,
    download_files: bool,
    max_bytes: int,
) -> list[MessageElement]:
    result: list[MessageElement] = []
    for element in elements:
        updated = element
        should_download = (
            download_media and element.type in MEDIA_RESOURCE_TYPES
        ) or (
            download_files and element.type in FILE_RESOURCE_TYPES
        )
        if should_download:
            src = str(element.attrs.get("src", ""))
            if src.startswith(("http://", "https://")):
                local_src = await _download_single_resource(
                    src,
                    cache_dir,
                    client,
                    max_bytes=max_bytes,
                )
                if local_src:
                    attrs = dict(element.attrs)
                    attrs["src"] = local_src
                    updated = element.model_copy(update={"attrs": attrs})
        if element.children:
            children = await _download_elements(
                element.children,
                cache_dir,
                client,
                download_media=download_media,
                download_files=download_files,
                max_bytes=max_bytes,
            )
            updated = updated.model_copy(update={"children": children})
        result.append(updated)
    return result


async def _download_single_resource(
    url: str,
    cache_dir: Path,
    client: httpx.AsyncClient,
    max_bytes: int,
) -> str | None:
    resource_hash = _hash_resource_url(url)
    try:
        async with client.stream("GET", url) as response:
            response.raise_for_status()
            content_length = response.headers.get("content-length")
            if _exceeds_max_bytes(content_length, max_bytes):
                return None
            content = bytearray()
            async for chunk in response.aiter_bytes():
                content.extend(chunk)
                if max_bytes > 0 and len(content) > max_bytes:
                    return None
            suffix = _guess_suffix(url, response.headers.get("content-type"))
        file_path = cache_dir / f"{resource_hash}{suffix}"
        file_path.write_bytes(bytes(content))
        return str(file_path.resolve())
    except (httpx.TimeoutException, httpx.HTTPError, OSError):
        return None


def _exceeds_max_bytes(content_length: str | None, max_bytes: int) -> bool:
    if not content_length:
        return False
    try:
        return int(content_length) > max_bytes
    except ValueError:
        return False


def _normalize_max_bytes(max_bytes: int) -> int:
    return max_bytes if max_bytes > 0 else DEFAULT_MAX_RESOURCE_BYTES


def summarize_message_modalities(elements: list[MessageElement]) -> dict[str, Any]:
    """Summarize the visible message modalities for audit logging."""

    counts = {
        "text": 0,
        "image": 0,
        "audio": 0,
        "video": 0,
        "file": 0,
        "other": 0,
    }

    def visit(items: list[MessageElement]) -> None:
        for element in items:
            if element.type == "text":
                counts["text"] += 1
            elif element.type == "img":
                counts["image"] += 1
            elif element.type == "audio":
                counts["audio"] += 1
            elif element.type == "video":
                counts["video"] += 1
            elif element.type == "file":
                counts["file"] += 1
            else:
                counts["other"] += 1
            if element.children:
                visit(element.children)

    visit(elements)

    total = sum(counts.values())
    ratios = {
        key: round((value / total) * 100, 2) if total else 0.0 for key, value in counts.items()
    }

    return {
        "total": total,
        "counts": counts,
        "ratios": ratios,
    }
