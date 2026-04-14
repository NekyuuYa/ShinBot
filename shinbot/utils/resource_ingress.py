"""Resource ingress helpers for media pre-download and AST rewriting."""

from __future__ import annotations

import asyncio
import hashlib
import mimetypes
from pathlib import Path
from typing import Any

import httpx

from shinbot.models.elements import MessageElement


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
) -> list[MessageElement]:
    """Download remote media elements to a local cache directory.

    Only img/video/file/audio elements with HTTP(S) src values are rewritten.
    Other elements are preserved as-is.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(timeout, connect=timeout, read=timeout, write=timeout, pool=timeout),
        follow_redirects=True,
    ) as client:
        return await _download_elements(elements, cache_dir, client, timeout)


async def _download_elements(
    elements: list[MessageElement],
    cache_dir: Path,
    client: httpx.AsyncClient,
    timeout: float,
) -> list[MessageElement]:
    result: list[MessageElement] = []
    for element in elements:
        updated = element
        if element.type in {"img", "video", "file", "audio"}:
            src = str(element.attrs.get("src", ""))
            if src.startswith(("http://", "https://")):
                local_src = await _download_single_resource(src, cache_dir, client, timeout)
                if local_src:
                    attrs = dict(element.attrs)
                    attrs["src"] = local_src
                    updated = element.model_copy(update={"attrs": attrs})
        if element.children:
            children = await _download_elements(element.children, cache_dir, client, timeout)
            updated = updated.model_copy(update={"children": children})
        result.append(updated)
    return result


async def _download_single_resource(
    url: str,
    cache_dir: Path,
    client: httpx.AsyncClient,
    timeout: float,
) -> str | None:
    resource_hash = _hash_resource_url(url)
    try:
        response = await asyncio.wait_for(client.get(url), timeout=timeout)
        response.raise_for_status()
        suffix = _guess_suffix(url, response.headers.get("content-type"))
        file_path = cache_dir / f"{resource_hash}{suffix}"
        file_path.write_bytes(response.content)
        return str(file_path.resolve())
    except httpx.TimeoutException, httpx.HTTPError, OSError:
        return None


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
