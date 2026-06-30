"""Tests for shinbot.utils.resource_ingress — download, cache, dedup."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from shinbot.schema.elements import MessageElement
from shinbot.utils.resource_ingress import (
    DEFAULT_MAX_RESOURCE_BYTES,
    _download_single_resource,
    _exceeds_max_bytes,
    _guess_suffix,
    _hash_resource_url,
    _normalize_max_bytes,
    download_resource_elements,
    summarize_message_modalities,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _async_iter(items):
    """Trivial async iterator over *items*."""
    for item in items:
        yield item


def _make_stream_response(
    *,
    status_code: int = 200,
    headers: dict[str, str] | None = None,
    chunks: list[bytes] | None = None,
) -> MagicMock:
    """Build a mock httpx streamed response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = headers or {}
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "error", request=MagicMock(), response=resp
        )
    resp.aiter_bytes = lambda: _async_iter(chunks or [b"fake-image-data"])
    return resp


def _patch_client_stream(response: MagicMock) -> MagicMock:
    """Return a mock AsyncClient whose .stream() yields *response* as context manager."""
    stream_cm = MagicMock()
    stream_cm.__aenter__ = AsyncMock(return_value=response)
    stream_cm.__aexit__ = AsyncMock(return_value=False)

    client = MagicMock()
    client.stream = MagicMock(return_value=stream_cm)
    return client


# ---------------------------------------------------------------------------
# Pure helper tests
# ---------------------------------------------------------------------------


class TestHashResourceUrl:
    def test_deterministic(self):
        a = _hash_resource_url("https://example.com/img.png")
        b = _hash_resource_url("https://example.com/img.png")
        assert a == b

    def test_different_urls_differ(self):
        a = _hash_resource_url("https://a.com/1.png")
        b = _hash_resource_url("https://b.com/2.png")
        assert a != b

    def test_returns_hex_string(self):
        h = _hash_resource_url("test")
        assert len(h) == 64  # sha256 hex digest length
        assert all(c in "0123456789abcdef" for c in h)


class TestGuessSuffix:
    def test_suffix_from_url(self):
        assert _guess_suffix("https://example.com/photo.jpg") == ".jpg"

    def test_suffix_from_url_with_query(self):
        assert _guess_suffix("https://example.com/photo.png?v=1") == ".png"

    def test_suffix_from_content_type(self):
        assert _guess_suffix("https://example.com/raw", "image/jpeg") == ".jpg"

    def test_content_type_with_charset(self):
        assert _guess_suffix("https://example.com/data", "image/png; charset=utf-8") == ".png"

    def test_fallback_to_bin(self):
        assert _guess_suffix("https://example.com/unknown") == ".bin"

    def test_url_suffix_takes_priority_over_content_type(self):
        assert _guess_suffix("https://example.com/f.gif", "image/png") == ".gif"


class TestExceedsMaxBytes:
    def test_none_content_length(self):
        assert _exceeds_max_bytes(None, 1024) is False

    def test_empty_content_length(self):
        assert _exceeds_max_bytes("", 1024) is False

    def test_within_limit(self):
        assert _exceeds_max_bytes("500", 1024) is False

    def test_exceeds_limit(self):
        assert _exceeds_max_bytes("2000", 1024) is True

    def test_invalid_content_length(self):
        assert _exceeds_max_bytes("not-a-number", 1024) is False


class TestNormalizeMaxBytes:
    def test_positive_passthrough(self):
        assert _normalize_max_bytes(5000) == 5000

    def test_zero_returns_default(self):
        assert _normalize_max_bytes(0) == DEFAULT_MAX_RESOURCE_BYTES

    def test_negative_returns_default(self):
        assert _normalize_max_bytes(-1) == DEFAULT_MAX_RESOURCE_BYTES


# ---------------------------------------------------------------------------
# _download_single_resource
# ---------------------------------------------------------------------------


class TestDownloadSingleResource:
    @pytest.fixture
    def cache_dir(self, tmp_path: Path) -> Path:
        d = tmp_path / "cache"
        d.mkdir()
        return d

    async def test_successful_download(self, cache_dir: Path):
        payload = b"\x89PNG\r\n"
        response = _make_stream_response(
            headers={"content-type": "image/png"},
            chunks=[payload],
        )
        client = _patch_client_stream(response)

        result = await _download_single_resource(
            "https://example.com/a.png", cache_dir, client, max_bytes=1024
        )

        assert result is not None
        assert result.endswith(".png")
        p = Path(result)
        assert p.exists()
        assert p.read_bytes() == payload

    async def test_returns_none_on_timeout(self, cache_dir: Path):
        stream_cm = MagicMock()
        stream_cm.__aenter__ = AsyncMock(side_effect=httpx.TimeoutException("timed out"))
        stream_cm.__aexit__ = AsyncMock(return_value=False)

        client = AsyncMock(spec=httpx.AsyncClient)
        client.stream = MagicMock(return_value=stream_cm)

        result = await _download_single_resource(
            "https://example.com/slow.png", cache_dir, client, max_bytes=1024
        )
        assert result is None

    async def test_returns_none_on_http_error(self, cache_dir: Path):
        response = _make_stream_response(status_code=404)
        client = _patch_client_stream(response)

        result = await _download_single_resource(
            "https://example.com/missing.png", cache_dir, client, max_bytes=1024
        )
        assert result is None

    async def test_returns_none_when_content_length_exceeds_max(self, cache_dir: Path):
        response = _make_stream_response(
            headers={"content-length": "999999"},
        )
        client = _patch_client_stream(response)

        result = await _download_single_resource(
            "https://example.com/huge.png", cache_dir, client, max_bytes=1024
        )
        assert result is None

    async def test_returns_none_when_streamed_bytes_exceed_max(self, cache_dir: Path):
        big_chunk = b"x" * 2048
        response = _make_stream_response(chunks=[big_chunk])
        client = _patch_client_stream(response)

        result = await _download_single_resource(
            "https://example.com/big.bin", cache_dir, client, max_bytes=1024
        )
        assert result is None

    async def test_dedup_same_url_writes_one_file(self, cache_dir: Path):
        response = _make_stream_response(chunks=[b"same-data"])
        client = _patch_client_stream(response)

        r1 = await _download_single_resource(
            "https://example.com/same.png", cache_dir, client, max_bytes=1024
        )
        r2 = await _download_single_resource(
            "https://example.com/same.png", cache_dir, client, max_bytes=1024
        )

        # Both succeed and point to the same path
        assert r1 is not None
        assert r2 is not None
        assert r1 == r2
        # Only one file in cache (hash-based naming)
        cached_files = list(cache_dir.iterdir())
        assert len(cached_files) == 1

    async def test_different_urls_write_different_files(self, cache_dir: Path):
        response = _make_stream_response(chunks=[b"data"])
        client = _patch_client_stream(response)

        r1 = await _download_single_resource(
            "https://a.com/1.png", cache_dir, client, max_bytes=1024
        )
        r2 = await _download_single_resource(
            "https://b.com/2.png", cache_dir, client, max_bytes=1024
        )

        assert r1 is not None
        assert r2 is not None
        assert r1 != r2
        assert len(list(cache_dir.iterdir())) == 2

    async def test_returns_none_on_os_error(self, cache_dir: Path):
        response = _make_stream_response(chunks=[b"data"])
        client = _patch_client_stream(response)

        with patch.object(Path, "write_bytes", side_effect=OSError("disk full")):
            result = await _download_single_resource(
                "https://example.com/fail.png", cache_dir, client, max_bytes=1024
            )
        assert result is None


# ---------------------------------------------------------------------------
# download_resource_elements (integration of the full pipeline)
# ---------------------------------------------------------------------------


class TestDownloadResourceElements:
    def _patch_httpx_client(self, response: MagicMock):
        """Return a context-manager patch for httpx.AsyncClient with a mock client."""
        mock_client = _patch_client_stream(response)
        mock_instance = MagicMock()
        mock_instance.__aenter__ = AsyncMock(return_value=mock_client)
        mock_instance.__aexit__ = AsyncMock(return_value=False)
        return patch(
            "shinbot.utils.resource_ingress.httpx.AsyncClient",
            return_value=mock_instance,
        )

    async def test_download_img_element(self, tmp_path: Path):
        cache_dir = tmp_path / "dl_cache"
        payload = b"\x89PNG\r\n"
        response = _make_stream_response(
            headers={"content-type": "image/png"},
            chunks=[payload],
        )

        elements = [MessageElement.img("https://example.com/photo.png")]

        with self._patch_httpx_client(response):
            result = await download_resource_elements(elements, cache_dir)

        assert len(result) == 1
        assert result[0].type == "img"
        src = result[0].attrs["src"]
        assert src.startswith(str(cache_dir))
        assert Path(src).exists()

    async def test_text_element_preserved(self, tmp_path: Path):
        cache_dir = tmp_path / "dl_cache"
        elements = [MessageElement.text("hello")]

        result = await download_resource_elements(elements, cache_dir)

        assert len(result) == 1
        assert result[0].type == "text"
        assert result[0].attrs["content"] == "hello"

    async def test_non_http_src_not_downloaded(self, tmp_path: Path):
        cache_dir = tmp_path / "dl_cache"
        elements = [MessageElement.img("file:///local/image.png")]

        result = await download_resource_elements(elements, cache_dir)

        assert result[0].attrs["src"] == "file:///local/image.png"

    async def test_file_element_not_downloaded_by_default(self, tmp_path: Path):
        """download_files defaults to False."""
        cache_dir = tmp_path / "dl_cache"
        elements = [MessageElement.file("https://example.com/doc.pdf")]

        result = await download_resource_elements(elements, cache_dir)

        assert result[0].attrs["src"] == "https://example.com/doc.pdf"

    async def test_file_element_downloaded_when_enabled(self, tmp_path: Path):
        cache_dir = tmp_path / "dl_cache"
        response = _make_stream_response(
            headers={"content-type": "application/pdf"},
            chunks=[b"%PDF-1.4"],
        )

        elements = [MessageElement.file("https://example.com/doc.pdf")]

        with self._patch_httpx_client(response):
            result = await download_resource_elements(
                elements, cache_dir, download_files=True
            )

        src = result[0].attrs["src"]
        assert src.startswith(str(cache_dir))

    async def test_media_download_disabled(self, tmp_path: Path):
        cache_dir = tmp_path / "dl_cache"
        elements = [MessageElement.img("https://example.com/photo.png")]

        result = await download_resource_elements(
            elements, cache_dir, download_media=False
        )

        assert result[0].attrs["src"] == "https://example.com/photo.png"

    async def test_video_element_downloaded(self, tmp_path: Path):
        cache_dir = tmp_path / "dl_cache"
        response = _make_stream_response(
            headers={"content-type": "video/mp4"},
            chunks=[b"\x00\x00\x00\x1cftyp"],
        )
        elements = [MessageElement.video("https://example.com/clip.mp4")]

        with self._patch_httpx_client(response):
            result = await download_resource_elements(elements, cache_dir)

        src = result[0].attrs["src"]
        assert src.endswith(".mp4")
        assert Path(src).exists()

    async def test_nested_children_processed(self, tmp_path: Path):
        """Images inside a quote element should be downloaded."""
        cache_dir = tmp_path / "dl_cache"
        response = _make_stream_response(
            headers={"content-type": "image/png"},
            chunks=[b"data"],
        )
        inner_img = MessageElement.img("https://example.com/nested.png")
        quote = MessageElement.quote(id="msg-1", children=[inner_img])
        elements = [quote]

        with self._patch_httpx_client(response):
            result = await download_resource_elements(elements, cache_dir)

        child_src = result[0].children[0].attrs["src"]
        assert child_src.startswith(str(cache_dir))

    async def test_empty_elements_list(self, tmp_path: Path):
        cache_dir = tmp_path / "dl_cache"
        result = await download_resource_elements([], cache_dir)
        assert result == []

    async def test_creates_cache_dir(self, tmp_path: Path):
        cache_dir = tmp_path / "deep" / "nested" / "cache"
        assert not cache_dir.exists()
        await download_resource_elements([], cache_dir)
        assert cache_dir.exists()

    async def test_http_error_returns_original_element(self, tmp_path: Path):
        """On HTTP error the element is kept with original src."""
        cache_dir = tmp_path / "dl_cache"
        response = _make_stream_response(status_code=500)
        elements = [MessageElement.img("https://example.com/broken.png")]

        with self._patch_httpx_client(response):
            result = await download_resource_elements(elements, cache_dir)

        # Download fails -> src unchanged
        assert result[0].attrs["src"] == "https://example.com/broken.png"

    async def test_mixed_elements(self, tmp_path: Path):
        """Non-resource elements pass through alongside downloaded ones."""
        cache_dir = tmp_path / "dl_cache"
        response = _make_stream_response(
            headers={"content-type": "image/png"},
            chunks=[b"img"],
        )
        elements = [
            MessageElement.text("hello"),
            MessageElement.img("https://example.com/a.png"),
            MessageElement.at(id="user-1"),
        ]

        with self._patch_httpx_client(response):
            result = await download_resource_elements(elements, cache_dir)

        assert result[0].type == "text"
        assert result[1].type == "img"
        assert result[1].attrs["src"].startswith(str(cache_dir))
        assert result[2].type == "at"


# ---------------------------------------------------------------------------
# summarize_message_modalities
# ---------------------------------------------------------------------------


class TestSummarizeMessageModalities:
    def test_empty_list(self):
        result = summarize_message_modalities([])
        assert result["total"] == 0
        assert all(v == 0 for v in result["counts"].values())
        assert all(v == 0.0 for v in result["ratios"].values())

    def test_single_text(self):
        result = summarize_message_modalities([MessageElement.text("hi")])
        assert result["total"] == 1
        assert result["counts"]["text"] == 1
        assert result["ratios"]["text"] == 100.0

    def test_multiple_types(self):
        elements = [
            MessageElement.text("hi"),
            MessageElement.img("x.png"),
            MessageElement.audio("x.mp3"),
            MessageElement.video("x.mp4"),
            MessageElement.file("x.pdf"),
            MessageElement.at(id="1"),
        ]
        result = summarize_message_modalities(elements)
        assert result["total"] == 6
        assert result["counts"]["text"] == 1
        assert result["counts"]["image"] == 1
        assert result["counts"]["audio"] == 1
        assert result["counts"]["video"] == 1
        assert result["counts"]["file"] == 1
        assert result["counts"]["other"] == 1

    def test_nested_children_counted(self):
        inner = MessageElement.img("nested.png")
        quote = MessageElement.quote(id="q1", children=[inner])
        result = summarize_message_modalities([quote])
        # quote is "other", inner img is "image"
        assert result["total"] == 2
        assert result["counts"]["image"] == 1
        assert result["counts"]["other"] == 1

    def test_ratios_are_percentages(self):
        elements = [MessageElement.text("a"), MessageElement.text("b")]
        result = summarize_message_modalities(elements)
        assert result["ratios"]["text"] == 100.0
        assert result["ratios"]["image"] == 0.0
