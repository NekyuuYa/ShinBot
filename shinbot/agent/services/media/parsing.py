"""Response parsing helpers for media inspection flows."""

from __future__ import annotations

import json
from typing import Any

# Use json_object instead of json_schema for broader model compatibility.
# Not all models support the strict json_schema response format.
MEDIA_INSPECTION_RESPONSE_FORMAT: dict[str, Any] = {
    "type": "json_object",
}


def parse_media_inspection_payload(text: str) -> dict[str, Any] | None:
    """Parse one JSON media inspection response payload."""

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    kind = str(payload.get("kind") or "")
    if kind not in {"generic_image", "meme_image", "emoji_native"}:
        return None
    return payload


def clip_media_digest(text: str) -> str:
    """Clamp media digest length to the configured summary budget."""

    value = text.strip()
    if len(value) <= 50:
        return value
    return value[:50]
