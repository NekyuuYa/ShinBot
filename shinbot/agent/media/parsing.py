"""Response parsing helpers for media inspection flows."""

from __future__ import annotations

import json
from typing import Any

MEDIA_INSPECTION_RESPONSE_FORMAT: dict[str, Any] = {
    "type": "json_schema",
    "json_schema": {
        "name": "media_inspection_result",
        "schema": {
            "type": "object",
            "properties": {
                "kind": {
                    "type": "string",
                    "enum": ["generic_image", "meme_image", "emoji_native"],
                },
                "digest": {"type": "string"},
                "confidence_band": {
                    "type": "string",
                    "enum": ["low", "medium", "high"],
                },
                "reason": {"type": "string"},
            },
            "required": ["kind", "digest", "confidence_band", "reason"],
            "additionalProperties": False,
        },
    },
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
