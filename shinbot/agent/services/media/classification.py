"""Small media classification helpers shared by ingestion and context rendering."""

from __future__ import annotations

from typing import Any

EMOJI_IMAGE_SUB_TYPES = frozenset({"1", "none", "null"})


def is_emoji_image_sub_type(value: Any, *, has_sub_type: bool) -> bool:
    """Return whether an image subtype represents a QQ sticker-like image."""
    if not has_sub_type:
        return False
    normalized = str(value).strip().lower()
    return normalized in EMOJI_IMAGE_SUB_TYPES
