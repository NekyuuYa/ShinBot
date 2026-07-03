"""Small media classification helpers shared by ingestion and context rendering."""

from __future__ import annotations

from typing import Any

# OneBot v11 implementations (Lagrange/NapCat) use sub_type for images:
#   0 = normal image, 1 = flash, 2 = screenshot, 3 = sticker/animated emoji
# Only sub_type "3" represents a sticker-like image.
EMOJI_IMAGE_SUB_TYPES = frozenset({"3"})


def is_emoji_image_sub_type(value: Any, *, has_sub_type: bool) -> bool:
    """Return whether an image subtype represents a sticker-like image."""
    if not has_sub_type:
        return False
    normalized = str(value).strip().lower()
    return normalized in EMOJI_IMAGE_SUB_TYPES
