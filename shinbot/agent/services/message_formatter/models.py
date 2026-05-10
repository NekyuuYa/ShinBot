"""Configuration and result models for the message formatter service."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class ImageMode(StrEnum):
    """How images are represented in formatted output."""

    ORIGINAL = "original"
    THUMBNAIL = "thumbnail"
    DESCRIPTION = "description"


class EmojiMode(StrEnum):
    """How custom emojis are represented in formatted output."""

    ORIGINAL = "original"
    THUMBNAIL = "thumbnail"
    SEMANTIC = "semantic"


class PackMode(StrEnum):
    """How multiple messages are packed into output."""

    INDIVIDUAL = "individual"
    PACK = "pack"


@dataclass(slots=True, frozen=True)
class MessageFormatConfig:
    """Controls how messages are rendered for LLM consumption."""

    image_mode: ImageMode = ImageMode.DESCRIPTION
    emoji_mode: EmojiMode = EmojiMode.SEMANTIC
    pack_mode: PackMode = PackMode.PACK
    timestamp_mode: str = "sparse"
    inject_sender: bool = True
    self_platform_id: str = ""
    now_ms: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class FormattedMessage:
    """A single formatted message ready for LLM consumption."""

    sender_id: str
    sender_label: str
    text: str
    created_at_ms: int
    record_id: int | None = None
    content_blocks: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class FormatResult:
    """Output of a message formatting pass."""

    messages: list[FormattedMessage]
    packed_text: str = ""
    content_blocks: list[dict[str, Any]] = field(default_factory=list)
    message_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)
