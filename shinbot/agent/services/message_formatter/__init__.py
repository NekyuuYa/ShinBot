"""Message formatter service for LLM-consumable message rendering.

Provides a stateless, config-driven formatting surface that converts raw
message records into text or content blocks suitable for prompt injection.

Usage::

    from shinbot.agent.services.message_formatter import (
        MessageFormatConfig, MessageFormatterService, ImageMode, EmojiMode,
    )

    svc = MessageFormatterService(identity_store=identity, media_service=media)
    result = svc.format(records, MessageFormatConfig(image_mode=ImageMode.DESCRIPTION))
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from shinbot.agent.services.context.builders.message_parts import parse_message_parts

from .formatter import format_messages
from .models import (
    EmojiMode,
    FormatResult,
    FormattedMessage,
    ImageMode,
    MessageFormatConfig,
    PackMode,
)

if TYPE_CHECKING:
    from shinbot.agent.services.identity import IdentityStore
    from shinbot.agent.services.media import MediaService


class MessageFormatterService:
    """Public API for formatting message records into LLM-consumable output.

    Wraps the pure formatting engine with optional identity/media resolution.
    """

    def __init__(
        self,
        *,
        identity_store: IdentityStore | None = None,
        media_service: MediaService | None = None,
    ) -> None:
        self._identity_store = identity_store
        self._media_service = media_service

    def format(
        self,
        records: list[dict[str, Any]],
        config: MessageFormatConfig | None = None,
    ) -> FormatResult:
        """Format message records into LLM-consumable output.

        Args:
            records: Message log records.
            config: Formatting options. Defaults to standard config.

        Returns:
            A :class:`FormatResult` with formatted messages.
        """
        resolved_config = config or MessageFormatConfig()
        display_names = self._resolve_display_names(records, resolved_config)
        image_descriptions = self._resolve_image_descriptions(records, resolved_config)
        return format_messages(
            records,
            resolved_config,
            display_names=display_names,
            image_descriptions=image_descriptions,
        )

    def format_text(
        self,
        records: list[dict[str, Any]],
        config: MessageFormatConfig | None = None,
    ) -> str:
        """Convenience: return only the packed text output."""
        result = self.format(records, config)
        return result.packed_text

    def _resolve_display_names(
        self,
        records: list[dict[str, Any]],
        config: MessageFormatConfig,
    ) -> dict[str, str]:
        if self._identity_store is None:
            return {}
        names: dict[str, str] = {}
        for record in records:
            sender_id = str(record.get("sender_id", "") or "").strip()
            if not sender_id or sender_id in names:
                continue
            if sender_id == config.self_platform_id:
                continue
            try:
                identity = self._identity_store.get_identity(sender_id)
                if identity and identity.get("name"):
                    names[sender_id] = str(identity["name"])
            except Exception:
                pass
        return names

    def _resolve_image_descriptions(
        self,
        records: list[dict[str, Any]],
        config: MessageFormatConfig,
    ) -> dict[str, str]:
        if self._media_service is None:
            return {}
        descriptions: dict[str, str] = {}
        for record in records:
            try:
                parts = parse_message_parts(record, self_platform_id=config.self_platform_id)
            except Exception:
                parts = []
            for part in parts:
                if part.kind != "image" or part.image is None:
                    continue
                raw_hash = str(part.image.raw_hash or "").strip()
                if raw_hash:
                    _collect_media_semantic(raw_hash, descriptions, self._media_service)

            content_json = str(record.get("content_json", "") or "").strip()
            if not content_json:
                continue
            try:
                payload = json.loads(content_json)
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(payload, list):
                continue
            _collect_image_hashes(payload, descriptions, self._media_service)
        return descriptions


def _collect_image_hashes(
    payload: list[Any],
    descriptions: dict[str, str],
    media_service: Any,
) -> None:
    for item in payload:
        if not isinstance(item, dict):
            continue
        attrs = item.get("attrs") if isinstance(item.get("attrs"), dict) else {}
        if item.get("type") == "img" and attrs.get("src"):
            src = str(attrs.get("src", "") or "").strip()
            if src:
                _collect_media_semantic(src, descriptions, media_service)
        children = item.get("children")
        if isinstance(children, list):
            _collect_image_hashes(children, descriptions, media_service)


def _collect_media_semantic(
    raw_hash: str,
    descriptions: dict[str, str],
    media_service: Any,
) -> None:
    if not raw_hash or raw_hash in descriptions:
        return
    try:
        semantics = media_service.get_media_semantic(raw_hash)
        if semantics and semantics.get("digest"):
            descriptions[raw_hash] = str(semantics["digest"])
    except Exception:
        pass


__all__ = [
    "EmojiMode",
    "FormatResult",
    "FormattedMessage",
    "ImageMode",
    "MessageFormatConfig",
    "MessageFormatterService",
    "PackMode",
    "format_messages",
]
