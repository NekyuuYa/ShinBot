"""Core message rendering engine.

Converts raw message records into LLM-consumable text/content blocks.
Reuses ``parse_message_parts`` for AST normalization but provides a
stateless, config-driven formatting surface decoupled from session state.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from shinbot.agent.services.context.builders.message_parts import (
    NormalizedMessagePart,
    parse_message_parts,
)
from shinbot.agent.services.message_formatter.models import (
    EmojiMode,
    FormatResult,
    FormattedMessage,
    ImageMode,
    MessageFormatConfig,
    PackMode,
)


def format_messages(
    records: list[dict[str, Any]],
    config: MessageFormatConfig,
    *,
    display_names: dict[str, str] | None = None,
    image_descriptions: dict[str, str] | None = None,
) -> FormatResult:
    """Format a list of message records into LLM-consumable output.

    Args:
        records: Message log records (dict with ``raw_text``, ``content_json``, etc.).
        config: Formatting configuration.
        display_names: Pre-resolved sender_id → display name map.
        image_descriptions: Pre-resolved image hash → description map.

    Returns:
        A :class:`FormatResult` with individual messages and optional packed output.
    """
    display_names = display_names or {}
    image_descriptions = image_descriptions or {}

    formatted: list[FormattedMessage] = []
    for record in records:
        msg = _format_single_record(
            record,
            config=config,
            display_names=display_names,
            image_descriptions=image_descriptions,
        )
        if msg is not None:
            formatted.append(msg)

    if config.pack_mode == PackMode.PACK:
        packed_text = _pack_messages(formatted, config=config)
        packed_blocks = [{"type": "text", "text": packed_text}] if packed_text else []
        return FormatResult(
            messages=formatted,
            packed_text=packed_text,
            content_blocks=packed_blocks,
            message_count=len(formatted),
        )

    return FormatResult(
        messages=formatted,
        message_count=len(formatted),
    )


def _format_single_record(
    record: dict[str, Any],
    *,
    config: MessageFormatConfig,
    display_names: dict[str, str],
    image_descriptions: dict[str, str],
) -> FormattedMessage | None:
    sender_id = str(record.get("sender_id", "") or "").strip()
    sender_label = _resolve_sender_label(record, display_names, config.self_platform_id)
    created_at_ms = _coerce_timestamp_ms(record.get("created_at"))
    record_id = record.get("id") if isinstance(record.get("id"), int) else None

    parts = parse_message_parts(record, self_platform_id=config.self_platform_id)
    if not parts:
        raw_text = str(record.get("raw_text", "") or "").strip()
        if not raw_text:
            return None
        parts = [NormalizedMessagePart(kind="text", text=raw_text)]

    text = _render_parts(
        parts,
        config=config,
        image_descriptions=image_descriptions,
        display_names=display_names,
        sender_id=sender_id,
        sender_label=sender_label,
    )
    if not text.strip():
        return None

    return FormattedMessage(
        sender_id=sender_id,
        sender_label=sender_label,
        text=text,
        created_at_ms=created_at_ms,
        record_id=record_id,
    )


def _render_parts(
    parts: list[NormalizedMessagePart],
    *,
    config: MessageFormatConfig,
    image_descriptions: dict[str, str],
    display_names: dict[str, str],
    sender_id: str = "",
    sender_label: str = "",
) -> str:
    fragments: list[str] = []
    for part in parts:
        if part.kind == "text":
            fragments.append(part.text)
            continue

        if part.kind == "mention":
            fragments.append(
                _format_mention(
                    part,
                    config.self_platform_id,
                    display_names=display_names,
                )
            )
            continue

        if part.kind == "quote":
            fragments.append(_format_quote(part))
            continue

        if part.kind == "poke":
            fragments.append(
                _format_poke(
                    part,
                    config.self_platform_id,
                    display_names=display_names,
                    sender_id=sender_id,
                    sender_label=sender_label,
                )
            )
            continue

        if part.kind == "image" and part.image is not None:
            fragments.append(
                _format_image(
                    part.image,
                    config=config,
                    image_descriptions=image_descriptions,
                )
            )
            continue

    return "".join(fragments)


def _format_image(
    image: Any,
    *,
    config: MessageFormatConfig,
    image_descriptions: dict[str, str],
) -> str:
    is_emoji = image.is_custom_emoji
    raw_hash = image.raw_hash
    description = _image_description(image, image_descriptions)

    if is_emoji:
        return _format_emoji(image, config=config, image_descriptions=image_descriptions)

    if config.image_mode == ImageMode.DESCRIPTION:
        if description:
            return f"[图片: {description}]"
        return "[图片]"

    if config.image_mode == ImageMode.THUMBNAIL:
        if description:
            return f"[图片: {description}]"
        return f"[图片缩略图:{raw_hash[:8]}]" if raw_hash else "[图片]"

    return f"[图片:{raw_hash[:8]}]" if raw_hash else "[图片]"


def _format_emoji(
    image: Any,
    *,
    config: MessageFormatConfig,
    image_descriptions: dict[str, str],
) -> str:
    raw_hash = image.raw_hash
    description = _image_description(image, image_descriptions)

    if config.emoji_mode == EmojiMode.SEMANTIC:
        if description:
            return f"[表情: {description}]"
        return "[表情]"

    if config.emoji_mode == EmojiMode.THUMBNAIL:
        if description:
            return f"[表情: {description}]"
        return f"[表情缩略图:{raw_hash[:8]}]" if raw_hash else "[表情]"

    return f"[表情:{raw_hash[:8]}]" if raw_hash else "[表情]"


def _image_description(image: Any, image_descriptions: dict[str, str]) -> str:
    for key in (image.raw_hash, image.source_path):
        normalized = str(key or "").strip()
        if normalized and image_descriptions.get(normalized):
            return image_descriptions[normalized]
    return ""


def _format_mention(
    part: NormalizedMessagePart,
    self_platform_id: str,
    *,
    display_names: dict[str, str],
) -> str:
    target_id = part.platform_id.strip()
    label = _format_identity_label(
        target_id,
        part.display_name or display_names.get(target_id, ""),
        self_platform_id=self_platform_id,
    )
    return f"[@ {label}]"


def _format_quote(part: NormalizedMessagePart) -> str:
    quote_id = part.quote_id.strip()
    if quote_id:
        return f"[引用消息 id:{quote_id}]"
    return "[引用消息]"


def _format_poke(
    part: NormalizedMessagePart,
    self_platform_id: str,
    *,
    display_names: dict[str, str],
    sender_id: str = "",
    sender_label: str = "",
) -> str:
    """Render a poke (戳一戳) part into a text placeholder.

    Resolution:
    - Target is the bot → ``[戳一戳: {sender}戳了你一下]``
    - Target is another user → ``[戳一戳: {sender}戳了 {target} 一下]``
    - No target info → ``[戳一戳]``

    When ``sender_label`` is empty (e.g. packed mode strips it), falls
    back to ``part.sender_id`` (platform ID from element attrs), then
    omits the sender entirely if neither is available.

    Args:
        part: A :class:`NormalizedMessagePart` with ``kind="poke"``.
        self_platform_id: The bot's own platform ID for self-detection.
        sender_label: Display name of the poke sender (from record level).

    Returns:
        Text placeholder string describing the poke action.
    """
    target_id = part.platform_id.strip()
    resolved_sender_id = (part.sender_id or sender_id).strip()
    resolved_sender_name = (
        sender_label
        if resolved_sender_id and resolved_sender_id == sender_id
        else display_names.get(resolved_sender_id, "")
    )
    sender = _format_identity_label(
        resolved_sender_id,
        resolved_sender_name,
        self_platform_id=self_platform_id,
        fallback="某人",
    )

    if target_id and self_platform_id and target_id == self_platform_id:
        target = _format_identity_label(
            target_id,
            part.display_name or display_names.get(target_id, ""),
            self_platform_id=self_platform_id,
        )
        return f"[戳一戳: {sender} -> {target}]"

    if target_id:
        target = _format_identity_label(
            target_id,
            part.display_name or display_names.get(target_id, ""),
            self_platform_id=self_platform_id,
        )
        return f"[戳一戳: {sender} -> {target}]"

    return "[戳一戳]"


def _format_identity_label(
    user_id: str,
    display_name: str = "",
    *,
    self_platform_id: str = "",
    fallback: str = "某人",
) -> str:
    normalized_id = str(user_id or "").strip()
    normalized_name = str(display_name or "").strip()
    if normalized_id and self_platform_id and normalized_id == self_platform_id:
        normalized_name = "你"
    if normalized_name and normalized_id:
        return f"{normalized_name}/{normalized_id}"
    if normalized_name:
        return normalized_name
    if normalized_id:
        return normalized_id
    return fallback


def _pack_messages(
    messages: list[FormattedMessage],
    *,
    config: MessageFormatConfig,
) -> str:
    if not messages:
        return ""

    lines: list[str] = []
    previous_created_at_ms = 0

    for msg in messages:
        if config.timestamp_mode == "sparse" and previous_created_at_ms:
            gap_ms = msg.created_at_ms - previous_created_at_ms
            if gap_ms > 3 * 60 * 1000:
                lines.append(_format_timestamp(msg.created_at_ms))

        id_prefix = (
            f"[msg_log_id:{msg.record_id}] "
            if config.inject_record_id and msg.record_id is not None
            else ""
        )
        if config.inject_sender:
            lines.append(f"{id_prefix}{msg.sender_label}: {msg.text}")
        else:
            lines.append(f"{id_prefix}{msg.text}")

        previous_created_at_ms = msg.created_at_ms

    return "\n".join(lines)


def _resolve_sender_label(
    record: dict[str, Any],
    display_names: dict[str, str],
    self_platform_id: str,
) -> str:
    role = str(record.get("role", "") or "").strip()
    if role == "assistant":
        return "你"

    sender_id = str(record.get("sender_id", "") or "").strip()
    if sender_id and self_platform_id and sender_id == self_platform_id:
        return "你"

    if sender_id and sender_id in display_names:
        return display_names[sender_id]

    sender_name = str(record.get("sender_name", "") or "").strip()
    return sender_name or sender_id or "unknown"


def _format_timestamp(created_at_ms: int) -> str:
    if created_at_ms <= 0:
        return "时间未知"
    return datetime.fromtimestamp(created_at_ms / 1000).strftime("%m-%d %H:%M")


def _coerce_timestamp_ms(value: Any) -> int:
    if value is None:
        return 0
    try:
        raw = float(value)
    except (TypeError, ValueError):
        return 0
    return int(raw if raw > 10_000_000_000 else raw * 1000)
