"""Instruction-stage content block builder for unread messages."""

from __future__ import annotations

import base64
import mimetypes
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from shinbot.agent.context.builders.image_summary import ContextImageRegistry
from shinbot.agent.context.builders.message_parts import NormalizedMessagePart, parse_message_parts
from shinbot.agent.context.projectors.projection import ContextProjectionState
from shinbot.agent.context.state.alias_table import SessionAliasTable

if TYPE_CHECKING:
    from shinbot.agent.media import MediaService

StickerSummaryResolver = Callable[[dict[str, Any], str], str]


@dataclass(slots=True)
class InstructionStageBuildConfig:
    include_summary_header: bool = True


class InstructionStageBuilder:
    """Render unread messages into one final content array for the instruction stage."""

    def __init__(
        self,
        *,
        media_service: MediaService | None = None,
        image_registry: ContextImageRegistry | None = None,
        config: InstructionStageBuildConfig | None = None,
        sticker_summary_resolver: StickerSummaryResolver | None = None,
    ) -> None:
        self._media_service = media_service
        self._image_registry = image_registry or ContextImageRegistry()
        self._config = config or InstructionStageBuildConfig()
        self._sticker_summary_resolver = sticker_summary_resolver

    @property
    def image_registry(self) -> ContextImageRegistry:
        return self._image_registry

    def build_content_blocks(
        self,
        unread_records: list[dict[str, Any]],
        *,
        alias_table: SessionAliasTable,
        projection_state: ContextProjectionState,
        previous_summary: str = "",
        self_platform_id: str = "",
        now_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        content_blocks: list[dict[str, Any]] = []
        if self._config.include_summary_header:
            header_lines: list[str] = []
            summary_text = previous_summary.strip()
            if summary_text:
                header_lines.append(f"[上轮观察摘要：{summary_text}]")
            header_lines.append(f"[以下是会话中 {len(unread_records)} 条未消费消息]")
            content_blocks.append({"type": "text", "text": "\n".join(header_lines)})

        for record in unread_records:
            content_blocks.extend(
                self._render_record(
                    record,
                    alias_table=alias_table,
                    projection_state=projection_state,
                    self_platform_id=self_platform_id,
                    now_ms=now_ms,
                )
            )
        return content_blocks

    def _render_record(
        self,
        record: dict[str, Any],
        *,
        alias_table: SessionAliasTable,
        projection_state: ContextProjectionState,
        self_platform_id: str,
        now_ms: int | None,
    ) -> list[dict[str, Any]]:
        sender_id = str(record.get("sender_id", "") or "").strip()
        sender_label = _resolve_sender_label(record, alias_table, self_platform_id)
        relative_time = _format_relative_timestamp(record.get("created_at"), now_ms=now_ms)
        parts = parse_message_parts(record, self_platform_id=self_platform_id)
        if not parts:
            text = str(record.get("raw_text", "") or "").strip() or "[无文本]"
            message_id = projection_state.assign_message_id(record)
            return [
                {
                    "type": "text",
                    "text": f"[{relative_time}] [msgid: {message_id}]{sender_label}: {text}",
                }
            ]

        if self._is_poke_only(parts):
            return [
                {
                    "type": "text",
                    "text": self._render_poke_only(
                        relative_time=relative_time,
                        sender_id=sender_id,
                        sender_label=sender_label,
                        part=parts[0],
                        alias_table=alias_table,
                        self_platform_id=self_platform_id,
                    ),
                }
            ]

        message_id = projection_state.assign_message_id(record)
        header = f"[{relative_time}] [msgid: {message_id}]{sender_label}: "
        blocks: list[dict[str, Any]] = []
        inline_fragments: list[str] = []
        requires_closure = False

        def append_inline(fragment: str) -> None:
            if fragment:
                inline_fragments.append(fragment)

        def flush_inline() -> None:
            if not inline_fragments:
                return
            text = "".join(inline_fragments)
            blocks.append({"type": "text", "text": header + text if not blocks else text})
            inline_fragments.clear()

        for part in parts:
            if part.kind == "text":
                append_inline(part.text or "")
                continue

            if part.kind == "mention":
                append_inline(_format_mention(part, alias_table, self_platform_id))
                continue

            if part.kind == "quote":
                append_inline(_format_quote(part))
                continue

            if part.kind == "poke":
                continue

            if part.kind == "image" and part.image is not None:
                summary_text = ""
                image_kind = "custom_emoji" if part.image.is_custom_emoji else "image"
                if self._media_service is not None and part.image.raw_hash:
                    semantics = self._media_service.get_media_semantic(part.image.raw_hash)
                    if semantics is not None:
                        summary_text = str(semantics.get("digest") or "").strip()
                        image_kind = str(semantics.get("kind") or image_kind).strip() or image_kind
                reference = projection_state.resolve_image_reference(
                    raw_hash=part.image.raw_hash,
                    strict_dhash=part.image.strict_dhash,
                    summary_text=summary_text,
                    kind=image_kind,
                    is_custom_emoji=part.image.is_custom_emoji,
                    metadata={"source_path": part.image.source_path},
                )

                if part.image.is_custom_emoji:
                    sticker_text = self._resolve_sticker_text(
                        record,
                        reference.image_id,
                        fallback_summary=reference.summary_text,
                    )
                    append_inline(f"$附图片[id: {reference.image_id}] {sticker_text}".strip())
                    continue

                append_inline(f"$附图片[id: {reference.image_id}]")
                flush_inline()
                image_block = _build_image_block(part.image.source_path)
                if image_block is not None:
                    blocks.append(image_block)
                else:
                    blocks.append(
                        {
                            "type": "text",
                            "text": f"[图片缺失 id:{reference.image_id}]",
                        }
                    )
                requires_closure = True

        flush_inline()
        if not blocks:
            blocks.append({"type": "text", "text": header + "[无文本]"})

        if requires_closure:
            if blocks[-1].get("type") == "text":
                blocks[-1]["text"] = str(blocks[-1].get("text", "") or "") + "$该消息结束"
            else:
                blocks.append({"type": "text", "text": "$该消息结束"})
        return blocks

    @staticmethod
    def _is_poke_only(parts: list[NormalizedMessagePart]) -> bool:
        meaningful_parts = [part for part in parts if part.kind != "text" or part.text.strip()]
        return len(meaningful_parts) == 1 and meaningful_parts[0].kind == "poke"

    def _resolve_sticker_text(
        self,
        record: dict[str, Any],
        image_id: str,
        *,
        fallback_summary: str = "",
    ) -> str:
        if fallback_summary.strip():
            return fallback_summary.strip()
        if self._sticker_summary_resolver is not None:
            resolved = self._sticker_summary_resolver(record, image_id).strip()
            if resolved:
                return resolved
        return f"[表情转述待补充 id:{image_id}]"

    @classmethod
    def _render_poke_only(
        cls,
        *,
        relative_time: str,
        sender_id: str,
        sender_label: str,
        part: NormalizedMessagePart,
        alias_table: SessionAliasTable,
        self_platform_id: str,
    ) -> str:
        return f"[{relative_time}] {cls._format_poke_fragment(sender_id=sender_id, sender_label=sender_label, part=part, alias_table=alias_table, self_platform_id=self_platform_id)}"

    @staticmethod
    def _format_poke_fragment(
        *,
        sender_id: str,
        sender_label: str,
        part: NormalizedMessagePart,
        alias_table: SessionAliasTable,
        self_platform_id: str,
    ) -> str:
        actor = _format_alias_with_platform(sender_label, sender_id)
        target_id = part.platform_id.strip()
        if target_id and self_platform_id and target_id == self_platform_id:
            target = "你"
        else:
            target_alias = alias_table.format_sender(target_id) if target_id else ""
            target = _format_alias_with_platform(target_alias, target_id) or "某人"
        return f"[戳一戳: {actor} 戳了 {target} 一下]"


def _build_image_block(source_path: str) -> dict[str, Any] | None:
    path = Path(source_path).expanduser()
    if not path.is_file():
        return None
    try:
        data = path.read_bytes()
    except OSError:
        return None
    mime_type, _ = mimetypes.guess_type(path.name)
    encoded = base64.b64encode(data).decode("ascii")
    return {
        "type": "image_url",
        "image_url": {"url": f"data:{mime_type or 'image/jpeg'};base64,{encoded}"},
    }


def _format_relative_timestamp(raw_created_at: Any, *, now_ms: int | None) -> str:
    created_at_ms = _coerce_timestamp_ms(raw_created_at)
    if created_at_ms <= 0:
        return "未知"
    current_ms = now_ms if now_ms is not None else _current_time_ms()
    elapsed_seconds = max(0, int((current_ms - created_at_ms) / 1000))
    if elapsed_seconds < 5:
        return "刚刚"
    if elapsed_seconds < 60:
        return f"{elapsed_seconds}秒前"
    elapsed_minutes = elapsed_seconds // 60
    if elapsed_minutes < 60:
        return f"{elapsed_minutes}分钟前"
    elapsed_hours = elapsed_minutes // 60
    if elapsed_hours < 48:
        return f"{elapsed_hours}小时前"
    elapsed_days = elapsed_hours // 24
    return f"{elapsed_days}天前"


def _coerce_timestamp_ms(value: Any) -> int:
    if value is None:
        return 0
    try:
        raw = float(value)
    except (TypeError, ValueError):
        return 0
    return int(raw if raw > 10_000_000_000 else raw * 1000)


def _current_time_ms() -> int:
    from time import time

    return int(time() * 1000)


def _format_alias_with_platform(alias: str, platform_id: str) -> str:
    left = alias.strip()
    right = platform_id.strip()
    if left and right and left != right:
        return f"{left}/{right}"
    return left or right


def _is_self_record(record: dict[str, Any], self_platform_id: str) -> bool:
    role = str(record.get("role", "") or "").strip()
    if role == "assistant":
        return True
    sender_id = str(record.get("sender_id", "") or "").strip()
    return bool(sender_id and self_platform_id and sender_id == self_platform_id)


def _resolve_sender_label(
    record: dict[str, Any],
    alias_table: SessionAliasTable,
    self_platform_id: str,
) -> str:
    sender_id = str(record.get("sender_id", "") or "").strip()
    sender_name = str(record.get("sender_name", "") or "").strip()
    if _is_self_record(record, self_platform_id):
        return "你"
    if sender_id:
        return alias_table.format_sender(sender_id)
    return sender_name or "unknown"


def _format_mention(
    part: NormalizedMessagePart,
    alias_table: SessionAliasTable,
    self_platform_id: str,
) -> str:
    target_id = part.platform_id.strip()
    if target_id and self_platform_id and target_id == self_platform_id:
        return "[@ 你]"
    alias = alias_table.format_sender(target_id) if target_id else ""
    if alias and target_id and alias != target_id:
        return f"[@ {alias}/{target_id}]"
    if target_id:
        return f"[@ {target_id}]"
    if part.display_name:
        return f"[@ {part.display_name}]"
    return "[@ 某人]"


def _format_quote(part: NormalizedMessagePart) -> str:
    quote_id = part.quote_id.strip()
    if quote_id:
        return f"[引用消息 id:{quote_id}]"
    return "[引用消息]"
