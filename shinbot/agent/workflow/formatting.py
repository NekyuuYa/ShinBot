"""Formatting helpers for conversation workflow batches."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from shinbot.agent.attention.repository import AttentionRepository
    from shinbot.agent.media import MediaService

# ── CJK-aware tokenization for cross-talk detection ──────────────────

_CJK_RANGES = (
    "\u4e00-\u9fff"    # CJK Unified Ideographs
    "\u3400-\u4dbf"    # CJK Unified Ideographs Extension A
    "\uf900-\ufaff"    # CJK Compatibility Ideographs
    "\U00020000-\U0002a6df"  # Extension B
    "\U0002a700-\U0002b73f"  # Extension C
)
_CJK_PATTERN = re.compile(f"[{_CJK_RANGES}]")


def _tokenize(text: str) -> set[str]:
    """Extract keywords from text, supporting both CJK and space-delimited languages."""

    tokens: set[str] = set()
    for word in text.split():
        normalized = word.lower().strip()
        if len(normalized) >= 2:
            tokens.add(normalized)

    cjk_chars = _CJK_PATTERN.findall(text)
    for index in range(len(cjk_chars) - 1):
        tokens.add(cjk_chars[index] + cjk_chars[index + 1])

    return tokens


def crosstalk_detect(batch: list[dict[str, Any]]) -> int:
    """Estimate the number of concurrent topic threads in a message batch."""

    if len(batch) <= 2:
        return 1

    sender_keywords: dict[str, set[str]] = {}
    for msg in batch:
        sender = str(msg.get("sender_id", ""))
        text = str(msg.get("raw_text", ""))
        sender_keywords.setdefault(sender, set()).update(_tokenize(text))

    if len(sender_keywords) <= 1:
        return 1

    senders = list(sender_keywords.keys())
    low_overlap_pairs = 0
    total_pairs = 0
    for i in range(len(senders)):
        for j in range(i + 1, len(senders)):
            a = sender_keywords[senders[i]]
            b = sender_keywords[senders[j]]
            if not a or not b:
                continue
            overlap = len(a & b) / min(len(a), len(b))
            total_pairs += 1
            if overlap < 0.15:
                low_overlap_pairs += 1

    if total_pairs == 0:
        return 1
    if low_overlap_pairs / total_pairs > 0.5:
        return min(len(sender_keywords), 3)
    return 1


def format_batch_context(
    batch: list[dict[str, Any]],
    *,
    session_id: str,
    attention_repo: AttentionRepository,
    media_service: MediaService | None = None,
) -> str:
    """Render the primary unread batch into workflow-facing text context."""

    lines: list[str] = []
    state = attention_repo.get_attention(session_id)
    prev_summary = ""
    if state is not None:
        prev_summary = str(state.metadata.get("internal_summary", "") or "")
    if prev_summary:
        lines.append(f"[上轮观察摘要：{prev_summary}]")
        lines.append("")
        attention_repo.clear_metadata_key(session_id, "internal_summary")

    lines.append(f"[以下是会话中 {len(batch)} 条未消费消息]")
    if batch_contains_media(batch, media_service):
        lines.append(
            "[提示：若需重新识别某条消息中的原图，请调用 media.inspect_original，"
            "并优先传入该消息行里的 message_log_id。]"
        )
    for msg in batch:
        lines.append(format_message_line(msg, media_service))
    return "\n".join(lines)


def format_incremental_messages(
    msgs: list[dict[str, Any]],
    *,
    media_service: MediaService | None = None,
) -> str:
    """Render incremental messages that arrived during tool execution."""

    lines = [
        f"[补充上下文：在你处理上一步期间，会话中新增了 {len(msgs)} 条消息。"
        "请结合这些新消息重新评估是否需要回复以及回复内容。]"
    ]
    if batch_contains_media(msgs, media_service):
        lines.append(
            "[提示：若需重新识别某条消息中的原图，请调用 media.inspect_original，"
            "并优先传入该消息行里的 message_log_id。]"
        )
    for msg in msgs:
        lines.append(format_message_line(msg, media_service))
    return "\n".join(lines)


def batch_contains_media(
    msgs: list[dict[str, Any]],
    media_service: MediaService | None = None,
) -> bool:
    if media_service is None:
        return False
    return any(media_service.summarize_message_media(msg) for msg in msgs)


def format_message_line(
    msg: dict[str, Any],
    media_service: MediaService | None = None,
) -> str:
    sender_name = str(msg.get("sender_name", "") or msg.get("sender_id", "unknown"))
    text = str(msg.get("raw_text", "") or "").strip() or "[无文本]"
    mentioned = " (@bot)" if msg.get("is_mentioned") else ""
    media_suffix = ""
    media_ref_suffix = ""
    if media_service is not None:
        media_notes = media_service.summarize_message_media(msg)
        if media_notes:
            media_suffix = " " + " ".join(media_notes)
            media_ref_suffix = format_media_reference(msg)
    return f"{sender_name}{mentioned}: {text}{media_suffix}{media_ref_suffix}"


def format_media_reference(msg: dict[str, Any]) -> str:
    refs: list[str] = []
    msg_id = msg.get("id")
    if isinstance(msg_id, int):
        refs.append(f"message_log_id={msg_id}")
    platform_msg_id = str(msg.get("platform_msg_id", "") or "").strip()
    if platform_msg_id:
        refs.append(f"platform_msg_id={platform_msg_id}")
    if not refs:
        return ""
    return " [媒体引用: " + " ".join(refs) + "]"
