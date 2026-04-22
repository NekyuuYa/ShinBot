"""Context-stage text packing and block chunking."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from shinbot.agent.context.alias_table import SessionAliasTable
from shinbot.agent.context.image_summary import ContextImageRegistry
from shinbot.agent.context.message_parts import NormalizedMessagePart, parse_message_parts
from shinbot.agent.context.state_store import ContextBlockState, ContextSessionState
from shinbot.agent.context.token_utils import estimate_text_tokens

if TYPE_CHECKING:
    from shinbot.agent.media import MediaService


@dataclass(slots=True)
class ContextStageBuildConfig:
    min_tokens: int = 300
    max_tokens: int = 1500
    timeout_ms: int = 10 * 60 * 1000
    gap_marker_ms: int = 3 * 60 * 1000


@dataclass(slots=True)
class ContextRenderedRow:
    sender_id: str
    sender_label: str
    text: str
    created_at_ms: int
    token_estimate: int
    message_id: str = ""
    record_id: int | None = None
    is_referenceable: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ContextRenderedRun:
    sender_id: str
    rows: list[ContextRenderedRow]
    token_estimate: int
    started_at_ms: int
    ended_at_ms: int


class ContextStageBuilder:
    """Render read-history messages into cache-friendly context message blocks."""

    def __init__(
        self,
        *,
        media_service: MediaService | None = None,
        image_registry: ContextImageRegistry | None = None,
        config: ContextStageBuildConfig | None = None,
    ) -> None:
        self._media_service = media_service
        self._image_registry = image_registry or ContextImageRegistry()
        self._config = config or ContextStageBuildConfig()

    def build_blocks(
        self,
        records: list[dict[str, Any]],
        *,
        alias_table: SessionAliasTable,
        session_state: ContextSessionState,
        self_platform_id: str = "",
        start_block_index: int = 0,
    ) -> list[ContextBlockState]:
        rows = [
            rendered
            for record in records
            if (rendered := self.render_record(
                record,
                alias_table=alias_table,
                session_state=session_state,
                self_platform_id=self_platform_id,
            ))
            is not None
        ]
        if not rows:
            return []

        runs = self._build_runs(rows)
        blocks: list[ContextBlockState] = []
        pending_runs: list[ContextRenderedRun] = []
        pending_tokens = 0
        previous_end_ms = 0

        for run in runs:
            if not pending_runs:
                pending_runs = [run]
                pending_tokens = run.token_estimate
                previous_end_ms = run.ended_at_ms
                continue

            gap_ms = max(0, run.started_at_ms - previous_end_ms)
            should_split_by_timeout = (
                pending_tokens >= self._config.min_tokens and gap_ms > self._config.timeout_ms
            )
            would_exceed_max = pending_tokens + run.token_estimate > self._config.max_tokens

            if should_split_by_timeout or would_exceed_max:
                blocks.append(
                    self._finalize_block(
                        pending_runs,
                        block_index=start_block_index + len(blocks),
                        alias_table=alias_table,
                    )
                )
                pending_runs = [run]
                pending_tokens = run.token_estimate
                previous_end_ms = run.ended_at_ms
                continue

            pending_runs.append(run)
            pending_tokens += run.token_estimate
            previous_end_ms = run.ended_at_ms

        if pending_runs:
            blocks.append(
                self._finalize_block(
                    pending_runs,
                    block_index=start_block_index + len(blocks),
                    alias_table=alias_table,
                )
            )

        for index, block in enumerate(blocks):
            block.sealed = index < len(blocks) - 1
        return blocks

    def build_prompt_messages(
        self,
        records: list[dict[str, Any]],
        *,
        alias_table: SessionAliasTable,
        session_state: ContextSessionState,
        self_platform_id: str = "",
    ) -> list[dict[str, Any]]:
        blocks = self.build_blocks(
            records,
            alias_table=alias_table,
            session_state=session_state,
            self_platform_id=self_platform_id,
        )
        session_state.blocks = blocks
        return [{"role": "user", "content": list(block.contents)} for block in blocks]

    def render_record(
        self,
        record: dict[str, Any],
        *,
        alias_table: SessionAliasTable,
        session_state: ContextSessionState,
        self_platform_id: str = "",
    ) -> ContextRenderedRow | None:
        sender_id = str(record.get("sender_id", "") or "").strip()
        sender_name = str(record.get("sender_name", "") or "").strip()
        sender_label = alias_table.format_sender(sender_id) if sender_id else (sender_name or "unknown")
        created_at_ms = _coerce_timestamp_ms(record.get("created_at"))
        parts = parse_message_parts(record, self_platform_id=self_platform_id)
        if not parts:
            raw_text = str(record.get("raw_text", "") or "").strip()
            if not raw_text:
                return None
            parts = [NormalizedMessagePart(kind="text", text=raw_text)]

        if self._is_poke_only(parts):
            special_text = self._render_poke_only(
                sender_id=sender_id,
                sender_label=sender_label,
                part=parts[0],
                alias_table=alias_table,
                self_platform_id=self_platform_id,
            )
            return ContextRenderedRow(
                sender_id=sender_id,
                sender_label=sender_label,
                text=special_text,
                created_at_ms=created_at_ms,
                token_estimate=estimate_text_tokens(special_text),
                message_id="",
                record_id=record.get("id") if isinstance(record.get("id"), int) else None,
                is_referenceable=False,
                metadata={
                    "referenced_platform_ids": _collect_referenced_platform_ids(sender_id, parts),
                },
            )

        rendered_body = self._render_parts_inline(
            record,
            parts=parts,
            alias_table=alias_table,
            session_state=session_state,
            self_platform_id=self_platform_id,
        ).strip() or "[无文本]"
        message_id = f"{session_state.message_ids.assign(_record_key(record)):04d}"
        line = f"[msgid: {message_id}]{sender_label}: {rendered_body}"
        return ContextRenderedRow(
            sender_id=sender_id,
            sender_label=sender_label,
            text=line,
            created_at_ms=created_at_ms,
            token_estimate=estimate_text_tokens(line),
            message_id=message_id,
            record_id=record.get("id") if isinstance(record.get("id"), int) else None,
            metadata={
                "referenced_platform_ids": _collect_referenced_platform_ids(sender_id, parts),
            },
        )

    def _build_runs(self, rows: list[ContextRenderedRow]) -> list[ContextRenderedRun]:
        runs: list[ContextRenderedRun] = []
        current_rows: list[ContextRenderedRow] = []
        current_sender_id = ""

        for row in rows:
            if not current_rows:
                current_rows = [row]
                current_sender_id = row.sender_id
                continue

            if row.sender_id == current_sender_id:
                current_rows.append(row)
                continue

            runs.append(_run_from_rows(current_rows))
            current_rows = [row]
            current_sender_id = row.sender_id

        if current_rows:
            runs.append(_run_from_rows(current_rows))
        return runs

    def _finalize_block(
        self,
        runs: list[ContextRenderedRun],
        *,
        block_index: int,
        alias_table: SessionAliasTable,
    ) -> ContextBlockState:
        flattened_rows = [row for run in runs for row in run.rows]
        content_blocks: list[dict[str, Any]] = []
        if flattened_rows:
            content_blocks.append(
                {"type": "text", "text": _format_absolute_timestamp(flattened_rows[0].created_at_ms)}
            )

        previous_created_at_ms = flattened_rows[0].created_at_ms if flattened_rows else 0
        for row in flattened_rows:
            if (
                content_blocks
                and previous_created_at_ms
                and row.created_at_ms - previous_created_at_ms > self._config.gap_marker_ms
            ):
                content_blocks.append(
                    {"type": "text", "text": _format_absolute_timestamp(row.created_at_ms)}
                )
            content_blocks.append({"type": "text", "text": row.text})
            previous_created_at_ms = row.created_at_ms

        token_estimate = sum(
            estimate_text_tokens(str(block.get("text", "") or "")) for block in content_blocks
        )
        return ContextBlockState(
            block_id=f"context-{block_index + 1:04d}",
            kind="context",
            token_estimate=token_estimate,
            sealed=False,
            contents=content_blocks,
            metadata={
                "message_count": len(flattened_rows),
                "record_ids": [row.record_id for row in flattened_rows if row.record_id is not None],
                "alias_entries": _build_block_alias_entries(flattened_rows, alias_table),
                "started_at_ms": flattened_rows[0].created_at_ms if flattened_rows else 0,
                "ended_at_ms": flattened_rows[-1].created_at_ms if flattened_rows else 0,
            },
        )

    def _render_parts_inline(
        self,
        record: dict[str, Any],
        *,
        parts: list[NormalizedMessagePart],
        alias_table: SessionAliasTable,
        session_state: ContextSessionState,
        self_platform_id: str,
    ) -> str:
        fragments: list[str] = []
        for part in parts:
            if part.kind == "text":
                fragments.append(part.text)
                continue

            if part.kind == "mention":
                fragments.append(self._format_mention(part, alias_table, self_platform_id))
                continue

            if part.kind == "poke":
                fragments.append(
                    self._render_poke_only(
                        sender_id=str(record.get("sender_id", "") or "").strip(),
                        sender_label=alias_table.format_sender(
                            str(record.get("sender_id", "") or "").strip()
                        ),
                        part=part,
                        alias_table=alias_table,
                        self_platform_id=self_platform_id,
                    )
                )
                continue

            if part.kind == "image" and part.image is not None:
                image_kind = "custom_emoji" if part.image.is_custom_emoji else "image"
                summary_text = ""
                if self._media_service is not None and part.image.raw_hash:
                    semantics = self._media_service.get_media_semantic(part.image.raw_hash)
                    if semantics is not None:
                        image_kind = str(semantics.get("kind") or image_kind).strip() or image_kind
                        summary_text = str(semantics.get("digest") or "").strip()
                reference = self._image_registry.get_or_create_reference(
                    session_state=session_state,
                    raw_hash=part.image.raw_hash,
                    strict_dhash=part.image.strict_dhash,
                    summary_text=summary_text,
                    kind=image_kind,
                    is_custom_emoji=part.image.is_custom_emoji,
                    metadata={"source_path": part.image.source_path},
                )
                label = "表情" if reference.is_custom_emoji or reference.kind in {"meme_image", "emoji_native"} else "图片"
                if reference.summary_text:
                    fragments.append(
                        f"[{label} id:{reference.image_id} 摘要:{reference.summary_text}]"
                    )
                else:
                    fragments.append(f"[{label} id:{reference.image_id}]")

        return "".join(fragments)

    @staticmethod
    def _is_poke_only(parts: list[NormalizedMessagePart]) -> bool:
        meaningful_parts = [part for part in parts if part.kind != "text" or part.text.strip()]
        return len(meaningful_parts) == 1 and meaningful_parts[0].kind == "poke"

    @staticmethod
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

    @staticmethod
    def _render_poke_only(
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


def _run_from_rows(rows: list[ContextRenderedRow]) -> ContextRenderedRun:
    return ContextRenderedRun(
        sender_id=rows[0].sender_id,
        rows=list(rows),
        token_estimate=sum(row.token_estimate for row in rows),
        started_at_ms=rows[0].created_at_ms,
        ended_at_ms=rows[-1].created_at_ms,
    )


def _collect_referenced_platform_ids(
    sender_id: str,
    parts: list[NormalizedMessagePart],
) -> list[str]:
    ordered_ids: list[str] = []
    seen: set[str] = set()

    normalized_sender_id = str(sender_id or "").strip()
    if normalized_sender_id:
        ordered_ids.append(normalized_sender_id)
        seen.add(normalized_sender_id)

    for part in parts:
        target_id = str(part.platform_id or "").strip()
        if not target_id or target_id in seen:
            continue
        ordered_ids.append(target_id)
        seen.add(target_id)

    return ordered_ids


def _build_block_alias_entries(
    rows: list[ContextRenderedRow],
    alias_table: SessionAliasTable,
) -> list[dict[str, str]]:
    ordered_ids: list[str] = []
    seen_ids: set[str] = set()
    for row in rows:
        referenced_ids = row.metadata.get("referenced_platform_ids", [])
        if not isinstance(referenced_ids, list):
            continue
        for raw_platform_id in referenced_ids:
            platform_id = str(raw_platform_id or "").strip()
            if not platform_id or platform_id in seen_ids:
                continue
            ordered_ids.append(platform_id)
            seen_ids.add(platform_id)

    entries: list[dict[str, str]] = []
    for platform_id in ordered_ids:
        entry = alias_table.resolve(platform_id)
        if entry is None:
            entries.append(
                {
                    "alias": platform_id,
                    "platform_id": platform_id,
                    "display_name": platform_id,
                }
            )
            continue
        entries.append(
            {
                "alias": entry.alias or entry.platform_id,
                "platform_id": entry.platform_id,
                "display_name": entry.display_name or entry.platform_id,
            }
        )
    return entries


def _coerce_timestamp_ms(value: Any) -> int:
    if value is None:
        return 0
    try:
        raw = float(value)
    except (TypeError, ValueError):
        return 0
    return int(raw if raw > 10_000_000_000 else raw * 1000)


def _format_absolute_timestamp(timestamp_ms: int) -> str:
    if timestamp_ms <= 0:
        return "时间未知"
    return datetime.fromtimestamp(timestamp_ms / 1000).strftime("%m-%d %H:%M")


def _record_key(record: dict[str, Any]) -> str:
    record_id = record.get("id")
    if isinstance(record_id, int):
        return f"record:{record_id}"
    platform_msg_id = str(record.get("platform_msg_id", "") or "").strip()
    if platform_msg_id:
        return f"platform:{platform_msg_id}"
    sender_id = str(record.get("sender_id", "") or "").strip()
    created_at = str(record.get("created_at", "") or "").strip()
    raw_text = str(record.get("raw_text", "") or "").strip()
    digest = hashlib.sha1(f"{sender_id}|{created_at}|{raw_text}".encode()).hexdigest()
    return f"synthetic:{digest}"


def _format_alias_with_platform(alias: str, platform_id: str) -> str:
    left = alias.strip()
    right = platform_id.strip()
    if left and right and left != right:
        return f"{left}/{right}"
    return left or right
