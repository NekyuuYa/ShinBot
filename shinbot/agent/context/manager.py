"""Active context pool and standardized retrieval manager."""

from __future__ import annotations

import re
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from shinbot.agent.context.alias_table import SessionAliasTable
from shinbot.agent.context.context_stage_builder import ContextStageBuilder
from shinbot.agent.context.eviction import (
    ContextEvictionConfig,
    evict_context_blocks,
    extract_total_tokens,
    select_blocks_for_eviction,
)
from shinbot.agent.context.instruction_stage_builder import InstructionStageBuilder
from shinbot.agent.context.state_store import (
    ContextBlockState,
    ContextSessionState,
    ContextStateStore,
)
from shinbot.agent.context.token_utils import estimate_role_content_tokens, estimate_text_tokens

if TYPE_CHECKING:
    from shinbot.agent.identity import IdentityStore
    from shinbot.agent.media import MediaService
    from shinbot.persistence.records import MessageLogRecord
    from shinbot.persistence.repos import ContextProvider


def estimate_context_tokens(turns: list[dict[str, Any]], summary: str = "") -> int:
    """Estimate token usage using the shared context packing heuristic."""
    text_parts = [summary] if summary else []
    text_parts.extend(
        f"{turn['role']}: {turn['content']}" if turn.get("role") else turn.get("content", "")
        for turn in turns
    )
    text = "\n".join(part for part in text_parts if part).strip()
    return estimate_text_tokens(text)


def _record_to_turn(item: dict[str, Any]) -> dict[str, Any] | None:
    """Convert a raw message record dict into a pre-processed turn dict.

    Returns None if the item has no usable content.
    """
    role = str(item.get("role", "") or "").strip()
    content = str(item.get("content") or item.get("raw_text") or "").strip()
    if not content:
        return None
    turn: dict[str, Any] = {"role": role, "content": content}
    sender_id = str(item.get("sender_id", "") or "").strip()
    if sender_id:
        turn["sender_id"] = sender_id
    sender_name = str(item.get("sender_name", "") or "").strip()
    if sender_name:
        turn["sender_name"] = sender_name
    platform = str(item.get("platform", "") or "").strip()
    if platform:
        turn["platform"] = platform
    # Preserve original record id for deduplication.
    record_id = item.get("id")
    if record_id is not None:
        turn["_record_id"] = record_id
    created_at = item.get("created_at")
    if created_at is not None:
        turn["_created_at"] = created_at
    if "is_read" in item:
        turn["_is_read"] = bool(item.get("is_read"))
    return turn


@dataclass(slots=True)
class ActiveContextPool:
    """Hot in-memory context state for a single active session.

    Stores pre-processed turn dicts and maintains an incremental token
    estimate so that callers never pay O(n) on append or trim.
    """

    session_id: str
    max_messages: int = 50
    summary: str = ""
    messages: deque[dict[str, Any]] = field(default_factory=deque)
    token_estimate: int = 0
    _per_turn_tokens: deque[int] = field(default_factory=deque)

    def __post_init__(self) -> None:
        # Ensure deques have maxlen set from the start.
        if not self.messages.maxlen:
            self.messages = deque(self.messages, maxlen=self.max_messages)
            self._per_turn_tokens = deque(self._per_turn_tokens, maxlen=self.max_messages)

    def load(self, items: list[dict[str, Any]]) -> None:
        """Load from provider results.  Items are expected in chronological
        order (oldest first).  Only the last ``max_messages`` are retained."""
        turns: list[dict[str, Any]] = []
        for item in items:
            turn = _record_to_turn(item)
            if turn is not None:
                turns.append(turn)
        tail = turns[-self.max_messages :] if len(turns) > self.max_messages else turns
        self.messages = deque(tail, maxlen=self.max_messages)
        self._per_turn_tokens = deque(
            (estimate_role_content_tokens(t.get("role", ""), t["content"]) for t in tail),
            maxlen=self.max_messages,
        )
        self.token_estimate = sum(self._per_turn_tokens)

    def append(self, item: dict[str, Any]) -> None:
        turn = _record_to_turn(item)
        if turn is None:
            return

        # Deduplication against the tail.
        if self.messages:
            tail = self.messages[-1]
            record_id = turn.get("_record_id")
            if record_id is not None and tail.get("_record_id") == record_id:
                return
            if (
                record_id is None
                and tail.get("_record_id") is None
                and tail.get("role") == turn.get("role")
                and tail.get("content") == turn.get("content")
                and tail.get("_created_at") == turn.get("_created_at")
            ):
                return

        tokens = estimate_role_content_tokens(turn.get("role", ""), turn["content"])

        # If deque is at max capacity, the leftmost element is auto-evicted.
        if self.messages.maxlen and len(self.messages) >= self.messages.maxlen:
            self.token_estimate -= self._per_turn_tokens[0]
            # deque auto-pops from left; mirror in _per_turn_tokens
            self._per_turn_tokens.popleft()

        self.messages.append(turn)
        self._per_turn_tokens.append(tokens)
        self.token_estimate += tokens

    def export_turns(self, *, read_only: bool = True) -> list[dict[str, Any]]:
        """Return turn dicts suitable for prompt assembly.

        Internal bookkeeping keys (``_record_id``, ``_created_at``) are
        stripped so the output is a clean list of turn dicts.
        """
        turns: list[dict[str, Any]] = []
        for item in self.messages:
            if read_only and not bool(item.get("_is_read", True)):
                continue
            turn = {k: v for k, v in item.items() if not k.startswith("_")}
            turns.append(turn)
        return turns

    def mark_read_until(self, msg_id: int) -> None:
        """Mark buffered message turns as readable up to the consumed cursor."""
        for item in self.messages:
            record_id = item.get("_record_id")
            if isinstance(record_id, int) and record_id <= msg_id:
                item["_is_read"] = True


class ContextManager:
    """Observer-backed session context manager with hot pools."""

    def __init__(
        self,
        provider: ContextProvider,
        *,
        data_dir: Path | str | None = "data",
        preload_limit: int = 50,
        max_pool_messages: int = 200,
        identity_store: IdentityStore | None = None,
        media_service: MediaService | None = None,
    ) -> None:
        self._provider = provider
        self._preload_limit = preload_limit
        self._max_pool_messages = max_pool_messages
        self._identity_store = identity_store
        self._media_service = media_service
        self._state_store = ContextStateStore(data_dir=data_dir)
        self._session_states: dict[str, ContextSessionState] = {}
        self._context_builder = ContextStageBuilder(media_service=self._media_service)
        self._instruction_builder = InstructionStageBuilder(media_service=self._media_service)
        self._pools: dict[str, ActiveContextPool] = {}

    def get_pool(self, session_id: str) -> ActiveContextPool:
        pool = self._pools.get(session_id)
        if pool is not None:
            return pool
        items = self._provider.get_recent(session_id, limit=self._preload_limit)
        pool = ActiveContextPool(session_id=session_id, max_messages=self._max_pool_messages)
        pool.load([self._build_pool_payload(item) for item in items])
        self._pools[session_id] = pool
        return pool

    def get_alias_table(self, session_id: str) -> SessionAliasTable:
        return self.get_session_state(session_id).alias_table

    def get_session_state(self, session_id: str) -> ContextSessionState:
        state = self._session_states.get(session_id)
        if state is not None:
            return state
        loaded = self._state_store.load(session_id)
        state = loaded or ContextSessionState(session_id=session_id)
        if not state.session_id:
            state.session_id = session_id
        if not state.alias_table.session_id:
            state.alias_table.session_id = session_id
        self._session_states[session_id] = state
        return state

    def rebuild_alias_table(
        self,
        session_id: str,
        *,
        now_ms: int,
        force: bool = False,
    ) -> SessionAliasTable:
        pool = self.get_pool(session_id)
        state = self.get_session_state(session_id)
        table = state.alias_table
        if not force and table.entries and not table.should_rebuild(now_ms):
            return table
        table.rebuild_from_messages(list(pool.messages), now_ms=now_ms)
        self._save_session_state(session_id)
        return table

    def build_context_stage_messages(
        self,
        session_id: str,
        *,
        self_platform_id: str = "",
        now_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        if not session_id:
            return []
        timestamp_ms = now_ms if now_ms is not None else int(time.time() * 1000)
        state = self.get_session_state(session_id)
        alias_rebuild_due = not bool(state.alias_table.entries) or state.alias_table.should_rebuild(
            timestamp_ms
        )
        alias_table = self.rebuild_alias_table(
            session_id,
            now_ms=timestamp_ms,
            force=alias_rebuild_due,
        )
        read_history = self.get_recent_messages(session_id, read_only=True)
        latest_history_id = _latest_record_id(read_history)
        latest_block_id = _latest_block_record_id(state.blocks)
        should_rebuild_blocks = (
            alias_rebuild_due or not state.blocks or latest_history_id > latest_block_id
        )

        if should_rebuild_blocks:
            messages = self._context_builder.build_prompt_messages(
                read_history,
                alias_table=alias_table,
                session_state=state,
                self_platform_id=self_platform_id,
            )
        else:
            messages = [{"role": "user", "content": list(block.contents)} for block in state.blocks]
        compressed_messages = self._build_compressed_memory_messages(state)
        self._save_session_state(session_id)
        return [*compressed_messages, *messages]

    def build_instruction_stage_content(
        self,
        session_id: str,
        unread_records: list[dict[str, Any]],
        *,
        previous_summary: str = "",
        self_platform_id: str = "",
        now_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        if not session_id:
            return []
        timestamp_ms = now_ms if now_ms is not None else int(time.time() * 1000)
        state = self.get_session_state(session_id)
        alias_table = self.rebuild_alias_table(
            session_id,
            now_ms=timestamp_ms,
            force=not bool(state.alias_table.entries),
        )
        content_blocks = self._instruction_builder.build_content_blocks(
            unread_records,
            alias_table=alias_table,
            session_state=state,
            previous_summary=previous_summary,
            self_platform_id=self_platform_id,
            now_ms=timestamp_ms,
        )
        self._save_session_state(session_id)
        return content_blocks

    def build_inactive_alias_context_message(
        self,
        session_id: str,
        *,
        now_ms: int | None = None,
    ) -> dict[str, Any] | None:
        timestamp_ms = now_ms if now_ms is not None else int(time.time() * 1000)
        table = self.rebuild_alias_table(
            session_id,
            now_ms=timestamp_ms,
            force=not bool(self.get_alias_table(session_id).entries),
        )
        inactive_entries, _active_entries = table.split_by_activity(now_ms=timestamp_ms)
        if not inactive_entries:
            return None
        lines = ["### 会话历史成员映射"]
        for entry in inactive_entries:
            alias_id = entry.alias or entry.platform_id
            display_name = entry.display_name or entry.platform_id
            lines.append(f"{alias_id} = {display_name} / {entry.platform_id}")
        return {"role": "user", "content": [{"type": "text", "text": "\n".join(lines)}]}

    def build_active_alias_constraint_text(
        self,
        session_id: str,
        *,
        now_ms: int | None = None,
    ) -> str:
        timestamp_ms = now_ms if now_ms is not None else int(time.time() * 1000)
        table = self.rebuild_alias_table(
            session_id,
            now_ms=timestamp_ms,
            force=not bool(self.get_alias_table(session_id).entries),
        )
        _inactive_entries, active_entries = table.split_by_activity(now_ms=timestamp_ms)
        if not active_entries:
            return ""
        lines = [
            "### 当前活跃成员映射",
            "如需称呼用户，优先使用有意义的称呼(display_name)而非代称。",
        ]
        for entry in active_entries:
            alias_id = entry.alias or entry.platform_id
            display_name = entry.display_name or entry.platform_id
            lines.append(f"{alias_id} = {display_name} / {entry.platform_id}")
        return "\n".join(lines)

    def apply_usage_eviction(
        self,
        session_id: str,
        usage: dict[str, Any] | None,
        *,
        max_context_tokens: int = 32_000,
        evict_ratio: float = 0.6,
        compressed_text: str = "",
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        if not session_id:
            return {"triggered": False, "evicted_count": 0, "remaining_count": 0}
        state = self.get_session_state(session_id)
        result = evict_context_blocks(
            state,
            total_tokens=extract_total_tokens(usage),
            config=ContextEvictionConfig(
                max_context_tokens=max_context_tokens,
                evict_ratio=evict_ratio,
            ),
            compressed_text=compressed_text,
            created_at_ms=now_ms,
        )
        self._save_session_state(session_id)
        return result

    def preview_usage_eviction(
        self,
        session_id: str,
        usage: dict[str, Any] | None,
        *,
        max_context_tokens: int = 32_000,
        evict_ratio: float = 0.6,
    ) -> dict[str, Any]:
        if not session_id:
            return {"triggered": False, "evicted_count": 0, "remaining_count": 0}
        state = self.get_session_state(session_id)
        total_tokens = extract_total_tokens(usage)
        config = ContextEvictionConfig(
            max_context_tokens=max_context_tokens,
            evict_ratio=evict_ratio,
        )
        evicted_blocks = select_blocks_for_eviction(
            state,
            total_tokens=total_tokens,
            config=config,
        )
        if not evicted_blocks:
            return {
                "triggered": False,
                "total_tokens": total_tokens,
                "evicted_count": 0,
                "remaining_count": len(state.blocks),
                "source_text": "",
                "source_block_ids": [],
            }

        return {
            "triggered": True,
            "total_tokens": total_tokens,
            "evicted_count": len(evicted_blocks),
            "remaining_count": max(0, len(state.blocks) - len(evicted_blocks)),
            "source_text": self._build_compression_source_text(state, evicted_blocks),
            "source_block_ids": [block.block_id for block in evicted_blocks],
        }

    def _save_session_state(self, session_id: str) -> None:
        state = self._session_states.get(session_id)
        if state is None:
            return
        self._state_store.save(state)

    def _build_compression_source_text(
        self,
        state: ContextSessionState,
        blocks: list[ContextBlockState],
    ) -> str:
        alias_lines: list[str] = []
        alias_entries = sorted(
            state.alias_table.entries.values(),
            key=lambda item: (item.alias.startswith("P"), item.alias, item.platform_id),
        )
        for entry in alias_entries:
            alias_id = entry.alias.strip()
            if not alias_id:
                continue
            display_name = entry.display_name or entry.platform_id
            alias_lines.append(f"{alias_id} = {display_name} / {entry.platform_id}")

        context_lines: list[str] = []
        for block in blocks:
            for content_block in block.contents:
                if str(content_block.get("type") or "") != "text":
                    continue
                text = str(content_block.get("text") or "").strip()
                if not text:
                    continue
                context_lines.append(_expand_aliases_for_compression(text, state.alias_table))

        sections: list[str] = []
        if alias_lines:
            sections.append("### 成员映射\n" + "\n".join(alias_lines))
        if context_lines:
            sections.append("### 待压缩上下文\n" + "\n".join(context_lines))
        return "\n\n".join(sections).strip()

    @staticmethod
    def _build_compressed_memory_messages(state: ContextSessionState) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = []
        for item in state.compressed_memories:
            if not item.text.strip():
                continue
            messages.append(
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": f"### 压缩记忆\n{item.text}",
                        }
                    ],
                }
            )
        return messages

    def track_message_record(self, record: MessageLogRecord, *, platform: str = "") -> None:
        if not record.session_id:
            return
        pool = self.get_pool(record.session_id)
        payload = self._build_pool_payload(
            {
                "id": record.id,
                "session_id": record.session_id,
                "role": record.role,
                "raw_text": record.raw_text,
                "content_json": record.content_json,
                "created_at": record.created_at,
                "sender_id": record.sender_id,
                "sender_name": record.sender_name,
                "platform_msg_id": record.platform_msg_id,
                "platform": platform,
                "is_read": record.is_read,
            }
        )
        pool.append(payload)
        self.get_alias_table(record.session_id).note_activity(record.created_at)
        self._save_session_state(record.session_id)

        if self._identity_store is not None and record.role == "user" and record.sender_id.strip():
            self._identity_store.ensure_user(
                user_id=record.sender_id,
                suggested_name=record.sender_name,
                platform=platform,
            )

        if record.is_read:
            return

    def _build_pool_payload(self, item: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "id": item.get("id"),
            "session_id": item.get("session_id", ""),
            "role": item.get("role", ""),
            "raw_text": item.get("raw_text", ""),
            "created_at": item.get("created_at"),
            "sender_id": item.get("sender_id", ""),
            "sender_name": item.get("sender_name", ""),
            "platform_msg_id": item.get("platform_msg_id", ""),
            "platform": item.get("platform", ""),
            "is_read": bool(item.get("is_read", False)),
            "content_json": item.get("content_json", "[]"),
        }
        merged_content = self._compose_content(payload)
        if merged_content:
            payload["content"] = merged_content
        return payload

    def _compose_content(self, item: dict[str, Any]) -> str:
        text = str(item.get("raw_text") or "").strip()
        if self._media_service is None:
            return text

        media_notes = self._media_service.summarize_message_media(item)
        if text and media_notes:
            return f"{text} {' '.join(media_notes)}"
        if media_notes:
            return " ".join(media_notes)
        return text

    def get_recent_messages(
        self,
        session_id: str,
        *,
        limit: int | None = None,
        read_only: bool = True,
    ) -> list[dict[str, Any]]:
        pool = self.get_pool(session_id)
        items = pool.export_turns(read_only=read_only)
        if limit is not None:
            items = items[-limit:]
        return items

    def get_context_inputs(
        self,
        session_id: str,
        *,
        fallback: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        payload = dict(fallback or {})
        if not session_id:
            return payload
        pool = self.get_pool(session_id)
        turns = pool.export_turns()
        if limit is not None:
            turns = turns[-limit:]
        payload["history_turns"] = turns
        payload["summary"] = payload.get("summary") or pool.summary
        payload["current_tokens"] = pool.token_estimate
        payload["context_source"] = "active_context_pool"
        return payload

    def mark_read_until(self, session_id: str, msg_id: int) -> None:
        if not session_id:
            return
        pool = self.get_pool(session_id)
        pool.mark_read_until(msg_id)
        self._save_session_state(session_id)


def _latest_record_id(records: list[dict[str, Any]]) -> int:
    record_ids = [int(item["id"]) for item in records if isinstance(item.get("id"), int)]
    return max(record_ids, default=0)


def _latest_block_record_id(blocks: list[ContextBlockState]) -> int:
    latest = 0
    for block in blocks:
        record_ids = block.metadata.get("record_ids", [])
        if not isinstance(record_ids, list):
            continue
        numeric_ids = [int(item) for item in record_ids if isinstance(item, int)]
        if numeric_ids:
            latest = max(latest, max(numeric_ids))
    return latest


_MESSAGE_ALIAS_PREFIX_PATTERN = re.compile(r"^(\[msgid: \d+\])(?P<alias>[AP]\d+)(?=: )")


def _expand_aliases_for_compression(text: str, alias_table: SessionAliasTable) -> str:
    alias_map = {
        entry.alias: (entry.display_name or entry.platform_id or entry.alias)
        for entry in alias_table.entries.values()
        if entry.alias
    }
    if not alias_map:
        return text

    expanded = _MESSAGE_ALIAS_PREFIX_PATTERN.sub(
        lambda match: (
            f"{match.group(1)}{alias_map.get(match.group('alias'), match.group('alias'))}"
        ),
        text,
    )
    for alias, display_name in alias_map.items():
        escaped = re.escape(alias)
        expanded = re.sub(
            rf"(?<![A-Za-z0-9_]){escaped}(?=(?:/|\]))",
            display_name,
            expanded,
        )
    return expanded
