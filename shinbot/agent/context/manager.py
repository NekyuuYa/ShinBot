"""Active context pool and standardized retrieval manager."""

from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from shinbot.agent.context.active_pool import ActiveContextPool
from shinbot.agent.context.alias_projector import AliasContextProjector
from shinbot.agent.context.alias_table import SessionAliasTable
from shinbot.agent.context.compressed_memory_projector import CompressedMemoryProjector
from shinbot.agent.context.context_stage_builder import ContextStageBuilder
from shinbot.agent.context.eviction import (
    ContextEvictionConfig,
    evict_context_blocks,
    extract_total_tokens,
    select_blocks_for_eviction,
)
from shinbot.agent.context.instruction_stage_builder import InstructionStageBuilder
from shinbot.agent.context.projection import (
    ContextProjectionState,
    PromptMemoryBundle,
    PromptMemoryProjectionRequest,
)
from shinbot.agent.context.prompt_memory_assembler import PromptMemoryAssembler
from shinbot.agent.context.state_store import ContextSessionState, ContextStateStore
from shinbot.agent.context.timeline_runtime import ContextTimelineRuntime
from shinbot.agent.context.token_utils import estimate_text_tokens

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
        self._timeline_runtime = ContextTimelineRuntime(self._context_builder)
        self._instruction_builder = InstructionStageBuilder(media_service=self._media_service)
        self._alias_projector = AliasContextProjector()
        self._compressed_memory_projector = CompressedMemoryProjector()
        self._prompt_memory_assembler = PromptMemoryAssembler(self)
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

    def get_cacheable_context_message_count(self, session_id: str) -> int:
        if not session_id:
            return 0
        state = self.get_session_state(session_id)
        return len(state.compressed_memories) + state.short_term_memory().cacheable_prefix_count()

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
    ) -> tuple[SessionAliasTable, bool]:
        pool = self.get_pool(session_id)
        state = self.get_session_state(session_id)
        table = state.alias_table
        if not force and table.entries and not table.should_rebuild(now_ms):
            return table, False
        changed = table.rebuild_from_messages(
            list(pool.messages),
            now_ms=now_ms,
            identity_store=self._identity_store,
        )
        self._save_session_state(session_id)
        return table, changed

    def sync_identity_display_name(
        self,
        session_id: str,
        *,
        user_id: str,
        now_ms: int | None = None,
    ) -> bool:
        if not session_id or self._identity_store is None:
            return False

        normalized_user_id = str(user_id or "").strip()
        if not normalized_user_id:
            return False

        identity = self._identity_store.get_identity(normalized_user_id)
        if identity is None:
            return False

        display_name = str(identity.get("name", "") or "").strip()
        if not display_name:
            return False

        timestamp_ms = now_ms if now_ms is not None else int(time.time() * 1000)
        table = self.get_alias_table(session_id)
        changed = table.apply_identity_display_name(
            normalized_user_id,
            display_name,
            now_ms=timestamp_ms,
        )
        if changed:
            self._save_session_state(session_id)
        return changed

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
        alias_table, alias_changed = self.rebuild_alias_table(
            session_id,
            now_ms=timestamp_ms,
            force=alias_rebuild_due,
        )
        read_history = self.get_recent_messages(session_id, read_only=True)
        existing_blocks = state.legacy_blocks()
        force_rebuild = alias_changed or not existing_blocks
        if force_rebuild:
            self._alias_projector.reset_inactive_snapshot(state)
        self._timeline_runtime.builder = self._context_builder
        messages = self._timeline_runtime.build_prompt_messages(
            read_history,
            alias_table=alias_table,
            session_state=state,
            force_rebuild=force_rebuild,
            self_platform_id=self_platform_id,
        )
        compressed_messages = self._compressed_memory_projector.build_messages(
            state.compressed_memories
        )
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
        alias_table, _alias_changed = self.rebuild_alias_table(
            session_id,
            now_ms=timestamp_ms,
            force=not bool(state.alias_table.entries),
        )
        content_blocks = self._instruction_builder.build_content_blocks(
            unread_records,
            alias_table=alias_table,
            projection_state=ContextProjectionState.from_session_state(
                session_state=state,
                image_registry=self._instruction_builder.image_registry,
            ),
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
        unread_records: list[dict[str, Any]] | None = None,
        now_ms: int | None = None,
    ) -> dict[str, Any] | None:
        timestamp_ms = now_ms if now_ms is not None else int(time.time() * 1000)
        state = self.get_session_state(session_id)
        existing_blocks = state.legacy_blocks()
        if (
            not existing_blocks
            or not state.alias_table.entries
            or state.alias_table.should_rebuild(timestamp_ms)
        ):
            self.build_context_stage_messages(
                session_id,
                now_ms=timestamp_ms,
            )
            state = self.get_session_state(session_id)
            existing_blocks = state.legacy_blocks()

        was_frozen = state.inactive_alias_table_frozen
        message = self._alias_projector.build_inactive_context_message(
            state=state,
            blocks=existing_blocks,
            unread_records=unread_records,
        )
        if not was_frozen and state.inactive_alias_table_frozen:
            self._save_session_state(session_id)
        return message

    def build_active_alias_constraint_text(
        self,
        session_id: str,
        *,
        unread_records: list[dict[str, Any]] | None = None,
        now_ms: int | None = None,
    ) -> str:
        timestamp_ms = now_ms if now_ms is not None else int(time.time() * 1000)
        state = self.get_session_state(session_id)
        existing_blocks = state.legacy_blocks()
        if not existing_blocks:
            self.build_context_stage_messages(
                session_id,
                now_ms=timestamp_ms,
            )
            state = self.get_session_state(session_id)
            existing_blocks = state.legacy_blocks()

        table, _alias_changed = self.rebuild_alias_table(
            session_id,
            now_ms=timestamp_ms,
            force=not bool(state.alias_table.entries) or state.alias_table.should_rebuild(timestamp_ms),
        )
        return self._alias_projector.build_active_constraint_text(
            alias_table=table,
            blocks=existing_blocks,
            unread_records=unread_records,
        )

    def build_prompt_memory_bundle(
        self,
        request: PromptMemoryProjectionRequest,
    ) -> PromptMemoryBundle:
        """Project session context into the prompt-facing memory bundle."""
        return self._prompt_memory_assembler.assemble(request)

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
        if result.get("triggered"):
            state.alias_table.request_rebuild()
            self._alias_projector.reset_inactive_snapshot(state)
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
                "remaining_count": state.short_term_memory().block_count(),
                "source_text": "",
                "source_block_ids": [],
            }

        return {
            "triggered": True,
            "total_tokens": total_tokens,
            "evicted_count": len(evicted_blocks),
            "remaining_count": max(0, state.short_term_memory().block_count() - len(evicted_blocks)),
            "source_text": self._compressed_memory_projector.build_source_text(
                alias_table=state.alias_table,
                blocks=evicted_blocks,
            ),
            "source_block_ids": [block.block_id for block in evicted_blocks],
        }

    def _save_session_state(self, session_id: str) -> None:
        state = self._session_states.get(session_id)
        if state is None:
            return
        self._state_store.save(state)

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
        items = pool.export_records(read_only=read_only)
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
