"""Prompt-facing runtime for context memory projection."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from shinbot.agent.context.runtime.alias_runtime import ContextAliasRuntime
from shinbot.agent.context.runtime.context_stage_runtime import ContextStageRuntime
from shinbot.agent.context.runtime.instruction_runtime import InstructionRuntime
from shinbot.agent.context.runtime.pool_runtime import ContextPoolRuntime
from shinbot.agent.context.runtime.session_runtime import ContextSessionRuntime
from shinbot.agent.context.state.alias_table import SessionAliasTable
from shinbot.agent.context.state.state_store import ContextSessionState

if TYPE_CHECKING:
    from shinbot.agent.identity import IdentityStore


@dataclass(slots=True)
class ContextPromptRuntime:
    """Coordinate prompt-facing memory stage projections."""

    pool_runtime: ContextPoolRuntime
    session_runtime: ContextSessionRuntime
    alias_runtime: ContextAliasRuntime
    context_stage_runtime: ContextStageRuntime
    instruction_runtime: InstructionRuntime
    identity_store: IdentityStore | None = None

    def get_cacheable_context_message_count(self, session_id: str) -> int:
        if not session_id:
            return 0
        state = self.get_session_state(session_id)
        return len(state.compressed_memories) + state.short_term_memory().cacheable_prefix_count()

    def get_session_state(self, session_id: str) -> ContextSessionState:
        return self.session_runtime.get_state(session_id)

    def rebuild_alias_table(
        self,
        session_id: str,
        *,
        now_ms: int,
        force: bool = False,
    ) -> tuple[SessionAliasTable, bool]:
        pool = self.pool_runtime.get_pool(session_id)
        state = self.get_session_state(session_id)
        should_rebuild = self.alias_runtime.needs_table_rebuild(
            state.alias_table,
            now_ms,
            force=force,
        )
        table, changed = self.alias_runtime.rebuild_table(
            state.alias_table,
            list(pool.messages),
            now_ms=now_ms,
            force=force,
            identity_store=self.identity_store,
        )
        if should_rebuild:
            self.save_session_state(session_id)
        return table, changed

    def sync_identity_display_name(
        self,
        session_id: str,
        *,
        user_id: str,
        now_ms: int | None = None,
    ) -> bool:
        if not session_id or self.identity_store is None:
            return False

        timestamp_ms = now_ms if now_ms is not None else int(time.time() * 1000)
        changed = self.alias_runtime.sync_identity_display_name(
            self.get_session_state(session_id).alias_table,
            self.identity_store,
            user_id=user_id,
            now_ms=timestamp_ms,
        )
        if changed:
            self.save_session_state(session_id)
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
        alias_rebuild_due = self.alias_runtime.needs_table_rebuild(
            state.alias_table,
            timestamp_ms,
        )
        alias_table, alias_changed = self.rebuild_alias_table(
            session_id,
            now_ms=timestamp_ms,
            force=alias_rebuild_due,
        )
        read_history = self.pool_runtime.get_recent_messages(session_id, read_only=True)
        messages = self.context_stage_runtime.build_messages(
            read_history,
            alias_table=alias_table,
            session_state=state,
            alias_changed=alias_changed,
            self_platform_id=self_platform_id,
        )
        self.save_session_state(session_id)
        return messages

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
        content_blocks = self.instruction_runtime.build_content_blocks(
            unread_records,
            alias_table=alias_table,
            session_state=state,
            previous_summary=previous_summary,
            self_platform_id=self_platform_id,
            now_ms=timestamp_ms,
        )
        self.save_session_state(session_id)
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
        existing_blocks = state.short_term_blocks()
        if self.alias_runtime.needs_inactive_context_refresh(
            state=state,
            blocks=existing_blocks,
            now_ms=timestamp_ms,
        ):
            self.build_context_stage_messages(
                session_id,
                now_ms=timestamp_ms,
            )
            state = self.get_session_state(session_id)
            existing_blocks = state.short_term_blocks()

        message, changed = self.alias_runtime.build_inactive_context_message(
            state=state,
            blocks=existing_blocks,
            unread_records=unread_records,
        )
        if changed:
            self.save_session_state(session_id)
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
        existing_blocks = state.short_term_blocks()
        if self.alias_runtime.needs_active_context_refresh(existing_blocks):
            self.build_context_stage_messages(
                session_id,
                now_ms=timestamp_ms,
            )
            state = self.get_session_state(session_id)
            existing_blocks = state.short_term_blocks()

        table, _alias_changed = self.rebuild_alias_table(
            session_id,
            now_ms=timestamp_ms,
            force=self.alias_runtime.needs_active_alias_rebuild(
                state.alias_table,
                timestamp_ms,
            ),
        )
        return self.alias_runtime.build_active_constraint_text(
            alias_table=table,
            blocks=existing_blocks,
            unread_records=unread_records,
        )

    def save_session_state(self, session_id: str) -> None:
        self.session_runtime.save(session_id)
