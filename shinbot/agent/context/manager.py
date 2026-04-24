"""Active context pool and standardized retrieval manager."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from shinbot.agent.context.builders.context_stage_builder import ContextStageBuilder
from shinbot.agent.context.builders.instruction_stage_builder import InstructionStageBuilder
from shinbot.agent.context.projectors.alias_projector import AliasContextProjector
from shinbot.agent.context.projectors.compressed_memory_projector import CompressedMemoryProjector
from shinbot.agent.context.projectors.projection import (
    PromptMemoryBundle,
    PromptMemoryProjectionRequest,
)
from shinbot.agent.context.runtime.alias_runtime import ContextAliasRuntime
from shinbot.agent.context.runtime.context_stage_runtime import ContextStageRuntime
from shinbot.agent.context.runtime.eviction_runtime import ContextEvictionRuntime
from shinbot.agent.context.runtime.instruction_runtime import InstructionRuntime
from shinbot.agent.context.runtime.pool_runtime import ContextPoolRuntime
from shinbot.agent.context.runtime.prompt_memory_assembler import PromptMemoryAssembler
from shinbot.agent.context.runtime.prompt_runtime import ContextPromptRuntime
from shinbot.agent.context.runtime.session_runtime import ContextSessionRuntime
from shinbot.agent.context.runtime.timeline_runtime import ContextTimelineRuntime
from shinbot.agent.context.state.active_pool import ActiveContextPool
from shinbot.agent.context.state.alias_table import SessionAliasTable
from shinbot.agent.context.state.state_store import ContextSessionState
from shinbot.agent.context.utils.token_utils import estimate_text_tokens

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
        self._identity_store = identity_store
        self._media_service = media_service
        self._pool_runtime = ContextPoolRuntime(
            provider=provider,
            preload_limit=preload_limit,
            max_pool_messages=max_pool_messages,
            media_service=self._media_service,
        )
        self._session_runtime = ContextSessionRuntime.from_data_dir(data_dir=data_dir)
        self._context_builder = ContextStageBuilder(media_service=self._media_service)
        self._timeline_runtime = ContextTimelineRuntime(self._context_builder)
        self._instruction_builder = InstructionStageBuilder(media_service=self._media_service)
        self._instruction_runtime = InstructionRuntime(self._instruction_builder)
        self._alias_projector = AliasContextProjector()
        self._alias_runtime = ContextAliasRuntime(self._alias_projector)
        self._compressed_memory_projector = CompressedMemoryProjector()
        self._context_stage_runtime = ContextStageRuntime(
            timeline_runtime=self._timeline_runtime,
            alias_projector=self._alias_projector,
            compressed_memory_projector=self._compressed_memory_projector,
        )
        self._eviction_runtime = ContextEvictionRuntime(
            alias_projector=self._alias_projector,
            compressed_memory_projector=self._compressed_memory_projector,
        )
        self._prompt_runtime = ContextPromptRuntime(
            pool_runtime=self._pool_runtime,
            session_runtime=self._session_runtime,
            alias_runtime=self._alias_runtime,
            context_stage_runtime=self._context_stage_runtime,
            instruction_runtime=self._instruction_runtime,
            identity_store=self._identity_store,
        )
        self._prompt_memory_assembler = PromptMemoryAssembler(self._prompt_runtime)

    def get_pool(self, session_id: str) -> ActiveContextPool:
        return self._pool_runtime.get_pool(session_id)

    def get_alias_table(self, session_id: str) -> SessionAliasTable:
        return self.get_session_state(session_id).alias_table

    def get_cacheable_context_message_count(self, session_id: str) -> int:
        return self._prompt_runtime.get_cacheable_context_message_count(session_id)

    def get_session_state(self, session_id: str) -> ContextSessionState:
        return self._session_runtime.get_state(session_id)

    def rebuild_alias_table(
        self,
        session_id: str,
        *,
        now_ms: int,
        force: bool = False,
    ) -> tuple[SessionAliasTable, bool]:
        self._sync_prompt_runtime()
        return self._prompt_runtime.rebuild_alias_table(
            session_id,
            now_ms=now_ms,
            force=force,
        )

    def sync_identity_display_name(
        self,
        session_id: str,
        *,
        user_id: str,
        now_ms: int | None = None,
    ) -> bool:
        self._sync_prompt_runtime()
        return self._prompt_runtime.sync_identity_display_name(
            session_id,
            user_id=user_id,
            now_ms=now_ms,
        )

    def build_context_stage_messages(
        self,
        session_id: str,
        *,
        self_platform_id: str = "",
        now_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        self._sync_prompt_runtime()
        return self._prompt_runtime.build_context_stage_messages(
            session_id,
            self_platform_id=self_platform_id,
            now_ms=now_ms,
        )

    def build_instruction_stage_content(
        self,
        session_id: str,
        unread_records: list[dict[str, Any]],
        *,
        previous_summary: str = "",
        self_platform_id: str = "",
        now_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        self._sync_prompt_runtime()
        return self._prompt_runtime.build_instruction_stage_content(
            session_id,
            unread_records,
            previous_summary=previous_summary,
            self_platform_id=self_platform_id,
            now_ms=now_ms,
        )

    def build_inactive_alias_context_message(
        self,
        session_id: str,
        *,
        unread_records: list[dict[str, Any]] | None = None,
        now_ms: int | None = None,
    ) -> dict[str, Any] | None:
        self._sync_prompt_runtime()
        return self._prompt_runtime.build_inactive_alias_context_message(
            session_id,
            unread_records=unread_records,
            now_ms=now_ms,
        )

    def build_active_alias_constraint_text(
        self,
        session_id: str,
        *,
        unread_records: list[dict[str, Any]] | None = None,
        now_ms: int | None = None,
    ) -> str:
        self._sync_prompt_runtime()
        return self._prompt_runtime.build_active_alias_constraint_text(
            session_id,
            unread_records=unread_records,
            now_ms=now_ms,
        )

    def build_prompt_memory_bundle(
        self,
        request: PromptMemoryProjectionRequest,
    ) -> PromptMemoryBundle:
        """Project session context into the prompt-facing memory bundle."""
        self._sync_prompt_runtime()
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
        result = self._eviction_runtime.apply(
            state,
            usage,
            max_context_tokens=max_context_tokens,
            evict_ratio=evict_ratio,
            compressed_text=compressed_text,
            now_ms=now_ms,
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
        return self._eviction_runtime.preview(
            state,
            usage,
            max_context_tokens=max_context_tokens,
            evict_ratio=evict_ratio,
        )

    def _save_session_state(self, session_id: str) -> None:
        self._session_runtime.save(session_id)

    def _sync_prompt_runtime(self) -> None:
        self._timeline_runtime.builder = self._context_builder
        self._instruction_runtime.builder = self._instruction_builder
        self._prompt_runtime.identity_store = self._identity_store

    def track_message_record(self, record: MessageLogRecord, *, platform: str = "") -> None:
        if not record.session_id:
            return
        self._pool_runtime.append_record(record, platform=platform)
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

    def get_recent_messages(
        self,
        session_id: str,
        *,
        limit: int | None = None,
        read_only: bool = True,
    ) -> list[dict[str, Any]]:
        return self._pool_runtime.get_recent_messages(
            session_id,
            limit=limit,
            read_only=read_only,
        )

    def get_context_inputs(
        self,
        session_id: str,
        *,
        fallback: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        return self._pool_runtime.get_context_inputs(
            session_id,
            fallback=fallback,
            limit=limit,
        )

    def mark_read_until(self, session_id: str, msg_id: int) -> None:
        if not session_id:
            return
        self._pool_runtime.mark_read_until(session_id, msg_id)
        self._save_session_state(session_id)
