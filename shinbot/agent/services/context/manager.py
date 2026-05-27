"""Active context pool and standardized retrieval manager.

Provides ``ContextManager``, the top-level orchestrator for building and
maintaining conversation context windows consumed by LLM calls. Internally
it delegates to a suite of specialized runtimes — pool, session, alias,
eviction, prompt, and timeline — that together implement ring-buffer
storage, user-ID/name alias mapping, eviction policies, and long-term
memory assembly from compressed summaries.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from shinbot.agent.services.context.builders.context_stage_builder import ContextStageBuilder
from shinbot.agent.services.context.projectors.alias_projector import AliasContextProjector
from shinbot.agent.services.context.projectors.compressed_memory_projector import (
    CompressedMemoryProjector,
)
from shinbot.agent.services.context.projectors.projection import (
    PromptMemoryBundle,
    PromptMemoryProjectionRequest,
)
from shinbot.agent.services.context.runtime.alias_runtime import ContextAliasRuntime
from shinbot.agent.services.context.runtime.context_stage_runtime import ContextStageRuntime
from shinbot.agent.services.context.runtime.eviction_runtime import ContextEvictionRuntime
from shinbot.agent.services.context.runtime.pool_runtime import ContextPoolRuntime
from shinbot.agent.services.context.runtime.prompt_memory_assembler import PromptMemoryAssembler
from shinbot.agent.services.context.runtime.prompt_runtime import ContextPromptRuntime
from shinbot.agent.services.context.runtime.session_runtime import ContextSessionRuntime
from shinbot.agent.services.context.runtime.timeline_runtime import ContextTimelineRuntime
from shinbot.agent.services.context.state.active_pool import ActiveContextPool
from shinbot.agent.services.context.state.alias_table import SessionAliasTable
from shinbot.agent.services.context.state.state_store import ContextSessionState
from shinbot.agent.services.context.utils.token_utils import estimate_text_tokens

if TYPE_CHECKING:
    from shinbot.agent.services.identity import IdentityStore
    from shinbot.agent.services.media import MediaService
    from shinbot.agent.services.summaries import SummaryService
    from shinbot.persistence.records import MessageLogRecord
    from shinbot.persistence.repos import ContextProvider

logger = logging.getLogger(__name__)


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
    """Orchestrates conversation context windows for LLM calls.

    Manages per-session context pools, alias tables, eviction policies, and
    long-term memory assembly using a projector/builder pattern backed by
    specialized runtimes.  Each session is identified by a *session_id* and
    holds a ring-buffer message pool, a user-ID-to-name alias table, and
    compressed memory summaries.

    Args:
        provider: Persistence layer for reading and writing context data.
        data_dir: Directory for session state files.  Defaults to ``"data"``.
        preload_limit: Maximum number of messages to preload per session.
        max_pool_messages: Hard cap on messages kept in the ring-buffer pool.
        identity_store: Optional store for bot/persona identity look-ups.
        media_service: Optional service for media content processing.
        summary_service: Optional service for persisting compressed summaries.
    """

    def __init__(
        self,
        provider: ContextProvider,
        *,
        data_dir: Path | str | None = "data",
        preload_limit: int = 50,
        max_pool_messages: int = 200,
        identity_store: IdentityStore | None = None,
        media_service: MediaService | None = None,
        summary_service: SummaryService | None = None,
    ) -> None:
        self._identity_store = identity_store
        self._media_service = media_service
        self._summary_service = summary_service
        self._pool_runtime = ContextPoolRuntime(
            provider=provider,
            preload_limit=preload_limit,
            max_pool_messages=max_pool_messages,
            media_service=self._media_service,
        )
        self._session_runtime = ContextSessionRuntime.from_data_dir(data_dir=data_dir)
        self._context_builder = ContextStageBuilder(media_service=self._media_service)
        self._timeline_runtime = ContextTimelineRuntime(self._context_builder)
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
            identity_store=self._identity_store,
        )
        from shinbot.agent.services.message_formatter import MessageFormatterService

        self._prompt_memory_assembler = PromptMemoryAssembler(
            self._prompt_runtime,
            message_formatter=MessageFormatterService(
                identity_store=self._identity_store,
                media_service=self._media_service,
            ),
        )

    def get_pool(self, session_id: str) -> ActiveContextPool:
        """Return the message pool for *session_id*.

        The pool is a ring-buffer of ``MessageLogRecord`` entries that
        provides fast access to recent messages while respecting the
        configured ``max_pool_messages`` cap.

        Args:
            session_id: Unique identifier for the conversation session.

        Returns:
            The active context pool for the requested session.
        """
        return self._pool_runtime.get_pool(session_id)

    def get_alias_table(self, session_id: str) -> SessionAliasTable:
        """Return the user-ID/name alias table for *session_id*.

        The alias table maps user IDs to display names and tracks per-user
        activity timestamps for context construction.

        Args:
            session_id: Unique identifier for the conversation session.

        Returns:
            The alias table associated with the session.
        """
        return self.get_session_state(session_id).alias_table

    def get_cacheable_context_message_count(self, session_id: str) -> int:
        """Return the number of messages eligible for cache optimization.

        Counts messages in the session that can be included in a
        cacheable context block, enabling cost-efficient LLM caching.

        Args:
            session_id: Unique identifier for the conversation session.

        Returns:
            Count of cacheable context messages.
        """
        return self._prompt_runtime.get_cacheable_context_message_count(session_id)

    def get_session_state(self, session_id: str) -> ContextSessionState:
        """Return the current context state for *session_id*.

        The session state holds the alias table, compressed memories, and
        other mutable state that persists across context window rebuilds.

        Args:
            session_id: Unique identifier for the conversation session.

        Returns:
            The session state object for the requested session.
        """
        return self._session_runtime.get_state(session_id)

    def rebuild_alias_table(
        self,
        session_id: str,
        *,
        now_ms: int,
        force: bool = False,
    ) -> tuple[SessionAliasTable, bool]:
        """Rebuild the alias table for *session_id* from current records.

        Refreshes the mapping of user IDs to display names and prunes
        stale entries.  A rebuild can be forced even when the table is
        already current.

        Args:
            session_id: Unique identifier for the conversation session.
            now_ms: Current epoch time in milliseconds.
            force: When ``True``, bypass staleness checks and always
                rebuild.

        Returns:
            A tuple of ``(alias_table, changed)`` where *changed* is
            ``True`` if the table was modified.
        """
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
        """Synchronize a user's display name in the alias table.

        Looks up the latest display name for *user_id* from the identity
        store and updates the alias table if it has changed.

        Args:
            session_id: Unique identifier for the conversation session.
            user_id: Platform-specific user identifier.
            now_ms: Optional epoch time in milliseconds; defaults to now.

        Returns:
            ``True`` if the alias table was updated, ``False`` otherwise.
        """
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
        """Build the context-stage messages for an LLM prompt.

        Assembles the ordered list of context messages (system hints,
        conversation history, alias summaries, etc.) that will be
        placed into the context stage of a prompt snapshot.

        Args:
            session_id: Unique identifier for the conversation session.
            self_platform_id: The bot's own platform user ID, used to
                distinguish self-messages from user messages.
            now_ms: Optional epoch time in milliseconds; defaults to now.

        Returns:
            List of message dictionaries suitable for prompt injection.
        """
        self._sync_prompt_runtime()
        return self._prompt_runtime.build_context_stage_messages(
            session_id,
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
        """Build a context message summarizing inactive users.

        Produces a single context message that lists users who have been
        silent since their last activity, giving the LLM visibility into
        who is present but not participating.

        Args:
            session_id: Unique identifier for the conversation session.
            unread_records: Optional pre-fetched unread message records
                to include in the summary.
            now_ms: Optional epoch time in milliseconds; defaults to now.

        Returns:
            A context message dictionary, or ``None`` when there are no
            inactive users to report.
        """
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
        """Build constraint text describing active participants.

        Returns a plain-text block listing users who are currently active
        in the session, suitable for injection into the system prompt to
        guide the LLM's addressing behaviour.

        Args:
            session_id: Unique identifier for the conversation session.
            unread_records: Optional pre-fetched unread message records.
            now_ms: Optional epoch time in milliseconds; defaults to now.

        Returns:
            Constraint text string (may be empty if no active users).
        """
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
        """Evict old context messages based on token usage.

        When the reported token usage exceeds ``max_context_tokens``,
        older messages are removed from the session pool up to
        ``evict_ratio`` of the excess.  Evicted content is compressed
        into a summary stored as a compressed memory entry.

        Args:
            session_id: Unique identifier for the conversation session.
            usage: Token usage report from the model runtime.  Pass
                ``None`` or an empty dict to skip eviction.
            max_context_tokens: Token budget threshold that triggers
                eviction.
            evict_ratio: Fraction of excess tokens to target when
                choosing how many messages to evict.
            compressed_text: Pre-computed compressed summary of the
                evicted messages to store.
            now_ms: Optional epoch time in milliseconds; defaults to now.

        Returns:
            A dict with eviction metadata: ``triggered``, ``evicted_count``,
            ``remaining_count``, and optionally ``compressed_added`` and
            ``source_block_ids``.
        """
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
        self._persist_compressed_context_summary(
            session_id=session_id,
            result=result,
            compressed_text=compressed_text,
            now_ms=now_ms,
        )
        return result

    def _persist_compressed_context_summary(
        self,
        *,
        session_id: str,
        result: dict[str, Any],
        compressed_text: str,
        now_ms: int | None,
    ) -> None:
        if (
            self._summary_service is None
            or not result.get("compressed_added")
            or not compressed_text.strip()
        ):
            return
        created_at_ms = now_ms
        if created_at_ms is None:
            memories = self.get_session_state(session_id).compressed_memories
            created_at_ms = memories[-1].created_at_ms if memories else 0
        source_run_id = f"context:{session_id}:{created_at_ms or 'unknown'}"
        try:
            self._summary_service.save_compressed_context(
                session_id=session_id,
                source_run_id=source_run_id,
                content=compressed_text.strip(),
                metadata={
                    "source_block_ids": list(result.get("source_block_ids") or []),
                    "evicted_count": int(result.get("evicted_count", 0) or 0),
                    "remaining_count": int(result.get("remaining_count", 0) or 0),
                    "total_tokens": int(result.get("total_tokens", 0) or 0),
                    "created_at_ms": int(created_at_ms or 0),
                },
            )
        except Exception:
            logger.warning(
                "Failed to persist compressed context summary for %s",
                session_id,
                exc_info=True,
            )

    def preview_usage_eviction(
        self,
        session_id: str,
        usage: dict[str, Any] | None,
        *,
        max_context_tokens: int = 32_000,
        evict_ratio: float = 0.6,
    ) -> dict[str, Any]:
        """Preview what eviction would remove without modifying state.

        Performs the same calculation as :meth:`apply_usage_eviction` but
        is read-only — no messages are removed and no summaries are
        persisted.

        Args:
            session_id: Unique identifier for the conversation session.
            usage: Token usage report from the model runtime.
            max_context_tokens: Token budget threshold for the preview.
            evict_ratio: Fraction of excess tokens to target.

        Returns:
            A dict with preview metadata: ``triggered``, ``evicted_count``,
            and ``remaining_count``.
        """
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
        self._prompt_runtime.identity_store = self._identity_store

    def track_message_record(self, record: MessageLogRecord, *, platform: str = "") -> None:
        """Ingest a new message record into the context system.

        Appends the record to the session pool, updates alias-table
        activity timestamps, persists state, and registers the sender
        with the identity store when available.

        Args:
            record: The message log record to track.
            platform: Optional platform identifier for the source adapter.
        """
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
        """Return recent messages from the session pool.

        Retrieves the most recent messages, optionally capped by *limit*.
        When ``read_only`` is ``True`` the messages are not marked as
        consumed.

        Args:
            session_id: Unique identifier for the conversation session.
            limit: Maximum number of messages to return.  ``None`` returns
                all available messages.
            read_only: If ``True``, do not update read-state flags.

        Returns:
            List of message dictionaries in chronological order.
        """
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
        """Return raw context inputs for the session.

        Provides a dictionary of context-building inputs (messages, pool
        metadata, alias data) that downstream components can use to
        construct prompt stages.

        Args:
            session_id: Unique identifier for the conversation session.
            fallback: Default values returned when the session has no
                data yet.
            limit: Optional cap on the number of messages included.

        Returns:
            Dictionary of context inputs for the session.
        """
        return self._pool_runtime.get_context_inputs(
            session_id,
            fallback=fallback,
            limit=limit,
        )

    def mark_read_until(self, session_id: str, msg_id: int) -> None:
        """Mark all messages up to *msg_id* as read for *session_id*.

        Updates the read-watermark in the pool so that unread-message
        queries only return messages newer than *msg_id*.

        Args:
            session_id: Unique identifier for the conversation session.
            msg_id: The highest message ID to mark as read (inclusive).
        """
        if not session_id:
            return
        self._pool_runtime.mark_read_until(session_id, msg_id)
        self._save_session_state(session_id)
