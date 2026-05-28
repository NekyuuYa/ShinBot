"""Prompt-facing runtime for context memory projection."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from shinbot.agent.services.context.runtime.alias_runtime import ContextAliasRuntime
from shinbot.agent.services.context.runtime.context_stage_runtime import ContextStageRuntime
from shinbot.agent.services.context.runtime.pool_runtime import ContextPoolRuntime
from shinbot.agent.services.context.runtime.session_runtime import ContextSessionRuntime
from shinbot.agent.services.context.state.alias_table import SessionAliasTable
from shinbot.agent.services.context.state.state_store import ContextSessionState

if TYPE_CHECKING:
    from shinbot.agent.services.identity import IdentityStore


@dataclass(slots=True)
class ContextPromptRuntime:
    """Coordinate prompt-facing memory stage projections."""

    pool_runtime: ContextPoolRuntime
    session_runtime: ContextSessionRuntime
    alias_runtime: ContextAliasRuntime
    context_stage_runtime: ContextStageRuntime
    identity_store: IdentityStore | None = None

    def get_cacheable_context_message_count(self, session_id: str) -> int:
        """Count messages eligible for prompt cache keying.

        Aggregates compressed memories and the cacheable prefix of the
        short-term memory ring buffer to determine how many messages
        can participate in automatic prompt-cache matching.

        Args:
            session_id: Target conversation session.

        Returns:
            Total count of cache-eligible context messages, or 0 when
            the session_id is empty.
        """
        if not session_id:
            return 0
        state = self.get_session_state(session_id)
        return len(state.compressed_memories) + state.short_term_memory().cacheable_prefix_count()

    def get_session_state(self, session_id: str) -> ContextSessionState:
        """Return the current session state, loading from storage if needed.

        Delegates to the session runtime which maintains an in-memory
        cache backed by on-disk persistence.

        Args:
            session_id: Target conversation session.

        Returns:
            The live ``ContextSessionState`` for the given session.
        """
        return self.session_runtime.get_state(session_id)

    def rebuild_alias_table(
        self,
        session_id: str,
        *,
        now_ms: int,
        force: bool = False,
    ) -> tuple[SessionAliasTable, bool]:
        """Rebuild the session alias table from the current message pool.

        Checks whether the existing table is stale before rebuilding.
        When a rebuild occurs the session state is persisted automatically.

        Args:
            session_id: Target conversation session.
            now_ms: Current wall-clock time in milliseconds, used for
                staleness checks.
            force: Skip staleness checks and always rebuild.

        Returns:
            A ``(table, changed)`` tuple where *table* is the (possibly
            unchanged) alias table and *changed* indicates whether the
            rebuild produced new or modified entries.
        """
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
        """Sync a user's display name from the identity store into the alias table.

        Updates the alias entry so that prompt projections reflect the
        latest known display name for the given user.

        Args:
            session_id: Target conversation session.
            user_id: Identity whose display name should be refreshed.
            now_ms: Wall-clock time in milliseconds. Defaults to the
                current time when *None*.

        Returns:
            ``True`` if the display name changed and the session state
            was persisted; ``False`` otherwise.
        """
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
        """Build the full set of context-stage messages for prompt assembly.

        Orchestrates alias table refresh and context-stage projection to
        produce the ordered list of messages that will be injected into
        the prompt's context stage.

        Args:
            session_id: Target conversation session.
            self_platform_id: Platform identifier of the bot user, used
                to distinguish self-messages during projection.
            now_ms: Wall-clock time in milliseconds. Defaults to the
                current time when *None*.

        Returns:
            Ordered list of context-stage message dicts ready for prompt
            injection. An empty list is returned when *session_id* is
            empty.
        """
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

    def build_inactive_alias_context_message(
        self,
        session_id: str,
        *,
        unread_records: list[dict[str, Any]] | None = None,
        now_ms: int | None = None,
    ) -> dict[str, Any] | None:
        """Build the inactive-alias context message for a session.

        Produces a synthetic message summarising the alias context that
        is not directly quoted in the short-term conversation history.
        Triggers a full context-stage rebuild when the underlying blocks
        are stale.

        Args:
            session_id: Target conversation session.
            unread_records: Recent unread message records that should
                influence the inactive context snapshot.
            now_ms: Wall-clock time in milliseconds. Defaults to the
                current time when *None*.

        Returns:
            A message dict representing the inactive-alias context block,
            or *None* when no meaningful context is produced.
        """
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
        """Build the active-alias constraint text for prompt injection.

        Produces a plain-text block summarising the currently active
        (recently referenced) aliases so the model can refer to them
        correctly. Refreshes context blocks and the alias table when
        they are stale.

        Args:
            session_id: Target conversation session.
            unread_records: Recent unread message records that should
                influence the active alias snapshot.
            now_ms: Wall-clock time in milliseconds. Defaults to the
                current time when *None*.

        Returns:
            Constraint text string suitable for inclusion in the prompt's
            context stage.
        """
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
        """Persist the current session state to disk.

        Delegates to the session runtime to write the in-memory state
        snapshot to the backing store.

        Args:
            session_id: Target conversation session.
        """
        self.session_runtime.save(session_id)
