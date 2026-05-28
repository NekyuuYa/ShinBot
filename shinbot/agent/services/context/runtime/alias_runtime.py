"""Runtime coordinator for session alias rebuild and prompt projections."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from shinbot.agent.services.context.projectors.alias_projector import AliasContextProjector
from shinbot.agent.services.context.state.alias_table import SessionAliasTable
from shinbot.agent.services.context.state.state_store import ContextBlockState, ContextSessionState

if TYPE_CHECKING:
    from shinbot.agent.services.identity.store import IdentityStore


@dataclass(slots=True)
class ContextAliasRuntime:
    """Coordinate alias table updates without owning storage or history retrieval."""

    projector: AliasContextProjector = field(default_factory=AliasContextProjector)

    def needs_table_rebuild(
        self,
        table: SessionAliasTable,
        now_ms: int,
        *,
        force: bool = False,
    ) -> bool:
        """Check whether the alias table needs to be rebuilt.

        Args:
            table: The session alias table to evaluate.
            now_ms: Current time in milliseconds for staleness checks.
            force: If True, always indicates a rebuild is needed.

        Returns:
            True if the table is empty, stale, or rebuild is forced.
        """
        return force or not table.entries or table.should_rebuild(now_ms)

    def rebuild_table(
        self,
        table: SessionAliasTable,
        messages: list[dict[str, Any]],
        *,
        now_ms: int,
        force: bool = False,
        identity_store: IdentityStore | None = None,
    ) -> tuple[SessionAliasTable, bool]:
        """Rebuild the alias table from conversation messages.

        Only performs a rebuild when the table is stale, empty, or forced.

        Args:
            table: The session alias table to rebuild in-place.
            messages: Conversation messages to extract user aliases from.
            now_ms: Current time in milliseconds for staleness checks.
            force: If True, bypass staleness checks and always rebuild.
            identity_store: Optional identity store for display name lookups.

        Returns:
            A tuple of (table, changed) where changed indicates whether any
            alias entries were modified during the rebuild.
        """
        if not self.needs_table_rebuild(table, now_ms, force=force):
            return table, False
        changed = table.rebuild_from_messages(
            messages,
            now_ms=now_ms,
            identity_store=identity_store,
        )
        return table, changed

    def sync_identity_display_name(
        self,
        table: SessionAliasTable,
        identity_store: IdentityStore | None,
        *,
        user_id: str,
        now_ms: int,
    ) -> bool:
        """Update an alias entry's display name from the identity store.

        Looks up the user in the identity store and applies the resolved
        display name to the corresponding alias table entry.

        Args:
            table: The session alias table containing the user's alias.
            identity_store: Identity store to resolve the display name from,
                or None to skip the update.
            user_id: The user ID whose display name should be synced.
            now_ms: Current time in milliseconds for staleness tracking.

        Returns:
            True if the alias entry was updated, False if no change was made
            or the identity/store/user_id was unavailable.
        """
        if identity_store is None:
            return False

        normalized_user_id = str(user_id or "").strip()
        if not normalized_user_id:
            return False

        identity = identity_store.get_identity(normalized_user_id)
        if identity is None:
            return False

        display_name = str(identity.get("name", "") or "").strip()
        if not display_name:
            return False

        return table.apply_identity_display_name(
            normalized_user_id,
            display_name,
            now_ms=now_ms,
        )

    def needs_inactive_context_refresh(
        self,
        *,
        state: ContextSessionState,
        blocks: list[ContextBlockState],
        now_ms: int,
    ) -> bool:
        """Check whether the inactive context summary needs a refresh.

        The inactive context needs refreshing when there are no blocks, the
        alias table has no entries, or the alias table is stale.

        Args:
            state: Current context session state holding the alias table.
            blocks: Existing context blocks to evaluate.
            now_ms: Current time in milliseconds for staleness checks.

        Returns:
            True if the inactive context should be refreshed.
        """
        return (
            not blocks
            or not state.alias_table.entries
            or state.alias_table.should_rebuild(now_ms)
        )

    def build_inactive_context_message(
        self,
        *,
        state: ContextSessionState,
        blocks: list[ContextBlockState],
        unread_records: list[dict[str, Any]] | None = None,
    ) -> tuple[dict[str, Any] | None, bool]:
        """Build an inactive context projection message.

        Delegates to the projector to create a context message summarising
        inactive (older) conversation blocks using the current alias table.

        Args:
            state: Current context session state.
            blocks: Context blocks to project into the inactive summary.
            unread_records: Optional unread message records to include.

        Returns:
            A tuple of (message, changed) where message is the projected
            context dict (or None if not applicable) and changed indicates
            whether the alias table was frozen as a side effect.
        """
        was_frozen = state.inactive_alias_table_frozen
        message = self.projector.build_inactive_context_message(
            state=state,
            blocks=blocks,
            unread_records=unread_records,
        )
        changed = not was_frozen and state.inactive_alias_table_frozen
        return message, changed

    def needs_active_context_refresh(self, blocks: list[ContextBlockState]) -> bool:
        """Check whether the active context needs a refresh.

        Args:
            blocks: Current list of active context blocks.

        Returns:
            True if there are no blocks and a refresh is needed.
        """
        return not blocks

    def needs_active_alias_rebuild(self, table: SessionAliasTable, now_ms: int) -> bool:
        """Check whether the active session's alias table needs rebuilding.

        Args:
            table: The session alias table to evaluate.
            now_ms: Current time in milliseconds for staleness checks.

        Returns:
            True if the alias table is empty, stale, or otherwise needs
            a rebuild.
        """
        return self.needs_table_rebuild(table, now_ms)

    def build_active_constraint_text(
        self,
        *,
        alias_table: SessionAliasTable,
        blocks: list[ContextBlockState],
        unread_records: list[dict[str, Any]] | None = None,
    ) -> str:
        """Build the active context constraint text with alias mappings.

        Produces the text snippet injected into the active context to
        constrain the model's understanding of user references.

        Args:
            alias_table: The session alias table with current mappings.
            blocks: Active context blocks to include.
            unread_records: Optional unread message records to include.

        Returns:
            The constraint text string for the active context.
        """
        return self.projector.build_active_constraint_text(
            alias_table=alias_table,
            blocks=blocks,
            unread_records=unread_records,
        )
