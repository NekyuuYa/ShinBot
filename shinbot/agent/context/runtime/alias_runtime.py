"""Runtime coordinator for session alias rebuild and prompt projections."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from shinbot.agent.context.projectors.alias_projector import AliasContextProjector
from shinbot.agent.context.state.alias_table import SessionAliasTable
from shinbot.agent.context.state.state_store import ContextBlockState, ContextSessionState

if TYPE_CHECKING:
    from shinbot.agent.identity.store import IdentityStore


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
        was_frozen = state.inactive_alias_table_frozen
        message = self.projector.build_inactive_context_message(
            state=state,
            blocks=blocks,
            unread_records=unread_records,
        )
        changed = not was_frozen and state.inactive_alias_table_frozen
        return message, changed

    def needs_active_context_refresh(self, blocks: list[ContextBlockState]) -> bool:
        return not blocks

    def needs_active_alias_rebuild(self, table: SessionAliasTable, now_ms: int) -> bool:
        return self.needs_table_rebuild(table, now_ms)

    def build_active_constraint_text(
        self,
        *,
        alias_table: SessionAliasTable,
        blocks: list[ContextBlockState],
        unread_records: list[dict[str, Any]] | None = None,
    ) -> str:
        return self.projector.build_active_constraint_text(
            alias_table=alias_table,
            blocks=blocks,
            unread_records=unread_records,
        )
