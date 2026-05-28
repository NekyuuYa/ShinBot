"""Prompt-facing alias context projection helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from shinbot.agent.services.context.builders.message_parts import parse_message_parts
from shinbot.agent.services.context.projectors.headings import (
    ACTIVE_ALIAS_HEADING,
    INACTIVE_ALIAS_HEADING,
)
from shinbot.agent.services.context.state.alias_table import SessionAliasTable
from shinbot.agent.services.context.state.state_store import ContextBlockState, ContextSessionState


@dataclass(slots=True)
class AliasContextProjector:
    """Project session alias state into prompt context and constraints."""

    def build_inactive_context_message(
        self,
        *,
        state: ContextSessionState,
        blocks: list[ContextBlockState],
        unread_records: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any] | None:
        """Build a prompt context message summarising inactive (silent) users.

        Uses the cached snapshot when the inactive alias table is already
        frozen in *state*; otherwise collects current platform IDs, selects
        inactive entries, caches them, and builds the message.

        Args:
            state: Session-level context state whose inactive alias cache
                may be updated.
            blocks: All context blocks for the current session.
            unread_records: Optional unread message records whose senders
                are considered active.

        Returns:
            A ``{"role": "user", "content": [...]}`` message dict listing
            inactive aliases, or ``None`` when there are no inactive entries.
        """
        if state.inactive_alias_table_frozen:
            return self._build_inactive_context_message(state.inactive_alias_entries)

        current_platform_ids = self.collect_current_platform_ids(
            blocks,
            unread_records or [],
        )
        inactive_entries = self.select_inactive_entries(
            blocks,
            current_platform_ids=current_platform_ids,
        )
        state.inactive_alias_entries = inactive_entries
        state.inactive_alias_table_frozen = True
        return self._build_inactive_context_message(inactive_entries)

    def build_active_constraint_text(
        self,
        *,
        alias_table: SessionAliasTable,
        blocks: list[ContextBlockState],
        unread_records: list[dict[str, Any]] | None = None,
    ) -> str:
        """Build a text block listing currently active user aliases.

        Active users are those with ``A``-prefixed aliases or whose platform
        IDs appear in the current (unsealed) context blocks or unread records.

        Args:
            alias_table: The session alias table to draw entries from.
            blocks: All context blocks for the current session.
            unread_records: Optional unread message records whose senders
                are considered active.

        Returns:
            A newline-separated string with a heading followed by alias
            mapping lines, or an empty string when no active entries exist.
        """
        active_entries = self.select_active_entries(
            alias_table,
            current_platform_ids=self.collect_current_platform_ids(blocks, unread_records or []),
        )
        if not active_entries:
            return ""
        lines = [
            ACTIVE_ALIAS_HEADING,
            "如需称呼用户，优先使用有意义的称呼(display_name)而非代称。",
        ]
        for entry in active_entries:
            alias_id = entry.alias or entry.platform_id
            display_name = entry.display_name or entry.platform_id
            lines.append(f"{alias_id} = {display_name} / {entry.platform_id}")
        return "\n".join(lines)

    def reset_inactive_snapshot(self, state: ContextSessionState) -> None:
        """Clear the cached inactive alias snapshot in *state*.

        This unfreezes the inactive alias table so the next call to
        :meth:`build_inactive_context_message` will recompute the entries
        from the current blocks.

        Args:
            state: Session-level context state whose inactive alias cache
                should be cleared.
        """
        state.inactive_alias_entries = []
        state.inactive_alias_table_frozen = False

    def collect_current_platform_ids(
        self,
        blocks: list[ContextBlockState],
        unread_records: list[dict[str, Any]],
    ) -> set[str]:
        """Gather platform IDs of users active in the current session window.

        Scans unsealed context blocks for alias entries and unread message
        records for sender / part platform IDs.

        Args:
            blocks: All context blocks for the current session. Sealed
                blocks are skipped.
            unread_records: Unread message records whose senders and
                message-part participants are considered active.

        Returns:
            A set of non-empty platform ID strings found in the active
            context.
        """
        current_platform_ids: set[str] = set()

        for block in blocks:
            if block.sealed:
                continue
            for entry in self.extract_block_alias_entries(block):
                platform_id = str(entry.get("platform_id", "") or "").strip()
                if platform_id:
                    current_platform_ids.add(platform_id)

        for record in unread_records:
            sender_id = str(record.get("sender_id", "") or "").strip()
            if sender_id:
                current_platform_ids.add(sender_id)
            for part in parse_message_parts(record):
                platform_id = str(part.platform_id or "").strip()
                if platform_id:
                    current_platform_ids.add(platform_id)

        return current_platform_ids

    def select_inactive_entries(
        self,
        blocks: list[ContextBlockState],
        *,
        current_platform_ids: set[str],
    ) -> list[dict[str, str]]:
        """Select alias entries from sealed blocks whose users are silent.

        An entry is considered *inactive* when it has a ``P``-prefixed alias,
        a non-empty platform ID, and that platform ID is absent from
        *current_platform_ids*.

        Args:
            blocks: All context blocks for the current session. Only sealed
                (archived) blocks are inspected.
            current_platform_ids: Platform IDs of users currently active in
                the session; these are excluded from the result.

        Returns:
            A sorted list of dicts with ``alias``, ``platform_id``, and
            ``display_name`` keys.
        """
        archived_by_platform_id: dict[str, dict[str, str]] = {}
        for block in blocks:
            if not block.sealed:
                continue
            for entry in self.extract_block_alias_entries(block):
                alias = str(entry.get("alias", "") or "").strip()
                platform_id = str(entry.get("platform_id", "") or "").strip()
                if not alias.startswith("P") or not platform_id or platform_id in current_platform_ids:
                    continue
                archived_by_platform_id.setdefault(
                    platform_id,
                    {
                        "alias": alias,
                        "platform_id": platform_id,
                        "display_name": str(entry.get("display_name", "") or platform_id).strip()
                        or platform_id,
                    },
                )

        return sorted(
            archived_by_platform_id.values(),
            key=lambda item: (
                str(item.get("alias", "") or ""),
                str(item.get("platform_id", "") or ""),
            ),
        )

    def select_active_entries(
        self,
        alias_table: SessionAliasTable,
        *,
        current_platform_ids: set[str],
    ) -> list[Any]:
        """Select alias entries for users who are currently active.

        An entry is considered *active* when it has an ``A``-prefixed alias
        or its platform ID is present in *current_platform_ids*.

        Args:
            alias_table: The session alias table to draw entries from.
            current_platform_ids: Platform IDs of users currently active in
                the session.

        Returns:
            A sorted list of alias-entry objects, ordered with ``P``-prefixed
            aliases last, then by alias and platform ID.
        """
        active_entries = []
        for entry in alias_table.entries.values():
            alias = entry.alias.strip()
            if not alias:
                continue
            if alias.startswith("A") or entry.platform_id in current_platform_ids:
                active_entries.append(entry)

        active_entries.sort(key=lambda item: (item.alias.startswith("P"), item.alias, item.platform_id))
        return active_entries

    def extract_block_alias_entries(self, block: ContextBlockState) -> list[dict[str, Any]]:
        """Extract alias-entry dicts from a context block's metadata.

        Looks for the ``alias_entries`` key in the block metadata and returns
        only the items that are dicts.

        Args:
            block: The context block whose metadata is inspected.

        Returns:
            A list of alias-entry dicts, or an empty list if the metadata
            key is missing or not a list.
        """
        raw_entries = block.metadata.get("alias_entries", [])
        if not isinstance(raw_entries, list):
            return []
        return [entry for entry in raw_entries if isinstance(entry, dict)]

    def _build_inactive_context_message(
        self,
        entries: list[dict[str, str]],
    ) -> dict[str, Any] | None:
        if not entries:
            return None

        lines = [INACTIVE_ALIAS_HEADING]
        for entry in entries:
            alias_id = str(entry.get("alias", "") or entry.get("platform_id", "")).strip()
            platform_id = str(entry.get("platform_id", "") or "").strip()
            display_name = str(entry.get("display_name", "") or platform_id).strip() or platform_id
            if alias_id and platform_id:
                lines.append(f"{alias_id} = {display_name} / {platform_id}")
        if len(lines) == 1:
            return None
        return {"role": "user", "content": [{"type": "text", "text": "\n".join(lines)}]}
