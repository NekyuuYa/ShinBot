"""Prompt-facing alias context projection helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from shinbot.agent.context.alias_table import SessionAliasTable
from shinbot.agent.context.message_parts import parse_message_parts
from shinbot.agent.context.state_store import ContextBlockState, ContextSessionState


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
        active_entries = self.select_active_entries(
            alias_table,
            current_platform_ids=self.collect_current_platform_ids(blocks, unread_records or []),
        )
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

    def reset_inactive_snapshot(self, state: ContextSessionState) -> None:
        state.inactive_alias_entries = []
        state.inactive_alias_table_frozen = False

    def collect_current_platform_ids(
        self,
        blocks: list[ContextBlockState],
        unread_records: list[dict[str, Any]],
    ) -> set[str]:
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

        lines = ["### 会话历史成员映射"]
        for entry in entries:
            alias_id = str(entry.get("alias", "") or entry.get("platform_id", "")).strip()
            platform_id = str(entry.get("platform_id", "") or "").strip()
            display_name = str(entry.get("display_name", "") or platform_id).strip() or platform_id
            if alias_id and platform_id:
                lines.append(f"{alias_id} = {display_name} / {platform_id}")
        if len(lines) == 1:
            return None
        return {"role": "user", "content": [{"type": "text", "text": "\n".join(lines)}]}
