"""Prompt projection helpers for compressed mid-term memories."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from shinbot.agent.context.alias_table import SessionAliasTable
from shinbot.agent.context.projection import block_text_parts
from shinbot.agent.context.state_store import CompressedMemoryState, ContextBlockState

_MESSAGE_ALIAS_PREFIX_PATTERN = re.compile(r"^(\[msgid: \d+\])(?P<alias>[AP]\d+)(?=: )")


@dataclass(slots=True)
class CompressedMemoryProjector:
    """Project compressed memories and compression candidates into prompt text."""

    def build_messages(self, memories: list[CompressedMemoryState]) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = []
        for item in memories:
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

    def build_source_text(
        self,
        *,
        alias_table: SessionAliasTable,
        blocks: list[ContextBlockState],
    ) -> str:
        alias_lines = self._build_alias_lines(alias_table)
        context_lines: list[str] = []
        for block in blocks:
            for text in block_text_parts(block):
                context_lines.append(self.expand_aliases(text, alias_table))

        sections: list[str] = []
        if alias_lines:
            sections.append("### 成员映射\n" + "\n".join(alias_lines))
        if context_lines:
            sections.append("### 待压缩上下文\n" + "\n".join(context_lines))
        return "\n\n".join(sections).strip()

    def expand_aliases(self, text: str, alias_table: SessionAliasTable) -> str:
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

    def _build_alias_lines(self, alias_table: SessionAliasTable) -> list[str]:
        lines: list[str] = []
        alias_entries = sorted(
            alias_table.entries.values(),
            key=lambda item: (item.alias.startswith("P"), item.alias, item.platform_id),
        )
        for entry in alias_entries:
            alias_id = entry.alias.strip()
            if not alias_id:
                continue
            display_name = entry.display_name or entry.platform_id
            lines.append(f"{alias_id} = {display_name} / {entry.platform_id}")
        return lines
