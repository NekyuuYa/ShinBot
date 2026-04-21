"""Context input normalization helpers for prompt management."""

from __future__ import annotations

from typing import Any


def normalize_history_turns(context_inputs: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalize heterogeneous history_turn inputs into registry turn objects."""

    raw_turns = context_inputs.get("history_turns", [])
    if not isinstance(raw_turns, list):
        return []

    turns: list[dict[str, Any]] = []
    for item in raw_turns:
        if isinstance(item, str):
            content = item.strip()
            if content:
                turns.append({"role": "", "content": content})
            continue
        if not isinstance(item, dict):
            continue
        role = str(item.get("role", "")).strip()
        raw_content = item.get("content", "")
        if isinstance(raw_content, list):
            content_parts: list[str] = []
            for block in raw_content:
                if isinstance(block, dict):
                    block_text = block.get("text")
                    if isinstance(block_text, str) and block_text.strip():
                        content_parts.append(block_text.strip())
            content = "\n".join(content_parts).strip()
        else:
            content = str(raw_content).strip()
        if not content:
            continue
        turn: dict[str, Any] = {"role": role, "content": content}
        sender_id = str(
            item.get("sender_id", item.get("senderId", item.get("name", ""))) or ""
        ).strip()
        if sender_id:
            turn["sender_id"] = sender_id
        sender_name = str(item.get("sender_name", item.get("senderName", "")) or "").strip()
        if sender_name:
            turn["sender_name"] = sender_name
        platform = str(item.get("platform", "") or "").strip()
        if platform:
            turn["platform"] = platform
        turns.append(turn)
    return turns


