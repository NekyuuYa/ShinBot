"""Prompt-facing context projection contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class PromptMemoryProjectionRequest:
    """Inputs needed to project session memory into prompt stages."""

    session_id: str
    unread_records: list[dict[str, Any]] = field(default_factory=list)
    previous_summary: str = ""
    self_platform_id: str = ""
    now_ms: int | None = None


@dataclass(slots=True)
class PromptMemoryBundle:
    """Context layer output consumed by PromptRegistry."""

    context_messages: list[dict[str, Any]] = field(default_factory=list)
    instruction_blocks: list[dict[str, Any]] = field(default_factory=list)
    constraint_text: str = ""
    cacheable_message_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)
