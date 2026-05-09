"""Context-building boundary for Agent active chat workflows."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(slots=True, frozen=True)
class ActiveChatContextBuildOptions:
    """Optional controls for building active chat prompt-adjacent context."""

    self_platform_id: str = ""
    previous_summary: str = ""
    now_ms: int | None = None
    include_context_stage: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class ActiveChatStageInput:
    """Structured input prepared for one active chat stage."""

    session_id: str
    purpose: str
    source_messages: list[dict[str, Any]]
    instruction_content: list[dict[str, Any]] = field(default_factory=list)
    context_messages: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


class ActiveChatContextBuilder(Protocol):
    """Build structured active chat stage input from selected message records."""

    def build_for_messages(
        self,
        *,
        session_id: str,
        messages: list[dict[str, Any]],
        purpose: str,
        options: ActiveChatContextBuildOptions | None = None,
    ) -> ActiveChatStageInput:
        """Return prompt-adjacent structured input for one active chat stage."""


class ActiveChatContextBuilderAdapter:
    """Thin adapter over existing ContextManager construction surfaces."""

    def __init__(self, context_manager=None) -> None:
        self._context_manager = context_manager

    def build_for_messages(
        self,
        *,
        session_id: str,
        messages: list[dict[str, Any]],
        purpose: str,
        options: ActiveChatContextBuildOptions | None = None,
    ) -> ActiveChatStageInput:
        resolved_options = options or ActiveChatContextBuildOptions()
        metadata = {"purpose": purpose, **resolved_options.metadata}
        if self._context_manager is None:
            return ActiveChatStageInput(
                session_id=session_id,
                purpose=purpose,
                source_messages=list(messages),
                metadata=metadata,
            )

        now_ms = resolved_options.now_ms
        if now_ms is None:
            now_ms = int(time.time() * 1000)
        instruction_content = self._context_manager.build_instruction_stage_content(
            session_id,
            list(messages),
            previous_summary=resolved_options.previous_summary,
            self_platform_id=resolved_options.self_platform_id,
            now_ms=now_ms,
        )
        context_messages = []
        if resolved_options.include_context_stage:
            context_messages = self._context_manager.build_context_stage_messages(
                session_id,
                self_platform_id=resolved_options.self_platform_id,
                now_ms=now_ms,
            )
        return ActiveChatStageInput(
            session_id=session_id,
            purpose=purpose,
            source_messages=list(messages),
            instruction_content=instruction_content,
            context_messages=context_messages,
            metadata=metadata,
        )


__all__ = [
    "ActiveChatContextBuilder",
    "ActiveChatContextBuilderAdapter",
    "ActiveChatContextBuildOptions",
    "ActiveChatStageInput",
]
