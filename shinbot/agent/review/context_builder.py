"""Context-building adapter boundary for Agent review workflow stages."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Protocol

from shinbot.agent.review.message_store import MessageLogPayload


@dataclass(slots=True, frozen=True)
class ReviewContextBuildOptions:
    """Optional controls for building one review stage input."""

    self_platform_id: str = ""
    previous_summary: str = ""
    now_ms: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class ReviewStageInput:
    """Structured input prepared for one review workflow stage."""

    session_id: str
    purpose: str
    source_messages: list[MessageLogPayload]
    instruction_content: list[dict[str, Any]] = field(default_factory=list)
    context_messages: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


class ReviewContextBuilder(Protocol):
    """Build structured stage input from selected message records."""

    def build_for_messages(
        self,
        *,
        session_id: str,
        messages: list[MessageLogPayload],
        purpose: str,
        options: ReviewContextBuildOptions | None = None,
    ) -> ReviewStageInput:
        """Return prompt-adjacent structured input for one review stage."""


class ReviewContextBuilderAdapter:
    """Thin adapter over existing ContextManager construction surfaces."""

    def __init__(self, context_manager=None) -> None:
        self._context_manager = context_manager

    def build_for_messages(
        self,
        *,
        session_id: str,
        messages: list[MessageLogPayload],
        purpose: str,
        options: ReviewContextBuildOptions | None = None,
    ) -> ReviewStageInput:
        resolved_options = options or ReviewContextBuildOptions()
        metadata = {"purpose": purpose, **resolved_options.metadata}
        if self._context_manager is None:
            return ReviewStageInput(
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
        return ReviewStageInput(
            session_id=session_id,
            purpose=purpose,
            source_messages=list(messages),
            instruction_content=instruction_content,
            metadata=metadata,
        )


__all__ = [
    "ReviewContextBuildOptions",
    "ReviewContextBuilder",
    "ReviewContextBuilderAdapter",
    "ReviewStageInput",
]
