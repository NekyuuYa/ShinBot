"""Context-building adapter boundary for Agent review workflow stages."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from shinbot.agent.coordinators.review.stores import MessageLogPayload


@dataclass(slots=True, frozen=True)
class ReviewContextBuildOptions:
    """Optional controls for building one review stage input."""

    instance_id: str = ""
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
    instance_id: str = ""
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

    def __init__(self, context_manager: Any = None) -> None:
        self._context_manager = context_manager

    def build_for_messages(
        self,
        *,
        session_id: str,
        messages: list[MessageLogPayload],
        purpose: str,
        options: ReviewContextBuildOptions | None = None,
    ) -> ReviewStageInput:
        """Build structured review stage input from message records.

        Args:
            session_id: Conversation session identifier.
            messages: Message log payloads to include.
            purpose: Human-readable purpose for this context build.
            options: Optional build controls.

        Returns:
            Structured input for one review workflow stage.
        """
        resolved_options = options or ReviewContextBuildOptions()
        metadata = {"purpose": purpose, **resolved_options.metadata}
        return ReviewStageInput(
            session_id=session_id,
            purpose=purpose,
            source_messages=list(messages),
            instance_id=resolved_options.instance_id,
            metadata=metadata,
        )


__all__ = [
    "ReviewContextBuildOptions",
    "ReviewContextBuilder",
    "ReviewContextBuilderAdapter",
    "ReviewStageInput",
]
