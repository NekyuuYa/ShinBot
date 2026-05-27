"""Context-building boundary for Agent active chat workflows."""

from __future__ import annotations

import time
from dataclasses import dataclass, field, replace
from typing import Any, Protocol

from shinbot.agent.services.message_formatter import (
    MessageFormatConfig,
    MessageFormatterService,
)
from shinbot.utils.logger import format_log_event, get_logger

logger = get_logger(__name__, source="agent:context", color="cyan")


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

    def __init__(
        self,
        context_manager=None,
        *,
        message_formatter: MessageFormatterService | None = None,
        message_format_config: MessageFormatConfig | None = None,
    ) -> None:
        self._context_manager = context_manager
        self._message_formatter = message_formatter
        self._message_format_config = message_format_config

    def build_for_messages(
        self,
        *,
        session_id: str,
        messages: list[dict[str, Any]],
        purpose: str,
        options: ActiveChatContextBuildOptions | None = None,
    ) -> ActiveChatStageInput:
        """Build structured stage input from selected message records.

        Formats messages through the message formatter and collects
        context-manager stage messages when available.

        Args:
            session_id: Conversation session identifier.
            messages: Raw message records to include.
            purpose: Human-readable purpose for this context build.
            options: Optional build controls.

        Returns:
            Structured input for one active chat stage.
        """
        resolved_options = options or ActiveChatContextBuildOptions()
        metadata = {"purpose": purpose, **resolved_options.metadata}
        instruction_content = []
        if self._message_formatter is not None:
            try:
                instruction_content = self._message_formatter.format_instruction_content(
                    list(messages),
                    _message_format_config(
                        self._message_format_config,
                        self_platform_id=resolved_options.self_platform_id,
                        now_ms=resolved_options.now_ms,
                    ),
                    previous_summary=resolved_options.previous_summary,
                )
            except Exception as exc:
                logger.exception(
                    format_log_event(
                        "agent.active_chat.context.format_failed",
                        session_id=session_id,
                        purpose=purpose,
                        message_count=len(messages),
                        error_code=type(exc).__name__,
                        trace_id=str(metadata.get("trace_id") or ""),
                    )
                )

        now_ms = resolved_options.now_ms
        if now_ms is None:
            now_ms = int(time.time() * 1000)
        context_messages = []
        if self._context_manager is not None and resolved_options.include_context_stage:
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


def _message_format_config(
    config: MessageFormatConfig | None,
    *,
    self_platform_id: str,
    now_ms: int | None,
) -> MessageFormatConfig:
    base = config or MessageFormatConfig(inject_record_id=True)
    return replace(
        base,
        self_platform_id=self_platform_id or base.self_platform_id,
        now_ms=now_ms if now_ms is not None else base.now_ms,
        inject_record_id=True,
    )
