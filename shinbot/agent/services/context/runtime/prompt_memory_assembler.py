"""Assemble prompt-facing memory bundles from context runtime projections."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from shinbot.agent.services.context.projectors.long_term_memory import (
    LongTermMemoryProjector,
    LongTermMemoryProvider,
    NoopLongTermMemoryProvider,
)
from shinbot.agent.services.context.projectors.projection import (
    PromptMemoryBundle,
    PromptMemoryProjectionRequest,
)
from shinbot.agent.services.message_formatter.models import MessageFormatConfig

if TYPE_CHECKING:
    from shinbot.agent.services.message_formatter import MessageFormatterService


class PromptMemoryRuntime(Protocol):
    """Runtime surface required by PromptMemoryAssembler."""

    def build_context_stage_messages(
        self,
        session_id: str,
        *,
        self_platform_id: str = "",
        now_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        """Build the context-stage messages for a session.

        Args:
            session_id: The session identifier to build context for.
            self_platform_id: Optional platform identifier for the bot itself.
            now_ms: Optional current timestamp in milliseconds.

        Returns:
            A list of context-stage message dicts.
        """
        ...

    def build_inactive_alias_context_message(
        self,
        session_id: str,
        *,
        unread_records: list[dict[str, Any]] | None = None,
        now_ms: int | None = None,
    ) -> dict[str, Any] | None:
        """Build a context message for inactive alias context.

        Args:
            session_id: The session identifier to build context for.
            unread_records: Optional list of unread message records.
            now_ms: Optional current timestamp in milliseconds.

        Returns:
            A context message dict, or None if no inactive alias context exists.
        """
        ...

    def get_cacheable_context_message_count(self, session_id: str) -> int:
        """Return the number of context messages eligible for caching.

        Args:
            session_id: The session identifier to query.

        Returns:
            The count of cacheable context messages.
        """
        ...

    def build_active_alias_constraint_text(
        self,
        session_id: str,
        *,
        unread_records: list[dict[str, Any]] | None = None,
        now_ms: int | None = None,
    ) -> str:
        """Build constraint text for the active alias.

        Args:
            session_id: The session identifier to build constraints for.
            unread_records: Optional list of unread message records.
            now_ms: Optional current timestamp in milliseconds.

        Returns:
            A constraint text string for the active alias.
        """
        ...


@dataclass(slots=True)
class PromptMemoryAssembler:
    """Assemble the context layer output consumed by PromptRegistry."""

    runtime: PromptMemoryRuntime
    message_formatter: MessageFormatterService | None = None
    long_term_provider: LongTermMemoryProvider = field(
        default_factory=NoopLongTermMemoryProvider
    )
    long_term_projector: LongTermMemoryProjector = field(default_factory=LongTermMemoryProjector)

    def assemble(self, request: PromptMemoryProjectionRequest) -> PromptMemoryBundle:
        """Assemble a complete prompt memory bundle from a projection request.

        Combines long-term memory messages, context-stage messages, inactive
        alias context, formatted instruction blocks, and active alias
        constraint text into a single ``PromptMemoryBundle``.

        Args:
            request: The projection request containing session info,
                unread records, and other parameters needed for assembly.

        Returns:
            A ``PromptMemoryBundle`` with all assembled context components.
        """
        if not request.session_id:
            return PromptMemoryBundle()

        long_term_messages = self.long_term_projector.build_messages(
            self.long_term_provider.retrieve(request)
        )
        context_messages = self.runtime.build_context_stage_messages(
            request.session_id,
            self_platform_id=request.self_platform_id,
            now_ms=request.now_ms,
        )
        inactive_alias_message = self.runtime.build_inactive_alias_context_message(
            request.session_id,
            unread_records=request.unread_records,
            now_ms=request.now_ms,
        )
        cacheable_message_count = self.runtime.get_cacheable_context_message_count(
            request.session_id
        )
        if inactive_alias_message is not None:
            context_messages = [inactive_alias_message, *context_messages]
            cacheable_message_count += 1
        if long_term_messages:
            context_messages = [*long_term_messages, *context_messages]

        instruction_blocks: list[dict[str, Any]] = []
        message_formatter = self.message_formatter
        if request.unread_records and message_formatter is None:
            from shinbot.agent.services.message_formatter import MessageFormatterService

            message_formatter = MessageFormatterService()
        if request.unread_records and message_formatter is not None:
            instruction_blocks = message_formatter.format_instruction_content(
                request.unread_records,
                MessageFormatConfig(
                    self_platform_id=request.self_platform_id,
                    now_ms=request.now_ms,
                    inject_record_id=True,
                ),
                previous_summary=request.previous_summary,
            )

        constraint_text = self.runtime.build_active_alias_constraint_text(
            request.session_id,
            unread_records=request.unread_records,
            now_ms=request.now_ms,
        )
        return PromptMemoryBundle(
            context_messages=context_messages,
            instruction_blocks=instruction_blocks,
            constraint_text=constraint_text,
            cacheable_message_count=cacheable_message_count,
            metadata={
                "session_id": request.session_id,
                "message_count": len(request.unread_records),
            },
        )
