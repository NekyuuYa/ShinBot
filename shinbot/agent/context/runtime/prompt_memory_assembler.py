"""Assemble prompt-facing memory bundles from context runtime projections."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from shinbot.agent.context.projectors.long_term_memory import (
    LongTermMemoryProjector,
    LongTermMemoryProvider,
    NoopLongTermMemoryProvider,
)
from shinbot.agent.context.projectors.projection import (
    PromptMemoryBundle,
    PromptMemoryProjectionRequest,
)


class PromptMemoryRuntime(Protocol):
    """Runtime surface required by PromptMemoryAssembler."""

    def build_context_stage_messages(
        self,
        session_id: str,
        *,
        self_platform_id: str = "",
        now_ms: int | None = None,
    ) -> list[dict[str, Any]]: ...

    def build_inactive_alias_context_message(
        self,
        session_id: str,
        *,
        unread_records: list[dict[str, Any]] | None = None,
        now_ms: int | None = None,
    ) -> dict[str, Any] | None: ...

    def get_cacheable_context_message_count(self, session_id: str) -> int: ...

    def build_instruction_stage_content(
        self,
        session_id: str,
        unread_records: list[dict[str, Any]],
        *,
        previous_summary: str = "",
        self_platform_id: str = "",
        now_ms: int | None = None,
    ) -> list[dict[str, Any]]: ...

    def build_active_alias_constraint_text(
        self,
        session_id: str,
        *,
        unread_records: list[dict[str, Any]] | None = None,
        now_ms: int | None = None,
    ) -> str: ...


@dataclass(slots=True)
class PromptMemoryAssembler:
    """Assemble the context layer output consumed by PromptRegistry."""

    runtime: PromptMemoryRuntime
    long_term_provider: LongTermMemoryProvider = field(
        default_factory=NoopLongTermMemoryProvider
    )
    long_term_projector: LongTermMemoryProjector = field(default_factory=LongTermMemoryProjector)

    def assemble(self, request: PromptMemoryProjectionRequest) -> PromptMemoryBundle:
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
        if request.unread_records:
            instruction_blocks = self.runtime.build_instruction_stage_content(
                request.session_id,
                request.unread_records,
                previous_summary=request.previous_summary,
                self_platform_id=request.self_platform_id,
                now_ms=request.now_ms,
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
