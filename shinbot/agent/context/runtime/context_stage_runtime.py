"""Runtime coordinator for Stage 3 prompt memory context messages."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from shinbot.agent.context.projectors.alias_projector import AliasContextProjector
from shinbot.agent.context.projectors.compressed_memory_projector import CompressedMemoryProjector
from shinbot.agent.context.runtime.timeline_runtime import ContextTimelineRuntime
from shinbot.agent.context.state.alias_table import SessionAliasTable
from shinbot.agent.context.state.state_store import ContextSessionState


@dataclass(slots=True)
class ContextStageRuntime:
    """Coordinate compressed memories and short-term timeline projection."""

    timeline_runtime: ContextTimelineRuntime
    alias_projector: AliasContextProjector = field(default_factory=AliasContextProjector)
    compressed_memory_projector: CompressedMemoryProjector = field(
        default_factory=CompressedMemoryProjector
    )

    def build_messages(
        self,
        read_history: list[dict[str, Any]],
        *,
        alias_table: SessionAliasTable,
        session_state: ContextSessionState,
        alias_changed: bool = False,
        self_platform_id: str = "",
    ) -> list[dict[str, Any]]:
        existing_blocks = session_state.short_term_blocks()
        force_rebuild = alias_changed or not existing_blocks
        if force_rebuild:
            self.alias_projector.reset_inactive_snapshot(session_state)

        timeline_messages = self.timeline_runtime.build_prompt_messages(
            read_history,
            alias_table=alias_table,
            session_state=session_state,
            force_rebuild=force_rebuild,
            self_platform_id=self_platform_id,
        )
        compressed_messages = self.compressed_memory_projector.build_messages(
            session_state.compressed_memories
        )
        return [*compressed_messages, *timeline_messages]
