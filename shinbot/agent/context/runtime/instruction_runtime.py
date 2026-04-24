"""Runtime helper for projecting current work input into instruction blocks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from shinbot.agent.context.builders.instruction_stage_builder import InstructionStageBuilder
from shinbot.agent.context.projectors.projection import ContextProjectionState
from shinbot.agent.context.state.alias_table import SessionAliasTable
from shinbot.agent.context.state.state_store import ContextSessionState


@dataclass(slots=True)
class InstructionRuntime:
    """Project unread/current-turn records into instruction-stage content blocks."""

    builder: InstructionStageBuilder

    def build_content_blocks(
        self,
        unread_records: list[dict[str, Any]],
        *,
        alias_table: SessionAliasTable,
        session_state: ContextSessionState,
        previous_summary: str = "",
        self_platform_id: str = "",
        now_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        return self.builder.build_content_blocks(
            unread_records,
            alias_table=alias_table,
            projection_state=ContextProjectionState.from_session_state(
                session_state=session_state,
                image_registry=self.builder.image_registry,
            ),
            previous_summary=previous_summary,
            self_platform_id=self_platform_id,
            now_ms=now_ms,
        )
