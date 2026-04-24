"""Runtime helper for short-term timeline block construction."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from shinbot.agent.context.alias_table import SessionAliasTable
from shinbot.agent.context.context_stage_builder import ContextStageBuilder
from shinbot.agent.context.projection import ContextProjectionState, block_to_prompt_message
from shinbot.agent.context.state_store import (
    ContextBlockState,
    ContextSessionState,
    ShortTermMemoryState,
)


@dataclass(slots=True)
class TimelineRun:
    """A contiguous run of timeline records with the same prompt role."""

    role: str
    records: list[dict[str, Any]]

    @classmethod
    def from_records(cls, records: list[dict[str, Any]]) -> list[TimelineRun]:
        runs: list[TimelineRun] = []
        current_role = ""
        current_records: list[dict[str, Any]] = []

        for record in records:
            role = cls.normalize_role(record)
            if not current_records:
                current_role = role
                current_records = [record]
                continue
            if role == current_role:
                current_records.append(record)
                continue
            runs.append(cls(role=current_role, records=current_records))
            current_role = role
            current_records = [record]

        if current_records:
            runs.append(cls(role=current_role, records=current_records))
        return runs

    @staticmethod
    def normalize_role(record: dict[str, Any]) -> str:
        role = str(record.get("role", "") or "").strip()
        return "assistant" if role == "assistant" else "user"


@dataclass(slots=True)
class ContextTimelineRuntime:
    """Build and incrementally refresh short-term timeline prompt blocks."""

    builder: ContextStageBuilder

    def build_prompt_messages(
        self,
        read_history: list[dict[str, Any]],
        *,
        alias_table: SessionAliasTable,
        session_state: ContextSessionState,
        force_rebuild: bool = False,
        self_platform_id: str = "",
    ) -> list[dict[str, Any]]:
        existing_blocks = session_state.legacy_blocks()
        latest_history_id = latest_record_id(read_history)
        latest_block_id = latest_block_record_id(existing_blocks)

        if force_rebuild or not existing_blocks:
            rebuilt_blocks = self.build_blocks(
                read_history,
                alias_table=alias_table,
                session_state=session_state,
                self_platform_id=self_platform_id,
            )
            session_state.set_legacy_blocks(rebuilt_blocks)
            return blocks_to_prompt_messages(rebuilt_blocks)

        if latest_history_id > latest_block_id:
            reusable_blocks, mutable_history = split_context_rebuild_scope(
                session_state.short_term_memory(),
                read_history,
            )
            rebuilt_tail = self.build_blocks(
                mutable_history,
                alias_table=alias_table,
                session_state=session_state,
                self_platform_id=self_platform_id,
                start_block_index=len(reusable_blocks),
            )
            next_blocks = [*reusable_blocks, *rebuilt_tail]
            session_state.set_legacy_blocks(next_blocks)
            return blocks_to_prompt_messages(next_blocks)

        return blocks_to_prompt_messages(existing_blocks)

    def build_blocks(
        self,
        records: list[dict[str, Any]],
        *,
        alias_table: SessionAliasTable,
        session_state: ContextSessionState,
        self_platform_id: str = "",
        start_block_index: int = 0,
    ) -> list[ContextBlockState]:
        if not records:
            return []

        blocks: list[ContextBlockState] = []
        projection_state = ContextProjectionState.from_session_state(
            session_state=session_state,
            image_registry=self.builder.image_registry,
        )
        for run in TimelineRun.from_records(records):
            if run.role == "assistant":
                blocks.extend(
                    self.builder.build_assistant_blocks(
                        run.records,
                        alias_table=alias_table,
                        projection_state=projection_state,
                        self_platform_id=self_platform_id,
                        start_block_index=start_block_index + len(blocks),
                    )
                )
            else:
                blocks.extend(
                    self.builder.build_blocks(
                        run.records,
                        alias_table=alias_table,
                        projection_state=projection_state,
                        self_platform_id=self_platform_id,
                        start_block_index=start_block_index + len(blocks),
                    )
                )

        for index, block in enumerate(blocks):
            block.sealed = index < len(blocks) - 1
        return blocks


def latest_record_id(records: list[dict[str, Any]]) -> int:
    record_ids = [int(item["id"]) for item in records if isinstance(item.get("id"), int)]
    return max(record_ids, default=0)


def latest_block_record_id(blocks: list[ContextBlockState]) -> int:
    latest = 0
    for block in blocks:
        record_ids = block.metadata.get("record_ids", [])
        if not isinstance(record_ids, list):
            continue
        numeric_ids = [int(item) for item in record_ids if isinstance(item, int)]
        if numeric_ids:
            latest = max(latest, max(numeric_ids))
    return latest


def split_context_rebuild_scope(
    memory: ShortTermMemoryState,
    read_history: list[dict[str, Any]],
) -> tuple[list[ContextBlockState], list[dict[str, Any]]]:
    if not memory.has_blocks():
        return [], list(read_history)

    reusable_blocks = memory.cacheable_prefix_blocks()
    reusable_latest_id = latest_block_record_id(reusable_blocks)
    if reusable_latest_id <= 0:
        return reusable_blocks, list(read_history)

    mutable_history = [
        record
        for record in read_history
        if isinstance(record.get("id"), int) and int(record["id"]) > reusable_latest_id
    ]
    return reusable_blocks, mutable_history


def blocks_to_prompt_messages(blocks: list[ContextBlockState]) -> list[dict[str, Any]]:
    return [block_to_prompt_message(block) for block in blocks]


def group_records_by_timeline_role(records: list[dict[str, Any]]) -> list[TimelineRun]:
    return TimelineRun.from_records(records)
