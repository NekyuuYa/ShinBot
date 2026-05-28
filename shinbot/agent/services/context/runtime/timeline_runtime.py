"""Runtime helper for short-term timeline block construction."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from shinbot.agent.services.context.builders.context_stage_builder import ContextStageBuilder
from shinbot.agent.services.context.projectors.projection import (
    ContextProjectionState,
    block_to_prompt_message,
)
from shinbot.agent.services.context.state.alias_table import SessionAliasTable
from shinbot.agent.services.context.state.state_store import (
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
        """Split records into contiguous runs grouped by normalized role.

        Adjacent records sharing the same role (``user`` or ``assistant``)
        are collected into a single :class:`TimelineRun`.  Role values are
        normalised via :meth:`normalize_role`.

        Args:
            records: Ordered list of timeline record dicts.

        Returns:
            A list of :class:`TimelineRun` objects, one per contiguous
            role segment.
        """
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
        """Return the normalised role for a single record.

        Only the literal ``"assistant"`` role is preserved; every other
        value (including empty or missing) is mapped to ``"user"``.

        Args:
            record: A timeline record dict containing an optional
                ``role`` key.

        Returns:
            ``"assistant"`` if the record's role is exactly
            ``"assistant"``, otherwise ``"user"``.
        """
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
        """Build or incrementally refresh prompt messages from read history.

        When *force_rebuild* is ``True`` or no cached blocks exist yet,
        the entire history is rebuilt from scratch.  Otherwise, if the
        history contains records newer than the last cached block, only
        the tail is rebuilt and the remaining prefix blocks are reused.

        The resulting blocks are persisted in *session_state* so that
        subsequent calls can perform incremental updates.

        Args:
            read_history: Full ordered list of timeline records.
            alias_table: Alias mapping for user / channel resolution.
            session_state: Mutable session state holding cached blocks.
            force_rebuild: Force a full rebuild regardless of cache.
            self_platform_id: Platform identifier for the bot's own
                messages (used to distinguish self from others).

        Returns:
            A list of prompt-message dicts ready for the LLM.
        """
        existing_blocks = session_state.short_term_blocks()
        latest_history_id = latest_record_id(read_history)
        latest_block_id = latest_block_record_id(existing_blocks)

        if force_rebuild or not existing_blocks:
            rebuilt_blocks = self.build_blocks(
                read_history,
                alias_table=alias_table,
                session_state=session_state,
                self_platform_id=self_platform_id,
            )
            session_state.set_short_term_blocks(rebuilt_blocks)
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
            session_state.set_short_term_blocks(next_blocks)
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
        """Convert raw timeline records into context blocks.

        Records are grouped into contiguous role runs via
        :meth:`TimelineRun.from_records` and each run is delegated to the
        appropriate builder method.  All blocks except the very last one
        are marked as *sealed*.

        Args:
            records: Ordered list of timeline record dicts.
            alias_table: Alias mapping for user / channel resolution.
            session_state: Current session state (used to initialise the
                projection state).
            self_platform_id: Platform identifier for the bot's own
                messages.
            start_block_index: Starting index offset for block indexing
                (useful when appending to an existing prefix).

        Returns:
            A list of :class:`ContextBlockState` objects.
        """
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
    """Return the highest numeric ``id`` found in *records*.

    Args:
        records: Ordered list of timeline record dicts, each potentially
            containing an integer ``id`` key.

    Returns:
        The maximum ``id`` value, or ``0`` if no numeric IDs are present.
    """
    record_ids = [int(item["id"]) for item in records if isinstance(item.get("id"), int)]
    return max(record_ids, default=0)


def latest_block_record_id(blocks: list[ContextBlockState]) -> int:
    """Return the highest record ID referenced across all blocks.

    Inspects the ``record_ids`` metadata entry of each block and
    returns the largest integer found.

    Args:
        blocks: List of context block state objects.

    Returns:
        The maximum record ID, or ``0`` if no numeric IDs exist.
    """
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
    """Determine which blocks can be reused and which records need rebuilding.

    Splits the history into a prefix of reusable cached blocks and a
    tail of records whose IDs exceed the latest cached block's record
    ID.  Callers can rebuild only the tail and prepend the reusable
    prefix.

    Args:
        memory: Current short-term memory state containing cached
            blocks.
        read_history: Full ordered list of timeline records.

    Returns:
        A 2-tuple of ``(reusable_blocks, mutable_history)`` where
        *mutable_history* contains only the records that need to be
        rebuilt.
    """
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
    """Project context blocks into LLM-ready prompt messages.

    Args:
        blocks: List of context block state objects.

    Returns:
        A list of prompt-message dicts, one per block.
    """
    return [block_to_prompt_message(block) for block in blocks]


def group_records_by_timeline_role(records: list[dict[str, Any]]) -> list[TimelineRun]:
    """Group records into contiguous runs by normalised role.

    Thin wrapper around :meth:`TimelineRun.from_records`.

    Args:
        records: Ordered list of timeline record dicts.

    Returns:
        A list of :class:`TimelineRun` objects.
    """
    return TimelineRun.from_records(records)
