"""Data models for the unified summaries service."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class SummaryType(StrEnum):
    """Semantic category of a summary record."""

    OVERFLOW_COMPRESSION = "overflow_compression"
    BLOCK_DIGEST = "block_digest"
    ACTIVE_CHAT = "active_chat"


@dataclass(slots=True, frozen=True)
class SummaryRecord:
    """An immutable summary record persisted in agent_summaries."""

    id: int
    session_id: str
    summary_type: SummaryType
    content: str
    source_run_id: str
    block_index: int | None = None
    msg_log_start: int | None = None
    msg_log_end: int | None = None
    msg_count: int = 0
    metadata_json: str = "{}"
    created_at: float = 0.0


@dataclass(slots=True, frozen=True)
class SummaryWriteRequest:
    """Input for writing a new summary record."""

    session_id: str
    summary_type: SummaryType
    content: str
    source_run_id: str
    block_index: int | None = None
    msg_log_start: int | None = None
    msg_log_end: int | None = None
    msg_count: int = 0
    metadata: dict[str, object] = field(default_factory=dict)
