"""Summary handoff contracts shared by coordinators and workflows."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True, frozen=True)
class SummaryHandoffEntry:
    """One summary entry handed from review into active chat."""

    content: str
    block_index: int | None = None
    msg_log_start: int | None = None
    msg_log_end: int | None = None
    msg_count: int = 0


@dataclass(slots=True, frozen=True)
class ReviewHandoffContext:
    """Structured review summary context passed to active chat initialization."""

    review_run_id: str
    explanation: Any = None
    overflow_summaries: list[SummaryHandoffEntry] = field(default_factory=list)
    block_digests: list[SummaryHandoffEntry] = field(default_factory=list)
    recent_active_chat_summary: str | None = None


__all__ = ["ReviewHandoffContext", "SummaryHandoffEntry"]
