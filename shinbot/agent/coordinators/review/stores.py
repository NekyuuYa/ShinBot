"""Store ports used by Agent review coordinators."""

from __future__ import annotations

from typing import Any, Protocol

from shinbot.agent.coordinators.review.models import UnreadRangeSummaryRecord
from shinbot.agent.scheduler.models import UnreadRange

MessageLogPayload = dict[str, Any]


class ReviewMessageStore(Protocol):
    """Read message logs for review stages without constructing prompts."""

    def list_for_unread_range(
        self,
        unread_range: UnreadRange,
        *,
        limit: int,
        offset: int = 0,
    ) -> list[MessageLogPayload]:
        """Read messages inside one unread range in chronological order."""

    def list_around_message(
        self,
        *,
        session_id: str,
        message_log_id: int,
        before: int,
        after: int,
    ) -> list[MessageLogPayload]:
        """Read a local context window around a candidate message."""

    def list_by_time(
        self,
        *,
        session_id: str,
        start_at: float,
        end_at: float,
        limit: int,
    ) -> list[MessageLogPayload]:
        """Read messages in a timestamp window in chronological order."""


class ReviewSummaryStore(Protocol):
    """Stores Agent-owned summaries produced while reviewing unread ranges."""

    def save_summary(
        self,
        record: UnreadRangeSummaryRecord,
        *,
        created_at: float | None = None,
    ) -> int:
        """Persist a review summary and return its row id."""

    def list_summaries(
        self,
        session_id: str,
        *,
        limit: int = 50,
    ) -> list[UnreadRangeSummaryRecord]:
        """Return recent review summaries for one session in timeline order."""


__all__ = ["MessageLogPayload", "ReviewMessageStore", "ReviewSummaryStore"]
