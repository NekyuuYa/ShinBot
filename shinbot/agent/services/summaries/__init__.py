"""Unified summaries service for agent context compression.

Provides a single write/read surface for all summary types:
- overflow compression (review Stage 1A)
- block digest (review Stage 1B)
- active_chat summary
- compressed context summaries

Callers should go through ``SummaryService`` rather than the repository directly.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from .handoff import ReviewHandoffContext, SummaryHandoffEntry
from .markdown import MarkdownSummaryStore
from .models import SummaryRecord, SummaryType, SummaryWriteRequest

if TYPE_CHECKING:
    from shinbot.persistence.repositories.agent_summaries import AgentSummaryRepository

logger = logging.getLogger(__name__)


class SummaryService:
    """Public API for writing and querying agent summaries.

    Wraps :class:`AgentSummaryRepository` so that coordinator/workflow code
    never touches database internals.
    """

    def __init__(
        self,
        repository: Any,
        *,
        markdown_store: MarkdownSummaryStore | None = None,
    ) -> None:
        self._repo: AgentSummaryRepository = repository
        self._markdown_store = markdown_store

    # -- write --

    def save(self, request: SummaryWriteRequest) -> int:
        """Persist a summary record and return its id."""
        created_at = time.time()
        record_id = self._repo.save(request, created_at=created_at)
        self._save_markdown(record_id, request, created_at=created_at)
        return record_id

    def _save_markdown(
        self,
        record_id: int,
        request: SummaryWriteRequest,
        *,
        created_at: float,
    ) -> None:
        if self._markdown_store is None:
            return
        try:
            self._markdown_store.save(record_id, request, created_at=created_at)
        except Exception:
            logger.warning(
                "Failed to persist summary markdown for session=%s type=%s",
                request.session_id,
                request.summary_type.value,
                exc_info=True,
            )

    # -- read: session timeline --

    def list_by_session(
        self,
        session_id: str,
        *,
        summary_type: SummaryType | None = None,
        limit: int = 50,
    ) -> list[SummaryRecord]:
        """Return summaries for a session in chronological order."""
        return self._repo.get_by_session(
            session_id, summary_type=summary_type, limit=limit,
        )

    def get_latest_by_session(
        self,
        session_id: str,
        *,
        summary_type: SummaryType | None = None,
    ) -> SummaryRecord | None:
        """Return the newest summary for a session."""
        return self._repo.get_latest_by_session(
            session_id,
            summary_type=summary_type,
        )

    # -- read: review run --

    def list_by_run_id(
        self,
        source_run_id: str,
        *,
        summary_type: SummaryType | None = None,
    ) -> list[SummaryRecord]:
        """Return all summaries produced by a specific review run."""
        return self._repo.get_by_run_id(source_run_id, summary_type=summary_type)

    def get_block_digest(
        self,
        source_run_id: str,
        block_index: int,
    ) -> SummaryRecord | None:
        """Return a single block digest by run id and block index."""
        return self._repo.get_by_run_id_and_block(source_run_id, block_index)

    # -- read: message range overlap --

    def list_by_message_range(
        self,
        session_id: str,
        *,
        msg_log_start: int,
        msg_log_end: int,
        summary_type: SummaryType | None = None,
    ) -> list[SummaryRecord]:
        """Return summaries whose message range overlaps the given bounds."""
        return self._repo.get_by_message_range(
            session_id,
            msg_log_start=msg_log_start,
            msg_log_end=msg_log_end,
            summary_type=summary_type,
        )

    # -- convenience write helpers --

    def save_overflow_compression(
        self,
        session_id: str,
        source_run_id: str,
        content: str,
        *,
        msg_log_start: int | None = None,
        msg_log_end: int | None = None,
        msg_count: int = 0,
        metadata: dict[str, object] | None = None,
    ) -> int:
        """Shorthand for writing an overflow compression summary."""
        return self.save(SummaryWriteRequest(
            session_id=session_id,
            summary_type=SummaryType.OVERFLOW_COMPRESSION,
            content=content,
            source_run_id=source_run_id,
            msg_log_start=msg_log_start,
            msg_log_end=msg_log_end,
            msg_count=msg_count,
            metadata=metadata or {},
        ))

    def save_block_digest(
        self,
        session_id: str,
        source_run_id: str,
        block_index: int,
        content: str,
        *,
        msg_log_start: int | None = None,
        msg_log_end: int | None = None,
        msg_count: int = 0,
        metadata: dict[str, object] | None = None,
    ) -> int:
        """Shorthand for writing a block digest summary."""
        return self.save(SummaryWriteRequest(
            session_id=session_id,
            summary_type=SummaryType.BLOCK_DIGEST,
            content=content,
            source_run_id=source_run_id,
            block_index=block_index,
            msg_log_start=msg_log_start,
            msg_log_end=msg_log_end,
            msg_count=msg_count,
            metadata=metadata or {},
        ))

    def save_active_chat_summary(
        self,
        session_id: str,
        source_run_id: str,
        content: str,
        *,
        msg_log_start: int | None = None,
        msg_log_end: int | None = None,
        msg_count: int = 0,
        metadata: dict[str, object] | None = None,
    ) -> int:
        """Shorthand for writing an active_chat summary."""
        return self.save(SummaryWriteRequest(
            session_id=session_id,
            summary_type=SummaryType.ACTIVE_CHAT,
            content=content,
            source_run_id=source_run_id,
            msg_log_start=msg_log_start,
            msg_log_end=msg_log_end,
            msg_count=msg_count,
            metadata=metadata or {},
        ))

    def save_compressed_context(
        self,
        session_id: str,
        source_run_id: str,
        content: str,
        *,
        metadata: dict[str, object] | None = None,
    ) -> int:
        """Shorthand for writing a compressed context summary."""
        return self.save(SummaryWriteRequest(
            session_id=session_id,
            summary_type=SummaryType.COMPRESSED_CONTEXT,
            content=content,
            source_run_id=source_run_id,
            metadata=metadata or {},
        ))


__all__ = [
    "SummaryRecord",
    "MarkdownSummaryStore",
    "ReviewHandoffContext",
    "SummaryService",
    "SummaryHandoffEntry",
    "SummaryType",
    "SummaryWriteRequest",
]
