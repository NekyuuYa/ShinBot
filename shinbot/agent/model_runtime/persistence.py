"""Persistence helpers for model runtime audit records."""

from __future__ import annotations

import logging
from typing import Any

from shinbot.persistence import AIInteractionRecord, ModelExecutionRecord

logger = logging.getLogger(__name__)


def persist_model_execution(
    database: Any,
    record: ModelExecutionRecord,
) -> None:
    """Persist one model execution audit record."""

    if database is None:
        return
    try:
        database.model_executions.insert(record)
    except Exception:
        logger.exception(
            "Failed to persist model execution %s (caller=%s, success=%s);"
            " API quota may have been consumed without a corresponding record",
            record.id,
            record.caller,
            record.success,
        )


def persist_ai_interaction(
    database: Any,
    record: AIInteractionRecord,
) -> None:
    """Persist one AI interaction summary record."""

    if database is None:
        return
    try:
        database.ai_interactions.insert(record)
    except Exception:
        logger.exception(
            "Failed to persist AI interaction for execution %s",
            record.execution_id,
        )
