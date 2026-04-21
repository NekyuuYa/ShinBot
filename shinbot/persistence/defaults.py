"""Persistence-owned seed data for built-in records."""

from __future__ import annotations

from shinbot.persistence.records import ContextStrategyRecord
from shinbot.schema.context_strategies import (
    BUILTIN_SLIDING_WINDOW_CONTEXT_RESOLVER,
    BUILTIN_SLIDING_WINDOW_CONTEXT_STRATEGY_ID,
    DEFAULT_SLIDING_WINDOW_CONTEXT_BUDGET,
)


def builtin_sliding_window_context_strategy(now: str) -> ContextStrategyRecord:
    """Return the built-in sliding-window context strategy seed record."""

    return ContextStrategyRecord(
        uuid=BUILTIN_SLIDING_WINDOW_CONTEXT_STRATEGY_ID,
        name="Built-in Sliding Window",
        type="sliding_window",
        resolver_ref=BUILTIN_SLIDING_WINDOW_CONTEXT_RESOLVER,
        description="Default context strategy that trims oldest turns when the context budget is full.",
        config={
            "builtin": True,
            "default": True,
            "budget": dict(DEFAULT_SLIDING_WINDOW_CONTEXT_BUDGET),
        },
        enabled=True,
        created_at=now,
        updated_at=now,
    )
