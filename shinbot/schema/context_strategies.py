"""Shared context strategy identifiers and defaults."""

from __future__ import annotations

BUILTIN_SLIDING_WINDOW_CONTEXT_STRATEGY_ID = "builtin.context.sliding_window"
BUILTIN_SLIDING_WINDOW_CONTEXT_RESOLVER = "builtin.context.sliding_window"

DEFAULT_SLIDING_WINDOW_CONTEXT_BUDGET: dict[str, int | float | str] = {
    "truncate_policy": "sliding_window",
    "trigger_ratio": 1.0,
    "max_context_tokens": 15000,
    "target_context_tokens": 6000,
    "trim_turns": 2,
}
