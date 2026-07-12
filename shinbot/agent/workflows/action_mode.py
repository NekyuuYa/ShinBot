"""Dependency-light execution modes shared by model-facing workflows."""

from __future__ import annotations

from enum import StrEnum


class ExternalActionToolMode(StrEnum):
    """Choose whether chat-action tool calls execute or become proposals."""

    EXECUTE = "execute"
    COLLECT_INTENTS = "collect_intents"


__all__ = ["ExternalActionToolMode"]
