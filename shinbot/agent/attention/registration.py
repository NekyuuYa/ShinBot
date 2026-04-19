"""Attention module runtime registration helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from shinbot.agent.attention.tools import register_attention_tools

if TYPE_CHECKING:
    from shinbot.agent.attention.engine import AttentionEngine
    from shinbot.agent.context import ContextManager
    from shinbot.agent.tools.registry import ToolRegistry
    from shinbot.core.platform.adapter_manager import AdapterManager
    from shinbot.persistence.engine import DatabaseManager


def register_attention_runtime(
    registry: ToolRegistry,
    *,
    engine: AttentionEngine,
    adapter_manager: AdapterManager,
    database: DatabaseManager | None = None,
    context_manager: ContextManager | None = None,
) -> None:
    """Register all attention runtime integrations for the current process."""

    register_attention_tools(
        registry,
        engine,
        adapter_manager,
        database,
        context_manager,
    )
