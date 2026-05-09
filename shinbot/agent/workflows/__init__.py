"""Agent workflow layer — concrete LLM/Tool execution loops.

Keep package exports lazy to avoid circular imports during bootstrap.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "ActiveChatFastRunner",
    "ActiveChatFastRunnerConfig",
    "ActiveChatToolLoop",
    "ActiveChatToolLoopResult",
]

_EXPORT_MODULES = {
    "ActiveChatFastRunner": "shinbot.agent.workflows.active_chat",
    "ActiveChatFastRunnerConfig": "shinbot.agent.workflows.active_chat",
    "ActiveChatToolLoop": "shinbot.agent.workflows.active_chat_tool_loop",
    "ActiveChatToolLoopResult": "shinbot.agent.workflows.active_chat_tool_loop",
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value
