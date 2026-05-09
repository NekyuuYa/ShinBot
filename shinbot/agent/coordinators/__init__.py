"""Agent coordinator layer — orchestration for review and active chat workflows.

Keep package exports lazy to avoid circular imports during bootstrap.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "ActiveChatCoordinator",
    "ActiveChatRoundHandler",
    "ReviewCoordinator",
]

_EXPORT_MODULES = {
    "ActiveChatCoordinator": "shinbot.agent.coordinators.active_chat",
    "ActiveChatRoundHandler": "shinbot.agent.coordinators.active_chat",
    "ReviewCoordinator": "shinbot.agent.coordinators.review",
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value
