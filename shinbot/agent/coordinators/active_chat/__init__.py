"""Active chat coordinator sub-package."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "ActiveChatActionKind",
    "ActiveChatAttention",
    "ActiveChatAttentionConfig",
    "ActiveChatAttentionState",
    "ActiveChatBatch",
    "ActiveChatCoordinator",
    "ActiveChatMessageSignal",
    "ActiveChatNotifyResult",
    "ActiveChatRoundHandler",
    "ActiveChatRoundResult",
    "ActiveChatStartResult",
    "ActiveChatTraceCompactor",
    "ActiveChatTraceConfig",
    "interest_effect_for_round",
    "sanitize_conversation_trace_messages",
]

_EXPORT_MODULES = {
    "ActiveChatCoordinator": "shinbot.agent.coordinators.active_chat.coordinator",
    "ActiveChatRoundHandler": "shinbot.agent.coordinators.active_chat.coordinator",
    "ActiveChatAttention": "shinbot.agent.coordinators.active_chat.attention",
    "ActiveChatAttentionConfig": "shinbot.agent.coordinators.active_chat.attention",
    "ActiveChatTraceCompactor": "shinbot.agent.coordinators.active_chat.trace",
    "ActiveChatTraceConfig": "shinbot.agent.coordinators.active_chat.trace",
    "sanitize_conversation_trace_messages": "shinbot.agent.coordinators.active_chat.trace",
    "interest_effect_for_round": "shinbot.agent.coordinators.active_chat.actions",
    "ActiveChatActionKind": "shinbot.agent.coordinators.active_chat.models",
    "ActiveChatAttentionState": "shinbot.agent.coordinators.active_chat.models",
    "ActiveChatBatch": "shinbot.agent.coordinators.active_chat.models",
    "ActiveChatMessageSignal": "shinbot.agent.coordinators.active_chat.models",
    "ActiveChatNotifyResult": "shinbot.agent.coordinators.active_chat.models",
    "ActiveChatRoundResult": "shinbot.agent.coordinators.active_chat.models",
    "ActiveChatStartResult": "shinbot.agent.coordinators.active_chat.models",
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value
