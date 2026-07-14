"""Active chat workflow sub-package."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "ACTIVE_CHAT_PROMPT_COMPONENT_IDS_BY_STAGE",
    "ACTOR_ACTIVE_CHAT_V3_PROMPT_COMPONENT_IDS",
    "ActiveChatFastRunner",
    "ActiveChatFastRunnerConfig",
    "ActiveChatActionKind",
    "ActiveChatBatch",
    "ActiveChatMessageSignal",
    "ActiveChatMode",
    "ActiveChatNoReplyIntensity",
    "ActiveChatReplyIntensity",
    "ActiveChatRoundResult",
    "ActiveChatToolLoop",
    "ActiveChatToolLoopResult",
    "register_active_chat_prompt_components",
]

_EXPORT_MODULES = {
    "ActiveChatFastRunner": "shinbot.agent.workflows.active_chat.runner",
    "ActiveChatFastRunnerConfig": "shinbot.agent.workflows.active_chat.runner",
    "ActiveChatActionKind": "shinbot.agent.workflows.active_chat.models",
    "ActiveChatBatch": "shinbot.agent.workflows.active_chat.models",
    "ActiveChatMessageSignal": "shinbot.agent.workflows.active_chat.models",
    "ActiveChatMode": "shinbot.agent.workflows.active_chat.models",
    "ActiveChatNoReplyIntensity": "shinbot.agent.workflows.active_chat.models",
    "ActiveChatReplyIntensity": "shinbot.agent.workflows.active_chat.models",
    "ActiveChatRoundResult": "shinbot.agent.workflows.active_chat.models",
    "ActiveChatToolLoop": "shinbot.agent.workflows.active_chat.tool_loop",
    "ActiveChatToolLoopResult": "shinbot.agent.workflows.active_chat.tool_loop",
    "register_active_chat_prompt_components": "shinbot.agent.workflows.active_chat.prompt_registration",
    "ACTIVE_CHAT_PROMPT_COMPONENT_IDS_BY_STAGE": "shinbot.agent.workflows.active_chat.prompt_registration",
    "ACTOR_ACTIVE_CHAT_V3_PROMPT_COMPONENT_IDS": "shinbot.agent.workflows.active_chat.prompt_registration",
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value
