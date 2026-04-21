"""Identity mapping primitives for multi-user conversations."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "IdentityStore",
    "inject_identity_layers_into_messages",
    "register_identity_prompt_components",
    "register_identity_tools",
    "resolve_identity_map_prompt",
]

_EXPORT_MODULES = {
    "IdentityStore": "shinbot.agent.identity.store",
    "inject_identity_layers_into_messages": "shinbot.agent.identity.prompt_runtime",
    "register_identity_prompt_components": "shinbot.agent.identity.prompt_registration",
    "register_identity_tools": "shinbot.agent.identity.tools",
    "resolve_identity_map_prompt": "shinbot.agent.identity.prompt_runtime",
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value
