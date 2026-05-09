"""Agent runner layer — single-call model stage runners."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "ReviewRunnerFactory",
    "ReviewRuntimeConfig",
    "ReviewStageRuntimeConfig",
    "register_review_prompt_components",
]

_EXPORT_MODULES = {
    "ReviewRunnerFactory": "shinbot.agent.runners._review_factory",
    "ReviewRuntimeConfig": "shinbot.agent.runners._review_factory",
    "ReviewStageRuntimeConfig": "shinbot.agent.runners._review_factory",
    "register_review_prompt_components": "shinbot.agent.runners._review_factory",
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value
