"""Application-layer runtime orchestration exports."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = ["ShinBot", "BootController", "BootState"]

_EXPORT_MODULES = {
    "ShinBot": "shinbot.core.application.app",
    "BootController": "shinbot.core.application.boot",
    "BootState": "shinbot.core.application.boot",
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value
