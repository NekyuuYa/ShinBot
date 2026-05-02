"""ShinBot core engine exports.

Keep this package lightweight: importing a leaf module such as
``shinbot.core.dispatch.command`` must not eagerly import the full application
runtime.  Public package attributes are resolved lazily below.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "ShinBot",
    "BootController",
    "BootState",
    "RuntimeControl",
    "RestartRequest",
    "RestartReason",
    "ProcessExitCode",
    "AdapterManager",
    "BaseAdapter",
    "MessageHandle",
    "CommandDef",
    "CommandMatch",
    "CommandMode",
    "CommandPriority",
    "CommandRegistry",
    "EventBus",
    "StopPropagation",
    "PermissionEngine",
    "PermissionGroup",
    "check_permission",
    "merge_permissions",
    "MessageContext",
    "Plugin",
    "PluginManager",
    "PluginMeta",
    "Session",
    "SessionConfig",
    "SessionManager",
]

_EXPORT_MODULES = {
    "ShinBot": "shinbot.core.application.app",
    "BootController": "shinbot.core.application.boot",
    "BootState": "shinbot.core.application.boot",
    "RuntimeControl": "shinbot.core.application.runtime_control",
    "RestartRequest": "shinbot.core.application.runtime_control",
    "RestartReason": "shinbot.core.application.runtime_control",
    "ProcessExitCode": "shinbot.core.application.runtime_control",
    "AdapterManager": "shinbot.core.platform.adapter_manager",
    "BaseAdapter": "shinbot.core.platform.adapter_manager",
    "MessageHandle": "shinbot.core.platform.adapter_manager",
    "CommandDef": "shinbot.core.dispatch.command",
    "CommandMatch": "shinbot.core.dispatch.command",
    "CommandMode": "shinbot.core.dispatch.command",
    "CommandPriority": "shinbot.core.dispatch.command",
    "CommandRegistry": "shinbot.core.dispatch.command",
    "EventBus": "shinbot.core.dispatch.event_bus",
    "StopPropagation": "shinbot.core.dispatch.event_bus",
    "PermissionEngine": "shinbot.core.security.permission",
    "PermissionGroup": "shinbot.core.security.permission",
    "check_permission": "shinbot.core.security.permission",
    "merge_permissions": "shinbot.core.security.permission",
    "MessageContext": "shinbot.core.dispatch.message_context",
    "Plugin": "shinbot.core.plugins.context",
    "PluginManager": "shinbot.core.plugins.manager",
    "PluginMeta": "shinbot.core.plugins.types",
    "Session": "shinbot.core.state.session",
    "SessionConfig": "shinbot.core.state.session",
    "SessionManager": "shinbot.core.state.session",
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value
