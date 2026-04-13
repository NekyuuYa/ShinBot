"""ShinBot core engine — all core subsystems."""

from shinbot.core.adapter_manager import AdapterManager, BaseAdapter, MessageHandle
from shinbot.core.app import ShinBot
from shinbot.core.boot import BootController, BootState
from shinbot.core.command import (
    CommandDef,
    CommandMatch,
    CommandMode,
    CommandPriority,
    CommandRegistry,
)
from shinbot.core.event_bus import EventBus, StopPropagation
from shinbot.core.permission import (
    PermissionEngine,
    PermissionGroup,
    check_permission,
    merge_permissions,
)
from shinbot.core.pipeline import MessageContext, MessagePipeline
from shinbot.core.plugin import PluginContext, PluginManager, PluginMeta
from shinbot.core.session import Session, SessionConfig, SessionManager

__all__ = [
    "ShinBot",
    "BootController",
    "BootState",
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
    "MessagePipeline",
    "PluginContext",
    "PluginManager",
    "PluginMeta",
    "Session",
    "SessionConfig",
    "SessionManager",
]
