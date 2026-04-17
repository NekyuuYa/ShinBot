"""ShinBot core engine — all core subsystems."""

from shinbot.core.application.app import ShinBot
from shinbot.core.application.boot import BootController, BootState
from shinbot.core.dispatch.command import (
    CommandDef,
    CommandMatch,
    CommandMode,
    CommandPriority,
    CommandRegistry,
)
from shinbot.core.dispatch.event_bus import EventBus, StopPropagation
from shinbot.core.dispatch.pipeline import MessageContext, MessagePipeline
from shinbot.core.platform.adapter_manager import AdapterManager, BaseAdapter, MessageHandle
from shinbot.core.plugins.context import Plugin
from shinbot.core.plugins.manager import PluginManager
from shinbot.core.plugins.types import PluginMeta
from shinbot.core.security.permission import (
    PermissionEngine,
    PermissionGroup,
    check_permission,
    merge_permissions,
)
from shinbot.core.state.session import Session, SessionConfig, SessionManager

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
    "Plugin",
    "PluginManager",
    "PluginMeta",
    "Session",
    "SessionConfig",
    "SessionManager",
]
