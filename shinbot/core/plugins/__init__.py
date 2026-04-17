"""Plugin lifecycle, registration, and configuration helpers."""

from shinbot.core.plugins.config import normalize_plugin_config, plugin_config_schema
from shinbot.core.plugins.context import Plugin
from shinbot.core.plugins.manager import PluginManager
from shinbot.core.plugins.types import PluginMeta, PluginRole, PluginState

__all__ = [
    "Plugin",
    "PluginManager",
    "PluginMeta",
    "PluginRole",
    "PluginState",
    "plugin_config_schema",
    "normalize_plugin_config",
]
