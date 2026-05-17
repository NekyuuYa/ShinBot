"""Shared plugin types."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class PluginRole(Enum):
    LOGIC = "logic"
    ADAPTER = "adapter"


class PluginState(Enum):
    LOADED = "loaded"
    ACTIVE = "active"
    DISABLED = "disabled"
    LOAD_FAILED = "load_failed"
    ERROR = "error"
    UNLOADED = "unloaded"


@dataclass
class PluginMeta:
    """Metadata for a loaded plugin."""

    id: str
    name: str = ""
    version: str = "0.0.0"
    description: str = ""
    author: str = ""
    role: PluginRole = PluginRole.LOGIC
    state: PluginState = PluginState.LOADED
    module_path: str = ""
    commands: list[str] = field(default_factory=list)
    event_types: list[str] = field(default_factory=list)
    keywords: list[str] = field(default_factory=list)
    routes: list[str] = field(default_factory=list)
    data_dir: str = ""
