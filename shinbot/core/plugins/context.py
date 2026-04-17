"""Plugin runtime capability object and registration decorators."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from shinbot.agent.model_runtime import ModelRuntime, ModelRuntimeObserver
from shinbot.agent.tools import ToolDefinition, ToolOwnerType, ToolRegistry, ToolVisibility
from shinbot.core.dispatch.command import CommandDef, CommandMode, CommandPriority, CommandRegistry
from shinbot.core.dispatch.event_bus import EventBus
from shinbot.utils.logger import get_plugin_logger

if TYPE_CHECKING:
    from shinbot.core.platform.adapter_manager import AdapterManager


class Plugin:
    """Capability object passed to plugins during initialization."""

    def __init__(
        self,
        plugin_id: str,
        command_registry: CommandRegistry,
        event_bus: EventBus,
        data_dir: Path | str | None = None,
        *,
        adapter_manager: AdapterManager | None = None,
        tool_registry: ToolRegistry | None = None,
        model_runtime: ModelRuntime | None = None,
    ):
        self.plugin_id = plugin_id
        self._command_registry = command_registry
        self._event_bus = event_bus
        self._adapter_manager = adapter_manager
        self._tool_registry = tool_registry
        self._model_runtime = model_runtime
        self.data_dir = (
            Path(data_dir) if data_dir is not None else Path("data") / "plugin_data" / plugin_id
        )
        self._registered_commands: list[str] = []
        self._registered_events: list[str] = []
        self._registered_tools: list[str] = []
        self._registered_model_observers: list[ModelRuntimeObserver] = []
        self.logger = get_plugin_logger(plugin_id)

    def on_command(
        self,
        name: str,
        *,
        aliases: list[str] | None = None,
        description: str = "",
        usage: str = "",
        permission: str = "",
        mode: CommandMode = CommandMode.DELEGATED,
        priority: CommandPriority = CommandPriority.P0_PREFIX,
        pattern: str | None = None,
    ) -> Callable:
        import re as _re

        def decorator(func: Callable) -> Callable:
            compiled_pattern = _re.compile(pattern) if pattern else None
            cmd = CommandDef(
                name=name,
                handler=func,
                mode=mode,
                aliases=aliases or [],
                description=description,
                usage=usage,
                permission=permission,
                priority=priority,
                pattern=compiled_pattern,
                owner=self.plugin_id,
            )
            self._command_registry.register(cmd)
            self._registered_commands.append(name)
            return func

        return decorator

    def on_event(
        self,
        event_type: str,
        *,
        priority: int = 100,
    ) -> Callable:
        def decorator(func: Callable) -> Callable:
            self._event_bus.on(event_type, func, priority=priority, owner=self.plugin_id)
            self._registered_events.append(event_type)
            return func

        return decorator

    def on_message(self, *, priority: int = 100) -> Callable:
        return self.on_event("message-created", priority=priority)

    def register_adapter_factory(self, name: str, factory: Callable) -> None:
        if self._adapter_manager is None:
            raise RuntimeError(
                f"Plugin {self.plugin_id!r} cannot register an adapter factory: "
                "no AdapterManager is available in this Plugin object."
            )
        self._adapter_manager.register_adapter(name, factory)

    def register_model_runtime_observer(self, observer: ModelRuntimeObserver) -> None:
        if self._model_runtime is None:
            raise RuntimeError(
                f"Plugin {self.plugin_id!r} cannot register a model runtime observer: "
                "no ModelRuntime is available in this Plugin object."
            )
        self._model_runtime.register_observer(observer)
        self._registered_model_observers.append(observer)

    def tool(
        self,
        *,
        name: str,
        description: str,
        input_schema: dict[str, Any],
        display_name: str = "",
        output_schema: dict[str, Any] | None = None,
        permission: str = "",
        enabled: bool = True,
        visibility: ToolVisibility = ToolVisibility.SCOPED,
        timeout_seconds: float = 30.0,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Callable:
        if self._tool_registry is None:
            raise RuntimeError(
                f"Plugin {self.plugin_id!r} cannot register tools: no ToolRegistry is available."
            )

        def decorator(func: Callable) -> Callable:
            tool_id = f"{self.plugin_id}.{name}"
            definition = ToolDefinition(
                id=tool_id,
                name=name,
                display_name=display_name or name,
                description=description,
                input_schema=input_schema,
                output_schema=output_schema,
                handler=func,
                owner_type=ToolOwnerType.PLUGIN,
                owner_id=self.plugin_id,
                owner_module=getattr(func, "__module__", ""),
                permission=permission,
                enabled=enabled,
                visibility=visibility,
                timeout_seconds=timeout_seconds,
                tags=list(tags or []),
                metadata=dict(metadata or {}),
            )
            self._tool_registry.register_tool(definition)
            self._registered_tools.append(tool_id)
            return func

        return decorator
