"""Plugin runtime capability object and registration decorators."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from shinbot.core.dispatch.command import CommandDef, CommandMode, CommandPriority, CommandRegistry
from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.dispatch.ingress import RouteDispatchContext, RouteTargetRegistry
from shinbot.core.dispatch.keyword import KeywordDef, KeywordRegistry
from shinbot.core.dispatch.routing import RouteCondition, RouteMatchMode, RouteRule, RouteTable
from shinbot.core.model_runtime import ModelRuntimeObserver, ModelRuntimeObserverRegistry
from shinbot.core.tools import ToolDefinition, ToolOwnerType, ToolRegistry, ToolVisibility
from shinbot.schema.elements import MessageElement
from shinbot.utils.logger import get_plugin_logger

if TYPE_CHECKING:
    from shinbot.core.platform.adapter_manager import AdapterManager, MessageHandle
    from shinbot.persistence.engine import DatabaseManager


_MESSAGE_EVENT_PREFIX = "message-"


def _ensure_non_message_event(event_type: str) -> None:
    if event_type.startswith(_MESSAGE_EVENT_PREFIX):
        raise ValueError(
            "Message events are routed by RouteTable, not EventBus. "
            "Use plg.on_command(), plg.on_keyword(), or plg.on_route() instead."
        )


class Plugin:
    """Capability object passed to plugins during initialization."""

    def __init__(
        self,
        plugin_id: str,
        command_registry: CommandRegistry,
        event_bus: EventBus,
        data_dir: Path | str | None = None,
        *,
        keyword_registry: KeywordRegistry | None = None,
        route_table: RouteTable | None = None,
        route_targets: RouteTargetRegistry | None = None,
        adapter_manager: AdapterManager | None = None,
        tool_registry: ToolRegistry | None = None,
        model_runtime: ModelRuntimeObserverRegistry | None = None,
        database: DatabaseManager | None = None,
    ):
        self.plugin_id = plugin_id
        self._command_registry = command_registry
        self._event_bus = event_bus
        self._keyword_registry = keyword_registry
        self._route_table = route_table
        self._route_targets = route_targets
        self._adapter_manager = adapter_manager
        self._tool_registry = tool_registry
        self._model_runtime = model_runtime
        self.database = database
        self.data_dir = (
            Path(data_dir) if data_dir is not None else Path("data") / "plugin_data" / plugin_id
        )
        self._registered_commands: list[str] = []
        self._registered_events: list[str] = []
        self._registered_keywords: list[str] = []
        self._registered_routes: list[str] = []
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

    def on_keyword(
        self,
        pattern: str,
        *,
        priority: int = 100,
        ignore_case: bool = True,
        regex: bool = False,
    ) -> Callable:
        if self._keyword_registry is None:
            raise RuntimeError(
                f"Plugin {self.plugin_id!r} cannot register keyword handlers: "
                "no KeywordRegistry is available in this Plugin object."
            )

        def decorator(func: Callable) -> Callable:
            keyword = KeywordDef(
                pattern=pattern,
                handler=func,
                priority=priority,
                ignore_case=ignore_case,
                regex=regex,
                owner=self.plugin_id,
            )
            self._keyword_registry.register(keyword)
            self._registered_keywords.append(pattern)
            return func

        return decorator

    def on_route(
        self,
        condition: RouteCondition,
        *,
        target: str | None = None,
        rule_id: str | None = None,
        priority: int = 100,
        match_mode: RouteMatchMode = RouteMatchMode.NORMAL,
        enabled: bool = True,
    ) -> Callable:
        if self._route_table is None or self._route_targets is None:
            raise RuntimeError(
                f"Plugin {self.plugin_id!r} cannot register route handlers: "
                "no RouteTable/RouteTargetRegistry is available in this Plugin object."
            )

        def decorator(func: Callable) -> Callable:
            seq = len(self._registered_routes) + 1
            resolved_target = target or f"plugin.{self.plugin_id}.{func.__name__}.{seq}"
            resolved_rule_id = rule_id or f"plugin.{self.plugin_id}.{func.__name__}.{seq}"

            async def handler(context: RouteDispatchContext, rule: RouteRule) -> None:
                result = func(context, rule)
                if inspect.isawaitable(result):
                    await result

            self._route_targets.register(resolved_target, handler, owner=self.plugin_id)
            try:
                self._route_table.register(
                    RouteRule(
                        id=resolved_rule_id,
                        priority=priority,
                        condition=condition,
                        target=resolved_target,
                        match_mode=match_mode,
                        enabled=enabled,
                        owner=self.plugin_id,
                    )
                )
            except Exception:
                self._route_targets.unregister(resolved_target)
                raise

            self._registered_routes.append(resolved_rule_id)
            return func

        return decorator

    def on_event(
        self,
        event_type: str,
        *,
        priority: int = 100,
    ) -> Callable:
        _ensure_non_message_event(event_type)

        def decorator(func: Callable) -> Callable:
            self._event_bus.on(event_type, func, priority=priority, owner=self.plugin_id)
            self._registered_events.append(event_type)
            return func

        return decorator

    def on_message(self, *, priority: int = 100) -> Callable:
        raise ValueError(
            "plg.on_message() has been removed from the EventBus path. "
            "Use plg.on_command(), plg.on_keyword(), or plg.on_route() instead."
        )

    async def send_to(self, session_id: str, elements: list[MessageElement]) -> MessageHandle:
        """Send a message to an arbitrary session by its URN.

        Intended for proactive (out-of-context) sends — e.g. from a scheduled
        task or a non-message event handler.  For in-command replies prefer
        ``ctx.send()`` on the ``MessageContext``.

        Args:
            session_id: Session URN (``{instance_id}:{type}:{target}``).
            elements:   Message element list to send.

        Raises:
            RuntimeError: If no AdapterManager is available, or no adapter
                          is registered for the given session's instance_id.
        """
        if self._adapter_manager is None:
            raise RuntimeError(
                f"Plugin {self.plugin_id!r} cannot send proactively: "
                "no AdapterManager is available in this Plugin object."
            )
        adapter = self._adapter_manager.get_instance_by_session(session_id)
        if adapter is None:
            raise RuntimeError(
                f"Plugin {self.plugin_id!r}: no adapter found for session {session_id!r}"
            )
        return await adapter.send(session_id, elements)

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
