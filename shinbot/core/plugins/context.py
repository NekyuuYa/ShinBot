"""Plugin runtime capability object and registration decorators."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.dispatch.ingress import RouteDispatchContext, RouteTargetRegistry
from shinbot.core.dispatch.routing import RouteCondition, RouteMatchMode, RouteRule, RouteTable
from shinbot.core.message_routes import (
    CommandDef,
    CommandMode,
    CommandPriority,
    CommandRegistry,
    KeywordDef,
    KeywordRegistry,
)
from shinbot.core.model_runtime import ModelRuntimeObserver, ModelRuntimeObserverRegistry
from shinbot.core.tools import ToolDefinition, ToolOwnerType, ToolRegistry, ToolVisibility
from shinbot.schema.elements import MessageElement
from shinbot.utils.logger import get_plugin_logger

if TYPE_CHECKING:
    from shinbot.agent.services.model_runtime import LLMCallResult
    from shinbot.core.platform.adapter_manager import AdapterManager, MessageHandle
    from shinbot.core.plugins.cron_manager import PluginCronManager
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
        agent_runtime: Any | None = None,
        database: DatabaseManager | None = None,
        cron_manager: PluginCronManager | None = None,
        plugin_manager: Any | None = None,
    ):
        """Initialize the plugin capability object.

        Args:
            plugin_id:        Unique identifier for this plugin (e.g. ``"shinbot_plugin_foo"``).
            command_registry: Shared registry for text command definitions.
            event_bus:        Shared event bus for non-message events.
            data_dir:         Base directory for plugin-scoped persistent data.
                            Defaults to ``data/plugin_data/{plugin_id}``.
            keyword_registry: Registry for keyword trigger definitions.  ``None``
                            disables keyword registration.
            route_table:      Shared route table for custom routing rules.  ``None``
                            disables route registration.
            route_targets:    Target registry used alongside *route_table*.
            adapter_manager:  Manages platform adapter instances.  Required for
                            proactive sends and adapter factory registration.
            tool_registry:    Shared registry for agent tool definitions.
            model_runtime:    Observer registry for model-runtime events.
            agent_runtime:    Reference to the agent runtime, if any.
            database:         Shared database manager instance.
            cron_manager:     Plugin cron scheduler for timed tasks.
            plugin_manager:   Reference to the PluginManager (for advanced
                            plugins that need to manage virtual plugins).
        """
        self.plugin_id = plugin_id
        self._command_registry = command_registry
        self._event_bus = event_bus
        self._keyword_registry = keyword_registry
        self._route_table = route_table
        self._route_targets = route_targets
        self._adapter_manager = adapter_manager
        self._tool_registry = tool_registry
        self._model_runtime = model_runtime
        self.agent_runtime = agent_runtime
        self.database = database
        self._cron_manager = cron_manager
        self._plugin_manager = plugin_manager
        self.data_dir = (
            Path(data_dir) if data_dir is not None else Path("data") / "plugin_data" / plugin_id
        )
        self._registered_commands: list[str] = []
        self._registered_events: list[str] = []
        self._registered_keywords: list[str] = []
        self._registered_routes: list[str] = []
        self._registered_tools: list[str] = []
        self._registered_model_observers: list[ModelRuntimeObserver] = []
        self._background_tasks: set[asyncio.Task[Any]] = set()
        self.logger = get_plugin_logger(plugin_id)

    # ── Background task management ────────────────────────────────────

    def create_task(self, coro: Any, *, name: str | None = None) -> asyncio.Task[Any]:
        """Create and track a background task for this plugin.

        Plugins should use this instead of ``asyncio.create_task()`` so
        that the plugin manager can cancel and await the task during
        unload.  Tasks created here are automatically removed from the
        tracking set when they complete.

        Args:
            coro: The coroutine to run as a background task.
            name: Optional name for debugging (shows in ``asyncio.all_tasks()``).

        Returns:
            The :class:`asyncio.Task` that was created.
        """
        task = asyncio.create_task(coro, name=f"plugin.{self.plugin_id}.{name or 'bg'}")
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    @property
    def background_tasks(self) -> frozenset[asyncio.Task[Any]]:
        """Return the set of currently running background tasks."""
        return frozenset(self._background_tasks)

    async def cancel_background_tasks(self) -> None:
        """Cancel and await all background tasks owned by this plugin.

        Safe to call even if no tasks are running.
        """
        if not self._background_tasks:
            return
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()

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
        """Register a text command triggered by a ``!name`` prefix or alias.

        Can be used as a decorator::

            @plg.on_command("ping", description="Pong!")
            async def handle_ping(ctx: MessageContext) -> None:
                await ctx.send([MessageElement.text("Pong!")])

        Args:
            name:        Primary command name (without the ``!`` prefix).
            aliases:     Alternative names that also trigger this command.
            description: Human-readable command description (shown in help).
            usage:       Usage hint string (e.g. ``"!weather <city>"``).
            permission:  Required permission node (e.g. ``"cmd.weather"``).
            mode:        ``DELEGATED`` for simple prefix commands, ``MANAGED``
                        for commands that need full control over routing.
            priority:    Match priority — ``P0_PREFIX`` (default) matches first,
                        ``P1_EXACT`` matches after exact-match checks,
                        ``P2_REGEX`` matches last.
            pattern:     Optional regex pattern for advanced argument matching.

        Returns:
            A decorator that can wrap the handler function.
        """
        import re as _re

        def decorator(func: Callable) -> Callable:
            """Register the wrapped function as a text command handler."""
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
        """Register a keyword trigger that fires when a message matches *pattern*.

        Keywords are matched against incoming message text independently of
        the command prefix.  Use this for ambient triggers that respond to
        natural-language phrases rather than explicit ``!command`` syntax.

        Can be used as a decorator::

            @plg.on_keyword("good morning", priority=50)
            async def handle_greeting(ctx: MessageContext) -> None:
                await ctx.send([MessageElement.text("Good morning!")])

        Args:
            pattern:     Text or regex pattern to match against message content.
            priority:    Lower numbers are evaluated first (default ``100``).
            ignore_case: Perform case-insensitive matching (default ``True``).
            regex:       Treat *pattern* as a regular expression instead of a
                        plain substring match.

        Returns:
            A decorator that can wrap the handler function.

        Raises:
            RuntimeError: If no ``KeywordRegistry`` is available in this
                ``Plugin`` instance.
        """
        if self._keyword_registry is None:
            raise RuntimeError(
                f"Plugin {self.plugin_id!r} cannot register keyword handlers: "
                "no KeywordRegistry is available in this Plugin object."
            )

        def decorator(func: Callable) -> Callable:
            """Register the wrapped function as a keyword trigger handler."""
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
        """Register a custom routing rule matched by a :class:`RouteCondition`.

        Use this for fine-grained control over which messages reach a handler.
        Conditions can filter on event types, element types, platform, privacy
        status, or a custom matcher callable.

        Can be used as a decorator::

            cond = RouteCondition(platforms=frozenset({"telegram"}))
            @plg.on_route(cond, priority=80)
            async def on_telegram(ctx: RouteDispatchContext, rule: RouteRule) -> None:
                ...

        Args:
            condition:  Structured route condition (event types, element types,
                        platform filters, privacy, custom matcher).
            target:     Explicit target identifier.  Auto-generated if omitted.
            rule_id:    Explicit rule identifier.  Auto-generated if omitted.
            priority:   Higher values are evaluated first (default ``100``).
            match_mode: ``NORMAL`` for standard matching, ``EXCLUSIVE`` to
                        consume the event exclusively, ``FALLBACK`` for
                        catch-all routes, or ``OBSERVE`` for passive monitoring.
            enabled:    Whether this rule is active (default ``True``).

        Returns:
            A decorator that can wrap the handler function.

        Raises:
            RuntimeError: If no ``RouteTable`` or ``RouteTargetRegistry`` is
                available in this ``Plugin`` instance.
        """
        if self._route_table is None or self._route_targets is None:
            raise RuntimeError(
                f"Plugin {self.plugin_id!r} cannot register route handlers: "
                "no RouteTable/RouteTargetRegistry is available in this Plugin object."
            )

        def decorator(func: Callable) -> Callable:
            """Register the wrapped function as a custom route handler."""
            seq = len(self._registered_routes) + 1
            resolved_target = target or f"plugin.{self.plugin_id}.{func.__name__}.{seq}"
            resolved_rule_id = rule_id or f"plugin.{self.plugin_id}.{func.__name__}.{seq}"

            async def handler(context: RouteDispatchContext, rule: RouteRule) -> None:
                """Invoke the registered route handler, awaiting if necessary."""
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
        """Register a handler for a non-message event on the shared :class:`EventBus`.

        Message-prefixed events (starting with ``"message-"``) are routed via
        :class:`RouteTable` instead.  Use this decorator for framework,
        notice, and lifecycle signals.

        Can be used as a decorator::

            @plg.on_event("plugin.loaded")
            async def on_loaded(event: dict[str, Any]) -> None:
                print("Another plugin loaded:", event)

        Args:
            event_type: The event name to listen for (must **not** start with
                        ``"message-"``).
            priority:   Lower numbers are evaluated first (default ``100``).

        Returns:
            A decorator that can wrap the handler function.

        Raises:
            ValueError: If *event_type* starts with ``"message-"``.
        """
        _ensure_non_message_event(event_type)

        def decorator(func: Callable) -> Callable:
            """Register the wrapped function as an event bus handler."""
            self._event_bus.on(event_type, func, priority=priority, owner=self.plugin_id)
            self._registered_events.append(event_type)
            return func

        return decorator

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
        """Register a platform adapter factory with the :class:`AdapterManager`.

        Adapter factories create :class:`BaseAdapter` instances for a given
        platform type.  This allows plugins to supply their own adapter
        implementations at runtime.

        Args:
            name:    Unique adapter name (e.g. ``"onebot_v11"``).
            factory: Callable that returns a new adapter instance when invoked.

        Raises:
            RuntimeError: If no ``AdapterManager`` is available in this
                ``Plugin`` instance.
        """
        if self._adapter_manager is None:
            raise RuntimeError(
                f"Plugin {self.plugin_id!r} cannot register an adapter factory: "
                "no AdapterManager is available in this Plugin object."
            )
        self._adapter_manager.register_adapter(name, factory)

    @property
    def has_model_runtime(self) -> bool:
        """Return ``True`` if a ``ModelRuntimeObserverRegistry`` is available."""
        return self._model_runtime is not None

    def register_model_runtime_observer(self, observer: ModelRuntimeObserver) -> None:
        """Register an observer that receives model-runtime lifecycle events.

        Observers are called with a ``dict[str, Any]`` payload whenever a
        model call starts, succeeds, or fails.  The callback may be sync or
        async.

        Args:
            observer: A callable ``(event: dict[str, Any]) -> None`` to invoke
                     on model-runtime events.

        Raises:
            RuntimeError: If no ``ModelRuntimeObserverRegistry`` is available
                in this ``Plugin`` instance.
        """
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
        """Register an agent tool that the LLM can invoke during a workflow.

        Tools are the primary mechanism for giving the model access to
        external capabilities (sending messages, querying databases, calling
        APIs, etc.).  Each tool receives its declared input schema and is
        executed by the :class:`ToolManager` during the model workflow loop.

        Can be used as a decorator::

            @plg.tool(name="get_weather", description="Get current weather", ...)
            async def get_weather(city: str) -> str:
                return f"Weather for {city}: sunny"

        Args:
            name:            Unique tool name within this plugin.  The full tool
                            ID is ``"{plugin_id}.{name}"``.
            description:     Human-readable description of what the tool does.
            input_schema:    JSON Schema dict describing the tool's input parameters.
            display_name:    Friendly name shown to users (defaults to *name*).
            output_schema:   Optional JSON Schema for the tool's return value.
            permission:      Required permission node for access control.
            enabled:         Whether the tool is active (default ``True``).
            visibility:      ``PRIVATE`` (only this plugin), ``SCOPED``
                            (same owner), or ``PUBLIC`` (all plugins).
            timeout_seconds: Maximum execution time in seconds (default ``30.0``).
            tags:            Optional categorisation tags for tool discovery.
            metadata:        Arbitrary key-value metadata attached to the tool.

        Returns:
            A decorator that can wrap the handler function.

        Raises:
            RuntimeError: If no ``ToolRegistry`` is available in this
                ``Plugin`` instance.
        """
        if self._tool_registry is None:
            raise RuntimeError(
                f"Plugin {self.plugin_id!r} cannot register tools: no ToolRegistry is available."
            )

        def decorator(func: Callable) -> Callable:
            """Register the wrapped function as an agent tool definition."""
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

    # ── LLM calling ────────────────────────────────────────────────────

    async def llm_call(
        self,
        *,
        prompt: str = "",
        system_prompt: str | None = None,
        model_id: str | None = None,
        route_id: str | None = None,
        messages: list[dict[str, Any]] | None = None,
        response_format: dict[str, Any] | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
        purpose: str = "",
    ) -> LLMCallResult:
        """Call an LLM directly, bypassing the agent workflow loop.

        This is the primary API for plugins that need to send a prompt
        to an LLM and receive a text response — e.g. for content
        analysis, summarisation, or structured data extraction.

        Args:
            prompt:          User prompt text.  Ignored when *messages* is
                             provided.
            system_prompt:   Optional system prompt prepended to the
                             conversation.
            model_id:        Explicit model ID (mutually exclusive with
                             *route_id*).
            route_id:        Model route ID for load-balanced/failover
                             selection (mutually exclusive with *model_id*).
            messages:        Full OpenAI-format messages list.  When
                             provided, *prompt* and *system_prompt* are
                             ignored.
            response_format: JSON Schema dict to constrain output format.
            temperature:     Sampling temperature (optional, uses model
                             default when omitted).
            max_tokens:      Maximum output tokens (optional).
            purpose:         Human-readable description for logging.

        Returns:
            An :class:`LLMCallResult` with ``text``, ``usage``,
            ``model_id``, ``provider_id``, ``execution_id``,
            and ``raw_response`` fields.

        Raises:
            RuntimeError:  If the model runtime is not available.
            ValueError:    If both *model_id* and *route_id* are specified,
                          or if no prompt/messages are provided.
            ModelCallError: If the call fails after all retries.
        """
        from shinbot.agent.services.model_runtime import (
            LLMCallResult as _LLMCallResult,
        )
        from shinbot.agent.services.model_runtime import (
            ModelRuntimeCall,
        )

        if model_id is not None and route_id is not None:
            raise ValueError(
                "model_id and route_id are mutually exclusive; "
                "specify only one."
            )

        if self.agent_runtime is None or not hasattr(self.agent_runtime, "model_runtime"):
            raise RuntimeError(
                f"Plugin {self.plugin_id!r} cannot call LLM: "
                "no ModelRuntime is available in this Plugin object."
            )

        model_runtime = self.agent_runtime.model_runtime

        # Build messages list
        if messages is None:
            if not prompt:
                raise ValueError(
                    "llm_call requires either a non-empty `prompt` "
                    "or a `messages` list."
                )
            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            messages.append({"role": "user", "content": prompt})

        # Build params
        params: dict[str, Any] = {}
        if temperature is not None:
            params["temperature"] = temperature
        if max_tokens is not None:
            params["max_tokens"] = max_tokens

        call = ModelRuntimeCall(
            caller=self.plugin_id,
            model_id=model_id,
            route_id=route_id,
            purpose=purpose or f"{self.plugin_id}.llm_call",
            messages=messages,
            response_format=response_format,
            params=params,
        )

        result = await model_runtime.generate(call)

        return _LLMCallResult(
            text=result.text,
            usage=result.usage,
            model_id=result.model_id,
            provider_id=result.provider_id,
            execution_id=result.execution_id,
            raw_response=result.raw_response,
        )

    # ── Model / Provider enumeration ──────────────────────────────────

    def list_models(self, *, provider_id: str | None = None) -> list[dict[str, Any]]:
        """List configured model definitions.

        Args:
            provider_id: Filter models by provider ID (optional).

        Returns:
            A list of model definition dicts, each containing keys
            such as ``id``, ``provider_id``, ``display_name``, etc.

        Raises:
            RuntimeError: If no database is available.
        """
        if self.database is None:
            raise RuntimeError(
                f"Plugin {self.plugin_id!r} cannot list models: "
                "no DatabaseManager is available in this Plugin object."
            )
        return self.database.model_registry.list_models(provider_id=provider_id)

    def get_model(self, model_id: str) -> dict[str, Any] | None:
        """Get a single model definition by ID.

        Args:
            model_id: The model identifier.

        Returns:
            The model definition dict, or ``None`` if not found.

        Raises:
            RuntimeError: If no database is available.
        """
        if self.database is None:
            raise RuntimeError(
                f"Plugin {self.plugin_id!r} cannot get model: "
                "no DatabaseManager is available in this Plugin object."
            )
        return self.database.model_registry.get_model(model_id)

    def list_providers(self) -> list[dict[str, Any]]:
        """List configured LLM provider definitions.

        Returns:
            A list of provider definition dicts, each containing keys
            such as ``id``, ``type``, ``base_url``, etc.

        Raises:
            RuntimeError: If no database is available.
        """
        if self.database is None:
            raise RuntimeError(
                f"Plugin {self.plugin_id!r} cannot list providers: "
                "no DatabaseManager is available in this Plugin object."
            )
        return self.database.model_registry.list_providers()

    def get_provider(self, provider_id: str) -> dict[str, Any] | None:
        """Get a single provider definition by ID.

        Args:
            provider_id: The provider identifier.

        Returns:
            The provider definition dict, or ``None`` if not found.

        Raises:
            RuntimeError: If no database is available.
        """
        if self.database is None:
            raise RuntimeError(
                f"Plugin {self.plugin_id!r} cannot get provider: "
                "no DatabaseManager is available in this Plugin object."
            )
        return self.database.model_registry.get_provider(provider_id)

    # ── Cron scheduling ───────────────────────────────────────────────

    def on_cron(
        self,
        cron_expr: str,
        *,
        timezone: str | None = None,
        job_id: str | None = None,
        description: str = "",
    ) -> Callable:
        """Register a cron-scheduled task.

        Can be used as a decorator::

            @plg.on_cron("0 23 * * *", description="Daily report")
            async def daily_report():
                ...

        Args:
            cron_expr:   Standard 5-field cron expression
                         (minute hour day month day_of_week).
            timezone:    Optional timezone name (e.g. ``"Asia/Shanghai"``).
                         Defaults to the system timezone.
            job_id:      Explicit job ID.  Auto-generated if omitted.
            description: Human-readable description for logging.

        Returns:
            A decorator that wraps the scheduled function.

        Raises:
            RuntimeError: If no cron manager is available.
            ValueError:   If *cron_expr* is not a valid 5-field expression.
        """
        if self._cron_manager is None:
            raise RuntimeError(
                f"Plugin {self.plugin_id!r} cannot register cron jobs: "
                "no cron manager is available in this Plugin object."
            )

        def decorator(func: Callable) -> Callable:
            self._cron_manager.add_cron_job(
                self.plugin_id,
                func,
                cron_expr,
                timezone=timezone,
                job_id=job_id,
                description=description,
            )
            return func

        return decorator
