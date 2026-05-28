"""ShinBot application — top-level orchestrator.

Wires together all core subsystems and provides the main entry point
for starting the bot framework.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from shinbot.core.application.bot_routing import BotRuntimeRouter
from shinbot.core.application.bots_config import BotServiceConfig
from shinbot.core.config_provider import ConfigProviderRegistry
from shinbot.core.dispatch.dispatchers import (
    AGENT_ENTRY_TARGET,
    NOTICE_DISPATCHER_TARGET,
    AgentEntryDispatcher,
    AgentSignalHandler,
    NoticeDispatcher,
    make_agent_entry_fallback_route_rule,
    make_notice_route_rule,
)
from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.dispatch.ingress import MessageIngress, RouteTargetRegistry
from shinbot.core.dispatch.routing import RouteTable
from shinbot.core.message_routes import (
    KEYWORD_DISPATCHER_TARGET,
    TEXT_COMMAND_DISPATCHER_TARGET,
    CommandRegistry,
    KeywordDispatcher,
    KeywordRegistry,
    TextCommandDispatcher,
    make_keyword_route_rule,
    make_text_command_route_rule,
)
from shinbot.core.platform.adapter_manager import AdapterManager, BaseAdapter
from shinbot.core.plugins.manager import PluginManager
from shinbot.core.security.audit import AuditLogger
from shinbot.core.security.permission import PermissionEngine
from shinbot.core.state.session import SessionManager
from shinbot.core.tools import ToolRegistry
from shinbot.persistence import DatabaseManager
from shinbot.schema.events import UnifiedEvent
from shinbot.utils.logger import format_log_event, get_logger

logger = get_logger(__name__, source="app", color="cyan")


class ShinBot:
    """The main ShinBot application instance.

    Holds references to all subsystems and orchestrates startup/shutdown.
    """

    def __init__(
        self,
        data_dir: Path | str | None = None,
        *,
        database_url: str | None = None,
        database_snapshot_ttl: int | None = None,
    ) -> None:
        # Core subsystems
        self.database: DatabaseManager | None = None
        self.runtime_control: Any | None = None
        self.data_dir = Path(data_dir) if data_dir is not None else Path("data")
        session_repo = None
        audit_repo = None
        if data_dir is not None or database_url is not None:
            self.database = DatabaseManager.from_bootstrap(
                data_dir=self.data_dir,
                url=database_url,
                snapshot_ttl=database_snapshot_ttl,
            )
            self.database.initialize()
            session_repo = self.database.sessions
            audit_repo = self.database.audit

        self.event_bus = EventBus()
        self.command_registry = CommandRegistry()
        self.keyword_registry = KeywordRegistry()
        self.route_table = RouteTable()
        self.route_targets = RouteTargetRegistry()
        self.session_manager = SessionManager(data_dir=data_dir, session_repo=session_repo)
        self.audit_logger = AuditLogger(data_dir=data_dir, audit_repo=audit_repo)
        self.permission_engine = PermissionEngine()
        self.tool_registry = ToolRegistry()
        self.adapter_manager = AdapterManager()
        self.config_provider_registry = ConfigProviderRegistry()
        self._register_builtin_config_providers()
        self.bot_service_configs: tuple[BotServiceConfig, ...] = ()
        self.bot_runtime_router: BotRuntimeRouter | None = None
        self.model_runtime_system: Any | None = None
        self.model_runtime: Any | None = None
        self.agent_runtime: Any | None = None
        self.plugin_manager = PluginManager(
            command_registry=self.command_registry,
            keyword_registry=self.keyword_registry,
            route_table=self.route_table,
            route_targets=self.route_targets,
            event_bus=self.event_bus,
            adapter_manager=self.adapter_manager,
            tool_registry=self.tool_registry,
            data_dir=data_dir,
            database=self.database,
            config_provider_registry=self.config_provider_registry,
        )

        self.text_command_dispatcher = TextCommandDispatcher(
            self.command_registry,
            audit_logger=self.audit_logger,
            session_manager=self.session_manager,
        )
        self.keyword_dispatcher = KeywordDispatcher(
            self.keyword_registry,
            session_manager=self.session_manager,
        )
        self.notice_dispatcher = NoticeDispatcher(self.event_bus)
        self.agent_entry_dispatcher = AgentEntryDispatcher()
        self.route_targets.register(TEXT_COMMAND_DISPATCHER_TARGET, self.text_command_dispatcher)
        self.route_targets.register(KEYWORD_DISPATCHER_TARGET, self.keyword_dispatcher)
        self.route_targets.register(NOTICE_DISPATCHER_TARGET, self.notice_dispatcher)
        self.route_targets.register(AGENT_ENTRY_TARGET, self.agent_entry_dispatcher)
        self.route_table.register(make_text_command_route_rule(self.text_command_dispatcher))
        self.route_table.register(make_keyword_route_rule(self.keyword_dispatcher))
        self.route_table.register(make_notice_route_rule(self.notice_dispatcher))
        self.route_table.register(make_agent_entry_fallback_route_rule())
        self.message_ingress = MessageIngress(
            session_manager=self.session_manager,
            permission_engine=self.permission_engine,
            route_table=self.route_table,
            route_targets=self.route_targets,
            audit_logger=self.audit_logger,
            database=self.database,
        )

    def _register_builtin_config_providers(self) -> None:
        from shinbot.agent.runtime.config_provider import register_builtin_agent_config_provider

        register_builtin_agent_config_provider(self.config_provider_registry)

    def configure_bot_service_configs(self, configs: tuple[BotServiceConfig, ...]) -> None:
        """Install parsed bot service-unit configs into ingress routing."""

        self.bot_service_configs = tuple(configs)
        self.bot_runtime_router = (
            BotRuntimeRouter(self.bot_service_configs) if self.bot_service_configs else None
        )
        self.message_ingress.set_bot_router(self.bot_runtime_router)

    # ── Event ingress callback ───────────────────────────────────────

    async def on_event(self, event: UnifiedEvent, adapter: BaseAdapter) -> None:
        """Entry point for all incoming events from adapters.

        This is the callback that gets registered with each adapter instance
        via adapter.set_event_callback().
        """
        try:
            logger.debug(
                format_log_event(
                    "adapter.event.received",
                    event_type=event.type,
                    platform=event.platform,
                    instance_id=adapter.instance_id,
                    platform_msg_id=event.message.id if event.message is not None else "",
                    sender_id=event.sender_id or "",
                )
            )
            await self.message_ingress.process_event(event, adapter)
        except Exception as exc:
            logger.exception(
                format_log_event(
                    "adapter.event.error",
                    event_type=event.type,
                    platform=event.platform,
                    instance_id=adapter.instance_id,
                    error_code=type(exc).__name__,
                )
            )

    def set_agent_signal_handler(self, handler: AgentSignalHandler | None) -> None:
        """Attach the Agent-side handler for unmatched user-message signals."""
        self.agent_entry_dispatcher.set_handler(handler)

    def mount_model_runtime(self, runtime: Any) -> None:
        """Mount the model runtime as a standalone framework capability."""
        model_runtime = getattr(runtime, "model_runtime", runtime)
        if self.model_runtime is not None and self.model_runtime is not model_runtime:
            raise RuntimeError("Model runtime is already mounted")

        self.model_runtime_system = runtime
        self.model_runtime = model_runtime
        self.plugin_manager.attach_runtime_services(model_runtime=self.model_runtime)

    def mount_agent_runtime(self, runtime: Any) -> None:
        """Mount an Agent-like runtime system onto the core application."""
        if self.agent_runtime is not None:
            raise RuntimeError("Agent runtime is already mounted")

        runtime_model = getattr(runtime, "model_runtime", None)
        if runtime_model is not None:
            if self.model_runtime is None:
                self.mount_model_runtime(runtime_model)
            elif self.model_runtime is not runtime_model:
                raise RuntimeError("Agent runtime uses a different model runtime")

        self.agent_runtime = runtime
        self.plugin_manager.attach_runtime_services(
            tool_registry=self.tool_registry,
            model_runtime=self.model_runtime,
            agent_runtime=self.agent_runtime,
        )
        ingress_handler = getattr(runtime, "handle_ingress_message", None)
        if ingress_handler is not None:
            self.message_ingress.add_pre_route_hook(ingress_handler)
        handler = getattr(runtime, "handle_agent_signal", None)
        if handler is not None:
            self.set_agent_signal_handler(handler)

    # ── Adapter management shortcuts ─────────────────────────────────

    def add_adapter(
        self,
        instance_id: str,
        platform: str,
        **kwargs: Any,
    ) -> BaseAdapter:
        """Create and register an adapter instance."""
        adapter = self.adapter_manager.create_instance(
            instance_id=instance_id,
            platform=platform,
            **kwargs,
        )
        # Wire up the event callback so the adapter feeds events into ingress.
        adapter.set_event_callback(lambda event: self.on_event(event, adapter))
        return adapter

    # ── Plugin management shortcuts ──────────────────────────────────

    def load_plugin(self, plugin_id: str, module_path: str) -> Any:
        """Load a plugin synchronously via the plugin manager.

        Args:
            plugin_id:  Unique identifier for the plugin.
            module_path: Dotted Python module path.

        Returns:
            Metadata describing the loaded plugin.
        """
        return self.plugin_manager.load_plugin(plugin_id, module_path)

    async def load_plugin_async(self, plugin_id: str, module_path: str) -> Any:
        """Load a plugin asynchronously via the plugin manager.

        Args:
            plugin_id:  Unique identifier for the plugin.
            module_path: Dotted Python module path.

        Returns:
            Metadata describing the loaded plugin.
        """
        return await self.plugin_manager.load_plugin_async(plugin_id, module_path)

    # ── Lifecycle ────────────────────────────────────────────────────

    async def start(self) -> None:
        """Start all adapter instances."""
        logger.info("ShinBot starting...")
        if self.agent_runtime is not None:
            start_background_tasks = getattr(self.agent_runtime, "start_background_tasks", None)
            if start_background_tasks is not None:
                start_background_tasks()
        await self.adapter_manager.start_all()
        logger.info("ShinBot started with %d adapters", len(self.adapter_manager.all_instances))

    async def shutdown(self) -> None:
        """Gracefully shut down all subsystems."""
        logger.info("ShinBot shutting down...")
        if self.agent_runtime is not None:
            shutdown = getattr(self.agent_runtime, "shutdown", None)
            if shutdown is not None:
                await shutdown()
        await self.adapter_manager.shutdown_all()
        logger.info("ShinBot shut down complete")
