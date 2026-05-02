"""ShinBot application — top-level orchestrator.

Wires together all core subsystems and provides the main entry point
for starting the bot framework.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from shinbot.agent.runtime.services import AgentRuntime
from shinbot.core.dispatch.dispatchers import (
    AGENT_ENTRY_TARGET,
    NOTICE_DISPATCHER_TARGET,
    AgentEntryDispatcher,
    AgentEntryHandler,
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
from shinbot.persistence import DatabaseManager
from shinbot.schema.events import UnifiedEvent

if TYPE_CHECKING:
    from shinbot.agent.attention import AttentionConfig, AttentionSchedulerConfig

logger = logging.getLogger(__name__)


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
        attention_config: AttentionConfig | None = None,
        attention_scheduler_config: AttentionSchedulerConfig | None = None,
        attention_debug: bool = False,
    ) -> None:
        # Core subsystems
        self.database: DatabaseManager | None = None
        self.runtime_control: Any | None = None
        runtime_data_dir = Path(data_dir) if data_dir is not None else Path("data")
        session_repo = None
        audit_repo = None
        if data_dir is not None or database_url is not None:
            self.database = DatabaseManager.from_bootstrap(
                data_dir=runtime_data_dir,
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
        self.adapter_manager = AdapterManager()
        self.agent_runtime = AgentRuntime(
            data_dir=runtime_data_dir,
            database=self.database,
            permission_engine=self.permission_engine,
            audit_logger=self.audit_logger,
            adapter_manager=self.adapter_manager,
            attention_config=attention_config,
            attention_scheduler_config=attention_scheduler_config,
            attention_debug=attention_debug,
        )
        self.model_runtime = self.agent_runtime.model_runtime
        self.identity_store = self.agent_runtime.identity_store
        self.media_service = self.agent_runtime.media_service
        self.context_manager = self.agent_runtime.context_manager
        self.prompt_registry = self.agent_runtime.prompt_registry
        self.media_inspection_runner = self.agent_runtime.media_inspection_runner
        self.tool_registry = self.agent_runtime.tool_registry
        self.tool_manager = self.agent_runtime.tool_manager
        self.attention_config = self.agent_runtime.attention_config
        self.attention_scheduler_config = self.agent_runtime.attention_scheduler_config
        self.attention_engine = self.agent_runtime.attention_engine
        self.attention_scheduler = self.agent_runtime.attention_scheduler
        self.workflow_runner = self.agent_runtime.workflow_runner
        self.plugin_manager = PluginManager(
            command_registry=self.command_registry,
            keyword_registry=self.keyword_registry,
            route_table=self.route_table,
            route_targets=self.route_targets,
            event_bus=self.event_bus,
            adapter_manager=self.adapter_manager,
            tool_registry=self.tool_registry,
            model_runtime=self.model_runtime,
            data_dir=data_dir,
            database=self.database,
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
        self.agent_entry_dispatcher = AgentEntryDispatcher(
            handler=self.agent_runtime.handle_agent_entry,
            database=self.database,
        )
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
            media_service=self.media_service,
            media_inspection_runner=self.media_inspection_runner,
        )

    # ── Event ingress callback ───────────────────────────────────────

    async def on_event(self, event: UnifiedEvent, adapter: BaseAdapter) -> None:
        """Entry point for all incoming events from adapters.

        This is the callback that gets registered with each adapter instance
        via adapter.set_event_callback().
        """
        try:
            await self.message_ingress.process_event(event, adapter)
        except Exception:
            logger.exception("Unhandled error processing event: %s", event.type)

    def set_agent_entry_handler(self, handler: AgentEntryHandler | None) -> None:
        """Attach the Agent-side handler for unmatched user-message signals."""
        self.agent_entry_dispatcher.set_handler(handler)

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
        return self.plugin_manager.load_plugin(plugin_id, module_path)

    async def load_plugin_async(self, plugin_id: str, module_path: str) -> Any:
        return await self.plugin_manager.load_plugin_async(plugin_id, module_path)

    # ── Lifecycle ────────────────────────────────────────────────────

    async def start(self) -> None:
        """Start all adapter instances."""
        logger.info("ShinBot starting...")
        await self.adapter_manager.start_all()
        logger.info("ShinBot started with %d adapters", len(self.adapter_manager.all_instances))

    async def shutdown(self) -> None:
        """Gracefully shut down all subsystems."""
        logger.info("ShinBot shutting down...")
        await self.agent_runtime.shutdown()
        await self.adapter_manager.shutdown_all()
        logger.info("ShinBot shut down complete")
