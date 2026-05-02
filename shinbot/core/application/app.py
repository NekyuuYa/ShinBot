"""ShinBot application — top-level orchestrator.

Wires together all core subsystems and provides the main entry point
for starting the bot framework.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from shinbot.agent.attention import (
    AttentionConfig,
    AttentionEngine,
    AttentionScheduler,
    AttentionSchedulerConfig,
    register_attention_runtime,
)
from shinbot.agent.context import ContextManager
from shinbot.agent.identity import (
    IdentityStore,
    register_identity_prompt_components,
    register_identity_tools,
)
from shinbot.agent.media import (
    MediaInspectionRunner,
    MediaService,
    register_media_prompt_components,
    register_media_runtime,
)
from shinbot.agent.model_runtime import ModelRuntime
from shinbot.agent.prompt_manager import PromptRegistry
from shinbot.agent.runtime import register_runtime_prompt_components
from shinbot.agent.tools import ToolManager, ToolRegistry
from shinbot.agent.workflow import WorkflowRunner
from shinbot.core.dispatch.command import CommandRegistry
from shinbot.core.dispatch.dispatchers import (
    AGENT_ENTRY_TARGET,
    KEYWORD_DISPATCHER_TARGET,
    NOTICE_DISPATCHER_TARGET,
    TEXT_COMMAND_DISPATCHER_TARGET,
    AgentEntryDispatcher,
    KeywordDispatcher,
    NoticeDispatcher,
    TextCommandDispatcher,
    make_agent_entry_fallback_route_rule,
    make_keyword_route_rule,
    make_notice_route_rule,
    make_text_command_route_rule,
)
from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.dispatch.ingress import MessageIngress, RouteTargetRegistry
from shinbot.core.dispatch.keyword import KeywordRegistry
from shinbot.core.dispatch.routing import RouteTable
from shinbot.core.platform.adapter_manager import AdapterManager, BaseAdapter
from shinbot.core.plugins.manager import PluginManager
from shinbot.core.security.audit import AuditLogger
from shinbot.core.security.permission import PermissionEngine
from shinbot.core.state.session import SessionManager
from shinbot.persistence import DatabaseManager
from shinbot.schema.events import UnifiedEvent

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
        self.model_runtime = ModelRuntime(self.database)
        self.identity_store = IdentityStore(runtime_data_dir / "identities.json")
        self.media_service = MediaService(self.database) if self.database is not None else None
        self.context_manager = (
            ContextManager(
                self.database.message_logs,
                data_dir=runtime_data_dir,
                identity_store=self.identity_store,
                media_service=self.media_service,
            )
            if self.database is not None
            else None
        )
        self.prompt_registry = PromptRegistry(
            context_manager=self.context_manager,
            identity_store=self.identity_store,
        )
        register_identity_prompt_components(
            self.prompt_registry,
            identity_store=self.identity_store,
        )
        register_runtime_prompt_components(
            self.prompt_registry,
            message_text_resolver=self.prompt_registry.resolve_builtin_message_text_prompt,
            current_time_resolver=self.prompt_registry.resolve_builtin_current_time_prompt,
        )
        register_media_prompt_components(self.prompt_registry)
        self.media_inspection_runner = (
            MediaInspectionRunner(
                self.database,
                self.prompt_registry,
                self.model_runtime,
                self.media_service,
            )
            if self.database is not None and self.media_service is not None
            else None
        )
        self.permission_engine = PermissionEngine()
        self.tool_registry = ToolRegistry()
        self.tool_manager = ToolManager(
            self.tool_registry,
            permission_engine=self.permission_engine,
            audit_logger=self.audit_logger,
        )
        register_identity_tools(self.tool_registry, self.identity_store, self.context_manager)
        self.adapter_manager = AdapterManager()
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

        # ── Attention-driven conversation workflow ──────────────────
        self.attention_config = attention_config or AttentionConfig()
        if attention_debug:
            self.attention_config.debug = True
        self.attention_scheduler_config = (
            attention_scheduler_config
            or AttentionSchedulerConfig.from_engine_config(self.attention_config)
        )
        self.attention_engine: AttentionEngine | None = None
        self.attention_scheduler: AttentionScheduler | None = None
        self.workflow_runner: WorkflowRunner | None = None

        if self.database is not None:
            self.attention_engine = AttentionEngine(
                self.attention_config,
                self.database.attention,
            )
            self.attention_scheduler = AttentionScheduler(
                self.attention_engine,
                self.attention_scheduler_config,
                context_manager=self.context_manager,
            )
            self.workflow_runner = WorkflowRunner(
                self.database,
                self.prompt_registry,
                self.model_runtime,
                self.tool_manager,
                self.attention_engine,
                self.adapter_manager,
                self.media_service,
                self.context_manager,
            )
            # Wire workflow dispatcher into the scheduler
            self.attention_scheduler.set_workflow_dispatcher(
                self._dispatch_attention_workflow,
            )
            register_attention_runtime(
                self.tool_registry,
                engine=self.attention_engine,
                adapter_manager=self.adapter_manager,
                database=self.database,
                context_manager=self.context_manager,
            )
            register_media_runtime(
                self.tool_registry,
                media_service=self.media_service,
                inspection_runner=self.media_inspection_runner,
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
            attention_scheduler=self.attention_scheduler,
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

    # ── Attention workflow dispatcher ────────────────────────────────

    async def _dispatch_attention_workflow(
        self,
        session_id: str,
        batch: list[dict[str, Any]],
        attention_state: Any,
        response_profile: str,
    ) -> None:
        """Callback for the attention scheduler to dispatch a workflow run."""
        if self.workflow_runner is None:
            return

        # Resolve instance_id from session_id (format: {instance_id}:group:...)
        parts = session_id.split(":", 2)
        instance_id = parts[0] if parts else ""

        try:
            await self.workflow_runner.run(
                session_id,
                batch,
                attention_state,
                instance_id=instance_id,
                response_profile=response_profile,
            )
        except Exception:
            logger.exception("Attention workflow failed for session %s", session_id)

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
        if self.attention_scheduler is not None:
            await self.attention_scheduler.shutdown()
        if self.media_inspection_runner is not None:
            await self.media_inspection_runner.shutdown()
        await self.adapter_manager.shutdown_all()
        logger.info("ShinBot shut down complete")
