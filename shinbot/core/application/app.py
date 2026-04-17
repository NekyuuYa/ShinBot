"""ShinBot application — top-level orchestrator.

Wires together all core subsystems and provides the main entry point
for starting the bot framework.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from shinbot.agent.model_runtime import ModelRuntime
from shinbot.agent.prompting import PromptRegistry
from shinbot.agent.tools import ToolManager, ToolRegistry
from shinbot.core.dispatch.command import CommandRegistry
from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.dispatch.pipeline import MessagePipeline
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
    ) -> None:
        # Core subsystems
        self.database: DatabaseManager | None = None
        session_repo = None
        audit_repo = None
        if data_dir is not None or database_url is not None:
            runtime_data_dir = Path(data_dir) if data_dir is not None else Path("data")
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
        self.session_manager = SessionManager(data_dir=data_dir, session_repo=session_repo)
        self.audit_logger = AuditLogger(data_dir=data_dir, audit_repo=audit_repo)
        self.model_runtime = ModelRuntime(self.database)
        self.prompt_registry = PromptRegistry()
        self.permission_engine = PermissionEngine()
        self.tool_registry = ToolRegistry()
        self.tool_manager = ToolManager(
            self.tool_registry,
            permission_engine=self.permission_engine,
            audit_logger=self.audit_logger,
        )
        self.adapter_manager = AdapterManager()
        self.plugin_manager = PluginManager(
            command_registry=self.command_registry,
            event_bus=self.event_bus,
            adapter_manager=self.adapter_manager,
            tool_registry=self.tool_registry,
            data_dir=data_dir,
        )
        self.pipeline = MessagePipeline(
            adapter_manager=self.adapter_manager,
            session_manager=self.session_manager,
            permission_engine=self.permission_engine,
            command_registry=self.command_registry,
            event_bus=self.event_bus,
            audit_logger=self.audit_logger,
            database=self.database,
        )

    # ── Event ingress callback ───────────────────────────────────────

    async def on_event(self, event: UnifiedEvent, adapter: BaseAdapter) -> None:
        """Entry point for all incoming events from adapters.

        This is the callback that gets registered with each adapter instance
        via adapter.set_event_callback().
        """
        try:
            await self.pipeline.process_event(event, adapter)
        except Exception:
            logger.exception("Unhandled error processing event: %s", event.type)

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
        # Wire up the event callback so the adapter feeds events into the pipeline
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
        await self.adapter_manager.shutdown_all()
        logger.info("ShinBot shut down complete")
