"""Agent runtime service assembly.

This module owns Agent-side service wiring so the core application only has to
attach the Agent entry handler to message routing.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

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
from shinbot.agent.prompt_manager import PromptRegistry
from shinbot.agent.runtime.prompt_registration import register_runtime_prompt_components
from shinbot.agent.tools import ToolManager, ToolRegistry
from shinbot.agent.workflow import WorkflowRunner
from shinbot.core.bot_config import select_response_profile

if TYPE_CHECKING:
    from shinbot.core.application.app import ShinBot
    from shinbot.core.dispatch.dispatchers import AgentEntrySignal
    from shinbot.core.platform.adapter_manager import AdapterManager
    from shinbot.core.security.audit import AuditLogger
    from shinbot.core.security.permission import PermissionEngine
    from shinbot.persistence import DatabaseManager

logger = logging.getLogger(__name__)


class AgentRuntime:
    """Owns Agent-side context, tools, media, attention, and workflow services."""

    def __init__(
        self,
        *,
        data_dir: Path | str,
        database: DatabaseManager | None,
        permission_engine: PermissionEngine,
        audit_logger: AuditLogger,
        adapter_manager: AdapterManager,
        model_runtime: Any,
        attention_config: AttentionConfig | None = None,
        attention_scheduler_config: AttentionSchedulerConfig | None = None,
        attention_debug: bool = False,
    ) -> None:
        runtime_data_dir = Path(data_dir)
        self.database = database
        self.model_runtime = model_runtime
        self.identity_store = IdentityStore(runtime_data_dir / "identities.json")
        self.media_service = MediaService(database) if database is not None else None
        self.context_manager = (
            ContextManager(
                database.message_logs,
                data_dir=runtime_data_dir,
                identity_store=self.identity_store,
                media_service=self.media_service,
            )
            if database is not None
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
                database,
                self.prompt_registry,
                self.model_runtime,
                self.media_service,
            )
            if database is not None and self.media_service is not None
            else None
        )
        self.tool_registry = ToolRegistry()
        self.tool_manager = ToolManager(
            self.tool_registry,
            permission_engine=permission_engine,
            audit_logger=audit_logger,
        )
        register_identity_tools(self.tool_registry, self.identity_store, self.context_manager)

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

        if database is None:
            return

        self.attention_engine = AttentionEngine(
            self.attention_config,
            database.attention,
        )
        self.attention_scheduler = AttentionScheduler(
            self.attention_engine,
            self.attention_scheduler_config,
            context_manager=self.context_manager,
        )
        self.workflow_runner = WorkflowRunner(
            database,
            self.prompt_registry,
            self.model_runtime,
            self.tool_manager,
            self.attention_engine,
            adapter_manager,
            self.media_service,
            self.context_manager,
        )
        self.attention_scheduler.set_workflow_dispatcher(
            self._dispatch_attention_workflow,
        )
        register_attention_runtime(
            self.tool_registry,
            engine=self.attention_engine,
            adapter_manager=adapter_manager,
            database=database,
            context_manager=self.context_manager,
        )
        register_media_runtime(
            self.tool_registry,
            media_service=self.media_service,
            inspection_runner=self.media_inspection_runner,
        )

    async def handle_agent_entry(self, signal: AgentEntrySignal) -> None:
        """Receive the minimal routing signal and let Agent internals process it."""
        if (
            self.attention_scheduler is None
            or signal.message_log_id is None
            or signal.already_handled
            or signal.is_stopped
        ):
            return

        await self.attention_scheduler.on_message(
            signal.session_id,
            signal.message_log_id,
            signal.sender_id,
            response_profile=self._resolve_response_profile(signal),
            is_mentioned=signal.is_mentioned,
            is_reply_to_bot=signal.is_reply_to_bot,
            self_platform_id=signal.self_id,
        )

    def _resolve_response_profile(self, signal: AgentEntrySignal) -> str:
        bot_config = None
        if self.database is not None:
            bot_config = self.database.bot_configs.get_by_instance_id(signal.instance_id)

        return select_response_profile(
            bot_config,
            is_private=signal.is_private,
            is_mentioned=signal.is_mentioned,
            is_reply_to_bot=signal.is_reply_to_bot,
        )

    async def shutdown(self) -> None:
        """Shut down Agent-side background services."""
        if self.attention_scheduler is not None:
            await self.attention_scheduler.shutdown()
        if self.media_inspection_runner is not None:
            await self.media_inspection_runner.shutdown()

    async def _dispatch_attention_workflow(
        self,
        session_id: str,
        batch: list[dict[str, Any]],
        attention_state: Any,
        response_profile: str,
    ) -> None:
        if self.workflow_runner is None:
            return

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


def install_agent_runtime(
    bot: ShinBot,
    *,
    attention_config: AttentionConfig | None = None,
    attention_scheduler_config: AttentionSchedulerConfig | None = None,
    attention_debug: bool = False,
) -> AgentRuntime:
    """Create and mount the default Agent runtime system onto a ShinBot app."""
    from shinbot.core.runtime.model import install_model_runtime

    model_runtime = install_model_runtime(bot)
    runtime = AgentRuntime(
        data_dir=bot.data_dir,
        database=bot.database,
        permission_engine=bot.permission_engine,
        audit_logger=bot.audit_logger,
        adapter_manager=bot.adapter_manager,
        model_runtime=model_runtime,
        attention_config=attention_config,
        attention_scheduler_config=attention_scheduler_config,
        attention_debug=attention_debug,
    )
    bot.mount_agent_runtime(runtime)
    return runtime


__all__ = ["AgentRuntime", "install_agent_runtime"]
