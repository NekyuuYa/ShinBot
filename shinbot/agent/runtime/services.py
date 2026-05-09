"""Agent runtime service assembly.

This module owns Agent-side service wiring so the core application only has to
attach the Agent entry handler to message routing.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from shinbot.agent.active_chat import (
    ActiveChatContextBuilderAdapter,
    ActiveChatCoordinator,
    ActiveChatFastRunner,
    register_active_chat_prompt_components,
)
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
    MediaIngressHook,
    MediaInspectionRunner,
    MediaService,
    register_media_prompt_components,
    register_media_runtime,
)
from shinbot.agent.prompt_manager import PromptRegistry
from shinbot.agent.review import (
    DatabaseReviewMessageStore,
    DatabaseReviewSummaryStore,
    ReviewContextBuilderAdapter,
    ReviewCoordinator,
    ReviewRunnerFactory,
    ReviewRuntimeConfig,
    ReviewWorkflowConfig,
    register_review_prompt_components,
)
from shinbot.agent.runtime.prompt_registration import register_runtime_prompt_components
from shinbot.agent.scheduler import (
    ActiveChatTimerService,
    AgentScheduler,
    AttentionActiveReplyDispatcher,
)
from shinbot.agent.tools import ToolManager, ToolRegistry
from shinbot.agent.workflow import AttentionCoordinator
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
        review_runtime_config: ReviewRuntimeConfig | dict[str, Any] | None = None,
    ) -> None:
        runtime_data_dir = Path(data_dir)
        self.database = database
        self.model_runtime = model_runtime
        self.review_runtime_config = _coerce_review_runtime_config(review_runtime_config)
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
        register_review_prompt_components(self.prompt_registry)
        register_active_chat_prompt_components(self.prompt_registry)

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
        self.media_ingress_hook = MediaIngressHook(
            self.media_service,
            self.media_inspection_runner,
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
        self.active_chat_timer = ActiveChatTimerService()
        self.agent_scheduler = self._create_agent_scheduler(workflow_dispatcher=None)
        self.attention_coordinator: AttentionCoordinator | None = None
        self.review_coordinator: ReviewCoordinator | None = None
        self.active_chat_workflow = ActiveChatCoordinator()

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
        self.attention_coordinator = AttentionCoordinator(
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
        self.review_coordinator = self._create_review_coordinator(database)
        self.active_chat_workflow = ActiveChatCoordinator()
        active_chat_fast_runner = ActiveChatFastRunner(
            self.model_runtime,
            prompt_registry=self.prompt_registry,
            tool_manager=self.tool_manager,
            message_store=database.message_logs,
            context_builder=ActiveChatContextBuilderAdapter(self.context_manager),
            pending_message_provider=self.active_chat_workflow.drain_pending_for_repair,
        )
        self.active_chat_workflow.set_round_handler(active_chat_fast_runner.run)
        self.agent_scheduler = self._create_agent_scheduler(
            workflow_dispatcher=AttentionActiveReplyDispatcher(
                self.attention_scheduler,
                review_coordinator=self.review_coordinator,
                active_chat_workflow=self.active_chat_workflow,
            ),
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
        await self.agent_scheduler.accept_signal(signal)

    def _create_agent_scheduler(self, workflow_dispatcher) -> AgentScheduler:
        store = getattr(self.database, "agent_scheduler", None)
        return AgentScheduler(
            workflow_dispatcher=workflow_dispatcher,
            response_profile_resolver=self._resolve_response_profile,
            inbox=store,
            state_store=store,
            active_chat_timer=self.active_chat_timer,
        )

    def _create_review_coordinator(self, database: DatabaseManager) -> ReviewCoordinator:
        config = ReviewWorkflowConfig()
        runner_factory = ReviewRunnerFactory(
            self.model_runtime,
            config=self.review_runtime_config,
            prompt_registry=self.prompt_registry,
            tool_manager=self.tool_manager,
        )
        return ReviewCoordinator(
            config,
            message_store=DatabaseReviewMessageStore(database),
            summary_store=DatabaseReviewSummaryStore(database),
            context_builder=ReviewContextBuilderAdapter(self.context_manager),
            **runner_factory.create_workflow_runner_kwargs(),
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

    def handle_ingress_message(self, context: Any) -> None:
        """Let Agent-owned media services observe accepted inbound messages."""
        self.media_ingress_hook(context)

    async def shutdown(self) -> None:
        """Shut down Agent-side background services."""
        if self.review_coordinator is not None:
            await self.review_coordinator.shutdown()
        await self.active_chat_workflow.shutdown()
        await self.active_chat_timer.shutdown()
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
        if self.attention_coordinator is None:
            return

        parts = session_id.split(":", 2)
        instance_id = parts[0] if parts else ""

        try:
            await self.attention_coordinator.run(
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
    review_runtime_config: ReviewRuntimeConfig | dict[str, Any] | None = None,
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
        review_runtime_config=review_runtime_config,
    )
    bot.mount_agent_runtime(runtime)
    return runtime


def _coerce_review_runtime_config(
    value: ReviewRuntimeConfig | dict[str, Any] | None,
) -> ReviewRuntimeConfig:
    if isinstance(value, ReviewRuntimeConfig):
        return value
    return ReviewRuntimeConfig.from_mapping(value)


__all__ = ["AgentRuntime", "install_agent_runtime"]
