"""Agent runtime service assembly.

This module owns Agent-side service wiring so the core application only has to
attach the Agent entry handler to message routing.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from shinbot.agent.coordinators.active_chat import ActiveChatCoordinator
from shinbot.agent.coordinators.active_chat import models as active_chat_coordinator_models
from shinbot.agent.coordinators.dispatcher import ActiveReplyDispatcher
from shinbot.agent.coordinators.review import ReviewCoordinator
from shinbot.agent.coordinators.review.factory import (
    ReviewRunnerFactory,
    ReviewRuntimeConfig,
    register_review_prompt_components,
)
from shinbot.agent.coordinators.review.models import ReviewWorkflowConfig
from shinbot.agent.runtime.prompt_registration import register_runtime_prompt_components
from shinbot.agent.runtime.review_message_store import DatabaseReviewMessageStore
from shinbot.agent.runtime.review_summary_store import DatabaseReviewSummaryStore
from shinbot.agent.scheduler import (
    ActiveChatTimerService,
    AgentScheduler,
)
from shinbot.agent.services.context import ContextManager
from shinbot.agent.services.context.active_chat_context import ActiveChatContextBuilderAdapter
from shinbot.agent.services.context.review_context_builder import ReviewContextBuilderAdapter
from shinbot.agent.services.identity import (
    IdentityStore,
    register_identity_prompt_components,
    register_identity_tools,
)
from shinbot.agent.services.media import (
    MediaIngressHook,
    MediaInspectionRunner,
    MediaService,
    register_media_prompt_components,
    register_media_runtime,
)
from shinbot.agent.services.prompt_engine import PromptRegistry
from shinbot.agent.services.tools import ToolManager, ToolRegistry
from shinbot.agent.workflows.active_chat import ActiveChatFastRunner
from shinbot.agent.workflows.active_chat import models as active_chat_workflow_models
from shinbot.agent.workflows.active_chat.prompt_registration import (
    register_active_chat_prompt_components,
)
from shinbot.agent.workflows.chat_actions import register_chat_action_tools
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
    """Owns Agent-side context, tools, media, and workflow services."""

    def __init__(
        self,
        *,
        data_dir: Path | str,
        database: DatabaseManager | None,
        permission_engine: PermissionEngine,
        audit_logger: AuditLogger,
        adapter_manager: AdapterManager,
        model_runtime: Any,
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

        self.active_chat_timer = ActiveChatTimerService()
        self.agent_scheduler = self._create_agent_scheduler(workflow_dispatcher=None)
        self.review_coordinator: ReviewCoordinator | None = None
        self.active_chat_workflow = ActiveChatCoordinator()

        if database is None:
            return

        self.review_coordinator = self._create_review_coordinator(database)
        self.active_chat_workflow = ActiveChatCoordinator()
        active_chat_fast_runner = ActiveChatFastRunner(
            self.model_runtime,
            prompt_registry=self.prompt_registry,
            tool_manager=self.tool_manager,
            message_store=database.message_logs,
            context_builder=ActiveChatContextBuilderAdapter(self.context_manager),
            pending_message_provider=lambda batch: self._drain_active_chat_pending_for_repair(
                batch
            ),
        )
        self.active_chat_workflow.set_round_handler(
            lambda batch: self._run_active_chat_fast_round(active_chat_fast_runner, batch)
        )
        self.agent_scheduler = self._create_agent_scheduler(
            workflow_dispatcher=ActiveReplyDispatcher(
                review_coordinator=self.review_coordinator,
                active_chat_workflow=self.active_chat_workflow,
            ),
        )
        register_chat_action_tools(
            self.tool_registry,
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

    async def _run_active_chat_fast_round(
        self,
        runner: ActiveChatFastRunner,
        batch: active_chat_coordinator_models.ActiveChatBatch,
    ) -> active_chat_coordinator_models.ActiveChatRoundResult:
        result = await runner.run(_workflow_active_chat_batch_from_coordinator(batch))
        return _coordinator_active_chat_result_from_workflow(result)

    async def _drain_active_chat_pending_for_repair(
        self,
        batch: active_chat_workflow_models.ActiveChatBatch,
    ) -> list[active_chat_workflow_models.ActiveChatMessageSignal]:
        coordinator_messages = await self.active_chat_workflow.drain_pending_for_repair(
            _coordinator_active_chat_batch_from_workflow(batch)
        )
        return [
            _workflow_active_chat_message_from_coordinator(message)
            for message in coordinator_messages
        ]

    async def shutdown(self) -> None:
        """Shut down Agent-side background services."""
        if self.review_coordinator is not None:
            await self.review_coordinator.shutdown()
        await self.active_chat_workflow.shutdown()
        await self.active_chat_timer.shutdown()
        if self.media_inspection_runner is not None:
            await self.media_inspection_runner.shutdown()


def install_agent_runtime(
    bot: ShinBot,
    *,
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


def _workflow_active_chat_batch_from_coordinator(
    batch: active_chat_coordinator_models.ActiveChatBatch,
) -> active_chat_workflow_models.ActiveChatBatch:
    return active_chat_workflow_models.ActiveChatBatch(
        session_id=batch.session_id,
        messages=[
            _workflow_active_chat_message_from_coordinator(message)
            for message in batch.messages
        ],
        active_chat_state=batch.active_chat_state,
        response_profile=batch.response_profile,
        mode=active_chat_workflow_models.ActiveChatMode(batch.mode.value),
        review_result_summary=batch.review_result_summary,
        conversation_summary=batch.conversation_summary,
        conversation_messages=list(batch.conversation_messages),
    )


def _coordinator_active_chat_batch_from_workflow(
    batch: active_chat_workflow_models.ActiveChatBatch,
) -> active_chat_coordinator_models.ActiveChatBatch:
    return active_chat_coordinator_models.ActiveChatBatch(
        session_id=batch.session_id,
        messages=[
            _coordinator_active_chat_message_from_workflow(message)
            for message in batch.messages
        ],
        active_chat_state=batch.active_chat_state,
        response_profile=batch.response_profile,
        mode=active_chat_coordinator_models.ActiveChatMode(batch.mode.value),
        review_result_summary=batch.review_result_summary,
        conversation_summary=batch.conversation_summary,
        conversation_messages=list(batch.conversation_messages),
    )


def _workflow_active_chat_message_from_coordinator(
    message: active_chat_coordinator_models.ActiveChatMessageSignal,
) -> active_chat_workflow_models.ActiveChatMessageSignal:
    return active_chat_workflow_models.ActiveChatMessageSignal(
        session_id=message.session_id,
        message_log_id=message.message_log_id,
        sender_id=message.sender_id,
        response_profile=message.response_profile,
        is_mentioned=message.is_mentioned,
        is_reply_to_bot=message.is_reply_to_bot,
        is_mention_to_other=message.is_mention_to_other,
        is_poke_to_bot=message.is_poke_to_bot,
        is_poke_to_other=message.is_poke_to_other,
        self_platform_id=message.self_platform_id,
        active_chat_state=message.active_chat_state,
        created_at=message.created_at,
    )


def _coordinator_active_chat_message_from_workflow(
    message: active_chat_workflow_models.ActiveChatMessageSignal,
) -> active_chat_coordinator_models.ActiveChatMessageSignal:
    return active_chat_coordinator_models.ActiveChatMessageSignal(
        session_id=message.session_id,
        message_log_id=message.message_log_id,
        sender_id=message.sender_id,
        response_profile=message.response_profile,
        is_mentioned=message.is_mentioned,
        is_reply_to_bot=message.is_reply_to_bot,
        is_mention_to_other=message.is_mention_to_other,
        is_poke_to_bot=message.is_poke_to_bot,
        is_poke_to_other=message.is_poke_to_other,
        self_platform_id=message.self_platform_id,
        active_chat_state=message.active_chat_state,
        created_at=message.created_at,
    )


def _coordinator_active_chat_result_from_workflow(
    result: active_chat_workflow_models.ActiveChatRoundResult,
) -> active_chat_coordinator_models.ActiveChatRoundResult:
    return active_chat_coordinator_models.ActiveChatRoundResult(
        success=result.success,
        reason=result.reason,
        action=active_chat_coordinator_models.ActiveChatActionKind(result.action.value),
        reply_intensity=active_chat_coordinator_models.ActiveChatReplyIntensity(
            result.reply_intensity.value
        ),
        no_reply_intensity=active_chat_coordinator_models.ActiveChatNoReplyIntensity(
            result.no_reply_intensity.value
        ),
        consumed_message_log_ids=list(result.consumed_message_log_ids),
        restored_messages=[
            _coordinator_active_chat_message_from_workflow(message)
            for message in result.restored_messages
        ],
        conversation_messages_delta=list(result.conversation_messages_delta),
    )


__all__ = ["AgentRuntime", "install_agent_runtime"]
