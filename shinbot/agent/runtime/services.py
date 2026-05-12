"""Agent runtime service assembly.

This module owns Agent-side service wiring so the core application only has to
attach the Agent entry handler to message routing.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

from shinbot.agent.coordinators.active_chat import ActiveChatCoordinator
from shinbot.agent.coordinators.active_chat import models as active_chat_coordinator_models
from shinbot.agent.coordinators.active_chat.attention import ActiveChatAttention
from shinbot.agent.coordinators.dispatcher import ActiveReplyDispatcher
from shinbot.agent.coordinators.review import ReviewCoordinator
from shinbot.agent.coordinators.review.factory import (
    ReviewRunnerFactory,
    ReviewRuntimeConfig,
    register_review_prompt_components,
)
from shinbot.agent.coordinators.review.models import ReviewWorkflowConfig
from shinbot.agent.runtime.config import (
    AgentRuntimeConfig,
    agent_runtime_config_from_mapping,
)
from shinbot.agent.runtime.prompt_registration import register_runtime_prompt_components
from shinbot.agent.runtime.review_stores import (
    DatabaseReviewMessageStore,
    DatabaseReviewSummaryStore,
)
from shinbot.agent.scheduler import (
    ActiveChatTimerService,
    AgentScheduler,
)
from shinbot.agent.scheduler.active_chat_policy import DefaultActiveChatPolicy
from shinbot.agent.scheduler.review_policy import DefaultReviewPolicy
from shinbot.agent.services.context import ContextManager
from shinbot.agent.services.context.active_chat_context import ActiveChatContextBuilderAdapter
from shinbot.agent.services.context.review_context_builder import ReviewContextBuilderAdapter
from shinbot.agent.services.identity import (
    IdentityStore,
    register_identity_file_prompt_components,
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
from shinbot.agent.services.message_formatter import MessageFormatterService
from shinbot.agent.services.prompt_engine import PromptFileLoadConfig, PromptRegistry
from shinbot.agent.services.summaries import SummaryService
from shinbot.agent.services.tools import ToolManager, ToolRegistry
from shinbot.agent.workflows.active_chat import ActiveChatFastRunner
from shinbot.agent.workflows.active_chat import models as active_chat_workflow_models
from shinbot.agent.workflows.active_chat.prompt_registration import (
    register_active_chat_prompt_components,
)
from shinbot.agent.workflows.chat_actions import register_chat_action_tools
from shinbot.core.instance_config import select_response_profile

if TYPE_CHECKING:
    from shinbot.core.application.app import ShinBot
    from shinbot.core.dispatch.dispatchers import AgentEntrySignal
    from shinbot.core.platform.adapter_manager import AdapterManager
    from shinbot.core.security.audit import AuditLogger
    from shinbot.core.security.permission import PermissionEngine
    from shinbot.persistence import DatabaseManager

logger = logging.getLogger(__name__)


class AgentRuntimeProfile:
    """Per-agent runtime wiring selected by bot id at Agent entry."""

    def __init__(
        self,
        owner: AgentRuntime,
        *,
        profile_id: str,
        config: AgentRuntimeConfig,
    ) -> None:
        self._owner = owner
        self.profile_id = profile_id
        self.config = config
        self.prompt_file_config = config.prompt_file_config or PromptFileLoadConfig.from_data_dir(
            owner.runtime_data_dir
        )
        self.review_runtime_config = config.review_runtime_config
        self.review_workflow_config = config.review_workflow_config
        self.prompt_registry = owner._create_prompt_registry(self.prompt_file_config)
        self.active_chat_timer = ActiveChatTimerService()
        self.review_coordinator: ReviewCoordinator | None = None
        self.active_chat_workflow = self._create_active_chat_workflow()
        self._workflow_dispatcher = ActiveReplyDispatcher(
            active_chat_workflow=self.active_chat_workflow,
            summary_service=owner.summary_service,
            review_config=self.review_workflow_config,
        )
        self.agent_scheduler = self._create_agent_scheduler(self._workflow_dispatcher)

        if owner.database is None:
            return

        self.review_coordinator = self._create_review_coordinator()
        self.active_chat_workflow = self._create_active_chat_workflow()
        active_chat_fast_runner = ActiveChatFastRunner(
            owner.model_runtime,
            prompt_registry=self.prompt_registry,
            tool_manager=owner.tool_manager,
            message_store=owner.database.message_logs,
            context_builder=ActiveChatContextBuilderAdapter(
                owner.context_manager,
                message_formatter=owner.message_formatter,
                message_format_config=config.default_message_format_config,
            ),
            message_formatter=owner.message_formatter,
            pending_message_provider=lambda batch: owner._drain_active_chat_pending_for_repair(
                self,
                batch,
            ),
            config=config.active_chat_fast_runner_config,
        )
        self.active_chat_workflow.set_round_handler(
            lambda batch: owner._run_active_chat_fast_round(
                self,
                active_chat_fast_runner,
                batch,
            )
        )
        self._workflow_dispatcher = ActiveReplyDispatcher(
            review_coordinator=self.review_coordinator,
            active_chat_workflow=self.active_chat_workflow,
            summary_service=owner.summary_service,
            review_config=self.review_workflow_config,
        )
        self.agent_scheduler = self._create_agent_scheduler(self._workflow_dispatcher)

    def reload_prompt_files(self) -> None:
        """Reload file-backed prompt components for this profile."""

        register_identity_file_prompt_components(
            self.prompt_registry,
            prompt_file_config=self.prompt_file_config,
        )
        register_media_prompt_components(
            self.prompt_registry,
            prompt_file_config=self.prompt_file_config,
        )
        register_review_prompt_components(
            self.prompt_registry,
            prompt_file_config=self.prompt_file_config,
        )
        register_active_chat_prompt_components(
            self.prompt_registry,
            prompt_file_config=self.prompt_file_config,
        )

    async def shutdown(self) -> None:
        """Shut down profile-owned background tasks."""

        if self.review_coordinator is not None:
            await self.review_coordinator.shutdown()
        self._workflow_dispatcher.flush_active_chat_summaries()
        await self.active_chat_workflow.shutdown()
        await self.active_chat_timer.shutdown()

    def _create_active_chat_workflow(self) -> ActiveChatCoordinator:
        return ActiveChatCoordinator(
            attention=ActiveChatAttention(self.config.active_chat_attention_config),
            conversation_message_limit=self.config.active_chat_conversation_message_limit,
            interest_effect_config=self.config.active_chat_interest_effect_config,
        )

    def _create_agent_scheduler(self, workflow_dispatcher) -> AgentScheduler:
        store = getattr(self._owner.database, "agent_scheduler", None)
        return AgentScheduler(
            workflow_dispatcher=workflow_dispatcher,
            response_profile_resolver=self._owner._resolve_response_profile,
            config=self.config.agent_scheduler_config,
            inbox=store,
            state_store=store,
            priority_policy=None,
            review_policy=DefaultReviewPolicy(self.config.review_policy_config),
            active_chat_policy=DefaultActiveChatPolicy(self.config.active_chat_policy_config),
            active_chat_timer=self.active_chat_timer,
        )

    def _create_review_coordinator(self) -> ReviewCoordinator:
        assert self._owner.database is not None
        runner_factory = ReviewRunnerFactory(
            self._owner.model_runtime,
            config=self.review_runtime_config,
            prompt_registry=self.prompt_registry,
            tool_manager=self._owner.tool_manager,
            summary_service=self._owner.summary_service,
            message_formatter=self._owner.message_formatter,
        )
        return ReviewCoordinator(
            self.review_workflow_config,
            message_store=DatabaseReviewMessageStore(self._owner.database),
            summary_store=DatabaseReviewSummaryStore(self._owner.database),
            context_builder=ReviewContextBuilderAdapter(),
            **runner_factory.create_workflow_runner_kwargs(),
        )


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
        prompt_file_config: PromptFileLoadConfig | dict[str, Any] | None = None,
        agent_config: AgentRuntimeConfig | dict[str, Any] | None = None,
        agent_configs_by_bot_id: dict[str, AgentRuntimeConfig | dict[str, Any]] | None = None,
    ) -> None:
        runtime_data_dir = Path(data_dir)
        self.runtime_data_dir = runtime_data_dir
        self.database = database
        self.model_runtime = model_runtime
        self.identity_store = IdentityStore(runtime_data_dir / "identities.json")
        self.media_service = MediaService(database) if database is not None else None
        self.message_formatter = MessageFormatterService(
            identity_store=self.identity_store,
            media_service=self.media_service,
        )
        self.summary_service = (
            SummaryService(database.agent_summaries) if database is not None else None
        )
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
        self.tool_registry = ToolRegistry()
        self.tool_manager = ToolManager(
            self.tool_registry,
            permission_engine=permission_engine,
            audit_logger=audit_logger,
        )
        register_identity_tools(self.tool_registry, self.identity_store, self.context_manager)

        default_config = _coerce_agent_runtime_config(
            agent_config,
            runtime_data_dir=runtime_data_dir,
        )
        if review_runtime_config is not None:
            default_config = replace(
                default_config,
                review_runtime_config=_coerce_review_runtime_config(review_runtime_config),
            )
        if prompt_file_config is not None:
            default_config = replace(
                default_config,
                prompt_file_config=_coerce_prompt_file_config(
                    prompt_file_config,
                    runtime_data_dir,
                ),
            )
        self._default_profile = AgentRuntimeProfile(
            self,
            profile_id=default_config.agent_id or "default",
            config=default_config,
        )
        self._profiles_by_bot_id: dict[str, AgentRuntimeProfile] = {}
        for bot_id, raw_config in (agent_configs_by_bot_id or {}).items():
            normalized_bot_id = str(bot_id or "").strip()
            if not normalized_bot_id:
                continue
            config = _coerce_agent_runtime_config(
                raw_config,
                runtime_data_dir=runtime_data_dir,
            )
            self._profiles_by_bot_id[normalized_bot_id] = AgentRuntimeProfile(
                self,
                profile_id=config.agent_id or normalized_bot_id,
                config=config,
            )
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
        if database is None:
            return

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

    @property
    def prompt_file_config(self) -> PromptFileLoadConfig:
        return self._default_profile.prompt_file_config

    @property
    def review_runtime_config(self) -> ReviewRuntimeConfig:
        return self._default_profile.review_runtime_config

    @property
    def review_workflow_config(self) -> ReviewWorkflowConfig:
        return self._default_profile.review_workflow_config

    @property
    def prompt_registry(self) -> PromptRegistry:
        return self._default_profile.prompt_registry

    @property
    def active_chat_timer(self) -> ActiveChatTimerService:
        return self._default_profile.active_chat_timer

    @property
    def review_coordinator(self) -> ReviewCoordinator | None:
        return self._default_profile.review_coordinator

    @review_coordinator.setter
    def review_coordinator(self, value: ReviewCoordinator | None) -> None:
        self._default_profile.review_coordinator = value

    @property
    def active_chat_workflow(self) -> ActiveChatCoordinator:
        return self._default_profile.active_chat_workflow

    @active_chat_workflow.setter
    def active_chat_workflow(self, value: ActiveChatCoordinator) -> None:
        self._default_profile.active_chat_workflow = value

    @property
    def agent_scheduler(self) -> AgentScheduler:
        return self._default_profile.agent_scheduler

    @agent_scheduler.setter
    def agent_scheduler(self, value: AgentScheduler) -> None:
        self._default_profile.agent_scheduler = value

    def agent_profile_for_bot(self, bot_id: str) -> AgentRuntimeProfile:
        """Return the profile selected for a bot id, falling back to default."""

        return self._profiles_by_bot_id.get(str(bot_id or "").strip(), self._default_profile)

    def _create_prompt_registry(
        self,
        prompt_file_config: PromptFileLoadConfig,
    ) -> PromptRegistry:
        registry = PromptRegistry(
            context_manager=self.context_manager,
            identity_store=self.identity_store,
        )
        register_identity_prompt_components(
            registry,
            identity_store=self.identity_store,
            prompt_file_config=prompt_file_config,
        )
        register_runtime_prompt_components(
            registry,
            message_text_resolver=registry.resolve_builtin_message_text_prompt,
            current_time_resolver=registry.resolve_builtin_current_time_prompt,
        )
        register_media_prompt_components(
            registry,
            prompt_file_config=prompt_file_config,
        )
        register_review_prompt_components(
            registry,
            prompt_file_config=prompt_file_config,
        )
        register_active_chat_prompt_components(
            registry,
            prompt_file_config=prompt_file_config,
        )
        return registry

    def reload_prompt_files(self) -> None:
        """Reload file-backed prompt components from the configured runtime prompt root."""

        for profile in self._unique_profiles():
            profile.reload_prompt_files()

    async def handle_agent_entry(self, signal: AgentEntrySignal) -> None:
        """Receive the minimal routing signal and let Agent internals process it."""
        await self.agent_profile_for_bot(signal.bot_id).agent_scheduler.accept_signal(signal)

    def _resolve_response_profile(self, signal: AgentEntrySignal) -> str:
        instance_config = None
        if self.database is not None:
            instance_config = self.database.instance_configs.get_by_instance_id(
                signal.instance_id
            )

        return select_response_profile(
            instance_config,
            is_private=signal.is_private,
            is_mentioned=signal.is_mentioned,
            is_reply_to_bot=signal.is_reply_to_bot,
        )

    def handle_ingress_message(self, context: Any) -> None:
        """Let Agent-owned media services observe accepted inbound messages."""
        self.media_ingress_hook(context)

    async def _run_active_chat_fast_round(
        self,
        profile: AgentRuntimeProfile,
        runner: ActiveChatFastRunner,
        batch: active_chat_coordinator_models.ActiveChatBatch,
    ) -> active_chat_coordinator_models.ActiveChatRoundResult:
        result = await runner.run(_workflow_active_chat_batch_from_coordinator(batch))
        return _coordinator_active_chat_result_from_workflow(result)

    async def _drain_active_chat_pending_for_repair(
        self,
        profile: AgentRuntimeProfile,
        batch: active_chat_workflow_models.ActiveChatBatch,
    ) -> list[active_chat_workflow_models.ActiveChatMessageSignal]:
        coordinator_messages = await profile.active_chat_workflow.drain_pending_for_repair(
            _coordinator_active_chat_batch_from_workflow(batch)
        )
        return [
            _workflow_active_chat_message_from_coordinator(message)
            for message in coordinator_messages
        ]

    async def shutdown(self) -> None:
        """Shut down Agent-side background services."""
        for profile in self._unique_profiles():
            await profile.shutdown()
        if self.media_inspection_runner is not None:
            await self.media_inspection_runner.shutdown()

    def _unique_profiles(self) -> list[AgentRuntimeProfile]:
        profiles = [self._default_profile, *self._profiles_by_bot_id.values()]
        result: list[AgentRuntimeProfile] = []
        seen: set[int] = set()
        for profile in profiles:
            marker = id(profile)
            if marker in seen:
                continue
            seen.add(marker)
            result.append(profile)
        return result


def install_agent_runtime(
    bot: ShinBot,
    *,
    review_runtime_config: ReviewRuntimeConfig | dict[str, Any] | None = None,
    prompt_file_config: PromptFileLoadConfig | dict[str, Any] | None = None,
    agent_config: AgentRuntimeConfig | dict[str, Any] | None = None,
    agent_configs_by_bot_id: dict[str, AgentRuntimeConfig | dict[str, Any]] | None = None,
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
        prompt_file_config=prompt_file_config,
        agent_config=agent_config,
        agent_configs_by_bot_id=agent_configs_by_bot_id,
    )
    bot.mount_agent_runtime(runtime)
    return runtime


def _coerce_agent_runtime_config(
    value: AgentRuntimeConfig | dict[str, Any] | None,
    *,
    runtime_data_dir: Path,
) -> AgentRuntimeConfig:
    if isinstance(value, AgentRuntimeConfig):
        return value
    if isinstance(value, dict):
        return agent_runtime_config_from_mapping(value, data_dir=runtime_data_dir)
    return agent_runtime_config_from_mapping({}, data_dir=runtime_data_dir)


def _coerce_review_runtime_config(
    value: ReviewRuntimeConfig | dict[str, Any] | None,
) -> ReviewRuntimeConfig:
    if isinstance(value, ReviewRuntimeConfig):
        return value
    return ReviewRuntimeConfig.from_mapping(value)


def _coerce_prompt_file_config(
    value: PromptFileLoadConfig | dict[str, Any] | None,
    runtime_data_dir: Path,
) -> PromptFileLoadConfig:
    if isinstance(value, PromptFileLoadConfig):
        return value
    if isinstance(value, dict):
        data_root = value.get("data_root") or value.get("prompt_data_root")
        raw_fallbacks = value.get("fallback_locales")
        if isinstance(raw_fallbacks, str):
            fallback_locales = (raw_fallbacks,)
        elif isinstance(raw_fallbacks, (list, tuple)):
            fallback_locales = tuple(
                str(item).strip() for item in raw_fallbacks if str(item).strip()
            )
        else:
            fallback_locales = ("en-US",)
        return PromptFileLoadConfig(
            locale=str(value.get("locale") or "zh-CN"),
            fallback_locales=fallback_locales,
            data_root=(
                _resolve_data_relative_path(data_root, runtime_data_dir)
                if data_root
                else runtime_data_dir / "prompts"
            ),
            sync_to_data=bool(value.get("sync_to_data", True)),
        )
    return PromptFileLoadConfig.from_data_dir(runtime_data_dir)


def _resolve_data_relative_path(value: Any, runtime_data_dir: Path) -> Path:
    path = Path(str(value))
    return path if path.is_absolute() else runtime_data_dir / path


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
