"""Agent runtime service assembly.

This module owns Agent-side service wiring so the core application only has to
attach the Agent entry handler to message routing.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

from shinbot.admin.persona_files import PersonaFileRepository, persona_prompt_component
from shinbot.admin.prompt_definition_admin import PromptDefinitionFileRepository
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
from shinbot.agent.coordinators.review.models import (
    ReviewSchedulerCommitDecision,
    ReviewSchedulerCommitIntent,
    ReviewSchedulerCommitKind,
    ReviewWorkflowConfig,
)
from shinbot.agent.runtime.config import (
    AgentRuntimeConfig,
    AgentRuntimeConfigError,
    SummaryMarkdownConfig,
    agent_runtime_config_from_mapping,
    validate_agent_runtime_config_references,
)
from shinbot.agent.runtime.instance_config import RuntimeModelTarget, parse_tagged_llm_ref
from shinbot.agent.runtime.legacy_session_quiescence import (
    LegacySessionAllProfilesTaskQuiescer,
    LegacySessionLocalTaskQuiescer,
)
from shinbot.agent.runtime.legacy_signal_admission import (
    LegacyAgentSignalAdmissionRegistry,
    LegacyAgentSignalFreezeTicket,
    LegacyAgentSignalFrozen,
    LegacyAgentSignalQuiescenceReceipt,
)
from shinbot.agent.runtime.prompt_registration import register_runtime_prompt_components
from shinbot.agent.runtime.review_stores import (
    DatabaseReviewMessageStore,
    DatabaseReviewSummaryStore,
)
from shinbot.agent.runtime.session_actor.active_chat_workflow import (
    RunnerActiveChatBootstrapWorkflow,
    RunnerActiveChatRoundWorkflow,
    build_actor_active_chat_bootstrap_runner,
    build_actor_active_chat_round_plan_runner,
)
from shinbot.agent.runtime.session_actor.active_chat_workflow_context import (
    ActorActiveChatBootstrapWorkflowContextProjector,
    ActorActiveChatRoundWorkflowContextProjector,
)
from shinbot.agent.runtime.session_actor.active_reply_workflow import (
    RunnerActiveReplyWorkflow,
    build_actor_active_reply_decision_runner,
)
from shinbot.agent.runtime.session_actor.active_reply_workflow_context import (
    ActorActiveReplyWorkflowContextProjector,
)
from shinbot.agent.runtime.session_actor.adapter_action_dispatch import (
    AdapterExternalActionDispatcher,
)
from shinbot.agent.runtime.session_actor.external_action_store import (
    SQLiteExternalActionReceiptStore,
)
from shinbot.agent.runtime.session_actor.idle_review_planning_adapter import (
    RunnerIdleReviewPlanningWorkflow,
)
from shinbot.agent.runtime.session_actor.idle_review_planning_context import (
    ActorIdleReviewPlanningContextProjector,
)
from shinbot.agent.runtime.session_actor.profile_handler_graph import (
    ActorProfileWorkflowPorts,
    ActorV2ProfileHandlerGraph,
)
from shinbot.agent.runtime.session_actor.review_due_scanner import (
    DurableReviewDueRepository,
    ManualReviewAdmissionError,
    ManualReviewAdmissionService,
)
from shinbot.agent.runtime.session_actor.review_workflow import (
    RunnerReviewWorkflow,
    build_actor_review_reply_decision_runner,
)
from shinbot.agent.runtime.session_actor.review_workflow_context import (
    ActorReviewWorkflowContextProjector,
)
from shinbot.agent.runtime.session_actor.runtime_assembly import (
    ActorV2RuntimeAssembly,
    ActorV2RuntimeCompositionPorts,
    ActorV2RuntimeDiagnostics,
    ActorV2WorkflowLedger,
)
from shinbot.agent.runtime.task_manager import AgentTaskManager
from shinbot.agent.scheduler import (
    ActiveChatTimerService,
    AgentScheduler,
    ReviewDueTimerService,
)
from shinbot.agent.scheduler.active_chat_policy import DefaultActiveChatPolicy
from shinbot.agent.scheduler.models import (
    ActiveChatBootstrapApplyDecision,
    ActiveChatState,
    AgentState,
    IdleReviewPlanningRequest,
    ReviewCompletionDecision,
    ReviewPlan,
    UnreadMessage,
    review_plan_fence_matches,
)
from shinbot.agent.scheduler.review_policy import DefaultReviewPolicy
from shinbot.agent.services.context import ContextManager
from shinbot.agent.services.context.active_chat_context import ActiveChatContextBuilderAdapter
from shinbot.agent.services.context.prompt_registration import register_context_prompt_components
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
from shinbot.agent.services.prompt_engine import PromptFileLoadConfig, PromptRegistry, PromptStage
from shinbot.agent.services.prompt_engine.runtime_sync import sync_prompt_definition_components
from shinbot.agent.services.summaries import MarkdownSummaryStore, SummaryService
from shinbot.agent.services.tools import ToolManager, ToolRegistry
from shinbot.agent.signals import AgentSignal, AgentSignalKind
from shinbot.agent.workflows.active_chat import ActiveChatFastRunner
from shinbot.agent.workflows.active_chat import models as active_chat_workflow_models
from shinbot.agent.workflows.active_chat.prompt_registration import (
    register_active_chat_prompt_components,
)
from shinbot.agent.workflows.chat_actions import register_chat_action_tools
from shinbot.core.dispatch.agent_identity import (
    DEFAULT_SESSION_ACTOR_PROFILE_ID,
    SessionKey,
)
from shinbot.core.dispatch.agent_ownership import (
    AgentRuntimeOwnershipMode,
    AgentRuntimeOwnershipStatus,
)
from shinbot.core.instance_config import (
    resolve_instance_runtime_config,
    select_response_profile,
)
from shinbot.core.state.session import (
    SESSION_STATE_AGENT_PAUSE_UNTIL_KEY,
    SessionManager,
    get_agent_pause_until,
    set_agent_pause_until,
)
from shinbot.utils.logger import format_log_event, get_logger

if TYPE_CHECKING:
    from shinbot.agent.runtime.legacy_session_local_drain import (
        LegacySessionLocalDrainParticipant,
    )
    from shinbot.core.application.app import ShinBot
    from shinbot.core.dispatch.ingress import MessageIngress
    from shinbot.core.platform.adapter_manager import AdapterManager
    from shinbot.core.security.audit import AuditLogger
    from shinbot.core.security.permission import PermissionEngine
    from shinbot.persistence import DatabaseManager

logger = get_logger(__name__, source="agent:runtime", color="magenta")


@dataclass(slots=True, frozen=True)
class AgentRuntimeReviewAdmissionResult:
    """Outcome of one profile-scoped management review request.

    ``actor_v2`` means the request was durably admitted to the fenced mailbox,
    not that an Actor target was published or that review work has started.
    """

    profile_id: str
    session_id: str
    success: bool
    runtime_kind: str
    disposition: str
    reason: str = ""
    request_id: str = ""
    event_id: str = ""
    mailbox_id: int | None = None


class AgentRuntimeProfile:
    """Per-bot runtime wiring with a stable durable ownership profile id.

    ``profile_id`` is either the selected ``BotServiceConfig.id`` or the
    reserved default-profile constant. The editable ``config.agent_id`` is
    behavior metadata and never participates in durable session ownership.
    """

    def __init__(
        self,
        owner: AgentRuntime,
        *,
        profile_id: str,
        bot_id: str = "",
        config: AgentRuntimeConfig,
    ) -> None:
        self._owner = owner
        self.profile_id = profile_id
        self.bot_id = bot_id
        self.config = config
        self._base_active_chat_attention_config = replace(config.active_chat_attention_config)
        self._background_tasks_started = False
        self._background_tasks_start_lock = asyncio.Lock()
        self.prompt_file_config = config.prompt_file_config or PromptFileLoadConfig.from_data_dir(
            owner.runtime_data_dir
        )
        self.prompt_registry = owner._create_prompt_registry(self.prompt_file_config)
        self._validate_config_references()
        persona_component_id = self._sync_persona_prompt_component()
        if persona_component_id:
            self.config = _agent_config_with_persona_component(
                self.config,
                persona_component_id,
            )
        self.review_runtime_config = self.config.review_runtime_config
        self.review_workflow_config = self.config.review_workflow_config
        self.active_chat_timer = ActiveChatTimerService(
            tick_interval_seconds=self.config.active_chat_policy_config.tick_interval_seconds
        )
        self.active_chat_timer.bind_agent_runtime(self._owner, bot_id=self.bot_id)
        self.active_chat_timer.bind_task_scope(
            self._owner.task_manager.scope(self._task_namespace("active_chat_timer"))
        )
        self.review_due_timer = ReviewDueTimerService(
            tick_interval_seconds=self.config.review_due_tick_interval_seconds
        )
        self.review_coordinator: ReviewCoordinator | None = None
        self.active_chat_workflow = self._create_active_chat_workflow()
        self.active_chat_workflow.bind_task_scope(
            self._owner.task_manager.scope(self._task_namespace("active_chat_workflow"))
        )
        self._workflow_dispatcher = ActiveReplyDispatcher(
            active_chat_workflow=self.active_chat_workflow,
            summary_service=owner.summary_service,
            review_config=self.review_workflow_config,
            active_reply_completion_handler=self._complete_active_reply_from_task,
            idle_review_planning_recorder=self._record_idle_review_planning_model_result,
        )
        self._workflow_dispatcher.bind_active_reply_task_scope(
            self._owner.task_manager.scope(self._task_namespace("active_reply"))
        )
        self._workflow_dispatcher.bind_review_task_scope(
            self._owner.task_manager.scope(self._task_namespace("review_workflow"))
        )
        self.agent_scheduler = self._create_agent_scheduler(self._workflow_dispatcher)
        self.review_due_timer.bind_agent_runtime(self._owner, bot_id=self.bot_id)
        self.review_due_timer.bind_task_scope(
            self._owner.task_manager.scope(self._task_namespace("review_due_timer"))
        )

        if owner.database is None:
            return

        self.review_coordinator = self._create_review_coordinator()
        self.active_chat_workflow = self._create_active_chat_workflow()
        self.active_chat_workflow.bind_task_scope(
            self._owner.task_manager.scope(self._task_namespace("active_chat_workflow"))
        )
        active_chat_fast_runner = ActiveChatFastRunner(
            owner.model_runtime,
            prompt_registry=self.prompt_registry,
            tool_manager=owner.tool_manager,
            message_store=owner.database.message_logs,
            context_builder=ActiveChatContextBuilderAdapter(
                owner.context_manager,
                message_formatter=owner.message_formatter,
                message_format_config=self.config.default_message_format_config,
            ),
            message_formatter=owner.message_formatter,
            pending_message_provider=lambda batch: owner._drain_active_chat_pending_for_repair(
                self,
                batch,
            ),
            config=replace(
                self.config.active_chat_fast_runner_config,
                instance_config_resolver=owner._resolve_instance_runtime_config,
                model_target_resolver=owner._resolve_model_target,
            ),
        )
        self.active_chat_workflow.set_round_handler(
            lambda batch: owner._run_active_chat_fast_round(
                self,
                active_chat_fast_runner,
                batch,
            )
        )
        runner_factory = self._create_review_runner_factory()
        self._workflow_dispatcher = ActiveReplyDispatcher(
            review_coordinator=self.review_coordinator,
            active_chat_workflow=self.active_chat_workflow,
            summary_service=owner.summary_service,
            review_config=self.review_workflow_config,
            active_reply_completion_handler=self._complete_active_reply_from_task,
            idle_review_planning_runner=runner_factory.create_idle_review_planning_runner(),
            idle_review_planning_recorder=self._record_idle_review_planning_model_result,
            review_run_recorder=owner._record_review_workflow_run,
        )
        self._workflow_dispatcher.bind_active_reply_task_scope(
            self._owner.task_manager.scope(self._task_namespace("active_reply"))
        )
        self._workflow_dispatcher.bind_review_task_scope(
            self._owner.task_manager.scope(self._task_namespace("review_workflow"))
        )
        self.agent_scheduler = self._create_agent_scheduler(self._workflow_dispatcher)
        self.review_due_timer.bind_agent_runtime(self._owner, bot_id=self.bot_id)
        self.review_due_timer.bind_task_scope(
            self._owner.task_manager.scope(self._task_namespace("review_due_timer"))
        )

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
        sync_prompt_definition_components(
            self.prompt_registry,
            self._owner.prompt_definitions,
        )

    async def shutdown(self) -> None:
        """Shut down profile-owned background tasks."""

        await self._owner.task_manager.shutdown(prefix=self._task_namespace("active_reply"))
        await self._owner.task_manager.shutdown(prefix=self._task_namespace("review_workflow"))
        if self.review_coordinator is not None:
            await self.review_coordinator.shutdown()
        self._workflow_dispatcher.flush_active_chat_summaries()
        await self.active_chat_workflow.shutdown()
        await self.active_chat_timer.shutdown()
        await self.review_due_timer.shutdown()
        await self._owner.task_manager.shutdown(prefix=self._task_namespace(""))

    def build_legacy_session_local_task_quiescer(
        self,
    ) -> LegacySessionLocalTaskQuiescer:
        """Build an unmounted current-process legacy task observer.

        The returned object has no ingress, scheduler, durable-ownership, or
        adapter authority. It is a future controller building block only and
        cannot authorize an Actor v2 cutover by itself.
        """

        return LegacySessionLocalTaskQuiescer(
            active_chat_timer=self.active_chat_timer,
            review_due_timer=self.review_due_timer,
            review_dispatcher=self._workflow_dispatcher,
            review_coordinator=self.review_coordinator,
            active_chat_workflow=self.active_chat_workflow,
        )

    async def start_background_tasks(self, *, start_timers: bool = True) -> None:
        """Recover scheduler state, then arm profile-owned timers.

        Recovery is part of the runtime lifecycle rather than a scheduler-only
        shortcut: every persisted session is reconciled under the same mutex as
        ingress, workflow completion, and management mutations. Timers are
        intentionally armed only after that pass completes.
        """

        async with self._background_tasks_start_lock:
            if self._background_tasks_started:
                return
            if not start_timers:
                self._background_tasks_started = True
                return
            await self._owner._reconcile_profile_startup_sessions(self)
            self.review_due_timer.start()
            self._background_tasks_started = True

    async def plan_idle_review_after_active_chat(
        self,
        request: IdleReviewPlanningRequest,
    ) -> ReviewPlan | None:
        """Run one frozen idle-review planner request outside the session mutex."""

        return await self._workflow_dispatcher.plan_idle_review_after_active_chat(
            request.session_id,
            request=request,
        )

    async def _complete_active_reply_from_task(self, session_id: str) -> None:
        """Commit an active-reply task completion through the runtime fence."""

        await self._owner._complete_active_reply_for_profile(self, session_id)

    async def restore_active_chat_session(
        self,
        *,
        session_id: str,
        active_chat_state: ActiveChatState,
        initial_unread_messages: list[UnreadMessage],
    ) -> None:
        """Rebuild process-local active-chat state from durable scheduler state.

        Scheduler interest state survives a restart, while the coordinator's
        attention buffer does not. Replaying its still-unread messages gives
        the restored coordinator the same work inventory without reapplying
        scheduler-side interest updates.
        """

        await self._workflow_dispatcher.start_active_chat(
            session_id=session_id,
            active_chat_state=active_chat_state,
            initial_unread_messages=initial_unread_messages,
        )

    async def _commit_active_chat_round_from_task(
        self,
        intent: active_chat_coordinator_models.ActiveChatRoundCommitIntent,
    ) -> active_chat_coordinator_models.ActiveChatRoundCommitDecision:
        """Commit one active-chat round through the runtime session fence."""

        return await self._owner._commit_active_chat_round_for_profile(self, intent)

    async def _commit_review_scheduler_mutation_from_task(
        self,
        intent: ReviewSchedulerCommitIntent,
    ) -> ReviewSchedulerCommitDecision:
        """Commit one review mutation through the runtime session fence."""

        return await self._owner._commit_review_scheduler_mutation_for_profile(
            self,
            intent,
        )

    def _record_idle_review_planning_model_result(
        self,
        request: IdleReviewPlanningRequest,
        result: Any,
    ) -> None:
        """Persist a sanitized planner outcome through the owning runtime."""

        self._owner._record_idle_review_planning_model_result(
            profile_id=self.profile_id,
            request=request,
            result=result,
        )

    def _record_idle_review_planning_application(
        self,
        request: IdleReviewPlanningRequest,
        model_plan: ReviewPlan | None,
        decision: Any,
    ) -> None:
        """Persist the scheduler's terminal fenced-plan application result."""

        skipped_reason = str(getattr(decision, "skipped_reason", "") or "")
        self._owner._record_idle_review_planning_application(
            profile=self,
            request=request,
            model_plan=model_plan,
            decision=decision,
            outcome=(
                "discarded"
                if skipped_reason
                else (
                    "applied_model_plan"
                    if model_plan is not None
                    else "applied_static_fallback"
                )
            ),
            reason=(
                skipped_reason
                or (
                    model_plan.reason
                    if model_plan is not None
                    else "scheduler_static_policy"
                )
            ),
        )

    def _create_active_chat_workflow(self) -> ActiveChatCoordinator:
        workflow = ActiveChatCoordinator(
            attention=ActiveChatAttention(self.config.active_chat_attention_config),
            conversation_message_limit=self.config.active_chat_conversation_message_limit,
            interest_effect_config=self.config.active_chat_interest_effect_config,
            round_commit_handler=self._commit_active_chat_round_from_task,
        )
        return workflow

    def set_active_chat_threshold_delta(self, delta: float, *, source: str = "") -> None:
        """Apply a runtime-only active chat threshold delta for this profile."""

        target_threshold = max(0.0, self._base_active_chat_attention_config.base_threshold + delta)
        next_config = replace(
            self._base_active_chat_attention_config,
            base_threshold=target_threshold,
        )
        self.config = replace(self.config, active_chat_attention_config=next_config)
        self.active_chat_workflow.update_attention_config(next_config)
        logger.debug(
            format_log_event(
                "agent.active_chat.threshold.updated",
                profile_id=self.profile_id,
                bot_id=self.bot_id,
                source=source,
                base=f"{self._base_active_chat_attention_config.base_threshold:.3f}",
                delta=f"{delta:.3f}",
                target=f"{target_threshold:.3f}",
            )
        )

    def _create_agent_scheduler(self, workflow_dispatcher: Any) -> AgentScheduler:
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
            idle_review_planning_application_recorder=(
                self._record_idle_review_planning_application
            ),
        )

    def _create_review_coordinator(self) -> ReviewCoordinator:
        assert self._owner.database is not None
        runner_factory = self._create_review_runner_factory()
        return ReviewCoordinator(
            self.review_workflow_config,
            message_store=DatabaseReviewMessageStore(self._owner.database),
            summary_store=DatabaseReviewSummaryStore(self._owner.database),
            context_builder=ReviewContextBuilderAdapter(),
            bootstrap_signal_handler=self._owner.handle_agent_signal,
            scheduler_commit_handler=self._commit_review_scheduler_mutation_from_task,
            bot_id=self.bot_id,
            bootstrap_task_scope=self._owner.task_manager.scope(
                self._task_namespace("review_bootstrap")
            ),
            block_digest_task_scope=self._owner.task_manager.scope(
                self._task_namespace("review_block_digest")
            ),
            **runner_factory.create_workflow_runner_kwargs(),
        )

    def _task_namespace(self, suffix: str) -> str:
        bot_part = self.bot_id or self.profile_id
        return f"agent:{bot_part}:{suffix}"

    def _session_id_prefix(self) -> str | None:
        return f"{self.bot_id}:" if self.bot_id else None

    def _create_review_runner_factory(self) -> ReviewRunnerFactory:
        return ReviewRunnerFactory(
            self._owner.model_runtime,
            config=self.review_runtime_config,
            prompt_registry=self.prompt_registry,
            tool_manager=self._owner.tool_manager,
            summary_service=self._owner.summary_service,
            message_formatter=self._owner.message_formatter,
            instance_config_resolver=self._owner._resolve_instance_runtime_config,
            model_target_resolver=self._owner._resolve_model_target,
        )

    def create_actor_v2_workflow_ports(
        self,
        *,
        ledger: ActorV2WorkflowLedger,
        message_store: DatabaseReviewMessageStore,
    ) -> ActorProfileWorkflowPorts:
        """Build strict Actor v2 workflow ports for this exact durable profile.

        These ports only project effect-fenced ledger rows and collect deferred
        action intents.  They intentionally do not receive the legacy
        scheduler, dispatcher, review coordinator, or Active Chat coordinator.
        """

        owner = self._owner
        reply_config = self.review_runtime_config.reply_decision.to_llm_config(
            instance_config_resolver=owner._resolve_instance_runtime_config,
            model_target_resolver=owner._resolve_model_target,
        )
        bootstrap_config = self.review_runtime_config.active_chat_bootstrap.to_llm_config(
            instance_config_resolver=owner._resolve_instance_runtime_config,
            model_target_resolver=owner._resolve_model_target,
        )
        round_config = replace(
            self.config.active_chat_fast_runner_config,
            instance_config_resolver=owner._resolve_instance_runtime_config,
            model_target_resolver=owner._resolve_model_target,
        )
        review_workflow = RunnerReviewWorkflow(
            projector=ActorReviewWorkflowContextProjector(message_store=message_store),
            reply_runner=build_actor_review_reply_decision_runner(
                owner.model_runtime,
                prompt_registry=self.prompt_registry,
                config=reply_config,
                tool_manager=owner.tool_manager,
                message_formatter=owner.message_formatter,
            ),
        )
        active_reply_workflow = RunnerActiveReplyWorkflow(
            projector=ActorActiveReplyWorkflowContextProjector(message_store=message_store),
            reply_runner=build_actor_active_reply_decision_runner(
                owner.model_runtime,
                prompt_registry=self.prompt_registry,
                config=reply_config,
                tool_manager=owner.tool_manager,
                message_formatter=owner.message_formatter,
            ),
        )
        active_chat_bootstrap_workflow = RunnerActiveChatBootstrapWorkflow(
            projector=ActorActiveChatBootstrapWorkflowContextProjector(
                ledger=ledger,
                message_store=message_store,
            ),
            bootstrap_runner=build_actor_active_chat_bootstrap_runner(
                owner.model_runtime,
                prompt_registry=self.prompt_registry,
                config=bootstrap_config,
                message_formatter=owner.message_formatter,
            ),
        )
        active_chat_round_workflow = RunnerActiveChatRoundWorkflow(
            projector=ActorActiveChatRoundWorkflowContextProjector(
                ledger=ledger,
                message_store=message_store,
            ),
            plan_runner=build_actor_active_chat_round_plan_runner(
                owner.model_runtime,
                prompt_registry=self.prompt_registry,
                tool_manager=owner.tool_manager,
                config=round_config,
                message_formatter=owner.message_formatter,
            ),
        )
        idle_review_planning_workflow = RunnerIdleReviewPlanningWorkflow(
            projector=ActorIdleReviewPlanningContextProjector(
                ledger=ledger,
                message_store=message_store,
            ),
            runner=self._create_review_runner_factory().create_idle_review_planning_runner(),
        )
        return ActorProfileWorkflowPorts(
            profile_id=self.profile_id,
            active_reply_workflow=active_reply_workflow,
            review_workflow=review_workflow,
            active_chat_bootstrap_workflow=active_chat_bootstrap_workflow,
            active_chat_round_workflow=active_chat_round_workflow,
            idle_review_planning_workflow=idle_review_planning_workflow,
        )

    def _validate_config_references(self) -> None:
        payload = self.config.raw_mapping
        if not payload and self.config.persona_id:
            payload = {"agent": {"persona_id": self.config.persona_id}}
        issues = validate_agent_runtime_config_references(
            payload,
            model_registry=(
                self._owner.database.model_registry if self._owner.database is not None else None
            ),
            prompt_registry=self.prompt_registry,
            persona_repository=self._owner.personas,
        )
        if issues:
            config_path = (
                Path(self.config.source_path) if self.config.source_path else Path(self.profile_id)
            )
            raise AgentRuntimeConfigError(
                _format_agent_config_reference_issues(config_path, list(issues))
            )

    def _sync_persona_prompt_component(self) -> str:
        if not self.config.persona_id:
            return ""
        persona = self._owner.personas.get(self.config.persona_id)
        if persona is None:
            return ""
        component = persona_prompt_component(persona)
        self.prompt_registry.upsert_component(component)
        return component.id


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
        session_manager: SessionManager | None,
        model_runtime: Any,
        tool_registry: ToolRegistry | None = None,
        review_runtime_config: ReviewRuntimeConfig | dict[str, Any] | None = None,
        prompt_file_config: PromptFileLoadConfig | dict[str, Any] | None = None,
        agent_config: AgentRuntimeConfig | dict[str, Any] | None = None,
        agent_configs_by_bot_id: dict[str, AgentRuntimeConfig | dict[str, Any]] | None = None,
    ) -> None:
        runtime_data_dir = Path(data_dir)
        default_config = _coerce_agent_runtime_config(
            agent_config,
            runtime_data_dir=runtime_data_dir,
        )
        self.runtime_data_dir = runtime_data_dir
        self.database = database
        self._actor_v2_assembly: ActorV2RuntimeAssembly | None = None
        self._actor_v2_handler_graph: ActorV2ProfileHandlerGraph | None = None
        self._manual_review_admission: ManualReviewAdmissionService | None = None
        self.personas = PersonaFileRepository.from_data_dir(runtime_data_dir)
        self.personas.ensure_default_persona()
        self.prompt_definitions = PromptDefinitionFileRepository.from_data_dir(runtime_data_dir)
        self.model_runtime = model_runtime
        self.adapter_manager = adapter_manager
        self.session_manager = session_manager
        self._session_signal_locks: dict[str, asyncio.Lock] = {}
        self._idle_review_planning_requests: dict[str, IdleReviewPlanningRequest] = {}
        self._legacy_signal_admission = LegacyAgentSignalAdmissionRegistry()
        self._audit_logger = audit_logger
        self.identity_store = IdentityStore(runtime_data_dir / "identities.json")
        self.media_service = MediaService(database) if database is not None else None
        self.message_formatter = MessageFormatterService(
            identity_store=self.identity_store,
            media_service=self.media_service,
        )
        self.summary_service = (
            SummaryService(
                database.agent_summaries,
                markdown_store=_summary_markdown_store(default_config.summary_markdown_config),
            )
            if database is not None
            else None
        )
        self.context_manager = (
            ContextManager(
                database.message_logs,
                data_dir=runtime_data_dir,
                identity_store=self.identity_store,
                media_service=self.media_service,
                summary_service=self.summary_service,
            )
            if database is not None
            else None
        )
        self.tool_registry = tool_registry or ToolRegistry()
        self.tool_manager = ToolManager(
            self.tool_registry,
            permission_engine=permission_engine,
            audit_logger=audit_logger,
        )
        self.task_manager = AgentTaskManager()
        register_identity_tools(self.tool_registry, self.identity_store, self.context_manager)

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
            profile_id=DEFAULT_SESSION_ACTOR_PROFILE_ID,
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
                profile_id=normalized_bot_id,
                bot_id=normalized_bot_id,
                config=config,
            )
        self.media_inspection_runner = (
            MediaInspectionRunner(
                database,
                self.prompt_registry,
                self.model_runtime,
                self.media_service,
                self.prompt_definitions,
            )
            if database is not None and self.media_service is not None
            else None
        )
        if self.media_inspection_runner is not None:
            self.media_inspection_runner.bind_task_scope(
                self.task_manager.scope("agent:media_inspection")
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
        # This producer has no mailbox notifier. Admission can only create
        # durable work for an already-owned Actor v2 session; a future target
        # lifecycle remains solely responsible for publication and delivery.
        self._manual_review_admission = ManualReviewAdmissionService(
            DurableReviewDueRepository(database)
        )
        self._actor_v2_assembly = self._compose_actor_v2_diagnostics(database)

    @property
    def prompt_file_config(self) -> PromptFileLoadConfig:
        """Prompt file load configuration for the default profile."""
        return self._default_profile.prompt_file_config

    @property
    def review_runtime_config(self) -> ReviewRuntimeConfig:
        """Review workflow runtime configuration for the default profile."""
        return self._default_profile.review_runtime_config

    @property
    def review_workflow_config(self) -> ReviewWorkflowConfig:
        """Review workflow stage configuration for the default profile."""
        return self._default_profile.review_workflow_config

    @property
    def prompt_registry(self) -> PromptRegistry:
        """Structured prompt component registry for the default profile."""
        return self._default_profile.prompt_registry

    @property
    def active_chat_timer(self) -> ActiveChatTimerService:
        """Timer service that drives active-chat idle callbacks."""
        return self._default_profile.active_chat_timer

    @property
    def review_coordinator(self) -> ReviewCoordinator | None:
        """Review coordinator, or ``None`` when no database is available."""
        return self._default_profile.review_coordinator

    @review_coordinator.setter
    def review_coordinator(self, value: ReviewCoordinator | None) -> None:
        """Set the review coordinator on the default profile."""
        self._default_profile.review_coordinator = value

    @property
    def active_chat_workflow(self) -> ActiveChatCoordinator:
        """Active chat workflow coordinator for the default profile."""
        return self._default_profile.active_chat_workflow

    @active_chat_workflow.setter
    def active_chat_workflow(self, value: ActiveChatCoordinator) -> None:
        """Set the active-chat workflow coordinator on the default profile."""
        self._default_profile.active_chat_workflow = value

    @property
    def agent_scheduler(self) -> AgentScheduler:
        """Agent task scheduler that dispatches workflow decisions."""
        return self._default_profile.agent_scheduler

    @agent_scheduler.setter
    def agent_scheduler(self, value: AgentScheduler) -> None:
        """Set the agent scheduler on the default profile."""
        self._default_profile.agent_scheduler = value

    def agent_profile_for_bot(self, bot_id: str) -> AgentRuntimeProfile:
        """Select by stable ``BotServiceConfig.id``, falling back to default."""

        return self._profiles_by_bot_id.get(str(bot_id or "").strip(), self._default_profile)

    async def request_review_for_profile(
        self,
        profile_id: str,
        session_id: str,
        *,
        request_id: str,
        requested_by: str,
        reason: str = "management_trigger_review",
    ) -> AgentRuntimeReviewAdmissionResult:
        """Request review through the runtime selected by an exact session key.

        Legacy ownership retains the existing scheduler behavior. An active
        Actor v2 owner receives only a durable ``ManualReviewRequested``
        mailbox admission; this method does not claim ownership, publish a
        target, or wake an Actor directly.

        Args:
            profile_id: Stable durable Agent profile id.
            session_id: Bot-scoped session id within ``profile_id``.
            request_id: Caller-owned idempotency identity.
            requested_by: Authenticated management principal for the journal.
            reason: Stable management reason carried by the mailbox event.

        Returns:
            A typed outcome that distinguishes a legacy trigger from an Actor
            v2 mailbox admission or fail-closed rejection.
        """

        normalized_profile_id = str(profile_id or "").strip()
        normalized_session_id = str(session_id or "").strip()
        normalized_request_id = str(request_id or "").strip()
        normalized_requested_by = str(requested_by or "").strip()
        normalized_reason = str(reason or "").strip()
        if not normalized_profile_id or not normalized_session_id:
            return AgentRuntimeReviewAdmissionResult(
                profile_id=normalized_profile_id,
                session_id=normalized_session_id,
                success=False,
                runtime_kind="unavailable",
                disposition="invalid_session_key",
                reason="profile_id_and_session_id_required",
            )
        if not normalized_request_id:
            raise ValueError("request_id must not be empty")
        if not normalized_requested_by:
            raise ValueError("requested_by must not be empty")
        if not normalized_reason:
            raise ValueError("reason must not be empty")

        profile = self._profile_for_id(normalized_profile_id)
        if profile is None:
            return AgentRuntimeReviewAdmissionResult(
                profile_id=normalized_profile_id,
                session_id=normalized_session_id,
                success=False,
                runtime_kind="unavailable",
                disposition="unknown_profile",
                reason="profile_not_configured_in_runtime",
                request_id=normalized_request_id,
            )

        database = self.database
        if database is None:
            return AgentRuntimeReviewAdmissionResult(
                profile_id=profile.profile_id,
                session_id=normalized_session_id,
                success=False,
                runtime_kind="unavailable",
                disposition="runtime_storage_unavailable",
                reason="agent_runtime_storage_unavailable",
                request_id=normalized_request_id,
            )

        key = SessionKey(profile.profile_id, normalized_session_id)
        ownership = database.agent_runtime_ownership.get(key)
        if ownership is None:
            admission_fence = database.actor_v2_admission_fences.get(key)
            if admission_fence is not None:
                return AgentRuntimeReviewAdmissionResult(
                    profile_id=profile.profile_id,
                    session_id=normalized_session_id,
                    success=False,
                    runtime_kind="unavailable",
                    disposition=f"admission_fence_{admission_fence.status.value}",
                    reason="session_ownership_is_reserved_for_actor_v2",
                    request_id=normalized_request_id,
                )
            return AgentRuntimeReviewAdmissionResult(
                profile_id=profile.profile_id,
                session_id=normalized_session_id,
                success=False,
                runtime_kind="unavailable",
                disposition="ownership_missing",
                reason="session_runtime_ownership_missing",
                request_id=normalized_request_id,
            )

        if (
            ownership.status is AgentRuntimeOwnershipStatus.ACTIVE
            and ownership.mode is AgentRuntimeOwnershipMode.LEGACY
        ):
            triggered = await self._trigger_legacy_review_for_profile(
                profile,
                normalized_session_id,
            )
            return AgentRuntimeReviewAdmissionResult(
                profile_id=profile.profile_id,
                session_id=normalized_session_id,
                success=triggered,
                runtime_kind="legacy",
                disposition=("triggered" if triggered else "not_triggered"),
                reason=("" if triggered else "legacy_review_not_startable"),
                request_id=normalized_request_id,
            )

        if ownership.status is not AgentRuntimeOwnershipStatus.ACTIVE:
            return AgentRuntimeReviewAdmissionResult(
                profile_id=profile.profile_id,
                session_id=normalized_session_id,
                success=False,
                runtime_kind="unavailable",
                disposition="ownership_migrating",
                reason="session_ownership_transition_in_progress",
                request_id=normalized_request_id,
            )

        if ownership.mode is not AgentRuntimeOwnershipMode.ACTOR_V2:
            return AgentRuntimeReviewAdmissionResult(
                profile_id=profile.profile_id,
                session_id=normalized_session_id,
                success=False,
                runtime_kind="unavailable",
                disposition="ownership_unavailable",
                reason="unsupported_runtime_ownership",
                request_id=normalized_request_id,
            )

        admission = self._manual_review_admission
        if admission is None:
            return AgentRuntimeReviewAdmissionResult(
                profile_id=profile.profile_id,
                session_id=normalized_session_id,
                success=False,
                runtime_kind="actor_v2",
                disposition="admission_unavailable",
                reason="actor_v2_manual_review_admission_unavailable",
                request_id=normalized_request_id,
            )
        try:
            result = await admission.request(
                key,
                request_id=normalized_request_id,
                requested_by=normalized_requested_by,
                reason=normalized_reason,
            )
        except ManualReviewAdmissionError as exc:
            logger.warning(
                format_log_event(
                    "agent.runtime.manual_review_admission_rejected",
                    profile_id=profile.profile_id,
                    session_id=normalized_session_id,
                    request_id=normalized_request_id,
                    error_code=type(exc).__name__,
                )
            )
            return AgentRuntimeReviewAdmissionResult(
                profile_id=profile.profile_id,
                session_id=normalized_session_id,
                success=False,
                runtime_kind="actor_v2",
                disposition="admission_error",
                reason="manual_review_admission_error",
                request_id=normalized_request_id,
            )
        return AgentRuntimeReviewAdmissionResult(
            profile_id=profile.profile_id,
            session_id=normalized_session_id,
            success=result.accepted,
            runtime_kind="actor_v2",
            disposition=result.disposition.value,
            reason=result.reason,
            request_id=result.request_id,
            event_id=result.event_id,
            mailbox_id=result.mailbox_id,
        )

    def freeze_legacy_session_signal_admission(
        self,
        session_id: str,
        *,
        cutover_id: str,
    ) -> LegacyAgentSignalFreezeTicket:
        """Freeze new local legacy Agent signals for one base session.

        This is an unmounted lifecycle primitive for a future controller. It
        does not establish durable admission, pause an adapter, reroute a
        signal to Actor v2, or authorize an ownership transition.
        """

        return self._legacy_signal_admission.freeze(
            session_id,
            cutover_id=cutover_id,
        )

    async def await_legacy_session_signal_quiescent(
        self,
        ticket: LegacyAgentSignalFreezeTicket,
        *,
        timeout_seconds: float | None = None,
    ) -> LegacyAgentSignalQuiescenceReceipt:
        """Observe pre-freeze direct legacy Agent signal calls locally."""

        return await self._legacy_signal_admission.await_quiescent(
            ticket,
            timeout_seconds=timeout_seconds,
        )

    def thaw_legacy_session_signal_admission(
        self,
        ticket: LegacyAgentSignalFreezeTicket,
    ) -> bool:
        """Release a locally drained legacy Agent signal freeze ticket."""

        return self._legacy_signal_admission.thaw(ticket)

    def is_legacy_session_signal_admission_frozen(self, session_id: str) -> bool:
        """Return whether a future local drain closed legacy signal entry.

        Timer services use this read-only check to avoid converting an
        intentional local lifecycle freeze into repeated supervision failures.
        It does not make the session durable or route it to Actor v2.
        """

        return self._legacy_signal_admission.is_frozen(session_id)

    def legacy_review_due_admission_reason(
        self,
        profile_id: str,
        session_id: str,
    ) -> str:
        """Return why a legacy review timer must not mutate one session.

        This is a compatibility guard around the legacy timer only. It keeps
        existing unowned legacy scheduler state eligible while preventing a
        due signal from becoming a second writer after Actor v2 ownership,
        migration, or a pending Actor admission fence appears.
        """

        database = self.database
        if database is None:
            return ""
        try:
            key = SessionKey(profile_id, session_id)
        except ValueError:
            return "invalid_session_key"
        ownership = database.agent_runtime_ownership.get(key)
        if ownership is None:
            admission_fence = database.actor_v2_admission_fences.get(key)
            if admission_fence is None:
                return ""
            return f"admission_fence_{admission_fence.status.value}"
        if ownership.status is not AgentRuntimeOwnershipStatus.ACTIVE:
            return "ownership_migrating"
        if ownership.mode is AgentRuntimeOwnershipMode.LEGACY:
            return ""
        return "actor_v2_owned"

    def build_legacy_base_session_local_task_quiescer(
        self,
    ) -> LegacySessionAllProfilesTaskQuiescer:
        """Build an unmounted task observer for every profile sharing a base session.

        A bot-specific profile is not enough here: legacy ingress and session
        locks are scoped to the unqualified base session, so another profile
        may still own task tails for that same session. This helper only
        builds the observer; it does not freeze signal admission or attach a
        production cutover path.
        """

        return LegacySessionAllProfilesTaskQuiescer(
            tuple(
                (
                    profile.profile_id,
                    profile.build_legacy_session_local_task_quiescer(),
                )
                for profile in self._unique_profiles()
            )
        )

    def build_legacy_session_local_drain_participant(
        self,
        ingress: MessageIngress,
    ) -> LegacySessionLocalDrainParticipant:
        """Build an unmounted local drain participant for a future controller.

        The returned object is intentionally not registered on ingress, timer,
        recovery, or management paths. It composes only this process's legacy
        state and cannot serve as Actor v2 activation authority.
        """

        from shinbot.agent.runtime.legacy_session_local_drain import (
            LegacySessionLocalDrainParticipant,
        )

        return LegacySessionLocalDrainParticipant(
            ingress=ingress,
            signal_admission=self._legacy_signal_admission,
            task_quiescer=self.build_legacy_base_session_local_task_quiescer(),
        )

    @property
    def actor_v2_diagnostics(self) -> ActorV2RuntimeDiagnostics | None:
        """Return an immutable Actor v2 readiness snapshot.

        Legacy ingress remains the only writer. This surface cannot expose the
        recovery scanner because scanning writes durable mailbox/case state and
        has no authorized production wake lifecycle.
        """

        assembly = self._actor_v2_assembly
        return None if assembly is None else assembly.diagnostics

    @property
    def actor_v2_handler_graph(self) -> ActorV2ProfileHandlerGraph | None:
        """Return the strict, unstarted profile handler graph for diagnostics."""

        return self._actor_v2_handler_graph

    def set_active_chat_threshold_delta(self, delta: float, *, source: str = "") -> None:
        """Apply a runtime-only base threshold delta to all Agent profiles."""

        for profile in self._unique_profiles():
            profile.set_active_chat_threshold_delta(delta, source=source)

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
        register_context_prompt_components(
            registry,
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
        sync_prompt_definition_components(registry, self.prompt_definitions)
        return registry

    def reload_prompt_files(self) -> None:
        """Reload file-backed prompt components from the configured runtime prompt root."""

        for profile in self._unique_profiles():
            profile.reload_prompt_files()

    async def handle_agent_signal(
        self,
        signal: AgentSignal,
    ) -> ActiveChatBootstrapApplyDecision | None:
        """Receive one legacy Agent signal through the local admission gate."""

        # Admission must precede the session lock so a task blocked on that
        # lock remains visible to a future local quiescence observation.
        async with self._legacy_signal_admission.admit_signal(signal.session_id):
            if signal.kind == AgentSignalKind.REVIEW_DUE:
                profile = self.agent_profile_for_bot(signal.bot_id)
                admission_reason = self.legacy_review_due_admission_reason(
                    profile.profile_id,
                    signal.session_id,
                )
                if admission_reason:
                    logger.debug(
                        format_log_event(
                            "agent.runtime.legacy_review_due_skipped",
                            signal_id=signal.signal_id,
                            session_id=signal.session_id,
                            bot_id=signal.bot_id,
                            profile_id=profile.profile_id,
                            reason=admission_reason,
                        )
                    )
                    return None
            return await self._handle_admitted_agent_signal(signal)

    async def _handle_admitted_agent_signal(
        self,
        signal: AgentSignal,
    ) -> ActiveChatBootstrapApplyDecision | None:
        """Apply one admitted signal without holding a session lock over model I/O."""

        profile: AgentRuntimeProfile | None = None
        planning_request: IdleReviewPlanningRequest | None = None
        async with self._session_signal_lock(signal.session_id):
            await self._reconcile_expired_session_pause_locked(signal.session_id)
            paused_until = self.session_pause_until(signal.session_id)
            session_paused = paused_until is not None
            should_skip = False
            skip_reason = ""
            if signal.kind == AgentSignalKind.MESSAGE:
                should_skip = session_paused
                skip_reason = "session_paused"
            elif signal.kind in {
                AgentSignalKind.REVIEW_DUE,
                AgentSignalKind.ACTIVE_CHAT_TICK,
            }:
                should_skip = self.should_pause_session(signal.session_id)
                skip_reason = "session_paused" if session_paused else "platform_unavailable"
            if should_skip:
                if signal.kind == AgentSignalKind.MESSAGE and signal.message is not None:
                    profile = self.agent_profile_for_bot(signal.bot_id)
                    profile.agent_scheduler.queue_paused_message(
                        signal,
                        pause_until=paused_until,
                    )
                logger.debug(
                    format_log_event(
                        "agent.runtime.signal_skipped",
                        kind=signal.kind.value,
                        source=signal.source.value,
                        signal_id=signal.signal_id,
                        session_id=signal.session_id,
                        bot_id=signal.bot_id,
                        reason=skip_reason,
                    )
                )
                return None
            profile = self.agent_profile_for_bot(signal.bot_id)
            logger.debug(
                format_log_event(
                    "agent.runtime.signal",
                    kind=signal.kind.value,
                    source=signal.source.value,
                    signal_id=signal.signal_id,
                    session_id=signal.session_id,
                    bot_id=signal.bot_id,
                    trace_id=str(signal.meta.get("trace_id") or ""),
                    profile_id=profile.profile_id,
                    selected_bot_id=profile.bot_id,
                    message_log_id=(
                        signal.message.message_log_id if signal.message is not None else None
                    ),
                )
            )
            prepare_idle_review_planning = getattr(
                profile.agent_scheduler,
                "prepare_idle_review_planning_request",
                None,
            )
            planning_request = (
                prepare_idle_review_planning(signal)
                if callable(prepare_idle_review_planning)
                else None
            )
            if planning_request is not None:
                if not self._register_idle_review_planning_request(
                    profile=profile,
                    request=planning_request,
                    origin=signal.kind.value,
                ):
                    return None
            else:
                decision = await profile.agent_scheduler.accept_signal(signal)
                self._log_legacy_signal_decision(
                    signal=signal,
                    profile=profile,
                    decision=decision,
                )
                if isinstance(decision, ActiveChatBootstrapApplyDecision):
                    return decision
                return None

        if profile is None or planning_request is None:
            raise RuntimeError("idle review planning request lost its runtime profile")

        decision = await self._run_idle_review_planning_request(
            profile=profile,
            request=planning_request,
            origin=signal.kind.value,
            discard_when_session_paused=(
                signal.kind == AgentSignalKind.ACTIVE_CHAT_TICK
            ),
        )
        if decision is not None:
            self._log_legacy_signal_decision(
                signal=signal,
                profile=profile,
                decision=decision,
            )
        if isinstance(decision, ActiveChatBootstrapApplyDecision):
            return decision
        return None

    async def _commit_active_chat_round_for_profile(
        self,
        profile: AgentRuntimeProfile,
        intent: active_chat_coordinator_models.ActiveChatRoundCommitIntent,
    ) -> active_chat_coordinator_models.ActiveChatRoundCommitDecision:
        """Commit one active-chat round without letting model work bypass fencing.

        The round runner has already completed its model/tool work when this
        method is called.  It may consume messages and apply a non-exiting
        interest delta while holding the session mutex.  An exit first freezes
        its exact scheduler snapshot, then delegates only planner I/O outside
        the mutex and applies the result through the existing scheduler fence.
        """

        planning_request: IdleReviewPlanningRequest | None = None
        scheduler = profile.agent_scheduler
        async with self._session_signal_lock(intent.session_id):
            current_state = scheduler.state_for(intent.session_id)
            if current_state == AgentState.ACTIVE_REPLY:
                # The legacy active-reply adapter temporarily borrows the
                # active-chat runner, but it does not install scheduler-owned
                # ActiveChatState. A successful round still owns an exact
                # message selection and must consume it before its terminal
                # ACTIVE_REPLY transition. Otherwise the next review sees the
                # already-handled mention again and can issue a second reply.
                scheduler.mark_active_chat_consumed(
                    intent.session_id,
                    list(intent.consumed_message_log_ids),
                )
                return active_chat_coordinator_models.ActiveChatRoundCommitDecision(
                    session_id=intent.session_id,
                    accepted=True,
                )
            active_chat_state = scheduler.active_chat_state_for(intent.session_id)
            if current_state != AgentState.ACTIVE_CHAT:
                return active_chat_coordinator_models.ActiveChatRoundCommitDecision(
                    session_id=intent.session_id,
                    accepted=False,
                    skipped_reason="not_active_chat",
                )
            if active_chat_state is None:
                return active_chat_coordinator_models.ActiveChatRoundCommitDecision(
                    session_id=intent.session_id,
                    accepted=False,
                    skipped_reason="missing_active_chat_state",
                )
            if active_chat_state.active_epoch != intent.active_epoch:
                return active_chat_coordinator_models.ActiveChatRoundCommitDecision(
                    session_id=intent.session_id,
                    accepted=False,
                    skipped_reason="active_epoch_mismatch",
                )

            scheduler.mark_active_chat_consumed(
                intent.session_id,
                list(intent.consumed_message_log_ids),
            )
            planning_request = scheduler.prepare_idle_review_planning_for_interest_adjustment(
                intent.session_id,
                delta=intent.interest_delta,
                force_exit=intent.force_exit,
                active_epoch=intent.active_epoch,
                reason=intent.reason,
            )
            if planning_request is None:
                decision = scheduler.adjust_active_chat_interest(
                    intent.session_id,
                    delta=intent.interest_delta,
                    force_exit=intent.force_exit,
                    active_epoch=intent.active_epoch,
                    reason=intent.reason,
                )
                return active_chat_coordinator_models.ActiveChatRoundCommitDecision(
                    session_id=intent.session_id,
                    accepted=True,
                    returned_to_idle=decision.returned_to_idle,
                    skipped_reason=decision.skipped_reason,
                )
            if not self._register_idle_review_planning_request(
                profile=profile,
                request=planning_request,
                origin="active_chat_round",
            ):
                return active_chat_coordinator_models.ActiveChatRoundCommitDecision(
                    session_id=intent.session_id,
                    accepted=True,
                    skipped_reason="idle_review_planning_coalesced",
                )

        decision = await self._run_idle_review_planning_request(
            profile=profile,
            request=planning_request,
            origin="active_chat_round",
            discard_when_session_paused=False,
        )
        return active_chat_coordinator_models.ActiveChatRoundCommitDecision(
            session_id=intent.session_id,
            accepted=True,
            returned_to_idle=bool(getattr(decision, "returned_to_idle", False)),
            skipped_reason=getattr(decision, "skipped_reason", None),
        )

    async def _commit_review_scheduler_mutation_for_profile(
        self,
        profile: AgentRuntimeProfile,
        intent: ReviewSchedulerCommitIntent,
    ) -> ReviewSchedulerCommitDecision:
        """Apply one review mutation through the runtime session boundary.

        The review coordinator may spend arbitrarily long in model stages. Its
        final unread consumption and state transition therefore revalidate the
        exact review plan while holding the session mutex, so a replacement
        workflow cannot consume or complete a newer scheduler state.
        """

        scheduler = profile.agent_scheduler
        async with self._session_signal_lock(intent.session_id):
            current_state = scheduler.state_for(intent.session_id)
            current_plan = scheduler.review_plan_for(intent.session_id)
            if current_state != AgentState.REVIEW:
                return ReviewSchedulerCommitDecision(
                    session_id=intent.session_id,
                    accepted=False,
                    completion=(
                        ReviewCompletionDecision(
                            session_id=intent.session_id,
                            state=current_state,
                            skipped_reason="not_review",
                        )
                        if intent.kind == ReviewSchedulerCommitKind.COMPLETE_REVIEW
                        else None
                    ),
                    skipped_reason="not_review",
                )
            if not review_plan_fence_matches(current_plan, intent.expected_review_plan):
                return ReviewSchedulerCommitDecision(
                    session_id=intent.session_id,
                    accepted=False,
                    completion=(
                        ReviewCompletionDecision(
                            session_id=intent.session_id,
                            state=current_state,
                            skipped_reason="review_plan_changed",
                        )
                        if intent.kind == ReviewSchedulerCommitKind.COMPLETE_REVIEW
                        else None
                    ),
                    skipped_reason="review_plan_changed",
                )

            intervals = [
                (item.start_msg_log_id, item.end_msg_log_id)
                for item in intent.consumed_ranges
                if item.session_id == intent.session_id
            ]
            if intent.kind == ReviewSchedulerCommitKind.CONSUME_RANGES:
                scheduler.consume_review_intervals(intent.session_id, intervals)
                return ReviewSchedulerCommitDecision(
                    session_id=intent.session_id,
                    accepted=True,
                    consumed_ranges=intent.consumed_ranges,
                )
            if intent.kind == ReviewSchedulerCommitKind.COMPLETE_REVIEW:
                completion = scheduler.complete_review(
                    intent.session_id,
                    enter_active_chat=intent.enter_active_chat,
                    active_chat_initial_interest=intent.active_chat_initial_interest,
                    active_chat_decay_half_life_seconds=(
                        intent.active_chat_decay_half_life_seconds
                    ),
                    next_review_plan=intent.next_review_plan,
                )
                if completion.skipped_reason is not None:
                    return ReviewSchedulerCommitDecision(
                        session_id=intent.session_id,
                        accepted=False,
                        completion=completion,
                        skipped_reason=completion.skipped_reason,
                    )
                scheduler.consume_review_intervals(intent.session_id, intervals)
                return ReviewSchedulerCommitDecision(
                    session_id=intent.session_id,
                    accepted=True,
                    completion=completion,
                    consumed_ranges=intent.consumed_ranges,
                )
            raise RuntimeError(
                f"unsupported review scheduler commit: {intent.kind!r}"
            )

    def _register_idle_review_planning_request(
        self,
        *,
        profile: AgentRuntimeProfile,
        request: IdleReviewPlanningRequest,
        origin: str,
    ) -> bool:
        """Register a frozen plan request while the caller owns its session lock."""

        existing_request = self._idle_review_planning_requests.get(request.session_id)
        if (
            existing_request is not None
            and existing_request.trigger == request.trigger
            and existing_request.expected_active_chat_state
            == request.expected_active_chat_state
            and review_plan_fence_matches(
                existing_request.expected_review_plan,
                request.expected_review_plan,
            )
        ):
            logger.debug(
                format_log_event(
                    "agent.runtime.idle_review_planning.coalesced",
                    origin=origin,
                    signal_id=request.signal_id,
                    session_id=request.session_id,
                    profile_id=profile.profile_id,
                    trigger=request.trigger.value,
                    active_epoch=request.active_epoch,
                    active_request_signal_id=existing_request.signal_id,
                )
            )
            return False
        if existing_request is not None:
            logger.debug(
                format_log_event(
                    "agent.runtime.idle_review_planning.superseded",
                    origin=origin,
                    signal_id=request.signal_id,
                    session_id=request.session_id,
                    profile_id=profile.profile_id,
                    trigger=request.trigger.value,
                    active_epoch=request.active_epoch,
                    superseded_signal_id=existing_request.signal_id,
                )
            )
        self._idle_review_planning_requests[request.session_id] = request
        logger.debug(
            format_log_event(
                "agent.runtime.idle_review_planning.deferred",
                origin=origin,
                signal_id=request.signal_id,
                session_id=request.session_id,
                profile_id=profile.profile_id,
                trigger=request.trigger.value,
                active_epoch=request.active_epoch,
            )
        )
        return True

    async def _run_idle_review_planning_request(
        self,
        *,
        profile: AgentRuntimeProfile,
        request: IdleReviewPlanningRequest,
        origin: str,
        discard_when_session_paused: bool,
    ) -> Any | None:
        """Run frozen planner I/O outside the lock and fence its terminal apply."""

        try:
            next_review_plan = await profile.plan_idle_review_after_active_chat(request)
            async with self._session_signal_lock(request.session_id):
                current_request = self._idle_review_planning_requests.get(request.session_id)
                if current_request != request:
                    self._record_idle_review_planning_application(
                        profile=profile,
                        request=request,
                        model_plan=next_review_plan,
                        decision=None,
                        outcome="discarded",
                        reason="superseded_before_apply",
                    )
                    logger.debug(
                        format_log_event(
                            "agent.runtime.idle_review_planning.discarded",
                            origin=origin,
                            signal_id=request.signal_id,
                            session_id=request.session_id,
                            profile_id=profile.profile_id,
                            reason="superseded_before_apply",
                        )
                    )
                    return None
                if (
                    discard_when_session_paused
                    and self.should_pause_session(request.session_id)
                ):
                    self._record_idle_review_planning_application(
                        profile=profile,
                        request=request,
                        model_plan=next_review_plan,
                        decision=None,
                        outcome="discarded",
                        reason="session_paused_before_apply",
                    )
                    logger.debug(
                        format_log_event(
                            "agent.runtime.idle_review_planning.discarded",
                            origin=origin,
                            signal_id=request.signal_id,
                            session_id=request.session_id,
                            profile_id=profile.profile_id,
                            reason="session_paused_before_apply",
                        )
                    )
                    return None
                return profile.agent_scheduler.apply_idle_review_planning_request(
                    request,
                    next_review_plan=next_review_plan,
                )
        finally:
            async with self._session_signal_lock(request.session_id):
                if self._idle_review_planning_requests.get(request.session_id) == request:
                    self._idle_review_planning_requests.pop(request.session_id, None)

    async def _complete_active_reply_for_profile(
        self,
        profile: AgentRuntimeProfile,
        session_id: str,
    ) -> None:
        """Serialize active-reply completion after its model task settles.

        The active-reply task itself is deliberately allowed to await model I/O
        without this mutex. Only its terminal scheduler transition comes back
        through the same per-session critical section as ingress signals.
        """

        async with self._session_signal_lock(session_id):
            decision = await profile.agent_scheduler.complete_active_reply(session_id)
            logger.debug(
                format_log_event(
                    "agent.runtime.active_reply.completed",
                    session_id=session_id,
                    profile_id=profile.profile_id,
                    state=getattr(decision.state, "value", str(decision.state)),
                    remaining_unread_count=decision.remaining_unread_count,
                    skipped_reason=decision.skipped_reason or "",
                )
            )

    def _log_legacy_signal_decision(
        self,
        *,
        signal: AgentSignal,
        profile: AgentRuntimeProfile,
        decision: Any,
    ) -> None:
        """Write the common post-decision runtime event without payload content."""

        logger.debug(
            format_log_event(
                "agent.runtime.decision",
                kind=signal.kind.value,
                signal_id=signal.signal_id,
                session_id=signal.session_id,
                trace_id=str(signal.meta.get("trace_id") or ""),
                profile_id=profile.profile_id,
                decision_type=type(decision).__name__ if decision is not None else "",
                skipped_reason=getattr(decision, "skipped_reason", ""),
                state=(
                    getattr(getattr(decision, "state", None), "value", "")
                    if decision is not None
                    else ""
                ),
            )
        )

    def _record_idle_review_planning_model_result(
        self,
        *,
        profile_id: str,
        request: IdleReviewPlanningRequest,
        result: Any,
    ) -> None:
        """Persist one sanitized model-stage outcome for a fenced plan request."""

        plan = getattr(result, "plan", None)
        self._write_idle_review_planning_audit(
            session_id=request.session_id,
            profile_id=profile_id,
            event_type="agent.idle_review_planning.model_result",
            metadata={
                "signal_id": request.signal_id,
                "trigger": request.trigger.value,
                "active_epoch": request.active_epoch,
                "checked_at": request.checked_at,
                "outcome": _bounded_audit_text(getattr(result, "outcome", "")),
                "reason": _bounded_audit_text(getattr(result, "reason", "")),
                "failure_code": _bounded_audit_text(
                    getattr(result, "failure_code", "")
                ),
                "model_execution_id": _bounded_audit_text(
                    getattr(result, "model_execution_id", "")
                ),
                "prompt_signature": _bounded_audit_text(
                    getattr(result, "prompt_signature", "")
                ),
                "requested_next_review_after_seconds": getattr(
                    result,
                    "requested_next_review_after_seconds",
                    None,
                ),
                "applied_next_review_after_seconds": getattr(
                    result,
                    "applied_next_review_after_seconds",
                    None,
                ),
                "proposed_next_review_at": (
                    getattr(plan, "next_review_at", None) if plan is not None else None
                ),
                "proposed_plan_reason": _bounded_audit_text(
                    getattr(plan, "reason", "") if plan is not None else ""
                ),
            },
        )

    def _record_idle_review_planning_application(
        self,
        *,
        profile: AgentRuntimeProfile,
        request: IdleReviewPlanningRequest,
        model_plan: ReviewPlan | None,
        decision: Any | None,
        outcome: str,
        reason: str,
    ) -> None:
        """Persist whether a previously recorded planner result won its state race."""

        applied_plan = getattr(decision, "next_review_plan", None)
        self._write_idle_review_planning_audit(
            session_id=request.session_id,
            profile_id=profile.profile_id,
            event_type="agent.idle_review_planning.application",
            metadata={
                "signal_id": request.signal_id,
                "trigger": request.trigger.value,
                "active_epoch": request.active_epoch,
                "checked_at": request.checked_at,
                "outcome": outcome,
                "reason": _bounded_audit_text(reason),
                "model_plan_supplied": model_plan is not None,
                "model_plan_reason": _bounded_audit_text(
                    model_plan.reason if model_plan is not None else ""
                ),
                "model_plan_next_review_at": (
                    model_plan.next_review_at if model_plan is not None else None
                ),
                "decision_skipped_reason": _bounded_audit_text(
                    getattr(decision, "skipped_reason", "") if decision is not None else ""
                ),
                "applied_plan_reason": _bounded_audit_text(
                    getattr(applied_plan, "reason", "") if applied_plan is not None else ""
                ),
                "applied_next_review_at": (
                    getattr(applied_plan, "next_review_at", None)
                    if applied_plan is not None
                    else None
                ),
                "scheduler_state": _bounded_audit_text(
                    getattr(getattr(decision, "state", None), "value", "")
                    if decision is not None
                    else ""
                ),
            },
        )

    def _write_idle_review_planning_audit(
        self,
        *,
        session_id: str,
        profile_id: str,
        event_type: str,
        metadata: dict[str, Any],
    ) -> None:
        """Write bounded internal scheduling evidence without changing execution flow."""

        try:
            self._audit_logger.log_message(
                event_type=event_type,
                plugin_id="agent_runtime",
                user_id="",
                session_id=session_id,
                instance_id=_instance_id_from_session_id(session_id),
                metadata={"profile_id": profile_id, **metadata},
            )
        except Exception as exc:
            logger.warning(
                format_log_event(
                    "agent.idle_review_planning.audit_failed",
                    session_id=session_id,
                    profile_id=profile_id,
                    event_type=event_type,
                    error_code=type(exc).__name__,
                ),
                exc_info=True,
            )

    def is_session_platform_connected(self, session_id: str) -> bool:
        """Return whether the session's adapter is explicitly connected now.

        Args:
            session_id: Bot-scoped session identifier whose adapter should be
                checked.
        """
        instance_id = _instance_id_from_session_id(session_id)
        if not instance_id:
            return False
        if self.adapter_manager.get_instance(instance_id) is None:
            return False
        return self.adapter_manager.is_connected(instance_id)

    def is_session_platform_available(self, session_id: str) -> bool:
        """Return whether the session's adapter should be treated as available.

        Args:
            session_id: Bot-scoped session identifier whose adapter should be
                checked.
        """
        instance_id = _instance_id_from_session_id(session_id)
        if not instance_id:
            return True
        if self.adapter_manager.get_instance(instance_id) is None:
            return True
        return self.adapter_manager.is_available(instance_id)

    def should_pause_session(self, session_id: str) -> bool:
        """Return whether Agent background work should pause for a session.

        Args:
            session_id: Bot-scoped session identifier whose adapter should be
                checked.

        This predicate is intentionally read-only because timer pollers call it
        before they enter the runtime's session mutation boundary.
        """
        return not self.is_session_platform_available(session_id) or self.is_session_paused(
            session_id
        )

    def session_pause_until(self, session_id: str) -> float | None:
        """Return the active pause deadline without mutating runtime state."""

        pause_until = self._stored_session_pause_until(session_id)
        return pause_until if pause_until is not None and pause_until > time.time() else None

    def is_session_paused(self, session_id: str) -> bool:
        """Return whether a session has an explicit Agent pause in effect."""

        return self.session_pause_until(session_id) is not None

    def _stored_session_pause_until(self, session_id: str) -> float | None:
        """Read a pause deadline without applying an expiry transition."""

        session = (
            self.session_manager.get(session_id)
            if self.session_manager is not None
            else None
        )
        pause_until = get_agent_pause_until(session) if session is not None else None
        if pause_until is None and self.database is not None:
            persisted = self.database.sessions.get(session_id)
            if persisted is not None:
                pause_until = get_agent_pause_until(persisted)
        return pause_until

    async def _reconcile_expired_session_pause_locked(self, session_id: str) -> None:
        """Clear an expired pause while the caller owns the session mutex."""

        pause_until = self._stored_session_pause_until(session_id)
        checked_at = time.time()
        if pause_until is None or pause_until > checked_at:
            return
        self._clear_session_pause_locked(session_id, checked_at=checked_at)

    async def pause_session_until(self, session_id: str, *, pause_until: float) -> None:
        """Immediately pause Agent activity through the session mutation gate."""

        checked_at = time.time()
        async with self._session_signal_lock(session_id):
            if self.session_manager is not None:
                session = self.session_manager.get(session_id)
                if session is not None:
                    set_agent_pause_until(session, pause_until)
                    self.session_manager.update(session)
            if self.database is not None:
                persisted = self.database.sessions.get(session_id)
                if persisted is not None:
                    state = dict(persisted.get("state") or {})
                    state[SESSION_STATE_AGENT_PAUSE_UNTIL_KEY] = float(pause_until)
                    persisted["state"] = state
                    self.database.sessions.upsert(persisted)

            for profile in self._unique_profiles():
                scheduler = profile.agent_scheduler
                if session_id not in set(scheduler.list_session_ids()):
                    continue
                scheduler.pause_session_until(
                    session_id,
                    pause_until=pause_until,
                    now=checked_at,
                )

    async def clear_session_pause(self, session_id: str) -> None:
        """Clear an explicit Agent pause through the session mutation gate."""

        async with self._session_signal_lock(session_id):
            self._clear_session_pause_locked(session_id, checked_at=time.time())

    def _clear_session_pause_locked(
        self,
        session_id: str,
        *,
        checked_at: float,
    ) -> None:
        """Clear pause persistence and reschedule while the session mutex is held."""

        if self.session_manager is not None:
            session = self.session_manager.get(session_id)
            if session is not None and get_agent_pause_until(session) is not None:
                set_agent_pause_until(session, None)
                self.session_manager.update(session)
        if self.database is not None:
            persisted = self.database.sessions.get(session_id)
            if persisted is not None and get_agent_pause_until(persisted) is not None:
                state = dict(persisted.get("state") or {})
                state.pop(SESSION_STATE_AGENT_PAUSE_UNTIL_KEY, None)
                persisted["state"] = state
                self.database.sessions.upsert(persisted)

        for profile in self._unique_profiles():
            scheduler = profile.agent_scheduler
            if session_id not in set(scheduler.list_session_ids()):
                continue
            scheduler.bring_review_plan_forward(
                session_id,
                next_review_at=checked_at,
                now=checked_at,
                reason="session_unmuted",
            )

    async def trigger_review(self, session_id: str) -> bool:
        """Trigger one unambiguous legacy scheduler session.

        This compatibility method intentionally does not infer an Actor v2
        profile from a bare session id. New management callers must use
        :meth:`request_review_for_profile` so their request retains the full
        durable ``SessionKey``.

        Args:
            session_id: The session to trigger a review for.

        Returns:
            ``True`` if exactly one legacy scheduler accepted the request;
            otherwise ``False`` without selecting among multiple profiles.
        """
        normalized_session_id = str(session_id or "").strip()
        if not normalized_session_id:
            return False
        profiles = [
            profile
            for profile in self._unique_profiles()
            if normalized_session_id in set(profile.agent_scheduler.list_session_ids())
        ]
        if len(profiles) != 1:
            return False
        database = self.database
        if database is None:
            return False
        key = SessionKey(profiles[0].profile_id, normalized_session_id)
        ownership = database.agent_runtime_ownership.get(key)
        if (
            ownership is None
            or ownership.status is not AgentRuntimeOwnershipStatus.ACTIVE
            or ownership.mode is not AgentRuntimeOwnershipMode.LEGACY
        ):
            return False
        return await self._trigger_legacy_review_for_profile(
            profiles[0],
            normalized_session_id,
        )

    async def _trigger_legacy_review_for_profile(
        self,
        profile: AgentRuntimeProfile,
        session_id: str,
    ) -> bool:
        """Admit one legacy management request to the local drain boundary."""

        try:
            async with self._legacy_signal_admission.admit_signal(session_id):
                return await self._trigger_admitted_legacy_review_for_profile(
                    profile,
                    session_id,
                )
        except LegacyAgentSignalFrozen:
            return False

    async def _trigger_admitted_legacy_review_for_profile(
        self,
        profile: AgentRuntimeProfile,
        session_id: str,
    ) -> bool:
        """Run the legacy review after its local admission is tracked."""

        checked_at = time.time()
        async with self._session_signal_lock(session_id):
            await self._reconcile_expired_session_pause_locked(session_id)
            scheduler = profile.agent_scheduler
            if session_id not in set(scheduler.list_session_ids()):
                return False
            current_state = scheduler.state_for(session_id)
            if current_state not in {AgentState.IDLE, AgentState.REVIEW}:
                logger.info(
                    format_log_event(
                        "agent.runtime.manual_review_skipped",
                        session_id=session_id,
                        state=current_state.value,
                        profile_id=profile.profile_id,
                    )
                )
                return False
            scheduler.bring_review_plan_forward(
                session_id,
                next_review_at=checked_at,
                now=checked_at,
                reason="manual_trigger",
            )
            decision = await scheduler.run_due_review(session_id, now=checked_at)
            started = bool(getattr(decision, "review_workflow_started", False))
            logger.info(
                format_log_event(
                    "agent.runtime.manual_review_triggered",
                    session_id=session_id,
                    review_workflow_started=started,
                    state=getattr(decision.state, "value", str(decision.state)),
                    profile_id=profile.profile_id,
                )
            )
            return started

    async def force_idle(self, session_id: str) -> bool:
        """Force a session back to IDLE from any active state.

        Delegates to the appropriate scheduler completion method depending
        on the current state, so transitions follow normal signal-flow
        rules rather than bypassing the state machine.

        Args:
            session_id: The session to return to idle.

        Returns:
            ``True`` if the state was changed or already idle, ``False``
            if the session was not found or the current state is unhandled.
        """
        async with self._session_signal_lock(session_id):
            await self._reconcile_expired_session_pause_locked(session_id)
            for profile in self._unique_profiles():
                scheduler = profile.agent_scheduler
                if session_id not in set(scheduler.list_session_ids()):
                    continue
                current_state = scheduler.state_for(session_id)
                if current_state == AgentState.IDLE:
                    return True
                if current_state == AgentState.REVIEW:
                    scheduler.complete_review(session_id, enter_active_chat=False)
                elif current_state == AgentState.ACTIVE_REPLY:
                    await scheduler.complete_active_reply(session_id, review_after=False)
                elif current_state == AgentState.ACTIVE_CHAT:
                    scheduler.adjust_active_chat_interest(
                        session_id,
                        force_exit=True,
                        reason="manual_force_idle",
                    )
                else:
                    logger.warning(
                        format_log_event(
                            "agent.runtime.force_idle_unhandled_state",
                            session_id=session_id,
                            state=current_state.value,
                            profile_id=profile.profile_id,
                        )
                    )
                    return False
                new_state = scheduler.state_for(session_id)
                logger.info(
                    format_log_event(
                        "agent.runtime.force_idle",
                        session_id=session_id,
                        previous_state=current_state.value,
                        new_state=new_state.value,
                        profile_id=profile.profile_id,
                    )
                )
                return True
        return False

    async def start_background_tasks(self) -> None:
        """Recover Agent state and start background services in lifecycle order."""

        start_default_timers = not self._profiles_by_bot_id
        for profile in self._unique_profiles():
            await profile.start_background_tasks(
                start_timers=start_default_timers or profile.bot_id != ""
            )

    async def _reconcile_profile_startup_sessions(
        self,
        profile: AgentRuntimeProfile,
    ) -> None:
        """Recover one profile's persisted sessions through runtime mutexes.

        Session ids are only a discovery snapshot. Each state decision is made
        after acquiring its own lock, so adapter ingress or an administrative
        operation that raced with startup always observes a single serialized
        scheduler transition.
        """

        scheduler = profile.agent_scheduler
        session_ids = scheduler.list_session_ids(prefix=profile._session_id_prefix())
        for session_id in session_ids:
            async with self._session_signal_lock(session_id):
                await self._reconcile_expired_session_pause_locked(session_id)
                scheduler.reconcile_transient_session(session_id)
                if session_id in set(profile.active_chat_workflow.active_session_ids()):
                    continue
                decision = scheduler.reconcile_active_chat_session(session_id)
                if decision is None or decision.state != AgentState.ACTIVE_CHAT:
                    continue
                active_chat_state = scheduler.active_chat_state_for(session_id)
                if active_chat_state is None:
                    continue
                try:
                    await profile.restore_active_chat_session(
                        session_id=session_id,
                        active_chat_state=active_chat_state,
                        initial_unread_messages=scheduler.unread_messages(session_id),
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.exception(
                        format_log_event(
                            "agent.active_chat.recovery.failed",
                            session_id=session_id,
                            profile_id=profile.profile_id,
                            error_code=type(exc).__name__,
                        )
                    )
                    scheduler.adjust_active_chat_interest(
                        session_id,
                        force_exit=True,
                        active_epoch=active_chat_state.active_epoch,
                        reason="startup_runtime_restore_failed",
                    )

    def _session_signal_lock(self, session_id: str) -> asyncio.Lock:
        lock = self._session_signal_locks.get(session_id)
        if lock is None:
            lock = asyncio.Lock()
            self._session_signal_locks[session_id] = lock
        return lock

    def _resolve_response_profile(self, signal: AgentSignal) -> str:
        message = signal.message
        if message is None:
            return ""
        instance_config = self._instance_config_payload(message.instance_id)
        return select_response_profile(
            instance_config,
            is_private=message.is_private,
            is_mentioned=message.is_mentioned,
            is_reply_to_bot=message.is_reply_to_bot,
        )

    def _resolve_instance_runtime_config(self, instance_id: str) -> Any:
        return resolve_instance_runtime_config(self._instance_config_payload(instance_id))

    def _compose_actor_v2_diagnostics(
        self,
        database: DatabaseManager,
    ) -> ActorV2RuntimeAssembly:
        """Compose profile-aware Actor v2 handlers without activating ownership.

        The graph is intentionally built only after every profile and chat
        action tool exists.  Construction registers durable handler metadata
        but does not scan, wake, recover, claim, or execute any actor work.
        """

        message_store = DatabaseReviewMessageStore(database)
        graph: ActorV2ProfileHandlerGraph | None = None

        def configure_profile_handlers(ports: ActorV2RuntimeCompositionPorts) -> None:
            """Build the profile graph while private assembly ports are in scope."""

            nonlocal graph
            graph = ActorV2ProfileHandlerGraph.compose(
                effect_contract_authority=ports.handler_registry.effect_contract_authority,
                ledger=ports.workflow_ledger,
                profiles=(
                    profile.create_actor_v2_workflow_ports(
                        ledger=ports.workflow_ledger,
                        message_store=message_store,
                    )
                    for profile in self._unique_profiles()
                ),
                external_action_receipts=SQLiteExternalActionReceiptStore(database),
                external_action_dispatcher=AdapterExternalActionDispatcher(
                    adapters=self.adapter_manager,
                    database=database,
                ),
                review_cancellation_control=ports.review_execution_gate_store,
                model_execution_cancellation_control=(
                    ports.model_execution_cancellation_gate_store
                ),
            )
            graph.register(ports.handler_registry)

        assembly = ActorV2RuntimeAssembly.compose_inactive(
            database,
            configure_profile_handlers=configure_profile_handlers,
        )
        assert graph is not None
        self._actor_v2_handler_graph = graph
        return assembly

    def _instance_config_payload(self, instance_id: str) -> dict[str, Any] | None:
        if self.database is None:
            return None
        normalized = str(instance_id or "").strip()
        if not normalized:
            return None
        return self.database.instance_configs.get_by_instance_id(normalized)

    def _resolve_model_target(self, target: str) -> RuntimeModelTarget | None:
        normalized = str(target or "").strip()
        if not normalized:
            return None
        tagged = parse_tagged_llm_ref(normalized)
        if tagged is not None:
            return tagged
        if self.database is not None:
            registry = self.database.model_registry
            if registry.get_route(normalized) is not None:
                return RuntimeModelTarget(route_id=normalized)
            if registry.get_model(normalized) is not None:
                return RuntimeModelTarget(model_id=normalized)
        return RuntimeModelTarget(route_id=normalized)

    def _record_review_workflow_run(
        self,
        session_id: str,
        result: Any,
        unread_messages: list[Any],
    ) -> None:
        if self.database is None:
            return
        message_ids = sorted(
            int(message.message_log_id)
            for message in unread_messages
            if getattr(message, "message_log_id", None) is not None
        )
        started_at = float(getattr(result, "review_started_at", 0.0) or time.time())
        finished_at = max(time.time(), started_at)
        with self.database.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO workflow_runs (
                    id, session_id, instance_id, response_profile,
                    batch_start_msg_id, batch_end_msg_id, batch_size,
                    trigger_attention, effective_threshold, tool_calls_json,
                    replied, response_summary, finish_reason, started_at, finished_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(getattr(result, "review_run_id", "")),
                    session_id,
                    _instance_id_from_session_id(session_id),
                    _response_profile_from_unread(unread_messages),
                    message_ids[0] if message_ids else None,
                    message_ids[-1] if message_ids else None,
                    len(message_ids),
                    0.0,
                    0.0,
                    json.dumps([], ensure_ascii=False),
                    1 if getattr(getattr(result, "reply", None), "replied", False) else 0,
                    _review_response_summary(result),
                    _review_finish_reason(result),
                    started_at,
                    finished_at,
                ),
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
        if self._actor_v2_assembly is not None:
            await self._actor_v2_assembly.shutdown()
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

    def _profile_for_id(self, profile_id: str) -> AgentRuntimeProfile | None:
        """Return an exact durable profile without default-profile fallback."""

        normalized_profile_id = str(profile_id or "").strip()
        if not normalized_profile_id:
            return None
        return next(
            (
                profile
                for profile in self._unique_profiles()
                if profile.profile_id == normalized_profile_id
            ),
            None,
        )


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
        session_manager=bot.session_manager,
        model_runtime=model_runtime,
        tool_registry=bot.tool_registry,
        review_runtime_config=review_runtime_config,
        prompt_file_config=prompt_file_config,
        agent_config=agent_config,
        agent_configs_by_bot_id=agent_configs_by_bot_id,
    )
    bot.mount_agent_runtime(runtime)
    return runtime


def _instance_id_from_session_id(session_id: str) -> str:
    return str(session_id or "").split(":", 1)[0]


def _bounded_audit_text(value: object, *, limit: int = 256) -> str:
    """Normalize non-sensitive decision metadata to a bounded audit field."""

    return str(value or "").strip()[:limit]


def _response_profile_from_unread(unread_messages: list[Any]) -> str:
    for message in unread_messages:
        value = str(getattr(message, "response_profile", "") or "").strip()
        if value:
            return value
    return ""


def _review_response_summary(result: Any) -> str:
    scan = getattr(result, "scan", None)
    reply = getattr(result, "reply", None)
    bootstrap = getattr(result, "bootstrap", None)
    parts = [
        f"scan={getattr(scan, 'scan_reason', '') or 'unknown'}",
        f"reply={getattr(reply, 'reply_reason', '') or 'unknown'}",
        f"active_chat={getattr(bootstrap, 'reason', '') or 'unknown'}",
    ]
    return "; ".join(parts)


def _review_finish_reason(result: Any) -> str:
    if getattr(result, "failed", False):
        return f"failed:{getattr(result, 'failure_reason', '') or 'unknown'}"
    completion = getattr(result, "completion", None)
    if completion is None:
        return "completed_without_scheduler_decision"
    skipped_reason = getattr(completion, "skipped_reason", None)
    if skipped_reason:
        return f"skipped:{skipped_reason}"
    if getattr(completion, "active_chat_started", False):
        return "active_chat_started"
    if getattr(completion, "returned_to_idle", False):
        return "returned_to_idle"
    state = getattr(completion, "state", None)
    return f"completed:{getattr(state, 'value', state) or 'unknown'}"


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


def _agent_config_with_persona_component(
    config: AgentRuntimeConfig,
    component_id: str,
) -> AgentRuntimeConfig:
    return replace(
        config,
        review_runtime_config=_review_runtime_config_with_persona_component(
            config.review_runtime_config,
            component_id,
        ),
        active_chat_fast_runner_config=_stage_config_with_persona_component(
            config.active_chat_fast_runner_config,
            component_id,
        ),
    )


def _review_runtime_config_with_persona_component(
    config: ReviewRuntimeConfig,
    component_id: str,
) -> ReviewRuntimeConfig:
    return replace(
        config,
        overflow_compression=_stage_config_with_persona_component(
            config.overflow_compression,
            component_id,
        ),
        review_scan=_stage_config_with_persona_component(
            config.review_scan,
            component_id,
        ),
        review_block_digest=_stage_config_with_persona_component(
            config.review_block_digest,
            component_id,
        ),
        reply_decision=_stage_config_with_persona_component(
            config.reply_decision,
            component_id,
        ),
        active_chat_bootstrap=_stage_config_with_persona_component(
            config.active_chat_bootstrap,
            component_id,
        ),
        idle_review_planning=_stage_config_with_persona_component(
            config.idle_review_planning,
            component_id,
        ),
    )


def _stage_config_with_persona_component(config: Any, component_id: str) -> Any:
    component_ids_by_stage = {
        stage: list(component_ids) for stage, component_ids in config.component_ids_by_stage.items()
    }
    identity_ids = component_ids_by_stage.setdefault(PromptStage.IDENTITY, [])
    if component_id not in identity_ids:
        identity_ids.append(component_id)
    return replace(config, component_ids_by_stage=component_ids_by_stage)


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


def _summary_markdown_store(
    config: SummaryMarkdownConfig,
) -> MarkdownSummaryStore | None:
    if not config.enabled:
        return None
    return MarkdownSummaryStore(config.directory)


def _format_agent_config_reference_issues(
    path: Path,
    issues: list[Any],
) -> str:
    lines = [f"Agent config {path} has invalid references:"]
    lines.extend(f"- {issue.path}: {issue.message} ({issue.code})" for issue in issues)
    return "\n".join(lines)


def _workflow_active_chat_batch_from_coordinator(
    batch: active_chat_coordinator_models.ActiveChatBatch,
) -> active_chat_workflow_models.ActiveChatBatch:
    return active_chat_workflow_models.ActiveChatBatch(
        session_id=batch.session_id,
        messages=[
            _workflow_active_chat_message_from_coordinator(message) for message in batch.messages
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
            _coordinator_active_chat_message_from_workflow(message) for message in batch.messages
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
        trace_id=message.trace_id,
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
        trace_id=message.trace_id,
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
