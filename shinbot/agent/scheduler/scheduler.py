"""Agent-internal scheduler entrypoint."""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any

from shinbot.agent.scheduler.active_chat_policy import (
    ActiveChatPolicy,
    DefaultActiveChatPolicy,
)
from shinbot.agent.scheduler.active_chat_timer import ActiveChatTimer
from shinbot.agent.scheduler.inbox import AgentInbox, InMemoryAgentInbox
from shinbot.agent.scheduler.models import (
    ActiveChatBootstrapApplyDecision,
    ActiveChatDisposition,
    ActiveChatInterestAdjustDecision,
    ActiveChatInterestAdjustmentPreview,
    ActiveChatState,
    ActiveChatTickDecision,
    ActiveChatTickPreview,
    ActiveReplyCompletionDecision,
    ActiveReplyResume,
    ActiveReplyResumeKind,
    AgentScheduleDecision,
    AgentState,
    HighPriorityEvent,
    HighPriorityEventKind,
    ReviewCompletionDecision,
    ReviewDueDecision,
    ReviewPlan,
    SchedulerEvent,
    SchedulerEventKind,
    SchedulerTransitionTrigger,
    UnreadMessage,
    UnreadRange,
)
from shinbot.agent.scheduler.priority_policy import (
    DefaultPriorityPolicy,
    PriorityPolicy,
    PriorityPolicyConfig,
)
from shinbot.agent.scheduler.review_policy import DefaultReviewPolicy, ReviewPolicy
from shinbot.agent.scheduler.state_store import AgentStateStore, InMemoryAgentStateStore
from shinbot.agent.scheduler.workflow_dispatcher import AgentWorkflowDispatcher
from shinbot.agent.signals import AgentMessageSignal, AgentSignal, AgentSignalKind
from shinbot.utils.logger import format_log_event, get_logger

logger = get_logger(__name__, source="agent:scheduler", color="magenta")

ResponseProfileResolver = Callable[[AgentSignal], str]
AgentSignalDecision = (
    AgentScheduleDecision
    | ReviewDueDecision
    | ActiveChatTickDecision
    | ActiveChatBootstrapApplyDecision
)
SchedulerEventHandler = Callable[[SchedulerEvent], Awaitable[AgentSignalDecision | None]]
PreparedMessageStateHandler = Callable[
    ["AgentScheduler", "PreparedMessageEvent"], Awaitable[AgentScheduleDecision]
]
PreparedReviewStateHandler = Callable[
    ["AgentScheduler", str, ReviewPlan, float], ReviewDueDecision
]
ActiveReplyResumeStateHandler = Callable[
    ["AgentScheduler", str, ActiveReplyResume, list[HighPriorityEvent], float],
    ActiveReplyCompletionDecision,
]


@dataclass(slots=True)
class AgentSchedulerConfig:
    """Minimal scheduler thresholds for the first Agent scheduling pass."""

    mention_wake_count: int = 1
    mention_wake_window_seconds: float = 60.0

    def to_priority_policy_config(self) -> PriorityPolicyConfig:
        """Build the default priority policy config from scheduler config."""
        return PriorityPolicyConfig(
            mention_wake_count=self.mention_wake_count,
            mention_wake_window_seconds=self.mention_wake_window_seconds,
        )


@dataclass(slots=True, frozen=True)
class TransitionEffects:
    """Side effects applied alongside one scheduler state transition."""

    cancel_active_chat_timer: bool = False
    stop_active_chat_runtime: bool = False
    cancel_review_runtime: bool = False
    clear_active_reply_resume: bool = False
    clear_active_chat_state: bool = False


@dataclass(slots=True, frozen=True)
class StateTransitionRule:
    """Declarative transition rule used by the scheduler state machine."""

    target_state: AgentState
    effects: TransitionEffects = field(default_factory=TransitionEffects)


@dataclass(slots=True, frozen=True)
class PreparedMessageEvent:
    """Normalized scheduler message event after validation and inbox updates."""

    signal: AgentSignal
    message: AgentMessageSignal
    initial_state: AgentState
    checked_at: float
    response_profile: str
    unread: UnreadMessage
    high_priority_events: list[HighPriorityEvent] = field(default_factory=list)
    should_start_active_reply: bool = False


@dataclass(slots=True, frozen=True)
class ActiveReplyWorkflowRequest:
    """Explicit workflow dispatch request for one active-reply run."""

    session_id: str
    message_log_id: int
    sender_id: str
    response_profile: str
    is_mentioned: bool
    is_reply_to_bot: bool
    is_mention_to_other: bool
    is_poke_to_bot: bool
    is_poke_to_other: bool
    self_platform_id: str
    events: list[HighPriorityEvent] = field(default_factory=list)
    trace_id: str = ""


@dataclass(slots=True, frozen=True)
class ReviewWorkflowRequest:
    """Explicit workflow dispatch request for one review run."""

    session_id: str
    review_plan: ReviewPlan
    unread_messages: list[UnreadMessage] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class ActiveChatExitPlan:
    """Normalized active-chat exit plan for transitions back to IDLE."""

    session_id: str
    active_chat_state: ActiveChatState
    review_plan: ReviewPlan
    trigger: SchedulerTransitionTrigger
    checked_at: float


def _handle_message_idle_or_review(
    scheduler: AgentScheduler,
    prepared: PreparedMessageEvent,
) -> Awaitable[AgentScheduleDecision]:
    return scheduler._handle_message_idle_or_review_state(prepared)


def _handle_message_active_reply(
    scheduler: AgentScheduler,
    prepared: PreparedMessageEvent,
) -> Awaitable[AgentScheduleDecision]:
    return scheduler._handle_message_active_reply_state(prepared)


def _handle_message_active_chat(
    scheduler: AgentScheduler,
    prepared: PreparedMessageEvent,
) -> Awaitable[AgentScheduleDecision]:
    return scheduler._handle_message_active_chat_state(prepared)


def _prepare_due_review_from_idle(
    scheduler: AgentScheduler,
    session_id: str,
    plan: ReviewPlan,
    checked_at: float,
) -> ReviewDueDecision:
    return scheduler._prepare_due_review_from_idle_state(session_id, plan, checked_at)


def _prepare_due_review_while_review(
    scheduler: AgentScheduler,
    session_id: str,
    plan: ReviewPlan,
    checked_at: float,
) -> ReviewDueDecision:
    return scheduler._skip_due_review_for_state(
        session_id,
        AgentState.REVIEW,
        plan,
        "review_already_running",
    )


def _prepare_due_review_while_active_reply(
    scheduler: AgentScheduler,
    session_id: str,
    plan: ReviewPlan,
    checked_at: float,
) -> ReviewDueDecision:
    return scheduler._skip_due_review_for_state(
        session_id,
        AgentState.ACTIVE_REPLY,
        plan,
        "active_reply_running",
    )


def _prepare_due_review_while_active_chat(
    scheduler: AgentScheduler,
    session_id: str,
    plan: ReviewPlan,
    checked_at: float,
) -> ReviewDueDecision:
    return scheduler._skip_due_review_for_state(
        session_id,
        AgentState.ACTIVE_CHAT,
        plan,
        "active_chat_running",
    )


def _resume_active_reply_to_review(
    scheduler: AgentScheduler,
    session_id: str,
    resume: ActiveReplyResume,
    handled_events: list[HighPriorityEvent],
    checked_at: float,
) -> ActiveReplyCompletionDecision:
    return scheduler._prepare_resumed_review_after_active_reply(
        session_id,
        resume=resume,
        handled_events=handled_events,
        checked_at=checked_at,
    )


class AgentScheduler:
    """Accepts unified Agent signals and decides which Agent workflow should run."""

    _SIGNAL_EVENT_KIND_MAP: dict[AgentSignalKind, SchedulerEventKind] = {
        AgentSignalKind.MESSAGE: SchedulerEventKind.MESSAGE,
        AgentSignalKind.REVIEW_DUE: SchedulerEventKind.REVIEW_DUE,
        AgentSignalKind.ACTIVE_CHAT_TICK: SchedulerEventKind.ACTIVE_CHAT_TICK,
        AgentSignalKind.ACTIVE_CHAT_BOOTSTRAP: SchedulerEventKind.ACTIVE_CHAT_BOOTSTRAP,
    }

    _STATE_TRANSITION_RULES: dict[AgentState, dict[AgentState, StateTransitionRule]] = {
        AgentState.IDLE: {
            AgentState.REVIEW: StateTransitionRule(target_state=AgentState.REVIEW),
            AgentState.ACTIVE_REPLY: StateTransitionRule(
                target_state=AgentState.ACTIVE_REPLY,
                effects=TransitionEffects(cancel_active_chat_timer=True),
            ),
        },
        AgentState.REVIEW: {
            AgentState.IDLE: StateTransitionRule(target_state=AgentState.IDLE),
            AgentState.ACTIVE_REPLY: StateTransitionRule(
                target_state=AgentState.ACTIVE_REPLY,
                effects=TransitionEffects(
                    cancel_active_chat_timer=True,
                    cancel_review_runtime=True,
                ),
            ),
            AgentState.ACTIVE_CHAT: StateTransitionRule(
                target_state=AgentState.ACTIVE_CHAT,
            ),
        },
        AgentState.ACTIVE_REPLY: {
            AgentState.IDLE: StateTransitionRule(
                target_state=AgentState.IDLE,
                effects=TransitionEffects(
                    cancel_active_chat_timer=True,
                    clear_active_reply_resume=True,
                ),
            ),
            AgentState.REVIEW: StateTransitionRule(
                target_state=AgentState.REVIEW,
                effects=TransitionEffects(clear_active_reply_resume=True),
            ),
        },
        AgentState.ACTIVE_CHAT: {
            AgentState.IDLE: StateTransitionRule(
                target_state=AgentState.IDLE,
                effects=TransitionEffects(
                    cancel_active_chat_timer=True,
                    stop_active_chat_runtime=True,
                    clear_active_chat_state=True,
                ),
            ),
        },
    }
    _MESSAGE_STATE_HANDLERS: dict[AgentState, PreparedMessageStateHandler] = {
        AgentState.IDLE: _handle_message_idle_or_review,
        AgentState.REVIEW: _handle_message_idle_or_review,
        AgentState.ACTIVE_REPLY: _handle_message_active_reply,
        AgentState.ACTIVE_CHAT: _handle_message_active_chat,
    }
    _REVIEW_DUE_STATE_HANDLERS: dict[AgentState, PreparedReviewStateHandler] = {
        AgentState.IDLE: _prepare_due_review_from_idle,
        AgentState.REVIEW: _prepare_due_review_while_review,
        AgentState.ACTIVE_REPLY: _prepare_due_review_while_active_reply,
        AgentState.ACTIVE_CHAT: _prepare_due_review_while_active_chat,
    }
    _ACTIVE_REPLY_RESUME_HANDLERS: dict[
        AgentState, ActiveReplyResumeStateHandler
    ] = {
        AgentState.REVIEW: _resume_active_reply_to_review,
    }

    def __init__(
        self,
        *,
        workflow_dispatcher: AgentWorkflowDispatcher | None = None,
        response_profile_resolver: ResponseProfileResolver,
        config: AgentSchedulerConfig | None = None,
        inbox: AgentInbox | None = None,
        state_store: AgentStateStore | None = None,
        priority_policy: PriorityPolicy | None = None,
        review_policy: ReviewPolicy | None = None,
        active_chat_policy: ActiveChatPolicy | None = None,
        active_chat_timer: ActiveChatTimer | None = None,
        now: Callable[[], float] | None = None,
    ) -> None:
        self._workflow_dispatcher = workflow_dispatcher
        self._response_profile_resolver = response_profile_resolver
        self._config = config or AgentSchedulerConfig()
        self._inbox = inbox or InMemoryAgentInbox()
        self._state_store = state_store or InMemoryAgentStateStore()
        self._priority_policy = priority_policy or DefaultPriorityPolicy(
            self._config.to_priority_policy_config()
        )
        self._review_policy = review_policy or DefaultReviewPolicy()
        self._active_chat_policy = active_chat_policy or DefaultActiveChatPolicy()
        self._active_chat_timer = active_chat_timer
        self._now = now or time.time
        self._unread_metadata: dict[tuple[str, int], UnreadMessage] = {}
        bind_scheduler = getattr(self._workflow_dispatcher, "bind_agent_scheduler", None)
        if bind_scheduler is not None:
            bind_scheduler(self)
        self._event_handlers: dict[SchedulerEventKind, SchedulerEventHandler] = {
            SchedulerEventKind.MESSAGE: self._handle_message_event,
            SchedulerEventKind.REVIEW_DUE: self._handle_review_due_event,
            SchedulerEventKind.ACTIVE_CHAT_TICK: self._handle_active_chat_tick_event,
            SchedulerEventKind.ACTIVE_CHAT_BOOTSTRAP: (
                self._handle_active_chat_bootstrap_event
            ),
        }

    async def accept_signal(self, signal: AgentSignal) -> AgentSignalDecision | None:
        """Accept one unified Agent signal and decide scheduler-side action."""
        self._log_signal_entry(signal)
        event = self._event_from_signal(signal)
        decision = await self._accept_event(event)
        self._log_signal_decision(signal, decision)
        return decision

    def queue_paused_message(
        self,
        signal: AgentSignal,
        *,
        pause_until: float,
    ) -> AgentScheduleDecision | None:
        """Store a paused-session message without starting Agent workflows.

        This keeps unread/high-priority backlog intact while a session-level
        Agent pause is active so the scheduler can resume naturally after the
        pause deadline passes.
        """
        self._log_signal_entry(signal)
        prepared = self._prepare_message_event(signal)
        if isinstance(prepared, AgentScheduleDecision):
            decision = prepared
        else:
            self._bring_review_plan_forward(
                signal.session_id,
                next_review_at=pause_until,
                now=prepared.checked_at,
                reason="session_agent_paused",
            )
            decision = AgentScheduleDecision(
                accepted=True,
                state=self._state_store.get_state(signal.session_id),
                unread_message=prepared.unread,
                high_priority_events=prepared.high_priority_events,
                skipped_reason="session_paused",
            )
        self._log_signal_decision(signal, decision)
        return decision

    def pause_session_until(
        self,
        session_id: str,
        *,
        pause_until: float,
        now: float | None = None,
    ) -> AgentState:
        """Pause scheduler-side work for one session until ``pause_until``.

        The scheduler preserves unread backlog and high-priority events, stops
        review/active-chat loops where possible, and ensures the next review is
        due once the pause deadline has passed.
        """
        checked_at = self._now() if now is None else now
        current_state = self._state_store.get_state(session_id)
        self._bring_review_plan_forward(
            session_id,
            next_review_at=pause_until,
            now=checked_at,
            reason="session_agent_paused",
        )
        if current_state == AgentState.REVIEW:
            self._cancel_review_runtime(session_id)
            self._transition_state(
                session_id,
                AgentState.IDLE,
                trigger=SchedulerTransitionTrigger.REVIEW_COMPLETE_RETURN_IDLE,
            )
        elif current_state == AgentState.ACTIVE_CHAT:
            self._transition_state(
                session_id,
                AgentState.IDLE,
                trigger=SchedulerTransitionTrigger.ACTIVE_CHAT_INTEREST_ADJUSTMENT_EXIT,
            )
        elif current_state == AgentState.ACTIVE_REPLY:
            self._stop_active_chat_runtime(session_id)
            self._transition_state(
                session_id,
                AgentState.IDLE,
                trigger=SchedulerTransitionTrigger.ACTIVE_REPLY_RETURN_IDLE,
            )
        logger.info(
            format_log_event(
                "agent.session.paused",
                session_id=session_id,
                previous_state=current_state.value,
                next_review_at=f"{pause_until:.2f}",
            )
        )
        return self._state_store.get_state(session_id)

    async def _accept_event(self, event: SchedulerEvent) -> AgentSignalDecision | None:
        handler = self._event_handlers.get(event.kind)
        if handler is None:
            raise RuntimeError(f"unsupported scheduler event kind: {event.kind!r}")
        return await handler(event)

    async def _handle_message_event(
        self,
        event: SchedulerEvent,
    ) -> AgentScheduleDecision:
        return await self._accept_message_signal(self._signal_from_event(event))

    async def _handle_review_due_event(
        self,
        event: SchedulerEvent,
    ) -> ReviewDueDecision:
        return await self._accept_review_due_signal(self._signal_from_event(event))

    async def _handle_active_chat_tick_event(
        self,
        event: SchedulerEvent,
    ) -> ActiveChatTickDecision:
        return await self._accept_active_chat_tick_signal(self._signal_from_event(event))

    async def _handle_active_chat_bootstrap_event(
        self,
        event: SchedulerEvent,
    ) -> ActiveChatBootstrapApplyDecision | None:
        return self._accept_active_chat_bootstrap_signal(self._signal_from_event(event))

    async def _accept_message_signal(self, signal: AgentSignal) -> AgentScheduleDecision:
        """Accept one message signal and decide scheduler-side action."""
        prepared = self._prepare_message_event(signal)
        if isinstance(prepared, AgentScheduleDecision):
            return prepared
        handler = self._MESSAGE_STATE_HANDLERS.get(prepared.initial_state)
        if handler is None:
            raise RuntimeError(
                f"unsupported message state handler: {prepared.initial_state!r}"
            )
        return await handler(self, prepared)

    async def _accept_review_due_signal(self, signal: AgentSignal) -> ReviewDueDecision:
        checked_at = self._timer_checked_at(signal)
        return await self.run_due_review(signal.session_id, now=checked_at)

    async def _accept_active_chat_tick_signal(
        self,
        signal: AgentSignal,
    ) -> ActiveChatTickDecision:
        checked_at = self._timer_checked_at(signal)
        next_review_plan = None
        preview = self.preview_active_chat_tick(signal.session_id, now=checked_at)
        if preview.will_return_idle:
            next_review_plan = await self.plan_idle_review_after_active_chat(signal.session_id)
        return self.tick_active_chat(
            signal.session_id,
            next_review_plan=next_review_plan,
            now=checked_at,
        )

    def _accept_active_chat_bootstrap_signal(
        self,
        signal: AgentSignal,
    ) -> ActiveChatBootstrapApplyDecision | None:
        payload = signal.active_chat_bootstrap
        if payload is None:
            return None
        return self.apply_active_chat_bootstrap(
            signal.session_id,
            disposition=payload.disposition,
            active_epoch=payload.active_epoch,
            now=signal.occurred_at,
        )

    def unread_messages(self, session_id: str) -> list[UnreadMessage]:
        """Return unread messages known to AgentScheduler for one session."""
        return [
            self._with_unread_metadata(message)
            for message in self._inbox.list_unread(session_id)
        ]

    def unread_ranges(self, session_id: str, *, limit: int = 50) -> list[UnreadRange]:
        """Return unread timeline ranges known to AgentScheduler for one session."""
        return self._inbox.list_unread_ranges(session_id, limit=limit)

    def count_unread_messages(self, session_id: str) -> int:
        """Return unread message count known to AgentScheduler for one session."""
        return self._inbox.count_unread_messages(session_id)

    def split_review_consumed(
        self,
        *,
        range_id: int,
        consumed_start_msg_log_id: int,
        consumed_end_msg_log_id: int,
    ) -> None:
        """Mark one interval inside an unread range consumed by review."""
        self._inbox.split_review_consumed(
            range_id=range_id,
            consumed_start_msg_log_id=consumed_start_msg_log_id,
            consumed_end_msg_log_id=consumed_end_msg_log_id,
        )

    def mark_ranges_review_consumed(self, range_ids: list[int]) -> None:
        """Mark whole unread ranges consumed by review."""
        self._inbox.mark_ranges_review_consumed(range_ids)

    def mark_active_chat_consumed(
        self,
        session_id: str,
        message_log_ids: list[int],
    ) -> list[UnreadMessage]:
        """Mark messages consumed by active chat."""
        consumed = self._inbox.mark_active_chat_consumed(
            session_id=session_id,
            message_log_ids=message_log_ids,
        )
        hydrated = [self._with_unread_metadata(message) for message in consumed]
        for message_log_id in message_log_ids:
            self._unread_metadata.pop((session_id, message_log_id), None)
        return hydrated

    def high_priority_events(self, session_id: str) -> list[HighPriorityEvent]:
        """Return high-priority events known to AgentScheduler for one session."""
        return self._inbox.list_high_priority_events(session_id)

    def list_session_ids(self, *, prefix: str | None = None) -> list[str]:
        """Return known scheduler session ids."""
        return self._state_store.list_session_ids(prefix=prefix)

    def state_for(self, session_id: str) -> AgentState:
        """Return current scheduler state for one session."""
        return self._state_store.get_state(session_id)

    def allowed_transitions_for(self, state: AgentState) -> frozenset[AgentState]:
        """Return the explicit state-transition targets allowed from *state*."""
        return frozenset(self._STATE_TRANSITION_RULES.get(state, {}).keys())

    def can_transition(self, current_state: AgentState, target_state: AgentState) -> bool:
        """Return whether a transition between two scheduler states is allowed."""
        if current_state == target_state:
            return True
        return target_state in self.allowed_transitions_for(current_state)

    def review_plan_for(self, session_id: str) -> ReviewPlan | None:
        """Return the current review plan for one session, if any."""
        return self._state_store.get_review_plan(session_id)

    def active_chat_state_for(self, session_id: str) -> ActiveChatState | None:
        """Return current active chat interest state for one session, if any."""
        return self._state_store.get_active_chat_state(session_id)

    async def plan_idle_review_after_active_chat(self, session_id: str) -> ReviewPlan | None:
        """Ask the workflow dispatcher to plan the next review before active chat exits."""
        planner = getattr(self._workflow_dispatcher, "plan_idle_review_after_active_chat", None)
        if planner is None:
            return None
        return await planner(session_id)

    def adjust_active_chat_interest(
        self,
        session_id: str,
        *,
        delta: float = 0.0,
        force_exit: bool = False,
        reason: str = "",
        next_review_plan: ReviewPlan | None = None,
        now: float | None = None,
    ) -> ActiveChatInterestAdjustDecision:
        """Apply workflow-driven active chat interest adjustment."""
        current_state = self._state_store.get_state(session_id)
        if current_state != AgentState.ACTIVE_CHAT:
            return ActiveChatInterestAdjustDecision(
                session_id=session_id,
                state=current_state,
                delta=delta,
                force_exit=force_exit,
                reason=reason,
                skipped_reason="not_active_chat",
            )

        active_chat_state = self._state_store.get_active_chat_state(session_id)
        if active_chat_state is None:
            return ActiveChatInterestAdjustDecision(
                session_id=session_id,
                state=current_state,
                delta=delta,
                force_exit=force_exit,
                reason=reason,
                skipped_reason="missing_active_chat_state",
            )

        checked_at = self._now() if now is None else now
        adjusted_state = self._active_chat_policy.adjust_interest(
            active_chat_state,
            delta=delta,
            force_exit=force_exit,
            now=checked_at,
        )
        self._state_store.set_active_chat_state(adjusted_state)
        if not force_exit and not self._active_chat_policy.should_return_idle(adjusted_state):
            return ActiveChatInterestAdjustDecision(
                session_id=session_id,
                state=AgentState.ACTIVE_CHAT,
                active_chat_state=adjusted_state,
                delta=delta,
                force_exit=force_exit,
                reason=reason,
            )

        exit_plan = self._prepare_active_chat_exit_plan(
            session_id=session_id,
            active_chat_state=adjusted_state,
            next_review_plan=next_review_plan,
            checked_at=checked_at,
            trigger=SchedulerTransitionTrigger.ACTIVE_CHAT_INTEREST_ADJUSTMENT_EXIT,
        )
        self._apply_active_chat_exit_plan(exit_plan)
        logger.info(
            format_log_event(
                "agent.active_chat.exit",
                cause="interest_adjustment",
                session_id=session_id,
                force_exit=force_exit,
                delta=f"{delta:.2f}",
                interest=f"{adjusted_state.interest_value:.2f}",
                reason=reason,
                next_review_at=f"{exit_plan.review_plan.next_review_at:.2f}",
                next_review_after_seconds=(
                    f"{max(0.0, exit_plan.review_plan.next_review_at - checked_at):.2f}"
                ),
            )
        )
        return ActiveChatInterestAdjustDecision(
            session_id=session_id,
            state=AgentState.IDLE,
            active_chat_state=adjusted_state,
            next_review_plan=exit_plan.review_plan,
            delta=delta,
            force_exit=force_exit,
            returned_to_idle=True,
            reason=reason,
        )

    def preview_active_chat_interest_adjustment(
        self,
        session_id: str,
        *,
        delta: float = 0.0,
        force_exit: bool = False,
        now: float | None = None,
    ) -> ActiveChatInterestAdjustmentPreview:
        """Preview a workflow-driven active chat interest adjustment."""
        current_state = self._state_store.get_state(session_id)
        if current_state != AgentState.ACTIVE_CHAT:
            return ActiveChatInterestAdjustmentPreview(
                session_id=session_id,
                state=current_state,
                delta=delta,
                force_exit=force_exit,
                skipped_reason="not_active_chat",
            )

        active_chat_state = self._state_store.get_active_chat_state(session_id)
        if active_chat_state is None:
            return ActiveChatInterestAdjustmentPreview(
                session_id=session_id,
                state=current_state,
                delta=delta,
                force_exit=force_exit,
                skipped_reason="missing_active_chat_state",
            )

        checked_at = self._now() if now is None else now
        adjusted_state = self._active_chat_policy.adjust_interest(
            active_chat_state,
            delta=delta,
            force_exit=force_exit,
            now=checked_at,
        )
        return ActiveChatInterestAdjustmentPreview(
            session_id=session_id,
            state=AgentState.ACTIVE_CHAT,
            active_chat_state=adjusted_state,
            delta=delta,
            force_exit=force_exit,
            will_return_idle=(
                force_exit or self._active_chat_policy.should_return_idle(adjusted_state)
            ),
        )

    def due_review_plans(self, *, now: float | None = None, limit: int = 50) -> list[ReviewPlan]:
        """Return review plans whose scheduled review time has arrived."""
        return self._state_store.list_due_review_plans(
            now=self._now() if now is None else now,
            limit=limit,
        )

    def prepare_due_review(
        self,
        session_id: str,
        *,
        now: float | None = None,
    ) -> ReviewDueDecision:
        """Prepare a due review, giving high-priority events a chance to interrupt."""
        current_state = self._state_store.get_state(session_id)
        plan = self._state_store.get_review_plan(session_id)
        if plan is None:
            return ReviewDueDecision(
                session_id=session_id,
                state=current_state,
                skipped_reason="missing_review_plan",
            )

        checked_at = self._now() if now is None else now
        if plan.next_review_at > checked_at:
            return ReviewDueDecision(
                session_id=session_id,
                state=current_state,
                review_plan=plan,
                skipped_reason="review_not_due",
            )
        handler = self._REVIEW_DUE_STATE_HANDLERS.get(current_state)
        if handler is None:
            raise RuntimeError(
                f"unsupported due-review state handler: {current_state!r}"
            )
        return handler(self, session_id, plan, checked_at)

    async def run_due_review(
        self,
        session_id: str,
        *,
        now: float | None = None,
    ) -> ReviewDueDecision:
        """Prepare and dispatch a due review workflow when no interrupt is pending."""
        checked_at = self._now() if now is None else now
        decision = self.prepare_due_review(session_id, now=checked_at)
        active_reply_request = self._active_reply_request_for_due_review(session_id, decision)
        if active_reply_request is not None:
            await self._dispatch_active_reply_workflow(active_reply_request)
            decision.state = self._state_store.get_state(session_id)
            review_request = self._review_request_for_due_review_followup(
                session_id,
                decision,
                checked_at=checked_at,
            )
            if review_request is None:
                return decision
            await self._dispatch_review_workflow(review_request)
            decision.review_workflow_started = True
            decision.state = self._state_store.get_state(session_id)
            return decision

        review_request = self._review_request_for_decision(session_id, decision)
        if review_request is None:
            return decision
        await self._dispatch_review_workflow(review_request)
        decision.review_workflow_started = True
        decision.state = self._state_store.get_state(session_id)
        return decision

    async def complete_active_reply(
        self,
        session_id: str,
        *,
        review_after: bool | None = None,
        now: float | None = None,
    ) -> ActiveReplyCompletionDecision:
        """Complete active reply and decide whether to resume review or return idle."""
        checked_at = self._now() if now is None else now
        decision = self._prepare_active_reply_completion_decision(
            session_id,
            review_after=review_after,
            checked_at=checked_at,
        )
        review_request = self._review_request_for_active_reply_completion(
            session_id,
            decision,
        )
        if review_request is None:
            return decision
        await self._dispatch_review_workflow(review_request)
        decision.review_workflow_started = True
        decision.state = self._state_store.get_state(session_id)
        return decision

    def complete_review(
        self,
        session_id: str,
        *,
        enter_active_chat: bool = False,
        active_chat_initial_interest: float | None = None,
        active_chat_decay_half_life_seconds: float | None = None,
        next_review_plan: ReviewPlan | None = None,
        now: float | None = None,
    ) -> ReviewCompletionDecision:
        """Complete review and transition into active chat or idle."""
        current_state = self._state_store.get_state(session_id)
        if current_state != AgentState.REVIEW:
            return ReviewCompletionDecision(
                session_id=session_id,
                state=current_state,
                skipped_reason="not_review",
            )

        checked_at = self._now() if now is None else now
        if enter_active_chat:
            active_chat_state = self._active_chat_policy.initial_state(
                session_id=session_id,
                now=checked_at,
                initial_interest_value=active_chat_initial_interest,
                decay_half_life_seconds=active_chat_decay_half_life_seconds,
            )
            self._transition_state(
                session_id,
                AgentState.ACTIVE_CHAT,
                trigger=SchedulerTransitionTrigger.REVIEW_COMPLETE_ENTER_ACTIVE_CHAT,
            )
            self._state_store.set_active_chat_state(active_chat_state)
            self._start_active_chat_timer(session_id)
            return ReviewCompletionDecision(
                session_id=session_id,
                state=AgentState.ACTIVE_CHAT,
                active_chat_state=active_chat_state,
                active_chat_started=True,
            )

        plan = next_review_plan or self._review_policy.plan_after_review(
            session_id=session_id,
            now=checked_at,
            previous_plan=self._state_store.get_review_plan(session_id),
        )
        self._transition_state(
            session_id,
            AgentState.IDLE,
            trigger=SchedulerTransitionTrigger.REVIEW_COMPLETE_RETURN_IDLE,
        )
        self._state_store.set_review_plan(plan)
        return ReviewCompletionDecision(
            session_id=session_id,
            state=AgentState.IDLE,
            next_review_plan=plan,
            returned_to_idle=True,
        )

    def tick_active_chat(
        self,
        session_id: str,
        *,
        next_review_plan: ReviewPlan | None = None,
        now: float | None = None,
    ) -> ActiveChatTickDecision:
        """Apply active chat interest decay and return idle if interest is exhausted."""
        current_state = self._state_store.get_state(session_id)
        if current_state != AgentState.ACTIVE_CHAT:
            return ActiveChatTickDecision(
                session_id=session_id,
                state=current_state,
                skipped_reason="not_active_chat",
            )

        checked_at = self._now() if now is None else now
        active_chat_state = self._state_store.get_active_chat_state(session_id)
        if active_chat_state is None:
            active_chat_state = self._active_chat_policy.initial_state(
                session_id=session_id,
                now=checked_at,
            )

        decayed_state = self._active_chat_policy.decay(
            active_chat_state,
            now=checked_at,
            count_tick=True,
        )
        self._state_store.set_active_chat_state(decayed_state)
        if not self._active_chat_policy.should_return_idle(decayed_state):
            return ActiveChatTickDecision(
                session_id=session_id,
                state=AgentState.ACTIVE_CHAT,
                active_chat_state=decayed_state,
            )

        exit_plan = self._prepare_active_chat_exit_plan(
            session_id=session_id,
            active_chat_state=decayed_state,
            next_review_plan=next_review_plan,
            checked_at=checked_at,
            trigger=SchedulerTransitionTrigger.ACTIVE_CHAT_DECAY_EXIT,
        )
        self._apply_active_chat_exit_plan(exit_plan)
        logger.info(
            format_log_event(
                "agent.active_chat.exit",
                cause="decay_tick",
                session_id=session_id,
                interest=f"{decayed_state.interest_value:.2f}",
                tick_count=decayed_state.tick_count,
                next_review_at=f"{exit_plan.review_plan.next_review_at:.2f}",
                next_review_after_seconds=(
                    f"{max(0.0, exit_plan.review_plan.next_review_at - checked_at):.2f}"
                ),
            )
        )
        return ActiveChatTickDecision(
            session_id=session_id,
            state=AgentState.IDLE,
            active_chat_state=decayed_state,
            next_review_plan=exit_plan.review_plan,
            returned_to_idle=True,
        )

    def reconcile_active_chat_sessions(
        self,
        *,
        now: float | None = None,
        prefix: str | None = None,
        exclude_session_ids: set[str] | None = None,
    ) -> list[ActiveChatTickDecision]:
        """Reconcile persisted active-chat states with runtime timer state.

        This is intended for process startup after scheduler state has been
        restored from persistence. Active-chat timers are in-memory only, so any
        persisted active-chat session must either be settled back to idle or have
        its timer re-armed.
        """
        checked_at = self._now() if now is None else now
        decisions: list[ActiveChatTickDecision] = []
        excluded = exclude_session_ids or set()
        for session_id in self._state_store.list_session_ids(prefix=prefix):
            if session_id in excluded:
                continue
            if self._state_store.get_state(session_id) != AgentState.ACTIVE_CHAT:
                continue
            decision = self.tick_active_chat(session_id, now=checked_at)
            decisions.append(decision)
            if decision.state == AgentState.ACTIVE_CHAT:
                self._start_active_chat_timer(session_id)
                logger.info(
                    format_log_event(
                        "agent.active_chat.reconciled",
                        session_id=session_id,
                        action="timer_started",
                        interest=(
                            f"{decision.active_chat_state.interest_value:.2f}"
                            if decision.active_chat_state is not None
                            else "-"
                        ),
                    )
                )
            elif decision.returned_to_idle:
                logger.info(
                    format_log_event(
                        "agent.active_chat.reconciled",
                        session_id=session_id,
                        action="returned_idle",
                    )
                )
        return decisions

    def reconcile_transient_sessions(
        self,
        *,
        now: float | None = None,
        prefix: str | None = None,
    ) -> list[str]:
        """Recover sessions left in transient states after an unclean shutdown."""

        checked_at = self._now() if now is None else now
        recovered: list[str] = []
        for session_id in self._state_store.list_session_ids(prefix=prefix):
            state = self._state_store.get_state(session_id)
            if state not in {AgentState.REVIEW, AgentState.ACTIVE_REPLY}:
                continue
            plan = self._state_store.get_review_plan(session_id)
            resume = self._state_store.get_active_reply_resume(session_id)
            if state == AgentState.ACTIVE_REPLY and resume is not None:
                plan = resume.review_plan or plan
            if plan is not None:
                resumed_at = min(plan.next_review_at, checked_at)
                self._state_store.set_review_plan(
                    replace(
                        plan,
                        next_review_at=resumed_at,
                        updated_at=checked_at,
                    )
                )
            self._transition_state(
                session_id,
                AgentState.IDLE,
                trigger=SchedulerTransitionTrigger.TRANSIENT_STATE_RECOVERED,
            )
            recovered.append(session_id)
            logger.info(
                format_log_event(
                    "agent.transient_state.recovered",
                    session_id=session_id,
                    previous_state=state.value,
                    next_review_at=(
                        f"{self._state_store.get_review_plan(session_id).next_review_at:.2f}"
                        if self._state_store.get_review_plan(session_id) is not None
                        else ""
                    ),
                )
            )
        return recovered

    def preview_active_chat_tick(
        self,
        session_id: str,
        *,
        now: float | None = None,
    ) -> ActiveChatTickPreview:
        """Preview one active chat decay tick without mutating scheduler state."""
        current_state = self._state_store.get_state(session_id)
        if current_state != AgentState.ACTIVE_CHAT:
            return ActiveChatTickPreview(
                session_id=session_id,
                state=current_state,
                skipped_reason="not_active_chat",
            )

        checked_at = self._now() if now is None else now
        active_chat_state = self._state_store.get_active_chat_state(session_id)
        if active_chat_state is None:
            active_chat_state = self._active_chat_policy.initial_state(
                session_id=session_id,
                now=checked_at,
            )
        decayed_state = self._active_chat_policy.decay(
            active_chat_state,
            now=checked_at,
            count_tick=True,
        )
        return ActiveChatTickPreview(
            session_id=session_id,
            state=AgentState.ACTIVE_CHAT,
            active_chat_state=decayed_state,
            will_return_idle=self._active_chat_policy.should_return_idle(decayed_state),
        )

    def apply_active_chat_bootstrap(
        self,
        session_id: str,
        *,
        disposition: ActiveChatDisposition,
        active_epoch: int | None = None,
        now: float | None = None,
    ) -> ActiveChatBootstrapApplyDecision:
        """Apply delayed review stage-3 disposition to the current active chat state."""
        current_state = self._state_store.get_state(session_id)
        if current_state != AgentState.ACTIVE_CHAT:
            return ActiveChatBootstrapApplyDecision(
                session_id=session_id,
                state=current_state,
                skipped_reason="not_active_chat",
            )

        active_chat_state = self._state_store.get_active_chat_state(session_id)
        if active_chat_state is None:
            return ActiveChatBootstrapApplyDecision(
                session_id=session_id,
                state=current_state,
                skipped_reason="missing_active_chat_state",
            )
        if active_epoch is not None and active_chat_state.active_epoch != active_epoch:
            return ActiveChatBootstrapApplyDecision(
                session_id=session_id,
                state=current_state,
                active_chat_state=active_chat_state,
                skipped_reason="active_epoch_mismatch",
            )
        if active_chat_state.bootstrap_applied:
            return ActiveChatBootstrapApplyDecision(
                session_id=session_id,
                state=current_state,
                active_chat_state=active_chat_state,
                skipped_reason="bootstrap_already_applied",
            )

        checked_at = self._now() if now is None else now
        corrected_state = self._active_chat_policy.apply_bootstrap_disposition(
            active_chat_state,
            disposition=disposition,
            now=checked_at,
        )
        self._state_store.set_active_chat_state(corrected_state)
        if not self._active_chat_policy.should_return_idle(corrected_state):
            return ActiveChatBootstrapApplyDecision(
                session_id=session_id,
                state=AgentState.ACTIVE_CHAT,
                active_chat_state=corrected_state,
                bootstrap_applied=True,
            )

        exit_plan = self._prepare_active_chat_exit_plan(
            session_id=session_id,
            active_chat_state=corrected_state,
            next_review_plan=None,
            checked_at=checked_at,
            trigger=SchedulerTransitionTrigger.ACTIVE_CHAT_BOOTSTRAP_EXIT,
        )
        self._apply_active_chat_exit_plan(exit_plan)
        return ActiveChatBootstrapApplyDecision(
            session_id=session_id,
            state=AgentState.IDLE,
            active_chat_state=corrected_state,
            next_review_plan=exit_plan.review_plan,
            bootstrap_applied=True,
            returned_to_idle=True,
        )

    def _observe_active_chat_message(
        self,
        *,
        session_id: str,
        now: float,
        is_from_bot: bool,
        is_mentioned: bool,
        is_reply_to_bot: bool,
        is_mention_to_other: bool,
        is_poke_to_bot: bool,
        is_poke_to_other: bool,
    ) -> ActiveChatState:
        active_chat_state = self._state_store.get_active_chat_state(session_id)
        if active_chat_state is None:
            active_chat_state = self._active_chat_policy.initial_state(
                session_id=session_id,
                now=now,
            )
        observed_state = self._active_chat_policy.observe_message(
            active_chat_state,
            now=now,
            is_from_bot=is_from_bot,
            is_mentioned=is_mentioned,
            is_reply_to_bot=is_reply_to_bot,
            is_mention_to_other=is_mention_to_other,
            is_poke_to_bot=is_poke_to_bot,
            is_poke_to_other=is_poke_to_other,
        )
        self._state_store.set_active_chat_state(observed_state)
        return observed_state

    def _start_active_chat_timer(self, session_id: str) -> None:
        if self._active_chat_timer is not None:
            self._active_chat_timer.start(session_id)

    def _cancel_active_chat_timer(self, session_id: str) -> None:
        if self._active_chat_timer is not None:
            self._active_chat_timer.cancel(session_id)

    def _stop_active_chat_runtime(self, session_id: str) -> None:
        stop_active_chat = getattr(self._workflow_dispatcher, "stop_active_chat", None)
        if stop_active_chat is not None:
            stop_active_chat(session_id)
        self._cancel_active_chat_timer(session_id)

    def _cancel_review_runtime(self, session_id: str) -> None:
        cancel_review = getattr(self._workflow_dispatcher, "cancel_review", None)
        if cancel_review is not None:
            cancel_review(session_id)

    def _prepare_message_event(
        self,
        signal: AgentSignal,
    ) -> AgentScheduleDecision | PreparedMessageEvent:
        initial_state = self._state_store.get_state(signal.session_id)
        message = signal.message
        if signal.kind != AgentSignalKind.MESSAGE or message is None:
            return AgentScheduleDecision(
                accepted=False,
                state=initial_state,
                skipped_reason="not_message_signal",
            )
        if message.message_log_id is None:
            return AgentScheduleDecision(
                accepted=False,
                state=initial_state,
                skipped_reason="missing_message_log_id",
            )
        if message.already_handled:
            return AgentScheduleDecision(
                accepted=False,
                state=initial_state,
                skipped_reason="already_handled",
            )
        if message.is_stopped:
            return AgentScheduleDecision(
                accepted=False,
                state=initial_state,
                skipped_reason="stopped",
            )
        if _is_self_message(signal):
            return AgentScheduleDecision(
                accepted=False,
                state=initial_state,
                skipped_reason="self_message",
            )

        checked_at = self._now()
        self._ensure_review_plan(signal.session_id, checked_at)
        response_profile = self._response_profile_resolver(signal)
        unread = UnreadMessage(
            session_id=signal.session_id,
            message_log_id=message.message_log_id,
            sender_id=message.sender_id,
            created_at=checked_at,
            response_profile=response_profile,
            is_mentioned=message.is_mentioned,
            is_reply_to_bot=message.is_reply_to_bot,
            is_mention_to_other=message.is_mention_to_other,
            is_poke_to_bot=message.is_poke_to_bot,
            is_poke_to_other=message.is_poke_to_other,
            self_platform_id=message.self_id,
            trace_id=_signal_trace_id(signal),
        )
        self._remember_unread(unread)
        high_priority_events, should_start_active_reply = (
            self._evaluate_message_priority(
                signal,
                initial_state=initial_state,
                now=checked_at,
            )
        )
        return PreparedMessageEvent(
            signal=signal,
            message=message,
            initial_state=initial_state,
            checked_at=checked_at,
            response_profile=response_profile,
            unread=unread,
            high_priority_events=high_priority_events,
            should_start_active_reply=should_start_active_reply,
        )

    def _remember_unread(self, unread: UnreadMessage) -> None:
        self._unread_metadata[(unread.session_id, unread.message_log_id)] = unread
        self._inbox.add_unread(unread)

    def _evaluate_message_priority(
        self,
        signal: AgentSignal,
        *,
        initial_state: AgentState,
        now: float,
    ) -> tuple[list[HighPriorityEvent], bool]:
        if initial_state == AgentState.ACTIVE_CHAT:
            return [], False
        decision = self._priority_policy.evaluate(
            signal,
            now=now,
            inbox=self._inbox,
        )
        if decision.events:
            self._inbox.add_high_priority_events(decision.events)
        return decision.events, decision.should_start_active_reply

    def _should_start_active_reply_for_message(
        self,
        prepared: PreparedMessageEvent,
    ) -> bool:
        return (
            prepared.should_start_active_reply
            and prepared.initial_state != AgentState.ACTIVE_REPLY
            and self._workflow_dispatcher is not None
        )

    async def _handle_message_idle_or_review_state(
        self,
        prepared: PreparedMessageEvent,
    ) -> AgentScheduleDecision:
        if self._should_start_active_reply_for_message(prepared):
            return await self._start_active_reply_for_message(prepared)
        return AgentScheduleDecision(
            accepted=True,
            state=self._state_store.get_state(prepared.signal.session_id),
            unread_message=prepared.unread,
            high_priority_events=prepared.high_priority_events,
            active_reply_started=False,
        )

    async def _handle_message_active_reply_state(
        self,
        prepared: PreparedMessageEvent,
    ) -> AgentScheduleDecision:
        return AgentScheduleDecision(
            accepted=True,
            state=self._state_store.get_state(prepared.signal.session_id),
            unread_message=prepared.unread,
            high_priority_events=prepared.high_priority_events,
            active_reply_started=False,
        )

    async def _handle_message_active_chat_state(
        self,
        prepared: PreparedMessageEvent,
    ) -> AgentScheduleDecision:
        return await self._observe_active_chat_message_event(prepared)

    async def _start_active_reply_for_message(
        self,
        prepared: PreparedMessageEvent,
    ) -> AgentScheduleDecision:
        self._enter_active_reply(
            prepared.signal.session_id,
            resume_kind=(
                ActiveReplyResumeKind.RESUME_INTERRUPTED_REVIEW
                if prepared.initial_state == AgentState.REVIEW
                else None
            ),
            resume_state=(
                AgentState.REVIEW if prepared.initial_state == AgentState.REVIEW else None
            ),
            review_plan=self._state_store.get_review_plan(prepared.signal.session_id),
            now=prepared.checked_at,
        )
        request = self._active_reply_request_from_prepared_message(prepared)
        await self._dispatch_active_reply_workflow(request)
        return AgentScheduleDecision(
            accepted=True,
            state=self._state_store.get_state(prepared.signal.session_id),
            unread_message=prepared.unread,
            high_priority_events=prepared.high_priority_events,
            active_reply_started=True,
        )

    async def _observe_active_chat_message_event(
        self,
        prepared: PreparedMessageEvent,
    ) -> AgentScheduleDecision:
        active_chat_state = self._observe_active_chat_message(
            session_id=prepared.signal.session_id,
            now=prepared.checked_at,
            is_from_bot=False,
            is_mentioned=prepared.message.is_mentioned,
            is_reply_to_bot=prepared.message.is_reply_to_bot,
            is_mention_to_other=prepared.message.is_mention_to_other,
            is_poke_to_bot=prepared.message.is_poke_to_bot,
            is_poke_to_other=prepared.message.is_poke_to_other,
        )
        self._start_active_chat_timer(prepared.signal.session_id)
        active_chat_workflow_notified = False
        if self._workflow_dispatcher is not None:
            await self._workflow_dispatcher.notify_active_chat_message(
                session_id=prepared.signal.session_id,
                message_log_id=prepared.message.message_log_id,
                sender_id=prepared.message.sender_id,
                response_profile=prepared.response_profile,
                is_mentioned=prepared.message.is_mentioned,
                is_reply_to_bot=prepared.message.is_reply_to_bot,
                is_mention_to_other=prepared.message.is_mention_to_other,
                is_poke_to_bot=prepared.message.is_poke_to_bot,
                is_poke_to_other=prepared.message.is_poke_to_other,
                self_platform_id=prepared.message.self_id,
                active_chat_state=active_chat_state,
                trace_id=prepared.unread.trace_id,
            )
            active_chat_workflow_notified = True
        return AgentScheduleDecision(
            accepted=True,
            state=self._state_store.get_state(prepared.signal.session_id),
            unread_message=prepared.unread,
            active_chat_state=active_chat_state,
            high_priority_events=prepared.high_priority_events,
            active_chat_observed=True,
            active_chat_workflow_notified=active_chat_workflow_notified,
            active_reply_started=False,
        )

    def _prepare_due_review_from_idle_state(
        self,
        session_id: str,
        plan: ReviewPlan,
        checked_at: float,
    ) -> ReviewDueDecision:
        high_priority_events = self._inbox.list_high_priority_events(session_id)
        if high_priority_events:
            self._enter_active_reply(
                session_id,
                resume_kind=ActiveReplyResumeKind.START_DEFERRED_REVIEW,
                resume_state=AgentState.REVIEW,
                review_plan=plan,
                now=checked_at,
            )
            return ReviewDueDecision(
                session_id=session_id,
                state=AgentState.ACTIVE_REPLY,
                review_plan=plan,
                high_priority_events=high_priority_events,
                active_reply_pending=True,
            )

        self._transition_state(
            session_id,
            AgentState.REVIEW,
            trigger=SchedulerTransitionTrigger.REVIEW_DUE,
        )
        return ReviewDueDecision(
            session_id=session_id,
            state=AgentState.REVIEW,
            review_plan=plan,
            review_started=True,
        )

    def _skip_due_review_for_state(
        self,
        session_id: str,
        state: AgentState,
        plan: ReviewPlan,
        skipped_reason: str,
    ) -> ReviewDueDecision:
        return ReviewDueDecision(
            session_id=session_id,
            state=state,
            review_plan=plan,
            skipped_reason=skipped_reason,
        )

    def _prepare_active_reply_completion_decision(
        self,
        session_id: str,
        *,
        review_after: bool | None,
        checked_at: float,
    ) -> ActiveReplyCompletionDecision:
        current_state = self._state_store.get_state(session_id)
        if current_state != AgentState.ACTIVE_REPLY:
            return ActiveReplyCompletionDecision(
                session_id=session_id,
                state=current_state,
                skipped_reason="not_active_reply",
            )

        handled_events = self._inbox.mark_high_priority_events_handled(session_id)
        resume = self._state_store.get_active_reply_resume(session_id)
        if resume is not None:
            handler = self._ACTIVE_REPLY_RESUME_HANDLERS.get(resume.resume_state)
            if handler is None:
                raise RuntimeError(
                    f"unsupported active-reply resume state: {resume.resume_state!r}"
                )
            return handler(
                self,
                session_id,
                resume,
                handled_events,
                checked_at,
            )

        plan = self._state_store.get_review_plan(session_id)
        should_review = self._should_review_after_active_reply(
            plan=plan,
            review_after=review_after,
            now=checked_at,
        )
        if not should_review or plan is None:
            return self._prepare_idle_after_active_reply(
                session_id,
                plan=plan,
                handled_events=handled_events,
            )
        return self._prepare_deferred_review_after_active_reply(
            session_id,
            plan=plan,
            handled_events=handled_events,
        )

    def _prepare_resumed_review_after_active_reply(
        self,
        session_id: str,
        *,
        resume: ActiveReplyResume,
        handled_events: list[HighPriorityEvent],
        checked_at: float,
    ) -> ActiveReplyCompletionDecision:
        plan = resume.review_plan or self._state_store.get_review_plan(session_id)
        if plan is None:
            initial_plan = self._review_policy.initial_plan(
                session_id=session_id,
                now=checked_at,
            )
            plan = replace(
                initial_plan,
                next_review_at=checked_at,
                reason=(
                    "resumed_interrupted_review"
                    if resume.kind == ActiveReplyResumeKind.RESUME_INTERRUPTED_REVIEW
                    else "deferred_review_after_active_reply"
                ),
                updated_at=checked_at,
            )
        else:
            plan = replace(
                plan,
                next_review_at=min(plan.next_review_at, checked_at),
                updated_at=checked_at,
            )
        self._state_store.set_review_plan(plan)
        self._transition_state(
            session_id,
            AgentState.REVIEW,
            trigger=(
                SchedulerTransitionTrigger.ACTIVE_REPLY_RESUME_INTERRUPTED_REVIEW
                if resume.kind == ActiveReplyResumeKind.RESUME_INTERRUPTED_REVIEW
                else SchedulerTransitionTrigger.ACTIVE_REPLY_START_DEFERRED_REVIEW
            ),
        )
        return ActiveReplyCompletionDecision(
            session_id=session_id,
            state=AgentState.REVIEW,
            review_plan=plan,
            handled_high_priority_events=handled_events,
            review_started=True,
        )

    def _prepare_idle_after_active_reply(
        self,
        session_id: str,
        *,
        plan: ReviewPlan | None,
        handled_events: list[HighPriorityEvent],
    ) -> ActiveReplyCompletionDecision:
        self._transition_state(
            session_id,
            AgentState.IDLE,
            trigger=SchedulerTransitionTrigger.ACTIVE_REPLY_RETURN_IDLE,
        )
        return ActiveReplyCompletionDecision(
            session_id=session_id,
            state=AgentState.IDLE,
            review_plan=plan,
            handled_high_priority_events=handled_events,
            returned_to_idle=True,
            skipped_reason="missing_review_plan" if plan is None else "review_not_requested",
        )

    def _prepare_deferred_review_after_active_reply(
        self,
        session_id: str,
        *,
        plan: ReviewPlan,
        handled_events: list[HighPriorityEvent],
    ) -> ActiveReplyCompletionDecision:
        self._transition_state(
            session_id,
            AgentState.REVIEW,
            trigger=SchedulerTransitionTrigger.ACTIVE_REPLY_START_DEFERRED_REVIEW,
        )
        return ActiveReplyCompletionDecision(
            session_id=session_id,
            state=AgentState.REVIEW,
            review_plan=plan,
            handled_high_priority_events=handled_events,
            review_started=True,
        )

    def _active_reply_request_from_prepared_message(
        self,
        prepared: PreparedMessageEvent,
    ) -> ActiveReplyWorkflowRequest:
        return ActiveReplyWorkflowRequest(
            session_id=prepared.signal.session_id,
            message_log_id=prepared.message.message_log_id,
            sender_id=prepared.message.sender_id,
            response_profile=prepared.response_profile,
            is_mentioned=prepared.message.is_mentioned,
            is_reply_to_bot=prepared.message.is_reply_to_bot,
            is_mention_to_other=prepared.message.is_mention_to_other,
            is_poke_to_bot=prepared.message.is_poke_to_bot,
            is_poke_to_other=prepared.message.is_poke_to_other,
            self_platform_id=prepared.message.self_id,
            events=prepared.high_priority_events,
            trace_id=prepared.unread.trace_id,
        )

    def _active_reply_request_for_due_review(
        self,
        session_id: str,
        decision: ReviewDueDecision,
    ) -> ActiveReplyWorkflowRequest | None:
        if (
            not decision.active_reply_pending
            or not decision.high_priority_events
            or self._workflow_dispatcher is None
        ):
            return None
        event = decision.high_priority_events[0]
        unread = self._find_unread_message(session_id, event.message_log_id)
        return ActiveReplyWorkflowRequest(
            session_id=session_id,
            message_log_id=event.message_log_id,
            sender_id=event.sender_id,
            response_profile=unread.response_profile if unread is not None else "",
            is_mentioned=_has_high_priority_kind(
                decision.high_priority_events,
                HighPriorityEventKind.MENTION,
                HighPriorityEventKind.REPEATED_MENTION,
            ),
            is_reply_to_bot=_has_high_priority_kind(
                decision.high_priority_events,
                HighPriorityEventKind.REPLY_TO_BOT,
            ),
            is_mention_to_other=unread.is_mention_to_other if unread is not None else False,
            is_poke_to_bot=_has_high_priority_kind(
                decision.high_priority_events,
                HighPriorityEventKind.POKE,
            ),
            is_poke_to_other=unread.is_poke_to_other if unread is not None else False,
            self_platform_id=unread.self_platform_id if unread is not None else "",
            events=decision.high_priority_events,
            trace_id=unread.trace_id if unread is not None else "",
        )

    def _review_request_for_due_review_followup(
        self,
        session_id: str,
        decision: ReviewDueDecision,
        *,
        checked_at: float,
    ) -> ReviewWorkflowRequest | None:
        if (
            decision.state != AgentState.IDLE
            or decision.review_plan is None
            or decision.review_plan.next_review_at > checked_at
        ):
            return None
        self._transition_state(
            session_id,
            AgentState.REVIEW,
            trigger=SchedulerTransitionTrigger.DEFERRED_REVIEW_AFTER_ACTIVE_REPLY,
        )
        decision.state = AgentState.REVIEW
        decision.review_started = True
        return self._build_review_workflow_request(
            session_id=session_id,
            review_plan=decision.review_plan,
        )

    def _review_request_for_decision(
        self,
        session_id: str,
        decision: ReviewDueDecision,
    ) -> ReviewWorkflowRequest | None:
        if (
            not decision.review_started
            or decision.review_plan is None
            or self._workflow_dispatcher is None
        ):
            return None
        return self._build_review_workflow_request(
            session_id=session_id,
            review_plan=decision.review_plan,
        )

    def _review_request_for_active_reply_completion(
        self,
        session_id: str,
        decision: ActiveReplyCompletionDecision,
    ) -> ReviewWorkflowRequest | None:
        if (
            not decision.review_started
            or decision.review_plan is None
            or self._workflow_dispatcher is None
        ):
            return None
        return self._build_review_workflow_request(
            session_id=session_id,
            review_plan=decision.review_plan,
        )

    def _build_review_workflow_request(
        self,
        *,
        session_id: str,
        review_plan: ReviewPlan,
    ) -> ReviewWorkflowRequest:
        return ReviewWorkflowRequest(
            session_id=session_id,
            review_plan=review_plan,
            unread_messages=self.unread_messages(session_id),
        )

    async def _dispatch_active_reply_workflow(
        self,
        request: ActiveReplyWorkflowRequest,
    ) -> None:
        workflow_dispatcher = self._workflow_dispatcher
        if workflow_dispatcher is None:
            raise RuntimeError("active reply workflow dispatcher is unavailable")
        await workflow_dispatcher.run_active_reply(
            session_id=request.session_id,
            message_log_id=request.message_log_id,
            sender_id=request.sender_id,
            response_profile=request.response_profile,
            is_mentioned=request.is_mentioned,
            is_reply_to_bot=request.is_reply_to_bot,
            is_mention_to_other=request.is_mention_to_other,
            is_poke_to_bot=request.is_poke_to_bot,
            is_poke_to_other=request.is_poke_to_other,
            self_platform_id=request.self_platform_id,
            events=request.events,
            trace_id=request.trace_id,
        )

    async def _dispatch_review_workflow(
        self,
        request: ReviewWorkflowRequest,
    ) -> None:
        workflow_dispatcher = self._workflow_dispatcher
        if workflow_dispatcher is None:
            raise RuntimeError("review workflow dispatcher is unavailable")
        await workflow_dispatcher.run_review(
            session_id=request.session_id,
            review_plan=request.review_plan,
            unread_messages=request.unread_messages,
        )

    def _prepare_active_chat_exit_plan(
        self,
        *,
        session_id: str,
        active_chat_state: ActiveChatState,
        next_review_plan: ReviewPlan | None,
        checked_at: float,
        trigger: SchedulerTransitionTrigger,
    ) -> ActiveChatExitPlan:
        review_plan = next_review_plan or self._review_policy.plan_after_review(
            session_id=session_id,
            now=checked_at,
            previous_plan=self._state_store.get_review_plan(session_id),
        )
        return ActiveChatExitPlan(
            session_id=session_id,
            active_chat_state=active_chat_state,
            review_plan=review_plan,
            trigger=trigger,
            checked_at=checked_at,
        )

    def _apply_active_chat_exit_plan(
        self,
        exit_plan: ActiveChatExitPlan,
    ) -> None:
        self._transition_state(
            exit_plan.session_id,
            AgentState.IDLE,
            trigger=exit_plan.trigger,
        )
        self._state_store.set_review_plan(exit_plan.review_plan)

    def _enter_active_reply(
        self,
        session_id: str,
        *,
        resume_kind: ActiveReplyResumeKind | None = None,
        resume_state: AgentState | None = None,
        review_plan: ReviewPlan | None = None,
        now: float | None = None,
    ) -> None:
        checked_at = self._now() if now is None else now
        if resume_state is not None:
            self._state_store.set_active_reply_resume(
                ActiveReplyResume(
                    session_id=session_id,
                    kind=resume_kind or ActiveReplyResumeKind.RESUME_INTERRUPTED_REVIEW,
                    resume_state=resume_state,
                    review_plan=review_plan,
                    updated_at=checked_at,
                )
            )
        else:
            self._state_store.clear_active_reply_resume(session_id)
        self._transition_state(
            session_id,
            AgentState.ACTIVE_REPLY,
            trigger=SchedulerTransitionTrigger.MESSAGE_PRIORITY_WAKE,
        )

    def _ensure_review_plan(self, session_id: str, now: float) -> None:
        if self._state_store.get_review_plan(session_id) is not None:
            return
        plan = self._review_policy.initial_plan(session_id=session_id, now=now)
        self._state_store.set_review_plan(plan)
        logger.debug(
            format_log_event(
                "agent.review.plan.created",
                session_id=session_id,
                reason=plan.reason,
                next_review_at=f"{plan.next_review_at:.2f}",
                next_review_after_seconds=f"{max(0.0, plan.next_review_at - now):.2f}",
            )
        )

    def _bring_review_plan_forward(
        self,
        session_id: str,
        *,
        next_review_at: float,
        now: float,
        reason: str,
    ) -> ReviewPlan:
        current = self._state_store.get_review_plan(session_id)
        if current is None:
            plan = replace(
                self._review_policy.initial_plan(session_id=session_id, now=now),
                next_review_at=next_review_at,
                reason=reason,
                updated_at=now,
            )
        elif current.next_review_at == next_review_at and current.reason == reason:
            return current
        else:
            plan = replace(
                current,
                next_review_at=next_review_at,
                reason=reason,
                updated_at=now,
            )
        self._state_store.set_review_plan(plan)
        return plan

    @staticmethod
    def _timer_checked_at(signal: AgentSignal) -> float:
        if signal.timer is not None and signal.timer.due_at is not None:
            return signal.timer.due_at
        return signal.occurred_at

    @staticmethod
    def _event_from_signal(signal: AgentSignal) -> SchedulerEvent:
        try:
            kind = AgentScheduler._SIGNAL_EVENT_KIND_MAP[signal.kind]
        except KeyError as exc:
            raise RuntimeError(f"unsupported agent signal kind: {signal.kind!r}") from exc
        return SchedulerEvent(kind=kind, signal=signal)

    @staticmethod
    def _signal_from_event(event: SchedulerEvent) -> AgentSignal:
        signal = event.signal
        if not isinstance(signal, AgentSignal):
            raise TypeError(f"scheduler event payload must be AgentSignal, got {type(signal)!r}")
        return signal

    def _with_unread_metadata(self, message: UnreadMessage) -> UnreadMessage:
        metadata = self._unread_metadata.get((message.session_id, message.message_log_id))
        if metadata is None:
            return message

        return replace(
            message,
            response_profile=message.response_profile or metadata.response_profile,
            is_mentioned=message.is_mentioned or metadata.is_mentioned,
            is_reply_to_bot=message.is_reply_to_bot or metadata.is_reply_to_bot,
            is_mention_to_other=message.is_mention_to_other or metadata.is_mention_to_other,
            is_poke_to_bot=message.is_poke_to_bot or metadata.is_poke_to_bot,
            is_poke_to_other=message.is_poke_to_other or metadata.is_poke_to_other,
            self_platform_id=message.self_platform_id or metadata.self_platform_id,
            trace_id=message.trace_id or metadata.trace_id,
        )

    def _find_unread_message(
        self,
        session_id: str,
        message_log_id: int,
    ) -> UnreadMessage | None:
        cached = self._unread_metadata.get((session_id, message_log_id))
        if cached is not None:
            return cached
        for message in self._inbox.list_unread(session_id):
            if message.message_log_id != message_log_id:
                continue
            hydrated = self._with_unread_metadata(message)
            self._unread_metadata[(session_id, message_log_id)] = hydrated
            return hydrated
        return None

    def _transition_state(
        self,
        session_id: str,
        target_state: AgentState,
        *,
        trigger: SchedulerTransitionTrigger,
    ) -> AgentState:
        current_state = self._state_store.get_state(session_id)
        if current_state == target_state:
            return current_state
        if not self.can_transition(current_state, target_state):
            raise RuntimeError(
                f"invalid agent state transition: {session_id}: {current_state.value} -> {target_state.value} ({trigger.value})"
            )
        rule = self._STATE_TRANSITION_RULES[current_state][target_state]
        self._apply_transition_effects(session_id, rule.effects)
        self._state_store.set_state(session_id, target_state)
        logger.info(
            format_log_event(
                "agent.state.transition",
                session_id=session_id,
                from_state=current_state.value,
                to_state=target_state.value,
                trigger=trigger.value,
            )
        )
        return current_state

    def _apply_transition_effects(
        self,
        session_id: str,
        effects: TransitionEffects,
    ) -> None:
        if effects.cancel_review_runtime:
            self._cancel_review_runtime(session_id)
        if effects.stop_active_chat_runtime:
            self._stop_active_chat_runtime(session_id)
        elif effects.cancel_active_chat_timer:
            self._cancel_active_chat_timer(session_id)
        if effects.clear_active_reply_resume:
            self._state_store.clear_active_reply_resume(session_id)
        if effects.clear_active_chat_state:
            self._state_store.clear_active_chat_state(session_id)

    def _log_signal_entry(self, signal: AgentSignal) -> None:
        if not logger.isEnabledFor(logging.DEBUG):
            return
        logger.debug(
            format_log_event(
                "agent.signal.entry",
                kind=_enum_value(signal.kind),
                source=_enum_value(signal.source),
                signal_id=signal.signal_id,
                session_id=signal.session_id,
                bot_id=signal.bot_id,
                trace_id=_signal_trace_id(signal),
                state=_enum_value(self._state_store.get_state(signal.session_id)),
                message_log_id=_signal_message_log_id(signal),
                timer_trigger=(
                    signal.timer.trigger if signal.timer is not None else ""
                ),
                timer_due_at=(
                    f"{signal.timer.due_at:.2f}"
                    if signal.timer is not None and signal.timer.due_at is not None
                    else ""
                ),
            )
        )

    def _log_signal_decision(
        self,
        signal: AgentSignal,
        decision: AgentSignalDecision | None,
    ) -> None:
        if not logger.isEnabledFor(logging.DEBUG):
            return
        if decision is None:
            logger.debug(
                format_log_event(
                    "agent.signal.decision",
                    kind=_enum_value(signal.kind),
                    signal_id=signal.signal_id,
                    session_id=signal.session_id,
                    trace_id=_signal_trace_id(signal),
                    status="ignored",
                )
            )
            return

        review_plan = _decision_review_plan(decision)
        active_chat_state = getattr(decision, "active_chat_state", None)
        logger.debug(
            format_log_event(
                "agent.signal.decision",
                kind=_enum_value(signal.kind),
                signal_id=signal.signal_id,
                session_id=signal.session_id,
                bot_id=signal.bot_id,
                trace_id=_signal_trace_id(signal),
                state=_enum_value(getattr(decision, "state", "")),
                skipped_reason=getattr(decision, "skipped_reason", ""),
                accepted=getattr(decision, "accepted", None),
                message_log_id=_signal_message_log_id(signal),
                high_priority_count=len(getattr(decision, "high_priority_events", []) or []),
                active_reply_started=getattr(decision, "active_reply_started", None),
                active_reply_pending=getattr(decision, "active_reply_pending", None),
                review_started=getattr(decision, "review_started", None),
                review_workflow_started=getattr(
                    decision,
                    "review_workflow_started",
                    None,
                ),
                active_chat_started=getattr(decision, "active_chat_started", None),
                active_chat_observed=getattr(decision, "active_chat_observed", None),
                active_chat_notified=getattr(
                    decision,
                    "active_chat_workflow_notified",
                    None,
                ),
                returned_to_idle=getattr(decision, "returned_to_idle", None),
                active_chat_interest=(
                    f"{active_chat_state.interest_value:.2f}"
                    if active_chat_state is not None
                    else ""
                ),
                next_review_at=(
                    f"{review_plan.next_review_at:.2f}"
                    if review_plan is not None
                    else ""
                ),
            )
        )

    @staticmethod
    def _should_review_after_active_reply(
        *,
        plan: ReviewPlan | None,
        review_after: bool | None,
        now: float,
    ) -> bool:
        if plan is None:
            return False
        if review_after is not None:
            return review_after
        return plan.next_review_at <= now


def _is_self_message(signal: AgentSignal) -> bool:
    message = signal.message
    if message is None:
        return False
    return bool(
        message.sender_id
        and message.self_id
        and message.sender_id == message.self_id
    )


def _has_high_priority_kind(
    events: list[HighPriorityEvent],
    *kinds: HighPriorityEventKind,
) -> bool:
    kind_set = set(kinds)
    return any(event.kind in kind_set for event in events)


def _enum_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    return value


def _signal_message_log_id(signal: AgentSignal) -> int | None:
    if signal.message is None:
        return None
    return signal.message.message_log_id


def _signal_trace_id(signal: AgentSignal) -> str:
    return str(signal.meta.get("trace_id") or "").strip()


def _decision_review_plan(decision: Any) -> ReviewPlan | None:
    plan = getattr(decision, "next_review_plan", None)
    if plan is not None:
        return plan
    return getattr(decision, "review_plan", None)
