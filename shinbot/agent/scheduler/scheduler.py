"""Agent-internal scheduler entrypoint."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from shinbot.agent.scheduler.active_chat_policy import (
    ActiveChatPolicy,
    DefaultActiveChatPolicy,
)
from shinbot.agent.scheduler.inbox import AgentInbox, InMemoryAgentInbox
from shinbot.agent.scheduler.models import (
    ActiveChatState,
    ActiveChatTickDecision,
    ActiveReplyCompletionDecision,
    AgentScheduleDecision,
    AgentState,
    HighPriorityEvent,
    ReviewCompletionDecision,
    ReviewDueDecision,
    ReviewPlan,
    UnreadMessage,
)
from shinbot.agent.scheduler.priority_policy import (
    DefaultPriorityPolicy,
    PriorityPolicy,
    PriorityPolicyConfig,
)
from shinbot.agent.scheduler.review_policy import DefaultReviewPolicy, ReviewPolicy
from shinbot.agent.scheduler.state_store import AgentStateStore, InMemoryAgentStateStore
from shinbot.agent.scheduler.workflow_dispatcher import AgentWorkflowDispatcher

if TYPE_CHECKING:
    from shinbot.core.dispatch.dispatchers import AgentEntrySignal

ResponseProfileResolver = Callable[["AgentEntrySignal"], str]


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


class AgentScheduler:
    """Accepts Agent entry signals and decides which Agent workflow should run."""

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
        self._now = now or time.time

    async def accept_signal(self, signal: AgentEntrySignal) -> AgentScheduleDecision:
        """Accept one message signal from core and decide scheduler-side action."""
        if signal.message_log_id is None:
            return AgentScheduleDecision(
                accepted=False,
                state=self._state_store.get_state(signal.session_id),
                skipped_reason="missing_message_log_id",
            )
        if signal.already_handled:
            return AgentScheduleDecision(
                accepted=False,
                state=self._state_store.get_state(signal.session_id),
                skipped_reason="already_handled",
            )
        if signal.is_stopped:
            return AgentScheduleDecision(
                accepted=False,
                state=self._state_store.get_state(signal.session_id),
                skipped_reason="stopped",
            )

        now = self._now()
        self._ensure_review_plan(signal.session_id, now)
        unread = UnreadMessage(
            session_id=signal.session_id,
            message_log_id=signal.message_log_id,
            sender_id=signal.sender_id,
            created_at=now,
        )
        self._inbox.add_unread(unread)

        priority_decision = self._priority_policy.evaluate(
            signal,
            now=now,
            inbox=self._inbox,
        )
        high_priority_events = priority_decision.events
        if high_priority_events:
            self._inbox.add_high_priority_events(high_priority_events)

        if priority_decision.should_start_active_reply and self._workflow_dispatcher is not None:
            self._state_store.set_state(signal.session_id, AgentState.ACTIVE_REPLY)
            await self._workflow_dispatcher.run_active_reply(
                session_id=signal.session_id,
                message_log_id=signal.message_log_id,
                sender_id=signal.sender_id,
                response_profile=self._response_profile_resolver(signal),
                is_mentioned=signal.is_mentioned,
                is_reply_to_bot=signal.is_reply_to_bot,
                self_platform_id=signal.self_id,
                events=high_priority_events,
            )
            return AgentScheduleDecision(
                accepted=True,
                state=self._state_store.get_state(signal.session_id),
                unread_message=unread,
                high_priority_events=high_priority_events,
                active_reply_started=True,
            )

        return AgentScheduleDecision(
            accepted=True,
            state=self._state_store.get_state(signal.session_id),
            unread_message=unread,
            high_priority_events=high_priority_events,
            active_reply_started=False,
        )

    def unread_messages(self, session_id: str) -> list[UnreadMessage]:
        """Return unread messages known to AgentScheduler for one session."""
        return self._inbox.list_unread(session_id)

    def high_priority_events(self, session_id: str) -> list[HighPriorityEvent]:
        """Return high-priority events known to AgentScheduler for one session."""
        return self._inbox.list_high_priority_events(session_id)

    def state_for(self, session_id: str) -> AgentState:
        """Return current scheduler state for one session."""
        return self._state_store.get_state(session_id)

    def review_plan_for(self, session_id: str) -> ReviewPlan | None:
        """Return the current review plan for one session, if any."""
        return self._state_store.get_review_plan(session_id)

    def active_chat_state_for(self, session_id: str) -> ActiveChatState | None:
        """Return current active chat interest state for one session, if any."""
        return self._state_store.get_active_chat_state(session_id)

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

        high_priority_events = self._inbox.list_high_priority_events(session_id)
        if high_priority_events:
            self._state_store.set_state(session_id, AgentState.ACTIVE_REPLY)
            return ReviewDueDecision(
                session_id=session_id,
                state=AgentState.ACTIVE_REPLY,
                review_plan=plan,
                high_priority_events=high_priority_events,
                active_reply_pending=True,
            )

        self._state_store.set_state(session_id, AgentState.REVIEW)
        return ReviewDueDecision(
            session_id=session_id,
            state=AgentState.REVIEW,
            review_plan=plan,
            review_started=True,
        )

    async def run_due_review(
        self,
        session_id: str,
        *,
        now: float | None = None,
    ) -> ReviewDueDecision:
        """Prepare and dispatch a due review workflow when no interrupt is pending."""
        decision = self.prepare_due_review(session_id, now=now)
        if (
            not decision.review_started
            or decision.review_plan is None
            or self._workflow_dispatcher is None
        ):
            return decision

        await self._workflow_dispatcher.run_review(
            session_id=session_id,
            review_plan=decision.review_plan,
            unread_messages=self._inbox.list_unread(session_id),
        )
        decision.review_workflow_started = True
        return decision

    async def complete_active_reply(
        self,
        session_id: str,
        *,
        review_after: bool | None = None,
        now: float | None = None,
    ) -> ActiveReplyCompletionDecision:
        """Complete active reply and decide whether to resume review or return idle."""
        current_state = self._state_store.get_state(session_id)
        if current_state != AgentState.ACTIVE_REPLY:
            return ActiveReplyCompletionDecision(
                session_id=session_id,
                state=current_state,
                skipped_reason="not_active_reply",
            )

        handled_events = self._inbox.mark_high_priority_events_handled(session_id)
        plan = self._state_store.get_review_plan(session_id)
        checked_at = self._now() if now is None else now
        should_review = self._should_review_after_active_reply(
            plan=plan,
            review_after=review_after,
            now=checked_at,
        )
        if not should_review or plan is None:
            self._state_store.set_state(session_id, AgentState.IDLE)
            return ActiveReplyCompletionDecision(
                session_id=session_id,
                state=AgentState.IDLE,
                review_plan=plan,
                handled_high_priority_events=handled_events,
                returned_to_idle=True,
                skipped_reason="missing_review_plan" if plan is None else "review_not_requested",
            )

        self._state_store.set_state(session_id, AgentState.REVIEW)
        decision = ActiveReplyCompletionDecision(
            session_id=session_id,
            state=AgentState.REVIEW,
            review_plan=plan,
            handled_high_priority_events=handled_events,
            review_started=True,
        )
        if self._workflow_dispatcher is None:
            return decision

        await self._workflow_dispatcher.run_review(
            session_id=session_id,
            review_plan=plan,
            unread_messages=self._inbox.list_unread(session_id),
        )
        decision.review_workflow_started = True
        return decision

    def complete_review(
        self,
        session_id: str,
        *,
        enter_active_chat: bool = False,
        active_chat_initial_interest: float | None = None,
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
            )
            self._state_store.set_state(session_id, AgentState.ACTIVE_CHAT)
            self._state_store.set_active_chat_state(active_chat_state)
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
        self._state_store.set_state(session_id, AgentState.IDLE)
        self._state_store.clear_active_chat_state(session_id)
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

        decayed_state = self._active_chat_policy.decay(active_chat_state, now=checked_at)
        self._state_store.set_active_chat_state(decayed_state)
        if not self._active_chat_policy.should_return_idle(decayed_state):
            return ActiveChatTickDecision(
                session_id=session_id,
                state=AgentState.ACTIVE_CHAT,
                active_chat_state=decayed_state,
            )

        plan = next_review_plan or self._review_policy.plan_after_review(
            session_id=session_id,
            now=checked_at,
            previous_plan=self._state_store.get_review_plan(session_id),
        )
        self._state_store.set_state(session_id, AgentState.IDLE)
        self._state_store.clear_active_chat_state(session_id)
        self._state_store.set_review_plan(plan)
        return ActiveChatTickDecision(
            session_id=session_id,
            state=AgentState.IDLE,
            active_chat_state=decayed_state,
            next_review_plan=plan,
            returned_to_idle=True,
        )

    def _ensure_review_plan(self, session_id: str, now: float) -> None:
        if self._state_store.get_review_plan(session_id) is not None:
            return
        self._state_store.set_review_plan(
            self._review_policy.initial_plan(session_id=session_id, now=now)
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
