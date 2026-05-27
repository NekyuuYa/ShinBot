"""Agent-internal scheduler entrypoint."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
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
    AgentScheduleDecision,
    AgentState,
    HighPriorityEvent,
    HighPriorityEventKind,
    ReviewCompletionDecision,
    ReviewDueDecision,
    ReviewPlan,
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
from shinbot.agent.signals import AgentSignal, AgentSignalKind
from shinbot.utils.logger import format_log_event, get_logger

logger = get_logger(__name__, source="agent:scheduler", color="magenta")

ResponseProfileResolver = Callable[[AgentSignal], str]
AgentSignalDecision = (
    AgentScheduleDecision
    | ReviewDueDecision
    | ActiveChatTickDecision
    | ActiveChatBootstrapApplyDecision
)


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
    """Accepts unified Agent signals and decides which Agent workflow should run."""

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

    async def accept_signal(self, signal: AgentSignal) -> AgentSignalDecision | None:
        """Accept one unified Agent signal and decide scheduler-side action."""
        self._log_signal_entry(signal)
        if signal.kind == AgentSignalKind.REVIEW_DUE:
            decision = await self._accept_review_due_signal(signal)
        elif signal.kind == AgentSignalKind.ACTIVE_CHAT_TICK:
            decision = await self._accept_active_chat_tick_signal(signal)
        elif signal.kind == AgentSignalKind.ACTIVE_CHAT_BOOTSTRAP:
            decision = self._accept_active_chat_bootstrap_signal(signal)
        else:
            decision = await self._accept_message_signal(signal)
        self._log_signal_decision(signal, decision)
        return decision

    async def _accept_message_signal(self, signal: AgentSignal) -> AgentScheduleDecision:
        """Accept one message signal and decide scheduler-side action."""
        message = signal.message
        if signal.kind != AgentSignalKind.MESSAGE or message is None:
            return AgentScheduleDecision(
                accepted=False,
                state=self._state_store.get_state(signal.session_id),
                skipped_reason="not_message_signal",
            )
        if message.message_log_id is None:
            return AgentScheduleDecision(
                accepted=False,
                state=self._state_store.get_state(signal.session_id),
                skipped_reason="missing_message_log_id",
            )
        if message.already_handled:
            return AgentScheduleDecision(
                accepted=False,
                state=self._state_store.get_state(signal.session_id),
                skipped_reason="already_handled",
            )
        if message.is_stopped:
            return AgentScheduleDecision(
                accepted=False,
                state=self._state_store.get_state(signal.session_id),
                skipped_reason="stopped",
            )
        if _is_self_message(signal):
            return AgentScheduleDecision(
                accepted=False,
                state=self._state_store.get_state(signal.session_id),
                skipped_reason="self_message",
            )

        now = self._now()
        initial_state = self._state_store.get_state(signal.session_id)
        self._ensure_review_plan(signal.session_id, now)
        response_profile = self._response_profile_resolver(signal)
        unread = UnreadMessage(
            session_id=signal.session_id,
            message_log_id=message.message_log_id,
            sender_id=message.sender_id,
            created_at=now,
            response_profile=response_profile,
            is_mentioned=message.is_mentioned,
            is_reply_to_bot=message.is_reply_to_bot,
            is_mention_to_other=message.is_mention_to_other,
            is_poke_to_bot=message.is_poke_to_bot,
            is_poke_to_other=message.is_poke_to_other,
            self_platform_id=message.self_id,
            trace_id=_signal_trace_id(signal),
        )
        self._unread_metadata[(unread.session_id, unread.message_log_id)] = unread
        self._inbox.add_unread(unread)

        if initial_state == AgentState.ACTIVE_CHAT:
            priority_decision = None
            high_priority_events = []
        else:
            priority_decision = self._priority_policy.evaluate(
                signal,
                now=now,
                inbox=self._inbox,
            )
            high_priority_events = priority_decision.events
            if high_priority_events:
                self._inbox.add_high_priority_events(high_priority_events)

        if (
            priority_decision is not None
            and priority_decision.should_start_active_reply
            and self._workflow_dispatcher is not None
        ):
            self._state_store.set_state(signal.session_id, AgentState.ACTIVE_REPLY)
            self._cancel_active_chat_timer(signal.session_id)
            await self._workflow_dispatcher.run_active_reply(
                session_id=signal.session_id,
                message_log_id=message.message_log_id,
                sender_id=message.sender_id,
                response_profile=response_profile,
                is_mentioned=message.is_mentioned,
                is_reply_to_bot=message.is_reply_to_bot,
                self_platform_id=message.self_id,
                events=high_priority_events,
            )
            return AgentScheduleDecision(
                accepted=True,
                state=self._state_store.get_state(signal.session_id),
                unread_message=unread,
                high_priority_events=high_priority_events,
                active_reply_started=True,
            )

        active_chat_state = None
        active_chat_observed = False
        active_chat_workflow_notified = False
        if initial_state == AgentState.ACTIVE_CHAT:
            active_chat_state = self._observe_active_chat_message(
                session_id=signal.session_id,
                now=now,
                is_from_bot=False,
                is_mentioned=message.is_mentioned,
                is_reply_to_bot=message.is_reply_to_bot,
                is_mention_to_other=message.is_mention_to_other,
                is_poke_to_bot=message.is_poke_to_bot,
                is_poke_to_other=message.is_poke_to_other,
            )
            self._start_active_chat_timer(signal.session_id)
            active_chat_observed = True
            if self._workflow_dispatcher is not None:
                await self._workflow_dispatcher.notify_active_chat_message(
                    session_id=signal.session_id,
                    message_log_id=message.message_log_id,
                    sender_id=message.sender_id,
                    response_profile=response_profile,
                    is_mentioned=message.is_mentioned,
                    is_reply_to_bot=message.is_reply_to_bot,
                    is_mention_to_other=message.is_mention_to_other,
                    is_poke_to_bot=message.is_poke_to_bot,
                    is_poke_to_other=message.is_poke_to_other,
                    self_platform_id=message.self_id,
                    active_chat_state=active_chat_state,
                    trace_id=unread.trace_id,
                )
                active_chat_workflow_notified = True

        return AgentScheduleDecision(
            accepted=True,
            state=self._state_store.get_state(signal.session_id),
            unread_message=unread,
            active_chat_state=active_chat_state,
            high_priority_events=high_priority_events,
            active_chat_observed=active_chat_observed,
            active_chat_workflow_notified=active_chat_workflow_notified,
            active_reply_started=False,
        )

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

        plan = next_review_plan or self._review_policy.plan_after_review(
            session_id=session_id,
            now=checked_at,
            previous_plan=self._state_store.get_review_plan(session_id),
        )
        self._state_store.set_state(session_id, AgentState.IDLE)
        self._state_store.clear_active_chat_state(session_id)
        self._state_store.set_review_plan(plan)
        logger.info(
            format_log_event(
                "agent.active_chat.exit",
                cause="interest_adjustment",
                session_id=session_id,
                force_exit=force_exit,
                delta=f"{delta:.2f}",
                interest=f"{adjusted_state.interest_value:.2f}",
                reason=reason,
                next_review_at=f"{plan.next_review_at:.2f}",
                next_review_after_seconds=f"{max(0.0, plan.next_review_at - checked_at):.2f}",
            )
        )
        self._stop_active_chat_runtime(session_id)
        return ActiveChatInterestAdjustDecision(
            session_id=session_id,
            state=AgentState.IDLE,
            active_chat_state=adjusted_state,
            next_review_plan=plan,
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

        if current_state == AgentState.REVIEW:
            return ReviewDueDecision(
                session_id=session_id,
                state=current_state,
                review_plan=plan,
                skipped_reason="review_already_running",
            )
        if current_state == AgentState.ACTIVE_REPLY:
            return ReviewDueDecision(
                session_id=session_id,
                state=current_state,
                review_plan=plan,
                skipped_reason="active_reply_running",
            )
        if current_state == AgentState.ACTIVE_CHAT:
            return ReviewDueDecision(
                session_id=session_id,
                state=current_state,
                review_plan=plan,
                skipped_reason="active_chat_running",
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
        checked_at = self._now() if now is None else now
        decision = self.prepare_due_review(session_id, now=checked_at)
        if (
            decision.active_reply_pending
            and decision.high_priority_events
            and self._workflow_dispatcher is not None
        ):
            event = decision.high_priority_events[0]
            unread = self._unread_metadata.get((session_id, event.message_log_id))
            await self._workflow_dispatcher.run_active_reply(
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
                self_platform_id=unread.self_platform_id if unread is not None else "",
                events=decision.high_priority_events,
            )
            decision.state = self._state_store.get_state(session_id)
            if (
                decision.state == AgentState.IDLE
                and decision.review_plan is not None
                and decision.review_plan.next_review_at <= checked_at
            ):
                self._state_store.set_state(session_id, AgentState.REVIEW)
                decision.state = AgentState.REVIEW
                decision.review_started = True
                await self._workflow_dispatcher.run_review(
                    session_id=session_id,
                    review_plan=decision.review_plan,
                    unread_messages=self.unread_messages(session_id),
                )
                decision.review_workflow_started = True
                decision.state = self._state_store.get_state(session_id)
            return decision
        if (
            not decision.review_started
            or decision.review_plan is None
            or self._workflow_dispatcher is None
        ):
            return decision

        await self._workflow_dispatcher.run_review(
            session_id=session_id,
            review_plan=decision.review_plan,
            unread_messages=self.unread_messages(session_id),
        )
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
            self._cancel_active_chat_timer(session_id)
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
            unread_messages=self.unread_messages(session_id),
        )
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
            self._state_store.set_state(session_id, AgentState.ACTIVE_CHAT)
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
        self._state_store.set_state(session_id, AgentState.IDLE)
        self._state_store.clear_active_chat_state(session_id)
        self._state_store.set_review_plan(plan)
        self._cancel_active_chat_timer(session_id)
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

        plan = next_review_plan or self._review_policy.plan_after_review(
            session_id=session_id,
            now=checked_at,
            previous_plan=self._state_store.get_review_plan(session_id),
        )
        self._state_store.set_state(session_id, AgentState.IDLE)
        self._state_store.clear_active_chat_state(session_id)
        self._state_store.set_review_plan(plan)
        logger.info(
            format_log_event(
                "agent.active_chat.exit",
                cause="decay_tick",
                session_id=session_id,
                interest=f"{decayed_state.interest_value:.2f}",
                tick_count=decayed_state.tick_count,
                next_review_at=f"{plan.next_review_at:.2f}",
                next_review_after_seconds=f"{max(0.0, plan.next_review_at - checked_at):.2f}",
            )
        )
        self._stop_active_chat_runtime(session_id)
        return ActiveChatTickDecision(
            session_id=session_id,
            state=AgentState.IDLE,
            active_chat_state=decayed_state,
            next_review_plan=plan,
            returned_to_idle=True,
        )

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

        plan = self._review_policy.plan_after_review(
            session_id=session_id,
            now=checked_at,
            previous_plan=self._state_store.get_review_plan(session_id),
        )
        self._state_store.set_state(session_id, AgentState.IDLE)
        self._state_store.clear_active_chat_state(session_id)
        self._state_store.set_review_plan(plan)
        self._stop_active_chat_runtime(session_id)
        return ActiveChatBootstrapApplyDecision(
            session_id=session_id,
            state=AgentState.IDLE,
            active_chat_state=corrected_state,
            next_review_plan=plan,
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

    @staticmethod
    def _timer_checked_at(signal: AgentSignal) -> float:
        if signal.timer is not None and signal.timer.due_at is not None:
            return signal.timer.due_at
        return signal.occurred_at

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
