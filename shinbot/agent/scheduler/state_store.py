"""State storage boundary for AgentScheduler."""

from __future__ import annotations

from collections import defaultdict
from typing import Protocol

from shinbot.agent.scheduler.models import (
    ActiveChatState,
    ActiveReplyResume,
    AgentState,
    ReviewPlan,
)


class AgentStateStore(Protocol):
    """Storage surface for per-session scheduler state."""

    def get_state(self, session_id: str) -> AgentState:
        """Return the current scheduler state for one session."""

    def set_state(self, session_id: str, state: AgentState) -> None:
        """Persist the current scheduler state for one session."""

    def get_review_plan(self, session_id: str) -> ReviewPlan | None:
        """Return the current review plan for one session, if any."""

    def set_review_plan(self, plan: ReviewPlan) -> None:
        """Persist the current review plan for one session."""

    def list_due_review_plans(self, *, now: float, limit: int = 50) -> list[ReviewPlan]:
        """Return review plans whose scheduled review time has arrived."""

    def get_active_chat_state(self, session_id: str) -> ActiveChatState | None:
        """Return active chat interest state for one session, if any."""

    def set_active_chat_state(self, state: ActiveChatState) -> None:
        """Persist active chat interest state for one session."""

    def clear_active_chat_state(self, session_id: str) -> None:
        """Clear active chat interest state for one session."""

    def get_active_reply_resume(self, session_id: str) -> ActiveReplyResume | None:
        """Return the stored active-reply resume target for one session."""

    def set_active_reply_resume(self, resume: ActiveReplyResume) -> None:
        """Persist the active-reply resume target for one session."""

    def clear_active_reply_resume(self, session_id: str) -> None:
        """Clear the active-reply resume target for one session."""

    def list_session_ids(self, *, prefix: str | None = None) -> list[str]:
        """Return known session ids, optionally filtered by prefix."""


class InMemoryAgentStateStore:
    """In-memory state store used before Agent scheduler persistence exists."""

    def __init__(self) -> None:
        """Initialise empty in-memory state containers."""
        self._states: dict[str, AgentState] = defaultdict(lambda: AgentState.IDLE)
        self._review_plans: dict[str, ReviewPlan] = {}
        self._active_chat_states: dict[str, ActiveChatState] = {}
        self._active_reply_resumes: dict[str, ActiveReplyResume] = {}

    def get_state(self, session_id: str) -> AgentState:
        """Return the current scheduler state for a session.

        Sessions that have never been seen default to ``AgentState.IDLE``.
        """
        return self._states[session_id]

    def set_state(self, session_id: str, state: AgentState) -> None:
        """Persist the scheduler state for a session."""
        self._states[session_id] = state

    def get_review_plan(self, session_id: str) -> ReviewPlan | None:
        """Return the review plan for a session, or ``None`` if none exists."""
        return self._review_plans.get(session_id)

    def set_review_plan(self, plan: ReviewPlan) -> None:
        """Persist a review plan, keyed by its session id."""
        self._review_plans[plan.session_id] = plan

    def list_due_review_plans(self, *, now: float, limit: int = 50) -> list[ReviewPlan]:
        """Return review plans whose scheduled review time has arrived.

        Plans are sorted by ``next_review_at`` (ascending) then by session id.
        At most *limit* results are returned.
        """
        plans = [
            plan
            for plan in self._review_plans.values()
            if plan.next_review_at <= now
        ]
        plans.sort(key=lambda item: (item.next_review_at, item.session_id))
        return plans[:limit]

    def get_active_chat_state(self, session_id: str) -> ActiveChatState | None:
        """Return active chat interest state for a session, or ``None``."""
        return self._active_chat_states.get(session_id)

    def set_active_chat_state(self, state: ActiveChatState) -> None:
        """Persist active chat interest state, keyed by its session id."""
        self._active_chat_states[state.session_id] = state

    def clear_active_chat_state(self, session_id: str) -> None:
        """Remove active chat interest state for a session.

        This is a no-op if no state existed for the session.
        """
        self._active_chat_states.pop(session_id, None)

    def get_active_reply_resume(self, session_id: str) -> ActiveReplyResume | None:
        """Return the stored active-reply resume target for a session."""

        return self._active_reply_resumes.get(session_id)

    def set_active_reply_resume(self, resume: ActiveReplyResume) -> None:
        """Persist an active-reply resume target for a session."""

        self._active_reply_resumes[resume.session_id] = resume

    def clear_active_reply_resume(self, session_id: str) -> None:
        """Remove any active-reply resume target for a session."""

        self._active_reply_resumes.pop(session_id, None)

    def list_session_ids(self, *, prefix: str | None = None) -> list[str]:
        """Return all known session ids, optionally filtered by prefix.

        Session ids are collected from every internal store (states, review
        plans, active chat states) and returned in sorted order.
        """
        session_ids = set(self._states)
        session_ids.update(self._review_plans)
        session_ids.update(self._active_chat_states)
        session_ids.update(self._active_reply_resumes)
        items = sorted(session_ids)
        if prefix is None:
            return items
        return [session_id for session_id in items if session_id.startswith(prefix)]


__all__ = ["AgentStateStore", "InMemoryAgentStateStore"]
