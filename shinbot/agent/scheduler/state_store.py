"""State storage boundary for AgentScheduler."""

from __future__ import annotations

from collections import defaultdict
from typing import Protocol

from shinbot.agent.scheduler.models import AgentState, ReviewPlan


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


class InMemoryAgentStateStore:
    """In-memory state store used before Agent scheduler persistence exists."""

    def __init__(self) -> None:
        self._states: dict[str, AgentState] = defaultdict(lambda: AgentState.IDLE)
        self._review_plans: dict[str, ReviewPlan] = {}

    def get_state(self, session_id: str) -> AgentState:
        return self._states[session_id]

    def set_state(self, session_id: str, state: AgentState) -> None:
        self._states[session_id] = state

    def get_review_plan(self, session_id: str) -> ReviewPlan | None:
        return self._review_plans.get(session_id)

    def set_review_plan(self, plan: ReviewPlan) -> None:
        self._review_plans[plan.session_id] = plan

    def list_due_review_plans(self, *, now: float, limit: int = 50) -> list[ReviewPlan]:
        plans = [
            plan
            for plan in self._review_plans.values()
            if plan.next_review_at <= now
        ]
        plans.sort(key=lambda item: (item.next_review_at, item.session_id))
        return plans[:limit]


__all__ = ["AgentStateStore", "InMemoryAgentStateStore"]
