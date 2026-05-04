"""State storage boundary for AgentScheduler."""

from __future__ import annotations

from collections import defaultdict
from typing import Protocol

from shinbot.agent.scheduler.models import AgentState


class AgentStateStore(Protocol):
    """Storage surface for per-session scheduler state."""

    def get_state(self, session_id: str) -> AgentState:
        """Return the current scheduler state for one session."""

    def set_state(self, session_id: str, state: AgentState) -> None:
        """Persist the current scheduler state for one session."""


class InMemoryAgentStateStore:
    """In-memory state store used before Agent scheduler persistence exists."""

    def __init__(self) -> None:
        self._states: dict[str, AgentState] = defaultdict(lambda: AgentState.IDLE)

    def get_state(self, session_id: str) -> AgentState:
        return self._states[session_id]

    def set_state(self, session_id: str, state: AgentState) -> None:
        self._states[session_id] = state


__all__ = ["AgentStateStore", "InMemoryAgentStateStore"]
