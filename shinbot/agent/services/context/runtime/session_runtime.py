"""Runtime wrapper for per-session context state storage."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from shinbot.agent.services.context.state.state_store import ContextSessionState, ContextStateStore


@dataclass(slots=True)
class ContextSessionRuntime:
    """Own the in-memory session-state cache and persistence boundary."""

    state_store: ContextStateStore
    session_states: dict[str, ContextSessionState] = field(default_factory=dict)

    @classmethod
    def from_data_dir(cls, data_dir: Path | str | None = "data") -> ContextSessionRuntime:
        """Create a runtime instance backed by the given data directory.

        Args:
            data_dir: Path to the data directory for state persistence.

        Returns:
            A new ContextSessionRuntime instance.
        """
        return cls(state_store=ContextStateStore(data_dir=data_dir))

    def get_state(self, session_id: str) -> ContextSessionState:
        """Retrieve or lazily load the session state for the given session.

        Args:
            session_id: Conversation session identifier.

        Returns:
            The ContextSessionState, loaded from disk or created fresh.
        """
        state = self.session_states.get(session_id)
        if state is not None:
            return state

        loaded = self.state_store.load(session_id)
        state = loaded or ContextSessionState(session_id=session_id)
        if not state.session_id:
            state.session_id = session_id
        if not state.alias_table.session_id:
            state.alias_table.session_id = session_id
        self.session_states[session_id] = state
        return state

    def save(self, session_id: str) -> bool:
        """Persist the in-memory session state to disk.

        Args:
            session_id: Conversation session identifier.

        Returns:
            True if the state was saved, False if not found.
        """
        state = self.session_states.get(session_id)
        if state is None:
            return False
        self.state_store.save(state)
        return True

    def delete(self, session_id: str) -> bool:
        """Delete in-memory and persisted context state for a session.

        Args:
            session_id: Conversation session identifier.

        Returns:
            True if cached state existed, otherwise False.
        """
        removed = self.session_states.pop(session_id, None)
        self.state_store.delete(session_id)
        return removed is not None
