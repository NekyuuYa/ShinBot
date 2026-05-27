"""Long-term memory projection interfaces."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from shinbot.agent.services.context.projectors.headings import LONG_TERM_MEMORY_HEADING
from shinbot.agent.services.context.projectors.projection import PromptMemoryProjectionRequest


@dataclass(slots=True)
class LongTermMemoryItem:
    """Stable semantic memory item projected ahead of session context."""

    text: str
    source: str = ""
    score: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


class LongTermMemoryProvider(Protocol):
    """Retrieve long-term memories relevant to the current prompt request."""

    def retrieve(self, request: PromptMemoryProjectionRequest) -> list[LongTermMemoryItem]:
        """Retrieve long-term memories relevant to the current request.

        Args:
            request: Projection request describing the retrieval context.

        Returns:
            List of relevant long-term memory items.
        """
        ...


@dataclass(slots=True)
class NoopLongTermMemoryProvider:
    """No-op provider used until semantic memory retrieval is implemented."""

    def retrieve(self, request: PromptMemoryProjectionRequest) -> list[LongTermMemoryItem]:
        """Return an empty list (no-op implementation).

        Args:
            request: Projection request (unused).

        Returns:
            An empty list.
        """
        return []


@dataclass(slots=True)
class LongTermMemoryProjector:
    """Project long-term memory items into prompt context messages."""

    def build_messages(self, memories: list[LongTermMemoryItem]) -> list[dict[str, Any]]:
        """Project long-term memory items into prompt user messages.

        Args:
            memories: List of long-term memory items.

        Returns:
            A single user-role message with bulleted memory lines,
            or an empty list if no memories have content.
        """
        lines = [item.text.strip() for item in memories if item.text.strip()]
        if not lines:
            return []
        return [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": LONG_TERM_MEMORY_HEADING + "\n" + "\n".join(
                            f"- {line}" for line in lines
                        ),
                    }
                ],
            }
        ]
