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

    def retrieve(self, request: PromptMemoryProjectionRequest) -> list[LongTermMemoryItem]: ...


@dataclass(slots=True)
class NoopLongTermMemoryProvider:
    """No-op provider used until semantic memory retrieval is implemented."""

    def retrieve(self, request: PromptMemoryProjectionRequest) -> list[LongTermMemoryItem]:
        return []


@dataclass(slots=True)
class LongTermMemoryProjector:
    """Project long-term memory items into prompt context messages."""

    def build_messages(self, memories: list[LongTermMemoryItem]) -> list[dict[str, Any]]:
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
