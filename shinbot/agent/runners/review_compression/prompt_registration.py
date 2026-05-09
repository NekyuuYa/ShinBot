"""Prompt components for the review overflow compression stage."""

from __future__ import annotations

from shinbot.agent.prompt_engine.schema import (
    PromptComponent,
    PromptComponentKind,
    PromptStage,
)

REVIEW_COMPRESSION_COMPONENT_IDS: dict[PromptStage, list[str]] = {
    PromptStage.SYSTEM_BASE: ["review.overflow_compression.system"],
    PromptStage.CONSTRAINTS: ["review.overflow_compression.constraints"],
}


def register_review_compression_prompt_components(registry) -> None:
    """Register review overflow compression prompt components."""
    for component in _components():
        registry.upsert_component(component)


def _components() -> list[PromptComponent]:
    return [
        PromptComponent(
            id="review.overflow_compression.system",
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.STATIC_TEXT,
            priority=100,
            enabled=True,
            content=(
                "You are an internal ShinBot Agent review workflow stage. Follow the stage "
                "contract exactly. Do not produce user-visible bare assistant text unless the "
                "stage explicitly asks for structured JSON fallback."
            ),
            tags=["review", "workflow"],
            metadata={
                "builtin": True,
                "display_name": "Review Overflow Compression System",
                "description": "Built-in prompt component for Agent review workflow stages.",
            },
        ),
        PromptComponent(
            id="review.overflow_compression.constraints",
            stage=PromptStage.CONSTRAINTS,
            kind=PromptComponentKind.STATIC_TEXT,
            priority=100,
            enabled=True,
            content=(
                "Compress only older overflow messages. Preserve unresolved topics, "
                "useful facts, and message ids that may deserve later reply review. "
                "Return the requested JSON object."
            ),
            tags=["review", "workflow"],
            metadata={
                "builtin": True,
                "display_name": "Review Overflow Compression Constraints",
                "description": "Built-in prompt component for Agent review workflow stages.",
            },
        ),
    ]
