"""Prompt components for the review active chat bootstrap stage."""

from __future__ import annotations

from shinbot.agent.services.prompt_engine.schema import (
    PromptComponent,
    PromptComponentKind,
    PromptStage,
)

REVIEW_BOOTSTRAP_COMPONENT_IDS: dict[PromptStage, list[str]] = {
    PromptStage.SYSTEM_BASE: ["review.active_chat_bootstrap.system"],
    PromptStage.CONSTRAINTS: ["review.active_chat_bootstrap.constraints"],
}


def register_review_bootstrap_prompt_components(registry) -> None:
    """Register review bootstrap prompt components."""
    for component in _components():
        registry.upsert_component(component)


def _components() -> list[PromptComponent]:
    return [
        PromptComponent(
            id="review.active_chat_bootstrap.system",
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
                "display_name": "Review Active Chat Bootstrap System",
                "description": "Built-in prompt component for Agent review workflow stages.",
            },
        ),
        PromptComponent(
            id="review.active_chat_bootstrap.constraints",
            stage=PromptStage.CONSTRAINTS,
            kind=PromptComponentKind.STATIC_TEXT,
            priority=100,
            enabled=True,
            content=(
                "Choose only active chat bootstrap parameters after review/reply has "
                "finished. Do not send replies. Choose one semantic disposition only: "
                "exit_soon, watch, casual, engaged, or focused. Do not output numeric "
                "interest or decay parameters; ShinBot maps the disposition to internal "
                "active chat curves and applies delayed correction itself. Return the "
                "requested JSON object."
            ),
            tags=["review", "workflow"],
            metadata={
                "builtin": True,
                "display_name": "Review Active Chat Bootstrap Constraints",
                "description": "Built-in prompt component for Agent review workflow stages.",
            },
        ),
    ]
