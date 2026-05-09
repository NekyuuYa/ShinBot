"""Prompt components for the review scan stage."""

from __future__ import annotations

from shinbot.agent.services.prompt_engine.schema import (
    PromptComponent,
    PromptComponentKind,
    PromptStage,
)

REVIEW_SCAN_COMPONENT_IDS: dict[PromptStage, list[str]] = {
    PromptStage.SYSTEM_BASE: ["review.review_scan.system"],
    PromptStage.CONSTRAINTS: ["review.review_scan.constraints"],
}


def register_review_scan_prompt_components(registry) -> None:
    """Register review scan prompt components."""
    for component in _components():
        registry.upsert_component(component)


def _components() -> list[PromptComponent]:
    return [
        PromptComponent(
            id="review.review_scan.system",
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
                "display_name": "Review Scan System",
                "description": "Built-in prompt component for Agent review workflow stages.",
            },
        ),
        PromptComponent(
            id="review.review_scan.constraints",
            stage=PromptStage.CONSTRAINTS,
            kind=PromptComponentKind.STATIC_TEXT,
            priority=100,
            enabled=True,
            content=(
                "Select message_log ids that may deserve a reply or closer local "
                "decision. Prefer high-signal messages and avoid over-selecting. "
                "Do not decide reply text or active chat parameters. Return the "
                "requested JSON object."
            ),
            tags=["review", "workflow"],
            metadata={
                "builtin": True,
                "display_name": "Review Scan Constraints",
                "description": "Built-in prompt component for Agent review workflow stages.",
            },
        ),
    ]
