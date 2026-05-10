"""Prompt components for review block digest."""

from __future__ import annotations

from shinbot.agent.services.prompt_engine.schema import (
    PromptComponent,
    PromptComponentKind,
    PromptStage,
)

REVIEW_BLOCK_DIGEST_COMPONENT_IDS: dict[PromptStage, list[str]] = {
    PromptStage.SYSTEM_BASE: ["review.block_digest.system"],
    PromptStage.CONSTRAINTS: ["review.block_digest.constraints"],
}


def register_review_block_digest_prompt_components(registry) -> None:
    """Register review block digest prompt components."""
    for component in _components():
        registry.upsert_component(component)


def _components() -> list[PromptComponent]:
    return [
        PromptComponent(
            id="review.block_digest.system",
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.STATIC_TEXT,
            priority=100,
            enabled=True,
            content=(
                "You are an internal ShinBot Agent review block digest stage. "
                "Summarize only the supplied message block. Do not infer facts "
                "outside this block."
            ),
            tags=["review", "workflow", "summary"],
            metadata={"builtin": True, "display_name": "Review Block Digest System"},
        ),
        PromptComponent(
            id="review.block_digest.constraints",
            stage=PromptStage.CONSTRAINTS,
            kind=PromptComponentKind.STATIC_TEXT,
            priority=100,
            enabled=True,
            content=(
                "Return concise JSON with summary and reason. The summary should "
                "preserve topics, participant dynamics, unresolved questions, and "
                "context that later active_chat or reply_decision may need. Do not "
                "write a whole-run digest; this is only for one review block."
            ),
            tags=["review", "workflow", "summary"],
            metadata={"builtin": True, "display_name": "Review Block Digest Constraints"},
        ),
    ]


__all__ = [
    "REVIEW_BLOCK_DIGEST_COMPONENT_IDS",
    "register_review_block_digest_prompt_components",
]
