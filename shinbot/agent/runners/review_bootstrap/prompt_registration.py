"""Prompt components for the review active chat bootstrap stage."""

from __future__ import annotations

from shinbot.agent.services.prompt_engine.files import register_prompt_files
from shinbot.agent.services.prompt_engine.schema import PromptStage

REVIEW_BOOTSTRAP_COMPONENT_IDS: dict[PromptStage, list[str]] = {
    PromptStage.SYSTEM_BASE: ["review.active_chat_bootstrap.system"],
    PromptStage.CONSTRAINTS: ["review.active_chat_bootstrap.constraints"],
    PromptStage.INSTRUCTIONS: ["review.active_chat_bootstrap.task"],
}


def register_review_bootstrap_prompt_components(registry) -> None:
    """Register review bootstrap prompt components."""
    register_prompt_files(
        registry,
        package=__package__,
        prompt_ids=[
            "review.active_chat_bootstrap.system",
            "review.active_chat_bootstrap.constraints",
            "review.active_chat_bootstrap.task",
        ],
    )
