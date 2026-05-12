"""Prompt components for the review overflow compression stage."""

from __future__ import annotations

from shinbot.agent.services.prompt_engine.files import register_prompt_files
from shinbot.agent.services.prompt_engine.schema import PromptStage

REVIEW_COMPRESSION_COMPONENT_IDS: dict[PromptStage, list[str]] = {
    PromptStage.SYSTEM_BASE: ["review.overflow_compression.system"],
    PromptStage.CONSTRAINTS: ["review.overflow_compression.constraints"],
    PromptStage.INSTRUCTIONS: ["review.overflow_compression.task"],
}


def register_review_compression_prompt_components(registry) -> None:
    """Register review overflow compression prompt components."""
    register_prompt_files(
        registry,
        package=__package__,
        prompt_ids=[
            "review.overflow_compression.system",
            "review.overflow_compression.constraints",
            "review.overflow_compression.task",
        ],
    )
