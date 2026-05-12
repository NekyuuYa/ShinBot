"""Prompt components for review block digest."""

from __future__ import annotations

from shinbot.agent.services.prompt_engine.files import PromptFileLoadConfig, register_prompt_files
from shinbot.agent.services.prompt_engine.schema import PromptStage

REVIEW_BLOCK_DIGEST_COMPONENT_IDS: dict[PromptStage, list[str]] = {
    PromptStage.SYSTEM_BASE: ["review.block_digest.system"],
    PromptStage.CONSTRAINTS: ["review.block_digest.constraints"],
    PromptStage.INSTRUCTIONS: ["review.block_digest.task"],
}


def register_review_block_digest_prompt_components(
    registry,
    *,
    prompt_file_config: PromptFileLoadConfig | None = None,
) -> None:
    """Register review block digest prompt components."""
    register_prompt_files(
        registry,
        package=__package__,
        file_config=prompt_file_config,
        prompt_ids=[
            "review.block_digest.system",
            "review.block_digest.constraints",
            "review.block_digest.task",
        ],
    )


__all__ = [
    "REVIEW_BLOCK_DIGEST_COMPONENT_IDS",
    "register_review_block_digest_prompt_components",
]
