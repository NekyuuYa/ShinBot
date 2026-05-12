"""Prompt components for the review reply decision stage."""

from __future__ import annotations

from shinbot.agent.services.prompt_engine.files import PromptFileLoadConfig, register_prompt_files
from shinbot.agent.services.prompt_engine.schema import PromptStage

REVIEW_REPLY_COMPONENT_IDS: dict[PromptStage, list[str]] = {
    PromptStage.SYSTEM_BASE: ["review.reply_decision.system"],
    PromptStage.CONSTRAINTS: ["review.reply_decision.constraints"],
    PromptStage.INSTRUCTIONS: ["review.reply_decision.task"],
}


def register_review_reply_prompt_components(
    registry,
    *,
    prompt_file_config: PromptFileLoadConfig | None = None,
) -> None:
    """Register review reply decision prompt components."""
    register_prompt_files(
        registry,
        package=__package__,
        file_config=prompt_file_config,
        prompt_ids=[
            "review.reply_decision.system",
            "review.reply_decision.constraints",
            "review.reply_decision.task",
            "review.reply_decision.repair",
        ],
    )
