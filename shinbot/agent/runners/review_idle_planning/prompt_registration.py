"""Prompt components for the active chat idle review planning stage."""

from __future__ import annotations

from shinbot.agent.services.prompt_engine.files import PromptFileLoadConfig, register_prompt_files
from shinbot.agent.services.prompt_engine.schema import PromptStage

IDLE_REVIEW_PLANNING_COMPONENT_IDS: dict[PromptStage, list[str]] = {
    PromptStage.SYSTEM_BASE: ["review.idle_review_planning.system"],
    PromptStage.CONSTRAINTS: ["review.idle_review_planning.constraints"],
    PromptStage.INSTRUCTIONS: ["review.idle_review_planning.task"],
}


def register_idle_review_planning_prompt_components(
    registry,
    *,
    prompt_file_config: PromptFileLoadConfig | None = None,
) -> None:
    """Register active chat idle review planning prompt components."""
    register_prompt_files(
        registry,
        package=__package__,
        file_config=prompt_file_config,
        prompt_ids=[
            "review.idle_review_planning.system",
            "review.idle_review_planning.constraints",
            "review.idle_review_planning.task",
        ],
    )
