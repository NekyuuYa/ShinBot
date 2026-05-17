"""Prompt components for the review scan stage."""

from __future__ import annotations

from shinbot.agent.services.prompt_engine.files import PromptFileLoadConfig, register_prompt_files
from shinbot.agent.services.prompt_engine.schema import PromptStage

REVIEW_SCAN_COMPONENT_IDS: dict[PromptStage, list[str]] = {
    PromptStage.SYSTEM_BASE: ["review.review_scan.system"],
    PromptStage.CONSTRAINTS: ["review.review_scan.constraints"],
    PromptStage.INSTRUCTIONS: ["review.review_scan.task"],
}


def register_review_scan_prompt_components(
    registry,
    *,
    prompt_file_config: PromptFileLoadConfig | None = None,
) -> None:
    """Register review scan prompt components."""
    register_prompt_files(
        registry,
        package=__package__,
        file_config=prompt_file_config,
        prompt_ids=[
            "review.review_scan.system",
            "review.review_scan.constraints",
            "review.review_scan.task",
        ],
    )
