"""Built-in prompt component registration for media inspection workflows."""

from __future__ import annotations

from shinbot.agent.media.config import (
    BUILTIN_MEDIA_INSPECTION_PROMPT,
    BUILTIN_MEDIA_INSPECTION_PROMPT_ID,
    BUILTIN_STICKER_SUMMARY_PROMPT,
    BUILTIN_STICKER_SUMMARY_PROMPT_ID,
)
from shinbot.agent.prompt_manager.schema import (
    PromptComponent,
    PromptComponentKind,
    PromptStage,
)


def register_media_prompt_components(registry) -> None:
    """Register built-in system prompts for media inspection and sticker summary."""

    registry.upsert_component(
        PromptComponent(
            id=BUILTIN_MEDIA_INSPECTION_PROMPT_ID,
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.STATIC_TEXT,
            priority=100,
            enabled=True,
            content=BUILTIN_MEDIA_INSPECTION_PROMPT,
            tags=["media", "inspection", "summary"],
            metadata={
                "builtin": True,
                "display_name": "Built-in Media Inspection Prompt",
                "description": "Default system prompt for repeated-image inspection and digest generation.",
            },
        )
    )
    registry.upsert_component(
        PromptComponent(
            id=BUILTIN_STICKER_SUMMARY_PROMPT_ID,
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.STATIC_TEXT,
            priority=100,
            enabled=True,
            content=BUILTIN_STICKER_SUMMARY_PROMPT,
            tags=["media", "sticker", "summary"],
            metadata={
                "builtin": True,
                "display_name": "Built-in Sticker Summary Prompt",
                "description": "Default system prompt for custom sticker and reaction-image summary.",
            },
        )
    )
