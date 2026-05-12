"""Built-in prompt component registration for media inspection workflows."""

from __future__ import annotations

from shinbot.agent.services.media.config import (
    BUILTIN_MEDIA_INSPECTION_PROMPT_ID,
    BUILTIN_MEDIA_REANALYSIS_PROMPT_ID,
    BUILTIN_STICKER_SUMMARY_PROMPT_ID,
)
from shinbot.agent.services.prompt_engine.files import register_prompt_files


def register_media_prompt_components(registry) -> None:
    """Register built-in system prompts for media inspection and sticker summary."""

    register_prompt_files(
        registry,
        package=__package__,
        prompt_ids=[
            BUILTIN_MEDIA_INSPECTION_PROMPT_ID,
            BUILTIN_STICKER_SUMMARY_PROMPT_ID,
            BUILTIN_MEDIA_REANALYSIS_PROMPT_ID,
        ],
    )
