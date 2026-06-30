"""Built-in prompt component registration for context projections."""

from __future__ import annotations

from typing import Any

from shinbot.agent.services.context.projectors.headings import (
    ACTIVE_ALIAS_COMPONENT_ID,
    COMPRESSED_MEMORY_ALIAS_COMPONENT_ID,
    COMPRESSED_MEMORY_COMPONENT_ID,
    COMPRESSED_MEMORY_SOURCE_COMPONENT_ID,
    INACTIVE_ALIAS_COMPONENT_ID,
    LONG_TERM_MEMORY_COMPONENT_ID,
)
from shinbot.agent.services.prompt_engine.files import PromptFileLoadConfig, register_prompt_files


def register_context_prompt_components(
    registry: Any,
    *,
    prompt_file_config: PromptFileLoadConfig | None = None,
) -> None:
    """Register built-in context projection heading prompts."""

    register_prompt_files(
        registry,
        package=__package__,
        file_config=prompt_file_config,
        prompt_ids=[
            LONG_TERM_MEMORY_COMPONENT_ID,
            COMPRESSED_MEMORY_COMPONENT_ID,
            COMPRESSED_MEMORY_SOURCE_COMPONENT_ID,
            COMPRESSED_MEMORY_ALIAS_COMPONENT_ID,
            INACTIVE_ALIAS_COMPONENT_ID,
            ACTIVE_ALIAS_COMPONENT_ID,
        ],
    )
