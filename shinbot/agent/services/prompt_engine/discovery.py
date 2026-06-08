"""Lightweight discovery for file-backed Agent prompt components."""

from __future__ import annotations

from pathlib import Path

from shinbot.agent.coordinators.review.factory import register_review_prompt_components
from shinbot.agent.services.context.prompt_registration import register_context_prompt_components
from shinbot.agent.services.identity.prompt_registration import (
    register_identity_file_prompt_components,
)
from shinbot.agent.services.media.prompt_registration import register_media_prompt_components
from shinbot.agent.services.prompt_engine.files import PromptFileLoadConfig
from shinbot.agent.services.prompt_engine.registry import PromptRegistry
from shinbot.agent.workflows.active_chat.prompt_registration import (
    register_active_chat_prompt_components,
)


def discover_file_backed_prompts(
    data_dir: Path | str,
    *,
    prompt_file_config: PromptFileLoadConfig | None = None,
) -> PromptRegistry:
    """Register file-backed prompts without constructing the full Agent runtime.

    Args:
        data_dir: ShinBot data directory used for runtime prompt copies.
        prompt_file_config: Optional prompt file loading config override.

    Returns:
        A prompt registry containing file-backed components and manifest entries.
    """

    config = prompt_file_config or PromptFileLoadConfig.from_data_dir(data_dir)
    registry = PromptRegistry()
    register_identity_file_prompt_components(
        registry,
        prompt_file_config=config,
    )
    register_context_prompt_components(
        registry,
        prompt_file_config=config,
    )
    register_media_prompt_components(
        registry,
        prompt_file_config=config,
    )
    register_review_prompt_components(
        registry,
        prompt_file_config=config,
    )
    register_active_chat_prompt_components(
        registry,
        prompt_file_config=config,
    )
    return registry


__all__ = ["discover_file_backed_prompts"]
