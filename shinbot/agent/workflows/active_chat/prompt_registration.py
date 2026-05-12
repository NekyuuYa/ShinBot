"""Built-in prompt components for Agent active chat workflows."""

from __future__ import annotations

from shinbot.agent.services.prompt_engine.files import PromptFileLoadConfig, register_prompt_files
from shinbot.agent.services.prompt_engine.schema import PromptStage

ACTIVE_CHAT_PROMPT_COMPONENT_IDS_BY_STAGE: dict[str, dict[PromptStage, list[str]]] = {
    "fast_mode": {
        PromptStage.SYSTEM_BASE: ["active_chat.fast_mode.system"],
        PromptStage.CONSTRAINTS: ["active_chat.fast_mode.constraints"],
    },
}


def register_active_chat_prompt_components(
    registry,
    *,
    prompt_file_config: PromptFileLoadConfig | None = None,
) -> None:
    """Register built-in active chat workflow prompt components."""

    register_prompt_files(
        registry,
        package=__package__,
        file_config=prompt_file_config,
        prompt_ids=[
            "active_chat.fast_mode.system",
            "active_chat.fast_mode.constraints",
            "active_chat.handoff.overflow",
            "active_chat.handoff.digest",
            "active_chat.handoff.legacy",
            "active_chat.fast_mode.repair",
            "active_chat.fast_mode.conversation_summary",
        ],
    )


__all__ = [
    "ACTIVE_CHAT_PROMPT_COMPONENT_IDS_BY_STAGE",
    "register_active_chat_prompt_components",
]
