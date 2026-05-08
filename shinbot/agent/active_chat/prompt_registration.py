"""Built-in prompt components for Agent active chat workflows."""

from __future__ import annotations

from shinbot.agent.prompt_manager.schema import (
    PromptComponent,
    PromptComponentKind,
    PromptStage,
)

ACTIVE_CHAT_PROMPT_COMPONENT_IDS_BY_STAGE: dict[str, dict[PromptStage, list[str]]] = {
    "fast_mode": {
        PromptStage.SYSTEM_BASE: ["active_chat.fast_mode.system"],
        PromptStage.CONSTRAINTS: ["active_chat.fast_mode.constraints"],
    },
}

_COMMON_SYSTEM_PROMPT = (
    "You are ShinBot's internal active chat fast-mode stage. You are already in "
    "an active chat session, so decide one immediate action for the supplied "
    "new message batch by using tools. Do not emit user-visible bare assistant text."
)


def register_active_chat_prompt_components(registry) -> None:
    """Register built-in active chat workflow prompt components."""

    for component in _active_chat_prompt_components():
        registry.upsert_component(component)


def _active_chat_prompt_components() -> list[PromptComponent]:
    return [
        _component(
            "active_chat.fast_mode.system",
            PromptStage.SYSTEM_BASE,
            _COMMON_SYSTEM_PROMPT,
            "Active Chat Fast Mode System",
        ),
        _component(
            "active_chat.fast_mode.constraints",
            PromptStage.CONSTRAINTS,
            "Active chat fast-mode tool rules: always use tools when tools are "
            "available. Use one or more send_reply tools when a visible reply is "
            "needed; multiple send_reply calls are sent in order. send_poke is a "
            "valid standalone lightweight interaction in active chat. Use no_reply "
            "when the batch is not worth responding to; set intensity to strong "
            "only when the conversation should cool down more quickly. Use "
            "request_think_mode with a reason when the batch needs deeper reasoning "
            "than fast mode. Use exit_active only when active chat should end now, "
            "and always include a clear reason. When several tools appear in one "
            "batch, ShinBot executes them in order and derives the interest change "
            "from the strongest semantic action.",
            "Active Chat Fast Mode Constraints",
        ),
    ]


def _component(
    component_id: str,
    stage: PromptStage,
    content: str,
    display_name: str,
) -> PromptComponent:
    return PromptComponent(
        id=component_id,
        stage=stage,
        kind=PromptComponentKind.STATIC_TEXT,
        priority=100,
        enabled=True,
        content=content,
        tags=["active_chat", "workflow"],
        metadata={
            "builtin": True,
            "display_name": display_name,
            "description": "Built-in prompt component for Agent active chat workflow stages.",
        },
    )


__all__ = [
    "ACTIVE_CHAT_PROMPT_COMPONENT_IDS_BY_STAGE",
    "register_active_chat_prompt_components",
]
