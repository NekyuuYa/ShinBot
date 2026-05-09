"""Prompt components for the review reply decision stage."""

from __future__ import annotations

from shinbot.agent.services.prompt_engine.schema import (
    PromptComponent,
    PromptComponentKind,
    PromptStage,
)

REVIEW_REPLY_COMPONENT_IDS: dict[PromptStage, list[str]] = {
    PromptStage.SYSTEM_BASE: ["review.reply_decision.system"],
    PromptStage.CONSTRAINTS: ["review.reply_decision.constraints"],
}


def register_review_reply_prompt_components(registry) -> None:
    """Register review reply decision prompt components."""
    for component in _components():
        registry.upsert_component(component)


def _components() -> list[PromptComponent]:
    return [
        PromptComponent(
            id="review.reply_decision.system",
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.STATIC_TEXT,
            priority=100,
            enabled=True,
            content=(
                "You are an internal ShinBot Agent review workflow stage. Follow the stage "
                "contract exactly. Do not produce user-visible bare assistant text unless the "
                "stage explicitly asks for structured JSON fallback."
            ),
            tags=["review", "workflow"],
            metadata={
                "builtin": True,
                "display_name": "Review Reply Decision System",
                "description": "Built-in prompt component for Agent review workflow stages.",
            },
        ),
        PromptComponent(
            id="review.reply_decision.constraints",
            stage=PromptStage.CONSTRAINTS,
            kind=PromptComponentKind.STATIC_TEXT,
            priority=100,
            enabled=True,
            content=(
                "Reply decision tool rules: call no_reply when no response is needed. "
                "When response is needed, call one or more send_reply tools in the "
                "exact order messages should be sent. The candidate_message_ids are "
                "the core messages under reply consideration; surrounding messages "
                "are only context. The first send_reply MUST include "
                "quote_message_log_id pointing to the specific core message being "
                "answered, because review replies may refer to older timeline points. "
                "Later send_reply calls may omit quote_message_log_id when they "
                "continue the same reply sequence. send_poke is optional and may "
                "appear anywhere in the same tool-call batch, but it only makes sense "
                "when accompanied by at least one send_reply. Bare assistant text is "
                "invalid in this stage; always use send_reply/no_reply, with optional "
                "send_poke. Do not decide or emit active chat parameters in this stage."
            ),
            tags=["review", "workflow"],
            metadata={
                "builtin": True,
                "display_name": "Review Reply Decision Constraints",
                "description": "Built-in prompt component for Agent review workflow stages.",
            },
        ),
    ]
