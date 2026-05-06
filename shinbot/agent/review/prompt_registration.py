"""Built-in prompt components for Agent review workflows."""

from __future__ import annotations

from shinbot.agent.prompt_manager.schema import (
    PromptComponent,
    PromptComponentKind,
    PromptStage,
)

REVIEW_PROMPT_COMPONENT_IDS_BY_STAGE: dict[str, dict[PromptStage, list[str]]] = {
    "overflow_compression": {
        PromptStage.SYSTEM_BASE: ["review.overflow_compression.system"],
        PromptStage.CONSTRAINTS: ["review.overflow_compression.constraints"],
    },
    "review_scan": {
        PromptStage.SYSTEM_BASE: ["review.review_scan.system"],
        PromptStage.CONSTRAINTS: ["review.review_scan.constraints"],
    },
    "reply_decision": {
        PromptStage.SYSTEM_BASE: ["review.reply_decision.system"],
        PromptStage.CONSTRAINTS: ["review.reply_decision.constraints"],
    },
    "active_chat_bootstrap": {
        PromptStage.SYSTEM_BASE: ["review.active_chat_bootstrap.system"],
        PromptStage.CONSTRAINTS: ["review.active_chat_bootstrap.constraints"],
    },
}

_COMMON_SYSTEM_PROMPT = (
    "You are an internal ShinBot Agent review workflow stage. Follow the stage "
    "contract exactly. Do not produce user-visible bare assistant text unless the "
    "stage explicitly asks for structured JSON fallback."
)


def register_review_prompt_components(registry) -> None:
    """Register built-in review workflow prompt components."""

    for component in _review_prompt_components():
        registry.upsert_component(component)


def _review_prompt_components() -> list[PromptComponent]:
    return [
        _component(
            "review.overflow_compression.system",
            PromptStage.SYSTEM_BASE,
            _COMMON_SYSTEM_PROMPT,
            "Review Overflow Compression System",
        ),
        _component(
            "review.overflow_compression.constraints",
            PromptStage.CONSTRAINTS,
            "Compress only older overflow messages. Preserve unresolved topics, "
            "useful facts, and message ids that may deserve later reply review. "
            "Return the requested JSON object.",
            "Review Overflow Compression Constraints",
        ),
        _component(
            "review.review_scan.system",
            PromptStage.SYSTEM_BASE,
            _COMMON_SYSTEM_PROMPT,
            "Review Scan System",
        ),
        _component(
            "review.review_scan.constraints",
            PromptStage.CONSTRAINTS,
            "Select message_log ids that may deserve a reply or closer local "
            "decision. Prefer high-signal messages and avoid over-selecting. "
            "Do not decide reply text or active chat parameters. Return the "
            "requested JSON object.",
            "Review Scan Constraints",
        ),
        _component(
            "review.reply_decision.system",
            PromptStage.SYSTEM_BASE,
            _COMMON_SYSTEM_PROMPT,
            "Review Reply Decision System",
        ),
        _component(
            "review.reply_decision.constraints",
            PromptStage.CONSTRAINTS,
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
            "send_poke. Do not decide or emit active chat parameters in this stage.",
            "Review Reply Decision Constraints",
        ),
        _component(
            "review.active_chat_bootstrap.system",
            PromptStage.SYSTEM_BASE,
            _COMMON_SYSTEM_PROMPT,
            "Review Active Chat Bootstrap System",
        ),
        _component(
            "review.active_chat_bootstrap.constraints",
            PromptStage.CONSTRAINTS,
            "Choose only active chat bootstrap parameters after review/reply has "
            "finished. Do not send replies. Use low interest for weak or merely "
            "informational observations, and higher interest for likely continued "
            "participation. Return the requested JSON object.",
            "Review Active Chat Bootstrap Constraints",
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
        tags=["review", "workflow"],
        metadata={
            "builtin": True,
            "display_name": display_name,
            "description": "Built-in prompt component for Agent review workflow stages.",
        },
    )


__all__ = [
    "REVIEW_PROMPT_COMPONENT_IDS_BY_STAGE",
    "register_review_prompt_components",
]
