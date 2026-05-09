"""Shared data models for Agent feature modules."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "ActiveChatActionKind",
    "ActiveChatAttentionState",
    "ActiveChatBatch",
    "ActiveChatMessageSignal",
    "ActiveChatMode",
    "ActiveChatNoReplyIntensity",
    "ActiveChatNotifyResult",
    "ActiveChatReplyIntensity",
    "ActiveChatRoundResult",
    "ActiveChatStartResult",
    "ActiveChatBootstrapResult",
    "ActiveChatBootstrapStageOutput",
    "ConsumedUnreadRange",
    "OverflowCompressionStageOutput",
    "ReplyDecisionResult",
    "ReplyDecisionStageOutput",
    "ReviewScanResult",
    "ReviewScanStageOutput",
    "ReviewStageExplanation",
    "ReviewStageTrace",
    "ReviewWorkflowConfig",
    "ReviewWorkflowExplanation",
    "ReviewWorkflowResult",
    "UnreadRangeIgnoreRecord",
    "UnreadRangeSummaryRecord",
    "build_review_workflow_explanation",
]

_EXPORT_MODULES = {
    "ActiveChatActionKind": "shinbot.agent.models.active_chat",
    "ActiveChatAttentionState": "shinbot.agent.models.active_chat",
    "ActiveChatBatch": "shinbot.agent.models.active_chat",
    "ActiveChatMessageSignal": "shinbot.agent.models.active_chat",
    "ActiveChatMode": "shinbot.agent.models.active_chat",
    "ActiveChatNoReplyIntensity": "shinbot.agent.models.active_chat",
    "ActiveChatNotifyResult": "shinbot.agent.models.active_chat",
    "ActiveChatReplyIntensity": "shinbot.agent.models.active_chat",
    "ActiveChatRoundResult": "shinbot.agent.models.active_chat",
    "ActiveChatStartResult": "shinbot.agent.models.active_chat",
    "ActiveChatBootstrapResult": "shinbot.agent.models.review",
    "ActiveChatBootstrapStageOutput": "shinbot.agent.models.review",
    "ConsumedUnreadRange": "shinbot.agent.models.review",
    "OverflowCompressionStageOutput": "shinbot.agent.models.review",
    "ReplyDecisionResult": "shinbot.agent.models.review",
    "ReplyDecisionStageOutput": "shinbot.agent.models.review",
    "ReviewScanResult": "shinbot.agent.models.review",
    "ReviewScanStageOutput": "shinbot.agent.models.review",
    "ReviewStageExplanation": "shinbot.agent.models.review",
    "ReviewStageTrace": "shinbot.agent.models.review",
    "ReviewWorkflowConfig": "shinbot.agent.models.review",
    "ReviewWorkflowExplanation": "shinbot.agent.models.review",
    "ReviewWorkflowResult": "shinbot.agent.models.review",
    "UnreadRangeIgnoreRecord": "shinbot.agent.models.review",
    "UnreadRangeSummaryRecord": "shinbot.agent.models.review",
    "build_review_workflow_explanation": "shinbot.agent.models.review",
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value
