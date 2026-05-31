"""LiteLLM chat completion endpoint."""

from __future__ import annotations

from typing import Any

from shinbot.agent.services.model_runtime import litellm_adapter
from shinbot.agent.services.model_runtime.backends.protocol import BackendRequestPlan
from shinbot.agent.services.model_runtime.extraction import (
    extract_injected_context,
    extract_text,
    extract_think_text,
    extract_tool_calls_list,
    response_to_dict,
)


def invoke_completion(plan: BackendRequestPlan) -> Any:
    """Invoke LiteLLM chat completion with a prepared request plan."""

    return litellm_adapter.completion(**plan.payload)


def normalize(response: Any, usage: dict[str, Any]) -> dict[str, Any]:
    """Extract structured completion data from a raw LiteLLM response."""

    messages = response_to_dict(response).get("messages") or []
    return {
        "text": extract_text(response),
        "tool_calls": extract_tool_calls_list(response),
        "think_text": extract_think_text(response),
        "injected_context": extract_injected_context(messages),
    }
