"""LiteLLM embedding endpoint."""

from __future__ import annotations

from typing import Any

from shinbot.agent.services.model_runtime import litellm_adapter
from shinbot.agent.services.model_runtime.backends.protocol import BackendRequestPlan
from shinbot.agent.services.model_runtime.extraction import extract_embedding


def invoke_embedding(plan: BackendRequestPlan) -> Any:
    """Invoke LiteLLM embedding with a prepared request plan."""

    return litellm_adapter.embedding(**plan.payload)


def normalize(response: Any, usage: dict[str, Any]) -> dict[str, Any]:
    """Extract structured embedding data from a raw LiteLLM response."""

    return {"embedding": extract_embedding(response)}
