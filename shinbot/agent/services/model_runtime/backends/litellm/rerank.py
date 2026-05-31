"""LiteLLM rerank endpoint."""

from __future__ import annotations

from typing import Any

from shinbot.agent.services.model_runtime import litellm_adapter
from shinbot.agent.services.model_runtime.backends.protocol import BackendRequestPlan
from shinbot.agent.services.model_runtime.extraction import extract_rerank_results


def invoke_rerank(plan: BackendRequestPlan) -> Any:
    """Invoke LiteLLM rerank with a prepared request plan."""

    return litellm_adapter.rerank(**plan.payload)


def normalize(response: Any, usage: dict[str, Any]) -> dict[str, Any]:
    """Extract structured rerank data from a raw LiteLLM response."""

    return {"results": extract_rerank_results(response)}
