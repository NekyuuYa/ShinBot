"""LiteLLM rerank endpoint."""

from __future__ import annotations

from typing import Any

from shinbot.agent.services.model_runtime import litellm_adapter
from shinbot.agent.services.model_runtime.backends.protocol import BackendRequestPlan


def invoke_rerank(plan: BackendRequestPlan) -> Any:
    """Invoke LiteLLM rerank with a prepared request plan."""

    return litellm_adapter.rerank(**plan.payload)
