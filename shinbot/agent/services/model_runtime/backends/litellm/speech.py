"""LiteLLM speech synthesis endpoint."""

from __future__ import annotations

from typing import Any

from shinbot.agent.services.model_runtime import litellm_adapter
from shinbot.agent.services.model_runtime.backends.protocol import BackendRequestPlan


def invoke_speech(plan: BackendRequestPlan) -> Any:
    """Invoke LiteLLM speech synthesis with a prepared request plan."""

    return litellm_adapter.speech(**plan.payload)
