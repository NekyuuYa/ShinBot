"""LiteLLM transcription endpoint."""

from __future__ import annotations

from typing import Any

from shinbot.agent.services.model_runtime import litellm_adapter
from shinbot.agent.services.model_runtime.backends.protocol import BackendRequestPlan


def invoke_transcription(plan: BackendRequestPlan) -> Any:
    """Invoke LiteLLM transcription with a prepared request plan."""

    return litellm_adapter.transcription(**plan.payload)
