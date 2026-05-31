"""LiteLLM speech synthesis endpoint."""

from __future__ import annotations

from typing import Any

from shinbot.agent.services.model_runtime import litellm_adapter
from shinbot.agent.services.model_runtime.backends.protocol import BackendRequestPlan
from shinbot.agent.services.model_runtime.extraction import extract_speech_bytes


def invoke_speech(plan: BackendRequestPlan) -> Any:
    """Invoke LiteLLM speech synthesis with a prepared request plan."""

    return litellm_adapter.speech(**plan.payload)


def normalize(response: Any, usage: dict[str, Any]) -> dict[str, Any]:
    """Extract structured speech data from a raw LiteLLM response."""

    return {"audio_bytes": extract_speech_bytes(response)}
