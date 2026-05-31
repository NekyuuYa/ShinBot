"""LiteLLM transcription endpoint."""

from __future__ import annotations

from typing import Any

from shinbot.agent.services.model_runtime import litellm_adapter
from shinbot.agent.services.model_runtime.backends.protocol import BackendRequestPlan
from shinbot.agent.services.model_runtime.extraction import extract_transcription_text


def invoke_transcription(plan: BackendRequestPlan) -> Any:
    """Invoke LiteLLM transcription with a prepared request plan."""

    return litellm_adapter.transcription(**plan.payload)


def normalize(response: Any, usage: dict[str, Any]) -> dict[str, Any]:
    """Extract structured transcription data from a raw LiteLLM response."""

    return {"text": extract_transcription_text(response)}
