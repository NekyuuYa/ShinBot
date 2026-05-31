"""LiteLLM video generation endpoint."""

from __future__ import annotations

from typing import Any

from shinbot.agent.services.model_runtime import litellm_adapter
from shinbot.agent.services.model_runtime.backends.protocol import BackendRequestPlan
from shinbot.agent.services.model_runtime.extraction import extract_image_urls


def invoke_video_generation(plan: BackendRequestPlan) -> Any:
    """Invoke LiteLLM video generation with a prepared request plan."""

    return litellm_adapter.video_generation(**plan.payload)


def normalize(response: Any, usage: dict[str, Any]) -> dict[str, Any]:
    """Extract structured video data from a raw LiteLLM response."""

    return {"urls": extract_image_urls(response)}
