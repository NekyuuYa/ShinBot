"""LiteLLM video generation endpoint."""

from __future__ import annotations

from typing import Any

from shinbot.agent.services.model_runtime import litellm_adapter
from shinbot.agent.services.model_runtime.backends.protocol import BackendRequestPlan


def invoke_video_generation(plan: BackendRequestPlan) -> Any:
    """Invoke LiteLLM video generation with a prepared request plan."""

    return litellm_adapter.video_generation(**plan.payload)
