"""OpenAI-compatible embedding endpoint."""

from __future__ import annotations

from typing import Any

from shinbot.agent.services.model_runtime.backends.protocol import BackendRequestPlan
from shinbot.agent.services.model_runtime.extraction import extract_embedding


def invoke_embedding(client: Any, plan: BackendRequestPlan) -> Any:
    """Invoke an OpenAI-compatible embedding with a prepared request plan."""

    return client.embeddings.create(**plan.payload)


def normalize(response: Any, usage: dict[str, Any]) -> dict[str, Any]:
    """Extract structured embedding data from a raw OpenAI response."""

    return {"embedding": extract_embedding(response)}
