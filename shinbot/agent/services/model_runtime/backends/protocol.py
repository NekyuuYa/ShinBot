"""Backend contracts for model runtime execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from shinbot.agent.services.model_runtime.types import ModelRuntimeCall

BackendOperation = Literal[
    "completion",
    "embedding",
    "rerank",
    "speech",
    "transcription",
    "image",
    "video",
]


@dataclass(slots=True)
class BackendRequestPlan:
    """Prepared backend request payload for one model execution attempt."""

    operation: BackendOperation
    payload: dict[str, Any]
    safe_payload: dict[str, Any]
    backend_name: str
    backend_model: str
    metadata: dict[str, Any] = field(default_factory=dict)


class ModelBackend(Protocol):
    """Execution backend used by the model runtime service."""

    name: str

    def plan_request(
        self,
        *,
        provider: dict[str, Any],
        model: dict[str, Any],
        call: ModelRuntimeCall,
        timeout_override: float | None,
        operation: BackendOperation,
    ) -> BackendRequestPlan:
        """Build the backend request payload for an execution attempt."""

    def invoke(self, plan: BackendRequestPlan) -> Any:
        """Execute a prepared request plan and return the raw backend response."""

    def normalize_response(
        self,
        *,
        operation: BackendOperation,
        response: Any,
        usage: dict[str, Any],
    ) -> dict[str, Any]:
        """Extract structured data from raw backend response."""
