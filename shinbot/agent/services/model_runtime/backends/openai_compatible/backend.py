"""OpenAI-compatible model runtime backend."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from openai import OpenAI

from shinbot.agent.services.model_runtime.backends.openai_compatible.completion import (
    invoke_completion,
)
from shinbot.agent.services.model_runtime.backends.openai_compatible.completion import (
    normalize as normalize_completion,
)
from shinbot.agent.services.model_runtime.backends.openai_compatible.embedding import (
    invoke_embedding,
)
from shinbot.agent.services.model_runtime.backends.openai_compatible.embedding import (
    normalize as normalize_embedding,
)
from shinbot.agent.services.model_runtime.backends.protocol import (
    BackendOperation,
    BackendRequestPlan,
)
from shinbot.agent.services.model_runtime.types import ModelRuntimeCall

_ClientFactory = Callable[[dict[str, Any]], OpenAI]
_EndpointInvoker = Callable[[OpenAI, BackendRequestPlan], Any]
_EndpointNormalizer = Callable[[Any, dict[str, Any]], dict[str, Any]]

_INVOKE: dict[BackendOperation, _EndpointInvoker] = {
    "completion": invoke_completion,
    "embedding": invoke_embedding,
}

_NORMALIZERS: dict[BackendOperation, _EndpointNormalizer] = {
    "completion": normalize_completion,
    "embedding": normalize_embedding,
}


class OpenAICompatibleBackend:
    """Backend that calls OpenAI-compatible API endpoints directly."""

    name: str = "openai_compatible"

    def __init__(self) -> None:
        self._client: OpenAI | None = None

    def _get_client(self, provider: dict[str, Any]) -> OpenAI:
        if self._client is None:
            auth: dict[str, Any] = provider.get("auth") or {}
            api_key = str(auth.get("api_key") or "")
            base_url = str(provider.get("base_url") or "")
            self._client = OpenAI(api_key=api_key, base_url=base_url)
        return self._client

    def plan_request(
        self,
        *,
        provider: dict[str, Any],
        model: dict[str, Any],
        call: ModelRuntimeCall,
        timeout_override: float | None,
        operation: BackendOperation,
    ) -> BackendRequestPlan:
        """Build the request payload for an OpenAI-compatible call."""
        backend_model = str(model.get("backend_model") or "")
        payload: dict[str, Any] = {"model": backend_model}

        if operation == "completion":
            payload["messages"] = call.messages
            if call.tools:
                payload["tools"] = call.tools
            if timeout_override is not None:
                payload["timeout"] = timeout_override
            payload.update(call.params)
        elif operation == "embedding":
            payload["input"] = call.messages[-1]["content"] if call.messages else ""

        safe_payload = {k: v for k, v in payload.items() if k != "api_key"}

        return BackendRequestPlan(
            operation=operation,
            payload=payload,
            safe_payload=safe_payload,
            backend_name=self.name,
            backend_model=backend_model,
        )

    def invoke(self, plan: BackendRequestPlan) -> Any:
        """Execute a prepared request plan via the OpenAI client."""
        invoker = _INVOKE.get(plan.operation)
        if invoker is not None:
            provider = plan.metadata.get("provider", {})
            client = self._get_client(provider)
            return invoker(client, plan)
        raise NotImplementedError(
            f"OpenAI-compatible backend does not support operation: {plan.operation}"
        )

    def normalize_response(
        self,
        *,
        operation: BackendOperation,
        response: Any,
        usage: dict[str, Any],
    ) -> dict[str, Any]:
        """Extract structured data from a raw OpenAI-compatible response."""
        normalizer = _NORMALIZERS.get(operation)
        if normalizer is not None:
            return normalizer(response, usage)
        raise NotImplementedError(
            f"OpenAI-compatible backend does not support normalization for: {operation}"
        )
