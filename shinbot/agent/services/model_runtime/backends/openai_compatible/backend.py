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
from shinbot.agent.services.model_runtime.planning import sanitize_backend_request_kwargs
from shinbot.agent.services.model_runtime.providers import require_provider_descriptor
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

_ALLOWED_PARAMS: dict[BackendOperation, frozenset[str]] = {
    "completion": frozenset(
        {
            "audio",
            "extra_body",
            "extra_headers",
            "extra_query",
            "frequency_penalty",
            "function_call",
            "functions",
            "logit_bias",
            "logprobs",
            "max_completion_tokens",
            "max_tokens",
            "metadata",
            "modalities",
            "n",
            "parallel_tool_calls",
            "prediction",
            "presence_penalty",
            "prompt_cache_key",
            "prompt_cache_retention",
            "reasoning_effort",
            "response_format",
            "safety_identifier",
            "seed",
            "service_tier",
            "stop",
            "store",
            "stream",
            "stream_options",
            "temperature",
            "timeout",
            "tool_choice",
            "tools",
            "top_logprobs",
            "top_p",
            "user",
            "verbosity",
            "web_search_options",
        }
    ),
    "embedding": frozenset(
        {
            "dimensions",
            "encoding_format",
            "extra_body",
            "extra_headers",
            "extra_query",
            "timeout",
            "user",
        }
    ),
}


class OpenAICompatibleBackend:
    """Backend that calls OpenAI-compatible API endpoints directly."""

    name: str = "openai_compatible"

    def __init__(self) -> None:
        self._clients: dict[tuple[str, str], OpenAI] = {}

    def _get_client(self, provider: dict[str, Any]) -> OpenAI:
        provider_type = str(provider.get("type") or "").strip()
        descriptor = require_provider_descriptor(provider_type)
        api_key = descriptor.backend_client_auth_value(provider, backend_name=self.name)
        base_url = str(provider.get("base_url") or "")
        cache_key = (base_url, api_key)
        client = self._clients.get(cache_key)
        if client is None:
            client = OpenAI(api_key=api_key, base_url=base_url)
            self._clients[cache_key] = client
        return client

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
        provider_type = str(provider.get("type") or "").strip()
        descriptor = require_provider_descriptor(provider_type)
        self._ensure_supported_provider(provider_type, descriptor=descriptor)
        backend_model = descriptor.request_model_name(
            str(model.get("backend_model") or ""),
            backend_name=self.name,
        )
        payload = self._base_payload(
            provider=provider,
            model=model,
            call=call,
            operation=operation,
        )
        payload["model"] = backend_model

        if operation == "completion":
            payload["messages"] = call.messages
            if call.tools:
                payload["tools"] = call.tools
            if call.response_format is not None:
                payload["response_format"] = call.response_format
            if timeout_override is not None:
                payload["timeout"] = timeout_override
        elif operation == "embedding":
            payload["input"] = call.input_data if call.input_data is not None else ""
            if timeout_override is not None:
                payload["timeout"] = timeout_override

        safe_payload = sanitize_backend_request_kwargs(payload)

        return BackendRequestPlan(
            operation=operation,
            payload=payload,
            safe_payload=safe_payload,
            backend_name=self.name,
            backend_model=backend_model,
            metadata={"provider": dict(provider)},
        )

    def _base_payload(
        self,
        *,
        provider: dict[str, Any],
        model: dict[str, Any],
        call: ModelRuntimeCall,
        operation: BackendOperation,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        descriptor = require_provider_descriptor(str(provider.get("type") or ""))
        default_params = provider.get("default_params") or {}
        model_params = model.get("default_params") or {}
        payload.update(self._without_header_params(default_params, descriptor=descriptor))
        payload.update(self._without_header_params(model_params, descriptor=descriptor))
        payload.update(self._without_header_params(call.params, descriptor=descriptor))
        extra_headers = descriptor.merge_request_header_params(
            default_params,
            model_params,
            call.params,
        )
        if extra_headers:
            payload["extra_headers"] = extra_headers
        return self._filter_operation_params(operation, payload)

    @staticmethod
    def _without_header_params(params: dict[str, Any], *, descriptor: Any) -> dict[str, Any]:
        request_header_keys = set(descriptor.request_headers_param_keys)
        return {
            key: value
            for key, value in params.items()
            if key not in request_header_keys
        }

    @staticmethod
    def _ensure_supported_provider(provider_type: str, *, descriptor: Any) -> None:
        if not descriptor.supports_backend("openai_compatible"):
            supported = "provider descriptor with openai_compatible backend support"
            raise NotImplementedError(
                f"OpenAI-compatible backend does not support provider type {provider_type!r}; "
                f"expected {supported}"
            )

    @staticmethod
    def _filter_operation_params(
        operation: BackendOperation,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        allowed = _ALLOWED_PARAMS.get(operation)
        if allowed is None:
            raise NotImplementedError(
                f"OpenAI-compatible backend does not support parameter planning for: {operation}"
            )
        filtered = {key: value for key, value in payload.items() if key in allowed}
        thinking = filtered.get("thinking")
        if isinstance(thinking, dict) and not thinking:
            filtered.pop("thinking", None)
        return filtered

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
