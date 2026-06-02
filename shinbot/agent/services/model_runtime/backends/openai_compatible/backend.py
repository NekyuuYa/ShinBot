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
from shinbot.agent.services.model_runtime.planning import sanitize_litellm_kwargs
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

_SUPPORTED_PROVIDER_TYPES = frozenset(
    {
        "azure_openai",
        "custom_openai",
        "dashscope",
        "deepseek",
        "ollama",
        "openai",
        "openrouter",
        "siliconflow",
        "xiaomi_mimo",
    }
)

_MODEL_PREFIX_BY_PROVIDER = {
    "azure_openai": "openai",
    "custom_openai": "openai",
    "deepseek": "deepseek",
    "ollama": "ollama",
    "openai": "openai",
    "openrouter": "openrouter",
    "xiaomi_mimo": "xiaomi_mimo",
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
        auth: dict[str, Any] = provider.get("auth") or {}
        api_key = str(auth.get("api_key") or "")
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
        self._ensure_supported_provider(provider_type)
        backend_model = self._request_model_name(
            provider_type=provider_type,
            backend_model=str(model.get("backend_model") or ""),
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

        safe_payload = sanitize_litellm_kwargs(payload)

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
        default_params = provider.get("default_params") or {}
        model_params = model.get("default_params") or {}
        payload.update(self._without_header_params(default_params))
        payload.update(self._without_header_params(model_params))
        payload.update(self._without_header_params(call.params))
        extra_headers = self._merge_request_headers(default_params, model_params, call.params)
        if extra_headers:
            payload["extra_headers"] = extra_headers
        return self._filter_operation_params(operation, payload)

    @staticmethod
    def _without_header_params(params: dict[str, Any]) -> dict[str, Any]:
        return {
            key: value
            for key, value in params.items()
            if key not in {"extra_headers", "requestHeaders"}
        }

    @staticmethod
    def _merge_request_headers(*sources: dict[str, Any]) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        for source in sources:
            extra_headers = source.get("extra_headers")
            if isinstance(extra_headers, dict):
                merged.update(extra_headers)
            request_headers = source.get("requestHeaders")
            if isinstance(request_headers, dict):
                merged.update(request_headers)
        return merged

    @staticmethod
    def _ensure_supported_provider(provider_type: str) -> None:
        if provider_type not in _SUPPORTED_PROVIDER_TYPES:
            supported = ", ".join(sorted(_SUPPORTED_PROVIDER_TYPES))
            raise NotImplementedError(
                f"OpenAI-compatible backend does not support provider type {provider_type!r}; "
                f"supported types: {supported}"
            )

    @staticmethod
    def _request_model_name(*, provider_type: str, backend_model: str) -> str:
        prefix = _MODEL_PREFIX_BY_PROVIDER.get(provider_type)
        if prefix:
            needle = f"{prefix}/"
            if backend_model.startswith(needle):
                return backend_model[len(needle) :]
        return backend_model

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
