"""Route resolution and backend request planning helpers."""

from __future__ import annotations

import random
from typing import Any

from shinbot.agent.services.model_runtime.extraction import provider_type_for_litellm
from shinbot.agent.services.model_runtime.providers import get_provider_descriptor
from shinbot.agent.services.model_runtime.types import ModelCallError, ModelRuntimeCall


def resolve_runtime_targets(
    *,
    database: Any,
    call: ModelRuntimeCall,
    picker: random.Random,
) -> list[dict[str, Any]]:
    """Resolve a runtime call into one or more provider/model attempts."""

    if database is None:
        raise ModelCallError("Model registry is not initialized")

    registry = database.model_registry
    if call.model_id:
        model = registry.get_model(call.model_id)
        if model is None:
            raise ModelCallError(f"Model {call.model_id!r} not found")
        provider = registry.get_provider(model["provider_id"])
        if provider is None:
            raise ModelCallError(f"Provider {model['provider_id']!r} not found")
        if not provider["enabled"] or not model["enabled"]:
            raise ModelCallError(f"Model {call.model_id!r} is disabled")
        return [
            {
                "provider": provider,
                "model": model,
                "timeout_override": None,
                "strategy": "direct",
            }
        ]

    assert call.route_id is not None
    route = database.model_registry.get_route(call.route_id)
    if route is None or not route["enabled"]:
        raise ModelCallError(f"Route {call.route_id!r} not found or disabled")

    members = database.model_registry.list_route_members(call.route_id)
    candidates: list[dict[str, Any]] = []
    for member in members:
        if not member["enabled"]:
            continue
        model = registry.get_model(member["model_id"])
        if model is None or not model["enabled"]:
            continue
        provider = registry.get_provider(model["provider_id"])
        if provider is None or not provider["enabled"]:
            continue
        candidates.append(
            {
                "provider": provider,
                "model": model,
                "timeout_override": member["timeout_override"],
                "priority": member["priority"],
                "weight": member["weight"],
                "strategy": route["strategy"],
            }
        )

    if not candidates:
        raise ModelCallError(f"Route {call.route_id!r} has no available models")

    if route["strategy"] == "weighted":
        first = weighted_pick(candidates, picker=picker)
        rest = [item for item in candidates if item["model"]["id"] != first["model"]["id"]]
        rest.sort(key=lambda item: (item["priority"], -item["weight"], item["model"]["id"]))
        return [first, *rest]

    candidates.sort(key=lambda item: (item["priority"], -item["weight"], item["model"]["id"]))
    return candidates


def weighted_pick(
    candidates: list[dict[str, Any]],
    *,
    picker: random.Random,
) -> dict[str, Any]:
    """Pick one weighted route member, falling back to deterministic priority order."""

    weights = [max(float(item["weight"]), 0.0) for item in candidates]
    if all(weight == 0.0 for weight in weights):
        return sorted(
            candidates,
            key=lambda item: (item["priority"], -item["weight"], item["model"]["id"]),
        )[0]
    return picker.choices(candidates, weights=weights, k=1)[0]


def build_backend_request_kwargs(
    *,
    provider: dict[str, Any],
    model: dict[str, Any],
    call: ModelRuntimeCall,
    timeout_override: float | None,
    mode: str = "completion",
) -> dict[str, Any]:
    """Build backend request kwargs for one execution attempt."""

    kwargs: dict[str, Any] = {}
    if provider["base_url"]:
        kwargs["api_base"] = provider["base_url"]
    custom_llm_provider = provider_type_for_litellm(str(provider.get("type", "")))
    if custom_llm_provider:
        kwargs["custom_llm_provider"] = custom_llm_provider

    kwargs.update(provider.get("auth") or {})
    kwargs.update(provider.get("default_params") or {})
    kwargs.update(model.get("default_params") or {})
    kwargs.update(call.params)
    descriptor = get_provider_descriptor(str(provider.get("type", "")))
    _normalize_openai_compatible_params(kwargs, descriptor=descriptor)
    _drop_empty_runtime_params(kwargs)
    kwargs["model"] = _runtime_request_model_name(
        str(model["backend_model"]),
        backend_name="litellm",
        descriptor=descriptor,
    )

    if timeout_override is not None:
        kwargs["timeout"] = timeout_override

    if mode == "completion":
        kwargs["messages"] = _normalize_chat_messages(
            call.messages,
            descriptor=descriptor,
            backend_name="litellm",
        )
        allowed_openai_params = _normalize_allowed_openai_params(kwargs)
        if call.tools:
            kwargs["tools"] = call.tools
            _allow_openai_param(allowed_openai_params, "tools")
            _allow_openai_param(allowed_openai_params, "tool_choice")
        if call.response_format is not None:
            kwargs["response_format"] = call.response_format
            _allow_openai_param(allowed_openai_params, "response_format")
        if allowed_openai_params:
            kwargs["allowed_openai_params"] = allowed_openai_params
    elif mode in ("embedding", "speech"):
        kwargs["input"] = call.input_data if call.input_data is not None else ""

    return kwargs


def _normalize_openai_compatible_params(
    kwargs: dict[str, Any],
    *,
    descriptor: Any = None,
) -> None:
    """Translate admin-facing OpenAI-compatible params into LiteLLM kwargs."""

    request_headers = None
    if descriptor is not None:
        request_headers = descriptor.merge_request_header_params(kwargs)
        for key in descriptor.request_headers_param_keys:
            kwargs.pop(key, None)
    else:
        request_headers = kwargs.pop("requestHeaders", None)

    if isinstance(request_headers, dict) and request_headers:
        extra_headers = kwargs.get("extra_headers")
        if not isinstance(extra_headers, dict):
            extra_headers = {}
        kwargs["extra_headers"] = {**extra_headers, **request_headers}


def _runtime_request_model_name(
    backend_model: str,
    *,
    descriptor: Any = None,
    backend_name: str,
) -> str:
    """Return the model name sent through one backend for one request."""
    if descriptor is not None:
        return descriptor.request_model_name(backend_model, backend_name=backend_name)
    return backend_model


def _normalize_allowed_openai_params(kwargs: dict[str, Any]) -> list[str]:
    value = kwargs.get("allowed_openai_params", [])
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, tuple):
        return [str(item) for item in value]
    return []


def _allow_openai_param(params: list[str], value: str) -> None:
    if value not in params:
        params.append(value)


def _drop_empty_runtime_params(kwargs: dict[str, Any]) -> None:
    """Remove empty optional provider params that LiteLLM still validates."""

    for key in ("thinking",):
        value = kwargs.get(key)
        if isinstance(value, dict) and not value:
            kwargs.pop(key, None)


def _normalize_chat_messages(
    messages: list[dict[str, Any]],
    *,
    descriptor: Any = None,
    backend_name: str,
) -> list[dict[str, Any]]:
    """Keep system messages at the beginning for strict runtime providers."""

    if descriptor is not None:
        return descriptor.normalize_runtime_messages(messages, backend_name=backend_name)
    return [dict(message) for message in messages]


def sanitize_backend_request_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    """Redact secrets from a request kwargs dict before logging or observer emission."""

    return _sanitize_runtime_payload(kwargs)


def build_litellm_kwargs(
    *,
    provider: dict[str, Any],
    model: dict[str, Any],
    call: ModelRuntimeCall,
    timeout_override: float | None,
    mode: str = "completion",
) -> dict[str, Any]:
    """Compatibility wrapper for the legacy LiteLLM request builder."""

    return build_backend_request_kwargs(
        provider=provider,
        model=model,
        call=call,
        timeout_override=timeout_override,
        mode=mode,
    )


def sanitize_litellm_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    """Compatibility wrapper for the legacy LiteLLM payload sanitizer."""

    return sanitize_backend_request_kwargs(kwargs)


_REDACTED_RUNTIME_KEYS = frozenset(
    {
        "access_token",
        "api-key",
        "api_key",
        "api_secret",
        "api_token",
        "app_secret",
        "authorization",
        "proxy-authorization",
        "x-api-key",
        "x-goog-api-key",
    }
)


def _sanitize_runtime_payload(value: Any, *, key: str = "") -> Any:
    normalized_key = key.strip().lower()
    if normalized_key in _REDACTED_RUNTIME_KEYS and value:
        return "***"
    if isinstance(value, dict):
        return {
            str(item_key): _sanitize_runtime_payload(item_value, key=str(item_key))
            for item_key, item_value in value.items()
        }
    if isinstance(value, list):
        return [_sanitize_runtime_payload(item, key=key) for item in value]
    if isinstance(value, tuple):
        return [_sanitize_runtime_payload(item, key=key) for item in value]
    return value
