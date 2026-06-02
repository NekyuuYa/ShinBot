"""Route resolution and LiteLLM kwarg planning helpers."""

from __future__ import annotations

import random
from typing import Any

from shinbot.agent.services.model_runtime.extraction import provider_type_for_litellm
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


def build_litellm_kwargs(
    *,
    provider: dict[str, Any],
    model: dict[str, Any],
    call: ModelRuntimeCall,
    timeout_override: float | None,
    mode: str = "completion",
) -> dict[str, Any]:
    """Build the LiteLLM kwargs payload for one execution attempt."""

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
    _normalize_openai_compatible_params(kwargs)
    _drop_empty_runtime_params(kwargs)
    kwargs["model"] = _litellm_request_model_name(
        str(model["backend_model"]),
        custom_llm_provider=custom_llm_provider,
        provider_type=str(provider.get("type", "")),
    )

    if timeout_override is not None:
        kwargs["timeout"] = timeout_override

    if mode == "completion":
        kwargs["messages"] = _normalize_chat_messages(
            call.messages,
            custom_llm_provider=custom_llm_provider,
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


def _normalize_openai_compatible_params(kwargs: dict[str, Any]) -> None:
    """Translate admin-facing OpenAI-compatible params into LiteLLM kwargs."""

    request_headers = kwargs.pop("requestHeaders", None)
    if isinstance(request_headers, dict):
        extra_headers = kwargs.get("extra_headers")
        if not isinstance(extra_headers, dict):
            extra_headers = {}
        kwargs["extra_headers"] = {**extra_headers, **request_headers}


def _litellm_request_model_name(
    backend_model: str,
    *,
    custom_llm_provider: str | None,
    provider_type: str,
) -> str:
    """Return the model name sent through LiteLLM for one request."""
    if custom_llm_provider == "openai" and provider_type == "xiaomi_mimo":
        return _strip_model_prefix(backend_model, "xiaomi_mimo")
    return backend_model


def _strip_model_prefix(model_name: str, prefix: str) -> str:
    needle = f"{prefix}/"
    return model_name[len(needle) :] if model_name.startswith(needle) else model_name


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
    custom_llm_provider: str | None,
) -> list[dict[str, Any]]:
    """Keep system messages at the beginning for strict OpenAI-compatible providers."""

    normalized: list[dict[str, Any]] = []
    seen_non_system = False
    for message in messages:
        copied = dict(message)
        if copied.get("role") == "system" and seen_non_system:
            copied["role"] = "user"
        elif copied.get("role") == "system":
            if custom_llm_provider == "dashscope":
                copied["content"] = _normalize_dashscope_system_content(copied.get("content"))
        else:
            seen_non_system = True
        normalized.append(copied)
    return normalized


def _normalize_dashscope_system_content(content: Any) -> Any:
    if not isinstance(content, list):
        return content
    if any(isinstance(item, dict) and item.get("cache_control") for item in content):
        return content

    text_parts: list[str] = []
    for item in content:
        if isinstance(item, dict) and item.get("type") == "text":
            text = str(item.get("text", "") or "").strip()
            if text:
                text_parts.append(text)
        elif isinstance(item, str) and item.strip():
            text_parts.append(item.strip())
    return "\n\n".join(text_parts)


def sanitize_litellm_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    """Redact secrets from a kwargs dict before logging or observer emission."""

    return _sanitize_runtime_payload(kwargs)


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
