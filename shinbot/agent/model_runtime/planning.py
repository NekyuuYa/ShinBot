"""Route resolution and LiteLLM kwarg planning helpers."""

from __future__ import annotations

import random
from typing import Any

from shinbot.agent.model_runtime.extraction import provider_type_for_litellm
from shinbot.agent.model_runtime.types import ModelCallError, ModelRuntimeCall


def resolve_runtime_targets(
    *,
    database: Any,
    call: ModelRuntimeCall,
    picker: random.Random,
) -> list[dict[str, Any]]:
    """Resolve a runtime call into one or more provider/model attempts."""

    if database is None:
        raise ModelCallError("Database-backed model registry is not initialized")

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
    kwargs["model"] = model["litellm_model"]

    if timeout_override is not None:
        kwargs["timeout"] = timeout_override

    if mode == "completion":
        kwargs["messages"] = call.messages
        if call.tools:
            kwargs["tools"] = call.tools
            # Workflow-style tool orchestration expects the model to finish via
            # tool calls instead of free-form assistant text.
            kwargs["tool_choice"] = "required"
        if call.response_format is not None:
            kwargs["response_format"] = call.response_format
    elif mode in ("embedding", "speech"):
        kwargs["input"] = call.input_data if call.input_data is not None else ""

    return kwargs


def sanitize_litellm_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    """Redact secrets from a kwargs dict before logging or observer emission."""

    redacted = dict(kwargs)
    for key in (
        "api_key",
        "api_token",
        "access_token",
        "authorization",
        "Authorization",
        "app_secret",
        "api_secret",
    ):
        if key in redacted and redacted[key]:
            redacted[key] = "***"
    return redacted
