"""Administrative helpers for model runtime management flows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from shinbot.agent.services.model_runtime import ModelCallError, ModelRuntimeCall, litellm_adapter
from shinbot.agent.services.model_runtime.providers import require_provider_descriptor


@dataclass(slots=True)
class ModelRuntimeAdminError(RuntimeError):
    """Structured admin-layer error for API adapters."""

    status_code: int
    code: str
    message: str

    def __str__(self) -> str:
        return self.message


def get_provider_or_raise(database: Any, provider_id: str) -> dict[str, Any]:
    """Look up a provider by ID or raise a 404 error.

    Args:
        database: Application database handle with a ``model_registry`` accessor.
        provider_id: Unique identifier of the provider to retrieve.

    Returns:
        Provider payload dictionary from the registry.

    Raises:
        ModelRuntimeAdminError: If the provider does not exist.
    """
    payload = database.model_registry.get_provider(provider_id)
    if payload is None:
        raise ModelRuntimeAdminError(
            status_code=404,
            code="PROVIDER_NOT_FOUND",
            message=f"Provider {provider_id!r} not found",
        )
    return payload


def get_model_or_raise(database: Any, model_id: str) -> dict[str, Any]:
    """Look up a model by ID or raise a 404 error.

    Args:
        database: Application database handle with a ``model_registry`` accessor.
        model_id: Unique identifier of the model to retrieve.

    Returns:
        Model payload dictionary from the registry.

    Raises:
        ModelRuntimeAdminError: If the model does not exist.
    """
    payload = database.model_registry.get_model(model_id)
    if payload is None:
        raise ModelRuntimeAdminError(
            status_code=404,
            code="MODEL_NOT_FOUND",
            message=f"Model {model_id!r} not found",
        )
    return payload


def get_route_or_raise(database: Any, route_id: str) -> dict[str, Any]:
    """Look up a route by ID or raise a 404 error.

    Args:
        database: Application database handle with a ``model_registry`` accessor.
        route_id: Unique identifier of the route to retrieve.

    Returns:
        Route payload dictionary from the registry.

    Raises:
        ModelRuntimeAdminError: If the route does not exist.
    """
    payload = database.model_registry.get_route(route_id)
    if payload is None:
        raise ModelRuntimeAdminError(
            status_code=404,
            code="ROUTE_NOT_FOUND",
            message=f"Route {route_id!r} not found",
        )
    return payload


def validate_route_member_ids(database: Any, model_ids: list[str]) -> None:
    """Validate that every model ID in a route actually exists.

    Args:
        database: Application database handle with a ``model_registry`` accessor.
        model_ids: List of model IDs referenced by a route.

    Raises:
        ModelRuntimeAdminError: If any model ID is not found.
    """
    for model_id in model_ids:
        get_model_or_raise(database, model_id)


def provider_request_headers(payload: dict[str, Any]) -> dict[str, str]:
    """Build HTTP headers required for outgoing provider API calls."""

    descriptor = require_provider_descriptor(str(payload.get("type") or ""))
    return descriptor.request_headers(payload)


def provider_type_for_model_info(provider_type: str) -> str | None:
    """Return the LiteLLM model-info override declared by a provider descriptor."""

    descriptor = require_provider_descriptor(provider_type)
    return descriptor.model_info_custom_llm_provider


def infer_context_window(provider: dict[str, Any], backend_model: str) -> int | None:
    """Infer the context window size for a model via LiteLLM metadata.

    Queries ``litellm_adapter.get_model_info`` and returns the larger of
    ``max_input_tokens`` and ``max_tokens``.  Returns ``None`` when the
    information is unavailable or the adapter raises.

    Args:
        provider: Provider configuration dictionary (used for the custom
            provider type override).
        backend_model: Fully-qualified LiteLLM model identifier.

    Returns:
        Context window size in tokens, or ``None`` if it could not be
        determined.
    """
    try:
        descriptor = require_provider_descriptor(str(provider["type"]))
        model_info = litellm_adapter.get_model_info(
            backend_model,
            custom_llm_provider=descriptor.model_info_custom_llm_provider,
            api_base=provider.get("base_url") or None,
        )
    except Exception:
        return None

    max_input_tokens = model_info.get("max_input_tokens")
    if isinstance(max_input_tokens, int) and max_input_tokens > 0:
        return max_input_tokens

    max_tokens = model_info.get("max_tokens")
    if isinstance(max_tokens, int) and max_tokens > 0:
        return max_tokens

    return None


def normalize_provider_catalog(payload: dict[str, Any], body: Any) -> list[dict[str, Any]]:
    """Normalize a raw provider API response into a standard catalog format."""

    descriptor = require_provider_descriptor(str(payload["type"]))
    return descriptor.normalize_catalog(
        payload,
        body,
        infer_context_window=infer_context_window,
    )


async def fetch_provider_catalog(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Asynchronously fetch the available model catalog from a provider.

    Determines the correct listing endpoint based on provider type, issues
    an HTTP GET with the provider's request headers, and normalizes the
    response through :func:`normalize_provider_catalog`.

    Args:
        payload: Provider configuration dictionary containing ``type``,
            ``base_url``, ``auth``, and ``default_params``.

    Returns:
        Normalized list of model catalog entries.

    Raises:
        ModelRuntimeAdminError: If the provider has no base URL, the
            provider type does not support catalog fetching, or the HTTP
            request fails.
    """
    descriptor = require_provider_descriptor(str(payload["type"]))
    url = descriptor.catalog_url(payload)
    if not url:
        raise ModelRuntimeAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message=(
                "Provider base URL is required"
                if not str(payload.get("base_url") or "").strip()
                else f"Provider type {payload['type']!r} does not support catalog fetch"
            ),
        )

    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(url, headers=provider_request_headers(payload))
        response.raise_for_status()
        body = response.json()
    return normalize_provider_catalog(payload, body)


async def probe_provider_runtime(
    *,
    database: Any,
    model_runtime: Any,
    provider_id: str,
    model_id: str | None = None,
    checked_at: str,
) -> dict[str, Any]:
    """Probe a provider's runtime to verify connectivity and capability.

    Selects an appropriate model (explicitly requested or the first enabled
    one), then dispatches a minimal test call appropriate for the model's
    capability type (embedding, completion, or catalog fetch).  When no
    models are registered, falls back to a catalog-only probe.

    Args:
        database: Application database handle with a ``model_registry`` accessor.
        model_runtime: Model runtime instance for executing test calls.
        provider_id: ID of the provider to probe.
        model_id: Optional specific model ID to test.  When ``None``, the
            first enabled model for the provider is used.
        checked_at: ISO-8601 timestamp recorded in the probe result.

    Returns:
        Probe result dictionary containing ``success``, ``providerId``,
        ``mode`` (``"chat"``, ``"embedding"``, ``"catalog"``, or
        ``"skipped"``), and other diagnostic fields.

    Raises:
        ModelRuntimeAdminError: If the provider or model is not found, the
            model does not belong to the provider, or a runtime call fails.
    """
    provider = get_provider_or_raise(database, provider_id)

    if model_id:
        model = get_model_or_raise(database, model_id)
        if model["provider_id"] != provider_id:
            raise ModelRuntimeAdminError(
                status_code=400,
                code="INVALID_ACTION",
                message=f"Model {model_id!r} does not belong to provider {provider_id!r}",
            )
    else:
        models = [
            item
            for item in database.model_registry.list_models(provider_id=provider_id)
            if item["enabled"]
        ]
        model = models[0] if models else None

    if model is None:
        catalog = await fetch_provider_catalog(provider)
        return {
            "success": True,
            "providerId": provider_id,
            "mode": "catalog",
            "checkedAt": checked_at,
            "latencyMs": 0,
            "catalogSize": len(catalog),
        }

    capability_type = provider.get("capability_type", "completion")

    if capability_type == "embedding" or "embedding" in model["capabilities"]:
        try:
            result = await model_runtime.embed(
                ModelRuntimeCall(
                    model_id=model["id"],
                    caller="webui.provider_probe",
                    purpose="provider_probe",
                    input_data="ping",
                    metadata={"probe": True},
                )
            )
        except ModelCallError as exc:
            raise ModelRuntimeAdminError(
                status_code=502,
                code="INTERNAL_ERROR",
                message=f"Provider probe failed: {exc}",
            ) from exc
        return {
            "success": True,
            "providerId": provider_id,
            "modelId": model["id"],
            "mode": "embedding",
            "checkedAt": checked_at,
            "executionId": result.execution_id,
        }

    if capability_type == "completion":
        try:
            result = await model_runtime.generate(
                ModelRuntimeCall(
                    model_id=model["id"],
                    caller="webui.provider_probe",
                    purpose="provider_probe",
                    messages=[{"role": "user", "content": "ping"}],
                    params={"max_tokens": 1, "drop_params": True},
                    metadata={"probe": True},
                )
            )
        except ModelCallError as exc:
            raise ModelRuntimeAdminError(
                status_code=502,
                code="INTERNAL_ERROR",
                message=f"Provider probe failed: {exc}",
            ) from exc
        return {
            "success": True,
            "providerId": provider_id,
            "modelId": model["id"],
            "mode": "chat",
            "checkedAt": checked_at,
            "executionId": result.execution_id,
        }

    try:
        catalog = await fetch_provider_catalog(provider)
        return {
            "success": True,
            "providerId": provider_id,
            "modelId": model["id"],
            "mode": "catalog",
            "checkedAt": checked_at,
            "catalogSize": len(catalog),
        }
    except Exception:
        return {
            "success": True,
            "providerId": provider_id,
            "modelId": model["id"],
            "mode": "skipped",
            "checkedAt": checked_at,
        }
