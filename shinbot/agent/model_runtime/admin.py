"""Administrative helpers for model runtime management flows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from shinbot.agent.model_runtime import ModelCallError, ModelRuntimeCall, litellm_adapter


@dataclass(slots=True)
class ModelRuntimeAdminError(RuntimeError):
    """Structured admin-layer error for API adapters."""

    status_code: int
    code: str
    message: str

    def __str__(self) -> str:
        return self.message


def get_provider_or_raise(database: Any, provider_id: str) -> dict[str, Any]:
    payload = database.model_registry.get_provider(provider_id)
    if payload is None:
        raise ModelRuntimeAdminError(
            status_code=404,
            code="PROVIDER_NOT_FOUND",
            message=f"Provider {provider_id!r} not found",
        )
    return payload


def get_model_or_raise(database: Any, model_id: str) -> dict[str, Any]:
    payload = database.model_registry.get_model(model_id)
    if payload is None:
        raise ModelRuntimeAdminError(
            status_code=404,
            code="MODEL_NOT_FOUND",
            message=f"Model {model_id!r} not found",
        )
    return payload


def get_route_or_raise(database: Any, route_id: str) -> dict[str, Any]:
    payload = database.model_registry.get_route(route_id)
    if payload is None:
        raise ModelRuntimeAdminError(
            status_code=404,
            code="ROUTE_NOT_FOUND",
            message=f"Route {route_id!r} not found",
        )
    return payload


def validate_route_member_ids(database: Any, model_ids: list[str]) -> None:
    for model_id in model_ids:
        get_model_or_raise(database, model_id)


def provider_request_headers(payload: dict[str, Any]) -> dict[str, str]:
    headers: dict[str, str] = {}
    auth = payload.get("auth") or {}
    default_params = payload.get("default_params") or {}
    api_key = auth.get("api_key")
    if api_key and payload["type"] == "azure_openai":
        headers["api-key"] = str(api_key)
    elif api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    request_headers = default_params.get("requestHeaders")
    if isinstance(request_headers, dict):
        for key, value in request_headers.items():
            if value is None:
                continue
            headers[str(key)] = str(value)
    return headers


def provider_type_for_model_info(provider_type: str) -> str | None:
    if provider_type == "custom_openai":
        return "openai"
    if provider_type == "azure_openai":
        return "azure"
    if provider_type in {"openai", "openrouter", "ollama"}:
        return None
    return None


def infer_context_window(provider: dict[str, Any], litellm_model: str) -> int | None:
    try:
        model_info = litellm_adapter.get_model_info(
            litellm_model,
            custom_llm_provider=provider_type_for_model_info(provider["type"]),
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
    provider_type = payload["type"]
    if provider_type == "ollama":
        models = []
        for item in body.get("models", []):
            model_id = item.get("name")
            if not model_id:
                continue
            models.append(
                {
                    "id": str(model_id),
                    "displayName": str(model_id),
                    "litellmModel": f"ollama/{model_id}",
                    "contextWindow": infer_context_window(payload, f"ollama/{model_id}"),
                }
            )
        return models

    items = body.get("data", [])
    models: list[dict[str, Any]] = []
    for item in items:
        model_id = item.get("id")
        if not model_id:
            continue
        litellm_model = str(model_id)
        if provider_type == "openrouter":
            litellm_model = f"openrouter/{model_id}"
        models.append(
            {
                "id": str(model_id),
                "displayName": str(item.get("name") or model_id),
                "litellmModel": litellm_model,
                "contextWindow": infer_context_window(payload, litellm_model),
            }
        )
    return models


async def fetch_provider_catalog(payload: dict[str, Any]) -> list[dict[str, Any]]:
    provider_type = payload["type"]
    base_url = (payload.get("base_url") or "").rstrip("/")
    if not base_url:
        raise ModelRuntimeAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="Provider base URL is required",
        )

    if provider_type == "ollama":
        url = f"{base_url}/api/tags"
    elif provider_type in {"openai", "openrouter", "custom_openai", "azure_openai"}:
        url = f"{base_url}/models"
    else:
        raise ModelRuntimeAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message=f"Provider type {provider_type!r} does not support catalog fetch",
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
