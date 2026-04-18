"""Model runtime management router: /api/v1/model-runtime"""

from __future__ import annotations

from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from shinbot.agent.model_runtime import ModelCallError, ModelRuntimeCall, litellm_adapter
from shinbot.api.deps import AuthRequired, BotDep
from shinbot.api.models import EC, ok
from shinbot.persistence.records import (
    ModelDefinitionRecord,
    ModelProviderRecord,
    ModelRouteMemberRecord,
    ModelRouteRecord,
    utc_now_iso,
)

router = APIRouter(
    prefix="/model-runtime",
    tags=["model-runtime"],
    dependencies=AuthRequired,
)


class ProviderRequest(BaseModel):
    id: str
    type: str
    displayName: str
    capabilityType: str = "completion"
    baseUrl: str = ""
    auth: dict[str, Any] = Field(default_factory=dict)
    defaultParams: dict[str, Any] = Field(default_factory=dict)
    enabled: bool = True


class ProviderPatchRequest(BaseModel):
    id: str | None = None
    type: str | None = None
    displayName: str | None = None
    capabilityType: str | None = None
    baseUrl: str | None = None
    auth: dict[str, Any] | None = None
    defaultParams: dict[str, Any] | None = None
    enabled: bool | None = None


class ModelRequest(BaseModel):
    id: str
    providerId: str
    litellmModel: str
    displayName: str
    capabilities: list[str] = Field(default_factory=list)
    contextWindow: int | None = None
    defaultParams: dict[str, Any] = Field(default_factory=dict)
    costMetadata: dict[str, Any] = Field(default_factory=dict)
    enabled: bool = True


class ModelPatchRequest(BaseModel):
    providerId: str | None = None
    litellmModel: str | None = None
    displayName: str | None = None
    capabilities: list[str] | None = None
    contextWindow: int | None = None
    defaultParams: dict[str, Any] | None = None
    costMetadata: dict[str, Any] | None = None
    enabled: bool | None = None


class RouteMemberRequest(BaseModel):
    modelId: str
    priority: int = 0
    weight: float = 1.0
    conditions: dict[str, Any] = Field(default_factory=dict)
    timeoutOverride: float | None = None
    enabled: bool = True


class RouteRequest(BaseModel):
    id: str
    purpose: str = ""
    strategy: str = "priority"
    enabled: bool = True
    stickySessions: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)
    members: list[RouteMemberRequest] = Field(default_factory=list)


class RoutePatchRequest(BaseModel):
    id: str | None = None
    purpose: str | None = None
    strategy: str | None = None
    enabled: bool | None = None
    stickySessions: bool | None = None
    metadata: dict[str, Any] | None = None
    members: list[RouteMemberRequest] | None = None


class ProviderProbeRequest(BaseModel):
    modelId: str | None = None


def _serialize_provider(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": payload["id"],
        "type": payload["type"],
        "displayName": payload["display_name"],
        "capabilityType": payload.get("capability_type", "completion"),
        "baseUrl": payload["base_url"],
        "hasAuth": bool(payload.get("auth")),
        "defaultParams": payload["default_params"],
        "enabled": payload["enabled"],
        "createdAt": payload["created_at"],
        "lastModified": payload["updated_at"],
    }


def _serialize_model(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": payload["id"],
        "providerId": payload["provider_id"],
        "litellmModel": payload["litellm_model"],
        "displayName": payload["display_name"],
        "capabilities": payload["capabilities"],
        "contextWindow": payload["context_window"],
        "defaultParams": payload["default_params"],
        "costMetadata": payload["cost_metadata"],
        "enabled": payload["enabled"],
        "createdAt": payload["created_at"],
        "lastModified": payload["updated_at"],
    }


def _serialize_route(payload: dict[str, Any], members: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "id": payload["id"],
        "purpose": payload["purpose"],
        "strategy": payload["strategy"],
        "enabled": payload["enabled"],
        "stickySessions": payload["sticky_sessions"],
        "metadata": payload["metadata"],
        "members": [
            {
                "modelId": member["model_id"],
                "priority": member["priority"],
                "weight": member["weight"],
                "conditions": member["conditions"],
                "timeoutOverride": member["timeout_override"],
                "enabled": member["enabled"],
            }
            for member in members
        ],
        "createdAt": payload["created_at"],
        "lastModified": payload["updated_at"],
    }


def _serialize_execution(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": payload["id"],
        "routeId": payload["route_id"],
        "providerId": payload["provider_id"],
        "modelId": payload["model_id"],
        "caller": payload["caller"],
        "sessionId": payload["session_id"],
        "instanceId": payload["instance_id"],
        "purpose": payload["purpose"],
        "startedAt": payload["started_at"],
        "firstTokenAt": payload["first_token_at"],
        "finishedAt": payload["finished_at"],
        "latencyMs": payload["latency_ms"],
        "timeToFirstTokenMs": payload["time_to_first_token_ms"],
        "inputTokens": payload["input_tokens"],
        "outputTokens": payload["output_tokens"],
        "cacheHit": payload["cache_hit"],
        "cacheReadTokens": payload["cache_read_tokens"],
        "cacheWriteTokens": payload["cache_write_tokens"],
        "success": payload["success"],
        "errorCode": payload["error_code"],
        "errorMessage": payload["error_message"],
        "fallbackFromModelId": payload["fallback_from_model_id"],
        "fallbackReason": payload["fallback_reason"],
        "estimatedCost": payload["estimated_cost"],
        "currency": payload["currency"],
        "metadata": payload["metadata"],
    }


def _provider_request_headers(payload: dict[str, Any]) -> dict[str, str]:
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


def _provider_type_for_model_info(provider_type: str) -> str | None:
    if provider_type == "custom_openai":
        return "openai"
    if provider_type == "azure_openai":
        return "azure"
    if provider_type in {"openai", "openrouter", "ollama"}:
        return None
    return None


def _infer_context_window(provider: dict[str, Any], litellm_model: str) -> int | None:
    try:
        model_info = litellm_adapter.get_model_info(
            litellm_model,
            custom_llm_provider=_provider_type_for_model_info(provider["type"]),
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


def _normalize_provider_catalog(payload: dict[str, Any], body: Any) -> list[dict[str, Any]]:
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
                    "contextWindow": _infer_context_window(payload, f"ollama/{model_id}"),
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
                "contextWindow": _infer_context_window(payload, litellm_model),
            }
        )
    return models


async def _fetch_provider_catalog(payload: dict[str, Any]) -> list[dict[str, Any]]:
    provider_type = payload["type"]
    base_url = (payload.get("base_url") or "").rstrip("/")
    if not base_url:
        raise HTTPException(
            status_code=400,
            detail={"code": EC.INVALID_ACTION, "message": "Provider base URL is required"},
        )

    if provider_type == "ollama":
        url = f"{base_url}/api/tags"
    elif provider_type in {"openai", "openrouter", "custom_openai", "azure_openai"}:
        url = f"{base_url}/models"
    else:
        raise HTTPException(
            status_code=400,
            detail={
                "code": EC.INVALID_ACTION,
                "message": f"Provider type {provider_type!r} does not support catalog fetch",
            },
        )

    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(url, headers=_provider_request_headers(payload))
        response.raise_for_status()
        body = response.json()
    return _normalize_provider_catalog(payload, body)


def _require_provider(bot: Any, provider_id: str) -> dict[str, Any]:
    payload = bot.database.model_registry.get_provider(provider_id)
    if payload is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.PROVIDER_NOT_FOUND,
                "message": f"Provider {provider_id!r} not found",
            },
        )
    return payload


def _require_model(bot: Any, model_id: str) -> dict[str, Any]:
    payload = bot.database.model_registry.get_model(model_id)
    if payload is None:
        raise HTTPException(
            status_code=404,
            detail={"code": EC.MODEL_NOT_FOUND, "message": f"Model {model_id!r} not found"},
        )
    return payload


def _require_route(bot: Any, route_id: str) -> dict[str, Any]:
    payload = bot.database.model_registry.get_route(route_id)
    if payload is None:
        raise HTTPException(
            status_code=404,
            detail={"code": EC.ROUTE_NOT_FOUND, "message": f"Route {route_id!r} not found"},
        )
    return payload


def _validate_route_members(bot: Any, members: list[RouteMemberRequest]) -> None:
    for member in members:
        _require_model(bot, member.modelId)


@router.get("/providers")
async def list_providers(bot=BotDep):
    providers = bot.database.model_registry.list_providers()
    return ok([_serialize_provider(item) for item in providers])


@router.post("/providers", status_code=201)
async def create_provider(body: ProviderRequest, bot=BotDep):
    if bot.database.model_registry.get_provider(body.id) is not None:
        raise HTTPException(
            status_code=409,
            detail={
                "code": EC.PROVIDER_ALREADY_EXISTS,
                "message": f"Provider {body.id!r} already exists",
            },
        )

    bot.database.model_registry.upsert_provider(
        ModelProviderRecord(
            id=body.id,
            type=body.type,
            display_name=body.displayName,
            capability_type=body.capabilityType,
            base_url=body.baseUrl,
            auth=body.auth,
            default_params=body.defaultParams,
            enabled=body.enabled,
        )
    )
    return ok(_serialize_provider(_require_provider(bot, body.id)))


@router.patch("/providers/{provider_id:path}")
async def update_provider(provider_id: str, body: ProviderPatchRequest, bot=BotDep):
    current = _require_provider(bot, provider_id)
    next_id = body.id if body.id is not None else provider_id
    if next_id != provider_id and bot.database.model_registry.get_provider(next_id) is not None:
        raise HTTPException(
            status_code=409,
            detail={
                "code": EC.PROVIDER_ALREADY_EXISTS,
                "message": f"Provider {next_id!r} already exists",
            },
        )
    if next_id != provider_id:
        bot.database.model_registry.rename_provider(provider_id, next_id)
        provider_id = next_id
        current = _require_provider(bot, provider_id)
    now = utc_now_iso()
    bot.database.model_registry.upsert_provider(
        ModelProviderRecord(
            id=provider_id,
            type=body.type if body.type is not None else current["type"],
            display_name=(
                body.displayName if body.displayName is not None else current["display_name"]
            ),
            capability_type=(
                body.capabilityType
                if body.capabilityType is not None
                else current.get("capability_type", "completion")
            ),
            base_url=body.baseUrl if body.baseUrl is not None else current["base_url"],
            auth=body.auth if body.auth is not None else current["auth"],
            default_params=(
                body.defaultParams if body.defaultParams is not None else current["default_params"]
            ),
            enabled=body.enabled if body.enabled is not None else current["enabled"],
            created_at=current["created_at"],
            updated_at=now,
        )
    )
    return ok(_serialize_provider(_require_provider(bot, provider_id)))


@router.delete("/providers/{provider_id:path}")
async def delete_provider(provider_id: str, bot=BotDep):
    _require_provider(bot, provider_id)
    bot.database.model_registry.delete_provider(provider_id)
    return ok({"id": provider_id, "deleted": True})


@router.get("/providers/{provider_id:path}/catalog")
async def get_provider_catalog(provider_id: str, bot=BotDep):
    provider = _require_provider(bot, provider_id)
    catalog = await _fetch_provider_catalog(provider)
    return ok(catalog)


@router.post("/providers/{provider_id:path}/probe")
async def probe_provider(provider_id: str, body: ProviderProbeRequest, bot=BotDep):
    provider = _require_provider(bot, provider_id)

    if body.modelId:
        model = _require_model(bot, body.modelId)
        if model["provider_id"] != provider_id:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": EC.INVALID_ACTION,
                    "message": f"Model {body.modelId!r} does not belong to provider {provider_id!r}",
                },
            )
    else:
        models = [
            item
            for item in bot.database.model_registry.list_models(provider_id=provider_id)
            if item["enabled"]
        ]
        model = models[0] if models else None

    started_at = utc_now_iso()

    if model is None:
        catalog = await _fetch_provider_catalog(provider)
        return ok(
            {
                "success": True,
                "providerId": provider_id,
                "mode": "catalog",
                "checkedAt": started_at,
                "latencyMs": 0,
                "catalogSize": len(catalog),
            }
        )

    capability_type = provider.get("capability_type", "completion")

    if capability_type == "embedding" or "embedding" in model["capabilities"]:
        try:
            result = await bot.model_runtime.embed(
                ModelRuntimeCall(
                    model_id=model["id"],
                    caller="webui.provider_probe",
                    purpose="provider_probe",
                    input_data="ping",
                    metadata={"probe": True},
                )
            )
        except ModelCallError as exc:
            raise HTTPException(
                status_code=502,
                detail={
                    "code": EC.INTERNAL_ERROR,
                    "message": f"Provider probe failed: {exc}",
                },
            ) from exc
        return ok(
            {
                "success": True,
                "providerId": provider_id,
                "modelId": model["id"],
                "mode": "embedding",
                "checkedAt": started_at,
                "executionId": result.execution_id,
            }
        )

    if capability_type == "completion":
        try:
            result = await bot.model_runtime.generate(
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
            raise HTTPException(
                status_code=502,
                detail={
                    "code": EC.INTERNAL_ERROR,
                    "message": f"Provider probe failed: {exc}",
                },
            ) from exc
        return ok(
            {
                "success": True,
                "providerId": provider_id,
                "modelId": model["id"],
                "mode": "chat",
                "checkedAt": started_at,
                "executionId": result.execution_id,
            }
        )

    # For rerank/tts/stt/image and other non-completion types: try catalog probe.
    # If catalog isn't supported, return a basic connectivity indication.
    try:
        catalog = await _fetch_provider_catalog(provider)
        return ok(
            {
                "success": True,
                "providerId": provider_id,
                "modelId": model["id"],
                "mode": "catalog",
                "checkedAt": started_at,
                "catalogSize": len(catalog),
            }
        )
    except Exception:
        return ok(
            {
                "success": True,
                "providerId": provider_id,
                "modelId": model["id"],
                "mode": "skipped",
                "checkedAt": started_at,
            }
        )


@router.get("/providers/{provider_id:path}")
async def get_provider(provider_id: str, bot=BotDep):
    return ok(_serialize_provider(_require_provider(bot, provider_id)))


@router.get("/models")
async def list_models(providerId: str | None = Query(default=None), bot=BotDep):
    models = bot.database.model_registry.list_models(provider_id=providerId)
    return ok([_serialize_model(item) for item in models])


@router.post("/models", status_code=201)
async def create_model(body: ModelRequest, bot=BotDep):
    if bot.database.model_registry.get_model(body.id) is not None:
        raise HTTPException(
            status_code=409,
            detail={
                "code": EC.MODEL_ALREADY_EXISTS,
                "message": f"Model {body.id!r} already exists",
            },
        )
    provider = _require_provider(bot, body.providerId)
    context_window = body.contextWindow
    if context_window is None:
        context_window = _infer_context_window(provider, body.litellmModel)

    bot.database.model_registry.upsert_model(
        ModelDefinitionRecord(
            id=body.id,
            provider_id=body.providerId,
            litellm_model=body.litellmModel,
            display_name=body.displayName,
            capabilities=body.capabilities,
            context_window=context_window,
            default_params=body.defaultParams,
            cost_metadata=body.costMetadata,
            enabled=body.enabled,
        )
    )
    return ok(_serialize_model(_require_model(bot, body.id)))


@router.get("/models/{model_id:path}")
async def get_model(model_id: str, bot=BotDep):
    return ok(_serialize_model(_require_model(bot, model_id)))


@router.patch("/models/{model_id:path}")
async def update_model(model_id: str, body: ModelPatchRequest, bot=BotDep):
    current = _require_model(bot, model_id)
    provider_id = body.providerId if body.providerId is not None else current["provider_id"]
    provider = _require_provider(bot, provider_id)
    now = utc_now_iso()
    litellm_model = body.litellmModel if body.litellmModel is not None else current["litellm_model"]
    context_window = (
        body.contextWindow if body.contextWindow is not None else current["context_window"]
    )
    if body.contextWindow is None and (
        body.litellmModel is not None or body.providerId is not None or context_window is None
    ):
        inferred_context_window = _infer_context_window(provider, litellm_model)
        if inferred_context_window is not None or context_window is None:
            context_window = inferred_context_window

    bot.database.model_registry.upsert_model(
        ModelDefinitionRecord(
            id=model_id,
            provider_id=provider_id,
            litellm_model=litellm_model,
            display_name=(
                body.displayName if body.displayName is not None else current["display_name"]
            ),
            capabilities=body.capabilities
            if body.capabilities is not None
            else current["capabilities"],
            context_window=context_window,
            default_params=(
                body.defaultParams if body.defaultParams is not None else current["default_params"]
            ),
            cost_metadata=(
                body.costMetadata if body.costMetadata is not None else current["cost_metadata"]
            ),
            enabled=body.enabled if body.enabled is not None else current["enabled"],
            created_at=current["created_at"],
            updated_at=now,
        )
    )
    return ok(_serialize_model(_require_model(bot, model_id)))


@router.delete("/models/{model_id:path}")
async def delete_model(model_id: str, bot=BotDep):
    _require_model(bot, model_id)
    bot.database.model_registry.delete_model(model_id)
    return ok({"id": model_id, "deleted": True})


@router.get("/routes")
async def list_routes(bot=BotDep):
    routes = bot.database.model_registry.list_routes()
    return ok(
        [
            _serialize_route(item, bot.database.model_registry.list_route_members(item["id"]))
            for item in routes
        ]
    )


@router.post("/routes", status_code=201)
async def create_route(body: RouteRequest, bot=BotDep):
    if bot.database.model_registry.get_route(body.id) is not None:
        raise HTTPException(
            status_code=409,
            detail={
                "code": EC.ROUTE_ALREADY_EXISTS,
                "message": f"Route {body.id!r} already exists",
            },
        )
    _validate_route_members(bot, body.members)
    now = utc_now_iso()
    bot.database.model_registry.upsert_route(
        ModelRouteRecord(
            id=body.id,
            purpose=body.purpose,
            strategy=body.strategy,
            enabled=body.enabled,
            sticky_sessions=body.stickySessions,
            metadata=body.metadata,
            created_at=now,
            updated_at=now,
        ),
        members=[
            ModelRouteMemberRecord(
                route_id=body.id,
                model_id=member.modelId,
                priority=member.priority,
                weight=member.weight,
                conditions=member.conditions,
                timeout_override=member.timeoutOverride,
                enabled=member.enabled,
            )
            for member in body.members
        ],
    )
    route = _require_route(bot, body.id)
    return ok(_serialize_route(route, bot.database.model_registry.list_route_members(body.id)))


@router.get("/routes/{route_id:path}")
async def get_route(route_id: str, bot=BotDep):
    route = _require_route(bot, route_id)
    members = bot.database.model_registry.list_route_members(route_id)
    return ok(_serialize_route(route, members))


@router.patch("/routes/{route_id:path}")
async def update_route(route_id: str, body: RoutePatchRequest, bot=BotDep):
    current = _require_route(bot, route_id)
    next_id = body.id if body.id is not None else route_id
    if next_id != route_id and bot.database.model_registry.get_route(next_id) is not None:
        raise HTTPException(
            status_code=409,
            detail={
                "code": EC.ROUTE_ALREADY_EXISTS,
                "message": f"Route {next_id!r} already exists",
            },
        )
    if next_id != route_id:
        bot.database.model_registry.rename_route(route_id, next_id)
        route_id = next_id
        current = _require_route(bot, route_id)
    members = body.members
    if members is not None:
        _validate_route_members(bot, members)

    now = utc_now_iso()
    bot.database.model_registry.upsert_route(
        ModelRouteRecord(
            id=route_id,
            purpose=body.purpose if body.purpose is not None else current["purpose"],
            strategy=body.strategy if body.strategy is not None else current["strategy"],
            enabled=body.enabled if body.enabled is not None else current["enabled"],
            sticky_sessions=(
                body.stickySessions
                if body.stickySessions is not None
                else current["sticky_sessions"]
            ),
            metadata=body.metadata if body.metadata is not None else current["metadata"],
            created_at=current["created_at"],
            updated_at=now,
        ),
        members=[
            ModelRouteMemberRecord(
                route_id=route_id,
                model_id=member.modelId,
                priority=member.priority,
                weight=member.weight,
                conditions=member.conditions,
                timeout_override=member.timeoutOverride,
                enabled=member.enabled,
            )
            for member in members
        ]
        if members is not None
        else None,
    )
    route = _require_route(bot, route_id)
    return ok(_serialize_route(route, bot.database.model_registry.list_route_members(route_id)))


@router.delete("/routes/{route_id:path}")
async def delete_route(route_id: str, bot=BotDep):
    _require_route(bot, route_id)
    bot.database.model_registry.delete_route(route_id)
    return ok({"id": route_id, "deleted": True})


@router.get("/executions")
async def list_model_executions(limit: int = Query(default=50, ge=1, le=200), bot=BotDep):
    records = bot.database.model_executions.list_recent(limit=limit)
    return ok([_serialize_execution(item) for item in records])
