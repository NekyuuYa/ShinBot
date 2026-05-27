"""Model runtime management router: /api/v1/model-runtime"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from shinbot.agent.services.model_runtime.admin import (
    ModelRuntimeAdminError,
    fetch_provider_catalog,
    get_model_or_raise,
    get_provider_or_raise,
    get_route_or_raise,
    infer_context_window,
    probe_provider_runtime,
    validate_route_member_ids,
)
from shinbot.agent.services.model_runtime.audit_store import ModelAuditPayloadStore
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


def _normalize_cost_metadata(cost_metadata: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(cost_metadata)
    price_fields = (
        "inputPerMillionTokens",
        "outputPerMillionTokens",
        "cacheWritePerMillionTokens",
        "cacheReadPerMillionTokens",
    )
    for field in price_fields:
        value = normalized.get(field)
        if value is None or value == "":
            normalized[field] = None
            continue
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=422,
                detail={
                    "code": EC.INVALID_ACTION,
                    "message": f"Invalid cost metadata field {field!r}: expected a number",
                },
            ) from exc
        if parsed < 0:
            raise HTTPException(
                status_code=422,
                detail={
                    "code": EC.INVALID_ACTION,
                    "message": f"Invalid cost metadata field {field!r}: must be >= 0",
                },
            )
        normalized[field] = parsed
    return normalized


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
    metadata = dict(payload["metadata"] or {})
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
        "promptSnapshotId": payload.get("prompt_snapshot_id", ""),
        "metadata": metadata,
        "auditPayloadRef": str(metadata.get("audit_payload_ref") or ""),
        "auditPayloadExpiresAt": str(metadata.get("audit_payload_expires_at") or ""),
        "auditPayloadAvailable": bool(metadata.get("audit_payload_ref")),
    }


def _serialize_execution_audit_page(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "items": [_serialize_execution(item) for item in payload["items"]],
        "total": payload["total"],
        "limit": payload["limit"],
        "offset": payload["offset"],
    }


def _serialize_token_summary(
    payload: dict[str, Any],
    *,
    days: int,
    since: str,
) -> dict[str, Any]:
    return {
        "windowDays": days,
        "since": since,
        "totalCalls": payload["total_calls"],
        "successfulCalls": payload["successful_calls"],
        "inputTokens": payload["input_tokens"],
        "outputTokens": payload["output_tokens"],
        "totalTokens": payload["total_tokens"],
        "cacheReadTokens": payload["cache_read_tokens"],
        "cacheWriteTokens": payload["cache_write_tokens"],
        "estimatedCost": payload["estimated_cost"],
        "currency": "USD",
        "topModels": [
            {
                "providerId": item["provider_id"],
                "modelId": item["model_id"],
                "totalCalls": item["total_calls"],
                "inputTokens": item["input_tokens"],
                "outputTokens": item["output_tokens"],
                "totalTokens": item["total_tokens"],
                "cacheReadTokens": item["cache_read_tokens"],
                "cacheWriteTokens": item["cache_write_tokens"],
            }
            for item in payload["top_models"]
        ],
    }


def _serialize_cost_bucket(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "bucketStart": payload["bucket_start"],
        "totalCalls": payload["total_calls"],
        "successfulCalls": payload["successful_calls"],
        "failedCalls": payload["failed_calls"],
        "cacheHits": payload["cache_hits"],
        "inputTokens": payload["input_tokens"],
        "outputTokens": payload["output_tokens"],
        "totalTokens": payload["total_tokens"],
        "cacheReadTokens": payload["cache_read_tokens"],
        "cacheWriteTokens": payload["cache_write_tokens"],
        "estimatedCost": payload["estimated_cost"],
    }


def _serialize_cost_model(payload: dict[str, Any]) -> dict[str, Any]:
    data = {
        "providerId": payload["provider_id"],
        "providerDisplayName": payload["provider_display_name"],
        "modelId": payload["model_id"],
        "modelDisplayName": payload["model_display_name"],
        "totalCalls": payload["total_calls"],
        "successfulCalls": payload["successful_calls"],
        "failedCalls": payload["failed_calls"],
        "successRate": payload["success_rate"],
        "cacheHits": payload["cache_hits"],
        "cacheHitRate": payload["cache_hit_rate"],
        "inputTokens": payload["input_tokens"],
        "outputTokens": payload["output_tokens"],
        "totalTokens": payload["total_tokens"],
        "cacheReadTokens": payload["cache_read_tokens"],
        "cacheWriteTokens": payload["cache_write_tokens"],
        "estimatedCost": payload["estimated_cost"],
        "averageLatencyMs": payload["average_latency_ms"],
        "averageTimeToFirstTokenMs": payload["average_time_to_first_token_ms"],
        "lastSeenAt": payload["last_seen_at"],
    }
    if "daily" in payload:
        data["daily"] = [_serialize_cost_bucket(item) for item in payload["daily"]]
    if "hourly" in payload:
        data["hourly"] = [_serialize_cost_bucket(item) for item in payload["hourly"]]
    return data


def _serialize_cost_analysis(
    payload: dict[str, Any],
    *,
    days: int,
    since: str,
    hourly_since: str,
) -> dict[str, Any]:
    summary = payload["summary"]
    return {
        "windowDays": days,
        "since": since,
        "hourlySince": hourly_since,
        "currency": payload["currency"],
        "summary": {
            "totalCalls": summary["total_calls"],
            "successfulCalls": summary["successful_calls"],
            "failedCalls": summary["failed_calls"],
            "successRate": summary["success_rate"],
            "cacheHits": summary["cache_hits"],
            "cacheHitRate": summary["cache_hit_rate"],
            "inputTokens": summary["input_tokens"],
            "outputTokens": summary["output_tokens"],
            "totalTokens": summary["total_tokens"],
            "cacheReadTokens": summary["cache_read_tokens"],
            "cacheWriteTokens": summary["cache_write_tokens"],
            "estimatedCost": summary["estimated_cost"],
            "averageLatencyMs": summary["average_latency_ms"],
            "averageTimeToFirstTokenMs": summary["average_time_to_first_token_ms"],
        },
        "timeline": {
            "daily": [_serialize_cost_bucket(item) for item in payload["timeline"]["daily"]],
            "hourly": [_serialize_cost_bucket(item) for item in payload["timeline"]["hourly"]],
        },
        "models": [_serialize_cost_model(item) for item in payload["models"]],
        "focusModels": [_serialize_cost_model(item) for item in payload["focus_models"]],
    }


def _raise_admin_http_error(exc: ModelRuntimeAdminError) -> None:
    raise HTTPException(
        status_code=exc.status_code,
        detail={"code": exc.code, "message": exc.message},
    ) from exc


def _require_provider(database: Any, provider_id: str) -> dict[str, Any]:
    try:
        return get_provider_or_raise(database, provider_id)
    except ModelRuntimeAdminError as exc:
        _raise_admin_http_error(exc)


def _require_model(database: Any, model_id: str) -> dict[str, Any]:
    try:
        return get_model_or_raise(database, model_id)
    except ModelRuntimeAdminError as exc:
        _raise_admin_http_error(exc)


def _require_route(database: Any, route_id: str) -> dict[str, Any]:
    try:
        return get_route_or_raise(database, route_id)
    except ModelRuntimeAdminError as exc:
        _raise_admin_http_error(exc)


def _validate_route_members(database: Any, members: list[RouteMemberRequest]) -> None:
    try:
        validate_route_member_ids(database, [member.modelId for member in members])
    except ModelRuntimeAdminError as exc:
        _raise_admin_http_error(exc)


async def _fetch_provider_catalog(database: Any, provider_id: str) -> list[dict[str, Any]]:
    try:
        provider = get_provider_or_raise(database, provider_id)
        return await fetch_provider_catalog(provider)
    except ModelRuntimeAdminError as exc:
        _raise_admin_http_error(exc)


@router.get("/providers")
async def list_providers(bot=BotDep):
    """List all registered model providers."""
    providers = bot.database.model_registry.list_providers()
    return ok([_serialize_provider(item) for item in providers])


@router.post("/providers", status_code=201)
async def create_provider(body: ProviderRequest, bot=BotDep):
    """Create a new model provider.

    Args:
        body: Provider creation request payload.
        bot: Bot dependency injection.
    """
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
    return ok(_serialize_provider(_require_provider(bot.database, body.id)))


@router.patch("/providers/{provider_id:path}")
async def update_provider(provider_id: str, body: ProviderPatchRequest, bot=BotDep):
    """Update an existing model provider.

    Args:
        provider_id: The provider identifier.
        body: Provider patch request payload.
        bot: Bot dependency injection.
    """
    current = _require_provider(bot.database, provider_id)
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
        current = _require_provider(bot.database, provider_id)
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
    return ok(_serialize_provider(_require_provider(bot.database, provider_id)))


@router.delete("/providers/{provider_id:path}")
async def delete_provider(provider_id: str, bot=BotDep):
    """Delete a model provider by identifier.

    Args:
        provider_id: The provider identifier to delete.
        bot: Bot dependency injection.
    """
    _require_provider(bot.database, provider_id)
    bot.database.model_registry.delete_provider(provider_id)
    return ok({"id": provider_id, "deleted": True})


@router.get("/providers/{provider_id:path}/catalog")
async def get_provider_catalog(provider_id: str, bot=BotDep):
    """Fetch the model catalog for a given provider.

    Args:
        provider_id: The provider identifier.
        bot: Bot dependency injection.
    """
    catalog = await _fetch_provider_catalog(bot.database, provider_id)
    return ok(catalog)


@router.post("/providers/{provider_id:path}/probe")
async def probe_provider(provider_id: str, body: ProviderProbeRequest, bot=BotDep):
    """Probe a provider's runtime connectivity for a specific model.

    Args:
        provider_id: The provider identifier.
        body: Probe request with optional model ID.
        bot: Bot dependency injection.
    """
    try:
        return ok(
            await probe_provider_runtime(
                database=bot.database,
                model_runtime=bot.model_runtime,
                provider_id=provider_id,
                model_id=body.modelId,
                checked_at=utc_now_iso(),
            )
        )
    except ModelRuntimeAdminError as exc:
        _raise_admin_http_error(exc)


@router.get("/providers/{provider_id:path}")
async def get_provider(provider_id: str, bot=BotDep):
    """Get a single provider by identifier.

    Args:
        provider_id: The provider identifier.
        bot: Bot dependency injection.
    """
    return ok(_serialize_provider(_require_provider(bot.database, provider_id)))


@router.get("/models")
async def list_models(providerId: str | None = Query(default=None), bot=BotDep):
    """List all registered models, optionally filtered by provider.

    Args:
        providerId: Optional provider ID to filter by.
        bot: Bot dependency injection.
    """
    models = bot.database.model_registry.list_models(provider_id=providerId)
    return ok([_serialize_model(item) for item in models])


@router.post("/models", status_code=201)
async def create_model(body: ModelRequest, bot=BotDep):
    """Create a new model definition.

    Args:
        body: Model creation request payload.
        bot: Bot dependency injection.
    """
    if bot.database.model_registry.get_model(body.id) is not None:
        raise HTTPException(
            status_code=409,
            detail={
                "code": EC.MODEL_ALREADY_EXISTS,
                "message": f"Model {body.id!r} already exists",
            },
        )
    provider = _require_provider(bot.database, body.providerId)
    context_window = body.contextWindow
    if context_window is None:
        context_window = infer_context_window(provider, body.litellmModel)
    cost_metadata = _normalize_cost_metadata(body.costMetadata)

    bot.database.model_registry.upsert_model(
        ModelDefinitionRecord(
            id=body.id,
            provider_id=body.providerId,
            litellm_model=body.litellmModel,
            display_name=body.displayName,
            capabilities=body.capabilities,
            context_window=context_window,
            default_params=body.defaultParams,
            cost_metadata=cost_metadata,
            enabled=body.enabled,
        )
    )
    return ok(_serialize_model(_require_model(bot.database, body.id)))


@router.get("/models/{model_id:path}")
async def get_model(model_id: str, bot=BotDep):
    """Get a single model by identifier.

    Args:
        model_id: The model identifier.
        bot: Bot dependency injection.
    """
    return ok(_serialize_model(_require_model(bot.database, model_id)))


@router.patch("/models/{model_id:path}")
async def update_model(model_id: str, body: ModelPatchRequest, bot=BotDep):
    """Update an existing model definition.

    Args:
        model_id: The model identifier.
        body: Model patch request payload.
        bot: Bot dependency injection.
    """
    current = _require_model(bot.database, model_id)
    provider_id = body.providerId if body.providerId is not None else current["provider_id"]
    provider = _require_provider(bot.database, provider_id)
    now = utc_now_iso()
    litellm_model = body.litellmModel if body.litellmModel is not None else current["litellm_model"]
    context_window = (
        body.contextWindow if body.contextWindow is not None else current["context_window"]
    )
    if body.contextWindow is None and (
        body.litellmModel is not None or body.providerId is not None or context_window is None
    ):
        inferred_context_window = infer_context_window(provider, litellm_model)
        if inferred_context_window is not None or context_window is None:
            context_window = inferred_context_window
    cost_metadata = (
        _normalize_cost_metadata(body.costMetadata)
        if body.costMetadata is not None
        else current["cost_metadata"]
    )

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
            cost_metadata=cost_metadata,
            enabled=body.enabled if body.enabled is not None else current["enabled"],
            created_at=current["created_at"],
            updated_at=now,
        )
    )
    return ok(_serialize_model(_require_model(bot.database, model_id)))


@router.delete("/models/{model_id:path}")
async def delete_model(model_id: str, bot=BotDep):
    """Delete a model definition by identifier.

    Args:
        model_id: The model identifier to delete.
        bot: Bot dependency injection.
    """
    _require_model(bot.database, model_id)
    bot.database.model_registry.delete_model(model_id)
    return ok({"id": model_id, "deleted": True})


@router.get("/routes")
async def list_routes(bot=BotDep):
    """List all model routes with their members."""
    routes = bot.database.model_registry.list_routes()
    return ok(
        [
            _serialize_route(item, bot.database.model_registry.list_route_members(item["id"]))
            for item in routes
        ]
    )


@router.post("/routes", status_code=201)
async def create_route(body: RouteRequest, bot=BotDep):
    """Create a new model route with members.

    Args:
        body: Route creation request payload.
        bot: Bot dependency injection.
    """
    if bot.database.model_registry.get_route(body.id) is not None:
        raise HTTPException(
            status_code=409,
            detail={
                "code": EC.ROUTE_ALREADY_EXISTS,
                "message": f"Route {body.id!r} already exists",
            },
        )
    _validate_route_members(bot.database, body.members)
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
    route = _require_route(bot.database, body.id)
    return ok(_serialize_route(route, bot.database.model_registry.list_route_members(body.id)))


@router.get("/routes/{route_id:path}")
async def get_route(route_id: str, bot=BotDep):
    """Get a single route by identifier including its members.

    Args:
        route_id: The route identifier.
        bot: Bot dependency injection.
    """
    route = _require_route(bot.database, route_id)
    members = bot.database.model_registry.list_route_members(route_id)
    return ok(_serialize_route(route, members))


@router.patch("/routes/{route_id:path}")
async def update_route(route_id: str, body: RoutePatchRequest, bot=BotDep):
    """Update an existing model route and its members.

    Args:
        route_id: The route identifier.
        body: Route patch request payload.
        bot: Bot dependency injection.
    """
    current = _require_route(bot.database, route_id)
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
        current = _require_route(bot.database, route_id)
    members = body.members
    if members is not None:
        _validate_route_members(bot.database, members)

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
    route = _require_route(bot.database, route_id)
    return ok(_serialize_route(route, bot.database.model_registry.list_route_members(route_id)))


@router.delete("/routes/{route_id:path}")
async def delete_route(route_id: str, bot=BotDep):
    """Delete a model route by identifier.

    Args:
        route_id: The route identifier to delete.
        bot: Bot dependency injection.
    """
    _require_route(bot.database, route_id)
    bot.database.model_registry.delete_route(route_id)
    return ok({"id": route_id, "deleted": True})


@router.get("/executions")
async def list_model_executions(limit: int = Query(default=50, ge=1, le=200), bot=BotDep):
    """List recent model execution records.

    Args:
        limit: Maximum number of records to return.
        bot: Bot dependency injection.
    """
    records = bot.database.model_executions.list_recent(limit=limit)
    return ok([_serialize_execution(item) for item in records])


@router.get("/executions/audit")
async def list_model_execution_audit_records(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    providerId: str | None = Query(default=None),
    modelId: str | None = Query(default=None),
    routeId: str | None = Query(default=None),
    caller: str | None = Query(default=None),
    sessionId: str | None = Query(default=None),
    instanceId: str | None = Query(default=None),
    success: bool | None = Query(default=None),
    query: str | None = Query(default=None, max_length=200),
    bot=BotDep,
):
    """List model execution audit records with filtering and pagination.

    Args:
        limit: Maximum number of records to return.
        offset: Number of records to skip.
        providerId: Optional provider ID filter.
        modelId: Optional model ID filter.
        routeId: Optional route ID filter.
        caller: Optional caller filter.
        sessionId: Optional session ID filter.
        instanceId: Optional instance ID filter.
        success: Optional success status filter.
        query: Optional text search query.
        bot: Bot dependency injection.
    """
    records = bot.database.model_executions.list_audit_records(
        limit=limit,
        offset=offset,
        provider_id=providerId,
        model_id=modelId,
        route_id=routeId,
        caller=caller,
        session_id=sessionId,
        instance_id=instanceId,
        success=success,
        query=query.strip() if query else None,
    )
    return ok(_serialize_execution_audit_page(records))


@router.get("/executions/{execution_id:path}/payload")
async def get_model_execution_payload(execution_id: str, bot=BotDep):
    """Retrieve the audit payload for a specific model execution.

    Args:
        execution_id: The execution identifier.
        bot: Bot dependency injection.
    """
    store = ModelAuditPayloadStore(bot.database.config.data_dir)
    payload = store.read(execution_id)
    if payload is None:
        return ok(
            {
                "available": False,
                "executionId": execution_id,
                "expired": False,
                "request": None,
                "response": None,
                "error": None,
                "meta": None,
            }
        )
    return ok(
        {
            "available": True,
            "executionId": execution_id,
            "expired": False,
            "request": payload.get("request"),
            "response": payload.get("response"),
            "return": payload.get("return"),
            "error": payload.get("error"),
            "meta": payload.get("meta"),
        }
    )


@router.get("/token-summary")
async def get_token_summary(days: int = Query(default=7, ge=1, le=365), bot=BotDep):
    """Get a token usage summary over a rolling time window.

    Args:
        days: Number of days to include in the summary.
        bot: Bot dependency injection.
    """
    since = (datetime.now(UTC) - timedelta(days=days)).isoformat()
    summary = bot.database.model_executions.summarize_tokens(since=since)
    return ok(_serialize_token_summary(summary, days=days, since=since))


@router.get("/cost-analysis")
async def get_cost_analysis(
    days: int = Query(default=7, ge=1, le=30),
    modelLimit: int = Query(default=8, ge=1, le=16),
    bot=BotDep,
):
    """Get a detailed cost analysis with per-model breakdowns.

    Args:
        days: Number of days to include in the analysis.
        modelLimit: Maximum number of models to include in breakdowns.
        bot: Bot dependency injection.
    """
    now = datetime.now(UTC)
    since_dt = (now - timedelta(days=days - 1)).replace(hour=0, minute=0, second=0, microsecond=0)
    hourly_since_dt = (now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=23))
    analysis = bot.database.model_executions.analyze_costs(
        since=since_dt.isoformat(),
        hourly_since=hourly_since_dt.isoformat(),
        model_limit=modelLimit,
    )
    return ok(
        _serialize_cost_analysis(
            analysis,
            days=days,
            since=analysis["since"],
            hourly_since=analysis["hourly_since"],
        )
    )
