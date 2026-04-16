"""Context strategy management router: /api/v1/context-strategies"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from shinbot.agent.prompting.registry import PromptRegistry
from shinbot.api.deps import AuthRequired, BotDep
from shinbot.api.models import EC, ok
from shinbot.persistence.records import ContextStrategyRecord, utc_now_iso

router = APIRouter(
    prefix="/context-strategies",
    tags=["context-strategies"],
    dependencies=AuthRequired,
)


class ContextStrategyRequest(BaseModel):
    name: str
    type: str
    resolverRef: str
    description: str = ""
    config: dict[str, Any] = Field(default_factory=dict)
    enabled: bool = True


class ContextStrategyPatchRequest(BaseModel):
    name: str | None = None
    type: str | None = None
    resolverRef: str | None = None
    description: str | None = None
    config: dict[str, Any] | None = None
    enabled: bool | None = None


def _serialize_strategy(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "uuid": payload["uuid"],
        "name": payload["name"],
        "type": payload["type"],
        "resolverRef": payload["resolver_ref"],
        "description": payload["description"],
        "config": payload["config"],
        "enabled": payload["enabled"],
        "createdAt": payload["created_at"],
        "lastModified": payload["updated_at"],
    }


def _normalize_strategy_input(
    *,
    name: str,
    strategy_type: str,
    resolver_ref: str,
) -> tuple[str, str, str]:
    normalized_name = name.strip()
    normalized_type = strategy_type.strip()
    normalized_resolver = resolver_ref.strip()
    if not normalized_name:
        raise HTTPException(
            status_code=400,
            detail={
                "code": EC.INVALID_ACTION,
                "message": "Context strategy name must not be empty",
            },
        )
    if not normalized_type:
        raise HTTPException(
            status_code=400,
            detail={
                "code": EC.INVALID_ACTION,
                "message": "Context strategy type must not be empty",
            },
        )
    if not normalized_resolver:
        raise HTTPException(
            status_code=400,
            detail={
                "code": EC.INVALID_ACTION,
                "message": "Context strategy resolverRef must not be empty",
            },
        )
    return normalized_name, normalized_type, normalized_resolver


@router.get("")
def list_context_strategies(bot=BotDep):
    return ok([_serialize_strategy(item) for item in bot.database.context_strategies.list()])


@router.post("", status_code=201)
def create_context_strategy(body: ContextStrategyRequest, bot=BotDep):
    name, strategy_type, resolver_ref = _normalize_strategy_input(
        name=body.name,
        strategy_type=body.type,
        resolver_ref=body.resolverRef,
    )
    if bot.database.context_strategies.get_by_name(name) is not None:
        raise HTTPException(
            status_code=409,
            detail={
                "code": EC.CONTEXT_STRATEGY_ALREADY_EXISTS,
                "message": f"Context strategy {name!r} already exists",
            },
        )

    now = utc_now_iso()
    record = ContextStrategyRecord(
        uuid=str(uuid4()),
        name=name,
        type=strategy_type,
        resolver_ref=resolver_ref,
        description=body.description,
        config=body.config,
        enabled=body.enabled,
        created_at=now,
        updated_at=now,
    )
    bot.database.context_strategies.upsert(record)
    payload = bot.database.context_strategies.get(record.uuid)
    assert payload is not None
    return ok(_serialize_strategy(payload))


@router.get("/{strategy_uuid}")
def get_context_strategy(strategy_uuid: str, bot=BotDep):
    payload = bot.database.context_strategies.get(strategy_uuid)
    if payload is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.CONTEXT_STRATEGY_NOT_FOUND,
                "message": f"Context strategy {strategy_uuid!r} was not found",
            },
        )
    return ok(_serialize_strategy(payload))


@router.patch("/{strategy_uuid}")
def patch_context_strategy(strategy_uuid: str, body: ContextStrategyPatchRequest, bot=BotDep):
    if strategy_uuid == PromptRegistry.BUILTIN_SLIDING_WINDOW_CONTEXT_STRATEGY_ID:
        raise HTTPException(
            status_code=400,
            detail={
                "code": EC.INVALID_ACTION,
                "message": "Built-in context strategy cannot be modified",
            },
        )
    current = bot.database.context_strategies.get(strategy_uuid)
    if current is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.CONTEXT_STRATEGY_NOT_FOUND,
                "message": f"Context strategy {strategy_uuid!r} was not found",
            },
        )

    next_name = body.name if body.name is not None else str(current["name"])
    next_resolver_ref = body.resolverRef if body.resolverRef is not None else str(
        current["resolver_ref"]
    )
    next_type = body.type if body.type is not None else str(current["type"])
    normalized_name, normalized_type, normalized_resolver = _normalize_strategy_input(
        name=next_name,
        strategy_type=next_type,
        resolver_ref=next_resolver_ref,
    )

    existing = bot.database.context_strategies.get_by_name(normalized_name)
    if existing is not None and existing["uuid"] != strategy_uuid:
        raise HTTPException(
            status_code=409,
            detail={
                "code": EC.CONTEXT_STRATEGY_ALREADY_EXISTS,
                "message": f"Context strategy {normalized_name!r} already exists",
            },
        )

    bot.database.context_strategies.upsert(
        ContextStrategyRecord(
            uuid=strategy_uuid,
            name=normalized_name,
            type=normalized_type,
            resolver_ref=normalized_resolver,
            description=(
                body.description if body.description is not None else str(current["description"])
            ),
            config=body.config if body.config is not None else dict(current["config"]),
            enabled=body.enabled if body.enabled is not None else bool(current["enabled"]),
            created_at=str(current["created_at"]),
            updated_at=utc_now_iso(),
        )
    )
    payload = bot.database.context_strategies.get(strategy_uuid)
    assert payload is not None
    return ok(_serialize_strategy(payload))


@router.delete("/{strategy_uuid}")
def delete_context_strategy(strategy_uuid: str, bot=BotDep):
    if strategy_uuid == PromptRegistry.BUILTIN_SLIDING_WINDOW_CONTEXT_STRATEGY_ID:
        raise HTTPException(
            status_code=400,
            detail={
                "code": EC.INVALID_ACTION,
                "message": "Built-in context strategy cannot be deleted",
            },
        )
    current = bot.database.context_strategies.get(strategy_uuid)
    if current is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.CONTEXT_STRATEGY_NOT_FOUND,
                "message": f"Context strategy {strategy_uuid!r} was not found",
            },
        )
    bot.database.context_strategies.delete(strategy_uuid)
    return ok({"deleted": True, "uuid": strategy_uuid})
