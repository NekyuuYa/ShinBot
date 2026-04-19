"""Context strategy management router: /api/v1/context-strategies"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from shinbot.agent.prompt_manager.registry import PromptRegistry
from shinbot.api.deps import AuthRequired, BotDep
from shinbot.api.models import EC, ok
from shinbot.core.context_strategy_admin import (
    ContextStrategyAdminError,
    assert_context_strategy_name_available,
    build_context_strategy_record,
    get_context_strategy_or_raise,
    normalize_context_strategy_input,
    serialize_context_strategy,
)

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


def _raise_admin_http_error(exc: ContextStrategyAdminError) -> None:
    raise HTTPException(
        status_code=exc.status_code,
        detail={"code": exc.code, "message": exc.message},
    ) from exc


@router.get("")
def list_context_strategies(bot=BotDep):
    return ok(
        [serialize_context_strategy(item) for item in bot.database.context_strategies.list()]
    )


@router.post("", status_code=201)
def create_context_strategy(body: ContextStrategyRequest, bot=BotDep):
    try:
        name, strategy_type, resolver_ref = normalize_context_strategy_input(
            name=body.name,
            strategy_type=body.type,
            resolver_ref=body.resolverRef,
        )
        assert_context_strategy_name_available(bot.database, name, current_uuid=None)
        record = build_context_strategy_record(
            strategy_uuid=None,
            name=name,
            strategy_type=strategy_type,
            resolver_ref=resolver_ref,
            description=body.description,
            config=body.config,
            enabled=body.enabled,
        )
    except ContextStrategyAdminError as exc:
        _raise_admin_http_error(exc)

    bot.database.context_strategies.upsert(record)
    payload = bot.database.context_strategies.get(record.uuid)
    assert payload is not None
    return ok(serialize_context_strategy(payload))


@router.get("/{strategy_uuid}")
def get_context_strategy(strategy_uuid: str, bot=BotDep):
    try:
        payload = get_context_strategy_or_raise(bot.database, strategy_uuid)
    except ContextStrategyAdminError as exc:
        _raise_admin_http_error(exc)
    return ok(serialize_context_strategy(payload))


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
    try:
        current = get_context_strategy_or_raise(bot.database, strategy_uuid)
        next_name = body.name if body.name is not None else str(current["name"])
        next_resolver_ref = (
            body.resolverRef if body.resolverRef is not None else str(current["resolver_ref"])
        )
        next_type = body.type if body.type is not None else str(current["type"])
        normalized_name, normalized_type, normalized_resolver = normalize_context_strategy_input(
            name=next_name,
            strategy_type=next_type,
            resolver_ref=next_resolver_ref,
        )
        assert_context_strategy_name_available(
            bot.database,
            normalized_name,
            current_uuid=strategy_uuid,
        )
        record = build_context_strategy_record(
            strategy_uuid=strategy_uuid,
            name=normalized_name,
            strategy_type=normalized_type,
            resolver_ref=normalized_resolver,
            description=(
                body.description if body.description is not None else str(current["description"])
            ),
            config=body.config if body.config is not None else dict(current["config"]),
            enabled=body.enabled if body.enabled is not None else bool(current["enabled"]),
            created_at=str(current["created_at"]),
        )
    except ContextStrategyAdminError as exc:
        _raise_admin_http_error(exc)

    bot.database.context_strategies.upsert(record)
    payload = bot.database.context_strategies.get(strategy_uuid)
    assert payload is not None
    return ok(serialize_context_strategy(payload))


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
    try:
        get_context_strategy_or_raise(bot.database, strategy_uuid)
    except ContextStrategyAdminError as exc:
        _raise_admin_http_error(exc)
    bot.database.context_strategies.delete(strategy_uuid)
    return ok({"deleted": True, "uuid": strategy_uuid})
