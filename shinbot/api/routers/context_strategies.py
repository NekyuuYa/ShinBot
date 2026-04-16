"""Context strategy management router: /api/v1/context-strategies"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

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
    resolverRef: str
    description: str = ""
    config: dict[str, Any] = Field(default_factory=dict)
    maxContextTokens: int | None = None
    maxHistoryTurns: int | None = None
    memorySummaryRequired: bool = False
    truncatePolicy: str = "tail"
    triggerRatio: float = 0.5
    trimRatio: float = 0.1
    enabled: bool = True


class ContextStrategyPatchRequest(BaseModel):
    name: str | None = None
    resolverRef: str | None = None
    description: str | None = None
    config: dict[str, Any] | None = None
    maxContextTokens: int | None = None
    maxHistoryTurns: int | None = None
    memorySummaryRequired: bool | None = None
    truncatePolicy: str | None = None
    triggerRatio: float | None = None
    trimRatio: float | None = None
    enabled: bool | None = None


def _serialize_strategy(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "uuid": payload["uuid"],
        "name": payload["name"],
        "resolverRef": payload["resolver_ref"],
        "description": payload["description"],
        "config": payload["config"],
        "maxContextTokens": payload["max_context_tokens"],
        "maxHistoryTurns": payload["max_history_turns"],
        "memorySummaryRequired": payload["memory_summary_required"],
        "truncatePolicy": payload["truncate_policy"],
        "triggerRatio": payload["trigger_ratio"],
        "trimRatio": payload["trim_ratio"],
        "enabled": payload["enabled"],
        "createdAt": payload["created_at"],
        "lastModified": payload["updated_at"],
    }


def _normalize_strategy_input(
    *,
    name: str,
    resolver_ref: str,
    truncate_policy: str,
    trigger_ratio: float,
    trim_ratio: float,
) -> tuple[str, str, str]:
    normalized_name = name.strip()
    normalized_resolver = resolver_ref.strip()
    normalized_policy = truncate_policy.strip()
    if not normalized_name:
        raise HTTPException(
            status_code=400,
            detail={
                "code": EC.INVALID_ACTION,
                "message": "Context strategy name must not be empty",
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
    if not normalized_policy:
        raise HTTPException(
            status_code=400,
            detail={
                "code": EC.INVALID_ACTION,
                "message": "Context strategy truncatePolicy must not be empty",
            },
        )
    if not 0 < trigger_ratio <= 1:
        raise HTTPException(
            status_code=400,
            detail={
                "code": EC.INVALID_ACTION,
                "message": "Context strategy triggerRatio must be within (0, 1]",
            },
        )
    if not 0 < trim_ratio <= 1:
        raise HTTPException(
            status_code=400,
            detail={
                "code": EC.INVALID_ACTION,
                "message": "Context strategy trimRatio must be within (0, 1]",
            },
        )
    return normalized_name, normalized_resolver, normalized_policy


@router.get("")
def list_context_strategies(bot=BotDep):
    return ok([_serialize_strategy(item) for item in bot.database.context_strategies.list()])


@router.post("", status_code=201)
def create_context_strategy(body: ContextStrategyRequest, bot=BotDep):
    name, resolver_ref, truncate_policy = _normalize_strategy_input(
        name=body.name,
        resolver_ref=body.resolverRef,
        truncate_policy=body.truncatePolicy,
        trigger_ratio=body.triggerRatio,
        trim_ratio=body.trimRatio,
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
        resolver_ref=resolver_ref,
        description=body.description,
        config=body.config,
        max_context_tokens=body.maxContextTokens,
        max_history_turns=body.maxHistoryTurns,
        memory_summary_required=body.memorySummaryRequired,
        truncate_policy=truncate_policy,
        trigger_ratio=body.triggerRatio,
        trim_ratio=body.trimRatio,
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
    next_truncate_policy = (
        body.truncatePolicy
        if body.truncatePolicy is not None
        else str(current["truncate_policy"])
    )
    normalized_name, normalized_resolver, normalized_policy = _normalize_strategy_input(
        name=next_name,
        resolver_ref=next_resolver_ref,
        truncate_policy=next_truncate_policy,
        trigger_ratio=(
            body.triggerRatio if body.triggerRatio is not None else float(current["trigger_ratio"])
        ),
        trim_ratio=body.trimRatio if body.trimRatio is not None else float(current["trim_ratio"]),
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
            resolver_ref=normalized_resolver,
            description=(
                body.description if body.description is not None else str(current["description"])
            ),
            config=body.config if body.config is not None else dict(current["config"]),
            max_context_tokens=(
                body.maxContextTokens
                if body.maxContextTokens is not None
                else current["max_context_tokens"]
            ),
            max_history_turns=(
                body.maxHistoryTurns
                if body.maxHistoryTurns is not None
                else current["max_history_turns"]
            ),
            memory_summary_required=(
                body.memorySummaryRequired
                if body.memorySummaryRequired is not None
                else bool(current["memory_summary_required"])
            ),
            truncate_policy=normalized_policy,
            trigger_ratio=(
                body.triggerRatio
                if body.triggerRatio is not None
                else float(current["trigger_ratio"])
            ),
            trim_ratio=(
                body.trimRatio if body.trimRatio is not None else float(current["trim_ratio"])
            ),
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
