"""Administrative helpers for context-strategy management flows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from shinbot.persistence.records import ContextStrategyRecord, utc_now_iso
from shinbot.schema.context_strategies import BUILTIN_SLIDING_WINDOW_CONTEXT_STRATEGY_ID


@dataclass(slots=True)
class ContextStrategyAdminError(RuntimeError):
    """Structured admin-layer error for API adapters."""

    status_code: int
    code: str
    message: str

    def __str__(self) -> str:
        return self.message


def serialize_context_strategy(payload: dict[str, Any]) -> dict[str, Any]:
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


def normalize_context_strategy_input(
    *,
    name: str,
    strategy_type: str,
    resolver_ref: str,
) -> tuple[str, str, str]:
    normalized_name = name.strip()
    normalized_type = strategy_type.strip()
    normalized_resolver = resolver_ref.strip()
    if not normalized_name:
        raise ContextStrategyAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="Context strategy name must not be empty",
        )
    if not normalized_type:
        raise ContextStrategyAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="Context strategy type must not be empty",
        )
    if not normalized_resolver:
        raise ContextStrategyAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="Context strategy resolverRef must not be empty",
        )
    return normalized_name, normalized_type, normalized_resolver


def get_context_strategy_or_raise(database: Any, strategy_uuid: str) -> dict[str, Any]:
    payload = database.context_strategies.get(strategy_uuid)
    if payload is None:
        raise ContextStrategyAdminError(
            status_code=404,
            code="CONTEXT_STRATEGY_NOT_FOUND",
            message=f"Context strategy {strategy_uuid!r} was not found",
        )
    return payload


def assert_context_strategy_mutable(payload: dict[str, Any]) -> None:
    config = payload.get("config")
    is_builtin = payload.get("uuid") == BUILTIN_SLIDING_WINDOW_CONTEXT_STRATEGY_ID
    if isinstance(config, dict):
        is_builtin = is_builtin or bool(config.get("builtin"))
    if is_builtin:
        raise ContextStrategyAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="Built-in context strategies cannot be modified",
        )


def assert_context_strategy_name_available(
    database: Any,
    name: str,
    *,
    current_uuid: str | None,
) -> None:
    existing = database.context_strategies.get_by_name(name)
    if existing is not None and existing["uuid"] != current_uuid:
        raise ContextStrategyAdminError(
            status_code=409,
            code="CONTEXT_STRATEGY_ALREADY_EXISTS",
            message=f"Context strategy {name!r} already exists",
        )


def build_context_strategy_record(
    *,
    strategy_uuid: str | None,
    name: str,
    strategy_type: str,
    resolver_ref: str,
    description: str,
    config: dict[str, Any],
    enabled: bool,
    created_at: str | None = None,
) -> ContextStrategyRecord:
    now = utc_now_iso()
    return ContextStrategyRecord(
        uuid=strategy_uuid or str(uuid4()),
        name=name,
        type=strategy_type,
        resolver_ref=resolver_ref,
        description=description,
        config=config,
        enabled=enabled,
        created_at=created_at or now,
        updated_at=now,
    )
