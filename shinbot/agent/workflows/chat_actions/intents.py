"""Pure normalization for model-selected externally visible chat actions."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from shinbot.agent.runtime.session_actor.external_actions import (
    ExternalActionIntent,
    ExternalActionKind,
)
from shinbot.agent.workflows.action_mode import ExternalActionToolMode

EXTERNAL_ACTION_TOOL_NAMES = frozenset(item.value for item in ExternalActionKind)

_RUNTIME_RESERVED_FIELDS = frozenset(
    {
        "claim_id",
        "contract_signature",
        "contract_version",
        "effect_id",
        "idempotency_key",
        "operation_id",
        "ownership_generation",
        "profile_id",
        "session_id",
    }
)
_SEMANTIC_FIELDS = frozenset({"intensity", "reason", "terminate_round"})


def collect_external_action_intent(
    *,
    tool_call_id: str,
    tool_name: str,
    arguments: Mapping[str, Any],
    action_ordinal: int,
) -> ExternalActionIntent:
    """Validate one model tool call without performing its external action."""

    normalized_name = str(tool_name or "").strip()
    try:
        kind = ExternalActionKind(normalized_name)
    except ValueError as exc:
        raise ValueError(f"unsupported external action tool: {normalized_name!r}") from exc
    values = {str(key): value for key, value in arguments.items()}
    reserved = sorted(_RUNTIME_RESERVED_FIELDS.intersection(values))
    if reserved:
        raise ValueError(
            "external action arguments contain runtime-reserved fields: "
            + ", ".join(reserved)
        )
    if kind is ExternalActionKind.SEND_REPLY:
        payload = _normalize_reply(values)
    elif kind is ExternalActionKind.SEND_POKE:
        payload = _normalize_poke(values)
    else:
        payload = _normalize_reaction(values)
    return ExternalActionIntent(
        kind=kind,
        tool_call_id=tool_call_id,
        action_ordinal=action_ordinal,
        payload=payload,
    )


def _normalize_reply(values: Mapping[str, Any]) -> dict[str, Any]:
    _reject_unknown_fields(
        values,
        allowed={"text", "quote_message_id", "quote_message_log_id"},
    )
    text = _required_text(values.get("text"), field_name="text")
    quote_message_id = _optional_text(values.get("quote_message_id"))
    quote_message_log_id = _optional_positive_int(
        values.get("quote_message_log_id"),
        field_name="quote_message_log_id",
    )
    if quote_message_id and quote_message_log_id is not None:
        raise ValueError(
            "quote_message_id and quote_message_log_id are mutually exclusive"
        )
    payload: dict[str, Any] = {"text": text}
    if quote_message_id:
        payload["quote_message_id"] = quote_message_id
    if quote_message_log_id is not None:
        payload["quote_message_log_id"] = quote_message_log_id
    return payload


def _normalize_poke(values: Mapping[str, Any]) -> dict[str, Any]:
    _reject_unknown_fields(values, allowed={"user_id"})
    return {"user_id": _required_text(values.get("user_id"), field_name="user_id")}


def _normalize_reaction(values: Mapping[str, Any]) -> dict[str, Any]:
    _reject_unknown_fields(
        values,
        allowed={
            "action",
            "emoji",
            "emoji_id",
            "message_id",
            "message_log_id",
            "reaction",
        },
    )
    emoji_id = ""
    for field_name in ("emoji_id", "emoji", "reaction"):
        emoji_id = _optional_text(values.get(field_name))
        if emoji_id:
            break
    if not emoji_id:
        raise ValueError("emoji_id is required")
    action = _optional_text(values.get("action")).lower() or "add"
    if action not in {"add", "remove"}:
        raise ValueError("action must be 'add' or 'remove'")
    message_id = _optional_text(values.get("message_id"))
    message_log_id = _optional_positive_int(
        values.get("message_log_id"),
        field_name="message_log_id",
    )
    if bool(message_id) == (message_log_id is not None):
        raise ValueError("exactly one of message_id or message_log_id is required")
    payload: dict[str, Any] = {"emoji_id": emoji_id, "action": action}
    if message_id:
        payload["message_id"] = message_id
    else:
        payload["message_log_id"] = message_log_id
    return payload


def _reject_unknown_fields(
    values: Mapping[str, Any],
    *,
    allowed: set[str],
) -> None:
    unknown = sorted(set(values).difference(allowed).difference(_SEMANTIC_FIELDS))
    if unknown:
        raise ValueError("unsupported external action fields: " + ", ".join(unknown))


def _required_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _optional_text(value: object) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise TypeError("optional action identifiers must be strings")
    return value.strip()


def _optional_positive_int(value: object, *, field_name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


__all__ = [
    "EXTERNAL_ACTION_TOOL_NAMES",
    "ExternalActionToolMode",
    "collect_external_action_intent",
]
