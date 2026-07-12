"""Adapter-backed dispatch port for durable external Agent actions.

This module is intentionally below the receipt-fenced effect handler and
above platform adapters.  It validates every action against its immutable
request before adapter I/O, so a failure that can be proven pre-dispatch stays
safe to retry.  Once an adapter method is awaited, the handler owns the
conservative ``unknown`` outcome policy.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from typing import Any, Protocol

from shinbot.agent.runtime.session_actor.external_action_handler import (
    ExternalActionDispatchResult,
    ExternalActionPreDispatchRejected,
)
from shinbot.agent.runtime.session_actor.external_action_store import (
    ClaimedExternalAction,
)
from shinbot.agent.runtime.session_actor.external_actions import (
    ExternalActionKind,
    ExternalActionRequest,
)
from shinbot.core.platform.adapter_manager import BaseAdapter, MessageHandle
from shinbot.persistence.records import MessageLogRecord
from shinbot.schema.elements import MessageElement


class AdapterActionLookup(Protocol):
    """Minimal adapter-manager surface required by visible actor actions."""

    def get_instance(self, instance_id: str) -> BaseAdapter | None:
        """Return one adapter instance by its durable identifier."""

    def is_connected(self, instance_id: str) -> bool:
        """Return whether adapter I/O is safe to begin now."""


class MessageLogLookup(Protocol):
    """Read-only message-log lookup used for quote and reaction targets."""

    def get(self, message_log_id: int) -> dict[str, Any] | None:
        """Return one persisted message record by primary key."""


class AdapterActionDatabase(Protocol):
    """Database surface required before adapter dispatch begins."""

    message_logs: MessageLogLookup


class AdapterExternalActionDispatcher:
    """Translate accepted actor actions into one guarded adapter invocation."""

    def __init__(
        self,
        *,
        adapters: AdapterActionLookup,
        database: AdapterActionDatabase,
        clock: Callable[[], float] | None = None,
    ) -> None:
        """Bind stable adapter lookup and message-log resolution dependencies."""

        self._adapters = adapters
        self._database = database
        self._clock = clock or time.time

    async def dispatch(
        self,
        request: ExternalActionRequest,
        claim: ClaimedExternalAction,
    ) -> ExternalActionDispatchResult:
        """Perform one validated adapter action for a live durable claim."""

        if claim.idempotency_key != request.idempotency_key:
            raise ExternalActionPreDispatchRejected(
                "action_claim_identity_changed",
                evidence={
                    "claim_idempotency_key": claim.idempotency_key,
                    "request_idempotency_key": request.idempotency_key,
                },
            )
        adapter = self._available_adapter(request)
        if request.intent.kind is ExternalActionKind.SEND_REPLY:
            return await self._send_reply(adapter, request, claim)
        if request.intent.kind is ExternalActionKind.SEND_POKE:
            return await self._send_poke(adapter, request, claim)
        if request.intent.kind is ExternalActionKind.SEND_REACTION:
            return await self._send_reaction(adapter, request, claim)
        raise ExternalActionPreDispatchRejected(
            "unsupported_external_action",
            evidence={"kind": request.intent.kind.value},
        )

    def _available_adapter(self, request: ExternalActionRequest) -> BaseAdapter:
        adapter = self._adapters.get_instance(request.instance_id)
        if adapter is None:
            raise ExternalActionPreDispatchRejected(
                "adapter_not_found",
                evidence={"instance_id": request.instance_id},
            )
        if adapter.instance_id != request.instance_id:
            raise ExternalActionPreDispatchRejected(
                "adapter_identity_changed",
                evidence={
                    "expected_instance_id": request.instance_id,
                    "actual_instance_id": adapter.instance_id,
                },
            )
        if not self._adapters.is_connected(request.instance_id):
            raise ExternalActionPreDispatchRejected(
                "adapter_not_connected",
                evidence={"instance_id": request.instance_id},
            )
        return adapter

    async def _send_reply(
        self,
        adapter: BaseAdapter,
        request: ExternalActionRequest,
        claim: ClaimedExternalAction,
    ) -> ExternalActionDispatchResult:
        payload = _action_payload(request, allowed={"text", "quote_message_id", "quote_message_log_id"})
        text = _required_text(payload.get("text"), field_name="text")
        quote_message_id = self._resolve_optional_message_id(
            payload,
            direct_field="quote_message_id",
            log_field="quote_message_log_id",
            request=request,
        )
        elements: list[MessageElement] = []
        if quote_message_id:
            elements.append(MessageElement.quote(quote_message_id))
        elements.append(MessageElement.text(text))
        handle = await adapter.send(request.target_session_id, elements)
        platform_message_id = _handle_message_id(handle)
        assistant_message = MessageLogRecord(
            session_id=request.target_session_id,
            platform_msg_id=platform_message_id,
            sender_id=adapter.instance_id,
            sender_name="",
            content_json=json.dumps(
                [element.model_dump(mode="json") for element in elements],
                ensure_ascii=False,
            ),
            raw_text=text,
            role="assistant",
            is_read=True,
            is_mentioned=False,
            created_at=self._clock() * 1000,
        )
        return ExternalActionDispatchResult(
            platform_result={
                "external_idempotency_key": claim.idempotency_key,
                "platform_msg_id": platform_message_id,
                "quote_message_id": quote_message_id,
                "target_session_id": request.target_session_id,
            },
            assistant_message=assistant_message,
        )

    async def _send_poke(
        self,
        adapter: BaseAdapter,
        request: ExternalActionRequest,
        claim: ClaimedExternalAction,
    ) -> ExternalActionDispatchResult:
        payload = _action_payload(request, allowed={"user_id"})
        params: dict[str, Any] = {"user_id": _required_text(payload.get("user_id"), field_name="user_id")}
        group_id = _group_id_from_transport_session(request.target_session_id)
        if group_id:
            params["group_id"] = group_id
        result = await adapter.call_api(f"internal.{adapter.platform}.poke", params)
        return ExternalActionDispatchResult(
            platform_result={
                "adapter_result": result,
                "external_idempotency_key": claim.idempotency_key,
                "target_session_id": request.target_session_id,
                "user_id": params["user_id"],
            }
        )

    async def _send_reaction(
        self,
        adapter: BaseAdapter,
        request: ExternalActionRequest,
        claim: ClaimedExternalAction,
    ) -> ExternalActionDispatchResult:
        payload = _action_payload(
            request,
            allowed={"action", "emoji_id", "message_id", "message_log_id"},
        )
        emoji_id = _required_text(payload.get("emoji_id"), field_name="emoji_id")
        action = _required_text(payload.get("action"), field_name="action").lower()
        if action not in {"add", "remove"}:
            raise ExternalActionPreDispatchRejected(
                "reaction_action_invalid",
                evidence={"action": action},
            )
        message_id = self._resolve_required_message_id(
            payload,
            direct_field="message_id",
            log_field="message_log_id",
            request=request,
        )
        method = "reaction.delete" if action == "remove" else "reaction.create"
        result = await adapter.call_api(
            method,
            {
                "emoji_id": emoji_id,
                "message_id": message_id,
                "session_id": request.target_session_id,
            },
        )
        return ExternalActionDispatchResult(
            platform_result={
                "adapter_result": result,
                "emoji_id": emoji_id,
                "external_idempotency_key": claim.idempotency_key,
                "message_id": message_id,
                "reaction_action": action,
                "target_session_id": request.target_session_id,
            }
        )

    def _resolve_optional_message_id(
        self,
        payload: Mapping[str, Any],
        *,
        direct_field: str,
        log_field: str,
        request: ExternalActionRequest,
    ) -> str:
        direct = _optional_text(payload.get(direct_field), field_name=direct_field)
        has_log = log_field in payload and payload.get(log_field) is not None
        if direct and has_log:
            raise ExternalActionPreDispatchRejected(
                "message_target_ambiguous",
                evidence={"direct_field": direct_field, "log_field": log_field},
            )
        if direct:
            return direct
        if not has_log:
            return ""
        return self._message_id_from_log(
            payload.get(log_field),
            field_name=log_field,
            request=request,
        )

    def _resolve_required_message_id(
        self,
        payload: Mapping[str, Any],
        *,
        direct_field: str,
        log_field: str,
        request: ExternalActionRequest,
    ) -> str:
        message_id = self._resolve_optional_message_id(
            payload,
            direct_field=direct_field,
            log_field=log_field,
            request=request,
        )
        if not message_id:
            raise ExternalActionPreDispatchRejected(
                "message_target_missing",
                evidence={"direct_field": direct_field, "log_field": log_field},
            )
        return message_id

    def _message_id_from_log(
        self,
        value: object,
        *,
        field_name: str,
        request: ExternalActionRequest,
    ) -> str:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ExternalActionPreDispatchRejected(
                "message_log_id_invalid",
                evidence={"field": field_name},
            )
        record = self._database.message_logs.get(value)
        if record is None:
            raise ExternalActionPreDispatchRejected(
                "message_log_not_found",
                evidence={"message_log_id": value},
            )
        if _required_text(record.get("session_id"), field_name="message_log.session_id") != request.target_session_id:
            raise ExternalActionPreDispatchRejected(
                "message_log_session_mismatch",
                evidence={"message_log_id": value},
            )
        message_id = _required_text(
            record.get("platform_msg_id"),
            field_name="message_log.platform_msg_id",
        )
        return message_id


def _action_payload(
    request: ExternalActionRequest,
    *,
    allowed: set[str],
) -> dict[str, Any]:
    """Copy a normalized action payload or reject it before adapter I/O."""

    payload = request.intent.payload
    unknown = sorted(set(payload).difference(allowed))
    if unknown:
        raise ExternalActionPreDispatchRejected(
            "action_payload_unsupported",
            evidence={"fields": unknown},
        )
    return {str(key): value for key, value in payload.items()}


def _required_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ExternalActionPreDispatchRejected(
            "action_argument_invalid",
            evidence={"field": field_name},
        )
    return value.strip()


def _optional_text(value: object, *, field_name: str) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ExternalActionPreDispatchRejected(
            "action_argument_invalid",
            evidence={"field": field_name},
        )
    return value.strip()


def _group_id_from_transport_session(session_id: str) -> str:
    """Return the group segment expected by legacy poke-capable adapters."""

    _instance_id, separator, rest = session_id.partition(":")
    if not separator or not rest.startswith("group:"):
        return ""
    return rest[len("group:") :].rsplit(":", 1)[-1].strip()


def _handle_message_id(handle: MessageHandle | None) -> str:
    """Normalize an adapter send handle without treating an empty id as failure."""

    return str(handle.message_id if handle is not None else "").strip()


__all__ = ["AdapterActionLookup", "AdapterExternalActionDispatcher"]
