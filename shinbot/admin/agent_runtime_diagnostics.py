"""Administrative projection for canonical Agent runtime diagnostics."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.persistence.repositories.agent_runtime_diagnostics import (
    AgentRuntimeDiagnosticsRepository,
    AgentRuntimeDiagnosticsSnapshot,
    DiagnosticCollectionSnapshot,
)

AGENT_RUNTIME_DIAGNOSTIC_SEGMENT_PATTERN = r"^[A-Za-z0-9._~:@+-]+$"
AGENT_RUNTIME_PROFILE_ID_MAX_LENGTH = 128
AGENT_RUNTIME_SESSION_ID_MAX_LENGTH = 512

_SEGMENT_RE = re.compile(AGENT_RUNTIME_DIAGNOSTIC_SEGMENT_PATTERN)

_SAFE_JSON_COLUMNS = frozenset({"active_reply_threshold_json"})
_SENSITIVE_TEXT_COLUMNS = frozenset(
    {"failure_message", "last_error", "last_error_message"}
)
_SAFE_REFERENCE_KEYS = frozenset(
    {
        "action_ordinal",
        "activity_generation",
        "active_epoch",
        "causation_id",
        "correlation_id",
        "delivery_id",
        "effect_id",
        "event_id",
        "expected_activity_generation",
        "expected_active_epoch",
        "message_log_id",
        "model_execution_id",
        "operation_id",
        "ownership_generation",
        "plan_id",
        "routing_job_id",
        "state_revision",
        "trace_id",
    }
)
_SAFE_OUTBOUND_BLOCKER_FIELDS = {
    "effect_id": "effectId",
    "failure_code": "failureCode",
    "failure_event_id": "failureEventId",
    "kind": "kind",
    "operation_id": "operationId",
    "receipt_status": "receiptStatus",
}

type RuntimeKind = Literal["legacy", "actor_v2", "unowned"]
type ActorDataStatus = Literal["available", "not_initialized", "not_applicable"]


class AgentRuntimeDiagnosticsError(RuntimeError):
    """Base error raised by the Agent runtime diagnostics admin service."""


class AgentRuntimeDiagnosticsInvalidKey(AgentRuntimeDiagnosticsError, ValueError):
    """Raised when a diagnostic identity is not a safe URL segment."""


class AgentRuntimeDiagnosticsNotFound(AgentRuntimeDiagnosticsError, LookupError):
    """Raised when no ownership decision or durable evidence exists."""


@dataclass(slots=True, frozen=True)
class AgentRuntimeSessionDiagnostics:
    """Typed management projection for one profile-scoped Agent session."""

    key: SessionKey
    runtime_kind: RuntimeKind
    actor_canonical: bool
    actor_data_status: ActorDataStatus
    ownership: dict[str, Any] | None
    ownership_events: tuple[dict[str, Any], ...]
    aggregate: dict[str, Any] | None
    mailbox: dict[str, Any]
    operations: dict[str, Any]
    effects: dict[str, Any]
    external_actions: dict[str, Any]
    review_schedule: dict[str, Any]
    route_deliveries: dict[str, Any]
    routing_jobs: tuple[dict[str, Any], ...]
    recent_transitions: tuple[dict[str, Any], ...]
    recent_schedule_events: tuple[dict[str, Any], ...]
    legacy: dict[str, Any] | None

    def to_payload(self) -> dict[str, Any]:
        """Return the stable camel-case API payload."""

        return {
            "profileId": self.key.profile_id,
            "sessionId": self.key.session_id,
            "sensitiveDataPolicy": "redacted",
            "runtimeKind": self.runtime_kind,
            "actorCanonical": self.actor_canonical,
            "actorDataStatus": self.actor_data_status,
            "ownership": self.ownership,
            "ownershipEvents": list(self.ownership_events),
            "aggregate": self.aggregate,
            "mailbox": self.mailbox,
            "operations": self.operations,
            "effects": self.effects,
            "externalActions": self.external_actions,
            "reviewSchedule": self.review_schedule,
            "routeDeliveries": self.route_deliveries,
            "routingJobs": list(self.routing_jobs),
            "recentTransitions": list(self.recent_transitions),
            "recentScheduleEvents": list(self.recent_schedule_events),
            "legacy": self.legacy,
        }


def get_agent_runtime_session_diagnostics(
    database: Any,
    *,
    profile_id: str,
    session_id: str,
) -> AgentRuntimeSessionDiagnostics:
    """Read and project canonical diagnostics for one Agent session.

    Args:
        database: Database manager that owns the canonical runtime store.
        profile_id: Stable bot/runtime profile id, not an editable Agent id.
        session_id: Stable bot-scoped Agent session id.

    Returns:
        Typed diagnostics with ownership and both migration evidence sides.

    Raises:
        AgentRuntimeDiagnosticsInvalidKey: If either identity is unsafe.
        AgentRuntimeDiagnosticsNotFound: If the key has no durable evidence.
    """

    key = SessionKey(
        _validate_segment(
            profile_id,
            field_name="profile_id",
            max_length=AGENT_RUNTIME_PROFILE_ID_MAX_LENGTH,
        ),
        _validate_segment(
            session_id,
            field_name="session_id",
            max_length=AGENT_RUNTIME_SESSION_ID_MAX_LENGTH,
        ),
    )
    repository = getattr(database, "agent_runtime_diagnostics", None)
    if repository is None:
        repository = AgentRuntimeDiagnosticsRepository(database)
    snapshot = repository.get_session(key)
    if not snapshot.has_data:
        raise AgentRuntimeDiagnosticsNotFound(
            f"Agent runtime session not found: {key.profile_id}:{key.session_id}"
        )
    return _project_snapshot(snapshot)


def _project_snapshot(
    snapshot: AgentRuntimeDiagnosticsSnapshot,
) -> AgentRuntimeSessionDiagnostics:
    ownership = _project_optional_row(snapshot.ownership)
    runtime_kind = _runtime_kind(snapshot)
    actor_canonical = bool(
        snapshot.ownership is not None
        and snapshot.ownership.get("mode") == "actor_v2"
        and snapshot.ownership.get("status") == "active"
    )
    actor_data_status = _actor_data_status(snapshot)
    aggregate = _project_optional_row(snapshot.aggregate)
    current_plan_id = (
        str(snapshot.aggregate.get("current_plan_id") or "").strip()
        if snapshot.aggregate is not None
        else ""
    )
    current_schedule = _project_optional_row(snapshot.current_review_schedule)
    if current_plan_id and current_schedule is None:
        schedule_resolution = "missing"
    elif current_schedule is not None:
        schedule_resolution = "resolved"
    else:
        schedule_resolution = "not_set"
    legacy = None
    if snapshot.legacy is not None:
        legacy_canonical = bool(
            snapshot.ownership is not None
            and snapshot.ownership.get("mode") == "legacy"
            and snapshot.ownership.get("status") == "active"
        )
        legacy = {
            "sessionId": snapshot.legacy.session_id,
            "canonical": legacy_canonical,
            "dataStatus": "available" if snapshot.legacy.has_data else "empty",
            "schedulerState": _project_optional_row(snapshot.legacy.scheduler_state),
            "unreadMessages": _camelize_mapping(snapshot.legacy.unread_messages),
            "unreadRanges": _camelize_mapping(snapshot.legacy.unread_ranges),
        }

    return AgentRuntimeSessionDiagnostics(
        key=snapshot.key,
        runtime_kind=runtime_kind,
        actor_canonical=actor_canonical,
        actor_data_status=actor_data_status,
        ownership=ownership,
        ownership_events=tuple(_project_row(row) for row in snapshot.ownership_events),
        aggregate=aggregate,
        mailbox=_project_collection(snapshot.mailbox),
        operations=_project_collection(snapshot.operations),
        effects=_project_collection(snapshot.effects),
        external_actions=_project_external_actions(
            snapshot,
            aggregate=snapshot.aggregate,
        ),
        review_schedule={
            "currentPlanId": current_plan_id,
            "resolution": schedule_resolution,
            "current": current_schedule,
            **_project_collection(snapshot.review_schedules),
        },
        route_deliveries=_project_collection(snapshot.route_deliveries),
        routing_jobs=tuple(_project_row(row) for row in snapshot.routing_jobs),
        recent_transitions=tuple(
            _project_row(row) for row in snapshot.state_transitions
        ),
        recent_schedule_events=tuple(
            _project_row(row) for row in snapshot.review_schedule_events
        ),
        legacy=legacy,
    )


def _runtime_kind(snapshot: AgentRuntimeDiagnosticsSnapshot) -> RuntimeKind:
    if snapshot.ownership is not None:
        mode = str(snapshot.ownership.get("mode") or "")
        if mode == "legacy":
            return "legacy"
        if mode == "actor_v2":
            return "actor_v2"
    return "unowned"


def _actor_data_status(snapshot: AgentRuntimeDiagnosticsSnapshot) -> ActorDataStatus:
    if snapshot.aggregate is not None:
        return "available"
    ownership_mode = (
        str(snapshot.ownership.get("mode") or "")
        if snapshot.ownership is not None
        else ""
    )
    pending_mode = (
        str(snapshot.ownership.get("pending_mode") or "")
        if snapshot.ownership is not None
        else ""
    )
    if (
        ownership_mode == "actor_v2"
        or pending_mode == "actor_v2"
        or snapshot.route_deliveries.total > 0
    ):
        return "not_initialized"
    return "not_applicable"


def _project_collection(collection: DiagnosticCollectionSnapshot) -> dict[str, Any]:
    return {
        "total": collection.total,
        "byStatus": dict(sorted(collection.by_status.items())),
        "recent": [_project_row(row) for row in collection.recent],
    }


def _project_external_actions(
    snapshot: AgentRuntimeDiagnosticsSnapshot,
    *,
    aggregate: Mapping[str, Any] | None,
) -> dict[str, Any]:
    receipt_statuses = snapshot.external_action_receipts.by_status
    unknown_count = int(receipt_statuses.get("unknown", 0))
    abandoned_before_dispatch_count = int(
        receipt_statuses.get("abandoned_before_dispatch", 0)
    )
    live_count = sum(
        int(receipt_statuses.get(status, 0))
        for status in ("prepared", "executing", "rejected_before_dispatch")
    )
    outbound_blocker = _project_outbound_blocker(aggregate)
    attention_required = unknown_count > 0 or outbound_blocker is not None
    if attention_required:
        status = "attention_required"
    elif live_count:
        status = "active"
    else:
        status = "ok"
    return {
        "status": status,
        "attentionRequired": attention_required,
        "unknownCount": unknown_count,
        "abandonedBeforeDispatchCount": abandoned_before_dispatch_count,
        "liveCount": live_count,
        "outboundBlocker": outbound_blocker,
        "receipts": _project_collection(snapshot.external_action_receipts),
        "attempts": _project_collection(snapshot.external_action_attempts),
    }


def _project_outbound_blocker(
    aggregate: Mapping[str, Any] | None,
) -> dict[str, str] | None:
    """Expose only actor-owned outbound blocker references for operators."""

    if not aggregate:
        return None
    raw_data = _decode_json(aggregate.get("data_json"))
    if not isinstance(raw_data, Mapping):
        return None
    raw_blocker = raw_data.get("outbound_blocked")
    if not isinstance(raw_blocker, Mapping):
        return None
    blocker: dict[str, str] = {}
    for raw_name, public_name in _SAFE_OUTBOUND_BLOCKER_FIELDS.items():
        value = raw_blocker.get(raw_name)
        if not isinstance(value, str):
            continue
        normalized = value.strip()
        if normalized:
            blocker[public_name] = normalized
    if blocker.get("kind") not in {"effect_failed", "receipt_terminal"}:
        return None
    return blocker


def _project_optional_row(row: dict[str, Any] | None) -> dict[str, Any] | None:
    return _project_row(row) if row is not None else None


def _project_row(row: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in row.items():
        if key.endswith("_json") and key not in _SAFE_JSON_COLUMNS:
            payload[_snake_to_camel(key.removesuffix("_json"))] = (
                _redacted_json(value)
            )
        elif key in _SENSITIVE_TEXT_COLUMNS:
            payload[_snake_to_camel(key)] = _redacted_text(value)
        elif key.endswith("_json"):
            payload[_snake_to_camel(key.removesuffix("_json"))] = _decode_json(value)
        else:
            payload[_snake_to_camel(key)] = value
    return payload


def _redacted_json(value: Any) -> dict[str, Any]:
    encoded = _encoded_value(value)
    parsed: Any = None
    json_type = "invalid"
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
    except (json.JSONDecodeError, TypeError):
        pass
    if isinstance(parsed, dict):
        json_type = "object"
    elif isinstance(parsed, list):
        json_type = "array"
    elif parsed is not None:
        json_type = "scalar"
    reference_items = parsed.items() if isinstance(parsed, dict) else ()
    references = {
        _snake_to_camel(str(key)): item
        for key, item in reference_items
        if key in _SAFE_REFERENCE_KEYS
        and isinstance(item, (str, int, float, bool))
    }
    return {
        "redacted": True,
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "sizeBytes": len(encoded),
        "jsonType": json_type,
        "references": references,
    }


def _redacted_text(value: Any) -> dict[str, Any] | None:
    if value is None or value == "":
        return None
    encoded = _encoded_value(value)
    return {
        "redacted": True,
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "sizeBytes": len(encoded),
    }


def _encoded_value(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8", errors="replace")
    return json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8")


def _decode_json(value: Any) -> Any:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return {
            "diagnosticParseError": "invalid_json",
            "raw": value,
        }


def _camelize_mapping(values: dict[str, int]) -> dict[str, int]:
    return {_snake_to_camel(key): value for key, value in values.items()}


def _snake_to_camel(value: str) -> str:
    head, *tail = value.split("_")
    return head + "".join(part.capitalize() for part in tail)


def _validate_segment(value: str, *, field_name: str, max_length: int) -> str:
    if not isinstance(value, str):
        raise AgentRuntimeDiagnosticsInvalidKey(f"{field_name} must be a string")
    if not value or value != value.strip():
        raise AgentRuntimeDiagnosticsInvalidKey(
            f"{field_name} must not be empty or contain surrounding whitespace"
        )
    if len(value) > max_length:
        raise AgentRuntimeDiagnosticsInvalidKey(
            f"{field_name} must not exceed {max_length} characters"
        )
    if value in {".", ".."} or _SEGMENT_RE.fullmatch(value) is None:
        raise AgentRuntimeDiagnosticsInvalidKey(
            f"{field_name} must be a URL-safe path segment"
        )
    return value


__all__ = [
    "AGENT_RUNTIME_DIAGNOSTIC_SEGMENT_PATTERN",
    "AGENT_RUNTIME_PROFILE_ID_MAX_LENGTH",
    "AGENT_RUNTIME_SESSION_ID_MAX_LENGTH",
    "AgentRuntimeDiagnosticsError",
    "AgentRuntimeDiagnosticsInvalidKey",
    "AgentRuntimeDiagnosticsNotFound",
    "AgentRuntimeSessionDiagnostics",
    "get_agent_runtime_session_diagnostics",
]
