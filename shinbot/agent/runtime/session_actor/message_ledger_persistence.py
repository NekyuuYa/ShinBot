"""SQLite operations for the actor-owned per-message ledger."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from typing import Any

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.message_ledger import (
    AppendMessageLedgerEntry,
    ConsumeMessageLedgerEntries,
    MessageConsumptionProvenance,
    MessageLedgerConsumptionKind,
    MessageLedgerConsumptionSelection,
    MessageLedgerEntry,
    MessageLedgerMutation,
    MessageLedgerProjectionKind,
    MessageLedgerRangeProjection,
    MessagePriorityFlags,
    project_message_ranges,
    validate_message_ledger_mutations,
)


class MessageLedgerConflict(RuntimeError):
    """Raised when a ledger replay changes durable identity or operation input."""


def apply_message_ledger_appends(
    conn: sqlite3.Connection,
    *,
    key: SessionKey,
    ownership_generation: int,
    source_event_id: str,
    mutations: tuple[MessageLedgerMutation, ...],
    committed_at: float,
) -> int:
    """Apply append mutations and return the transaction's ledger boundary."""

    validated = validate_message_ledger_mutations(
        mutations,
        key=key,
        ownership_generation=ownership_generation,
        source_event_id=source_event_id,
    )
    for append in (
        item for item in validated if isinstance(item, AppendMessageLedgerEntry)
    ):
        _append_message(conn, append, committed_at=committed_at)
    row = conn.execute(
        """
        SELECT COALESCE(MAX(ledger_sequence), 0) AS ledger_sequence
        FROM agent_message_ledger
        WHERE profile_id = ? AND session_id = ?
        """,
        (key.profile_id, key.session_id),
    ).fetchone()
    assert row is not None
    return int(row["ledger_sequence"])


def apply_message_ledger_consumptions(
    conn: sqlite3.Connection,
    *,
    key: SessionKey,
    ownership_generation: int,
    source_event_id: str,
    mutations: tuple[MessageLedgerMutation, ...],
    committed_at: float,
) -> None:
    """Apply consumption mutations after their operation rows exist."""

    validated = validate_message_ledger_mutations(
        mutations,
        key=key,
        ownership_generation=ownership_generation,
        source_event_id=source_event_id,
    )
    for consumption in (
        item for item in validated if isinstance(item, ConsumeMessageLedgerEntries)
    ):
        _consume_messages(conn, consumption, committed_at=committed_at)


def load_message_ledger_entries(
    conn: sqlite3.Connection,
    key: SessionKey,
    *,
    projection: MessageLedgerProjectionKind | None = None,
) -> tuple[MessageLedgerEntry, ...]:
    """Load a profile-scoped ledger projection in durable sequence order."""

    where = ["ledger.profile_id = ?", "ledger.session_id = ?"]
    params: list[object] = [key.profile_id, key.session_id]
    if projection is not None:
        try:
            kind = MessageLedgerProjectionKind(projection)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"unsupported message ledger projection: {projection}") from exc
        if kind in {
            MessageLedgerProjectionKind.UNREAD,
            MessageLedgerProjectionKind.REVIEW_PENDING,
            MessageLedgerProjectionKind.CHAT_PENDING,
        }:
            where.extend(
                [
                    "ledger.eligible_for_work = 1",
                    "ledger.review_consumption_id IS NULL",
                    "ledger.chat_consumption_id IS NULL",
                ]
            )
        else:
            where.extend(
                [
                    "ledger.eligible_for_work = 1",
                    "ledger.high_priority_consumption_id IS NULL",
                    "(ledger.priority_mention = 1 "
                    "OR ledger.priority_reply_to_bot = 1 "
                    "OR ledger.priority_repeated_mention = 1 "
                    "OR ledger.priority_poke_to_bot = 1)",
                ]
            )
    rows = conn.execute(
        f"""
        SELECT ledger.*,
               ledger.high_priority_consumption_id AS priority_consumption_id,
               review.idempotency_key AS review_idempotency_key,
               review.operation_id AS review_operation_id,
               review.source_event_id AS review_source_event_id,
               review.input_watermark AS review_input_watermark,
               review.input_ledger_sequence AS review_input_ledger_sequence,
               review.ownership_generation AS review_ownership_generation,
               review.committed_at AS review_committed_at,
               chat.idempotency_key AS chat_idempotency_key,
               chat.operation_id AS chat_operation_id,
               chat.source_event_id AS chat_source_event_id,
               chat.input_watermark AS chat_input_watermark,
               chat.input_ledger_sequence AS chat_input_ledger_sequence,
               chat.ownership_generation AS chat_ownership_generation,
               chat.committed_at AS chat_committed_at,
               priority.idempotency_key AS priority_idempotency_key,
               priority.operation_id AS priority_operation_id,
               priority.source_event_id AS priority_source_event_id,
               priority.input_watermark AS priority_input_watermark,
               priority.input_ledger_sequence AS priority_input_ledger_sequence,
               priority.ownership_generation AS priority_ownership_generation,
               priority.committed_at AS priority_committed_at
        FROM agent_message_ledger AS ledger
        LEFT JOIN agent_message_ledger_consumptions AS review
          ON review.consumption_id = ledger.review_consumption_id
        LEFT JOIN agent_message_ledger_consumptions AS chat
          ON chat.consumption_id = ledger.chat_consumption_id
        LEFT JOIN agent_message_ledger_consumptions AS priority
          ON priority.consumption_id = ledger.high_priority_consumption_id
        WHERE {" AND ".join(where)}
        ORDER BY ledger.ledger_sequence ASC
        """,
        tuple(params),
    ).fetchall()
    return tuple(_entry_from_row(row) for row in rows)


def load_captured_unread_message_ledger_entries(
    conn: sqlite3.Connection,
    key: SessionKey,
    *,
    ownership_generation: int,
    input_watermark: int,
    input_ledger_sequence: int,
) -> tuple[MessageLedgerEntry, ...]:
    """Load exactly the unread ledger projection visible to one operation.

    The ownership generation and both boundaries are required. Message-log ids
    and ledger sequence are intentionally independent: a late durable delivery
    can carry an older message-log id, so filtering only one boundary would
    let a workflow read input that did not belong to its accepted operation
    snapshot.  Ownership is checked after snapshot selection rather than used
    as a SQL filter, so a corrupt or stale row cannot be silently omitted.
    """

    normalized_ownership_generation = _positive_int(
        ownership_generation,
        field_name="ownership_generation",
    )
    _nonnegative_int(input_watermark, field_name="input_watermark")
    _nonnegative_int(
        input_ledger_sequence,
        field_name="input_ledger_sequence",
    )
    entries = load_message_ledger_entries(
        conn,
        key,
        projection=MessageLedgerProjectionKind.UNREAD,
    )
    captured = tuple(
        entry
        for entry in entries
        if (
            entry.message_log_id <= input_watermark
            and entry.ledger_sequence <= input_ledger_sequence
        )
    )
    for entry in captured:
        if entry.message.ownership_generation != normalized_ownership_generation:
            raise MessageLedgerConflict(
                "captured unread ledger entry ownership_generation does not match "
                "operation fence: "
                f"message_log_id={entry.message_log_id}, "
                f"ledger_sequence={entry.ledger_sequence}, "
                "entry_ownership_generation="
                f"{entry.message.ownership_generation}, "
                "expected_ownership_generation="
                f"{normalized_ownership_generation}"
            )
    return captured


def count_message_ledger_entries(
    conn: sqlite3.Connection,
    key: SessionKey,
    *,
    projection: MessageLedgerProjectionKind = MessageLedgerProjectionKind.UNREAD,
) -> int:
    """Count one profile-scoped projection without materializing ranges."""

    kind = MessageLedgerProjectionKind(projection)
    where = ["profile_id = ?", "session_id = ?"]
    if kind in {
        MessageLedgerProjectionKind.UNREAD,
        MessageLedgerProjectionKind.REVIEW_PENDING,
        MessageLedgerProjectionKind.CHAT_PENDING,
    }:
        where.extend(
            [
                "eligible_for_work = 1",
                "review_consumption_id IS NULL",
                "chat_consumption_id IS NULL",
            ]
        )
    else:
        where.extend(
            [
                "eligible_for_work = 1",
                "high_priority_consumption_id IS NULL",
                "(priority_mention = 1 OR priority_reply_to_bot = 1 "
                "OR priority_repeated_mention = 1 OR priority_poke_to_bot = 1)",
            ]
        )
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM agent_message_ledger
        WHERE {" AND ".join(where)}
        """,
        (key.profile_id, key.session_id),
    ).fetchone()
    return int(row["count"]) if row is not None else 0


def load_message_ledger_ranges(
    conn: sqlite3.Connection,
    key: SessionKey,
    *,
    projection: MessageLedgerProjectionKind = MessageLedgerProjectionKind.UNREAD,
) -> tuple[MessageLedgerRangeProjection, ...]:
    """Derive ranges from the complete ordered ledger; no range rows are stored."""

    entries = load_message_ledger_entries(conn, key)
    return project_message_ranges(entries, kind=projection)


def _append_message(
    conn: sqlite3.Connection,
    append: AppendMessageLedgerEntry,
    *,
    committed_at: float,
) -> None:
    existing_rows = conn.execute(
        """
        SELECT message_log_id, source_event_id, ownership_generation,
               canonical_json
        FROM agent_message_ledger
        WHERE profile_id = ?
          AND session_id = ?
          AND (message_log_id = ? OR source_event_id = ?)
        """,
        (
            append.key.profile_id,
            append.key.session_id,
            append.message_log_id,
            append.source_event_id,
        ),
    ).fetchall()
    if existing_rows:
        if len(existing_rows) == 1 and (
            int(existing_rows[0]["message_log_id"]) == append.message_log_id
            and str(existing_rows[0]["source_event_id"]) == append.source_event_id
            and int(existing_rows[0]["ownership_generation"])
            == append.ownership_generation
            and str(existing_rows[0]["canonical_json"]) == append.canonical_json
        ):
            return
        raise MessageLedgerConflict(
            "message_log_id or source_event_id was reused with different ledger content"
        )

    sequence_row = conn.execute(
        """
        SELECT COALESCE(MAX(ledger_sequence), 0) + 1 AS next_sequence
        FROM agent_message_ledger
        WHERE profile_id = ? AND session_id = ?
        """,
        (append.key.profile_id, append.key.session_id),
    ).fetchone()
    assert sequence_row is not None
    ledger_sequence = int(sequence_row["next_sequence"])
    record = append.to_record()
    priority = append.priority
    values: dict[str, object] = {
        "profile_id": append.key.profile_id,
        "session_id": append.key.session_id,
        "ledger_sequence": ledger_sequence,
        "message_log_id": append.message_log_id,
        "ownership_generation": append.ownership_generation,
        "source_event_id": append.source_event_id,
        "actor_event_id": append.actor_event_id,
        "delivery_version": append.delivery_version,
        "event_source": append.event_source,
        "sender_id": append.sender_id,
        "instance_id": append.instance_id,
        "event_type": append.event_type,
        "bot_id": append.bot_id,
        "bot_binding_id": append.bot_binding_id,
        "base_session_id": append.base_session_id,
        "bot_session_id": append.bot_session_id,
        "platform": append.platform,
        "self_id": append.self_id,
        "is_private": int(append.is_private),
        "is_mentioned": int(append.is_mentioned),
        "is_mention_to_other": int(append.is_mention_to_other),
        "is_reply_to_bot": int(append.is_reply_to_bot),
        "is_poke_to_bot": int(append.is_poke_to_bot),
        "is_poke_to_other": int(append.is_poke_to_other),
        "already_handled": int(append.already_handled),
        "is_stopped": int(append.is_stopped),
        "is_self_message": int(append.is_self_message),
        "eligible_for_work": int(append.eligible_for_work),
        "suppression_reason": append.suppression_reason,
        "response_profile": append.response_profile,
        "priority_mention": int(priority.mention),
        "priority_reply_to_bot": int(priority.reply_to_bot),
        "priority_repeated_mention": int(priority.repeated_mention),
        "priority_poke_to_bot": int(priority.poke_to_bot),
        "priority_should_wake": int(priority.should_wake_active_reply),
        "priority_reasons_json": _canonical_json(priority.to_record()["reasons"]),
        "causation_id": append.causation_id,
        "correlation_id": append.correlation_id,
        "trace_id": append.trace_id,
        "observed_at": append.observed_at,
        "occurred_at": append.occurred_at,
        "event_created_at": append.event_created_at,
        "metadata_json": _canonical_json(record["metadata"]),
        "canonical_json": append.canonical_json,
        "recorded_at": committed_at,
        "updated_at": committed_at,
    }
    columns = tuple(values)
    conn.execute(
        f"""
        INSERT INTO agent_message_ledger ({", ".join(columns)})
        VALUES ({", ".join("?" for _ in columns)})
        """,
        tuple(values[column] for column in columns),
    )


def _consume_messages(
    conn: sqlite3.Connection,
    consumption: ConsumeMessageLedgerEntries,
    *,
    committed_at: float,
) -> None:
    existing = conn.execute(
        """
        SELECT *
        FROM agent_message_ledger_consumptions
        WHERE consumption_id = ?
           OR (
               profile_id = ? AND session_id = ? AND kind = ?
               AND idempotency_key = ?
           )
        """,
        (
            consumption.consumption_id,
            consumption.key.profile_id,
            consumption.key.session_id,
            consumption.kind.value,
            consumption.idempotency_key,
        ),
    ).fetchall()
    if existing:
        if len(existing) == 1 and (
            str(existing[0]["consumption_id"]) == consumption.consumption_id
            and int(existing[0]["ownership_generation"])
            == consumption.ownership_generation
            and str(existing[0]["canonical_json"]) == consumption.canonical_json
        ):
            return
        raise MessageLedgerConflict(
            "consumption id or idempotency key was reused with different content"
        )

    operation = conn.execute(
        """
        SELECT profile_id, session_id, ownership_generation,
               input_watermark, input_ledger_sequence, status
        FROM agent_session_operations
        WHERE operation_id = ?
        """,
        (consumption.operation_id,),
    ).fetchone()
    if operation is None:
        raise MessageLedgerConflict("message consumption operation does not exist")
    operation_identity = (
        str(operation["profile_id"]),
        str(operation["session_id"]),
        int(operation["ownership_generation"]),
    )
    expected_identity = (
        consumption.key.profile_id,
        consumption.key.session_id,
        consumption.ownership_generation,
    )
    if operation_identity != expected_identity:
        raise MessageLedgerConflict(
            "message consumption operation ownership does not match the actor"
        )
    if operation["input_watermark"] is None or (
        int(operation["input_watermark"]) != consumption.input_watermark
    ):
        raise MessageLedgerConflict(
            "message consumption input watermark does not match its operation"
        )
    if operation["input_ledger_sequence"] is None or (
        int(operation["input_ledger_sequence"])
        != consumption.input_ledger_sequence
    ):
        raise MessageLedgerConflict(
            "message consumption input ledger sequence does not match its operation"
        )
    if str(operation["status"]) not in {"pending", "running", "completed"}:
        raise MessageLedgerConflict(
            "terminal unsuccessful operation cannot consume message input"
        )

    target_ids = _validate_consumption_targets(conn, consumption)
    conn.execute(
        """
        INSERT INTO agent_message_ledger_consumptions (
            consumption_id, profile_id, session_id, ownership_generation,
            kind, selection, idempotency_key, operation_id, source_event_id,
            input_watermark, input_ledger_sequence,
            explicit_message_log_ids_json, canonical_json,
            reason, trace_id, occurred_at, committed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            consumption.consumption_id,
            consumption.key.profile_id,
            consumption.key.session_id,
            consumption.ownership_generation,
            consumption.kind.value,
            consumption.selection.value,
            consumption.idempotency_key,
            consumption.operation_id,
            consumption.source_event_id,
            consumption.input_watermark,
            consumption.input_ledger_sequence,
            _canonical_json(list(consumption.explicit_message_log_ids)),
            consumption.canonical_json,
            consumption.reason,
            consumption.trace_id,
            consumption.occurred_at,
            committed_at,
        ),
    )
    _apply_consumption_to_targets(
        conn,
        consumption,
        explicit_target_ids=target_ids,
        committed_at=committed_at,
    )


def _validate_consumption_targets(
    conn: sqlite3.Connection,
    consumption: ConsumeMessageLedgerEntries,
) -> tuple[int, ...] | None:
    if (
        consumption.selection
        is MessageLedgerConsumptionSelection.ALL_THROUGH_WATERMARK
    ):
        return None
    ids = consumption.explicit_message_log_ids
    placeholders = ", ".join("?" for _ in ids)
    rows = conn.execute(
        f"""
        SELECT message_log_id, ledger_sequence,
               review_consumption_id, chat_consumption_id,
               high_priority_consumption_id,
               eligible_for_work,
               priority_mention, priority_reply_to_bot,
               priority_repeated_mention, priority_poke_to_bot
        FROM agent_message_ledger
        WHERE profile_id = ? AND session_id = ?
          AND ownership_generation = ?
          AND message_log_id IN ({placeholders})
        """,
        (
            consumption.key.profile_id,
            consumption.key.session_id,
            consumption.ownership_generation,
            *ids,
        ),
    ).fetchall()
    if len(rows) != len(ids):
        raise MessageLedgerConflict(
            "explicit message consumption requires every selected ledger row"
        )
    for row in rows:
        if int(row["ledger_sequence"]) > consumption.input_ledger_sequence:
            raise MessageLedgerConflict(
                "message consumption selected input beyond its ledger boundary"
            )
        if not bool(row["eligible_for_work"]):
            raise MessageLedgerConflict(
                "message consumption selected a suppressed ledger row"
            )
        if consumption.kind in {
            MessageLedgerConsumptionKind.REVIEW,
            MessageLedgerConsumptionKind.CHAT,
        }:
            if (
                row["review_consumption_id"] is not None
                or row["chat_consumption_id"] is not None
            ):
                raise MessageLedgerConflict(
                    "message input was already consumed by a conversational workflow"
                )
        else:
            is_priority = any(
                bool(row[name])
                for name in (
                    "priority_mention",
                    "priority_reply_to_bot",
                    "priority_repeated_mention",
                    "priority_poke_to_bot",
                )
            )
            if not is_priority or row["high_priority_consumption_id"] is not None:
                raise MessageLedgerConflict(
                    "high-priority consumption selected an inapplicable message"
                )
    return ids


def _apply_consumption_to_targets(
    conn: sqlite3.Connection,
    consumption: ConsumeMessageLedgerEntries,
    *,
    explicit_target_ids: tuple[int, ...] | None,
    committed_at: float,
) -> None:
    if consumption.kind is MessageLedgerConsumptionKind.REVIEW:
        column = "review_consumption_id"
        pending = (
            "eligible_for_work = 1 AND review_consumption_id IS NULL "
            "AND chat_consumption_id IS NULL"
        )
    elif consumption.kind is MessageLedgerConsumptionKind.CHAT:
        column = "chat_consumption_id"
        pending = (
            "eligible_for_work = 1 AND review_consumption_id IS NULL "
            "AND chat_consumption_id IS NULL"
        )
    else:
        column = "high_priority_consumption_id"
        pending = (
            "eligible_for_work = 1 AND high_priority_consumption_id IS NULL "
            "AND (priority_mention = 1 OR priority_reply_to_bot = 1 "
            "OR priority_repeated_mention = 1 OR priority_poke_to_bot = 1)"
        )
    params: list[object] = [
        consumption.consumption_id,
        committed_at,
        consumption.key.profile_id,
        consumption.key.session_id,
        consumption.ownership_generation,
        consumption.input_watermark,
        consumption.input_ledger_sequence,
    ]
    explicit_clause = ""
    if explicit_target_ids is not None:
        explicit_clause = (
            " AND message_log_id IN ("
            + ", ".join("?" for _ in explicit_target_ids)
            + ")"
        )
        params.extend(explicit_target_ids)
    updated = conn.execute(
        f"""
        UPDATE agent_message_ledger
        SET {column} = ?, updated_at = ?
        WHERE profile_id = ? AND session_id = ?
          AND ownership_generation = ?
          AND message_log_id <= ?
          AND ledger_sequence <= ?
          AND {pending}
          {explicit_clause}
        """,
        tuple(params),
    )
    if explicit_target_ids is not None and updated.rowcount != len(explicit_target_ids):
        raise MessageLedgerConflict(
            "explicit message consumption changed during atomic application"
        )


def _entry_from_row(row: sqlite3.Row) -> MessageLedgerEntry:
    raw = _json_object(row["canonical_json"], field_name="ledger canonical_json")
    priority_raw = _required_mapping(raw, "priority")
    append = AppendMessageLedgerEntry(
        key=SessionKey(
            profile_id=_required_text(raw, "profile_id"),
            session_id=_required_text(raw, "session_id"),
        ),
        message_log_id=_required_int(raw, "message_log_id"),
        ownership_generation=int(row["ownership_generation"]),
        source_event_id=_required_text(raw, "source_event_id"),
        actor_event_id=_required_text(raw, "actor_event_id"),
        delivery_version=_required_int(raw, "delivery_version"),
        event_source=_required_text(raw, "event_source"),
        sender_id=_optional_text(raw, "sender_id"),
        instance_id=_required_text(raw, "instance_id"),
        event_type=_required_text(raw, "event_type"),
        bot_id=_optional_text(raw, "bot_id"),
        bot_binding_id=_optional_text(raw, "bot_binding_id"),
        base_session_id=_optional_text(raw, "base_session_id"),
        bot_session_id=_optional_text(raw, "bot_session_id"),
        platform=_optional_text(raw, "platform"),
        self_id=_optional_text(raw, "self_id"),
        is_private=_required_bool(raw, "is_private"),
        is_mentioned=_required_bool(raw, "is_mentioned"),
        is_mention_to_other=_required_bool(raw, "is_mention_to_other"),
        is_reply_to_bot=_required_bool(raw, "is_reply_to_bot"),
        is_poke_to_bot=_required_bool(raw, "is_poke_to_bot"),
        is_poke_to_other=_required_bool(raw, "is_poke_to_other"),
        already_handled=_required_bool(raw, "already_handled"),
        is_stopped=_required_bool(raw, "is_stopped"),
        is_self_message=_required_bool(raw, "is_self_message"),
        eligible_for_work=_required_bool(raw, "eligible_for_work"),
        suppression_reason=_optional_text(raw, "suppression_reason"),
        response_profile=_optional_text(raw, "response_profile"),
        priority=MessagePriorityFlags(
            mention=_required_bool(priority_raw, "mention"),
            reply_to_bot=_required_bool(priority_raw, "reply_to_bot"),
            repeated_mention=_required_bool(priority_raw, "repeated_mention"),
            poke_to_bot=_required_bool(priority_raw, "poke_to_bot"),
            should_wake_active_reply=_required_bool(
                priority_raw,
                "should_wake_active_reply",
            ),
            reasons=dict(_required_mapping(priority_raw, "reasons")),
        ),
        causation_id=_optional_text(raw, "causation_id"),
        correlation_id=_optional_text(raw, "correlation_id"),
        trace_id=_optional_text(raw, "trace_id"),
        observed_at=_required_float(raw, "observed_at"),
        occurred_at=_required_float(raw, "occurred_at"),
        event_created_at=_required_float(raw, "event_created_at"),
        metadata=dict(_required_mapping(raw, "metadata")),
    )
    if append.canonical_json != str(row["canonical_json"]):
        raise MessageLedgerConflict("stored ledger columns contain non-canonical content")
    return MessageLedgerEntry(
        message=append,
        ledger_sequence=int(row["ledger_sequence"]),
        recorded_at=float(row["recorded_at"]),
        updated_at=float(row["updated_at"]),
        review_consumption=_provenance_from_row(row, prefix="review"),
        chat_consumption=_provenance_from_row(row, prefix="chat"),
        high_priority_consumption=_provenance_from_row(row, prefix="priority"),
    )


def _provenance_from_row(
    row: sqlite3.Row,
    *,
    prefix: str,
) -> MessageConsumptionProvenance | None:
    consumption_id = row[f"{prefix}_consumption_id"]
    if consumption_id is None:
        return None
    required = (
        "idempotency_key",
        "operation_id",
        "source_event_id",
        "input_watermark",
        "input_ledger_sequence",
        "ownership_generation",
        "committed_at",
    )
    if any(row[f"{prefix}_{field}"] is None for field in required):
        raise MessageLedgerConflict("ledger consumption provenance is incomplete")
    return MessageConsumptionProvenance(
        consumption_id=str(consumption_id),
        idempotency_key=str(row[f"{prefix}_idempotency_key"]),
        operation_id=str(row[f"{prefix}_operation_id"]),
        source_event_id=str(row[f"{prefix}_source_event_id"]),
        input_watermark=int(row[f"{prefix}_input_watermark"]),
        input_ledger_sequence=int(row[f"{prefix}_input_ledger_sequence"]),
        ownership_generation=int(row[f"{prefix}_ownership_generation"]),
        committed_at=float(row[f"{prefix}_committed_at"]),
    )


def _json_object(value: object, *, field_name: str) -> dict[str, Any]:
    if not isinstance(value, str):
        raise MessageLedgerConflict(f"{field_name} must be JSON text")
    try:
        parsed = json.loads(
            value,
            parse_constant=lambda token: (_raise_nonfinite_json(token)),
        )
    except (TypeError, ValueError) as exc:
        raise MessageLedgerConflict(f"{field_name} is invalid JSON") from exc
    if not isinstance(parsed, dict) or any(
        not isinstance(key, str) for key in parsed
    ):
        raise MessageLedgerConflict(f"{field_name} must contain an object")
    return parsed


def _raise_nonfinite_json(token: str) -> None:
    raise ValueError(f"non-finite JSON token: {token}")


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _required_mapping(
    values: Mapping[str, Any],
    field_name: str,
) -> Mapping[str, Any]:
    value = values.get(field_name)
    if not isinstance(value, Mapping) or any(
        not isinstance(key, str) for key in value
    ):
        raise MessageLedgerConflict(f"{field_name} must be an object")
    return value


def _optional_text(values: Mapping[str, Any], field_name: str) -> str:
    value = values.get(field_name, "")
    if not isinstance(value, str):
        raise MessageLedgerConflict(f"{field_name} must be a string")
    return value


def _required_text(values: Mapping[str, Any], field_name: str) -> str:
    value = _optional_text(values, field_name).strip()
    if not value:
        raise MessageLedgerConflict(f"{field_name} must not be empty")
    return value


def _required_int(values: Mapping[str, Any], field_name: str) -> int:
    value = values.get(field_name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise MessageLedgerConflict(f"{field_name} must be an integer")
    return value


def _required_bool(values: Mapping[str, Any], field_name: str) -> bool:
    value = values.get(field_name)
    if not isinstance(value, bool):
        raise MessageLedgerConflict(f"{field_name} must be a boolean")
    return value


def _required_float(values: Mapping[str, Any], field_name: str) -> float:
    value = values.get(field_name)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MessageLedgerConflict(f"{field_name} must be a number")
    return float(value)


def _nonnegative_int(value: object, *, field_name: str) -> int:
    """Require a non-boolean integer boundary at or above zero."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a nonnegative integer")
    return value


def _positive_int(value: object, *, field_name: str) -> int:
    """Require a non-boolean integer boundary above zero."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


__all__ = [
    "MessageLedgerConflict",
    "apply_message_ledger_appends",
    "apply_message_ledger_consumptions",
    "count_message_ledger_entries",
    "load_captured_unread_message_ledger_entries",
    "load_message_ledger_entries",
    "load_message_ledger_ranges",
]
