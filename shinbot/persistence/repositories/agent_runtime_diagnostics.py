"""Consistent read models for durable Agent runtime diagnostics."""

from __future__ import annotations

from dataclasses import dataclass
from sqlite3 import Connection
from typing import TYPE_CHECKING, Any

from shinbot.core.dispatch.agent_identity import SessionKey

from .base import Repository

if TYPE_CHECKING:
    from shinbot.persistence.engine import DatabaseManager


DIAGNOSTIC_RECENT_LIMIT = 50

_COLLECTION_QUERIES = {
    "agent_session_mailbox": "mailbox_id DESC",
    "agent_session_operations": "started_at DESC, operation_id DESC",
    "agent_effect_outbox": "effect_seq DESC",
    "agent_external_action_receipts": "receipt_seq DESC",
    "agent_review_schedules": "plan_revision DESC, created_at DESC",
    "agent_route_outbox": "outbox_seq DESC",
}


@dataclass(slots=True, frozen=True)
class DiagnosticCollectionSnapshot:
    """Recent rows and complete status counts for one durable collection."""

    total: int
    by_status: dict[str, int]
    recent: tuple[dict[str, Any], ...]


@dataclass(slots=True, frozen=True)
class LegacyRuntimeDiagnosticsSnapshot:
    """Legacy scheduler evidence addressed by an ownership migration alias."""

    session_id: str
    scheduler_state: dict[str, Any] | None
    unread_messages: dict[str, int]
    unread_ranges: dict[str, int]

    @property
    def has_data(self) -> bool:
        """Return whether any durable legacy runtime evidence exists."""

        return bool(
            self.scheduler_state is not None
            or self.unread_messages.get("total_rows", 0)
            or self.unread_ranges.get("total_rows", 0)
        )


@dataclass(slots=True, frozen=True)
class AgentRuntimeDiagnosticsSnapshot:
    """One transactionally consistent diagnostic snapshot for a session key."""

    key: SessionKey
    ownership: dict[str, Any] | None
    ownership_events: tuple[dict[str, Any], ...]
    aggregate: dict[str, Any] | None
    mailbox: DiagnosticCollectionSnapshot
    operations: DiagnosticCollectionSnapshot
    effects: DiagnosticCollectionSnapshot
    external_action_receipts: DiagnosticCollectionSnapshot
    external_action_attempts: DiagnosticCollectionSnapshot
    review_schedules: DiagnosticCollectionSnapshot
    current_review_schedule: dict[str, Any] | None
    route_deliveries: DiagnosticCollectionSnapshot
    routing_jobs: tuple[dict[str, Any], ...]
    state_transitions: tuple[dict[str, Any], ...]
    review_schedule_events: tuple[dict[str, Any], ...]
    legacy: LegacyRuntimeDiagnosticsSnapshot | None

    @property
    def has_data(self) -> bool:
        """Return whether the key has ownership or any durable runtime evidence."""

        return bool(
            self.ownership is not None
            or self.aggregate is not None
            or self.mailbox.total
            or self.operations.total
            or self.effects.total
            or self.external_action_receipts.total
            or self.external_action_attempts.total
            or self.review_schedules.total
            or self.route_deliveries.total
            or self.routing_jobs
            or self.state_transitions
            or self.review_schedule_events
            or (self.legacy is not None and self.legacy.has_data)
        )


class AgentRuntimeDiagnosticsRepository(Repository):
    """Read canonical Agent session evidence without importing Agent runtime code."""

    def __init__(
        self,
        db: DatabaseManager,
        *,
        recent_limit: int = DIAGNOSTIC_RECENT_LIMIT,
    ) -> None:
        """Initialize the diagnostics reader.

        Args:
            db: Database manager or compatible connection provider.
            recent_limit: Maximum recent rows returned for each journal.

        Raises:
            ValueError: If ``recent_limit`` is outside the supported range.
        """

        super().__init__(db)
        if isinstance(recent_limit, bool) or not 1 <= recent_limit <= 200:
            raise ValueError("recent_limit must be between 1 and 200")
        self._recent_limit = recent_limit

    def get_session(self, key: SessionKey) -> AgentRuntimeDiagnosticsSnapshot:
        """Read all durable diagnostic evidence for ``key`` in one snapshot.

        Args:
            key: Stable profile-scoped Agent session identity.

        Returns:
            A snapshot. Callers decide whether an evidence-free snapshot is a
            not-found result.
        """

        with self.connect() as conn:
            # Explicit BEGIN makes all SELECT statements below observe the same
            # WAL snapshot, even while actors and relays continue committing.
            conn.execute("BEGIN")
            ownership = self._select_one(
                conn,
                """
                SELECT *
                FROM agent_session_runtime_ownership
                WHERE profile_id = ? AND session_id = ?
                """,
                (key.profile_id, key.session_id),
            )
            ownership_events = self._select_many(
                conn,
                """
                SELECT *
                FROM agent_session_runtime_ownership_events
                WHERE profile_id = ? AND session_id = ?
                ORDER BY event_seq DESC
                LIMIT ?
                """,
                (key.profile_id, key.session_id, self._recent_limit),
            )
            aggregate = self._select_one(
                conn,
                """
                SELECT *
                FROM agent_session_aggregates
                WHERE profile_id = ? AND session_id = ?
                """,
                (key.profile_id, key.session_id),
            )
            mailbox = self._read_collection(conn, "agent_session_mailbox", key)
            operations = self._read_collection(conn, "agent_session_operations", key)
            effects = self._read_collection(conn, "agent_effect_outbox", key)
            external_action_receipts = self._read_collection(
                conn,
                "agent_external_action_receipts",
                key,
            )
            external_action_attempts = self._read_external_action_attempts(
                conn,
                key,
            )
            review_schedules = self._read_collection(conn, "agent_review_schedules", key)
            current_review_schedule = self._read_current_schedule(
                conn,
                key,
                aggregate=aggregate,
            )
            route_deliveries = self._read_collection(conn, "agent_route_outbox", key)
            routing_jobs = self._read_routing_jobs(
                conn,
                key=key,
                route_deliveries=route_deliveries,
            )
            state_transitions = self._select_many(
                conn,
                """
                SELECT *
                FROM agent_state_transitions
                WHERE profile_id = ? AND session_id = ?
                ORDER BY transition_seq DESC
                LIMIT ?
                """,
                (key.profile_id, key.session_id, self._recent_limit),
            )
            review_schedule_events = self._select_many(
                conn,
                """
                SELECT *
                FROM agent_review_schedule_events
                WHERE profile_id = ? AND session_id = ?
                ORDER BY schedule_event_seq DESC
                LIMIT ?
                """,
                (key.profile_id, key.session_id, self._recent_limit),
            )
            legacy_session_id = (
                str(ownership.get("legacy_session_id") or "").strip()
                if ownership is not None
                else ""
            )
            legacy = (
                self._read_legacy(conn, legacy_session_id)
                if legacy_session_id
                else None
            )

        return AgentRuntimeDiagnosticsSnapshot(
            key=key,
            ownership=ownership,
            ownership_events=ownership_events,
            aggregate=aggregate,
            mailbox=mailbox,
            operations=operations,
            effects=effects,
            external_action_receipts=external_action_receipts,
            external_action_attempts=external_action_attempts,
            review_schedules=review_schedules,
            current_review_schedule=current_review_schedule,
            route_deliveries=route_deliveries,
            routing_jobs=routing_jobs,
            state_transitions=state_transitions,
            review_schedule_events=review_schedule_events,
            legacy=legacy,
        )

    def _read_collection(
        self,
        conn: Connection,
        table: str,
        key: SessionKey,
    ) -> DiagnosticCollectionSnapshot:
        order_by = _COLLECTION_QUERIES.get(table)
        if order_by is None:
            raise ValueError(f"unsupported diagnostic collection: {table}")
        count_rows = conn.execute(
            f"""
            SELECT status, COUNT(*) AS item_count
            FROM {table}
            WHERE profile_id = ? AND session_id = ?
            GROUP BY status
            """,  # noqa: S608 - table names come from the private whitelist above.
            (key.profile_id, key.session_id),
        ).fetchall()
        by_status = {
            str(row["status"]): int(row["item_count"] or 0)
            for row in count_rows
        }
        rows = conn.execute(
            f"""
            SELECT *
            FROM {table}
            WHERE profile_id = ? AND session_id = ?
            ORDER BY {order_by}
            LIMIT ?
            """,  # noqa: S608 - table and ordering come from the private whitelist above.
            (key.profile_id, key.session_id, self._recent_limit),
        ).fetchall()
        return DiagnosticCollectionSnapshot(
            total=sum(by_status.values()),
            by_status=by_status,
            recent=tuple(self._row(row) for row in rows),
        )

    def _read_external_action_attempts(
        self,
        conn: Connection,
        key: SessionKey,
    ) -> DiagnosticCollectionSnapshot:
        count_rows = conn.execute(
            """
            SELECT attempt.status, COUNT(*) AS item_count
            FROM agent_external_action_attempts AS attempt
            JOIN agent_external_action_receipts AS receipt
              ON receipt.idempotency_key = attempt.idempotency_key
            WHERE receipt.profile_id = ? AND receipt.session_id = ?
            GROUP BY attempt.status
            """,
            (key.profile_id, key.session_id),
        ).fetchall()
        by_status = {
            str(row["status"]): int(row["item_count"] or 0)
            for row in count_rows
        }
        rows = conn.execute(
            """
            SELECT attempt.*
            FROM agent_external_action_attempts AS attempt
            JOIN agent_external_action_receipts AS receipt
              ON receipt.idempotency_key = attempt.idempotency_key
            WHERE receipt.profile_id = ? AND receipt.session_id = ?
            ORDER BY attempt.attempt_seq DESC
            LIMIT ?
            """,
            (key.profile_id, key.session_id, self._recent_limit),
        ).fetchall()
        return DiagnosticCollectionSnapshot(
            total=sum(by_status.values()),
            by_status=by_status,
            recent=tuple(self._row(row) for row in rows),
        )

    @staticmethod
    def _read_current_schedule(
        conn: Connection,
        key: SessionKey,
        *,
        aggregate: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if aggregate is None:
            return None
        current_plan_id = str(aggregate.get("current_plan_id") or "").strip()
        if not current_plan_id:
            return None
        return AgentRuntimeDiagnosticsRepository._select_one(
            conn,
            """
            SELECT *
            FROM agent_review_schedules
            WHERE profile_id = ? AND session_id = ? AND plan_id = ?
            """,
            (key.profile_id, key.session_id, current_plan_id),
        )

    def _read_routing_jobs(
        self,
        conn: Connection,
        *,
        key: SessionKey,
        route_deliveries: DiagnosticCollectionSnapshot,
    ) -> tuple[dict[str, Any], ...]:
        columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(message_routing_jobs)").fetchall()
        }
        if {"profile_id", "session_id"}.issubset(columns):
            direct_rows = conn.execute(
                """
                SELECT *
                FROM message_routing_jobs
                WHERE profile_id = ? AND session_id = ?
                ORDER BY routing_job_seq DESC
                LIMIT ?
                """,
                (key.profile_id, key.session_id, self._recent_limit),
            ).fetchall()
            direct_ids = {
                str(row["routing_job_id"])
                for row in direct_rows
            }
            legacy_ids = tuple(
                dict.fromkeys(
                    str(row.get("routing_job_id") or "").strip()
                    for row in route_deliveries.recent
                    if str(row.get("routing_job_id") or "").strip()
                    and str(row.get("routing_job_id") or "").strip()
                    not in direct_ids
                )
            )
            compatibility_rows = []
            if legacy_ids:
                placeholders = ", ".join("?" for _item in legacy_ids)
                compatibility_rows = conn.execute(
                    f"""
                    SELECT *
                    FROM message_routing_jobs
                    WHERE routing_job_id IN ({placeholders})
                    """,  # noqa: S608 - placeholders are generated and values remain bound.
                    legacy_ids,
                ).fetchall()
            rows = sorted(
                [*direct_rows, *compatibility_rows],
                key=lambda row: int(row["routing_job_seq"]),
                reverse=True,
            )[: self._recent_limit]
            return tuple(AgentRuntimeDiagnosticsRepository._row(row) for row in rows)

        # Compatibility for databases created before routing jobs carried a
        # canonical SessionKey. Such stores can only associate completed
        # decisions through their route outbox rows.
        routing_job_ids = tuple(
            dict.fromkeys(
                str(row.get("routing_job_id") or "").strip()
                for row in route_deliveries.recent
                if str(row.get("routing_job_id") or "").strip()
            )
        )
        if not routing_job_ids:
            return ()
        placeholders = ", ".join("?" for _item in routing_job_ids)
        rows = conn.execute(
            f"""
            SELECT *
            FROM message_routing_jobs
            WHERE routing_job_id IN ({placeholders})
            ORDER BY routing_job_seq DESC
            """,  # noqa: S608 - placeholders are generated, values remain bound.
            routing_job_ids,
        ).fetchall()
        return tuple(AgentRuntimeDiagnosticsRepository._row(row) for row in rows)

    @staticmethod
    def _read_legacy(
        conn: Connection,
        legacy_session_id: str,
    ) -> LegacyRuntimeDiagnosticsSnapshot:
        scheduler_state = AgentRuntimeDiagnosticsRepository._select_one(
            conn,
            "SELECT * FROM agent_scheduler_states WHERE session_id = ?",
            (legacy_session_id,),
        )
        unread_messages_row = conn.execute(
            """
            SELECT
                COUNT(*) AS total_rows,
                COALESCE(SUM(
                    CASE
                        WHEN review_consumed = 0 AND chat_consumed = 0 THEN 1
                        ELSE 0
                    END
                ), 0) AS pending,
                COALESCE(SUM(
                    CASE
                        WHEN review_consumed = 0 AND chat_consumed = 0 AND (
                            is_mentioned = 1
                            OR is_reply_to_bot = 1
                            OR is_poke_to_bot = 1
                        ) THEN 1
                        ELSE 0
                    END
                ), 0) AS high_priority_pending,
                COALESCE(SUM(review_consumed), 0) AS review_consumed_rows,
                COALESCE(SUM(chat_consumed), 0) AS chat_consumed_rows
            FROM agent_unread_messages
            WHERE session_id = ?
            """,
            (legacy_session_id,),
        ).fetchone()
        unread_ranges_row = conn.execute(
            """
            SELECT
                COUNT(*) AS total_rows,
                COALESCE(SUM(
                    CASE
                        WHEN review_consumed = 0 AND chat_consumed = 0 THEN 1
                        ELSE 0
                    END
                ), 0) AS pending,
                COALESCE(SUM(message_count), 0) AS total_message_count,
                COALESCE(SUM(
                    CASE
                        WHEN review_consumed = 0 AND chat_consumed = 0
                            THEN message_count
                        ELSE 0
                    END
                ), 0) AS pending_message_count,
                COALESCE(SUM(review_consumed), 0) AS review_consumed_rows,
                COALESCE(SUM(chat_consumed), 0) AS chat_consumed_rows
            FROM agent_unread_ranges
            WHERE session_id = ?
            """,
            (legacy_session_id,),
        ).fetchone()
        return LegacyRuntimeDiagnosticsSnapshot(
            session_id=legacy_session_id,
            scheduler_state=scheduler_state,
            unread_messages=AgentRuntimeDiagnosticsRepository._integer_row(
                unread_messages_row
            ),
            unread_ranges=AgentRuntimeDiagnosticsRepository._integer_row(
                unread_ranges_row
            ),
        )

    @staticmethod
    def _select_one(
        conn: Connection,
        query: str,
        params: tuple[Any, ...],
    ) -> dict[str, Any] | None:
        row = conn.execute(query, params).fetchone()
        return AgentRuntimeDiagnosticsRepository._row(row) if row is not None else None

    @staticmethod
    def _select_many(
        conn: Connection,
        query: str,
        params: tuple[Any, ...],
    ) -> tuple[dict[str, Any], ...]:
        return tuple(
            AgentRuntimeDiagnosticsRepository._row(row)
            for row in conn.execute(query, params).fetchall()
        )

    @staticmethod
    def _row(row: Any) -> dict[str, Any]:
        return {str(key): row[key] for key in row.keys()}

    @staticmethod
    def _integer_row(row: Any) -> dict[str, int]:
        if row is None:
            return {}
        return {str(key): int(row[key] or 0) for key in row.keys()}


__all__ = [
    "AgentRuntimeDiagnosticsRepository",
    "AgentRuntimeDiagnosticsSnapshot",
    "DIAGNOSTIC_RECENT_LIMIT",
    "DiagnosticCollectionSnapshot",
    "LegacyRuntimeDiagnosticsSnapshot",
]
