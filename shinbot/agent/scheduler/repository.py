"""SQLite-backed Agent scheduler stores."""

from __future__ import annotations

import json
import time

from shinbot.agent.scheduler.inbox import AgentInbox
from shinbot.agent.scheduler.models import (
    ActiveReplyThreshold,
    AgentState,
    HighPriorityEvent,
    HighPriorityEventKind,
    MentionSensitivity,
    ReviewPlan,
    UnreadMessage,
)
from shinbot.agent.scheduler.state_store import AgentStateStore
from shinbot.persistence.repositories.base import Repository


class AgentSchedulerRepository(Repository, AgentInbox, AgentStateStore):
    """Persistence-backed inbox and state store for AgentScheduler."""

    def get_state(self, session_id: str) -> AgentState:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT state
                FROM agent_scheduler_states
                WHERE session_id = ?
                """,
                (session_id,),
            ).fetchone()
        if row is None:
            return AgentState.IDLE
        try:
            return AgentState(str(row["state"]))
        except ValueError:
            return AgentState.IDLE

    def get_review_plan(self, session_id: str) -> ReviewPlan | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT session_id, next_review_at, review_reason,
                       mention_sensitivity, active_reply_threshold_json, updated_at
                FROM agent_scheduler_states
                WHERE session_id = ?
                """,
                (session_id,),
            ).fetchone()
        if row is None or row["next_review_at"] is None:
            return None
        return self._review_plan_from_row(row)

    def set_review_plan(self, plan: ReviewPlan) -> None:
        current_state = self.get_state(plan.session_id)
        threshold_json = json.dumps(
            {
                "at_count": plan.active_reply_threshold.at_count,
                "window_seconds": plan.active_reply_threshold.window_seconds,
            },
            ensure_ascii=False,
        )
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO agent_scheduler_states (
                    session_id, state, next_review_at, review_reason,
                    mention_sensitivity, active_reply_threshold_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    next_review_at = excluded.next_review_at,
                    review_reason = excluded.review_reason,
                    mention_sensitivity = excluded.mention_sensitivity,
                    active_reply_threshold_json = excluded.active_reply_threshold_json,
                    updated_at = excluded.updated_at
                """,
                (
                    plan.session_id,
                    current_state.value,
                    plan.next_review_at,
                    plan.reason,
                    plan.mention_sensitivity.value,
                    threshold_json,
                    plan.updated_at,
                ),
            )

    def list_due_review_plans(self, *, now: float, limit: int = 50) -> list[ReviewPlan]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT session_id, next_review_at, review_reason,
                       mention_sensitivity, active_reply_threshold_json, updated_at
                FROM agent_scheduler_states
                WHERE next_review_at IS NOT NULL
                  AND next_review_at <= ?
                ORDER BY next_review_at ASC, session_id ASC
                LIMIT ?
                """,
                (now, limit),
            ).fetchall()
        return [self._review_plan_from_row(row) for row in rows]

    def set_state(self, session_id: str, state: AgentState) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO agent_scheduler_states (session_id, state, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    state = excluded.state,
                    updated_at = excluded.updated_at
                """,
                (session_id, state.value, time.time()),
            )

    def add_unread(self, message: UnreadMessage) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO agent_unread_messages (
                    session_id, message_log_id, sender_id, created_at,
                    review_consumed, chat_consumed
                ) VALUES (?, ?, ?, ?, 0, 0)
                """,
                (
                    message.session_id,
                    message.message_log_id,
                    message.sender_id,
                    message.created_at,
                ),
            )

    def list_unread(self, session_id: str) -> list[UnreadMessage]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT session_id, message_log_id, sender_id, created_at
                FROM agent_unread_messages
                WHERE session_id = ?
                  AND review_consumed = 0
                ORDER BY created_at ASC, id ASC
                """,
                (session_id,),
            ).fetchall()
        return [
            UnreadMessage(
                session_id=str(row["session_id"]),
                message_log_id=int(row["message_log_id"]),
                sender_id=str(row["sender_id"]),
                created_at=float(row["created_at"]),
            )
            for row in rows
        ]

    def add_high_priority_events(self, events: list[HighPriorityEvent]) -> None:
        if not events:
            return
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT INTO agent_high_priority_events (
                    session_id, message_log_id, sender_id, kind, reason, created_at, handled
                ) VALUES (?, ?, ?, ?, ?, ?, 0)
                """,
                [
                    (
                        event.session_id,
                        event.message_log_id,
                        event.sender_id,
                        event.kind.value,
                        event.reason,
                        event.created_at,
                    )
                    for event in events
                ],
            )

    def list_high_priority_events(self, session_id: str) -> list[HighPriorityEvent]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT session_id, message_log_id, sender_id, kind, reason, created_at
                FROM agent_high_priority_events
                WHERE session_id = ?
                  AND handled = 0
                ORDER BY created_at ASC, id ASC
                """,
                (session_id,),
            ).fetchall()
        return [self._high_priority_from_row(row) for row in rows]

    def mark_high_priority_events_handled(self, session_id: str) -> list[HighPriorityEvent]:
        events = self.list_high_priority_events(session_id)
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE agent_high_priority_events
                SET handled = 1
                WHERE session_id = ?
                  AND handled = 0
                """,
                (session_id,),
            )
        return events

    def record_mention(self, session_id: str, timestamp: float) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO agent_recent_mentions (session_id, timestamp)
                VALUES (?, ?)
                """,
                (session_id, timestamp),
            )

    def count_recent_mentions(self, session_id: str, *, now: float, window_seconds: float) -> int:
        cutoff = now - window_seconds
        with self.connect() as conn:
            conn.execute(
                """
                DELETE FROM agent_recent_mentions
                WHERE session_id = ?
                  AND timestamp < ?
                """,
                (session_id, cutoff),
            )
            row = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM agent_recent_mentions
                WHERE session_id = ?
                  AND timestamp >= ?
                """,
                (session_id, cutoff),
            ).fetchone()
        return int(row["cnt"]) if row is not None else 0

    @staticmethod
    def _high_priority_from_row(row) -> HighPriorityEvent:
        try:
            kind = HighPriorityEventKind(str(row["kind"]))
        except ValueError:
            kind = HighPriorityEventKind.MENTION
        return HighPriorityEvent(
            session_id=str(row["session_id"]),
            message_log_id=int(row["message_log_id"]),
            sender_id=str(row["sender_id"]),
            kind=kind,
            created_at=float(row["created_at"]),
            reason=str(row["reason"]),
        )

    @staticmethod
    def _review_plan_from_row(row) -> ReviewPlan:
        try:
            sensitivity = MentionSensitivity(str(row["mention_sensitivity"]))
        except ValueError:
            sensitivity = MentionSensitivity.NORMAL
        try:
            threshold_payload = json.loads(row["active_reply_threshold_json"] or "{}")
        except Exception:
            threshold_payload = {}
        return ReviewPlan(
            session_id=str(row["session_id"]),
            next_review_at=float(row["next_review_at"]),
            reason=str(row["review_reason"] or ""),
            mention_sensitivity=sensitivity,
            active_reply_threshold=ActiveReplyThreshold(
                at_count=int(threshold_payload.get("at_count") or 1),
                window_seconds=float(threshold_payload.get("window_seconds") or 60.0),
            ),
            updated_at=float(row["updated_at"] or 0.0),
        )


__all__ = ["AgentSchedulerRepository"]
