"""SQLite-backed Agent scheduler stores."""

from __future__ import annotations

import json
import time

from shinbot.agent.scheduler.inbox import AgentInbox
from shinbot.agent.scheduler.models import (
    ActiveChatDisposition,
    ActiveChatState,
    ActiveReplyThreshold,
    AgentState,
    HighPriorityEvent,
    HighPriorityEventKind,
    MentionSensitivity,
    ReviewPlan,
    UnreadMessage,
    UnreadRange,
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

    def get_active_chat_state(self, session_id: str) -> ActiveChatState | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT active_chat_state_json
                FROM agent_scheduler_states
                WHERE session_id = ?
                """,
                (session_id,),
            ).fetchone()
        if row is None:
            return None
        try:
            payload = json.loads(row["active_chat_state_json"] or "{}")
        except Exception:
            return None
        if not payload:
            return None
        disposition_value = payload.get("bootstrap_disposition")
        try:
            bootstrap_disposition = (
                ActiveChatDisposition(str(disposition_value))
                if disposition_value
                else None
            )
        except ValueError:
            bootstrap_disposition = None
        return ActiveChatState(
            session_id=session_id,
            interest_value=float(payload.get("interest_value") or 0.0),
            decay_half_life_seconds=float(payload.get("decay_half_life_seconds") or 0.0),
            entered_at=float(payload.get("entered_at") or 0.0),
            updated_at=float(payload.get("updated_at") or 0.0),
            tick_count=int(payload.get("tick_count") or 0),
            active_epoch=int(payload.get("active_epoch") or 0),
            bootstrap_applied=bool(payload.get("bootstrap_applied") or False),
            bootstrap_disposition=bootstrap_disposition,
        )

    def set_active_chat_state(self, state: ActiveChatState) -> None:
        payload = json.dumps(
            {
                "interest_value": state.interest_value,
                "decay_half_life_seconds": state.decay_half_life_seconds,
                "entered_at": state.entered_at,
                "updated_at": state.updated_at,
                "tick_count": state.tick_count,
                "active_epoch": state.active_epoch,
                "bootstrap_applied": state.bootstrap_applied,
                "bootstrap_disposition": (
                    state.bootstrap_disposition.value
                    if state.bootstrap_disposition is not None
                    else None
                ),
            },
            ensure_ascii=False,
        )
        current_state = self.get_state(state.session_id)
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO agent_scheduler_states (
                    session_id, state, active_chat_state_json, updated_at
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    active_chat_state_json = excluded.active_chat_state_json,
                    updated_at = excluded.updated_at
                """,
                (state.session_id, current_state.value, payload, state.updated_at),
            )

    def clear_active_chat_state(self, session_id: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE agent_scheduler_states
                SET active_chat_state_json = '{}',
                    updated_at = ?
                WHERE session_id = ?
                """,
                (time.time(), session_id),
            )

    def add_unread(self, message: UnreadMessage) -> None:
        with self.connect() as conn:
            existing = conn.execute(
                """
                SELECT id
                FROM agent_unread_ranges
                WHERE session_id = ?
                  AND review_consumed = 0
                  AND start_msg_log_id <= ?
                  AND end_msg_log_id >= ?
                LIMIT 1
                """,
                (
                    message.session_id,
                    message.message_log_id,
                    message.message_log_id,
                ),
            ).fetchone()
            if existing is not None:
                return

            tail = conn.execute(
                """
                SELECT id, end_msg_log_id, end_at, message_count
                FROM agent_unread_ranges
                WHERE session_id = ?
                  AND review_consumed = 0
                  AND chat_consumed = 0
                ORDER BY end_at DESC, end_msg_log_id DESC
                LIMIT 1
                """,
                (message.session_id,),
            ).fetchone()
            if tail is not None and self._can_extend_tail_range(conn, message, tail):
                conn.execute(
                    """
                    UPDATE agent_unread_ranges
                    SET end_msg_log_id = ?,
                        end_at = ?,
                        message_count = message_count + 1
                    WHERE id = ?
                    """,
                    (message.message_log_id, message.created_at, int(tail["id"])),
                )
                return

            conn.execute(
                """
                INSERT INTO agent_unread_ranges (
                    session_id, start_msg_log_id, end_msg_log_id, start_at, end_at,
                    message_count, review_consumed, chat_consumed
                ) VALUES (?, ?, ?, ?, ?, 1, 0, 0)
                """,
                (
                    message.session_id,
                    message.message_log_id,
                    message.message_log_id,
                    message.created_at,
                    message.created_at,
                ),
            )

    def list_unread(self, session_id: str) -> list[UnreadMessage]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT m.session_id, m.id AS message_log_id, m.sender_id,
                       m.created_at, m.is_mentioned
                FROM agent_unread_ranges r
                JOIN message_logs m
                  ON m.session_id = r.session_id
                 AND m.id >= r.start_msg_log_id
                 AND m.id <= r.end_msg_log_id
                WHERE r.session_id = ?
                  AND r.review_consumed = 0
                  AND r.chat_consumed = 0
                ORDER BY m.created_at ASC, m.id ASC
                """,
                (session_id,),
            ).fetchall()
        return [
            UnreadMessage(
                session_id=str(row["session_id"]),
                message_log_id=int(row["message_log_id"]),
                sender_id=str(row["sender_id"]),
                created_at=float(row["created_at"]),
                is_mentioned=bool(row["is_mentioned"]),
            )
            for row in rows
        ]

    def list_unread_ranges(self, session_id: str, *, limit: int = 50) -> list[UnreadRange]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, session_id, start_msg_log_id, end_msg_log_id,
                       start_at, end_at, message_count, review_consumed, chat_consumed
                FROM agent_unread_ranges
                WHERE session_id = ?
                  AND review_consumed = 0
                  AND chat_consumed = 0
                ORDER BY start_at ASC, start_msg_log_id ASC
                LIMIT ?
                """,
                (session_id, limit),
            ).fetchall()
        return [self._unread_range_from_row(row) for row in rows]

    def count_unread_messages(self, session_id: str) -> int:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT COALESCE(SUM(message_count), 0) AS cnt
                FROM agent_unread_ranges
                WHERE session_id = ?
                  AND review_consumed = 0
                  AND chat_consumed = 0
                """,
                (session_id,),
            ).fetchone()
        return int(row["cnt"]) if row is not None else 0

    def split_review_consumed(
        self,
        *,
        range_id: int,
        consumed_start_msg_log_id: int,
        consumed_end_msg_log_id: int,
    ) -> None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT id, session_id, start_msg_log_id, end_msg_log_id,
                       start_at, end_at, message_count, review_consumed, chat_consumed
                FROM agent_unread_ranges
                WHERE id = ?
                  AND review_consumed = 0
                  AND chat_consumed = 0
                """,
                (range_id,),
            ).fetchone()
            if row is None:
                return

            unread_range = self._unread_range_from_row(row)
            conn.execute("DELETE FROM agent_unread_ranges WHERE id = ?", (range_id,))
            for replacement in self._remaining_ranges_after_consumed(
                conn,
                unread_range,
                consumed_start_msg_log_id=consumed_start_msg_log_id,
                consumed_end_msg_log_id=consumed_end_msg_log_id,
            ):
                conn.execute(
                    """
                    INSERT INTO agent_unread_ranges (
                        session_id, start_msg_log_id, end_msg_log_id, start_at, end_at,
                        message_count, review_consumed, chat_consumed
                    ) VALUES (?, ?, ?, ?, ?, ?, 0, 0)
                    """,
                    (
                        replacement.session_id,
                        replacement.start_msg_log_id,
                        replacement.end_msg_log_id,
                        replacement.start_at,
                        replacement.end_at,
                        replacement.message_count,
                    ),
                )

    def mark_ranges_review_consumed(self, range_ids: list[int]) -> None:
        if not range_ids:
            return
        placeholders = ",".join("?" for _ in range_ids)
        with self.connect() as conn:
            conn.execute(
                f"""
                UPDATE agent_unread_ranges
                SET review_consumed = 1
                WHERE id IN ({placeholders})
                """,
                tuple(range_ids),
            )

    def mark_active_chat_consumed(
        self,
        *,
        session_id: str,
        message_log_ids: list[int],
    ) -> list[UnreadMessage]:
        if not message_log_ids:
            return []
        consumed: list[UnreadMessage] = []
        groups = _group_consecutive_ids(sorted(set(message_log_ids)))
        consumed_message_ids: list[int] = []
        with self.connect() as conn:
            for start_msg_log_id, end_msg_log_id in groups:
                rows = conn.execute(
                    """
                    SELECT id, session_id, start_msg_log_id, end_msg_log_id,
                           start_at, end_at, message_count, review_consumed, chat_consumed
                    FROM agent_unread_ranges
                    WHERE session_id = ?
                      AND review_consumed = 0
                      AND chat_consumed = 0
                      AND end_msg_log_id >= ?
                      AND start_msg_log_id <= ?
                    ORDER BY start_msg_log_id ASC
                    """,
                    (session_id, start_msg_log_id, end_msg_log_id),
                ).fetchall()
                for row in rows:
                    unread_range = self._unread_range_from_row(row)
                    consumed_start = max(unread_range.start_msg_log_id, start_msg_log_id)
                    consumed_end = min(unread_range.end_msg_log_id, end_msg_log_id)
                    if consumed_start > consumed_end:
                        continue
                    consumed_message_ids.extend(range(consumed_start, consumed_end + 1))

                    conn.execute(
                        "DELETE FROM agent_unread_ranges WHERE id = ?",
                        (unread_range.id,),
                    )
                    for replacement, chat_consumed in (
                        (
                            self._range_for_message_bounds(
                                conn,
                                unread_range,
                                start_msg_log_id=unread_range.start_msg_log_id,
                                end_msg_log_id=consumed_start - 1,
                            ),
                            False,
                        ),
                        (
                            self._range_for_message_bounds(
                                conn,
                                unread_range,
                                start_msg_log_id=consumed_start,
                                end_msg_log_id=consumed_end,
                            ),
                            True,
                        ),
                        (
                            self._range_for_message_bounds(
                                conn,
                                unread_range,
                                start_msg_log_id=consumed_end + 1,
                                end_msg_log_id=unread_range.end_msg_log_id,
                            ),
                            False,
                        ),
                    ):
                        if replacement is None:
                            continue
                        conn.execute(
                            """
                            INSERT INTO agent_unread_ranges (
                                session_id, start_msg_log_id, end_msg_log_id,
                                start_at, end_at, message_count,
                                review_consumed, chat_consumed
                            ) VALUES (?, ?, ?, ?, ?, ?, 0, ?)
                            """,
                            (
                                replacement.session_id,
                                replacement.start_msg_log_id,
                                replacement.end_msg_log_id,
                                replacement.start_at,
                                replacement.end_at,
                                replacement.message_count,
                                1 if chat_consumed else 0,
                            ),
                        )

            if consumed_message_ids:
                placeholders = ",".join("?" for _ in consumed_message_ids)
                rows = conn.execute(
                    f"""
                    SELECT session_id, id AS message_log_id, sender_id,
                           created_at, is_mentioned
                    FROM message_logs
                    WHERE session_id = ?
                      AND id IN ({placeholders})
                    ORDER BY created_at ASC, id ASC
                    """,
                    (session_id, *consumed_message_ids),
                ).fetchall()
                consumed = [
                    UnreadMessage(
                        session_id=str(row["session_id"]),
                        message_log_id=int(row["message_log_id"]),
                        sender_id=str(row["sender_id"]),
                        created_at=float(row["created_at"]),
                        is_mentioned=bool(row["is_mentioned"]),
                    )
                    for row in rows
                ]
        return consumed

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
    def _can_extend_tail_range(conn, message: UnreadMessage, tail) -> bool:
        if int(tail["end_msg_log_id"]) >= message.message_log_id:
            return False
        if float(tail["end_at"]) > message.created_at:
            return False
        gap = conn.execute(
            """
            SELECT 1
            FROM message_logs
            WHERE session_id = ?
              AND id > ?
              AND id < ?
            LIMIT 1
            """,
            (message.session_id, int(tail["end_msg_log_id"]), message.message_log_id),
        ).fetchone()
        return gap is None

    @staticmethod
    def _remaining_ranges_after_consumed(
        conn,
        unread_range: UnreadRange,
        *,
        consumed_start_msg_log_id: int,
        consumed_end_msg_log_id: int,
    ) -> list[UnreadRange]:
        ranges: list[UnreadRange] = []
        before = AgentSchedulerRepository._range_for_message_bounds(
            conn,
            unread_range,
            start_msg_log_id=unread_range.start_msg_log_id,
            end_msg_log_id=consumed_start_msg_log_id - 1,
        )
        if before is not None:
            ranges.append(before)
        after = AgentSchedulerRepository._range_for_message_bounds(
            conn,
            unread_range,
            start_msg_log_id=consumed_end_msg_log_id + 1,
            end_msg_log_id=unread_range.end_msg_log_id,
        )
        if after is not None:
            ranges.append(after)
        return ranges

    @staticmethod
    def _range_for_message_bounds(
        conn,
        source: UnreadRange,
        *,
        start_msg_log_id: int,
        end_msg_log_id: int,
    ) -> UnreadRange | None:
        if start_msg_log_id > end_msg_log_id:
            return None
        row = conn.execute(
            """
            SELECT MIN(id) AS start_id,
                   MAX(id) AS end_id,
                   MIN(created_at) AS start_at,
                   MAX(created_at) AS end_at,
                   COUNT(*) AS cnt
            FROM message_logs
            WHERE session_id = ?
              AND id >= ?
              AND id <= ?
            """,
            (source.session_id, start_msg_log_id, end_msg_log_id),
        ).fetchone()
        if row is None or int(row["cnt"] or 0) == 0:
            return None
        return UnreadRange(
            id=None,
            session_id=source.session_id,
            start_msg_log_id=int(row["start_id"]),
            end_msg_log_id=int(row["end_id"]),
            start_at=float(row["start_at"]),
            end_at=float(row["end_at"]),
            message_count=int(row["cnt"]),
            chat_consumed=source.chat_consumed,
        )

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
    def _unread_range_from_row(row) -> UnreadRange:
        return UnreadRange(
            id=int(row["id"]),
            session_id=str(row["session_id"]),
            start_msg_log_id=int(row["start_msg_log_id"]),
            end_msg_log_id=int(row["end_msg_log_id"]),
            start_at=float(row["start_at"]),
            end_at=float(row["end_at"]),
            message_count=int(row["message_count"]),
            review_consumed=bool(row["review_consumed"]),
            chat_consumed=bool(row["chat_consumed"]),
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


def _group_consecutive_ids(message_log_ids: list[int]) -> list[tuple[int, int]]:
    if not message_log_ids:
        return []
    groups: list[tuple[int, int]] = []
    start = message_log_ids[0]
    previous = message_log_ids[0]
    for message_log_id in message_log_ids[1:]:
        if message_log_id == previous + 1:
            previous = message_log_id
            continue
        groups.append((start, previous))
        start = message_log_id
        previous = message_log_id
    groups.append((start, previous))
    return groups


__all__ = ["AgentSchedulerRepository"]
