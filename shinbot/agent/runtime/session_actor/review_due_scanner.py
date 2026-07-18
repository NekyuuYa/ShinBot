"""Durable review-due dispatch for profile-scoped session actors.

The scanner performs no workflow work and does not inspect the actor's busy
state.  Its only job is to atomically fence one current review schedule,
enqueue a deterministic ``ReviewDue`` mailbox event, and mark that schedule
claimed.  Actor reduction decides what the event means for the current state.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import math
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from enum import StrEnum
from sqlite3 import Connection, Row
from typing import TYPE_CHECKING

from shinbot.agent.runtime.service_health import (
    RuntimeServiceHealth,
    RuntimeServiceHealthSnapshot,
    supervised_backoff_seconds,
)
from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.manual_review import (
    MANUAL_REVIEW_EVENT_KIND,
    MANUAL_REVIEW_EVENT_SOURCE,
    ManualReviewRequest,
    ManualReviewRequestError,
)
from shinbot.agent.runtime.session_actor.review_due_identity import (
    REVIEW_DUE_EVENT_KIND,
    REVIEW_DUE_EVENT_SOURCE,
    review_due_event_id,
)
from shinbot.core.dispatch.actor_v2_admission import (
    ActorV2AdmissionFenceConflict,
    ActorV2AdmissionFenceExpired,
    ActorV2AdmissionFenceNotFound,
)
from shinbot.core.dispatch.agent_ownership import (
    AgentRuntimeOwnership,
    AgentRuntimeOwnershipError,
    AgentRuntimeOwnershipMode,
    AgentRuntimeOwnershipStatus,
)
from shinbot.core.dispatch.fenced_wake import FencedMailboxWakeRequest
from shinbot.core.dispatch.mailbox_handoff import MailboxHandoffNotifier
from shinbot.utils.logger import format_log_event, get_logger

if TYPE_CHECKING:
    from shinbot.persistence.engine import DatabaseManager

logger = get_logger(__name__, source="agent:review-due", color="yellow")

GLOBAL_REVIEW_DUE_HEALTH_PROFILE_ID = "__actor_v2_global__"
_SCHEDULE_EVENT_NAMESPACE = uuid.UUID("b022e214-5043-59f4-abcb-a55d654dd7f0")

type _DueScanCursor = tuple[float, float, str, str, int, str]
type _TypedSQLiteRecord = tuple[tuple[str, str, object], ...]


@dataclass(slots=True, frozen=True)
class _ScheduleMailboxDelivery:
    """Immutable mailbox and schedule-journal values for one claimed plan."""

    event_id: str
    kind: str
    source: str
    occurred_at: float
    payload_json: str
    causation_id: str
    correlation_id: str
    trace_id: str
    schedule_event_type: str
    schedule_event_outcome: str
    reason: str
    metadata: Mapping[str, object]


@dataclass(slots=True, frozen=True)
class _ScheduleMailboxAdmission:
    """Exact durable mailbox identity returned by one schedule admission."""

    mailbox_id: int
    mailbox_inserted: bool

    def __post_init__(self) -> None:
        """Reject an admission result that cannot name its committed mailbox."""

        if isinstance(self.mailbox_id, bool) or not isinstance(self.mailbox_id, int):
            raise ValueError("mailbox_id must be an integer")
        if self.mailbox_id < 1:
            raise ValueError("mailbox_id must be positive")


@dataclass(slots=True, frozen=True)
class ReviewWakeCursor:
    """Stable keyset position for one current review mailbox wake debt."""

    mailbox_id: int
    profile_id: str
    session_id: str
    ownership_generation: int
    admission_fence_id: str = ""
    admission_fence_generation: int = 0

    def __post_init__(self) -> None:
        """Reject a cursor that cannot identify one ordered mailbox debt row."""

        if isinstance(self.mailbox_id, bool) or not isinstance(self.mailbox_id, int):
            raise ValueError("mailbox_id must be an integer")
        if self.mailbox_id < 1:
            raise ValueError("mailbox_id must be positive")
        profile_id = str(self.profile_id or "").strip()
        session_id = str(self.session_id or "").strip()
        if not profile_id or not session_id:
            raise ValueError("review wake cursor requires a complete session key")
        if (
            isinstance(self.ownership_generation, bool)
            or not isinstance(self.ownership_generation, int)
            or self.ownership_generation < 1
        ):
            raise ValueError("ownership_generation must be a positive integer")
        fence_id = str(self.admission_fence_id or "").strip()
        fence_generation = self.admission_fence_generation
        if isinstance(fence_generation, bool) or not isinstance(fence_generation, int):
            raise ValueError("admission_fence_generation must be an integer")
        if fence_generation < 0 or bool(fence_id) != bool(fence_generation):
            raise ValueError("review wake cursor fence identity is inconsistent")
        object.__setattr__(self, "profile_id", profile_id)
        object.__setattr__(self, "session_id", session_id)
        object.__setattr__(self, "admission_fence_id", fence_id)


@dataclass(slots=True, frozen=True)
class _PendingReviewWakeDebt:
    """One latest pending mailbox event for an exact Actor incarnation."""

    request: FencedMailboxWakeRequest
    event_id: str
    cursor: ReviewWakeCursor | None = dataclass_field(
        default=None,
        compare=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        """Reject an ambiguous cache key before it can suppress a new delivery."""

        if not isinstance(self.request, FencedMailboxWakeRequest):
            raise TypeError("request must be a FencedMailboxWakeRequest")
        event_id = str(self.event_id or "").strip()
        if not event_id:
            raise ValueError("event_id must not be empty")
        cursor = self.cursor
        if cursor is not None:
            if not isinstance(cursor, ReviewWakeCursor):
                raise TypeError("cursor must be a ReviewWakeCursor")
            if (
                cursor.profile_id != self.request.key.profile_id
                or cursor.session_id != self.request.key.session_id
                or cursor.ownership_generation != self.request.ownership_generation
                or cursor.admission_fence_id != self.request.admission_fence_id
                or cursor.admission_fence_generation
                != self.request.admission_fence_generation
            ):
                raise ValueError("review wake cursor differs from request identity")
        object.__setattr__(self, "event_id", event_id)


class ReviewDueRepositoryError(RuntimeError):
    """Base error raised by durable review-due persistence."""


class ReviewDueConflict(ReviewDueRepositoryError):
    """Raised when deterministic durable identity resolves to other work."""


class ManualReviewAdmissionError(ReviewDueRepositoryError):
    """Raised when a manual request conflicts with durable schedule evidence."""


class ReviewDueDisposition(StrEnum):
    """Durable result of processing one due schedule row."""

    DISPATCHED = "dispatched"
    SUPERSEDED = "superseded"
    RETRY_DEFERRED = "retry_deferred"
    FENCE_SKIPPED = "fence_skipped"


class ManualReviewAdmissionDisposition(StrEnum):
    """Durable outcome for one explicit manual review request."""

    ADMITTED = "admitted"
    DUPLICATE = "duplicate"
    ALREADY_CLAIMED = "already_claimed"
    REJECTED = "rejected"


class _WakeAttemptDisposition(StrEnum):
    """Process-local outcome for one post-commit review wake handoff."""

    HANDLED = "handled"
    DEFERRED = "deferred"
    IN_FLIGHT = "in_flight"
    RETRY = "retry"


class _ScheduleMailboxAdmissionLost(ReviewDueRepositoryError):
    """Carry a final admission failure observed during mailbox handoff staging."""

    def __init__(self, reason: str) -> None:
        normalized_reason = str(reason or "").strip()
        if not normalized_reason:
            raise ValueError("admission-loss reason must not be empty")
        self.reason = normalized_reason
        super().__init__(normalized_reason)


@dataclass(slots=True, frozen=True)
class ReviewDueDispatchResult:
    """Outcome for one schedule inspected in a short transaction."""

    key: SessionKey
    plan_id: str
    plan_revision: int
    ownership_generation: int
    delivery_cycle: int
    disposition: ReviewDueDisposition
    event_id: str = ""
    mailbox_id: int | None = None
    mailbox_inserted: bool = False
    reason: str = ""
    retry_at: float | None = None
    wake_request: FencedMailboxWakeRequest | None = None


@dataclass(slots=True, frozen=True)
class ManualReviewAdmissionResult:
    """Outcome of atomically admitting one manual review request."""

    key: SessionKey
    request_id: str
    disposition: ManualReviewAdmissionDisposition
    event_id: str = ""
    plan_id: str = ""
    plan_revision: int = 0
    ownership_generation: int = 0
    mailbox_id: int | None = None
    mailbox_inserted: bool = False
    reason: str = ""
    wake_request: FencedMailboxWakeRequest | None = None

    @property
    def accepted(self) -> bool:
        """Return whether the request has a durable mailbox identity."""

        return self.disposition in {
            ManualReviewAdmissionDisposition.ADMITTED,
            ManualReviewAdmissionDisposition.DUPLICATE,
        }


@dataclass(slots=True, frozen=True)
class ReviewDueScanSummary:
    """Bounded aggregate result for one repository scan pass."""

    results: tuple[ReviewDueDispatchResult, ...] = ()

    @property
    def attempted_count(self) -> int:
        """Return the number of schedule rows removed from the due page."""

        return len(self.results)

    @property
    def dispatched_count(self) -> int:
        """Return the number of current plans durably dispatched."""

        return sum(
            result.disposition is ReviewDueDisposition.DISPATCHED
            for result in self.results
        )

    @property
    def superseded_count(self) -> int:
        """Return the number of stale plans atomically superseded."""

        return sum(
            result.disposition is ReviewDueDisposition.SUPERSEDED
            for result in self.results
        )

    @property
    def deferred_count(self) -> int:
        """Return the number of unavailable rows assigned an explicit retry."""

        return sum(
            result.disposition is ReviewDueDisposition.RETRY_DEFERRED
            for result in self.results
        )

    @property
    def fence_skipped_count(self) -> int:
        """Return rows left untouched because no exact actor fence existed."""

        return sum(
            result.disposition is ReviewDueDisposition.FENCE_SKIPPED
            for result in self.results
        )

    @property
    def skipped_count(self) -> int:
        """Return rows that did not dispatch a ReviewDue mailbox event."""

        return self.attempted_count - self.dispatched_count

    @property
    def dispatched_keys(self) -> tuple[SessionKey, ...]:
        """Return unique keys which received a committed mailbox event."""

        return _unique_keys(
            result.key
            for result in self.results
            if result.disposition is ReviewDueDisposition.DISPATCHED
        )

    @property
    def dispatched_wake_requests(self) -> tuple[FencedMailboxWakeRequest, ...]:
        """Return exact ownership identities for committed mailbox deliveries."""

        requests: list[FencedMailboxWakeRequest] = []
        for result in self.results:
            if result.disposition is not ReviewDueDisposition.DISPATCHED:
                continue
            if result.wake_request is not None:
                requests.append(result.wake_request)
        return _unique_wake_requests(requests)

    @property
    def dispatched_mailbox_ids(self) -> tuple[int, ...]:
        """Return exact durable identities for newly dispatched review events."""

        mailbox_ids: list[int] = []
        seen: set[int] = set()
        for result in self.results:
            if result.disposition is not ReviewDueDisposition.DISPATCHED:
                continue
            mailbox_id = result.mailbox_id
            if mailbox_id is None:
                raise ReviewDueConflict("dispatched review result is missing mailbox_id")
            if mailbox_id not in seen:
                seen.add(mailbox_id)
                mailbox_ids.append(mailbox_id)
        return tuple(mailbox_ids)


class DurableReviewDueRepository:
    """SQLite repository for exact current-plan ReviewDue dispatch."""

    def __init__(
        self,
        database: DatabaseManager,
        *,
        retry_base_seconds: float = 5.0,
        retry_max_seconds: float = 300.0,
        clock: Callable[[], float] | None = None,
        profile_id: str | None = None,
    ) -> None:
        """Initialize bounded retry and optional profile filtering."""

        self._database = database
        self._retry_base_seconds = _positive_finite(
            retry_base_seconds,
            field_name="retry_base_seconds",
        )
        self._retry_max_seconds = _positive_finite(
            retry_max_seconds,
            field_name="retry_max_seconds",
        )
        if self._retry_max_seconds < self._retry_base_seconds:
            raise ValueError("retry_max_seconds cannot be below retry_base_seconds")
        self._clock = clock or time.time
        normalized_profile_id = None
        if profile_id is not None:
            normalized_profile_id = str(profile_id or "").strip()
            if not normalized_profile_id:
                raise ValueError("profile_id filter must not be empty")
        self._profile_id = normalized_profile_id

    @property
    def health_profile_id(self) -> str:
        """Return the durable service-health ownership key for this scanner."""

        return self._profile_id or GLOBAL_REVIEW_DUE_HEALTH_PROFILE_ID

    def _require_actor_admission(
        self,
        conn: Connection,
        *,
        key: SessionKey,
        owner: Row | None,
    ) -> tuple[AgentRuntimeOwnership | None, str]:
        """Return the exact active owner or why this transaction cannot write."""

        unavailable_reason = _ownership_unavailable_reason(owner)
        if unavailable_reason:
            return None, unavailable_reason
        assert owner is not None
        try:
            ownership = self._database.agent_runtime_ownership.require_actor_v2_in_transaction(
                conn,
                key,
                expected_generation=int(owner["generation"]),
            )
        except ActorV2AdmissionFenceExpired:
            return None, "admission_fence_expired"
        except ActorV2AdmissionFenceNotFound:
            return None, "admission_fence_missing"
        except ActorV2AdmissionFenceConflict:
            fence = conn.execute(
                """
                SELECT status
                FROM agent_session_actor_v2_admission_fences
                WHERE profile_id = ? AND session_id = ?
                """,
                (key.profile_id, key.session_id),
            ).fetchone()
            if fence is not None and str(fence["status"]) == "revoked":
                return None, "admission_fence_revoked"
            return None, "admission_fence_invalid"
        return ownership, ""

    def _final_actor_admission_reason(
        self,
        conn: Connection,
        *,
        key: SessionKey,
        ownership: AgentRuntimeOwnership,
    ) -> str:
        """Return why a staged candidate can no longer commit under its fence."""

        try:
            self._database.agent_runtime_ownership.require_actor_v2_in_transaction(
                conn,
                key,
                expected_generation=ownership.generation,
                expected_admission_fence_id=ownership.admission_fence_id,
                expected_admission_fence_generation=(
                    ownership.admission_fence_generation
                ),
            )
        except ActorV2AdmissionFenceExpired:
            return "admission_fence_expired"
        except ActorV2AdmissionFenceNotFound:
            return "admission_fence_missing"
        except ActorV2AdmissionFenceConflict:
            fence = conn.execute(
                """
                SELECT status
                FROM agent_session_actor_v2_admission_fences
                WHERE profile_id = ? AND session_id = ?
                """,
                (key.profile_id, key.session_id),
            ).fetchone()
            if fence is not None and str(fence["status"]) == "revoked":
                return "admission_fence_revoked"
            return "admission_fence_invalid"
        except AgentRuntimeOwnershipError:
            return "actor_v2_ownership_changed"
        return ""

    def dispatch_due(self, *, limit: int = 50) -> ReviewDueScanSummary:
        """Process at most ``limit`` rows, each in its own write transaction."""

        normalized_limit = _positive_int(limit, field_name="limit")
        results: list[ReviewDueDispatchResult] = []
        cursor: _DueScanCursor | None = None
        for _index in range(normalized_limit):
            scanned = self._dispatch_next_due(after=cursor)
            if scanned is None:
                break
            result, cursor = scanned
            results.append(result)
        return ReviewDueScanSummary(results=tuple(results))

    def admit_manual_review(
        self,
        key: SessionKey,
        *,
        request_id: str,
        requested_by: str,
        reason: str = "manual_review_requested",
    ) -> ManualReviewAdmissionResult:
        """Atomically claim the current schedule and enqueue a manual request.

        The admission transaction owns the schedule claim, not merely the
        mailbox write. A due scanner therefore cannot enqueue a competing
        ``ReviewDue`` event after an operator request has been accepted.
        """

        normalized_request_id = str(request_id or "").strip()
        normalized_requested_by = str(requested_by or "").strip()
        normalized_reason = str(reason or "").strip()
        if not normalized_request_id:
            raise ManualReviewAdmissionError("request_id must not be empty")
        if not normalized_requested_by:
            raise ManualReviewAdmissionError("requested_by must not be empty")
        if not normalized_reason:
            raise ManualReviewAdmissionError("reason must not be empty")
        if self._profile_id is not None and key.profile_id != self._profile_id:
            return ManualReviewAdmissionResult(
                key=key,
                request_id=normalized_request_id,
                disposition=ManualReviewAdmissionDisposition.REJECTED,
                reason="profile_filter_mismatch",
            )
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = _nonnegative_finite(self._clock(), field_name="clock")
            owner = conn.execute(
                """
                SELECT mode, status, generation
                FROM agent_session_runtime_ownership
                WHERE profile_id = ? AND session_id = ?
                """,
                (key.profile_id, key.session_id),
            ).fetchone()
            ownership, unavailable_reason = self._require_actor_admission(
                conn,
                key=key,
                owner=owner,
            )
            if unavailable_reason:
                return ManualReviewAdmissionResult(
                    key=key,
                    request_id=normalized_request_id,
                    disposition=ManualReviewAdmissionDisposition.REJECTED,
                    reason=unavailable_reason,
                )
            assert ownership is not None
            ownership_generation = ownership.generation
            wake_request = _wake_request_for_ownership(ownership)
            duplicate = self._manual_review_duplicate_for_request_id(
                conn,
                key=key,
                ownership_generation=ownership_generation,
                wake_request=wake_request,
                request_id=normalized_request_id,
                requested_by=normalized_requested_by,
                reason=normalized_reason,
            )
            if duplicate is not None:
                return duplicate
            aggregate = conn.execute(
                """
                SELECT ownership_generation, current_plan_id,
                       review_plan_revision, state_revision
                FROM agent_session_aggregates
                WHERE profile_id = ? AND session_id = ?
                """,
                (key.profile_id, key.session_id),
            ).fetchone()
            if aggregate is None:
                return ManualReviewAdmissionResult(
                    key=key,
                    request_id=normalized_request_id,
                    disposition=ManualReviewAdmissionDisposition.REJECTED,
                    ownership_generation=ownership_generation,
                    reason="aggregate_missing",
                )
            if int(aggregate["ownership_generation"]) != ownership_generation:
                return ManualReviewAdmissionResult(
                    key=key,
                    request_id=normalized_request_id,
                    disposition=ManualReviewAdmissionDisposition.REJECTED,
                    ownership_generation=ownership_generation,
                    reason="aggregate_generation_mismatch",
                )
            plan_id = str(aggregate["current_plan_id"])
            plan_revision = int(aggregate["review_plan_revision"])
            if not plan_id or plan_revision < 1:
                return ManualReviewAdmissionResult(
                    key=key,
                    request_id=normalized_request_id,
                    disposition=ManualReviewAdmissionDisposition.REJECTED,
                    ownership_generation=ownership_generation,
                    reason="current_review_plan_missing",
                )
            schedule = conn.execute(
                """
                SELECT *
                FROM agent_review_schedules
                WHERE profile_id = ? AND session_id = ?
                  AND ownership_generation = ?
                  AND plan_id = ? AND plan_revision = ?
                """,
                (
                    key.profile_id,
                    key.session_id,
                    ownership_generation,
                    plan_id,
                    plan_revision,
                ),
            ).fetchone()
            if schedule is None:
                return ManualReviewAdmissionResult(
                    key=key,
                    request_id=normalized_request_id,
                    disposition=ManualReviewAdmissionDisposition.REJECTED,
                    plan_id=plan_id,
                    plan_revision=plan_revision,
                    ownership_generation=ownership_generation,
                    reason="current_review_schedule_missing",
                )
            if str(schedule["status"]) != "scheduled":
                return ManualReviewAdmissionResult(
                    key=key,
                    request_id=normalized_request_id,
                    disposition=ManualReviewAdmissionDisposition.ALREADY_CLAIMED,
                    plan_id=plan_id,
                    plan_revision=plan_revision,
                    ownership_generation=ownership_generation,
                    reason=f"schedule_not_scheduled:{schedule['status']}",
                )
            request = ManualReviewRequest(
                key=key,
                request_id=normalized_request_id,
                ownership_generation=ownership_generation,
                plan_id=plan_id,
                plan_revision=plan_revision,
                delivery_cycle=_schedule_delivery_cycle(schedule),
                requested_by=normalized_requested_by,
                reason=normalized_reason,
            )
            delivery = _ScheduleMailboxDelivery(
                event_id=request.event_id,
                kind=MANUAL_REVIEW_EVENT_KIND,
                source=MANUAL_REVIEW_EVENT_SOURCE,
                occurred_at=now,
                payload_json=_canonical_json(request.to_payload()),
                causation_id=request.request_id,
                correlation_id=request.request_id,
                trace_id=request.event_id,
                schedule_event_type="manual_dispatched",
                schedule_event_outcome="claimed",
                reason=request.reason,
                metadata={
                    "request_id": request.request_id,
                    "requested_by": request.requested_by,
                },
            )
            conn.execute("SAVEPOINT manual_review_candidate")
            try:
                admission = self._claim_current_schedule_for_delivery(
                    conn,
                    schedule,
                    aggregate,
                    delivery=delivery,
                    now=now,
                    ownership=ownership,
                )
                final_reason = self._final_actor_admission_reason(
                    conn,
                    key=key,
                    ownership=ownership,
                )
                if final_reason:
                    conn.execute("ROLLBACK TO manual_review_candidate")
                    conn.execute("RELEASE manual_review_candidate")
                    return ManualReviewAdmissionResult(
                        key=key,
                        request_id=request.request_id,
                        disposition=ManualReviewAdmissionDisposition.REJECTED,
                        plan_id=request.plan_id,
                        plan_revision=request.plan_revision,
                        ownership_generation=request.ownership_generation,
                        reason=final_reason,
                    )
                conn.execute("RELEASE manual_review_candidate")
            except _ScheduleMailboxAdmissionLost as exc:
                conn.execute("ROLLBACK TO manual_review_candidate")
                conn.execute("RELEASE manual_review_candidate")
                return ManualReviewAdmissionResult(
                    key=key,
                    request_id=request.request_id,
                    disposition=ManualReviewAdmissionDisposition.REJECTED,
                    plan_id=request.plan_id,
                    plan_revision=request.plan_revision,
                    ownership_generation=request.ownership_generation,
                    reason=exc.reason,
                )
            except ReviewDueConflict as exc:
                conn.execute("ROLLBACK TO manual_review_candidate")
                conn.execute("RELEASE manual_review_candidate")
                raise ManualReviewAdmissionError(str(exc)) from exc
            except BaseException:
                conn.execute("ROLLBACK TO manual_review_candidate")
                conn.execute("RELEASE manual_review_candidate")
                raise
            return ManualReviewAdmissionResult(
                key=key,
                request_id=request.request_id,
                disposition=ManualReviewAdmissionDisposition.ADMITTED,
                event_id=request.event_id,
                plan_id=request.plan_id,
                plan_revision=request.plan_revision,
                ownership_generation=request.ownership_generation,
                mailbox_id=admission.mailbox_id,
                mailbox_inserted=admission.mailbox_inserted,
                reason=request.reason,
                wake_request=wake_request,
            )

    @staticmethod
    def _manual_review_duplicate_for_request_id(
        conn: Connection,
        *,
        key: SessionKey,
        ownership_generation: int,
        wake_request: FencedMailboxWakeRequest,
        request_id: str,
        requested_by: str,
        reason: str,
    ) -> ManualReviewAdmissionResult | None:
        """Return a prior request id or reject an attempted generation rebase."""

        rows = conn.execute(
            """
            SELECT mailbox_id, event_id, profile_id, session_id, ownership_generation,
                   kind, source, payload_json, status
            FROM agent_session_mailbox
            WHERE CAST(profile_id AS BLOB) = ?
              AND CAST(session_id AS BLOB) = ?
              AND CAST(kind AS BLOB) = ?
              AND CAST(source AS BLOB) = ?
              AND CAST(causation_id AS BLOB) = ?
            ORDER BY mailbox_id
            """,
            (
                key.profile_id.encode(),
                key.session_id.encode(),
                MANUAL_REVIEW_EVENT_KIND.encode(),
                MANUAL_REVIEW_EVENT_SOURCE.encode(),
                request_id.encode(),
            ),
        ).fetchall()
        if not rows:
            return None
        if len(rows) != 1:
            raise ManualReviewAdmissionError(
                "manual review request id resolves to multiple durable events"
            )
        row = rows[0]
        event_id = str(row["event_id"])
        if (
            str(row["kind"]) != MANUAL_REVIEW_EVENT_KIND
            or str(row["source"]) != MANUAL_REVIEW_EVENT_SOURCE
            or int(row["ownership_generation"]) != ownership_generation
        ):
            raise ManualReviewAdmissionError(
                "manual review request id resolves to different durable work"
            )
        try:
            payload = json.loads(str(row["payload_json"]))
        except json.JSONDecodeError as exc:
            raise ManualReviewAdmissionError(
                "manual review duplicate payload is not valid JSON"
            ) from exc
        if not isinstance(payload, Mapping):
            raise ManualReviewAdmissionError(
                "manual review duplicate payload is not an object"
            )
        try:
            request = ManualReviewRequest.from_payload(
                payload,
                event_id=event_id,
                key=key,
                ownership_generation=ownership_generation,
            )
        except ManualReviewRequestError as exc:
            raise ManualReviewAdmissionError(
                "manual review request id resolves to invalid durable work"
            ) from exc
        if (
            request.request_id != request_id
            or request.requested_by != requested_by
            or request.reason != reason
        ):
            raise ManualReviewAdmissionError(
                "manual review request id changed immutable request fields"
            )
        return ManualReviewAdmissionResult(
            key=key,
            request_id=request.request_id,
            disposition=ManualReviewAdmissionDisposition.DUPLICATE,
            event_id=event_id,
            plan_id=request.plan_id,
            plan_revision=request.plan_revision,
            ownership_generation=request.ownership_generation,
            mailbox_id=int(row["mailbox_id"]),
            mailbox_inserted=False,
            reason="manual_request_duplicate",
            wake_request=wake_request,
        )

    def pending_review_due_wake_requests(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[FencedMailboxWakeRequest, ...]:
        """Discover exact wake debt from pending ReviewDue mailbox events."""

        normalized_limit = _positive_int(limit, field_name="limit")
        normalized_offset = _nonnegative_int(offset, field_name="offset")
        now = _nonnegative_finite(self._clock(), field_name="clock")
        profile_clause = ""
        params: list[object] = []
        if self._profile_id is not None:
            profile_clause = " AND mailbox.profile_id = ?"
            params.append(self._profile_id)
        params.append(normalized_limit)
        params.append(normalized_offset)
        with self._database.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT mailbox.profile_id, mailbox.session_id,
                       ownership.generation AS ownership_generation,
                       ownership.admission_fence_id AS admission_fence_id,
                       ownership.admission_fence_generation
                           AS admission_fence_generation,
                       MIN(mailbox.mailbox_id) AS first_mailbox_id
                FROM agent_session_mailbox AS mailbox
                JOIN agent_session_runtime_ownership AS ownership
                  ON ownership.profile_id = mailbox.profile_id
                 AND ownership.session_id = mailbox.session_id
                 AND ownership.mode = 'actor_v2'
                 AND ownership.status = 'active'
                 AND ownership.generation = mailbox.ownership_generation
                LEFT JOIN agent_session_actor_v2_admission_fences AS admission
                  ON admission.profile_id = ownership.profile_id
                 AND admission.session_id = ownership.session_id
                 AND admission.fence_id = ownership.admission_fence_id
                 AND admission.generation = ownership.admission_fence_generation
                WHERE mailbox.kind = ?
                  AND mailbox.source = ?
                  AND mailbox.status IN ('pending', 'processing')
                  AND (
                        ownership.admission_fence_id = ''
                     OR (
                            admission.status = 'committed'
                        AND admission.expires_at > ?
                     )
                  )
                  {profile_clause}
                GROUP BY mailbox.profile_id,
                         mailbox.session_id,
                         ownership.generation,
                         ownership.admission_fence_id,
                         ownership.admission_fence_generation
                ORDER BY first_mailbox_id ASC,
                         mailbox.profile_id ASC, mailbox.session_id ASC
                LIMIT ? OFFSET ?
                """,
                (REVIEW_DUE_EVENT_KIND, REVIEW_DUE_EVENT_SOURCE, now, *params),
            ).fetchall()
        return tuple(
            FencedMailboxWakeRequest(
                key=SessionKey(str(row["profile_id"]), str(row["session_id"])),
                ownership_generation=int(row["ownership_generation"]),
                admission_fence_id=str(row["admission_fence_id"]),
                admission_fence_generation=int(row["admission_fence_generation"]),
            )
            for row in rows
        )

    def pending_review_due_keys(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[SessionKey, ...]:
        """Project ReviewDue wake debt to legacy key-only compatibility values."""

        return _unique_keys(
            request.key
            for request in self.pending_review_due_wake_requests(
                limit=limit,
                offset=offset,
            )
        )

    def pending_review_wake_debts(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        after: ReviewWakeCursor | None = None,
    ) -> tuple[_PendingReviewWakeDebt, ...]:
        """Discover the newest pending event for each exact wake identity.

        A manual admission commits its schedule claim before its post-commit
        wake. Including it here makes a crash or one failed wake recoverable
        through the same scanner supervision that redrives due-review debt.
        ``after`` advances a stable mailbox keyset; ``offset`` remains for
        compatibility with older callers that cannot retain a cursor.
        """

        normalized_limit = _positive_int(limit, field_name="limit")
        normalized_offset = _nonnegative_int(offset, field_name="offset")
        if after is not None and not isinstance(after, ReviewWakeCursor):
            raise TypeError("after must be a ReviewWakeCursor")
        if after is not None and normalized_offset:
            raise ValueError("offset cannot be combined with a keyset cursor")
        now = _nonnegative_finite(self._clock(), field_name="clock")
        profile_clause = ""
        profile_params: list[object] = []
        if self._profile_id is not None:
            profile_clause = " AND mailbox.profile_id = ?"
            profile_params.append(self._profile_id)
        after_clause = ""
        after_params: tuple[object, ...] = ()
        if after is not None:
            after_clause = """
              AND (
                    debt.mailbox_id > ?
                 OR (
                        debt.mailbox_id = ?
                    AND debt.profile_id > ?
                 )
                 OR (
                        debt.mailbox_id = ?
                    AND debt.profile_id = ?
                    AND debt.session_id > ?
                 )
                 OR (
                        debt.mailbox_id = ?
                    AND debt.profile_id = ?
                    AND debt.session_id = ?
                    AND debt.ownership_generation > ?
                 )
                 OR (
                        debt.mailbox_id = ?
                    AND debt.profile_id = ?
                    AND debt.session_id = ?
                    AND debt.ownership_generation = ?
                    AND debt.admission_fence_id > ?
                 )
                 OR (
                        debt.mailbox_id = ?
                    AND debt.profile_id = ?
                    AND debt.session_id = ?
                    AND debt.ownership_generation = ?
                    AND debt.admission_fence_id = ?
                    AND debt.admission_fence_generation > ?
                 )
              )
            """
            after_params = _review_wake_cursor_parameters(after)
        with self._database.connect() as conn:
            rows = conn.execute(
                f"""
                WITH ranked_review_mailbox AS (
                    SELECT mailbox.profile_id,
                           mailbox.session_id,
                           mailbox.event_id,
                           mailbox.mailbox_id,
                           ownership.generation AS ownership_generation,
                           ownership.admission_fence_id AS admission_fence_id,
                           ownership.admission_fence_generation
                               AS admission_fence_generation,
                           ROW_NUMBER() OVER (
                               PARTITION BY mailbox.profile_id,
                                            mailbox.session_id,
                                            ownership.generation,
                                            ownership.admission_fence_id,
                                            ownership.admission_fence_generation
                               ORDER BY mailbox.mailbox_id DESC
                           ) AS mailbox_rank
                    FROM agent_session_mailbox AS mailbox
                    JOIN agent_session_runtime_ownership AS ownership
                      ON ownership.profile_id = mailbox.profile_id
                     AND ownership.session_id = mailbox.session_id
                     AND ownership.mode = 'actor_v2'
                     AND ownership.status = 'active'
                     AND ownership.generation = mailbox.ownership_generation
                    LEFT JOIN agent_session_actor_v2_admission_fences AS admission
                      ON admission.profile_id = ownership.profile_id
                     AND admission.session_id = ownership.session_id
                     AND admission.fence_id = ownership.admission_fence_id
                     AND admission.generation = ownership.admission_fence_generation
                    WHERE (
                            (mailbox.kind = ? AND mailbox.source = ?)
                         OR (mailbox.kind = ? AND mailbox.source = ?)
                    )
                      AND mailbox.status IN ('pending', 'processing')
                      AND (
                            ownership.admission_fence_id = ''
                         OR (
                                admission.status = 'committed'
                            AND admission.expires_at > ?
                         )
                      )
                      {profile_clause}
                )
                SELECT profile_id, session_id, event_id, mailbox_id,
                       ownership_generation, admission_fence_id,
                       admission_fence_generation
                FROM ranked_review_mailbox AS debt
                WHERE debt.mailbox_rank = 1
                {after_clause}
                ORDER BY debt.mailbox_id ASC,
                         debt.profile_id ASC,
                         debt.session_id ASC,
                         debt.ownership_generation ASC,
                         debt.admission_fence_id ASC,
                         debt.admission_fence_generation ASC
                LIMIT ? OFFSET ?
                """,
                (
                    REVIEW_DUE_EVENT_KIND,
                    REVIEW_DUE_EVENT_SOURCE,
                    MANUAL_REVIEW_EVENT_KIND,
                    MANUAL_REVIEW_EVENT_SOURCE,
                    now,
                    *profile_params,
                    *after_params,
                    normalized_limit,
                    normalized_offset,
                ),
            ).fetchall()
        return tuple(
            _PendingReviewWakeDebt(
                request=FencedMailboxWakeRequest(
                    key=SessionKey(
                        str(row["profile_id"]),
                        str(row["session_id"]),
                    ),
                    ownership_generation=int(row["ownership_generation"]),
                    admission_fence_id=str(row["admission_fence_id"]),
                    admission_fence_generation=int(
                        row["admission_fence_generation"]
                    ),
                ),
                event_id=str(row["event_id"]),
                cursor=ReviewWakeCursor(
                    mailbox_id=int(row["mailbox_id"]),
                    profile_id=str(row["profile_id"]),
                    session_id=str(row["session_id"]),
                    ownership_generation=int(row["ownership_generation"]),
                    admission_fence_id=str(row["admission_fence_id"]),
                    admission_fence_generation=int(
                        row["admission_fence_generation"]
                    ),
                ),
            )
            for row in rows
        )

    def pending_review_wake_requests(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        after: ReviewWakeCursor | None = None,
    ) -> tuple[FencedMailboxWakeRequest, ...]:
        """Project current review wake debt to exact Actor identities."""

        return _unique_wake_requests(
            debt.request
            for debt in self.pending_review_wake_debts(
                limit=limit,
                offset=offset,
                after=after,
            )
        )

    def is_pending_review_wake_request(
        self,
        request: FencedMailboxWakeRequest,
    ) -> bool:
        """Return whether an exact identity still has live review mailbox debt."""

        if not isinstance(request, FencedMailboxWakeRequest):
            raise TypeError("request must be a FencedMailboxWakeRequest")
        now = _nonnegative_finite(self._clock(), field_name="clock")
        with self._database.connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM agent_session_mailbox AS mailbox
                JOIN agent_session_runtime_ownership AS ownership
                  ON ownership.profile_id = mailbox.profile_id
                 AND ownership.session_id = mailbox.session_id
                 AND ownership.mode = 'actor_v2'
                 AND ownership.status = 'active'
                 AND ownership.generation = mailbox.ownership_generation
                LEFT JOIN agent_session_actor_v2_admission_fences AS admission
                  ON admission.profile_id = ownership.profile_id
                 AND admission.session_id = ownership.session_id
                 AND admission.fence_id = ownership.admission_fence_id
                 AND admission.generation = ownership.admission_fence_generation
                WHERE mailbox.profile_id = ?
                  AND mailbox.session_id = ?
                  AND mailbox.ownership_generation = ?
                  AND ownership.admission_fence_id = ?
                  AND ownership.admission_fence_generation = ?
                  AND (
                        (mailbox.kind = ? AND mailbox.source = ?)
                     OR (mailbox.kind = ? AND mailbox.source = ?)
                  )
                  AND mailbox.status IN ('pending', 'processing')
                  AND (
                        ownership.admission_fence_id = ''
                     OR (
                            admission.status = 'committed'
                        AND admission.expires_at > ?
                     )
                  )
                LIMIT 1
                """,
                (
                    request.key.profile_id,
                    request.key.session_id,
                    request.ownership_generation,
                    request.admission_fence_id,
                    request.admission_fence_generation,
                    REVIEW_DUE_EVENT_KIND,
                    REVIEW_DUE_EVENT_SOURCE,
                    MANUAL_REVIEW_EVENT_KIND,
                    MANUAL_REVIEW_EVENT_SOURCE,
                    now,
                ),
            ).fetchone()
        return row is not None

    def is_pending_review_wake_debt(self, debt: _PendingReviewWakeDebt) -> bool:
        """Return whether one selected mailbox event remains the live debt."""

        if not isinstance(debt, _PendingReviewWakeDebt):
            raise TypeError("debt must be a _PendingReviewWakeDebt")
        request = debt.request
        now = _nonnegative_finite(self._clock(), field_name="clock")
        mailbox_id_clause = ""
        mailbox_id_params: tuple[object, ...] = ()
        if debt.cursor is not None:
            mailbox_id_clause = " AND mailbox.mailbox_id = ?"
            mailbox_id_params = (debt.cursor.mailbox_id,)
        with self._database.connect() as conn:
            row = conn.execute(
                f"""
                SELECT 1
                FROM agent_session_mailbox AS mailbox
                JOIN agent_session_runtime_ownership AS ownership
                  ON ownership.profile_id = mailbox.profile_id
                 AND ownership.session_id = mailbox.session_id
                 AND ownership.mode = 'actor_v2'
                 AND ownership.status = 'active'
                 AND ownership.generation = mailbox.ownership_generation
                LEFT JOIN agent_session_actor_v2_admission_fences AS admission
                  ON admission.profile_id = ownership.profile_id
                 AND admission.session_id = ownership.session_id
                 AND admission.fence_id = ownership.admission_fence_id
                 AND admission.generation = ownership.admission_fence_generation
                WHERE mailbox.profile_id = ?
                  AND mailbox.session_id = ?
                  AND mailbox.ownership_generation = ?
                  AND mailbox.event_id = ?
                  {mailbox_id_clause}
                  AND ownership.admission_fence_id = ?
                  AND ownership.admission_fence_generation = ?
                  AND (
                        (mailbox.kind = ? AND mailbox.source = ?)
                     OR (mailbox.kind = ? AND mailbox.source = ?)
                  )
                  AND mailbox.status IN ('pending', 'processing')
                  AND (
                        ownership.admission_fence_id = ''
                     OR (
                            admission.status = 'committed'
                        AND admission.expires_at > ?
                     )
                  )
                  AND NOT EXISTS (
                        SELECT 1
                        FROM agent_session_mailbox AS newer
                        WHERE newer.profile_id = mailbox.profile_id
                          AND newer.session_id = mailbox.session_id
                          AND newer.ownership_generation = mailbox.ownership_generation
                          AND (
                                (newer.kind = ? AND newer.source = ?)
                             OR (newer.kind = ? AND newer.source = ?)
                          )
                          AND newer.status IN ('pending', 'processing')
                          AND newer.mailbox_id > mailbox.mailbox_id
                  )
                LIMIT 1
                """,
                (
                    request.key.profile_id,
                    request.key.session_id,
                    request.ownership_generation,
                    debt.event_id,
                    *mailbox_id_params,
                    request.admission_fence_id,
                    request.admission_fence_generation,
                    REVIEW_DUE_EVENT_KIND,
                    REVIEW_DUE_EVENT_SOURCE,
                    MANUAL_REVIEW_EVENT_KIND,
                    MANUAL_REVIEW_EVENT_SOURCE,
                    now,
                    REVIEW_DUE_EVENT_KIND,
                    REVIEW_DUE_EVENT_SOURCE,
                    MANUAL_REVIEW_EVENT_KIND,
                    MANUAL_REVIEW_EVENT_SOURCE,
                ),
            ).fetchone()
        return row is not None

    def pending_review_wake_keys(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        after: ReviewWakeCursor | None = None,
    ) -> tuple[SessionKey, ...]:
        """Project due and manual-review debt to legacy key-only values."""

        return _unique_keys(
            request.key
            for request in self.pending_review_wake_requests(
                limit=limit,
                offset=offset,
                after=after,
            )
        )

    def record_service_health(
        self,
        snapshot: RuntimeServiceHealthSnapshot,
        summary: ReviewDueScanSummary,
        *,
        runtime_id: str,
    ) -> None:
        """Persist one completed supervision pass for restart diagnostics."""

        normalized_runtime_id = str(runtime_id or "").strip()
        if not normalized_runtime_id:
            raise ValueError("runtime_id must not be empty")
        now = _nonnegative_finite(self._clock(), field_name="clock")
        skipped = summary.skipped_count
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO agent_runtime_service_health (
                    profile_id, service_name, runtime_id, status, expected,
                    started_at, heartbeat_at, last_scan_started_at,
                    last_scan_finished_at, last_success_at, last_error_at,
                    last_error_code, last_error_message, consecutive_failures,
                    restart_count, scan_count, due_seen_count, dispatch_count,
                    skip_count, in_flight_count, lease_owner, updated_at
                ) VALUES (?, 'durable_review_due_scanner', ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                          0, 1, ?, ?, ?, 0, '', ?)
                ON CONFLICT(profile_id, service_name) DO UPDATE SET
                    runtime_id = excluded.runtime_id,
                    status = excluded.status,
                    expected = 1,
                    started_at = CASE
                        WHEN agent_runtime_service_health.started_at IS NULL
                             OR agent_runtime_service_health.started_at = 0
                        THEN excluded.started_at
                        ELSE agent_runtime_service_health.started_at
                    END,
                    heartbeat_at = excluded.heartbeat_at,
                    last_scan_started_at = excluded.last_scan_started_at,
                    last_scan_finished_at = excluded.last_scan_finished_at,
                    last_success_at = excluded.last_success_at,
                    last_error_at = excluded.last_error_at,
                    last_error_code = excluded.last_error_code,
                    last_error_message = excluded.last_error_message,
                    consecutive_failures = excluded.consecutive_failures,
                    scan_count = agent_runtime_service_health.scan_count + 1,
                    due_seen_count = agent_runtime_service_health.due_seen_count
                                     + excluded.due_seen_count,
                    dispatch_count = agent_runtime_service_health.dispatch_count
                                     + excluded.dispatch_count,
                    skip_count = agent_runtime_service_health.skip_count
                                 + excluded.skip_count,
                    in_flight_count = 0,
                    lease_owner = '',
                    updated_at = excluded.updated_at
                """,
                (
                    self.health_profile_id,
                    normalized_runtime_id,
                    snapshot.status.value,
                    snapshot.started_at,
                    now,
                    snapshot.last_scan_started_at,
                    now,
                    snapshot.last_success_at,
                    snapshot.last_error_at,
                    snapshot.last_error_code,
                    snapshot.last_error_message,
                    snapshot.consecutive_failures,
                    summary.attempted_count,
                    summary.dispatched_count,
                    skipped,
                    now,
                ),
            )

    def _dispatch_next_due(
        self,
        *,
        after: _DueScanCursor | None,
    ) -> tuple[ReviewDueDispatchResult, _DueScanCursor] | None:
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = _nonnegative_finite(self._clock(), field_name="clock")
            schedule = self._select_next_due(conn, now=now, after=after)
            if schedule is None:
                return None
            cursor = _due_scan_cursor(schedule)
            key = SessionKey(
                str(schedule["profile_id"]),
                str(schedule["session_id"]),
            )
            owner = conn.execute(
                """
                SELECT mode, status, generation
                FROM agent_session_runtime_ownership
                WHERE profile_id = ? AND session_id = ?
                """,
                (key.profile_id, key.session_id),
            ).fetchone()
            ownership, unavailable_reason = self._require_actor_admission(
                conn,
                key=key,
                owner=owner,
            )
            if unavailable_reason:
                return self._skip_unfenced_schedule(
                    schedule,
                    reason=unavailable_reason,
                ), cursor

            assert ownership is not None
            owner_generation = ownership.generation
            if int(schedule["ownership_generation"]) != owner_generation:
                return self._skip_unfenced_schedule(
                    schedule,
                    reason="schedule_generation_mismatch",
                ), cursor
            aggregate = conn.execute(
                """
                SELECT ownership_generation, current_plan_id,
                       review_plan_revision, state_revision
                FROM agent_session_aggregates
                WHERE profile_id = ? AND session_id = ?
                """,
                (key.profile_id, key.session_id),
            ).fetchone()
            if aggregate is None:
                return self._skip_unfenced_schedule(
                    schedule,
                    reason="aggregate_missing",
                ), cursor
            if int(aggregate["ownership_generation"]) != owner_generation:
                return self._skip_unfenced_schedule(
                    schedule,
                    reason="aggregate_generation_mismatch",
                ), cursor

            plan_id = str(schedule["plan_id"])
            plan_revision = int(schedule["plan_revision"])
            if (
                str(aggregate["current_plan_id"]) != plan_id
                or int(aggregate["review_plan_revision"]) != plan_revision
            ):
                candidate_is_supersede = True
            else:
                candidate_is_supersede = False
            conn.execute("SAVEPOINT review_due_dispatch_candidate")
            try:
                if candidate_is_supersede:
                    result = self._supersede_stale_schedule(
                        conn,
                        schedule,
                        aggregate,
                        now=now,
                        reason="aggregate_current_plan_mismatch",
                    )
                else:
                    result = self._dispatch_current_schedule(
                        conn,
                        schedule,
                        aggregate,
                        now=now,
                        ownership=ownership,
                    )
                final_reason = self._final_actor_admission_reason(
                    conn,
                    key=key,
                    ownership=ownership,
                )
                if final_reason:
                    conn.execute("ROLLBACK TO review_due_dispatch_candidate")
                    conn.execute("RELEASE review_due_dispatch_candidate")
                    return self._skip_unfenced_schedule(
                        schedule,
                        reason=final_reason,
                    ), cursor
                conn.execute("RELEASE review_due_dispatch_candidate")
            except _ScheduleMailboxAdmissionLost as exc:
                conn.execute("ROLLBACK TO review_due_dispatch_candidate")
                conn.execute("RELEASE review_due_dispatch_candidate")
                return self._skip_unfenced_schedule(
                    schedule,
                    reason=exc.reason,
                ), cursor
            except BaseException:
                conn.execute("ROLLBACK TO review_due_dispatch_candidate")
                conn.execute("RELEASE review_due_dispatch_candidate")
                raise
            return result, cursor

    def _select_next_due(
        self,
        conn: Connection,
        *,
        now: float,
        after: _DueScanCursor | None,
    ) -> Row | None:
        profile_clause = ""
        params: list[object] = [now, now]
        if self._profile_id is not None:
            profile_clause = " AND schedule.profile_id = ?"
            params.append(self._profile_id)
        cursor_clause = ""
        if after is not None:
            cursor_clause = """
              AND (
                    schedule.available_at, schedule.next_review_at,
                    schedule.profile_id, schedule.session_id,
                    schedule.plan_revision, schedule.plan_id
                  ) > (?, ?, ?, ?, ?, ?)
            """
            params.extend(after)
        # The live-fence check appears after the profile/cursor predicates in
        # the statement below, so keep its bind value last.
        params.append(now)
        return conn.execute(
            f"""
            SELECT schedule.*
            FROM agent_review_schedules AS schedule
            WHERE schedule.status = 'scheduled'
              AND schedule.available_at <= ?
              AND schedule.next_review_at <= ?
              {profile_clause}
              {cursor_clause}
            ORDER BY CASE
                         WHEN EXISTS (
                             SELECT 1
                             FROM agent_session_runtime_ownership AS ownership
                             JOIN agent_session_aggregates AS aggregate
                               ON aggregate.profile_id = ownership.profile_id
                              AND aggregate.session_id = ownership.session_id
                              AND aggregate.ownership_generation =
                                  schedule.ownership_generation
                             WHERE ownership.profile_id = schedule.profile_id
                               AND ownership.session_id = schedule.session_id
                               AND ownership.mode = 'actor_v2'
                               AND ownership.status = 'active'
                               AND ownership.generation =
                                   schedule.ownership_generation
                               AND (
                                     ownership.admission_fence_id = ''
                                  OR EXISTS (
                                         SELECT 1
                                         FROM agent_session_actor_v2_admission_fences
                                         AS admission
                                         WHERE admission.profile_id =
                                                   ownership.profile_id
                                           AND admission.session_id =
                                                   ownership.session_id
                                           AND admission.fence_id =
                                                   ownership.admission_fence_id
                                           AND admission.generation =
                                                   ownership.admission_fence_generation
                                           AND admission.status = 'committed'
                                           AND admission.expires_at > ?
                                     )
                                   )
                         ) THEN 0
                         ELSE 1
                     END ASC,
                     schedule.available_at ASC,
                     schedule.next_review_at ASC,
                     schedule.profile_id ASC,
                     schedule.session_id ASC,
                     schedule.plan_revision ASC,
                     schedule.plan_id ASC
            LIMIT 1
            """,
            tuple(params),
        ).fetchone()

    @staticmethod
    def _skip_unfenced_schedule(
        schedule: Row,
        *,
        reason: str,
    ) -> ReviewDueDispatchResult:
        """Leave actor-owned state frozen when the exact writer fence is absent."""

        return _result(
            schedule,
            disposition=ReviewDueDisposition.FENCE_SKIPPED,
            reason=reason,
        )

    def _defer_schedule(
        self,
        conn: Connection,
        schedule: Row,
        aggregate: Row,
        *,
        now: float,
        reason: str,
    ) -> ReviewDueDispatchResult:
        attempt_count = int(schedule["attempt_count"]) + 1
        retry_at = _nonnegative_finite(
            now + self._retry_delay(attempt_count),
            field_name="retry_at",
        )
        updated = conn.execute(
            """
            UPDATE agent_review_schedules
            SET available_at = ?, attempt_count = ?, last_error = ?,
                claim_owner = '', claim_until = NULL, updated_at = ?
            WHERE plan_id = ? AND profile_id = ? AND session_id = ?
              AND plan_revision = ? AND ownership_generation = ?
              AND status = 'scheduled'
              AND EXISTS (
                    SELECT 1
                    FROM agent_session_runtime_ownership AS ownership
                    WHERE ownership.profile_id =
                          agent_review_schedules.profile_id
                      AND ownership.session_id =
                          agent_review_schedules.session_id
                      AND ownership.mode = 'actor_v2'
                      AND ownership.status = 'active'
                      AND ownership.generation =
                          agent_review_schedules.ownership_generation
              )
              AND EXISTS (
                    SELECT 1
                    FROM agent_session_aggregates AS aggregate
                    WHERE aggregate.profile_id =
                          agent_review_schedules.profile_id
                      AND aggregate.session_id =
                          agent_review_schedules.session_id
                      AND aggregate.ownership_generation =
                          agent_review_schedules.ownership_generation
                      AND aggregate.state_revision = ?
                      AND aggregate.current_plan_id = ?
                      AND aggregate.review_plan_revision = ?
              )
            """,
            (
                retry_at,
                attempt_count,
                reason,
                now,
                schedule["plan_id"],
                schedule["profile_id"],
                schedule["session_id"],
                schedule["plan_revision"],
                schedule["ownership_generation"],
                aggregate["state_revision"],
                aggregate["current_plan_id"],
                aggregate["review_plan_revision"],
            ),
        )
        if updated.rowcount != 1:
            raise ReviewDueConflict("due schedule changed while retry was committed")
        return _result(
            schedule,
            disposition=ReviewDueDisposition.RETRY_DEFERRED,
            reason=reason,
            retry_at=retry_at,
        )

    def _supersede_stale_schedule(
        self,
        conn: Connection,
        schedule: Row,
        aggregate: Row,
        *,
        now: float,
        reason: str,
    ) -> ReviewDueDispatchResult:
        event_id = _review_due_event_id(schedule)
        updated = conn.execute(
            """
            UPDATE agent_review_schedules
            SET status = 'superseded', claim_owner = '', claim_until = NULL,
                last_error = ?, updated_at = ?
            WHERE plan_id = ? AND profile_id = ? AND session_id = ?
              AND plan_revision = ? AND ownership_generation = ?
              AND status = 'scheduled'
              AND EXISTS (
                    SELECT 1
                    FROM agent_session_runtime_ownership AS ownership
                    WHERE ownership.profile_id =
                          agent_review_schedules.profile_id
                      AND ownership.session_id =
                          agent_review_schedules.session_id
                      AND ownership.mode = 'actor_v2'
                      AND ownership.status = 'active'
                      AND ownership.generation =
                          agent_review_schedules.ownership_generation
              )
              AND EXISTS (
                    SELECT 1
                    FROM agent_session_aggregates AS current_aggregate
                    WHERE current_aggregate.profile_id =
                          agent_review_schedules.profile_id
                      AND current_aggregate.session_id =
                          agent_review_schedules.session_id
                      AND current_aggregate.ownership_generation =
                          agent_review_schedules.ownership_generation
                      AND current_aggregate.state_revision = ?
                      AND current_aggregate.current_plan_id = ?
                      AND current_aggregate.review_plan_revision = ?
              )
            """,
            (
                reason,
                now,
                schedule["plan_id"],
                schedule["profile_id"],
                schedule["session_id"],
                schedule["plan_revision"],
                schedule["ownership_generation"],
                aggregate["state_revision"],
                aggregate["current_plan_id"],
                aggregate["review_plan_revision"],
            ),
        )
        if updated.rowcount != 1:
            raise ReviewDueConflict("stale schedule changed while superseding")
        self._append_schedule_event(
            conn,
            schedule,
            aggregate,
            event_id=event_id,
            event_type="superseded",
            outcome="superseded",
            reason=reason,
            now=now,
        )
        return _result(
            schedule,
            disposition=ReviewDueDisposition.SUPERSEDED,
            event_id=event_id,
            reason=reason,
        )

    def _dispatch_current_schedule(
        self,
        conn: Connection,
        schedule: Row,
        aggregate: Row,
        *,
        now: float,
        ownership: AgentRuntimeOwnership,
    ) -> ReviewDueDispatchResult:
        """Dispatch one due schedule using the ownership captured by this transaction."""

        wake_request = _wake_request_for_ownership(ownership)
        event_id = _review_due_event_id(schedule)
        delivery = _ScheduleMailboxDelivery(
            event_id=event_id,
            kind=REVIEW_DUE_EVENT_KIND,
            source=REVIEW_DUE_EVENT_SOURCE,
            occurred_at=float(schedule["next_review_at"]),
            payload_json=_canonical_json(
                _review_due_payload(schedule, event_id=event_id)
            ),
            causation_id=str(schedule["plan_id"]),
            correlation_id=str(schedule["plan_id"]),
            trace_id=event_id,
            schedule_event_type="due_dispatched",
            schedule_event_outcome="claimed",
            reason="review_schedule_due",
            metadata={},
        )
        try:
            existing_mailbox_id = self._validate_mailbox_logical_key(
                conn,
                _schedule_mailbox_record(schedule, delivery=delivery, now=now),
                allow_missing=True,
            )
        except ReviewDueConflict:
            return self._defer_schedule(
                conn,
                schedule,
                aggregate,
                now=now,
                reason="mailbox_identity_conflict",
            )
        admission = self._claim_current_schedule_for_delivery(
            conn,
            schedule,
            aggregate,
            delivery=delivery,
            now=now,
            existing_mailbox_id=existing_mailbox_id,
            ownership=ownership,
        )

        return _result(
            schedule,
            disposition=ReviewDueDisposition.DISPATCHED,
            event_id=event_id,
            mailbox_id=admission.mailbox_id,
            mailbox_inserted=admission.mailbox_inserted,
            reason="review_schedule_due",
            wake_request=wake_request,
        )

    def _claim_current_schedule_for_delivery(
        self,
        conn: Connection,
        schedule: Row,
        aggregate: Row,
        *,
        delivery: _ScheduleMailboxDelivery,
        now: float,
        ownership: AgentRuntimeOwnership,
        existing_mailbox_id: int | None = None,
    ) -> _ScheduleMailboxAdmission:
        """Insert one exact mailbox delivery and atomically claim its schedule.

        Only a row inserted by this transaction receives handoff evidence. A
        duplicate mailbox is historical durable work, so its absent or unknown
        sidecar evidence must remain fail-closed rather than being rebuilt from
        the current ownership row.
        """

        mailbox_record = _schedule_mailbox_record(
            schedule,
            delivery=delivery,
            now=now,
        )
        mailbox_key = SessionKey(
            str(schedule["profile_id"]),
            str(schedule["session_id"]),
        )
        if (
            ownership.key != mailbox_key
            or ownership.generation != int(schedule["ownership_generation"])
        ):
            raise ReviewDueConflict(
                "captured ownership differs from the schedule mailbox identity"
            )
        if existing_mailbox_id is None:
            existing_mailbox_id = self._validate_mailbox_logical_key(
                conn,
                mailbox_record,
                allow_missing=True,
            )
        self._fail_superseded_due_events(
            conn,
            schedule,
            current_event_id=delivery.event_id,
            now=now,
        )
        mailbox_inserted = False
        mailbox_id = existing_mailbox_id
        if mailbox_id is None:
            inserted = conn.execute(
                """
                INSERT INTO agent_session_mailbox (
                    event_id, profile_id, session_id, ownership_generation,
                    kind, source, occurred_at, payload_json, causation_id,
                    correlation_id, trace_id, status, attempt_count,
                    available_at, claim_id, lease_owner, lease_until,
                    created_at, handled_at, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, '', '', NULL, ?, NULL, '')
                """,
                (
                    _sqlite_record_value(mailbox_record, "event_id"),
                    _sqlite_record_value(mailbox_record, "profile_id"),
                    _sqlite_record_value(mailbox_record, "session_id"),
                    _sqlite_record_value(
                        mailbox_record,
                        "ownership_generation",
                    ),
                    _sqlite_record_value(mailbox_record, "kind"),
                    _sqlite_record_value(mailbox_record, "source"),
                    _sqlite_record_value(mailbox_record, "occurred_at"),
                    _sqlite_record_value(mailbox_record, "payload_json"),
                    _sqlite_record_value(mailbox_record, "causation_id"),
                    _sqlite_record_value(mailbox_record, "correlation_id"),
                    _sqlite_record_value(mailbox_record, "trace_id"),
                    _sqlite_record_value(mailbox_record, "occurred_at"),
                    _sqlite_record_value(mailbox_record, "created_at"),
                ),
            )
            if inserted.rowcount != 1:
                raise ReviewDueConflict(
                    "review schedule mailbox insert did not create exactly one row"
                )
            mailbox_inserted = True
            if inserted.lastrowid is None or inserted.lastrowid < 1:
                raise ReviewDueConflict(
                    "review schedule mailbox insert returned no mailbox id"
                )
            mailbox_id = inserted.lastrowid
        validated_mailbox_id = self._validate_mailbox_logical_key(
            conn,
            mailbox_record,
            allow_missing=False,
        )
        if validated_mailbox_id is None:
            raise ReviewDueConflict("review schedule mailbox identity disappeared")
        if mailbox_id is not None and mailbox_id != validated_mailbox_id:
            raise ReviewDueConflict("review schedule mailbox identity changed")
        mailbox_id = validated_mailbox_id
        if mailbox_inserted:
            assert mailbox_id is not None
            self._record_new_schedule_mailbox_handoff(
                conn,
                mailbox_id=mailbox_id,
                ownership=ownership,
            )
        # The row stores the next cycle. This transaction emits the old cycle
        # and advances it only if the exact schedule fence is claimed.
        delivery_cycle = _schedule_delivery_cycle(schedule)
        updated = conn.execute(
            """
            UPDATE agent_review_schedules
            SET status = 'claimed', claim_owner = '', claim_until = NULL,
                delivery_cycle = ?, last_error = '', updated_at = ?
            WHERE plan_id = ? AND profile_id = ? AND session_id = ?
              AND plan_revision = ? AND ownership_generation = ?
              AND delivery_cycle = ?
              AND status = 'scheduled'
              AND EXISTS (
                    SELECT 1
                    FROM agent_session_runtime_ownership AS ownership
                    WHERE ownership.profile_id =
                          agent_review_schedules.profile_id
                      AND ownership.session_id =
                          agent_review_schedules.session_id
                      AND ownership.mode = 'actor_v2'
                      AND ownership.status = 'active'
                      AND ownership.generation =
                          agent_review_schedules.ownership_generation
              )
              AND EXISTS (
                    SELECT 1
                    FROM agent_session_aggregates AS current_aggregate
                    WHERE current_aggregate.profile_id =
                          agent_review_schedules.profile_id
                      AND current_aggregate.session_id =
                          agent_review_schedules.session_id
                      AND current_aggregate.ownership_generation =
                          agent_review_schedules.ownership_generation
                      AND current_aggregate.state_revision = ?
                      AND current_aggregate.current_plan_id = ?
                      AND current_aggregate.review_plan_revision = ?
              )
            """,
            (
                delivery_cycle + 1,
                now,
                schedule["plan_id"],
                schedule["profile_id"],
                schedule["session_id"],
                schedule["plan_revision"],
                schedule["ownership_generation"],
                delivery_cycle,
                aggregate["state_revision"],
                aggregate["current_plan_id"],
                aggregate["review_plan_revision"],
            ),
        )
        if updated.rowcount != 1:
            raise ReviewDueConflict("current schedule changed while dispatching")
        self._append_schedule_event(
            conn,
            schedule,
            aggregate,
            event_id=delivery.event_id,
            event_type=delivery.schedule_event_type,
            outcome=delivery.schedule_event_outcome,
            source=delivery.source,
            reason=delivery.reason,
            metadata=delivery.metadata,
            now=now,
        )
        return _ScheduleMailboxAdmission(
            mailbox_id=mailbox_id,
            mailbox_inserted=mailbox_inserted,
        )

    def _record_new_schedule_mailbox_handoff(
        self,
        conn: Connection,
        *,
        mailbox_id: int,
        ownership: AgentRuntimeOwnership,
    ) -> None:
        """Write immutable handoff evidence from the already-captured owner.

        The sidecar repository revalidates a fenced owner before and after its
        own write. If an enclosing trigger invalidated the fence after the
        mailbox insert, preserve that observation until the outer candidate
        savepoint can roll back the mailbox, schedule, journal, and sidecar as
        one unit.
        """

        request = _wake_request_for_ownership(ownership)
        if not request.has_admission_fence:
            self._database.actor_v2_mailbox_handoffs.record_unfenced_legacy_handoff_in_transaction(
                conn,
                mailbox_id,
            )
            return
        try:
            self._database.actor_v2_mailbox_handoffs.record_fenced_handoff_in_transaction(
                conn,
                mailbox_id,
                request,
            )
        except (
            ActorV2AdmissionFenceConflict,
            ActorV2AdmissionFenceExpired,
            ActorV2AdmissionFenceNotFound,
            AgentRuntimeOwnershipError,
        ) as exc:
            reason = self._final_actor_admission_reason(
                conn,
                key=ownership.key,
                ownership=ownership,
            )
            if reason:
                raise _ScheduleMailboxAdmissionLost(reason) from exc
            raise

    @staticmethod
    def _fail_superseded_due_events(
        conn: Connection,
        schedule: Row,
        *,
        current_event_id: str,
        now: float,
    ) -> None:
        """Fence generation-stale ReviewDue debt before dispatching its successor."""

        conn.execute(
            """
            UPDATE agent_session_mailbox
            SET status = 'failed', handled_at = ?, claim_id = '',
                lease_owner = '', lease_until = NULL,
                last_error = 'review_due_exact_plan_fence_superseded'
            WHERE profile_id = ? AND session_id = ?
              AND kind = ? AND source = ? AND causation_id = ?
              AND event_id != ? AND status IN ('pending', 'processing')
            """,
            (
                now,
                schedule["profile_id"],
                schedule["session_id"],
                REVIEW_DUE_EVENT_KIND,
                REVIEW_DUE_EVENT_SOURCE,
                schedule["plan_id"],
                current_event_id,
            ),
        )

    @staticmethod
    def _validate_mailbox_logical_key(
        conn: Connection,
        expected: _TypedSQLiteRecord,
        *,
        allow_missing: bool,
    ) -> int | None:
        rows = conn.execute(
            """
            SELECT mailbox_id,
                   CAST(event_id AS BLOB) AS event_id,
                   typeof(event_id) AS event_id_storage_class,
                   CAST(profile_id AS BLOB) AS profile_id,
                   typeof(profile_id) AS profile_id_storage_class,
                   CAST(session_id AS BLOB) AS session_id,
                   typeof(session_id) AS session_id_storage_class,
                   ownership_generation,
                   typeof(ownership_generation)
                       AS ownership_generation_storage_class,
                   CAST(kind AS BLOB) AS kind,
                   typeof(kind) AS kind_storage_class,
                   CAST(source AS BLOB) AS source,
                   typeof(source) AS source_storage_class,
                   occurred_at,
                   typeof(occurred_at) AS occurred_at_storage_class,
                   CAST(payload_json AS BLOB) AS payload_json,
                   typeof(payload_json) AS payload_json_storage_class,
                   CAST(causation_id AS BLOB) AS causation_id,
                   typeof(causation_id) AS causation_id_storage_class,
                   CAST(correlation_id AS BLOB) AS correlation_id,
                   typeof(correlation_id) AS correlation_id_storage_class,
                   CAST(trace_id AS BLOB) AS trace_id,
                   typeof(trace_id) AS trace_id_storage_class,
                   created_at, typeof(created_at) AS created_at_storage_class
            FROM agent_session_mailbox
            WHERE CAST(profile_id AS BLOB) = ?
              AND CAST(session_id AS BLOB) = ?
              AND CAST(event_id AS BLOB) = ?
            ORDER BY mailbox_id
            """,
            (
                _sqlite_text_key_bytes(expected, "profile_id"),
                _sqlite_text_key_bytes(expected, "session_id"),
                _sqlite_text_key_bytes(expected, "event_id"),
            ),
        ).fetchall()
        if not rows and allow_missing:
            return None
        if not rows:
            raise ReviewDueConflict("deterministic ReviewDue mailbox row disappeared")
        if len(rows) != 1:
            raise ReviewDueConflict(
                "deterministic ReviewDue logical key contains multiple rows"
            )
        # Delivery status, attempts, claims, leases, handled/error fields, and
        # available_at are mutable after enqueue. Everything selected here is
        # the immutable event envelope and must retain its SQLite representation.
        _validate_exact_sqlite_record(
            rows[0],
            expected,
            conflict_message=(
                "deterministic ReviewDue event id contains conflicting payload"
            ),
        )
        mailbox_id = int(rows[0]["mailbox_id"])
        if mailbox_id < 1:
            raise ReviewDueConflict("deterministic ReviewDue mailbox has invalid id")
        return mailbox_id

    @staticmethod
    def _append_schedule_event(
        conn: Connection,
        schedule: Row,
        aggregate: Row,
        *,
        event_id: str,
        event_type: str,
        outcome: str,
        reason: str,
        now: float,
        source: str = REVIEW_DUE_EVENT_SOURCE,
        metadata: Mapping[str, object] | None = None,
    ) -> None:
        profile_id = _canonical_text(
            schedule["profile_id"],
            field_name="schedule.profile_id",
        )
        session_id = _canonical_text(
            schedule["session_id"],
            field_name="schedule.session_id",
        )
        ownership_generation = _canonical_integer(
            schedule["ownership_generation"],
            field_name="schedule.ownership_generation",
        )
        plan_id = _canonical_text(
            schedule["plan_id"],
            field_name="schedule.plan_id",
        )
        plan_revision = _canonical_integer(
            schedule["plan_revision"],
            field_name="schedule.plan_revision",
        )
        trigger = _canonical_text(
            schedule["trigger"],
            field_name="schedule.trigger",
        )
        current_plan_id = _canonical_text(
            aggregate["current_plan_id"],
            field_name="aggregate.current_plan_id",
        )
        current_plan_revision = _canonical_integer(
            aggregate["review_plan_revision"],
            field_name="aggregate.review_plan_revision",
        )
        committed_state_revision = _canonical_integer(
            aggregate["state_revision"],
            field_name="aggregate.state_revision",
        )
        event_id = _canonical_text(event_id, field_name="event_id")
        event_type = _canonical_text(event_type, field_name="event_type")
        outcome = _canonical_text(outcome, field_name="outcome")
        reason = _canonical_text(reason, field_name="reason")
        source = _canonical_text(source, field_name="source")
        created_at = _canonical_real(now, field_name="now")
        schedule_event_id = _schedule_event_id(schedule, event_type=event_type)
        event_metadata: dict[str, object] = {
            "current_plan_id": current_plan_id,
            "current_plan_revision": current_plan_revision,
            "schedule_plan_id": plan_id,
            "schedule_plan_revision": plan_revision,
            "ownership_generation": ownership_generation,
            "delivery_cycle": _schedule_delivery_cycle(schedule),
        }
        if metadata:
            event_metadata["admission"] = dict(metadata)
        metadata_json = _canonical_json(event_metadata)
        expected: _TypedSQLiteRecord = (
            ("schedule_event_id", "text", schedule_event_id),
            ("profile_id", "text", profile_id),
            ("session_id", "text", session_id),
            ("ownership_generation", "integer", ownership_generation),
            ("event_id", "text", event_id),
            ("plan_id", "text", plan_id),
            ("previous_plan_id", "text", plan_id),
            ("event_type", "text", event_type),
            ("trigger", "text", trigger),
            ("outcome", "text", outcome),
            ("source", "text", source),
            ("requested_delay_seconds", "null", None),
            ("applied_delay_seconds", "null", None),
            ("scheduled_from", "null", None),
            ("next_review_at", "null", None),
            ("reason", "text", reason),
            ("fallback_reason", "text", ""),
            ("model_execution_id", "text", ""),
            ("prompt_signature", "text", ""),
            ("expected_active_epoch", "null", None),
            ("expected_activity_generation", "null", None),
            (
                "committed_state_revision",
                "integer",
                committed_state_revision,
            ),
            ("operation_id", "text", ""),
            ("trace_id", "text", event_id),
            ("metadata_json", "text", metadata_json),
            ("created_at", "real", created_at),
        )
        if _validate_schedule_event_logical_key(
            conn,
            expected,
            allow_missing=True,
        ):
            return
        inserted = conn.execute(
            """
            INSERT INTO agent_review_schedule_events (
                schedule_event_id, profile_id, session_id,
                ownership_generation, event_id, plan_id, previous_plan_id,
                event_type, trigger, outcome, source,
                requested_delay_seconds, applied_delay_seconds, scheduled_from,
                next_review_at, reason, fallback_reason, model_execution_id,
                prompt_signature, expected_active_epoch,
                expected_activity_generation, committed_state_revision,
                operation_id, trace_id, metadata_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            _sqlite_record_values(expected),
        )
        if inserted.rowcount != 1:
            raise ReviewDueConflict(
                "review schedule event insert did not create exactly one row"
            )
        _validate_schedule_event_logical_key(
            conn,
            expected,
            allow_missing=False,
        )

    def _retry_delay(self, attempt_count: int) -> float:
        exponent = min(30, max(0, attempt_count - 1))
        return min(
            self._retry_max_seconds,
            self._retry_base_seconds * (2.0**exponent),
        )

class ManualReviewAdmissionService:
    """Durably admit manual review requests without directly waking an Actor.

    An accepted request may publish an advisory exact-mailbox hint. The shared
    mailbox handoff dispatcher remains solely responsible for claiming sidecar
    evidence and presenting it to a target incarnation.
    """

    def __init__(
        self,
        repository: DurableReviewDueRepository,
        *,
        mailbox_handoff_notifier: MailboxHandoffNotifier | None = None,
    ) -> None:
        """Initialize the manual-admission producer boundary."""

        if mailbox_handoff_notifier is not None and not callable(
            getattr(mailbox_handoff_notifier, "notify", None)
        ):
            raise TypeError("mailbox_handoff_notifier must implement notify(mailbox_id)")
        self._repository = repository
        self._mailbox_handoff_notifier = mailbox_handoff_notifier

    def bind_mailbox_handoff_notifier(
        self,
        notifier: MailboxHandoffNotifier | None,
    ) -> None:
        """Replace the advisory notifier without binding an Actor target."""

        if notifier is not None and not callable(getattr(notifier, "notify", None)):
            raise TypeError("notifier must implement notify(mailbox_id)")
        self._mailbox_handoff_notifier = notifier

    async def request(
        self,
        key: SessionKey,
        *,
        request_id: str,
        requested_by: str,
        reason: str = "manual_review_requested",
    ) -> ManualReviewAdmissionResult:
        """Admit one request and best-effort hint its exact fenced mailbox."""

        result = self._repository.admit_manual_review(
            key,
            request_id=request_id,
            requested_by=requested_by,
            reason=reason,
        )
        if not result.accepted:
            return result
        await self._notify_admission(result)
        return result

    async def _notify_admission(self, result: ManualReviewAdmissionResult) -> None:
        """Publish an advisory fenced hint without falling back to key wake."""

        wake_request = result.wake_request
        if wake_request is None:
            raise ManualReviewAdmissionError(
                "accepted manual review request is missing wake evidence"
            )
        if not wake_request.has_admission_fence:
            logger.debug(
                format_log_event(
                    "agent.manual_review.unfenced_mailbox_handoff_deferred",
                    profile_id=wake_request.key.profile_id,
                    session_id=wake_request.key.session_id,
                    ownership_generation=wake_request.ownership_generation,
                )
            )
            return
        mailbox_id = result.mailbox_id
        if mailbox_id is None:
            raise ManualReviewAdmissionError(
                "accepted fenced manual review request is missing mailbox_id"
            )
        notifier = self._mailbox_handoff_notifier
        if notifier is None:
            logger.debug(
                format_log_event(
                    "agent.manual_review.fenced_mailbox_handoff_deferred",
                    mailbox_id=mailbox_id,
                    profile_id=wake_request.key.profile_id,
                    session_id=wake_request.key.session_id,
                    ownership_generation=wake_request.ownership_generation,
                    admission_fence_id=wake_request.admission_fence_id,
                    admission_fence_generation=wake_request.admission_fence_generation,
                )
            )
            return
        try:
            notification = notifier.notify(mailbox_id)
            if inspect.isawaitable(notification):
                await notification
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                format_log_event(
                    "agent.manual_review.fenced_mailbox_handoff_notify_failed",
                    mailbox_id=mailbox_id,
                    profile_id=wake_request.key.profile_id,
                    session_id=wake_request.key.session_id,
                    ownership_generation=wake_request.ownership_generation,
                    admission_fence_id=wake_request.admission_fence_id,
                    admission_fence_generation=wake_request.admission_fence_generation,
                )
            )


class DurableReviewDueScannerService:
    """Supervise durable ReviewDue admission and advisory handoff publication.

    This producer never scans mailbox debt to wake an Actor. Restart discovery,
    claiming, receipt validation, and target-incarnation handling belong to the
    shared mailbox handoff dispatcher.
    """

    def __init__(
        self,
        repository: DurableReviewDueRepository,
        *,
        mailbox_handoff_notifier: MailboxHandoffNotifier | None = None,
        tick_interval_seconds: float = 5.0,
        batch_limit: int = 50,
        runtime_id: str | None = None,
    ) -> None:
        """Initialize bounded durable schedule admission supervision."""

        if mailbox_handoff_notifier is not None and not callable(
            getattr(mailbox_handoff_notifier, "notify", None)
        ):
            raise TypeError("mailbox_handoff_notifier must implement notify(mailbox_id)")
        self._repository = repository
        self._mailbox_handoff_notifier = mailbox_handoff_notifier
        self._tick_interval_seconds = _positive_finite(
            tick_interval_seconds,
            field_name="tick_interval_seconds",
        )
        self._batch_limit = _positive_int(batch_limit, field_name="batch_limit")
        self._runtime_id = str(
            runtime_id or f"review-due-scanner:{uuid.uuid4().hex}"
        ).strip()
        if not self._runtime_id:
            raise ValueError("runtime_id must not be empty")
        self._task: asyncio.Task[None] | None = None
        self._run_once_lock = asyncio.Lock()
        self._active_run_once_task: asyncio.Task[ReviewDueScanSummary] | None = None
        self._health = RuntimeServiceHealth("durable_review_due_scanner")
        self._last_summary = ReviewDueScanSummary()

    @property
    def last_summary(self) -> ReviewDueScanSummary:
        """Return the last committed repository pass summary."""

        return self._last_summary

    def health_snapshot(self) -> RuntimeServiceHealthSnapshot:
        """Return current bounded-loop supervision health."""

        return self._health.snapshot()

    def bind_mailbox_handoff_notifier(
        self,
        notifier: MailboxHandoffNotifier | None,
    ) -> None:
        """Replace an advisory source notifier without binding an Actor target."""

        if notifier is not None and not callable(getattr(notifier, "notify", None)):
            raise TypeError("notifier must implement notify(mailbox_id)")
        self._mailbox_handoff_notifier = notifier

    def start(self) -> None:
        """Start the supervised scanner loop when an event loop is running."""

        if self._task is not None and not self._task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.debug("agent.review_due_scanner.start_skipped | no_running_loop")
            return
        self._health.start()
        self._task = loop.create_task(
            self._run_loop(),
            name="agent-durable-review-due-scanner",
        )

    async def shutdown(self) -> None:
        """Stop local supervision without changing durable schedule or handoff state."""

        active_run_once_task = self._active_run_once_task
        self._active_run_once_task = None
        if active_run_once_task is not None and not active_run_once_task.done():
            active_run_once_task.cancel()
        task = self._task
        self._task = None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if active_run_once_task is not None and active_run_once_task is not task:
            await asyncio.gather(active_run_once_task, return_exceptions=True)
        self._health.stop()

    async def run_once(self) -> ReviewDueScanSummary:
        """Join or create one serialized durable admission pass."""

        async with self._run_once_lock:
            active_task = self._active_run_once_task
            if active_task is None or active_task.done():
                active_task = asyncio.create_task(
                    self._run_once_leader(),
                    name=f"agent-durable-review-due-pass:{self._runtime_id}",
                )
                self._active_run_once_task = active_task
                active_task.add_done_callback(self._finish_run_once_task)
        return await asyncio.shield(active_task)

    def _finish_run_once_task(
        self,
        completed: asyncio.Task[ReviewDueScanSummary],
    ) -> None:
        """Forget one completed single-flight pass."""

        if self._active_run_once_task is completed:
            self._active_run_once_task = None

    async def _run_once_leader(self) -> ReviewDueScanSummary:
        """Dispatch due schedules and publish only exact fenced mailbox hints."""

        self._health.scan_started()
        summary = ReviewDueScanSummary()
        try:
            summary = self._repository.dispatch_due(limit=self._batch_limit)
            self._last_summary = summary
            await self._notify_dispatched_handoffs(summary)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._health.failed(exc)
            self._repository.record_service_health(
                self._health.snapshot(),
                summary,
                runtime_id=self._runtime_id,
            )
            raise
        self._health.succeeded()
        self._repository.record_service_health(
            self._health.snapshot(),
            summary,
            runtime_id=self._runtime_id,
        )
        return summary

    async def _notify_dispatched_handoffs(
        self,
        summary: ReviewDueScanSummary,
    ) -> None:
        """Best-effort publish exact identifiers without inspecting sidecar state."""

        notifier = self._mailbox_handoff_notifier
        for result in summary.results:
            if result.disposition is not ReviewDueDisposition.DISPATCHED:
                continue
            wake_request = result.wake_request
            if wake_request is None:
                raise ReviewDueConflict("dispatched review result is missing wake evidence")
            if not wake_request.has_admission_fence:
                logger.debug(
                    format_log_event(
                        "agent.review_due_scanner.unfenced_mailbox_handoff_deferred",
                        profile_id=wake_request.key.profile_id,
                        session_id=wake_request.key.session_id,
                        ownership_generation=wake_request.ownership_generation,
                    )
                )
                continue
            mailbox_id = result.mailbox_id
            if mailbox_id is None:
                raise ReviewDueConflict(
                    "dispatched fenced review result is missing mailbox_id"
                )
            if notifier is None:
                logger.debug(
                    format_log_event(
                        "agent.review_due_scanner.fenced_mailbox_handoff_deferred",
                        mailbox_id=mailbox_id,
                        profile_id=wake_request.key.profile_id,
                        session_id=wake_request.key.session_id,
                        ownership_generation=wake_request.ownership_generation,
                        admission_fence_id=wake_request.admission_fence_id,
                        admission_fence_generation=(
                            wake_request.admission_fence_generation
                        ),
                    )
                )
                continue
            try:
                notification = notifier.notify(mailbox_id)
                if inspect.isawaitable(notification):
                    await notification
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    format_log_event(
                        "agent.review_due_scanner.fenced_mailbox_handoff_notify_failed",
                        mailbox_id=mailbox_id,
                        profile_id=wake_request.key.profile_id,
                        session_id=wake_request.key.session_id,
                        ownership_generation=wake_request.ownership_generation,
                        admission_fence_id=wake_request.admission_fence_id,
                        admission_fence_generation=(
                            wake_request.admission_fence_generation
                        ),
                    )
                )

    async def _run_loop(self) -> None:
        """Run bounded admission passes until shutdown cancels the loop."""

        delay = 0.0
        try:
            while True:
                if delay > 0:
                    await asyncio.sleep(delay)
                try:
                    await self.run_once()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.exception(
                        format_log_event(
                            "agent.review_due_scanner.iteration_failed",
                            error_code=type(exc).__name__,
                            consecutive_failures=(
                                self._health.snapshot().consecutive_failures
                            ),
                        )
                    )
                    delay = supervised_backoff_seconds(
                        base_seconds=self._tick_interval_seconds,
                        consecutive_failures=(
                            self._health.snapshot().consecutive_failures
                        ),
                    )
                    continue
                delay = self._tick_interval_seconds
        finally:
            self._health.stop()


def _ownership_unavailable_reason(owner: Row | None) -> str:
    if owner is None:
        return "ownership_missing"
    try:
        status = AgentRuntimeOwnershipStatus(str(owner["status"]))
        mode = AgentRuntimeOwnershipMode(str(owner["mode"]))
    except ValueError:
        return "ownership_invalid"
    if status is AgentRuntimeOwnershipStatus.MIGRATING:
        return "ownership_migrating"
    if status is not AgentRuntimeOwnershipStatus.ACTIVE:
        return "ownership_not_active"
    if mode is not AgentRuntimeOwnershipMode.ACTOR_V2:
        return "ownership_not_actor_v2"
    return ""


def _schedule_mailbox_record(
    schedule: Row,
    *,
    delivery: _ScheduleMailboxDelivery,
    now: float,
) -> _TypedSQLiteRecord:
    return (
        ("event_id", "text", _canonical_text(delivery.event_id, field_name="event_id")),
        (
            "profile_id",
            "text",
            _canonical_text(
                schedule["profile_id"],
                field_name="schedule.profile_id",
            ),
        ),
        (
            "session_id",
            "text",
            _canonical_text(
                schedule["session_id"],
                field_name="schedule.session_id",
            ),
        ),
        (
            "ownership_generation",
            "integer",
            _canonical_integer(
                schedule["ownership_generation"],
                field_name="schedule.ownership_generation",
            ),
        ),
        ("kind", "text", _canonical_text(delivery.kind, field_name="kind")),
        ("source", "text", _canonical_text(delivery.source, field_name="source")),
        (
            "occurred_at",
            "real",
            _canonical_real(
                delivery.occurred_at,
                field_name="delivery.occurred_at",
            ),
        ),
        (
            "payload_json",
            "text",
            _canonical_text(delivery.payload_json, field_name="payload_json"),
        ),
        (
            "causation_id",
            "text",
            _canonical_text(delivery.causation_id, field_name="causation_id"),
        ),
        (
            "correlation_id",
            "text",
            _canonical_text(delivery.correlation_id, field_name="correlation_id"),
        ),
        ("trace_id", "text", _canonical_text(delivery.trace_id, field_name="trace_id")),
        ("created_at", "real", _canonical_real(now, field_name="now")),
    )


def _review_due_payload(schedule: Row, *, event_id: str) -> dict[str, object]:
    delivery_cycle = _schedule_delivery_cycle(schedule)
    payload: dict[str, object] = {
        "version": 1 if delivery_cycle == 0 else 2,
        "event_id": event_id,
        "session_key": {
            "profile_id": str(schedule["profile_id"]),
            "session_id": str(schedule["session_id"]),
        },
        "plan_id": str(schedule["plan_id"]),
        "plan_revision": int(schedule["plan_revision"]),
        "ownership_generation": int(schedule["ownership_generation"]),
        "trigger": str(schedule["trigger"]),
        "outcome": str(schedule["outcome"]),
        "reason": str(schedule["reason"]),
        "scheduled_from": float(schedule["scheduled_from"]),
        "next_review_at": float(schedule["next_review_at"]),
        "attempt_count": int(schedule["attempt_count"]),
        "committed_state_revision": int(schedule["committed_state_revision"]),
        "expected_active_epoch": _optional_int(schedule["expected_active_epoch"]),
        "expected_activity_generation": _optional_int(
            schedule["expected_activity_generation"]
        ),
    }
    if delivery_cycle > 0:
        payload["delivery_cycle"] = delivery_cycle
    return payload


def _review_due_event_id(schedule: Mapping[str, object]) -> str:
    return review_due_event_id(
        key=SessionKey(
            str(schedule["profile_id"]),
            str(schedule["session_id"]),
        ),
        plan_id=str(schedule["plan_id"]),
        plan_revision=int(schedule["plan_revision"]),
        ownership_generation=int(schedule["ownership_generation"]),
        delivery_cycle=_schedule_delivery_cycle(schedule),
    )


def _schedule_event_id(
    schedule: Mapping[str, object],
    *,
    event_type: str,
) -> str:
    identity_parts: list[object] = [
        str(schedule["profile_id"]),
        str(schedule["session_id"]),
        str(schedule["plan_id"]),
        int(schedule["plan_revision"]),
        int(schedule["ownership_generation"]),
        event_type,
    ]
    delivery_cycle = _schedule_delivery_cycle(schedule)
    if delivery_cycle > 0:
        identity_parts.append(delivery_cycle)
    identity = _canonical_json(identity_parts)
    digest = uuid.uuid5(_SCHEDULE_EVENT_NAMESPACE, identity).hex
    return f"schedule-event:{event_type}:{digest}"


def _wake_request_for_ownership(
    ownership: AgentRuntimeOwnership,
) -> FencedMailboxWakeRequest:
    """Project an already-validated ownership row to a wake identity."""

    if not ownership.actor_v2_active:
        raise ValueError("wake requests require active actor_v2 ownership")
    return FencedMailboxWakeRequest(
        key=ownership.key,
        ownership_generation=ownership.generation,
        admission_fence_id=ownership.admission_fence_id,
        admission_fence_generation=ownership.admission_fence_generation,
    )


def _result(
    schedule: Row,
    *,
    disposition: ReviewDueDisposition,
    event_id: str = "",
    mailbox_id: int | None = None,
    mailbox_inserted: bool = False,
    reason: str = "",
    retry_at: float | None = None,
    wake_request: FencedMailboxWakeRequest | None = None,
) -> ReviewDueDispatchResult:
    return ReviewDueDispatchResult(
        key=SessionKey(
            str(schedule["profile_id"]),
            str(schedule["session_id"]),
        ),
        plan_id=str(schedule["plan_id"]),
        plan_revision=int(schedule["plan_revision"]),
        ownership_generation=int(schedule["ownership_generation"]),
        delivery_cycle=_schedule_delivery_cycle(schedule),
        disposition=disposition,
        event_id=event_id,
        mailbox_id=mailbox_id,
        mailbox_inserted=mailbox_inserted,
        reason=reason,
        retry_at=retry_at,
        wake_request=wake_request,
    )


def _due_scan_cursor(schedule: Mapping[str, object]) -> _DueScanCursor:
    return (
        float(schedule["available_at"]),
        float(schedule["next_review_at"]),
        str(schedule["profile_id"]),
        str(schedule["session_id"]),
        int(schedule["plan_revision"]),
        str(schedule["plan_id"]),
    )


def _review_wake_cursor_parameters(
    cursor: ReviewWakeCursor,
) -> tuple[object, ...]:
    """Expand one review wake cursor for the deterministic SQL keyset chain."""

    return (
        cursor.mailbox_id,
        cursor.mailbox_id,
        cursor.profile_id,
        cursor.mailbox_id,
        cursor.profile_id,
        cursor.session_id,
        cursor.mailbox_id,
        cursor.profile_id,
        cursor.session_id,
        cursor.ownership_generation,
        cursor.mailbox_id,
        cursor.profile_id,
        cursor.session_id,
        cursor.ownership_generation,
        cursor.admission_fence_id,
        cursor.mailbox_id,
        cursor.profile_id,
        cursor.session_id,
        cursor.ownership_generation,
        cursor.admission_fence_id,
        cursor.admission_fence_generation,
    )


def _unique_keys(keys: Iterable[SessionKey]) -> tuple[SessionKey, ...]:
    result: list[SessionKey] = []
    seen: set[SessionKey] = set()
    for key in keys:
        if not isinstance(key, SessionKey):
            raise TypeError("wake debt must contain SessionKey values")
        if key in seen:
            continue
        seen.add(key)
        result.append(key)
    return tuple(result)


def _unique_wake_requests(
    requests: Iterable[FencedMailboxWakeRequest],
) -> tuple[FencedMailboxWakeRequest, ...]:
    """Preserve order while deduplicating only identical Actor incarnations."""

    result: list[FencedMailboxWakeRequest] = []
    seen: set[FencedMailboxWakeRequest] = set()
    for request in requests:
        if not isinstance(request, FencedMailboxWakeRequest):
            raise TypeError("wake debt must contain FencedMailboxWakeRequest values")
        if request in seen:
            continue
        seen.add(request)
        result.append(request)
    return tuple(result)


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _canonical_text(value: object, *, field_name: str) -> str:
    if type(value) is not str:
        raise ReviewDueConflict(f"{field_name} must use SQLite TEXT storage")
    return value


def _canonical_integer(value: object, *, field_name: str) -> int:
    if type(value) is not int:
        raise ReviewDueConflict(f"{field_name} must use SQLite INTEGER storage")
    return value


def _canonical_real(value: object, *, field_name: str) -> float:
    if type(value) is not float or not math.isfinite(value):
        raise ReviewDueConflict(f"{field_name} must use finite SQLite REAL storage")
    return value


def _sqlite_record_values(record: _TypedSQLiteRecord) -> tuple[object, ...]:
    return tuple(value for _field, _storage_class, value in record)


def _sqlite_record_value(record: _TypedSQLiteRecord, field_name: str) -> object:
    for field, _storage_class, value in record:
        if field == field_name:
            return value
    raise ReviewDueConflict(f"expected SQLite record is missing {field_name}")


def _sqlite_text_key_bytes(
    record: _TypedSQLiteRecord,
    field_name: str,
) -> bytes:
    value = _canonical_text(
        _sqlite_record_value(record, field_name),
        field_name=field_name,
    )
    return value.encode("utf-8", errors="strict")


def _validate_schedule_event_logical_key(
    conn: Connection,
    expected: _TypedSQLiteRecord,
    *,
    allow_missing: bool,
) -> bool:
    rows = conn.execute(
        """
        SELECT schedule_event_seq,
               CAST(schedule_event_id AS BLOB) AS schedule_event_id,
               typeof(schedule_event_id)
                   AS schedule_event_id_storage_class,
               CAST(profile_id AS BLOB) AS profile_id,
               typeof(profile_id) AS profile_id_storage_class,
               CAST(session_id AS BLOB) AS session_id,
               typeof(session_id) AS session_id_storage_class,
               ownership_generation,
               typeof(ownership_generation)
                   AS ownership_generation_storage_class,
               CAST(event_id AS BLOB) AS event_id,
               typeof(event_id) AS event_id_storage_class,
               CAST(plan_id AS BLOB) AS plan_id,
               typeof(plan_id) AS plan_id_storage_class,
               CAST(previous_plan_id AS BLOB) AS previous_plan_id,
               typeof(previous_plan_id)
                   AS previous_plan_id_storage_class,
               CAST(event_type AS BLOB) AS event_type,
               typeof(event_type) AS event_type_storage_class,
               CAST(trigger AS BLOB) AS trigger,
               typeof(trigger) AS trigger_storage_class,
               CAST(outcome AS BLOB) AS outcome,
               typeof(outcome) AS outcome_storage_class,
               CAST(source AS BLOB) AS source,
               typeof(source) AS source_storage_class,
               requested_delay_seconds,
               typeof(requested_delay_seconds)
                   AS requested_delay_seconds_storage_class,
               applied_delay_seconds,
               typeof(applied_delay_seconds)
                   AS applied_delay_seconds_storage_class,
               scheduled_from,
               typeof(scheduled_from) AS scheduled_from_storage_class,
               next_review_at,
               typeof(next_review_at) AS next_review_at_storage_class,
               CAST(reason AS BLOB) AS reason,
               typeof(reason) AS reason_storage_class,
               CAST(fallback_reason AS BLOB) AS fallback_reason,
               typeof(fallback_reason) AS fallback_reason_storage_class,
               CAST(model_execution_id AS BLOB) AS model_execution_id,
               typeof(model_execution_id)
                   AS model_execution_id_storage_class,
               CAST(prompt_signature AS BLOB) AS prompt_signature,
               typeof(prompt_signature)
                   AS prompt_signature_storage_class,
               expected_active_epoch,
               typeof(expected_active_epoch)
                   AS expected_active_epoch_storage_class,
               expected_activity_generation,
               typeof(expected_activity_generation)
                   AS expected_activity_generation_storage_class,
               committed_state_revision,
               typeof(committed_state_revision)
                   AS committed_state_revision_storage_class,
               CAST(operation_id AS BLOB) AS operation_id,
               typeof(operation_id) AS operation_id_storage_class,
               CAST(trace_id AS BLOB) AS trace_id,
               typeof(trace_id) AS trace_id_storage_class,
               CAST(metadata_json AS BLOB) AS metadata_json,
               typeof(metadata_json) AS metadata_json_storage_class,
               created_at, typeof(created_at) AS created_at_storage_class
        FROM agent_review_schedule_events
        WHERE CAST(schedule_event_id AS BLOB) = ?
        ORDER BY schedule_event_seq
        """,
        (_sqlite_text_key_bytes(expected, "schedule_event_id"),),
    ).fetchall()
    if not rows and allow_missing:
        return False
    if not rows:
        raise ReviewDueConflict("deterministic schedule event disappeared")
    if len(rows) != 1:
        raise ReviewDueConflict(
            "deterministic schedule event logical key contains multiple rows"
        )
    # A committed row can only be an exact replay when every value,
    # including created_at, came from the same deterministic attempt. A prior
    # failed transaction cannot leave this row behind in SQLite.
    _validate_exact_sqlite_record(
        rows[0],
        expected,
        conflict_message=(
            "deterministic review schedule event contains conflicting payload"
        ),
    )
    return True


def _validate_exact_sqlite_record(
    row: Row,
    expected: _TypedSQLiteRecord,
    *,
    conflict_message: str,
) -> None:
    for field, storage_class, expected_value in expected:
        actual_storage_class = row[f"{field}_storage_class"]
        if actual_storage_class != storage_class:
            raise ReviewDueConflict(f"{conflict_message}: {field}")
        actual_value = row[field]
        if storage_class == "text":
            if type(actual_value) is not bytes:
                raise ReviewDueConflict(f"{conflict_message}: {field}")
            try:
                actual_value = actual_value.decode("utf-8", errors="strict")
            except UnicodeDecodeError as exc:
                raise ReviewDueConflict(f"{conflict_message}: {field}") from exc
        if actual_value != expected_value:
            raise ReviewDueConflict(f"{conflict_message}: {field}")


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ReviewDueConflict("schedule integer fence is invalid")
    return value


def _schedule_delivery_cycle(schedule: Mapping[str, object]) -> int:
    value = schedule["delivery_cycle"]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ReviewDueConflict("schedule delivery_cycle fence is invalid")
    return value


def _positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _nonnegative_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def _positive_finite(value: object, *, field_name: str) -> float:
    numeric = _nonnegative_finite(value, field_name=field_name)
    if numeric <= 0:
        raise ValueError(f"{field_name} must be finite and positive")
    return numeric


def _nonnegative_finite(value: object, *, field_name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be finite and non-negative")
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be finite and non-negative") from exc
    if not math.isfinite(numeric) or numeric < 0:
        raise ValueError(f"{field_name} must be finite and non-negative")
    return numeric


__all__ = [
    "REVIEW_DUE_EVENT_KIND",
    "REVIEW_DUE_EVENT_SOURCE",
    "GLOBAL_REVIEW_DUE_HEALTH_PROFILE_ID",
    "DurableReviewDueRepository",
    "DurableReviewDueScannerService",
    "ManualReviewAdmissionDisposition",
    "ManualReviewAdmissionError",
    "ManualReviewAdmissionResult",
    "ManualReviewAdmissionService",
    "ReviewDueConflict",
    "ReviewDueDispatchResult",
    "ReviewDueDisposition",
    "ReviewDueRepositoryError",
    "ReviewDueScanSummary",
    "ReviewWakeCursor",
    "review_due_event_id",
]
