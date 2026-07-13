"""Durable, inactive recovery-graph discovery for session actors.

The scanner intentionally has no registry or workflow dependency. It only
turns one transactionally consistent persistence projection into a typed
recovery certificate, case, and mailbox delivery. A future commit coordinator
must re-read the same graph before any delivery can alter an aggregate.
"""

from __future__ import annotations

import math
import sqlite3
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import TYPE_CHECKING

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.recovery import (
    RECOVERY_DELIVERY_EVENT_KIND,
    RECOVERY_DELIVERY_EVENT_SOURCE,
    RecoveryCertificate,
    RecoveryDecisionKind,
    RecoveryDeliveryPayload,
    RecoverySubject,
    RecoveryV1Policy,
    canonical_recovery_digest,
    canonical_recovery_json,
)
from shinbot.agent.runtime.session_actor.recovery_graph_reader import (
    MAX_RECOVERY_RAW_FIELD_BYTES,
    MAX_RECOVERY_SOURCE_ROWS,
    RecoveryGraphAuthority,
    RecoveryGraphNotEligible,
    RecoveryGraphReadError,
    SQLiteRecoveryGraphReader,
)

if TYPE_CHECKING:
    from shinbot.persistence.engine import DatabaseManager


MAX_RECOVERY_SCAN_CANDIDATES = 64
MAX_RECOVERY_DELIVERY_CYCLES = 3


class RecoveryScanDisposition(StrEnum):
    """One durable outcome from scanning a candidate actor aggregate."""

    SKIPPED = "skipped"
    NO_RECOVERY = "no_recovery"
    WAITING = "waiting"
    BLOCKED = "blocked"
    DELIVERED = "delivered"
    ALREADY_DELIVERED = "already_delivered"
    DELIVERY_EXHAUSTED = "delivery_exhausted"
    FINDING_RECORDED = "finding_recorded"


@dataclass(slots=True, frozen=True)
class RecoveryScanResult:
    """Result of one bounded recovery discovery transaction."""

    key: SessionKey
    disposition: RecoveryScanDisposition
    case_id: str = ""
    event_id: str = ""
    reason_codes: tuple[str, ...] = ()


@dataclass(slots=True, frozen=True)
class RecoveryScanSummary:
    """Aggregate result of one explicit scanner pass."""

    results: tuple[RecoveryScanResult, ...]

    @property
    def delivered_count(self) -> int:
        """Return the number of newly committed typed mailbox deliveries."""

        return sum(
            result.disposition is RecoveryScanDisposition.DELIVERED
            for result in self.results
        )

    @property
    def finding_count(self) -> int:
        """Return candidates whose raw authority could not be certified."""

        return sum(
            result.disposition is RecoveryScanDisposition.FINDING_RECORDED
            for result in self.results
        )


@dataclass(slots=True, frozen=True)
class _RecoveryCaseRow:
    """Minimal mutable recovery-case projection used by one scanner transaction."""

    case_id: str
    status: str
    next_delivery_cycle: int
    delivery_count: int
    last_event_id: str
    latest_certificate_digest: str
    last_error: str
    updated_at: float


class SQLiteRecoveryGraphScanner:
    """Build and emit typed recovery authority without activating Actor v2.

    The class is deliberately independent from ``AgentSessionActorRegistry``.
    Calling :meth:`scan` commits only recovery cases, findings, and mailbox
    rows; callers must wake actors after the transaction and only after the
    future activation gate permits it.
    """

    def __init__(
        self,
        database: DatabaseManager,
        *,
        clock: Callable[[], float] | None = None,
        policy: RecoveryV1Policy | None = None,
        max_delivery_cycles: int = MAX_RECOVERY_DELIVERY_CYCLES,
    ) -> None:
        """Bind one scanner to exactly one durable persistence domain."""

        if max_delivery_cycles < 1:
            raise ValueError("max_delivery_cycles must be at least one")
        self._database = database
        self._clock = clock or time.time
        self._reader = SQLiteRecoveryGraphReader(database, policy=policy)
        self._max_delivery_cycles = max_delivery_cycles

    @property
    def persistence_domain(self) -> object:
        """Return the exact database domain scanned by this instance."""

        return self._database

    @property
    def policy(self) -> RecoveryV1Policy:
        """Return the pure decision policy used for every graph build."""

        return self._reader.policy

    @property
    def graph_reader(self) -> RecoveryGraphAuthority:
        """Expose the shared read-only authority port without writer methods."""

        return self._reader

    def scan(
        self,
        *,
        limit: int = MAX_RECOVERY_SCAN_CANDIDATES,
        profile_id: str | None = None,
    ) -> RecoveryScanSummary:
        """Scan a bounded page of active non-idle Actor v2 aggregates."""

        if limit < 1 or limit > MAX_RECOVERY_SCAN_CANDIDATES:
            raise ValueError(
                "limit must be between 1 and " f"{MAX_RECOVERY_SCAN_CANDIDATES}"
            )
        candidates = self._candidate_keys(limit=limit, profile_id=profile_id)
        results: list[RecoveryScanResult] = []
        for key in candidates:
            with self._database.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                results.append(self._scan_candidate(conn, key=key))
        return RecoveryScanSummary(results=tuple(results))

    def rebuild_certificate(
        self,
        conn: sqlite3.Connection,
        *,
        key: SessionKey,
        ownership_generation: int,
    ) -> RecoveryCertificate:
        """Rebuild the exact scanner graph inside an existing write transaction."""

        return self._reader.rebuild_certificate(
            conn,
            key=key,
            ownership_generation=ownership_generation,
        )

    def _candidate_keys(
        self,
        *,
        limit: int,
        profile_id: str | None,
    ) -> tuple[SessionKey, ...]:
        where_profile = ""
        params: list[object] = []
        if profile_id is not None:
            normalized_profile_id = _required_text(profile_id, field_name="profile_id")
            where_profile = " AND aggregate.profile_id = ?"
            params.append(normalized_profile_id)
        params.append(limit)
        with self._database.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT aggregate.profile_id, aggregate.session_id
                FROM agent_session_aggregates AS aggregate
                JOIN agent_session_runtime_ownership AS ownership
                  ON ownership.profile_id = aggregate.profile_id
                 AND ownership.session_id = aggregate.session_id
                 AND ownership.mode = 'actor_v2'
                 AND ownership.status = 'active'
                 AND ownership.generation = aggregate.ownership_generation
                WHERE aggregate.state != 'idle'
                  AND aggregate.ownership_generation >= 1
                  {where_profile}
                ORDER BY aggregate.profile_id ASC, aggregate.session_id ASC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
        return tuple(
            SessionKey(str(row["profile_id"]), str(row["session_id"]))
            for row in rows
        )

    def _scan_candidate(
        self,
        conn: sqlite3.Connection,
        *,
        key: SessionKey,
    ) -> RecoveryScanResult:
        now = _nonnegative_finite(self._clock(), field_name="clock")
        ownership_row = conn.execute(
            """
            SELECT generation
            FROM agent_session_runtime_ownership
            WHERE profile_id = ?
              AND session_id = ?
              AND mode = 'actor_v2'
              AND status = 'active'
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
        if ownership_row is None:
            return RecoveryScanResult(key, RecoveryScanDisposition.SKIPPED)
        ownership_generation = _required_positive_int(
            ownership_row["generation"],
            field_name="ownership_generation",
        )
        try:
            certificate = self._reader.rebuild_certificate(
                conn,
                key=key,
                ownership_generation=ownership_generation,
            )
        except RecoveryGraphNotEligible:
            return RecoveryScanResult(key, RecoveryScanDisposition.SKIPPED)
        except RecoveryGraphReadError as exc:
            self._record_finding(
                conn,
                key=key,
                ownership_generation=ownership_generation,
                error=exc,
                now=now,
            )
            return RecoveryScanResult(
                key,
                RecoveryScanDisposition.FINDING_RECORDED,
                reason_codes=(exc.code,),
            )

        def complete(result: RecoveryScanResult) -> RecoveryScanResult:
            """Resolve prior findings only after this whole candidate succeeds."""

            self._resolve_findings(
                conn,
                key=key,
                ownership_generation=ownership_generation,
                now=now,
            )
            return result

        decision = certificate.decision
        if decision.kind is RecoveryDecisionKind.NO_RECOVERY:
            return complete(
                RecoveryScanResult(
                    key,
                    RecoveryScanDisposition.NO_RECOVERY,
                    reason_codes=decision.reason_codes,
                )
            )
        if decision.kind is RecoveryDecisionKind.WAIT_FOR_PROGRESS:
            return complete(
                RecoveryScanResult(
                    key,
                    RecoveryScanDisposition.WAITING,
                    reason_codes=decision.reason_codes,
                )
            )
        if decision.kind is RecoveryDecisionKind.RECORD_BLOCKER:
            self._record_case_blocker(conn, certificate=certificate, now=now)
            return complete(
                RecoveryScanResult(
                    key,
                    RecoveryScanDisposition.BLOCKED,
                    case_id=certificate.case_identity.case_id,
                    reason_codes=decision.reason_codes,
                )
            )
        if decision.kind is RecoveryDecisionKind.RECOVER_ORPHANED_WORK:
            try:
                conn.execute("SAVEPOINT recovery_delivery")
                try:
                    result = self._emit_delivery(
                        conn,
                        certificate=certificate,
                        now=now,
                    )
                except Exception:
                    conn.execute("ROLLBACK TO SAVEPOINT recovery_delivery")
                    conn.execute("RELEASE SAVEPOINT recovery_delivery")
                    raise
                conn.execute("RELEASE SAVEPOINT recovery_delivery")
                return complete(result)
            except RecoveryGraphReadError as exc:
                self._record_finding(
                    conn,
                    key=key,
                    ownership_generation=ownership_generation,
                    error=exc,
                    now=now,
                )
                return RecoveryScanResult(
                    key,
                    RecoveryScanDisposition.FINDING_RECORDED,
                    reason_codes=(exc.code,),
                )
        raise AssertionError(f"unexpected recovery decision: {decision.kind!r}")

    def _record_finding(
        self,
        conn: sqlite3.Connection,
        *,
        key: SessionKey,
        ownership_generation: int,
        error: RecoveryGraphReadError,
        now: float,
    ) -> None:
        evidence = {
            "code": error.code,
            "evidence": dict(error.evidence),
        }
        evidence_json = canonical_recovery_json(evidence)
        evidence_digest = canonical_recovery_digest(evidence)
        finding_id = "recovery-finding:v1:" + canonical_recovery_digest(
            {
                "code": error.code,
                "evidence_digest": evidence_digest,
                "ownership_generation": ownership_generation,
                "profile_id": key.profile_id,
                "session_id": key.session_id,
            }
        )
        existing = conn.execute(
            """
            SELECT last_seen_at
            FROM agent_session_recovery_findings
            WHERE profile_id = ?
              AND session_id = ?
              AND ownership_generation = ?
              AND code = ?
              AND evidence_digest = ?
            """,
            (
                key.profile_id,
                key.session_id,
                ownership_generation,
                error.code,
                evidence_digest,
            ),
        ).fetchone()
        if existing is None:
            conn.execute(
                """
                INSERT INTO agent_session_recovery_findings (
                    finding_id, profile_id, session_id, ownership_generation,
                    code, evidence_digest, evidence_json, status,
                    occurrence_count, first_seen_at, last_seen_at, resolved_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'open', 1, ?, ?, NULL)
                """,
                (
                    finding_id,
                    key.profile_id,
                    key.session_id,
                    ownership_generation,
                    error.code,
                    evidence_digest,
                    evidence_json,
                    now,
                    now,
                ),
            )
            return
        last_seen_at = _nonnegative_finite(
            existing["last_seen_at"],
            field_name="finding.last_seen_at",
        )
        conn.execute(
            """
            UPDATE agent_session_recovery_findings
            SET evidence_json = ?, status = 'open',
                occurrence_count = occurrence_count + 1,
                last_seen_at = ?, resolved_at = NULL
            WHERE profile_id = ?
              AND session_id = ?
              AND ownership_generation = ?
              AND code = ?
              AND evidence_digest = ?
            """,
            (
                evidence_json,
                _next_monotonic_time(last_seen_at, now),
                key.profile_id,
                key.session_id,
                ownership_generation,
                error.code,
                evidence_digest,
            ),
        )

    @staticmethod
    def _resolve_findings(
        conn: sqlite3.Connection,
        *,
        key: SessionKey,
        ownership_generation: int,
        now: float,
    ) -> None:
        rows = conn.execute(
            """
            SELECT finding_id, last_seen_at
            FROM agent_session_recovery_findings
            WHERE profile_id = ?
              AND session_id = ?
              AND ownership_generation = ?
              AND status = 'open'
            ORDER BY finding_seq ASC
            """,
            (key.profile_id, key.session_id, ownership_generation),
        ).fetchall()
        for row in rows:
            last_seen_at = _nonnegative_finite(
                row["last_seen_at"],
                field_name="finding.last_seen_at",
            )
            resolved_at = _next_monotonic_time(last_seen_at, now)
            updated = conn.execute(
                """
                UPDATE agent_session_recovery_findings
                SET status = 'resolved', last_seen_at = ?, resolved_at = ?
                WHERE finding_id = ? AND status = 'open'
                """,
                (resolved_at, resolved_at, row["finding_id"]),
            )
            if updated.rowcount != 1:
                raise sqlite3.IntegrityError("recovery finding changed while resolving")

    def _record_case_blocker(
        self,
        conn: sqlite3.Connection,
        *,
        certificate: RecoveryCertificate,
        now: float,
    ) -> None:
        case = self._load_case(conn, certificate=certificate)
        reason = _bounded_reason(certificate.decision.reason_codes)
        if case is None:
            self._insert_case(
                conn,
                certificate=certificate,
                status="scanner_blocked",
                last_error=reason,
                now=now,
            )
            return
        if case.status in {"applied", "superseded", "delivery_exhausted"}:
            return
        updated = conn.execute(
            """
            UPDATE agent_session_recovery_cases
            SET latest_certificate_digest = ?, status = 'scanner_blocked',
                last_error = ?, updated_at = ?
            WHERE case_id = ?
              AND status IN ('open', 'scanner_blocked')
            """,
            (
                certificate.certificate_digest,
                reason,
                _next_monotonic_time(case.updated_at, now),
                case.case_id,
            ),
        )
        if updated.rowcount != 1:
            raise sqlite3.IntegrityError("recovery case changed while recording blocker")

    def _emit_delivery(
        self,
        conn: sqlite3.Connection,
        *,
        certificate: RecoveryCertificate,
        now: float,
    ) -> RecoveryScanResult:
        case = self._load_case(conn, certificate=certificate)
        case_is_new = case is None
        if case is None:
            case = _RecoveryCaseRow(
                case_id=certificate.case_identity.case_id,
                status="open",
                next_delivery_cycle=0,
                delivery_count=0,
                last_event_id="",
                latest_certificate_digest=certificate.certificate_digest,
                last_error="",
                updated_at=now,
            )
        if case.status == "delivery_exhausted":
            return RecoveryScanResult(
                _subject_key(certificate.subject),
                RecoveryScanDisposition.DELIVERY_EXHAUSTED,
                case_id=case.case_id,
                reason_codes=("recovery_delivery_exhausted",),
            )
        reopened_from_blocker = False
        if case.status == "scanner_blocked":
            if case.last_error == "recovery_refresh_cycle_limit_reached":
                return RecoveryScanResult(
                    _subject_key(certificate.subject),
                    RecoveryScanDisposition.BLOCKED,
                    case_id=case.case_id,
                    reason_codes=("recovery_refresh_cycle_limit_reached",),
                )
            reopened_at = _next_monotonic_time(case.updated_at, now)
            reopened = conn.execute(
                """
                UPDATE agent_session_recovery_cases
                SET status = 'open', last_error = '', updated_at = ?
                WHERE case_id = ?
                  AND status = 'scanner_blocked'
                  AND next_delivery_cycle = ?
                  AND delivery_count = ?
                  AND last_event_id = ?
                  AND latest_certificate_digest = ?
                  AND updated_at = ?
                """,
                (
                    reopened_at,
                    case.case_id,
                    case.next_delivery_cycle,
                    case.delivery_count,
                    case.last_event_id,
                    case.latest_certificate_digest,
                    case.updated_at,
                ),
            )
            if reopened.rowcount != 1:
                raise sqlite3.IntegrityError(
                    "recovery case changed while reopening a resolved blocker"
                )
            case = replace(
                case,
                status="open",
                last_error="",
                updated_at=reopened_at,
            )
            reopened_from_blocker = True
        if case.status != "open":
            return RecoveryScanResult(
                _subject_key(certificate.subject),
                RecoveryScanDisposition.BLOCKED,
                case_id=case.case_id,
                reason_codes=(f"recovery_case_{case.status}",),
            )
        previous_delivery_status: str | None = None
        if case.last_event_id:
            previous_delivery = self._case_delivery_status(
                conn,
                case=case,
                certificate=certificate,
            )
            if previous_delivery is None:
                raise RecoveryGraphReadError(
                    "recovery_delivery_case_progress_divergent",
                    evidence={
                        "case_id": case.case_id,
                        "event_id": case.last_event_id,
                    },
                )
            previous_payload, delivery_status = previous_delivery
            previous_delivery_status = delivery_status
            if delivery_status in {"pending", "processing"}:
                return RecoveryScanResult(
                    _subject_key(certificate.subject),
                    RecoveryScanDisposition.ALREADY_DELIVERED,
                    case_id=case.case_id,
                    event_id=case.last_event_id,
                    reason_codes=("recovery_delivery_in_flight",),
                )
            if delivery_status == "completed":
                if (
                    previous_payload.certificate.certificate_digest
                    != case.latest_certificate_digest
                ):
                    raise RecoveryGraphReadError(
                        "recovery_delivery_case_certificate_divergent",
                        evidence={
                            "case_id": case.case_id,
                            "event_id": case.last_event_id,
                        },
                    )
                if (
                    not reopened_from_blocker
                    and case.latest_certificate_digest
                    == certificate.certificate_digest
                ):
                    raise RecoveryGraphReadError(
                        "recovery_completed_delivery_without_refresh",
                        evidence={
                            "case_id": case.case_id,
                            "event_id": case.last_event_id,
                        },
                    )
            elif delivery_status != "failed":
                raise RecoveryGraphReadError(
                    "recovery_delivery_case_status_divergent",
                    evidence={
                        "case_id": case.case_id,
                        "event_id": case.last_event_id,
                        "status": delivery_status,
                    },
                )
        if case.delivery_count >= self._max_delivery_cycles:
            terminal_status = "delivery_exhausted"
            terminal_error = "recovery_delivery_cycle_limit_reached"
            disposition = RecoveryScanDisposition.DELIVERY_EXHAUSTED
            reason_code = "recovery_delivery_exhausted"
            if previous_delivery_status == "completed":
                terminal_status = "scanner_blocked"
                terminal_error = "recovery_refresh_cycle_limit_reached"
                disposition = RecoveryScanDisposition.BLOCKED
                reason_code = terminal_error
            updated = conn.execute(
                """
                UPDATE agent_session_recovery_cases
                SET status = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE case_id = ?
                  AND status = 'open'
                  AND delivery_count = ?
                """,
                (
                    terminal_status,
                    terminal_error,
                    _next_monotonic_time(case.updated_at, now),
                    case.case_id,
                    case.delivery_count,
                ),
            )
            if updated.rowcount != 1:
                raise sqlite3.IntegrityError(
                    "recovery case changed while exhausting delivery cycles"
                )
            return RecoveryScanResult(
                _subject_key(certificate.subject),
                disposition,
                case_id=case.case_id,
                reason_codes=(reason_code,),
            )
        payload = RecoveryDeliveryPayload(
            certificate=certificate,
            delivery_cycle=case.next_delivery_cycle,
        )
        payload_json = canonical_recovery_json(payload.to_record())
        if self._reader.validate_delivery_mailbox(
            conn,
            payload=payload,
            payload_json=payload_json,
            now=now,
            allow_missing=True,
        ):
            raise RecoveryGraphReadError(
                "recovery_delivery_case_progress_divergent",
                evidence={
                    "case_id": case.case_id,
                    "event_id": payload.event_id,
                },
            )
        if case_is_new:
            self._insert_case(
                conn,
                certificate=certificate,
                status="open",
                last_error="",
                now=now,
            )
        inserted = self._insert_or_validate_delivery_mailbox(
            conn,
            payload=payload,
            payload_json=payload_json,
            now=now,
        )
        if not inserted:
            raise RecoveryGraphReadError(
                "recovery_delivery_case_progress_divergent",
                evidence={
                    "case_id": case.case_id,
                    "event_id": payload.event_id,
                },
            )
        updated = conn.execute(
            """
            UPDATE agent_session_recovery_cases
            SET next_delivery_cycle = ?, delivery_count = ?,
                last_event_id = ?, latest_certificate_digest = ?,
                status = 'open', last_error = '', updated_at = ?
            WHERE case_id = ?
              AND status = 'open'
              AND next_delivery_cycle = ?
              AND delivery_count = ?
            """,
            (
                case.next_delivery_cycle + 1,
                case.delivery_count + 1,
                payload.event_id,
                certificate.certificate_digest,
                _next_monotonic_time(case.updated_at, now),
                case.case_id,
                case.next_delivery_cycle,
                case.delivery_count,
            ),
        )
        if updated.rowcount != 1:
            raise sqlite3.IntegrityError("recovery case changed while emitting delivery")
        return RecoveryScanResult(
            _subject_key(certificate.subject),
            RecoveryScanDisposition.DELIVERED,
            case_id=case.case_id,
            event_id=payload.event_id,
            reason_codes=certificate.decision.reason_codes,
        )

    @staticmethod
    def _insert_case(
        conn: sqlite3.Connection,
        *,
        certificate: RecoveryCertificate,
        status: str,
        last_error: str,
        now: float,
    ) -> None:
        conn.execute(
            """
            INSERT INTO agent_session_recovery_cases (
                case_id, profile_id, session_id, ownership_generation,
                certificate_version, policy_version, work_graph_digest,
                latest_certificate_digest, status, next_delivery_cycle,
                delivery_count, last_event_id, last_error, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, '', ?, ?, ?)
            """,
            (
                certificate.case_identity.case_id,
                certificate.subject.profile_id,
                certificate.subject.session_id,
                certificate.subject.ownership_generation,
                certificate.version,
                certificate.policy_version,
                certificate.work_graph_digest,
                certificate.certificate_digest,
                status,
                last_error,
                now,
                now,
            ),
        )

    @staticmethod
    def _load_case(
        conn: sqlite3.Connection,
        *,
        certificate: RecoveryCertificate,
    ) -> _RecoveryCaseRow | None:
        row = conn.execute(
            """
            SELECT case_id, status, next_delivery_cycle, delivery_count,
                   last_event_id, latest_certificate_digest, updated_at,
                   last_error,
                   certificate_version, policy_version, work_graph_digest,
                   profile_id, session_id, ownership_generation
            FROM agent_session_recovery_cases
            WHERE profile_id = ?
              AND session_id = ?
              AND ownership_generation = ?
              AND policy_version = ?
              AND work_graph_digest = ?
            """,
            (
                certificate.subject.profile_id,
                certificate.subject.session_id,
                certificate.subject.ownership_generation,
                certificate.policy_version,
                certificate.work_graph_digest,
            ),
        ).fetchone()
        if row is None:
            return None
        if (
            str(row["case_id"]) != certificate.case_identity.case_id
            or int(row["certificate_version"]) != certificate.version
            or str(row["profile_id"]) != certificate.subject.profile_id
            or str(row["session_id"]) != certificate.subject.session_id
            or int(row["ownership_generation"])
            != certificate.subject.ownership_generation
        ):
            raise sqlite3.IntegrityError("recovery case identity conflicts with graph")
        return _RecoveryCaseRow(
            case_id=str(row["case_id"]),
            status=str(row["status"]),
            next_delivery_cycle=_nonnegative_int(
                row["next_delivery_cycle"],
                field_name="case.next_delivery_cycle",
            ),
            delivery_count=_nonnegative_int(
                row["delivery_count"],
                field_name="case.delivery_count",
            ),
            last_event_id=str(row["last_event_id"] or ""),
            latest_certificate_digest=str(row["latest_certificate_digest"]),
            last_error=str(row["last_error"] or ""),
            updated_at=_nonnegative_finite(row["updated_at"], field_name="case.updated_at"),
        )

    def _case_delivery_status(
        self,
        conn: sqlite3.Connection,
        *,
        case: _RecoveryCaseRow,
        certificate: RecoveryCertificate,
    ) -> tuple[RecoveryDeliveryPayload, str] | None:
        delivery = self._reader.load_delivery(
            conn,
            profile_id=certificate.subject.profile_id,
            session_id=certificate.subject.session_id,
            event_id=case.last_event_id,
        )
        if delivery is None:
            return None
        payload, status = delivery
        if (
            payload.case_id != case.case_id
            or payload.delivery_cycle != case.delivery_count - 1
            or payload.certificate.subject != certificate.subject
            or payload.certificate.policy_version != certificate.policy_version
            or payload.certificate.work_graph_digest != certificate.work_graph_digest
            or payload.certificate.certificate_digest != case.latest_certificate_digest
        ):
            raise RecoveryGraphReadError(
                "recovery_delivery_case_identity_conflict",
                evidence={
                    "case_id": case.case_id,
                    "event_id": case.last_event_id,
                },
            )
        return payload, status

    def _insert_or_validate_delivery_mailbox(
        self,
        conn: sqlite3.Connection,
        *,
        payload: RecoveryDeliveryPayload,
        payload_json: str,
        now: float,
    ) -> bool:
        """Insert one exact typed delivery after raw logical-key preflight."""

        if self._reader.validate_delivery_mailbox(
            conn,
            payload=payload,
            payload_json=payload_json,
            now=now,
            allow_missing=True,
        ):
            return False
        certificate = payload.certificate
        inserted = conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation,
                kind, source, occurred_at, payload_json,
                causation_id, correlation_id, trace_id,
                status, attempt_count, available_at, claim_id, lease_owner,
                lease_until, created_at, handled_at, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                      'pending', 0, ?, '', '', NULL, ?, NULL, '')
            """,
            (
                payload.event_id,
                certificate.subject.profile_id,
                certificate.subject.session_id,
                certificate.subject.ownership_generation,
                RECOVERY_DELIVERY_EVENT_KIND,
                RECOVERY_DELIVERY_EVENT_SOURCE,
                now,
                payload_json,
                payload.case_id,
                payload.case_id,
                payload.event_id,
                now,
                now,
            ),
        )
        if inserted.rowcount != 1:
            raise sqlite3.IntegrityError(
                "recovery delivery insert did not create exactly one mailbox row"
            )
        self._reader.validate_delivery_mailbox(
            conn,
            payload=payload,
            payload_json=payload_json,
            now=now,
            allow_missing=False,
        )
        return True

def _required_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized or normalized != value:
        raise ValueError(f"{field_name} must be non-empty canonical text")
    return normalized


def _required_positive_int(value: object, *, field_name: str) -> int:
    if type(value) is not int or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _nonnegative_int(value: object, *, field_name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def _nonnegative_finite(value: object, *, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a finite non-negative number")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"{field_name} must be a finite non-negative number")
    return result


def _next_monotonic_time(previous: float, candidate: float) -> float:
    normalized_previous = _nonnegative_finite(previous, field_name="previous time")
    normalized_candidate = _nonnegative_finite(candidate, field_name="candidate time")
    if normalized_candidate > normalized_previous:
        return normalized_candidate
    return math.nextafter(normalized_previous, math.inf)


def _bounded_reason(reason_codes: Sequence[str]) -> str:
    rendered = ",".join(sorted(set(reason_codes)))
    if not rendered:
        return "recovery_policy_blocked"
    return rendered[:4_096]


def _subject_key(subject: RecoverySubject) -> SessionKey:
    return SessionKey(subject.profile_id, subject.session_id)


__all__ = [
    "MAX_RECOVERY_DELIVERY_CYCLES",
    "MAX_RECOVERY_RAW_FIELD_BYTES",
    "MAX_RECOVERY_SCAN_CANDIDATES",
    "MAX_RECOVERY_SOURCE_ROWS",
    "RecoveryGraphReadError",
    "RecoveryScanDisposition",
    "RecoveryScanResult",
    "RecoveryScanSummary",
    "SQLiteRecoveryGraphScanner",
]
