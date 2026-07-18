"""Dormant persistence primitive for exact Actor v2 mailbox handoff evidence.

This repository deliberately has no runtime registration, scanner, timer, or
wake target.  It records the immutable proof a later typed handoff dispatcher
must consume, and keeps the mutable consumer lease separate from that proof.
"""

from __future__ import annotations

import math
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from sqlite3 import Connection, Row

from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.fenced_wake import (
    FencedMailboxWakeDisposition,
    FencedMailboxWakeRequest,
)
from shinbot.core.dispatch.mailbox_handoff import (
    FencedMailboxHandoffClaim,
    FencedMailboxHandoffReceipt,
    MailboxHandoffEvidence,
    MailboxHandoffEvidenceState,
    MailboxHandoffIdentity,
    MailboxHandoffState,
    MailboxHandoffTarget,
)

from .base import Repository

_MAX_LEASE_SECONDS = 300.0
_MAX_DISCOVERY_LIMIT = 1000


class MailboxHandoffError(RuntimeError):
    """Base error for fail-closed mailbox handoff persistence operations."""


class MailboxHandoffNotFound(MailboxHandoffError):
    """Raised when a requested mailbox row itself does not exist."""


class MailboxHandoffEvidenceUnavailable(MailboxHandoffError):
    """Raised when no immutable fenced evidence is available for a mailbox."""


class MailboxHandoffEvidenceConflict(MailboxHandoffError):
    """Raised when immutable sidecar evidence differs from its mailbox source."""


class MailboxHandoffLeaseLost(MailboxHandoffError):
    """Raised when a target tries to mutate a claim it no longer owns."""


@dataclass(slots=True, frozen=True)
class MailboxHandoffRecord:
    """Typed snapshot of one immutable evidence sidecar and mutable handoff state."""

    handoff_id: str
    evidence: MailboxHandoffEvidence
    state: MailboxHandoffState
    attempt_count: int
    available_at: float
    claim_id: str = ""
    lease_owner: str = ""
    lease_until: float | None = None
    target: MailboxHandoffTarget | None = None
    target_disposition: str = ""
    created_at: float = 0.0
    updated_at: float = 0.0
    claimed_at: float | None = None
    settled_at: float | None = None
    last_error: str = ""

    def __post_init__(self) -> None:
        """Validate a decoded durable handoff snapshot before returning it."""

        handoff_id = _required_identifier(self.handoff_id, "handoff_id")
        if not isinstance(self.evidence, MailboxHandoffEvidence):
            raise TypeError("evidence must be a MailboxHandoffEvidence")
        state = MailboxHandoffState(self.state)
        if isinstance(self.attempt_count, bool) or not isinstance(self.attempt_count, int):
            raise ValueError("attempt_count must be an integer")
        if self.attempt_count < 0:
            raise ValueError("attempt_count must not be negative")
        available_at = _finite_time(self.available_at, "available_at")
        created_at = _finite_time(self.created_at, "created_at")
        updated_at = _finite_time(self.updated_at, "updated_at")
        lease_until = (
            _finite_time(self.lease_until, "lease_until")
            if self.lease_until is not None
            else None
        )
        settled_at = (
            _finite_time(self.settled_at, "settled_at")
            if self.settled_at is not None
            else None
        )
        claimed_at = (
            _finite_time(self.claimed_at, "claimed_at")
            if self.claimed_at is not None
            else None
        )
        claim_id = str(self.claim_id or "").strip()
        lease_owner = str(self.lease_owner or "").strip()
        target_disposition = str(self.target_disposition or "").strip()
        if state is MailboxHandoffState.CLAIMED:
            if (
                not claim_id
                or not lease_owner
                or lease_until is None
                or claimed_at is None
                or self.target is None
            ):
                raise ValueError("claimed mailbox handoff has incomplete lease state")
            if not self.evidence.is_fenced:
                raise ValueError("only fenced handoffs may be claimed")
            if lease_until <= claimed_at or lease_until > claimed_at + _MAX_LEASE_SECONDS:
                raise ValueError("claimed mailbox handoff lease exceeds its bounded interval")
        elif state is MailboxHandoffState.SETTLED:
            if claim_id or lease_owner or lease_until is not None:
                raise ValueError("settled mailbox handoff retains an active lease")
            if self.target is None or not target_disposition or settled_at is None:
                raise ValueError("settled mailbox handoff lacks target receipt state")
            if claimed_at is not None or not self.evidence.is_fenced:
                raise ValueError("settled mailbox handoff has invalid evidence state")
            if target_disposition not in {"accepted", "stale"}:
                raise ValueError("settled mailbox handoff has an unknown target disposition")
        elif state is MailboxHandoffState.PENDING:
            if claim_id or lease_owner or lease_until is not None or self.target is not None:
                raise ValueError("pending mailbox handoff retains active target state")
            if target_disposition or claimed_at is not None or settled_at is not None:
                raise ValueError("pending mailbox handoff retains settlement state")
            if not self.evidence.is_fenced:
                raise ValueError("only fenced handoffs may be pending")
        else:
            if claim_id or lease_owner or lease_until is not None or self.target is not None:
                raise ValueError("blocked mailbox handoff retains active target state")
            if target_disposition or claimed_at is not None or settled_at is not None:
                raise ValueError("blocked mailbox handoff retains settlement state")
            if self.evidence.is_fenced:
                raise ValueError("fenced mailbox handoff cannot be blocked")
        object.__setattr__(self, "handoff_id", handoff_id)
        object.__setattr__(self, "state", state)
        object.__setattr__(self, "available_at", available_at)
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "updated_at", updated_at)
        object.__setattr__(self, "lease_until", lease_until)
        object.__setattr__(self, "claimed_at", claimed_at)
        object.__setattr__(self, "settled_at", settled_at)
        object.__setattr__(self, "claim_id", claim_id)
        object.__setattr__(self, "lease_owner", lease_owner)
        object.__setattr__(self, "target_disposition", target_disposition)
        object.__setattr__(self, "last_error", str(self.last_error or "").strip())


@dataclass(slots=True, frozen=True)
class MailboxHandoffDiscoveryCursor:
    """Stable keyset position for one immutable mailbox handoff row.

    The cursor carries the complete copied identity rather than only a
    ``SessionKey``.  ``mailbox_id`` is the ordering key; the remaining fields
    let the repository fail closed if a caller presents a cursor copied from a
    different sidecar row.
    """

    mailbox_id: int
    handoff_id: str
    profile_id: str
    session_id: str
    event_id: str
    ownership_generation: int
    filter_profile_id: str | None = None
    filter_session_id: str | None = None
    filter_request: FencedMailboxWakeRequest | None = None

    def __post_init__(self) -> None:
        """Validate a cursor before it reaches a keyset SQL predicate."""

        mailbox_id = _positive_mailbox_id(self.mailbox_id)
        handoff_id = _strict_identifier(self.handoff_id, "handoff_id")
        profile_id = _strict_identifier(self.profile_id, "profile_id")
        session_id = _strict_identifier(self.session_id, "session_id")
        event_id = _strict_identifier(self.event_id, "event_id")
        ownership_generation = self.ownership_generation
        if (
            isinstance(ownership_generation, bool)
            or not isinstance(ownership_generation, int)
            or ownership_generation < 0
        ):
            raise ValueError("ownership_generation must be a non-negative integer")
        object.__setattr__(self, "mailbox_id", mailbox_id)
        object.__setattr__(self, "handoff_id", handoff_id)
        object.__setattr__(self, "profile_id", profile_id)
        object.__setattr__(self, "session_id", session_id)
        object.__setattr__(self, "event_id", event_id)
        filter_profile_id = _optional_filter(self.filter_profile_id, "filter_profile_id")
        filter_session_id = _optional_filter(self.filter_session_id, "filter_session_id")
        if filter_profile_id is not None and filter_profile_id != profile_id:
            raise ValueError("filter_profile_id must match the cursor profile_id")
        if filter_session_id is not None and filter_session_id != session_id:
            raise ValueError("filter_session_id must match the cursor session_id")
        filter_request = self.filter_request
        if filter_request is not None:
            if not isinstance(filter_request, FencedMailboxWakeRequest):
                raise TypeError("filter_request must be a FencedMailboxWakeRequest or None")
            if (
                filter_request.key.profile_id != profile_id
                or filter_request.key.session_id != session_id
                or filter_request.ownership_generation != ownership_generation
            ):
                raise ValueError("filter_request must match the cursor owner identity")
        object.__setattr__(self, "filter_profile_id", filter_profile_id)
        object.__setattr__(self, "filter_session_id", filter_session_id)
        object.__setattr__(self, "filter_request", filter_request)

    @classmethod
    def from_record(
        cls,
        record: MailboxHandoffRecord,
        *,
        profile_id: str | None = None,
        session_id: str | None = None,
        expected_request: FencedMailboxWakeRequest | None = None,
    ) -> MailboxHandoffDiscoveryCursor:
        """Build a continuation cursor from one source-validated record."""

        if not isinstance(record, MailboxHandoffRecord):
            raise TypeError("record must be a MailboxHandoffRecord")
        if expected_request is not None:
            if not isinstance(expected_request, FencedMailboxWakeRequest):
                raise TypeError("expected_request must be a FencedMailboxWakeRequest or None")
            if (
                not record.evidence.is_fenced
                or record.evidence.as_fenced_wake_request() != expected_request
            ):
                raise MailboxHandoffEvidenceConflict(
                    "discovery record differs from the expected fenced wake request"
                )
        identity = record.evidence.identity
        return cls(
            mailbox_id=identity.mailbox_id,
            handoff_id=record.handoff_id,
            profile_id=identity.key.profile_id,
            session_id=identity.key.session_id,
            event_id=identity.event_id,
            ownership_generation=identity.ownership_generation,
            filter_profile_id=profile_id,
            filter_session_id=session_id,
            filter_request=expected_request,
        )


@dataclass(slots=True, frozen=True)
class MailboxHandoffDiscoveryPage:
    """Bounded page of source-validated pending fenced handoff records."""

    records: tuple[MailboxHandoffRecord, ...]
    next_cursor: MailboxHandoffDiscoveryCursor | None
    has_more: bool

    def __post_init__(self) -> None:
        """Keep page contents immutable and cursor semantics explicit."""

        if not isinstance(self.records, tuple):
            raise TypeError("records must be a tuple")
        if any(not isinstance(record, MailboxHandoffRecord) for record in self.records):
            raise TypeError("records must contain MailboxHandoffRecord values")
        if self.next_cursor is not None and not isinstance(
            self.next_cursor,
            MailboxHandoffDiscoveryCursor,
        ):
            raise TypeError("next_cursor must be a MailboxHandoffDiscoveryCursor or None")
        if not isinstance(self.has_more, bool):
            raise TypeError("has_more must be a bool")
        if self.has_more and self.next_cursor is None:
            raise ValueError("a page with more records requires a next cursor")


class ActorV2MailboxHandoffRepository(Repository):
    """Record and lease immutable handoffs without activating Actor v2.

    A fenced handoff is accepted only while the mailbox identity, active Actor
    ownership generation, and committed admission fence all match in the same
    SQLite transaction.  Existing mailbox rows are never upgraded from
    ``unknown`` by consulting a later owner.
    """

    def __init__(
        self,
        db: object,
        *,
        clock: Callable[[], float] | None = None,
        handoff_id_factory: Callable[[], str] | None = None,
        claim_id_factory: Callable[[], str] | None = None,
        lease_seconds: float = 30.0,
    ) -> None:
        """Initialize the dormant handoff store with bounded local leases."""

        super().__init__(db)
        self._clock = clock or time.time
        self._handoff_id_factory = handoff_id_factory or (lambda: uuid.uuid4().hex)
        self._claim_id_factory = claim_id_factory or (lambda: uuid.uuid4().hex)
        self._lease_seconds = _bounded_lease_seconds(lease_seconds)

    @property
    def persistence_domain(self) -> object:
        """Return the exact database domain that owns sidecar state."""

        return self._db

    def read(self, mailbox_id: int) -> MailboxHandoffRecord | None:
        """Read one sidecar after verifying its copied mailbox identity exactly."""

        normalized_mailbox_id = _positive_mailbox_id(mailbox_id)
        with self.connect() as conn:
            row = self._select_handoff(conn, normalized_mailbox_id)
            if row is None:
                return None
            return self._validated_record_from_row(conn, row)

    def discover_fenced_pending(
        self,
        *,
        limit: int = 100,
        after: MailboxHandoffDiscoveryCursor | None = None,
        profile_id: str | None = None,
        session_id: str | None = None,
        expected_request: FencedMailboxWakeRequest | None = None,
    ) -> MailboxHandoffDiscoveryPage:
        """Discover a bounded keyset page of pending fenced handoffs.

        Rows are ordered by the immutable ``mailbox_id`` primary key.  The
        query deliberately excludes claimed, settled, blocked, unknown, and
        legacy evidence; a target that needs to retry an expired lease must use
        the explicit claim API for that one mailbox instead of broad discovery.
        Every returned SQL row is decoded through
        :meth:`_validated_record_from_row` before it leaves this repository.

        Args:
            limit: Maximum number of records in the page (1 through 1000).
            after: Immutable keyset cursor from a prior page.
            profile_id: Optional exact profile filter.
            session_id: Optional exact session filter.
            expected_request: Optional complete ownership and admission-fence
                scope. When provided, discovery cannot return a sidecar for a
                different owner incarnation in the same session.

        Raises:
            MailboxHandoffEvidenceConflict: If a cursor or returned sidecar
                fails immutable source validation.
            TypeError: If a cursor or filter has the wrong type.
            ValueError: If pagination input is outside the supported bounds.
        """

        _validate_discovery_limit(limit)
        normalized_profile_id = _optional_filter(profile_id, "profile_id")
        normalized_session_id = _optional_filter(session_id, "session_id")
        if expected_request is not None and not isinstance(
            expected_request,
            FencedMailboxWakeRequest,
        ):
            raise TypeError("expected_request must be a FencedMailboxWakeRequest or None")
        if expected_request is not None:
            if (
                normalized_profile_id is not None
                and normalized_profile_id != expected_request.key.profile_id
            ):
                raise ValueError("profile_id differs from expected_request")
            if (
                normalized_session_id is not None
                and normalized_session_id != expected_request.key.session_id
            ):
                raise ValueError("session_id differs from expected_request")
            normalized_profile_id = expected_request.key.profile_id
            normalized_session_id = expected_request.key.session_id
        if after is not None and not isinstance(after, MailboxHandoffDiscoveryCursor):
            raise TypeError("after must be a MailboxHandoffDiscoveryCursor or None")
        if after is not None:
            if normalized_profile_id != after.filter_profile_id:
                raise ValueError("profile_id filter differs from the keyset cursor")
            if normalized_session_id != after.filter_session_id:
                raise ValueError("session_id filter differs from the keyset cursor")
            if expected_request != after.filter_request:
                raise ValueError("expected_request filter differs from the keyset cursor")

        clauses = ["evidence_state = 'fenced'", "state = 'pending'"]
        params: list[object] = []
        if normalized_profile_id is not None:
            clauses.append("profile_id = ?")
            params.append(normalized_profile_id)
        if normalized_session_id is not None:
            clauses.append("session_id = ?")
            params.append(normalized_session_id)
        if expected_request is not None:
            clauses.extend(
                (
                    "ownership_generation = ?",
                    "admission_fence_id = ?",
                    "admission_fence_generation = ?",
                )
            )
            params.extend(
                (
                    expected_request.ownership_generation,
                    expected_request.admission_fence_id,
                    expected_request.admission_fence_generation,
                )
            )
        if after is not None:
            clauses.append("mailbox_id > ?")
            params.append(after.mailbox_id)

        with self.connect() as conn:
            if after is not None:
                self._validate_discovery_cursor(conn, after)
            rows = conn.execute(
                f"""
                SELECT *
                FROM agent_session_mailbox_handoffs
                WHERE {' AND '.join(clauses)}
                ORDER BY mailbox_id ASC
                LIMIT ?
                """,
                (*params, limit + 1),
            ).fetchall()
            has_more = len(rows) > limit
            page_rows = rows[:limit]
            records = tuple(
                self._validated_record_from_row(conn, row) for row in page_rows
            )

        next_cursor = (
            MailboxHandoffDiscoveryCursor.from_record(
                records[-1],
                profile_id=normalized_profile_id,
                session_id=normalized_session_id,
                expected_request=expected_request,
            )
            if records
            else None
        )
        return MailboxHandoffDiscoveryPage(
            records=records,
            next_cursor=next_cursor,
            has_more=has_more,
        )

    def read_evidence(self, mailbox_id: int) -> MailboxHandoffEvidence:
        """Return durable evidence, treating a missing sidecar as untrusted unknown.

        The returned ``UNKNOWN`` view is intentionally not persisted or upgraded
        from current ownership.  It is useful for diagnostics only; callers that
        require a typed wake request must use :meth:`require_fenced_evidence`.
        """

        normalized_mailbox_id = _positive_mailbox_id(mailbox_id)
        with self.connect() as conn:
            row = self._select_handoff(conn, normalized_mailbox_id)
            if row is None:
                return MailboxHandoffEvidence(
                    identity=self._require_mailbox_identity(conn, normalized_mailbox_id),
                    state=MailboxHandoffEvidenceState.UNKNOWN,
                )
            return self._validated_record_from_row(conn, row).evidence

    def require_fenced_evidence(
        self,
        mailbox_id: int,
        *,
        expected_request: FencedMailboxWakeRequest | None = None,
    ) -> MailboxHandoffEvidence:
        """Require immutable exact fenced evidence or fail closed.

        This intentionally does not consult current ownership to promote a
        missing, unknown, or legacy sidecar row.
        """

        normalized_mailbox_id = _positive_mailbox_id(mailbox_id)
        with self.connect() as conn:
            return self.require_fenced_evidence_in_transaction(
                conn,
                normalized_mailbox_id,
                expected_request=expected_request,
            )

    def require_fenced_evidence_in_transaction(
        self,
        conn: Connection,
        mailbox_id: int,
        *,
        expected_request: FencedMailboxWakeRequest | None = None,
    ) -> MailboxHandoffEvidence:
        """Require exact fenced evidence while a caller owns the transaction."""

        normalized_mailbox_id = _positive_mailbox_id(mailbox_id)
        row = self._select_handoff(conn, normalized_mailbox_id)
        if row is None:
            raise MailboxHandoffEvidenceUnavailable(
                f"mailbox {normalized_mailbox_id} has no handoff sidecar"
            )
        record = self._validated_record_from_row(conn, row)
        evidence = record.evidence
        if not evidence.is_fenced:
            raise MailboxHandoffEvidenceUnavailable(
                f"mailbox {normalized_mailbox_id} has {evidence.state.value} handoff evidence"
            )
        request = evidence.as_fenced_wake_request()
        if expected_request is not None:
            if not isinstance(expected_request, FencedMailboxWakeRequest):
                raise TypeError("expected_request must be a FencedMailboxWakeRequest")
            if request != expected_request:
                raise MailboxHandoffEvidenceConflict(
                    "fenced handoff evidence differs from the expected wake request"
                )
        return evidence

    def record_fenced_handoff(
        self,
        mailbox_id: int,
        request: FencedMailboxWakeRequest,
    ) -> MailboxHandoffRecord:
        """Atomically record canonical evidence for one newly fenced mailbox event."""

        normalized_mailbox_id = _positive_mailbox_id(mailbox_id)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            return self.record_fenced_handoff_in_transaction(
                conn,
                normalized_mailbox_id,
                request,
            )

    def record_fenced_handoff_in_transaction(
        self,
        conn: Connection,
        mailbox_id: int,
        request: FencedMailboxWakeRequest,
    ) -> MailboxHandoffRecord:
        """Record one fenced handoff inside a producer's candidate transaction.

        New evidence receives both a pre-write and final exact ownership/fence
        gate.  An idempotent replay only re-reads existing immutable evidence;
        it does not reject durable debt merely because the future target has
        since become stale.
        """

        normalized_mailbox_id = _positive_mailbox_id(mailbox_id)
        if not isinstance(request, FencedMailboxWakeRequest):
            raise TypeError("request must be a FencedMailboxWakeRequest")
        if not request.has_admission_fence:
            raise ValueError("fenced mailbox handoff requires an admission fence")
        now = _finite_time(self._clock(), "clock")
        conn.execute("SAVEPOINT actor_v2_mailbox_handoff_record")
        try:
            source = self._require_mailbox_identity(conn, normalized_mailbox_id)
            self._validate_request_matches_identity(request, source)
            existing_row = self._select_handoff(conn, normalized_mailbox_id)
            if existing_row is not None:
                existing = self._validated_record_from_row(conn, existing_row)
                if existing.evidence.state is not MailboxHandoffEvidenceState.FENCED:
                    raise MailboxHandoffEvidenceUnavailable(
                        "existing mailbox handoff evidence cannot be upgraded to fenced"
                    )
                if existing.evidence.as_fenced_wake_request() != request:
                    raise MailboxHandoffEvidenceConflict(
                        "existing mailbox handoff fence identity differs from request"
                    )
                result = existing
            else:
                self._require_current_fenced_owner(conn, request)
                handoff_id = _required_identifier(self._handoff_id_factory(), "handoff_id")
                conn.execute(
                    """
                    INSERT INTO agent_session_mailbox_handoffs (
                        mailbox_id, handoff_id,
                        profile_id, session_id, event_id, ownership_generation,
                        evidence_state, admission_fence_id, admission_fence_generation,
                        state, attempt_count, available_at,
                        claim_id, lease_owner, lease_until,
                        target_id, target_incarnation_id, target_disposition,
                        created_at, updated_at, claimed_at, settled_at, last_error
                    ) VALUES (?, ?, ?, ?, ?, ?, 'fenced', ?, ?,
                              'pending', 0, ?, '', '', NULL, '', '', '', ?, ?, NULL, NULL, '')
                    """,
                    (
                        source.mailbox_id,
                        handoff_id,
                        source.key.profile_id,
                        source.key.session_id,
                        source.event_id,
                        source.ownership_generation,
                        request.admission_fence_id,
                        request.admission_fence_generation,
                        now,
                        now,
                        now,
                    ),
                )
                # Re-check after all candidate writes.  A test or future
                # trigger may mutate ownership/fence state inside this savepoint.
                self._require_current_fenced_owner(conn, request)
                inserted = self._select_handoff(conn, normalized_mailbox_id)
                if inserted is None:
                    raise MailboxHandoffEvidenceConflict(
                        "new fenced mailbox handoff disappeared before commit"
                    )
                result = self._validated_record_from_row(conn, inserted)
            conn.execute("RELEASE SAVEPOINT actor_v2_mailbox_handoff_record")
            return result
        except BaseException:
            conn.execute("ROLLBACK TO SAVEPOINT actor_v2_mailbox_handoff_record")
            conn.execute("RELEASE SAVEPOINT actor_v2_mailbox_handoff_record")
            raise

    def record_unfenced_legacy_handoff(self, mailbox_id: int) -> MailboxHandoffRecord:
        """Explicitly mark a new legacy handoff as blocked and non-projectable.

        This is a migration/audit primitive only.  It cannot be claimed through
        the typed Actor v2 wake path and deliberately carries blank fence fields.
        """

        normalized_mailbox_id = _positive_mailbox_id(mailbox_id)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            return self.record_unfenced_legacy_handoff_in_transaction(
                conn,
                normalized_mailbox_id,
            )

    def record_unfenced_legacy_handoff_in_transaction(
        self,
        conn: Connection,
        mailbox_id: int,
    ) -> MailboxHandoffRecord:
        """Record explicit non-fenced legacy evidence in a producer transaction.

        This boundary only copies the mailbox's immutable identity.  It does
        not query current ownership or an admission fence, so callers cannot
        retroactively promote historical legacy work to a fenced handoff.
        """

        normalized_mailbox_id = _positive_mailbox_id(mailbox_id)
        now = _finite_time(self._clock(), "clock")
        conn.execute("SAVEPOINT actor_v2_mailbox_handoff_legacy_record")
        try:
            source = self._require_mailbox_identity(conn, normalized_mailbox_id)
            existing = self._select_handoff(conn, normalized_mailbox_id)
            if existing is not None:
                record = self._validated_record_from_row(conn, existing)
                if record.evidence.state is not MailboxHandoffEvidenceState.UNFENCED_LEGACY:
                    raise MailboxHandoffEvidenceConflict(
                        "existing mailbox handoff evidence differs from explicit legacy evidence"
                    )
                result = record
            else:
                handoff_id = _required_identifier(self._handoff_id_factory(), "handoff_id")
                conn.execute(
                    """
                    INSERT INTO agent_session_mailbox_handoffs (
                        mailbox_id, handoff_id,
                        profile_id, session_id, event_id, ownership_generation,
                        evidence_state, admission_fence_id, admission_fence_generation,
                        state, attempt_count, available_at,
                        claim_id, lease_owner, lease_until,
                        target_id, target_incarnation_id, target_disposition,
                        created_at, updated_at, claimed_at, settled_at, last_error
                    ) VALUES (?, ?, ?, ?, ?, ?, 'unfenced_legacy', '', 0,
                            'blocked', 0, ?, '', '', NULL, '', '', '', ?, ?, NULL, NULL, '')
                    """,
                    (
                        source.mailbox_id,
                        handoff_id,
                        source.key.profile_id,
                        source.key.session_id,
                        source.event_id,
                        source.ownership_generation,
                        now,
                        now,
                        now,
                    ),
                )
                row = self._select_handoff(conn, normalized_mailbox_id)
                if row is None:
                    raise MailboxHandoffEvidenceConflict("legacy mailbox handoff disappeared")
                result = self._validated_record_from_row(conn, row)
            conn.execute("RELEASE SAVEPOINT actor_v2_mailbox_handoff_legacy_record")
            return result
        except BaseException:
            conn.execute("ROLLBACK TO SAVEPOINT actor_v2_mailbox_handoff_legacy_record")
            conn.execute("RELEASE SAVEPOINT actor_v2_mailbox_handoff_legacy_record")
            raise

    def claim_fenced_handoff(
        self,
        mailbox_id: int,
        *,
        worker_id: str,
        target: MailboxHandoffTarget,
    ) -> FencedMailboxHandoffClaim | None:
        """Claim one pending or expired fenced handoff for a bounded target lease."""

        normalized_mailbox_id = _positive_mailbox_id(mailbox_id)
        worker = _required_identifier(worker_id, "worker_id")
        if not isinstance(target, MailboxHandoffTarget):
            raise TypeError("target must be a MailboxHandoffTarget")
        now = _finite_time(self._clock(), "clock")
        lease_expires_at = self._lease_expiry(now)
        claim_id = _required_identifier(self._claim_id_factory(), "claim_id")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = self._select_handoff(conn, normalized_mailbox_id)
            if row is None:
                raise MailboxHandoffEvidenceUnavailable(
                    f"mailbox {normalized_mailbox_id} has no handoff sidecar"
                )
            record = self._validated_record_from_row(conn, row)
            evidence = record.evidence
            if not evidence.is_fenced:
                raise MailboxHandoffEvidenceUnavailable(
                    f"mailbox {normalized_mailbox_id} has {evidence.state.value} handoff evidence"
                )
            if record.state is MailboxHandoffState.SETTLED:
                return None
            if record.state is MailboxHandoffState.BLOCKED:
                raise MailboxHandoffEvidenceUnavailable("fenced handoff is unexpectedly blocked")
            claimable = record.state is MailboxHandoffState.PENDING and record.available_at <= now
            expired = (
                record.state is MailboxHandoffState.CLAIMED
                and record.lease_until is not None
                and record.lease_until <= now
            )
            if not claimable and not expired:
                return None
            updated = conn.execute(
                """
                UPDATE agent_session_mailbox_handoffs
                SET state = 'claimed',
                    attempt_count = attempt_count + 1,
                    claim_id = ?,
                    lease_owner = ?,
                    lease_until = ?,
                    target_id = ?,
                    target_incarnation_id = ?,
                    target_disposition = '',
                    updated_at = ?,
                    claimed_at = ?,
                    settled_at = NULL,
                    last_error = ''
                WHERE mailbox_id = ?
                  AND (
                      (state = 'pending' AND available_at <= ?)
                      OR (state = 'claimed' AND COALESCE(lease_until, 0) <= ?)
                  )
                """,
                (
                    claim_id,
                    worker,
                    lease_expires_at,
                    target.target_id,
                    target.incarnation_id,
                    now,
                    now,
                    normalized_mailbox_id,
                    now,
                    now,
                ),
            )
            if updated.rowcount != 1:
                return None
            claimed_row = self._select_handoff(conn, normalized_mailbox_id)
            if claimed_row is None:
                raise MailboxHandoffEvidenceConflict("claimed mailbox handoff disappeared")
            claimed_record = self._validated_record_from_row(conn, claimed_row)
            return FencedMailboxHandoffClaim(
                handoff_id=claimed_record.handoff_id,
                evidence=claimed_record.evidence,
                claim_id=claim_id,
                worker_id=worker,
                target=target,
                attempt_count=claimed_record.attempt_count,
                claimed_at=now,
                lease_expires_at=lease_expires_at,
            )

    def renew_fenced_claim(
        self,
        claim: FencedMailboxHandoffClaim,
    ) -> FencedMailboxHandoffClaim:
        """Renew one live claim without extending it beyond the bounded lease cap."""

        if not isinstance(claim, FencedMailboxHandoffClaim):
            raise TypeError("claim must be a FencedMailboxHandoffClaim")
        now = _finite_time(self._clock(), "clock")
        lease_expires_at = self._lease_expiry(now)
        mailbox_id = claim.evidence.identity.mailbox_id
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self.require_fenced_evidence_in_transaction(
                conn,
                mailbox_id,
                expected_request=claim.request,
            )
            updated = conn.execute(
                """
                UPDATE agent_session_mailbox_handoffs
                SET lease_until = ?, updated_at = ?, claimed_at = ?
                WHERE mailbox_id = ?
                  AND state = 'claimed'
                  AND handoff_id = ?
                  AND claim_id = ?
                  AND lease_owner = ?
                  AND target_id = ?
                  AND target_incarnation_id = ?
                  AND COALESCE(lease_until, 0) > ?
                """,
                (
                    lease_expires_at,
                    now,
                    now,
                    mailbox_id,
                    claim.handoff_id,
                    claim.claim_id,
                    claim.worker_id,
                    claim.target.target_id,
                    claim.target.incarnation_id,
                    now,
                ),
            )
            if updated.rowcount != 1:
                raise MailboxHandoffLeaseLost("mailbox handoff lease is expired or no longer owned")
        return FencedMailboxHandoffClaim(
            handoff_id=claim.handoff_id,
            evidence=claim.evidence,
            claim_id=claim.claim_id,
            worker_id=claim.worker_id,
            target=claim.target,
            attempt_count=claim.attempt_count,
            claimed_at=now,
            lease_expires_at=lease_expires_at,
        )

    def require_live_fenced_claim_in_transaction(
        self,
        conn: Connection,
        claim: FencedMailboxHandoffClaim,
        *,
        now: float | None = None,
    ) -> FencedMailboxHandoffClaim:
        """Require one exact unexpired handoff claim in a caller transaction.

        This is the target-side acceptance primitive.  It deliberately checks
        the complete mailbox identity, claim epoch, worker, and target rather
        than treating a matching ``FencedMailboxWakeRequest`` as permission to
        consume any event for that session.  A renewed claim returns its latest
        durable timestamps while retaining the same immutable claim identity.
        """

        if not isinstance(claim, FencedMailboxHandoffClaim):
            raise TypeError("claim must be a FencedMailboxHandoffClaim")
        observed_at = _finite_time(self._clock() if now is None else now, "clock")
        mailbox_id = claim.evidence.identity.mailbox_id
        row = self._select_handoff(conn, mailbox_id)
        if row is None:
            raise MailboxHandoffEvidenceUnavailable(
                f"mailbox {mailbox_id} has no handoff sidecar"
            )
        record = self._validated_record_from_row(conn, row)
        if record.evidence != claim.evidence:
            raise MailboxHandoffEvidenceConflict(
                "live handoff evidence differs from the presented claim"
            )
        if record.state is not MailboxHandoffState.CLAIMED:
            raise MailboxHandoffLeaseLost("mailbox handoff is no longer claimed")
        if (
            record.handoff_id != claim.handoff_id
            or record.claim_id != claim.claim_id
            or record.lease_owner != claim.worker_id
            or record.target != claim.target
            or record.attempt_count != claim.attempt_count
        ):
            raise MailboxHandoffLeaseLost(
                "mailbox handoff no longer belongs to this exact claim"
            )
        if record.lease_until is None or record.claimed_at is None:
            raise MailboxHandoffEvidenceConflict(
                "claimed mailbox handoff lacks a live lease boundary"
            )
        if record.lease_until <= observed_at:
            raise MailboxHandoffLeaseLost("mailbox handoff claim has expired")
        if record.target is None:
            raise MailboxHandoffEvidenceConflict(
                "claimed mailbox handoff lacks a target identity"
            )
        return FencedMailboxHandoffClaim(
            handoff_id=record.handoff_id,
            evidence=record.evidence,
            claim_id=record.claim_id,
            worker_id=record.lease_owner,
            target=record.target,
            attempt_count=record.attempt_count,
            claimed_at=record.claimed_at,
            lease_expires_at=record.lease_until,
        )

    def release_fenced_claim(
        self,
        claim: FencedMailboxHandoffClaim,
        *,
        retry_at: float | None = None,
        error_message: str = "",
    ) -> MailboxHandoffRecord:
        """Release one live fenced claim back to pending without altering evidence."""

        if not isinstance(claim, FencedMailboxHandoffClaim):
            raise TypeError("claim must be a FencedMailboxHandoffClaim")
        now = _finite_time(self._clock(), "clock")
        available_at = now if retry_at is None else _finite_time(retry_at, "retry_at")
        if available_at < now:
            raise ValueError("retry_at must not be in the past")
        mailbox_id = claim.evidence.identity.mailbox_id
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self.require_fenced_evidence_in_transaction(
                conn,
                mailbox_id,
                expected_request=claim.request,
            )
            updated = conn.execute(
                """
                UPDATE agent_session_mailbox_handoffs
                SET state = 'pending',
                    available_at = ?,
                    claim_id = '',
                    lease_owner = '',
                    lease_until = NULL,
                    target_id = '',
                    target_incarnation_id = '',
                    target_disposition = '',
                    updated_at = ?,
                    claimed_at = NULL,
                    settled_at = NULL,
                    last_error = ?
                WHERE mailbox_id = ?
                  AND state = 'claimed'
                  AND handoff_id = ?
                  AND claim_id = ?
                  AND lease_owner = ?
                  AND target_id = ?
                  AND target_incarnation_id = ?
                  AND COALESCE(lease_until, 0) > ?
                """,
                (
                    available_at,
                    now,
                    str(error_message or "").strip(),
                    mailbox_id,
                    claim.handoff_id,
                    claim.claim_id,
                    claim.worker_id,
                    claim.target.target_id,
                    claim.target.incarnation_id,
                    now,
                ),
            )
            if updated.rowcount != 1:
                raise MailboxHandoffLeaseLost("mailbox handoff lease is expired or no longer owned")
            row = self._select_handoff(conn, mailbox_id)
            if row is None:
                raise MailboxHandoffEvidenceConflict("released mailbox handoff disappeared")
            return self._validated_record_from_row(conn, row)

    def defer_fenced_claim(
        self,
        receipt: FencedMailboxHandoffReceipt,
        *,
        retry_at: float | None = None,
    ) -> MailboxHandoffRecord:
        """Return one target-deferred claim to pending without settling it.

        A typed ``DEFERRED`` receipt proves only that the target did not accept
        this delivery attempt. It is not evidence that the immutable owner
        request is stale, so it must never enter the terminal settled state.
        This method keeps the receipt shape at the persistence boundary and
        reuses the exact-claim release checks for the mutable transition.
        """

        if not isinstance(receipt, FencedMailboxHandoffReceipt):
            raise TypeError("receipt must be a FencedMailboxHandoffReceipt")
        if receipt.disposition is not FencedMailboxWakeDisposition.DEFERRED:
            raise ValueError("only a deferred handoff receipt may release a claim")
        return self.release_fenced_claim(
            receipt.claim,
            retry_at=retry_at,
            error_message="target deferred fenced mailbox handoff",
        )

    def settle_fenced_claim(
        self,
        receipt: FencedMailboxHandoffReceipt,
    ) -> MailboxHandoffRecord:
        """Terminally settle a live claim using a target-bound typed receipt."""

        if not isinstance(receipt, FencedMailboxHandoffReceipt):
            raise TypeError("receipt must be a FencedMailboxHandoffReceipt")
        if receipt.disposition not in {
            FencedMailboxWakeDisposition.ACCEPTED,
            FencedMailboxWakeDisposition.STALE,
        }:
            raise ValueError("only terminal handoff receipts may settle a claim")
        claim = receipt.claim
        now = _finite_time(self._clock(), "clock")
        mailbox_id = claim.evidence.identity.mailbox_id
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self.require_fenced_evidence_in_transaction(
                conn,
                mailbox_id,
                expected_request=claim.request,
            )
            updated = conn.execute(
                """
                UPDATE agent_session_mailbox_handoffs
                SET state = 'settled',
                    claim_id = '',
                    lease_owner = '',
                    lease_until = NULL,
                    target_disposition = ?,
                    updated_at = ?,
                    claimed_at = NULL,
                    settled_at = ?,
                    last_error = ''
                WHERE mailbox_id = ?
                  AND state = 'claimed'
                  AND handoff_id = ?
                  AND claim_id = ?
                  AND lease_owner = ?
                  AND target_id = ?
                  AND target_incarnation_id = ?
                  AND COALESCE(lease_until, 0) > ?
                """,
                (
                    receipt.disposition.value,
                    now,
                    now,
                    mailbox_id,
                    claim.handoff_id,
                    claim.claim_id,
                    claim.worker_id,
                    claim.target.target_id,
                    claim.target.incarnation_id,
                    now,
                ),
            )
            if updated.rowcount != 1:
                raise MailboxHandoffLeaseLost("mailbox handoff lease is expired or no longer owned")
            row = self._select_handoff(conn, mailbox_id)
            if row is None:
                raise MailboxHandoffEvidenceConflict("settled mailbox handoff disappeared")
            return self._validated_record_from_row(conn, row)

    def _require_current_fenced_owner(
        self,
        conn: Connection,
        request: FencedMailboxWakeRequest,
    ) -> None:
        """Use the exact request as the new-evidence admission proof only."""

        self._db.agent_runtime_ownership.require_actor_v2_in_transaction(
            conn,
            request.key,
            expected_generation=request.ownership_generation,
            expected_admission_fence_id=request.admission_fence_id,
            expected_admission_fence_generation=request.admission_fence_generation,
        )

    def _lease_expiry(self, now: float) -> float:
        """Calculate one representable bounded lease deadline from the local clock."""

        lease_expires_at = _finite_time(now + self._lease_seconds, "lease_expires_at")
        if lease_expires_at <= now:
            raise ValueError("lease_seconds is too small to advance the current clock value")
        return lease_expires_at

    def _require_mailbox_identity(
        self,
        conn: Connection,
        mailbox_id: int,
    ) -> MailboxHandoffIdentity:
        """Read one source mailbox identity before copying it into the sidecar."""

        row = conn.execute(
            """
            SELECT mailbox_id, event_id, profile_id, session_id, ownership_generation
            FROM agent_session_mailbox
            WHERE mailbox_id = ?
            """,
            (mailbox_id,),
        ).fetchone()
        if row is None:
            raise MailboxHandoffNotFound(f"mailbox {mailbox_id} does not exist")
        try:
            return _identity_from_mailbox_row(row)
        except (TypeError, ValueError) as exc:
            raise MailboxHandoffEvidenceConflict(
                f"mailbox {mailbox_id} has an invalid immutable identity"
            ) from exc

    def _validated_record_from_row(
        self,
        conn: Connection,
        row: Row,
    ) -> MailboxHandoffRecord:
        """Decode a sidecar only after its copied source identity still matches."""

        try:
            source = self._require_mailbox_identity(conn, int(row["mailbox_id"]))
            evidence = _evidence_from_handoff_row(row)
        except (TypeError, ValueError) as exc:
            raise MailboxHandoffEvidenceConflict("mailbox handoff contains invalid durable data") from exc
        if evidence.identity != source:
            raise MailboxHandoffEvidenceConflict(
                "mailbox handoff immutable identity no longer matches its source mailbox"
            )
        try:
            return _record_from_row(row, evidence)
        except (TypeError, ValueError) as exc:
            raise MailboxHandoffEvidenceConflict("mailbox handoff has invalid mutable state") from exc

    def _validate_discovery_cursor(
        self,
        conn: Connection,
        cursor: MailboxHandoffDiscoveryCursor,
    ) -> None:
        """Verify that a continuation cursor still names its immutable row."""

        row = self._select_handoff(conn, cursor.mailbox_id)
        if row is None:
            raise MailboxHandoffEvidenceConflict(
                f"discovery cursor mailbox {cursor.mailbox_id} no longer exists"
            )
        record = self._validated_record_from_row(conn, row)
        current = MailboxHandoffDiscoveryCursor.from_record(
            record,
            profile_id=cursor.filter_profile_id,
            session_id=cursor.filter_session_id,
            expected_request=cursor.filter_request,
        )
        if current != cursor:
            raise MailboxHandoffEvidenceConflict(
                "discovery cursor immutable identity differs from its sidecar row"
            )

    @staticmethod
    def _select_handoff(conn: Connection, mailbox_id: int) -> Row | None:
        """Read the one-to-one sidecar row by its source mailbox primary key."""

        return conn.execute(
            """
            SELECT *
            FROM agent_session_mailbox_handoffs
            WHERE mailbox_id = ?
            """,
            (mailbox_id,),
        ).fetchone()

    @staticmethod
    def _validate_request_matches_identity(
        request: FencedMailboxWakeRequest,
        identity: MailboxHandoffIdentity,
    ) -> None:
        """Reject a request copied from a different mailbox owner incarnation."""

        if request.key != identity.key or request.ownership_generation != identity.ownership_generation:
            raise MailboxHandoffEvidenceConflict(
                "fenced wake request does not match mailbox key and ownership generation"
            )


def _identity_from_mailbox_row(row: Row) -> MailboxHandoffIdentity:
    """Decode mailbox columns using the same immutable identity carried by the sidecar."""

    return MailboxHandoffIdentity(
        mailbox_id=int(row["mailbox_id"]),
        event_id=str(row["event_id"]),
        key=SessionKey(
            str(row["profile_id"]),
            str(row["session_id"]),
        ),
        ownership_generation=int(row["ownership_generation"]),
    )


def _evidence_from_handoff_row(row: Row) -> MailboxHandoffEvidence:
    """Decode immutable handoff evidence without consulting current ownership."""

    identity = MailboxHandoffIdentity(
        mailbox_id=int(row["mailbox_id"]),
        event_id=str(row["event_id"]),
        key=SessionKey(
            str(row["profile_id"]),
            str(row["session_id"]),
        ),
        ownership_generation=int(row["ownership_generation"]),
    )
    return MailboxHandoffEvidence(
        identity=identity,
        state=MailboxHandoffEvidenceState(str(row["evidence_state"])),
        admission_fence_id=str(row["admission_fence_id"]),
        admission_fence_generation=int(row["admission_fence_generation"]),
    )


def _record_from_row(row: Row, evidence: MailboxHandoffEvidence) -> MailboxHandoffRecord:
    """Decode one already source-validated sidecar record."""

    target_id = str(row["target_id"] or "").strip()
    target_incarnation_id = str(row["target_incarnation_id"] or "").strip()
    target = (
        MailboxHandoffTarget(target_id=target_id, incarnation_id=target_incarnation_id)
        if target_id or target_incarnation_id
        else None
    )
    return MailboxHandoffRecord(
        handoff_id=str(row["handoff_id"]),
        evidence=evidence,
        state=MailboxHandoffState(str(row["state"])),
        attempt_count=int(row["attempt_count"]),
        available_at=float(row["available_at"]),
        claim_id=str(row["claim_id"]),
        lease_owner=str(row["lease_owner"]),
        lease_until=(float(row["lease_until"]) if row["lease_until"] is not None else None),
        target=target,
        target_disposition=str(row["target_disposition"]),
        created_at=float(row["created_at"]),
        updated_at=float(row["updated_at"]),
        claimed_at=(float(row["claimed_at"]) if row["claimed_at"] is not None else None),
        settled_at=(float(row["settled_at"]) if row["settled_at"] is not None else None),
        last_error=str(row["last_error"]),
    )


def _positive_mailbox_id(value: object) -> int:
    """Validate the integer primary key used as the sidecar's one-to-one link."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("mailbox_id must be a positive integer")
    return value


def _required_identifier(value: object, field_name: str) -> str:
    """Normalize one opaque non-empty durable identifier."""

    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _strict_identifier(value: object, field_name: str) -> str:
    """Require a caller-provided cursor identity to be non-empty text."""

    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    return _required_identifier(value, field_name)


def _finite_time(value: object, field_name: str) -> float:
    """Validate finite repository clock and deadline values."""

    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be finite")
    numeric = float(value)
    if not math.isfinite(numeric):
        raise ValueError(f"{field_name} must be finite")
    return numeric


def _bounded_lease_seconds(value: object) -> float:
    """Require every local handoff lease to fit the schema's hard upper bound."""

    seconds = _finite_time(value, "lease_seconds")
    if seconds <= 0 or seconds > _MAX_LEASE_SECONDS:
        raise ValueError(
            f"lease_seconds must be positive and at most {_MAX_LEASE_SECONDS:g} seconds"
        )
    return seconds


def _validate_discovery_limit(value: object) -> int:
    """Require a bounded positive page size for handoff discovery."""

    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < 1
        or value > _MAX_DISCOVERY_LIMIT
    ):
        raise ValueError(
            f"limit must be an integer between 1 and {_MAX_DISCOVERY_LIMIT}"
        )
    return value


def _optional_filter(value: object, field_name: str) -> str | None:
    """Validate one optional exact-text discovery filter."""

    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string or None")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty when provided")
    return normalized


__all__ = [
    "ActorV2MailboxHandoffRepository",
    "FencedMailboxHandoffClaim",
    "FencedMailboxHandoffReceipt",
    "MailboxHandoffError",
    "MailboxHandoffEvidence",
    "MailboxHandoffEvidenceConflict",
    "MailboxHandoffEvidenceState",
    "MailboxHandoffEvidenceUnavailable",
    "MailboxHandoffIdentity",
    "MailboxHandoffLeaseLost",
    "MailboxHandoffNotFound",
    "MailboxHandoffDiscoveryCursor",
    "MailboxHandoffDiscoveryPage",
    "MailboxHandoffRecord",
    "MailboxHandoffState",
    "MailboxHandoffTarget",
]
