"""Durable publication leases for future fenced Actor wake targets."""

from __future__ import annotations

import hashlib
import math
import time
import uuid
from collections.abc import Callable
from sqlite3 import Connection, Row

from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.fenced_wake import FencedMailboxWakeRequest
from shinbot.core.dispatch.fenced_wake_target_lease import (
    FencedWakeTargetLease,
    FencedWakeTargetLeaseConflict,
    FencedWakeTargetLeaseExpired,
    FencedWakeTargetLeaseGrant,
    FencedWakeTargetLeaseLost,
    FencedWakeTargetLeaseStatus,
)
from shinbot.core.dispatch.mailbox_handoff import MailboxHandoffTarget
from shinbot.persistence.repositories.base import Repository

_MAX_LEASE_SECONDS = 300.0


class ActorV2FencedWakeTargetLeaseRepository(Repository):
    """Lease one target incarnation for one durable fenced Actor owner.

    The repository does not publish a process-local target or dispatch a
    mailbox handoff. It only makes a future publication controller prove that
    its target incarnation is current before it accepts a durable handoff.
    """

    def __init__(
        self,
        db: object,
        *,
        clock: Callable[[], float] | None = None,
        holder_token_factory: Callable[[], str] | None = None,
    ) -> None:
        """Initialize a repository with injectable clock and capability source."""

        super().__init__(db)
        self._clock = clock or time.time
        self._holder_token_factory = holder_token_factory or (lambda: uuid.uuid4().hex)

    @property
    def persistence_domain(self) -> object:
        """Return the exact durable domain guarded by publication leases."""

        return self._db

    def get(self, request: FencedMailboxWakeRequest) -> FencedWakeTargetLease | None:
        """Return the token-free record only for one exact owner request.

        The table has one durable row per session key, but that storage layout
        must not let a caller collapse an owner incarnation back to a key.  A
        row belonging to a different fenced request is therefore a conflict,
        not an observable target snapshot for the caller.
        """

        _require_fenced_request(request)
        with self.connect() as conn:
            row = _select_lease(conn, request)
        if row is None:
            return None
        lease = _lease_from_row(row)
        if lease.request != request:
            raise FencedWakeTargetLeaseConflict(
                "wake target lease history belongs to another ownership incarnation"
            )
        return lease

    def acquire(
        self,
        request: FencedMailboxWakeRequest,
        *,
        target: MailboxHandoffTarget,
        ttl_seconds: float,
    ) -> FencedWakeTargetLeaseGrant:
        """Publish one new target incarnation under the current Actor owner.

        An active unexpired publication is exclusive. An expired or released
        row can be replaced only with a different target incarnation, so a
        late prior process cannot resume under the same durable identity.
        """

        _require_fenced_request(request)
        if not isinstance(target, MailboxHandoffTarget):
            raise TypeError("target must be a MailboxHandoffTarget")
        ttl = _bounded_lease_seconds(ttl_seconds)
        token = _required_identifier(self._holder_token_factory(), "holder_token")
        now = _finite_time(self._clock(), "clock")
        expires_at = _lease_expires_at(now, ttl)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._require_active_owner(conn, request)
            row = _select_lease(conn, request)
            if row is None:
                epoch = 1
                conn.execute(
                    """
                    INSERT INTO agent_session_actor_v2_fenced_wake_target_leases (
                        profile_id, session_id,
                        ownership_generation,
                        admission_fence_id, admission_fence_generation,
                        lease_epoch, target_id, target_incarnation_id,
                        holder_token_digest, status, expires_at,
                        created_at, updated_at, released_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, NULL)
                    """,
                    (
                        request.key.profile_id,
                        request.key.session_id,
                        request.ownership_generation,
                        request.admission_fence_id,
                        request.admission_fence_generation,
                        epoch,
                        target.target_id,
                        target.incarnation_id,
                        _token_digest(token),
                        expires_at,
                        now,
                        now,
                    ),
                )
            else:
                current = _lease_from_row(row)
                if current.request != request:
                    raise FencedWakeTargetLeaseConflict(
                        "wake target lease history belongs to another ownership incarnation"
                    )
                if (
                    current.status is FencedWakeTargetLeaseStatus.ACTIVE
                    and not current.expired_at(now)
                ):
                    raise FencedWakeTargetLeaseConflict(
                        "an active wake target lease already exists for this Actor owner"
                    )
                if current.target == target:
                    raise FencedWakeTargetLeaseConflict(
                        "a replacement wake target must use a new incarnation id"
                    )
                epoch = current.lease_epoch + 1
                updated = conn.execute(
                    """
                    UPDATE agent_session_actor_v2_fenced_wake_target_leases
                    SET lease_epoch = ?,
                        target_id = ?,
                        target_incarnation_id = ?,
                        holder_token_digest = ?,
                        status = 'active',
                        expires_at = ?,
                        created_at = ?,
                        updated_at = ?,
                        released_at = NULL
                    WHERE profile_id = ?
                      AND session_id = ?
                      AND lease_epoch = ?
                    """,
                    (
                        epoch,
                        target.target_id,
                        target.incarnation_id,
                        _token_digest(token),
                        expires_at,
                        now,
                        now,
                        request.key.profile_id,
                        request.key.session_id,
                        current.lease_epoch,
                    ),
                )
                if updated.rowcount != 1:
                    raise FencedWakeTargetLeaseConflict(
                        "wake target lease changed while replacing its incarnation"
                    )
        return FencedWakeTargetLeaseGrant(
            lease=FencedWakeTargetLease(
                request=request,
                target=target,
                lease_epoch=epoch,
                status=FencedWakeTargetLeaseStatus.ACTIVE,
                expires_at=expires_at,
                created_at=now,
                updated_at=now,
            ),
            holder_token=token,
        )

    def renew(
        self,
        grant: FencedWakeTargetLeaseGrant,
        *,
        ttl_seconds: float,
    ) -> FencedWakeTargetLeaseGrant:
        """Renew a current target publication without changing its incarnation."""

        ttl = _bounded_lease_seconds(ttl_seconds)
        now = _finite_time(self._clock(), "clock")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = self._require_active_grant(conn, grant, now=now)
            expires_at = _lease_expires_at(now, ttl)
            updated = conn.execute(
                """
                UPDATE agent_session_actor_v2_fenced_wake_target_leases
                SET expires_at = ?, updated_at = ?
                WHERE profile_id = ?
                  AND session_id = ?
                  AND lease_epoch = ?
                  AND holder_token_digest = ?
                  AND status = 'active'
                """,
                (
                    expires_at,
                    now,
                    current.request.key.profile_id,
                    current.request.key.session_id,
                    current.lease_epoch,
                    _token_digest(grant.holder_token),
                ),
            )
            if updated.rowcount != 1:
                raise FencedWakeTargetLeaseLost(
                    "wake target lease changed while renewing"
                )
        return FencedWakeTargetLeaseGrant(
            lease=FencedWakeTargetLease(
                request=current.request,
                target=current.target,
                lease_epoch=current.lease_epoch,
                status=FencedWakeTargetLeaseStatus.ACTIVE,
                expires_at=expires_at,
                created_at=current.created_at,
                updated_at=now,
            ),
            holder_token=grant.holder_token,
        )

    def validate(self, grant: FencedWakeTargetLeaseGrant) -> FencedWakeTargetLease:
        """Validate one target publication at an explicit lifecycle boundary."""

        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            return self._require_active_grant(conn, grant)

    def validate_in_transaction(
        self,
        conn: Connection,
        grant: FencedWakeTargetLeaseGrant,
    ) -> FencedWakeTargetLease:
        """Validate a target publication inside a caller-owned transaction."""

        return self._require_active_grant(conn, grant)

    def release(self, grant: FencedWakeTargetLeaseGrant) -> FencedWakeTargetLease:
        """Mark one exact publication released without reopening legacy routing.

        Release intentionally does not require live Actor ownership: an owner
        may already be revoked, but its process must still retire the target
        identity. A stale holder cannot release a replacement epoch because its
        token digest and epoch must both match.
        """

        now = _finite_time(self._clock(), "clock")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = self._require_grant(conn, grant)
            if current.status is FencedWakeTargetLeaseStatus.RELEASED:
                return current
            updated = conn.execute(
                """
                UPDATE agent_session_actor_v2_fenced_wake_target_leases
                SET status = 'released', updated_at = ?, released_at = ?
                WHERE profile_id = ?
                  AND session_id = ?
                  AND lease_epoch = ?
                  AND holder_token_digest = ?
                  AND status = 'active'
                """,
                (
                    now,
                    now,
                    current.request.key.profile_id,
                    current.request.key.session_id,
                    current.lease_epoch,
                    _token_digest(grant.holder_token),
                ),
            )
            if updated.rowcount != 1:
                raise FencedWakeTargetLeaseLost(
                    "wake target lease changed while releasing"
                )
        return FencedWakeTargetLease(
            request=current.request,
            target=current.target,
            lease_epoch=current.lease_epoch,
            status=FencedWakeTargetLeaseStatus.RELEASED,
            expires_at=current.expires_at,
            created_at=current.created_at,
            updated_at=now,
            released_at=now,
        )

    def _require_active_grant(
        self,
        conn: Connection,
        grant: FencedWakeTargetLeaseGrant,
        *,
        now: float | None = None,
    ) -> FencedWakeTargetLease:
        """Require a matching active unexpired grant and current Actor owner."""

        current = self._require_grant(conn, grant)
        if current.status is not FencedWakeTargetLeaseStatus.ACTIVE:
            raise FencedWakeTargetLeaseLost("wake target lease is no longer active")
        observed_at = _finite_time(self._clock() if now is None else now, "clock")
        if current.expired_at(observed_at):
            raise FencedWakeTargetLeaseExpired("wake target lease has expired")
        self._require_active_owner(conn, current.request)
        return current

    def _require_grant(
        self,
        conn: Connection,
        grant: FencedWakeTargetLeaseGrant,
    ) -> FencedWakeTargetLease:
        """Require that a caller still names the exact target lease epoch."""

        if not isinstance(grant, FencedWakeTargetLeaseGrant):
            raise TypeError("grant must be a FencedWakeTargetLeaseGrant")
        row = _select_lease(conn, grant.lease.request)
        if row is None:
            raise FencedWakeTargetLeaseLost("wake target lease does not exist")
        current = _lease_from_row(row)
        if (
            current.request != grant.lease.request
            or current.target != grant.lease.target
            or current.lease_epoch != grant.lease.lease_epoch
            or str(row["holder_token_digest"]) != _token_digest(grant.holder_token)
        ):
            raise FencedWakeTargetLeaseLost(
                "wake target lease no longer belongs to this target incarnation"
            )
        return current

    def _require_active_owner(
        self,
        conn: Connection,
        request: FencedMailboxWakeRequest,
    ) -> None:
        """Require the same active Actor owner and committed admission fence."""

        self._db.agent_runtime_ownership.require_actor_v2_in_transaction(
            conn,
            request.key,
            expected_generation=request.ownership_generation,
            expected_admission_fence_id=request.admission_fence_id,
            expected_admission_fence_generation=request.admission_fence_generation,
        )


def _select_lease(conn: Connection, request: FencedMailboxWakeRequest) -> Row | None:
    """Read the one current publication row for a stable session key."""

    return conn.execute(
        """
        SELECT *
        FROM agent_session_actor_v2_fenced_wake_target_leases
        WHERE profile_id = ? AND session_id = ?
        """,
        (request.key.profile_id, request.key.session_id),
    ).fetchone()


def _lease_from_row(row: Row) -> FencedWakeTargetLease:
    """Decode a token-free persisted publication row."""

    try:
        request = FencedMailboxWakeRequest(
            key=SessionKey(
                str(row["profile_id"]),
                str(row["session_id"]),
            ),
            ownership_generation=int(row["ownership_generation"]),
            admission_fence_id=str(row["admission_fence_id"]),
            admission_fence_generation=int(row["admission_fence_generation"]),
        )
        target = MailboxHandoffTarget(
            target_id=str(row["target_id"]),
            incarnation_id=str(row["target_incarnation_id"]),
        )
        return FencedWakeTargetLease(
            request=request,
            target=target,
            lease_epoch=int(row["lease_epoch"]),
            status=FencedWakeTargetLeaseStatus(str(row["status"])),
            expires_at=float(row["expires_at"]),
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
            released_at=(
                float(row["released_at"])
                if row["released_at"] is not None
                else None
            ),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise FencedWakeTargetLeaseConflict(
            "wake target lease contains invalid durable state"
        ) from exc


def _require_fenced_request(request: FencedMailboxWakeRequest) -> None:
    """Require a complete fenced owner identity before opening a lease slot."""

    if not isinstance(request, FencedMailboxWakeRequest):
        raise TypeError("request must be a FencedMailboxWakeRequest")
    if not request.has_admission_fence:
        raise ValueError("wake target lease requires an admission-fenced request")


def _required_identifier(value: object, field_name: str) -> str:
    """Normalize one non-empty opaque capability field."""

    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _bounded_lease_seconds(value: object) -> float:
    """Require a bounded positive publication interval."""

    if isinstance(value, bool):
        raise ValueError("ttl_seconds must be finite and positive")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized <= 0 or normalized > _MAX_LEASE_SECONDS:
        raise ValueError(f"ttl_seconds must be finite, positive, and at most {_MAX_LEASE_SECONDS}")
    return normalized


def _finite_time(value: object, field_name: str) -> float:
    """Require one finite durable timestamp."""

    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be finite")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"{field_name} must be finite")
    return normalized


def _lease_expires_at(now: float, ttl_seconds: float) -> float:
    """Calculate one representable future expiry without clock-wrap ambiguity."""

    expires_at = _finite_time(now + ttl_seconds, "lease expiry")
    if expires_at <= now:
        raise ValueError("ttl_seconds cannot produce a future lease expiry")
    return expires_at


def _token_digest(token: str) -> str:
    """Return a fixed digest without persisting a live holder capability."""

    return hashlib.sha256(token.encode("utf-8")).hexdigest()


__all__ = ["ActorV2FencedWakeTargetLeaseRepository"]
