"""Durable domain-wide isolation leases for future Actor v2 clean canaries."""

from __future__ import annotations

import hashlib
import math
import time
import uuid
from collections.abc import Callable
from sqlite3 import Connection, Row

from shinbot.core.dispatch.actor_v2_canary_isolation import (
    ActorV2CanaryIsolationLeaseBlocked,
    ActorV2CanaryIsolationLeaseConflict,
    ActorV2CanaryIsolationLeaseError,
    ActorV2CanaryIsolationLeaseGrant,
    ActorV2CanaryIsolationLeaseLost,
    ActorV2CanaryIsolationLeaseNotFound,
    ActorV2CanaryIsolationLeaseSnapshot,
    ActorV2CanaryIsolationLeaseStatus,
)
from shinbot.persistence.repositories.base import Repository


class ActorV2CanaryIsolationLeaseRepository(Repository):
    """Own the durable canary-isolation slot for one SQLite persistence domain.

    This is deliberately a non-expiring lease. A stale process may retain a
    live harness after any wall-clock deadline, so automatic takeover would
    weaken the stop-before-release proof. An operator can revoke an exact
    observed epoch only after externally proving the old holder is gone.
    """

    def __init__(
        self,
        db: object,
        *,
        clock: Callable[[], float] | None = None,
        holder_token_factory: Callable[[], str] | None = None,
    ) -> None:
        """Initialize the singleton lease repository.

        Args:
            db: Database domain that owns the singleton isolation slot.
            clock: Injectable timestamp source for durable lifecycle records.
            holder_token_factory: Source for opaque holder capabilities.
        """

        super().__init__(db)
        self._clock = clock or time.time
        self._holder_token_factory = holder_token_factory or (lambda: uuid.uuid4().hex)

    @property
    def persistence_domain(self) -> object:
        """Return the exact database domain guarded by this lease."""

        return self._db

    def get(self) -> ActorV2CanaryIsolationLeaseSnapshot | None:
        """Return the token-free current epoch, or ``None`` before first use."""

        with self.connect() as conn:
            row = _select_lease(conn)
        return None if row is None else _snapshot_from_row(row)

    def acquire(self, *, holder_id: str) -> ActorV2CanaryIsolationLeaseGrant:
        """Acquire the domain-wide canary slot under a fresh opaque capability.

        A released or explicitly revoked epoch can be replaced, but a live
        lease never expires by elapsed time. The legacy broad-recovery gate is
        checked in the same SQLite write transaction so neither controller can
        start beside the other.
        """

        holder = _required_identifier(holder_id, "holder_id")
        token = _required_identifier(self._holder_token_factory(), "holder_token")
        now = _finite_time(self._clock(), "clock")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._db.actor_v2_legacy_recovery_gate.require_no_legacy_recovery_in_transaction(
                conn
            )
            row = _select_lease(conn)
            if row is None:
                epoch = 1
                conn.execute(
                    """
                    INSERT INTO agent_runtime_actor_v2_canary_isolation_leases (
                        lease_id, lease_epoch, holder_id, holder_token_digest,
                        status, created_at, updated_at,
                        released_at, revoked_at, revocation_reason
                    ) VALUES (1, ?, ?, ?, 'active', ?, ?, NULL, NULL, '')
                    """,
                    (epoch, holder, _token_digest(token), now, now),
                )
            else:
                current = _snapshot_from_row(row)
                if current.status is ActorV2CanaryIsolationLeaseStatus.ACTIVE:
                    raise ActorV2CanaryIsolationLeaseConflict(
                        "an active Actor v2 canary isolation lease already exists"
                    )
                epoch = current.lease_epoch + 1
                updated = conn.execute(
                    """
                    UPDATE agent_runtime_actor_v2_canary_isolation_leases
                    SET lease_epoch = ?,
                        holder_id = ?,
                        holder_token_digest = ?,
                        status = 'active',
                        created_at = ?,
                        updated_at = ?,
                        released_at = NULL,
                        revoked_at = NULL,
                        revocation_reason = ''
                    WHERE lease_id = 1
                      AND lease_epoch = ?
                      AND status IN ('released', 'revoked')
                    """,
                    (
                        epoch,
                        holder,
                        _token_digest(token),
                        now,
                        now,
                        current.lease_epoch,
                    ),
                )
                if updated.rowcount != 1:
                    raise ActorV2CanaryIsolationLeaseConflict(
                        "canary isolation lease changed while acquiring a replacement epoch"
                    )
        return ActorV2CanaryIsolationLeaseGrant(
            lease=ActorV2CanaryIsolationLeaseSnapshot(
                lease_epoch=epoch,
                holder_id=holder,
                status=ActorV2CanaryIsolationLeaseStatus.ACTIVE,
                created_at=now,
                updated_at=now,
            ),
            holder_token=token,
        )

    def validate(
        self,
        grant: ActorV2CanaryIsolationLeaseGrant,
    ) -> ActorV2CanaryIsolationLeaseSnapshot:
        """Require that a capability still names the active durable epoch."""

        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            return self.validate_in_transaction(conn, grant)

    def validate_in_transaction(
        self,
        conn: Connection,
        grant: ActorV2CanaryIsolationLeaseGrant,
    ) -> ActorV2CanaryIsolationLeaseSnapshot:
        """Validate one active grant inside a caller-owned transaction."""

        current = self._require_grant_in_transaction(conn, grant)
        if current.status is not ActorV2CanaryIsolationLeaseStatus.ACTIVE:
            raise ActorV2CanaryIsolationLeaseLost(
                "canary isolation lease is no longer active"
            )
        return current

    def release(
        self,
        grant: ActorV2CanaryIsolationLeaseGrant,
    ) -> ActorV2CanaryIsolationLeaseSnapshot:
        """Release one exact holder epoch after its harness has stopped.

        Release is idempotent for the same epoch, including one already
        revoked by an explicit operator path. A stale capability cannot touch
        a replacement epoch because both the token digest and epoch are bound.
        """

        now = _finite_time(self._clock(), "clock")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = self._require_grant_in_transaction(conn, grant)
            if current.status is not ActorV2CanaryIsolationLeaseStatus.ACTIVE:
                return current
            updated = conn.execute(
                """
                UPDATE agent_runtime_actor_v2_canary_isolation_leases
                SET status = 'released',
                    updated_at = ?,
                    released_at = ?,
                    revoked_at = NULL,
                    revocation_reason = ''
                WHERE lease_id = 1
                  AND lease_epoch = ?
                  AND holder_id = ?
                  AND holder_token_digest = ?
                  AND status = 'active'
                """,
                (
                    now,
                    now,
                    current.lease_epoch,
                    current.holder_id,
                    _token_digest(grant.holder_token),
                ),
            )
            if updated.rowcount != 1:
                raise ActorV2CanaryIsolationLeaseLost(
                    "canary isolation lease changed while releasing"
                )
            row = _select_lease(conn)
            if row is None:
                raise ActorV2CanaryIsolationLeaseError(
                    "canary isolation lease disappeared while releasing"
                )
            return _snapshot_from_row(row)

    def revoke(
        self,
        snapshot: ActorV2CanaryIsolationLeaseSnapshot,
        *,
        reason: str,
    ) -> ActorV2CanaryIsolationLeaseSnapshot:
        """Explicitly revoke one observed active epoch after an external stop proof.

        This operation intentionally requires a token-free exact epoch snapshot
        rather than revoking whatever holder happens to be current. It is not
        an automatic recovery mechanism and does not stop a process by itself.
        """

        if not isinstance(snapshot, ActorV2CanaryIsolationLeaseSnapshot):
            raise TypeError("snapshot must be an ActorV2CanaryIsolationLeaseSnapshot")
        normalized_reason = _required_identifier(reason, "reason")
        now = _finite_time(self._clock(), "clock")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = _select_lease(conn)
            if row is None:
                raise ActorV2CanaryIsolationLeaseNotFound(
                    "canary isolation lease does not exist"
                )
            current = _snapshot_from_row(row)
            if (
                current.lease_epoch != snapshot.lease_epoch
                or current.holder_id != snapshot.holder_id
                or current.created_at != snapshot.created_at
            ):
                raise ActorV2CanaryIsolationLeaseConflict(
                    "canary isolation lease changed before revocation"
                )
            if current.status is ActorV2CanaryIsolationLeaseStatus.REVOKED:
                return current
            if current.status is not ActorV2CanaryIsolationLeaseStatus.ACTIVE:
                raise ActorV2CanaryIsolationLeaseConflict(
                    "released canary isolation lease cannot be revoked"
                )
            updated = conn.execute(
                """
                UPDATE agent_runtime_actor_v2_canary_isolation_leases
                SET status = 'revoked',
                    updated_at = ?,
                    released_at = NULL,
                    revoked_at = ?,
                    revocation_reason = ?
                WHERE lease_id = 1
                  AND lease_epoch = ?
                  AND holder_id = ?
                  AND created_at = ?
                  AND status = 'active'
                """,
                (
                    now,
                    now,
                    normalized_reason,
                    current.lease_epoch,
                    current.holder_id,
                    current.created_at,
                ),
            )
            if updated.rowcount != 1:
                raise ActorV2CanaryIsolationLeaseConflict(
                    "canary isolation lease changed while revoking"
                )
            revoked_row = _select_lease(conn)
            if revoked_row is None:
                raise ActorV2CanaryIsolationLeaseError(
                    "canary isolation lease disappeared while revoking"
                )
            return _snapshot_from_row(revoked_row)

    def require_no_active_isolation_in_transaction(self, conn: Connection) -> None:
        """Reject competing Actor v2 admission or recovery while a canary is live."""

        row = _select_lease(conn)
        if row is None:
            return
        current = _snapshot_from_row(row)
        if current.status is ActorV2CanaryIsolationLeaseStatus.ACTIVE:
            raise ActorV2CanaryIsolationLeaseBlocked(
                "Actor v2 work is blocked by an active canary isolation lease"
            )

    def _require_grant_in_transaction(
        self,
        conn: Connection,
        grant: ActorV2CanaryIsolationLeaseGrant,
    ) -> ActorV2CanaryIsolationLeaseSnapshot:
        """Require a grant to name its exact durable lease epoch."""

        if not isinstance(grant, ActorV2CanaryIsolationLeaseGrant):
            raise TypeError("grant must be an ActorV2CanaryIsolationLeaseGrant")
        row = _select_lease(conn)
        if row is None:
            raise ActorV2CanaryIsolationLeaseNotFound(
                "canary isolation lease does not exist"
            )
        current = _snapshot_from_row(row)
        if (
            current.lease_epoch != grant.lease.lease_epoch
            or current.holder_id != grant.lease.holder_id
            or str(row["holder_token_digest"]) != _token_digest(grant.holder_token)
        ):
            raise ActorV2CanaryIsolationLeaseLost(
                "canary isolation lease no longer belongs to this holder epoch"
            )
        return current


def _select_lease(conn: Connection) -> Row | None:
    """Read the single durable isolation row without exposing its token."""

    return conn.execute(
        "SELECT * FROM agent_runtime_actor_v2_canary_isolation_leases WHERE lease_id = 1"
    ).fetchone()


def _snapshot_from_row(row: Row) -> ActorV2CanaryIsolationLeaseSnapshot:
    """Decode one durable singleton row into token-free typed state."""

    try:
        return ActorV2CanaryIsolationLeaseSnapshot(
            lease_epoch=int(row["lease_epoch"]),
            holder_id=str(row["holder_id"]),
            status=ActorV2CanaryIsolationLeaseStatus(str(row["status"])),
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
            released_at=(
                float(row["released_at"])
                if row["released_at"] is not None
                else None
            ),
            revoked_at=(
                float(row["revoked_at"])
                if row["revoked_at"] is not None
                else None
            ),
            revocation_reason=str(row["revocation_reason"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ActorV2CanaryIsolationLeaseConflict(
            "canary isolation lease contains invalid durable state"
        ) from exc


def _required_identifier(value: object, field_name: str) -> str:
    """Normalize one required opaque lease identifier."""

    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _token_digest(token: str) -> str:
    """Return the fixed digest retained for an opaque holder token."""

    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _finite_time(value: object, field_name: str) -> float:
    """Require a finite timestamp before it enters durable state."""

    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be finite")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"{field_name} must be finite")
    return normalized


__all__ = ["ActorV2CanaryIsolationLeaseRepository"]
