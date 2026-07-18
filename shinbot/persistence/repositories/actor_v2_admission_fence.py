"""Durable reservation primitive for a future fenced Actor v2 admission."""

from __future__ import annotations

import hashlib
import math
import time
import uuid
from collections.abc import Callable
from sqlite3 import Connection, Row

from shinbot.core.dispatch.actor_v2_admission import (
    ActorV2AdmissionFence,
    ActorV2AdmissionFenceConflict,
    ActorV2AdmissionFenceExpired,
    ActorV2AdmissionFenceNotFound,
    ActorV2AdmissionFenceStatus,
    ActorV2AdmissionGrant,
)
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.persistence.repositories.base import Repository


class ActorV2AdmissionFenceRepository(Repository):
    """Persist opaque, generation-fenced reservations without admitting traffic.

    This repository deliberately does not publish a wake target, choose runtime
    ownership, or route a message. It provides the durable primitive later core
    commit paths must consume together.
    """

    def __init__(
        self,
        db: object,
        *,
        clock: Callable[[], float] | None = None,
        fence_id_factory: Callable[[], str] | None = None,
        holder_token_factory: Callable[[], str] | None = None,
    ) -> None:
        """Initialize one repository with injectable clock and opaque identifiers."""

        super().__init__(db)
        self._clock = clock or time.time
        self._fence_id_factory = fence_id_factory or (lambda: uuid.uuid4().hex)
        self._holder_token_factory = holder_token_factory or (lambda: uuid.uuid4().hex)

    def reserve(
        self,
        key: SessionKey,
        *,
        holder_id: str,
        ttl_seconds: float,
    ) -> ActorV2AdmissionGrant:
        """Reserve one currently unowned session for a future Actor v2 cutover.

        The reservation is intentionally stricter than a lock retry: historical
        fence evidence remains durable and blocks a later clean-domain canary.
        A caller must use a new database domain instead of silently recycling a
        lost or revoked reservation.
        """

        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            return self.reserve_clean_in_transaction(
                conn,
                key,
                holder_id=holder_id,
                ttl_seconds=ttl_seconds,
                now=self._clock(),
            )

    def reserve_clean_in_transaction(
        self,
        conn: Connection,
        key: SessionKey,
        *,
        holder_id: str,
        ttl_seconds: float,
        now: float | None = None,
    ) -> ActorV2AdmissionGrant:
        """Reserve one clean session inside a caller-owned SQLite transaction.

        This is the transactional counterpart used by a cutover journal to
        commit the durable reservation and its phase transition together. It
        retains the public :meth:`reserve` preconditions and does not authorize
        reuse, legacy migration, or automatic ownership selection.
        """

        if not isinstance(conn, Connection):
            raise TypeError("conn must be a sqlite3 Connection")
        if not isinstance(key, SessionKey):
            raise TypeError("key must be a SessionKey")
        holder = _required_identifier(holder_id, "holder_id")
        ttl = _positive_finite(ttl_seconds, "ttl_seconds")
        observed_at = _finite_time(self._clock() if now is None else now)
        fence_id = _required_identifier(self._fence_id_factory(), "fence_id")
        holder_token = _required_identifier(self._holder_token_factory(), "holder_token")
        expires_at = observed_at + ttl

        self._db.actor_v2_canary_isolation_leases.require_no_active_isolation_in_transaction(
            conn
        )
        self._db.actor_v2_legacy_recovery_gate.enter_fenced_only_in_transaction(conn)
        existing = _select_fence(conn, key)
        if existing is not None:
            raise ActorV2AdmissionFenceConflict(
                "Actor v2 admission fence history already exists for "
                f"{key.profile_id}:{key.session_id}"
            )
        ownership = conn.execute(
            """
            SELECT mode, status
            FROM agent_session_runtime_ownership
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
        if ownership is not None:
            raise ActorV2AdmissionFenceConflict(
                "Actor v2 admission fence requires an unowned session"
            )
        conn.execute(
            """
            INSERT INTO agent_session_actor_v2_admission_fences (
                profile_id, session_id, fence_id, generation,
                holder_token_digest, holder_id, status, expires_at,
                created_at, updated_at, committed_at, revoked_at,
                revocation_reason
            ) VALUES (?, ?, ?, 1, ?, ?, 'reserved', ?, ?, ?, NULL, NULL, '')
            """,
            (
                key.profile_id,
                key.session_id,
                fence_id,
                _token_digest(holder_token),
                holder,
                expires_at,
                observed_at,
                observed_at,
            ),
        )
        self._db.actor_v2_legacy_recovery_gate.require_fenced_only_in_transaction(
            conn
        )
        fence = ActorV2AdmissionFence(
            key=key,
            fence_id=fence_id,
            generation=1,
            status=ActorV2AdmissionFenceStatus.RESERVED,
            holder_id=holder,
            expires_at=expires_at,
            created_at=observed_at,
            updated_at=observed_at,
        )
        return ActorV2AdmissionGrant(fence=fence, holder_token=holder_token)

    def get(self, key: SessionKey) -> ActorV2AdmissionFence | None:
        """Return a token-free durable fence snapshot for one session key."""

        with self.connect() as conn:
            row = _select_fence(conn, key)
        return _fence_from_row(row) if row is not None else None

    def renew(
        self,
        grant: ActorV2AdmissionGrant,
        *,
        ttl_seconds: float,
    ) -> ActorV2AdmissionGrant:
        """Extend one live holder's fence without changing its generation."""

        ttl = _positive_finite(ttl_seconds, "ttl_seconds")
        now = _finite_time(self._clock())
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = self.require_live_holder_in_transaction(conn, grant, now=now)
            expires_at = now + ttl
            updated = conn.execute(
                """
                UPDATE agent_session_actor_v2_admission_fences
                SET expires_at = ?, updated_at = ?
                WHERE profile_id = ?
                  AND session_id = ?
                  AND fence_id = ?
                  AND generation = ?
                  AND holder_token_digest = ?
                """,
                (
                    expires_at,
                    now,
                    current.key.profile_id,
                    current.key.session_id,
                    current.fence_id,
                    current.generation,
                    _token_digest(grant.holder_token),
                ),
            )
            if updated.rowcount != 1:
                raise ActorV2AdmissionFenceConflict("admission fence changed while renewing")
        renewed = ActorV2AdmissionFence(
            key=current.key,
            fence_id=current.fence_id,
            generation=current.generation,
            status=current.status,
            holder_id=current.holder_id,
            expires_at=expires_at,
            created_at=current.created_at,
            updated_at=now,
            committed_at=current.committed_at,
            revoked_at=current.revoked_at,
            revocation_reason=current.revocation_reason,
        )
        return ActorV2AdmissionGrant(fence=renewed, holder_token=grant.holder_token)

    def revoke(self, grant: ActorV2AdmissionGrant, *, reason: str) -> ActorV2AdmissionFence:
        """Durably revoke a holder's reservation without falling back to legacy."""

        normalized_reason = _required_identifier(reason, "reason")
        now = _finite_time(self._clock())
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = self._require_holder_in_transaction(conn, grant)
            if current.status is ActorV2AdmissionFenceStatus.REVOKED:
                return current
            updated = conn.execute(
                """
                UPDATE agent_session_actor_v2_admission_fences
                SET status = 'revoked', revoked_at = ?, revocation_reason = ?, updated_at = ?
                WHERE profile_id = ?
                  AND session_id = ?
                  AND fence_id = ?
                  AND generation = ?
                  AND holder_token_digest = ?
                  AND status IN ('reserved', 'committed')
                """,
                (
                    now,
                    normalized_reason,
                    now,
                    current.key.profile_id,
                    current.key.session_id,
                    current.fence_id,
                    current.generation,
                    _token_digest(grant.holder_token),
                ),
            )
            if updated.rowcount != 1:
                raise ActorV2AdmissionFenceConflict("admission fence changed while revoking")
        return ActorV2AdmissionFence(
            key=current.key,
            fence_id=current.fence_id,
            generation=current.generation,
            status=ActorV2AdmissionFenceStatus.REVOKED,
            holder_id=current.holder_id,
            expires_at=current.expires_at,
            created_at=current.created_at,
            updated_at=now,
            committed_at=current.committed_at,
            revoked_at=now,
            revocation_reason=normalized_reason,
        )

    def require_live_holder_in_transaction(
        self,
        conn: Connection,
        grant: ActorV2AdmissionGrant,
        *,
        now: float | None = None,
    ) -> ActorV2AdmissionFence:
        """Require a matching reserved or committed grant in a caller transaction."""

        current = self._require_holder_in_transaction(conn, grant)
        observed_at = _finite_time(self._clock() if now is None else now)
        if current.status is ActorV2AdmissionFenceStatus.REVOKED:
            raise ActorV2AdmissionFenceConflict("admission fence has been revoked")
        if current.expired_at(observed_at):
            raise ActorV2AdmissionFenceExpired("admission fence has expired")
        return current

    def require_reserved_in_transaction(
        self,
        conn: Connection,
        grant: ActorV2AdmissionGrant,
        *,
        now: float | None = None,
    ) -> ActorV2AdmissionFence:
        """Require a live reservation before actor ownership is committed."""

        current = self.require_live_holder_in_transaction(conn, grant, now=now)
        if current.status is not ActorV2AdmissionFenceStatus.RESERVED:
            raise ActorV2AdmissionFenceConflict(
                "admission fence is not awaiting Actor v2 ownership commitment"
            )
        return current

    def commit_in_transaction(
        self,
        conn: Connection,
        grant: ActorV2AdmissionGrant,
        *,
        now: float | None = None,
    ) -> ActorV2AdmissionFence:
        """Mark a live reserved fence committed inside an ownership transaction."""

        current = self.require_reserved_in_transaction(conn, grant, now=now)
        committed_at = _finite_time(self._clock() if now is None else now)
        updated = conn.execute(
            """
            UPDATE agent_session_actor_v2_admission_fences
            SET status = 'committed', committed_at = ?, updated_at = ?
            WHERE profile_id = ?
              AND session_id = ?
              AND fence_id = ?
              AND generation = ?
              AND holder_token_digest = ?
              AND status = 'reserved'
            """,
            (
                committed_at,
                committed_at,
                current.key.profile_id,
                current.key.session_id,
                current.fence_id,
                current.generation,
                _token_digest(grant.holder_token),
            ),
        )
        if updated.rowcount != 1:
            raise ActorV2AdmissionFenceConflict("admission fence changed while committing")
        return ActorV2AdmissionFence(
            key=current.key,
            fence_id=current.fence_id,
            generation=current.generation,
            status=ActorV2AdmissionFenceStatus.COMMITTED,
            holder_id=current.holder_id,
            expires_at=current.expires_at,
            created_at=current.created_at,
            updated_at=committed_at,
            committed_at=committed_at,
        )

    def require_committed_in_transaction(
        self,
        conn: Connection,
        *,
        key: SessionKey,
        fence_id: str,
        generation: int,
        now: float | None = None,
    ) -> ActorV2AdmissionFence:
        """Require one live committed fence by token-free durable identity."""

        normalized_id = _required_identifier(fence_id, "fence_id")
        normalized_generation = _positive_generation(generation)
        row = _select_fence(conn, key)
        if row is None:
            raise ActorV2AdmissionFenceNotFound("admission fence does not exist")
        current = _fence_from_row(row)
        if current.fence_id != normalized_id or current.generation != normalized_generation:
            raise ActorV2AdmissionFenceConflict("admission fence generation changed")
        if current.status is not ActorV2AdmissionFenceStatus.COMMITTED:
            raise ActorV2AdmissionFenceConflict("admission fence is not committed")
        observed_at = _finite_time(self._clock() if now is None else now)
        if current.expired_at(observed_at):
            raise ActorV2AdmissionFenceExpired("admission fence has expired")
        return current

    def require_legacy_admission_open_in_transaction(
        self,
        conn: Connection,
        key: SessionKey,
    ) -> None:
        """Reject implicit legacy ownership while any fence history exists."""

        row = _select_fence(conn, key)
        if row is None:
            return
        from shinbot.core.dispatch.actor_v2_admission import ActorV2AdmissionFenceReserved

        raise ActorV2AdmissionFenceReserved(_fence_from_row(row))

    def require_actor_v2_claim_open_in_transaction(
        self,
        conn: Connection,
        key: SessionKey,
    ) -> None:
        """Reject an unfenced first Actor v2 claim while fence history exists."""

        row = _select_fence(conn, key)
        if row is None:
            return
        raise ActorV2AdmissionFenceConflict(
            "Actor v2 ownership claim requires the matching admission grant"
        )

    def _require_holder_in_transaction(
        self,
        conn: Connection,
        grant: ActorV2AdmissionGrant,
    ) -> ActorV2AdmissionFence:
        """Require matching raw-holder capability without exposing its digest."""

        if not isinstance(grant, ActorV2AdmissionGrant):
            raise TypeError("admission fence operation requires an ActorV2AdmissionGrant")
        row = _select_fence(conn, grant.fence.key)
        if row is None:
            raise ActorV2AdmissionFenceNotFound("admission fence does not exist")
        current = _fence_from_row(row)
        if (
            current.fence_id != grant.fence.fence_id
            or current.generation != grant.fence.generation
            or str(row["holder_token_digest"]) != _token_digest(grant.holder_token)
        ):
            raise ActorV2AdmissionFenceConflict("admission fence holder token is stale")
        return current


def _select_fence(conn: Connection, key: SessionKey) -> Row | None:
    """Return one fence row inside the caller's transaction."""

    return conn.execute(
        """
        SELECT *
        FROM agent_session_actor_v2_admission_fences
        WHERE profile_id = ? AND session_id = ?
        """,
        (key.profile_id, key.session_id),
    ).fetchone()


def _fence_from_row(row: Row) -> ActorV2AdmissionFence:
    """Decode one durable token-free fence snapshot."""

    return ActorV2AdmissionFence(
        key=SessionKey(str(row["profile_id"]), str(row["session_id"])),
        fence_id=str(row["fence_id"]),
        generation=int(row["generation"]),
        status=ActorV2AdmissionFenceStatus(str(row["status"])),
        holder_id=str(row["holder_id"]),
        expires_at=float(row["expires_at"]),
        created_at=float(row["created_at"]),
        updated_at=float(row["updated_at"]),
        committed_at=(float(row["committed_at"]) if row["committed_at"] is not None else None),
        revoked_at=(float(row["revoked_at"]) if row["revoked_at"] is not None else None),
        revocation_reason=str(row["revocation_reason"]),
    )


def _token_digest(token: str) -> str:
    """Return a fixed digest for one opaque holder capability."""

    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _required_identifier(value: object, field_name: str) -> str:
    """Normalize a non-empty opaque identifier."""

    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _positive_finite(value: object, field_name: str) -> float:
    """Validate a positive finite duration."""

    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be positive and finite")
    numeric = float(value)
    if not math.isfinite(numeric) or numeric <= 0:
        raise ValueError(f"{field_name} must be positive and finite")
    return numeric


def _positive_generation(value: object) -> int:
    """Validate a positive non-boolean fence generation."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("admission fence generation must be positive")
    return value


def _finite_time(value: object) -> float:
    """Validate a finite wall-clock instant supplied by the repository clock."""

    if isinstance(value, bool):
        raise ValueError("admission fence clock must return a finite time")
    numeric = float(value)
    if not math.isfinite(numeric):
        raise ValueError("admission fence clock must return a finite time")
    return numeric


__all__ = ["ActorV2AdmissionFenceRepository"]
