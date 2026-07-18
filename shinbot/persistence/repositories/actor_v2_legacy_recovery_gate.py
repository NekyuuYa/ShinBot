"""Durable interlock between legacy broad recovery and Actor v2 admission."""

from __future__ import annotations

import hashlib
import math
import time
import uuid
from collections.abc import Callable
from sqlite3 import Connection, Row

from shinbot.core.dispatch.legacy_recovery_gate import (
    LegacyRecoveryGateBlocked,
    LegacyRecoveryGateError,
    LegacyRecoveryGateMode,
    LegacyRecoveryGateSnapshot,
    LegacyRecoveryPermit,
    LegacyRecoveryPermitLost,
)
from shinbot.persistence.repositories.base import Repository


class ActorV2LegacyRecoveryGateRepository(Repository):
    """Own the database-wide, fail-closed legacy broad-recovery interlock.

    The gate has no automatic expiry.  If a process dies while it owns a legacy
    recovery permit, future Actor v2 reservations remain blocked until an
    explicit administrative recovery path proves that the holder is gone.
    That loss of availability is intentional: allowing an old key-only
    recovery to continue after a lease expiry would reintroduce the admission
    fence race this repository prevents.
    """

    def __init__(
        self,
        db: object,
        *,
        clock: Callable[[], float] | None = None,
        holder_token_factory: Callable[[], str] | None = None,
    ) -> None:
        """Initialize the gate repository with injectable durable identities."""

        super().__init__(db)
        self._clock = clock or time.time
        self._holder_token_factory = holder_token_factory or (lambda: uuid.uuid4().hex)

    @property
    def persistence_domain(self) -> object:
        """Return the exact database domain protected by this gate."""

        return self._db

    def snapshot(self) -> LegacyRecoveryGateSnapshot:
        """Return the current token-free gate state."""

        with self.connect() as conn:
            return _snapshot_from_row(self._require_row(conn))

    def acquire_legacy_recovery(self, *, holder_id: str) -> LegacyRecoveryPermit:
        """Acquire the only permit that may authorize broad legacy recovery.

        Only a lifecycle-owning controller may acquire this permit. It must
        retain the capability until all actors created by its broad recovery
        path have stopped, then release it after their shutdown proof. This
        repository deliberately does not invoke a recovery target itself, so a
        short caller cannot turn a permit into an unsafe key-only wake.
        """

        holder = _required_identifier(holder_id, "holder_id")
        token = _required_identifier(self._holder_token_factory(), "holder_token")
        now = _finite_time(self._clock(), "clock")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._db.actor_v2_canary_isolation_leases.require_no_active_isolation_in_transaction(
                conn
            )
            row = self._require_row(conn)
            current = _snapshot_from_row(row)
            if current.mode is not LegacyRecoveryGateMode.LEGACY_OPEN:
                raise LegacyRecoveryGateBlocked(
                    "legacy broad recovery is unavailable while gate mode is "
                    f"{current.mode.value}"
                )
            next_epoch = current.epoch + 1
            updated = conn.execute(
                """
                UPDATE agent_runtime_legacy_recovery_gate
                SET mode = 'legacy_recovery_active',
                    epoch = ?,
                    holder_id = ?,
                    holder_token_digest = ?,
                    activated_at = ?,
                    updated_at = ?
                WHERE gate_id = 1
                  AND mode = 'legacy_open'
                  AND epoch = ?
                  AND holder_id = ''
                  AND holder_token_digest = ''
                  AND activated_at IS NULL
                """,
                (
                    next_epoch,
                    holder,
                    _token_digest(token),
                    now,
                    now,
                    current.epoch,
                ),
            )
            if updated.rowcount != 1:
                raise LegacyRecoveryGateBlocked(
                    "legacy recovery gate changed while acquiring recovery permit"
                )
        return LegacyRecoveryPermit(
            epoch=next_epoch,
            holder_id=holder,
            holder_token=token,
        )

    def release_legacy_recovery(self, permit: LegacyRecoveryPermit) -> None:
        """Release one exact permit back to ``legacy_open``.

        A lost permit is deliberately not treated as success: silently opening
        a gate after another process has changed it would violate the durable
        ownership proof.
        """

        if not isinstance(permit, LegacyRecoveryPermit):
            raise TypeError("permit must be a LegacyRecoveryPermit")
        now = _finite_time(self._clock(), "clock")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._require_row(conn)
            updated = conn.execute(
                """
                UPDATE agent_runtime_legacy_recovery_gate
                SET mode = 'legacy_open',
                    holder_id = '',
                    holder_token_digest = '',
                    activated_at = NULL,
                    updated_at = ?
                WHERE gate_id = 1
                  AND mode = 'legacy_recovery_active'
                  AND epoch = ?
                  AND holder_id = ?
                  AND holder_token_digest = ?
                """,
                (
                    now,
                    permit.epoch,
                    permit.holder_id,
                    _token_digest(permit.holder_token),
                ),
            )
            if updated.rowcount != 1:
                raise LegacyRecoveryPermitLost(
                    "legacy recovery permit no longer owns the durable gate"
                )

    def validate_legacy_recovery_permit(self, permit: LegacyRecoveryPermit) -> None:
        """Validate an active permit at a controller lifecycle boundary."""

        if not isinstance(permit, LegacyRecoveryPermit):
            raise TypeError("permit must be a LegacyRecoveryPermit")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self.validate_legacy_recovery_permit_in_transaction(conn, permit)

    def enter_fenced_only_in_transaction(
        self,
        conn: Connection,
    ) -> LegacyRecoveryGateSnapshot:
        """Irreversibly disable legacy broad recovery in an admission transaction.

        This method must be called in the same write transaction as the first
        successful admission-fence reservation.  SQLite's writer serialization
        then gives one total order: either recovery owns the gate and admission
        fails, or admission flips the gate and recovery cannot start.
        """

        current = _snapshot_from_row(self._require_row(conn))
        if current.mode is LegacyRecoveryGateMode.FENCED_ONLY:
            return current
        if current.mode is LegacyRecoveryGateMode.LEGACY_RECOVERY_ACTIVE:
            raise LegacyRecoveryGateBlocked(
                "Actor v2 admission is blocked by an active legacy recovery permit"
            )
        now = _finite_time(self._clock(), "clock")
        next_epoch = current.epoch + 1
        updated = conn.execute(
            """
            UPDATE agent_runtime_legacy_recovery_gate
            SET mode = 'fenced_only',
                epoch = ?,
                holder_id = '',
                holder_token_digest = '',
                activated_at = NULL,
                updated_at = ?
            WHERE gate_id = 1
              AND mode = 'legacy_open'
              AND epoch = ?
              AND holder_id = ''
              AND holder_token_digest = ''
              AND activated_at IS NULL
            """,
            (next_epoch, now, current.epoch),
        )
        if updated.rowcount != 1:
            raise LegacyRecoveryGateBlocked(
                "legacy recovery gate changed while Actor v2 admission was starting"
            )
        return LegacyRecoveryGateSnapshot(
            mode=LegacyRecoveryGateMode.FENCED_ONLY,
            epoch=next_epoch,
            updated_at=now,
        )

    def require_fenced_only_in_transaction(self, conn: Connection) -> None:
        """Require the irreversible admission boundary at a caller's final gate."""

        current = _snapshot_from_row(self._require_row(conn))
        if current.mode is not LegacyRecoveryGateMode.FENCED_ONLY:
            raise LegacyRecoveryGateError(
                "Actor v2 admission requires the durable fenced_only recovery gate"
            )

    def require_no_legacy_recovery_in_transaction(self, conn: Connection) -> None:
        """Reject a canary lifecycle while broad legacy recovery is active."""

        current = _snapshot_from_row(self._require_row(conn))
        if current.mode is LegacyRecoveryGateMode.LEGACY_RECOVERY_ACTIVE:
            raise LegacyRecoveryGateBlocked(
                "Actor v2 canary isolation is blocked by an active legacy recovery permit"
            )

    def validate_legacy_recovery_permit_in_transaction(
        self,
        conn: Connection,
        permit: LegacyRecoveryPermit,
    ) -> None:
        """Require that a guarded target still holds its exact permit.

        No current runtime target invokes this method yet. It is exposed for a
        future lifecycle-owning controller to make every broad discovery read
        and write fail closed if the permit is ever replaced or administratively
        fenced.
        """

        if not isinstance(permit, LegacyRecoveryPermit):
            raise TypeError("permit must be a LegacyRecoveryPermit")
        row = self._require_row(conn)
        current = _snapshot_from_row(row)
        if (
            current.mode is not LegacyRecoveryGateMode.LEGACY_RECOVERY_ACTIVE
            or current.epoch != permit.epoch
            or current.holder_id != permit.holder_id
            or str(row["holder_token_digest"]) != _token_digest(permit.holder_token)
        ):
            raise LegacyRecoveryPermitLost(
                "legacy recovery permit no longer matches the durable gate"
            )

    @staticmethod
    def _require_row(conn: Connection) -> Row:
        """Return the singleton row or fail closed on schema/data corruption."""

        row = conn.execute(
            "SELECT * FROM agent_runtime_legacy_recovery_gate WHERE gate_id = 1"
        ).fetchone()
        if row is None:
            raise LegacyRecoveryGateError("legacy recovery gate singleton is missing")
        return row


def _snapshot_from_row(row: Row) -> LegacyRecoveryGateSnapshot:
    """Decode the singleton row without exposing its capability digest."""

    try:
        return LegacyRecoveryGateSnapshot(
            mode=LegacyRecoveryGateMode(str(row["mode"])),
            epoch=int(row["epoch"]),
            holder_id=str(row["holder_id"]),
            activated_at=(
                float(row["activated_at"])
                if row["activated_at"] is not None
                else None
            ),
            updated_at=float(row["updated_at"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise LegacyRecoveryGateError("legacy recovery gate contains invalid durable state") from exc


def _required_identifier(value: object, field_name: str) -> str:
    """Normalize a non-empty opaque gate identifier."""

    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _token_digest(token: str) -> str:
    """Return the fixed digest retained for one opaque holder capability."""

    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _finite_time(value: object, field_name: str) -> float:
    """Require finite repository time before it enters durable state."""

    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be finite")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"{field_name} must be finite")
    return normalized


__all__ = ["ActorV2LegacyRecoveryGateRepository"]
