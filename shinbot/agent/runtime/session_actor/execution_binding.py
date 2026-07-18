"""Shared target-lease validation for fenced Actor v2 execution boundaries."""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.fenced_wake_target_lease import FencedActorExecutionBinding

if TYPE_CHECKING:
    from shinbot.persistence.engine import DatabaseManager


def require_live_execution_binding_in_transaction(
    database: DatabaseManager,
    conn: sqlite3.Connection,
    execution_binding: FencedActorExecutionBinding | None,
    *,
    key: SessionKey | None = None,
    ownership_generation: int | None = None,
) -> None:
    """Validate a scoped target lease alongside one caller-owned transaction.

    The caller remains responsible for proving its domain-specific ownership,
    effect claim, receipt claim, or execution witness. This helper only keeps
    an already-scoped Actor v2 execution boundary from silently degrading back
    to a key or ownership-generation check after its target incarnation ends.

    Args:
        database: Durable domain that owns the target-lease repository.
        conn: Active caller-owned SQLite transaction.
        execution_binding: Exact target capability, or ``None`` for legacy
            unscoped execution.
        key: Optional exact session expected by the boundary.
        ownership_generation: Optional exact Actor v2 generation expected by
            the boundary.

    Raises:
        FencedWakeTargetLeaseError: If the target capability is stale, expired,
            released, or names a no-longer-active Actor owner.
        ValueError: If the supplied binding widens or changes the caller's
            exact session or ownership generation.
    """

    if execution_binding is None:
        return
    if not isinstance(execution_binding, FencedActorExecutionBinding):
        raise TypeError("execution_binding must be a FencedActorExecutionBinding")
    request = execution_binding.request
    if key is not None and request.key != key:
        raise ValueError("execution_binding key does not match execution boundary")
    if ownership_generation is not None:
        if isinstance(ownership_generation, bool) or ownership_generation < 1:
            raise ValueError("execution ownership_generation must be positive")
        if request.ownership_generation != ownership_generation:
            raise ValueError(
                "execution_binding generation does not match execution boundary"
            )
    database.actor_v2_fenced_wake_target_leases.validate_in_transaction(
        conn,
        execution_binding.target_lease,
    )


__all__ = ["require_live_execution_binding_in_transaction"]
