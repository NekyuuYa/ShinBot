"""Terminal reconciliation for external actions that never reached dispatch.

The outer durable effect owns retry policy. A receipt remains retryable while
that effect can still be claimed, but a failed outer effect can no longer
reach the action handler. Leaving a prepared or pre-dispatch-rejected receipt
in that state would permanently block ownership migration despite there being
no possible adapter dispatch.
"""

from __future__ import annotations

import math
import sqlite3
import time

from shinbot.core.dispatch.agent_identity import SessionKey


def reconcile_abandoned_before_dispatch_receipts(
    conn: sqlite3.Connection,
    key: SessionKey,
    *,
    now: float | None = None,
) -> int:
    """Terminally abandon receipts fenced by a matching failed parent effect.

    This transition is deliberately narrow. It only applies to states that
    have durable proof they never crossed the adapter-dispatch boundary, and
    only after the exact parent effect reached its terminal ``failed`` state.
    ``executing`` and ``unknown`` rows are never reconciled here because they
    may already have caused externally visible work.

    The caller must use the same transaction that validates an ownership
    transition. The SQL repeats every immutable parent identity in its
    ``EXISTS`` fence, so an altered or unrelated failed effect fails closed.

    Args:
        conn: Open SQLite connection owned by the transition transaction.
        key: Stable profile-scoped session identity.
        now: Optional committed timestamp. Defaults to the current time.

    Returns:
        Number of receipts changed to ``abandoned_before_dispatch``.
    """

    committed_at = _nonnegative_finite(
        time.time() if now is None else now,
        field_name="now",
    )
    updated = conn.execute(
        """
        UPDATE agent_external_action_receipts AS receipt
        SET status = 'abandoned_before_dispatch',
            lease_until = NULL,
            settled_at = ?,
            updated_at = ?
        WHERE receipt.profile_id = ?
          AND receipt.session_id = ?
          AND receipt.status IN ('prepared', 'rejected_before_dispatch')
          AND EXISTS (
              SELECT 1
              FROM agent_effect_outbox AS effect
              WHERE effect.effect_id = receipt.effect_id
                AND effect.idempotency_key = receipt.idempotency_key
                AND effect.profile_id = receipt.profile_id
                AND effect.session_id = receipt.session_id
                AND effect.ownership_generation = receipt.ownership_generation
                AND effect.operation_id = receipt.operation_id
                AND effect.kind = receipt.action_kind
                AND effect.contract_version = receipt.contract_version
                AND effect.payload_json = receipt.request_json
                AND effect.status = 'failed'
          )
        """,
        (committed_at, committed_at, key.profile_id, key.session_id),
    )
    return int(updated.rowcount)


def _nonnegative_finite(value: object, *, field_name: str) -> float:
    """Normalize a finite persisted timestamp."""

    try:
        normalized = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be finite and non-negative") from exc
    if not math.isfinite(normalized) or normalized < 0:
        raise ValueError(f"{field_name} must be finite and non-negative")
    return normalized


__all__ = ["reconcile_abandoned_before_dispatch_receipts"]
