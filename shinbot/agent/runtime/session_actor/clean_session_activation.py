"""Fail-closed residual-state checks for a future clean Actor v2 activation.

The clean-session activation scope is deliberately narrower than runtime
migration. It may start only in a persistence domain with no Actor v2 history,
so the executor never has to guess whether an unbound historical effect is safe
to ignore or run. Stateful migration and restart recovery require separate
protocols and are not authorized by this preflight.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from shinbot.persistence import DatabaseManager


_ACTOR_V2_RESIDUAL_TABLES: tuple[str, ...] = (
    "agent_session_aggregates",
    "agent_session_mailbox",
    "agent_session_operations",
    "agent_message_ledger_consumptions",
    "agent_message_ledger",
    "agent_review_schedules",
    "agent_state_transitions",
    "agent_review_schedule_events",
    "agent_effect_outbox",
    "agent_historical_effect_terminalizations",
    "agent_review_cancellation_gates",
    "agent_review_execution_runs",
    "agent_model_execution_runs",
    "agent_model_execution_cancellation_gates",
    "agent_session_recovery_cases",
    "agent_session_recovery_findings",
    "agent_session_actor_v2_admission_fences",
    "agent_session_actor_v2_cutover_journal",
    "agent_session_actor_v2_cutover_events",
    "agent_session_actor_v2_legacy_state_handoff_manifests",
    "agent_session_actor_v2_legacy_state_handoff_materializations",
    "agent_session_actor_v2_legacy_state_handoff_finalizations",
    "agent_external_action_receipts",
    "agent_external_action_attempts",
    "agent_route_outbox",
)
"""Actor-only durable tables that make a domain ineligible for clean activation."""


@dataclass(slots=True, frozen=True)
class CleanSessionActivationBlocker:
    """One durable evidence class that rejects clean-session activation."""

    code: str
    count: int

    def __post_init__(self) -> None:
        """Normalize a stable error code and non-negative evidence count."""

        normalized_code = str(self.code or "").strip()
        if not normalized_code:
            raise ValueError("clean-session activation blocker code must not be empty")
        if isinstance(self.count, bool) or not isinstance(self.count, int) or self.count < 1:
            raise ValueError("clean-session activation blocker count must be positive")
        object.__setattr__(self, "code", normalized_code)


@dataclass(slots=True, frozen=True)
class CleanSessionActivationReadiness:
    """Read-only proof that a domain is empty enough for clean Actor startup."""

    blockers: tuple[CleanSessionActivationBlocker, ...] = ()

    def __post_init__(self) -> None:
        """Require a stable, typed set of residual-state observations."""

        blockers = tuple(self.blockers)
        if any(not isinstance(blocker, CleanSessionActivationBlocker) for blocker in blockers):
            raise TypeError("clean-session activation readiness blockers must be typed")
        codes = [blocker.code for blocker in blockers]
        if len(set(codes)) != len(codes):
            raise ValueError("clean-session activation readiness blocker codes must be unique")
        object.__setattr__(self, "blockers", blockers)

    @property
    def permitted(self) -> bool:
        """Return whether no Actor v2 history or pending target was found."""

        return not self.blockers


class CleanSessionActivationPreflight(Protocol):
    """Read-only proof required before clean-session harness activation."""

    @property
    def persistence_domain(self) -> object:
        """Return the exact durable domain inspected by this preflight."""

    async def check(self) -> CleanSessionActivationReadiness:
        """Return a durable empty-domain readiness snapshot."""


class SQLiteCleanSessionActivationPreflight:
    """Inspect one SQLite domain for Actor v2 history without mutating it."""

    def __init__(self, database: DatabaseManager) -> None:
        """Bind the preflight to the exact database used by Actor stores."""

        self._database = database

    @property
    def persistence_domain(self) -> object:
        """Return the database domain inspected by this preflight."""

        return self._database

    async def check(self) -> CleanSessionActivationReadiness:
        """Return a consistent, fail-closed snapshot of residual Actor state."""

        with self._database.connect() as conn:
            conn.execute("BEGIN")
            try:
                blockers = _load_blockers(conn, _ACTOR_V2_RESIDUAL_TABLES)
            finally:
                conn.execute("ROLLBACK")
        return CleanSessionActivationReadiness(blockers=tuple(blockers))


def _load_blockers(
    conn: sqlite3.Connection,
    residual_tables: Sequence[str],
) -> list[CleanSessionActivationBlocker]:
    """Read all fixed actor-only residual counts in one SQLite snapshot."""

    ownership_row = conn.execute(
        """
        SELECT COUNT(*) AS count
        FROM (
            SELECT profile_id, session_id
            FROM agent_session_runtime_ownership
            WHERE mode = 'actor_v2' OR pending_mode = 'actor_v2'
            UNION
            SELECT profile_id, session_id
            FROM agent_session_runtime_ownership_events
            WHERE from_mode = 'actor_v2' OR to_mode = 'actor_v2'
        ) AS actor_v2_ownership_history
        """
    ).fetchone()
    if ownership_row is None:
        raise RuntimeError("clean-session activation ownership query returned no row")
    blockers: list[CleanSessionActivationBlocker] = []
    ownership_count = int(ownership_row["count"])
    if ownership_count:
        blockers.append(
            CleanSessionActivationBlocker(
                code="actor_v2_ownership_history_present",
                count=ownership_count,
            )
        )
    scrub_row = conn.execute(
        """
        SELECT COUNT(*) AS count
        FROM agent_effect_scrub_state
        WHERE last_effect_seq > 0
        """
    ).fetchone()
    if scrub_row is None:
        raise RuntimeError("clean-session activation scrub-state query returned no row")
    scrub_count = int(scrub_row["count"])
    if scrub_count:
        blockers.append(
            CleanSessionActivationBlocker(
                code="actor_v2_residual_agent_effect_scrub_state",
                count=scrub_count,
            )
        )
    for table_name in residual_tables:
        row = conn.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
        if row is None:
            raise RuntimeError(
                "clean-session activation residual query returned no row: " + table_name
            )
        count = int(row["count"])
        if count:
            blockers.append(
                CleanSessionActivationBlocker(
                    code="actor_v2_residual_" + table_name,
                    count=count,
                )
            )
    return blockers


__all__ = [
    "CleanSessionActivationBlocker",
    "CleanSessionActivationPreflight",
    "CleanSessionActivationReadiness",
    "SQLiteCleanSessionActivationPreflight",
]
