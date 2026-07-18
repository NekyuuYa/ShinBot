"""Diagnostic helpers shared by durable Agent runtime tests."""

from __future__ import annotations

import asyncio

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.registry import AgentSessionActorRegistry
from shinbot.persistence import DatabaseManager


async def wait_for_session_actor_idle(
    database: DatabaseManager,
    registry: AgentSessionActorRegistry,
    key: SessionKey,
    *,
    checkpoint: str = "session actor idle checkpoint",
    timeout_seconds: float = 5.0,
) -> None:
    """Wait for an Actor test checkpoint without masking a durable retry failure.

    Actor tests frequently use a frozen clock. If an event fails, its retry can
    remain unavailable forever until the test advances that clock, so an
    unbounded idle wait conceals the original failure. Include enough durable
    mailbox and outbox evidence for the timeout to be actionable.
    """

    try:
        await asyncio.wait_for(registry.wait_idle(key), timeout=timeout_seconds)
    except TimeoutError as exc:
        actor = registry.actor_for(key)
        with database.connect() as conn:
            mailbox_rows = conn.execute(
                """
                SELECT event_id, kind, status, attempt_count, available_at,
                       lease_owner, lease_until, last_error
                FROM agent_session_mailbox
                WHERE profile_id = ? AND session_id = ?
                  AND status IN ('pending', 'processing')
                ORDER BY mailbox_id
                LIMIT 8
                """,
                (key.profile_id, key.session_id),
            ).fetchall()
            effect_rows = conn.execute(
                """
                SELECT effect_id, kind, status, attempt_count, available_at,
                       lease_owner, lease_until, last_error
                FROM agent_effect_outbox
                WHERE profile_id = ? AND session_id = ?
                  AND status IN ('pending', 'processing')
                ORDER BY effect_seq
                LIMIT 8
                """,
                (key.profile_id, key.session_id),
            ).fetchall()
        mailbox_evidence = [dict(row) for row in mailbox_rows]
        effect_evidence = [dict(row) for row in effect_rows]
        raise AssertionError(
            "Actor did not become idle at "
            f"{checkpoint}; profile_id={key.profile_id!r}; "
            f"session_id={key.session_id!r}; "
            f"actor_error={actor.last_error if actor else None!r}; "
            f"mailbox={mailbox_evidence!r}; effects={effect_evidence!r}"
        ) from exc
