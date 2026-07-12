"""Lifecycle registry for profile-scoped Agent session actors."""

from __future__ import annotations

import asyncio
import math
import uuid
from typing import Protocol, runtime_checkable

from shinbot.agent.runtime.session_actor.actor import (
    AgentSessionActor,
    SessionActorStore,
    SessionEventHandler,
)
from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.events import (
    EventEnqueueResult,
    SessionEventEnvelope,
)


@runtime_checkable
class _RecoveryDiscoveryStore(Protocol):
    """Optional startup discovery supported by durable actor stores."""

    async def enqueue_recovery_requests(self) -> int:
        """Enqueue recovery events for orphaned non-idle aggregates."""


class AgentSessionActorRegistry:
    """Create one actor per profile/session key and supervise its mailbox wakeups."""

    def __init__(
        self,
        *,
        store: SessionActorStore,
        handler: SessionEventHandler,
        worker_id: str | None = None,
        retry_delay_seconds: float = 1.0,
        max_attempts: int = 5,
    ) -> None:
        """Initialize an empty actor registry.

        Args:
            store: Shared durable mailbox and aggregate store.
            handler: Event handler used by every session actor.
            worker_id: Optional process-level lease owner prefix.
            retry_delay_seconds: Delay before actors retry released events.
            max_attempts: Infrastructure attempts before an event is failed.
        """

        if max_attempts < 1:
            raise ValueError("max_attempts must be at least one")
        normalized_retry_delay_seconds = float(retry_delay_seconds)
        if (
            not math.isfinite(normalized_retry_delay_seconds)
            or normalized_retry_delay_seconds < 0
        ):
            raise ValueError("retry_delay_seconds must be finite and non-negative")
        self._store = store
        self._handler = handler
        self._worker_id = str(worker_id or f"session-actor-registry:{uuid.uuid4().hex}")
        self._retry_delay_seconds = normalized_retry_delay_seconds
        self._max_attempts = max_attempts
        self._actors: dict[SessionKey, AgentSessionActor] = {}
        self._actors_lock = asyncio.Lock()
        self._lifecycle = asyncio.Condition()
        self._accepting = True
        self._submissions_in_flight = 0

    @property
    def accepting(self) -> bool:
        """Return whether new mailbox submissions are accepted."""

        return self._accepting

    def actor_for(self, key: SessionKey) -> AgentSessionActor | None:
        """Return the currently supervised actor for a key, if started."""

        return self._actors.get(key)

    async def submit(self, envelope: SessionEventEnvelope) -> EventEnqueueResult:
        """Durably enqueue an event and then wake exactly one owning actor."""

        await self._begin_submission()
        try:
            result = await self._store.enqueue(envelope)
            actor = await self._ensure_actor(envelope.key)
            actor.wake()
            return result
        finally:
            await self._finish_submission()

    async def recover(self) -> int:
        """Discover orphaned state, then wake all keys with mailbox work."""

        await self._begin_submission()
        try:
            if isinstance(self._store, _RecoveryDiscoveryStore):
                await self._store.enqueue_recovery_requests()
            keys = await self._store.pending_keys()
            for key in keys:
                actor = await self._ensure_actor(key)
                actor.wake()
            return len(keys)
        finally:
            await self._finish_submission()

    async def wake(self, key: SessionKey) -> None:
        """Wake one actor after its mailbox event has already committed.

        This method deliberately performs no mailbox write. Durable effect
        settlement uses it only after ``complete_with_event`` returns, while a
        failed wake remains recoverable through :meth:`recover`.
        """

        await self._begin_submission()
        try:
            actor = await self._ensure_actor(key)
            actor.wake()
        finally:
            await self._finish_submission()

    async def wait_idle(self, key: SessionKey | None = None) -> None:
        """Wait for one actor or all currently supervised actors to become idle."""

        if key is not None:
            actor = self._actors.get(key)
            if actor is not None:
                await actor.wait_idle()
            return
        actors = list(self._actors.values())
        if actors:
            await asyncio.gather(*(actor.wait_idle() for actor in actors))

    async def shutdown(self, *, drain: bool = True) -> None:
        """Stop accepting submissions and shut down all supervised actors."""

        async with self._lifecycle:
            self._accepting = False
            await self._lifecycle.wait_for(lambda: self._submissions_in_flight == 0)
        async with self._actors_lock:
            actors = list(self._actors.values())
        if actors:
            await asyncio.gather(
                *(actor.shutdown(drain=drain) for actor in actors),
                return_exceptions=False,
            )

    async def _ensure_actor(self, key: SessionKey) -> AgentSessionActor:
        async with self._actors_lock:
            actor = self._actors.get(key)
            if actor is not None:
                return actor
            actor = AgentSessionActor(
                key=key,
                store=self._store,
                handler=self._handler,
                worker_id=(
                    f"{self._worker_id}:{key.profile_id or 'default'}:{key.session_id}"
                ),
                retry_delay_seconds=self._retry_delay_seconds,
                max_attempts=self._max_attempts,
            )
            await actor.start()
            self._actors[key] = actor
            return actor

    async def _begin_submission(self) -> None:
        async with self._lifecycle:
            if not self._accepting:
                raise RuntimeError("actor registry is shutting down")
            self._submissions_in_flight += 1

    async def _finish_submission(self) -> None:
        async with self._lifecycle:
            self._submissions_in_flight -= 1
            if self._submissions_in_flight == 0:
                self._lifecycle.notify_all()


__all__ = ["AgentSessionActorRegistry"]
