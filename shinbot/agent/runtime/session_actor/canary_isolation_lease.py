"""Runtime adapter for the dormant durable Actor v2 canary isolation lease."""

from __future__ import annotations

from sqlite3 import Error as SQLiteError

from shinbot.core.dispatch.actor_v2_canary_isolation import (
    ActorV2CanaryIsolationLeaseError,
    ActorV2CanaryIsolationLeaseGrant,
)
from shinbot.persistence.repositories.actor_v2_canary_isolation_lease import (
    ActorV2CanaryIsolationLeaseRepository,
)


class SQLiteActorV2CanaryIsolationLease:
    """Adapt one durable grant to the canary lifecycle's narrow lease protocol.

    The adapter intentionally has no renewal or takeover method. Its owner must
    stop the harness and call ``release``; an external operator revocation is a
    separate repository operation with an explicit stop proof.
    """

    def __init__(
        self,
        *,
        repository: ActorV2CanaryIsolationLeaseRepository,
        grant: ActorV2CanaryIsolationLeaseGrant,
    ) -> None:
        """Bind one active grant to the repository that owns its domain."""

        if not isinstance(repository, ActorV2CanaryIsolationLeaseRepository):
            raise TypeError("repository must be an ActorV2CanaryIsolationLeaseRepository")
        if not isinstance(grant, ActorV2CanaryIsolationLeaseGrant):
            raise TypeError("grant must be an ActorV2CanaryIsolationLeaseGrant")
        self._repository = repository
        self._grant = grant

    @classmethod
    def acquire(
        cls,
        repository: ActorV2CanaryIsolationLeaseRepository,
        *,
        holder_id: str,
    ) -> SQLiteActorV2CanaryIsolationLease:
        """Acquire one durable canary-isolation grant for a lifecycle owner."""

        if not isinstance(repository, ActorV2CanaryIsolationLeaseRepository):
            raise TypeError("repository must be an ActorV2CanaryIsolationLeaseRepository")
        return cls(repository=repository, grant=repository.acquire(holder_id=holder_id))

    @property
    def persistence_domain(self) -> object:
        """Return the exact durable domain protected by this grant."""

        return self._repository.persistence_domain

    @property
    def active(self) -> bool:
        """Return false for every validation failure so lifecycle checks fail closed."""

        try:
            self._repository.validate(self._grant)
        except (ActorV2CanaryIsolationLeaseError, SQLiteError, TypeError, ValueError):
            return False
        return True

    async def release(self) -> None:
        """Release this exact durable epoch after the lifecycle proves shutdown."""

        self._repository.release(self._grant)


__all__ = ["SQLiteActorV2CanaryIsolationLease"]
