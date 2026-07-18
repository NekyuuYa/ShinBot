"""Dormant registry for actors bound to one fenced ownership incarnation.

Unlike :mod:`registry`, this type never accepts a bare ``SessionKey``. It is a
lower-level future wake primitive only: no ingress, recovery discovery,
handoff-claim validation, or production publication is performed here.
"""

from __future__ import annotations

import asyncio
import math
import uuid

from shinbot.agent.runtime.session_actor.actor import (
    AgentSessionActor,
    SessionActorStore,
    SessionEventHandler,
)
from shinbot.agent.runtime.session_actor.effect_contracts import EffectContractAuthority
from shinbot.core.dispatch.actor_v2_admission import ActorV2AdmissionFenceError
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipError
from shinbot.core.dispatch.fenced_wake import (
    FencedMailboxWakeDisposition,
    FencedMailboxWakeReceipt,
    FencedMailboxWakeRequest,
)
from shinbot.core.dispatch.fenced_wake_target_lease import (
    FencedActorExecutionBinding,
    FencedWakeTargetLeaseError,
)


class FencedSessionActorRegistry:
    """Supervise actors by immutable fenced ownership request.

    This registry deliberately cannot satisfy ``FencedMailboxHandoffPort``.
    A later handoff target must first validate the leased mailbox and target
    incarnation, then may use this request-bound registry as one internal wake
    primitive. Constructing this registry does not publish it anywhere.
    """

    def __init__(
        self,
        *,
        store: SessionActorStore,
        handler: SessionEventHandler,
        worker_id: str | None = None,
        retry_delay_seconds: float = 1.0,
        max_attempts: int = 5,
    ) -> None:
        """Create an inactive registry for one stable persistence domain.

        Args:
            store: Actor store that validates an ownership binding per call.
            handler: Pure reducer used by every request-bound actor.
            worker_id: Optional process-stable worker-id prefix.
            retry_delay_seconds: Actor retry delay for non-binding failures.
            max_attempts: Maximum infrastructure attempts per mailbox event.

        Raises:
            ValueError: If lifecycle values are invalid or the store cannot
                name a stable persistence domain.
            TypeError: If the store has no sealed effect contract authority.
        """

        if max_attempts < 1:
            raise ValueError("max_attempts must be at least one")
        normalized_retry_delay_seconds = float(retry_delay_seconds)
        if (
            not math.isfinite(normalized_retry_delay_seconds)
            or normalized_retry_delay_seconds < 0
        ):
            raise ValueError("retry_delay_seconds must be finite and non-negative")
        persistence_domain = store.persistence_domain
        if persistence_domain is None:
            raise ValueError("fenced actor registry store persistence_domain must not be None")
        authority = store.effect_contract_authority
        if not isinstance(authority, EffectContractAuthority):
            raise TypeError("fenced actor registry store must expose an EffectContractAuthority")
        if not authority.sealed:
            raise TypeError("fenced actor registry effect authority must be sealed")

        self._store = store
        self._handler = handler
        self._persistence_domain = persistence_domain
        self._effect_contract_authority = authority
        self._worker_id = str(worker_id or f"fenced-session-actor-registry:{uuid.uuid4().hex}")
        self._retry_delay_seconds = normalized_retry_delay_seconds
        self._max_attempts = max_attempts
        self._actors: dict[FencedMailboxWakeRequest, AgentSessionActor] = {}
        self._lifecycle_lock = asyncio.Lock()
        self._closed = False
        self._shutdown_complete = False

    @property
    def closed(self) -> bool:
        """Return whether this registry has stopped accepting fenced wakes."""

        return self._closed

    @property
    def shutdown_complete(self) -> bool:
        """Return whether all request-bound actors confirmed shutdown."""

        return self._shutdown_complete

    @property
    def persistence_domain(self) -> object:
        """Return the exact store domain without publishing a wake target."""

        current = self._store.persistence_domain
        if current is not self._persistence_domain:
            raise RuntimeError("fenced actor registry store changed persistence domain")
        return self._persistence_domain

    @property
    def effect_contract_authority(self) -> EffectContractAuthority:
        """Return the immutable authority shared by every owned actor."""

        current = self._store.effect_contract_authority
        if current is not self._effect_contract_authority:
            raise RuntimeError("fenced actor registry store changed effect authority")
        return self._effect_contract_authority

    def actor_for(self, request: FencedMailboxWakeRequest) -> AgentSessionActor | None:
        """Return one actor only by its full immutable ownership request."""

        if not isinstance(request, FencedMailboxWakeRequest):
            raise TypeError("request must be a FencedMailboxWakeRequest")
        return self._actors.get(request)

    async def wake_fenced(
        self,
        request: FencedMailboxWakeRequest,
    ) -> FencedMailboxWakeReceipt:
        """Start or wake exactly one current fenced ownership incarnation.

        A stale fence, changed ownership generation, or closed registry returns
        a typed ``STALE`` receipt. Other store failures remain visible because a
        target must not reinterpret malformed durable state as ordinary stale
        ownership.
        """

        self._validate_request(request)
        return await self._wake(request, execution_binding=None)

    async def wake_leased(
        self,
        execution_binding: FencedActorExecutionBinding,
    ) -> FencedMailboxWakeReceipt:
        """Wake one actor only while its target lease remains current.

        This remains a lower-level primitive, not a mailbox-handoff target. A
        future target must validate the complete handoff claim before calling
        it. The additional binding prevents an actor created by an expired
        target incarnation from continuing to claim or commit session work.
        """

        if not isinstance(execution_binding, FencedActorExecutionBinding):
            raise TypeError("execution_binding must be a FencedActorExecutionBinding")
        self._validate_request(execution_binding.request)
        return await self._wake(
            execution_binding.request,
            execution_binding=execution_binding,
        )

    async def _wake(
        self,
        request: FencedMailboxWakeRequest,
        *,
        execution_binding: FencedActorExecutionBinding | None,
    ) -> FencedMailboxWakeReceipt:
        """Start or wake one actor with an optional target-lease capability."""

        async with self._lifecycle_lock:
            if self._closed:
                return self._receipt(request, FencedMailboxWakeDisposition.STALE)
            self._validate_store_binding()
            actor = self._actors.get(request)
            if actor is not None:
                if actor.closed or not actor.started:
                    self._actors.pop(request, None)
                    actor = None
                if actor is not None:
                    current_binding = actor.execution_binding
                    if current_binding is None and execution_binding is not None:
                        return self._receipt(request, FencedMailboxWakeDisposition.STALE)
                    if current_binding is not None and execution_binding is None:
                        return self._receipt(request, FencedMailboxWakeDisposition.STALE)
                    if (
                        current_binding is not None
                        and execution_binding is not None
                        and not current_binding.has_same_authority(execution_binding)
                    ):
                        await actor.shutdown(drain=False)
                        self._actors.pop(request, None)
                        actor = None
            if actor is not None:
                try:
                    await self._store.load(
                        request.key,
                        ownership_binding=request,
                        execution_binding=execution_binding,
                    )
                except (
                    ActorV2AdmissionFenceError,
                    AgentRuntimeOwnershipError,
                    FencedWakeTargetLeaseError,
                ):
                    await actor.shutdown(drain=False)
                    self._actors.pop(request, None)
                    return self._receipt(request, FencedMailboxWakeDisposition.STALE)
                self._validate_store_binding()
                actor.wake()
                return self._receipt(request, FencedMailboxWakeDisposition.ACCEPTED)

            actor = AgentSessionActor(
                key=request.key,
                store=self._store,
                handler=self._handler,
                worker_id=self._actor_worker_id(request),
                retry_delay_seconds=self._retry_delay_seconds,
                max_attempts=self._max_attempts,
                ownership_binding=request,
                execution_binding=execution_binding,
            )
            try:
                await actor.start()
            except (
                ActorV2AdmissionFenceError,
                AgentRuntimeOwnershipError,
                FencedWakeTargetLeaseError,
            ):
                await actor.shutdown(drain=False)
                return self._receipt(request, FencedMailboxWakeDisposition.STALE)
            self._validate_store_binding()
            self._actors[request] = actor
            return self._receipt(request, FencedMailboxWakeDisposition.ACCEPTED)

    async def shutdown(self, *, drain: bool = False) -> None:
        """Stop every bound actor without publishing or recovering new work.

        Args:
            drain: Whether actors may finish already-valid mailbox work before
                stopping. Future cutover lifecycles should normally use
                ``False`` until they own a stronger stop proof.
        """

        async with self._lifecycle_lock:
            if self._shutdown_complete:
                return
            self._closed = True
            actors = tuple(self._actors.values())
            if actors:
                await asyncio.gather(
                    *(actor.shutdown(drain=drain) for actor in actors),
                    return_exceptions=False,
                )
            self._shutdown_complete = True

    def _validate_request(self, request: FencedMailboxWakeRequest) -> None:
        """Require a fully fenced request before any actor may be created."""

        if not isinstance(request, FencedMailboxWakeRequest):
            raise TypeError("request must be a FencedMailboxWakeRequest")
        if not request.has_admission_fence:
            raise ValueError("fenced actor registry requires an admission-fenced request")

    def _validate_store_binding(self) -> None:
        """Check immutable composition identities at every wake boundary."""

        _ = self.persistence_domain
        _ = self.effect_contract_authority

    def _actor_worker_id(self, request: FencedMailboxWakeRequest) -> str:
        """Build a diagnostic worker id that cannot hide its owner generation."""

        return (
            f"{self._worker_id}:{request.key.profile_id}:{request.key.session_id}:"
            f"{request.ownership_generation}:{request.admission_fence_id}:"
            f"{request.admission_fence_generation}"
        )

    @staticmethod
    def _receipt(
        request: FencedMailboxWakeRequest,
        disposition: FencedMailboxWakeDisposition,
    ) -> FencedMailboxWakeReceipt:
        """Build one typed result without exposing a mutable actor instance."""

        return FencedMailboxWakeReceipt(request=request, disposition=disposition)


__all__ = ["FencedSessionActorRegistry"]
