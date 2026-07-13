"""Single-writer actor for one profile-scoped Agent session mailbox."""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Callable
from inspect import isawaitable
from typing import Protocol

from shinbot.agent.runtime.session_actor.aggregate import (
    AgentSessionAggregate,
    SessionKey,
)
from shinbot.agent.runtime.session_actor.effect_contracts import (
    EffectContractAuthority,
)
from shinbot.agent.runtime.session_actor.events import (
    ClaimedSessionEvent,
    EventEnqueueResult,
    SessionEventEnvelope,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.recovery import (
    RECOVERY_DELIVERY_EVENT_KIND,
    RECOVERY_DELIVERY_EVENT_SOURCE,
)
from shinbot.agent.runtime.session_actor.recovery_commit import (
    RecoveryDeliveryClaimLost,
)
from shinbot.agent.runtime.session_actor.transition_validation import (
    validate_session_transition,
)

logger = logging.getLogger(__name__)

SessionEventHandler = Callable[
    [AgentSessionAggregate, SessionEventEnvelope],
    SessionTransition,
]


class SessionActorStore(Protocol):
    """Durable mailbox and aggregate operations required by a session actor."""

    @property
    def persistence_domain(self) -> object:
        """Return the stable identity of the backing transaction domain."""

    @property
    def effect_contract_authority(self) -> EffectContractAuthority:
        """Return the sealed effect authority shared with durable execution."""

    async def enqueue(self, envelope: SessionEventEnvelope) -> EventEnqueueResult:
        """Idempotently persist an event before its actor is awakened."""

    async def ensure(self, key: SessionKey) -> AgentSessionAggregate:
        """Ensure and return the durable aggregate for a session."""

    async def load(self, key: SessionKey) -> AgentSessionAggregate:
        """Load the latest durable aggregate for a session."""

    async def claim_next(
        self,
        key: SessionKey,
        *,
        worker_id: str,
    ) -> ClaimedSessionEvent | None:
        """Atomically lease the next available event for a session."""

    async def commit(
        self,
        claim: ClaimedSessionEvent,
        transition: SessionTransition,
        *,
        expected_revision: int,
    ) -> AgentSessionAggregate:
        """Atomically commit a transition and complete its claimed event."""

    async def release(
        self,
        claim: ClaimedSessionEvent,
        *,
        error: str,
    ) -> None:
        """Release a failed claim back to the durable pending queue."""

    async def fail(
        self,
        claim: ClaimedSessionEvent,
        *,
        error: str,
    ) -> None:
        """Move a claimed poison event into a terminal failed state."""

    async def recover(self, key: SessionKey, *, worker_id: str) -> int:
        """Release expired claims that may be retried by this actor."""

    async def pending_keys(self) -> list[SessionKey]:
        """Return session keys with recoverable pending mailbox events."""


class AgentSessionActor:
    """Drain one durable session mailbox through a single event handler."""

    def __init__(
        self,
        *,
        key: SessionKey,
        store: SessionActorStore,
        handler: SessionEventHandler,
        worker_id: str | None = None,
        retry_delay_seconds: float = 1.0,
        max_attempts: int = 5,
    ) -> None:
        """Initialize an actor without starting its background task.

        Args:
            key: Profile-scoped session identity owned by this actor.
            store: Durable mailbox and aggregate store.
            handler: Pure orchestration handler returning a declarative transition.
            worker_id: Optional durable lease owner identifier.
            retry_delay_seconds: Delay before retrying a released failed event.
            max_attempts: Infrastructure attempts before an event is failed.
        """

        if max_attempts < 1:
            raise ValueError("max_attempts must be at least one")
        self.key = key
        self._store = store
        self._handler = handler
        self._effect_contract_authority = store.effect_contract_authority
        if not isinstance(self._effect_contract_authority, EffectContractAuthority):
            raise TypeError(
                "session actor store must expose an EffectContractAuthority"
            )
        if not self._effect_contract_authority.sealed:
            raise TypeError("session actor effect contract authority must be sealed")
        self.worker_id = str(worker_id or f"session-actor:{uuid.uuid4().hex}")
        self._retry_delay_seconds = max(0.0, float(retry_delay_seconds))
        self._max_attempts = max_attempts
        self._wake_event = asyncio.Event()
        self._idle_event = asyncio.Event()
        self._idle_event.set()
        self._stopped_event = asyncio.Event()
        self._start_lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._retry_handle: asyncio.TimerHandle | None = None
        self._current_claim: ClaimedSessionEvent | None = None
        self._closing = False
        self._drain_on_shutdown = True
        self._started = False
        self._last_error: str | None = None

    @property
    def started(self) -> bool:
        """Return whether the actor background task has been started."""

        return self._started

    @property
    def closed(self) -> bool:
        """Return whether actor shutdown has been requested."""

        return self._closing

    @property
    def last_error(self) -> str | None:
        """Return the latest mailbox processing error, if any."""

        return self._last_error

    async def start(self) -> None:
        """Ensure durable state, recover stale claims, and start draining."""

        async with self._start_lock:
            if self._started:
                return
            if self._closing:
                raise RuntimeError("a closed session actor cannot be restarted")
            await self._store.ensure(self.key)
            await self._store.recover(self.key, worker_id=self.worker_id)
            self._task = asyncio.create_task(
                self._run(),
                name=(
                    "agent-session-actor:"
                    f"{self.key.profile_id or 'default'}:{self.key.session_id}"
                ),
            )
            self._started = True
            self.wake()

    def wake(self) -> None:
        """Notify the actor that durable mailbox work may be available."""

        if self._closing and not self._drain_on_shutdown:
            return
        self._idle_event.clear()
        self._wake_event.set()

    async def wait_idle(self) -> None:
        """Wait until the durable mailbox has been observed empty."""

        await self._idle_event.wait()

    async def shutdown(self, *, drain: bool = True) -> None:
        """Stop the actor, optionally draining all currently durable events.

        A failed event is released before shutdown and remains durable for the
        next recovery pass; graceful shutdown does not retry failures forever.

        Args:
            drain: Whether to finish the current event and drain pending work.
        """

        if self._closing:
            await self._stopped_event.wait()
            return
        self._closing = True
        self._drain_on_shutdown = drain
        self._cancel_retry()
        task = self._task
        if task is None:
            self._started = False
            self._idle_event.set()
            self._stopped_event.set()
            return
        if drain:
            self.wake()
        else:
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def _run(self) -> None:
        try:
            while True:
                await self._wake_event.wait()
                self._wake_event.clear()
                if self._closing and not self._drain_on_shutdown:
                    break
                drained = await self._drain_mailbox()
                if self._closing:
                    break
                if not drained:
                    self._schedule_retry()
        except asyncio.CancelledError:
            claim = self._current_claim
            if claim is not None:
                await self._release_after_cancellation(claim)
            raise
        finally:
            self._cancel_retry()
            self._current_claim = None
            self._started = False
            self._idle_event.set()
            self._stopped_event.set()

    async def _drain_mailbox(self) -> bool:
        while not (self._closing and not self._drain_on_shutdown):
            try:
                claim = await self._store.claim_next(
                    self.key,
                    worker_id=self.worker_id,
                )
            except Exception as exc:
                self._record_error(exc, event_id="", phase="claim")
                return False
            if claim is None:
                if self._wake_event.is_set():
                    continue
                try:
                    pending_keys = await self._store.pending_keys()
                except Exception as exc:
                    self._record_error(exc, event_id="", phase="pending_check")
                    return False
                if self.key in pending_keys:
                    return False
                self._idle_event.set()
                if self._wake_event.is_set():
                    self._idle_event.clear()
                    continue
                return True
            if claim.key != self.key:
                error = RuntimeError("store returned a claim for a different session actor")
                await self._release_failed_claim(claim, error, phase="claim_validation")
                return False

            self._current_claim = claim
            try:
                aggregate = await self._store.load(self.key)
            except asyncio.CancelledError:
                await self._release_after_cancellation(claim)
                self._current_claim = None
                raise
            except Exception as exc:
                terminal = await self._retry_or_fail_claim(
                    claim,
                    exc,
                    phase="load",
                )
                self._current_claim = None
                if terminal:
                    continue
                return False

            try:
                transition = self._handler(aggregate, claim.envelope)
                if isawaitable(transition):
                    close = getattr(transition, "close", None)
                    if callable(close):
                        close()
                    raise TypeError(
                        "session event handlers must be synchronous and return "
                        "declarative SessionTransition values"
                    )
                self._validate_transition(aggregate, transition)
            except Exception as exc:
                terminal = await self._fail_claim(claim, exc, phase="reduce")
                self._current_claim = None
                if terminal:
                    continue
                return False

            try:
                await self._store.commit(
                    claim,
                    transition,
                    expected_revision=aggregate.state_revision,
                )
            except asyncio.CancelledError:
                await self._release_after_cancellation(claim)
                self._current_claim = None
                raise
            except RecoveryDeliveryClaimLost as exc:
                self._record_claim_lost(claim, exc)
                self._current_claim = None
                return False
            except Exception as exc:
                terminal = await self._retry_or_fail_claim(
                    claim,
                    exc,
                    phase="commit",
                )
                self._current_claim = None
                if terminal:
                    continue
                return False
            self._current_claim = None
            self._last_error = None
        return True

    def _validate_transition(
        self,
        current: AgentSessionAggregate,
        transition: SessionTransition,
    ) -> None:
        validate_session_transition(
            current,
            transition,
            effect_contract_authority=self._effect_contract_authority,
        )

    async def _release_failed_claim(
        self,
        claim: ClaimedSessionEvent,
        exc: BaseException,
        *,
        phase: str,
    ) -> None:
        if self._preserve_unproven_typed_recovery(claim, exc, phase=phase):
            return
        self._record_error(exc, event_id=claim.envelope.event_id, phase=phase)
        try:
            await self._store.release(claim, error=self._last_error or type(exc).__name__)
        except Exception as release_exc:
            self._record_error(
                release_exc,
                event_id=claim.envelope.event_id,
                phase="release",
            )

    async def _retry_or_fail_claim(
        self,
        claim: ClaimedSessionEvent,
        exc: BaseException,
        *,
        phase: str,
    ) -> bool:
        if self._preserve_unproven_typed_recovery(claim, exc, phase=phase):
            return False
        if claim.attempt_count >= self._max_attempts:
            return await self._fail_claim(claim, exc, phase=f"{phase}_exhausted")
        await self._release_failed_claim(claim, exc, phase=phase)
        return False

    async def _fail_claim(
        self,
        claim: ClaimedSessionEvent,
        exc: BaseException,
        *,
        phase: str,
    ) -> bool:
        if self._preserve_unproven_typed_recovery(claim, exc, phase=phase):
            return False
        self._record_error(exc, event_id=claim.envelope.event_id, phase=phase)
        try:
            await self._store.fail(
                claim,
                error=self._last_error or type(exc).__name__,
            )
        except Exception as fail_exc:
            self._record_error(
                fail_exc,
                event_id=claim.envelope.event_id,
                phase="dead_letter",
            )
            try:
                await self._store.release(
                    claim,
                    error=self._last_error or type(fail_exc).__name__,
                )
            except Exception as release_exc:
                self._record_error(
                    release_exc,
                    event_id=claim.envelope.event_id,
                    phase="release_after_dead_letter_failure",
                )
            return False
        return True

    async def _release_after_cancellation(self, claim: ClaimedSessionEvent) -> None:
        release_task = asyncio.create_task(
            self._store.release(claim, error="actor_cancelled"),
            name=f"agent-session-actor-release:{claim.envelope.event_id}",
        )
        try:
            await asyncio.shield(release_task)
        except asyncio.CancelledError:
            await release_task
        except Exception as exc:
            self._record_error(
                exc,
                event_id=claim.envelope.event_id,
                phase="release_after_cancel",
            )

    def _record_error(self, exc: BaseException, *, event_id: str, phase: str) -> None:
        self._last_error = f"{type(exc).__name__}: {exc}"
        logger.exception(
            "Agent session actor processing failed",
            extra={
                "profile_id": self.key.profile_id,
                "session_id": self.key.session_id,
                "event_id": event_id,
                "phase": phase,
            },
            exc_info=(type(exc), exc, exc.__traceback__),
        )

    def _record_claim_lost(
        self,
        claim: ClaimedSessionEvent,
        exc: RecoveryDeliveryClaimLost,
    ) -> None:
        """Record a lost recovery claim without mutating another owner's work."""

        self._last_error = f"{type(exc).__name__}: {exc}"
        logger.warning(
            "Agent session actor lost a typed recovery claim",
            extra={
                "profile_id": self.key.profile_id,
                "session_id": self.key.session_id,
                "event_id": claim.envelope.event_id,
                "phase": "commit_claim_lost",
                "recovery_claim_code": exc.code,
            },
        )

    def _preserve_unproven_typed_recovery(
        self,
        claim: ClaimedSessionEvent,
        exc: BaseException,
        *,
        phase: str,
    ) -> bool:
        """Keep a typed delivery out of generic retry and dead-letter handling.

        A scanner-owned recovery event may only change durable state through the
        coordinator's raw-proof transaction. Before that proof completes, an
        actor cannot safely release, fail, or advance it using ordinary mailbox
        semantics. A future scanner pass can record the raw finding or issue a
        proven blocker; this actor leaves the held claim unchanged.
        """

        envelope = claim.envelope
        if (
            envelope.kind != RECOVERY_DELIVERY_EVENT_KIND
            or envelope.source != RECOVERY_DELIVERY_EVENT_SOURCE
        ):
            return False
        self._last_error = f"{type(exc).__name__}: {exc}"
        logger.warning(
            "Agent session actor preserved an unproven typed recovery delivery",
            extra={
                "profile_id": self.key.profile_id,
                "session_id": self.key.session_id,
                "event_id": envelope.event_id,
                "phase": f"typed_recovery_{phase}",
                "recovery_error_type": type(exc).__name__,
            },
            exc_info=(type(exc), exc, exc.__traceback__),
        )
        return True

    def _schedule_retry(self) -> None:
        if self._closing or self._retry_handle is not None:
            return
        loop = asyncio.get_running_loop()

        def _wake_for_retry() -> None:
            self._retry_handle = None
            self.wake()

        self._retry_handle = loop.call_later(
            self._retry_delay_seconds,
            _wake_for_retry,
        )

    def _cancel_retry(self) -> None:
        handle = self._retry_handle
        self._retry_handle = None
        if handle is not None:
            handle.cancel()


__all__ = [
    "AgentSessionActor",
    "SessionActorStore",
    "SessionEventHandler",
]
